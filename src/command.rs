use std::fmt;
use std::ops::Add;

use aws_config::BehaviorVersion;
use aws_config::meta::credentials::CredentialsProviderChain;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::types::Object;
use futures::Stream;
use humansize::*;

use crate::arg::*;
use crate::filter::Filter;
use crate::function::*;

pub struct AWSPair {
    access: Option<String>,
    secret: Option<String>,
}

pub struct FilterList<'a>(pub Vec<&'a dyn Filter>);

impl<'a> FilterList<'a> {
    pub async fn test_match(&self, object: Object) -> bool {
        for item in &self.0 {
            if !item.filter(&object) {
                return false;
            }
        }

        true
    }

    #[inline]
    pub fn add_filter(mut self, filter: &'a dyn Filter) -> Self {
        self.0.push(filter);
        self
    }

    #[inline]
    pub fn add_filters<F: Filter>(mut self, filters: &'a [F]) -> Self {
        for filter in filters {
            self.0.push(filter);
        }
        self
    }

    pub fn new() -> FilterList<'a> {
        FilterList(Vec::new())
    }
}

impl Default for FilterList<'_> {
    fn default() -> Self {
        Self::new()
    }
}

pub struct Find {
    pub client: Client,
    pub path: S3Path,
    pub limit: Option<usize>,
    pub page_size: i64,
    pub stats: bool,
    pub summarize: bool,
    pub command: Box<dyn RunCommand>,
}

impl Find {
    pub async fn new(
        aws_credentials: AWSPair,
        aws_region: &Region,
        cmd: Option<Cmd>,
        path: S3Path,
        page_size: i64,
        summarize: bool,
        limit: Option<usize>,
    ) -> Self {
        let client = get_s3_client(
            aws_credentials.access,
            aws_credentials.secret,
            aws_region.to_owned(),
        )
        .await;
        let command = cmd.unwrap_or_default().downcast();

        Find {
            client,
            path,
            command,
            page_size,
            summarize,
            limit,
            stats: summarize,
        }
    }

    pub async fn exec(&self, acc: Option<FindStat>, list: Vec<Object>) -> Option<FindStat> {
        let status = acc.map(|stat| stat + &list);

        self.command
            .execute(&self.client, &self.path, &list)
            .await
            .unwrap();
        status
    }

    pub fn to_stream(&self) -> FindStream {
        FindStream {
            client: self.client.clone(),
            path: self.path.clone(),
            token: None,
            page_size: self.page_size,
            initial: true,
        }
    }

    pub async fn from_opts(opts: &FindOpt) -> (Find, FilterList<'_>) {
        let FindOpt {
            aws_access_key,
            aws_secret_key,
            aws_region,
            path,
            cmd,
            page_size,
            summarize,
            limit,
            name,
            iname,
            regex,
            size,
            mtime,
            ..
        } = opts;

        let path = S3Path {
            region: aws_region.to_owned(),
            ..path.clone()
        };

        let find = Find::new(
            AWSPair {
                access: aws_access_key.clone(),
                secret: aws_secret_key.clone(),
            },
            aws_region,
            cmd.clone(),
            path,
            *page_size,
            *summarize,
            *limit,
        )
        .await;

        let filters = FilterList::new()
            .add_filters(name)
            .add_filters(iname)
            .add_filters(mtime)
            .add_filters(regex)
            .add_filters(size);

        (find, filters)
    }
}

pub fn default_stats(summarize: bool) -> Option<FindStat> {
    if summarize {
        Some(FindStat::default())
    } else {
        None
    }
}

pub struct FindStream {
    pub client: Client,
    pub path: S3Path,
    pub token: Option<String>,
    pub page_size: i64,
    pub initial: bool,
}

impl FindStream {
    async fn list(mut self) -> Option<(Vec<Object>, Self)> {
        if !self.initial && self.token.is_none() {
            return None;
        }

        let (token, objects) = self
            .client
            .list_objects_v2()
            .bucket(self.path.bucket.clone())
            .prefix(self.path.prefix.clone().unwrap_or_else(|| "".to_owned()))
            .max_keys(self.page_size as i32)
            .set_continuation_token(self.token)
            .send()
            .await
            .map(|x| (x.next_continuation_token, x.contents))
            .unwrap();

        self.initial = false;
        self.token = token;
        objects.map(|x| (x, self))
    }

    pub fn stream(self) -> impl Stream<Item = Vec<Object>> {
        futures::stream::unfold(self, |s| async { s.list().await })
    }
}

impl PartialEq for FindStream {
    fn eq(&self, other: &Self) -> bool {
        self.path == other.path
            && self.token == other.token
            && self.page_size == other.page_size
            && self.initial == other.initial
    }
}

impl fmt::Debug for FindStream {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "\
FindStream {{
    client,
    path: {:?},
    token: {:?},
    page_size: {},
    initial: {},
}}",
            self.path, self.token, self.page_size, self.initial
        )
    }
}

#[inline]
async fn get_s3_client(
    aws_access_key: Option<String>,
    aws_secret_key: Option<String>,
    region: Region,
) -> Client {
    let region_provider =
        aws_config::meta::region::RegionProviderChain::first_try(region).or_default_provider();

    let shared_config = match (aws_access_key, aws_secret_key) {
        (Some(aws_access_key), Some(aws_secret_key)) => {
            let credentials_provider =
                Credentials::new(aws_access_key, aws_secret_key, None, None, "static");
            aws_config::ConfigLoader::default()
                .behavior_version(BehaviorVersion::v2025_01_17())
                .region(region_provider)
                .credentials_provider(credentials_provider)
                .load()
                .await
        }
        _ => {
            let credentials_provider = CredentialsProviderChain::default_provider().await;
            aws_config::ConfigLoader::default()
                .behavior_version(BehaviorVersion::v2025_01_17())
                .region(region_provider)
                .credentials_provider(credentials_provider)
                .load()
                .await
        }
    };

    Client::new(&shared_config)
}

impl fmt::Display for FindStat {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let file_size = make_format(BINARY);
        writeln!(f)?;
        writeln!(f, "Summary")?;
        writeln!(f, "{:19} {}", "Total files:", &self.total_files)?;
        writeln!(
            f,
            "Total space:        {}",
            file_size(self.total_space as u64),
        )?;
        writeln!(f, "{:19} {}", "Largest file:", &self.max_key)?;
        writeln!(
            f,
            "{:19} {}",
            "Largest file size:",
            file_size(self.max_size.unwrap_or_default() as u64),
        )?;
        writeln!(f, "{:19} {}", "Smallest file:", &self.min_key)?;
        writeln!(f, "{:19} {}", "Smallest file size:", self.min_key,)?;
        writeln!(
            f,
            "{:19} {}",
            "Average file size:",
            file_size(self.average_size as u64),
        )?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FindStat {
    pub total_files: usize,
    pub total_space: i64,
    pub max_size: Option<i64>,
    pub min_size: Option<i64>,
    pub max_key: String,
    pub min_key: String,
    pub average_size: i64,
}

impl Add<&[Object]> for FindStat {
    type Output = FindStat;

    #[allow(clippy::suspicious_arithmetic_impl)]
    fn add(mut self: FindStat, list: &[Object]) -> Self {
        for x in list {
            self.total_files += 1;
            let size = x.size;
            self.total_space += size.unwrap_or_default();

            match self.max_size {
                None => {
                    self.max_size = size;
                    self.max_key = x.key.clone().unwrap_or_default();
                }
                Some(max_size) if max_size <= size.unwrap_or_default() => {
                    self.max_size = size;
                    self.max_key = x.key.clone().unwrap_or_default();
                }
                _ => {}
            }

            match self.min_size {
                None => {
                    self.min_size = size;
                    self.min_key = x.key.clone().unwrap_or_default();
                }
                Some(min_size) if min_size > size.unwrap_or_default() => {
                    self.min_size = size;
                    self.min_key = x.key.clone().unwrap_or_default();
                }
                _ => {}
            }

            self.average_size = self.total_space / (self.total_files as i64);
        }
        self
    }
}

impl Default for FindStat {
    fn default() -> Self {
        FindStat {
            total_files: 0,
            total_space: 0,
            max_size: None,
            min_size: None,
            max_key: "".to_owned(),
            min_key: "".to_owned(),
            average_size: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use glob::Pattern;
    use regex::Regex;

    #[tokio::test]
    async fn test_filter_list_test_match() {
        let object = Object::builder().key("test-object.txt").size(100).build();

        struct AlwaysTrueFilter;
        impl Filter for AlwaysTrueFilter {
            fn filter(&self, _: &Object) -> bool {
                true
            }
        }

        struct AlwaysFalseFilter;
        impl Filter for AlwaysFalseFilter {
            fn filter(&self, _: &Object) -> bool {
                false
            }
        }

        let true_filter = AlwaysTrueFilter;
        let filter_list = FilterList::new()
            .add_filter(&true_filter)
            .add_filter(&true_filter);

        assert!(
            filter_list.test_match(object.clone()).await,
            "all true filters failed"
        );

        let false_filter = AlwaysFalseFilter;
        let filter_list = FilterList::new()
            .add_filter(&true_filter)
            .add_filter(&false_filter);

        assert!(
            !filter_list.test_match(object.clone()).await,
            "one false filter failed"
        );
    }

    #[test]
    fn test_filter_list_new() {
        let name_patterns = vec![Pattern::new("*.txt").unwrap()];
        let iname_globs = vec![InameGlob(Pattern::new("*.TXT").unwrap())];
        let regexs = vec![Regex::new(r"test.*\.txt").unwrap()];
        let sizes = vec![FindSize::Bigger(100)];
        let mtimes = vec![FindTime::Lower(3600 * 24)];

        let filter_list = FilterList::new()
            .add_filters(&name_patterns)
            .add_filters(&iname_globs)
            .add_filters(&regexs)
            .add_filters(&sizes)
            .add_filters(&mtimes);

        assert_eq!(filter_list.0.len(), 5, "it should contains 5 filters");
    }

    #[test]
    fn test_default_stats() {
        let stats = default_stats(true);
        assert!(stats.is_some());

        let stats = default_stats(false);
        assert!(stats.is_none());
    }

    #[tokio::test]
    async fn test_get_s3_client() {
        let client_with_creds = get_s3_client(
            Some("mock_access".to_string()),
            Some("mock_secret".to_string()),
            Region::new("mock-region"),
        )
        .await;

        let client_without_creds = get_s3_client(None, None, Region::new("mock-region")).await;

        assert!(client_with_creds.config().region().is_some());
        assert!(client_without_creds.config().region().is_some());
    }

    #[test]
    fn test_filter_list_default() {
        let filter_list = FilterList::default();

        assert_eq!(
            filter_list.0.len(),
            0,
            "default filter list should be empty"
        );

        let test_filter = Pattern::new("*.txt").unwrap();
        let filter_list = filter_list.add_filter(&test_filter);

        assert_eq!(
            filter_list.0.len(),
            1,
            "should be able to add filters to default list"
        );
    }

    #[test]
    fn test_find_stat_display() {
        let find_stat = FindStat {
            total_files: 42,
            total_space: 1234567890,
            max_size: Some(987654321),
            min_size: Some(123),
            max_key: "largest-file.txt".to_owned(),
            min_key: "smallest-file.txt".to_owned(),
            average_size: 29394474,
        };

        let display_output = find_stat.to_string();

        assert!(display_output.contains("Summary"), "Missing Summary header");
        assert!(
            display_output.contains("Total files:        42"),
            "Incorrect total files count"
        );
        assert!(
            display_output.contains("Total space:"),
            "Missing total space"
        );
        assert!(
            display_output.contains("Largest file:       largest-file.txt"),
            "Incorrect largest file name"
        );
        assert!(
            display_output.contains("Largest file size:"),
            "Missing largest file size"
        );
        assert!(
            display_output.contains("Smallest file:      smallest-file.txt"),
            "Incorrect smallest file name"
        );
        assert!(
            display_output.contains("Smallest file size:"),
            "Missing smallest file size"
        );
        assert!(
            display_output.contains("Average file size:"),
            "Missing average file size"
        );

        assert!(
            display_output.contains("GiB"),
            "Expected binary size format (MiB/GiB)"
        );
    }

    #[test]
    fn test_find_stat_add() {
        let mut stat = FindStat::default();

        let objects = vec![
            Object::builder().key("file1.txt").size(100).build(),
            Object::builder().key("file2.txt").size(200).build(),
            Object::builder().key("file3.txt").size(50).build(),
        ];

        stat = stat + &objects;

        assert_eq!(stat.total_files, 3, "Total files count incorrect");
        assert_eq!(stat.total_space, 350, "Total space incorrect");
        assert_eq!(stat.max_size, Some(200), "Max size incorrect");
        assert_eq!(stat.max_key, "file2.txt", "Max key incorrect");
        assert_eq!(stat.min_size, Some(50), "Min size incorrect");
        assert_eq!(stat.min_key, "file3.txt", "Min key incorrect");
        assert_eq!(stat.average_size, 116, "Average size incorrect");

        let more_objects = vec![
            Object::builder().key("file4.txt").size(500).build(),
            Object::builder().key("file5.txt").size(10).build(),
        ];

        stat = stat + &more_objects;

        assert_eq!(
            stat.total_files, 5,
            "Total files count incorrect after second add"
        );
        assert_eq!(
            stat.total_space, 860,
            "Total space incorrect after second add"
        );
        assert_eq!(
            stat.max_size,
            Some(500),
            "Max size incorrect after second add"
        );
        assert_eq!(
            stat.max_key, "file4.txt",
            "Max key incorrect after second add"
        );
        assert_eq!(
            stat.min_size,
            Some(10),
            "Min size incorrect after second add"
        );
        assert_eq!(
            stat.min_key, "file5.txt",
            "Min key incorrect after second add"
        );
        assert_eq!(
            stat.average_size, 172,
            "Average size incorrect after second add"
        );

        let object_without_size = vec![Object::builder().key("no-size.txt").build()];

        let before_total_space = stat.total_space;
        stat = stat + &object_without_size;

        assert_eq!(
            stat.total_files, 6,
            "Total files should increase even for objects with no size"
        );
        assert_eq!(
            stat.total_space, before_total_space,
            "Total space shouldn't change for object with no size"
        );

        let empty_list: Vec<Object> = vec![];
        let before = stat.clone();
        stat = stat + &empty_list;

        assert_eq!(stat, before, "Adding empty list should not change stats");
    }

    #[tokio::test]
    async fn test_find_exec() {
        let path = S3Path {
            bucket: "test-bucket".to_string(),
            prefix: Some("test-prefix".to_string()),
            region: Region::new("mock-region"),
        };

        let command = DoNothing {};

        let config = aws_sdk_s3::Config::builder()
            .region(Region::new("mock-region"))
            .credentials_provider(Credentials::new("mock", "mock", None, None, "mock"))
            .behavior_version(BehaviorVersion::v2025_01_17())
            .build();

        let client = Client::from_conf(config);

        let find = Find {
            client,
            path,
            limit: None,
            page_size: 1000,
            stats: true,
            summarize: true,
            command: Box::new(command),
        };

        // Create test objects
        let objects = vec![
            Object::builder().key("object1").size(100).build(),
            Object::builder().key("object2").size(200).build(),
        ];

        // Execute find with stats
        let acc = Some(FindStat::default());
        let result = find.exec(acc, objects).await;

        // Verify stats were updated
        assert!(result.is_some());
        let stats = result.unwrap();
        assert_eq!(stats.total_files, 2);
        assert_eq!(stats.total_space, 300);
    }

    #[tokio::test]
    async fn test_find_stream_list() {
        let path = S3Path {
            bucket: "test-bucket".to_string(),
            prefix: Some("test-prefix".to_string()),
            region: Region::new("mock-region"),
        };

        let config = aws_sdk_s3::Config::builder()
            .region(Region::new("mock-region"))
            .credentials_provider(Credentials::new("mock", "mock", None, None, "mock"))
            .behavior_version(BehaviorVersion::v2025_01_17())
            .build();

        let client = Client::from_conf(config);

        let find_stream = FindStream {
            client: client.clone(),
            path: path.clone(),
            token: None,
            page_size: 1000,
            initial: true,
        };

        assert_eq!(find_stream.token, None);
        assert_eq!(find_stream.page_size, 1000);
        assert!(find_stream.initial);
        assert_eq!(find_stream.path, path);

        let same_stream = FindStream {
            client: client.clone(),
            path: path.clone(),
            token: None,
            page_size: 1000,
            initial: true,
        };

        assert_eq!(find_stream, same_stream);

        let different_stream = FindStream {
            client,
            path: path.clone(),
            token: Some("token".to_string()),
            page_size: 1000,
            initial: true,
        };

        assert_ne!(find_stream, different_stream);
    }

    #[tokio::test]
    async fn test_find_stream_stream_compile() {
        let path = S3Path {
            bucket: "test-bucket".to_string(),
            prefix: Some("test-prefix".to_string()),
            region: Region::new("mock-region"),
        };

        let config = aws_sdk_s3::Config::builder()
            .region(Region::new("mock-region"))
            .credentials_provider(Credentials::new("mock", "mock", None, None, "mock"))
            .behavior_version(BehaviorVersion::v2025_01_17())
            .build();

        let client = Client::from_conf(config);

        let find_stream = FindStream {
            client,
            path,
            token: None,
            page_size: 1000,
            initial: true,
        };

        let _stream = find_stream.stream();
    }
}
