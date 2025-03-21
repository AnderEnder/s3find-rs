use std::fmt;
use std::ops::Add;

use aws_config::BehaviorVersion;
use aws_config::meta::credentials::CredentialsProviderChain;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{Credentials, Region};

use futures::Stream;
use glob::Pattern;
use humansize::*;
use regex::Regex;

use crate::arg::*;
use crate::filter::Filter;
use crate::function::*;

pub struct AWSPair {
    access: Option<String>,
    secret: Option<String>,
}

pub struct FilterList<'a>(pub Vec<&'a dyn Filter>);

impl<'a> FilterList<'a> {
    pub async fn test_match(&self, object: aws_sdk_s3::types::Object) -> bool {
        for item in &self.0 {
            if !item.filter(&object) {
                return false;
            }
        }

        true
    }

    pub fn new(
        name: &'a [Pattern],
        iname: &'a [InameGlob],
        regex: &'a [Regex],
        size: &'a [FindSize],
        mtime: &'a [FindTime],
    ) -> FilterList<'a> {
        let mut list: Vec<&dyn Filter> = Vec::new();

        for filter in name {
            list.push(filter);
        }

        for filter in iname {
            list.push(filter);
        }

        for filter in regex {
            list.push(filter);
        }

        for filter in size {
            list.push(filter);
        }

        for filter in mtime {
            list.push(filter);
        }

        FilterList(list)
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

    pub async fn exec(
        &self,
        acc: Option<FindStat>,
        list: Vec<aws_sdk_s3::types::Object>,
    ) -> Option<FindStat> {
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

        let filters = FilterList::new(name, iname, regex, size, mtime);

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
    async fn list(mut self) -> Option<(Vec<aws_sdk_s3::types::Object>, Self)> {
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

    pub fn stream(self) -> impl Stream<Item = Vec<aws_sdk_s3::types::Object>> {
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

impl Add<&[aws_sdk_s3::types::Object]> for FindStat {
    type Output = FindStat;

    #[allow(clippy::suspicious_arithmetic_impl)]
    fn add(mut self: FindStat, list: &[aws_sdk_s3::types::Object]) -> Self {
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
    use async_trait::async_trait;
    use aws_sdk_s3::types::Object;

    // Simple mock for S3 client - we won't use actual AWS SDK test utilities
    // which require feature flags
    struct MockS3ClientBuilder {}

    impl MockS3ClientBuilder {
        fn new() -> Self {
            Self {}
        }

        fn build(self) -> Client {
            let conf = aws_sdk_s3::Config::builder()
                .region(Region::new("mock-region"))
                .credentials_provider(Credentials::new("mock", "mock", None, None, "mock"))
                .behavior_version(BehaviorVersion::v2025_01_17())
                .build();

            Client::from_conf(conf)
        }
    }

    #[tokio::test]
    async fn test_filter_list_test_match() {
        // Create a test object
        let object = Object::builder().key("test-object.txt").size(100).build();

        // Create a test filter that always returns true
        struct AlwaysTrueFilter;
        impl Filter for AlwaysTrueFilter {
            fn filter(&self, _: &Object) -> bool {
                true
            }
        }

        // Create a test filter that always returns false
        struct AlwaysFalseFilter;
        impl Filter for AlwaysFalseFilter {
            fn filter(&self, _: &Object) -> bool {
                false
            }
        }

        // Test with all true filters
        let true_filter = AlwaysTrueFilter;
        let filter_list = FilterList(vec![&true_filter, &true_filter]);
        assert!(filter_list.test_match(object.clone()).await);

        // Test with one false filter
        let false_filter = AlwaysFalseFilter;
        let filter_list = FilterList(vec![&true_filter, &false_filter]);
        assert!(!filter_list.test_match(object.clone()).await);
    }

    #[test]
    fn test_filter_list_new() {
        // Create test filters
        let name_patterns = vec![Pattern::new("*.txt").unwrap()];
        let iname_globs = vec![InameGlob(Pattern::new("*.TXT").unwrap())];
        let regexs = vec![Regex::new(r"test.*\.txt").unwrap()];
        // Directly create FindSize instance instead of using parse
        let sizes = vec![FindSize::Bigger(100)];
        // Directly create FindTime instance
        let mtimes = vec![FindTime::Lower(3600 * 24)];

        // Create filter list
        let filter_list = FilterList::new(&name_patterns, &iname_globs, &regexs, &sizes, &mtimes);

        // Verify the filter list contains all filters
        assert_eq!(filter_list.0.len(), 5);
    }

    #[tokio::test]
    async fn test_find_exec() {
        // We don't need to use credentials in tests
        let path = S3Path {
            bucket: "test-bucket".to_string(),
            prefix: Some("test-prefix".to_string()),
            region: Region::new("mock-region"),
        };

        // Create a mock command that counts the objects
        struct MockCommand {
            pub count: std::sync::atomic::AtomicUsize,
        }

        #[async_trait]
        impl RunCommand for MockCommand {
            async fn execute(
                &self,
                _client: &dyn S3Client,
                _path: &S3Path,
                objects: &[Object],
            ) -> Result<(), anyhow::Error> {
                self.count
                    .fetch_add(objects.len(), std::sync::atomic::Ordering::SeqCst);
                Ok(())
            }
        }

        let mock_command = MockCommand {
            count: std::sync::atomic::AtomicUsize::new(0),
        };

        let find = Find {
            client: MockS3ClientBuilder::new().build(),
            path,
            limit: None,
            page_size: 1000,
            stats: true,
            summarize: true,
            command: Box::new(mock_command),
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

    #[test]
    fn test_default_stats() {
        // Test with summarize = true
        let stats = default_stats(true);
        assert!(stats.is_some());

        // Test with summarize = false
        let stats = default_stats(false);
        assert!(stats.is_none());
    }

    #[tokio::test]
    async fn test_find_stream_list() {
        // Instead of testing the actual list method which makes a network call,
        // let's test the FindStream struct properties and initialization

        let path = S3Path {
            bucket: "test-bucket".to_string(),
            prefix: Some("test-prefix".to_string()),
            region: Region::new("mock-region"),
        };

        // Create FindStream
        let find_stream = FindStream {
            client: MockS3ClientBuilder::new().build(),
            path: path.clone(),
            token: None,
            page_size: 1000,
            initial: true,
        };

        // Test the initial state
        assert_eq!(find_stream.token, None);
        assert_eq!(find_stream.page_size, 1000);
        assert!(find_stream.initial);
        assert_eq!(find_stream.path, path);

        // Test equality
        let same_stream = FindStream {
            client: MockS3ClientBuilder::new().build(),
            path: path.clone(),
            token: None,
            page_size: 1000,
            initial: true,
        };

        assert_eq!(find_stream, same_stream);

        // Test with different values
        let different_stream = FindStream {
            client: MockS3ClientBuilder::new().build(),
            path: path.clone(),
            token: Some("token".to_string()), // Different token
            page_size: 1000,
            initial: true,
        };

        assert_ne!(find_stream, different_stream);
    }

    #[tokio::test]
    async fn test_find_stream_stream() {
        // This test requires a more complex setup with multiple pages
        // For simplicity, we'll just verify that the stream function returns a Stream
        let path = S3Path {
            bucket: "test-bucket".to_string(),
            prefix: Some("test-prefix".to_string()),
            region: Region::new("mock-region"),
        };

        let find_stream = FindStream {
            client: MockS3ClientBuilder::new().build(),
            path,
            token: None,
            page_size: 1000,
            initial: true,
        };

        // Call stream
        let _stream = find_stream.stream();
        // We're just testing that it compiles and returns a stream
        // Testing the actual stream would require much more setup
    }

    #[tokio::test]
    async fn test_get_s3_client() {
        // Test with credentials
        let client1 = get_s3_client(
            Some("mock_access".to_string()),
            Some("mock_secret".to_string()),
            Region::new("mock-region"),
        )
        .await;

        // Test without credentials
        let client2 = get_s3_client(None, None, Region::new("mock-region")).await;

        // We can't easily test the actual client behavior, but we can verify that
        // the function returns a client in both cases without error
        assert!(client1.config().region().is_some());
        assert!(client2.config().region().is_some());
    }
}
