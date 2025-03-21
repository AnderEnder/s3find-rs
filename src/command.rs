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
    use aws_sdk_s3::types::Object;
    use std::io::Write;

    #[tokio::test]
    async fn test_find_new() {
        let aws_credentials = AWSPair {
            access: Some("access_key".to_string()),
            secret: Some("secret_key".to_string()),
        };
        let aws_region = Region::new("us-east-1");
        let cmd = Some(Cmd::Ls(FastPrint {}));
        let path = S3Path {
            bucket: "test-bucket".to_string(),
            prefix: Some("test-prefix".to_string()),
            region: aws_region.clone(),
        };
        let page_size = 1000;
        let summarize = true;
        let limit = Some(10);

        let find = Find::new(
            aws_credentials,
            &aws_region,
            cmd,
            path.clone(),
            page_size,
            summarize,
            limit,
        )
        .await;

        assert_eq!(find.path, path);
        assert_eq!(find.page_size, page_size);
        assert_eq!(find.summarize, summarize);
        assert_eq!(find.limit, limit);
    }

    // #[tokio::test]
    // async fn test_find_exec() {
    //     let aws_credentials = AWSPair {
    //         access: Some("access_key".to_string()),
    //         secret: Some("secret_key".to_string()),
    //     };
    //     let aws_region = Region::new("us-east-1");
    //     let cmd = Some(Cmd::Ls(FastPrint {}));
    //     let path = S3Path {
    //         bucket: "test-bucket".to_string(),
    //         prefix: Some("test-prefix".to_string()),
    //         region: aws_region.clone(),
    //     };
    //     let page_size = 1000;
    //     let summarize = true;
    //     let limit = Some(10);

    //     let find = Find::new(
    //         aws_credentials,
    //         &aws_region,
    //         cmd,
    //         path.clone(),
    //         page_size,
    //         summarize,
    //         limit,
    //     )
    //     .await;

    //     let objects = vec![
    //         Object::builder().key("object1").build(),
    //         Object::builder().key("object2").build(),
    //     ];

    //     let result = find.exec(None, objects.clone()).await;

    //     assert_eq!(result.unwrap().total_files, objects.len());
    // }

    #[tokio::test]
    async fn test_find_to_stream() {
        let aws_credentials = AWSPair {
            access: Some("access_key".to_string()),
            secret: Some("secret_key".to_string()),
        };
        let aws_region = Region::new("us-east-1");
        let cmd = Some(Cmd::Ls(FastPrint {}));
        let path = S3Path {
            bucket: "test-bucket".to_string(),
            prefix: Some("test-prefix".to_string()),
            region: aws_region.clone(),
        };
        let page_size = 1000;
        let summarize = true;
        let limit = Some(10);

        let find = Find::new(
            aws_credentials,
            &aws_region,
            cmd,
            path.clone(),
            page_size,
            summarize,
            limit,
        )
        .await;

        let stream = find.to_stream();

        assert_eq!(stream.path, path);
        assert_eq!(stream.page_size, page_size);
        assert!(stream.initial);
    }

    #[tokio::test]
    async fn test_find_from_opts() {
        let opts = FindOpt {
            aws_access_key: Some("access_key".to_string()),
            aws_secret_key: Some("secret_key".to_string()),
            aws_region: Region::new("us-east-1"),
            path: S3Path {
                bucket: "test-bucket".to_string(),
                prefix: Some("test-prefix".to_string()),
                region: Region::new("us-east-1"),
            },
            cmd: Some(Cmd::Ls(FastPrint {})),
            page_size: 1000,
            summarize: true,
            limit: Some(10),
            name: vec![],
            iname: vec![],
            regex: vec![],
            size: vec![],
            mtime: vec![],
        };

        let (find, filters) = Find::from_opts(&opts).await;

        assert_eq!(find.path, opts.path);
        assert_eq!(find.page_size, opts.page_size);
        assert_eq!(find.summarize, opts.summarize);
        assert_eq!(find.limit, opts.limit);
        assert!(filters.0.is_empty());
    }

    // #[tokio::test]
    // async fn test_find_stream_list() {
    //     let aws_credentials = AWSPair {
    //         access: Some("access_key".to_string()),
    //         secret: Some("secret_key".to_string()),
    //     };
    //     let aws_region = Region::new("us-east-1");
    //     let cmd = Some(Cmd::Ls(FastPrint {}));
    //     let path = S3Path {
    //         bucket: "test-bucket".to_string(),
    //         prefix: Some("test-prefix".to_string()),
    //         region: aws_region.clone(),
    //     };
    //     let page_size = 1000;
    //     let summarize = true;
    //     let limit = Some(10);

    //     let find = Find::new(
    //         aws_credentials,
    //         &aws_region,
    //         cmd,
    //         path.clone(),
    //         page_size,
    //         summarize,
    //         limit,
    //     )
    //     .await;

    //     let stream = find.to_stream();
    //     let result = stream.list().await;

    //     assert!(result.is_none());
    // }

    // #[tokio::test]
    // async fn test_find_stream_stream() {
    //     let aws_credentials = AWSPair {
    //         access: Some("access_key".to_string()),
    //         secret: Some("secret_key".to_string()),
    //     };
    //     let aws_region = Region::new("us-east-1");
    //     let cmd = Some(Cmd::Ls(FastPrint {}));
    //     let path = S3Path {
    //         bucket: "test-bucket".to_string(),
    //         prefix: Some("test-prefix".to_string()),
    //         region: aws_region.clone(),
    //     };
    //     let page_size = 1000;
    //     let summarize = true;
    //     let limit = Some(10);

    //     let find = Find::new(
    //         aws_credentials,
    //         &aws_region,
    //         cmd,
    //         path.clone(),
    //         page_size,
    //         summarize,
    //         limit,
    //     )
    //     .await;

    //     let stream = find.to_stream();
    //     let result: Vec<_> = stream.stream().collect().await;

    //     assert!(result.is_empty());
    // }

    #[test]
    fn test_find_stat_add() {
        let objects = vec![
            Object::builder().key("object1").size(100).build(),
            Object::builder().key("object2").size(200).build(),
            Object::builder().key("object3").size(300).build(),
        ];

        let stat = FindStat::default() + &objects;

        assert_eq!(stat.total_files, objects.len());
        assert_eq!(stat.total_space, 600);
        assert_eq!(stat.max_size, Some(300));
        assert_eq!(stat.min_size, Some(100));
        assert_eq!(stat.max_key, "object3");
        assert_eq!(stat.min_key, "object1");
        assert_eq!(stat.average_size, 200);
    }

    #[test]
    fn test_find_stat_default() {
        let stat = FindStat::default();

        assert_eq!(stat.total_files, 0);
        assert_eq!(stat.total_space, 0);
        assert_eq!(stat.max_size, None);
        assert_eq!(stat.min_size, None);
        assert_eq!(stat.max_key, "");
        assert_eq!(stat.min_key, "");
        assert_eq!(stat.average_size, 0);
    }

    #[test]
    fn test_find_stat_fmt() {
        let stat = FindStat {
            total_files: 3,
            total_space: 600,
            max_size: Some(300),
            min_size: Some(100),
            max_key: "object3".to_string(),
            min_key: "object1".to_string(),
            average_size: 200,
        };

        let mut buf = Vec::new();
        write!(&mut buf, "{}", stat).unwrap();
        let out = std::str::from_utf8(&buf).unwrap();

        assert!(out.contains("Total files:"));
        assert!(out.contains("Total space:"));
        assert!(out.contains("Largest file:"));
        assert!(out.contains("Largest file size:"));
        assert!(out.contains("Smallest file:"));
        assert!(out.contains("Smallest file size:"));
        assert!(out.contains("Average file size:"));
    }
}
