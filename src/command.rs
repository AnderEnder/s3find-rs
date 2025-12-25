use std::fmt;
use std::ops::Add;

use aws_sdk_s3::Client;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::list_objects_v2::{ListObjectsV2Error, ListObjectsV2Output};
use aws_sdk_s3::types::Object;
use aws_smithy_async::future::pagination_stream::PaginationStream;
use aws_smithy_runtime_api::http::Response;

use futures::Stream;
use humansize::*;

use crate::arg::*;
use crate::run_command::*;

pub struct FindCommand {
    pub client: Client,
    pub path: S3Path,
    pub command: Box<dyn RunCommand>,
}

impl FindCommand {
    pub fn new(cmd: Option<Cmd>, path: S3Path, client: Client) -> Self {
        let command = cmd.unwrap_or_default().downcast();

        FindCommand {
            client,
            path,
            command,
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

    pub fn from_opts(opts: &FindOpt, client: Client) -> FindCommand {
        let FindOpt { path, cmd, .. } = opts;

        let path = S3Path { ..path.clone() };

        FindCommand::new(cmd.clone(), path, client)
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
    pub fn from_opts(opts: &FindOpt, client: Client) -> Self {
        let path = opts.path.clone();

        FindStream {
            client,
            path,
            token: None,
            page_size: opts.page_size,
            initial: true,
        }
    }

    async fn paginator(
        self,
    ) -> PaginationStream<Result<ListObjectsV2Output, SdkError<ListObjectsV2Error, Response>>> {
        self.client
            .list_objects_v2()
            .bucket(self.path.bucket.clone())
            .prefix(self.path.prefix.clone().unwrap_or_else(|| "".to_owned()))
            .max_keys(self.page_size as i32)
            .into_paginator()
            .send()
    }

    pub async fn stream(self) -> impl Stream<Item = Vec<Object>> {
        let ps = self.paginator().await;
        futures::stream::unfold(ps, |mut p| async {
            let next_page = p.next().await;

            match next_page {
                Some(Ok(output)) => {
                    let objects = output.contents.unwrap_or(Vec::new());
                    Some((objects, p))
                }
                Some(Err(e)) => {
                    eprintln!("Error listing objects: {:?}", e);
                    None
                }
                None => None,
            }
        })
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

    use crate::adapters::aws::setup_client;

    use super::*;
    use aws_config::{BehaviorVersion, Region};
    use aws_sdk_s3::{config::Credentials, types::ObjectStorageClass};
    use aws_smithy_runtime::client::http::test_util::{ReplayEvent, StaticReplayClient};
    use aws_smithy_types::body::SdkBody;
    use futures::StreamExt;
    use glob::Pattern;
    use http::{HeaderValue, StatusCode};
    use regex::Regex;

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
        };

        let command = DoNothing {};

        let config = aws_sdk_s3::Config::builder()
            .region(Region::new("mock-region"))
            .credentials_provider(Credentials::new("mock", "mock", None, None, "mock"))
            .behavior_version(BehaviorVersion::latest())
            .build();

        let client = Client::from_conf(config);

        let find = FindCommand {
            client,
            path,
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
        };

        let config = aws_sdk_s3::Config::builder()
            .region(Region::new("mock-region"))
            .credentials_provider(Credentials::new("mock", "mock", None, None, "mock"))
            .behavior_version(BehaviorVersion::latest())
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
            path,
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
        };

        let config = aws_sdk_s3::Config::builder()
            .region(Region::new("mock-region"))
            .credentials_provider(Credentials::new("mock", "mock", None, None, "mock"))
            .behavior_version(BehaviorVersion::latest())
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

    #[test]
    fn test_find_stream_debug() {
        let path = S3Path {
            bucket: "test-bucket".to_string(),
            prefix: Some("test-prefix/".to_string()),
        };

        let config = aws_sdk_s3::Config::builder()
            .region(Region::new("us-west-2"))
            .credentials_provider(Credentials::new("mock", "mock", None, None, "mock"))
            .behavior_version(BehaviorVersion::latest())
            .build();

        let find_stream = FindStream {
            client: Client::from_conf(config),
            path: path.clone(),
            token: Some("test-token".to_string()),
            page_size: 1000,
            initial: true,
        };

        let debug_str = format!("{:?}", find_stream);

        assert!(debug_str.contains("FindStream {"));
        assert!(debug_str.contains(&format!("path: {:?}", path)));
        assert!(debug_str.contains("token: Some(\"test-token\")"));
        assert!(debug_str.contains("page_size: 1000"));
        assert!(debug_str.contains("initial: true"));
    }

    #[tokio::test]
    async fn test_find_new() {
        let args = FindOpt {
            aws_access_key: Some("mock_access".to_string()),
            aws_secret_key: Some("mock_secret".to_string()),
            aws_region: Some(Region::new("mock-region")),
            endpoint_url: None,
            force_path_style: false,
            path: S3Path {
                bucket: "test-bucket".to_string(),
                prefix: Some("test-prefix/".to_string()),
            },
            name: Vec::new(),
            iname: Vec::new(),
            mtime: Vec::new(),
            regex: Vec::new(),
            size: Default::default(),
            cmd: None,
            storage_class: None,
            page_size: 500,
            summarize: true,
            limit: Some(100),
        };
        let client = setup_client(&args).await;

        let find = FindCommand::new(args.cmd, args.path.clone(), client.clone());

        assert_eq!(find.path.bucket, "test-bucket");
        assert_eq!(find.path.prefix, Some("test-prefix/".to_string()));
    }

    #[test]
    fn test_find_to_stream() {
        let path = S3Path {
            bucket: "test-bucket".to_string(),
            prefix: Some("test-prefix".to_string()),
        };

        let config = aws_sdk_s3::Config::builder()
            .region(Region::new("mock-region"))
            .credentials_provider(Credentials::new("mock", "mock", None, None, "mock"))
            .behavior_version(BehaviorVersion::latest())
            .build();

        let client = Client::from_conf(config);
        let page_size = 1000;

        let stream = FindStream {
            path: path.clone(),
            token: None,
            page_size,
            initial: true,
            client,
        };

        assert_eq!(stream.path, path);
        assert_eq!(stream.token, None);
        assert_eq!(stream.page_size, page_size);
        assert!(stream.initial);
    }

    #[tokio::test]
    async fn test_find_from_opts() {
        let bucket = "test-bucket".to_string();
        let region = Region::new("test-region");
        let path = S3Path {
            bucket: bucket.clone(),
            prefix: Some("test-prefix/".to_string()),
        };

        let name_patterns = vec![Pattern::new("*.txt").unwrap()];
        let iname_globs = vec![InameGlob(Pattern::new("*.TXT").unwrap())];
        let regexes = vec![Regex::new(r"test.*\.txt").unwrap()];
        let sizes = vec![FindSize::Bigger(100)];
        let mtimes = vec![FindTime::Lower(3600 * 24)];

        let page_size = 500;
        let summarize = true;
        let limit = Some(100);

        let opts = FindOpt {
            aws_access_key: Some("test-access".to_string()),
            aws_secret_key: Some("test-secret".to_string()),
            aws_region: Some(region.clone()),
            endpoint_url: None,
            force_path_style: false,
            path: path.clone(),
            cmd: None,
            page_size,
            summarize,
            limit,
            name: name_patterns,
            iname: iname_globs,
            regex: regexes,
            size: sizes,
            mtime: mtimes,
            storage_class: Some(ObjectStorageClass::Standard),
        };

        let client = setup_client(&opts).await;

        let find = FindCommand::from_opts(&opts, client);

        assert_eq!(find.path.bucket, bucket);
    }

    #[tokio::test]
    async fn test_find_stream_list_with_replay_client() -> Result<(), Box<dyn std::error::Error>> {
        let path = S3Path {
            bucket: "test-bucket".to_string(),
            prefix: Some("test-prefix/".to_string()),
        };

        let req_initial = http::Request::builder()
            .method("GET")
            .uri("https://test-bucket.s3.amazonaws.com/?list-type=2&max-keys=1000&prefix=test-prefix%2F")
            .body(SdkBody::empty())
            .unwrap();

        let resp_initial_body = r#"<?xml version="1.0" encoding="UTF-8"?>
        <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Name>test-bucket</Name>
            <Prefix>test-prefix/</Prefix>
            <MaxKeys>1000</MaxKeys>
            <IsTruncated>true</IsTruncated>
            <NextContinuationToken>1ueGcxLPRx1Tr/XYExHnhbYLgveDs2J/wm36Hy4vbOwM=</NextContinuationToken>
            <Contents>
                <Key>test-prefix/file1.txt</Key>
                <LastModified>2023-01-01T00:00:00.000Z</LastModified>
                <ETag>&quot;d41d8cd98f00b204e9800998ecf8427e&quot;</ETag>
                <Size>100</Size>
                <StorageClass>STANDARD</StorageClass>
            </Contents>
            <Contents>
                <Key>test-prefix/file2.txt</Key>
                <LastModified>2023-01-01T00:00:00.000Z</LastModified>
                <ETag>&quot;d41d8cd98f00b204e9800998ecf8427f&quot;</ETag>
                <Size>200</Size>
                <StorageClass>STANDARD</StorageClass>
            </Contents>
        </ListBucketResult>"#;

        let resp_initial = http::Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", HeaderValue::from_static("application/xml"))
            .body(SdkBody::from(resp_initial_body))
            .unwrap();

        let req_continuation = http::Request::builder()
            .method("GET")
            .uri("https://test-bucket.s3.amazonaws.com/?continuation-token=1ueGcxLPRx1Tr%2FXYExHnhbYLgveDs2J%2Fwm36Hy4vbOwM%3D&list-type=2&max-keys=1000&prefix=test-prefix%2F")
            .body(SdkBody::empty())
            .unwrap();

        let resp_continuation_body = r#"<?xml version="1.0" encoding="UTF-8"?>
        <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Name>test-bucket</Name>
            <Prefix>test-prefix/</Prefix>
            <MaxKeys>1000</MaxKeys>
            <IsTruncated>false</IsTruncated>
            <Contents>
                <Key>test-prefix/file3.txt</Key>
                <LastModified>2023-01-01T00:00:00.000Z</LastModified>
                <ETag>&quot;d41d8cd98f00b204e9800998ecf8427g&quot;</ETag>
                <Size>300</Size>
                <StorageClass>STANDARD</StorageClass>
            </Contents>
        </ListBucketResult>"#;

        let resp_continuation = http::Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", HeaderValue::from_static("application/xml"))
            .body(SdkBody::from(resp_continuation_body))
            .unwrap();

        let events = vec![
            ReplayEvent::new(req_initial, resp_initial),
            ReplayEvent::new(req_continuation, resp_continuation),
        ];

        let replay_client = StaticReplayClient::new(events);

        let client: aws_sdk_s3::Client = aws_sdk_s3::Client::from_conf(
            aws_sdk_s3::Config::builder()
                .behavior_version(BehaviorVersion::latest())
                .credentials_provider(aws_sdk_s3::config::Credentials::new(
                    "ATESTCLIENT",
                    "astestsecretkey",
                    Some("atestsessiontoken".to_string()),
                    None,
                    "",
                ))
                .region(aws_sdk_s3::config::Region::new("us-east-1"))
                .http_client(replay_client.clone())
                .build(),
        );

        let find_stream = FindStream {
            client,
            path,
            token: None,
            page_size: 1000,
            initial: true,
        };

        let mut paginator = find_stream.paginator().await;
        let objects1 = paginator.next().await.unwrap().unwrap().contents.unwrap();

        assert_eq!(objects1.len(), 2, "First result should contain 2 objects");
        assert_eq!(objects1[0].key.as_ref().unwrap(), "test-prefix/file1.txt");
        assert_eq!(objects1[1].key.as_ref().unwrap(), "test-prefix/file2.txt");

        let objects2 = paginator.next().await.unwrap().unwrap().contents.unwrap();

        assert_eq!(objects2.len(), 1, "Second result should contain 1 object");
        assert_eq!(objects2[0].key.as_ref().unwrap(), "test-prefix/file3.txt");

        Ok(())
    }

    #[tokio::test]
    async fn test_find_stream_with_error_response() -> Result<(), Box<dyn std::error::Error>> {
        let path = S3Path {
            bucket: "test-bucket".to_string(),
            prefix: Some("test-prefix/".to_string()),
        };

        let req_error = http::Request::builder()
            .method("GET")
            .uri("https://test-bucket.s3.amazonaws.com/?list-type=2&max-keys=1000&prefix=test-prefix%2F")
            .body(SdkBody::empty())
            .unwrap();

        let resp_error = http::Response::builder()
            .status(StatusCode::FORBIDDEN)
            .header("Content-Type", HeaderValue::from_static("application/xml"))
            .body(SdkBody::from(
                r#"<?xml version="1.0" encoding="UTF-8"?>
                <Error>
                    <Code>AccessDenied</Code>
                    <Message>Access Denied</Message>
                    <RequestId>1D5H9EXAMPLE</RequestId>
                    <HostId>nh8QbPEXAMPLE</HostId>
                </Error>"#,
            ))
            .unwrap();

        let events = vec![ReplayEvent::new(req_error, resp_error)];
        let replay_client = StaticReplayClient::new(events);

        let client = aws_sdk_s3::Client::from_conf(
            aws_sdk_s3::Config::builder()
                .behavior_version(BehaviorVersion::latest())
                .credentials_provider(aws_sdk_s3::config::Credentials::new(
                    "ATESTCLIENT",
                    "astestsecretkey",
                    Some("atestsessiontoken".to_string()),
                    None,
                    "",
                ))
                .region(aws_sdk_s3::config::Region::new("us-east-1"))
                .http_client(replay_client)
                .build(),
        );

        let find_stream = FindStream {
            client,
            path,
            token: None,
            page_size: 1000,
            initial: true,
        };

        let mut paginator = find_stream.paginator().await;
        let result = paginator.next().await;

        assert!(result.is_some(), "Expected Some result from paginator");
        let err = result.unwrap();
        assert!(err.is_err(), "Expected error response");

        if let Err(SdkError::ServiceError(service_error)) = err {
            assert_eq!(service_error.err().meta().code(), Some("AccessDenied"));
        }

        assert!(
            paginator.next().await.is_none(),
            "Expected None after error"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_find_stream_paginator_with_empty_page() -> Result<(), Box<dyn std::error::Error>>
    {
        let path = S3Path {
            bucket: "test-bucket".to_string(),
            prefix: Some("empty-prefix/".to_string()),
        };

        let req_empty = http::Request::builder()
            .method("GET")
            .uri("https://test-bucket.s3.amazonaws.com/?list-type=2&max-keys=1000&prefix=empty-prefix%2F")
            .body(SdkBody::empty())
            .unwrap();

        let resp_empty_body = r#"<?xml version="1.0" encoding="UTF-8"?>
            <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                <Name>test-bucket</Name>
                <Prefix>empty-prefix/</Prefix>
                <MaxKeys>1000</MaxKeys>
                <IsTruncated>false</IsTruncated>
            </ListBucketResult>"#;

        let resp_empty = http::Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", HeaderValue::from_static("application/xml"))
            .body(SdkBody::from(resp_empty_body))
            .unwrap();

        let events = vec![ReplayEvent::new(req_empty, resp_empty)];
        let replay_client = StaticReplayClient::new(events);

        let client = aws_sdk_s3::Client::from_conf(
            aws_sdk_s3::Config::builder()
                .behavior_version(BehaviorVersion::latest())
                .credentials_provider(aws_sdk_s3::config::Credentials::new(
                    "ATESTCLIENT",
                    "astestsecretkey",
                    Some("atestsessiontoken".to_string()),
                    None,
                    "",
                ))
                .region(aws_sdk_s3::config::Region::new("us-east-1"))
                .http_client(replay_client)
                .build(),
        );

        let find_stream = FindStream {
            client,
            path,
            token: None,
            page_size: 1000,
            initial: true,
        };

        let mut paginator = find_stream.paginator().await;
        let result = paginator.next().await;

        assert!(result.is_some(), "Expected Some result from paginator");
        let output = result.unwrap().expect("Expected successful response");

        if let Some(contents) = output.contents {
            assert!(contents.is_empty(), "Expected empty contents list");
        }

        assert!(
            paginator.next().await.is_none(),
            "Expected None for next page"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_find_stream_stream_with_empty_results() -> Result<(), Box<dyn std::error::Error>>
    {
        let path = S3Path {
            bucket: "test-bucket".to_string(),
            prefix: Some("empty-prefix/".to_string()),
        };

        let req_empty = http::Request::builder()
            .method("GET")
            .uri("https://test-bucket.s3.amazonaws.com/?list-type=2&max-keys=1000&prefix=empty-prefix%2F")
            .body(SdkBody::empty())
            .unwrap();

        let resp_empty_body = r#"<?xml version="1.0" encoding="UTF-8"?>
            <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                <Name>test-bucket</Name>
                <Prefix>empty-prefix/</Prefix>
                <MaxKeys>1000</MaxKeys>
                <IsTruncated>false</IsTruncated>
                <Contents></Contents>
            </ListBucketResult>"#;

        let resp_empty = http::Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", HeaderValue::from_static("application/xml"))
            .body(SdkBody::from(resp_empty_body))
            .unwrap();

        let events = vec![ReplayEvent::new(req_empty, resp_empty)];
        let replay_client = StaticReplayClient::new(events);

        let client = aws_sdk_s3::Client::from_conf(
            aws_sdk_s3::Config::builder()
                .behavior_version(BehaviorVersion::latest())
                .credentials_provider(aws_sdk_s3::config::Credentials::new(
                    "ATESTCLIENT",
                    "astestsecretkey",
                    Some("atestsessiontoken".to_string()),
                    None,
                    "",
                ))
                .region(aws_sdk_s3::config::Region::new("us-east-1"))
                .http_client(replay_client)
                .build(),
        );

        let find_stream = FindStream {
            client,
            path,
            token: None,
            page_size: 1000,
            initial: true,
        };

        let stream = find_stream.stream().await;

        let objects = stream
            .map(|x| futures::stream::iter(x.into_iter()))
            .flatten()
            .collect::<Vec<_>>()
            .await;

        assert_eq!(objects.len(), 1, "Expected vector with 1 empty object");

        Ok(())
    }

    #[tokio::test]
    async fn test_find_stream_with_empty_page() -> Result<(), Box<dyn std::error::Error>> {
        let path = S3Path {
            bucket: "test-bucket".to_string(),
            prefix: Some("empty-prefix/".to_string()),
        };

        let req_empty = http::Request::builder()
            .method("GET")
            .uri("https://test-bucket.s3.amazonaws.com/?list-type=2&max-keys=1000&prefix=empty-prefix%2F")
            .body(SdkBody::empty())
            .unwrap();

        let resp_empty_body = r#"<?xml version="1.0" encoding="UTF-8"?>
            <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                <Name>test-bucket</Name>
                <Prefix>empty-prefix/</Prefix>
                <MaxKeys>1000</MaxKeys>
                <IsTruncated>false</IsTruncated>
            </ListBucketResult>"#;

        let resp_empty = http::Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", HeaderValue::from_static("application/xml"))
            .body(SdkBody::from(resp_empty_body))
            .unwrap();

        let events = vec![ReplayEvent::new(req_empty, resp_empty)];
        let replay_client = StaticReplayClient::new(events);

        let client = aws_sdk_s3::Client::from_conf(
            aws_sdk_s3::Config::builder()
                .behavior_version(BehaviorVersion::latest())
                .credentials_provider(aws_sdk_s3::config::Credentials::new(
                    "ATESTCLIENT",
                    "astestsecretkey",
                    Some("atestsessiontoken".to_string()),
                    None,
                    "",
                ))
                .region(aws_sdk_s3::config::Region::new("us-east-1"))
                .http_client(replay_client)
                .build(),
        );

        let find_stream = FindStream {
            client,
            path,
            token: None,
            page_size: 1000,
            initial: true,
        };

        let stream = find_stream.stream().await;

        let objects = stream
            .map(|x| futures::stream::iter(x.into_iter()))
            .flatten()
            .collect::<Vec<_>>()
            .await;

        assert_eq!(objects.len(), 0, "Expected vector without object");

        Ok(())
    }

    #[tokio::test]
    async fn test_find_stream_from_opts() {
        let path1 = S3Path {
            bucket: "test-bucket".to_string(),
            prefix: Some("test-prefix/".to_string()),
        };

        let opts1 = FindOpt {
            aws_access_key: Some("test-access".to_string()),
            aws_secret_key: Some("test-secret".to_string()),
            aws_region: Some(Region::new("test-region")),
            endpoint_url: None,
            force_path_style: false,
            path: path1.clone(),
            cmd: None,
            page_size: 500,
            summarize: true,
            limit: Some(100),
            name: vec![],
            iname: vec![],
            regex: vec![],
            size: vec![],
            mtime: vec![],
            storage_class: None,
        };

        let client1 = setup_client(&opts1).await;
        let find_stream1 = FindStream::from_opts(&opts1, client1);

        assert_eq!(find_stream1.path, path1);
        assert_eq!(find_stream1.page_size, 500);
        assert_eq!(find_stream1.token, None);
        assert!(find_stream1.initial);

        let path_without_prefix = S3Path {
            bucket: "another-bucket".to_string(),
            prefix: None,
        };

        let opts_withour_prefix = FindOpt {
            aws_access_key: None,
            aws_secret_key: None,
            aws_region: Some(Region::new("us-west-2")),
            endpoint_url: None,
            force_path_style: false,
            path: path_without_prefix.clone(),
            cmd: Some(Cmd::Ls(FastPrint {})),
            page_size: 1000,
            summarize: false,
            limit: None,
            name: vec![],
            iname: vec![],
            regex: vec![],
            size: vec![],
            mtime: vec![],
            storage_class: None,
        };

        let client2 = setup_client(&opts_withour_prefix).await;
        let find_stream2 = FindStream::from_opts(&opts_withour_prefix, client2);

        assert_eq!(find_stream2.path, path_without_prefix);
        assert_eq!(find_stream2.page_size, 1000);
        assert_eq!(find_stream2.token, None);
        assert!(find_stream2.initial);
    }
}
