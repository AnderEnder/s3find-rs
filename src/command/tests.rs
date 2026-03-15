use crate::adapters::aws::setup_client;
use crate::arg::*;
use crate::error::{S3FindError, S3FindResult};
use crate::run_command::RunCommand;

use super::*;
use anyhow::anyhow;
use async_trait::async_trait;
use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3::{
    Client,
    config::Credentials,
    types::{Object, ObjectStorageClass, Tag},
};
use aws_smithy_runtime::client::http::test_util::{ReplayEvent, StaticReplayClient};
use aws_smithy_types::body::SdkBody;
use futures::StreamExt;
use glob::Pattern;
use http::{HeaderValue, StatusCode};
use humansize::{BINARY, make_format};
use regex::Regex;

async fn collect_stream_objects(
    stream: BoxedStream<'static, S3FindResult<Vec<StreamObject>>>,
) -> S3FindResult<Vec<StreamObject>> {
    let mut stream = stream;
    let mut objects = Vec::new();

    while let Some(batch) = stream.next().await {
        objects.extend(batch?);
    }

    Ok(objects)
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
    let file_size = make_format(BINARY);
    let expected_smallest_size = format!("{:19} {}", "Smallest file size:", file_size(123_u64));
    assert!(
        display_output.contains(&expected_smallest_size),
        "Incorrect smallest file size line"
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

    // Create test objects wrapped in StreamObject
    let stream_objects = vec![
        StreamObject::from_object(Object::builder().key("object1").size(100).build()),
        StreamObject::from_object(Object::builder().key("object2").size(200).build()),
    ];

    // Execute find with stats
    let acc = Some(FindStat::default());
    let result = find.exec(acc, stream_objects).await.unwrap();

    // Verify stats were updated
    assert!(result.is_some());
    let stats = result.unwrap();
    assert_eq!(stats.total_files, 2);
    assert_eq!(stats.total_space, 300);
}

#[tokio::test]
async fn test_find_exec_propagates_command_errors() {
    struct FailingCommand;

    #[async_trait]
    impl RunCommand for FailingCommand {
        async fn execute(
            &self,
            _client: &dyn crate::adapters::aws::CommandS3Client,
            _path: &S3Path,
            _list: &[StreamObject],
        ) -> Result<(), anyhow::Error> {
            Err(anyhow!("command failed"))
        }
    }

    let config = aws_sdk_s3::Config::builder()
        .region(Region::new("mock-region"))
        .credentials_provider(Credentials::new("mock", "mock", None, None, "mock"))
        .behavior_version(BehaviorVersion::latest())
        .build();

    let find = FindCommand {
        client: Client::from_conf(config),
        path: S3Path {
            bucket: "test-bucket".to_string(),
            prefix: Some("test-prefix".to_string()),
        },
        command: Box::new(FailingCommand),
    };

    let result = find
        .exec(
            Some(FindStat::default()),
            vec![StreamObject::from_object(
                Object::builder().key("object1").size(100).build(),
            )],
        )
        .await;

    assert!(matches!(result, Err(S3FindError::CommandExecution { .. })));
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
        maxdepth: None,
        all_versions: false,
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
        maxdepth: None,
        all_versions: false,
    };

    assert_eq!(find_stream, same_stream);

    let different_stream = FindStream {
        client,
        path,
        token: Some("token".to_string()),
        page_size: 1000,
        initial: true,
        maxdepth: None,
        all_versions: false,
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
        maxdepth: None,
        all_versions: false,
    };

    let _stream = find_stream.stream(); // Non-async, returns stream immediately
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
        maxdepth: None,
        all_versions: false,
    };

    let debug_str = format!("{:?}", find_stream);

    assert!(debug_str.contains("FindStream {"));
    assert!(debug_str.contains(&format!("path: {:?}", path)));
    assert!(debug_str.contains("token: Some(\"test-token\")"));
    assert!(debug_str.contains("page_size: 1000"));
    assert!(debug_str.contains("initial: true"));
    assert!(debug_str.contains("maxdepth: None"));
    assert!(debug_str.contains("all_versions: false"));
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
        maxdepth: None,
        all_versions: false,
        tag: Vec::new(),
        tag_exists: Vec::new(),
        tag_concurrency: 50,
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
        maxdepth: None,
        all_versions: false,
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
        maxdepth: None,
        all_versions: false,
        tag: Vec::new(),
        tag_exists: Vec::new(),
        tag_concurrency: 50,
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
        .uri(
            "https://test-bucket.s3.amazonaws.com/?list-type=2&max-keys=1000&prefix=test-prefix%2F",
        )
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
        maxdepth: None,
        all_versions: false,
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
        .uri(
            "https://test-bucket.s3.amazonaws.com/?list-type=2&max-keys=1000&prefix=test-prefix%2F",
        )
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
        maxdepth: None,
        all_versions: false,
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
async fn test_find_stream_stream_propagates_error_response() {
    let path = S3Path {
        bucket: "test-bucket".to_string(),
        prefix: Some("test-prefix/".to_string()),
    };

    let req_error = http::Request::builder()
        .method("GET")
        .uri(
            "https://test-bucket.s3.amazonaws.com/?list-type=2&max-keys=1000&prefix=test-prefix%2F",
        )
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

    let replay_client = StaticReplayClient::new(vec![ReplayEvent::new(req_error, resp_error)]);

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

    let result = collect_stream_objects(
        FindStream {
            client,
            path,
            token: None,
            page_size: 1000,
            initial: true,
            maxdepth: None,
            all_versions: false,
        }
        .stream(),
    )
    .await;

    assert!(matches!(result, Err(S3FindError::ListObjects { .. })));
}

#[tokio::test]
async fn test_find_stream_paginator_with_empty_page() -> Result<(), Box<dyn std::error::Error>> {
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
        maxdepth: None,
        all_versions: false,
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
async fn test_find_stream_stream_with_empty_results() -> Result<(), Box<dyn std::error::Error>> {
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
        maxdepth: None,
        all_versions: false,
    };

    let stream = find_stream.stream();
    let stream_objects = collect_stream_objects(stream).await?;

    assert_eq!(
        stream_objects.len(),
        1,
        "Expected vector with 1 empty object"
    );

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
        maxdepth: None,
        all_versions: false,
    };

    let stream = find_stream.stream();
    let stream_objects = collect_stream_objects(stream).await?;

    assert_eq!(stream_objects.len(), 0, "Expected vector without object");

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
        maxdepth: None,
        all_versions: false,
        tag: Vec::new(),
        tag_exists: Vec::new(),
        tag_concurrency: 50,
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
        maxdepth: None,
        all_versions: false,
        tag: Vec::new(),
        tag_exists: Vec::new(),
        tag_concurrency: 50,
    };

    let client2 = setup_client(&opts_withour_prefix).await;
    let find_stream2 = FindStream::from_opts(&opts_withour_prefix, client2);

    assert_eq!(find_stream2.path, path_without_prefix);
    assert_eq!(find_stream2.page_size, 1000);
    assert_eq!(find_stream2.token, None);
    assert!(find_stream2.initial);
}

#[tokio::test]
async fn test_maxdepth_zero() -> Result<(), Box<dyn std::error::Error>> {
    // Test that maxdepth=0 returns only objects at prefix level
    let path = S3Path {
        bucket: "test-bucket".to_string(),
        prefix: Some("data/".to_string()),
    };

    let req = http::Request::builder()
            .method("GET")
            .uri("https://test-bucket.s3.amazonaws.com/?delimiter=%2F&list-type=2&max-keys=1000&prefix=data%2F")
            .body(SdkBody::empty())
            .unwrap();

    let resp_body = r#"<?xml version="1.0" encoding="UTF-8"?>
        <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Name>test-bucket</Name>
            <Prefix>data/</Prefix>
            <Delimiter>/</Delimiter>
            <MaxKeys>1000</MaxKeys>
            <IsTruncated>false</IsTruncated>
            <Contents>
                <Key>data/file1.txt</Key>
                <Size>100</Size>
            </Contents>
            <Contents>
                <Key>data/file2.txt</Key>
                <Size>200</Size>
            </Contents>
            <CommonPrefixes>
                <Prefix>data/2024/</Prefix>
            </CommonPrefixes>
        </ListBucketResult>"#;

    let resp = http::Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", HeaderValue::from_static("application/xml"))
        .body(SdkBody::from(resp_body))
        .unwrap();

    let events = vec![ReplayEvent::new(req, resp)];
    let replay_client = StaticReplayClient::new(events);

    let client = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .credentials_provider(aws_sdk_s3::config::Credentials::new(
                "test", "test", None, None, "test",
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
        maxdepth: Some(0),
        all_versions: false,
    };

    let stream = find_stream.stream();
    let stream_objects = collect_stream_objects(stream).await?;

    // With maxdepth=0, should only get objects at prefix level
    assert_eq!(stream_objects.len(), 2);
    assert_eq!(stream_objects[0].key().unwrap(), "data/file1.txt");
    assert_eq!(stream_objects[1].key().unwrap(), "data/file2.txt");

    Ok(())
}

#[tokio::test]
async fn test_maxdepth_one_with_delimiter() -> Result<(), Box<dyn std::error::Error>> {
    // Test that maxdepth=1 uses delimiter and recurses one level
    let path = S3Path {
        bucket: "test-bucket".to_string(),
        prefix: Some("logs/".to_string()),
    };

    // First request: root level with delimiter
    let req1 = http::Request::builder()
            .method("GET")
            .uri("https://test-bucket.s3.amazonaws.com/?delimiter=%2F&list-type=2&max-keys=100&prefix=logs%2F")
            .body(SdkBody::empty())
            .unwrap();

    let resp1_body = r#"<?xml version="1.0" encoding="UTF-8"?>
        <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Name>test-bucket</Name>
            <Prefix>logs/</Prefix>
            <Delimiter>/</Delimiter>
            <IsTruncated>false</IsTruncated>
            <Contents>
                <Key>logs/root.txt</Key>
                <Size>100</Size>
            </Contents>
            <CommonPrefixes>
                <Prefix>logs/2024/</Prefix>
            </CommonPrefixes>
        </ListBucketResult>"#;

    let resp1 = http::Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", HeaderValue::from_static("application/xml"))
        .body(SdkBody::from(resp1_body))
        .unwrap();

    // Second request: subdirectory level with delimiter
    let req2 = http::Request::builder()
            .method("GET")
            .uri("https://test-bucket.s3.amazonaws.com/?delimiter=%2F&list-type=2&max-keys=100&prefix=logs%2F2024%2F")
            .body(SdkBody::empty())
            .unwrap();

    let resp2_body = r#"<?xml version="1.0" encoding="UTF-8"?>
        <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Name>test-bucket</Name>
            <Prefix>logs/2024/</Prefix>
            <Delimiter>/</Delimiter>
            <IsTruncated>false</IsTruncated>
            <Contents>
                <Key>logs/2024/jan.txt</Key>
                <Size>200</Size>
            </Contents>
            <CommonPrefixes>
                <Prefix>logs/2024/01/</Prefix>
            </CommonPrefixes>
        </ListBucketResult>"#;

    let resp2 = http::Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", HeaderValue::from_static("application/xml"))
        .body(SdkBody::from(resp2_body))
        .unwrap();

    let events = vec![ReplayEvent::new(req1, resp1), ReplayEvent::new(req2, resp2)];
    let replay_client = StaticReplayClient::new(events);

    let client = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .credentials_provider(aws_sdk_s3::config::Credentials::new(
                "test", "test", None, None, "test",
            ))
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .http_client(replay_client)
            .build(),
    );

    let find_stream = FindStream {
        client,
        path,
        token: None,
        page_size: 100,
        initial: true,
        maxdepth: Some(1),
        all_versions: false,
    };

    let stream = find_stream.stream();
    let objects = collect_stream_objects(stream).await.unwrap();

    // Should get objects from root level and one subdirectory level
    assert_eq!(objects.len(), 2);
    assert_eq!(objects[0].key().unwrap(), "logs/root.txt");
    assert_eq!(objects[1].key().unwrap(), "logs/2024/jan.txt");
    // Should NOT recurse into logs/2024/01/ due to maxdepth=1

    Ok(())
}

#[tokio::test]
async fn test_maxdepth_none_uses_standard_pagination() {
    // Test that maxdepth=None uses standard pagination without delimiter
    let path = S3Path {
        bucket: "test-bucket".to_string(),
        prefix: Some("data/".to_string()),
    };

    let req = http::Request::builder()
        .method("GET")
        .uri("https://test-bucket.s3.amazonaws.com/?list-type=2&max-keys=1000&prefix=data%2F")
        .body(SdkBody::empty())
        .unwrap();

    let resp_body = r#"<?xml version="1.0" encoding="UTF-8"?>
        <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Name>test-bucket</Name>
            <Prefix>data/</Prefix>
            <MaxKeys>1000</MaxKeys>
            <IsTruncated>false</IsTruncated>
            <Contents>
                <Key>data/file.txt</Key>
                <Size>100</Size>
            </Contents>
        </ListBucketResult>"#;

    let resp = http::Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", HeaderValue::from_static("application/xml"))
        .body(SdkBody::from(resp_body))
        .unwrap();

    let events = vec![ReplayEvent::new(req, resp)];
    let replay_client = StaticReplayClient::new(events);

    let client = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .credentials_provider(aws_sdk_s3::config::Credentials::new(
                "test", "test", None, None, "test",
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
        maxdepth: None, // No maxdepth - should use standard pagination
        all_versions: false,
    };

    let stream = find_stream.stream();
    let objects = collect_stream_objects(stream).await.unwrap();

    assert_eq!(objects.len(), 1);
    assert_eq!(objects[0].key().unwrap(), "data/file.txt");
}

#[tokio::test]
async fn test_maxdepth_with_empty_subdirectories() -> Result<(), Box<dyn std::error::Error>> {
    // Test that empty subdirectories don't cause issues
    let path = S3Path {
        bucket: "test-bucket".to_string(),
        prefix: Some("empty/".to_string()),
    };

    // Root level
    let req1 = http::Request::builder()
            .method("GET")
            .uri("https://test-bucket.s3.amazonaws.com/?delimiter=%2F&list-type=2&max-keys=100&prefix=empty%2F")
            .body(SdkBody::empty())
            .unwrap();

    let resp1_body = r#"<?xml version="1.0" encoding="UTF-8"?>
        <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Name>test-bucket</Name>
            <Prefix>empty/</Prefix>
            <Delimiter>/</Delimiter>
            <IsTruncated>false</IsTruncated>
            <CommonPrefixes>
                <Prefix>empty/sub/</Prefix>
            </CommonPrefixes>
        </ListBucketResult>"#;

    let resp1 = http::Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", HeaderValue::from_static("application/xml"))
        .body(SdkBody::from(resp1_body))
        .unwrap();

    // Empty subdirectory
    let req2 = http::Request::builder()
            .method("GET")
            .uri("https://test-bucket.s3.amazonaws.com/?delimiter=%2F&list-type=2&max-keys=100&prefix=empty%2Fsub%2F")
            .body(SdkBody::empty())
            .unwrap();

    let resp2_body = r#"<?xml version="1.0" encoding="UTF-8"?>
        <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Name>test-bucket</Name>
            <Prefix>empty/sub/</Prefix>
            <Delimiter>/</Delimiter>
            <IsTruncated>false</IsTruncated>
        </ListBucketResult>"#;

    let resp2 = http::Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", HeaderValue::from_static("application/xml"))
        .body(SdkBody::from(resp2_body))
        .unwrap();

    let events = vec![ReplayEvent::new(req1, resp1), ReplayEvent::new(req2, resp2)];
    let replay_client = StaticReplayClient::new(events);

    let client = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .credentials_provider(aws_sdk_s3::config::Credentials::new(
                "test", "test", None, None, "test",
            ))
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .http_client(replay_client)
            .build(),
    );

    let find_stream = FindStream {
        client,
        path,
        token: None,
        page_size: 100,
        initial: true,
        maxdepth: Some(1),
        all_versions: false,
    };

    let stream = find_stream.stream();
    let objects = collect_stream_objects(stream).await?;

    // Empty subdirectory should yield no objects but not cause errors
    assert_eq!(objects.len(), 0);

    Ok(())
}

#[tokio::test]
async fn test_all_versions_listing() -> Result<(), Box<dyn std::error::Error>> {
    // Test that all_versions uses ListObjectVersions API
    let path = S3Path {
        bucket: "test-bucket".to_string(),
        prefix: Some("data/".to_string()),
    };

    let req = http::Request::builder()
        .method("GET")
        .uri("https://test-bucket.s3.amazonaws.com/?max-keys=1000&prefix=data%2F&versions=")
        .body(SdkBody::empty())
        .unwrap();

    let resp_body = r#"<?xml version="1.0" encoding="UTF-8"?>
        <ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Name>test-bucket</Name>
            <Prefix>data/</Prefix>
            <MaxKeys>1000</MaxKeys>
            <IsTruncated>false</IsTruncated>
            <Version>
                <Key>data/file1.txt</Key>
                <VersionId>v1</VersionId>
                <IsLatest>true</IsLatest>
                <LastModified>2023-01-01T00:00:00.000Z</LastModified>
                <Size>100</Size>
                <StorageClass>STANDARD</StorageClass>
            </Version>
            <Version>
                <Key>data/file1.txt</Key>
                <VersionId>v0</VersionId>
                <IsLatest>false</IsLatest>
                <LastModified>2022-12-01T00:00:00.000Z</LastModified>
                <Size>90</Size>
                <StorageClass>STANDARD</StorageClass>
            </Version>
            <DeleteMarker>
                <Key>data/deleted.txt</Key>
                <VersionId>dm1</VersionId>
                <IsLatest>true</IsLatest>
                <LastModified>2023-02-01T00:00:00.000Z</LastModified>
            </DeleteMarker>
        </ListVersionsResult>"#;

    let resp = http::Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", HeaderValue::from_static("application/xml"))
        .body(SdkBody::from(resp_body))
        .unwrap();

    let events = vec![ReplayEvent::new(req, resp)];
    let replay_client = StaticReplayClient::new(events);

    let client = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .credentials_provider(aws_sdk_s3::config::Credentials::new(
                "test", "test", None, None, "test",
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
        maxdepth: None,
        all_versions: true, // Enable version listing
    };

    let stream = find_stream.stream();
    let objects = collect_stream_objects(stream).await?;

    // Should get all versions and delete markers
    assert_eq!(objects.len(), 3);

    // Objects are sorted by key (alphabetically), then by last_modified descending
    // So deleted.txt comes before file1.txt

    // Check delete marker (first alphabetically: "data/deleted.txt" < "data/file1.txt")
    assert_eq!(objects[0].key().unwrap(), "data/deleted.txt");
    assert_eq!(objects[0].version_id.as_ref().unwrap(), "dm1");
    assert_eq!(objects[0].is_latest, Some(true));
    assert!(objects[0].is_delete_marker);
    assert_eq!(
        objects[0].display_key(),
        "data/deleted.txt?versionId=dm1 (latest) (delete marker)"
    );
    // Delete marker has size 0
    assert_eq!(objects[0].object.size(), Some(0));

    // Check first version of file1.txt (latest, newer last_modified)
    assert_eq!(objects[1].key().unwrap(), "data/file1.txt");
    assert_eq!(objects[1].version_id.as_ref().unwrap(), "v1");
    assert_eq!(objects[1].is_latest, Some(true));
    assert_eq!(
        objects[1].display_key(),
        "data/file1.txt?versionId=v1 (latest)"
    );

    // Check older version of file1.txt
    assert_eq!(objects[2].key().unwrap(), "data/file1.txt");
    assert_eq!(objects[2].version_id.as_ref().unwrap(), "v0");
    assert_eq!(objects[2].is_latest, Some(false));
    assert_eq!(objects[2].display_key(), "data/file1.txt?versionId=v0");

    Ok(())
}

#[test]
fn test_stream_object_from_version_with_version_id() {
    use aws_sdk_s3::types::ObjectVersion;
    use aws_sdk_s3::types::ObjectVersionStorageClass;

    let version = ObjectVersion::builder()
        .key("test.txt")
        .version_id("abc123")
        .is_latest(true)
        .size(100)
        .storage_class(ObjectVersionStorageClass::Standard)
        .build();

    let stream_obj = StreamObject::from_version(version);

    // key() returns the original key
    assert_eq!(stream_obj.key(), Some("test.txt"));
    // Version info is in separate fields
    assert_eq!(stream_obj.version_id, Some("abc123".to_string()));
    assert_eq!(stream_obj.is_latest, Some(true));
    assert!(!stream_obj.is_delete_marker);
    // display_key() shows full version info
    assert_eq!(
        stream_obj.display_key(),
        "test.txt?versionId=abc123 (latest)"
    );
    assert_eq!(stream_obj.object.size(), Some(100));
}

#[test]
fn test_stream_object_from_version_not_latest() {
    use aws_sdk_s3::types::ObjectVersion;

    let version = ObjectVersion::builder()
        .key("test.txt")
        .version_id("old123")
        .is_latest(false)
        .size(50)
        .build();

    let stream_obj = StreamObject::from_version(version);

    // key() returns the original key
    assert_eq!(stream_obj.key(), Some("test.txt"));
    // Version info is in separate fields
    assert_eq!(stream_obj.version_id, Some("old123".to_string()));
    assert_eq!(stream_obj.is_latest, Some(false));
    // display_key() shows version but not (latest)
    assert_eq!(stream_obj.display_key(), "test.txt?versionId=old123");
    assert_eq!(stream_obj.object.size(), Some(50));
}

#[test]
fn test_stream_object_from_delete_marker() {
    use aws_sdk_s3::types::DeleteMarkerEntry;

    let marker = DeleteMarkerEntry::builder()
        .key("deleted.txt")
        .version_id("del456")
        .is_latest(true)
        .build();

    let stream_obj = StreamObject::from_delete_marker(marker);

    // key() returns the original key
    assert_eq!(stream_obj.key(), Some("deleted.txt"));
    // Version info is in separate fields
    assert_eq!(stream_obj.version_id, Some("del456".to_string()));
    assert_eq!(stream_obj.is_latest, Some(true));
    assert!(stream_obj.is_delete_marker);
    // display_key() shows full info including delete marker
    assert_eq!(
        stream_obj.display_key(),
        "deleted.txt?versionId=del456 (latest) (delete marker)"
    );
    assert_eq!(stream_obj.object.size(), Some(0));
}

#[test]
fn test_stream_object_from_object_non_versioned() {
    let object = Object::builder().key("simple.txt").size(200).build();

    let stream_obj = StreamObject::from_object(object);

    // key() returns the original key
    assert_eq!(stream_obj.key(), Some("simple.txt"));
    // No version info for non-versioned objects
    assert_eq!(stream_obj.version_id, None);
    assert_eq!(stream_obj.is_latest, None);
    assert!(!stream_obj.is_delete_marker);
    // display_key() is same as key for non-versioned
    assert_eq!(stream_obj.display_key(), "simple.txt");
    assert_eq!(stream_obj.object.size(), Some(200));
}

#[test]
fn test_stream_object_has_tags() {
    let object = Object::builder().key("test.txt").build();
    let mut stream_obj = StreamObject::from_object(object);

    // Initially, tags are not fetched
    assert!(!stream_obj.has_tags());

    // After setting empty tags
    stream_obj.tags = Some(vec![]);
    assert!(stream_obj.has_tags());

    // After setting actual tags
    stream_obj.tags = Some(vec![
        Tag::builder().key("env").value("prod").build().unwrap(),
    ]);
    assert!(stream_obj.has_tags());
}

#[test]
fn test_stream_object_get_tag() {
    let object = Object::builder().key("test.txt").build();
    let mut stream_obj = StreamObject::from_object(object);

    // Tags not fetched
    assert_eq!(stream_obj.get_tag("env"), None);

    // Set tags
    stream_obj.tags = Some(vec![
        Tag::builder().key("env").value("prod").build().unwrap(),
        Tag::builder().key("team").value("data").build().unwrap(),
    ]);

    // Get existing tag
    assert_eq!(stream_obj.get_tag("env"), Some("prod"));
    assert_eq!(stream_obj.get_tag("team"), Some("data"));

    // Get non-existent tag
    assert_eq!(stream_obj.get_tag("nonexistent"), None);
}

#[test]
fn test_stream_object_has_tag_key() {
    let object = Object::builder().key("test.txt").build();
    let mut stream_obj = StreamObject::from_object(object);

    // Tags not fetched - should return false
    assert!(!stream_obj.has_tag_key("env"));

    // Empty tags
    stream_obj.tags = Some(vec![]);
    assert!(!stream_obj.has_tag_key("env"));

    // Set tags
    stream_obj.tags = Some(vec![
        Tag::builder().key("env").value("prod").build().unwrap(),
        Tag::builder().key("team").value("data").build().unwrap(),
    ]);

    // Check existing keys
    assert!(stream_obj.has_tag_key("env"));
    assert!(stream_obj.has_tag_key("team"));

    // Check non-existent key
    assert!(!stream_obj.has_tag_key("nonexistent"));
}

#[test]
fn test_stream_object_display_key_variants() {
    // Test all display_key() variants for better coverage
    let object = Object::builder().key("file.txt").build();

    // Non-versioned object
    let stream_obj = StreamObject {
        object: object.clone(),
        version_id: None,
        is_latest: None,
        is_delete_marker: false,
        tags: None,
    };
    assert_eq!(stream_obj.display_key(), "file.txt");

    // Versioned object (not latest, not delete marker)
    let stream_obj = StreamObject {
        object: object.clone(),
        version_id: Some("v123".to_string()),
        is_latest: Some(false),
        is_delete_marker: false,
        tags: None,
    };
    assert_eq!(stream_obj.display_key(), "file.txt?versionId=v123");

    // Latest version (not delete marker)
    let stream_obj = StreamObject {
        object: object.clone(),
        version_id: Some("v456".to_string()),
        is_latest: Some(true),
        is_delete_marker: false,
        tags: None,
    };
    assert_eq!(stream_obj.display_key(), "file.txt?versionId=v456 (latest)");

    // Delete marker (not latest)
    let stream_obj = StreamObject {
        object: object.clone(),
        version_id: Some("v789".to_string()),
        is_latest: Some(false),
        is_delete_marker: true,
        tags: None,
    };
    assert_eq!(
        stream_obj.display_key(),
        "file.txt?versionId=v789 (delete marker)"
    );

    // Delete marker that is also latest
    let stream_obj = StreamObject {
        object: object.clone(),
        version_id: Some("v999".to_string()),
        is_latest: Some(true),
        is_delete_marker: true,
        tags: None,
    };
    assert_eq!(
        stream_obj.display_key(),
        "file.txt?versionId=v999 (latest) (delete marker)"
    );

    // Delete marker with no is_latest info
    let stream_obj = StreamObject {
        object,
        version_id: Some("v111".to_string()),
        is_latest: None,
        is_delete_marker: true,
        tags: None,
    };
    assert_eq!(
        stream_obj.display_key(),
        "file.txt?versionId=v111 (delete marker)"
    );
}
