use super::*;
use crate::testing::{FakeS3Client, ListCall, opts, replay_s3_client};
use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::DateTime;
use aws_sdk_s3::primitives::SdkBody;
use aws_sdk_s3::types::{DeleteMarkerEntry, ObjectVersion};
use aws_smithy_runtime::client::http::test_util::{ReplayEvent, StaticReplayClient};
use http::{HeaderValue, StatusCode};
use std::time::Duration;

fn options(prefix: Option<&str>, page_size: u16, maxdepth: Option<usize>) -> ListingOptions {
    ListingOptions {
        path: S3Path {
            bucket: "test-bucket".to_string(),
            prefix: prefix.map(str::to_string),
        },
        page_size,
        maxdepth,
        all_versions: false,
    }
}

fn versioned(prefix: Option<&str>) -> ListingOptions {
    ListingOptions {
        all_versions: true,
        ..options(prefix, 1000, None)
    }
}

async fn collect(
    client: &impl S3Client,
    options: &ListingOptions,
) -> S3FindResult<Vec<StreamObject>> {
    list_objects(client, options).try_collect().await
}

fn keys(objects: &[StreamObject]) -> Vec<&str> {
    objects.iter().map(|object| object.key().unwrap()).collect()
}

#[test]
fn test_listing_options_from_opts() {
    let options = ListingOptions::from_opts(&opts(&[
        "s3://test-bucket/test-prefix",
        "--number",
        "500",
        "--maxdepth",
        "2",
    ]));

    assert_eq!(options.path.bucket, "test-bucket");
    assert_eq!(options.path.prefix.as_deref(), Some("test-prefix"));
    assert_eq!(options.page_size, 500);
    assert_eq!(options.maxdepth, Some(2));
    assert!(!options.all_versions);

    let options = ListingOptions::from_opts(&opts(&["s3://another-bucket", "--all-versions"]));
    assert_eq!(options.path.prefix, None);
    assert_eq!(options.page_size, 1000);
    assert!(options.all_versions);
}

#[tokio::test]
async fn test_flat_listing_follows_continuation_tokens() {
    let client = FakeS3Client::default().with_objects(&[
        ("data/a", 1),
        ("data/b", 2),
        ("data/c/d", 3),
        ("other/e", 4),
    ]);

    let objects = collect(&client, &options(Some("data/"), 2, None))
        .await
        .unwrap();

    assert_eq!(keys(&objects), ["data/a", "data/b", "data/c/d"]);
    assert_eq!(
        *client.list_calls.lock().unwrap(),
        [
            ListCall {
                prefix: "data/".to_string(),
                delimiter: None,
                continuation_token: None,
                start_after: None,
            },
            ListCall {
                prefix: "data/".to_string(),
                delimiter: None,
                continuation_token: Some("data/b".to_string()),
                start_after: None,
            },
        ]
    );
}

#[tokio::test]
async fn test_listing_without_prefix_lists_whole_bucket() {
    let client = FakeS3Client::default().with_objects(&[("a", 1), ("b/c", 1)]);

    let objects = collect(&client, &options(None, 1000, None)).await.unwrap();

    assert_eq!(keys(&objects), ["a", "b/c"]);
    assert_eq!(client.list_calls.lock().unwrap()[0].prefix, "");
}

#[tokio::test]
async fn test_listing_error_ends_stream() {
    let client = FakeS3Client::default().with_list_error("boom");

    let results: Vec<_> = list_objects(&client, &options(None, 1000, None))
        .collect()
        .await;

    assert_eq!(results.len(), 1);
    let err = results.into_iter().next().unwrap().unwrap_err();
    assert!(matches!(err, S3FindError::ListObjects { .. }));
    assert_eq!(err.to_string(), "failed to list S3 objects: boom");
}

fn bucket_with(keys: &[String]) -> FakeS3Client {
    let objects: Vec<_> = keys.iter().map(|key| (key.as_str(), 1)).collect();
    FakeS3Client::default().with_objects(&objects)
}

fn sorted(keys: &[String]) -> Vec<&str> {
    let mut sorted: Vec<_> = keys.iter().map(String::as_str).collect();
    sorted.sort_unstable();
    sorted
}

fn split_calls(client: &FakeS3Client) -> Vec<ListCall> {
    client
        .list_calls
        .lock()
        .unwrap()
        .iter()
        .filter(|call| call.delimiter.is_some())
        .cloned()
        .collect()
}

/// Keys around directory boundaries: `a.txt` and `a0` sort on either side
/// of `a/`, which is also a folder marker.
fn wide_tree() -> Vec<String> {
    let mut keys = vec!["a.txt".to_owned(), "a/".to_owned(), "a0".to_owned()];
    for dir in ["a", "b", "c", "d", "e"] {
        keys.push(format!("{dir}/top.txt"));
        for sub in ["x", "y"] {
            keys.extend((0..3).map(|i| format!("{dir}/{sub}/{i}.txt")));
        }
    }
    keys.push("z.txt".to_owned());
    keys
}

#[tokio::test]
async fn test_split_flat_listing_keeps_key_order() {
    let keys = wide_tree();

    for page_size in 1..=6 {
        let client = bucket_with(&keys);
        let objects = collect(&client, &options(None, page_size, None))
            .await
            .unwrap();

        assert_eq!(
            super::tests::keys(&objects),
            sorted(&keys),
            "page size {page_size}"
        );
        assert!(!split_calls(&client).is_empty(), "page size {page_size}");
    }

    let client = bucket_with(&keys);
    let objects = collect(&client, &options(Some("c/"), 2, None))
        .await
        .unwrap();
    let under_c: Vec<_> = keys
        .iter()
        .filter(|key| key.starts_with("c/"))
        .cloned()
        .collect();
    assert_eq!(super::tests::keys(&objects), sorted(&under_c));
}

#[tokio::test(start_paused = true)]
async fn test_split_flat_listing_lists_ranges_concurrently() {
    let keys: Vec<String> = (0..8)
        .flat_map(|dir| (0..10).map(move |i| format!("d{dir}/{i}")))
        .collect();
    let client = bucket_with(&keys).with_list_delay(10);

    let start = tokio::time::Instant::now();
    let objects = collect(&client, &options(None, 5, None)).await.unwrap();

    assert_eq!(super::tests::keys(&objects), sorted(&keys));
    // Listed one page after another, 80 keys take 16 calls. Here: two pages,
    // one call for the directories after d0/9, then one range per directory,
    // all at once; the longest range takes three calls.
    assert_eq!(start.elapsed(), Duration::from_millis(10 * (2 + 1 + 3)));
    assert_eq!(
        split_calls(&client),
        [ListCall {
            prefix: String::new(),
            delimiter: Some("/".to_owned()),
            continuation_token: None,
            start_after: Some("d0/9".to_owned()),
        }]
    );
}

#[tokio::test]
async fn test_split_flat_listing_ends_with_the_error_of_any_range() {
    let keys: Vec<String> = (0..8)
        .flat_map(|dir| (0..10).map(move |i| format!("d{dir}/{i}")))
        .collect();
    let client = bucket_with(&keys).with_failing_start_after("d5/");

    let err = collect(&client, &options(None, 5, None)).await.unwrap_err();

    assert_eq!(err.to_string(), "failed to list S3 objects: SlowDown");
}

#[tokio::test]
async fn test_flat_listing_without_directories_continues_page_by_page() {
    let keys: Vec<String> = (0..10).map(|i| format!("k{i:02}")).collect();
    let client = bucket_with(&keys);

    let objects = collect(&client, &options(None, 2, None)).await.unwrap();

    assert_eq!(super::tests::keys(&objects), sorted(&keys));
    // Five pages, plus one look for directories to split at.
    assert_eq!(client.list_calls.lock().unwrap().len(), 6);
    assert_eq!(split_calls(&client).len(), 1);
}

#[tokio::test]
async fn test_flat_listing_splits_inside_the_only_directory() {
    let keys: Vec<String> = ["a", "b", "c"]
        .iter()
        .flat_map(|dir| (0..4).map(move |i| format!("data/{dir}/{i}")))
        .collect();
    let client = bucket_with(&keys);

    let objects = collect(&client, &options(None, 2, None)).await.unwrap();

    assert_eq!(super::tests::keys(&objects), sorted(&keys));
    let prefixes: Vec<_> = split_calls(&client)
        .into_iter()
        .map(|call| call.prefix)
        .collect();
    assert_eq!(prefixes, ["", "data/"]);
}

#[test]
fn test_spread_keeps_evenly_spaced_items_in_order() {
    let items: Vec<usize> = (0..100).collect();
    assert_eq!(spread(items.clone(), 3), [25, 50, 75]);
    assert_eq!(spread(items[..3].to_vec(), 5), [0, 1, 2]);
}

fn hierarchy() -> FakeS3Client {
    FakeS3Client::default().with_objects(&[
        ("root.txt", 1),
        ("dir1/file.txt", 1),
        ("dir1/subdir/deep.txt", 1),
        ("dir1/subdir/deeper/verydeep.txt", 1),
        ("dir2/file.txt", 1),
    ])
}

#[tokio::test]
async fn test_maxdepth_limits_descent() {
    let client = hierarchy();

    let depth0 = collect(&client, &options(None, 1000, Some(0)))
        .await
        .unwrap();
    assert_eq!(keys(&depth0), ["root.txt"]);

    let depth1 = collect(&client, &options(None, 1000, Some(1)))
        .await
        .unwrap();
    assert_eq!(
        keys(&depth1),
        ["root.txt", "dir1/file.txt", "dir2/file.txt"]
    );

    let depth2 = collect(&client, &options(None, 1000, Some(2)))
        .await
        .unwrap();
    assert_eq!(
        keys(&depth2),
        [
            "root.txt",
            "dir1/file.txt",
            "dir1/subdir/deep.txt",
            "dir2/file.txt"
        ]
    );
}

#[tokio::test]
async fn test_maxdepth_lists_each_directory_with_delimiter() {
    let client = hierarchy();

    collect(&client, &options(None, 1000, Some(1)))
        .await
        .unwrap();

    let calls = client.list_calls.lock().unwrap();
    let prefixes: Vec<_> = calls.iter().map(|call| call.prefix.as_str()).collect();
    assert_eq!(prefixes, ["", "dir1/", "dir2/"]);
    assert!(
        calls
            .iter()
            .all(|call| call.delimiter.as_deref() == Some("/"))
    );
}

#[tokio::test(start_paused = true)]
async fn test_maxdepth_lists_subdirectories_concurrently() {
    let subdirectories: usize = 12;
    let expected: Vec<String> = (0..subdirectories)
        .map(|i| format!("dir{i:02}/file.txt"))
        .collect();
    let objects: Vec<_> = expected.iter().map(|key| (key.as_str(), 1)).collect();
    let client = FakeS3Client::default()
        .with_objects(&objects)
        .with_list_delay(100);

    let start = tokio::time::Instant::now();
    let listed = collect(&client, &options(None, 1000, Some(1)))
        .await
        .unwrap();

    assert_eq!(keys(&listed), expected);
    // The root, then the subdirectories SUBDIRECTORY_PREFETCH at a time,
    // instead of one listing after another.
    let rounds = 1 + subdirectories.div_ceil(SUBDIRECTORY_PREFETCH) as u64;
    assert_eq!(start.elapsed(), Duration::from_millis(100 * rounds));
}

#[tokio::test(start_paused = true)]
async fn test_maxdepth_prefetch_only_descends_into_the_walk_being_read() {
    let client = FakeS3Client::default()
        .with_objects(&[
            ("a/1/file.txt", 1),
            ("a/2/file.txt", 1),
            ("b/1/file.txt", 1),
            ("b/2/file.txt", 1),
        ])
        .with_list_delay(100);

    let options = options(None, 1000, Some(2));
    let mut objects = list_objects(&client, &options);
    let first = objects.next().await.unwrap().unwrap();
    assert_eq!(first.key(), Some("a/1/file.txt"));

    // b/ had its first page fetched ahead, but isn't walked until it is read.
    let calls = client.list_calls.lock().unwrap();
    let prefixes: Vec<_> = calls.iter().map(|call| call.prefix.as_str()).collect();
    assert_eq!(prefixes, ["", "a/", "b/", "a/1/", "a/2/"]);
}

#[tokio::test]
async fn test_maxdepth_follows_pages_within_a_level() {
    let client = FakeS3Client::default().with_objects(&[
        ("a.txt", 1),
        ("b.txt", 1),
        ("x/1.txt", 1),
        ("y/1.txt", 1),
        ("z.txt", 1),
    ]);

    let objects = collect(&client, &options(None, 2, Some(1))).await.unwrap();

    // Each page's objects come first, then its subdirectories, depth first.
    assert_eq!(
        keys(&objects),
        ["a.txt", "b.txt", "x/1.txt", "y/1.txt", "z.txt"]
    );
}

#[tokio::test]
async fn test_maxdepth_error_in_subdirectory_ends_stream() {
    let client = FakeS3Client::default().with_list_error("denied");

    let results: Vec<_> = list_objects(&client, &options(None, 1000, Some(3)))
        .collect()
        .await;

    assert_eq!(results.len(), 1);
    assert!(matches!(results[0], Err(S3FindError::ListObjects { .. })));
}

fn version(key: &str, id: &str, latest: bool, modified_secs: i64) -> ObjectVersion {
    ObjectVersion::builder()
        .key(key)
        .version_id(id)
        .is_latest(latest)
        .last_modified(DateTime::from_secs(modified_secs))
        .size(10)
        .build()
}

fn delete_marker(key: &str, id: &str, modified_secs: i64) -> DeleteMarkerEntry {
    DeleteMarkerEntry::builder()
        .key(key)
        .version_id(id)
        .is_latest(true)
        .last_modified(DateTime::from_secs(modified_secs))
        .build()
}

#[tokio::test]
async fn test_versions_sorted_by_key_then_newest_first() {
    let client = FakeS3Client::default().with_version_pages(vec![(
        vec![
            version("b.txt", "b-old", false, 100),
            version("a.txt", "a1", true, 200),
            version("b.txt", "b-new", false, 300),
        ],
        vec![delete_marker("b.txt", "b-dm", 400)],
    )]);

    let objects = collect(&client, &versioned(None)).await.unwrap();

    let ids: Vec<_> = objects
        .iter()
        .map(|object| object.version_id.as_deref().unwrap())
        .collect();
    assert_eq!(ids, ["a1", "b-dm", "b-new", "b-old"]);
    assert!(objects[1].is_delete_marker);
    assert_eq!(objects[1].object.size(), Some(0));
}

#[tokio::test]
async fn test_versions_follow_markers_across_pages() {
    let client = FakeS3Client::default().with_version_pages(vec![
        (vec![version("a.txt", "a1", true, 1)], vec![]),
        (vec![version("b.txt", "b1", true, 1)], vec![]),
    ]);

    let objects = collect(&client, &versioned(Some("data"))).await.unwrap();

    assert_eq!(keys(&objects), ["a.txt", "b.txt"]);
    let calls = client.list_version_calls.lock().unwrap();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0], None);
    assert_eq!(calls[1].as_ref().unwrap().key_marker, "1");
}

#[tokio::test]
async fn test_all_versions_takes_precedence_over_maxdepth() {
    let client = FakeS3Client::default()
        .with_objects(&[("a.txt", 1)])
        .with_version_pages(vec![(vec![version("a.txt", "v1", true, 1)], vec![])]);
    let options = ListingOptions {
        maxdepth: Some(0),
        ..versioned(None)
    };

    let objects = collect(&client, &options).await.unwrap();

    assert_eq!(objects[0].version_id.as_deref(), Some("v1"));
    assert!(client.list_calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn test_versions_error_ends_stream() {
    let client = FakeS3Client::default().with_list_error("boom");

    let result = collect(&client, &versioned(None)).await;

    assert!(matches!(
        result,
        Err(S3FindError::ListObjectVersions { .. })
    ));
}

// The tests below run listing through the real SDK client against replayed
// HTTP responses, covering request building and response parsing.

fn replay(events: Vec<(&str, StatusCode, &str)>) -> (Client, StaticReplayClient) {
    let events = events
        .into_iter()
        .map(|(query, status, body)| {
            let request = http::Request::builder()
                .method("GET")
                .uri(format!(
                    "https://test-bucket.s3.us-east-1.amazonaws.com/?{query}"
                ))
                .body(SdkBody::empty())
                .unwrap();
            let response = http::Response::builder()
                .status(status)
                .header("Content-Type", HeaderValue::from_static("application/xml"))
                .body(SdkBody::from(body.to_string()))
                .unwrap();
            ReplayEvent::new(request, response)
        })
        .collect();
    replay_s3_client(events)
}

fn list_result(body: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
        <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">{body}</ListBucketResult>"#
    )
}

#[tokio::test]
async fn test_sdk_flat_listing_paginates() {
    let page1 = list_result(
        r#"<IsTruncated>true</IsTruncated>
        <NextContinuationToken>1ueGcxLPRx1Tr/XYExHnhbYLgveDs2J/wm36Hy4vbOwM=</NextContinuationToken>
        <Contents>
            <Key>test-prefix/file1.txt</Key>
            <LastModified>2023-01-01T00:00:00.000Z</LastModified>
            <ETag>&quot;d41d8cd98f00b204e9800998ecf8427e&quot;</ETag>
            <Size>100</Size>
            <StorageClass>STANDARD</StorageClass>
        </Contents>
        <Contents><Key>test-prefix/file2.txt</Key><Size>200</Size></Contents>"#,
    );
    let page2 = list_result(
        r#"<IsTruncated>false</IsTruncated>
        <Contents><Key>test-prefix/file3.txt</Key><Size>300</Size></Contents>"#,
    );
    let (client, replay_client) = replay(vec![
        (
            "list-type=2&max-keys=1000&prefix=test-prefix%2F",
            StatusCode::OK,
            &page1,
        ),
        (
            "list-type=2&max-keys=1000&prefix=test-prefix%2F&continuation-token=1ueGcxLPRx1Tr%2FXYExHnhbYLgveDs2J%2Fwm36Hy4vbOwM%3D",
            StatusCode::OK,
            &page2,
        ),
    ]);

    let objects = collect(&client, &options(Some("test-prefix/"), 1000, None))
        .await
        .unwrap();

    assert_eq!(
        keys(&objects),
        [
            "test-prefix/file1.txt",
            "test-prefix/file2.txt",
            "test-prefix/file3.txt"
        ]
    );
    assert_eq!(objects[0].object.size(), Some(100));
    assert_eq!(
        objects[0]
            .object
            .storage_class()
            .map(|class| class.as_str()),
        Some("STANDARD")
    );
    replay_client.assert_requests_match(&[]);
}

#[tokio::test]
async fn test_sdk_listing_error_includes_s3_error_code() {
    let (client, _replay_client) = replay(vec![(
        "list-type=2&max-keys=1000&prefix=test-prefix%2F",
        StatusCode::FORBIDDEN,
        r#"<?xml version="1.0" encoding="UTF-8"?>
        <Error>
            <Code>AccessDenied</Code>
            <Message>Access Denied</Message>
            <RequestId>1D5H9EXAMPLE</RequestId>
        </Error>"#,
    )]);

    let err = collect(&client, &options(Some("test-prefix/"), 1000, None))
        .await
        .unwrap_err();

    assert!(matches!(err, S3FindError::ListObjects { .. }));
    assert_eq!(
        err.to_string(),
        r#"failed to list S3 objects: service error: unhandled error (AccessDenied): Error { code: "AccessDenied", message: "Access Denied" }"#
    );
}

#[tokio::test]
async fn test_sdk_empty_listing() {
    let (client, replay_client) = replay(vec![(
        "list-type=2&max-keys=1000&prefix=empty-prefix%2F",
        StatusCode::OK,
        &list_result("<IsTruncated>false</IsTruncated>"),
    )]);

    let objects = collect(&client, &options(Some("empty-prefix/"), 1000, None))
        .await
        .unwrap();

    assert!(objects.is_empty());
    replay_client.assert_requests_match(&[]);
}

#[tokio::test]
async fn test_sdk_listing_keeps_empty_contents_entry() {
    let (client, _replay_client) = replay(vec![(
        "list-type=2&max-keys=1000&prefix=empty-prefix%2F",
        StatusCode::OK,
        &list_result("<IsTruncated>false</IsTruncated><Contents></Contents>"),
    )]);

    let objects = collect(&client, &options(Some("empty-prefix/"), 1000, None))
        .await
        .unwrap();

    assert_eq!(objects.len(), 1);
    assert_eq!(objects[0].key(), None);
}

#[tokio::test]
async fn test_sdk_maxdepth_uses_delimiter_per_level() {
    let root = list_result(
        r#"<IsTruncated>false</IsTruncated>
        <Contents><Key>logs/root.txt</Key><Size>100</Size></Contents>
        <CommonPrefixes><Prefix>logs/2024/</Prefix></CommonPrefixes>"#,
    );
    let level1 = list_result(
        r#"<IsTruncated>false</IsTruncated>
        <Contents><Key>logs/2024/jan.txt</Key><Size>200</Size></Contents>
        <CommonPrefixes><Prefix>logs/2024/01/</Prefix></CommonPrefixes>"#,
    );
    let (client, replay_client) = replay(vec![
        (
            "list-type=2&delimiter=%2F&max-keys=100&prefix=logs%2F",
            StatusCode::OK,
            &root,
        ),
        (
            "list-type=2&delimiter=%2F&max-keys=100&prefix=logs%2F2024%2F",
            StatusCode::OK,
            &level1,
        ),
    ]);

    // Inside logs/, root.txt is at depth 1 and 2024/jan.txt at depth 2.
    let objects = collect(&client, &options(Some("logs/"), 100, Some(2)))
        .await
        .unwrap();

    // logs/2024/01/ is three levels deep and is not listed.
    assert_eq!(keys(&objects), ["logs/root.txt", "logs/2024/jan.txt"]);
    replay_client.assert_requests_match(&[]);
}

#[tokio::test]
async fn test_sdk_listing_sends_start_after() {
    let (client, replay_client) = replay(vec![(
        "list-type=2&delimiter=%2F&max-keys=10&prefix=logs%2F&start-after=logs%2Fa%2F9",
        StatusCode::OK,
        &list_result("<IsTruncated>false</IsTruncated>"),
    )]);

    let request = ListObjectsRequest {
        bucket: "test-bucket",
        prefix: "logs/",
        delimiter: Some("/"),
        continuation_token: None,
        start_after: Some("logs/a/9"),
        max_keys: 10,
    };
    S3Client::list_objects(&client, request).await.unwrap();

    replay_client.assert_requests_match(&[]);
}

#[tokio::test]
async fn test_sdk_versions_listing() {
    let body = r#"<?xml version="1.0" encoding="UTF-8"?>
        <ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
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
            </Version>
            <DeleteMarker>
                <Key>data/deleted.txt</Key>
                <VersionId>dm1</VersionId>
                <IsLatest>true</IsLatest>
                <LastModified>2023-02-01T00:00:00.000Z</LastModified>
            </DeleteMarker>
        </ListVersionsResult>"#;
    let (client, replay_client) = replay(vec![(
        "versions&max-keys=1000&prefix=data%2F",
        StatusCode::OK,
        body,
    )]);

    let objects = collect(&client, &versioned(Some("data/"))).await.unwrap();

    let display: Vec<_> = objects
        .iter()
        .map(|object| object.display_key().to_string())
        .collect();
    assert_eq!(
        display,
        [
            "data/deleted.txt?versionId=dm1 (latest) (delete marker)",
            "data/file1.txt?versionId=v1 (latest)",
            "data/file1.txt?versionId=v0",
        ]
    );
    replay_client.assert_requests_match(&[]);
}

#[tokio::test]
async fn test_sdk_versions_listing_follows_markers() {
    let page1 = r#"<?xml version="1.0" encoding="UTF-8"?>
        <ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <IsTruncated>true</IsTruncated>
            <NextKeyMarker>a.txt</NextKeyMarker>
            <NextVersionIdMarker>a1</NextVersionIdMarker>
            <Version><Key>a.txt</Key><VersionId>a1</VersionId></Version>
        </ListVersionsResult>"#;
    let page2 = r#"<?xml version="1.0" encoding="UTF-8"?>
        <ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <IsTruncated>false</IsTruncated>
            <Version><Key>b.txt</Key><VersionId>b1</VersionId></Version>
        </ListVersionsResult>"#;
    let (client, replay_client) = replay(vec![
        ("versions&max-keys=1000&prefix=", StatusCode::OK, page1),
        (
            "versions&key-marker=a.txt&max-keys=1000&prefix=&version-id-marker=a1",
            StatusCode::OK,
            page2,
        ),
    ]);

    let objects = collect(&client, &versioned(None)).await.unwrap();

    assert_eq!(keys(&objects), ["a.txt", "b.txt"]);
    replay_client.assert_requests_match(&[]);
}
