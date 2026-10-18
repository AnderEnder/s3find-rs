use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures::{FutureExt, Stream, TryStreamExt};

use crate::adapters::aws::S3Client;
use crate::error::{S3FindResult, TagFetchError};
use crate::object::StreamObject;

/// Default maximum of concurrent GetObjectTagging requests.
pub const DEFAULT_TAG_CONCURRENCY: usize = 50;

/// Counters for tag fetching, safe to update from concurrent requests.
#[derive(Debug, Default)]
pub struct TagFetchStats {
    pub success: AtomicUsize,
    pub failed: AtomicUsize,
    pub access_denied: AtomicUsize,
    pub excluded: AtomicUsize,
}

impl TagFetchStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_success(&self) {
        self.success.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_failure(&self) {
        self.failed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_access_denied(&self) {
        self.access_denied.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_excluded(&self) {
        self.excluded.fetch_add(1, Ordering::Relaxed);
    }

    /// True when no tag fetch was attempted.
    pub fn is_empty(&self) -> bool {
        [
            &self.success,
            &self.failed,
            &self.access_denied,
            &self.excluded,
        ]
        .iter()
        .all(|counter| counter.load(Ordering::Relaxed) == 0)
    }
}

impl fmt::Display for TagFetchStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} success, {} failed, {} access denied, {} excluded",
            self.success.load(Ordering::Relaxed),
            self.failed.load(Ordering::Relaxed),
            self.access_denied.load(Ordering::Relaxed),
            self.excluded.load(Ordering::Relaxed),
        )
    }
}

/// Populates `StreamObject::tags` with GetObjectTagging, with bounded concurrency.
pub struct TagFetcher {
    bucket: String,
    concurrency: usize,
    stats: TagFetchStats,
}

impl TagFetcher {
    pub fn new(bucket: impl Into<String>, concurrency: usize) -> Self {
        Self {
            bucket: bucket.into(),
            concurrency: concurrency.max(1),
            stats: TagFetchStats::new(),
        }
    }

    pub fn concurrency(&self) -> usize {
        self.concurrency
    }

    pub fn stats(&self) -> &TagFetchStats {
        &self.stats
    }

    /// Fetches tags for each object in the stream, keeping up to `concurrency`
    /// requests in flight and the input order intact.
    pub fn attach_tags_to_stream<'a, C, S>(
        &'a self,
        client: &'a C,
        objects: S,
    ) -> impl Stream<Item = S3FindResult<StreamObject>> + Send + 'a
    where
        C: S3Client,
        S: Stream<Item = S3FindResult<StreamObject>> + Send + 'a,
    {
        objects
            .map_ok(move |object| self.attach_tags(client, object).map(Ok))
            .try_buffered(self.concurrency)
    }

    /// Makes sure `object.tags` is populated.
    ///
    /// Objects whose tags can't be fetched get an empty tag set, so they never
    /// match a tag filter: an object is only included when its tags are verified.
    pub async fn attach_tags(
        &self,
        client: &impl S3Client,
        mut object: StreamObject,
    ) -> StreamObject {
        if object.tags.is_some() {
            return object;
        }

        // Delete markers and keyless objects have no tags to fetch.
        let Some(key) = object.key().filter(|_| !object.is_delete_marker) else {
            object.tags = Some(Vec::new());
            return object;
        };

        let tags = match client
            .get_object_tagging(&self.bucket, key, object.version_id.as_deref())
            .await
        {
            Ok(tags) => {
                self.stats.record_success();
                tags
            }
            Err(err) => {
                match err {
                    TagFetchError::AccessDenied { .. } => self.stats.record_access_denied(),
                    _ => self.stats.record_failure(),
                }
                self.stats.record_excluded();
                eprintln!("Warning: Failed to fetch tags for {key}: {err}");
                Vec::new()
            }
        };

        object.tags = Some(tags);
        object
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{FakeS3Client, replay_s3_client};
    use aws_sdk_s3::primitives::SdkBody;
    use aws_sdk_s3::types::{Object, Tag};
    use aws_smithy_runtime::client::http::test_util::ReplayEvent;
    use futures::{StreamExt, stream};
    use http::{HeaderValue, StatusCode};

    fn keyed(key: &str) -> StreamObject {
        StreamObject::from_object(Object::builder().key(key).build())
    }

    fn tag(key: &str, value: &str) -> Tag {
        Tag::builder().key(key).value(value).build().unwrap()
    }

    fn fetcher(concurrency: usize) -> TagFetcher {
        TagFetcher::new("test-bucket", concurrency)
    }

    async fn attach_all(
        fetcher: &TagFetcher,
        client: &impl S3Client,
        objects: Vec<StreamObject>,
    ) -> Vec<StreamObject> {
        fetcher
            .attach_tags_to_stream(client, stream::iter(objects.into_iter().map(Ok)))
            .map(Result::unwrap)
            .collect()
            .await
    }

    #[test]
    fn test_tag_fetch_stats() {
        let stats = TagFetchStats::new();
        assert!(stats.is_empty());

        stats.record_success();
        stats.record_success();
        stats.record_failure();
        stats.record_access_denied();
        stats.record_excluded();

        assert!(!stats.is_empty());
        assert_eq!(stats.success.load(Ordering::Relaxed), 2);
        assert_eq!(stats.failed.load(Ordering::Relaxed), 1);
        assert_eq!(stats.access_denied.load(Ordering::Relaxed), 1);
        assert_eq!(stats.excluded.load(Ordering::Relaxed), 1);
        assert_eq!(
            stats.to_string(),
            "2 success, 1 failed, 1 access denied, 1 excluded"
        );
    }

    #[test]
    fn test_tag_fetch_stats_concurrent_updates() {
        use std::sync::Arc;
        use std::thread;

        let stats = Arc::new(TagFetchStats::new());
        let handles: Vec<_> = (0..4)
            .map(|_| {
                let stats = Arc::clone(&stats);
                thread::spawn(move || {
                    for _ in 0..100 {
                        stats.record_success();
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(stats.success.load(Ordering::Relaxed), 400);
    }

    #[test]
    fn test_concurrency_is_at_least_one() {
        assert_eq!(fetcher(0).concurrency(), 1);
        assert_eq!(fetcher(7).concurrency(), 7);
    }

    #[tokio::test]
    async fn test_attach_tags_fetches_with_version_id() {
        let client = FakeS3Client::default().with_tags(
            "test-bucket",
            "versioned.txt",
            Some("v123"),
            vec![tag("version", "v123")],
        );
        let fetcher = fetcher(1);

        let mut object = keyed("versioned.txt");
        object.version_id = Some("v123".to_string());

        let object = fetcher.attach_tags(&client, object).await;

        assert_eq!(object.get_tag("version"), Some("v123"));
        assert_eq!(fetcher.stats().success.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_attach_tags_skips_objects_without_fetchable_tags() {
        let client = FakeS3Client::default();
        let fetcher = fetcher(1);

        let cached = {
            let mut object = keyed("cached.txt");
            object.tags = Some(vec![tag("cached", "true")]);
            object
        };
        let delete_marker = {
            let mut object = keyed("deleted.txt");
            object.is_delete_marker = true;
            object
        };
        let keyless = StreamObject::from_object(Object::builder().build());

        let results = attach_all(&fetcher, &client, vec![cached, delete_marker, keyless]).await;

        assert_eq!(results[0].get_tag("cached"), Some("true"));
        assert_eq!(results[1].tags, Some(Vec::new()));
        assert_eq!(results[2].tags, Some(Vec::new()));
        assert!(fetcher.stats().is_empty(), "no API calls expected");
    }

    #[tokio::test]
    async fn test_attach_tags_failures_yield_empty_tags() {
        let client = FakeS3Client::default()
            .with_tag_error("test-bucket", "forbidden.txt", 403)
            .with_tag_error("test-bucket", "missing.txt", 404)
            .with_tag_error("test-bucket", "broken.txt", 500);
        let fetcher = fetcher(2);

        let results = attach_all(
            &fetcher,
            &client,
            vec![
                keyed("forbidden.txt"),
                keyed("missing.txt"),
                keyed("broken.txt"),
            ],
        )
        .await;

        assert!(results.iter().all(|object| object.tags == Some(Vec::new())));
        let stats = fetcher.stats();
        assert_eq!(stats.access_denied.load(Ordering::Relaxed), 1);
        assert_eq!(stats.failed.load(Ordering::Relaxed), 2);
        assert_eq!(stats.excluded.load(Ordering::Relaxed), 3);
        assert_eq!(stats.success.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn test_attach_tags_to_stream_preserves_order() {
        let client = FakeS3Client::default()
            .with_tag_delay("test-bucket", "slow.txt", 30)
            .with_tags("test-bucket", "slow.txt", None, vec![tag("n", "1")])
            .with_tags("test-bucket", "fast.txt", None, vec![tag("n", "2")]);
        let fetcher = fetcher(2);

        let results = attach_all(
            &fetcher,
            &client,
            vec![keyed("slow.txt"), keyed("fast.txt")],
        )
        .await;

        let keys: Vec<_> = results.iter().map(|object| object.key().unwrap()).collect();
        assert_eq!(keys, ["slow.txt", "fast.txt"]);
        assert_eq!(results[0].get_tag("n"), Some("1"));
        assert_eq!(results[1].get_tag("n"), Some("2"));
    }

    #[tokio::test]
    async fn test_attach_tags_to_stream_propagates_upstream_errors() {
        let client = FakeS3Client::default();
        let fetcher = fetcher(2);
        let upstream = stream::iter(vec![Err(crate::error::S3FindError::list_objects(
            anyhow::anyhow!("listing failed"),
        ))]);

        let results: Vec<_> = fetcher
            .attach_tags_to_stream(&client, upstream)
            .collect()
            .await;

        assert_eq!(results.len(), 1);
        assert!(results[0].is_err());
    }

    // The tests below go through the real SDK client to cover the adapter's
    // request building and error classification.

    fn tagging_request(uri_suffix: &str) -> http::Request<SdkBody> {
        http::Request::builder()
            .method("GET")
            .uri(format!(
                "https://test-bucket.s3.us-east-1.amazonaws.com/{uri_suffix}"
            ))
            .body(SdkBody::empty())
            .unwrap()
    }

    fn tag_response(uri_suffix: &str, tags: &[(&str, &str)]) -> ReplayEvent {
        let tag_xml: String = tags
            .iter()
            .map(|(k, v)| format!("<Tag><Key>{k}</Key><Value>{v}</Value></Tag>"))
            .collect();

        let resp = http::Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", HeaderValue::from_static("application/xml"))
            .body(SdkBody::from(format!(
                r#"<?xml version="1.0" encoding="UTF-8"?>
                <Tagging xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                    <TagSet>{tag_xml}</TagSet>
                </Tagging>"#
            )))
            .unwrap();

        ReplayEvent::new(tagging_request(uri_suffix), resp)
    }

    fn error_response(uri_suffix: &str, status: u16) -> ReplayEvent {
        let resp = http::Response::builder()
            .status(StatusCode::from_u16(status).unwrap())
            .header("Content-Type", HeaderValue::from_static("application/xml"))
            .body(SdkBody::from(""))
            .unwrap();

        ReplayEvent::new(tagging_request(uri_suffix), resp)
    }

    #[tokio::test]
    async fn test_sdk_client_fetches_tags() {
        let (client, replay) = replay_s3_client(vec![
            tag_response("file1.txt?tagging", &[("env", "prod"), ("team", "data")]),
            tag_response(
                "versioned.txt?tagging&versionId=v123",
                &[("version", "v123")],
            ),
        ]);
        let fetcher = fetcher(1);

        let mut versioned = keyed("versioned.txt");
        versioned.version_id = Some("v123".to_string());

        let results = attach_all(&fetcher, &client, vec![keyed("file1.txt"), versioned]).await;

        assert_eq!(results[0].get_tag("env"), Some("prod"));
        assert_eq!(results[0].get_tag("team"), Some("data"));
        assert_eq!(results[1].get_tag("version"), Some("v123"));
        assert_eq!(fetcher.stats().success.load(Ordering::Relaxed), 2);
        replay.assert_requests_match(&[]);
    }

    #[tokio::test]
    async fn test_sdk_client_classifies_tagging_errors() {
        let (client, replay) = replay_s3_client(vec![
            error_response("forbidden.txt?tagging", 403),
            error_response("missing.txt?tagging", 404),
        ]);
        let fetcher = fetcher(1);

        let results = attach_all(
            &fetcher,
            &client,
            vec![keyed("forbidden.txt"), keyed("missing.txt")],
        )
        .await;

        assert!(results.iter().all(|object| object.tags == Some(Vec::new())));
        let stats = fetcher.stats();
        assert_eq!(stats.access_denied.load(Ordering::Relaxed), 1);
        assert_eq!(stats.failed.load(Ordering::Relaxed), 1);
        assert_eq!(stats.excluded.load(Ordering::Relaxed), 2);
        replay.assert_requests_match(&[]);
    }
}
