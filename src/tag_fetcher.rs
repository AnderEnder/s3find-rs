use aws_sdk_s3::Client;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::get_object_tagging::GetObjectTaggingError;
use aws_sdk_s3::types::Tag;
use futures::{StreamExt, stream};
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use thiserror::Error;

use crate::command::StreamObject;

/// Error types for tag fetching operations
#[derive(Error, Debug)]
pub enum TagFetchError {
    #[error("Access denied for s3:GetObjectTagging on {bucket}/{key}")]
    AccessDenied { bucket: String, key: String },

    #[error("Object not found: {bucket}/{key}")]
    NotFound { bucket: String, key: String },

    #[error("S3 API error: {0}")]
    ApiError(String),

    #[error("Missing object key")]
    MissingKey,
}

/// Statistics for tag fetching operations
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
}

/// Configuration for tag fetching
#[derive(Debug, Clone)]
pub struct TagFetchConfig {
    /// Maximum concurrent tag fetch requests
    pub concurrency: usize,
}

impl Default for TagFetchConfig {
    fn default() -> Self {
        Self { concurrency: 50 }
    }
}

impl TagFetchConfig {
    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }
}

/// Fetches tags for a single object; throttle/transient retries are handled by the SDK.
async fn fetch_object_tags(
    client: &Client,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
    stats: &TagFetchStats,
) -> Result<Vec<Tag>, TagFetchError> {
    let mut request = client.get_object_tagging().bucket(bucket).key(key);

    if let Some(vid) = version_id {
        request = request.version_id(vid);
    }

    match request.send().await {
        Ok(output) => {
            stats.record_success();
            Ok(output.tag_set().to_vec())
        }
        Err(err) => {
            let fetch_error = classify_error(&err, bucket, key);
            match &fetch_error {
                TagFetchError::AccessDenied { .. } => stats.record_access_denied(),
                _ => stats.record_failure(),
            }
            Err(fetch_error)
        }
    }
}

/// Classifies SDK errors into TagFetchError variants
fn classify_error(err: &SdkError<GetObjectTaggingError>, bucket: &str, key: &str) -> TagFetchError {
    match err {
        SdkError::ServiceError(service_err) => {
            let raw = service_err.raw();
            let status = raw.status().as_u16();

            match status {
                403 => TagFetchError::AccessDenied {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                },
                404 => TagFetchError::NotFound {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                },
                _ => TagFetchError::ApiError(format!("HTTP {}: {:?}", status, service_err.err())),
            }
        }
        _ => TagFetchError::ApiError(err.to_string()),
    }
}

/// Fetches tags for a collection of objects with concurrency control
///
/// This function takes an iterable collection of StreamObjects, fetches tags
/// for each one concurrently (up to the configured limit), and returns a
/// `Vec<StreamObject>` with the tags populated.
async fn map_with_concurrency_in_order<I, F, Fut, T>(
    items: I,
    concurrency: usize,
    mapper: F,
) -> Vec<T>
where
    I: IntoIterator,
    F: FnMut(I::Item) -> Fut,
    Fut: Future<Output = T>,
{
    stream::iter(items.into_iter().map(mapper))
        .buffered(concurrency)
        .collect()
        .await
}

pub async fn fetch_tags_for_objects<I>(
    client: Client,
    bucket: String,
    objects: I,
    config: TagFetchConfig,
    stats: Arc<TagFetchStats>,
) -> Vec<StreamObject>
where
    I: IntoIterator<Item = StreamObject>,
{
    let objects: Vec<StreamObject> = objects.into_iter().collect();

    map_with_concurrency_in_order(objects, config.concurrency, |mut obj| {
        let client = client.clone();
        let bucket = bucket.clone();
        let config = config.clone();
        let stats = Arc::clone(&stats);

        async move {
            // Skip if already has tags
            if obj.tags.is_some() {
                return obj;
            }

            // Skip delete markers (they don't have tags)
            if obj.is_delete_marker {
                obj.tags = Some(Vec::new());
                return obj;
            }

            let key = match obj.object.key() {
                Some(k) => k.to_string(),
                None => {
                    // Object without key - can't fetch tags
                    obj.tags = Some(Vec::new());
                    return obj;
                }
            };

            let version_id = obj.version_id.as_deref();

            match fetch_object_tags(&client, &bucket, &key, version_id, &stats).await {
                Ok(tags) => {
                    obj.tags = Some(tags);
                }
                Err(e) => {
                    // Log the error but continue processing.
                    // Objects with failed tag fetches get empty tags to allow filtering
                    // to continue. This means they won't match any tag filter, which is
                    // the safest behavior (don't include objects we can't verify).
                    stats.record_excluded();
                    eprintln!("Warning: Failed to fetch tags for {}: {}", key, e);
                    obj.tags = Some(Vec::new());
                }
            }

            obj
        }
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_s3::types::Object;

    #[test]
    fn test_tag_fetch_stats() {
        let stats = TagFetchStats::new();

        stats.record_success();
        stats.record_success();
        stats.record_failure();
        stats.record_access_denied();
        stats.record_excluded();

        assert_eq!(stats.success.load(Ordering::Relaxed), 2);
        assert_eq!(stats.failed.load(Ordering::Relaxed), 1);
        assert_eq!(stats.access_denied.load(Ordering::Relaxed), 1);
        assert_eq!(stats.excluded.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_tag_fetch_config_default() {
        let config = TagFetchConfig::default();
        assert_eq!(config.concurrency, 50);
    }

    #[test]
    fn test_tag_fetch_config_with_concurrency() {
        let config = TagFetchConfig::default().with_concurrency(100);
        assert_eq!(config.concurrency, 100);
    }

    #[test]
    fn test_stream_object_with_tags() {
        let object = Object::builder().key("test.txt").build();
        let stream_obj = StreamObject {
            object,
            version_id: None,
            is_latest: None,
            is_delete_marker: false,
            tags: Some(vec![]),
        };

        assert!(stream_obj.tags.is_some());
    }

    #[test]
    fn test_delete_marker_has_empty_tags() {
        let object = Object::builder().key("deleted.txt").build();
        let mut stream_obj = StreamObject {
            object,
            version_id: None,
            is_latest: None,
            is_delete_marker: true,
            tags: None,
        };

        if stream_obj.is_delete_marker {
            stream_obj.tags = Some(Vec::new());
        }

        assert!(stream_obj.tags.is_some());
        assert!(stream_obj.tags.unwrap().is_empty());
    }

    #[test]
    fn test_classify_error_non_service_error() {
        use aws_smithy_runtime_api::client::orchestrator::HttpResponse;
        use aws_smithy_runtime_api::client::result::SdkError;

        let timeout_err: SdkError<GetObjectTaggingError, HttpResponse> = SdkError::timeout_error(
            Box::new(std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout")),
        );

        let result = classify_error(&timeout_err, "bucket", "key");
        assert!(
            matches!(result, TagFetchError::ApiError(_)),
            "Expected ApiError, got: {:?}",
            result
        );
    }

    #[test]
    fn test_tag_fetch_error_display() {
        let err = TagFetchError::AccessDenied {
            bucket: "my-bucket".to_string(),
            key: "my-key".to_string(),
        };
        assert!(err.to_string().contains("Access denied"));
        assert!(err.to_string().contains("my-bucket"));
        assert!(err.to_string().contains("my-key"));

        let err = TagFetchError::NotFound {
            bucket: "bucket".to_string(),
            key: "key".to_string(),
        };
        assert!(err.to_string().contains("Object not found"));

        let err = TagFetchError::ApiError("Custom error".to_string());
        assert!(err.to_string().contains("Custom error"));

        let err = TagFetchError::MissingKey;
        assert!(err.to_string().contains("Missing object key"));
    }

    #[test]
    fn test_tag_fetch_stats_concurrent_updates() {
        use std::thread;

        let stats = Arc::new(TagFetchStats::new());
        let mut handles = vec![];

        for _ in 0..4 {
            let stats_clone = Arc::clone(&stats);
            handles.push(thread::spawn(move || {
                for _ in 0..100 {
                    stats_clone.record_success();
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(stats.success.load(Ordering::Relaxed), 400);
    }

    #[test]
    fn test_object_without_key_handling() {
        let object = Object::builder().build();
        let stream_obj = StreamObject {
            object,
            version_id: None,
            is_latest: None,
            is_delete_marker: false,
            tags: None,
        };

        assert!(stream_obj.key().is_none());
    }

    // Mock-based tests for fetch_tags_for_objects
    use aws_config::BehaviorVersion;
    use aws_smithy_runtime::client::http::test_util::{ReplayEvent, StaticReplayClient};
    use aws_smithy_types::body::SdkBody;
    use http::{HeaderValue, StatusCode};
    use std::time::Duration;
    use tokio::time::sleep;

    fn make_test_client(events: Vec<ReplayEvent>) -> Client {
        let replay_client = StaticReplayClient::new(events);
        Client::from_conf(
            aws_sdk_s3::Config::builder()
                .behavior_version(BehaviorVersion::latest())
                .credentials_provider(aws_sdk_s3::config::Credentials::new(
                    "test", "test", None, None, "test",
                ))
                .region(aws_sdk_s3::config::Region::new("us-east-1"))
                .http_client(replay_client)
                .build(),
        )
    }

    fn make_tag_response(key: &str, tags: &[(&str, &str)]) -> ReplayEvent {
        let uri = format!(
            "https://test-bucket.s3.us-east-1.amazonaws.com/{}?tagging",
            key
        );
        let req = http::Request::builder()
            .method("GET")
            .uri(&uri)
            .body(SdkBody::empty())
            .unwrap();

        let tag_xml: String = tags
            .iter()
            .map(|(k, v)| format!("<Tag><Key>{}</Key><Value>{}</Value></Tag>", k, v))
            .collect::<Vec<_>>()
            .join("");

        let resp_body = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
            <Tagging xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                <TagSet>{}</TagSet>
            </Tagging>"#,
            tag_xml
        );

        let resp = http::Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", HeaderValue::from_static("application/xml"))
            .body(SdkBody::from(resp_body))
            .unwrap();

        ReplayEvent::new(req, resp)
    }

    fn make_error_response(key: &str, status: u16) -> ReplayEvent {
        let uri = format!(
            "https://test-bucket.s3.us-east-1.amazonaws.com/{}?tagging",
            key
        );
        let req = http::Request::builder()
            .method("GET")
            .uri(&uri)
            .body(SdkBody::empty())
            .unwrap();

        let resp = http::Response::builder()
            .status(StatusCode::from_u16(status).unwrap())
            .header("Content-Type", HeaderValue::from_static("application/xml"))
            .body(SdkBody::from(""))
            .unwrap();

        ReplayEvent::new(req, resp)
    }

    #[tokio::test]
    async fn test_fetch_tags_for_objects_success() {
        let events = vec![
            make_tag_response("file1.txt", &[("env", "prod"), ("team", "data")]),
            make_tag_response("file2.txt", &[("env", "dev")]),
        ];

        let client = make_test_client(events);
        let stats = Arc::new(TagFetchStats::new());
        let config = TagFetchConfig::default().with_concurrency(1);

        let objects = vec![
            StreamObject::from_object(Object::builder().key("file1.txt").build()),
            StreamObject::from_object(Object::builder().key("file2.txt").build()),
        ];

        let results = fetch_tags_for_objects(
            client,
            "test-bucket".to_string(),
            objects,
            config,
            Arc::clone(&stats),
        )
        .await;

        assert_eq!(results.len(), 2);
        assert!(results[0].tags.is_some());
        assert!(results[1].tags.is_some());
        assert_eq!(stats.success.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn test_map_with_concurrency_in_order_preserves_input_order() {
        let results = map_with_concurrency_in_order(vec![1_usize, 2, 3], 3, |value| async move {
            let delay_ms = [30_u64, 5, 10][value - 1];
            sleep(Duration::from_millis(delay_ms)).await;
            value
        })
        .await;

        assert_eq!(results, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn test_fetch_tags_for_objects_with_delete_marker() {
        let client = make_test_client(vec![]);
        let stats = Arc::new(TagFetchStats::new());
        let config = TagFetchConfig::default();

        let object = Object::builder().key("deleted.txt").build();
        let objects = vec![StreamObject {
            object,
            version_id: Some("v1".to_string()),
            is_latest: Some(true),
            is_delete_marker: true,
            tags: None,
        }];

        let results = fetch_tags_for_objects(
            client,
            "test-bucket".to_string(),
            objects,
            config,
            Arc::clone(&stats),
        )
        .await;

        assert_eq!(results.len(), 1);
        assert!(results[0].tags.is_some());
        assert!(results[0].tags.as_ref().unwrap().is_empty());
        assert_eq!(stats.success.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn test_fetch_tags_for_objects_already_has_tags() {
        let client = make_test_client(vec![]);
        let stats = Arc::new(TagFetchStats::new());
        let config = TagFetchConfig::default();

        let object = Object::builder().key("cached.txt").build();
        let existing_tags = vec![Tag::builder().key("cached").value("true").build().unwrap()];
        let objects = vec![StreamObject {
            object,
            version_id: None,
            is_latest: None,
            is_delete_marker: false,
            tags: Some(existing_tags),
        }];

        let results = fetch_tags_for_objects(
            client,
            "test-bucket".to_string(),
            objects,
            config,
            Arc::clone(&stats),
        )
        .await;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].tags.as_ref().unwrap().len(), 1);
        assert_eq!(stats.success.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn test_fetch_tags_for_objects_access_denied() {
        let events = vec![make_error_response("forbidden.txt", 403)];

        let client = make_test_client(events);
        let stats = Arc::new(TagFetchStats::new());
        let config = TagFetchConfig::default().with_concurrency(1);

        let objects = vec![StreamObject::from_object(
            Object::builder().key("forbidden.txt").build(),
        )];

        let results = fetch_tags_for_objects(
            client,
            "test-bucket".to_string(),
            objects,
            config,
            Arc::clone(&stats),
        )
        .await;

        assert_eq!(results.len(), 1);
        assert!(results[0].tags.as_ref().unwrap().is_empty());
        assert_eq!(stats.access_denied.load(Ordering::Relaxed), 1);
        assert_eq!(stats.excluded.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_fetch_tags_for_objects_not_found() {
        let events = vec![make_error_response("missing.txt", 404)];

        let client = make_test_client(events);
        let stats = Arc::new(TagFetchStats::new());
        let config = TagFetchConfig::default().with_concurrency(1);

        let objects = vec![StreamObject::from_object(
            Object::builder().key("missing.txt").build(),
        )];

        let results = fetch_tags_for_objects(
            client,
            "test-bucket".to_string(),
            objects,
            config,
            Arc::clone(&stats),
        )
        .await;

        assert_eq!(results.len(), 1);
        assert!(results[0].tags.as_ref().unwrap().is_empty());
        assert_eq!(stats.failed.load(Ordering::Relaxed), 1);
        assert_eq!(stats.excluded.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_fetch_tags_for_objects_without_key() {
        let client = make_test_client(vec![]);
        let stats = Arc::new(TagFetchStats::new());
        let config = TagFetchConfig::default();

        let objects = vec![StreamObject::from_object(Object::builder().build())];

        let results = fetch_tags_for_objects(
            client,
            "test-bucket".to_string(),
            objects,
            config,
            Arc::clone(&stats),
        )
        .await;

        assert_eq!(results.len(), 1);
        assert!(results[0].tags.as_ref().unwrap().is_empty());
        assert_eq!(stats.success.load(Ordering::Relaxed), 0);
    }

    fn make_versioned_tag_response(
        key: &str,
        version_id: &str,
        tags: &[(&str, &str)],
    ) -> ReplayEvent {
        let uri = format!(
            "https://test-bucket.s3.us-east-1.amazonaws.com/{}?tagging&versionId={}",
            key, version_id
        );
        let req = http::Request::builder()
            .method("GET")
            .uri(&uri)
            .body(SdkBody::empty())
            .unwrap();

        let tag_xml: String = tags
            .iter()
            .map(|(k, v)| format!("<Tag><Key>{}</Key><Value>{}</Value></Tag>", k, v))
            .collect::<Vec<_>>()
            .join("");

        let resp_body = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
            <Tagging xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                <TagSet>{}</TagSet>
            </Tagging>"#,
            tag_xml
        );

        let resp = http::Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", HeaderValue::from_static("application/xml"))
            .body(SdkBody::from(resp_body))
            .unwrap();

        ReplayEvent::new(req, resp)
    }

    #[tokio::test]
    async fn test_fetch_tags_for_objects_with_version_id() {
        let events = vec![make_versioned_tag_response(
            "versioned.txt",
            "v123",
            &[("version", "v123")],
        )];

        let client = make_test_client(events);
        let stats = Arc::new(TagFetchStats::new());
        let config = TagFetchConfig::default().with_concurrency(1);

        let object = Object::builder().key("versioned.txt").build();
        let objects = vec![StreamObject {
            object,
            version_id: Some("v123".to_string()),
            is_latest: Some(true),
            is_delete_marker: false,
            tags: None,
        }];

        let results = fetch_tags_for_objects(
            client,
            "test-bucket".to_string(),
            objects,
            config,
            Arc::clone(&stats),
        )
        .await;

        assert_eq!(results.len(), 1);
        let tags = results[0].tags.as_ref().unwrap();
        assert_eq!(tags.len(), 1);
        assert_eq!(tags[0].key(), "version");
        assert_eq!(tags[0].value(), "v123");
        assert_eq!(stats.success.load(Ordering::Relaxed), 1);
    }
}
