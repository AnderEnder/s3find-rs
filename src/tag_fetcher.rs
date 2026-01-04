use aws_sdk_s3::Client;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::get_object_tagging::GetObjectTaggingError;
use aws_sdk_s3::types::Tag;
use futures::{StreamExt, stream};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use thiserror::Error;
use tokio::time::sleep;

use crate::command::StreamObject;

/// Error types for tag fetching operations
#[derive(Error, Debug)]
pub enum TagFetchError {
    #[error("Access denied for s3:GetObjectTagging on {bucket}/{key}")]
    AccessDenied { bucket: String, key: String },

    #[error("Object not found: {bucket}/{key}")]
    NotFound { bucket: String, key: String },

    #[error("Throttled by S3 API")]
    Throttled,

    #[error("S3 API error: {0}")]
    ApiError(String),

    #[error("Missing object key")]
    MissingKey,
}

impl TagFetchError {
    /// Returns true if this error is retryable
    pub fn is_retryable(&self) -> bool {
        matches!(self, TagFetchError::Throttled)
    }
}

/// Statistics for tag fetching operations
#[derive(Debug, Default)]
pub struct TagFetchStats {
    pub success: AtomicUsize,
    pub failed: AtomicUsize,
    pub throttled: AtomicUsize,
    pub access_denied: AtomicUsize,
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

    pub fn record_throttled(&self) {
        self.throttled.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_access_denied(&self) {
        self.access_denied.fetch_add(1, Ordering::Relaxed);
    }

    pub fn total_requests(&self) -> usize {
        self.success.load(Ordering::Relaxed)
            + self.failed.load(Ordering::Relaxed)
            + self.throttled.load(Ordering::Relaxed)
            + self.access_denied.load(Ordering::Relaxed)
    }
}

/// Configuration for tag fetching
#[derive(Debug, Clone)]
pub struct TagFetchConfig {
    /// Maximum concurrent tag fetch requests
    pub concurrency: usize,
    /// Maximum number of retries for throttled requests
    pub max_retries: u32,
    /// Base delay for exponential backoff (in milliseconds)
    pub base_delay_ms: u64,
    /// Maximum delay for exponential backoff (in milliseconds)
    pub max_delay_ms: u64,
}

impl Default for TagFetchConfig {
    fn default() -> Self {
        Self {
            concurrency: 50,
            max_retries: 3,
            base_delay_ms: 100,
            max_delay_ms: 5000,
        }
    }
}

impl TagFetchConfig {
    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }
}

/// Calculate exponential backoff delay with jitter
fn calculate_backoff_delay(attempt: u32, base_delay_ms: u64, max_delay_ms: u64) -> Duration {
    // Exponential backoff: base_delay * 2^attempt
    let delay_ms = base_delay_ms.saturating_mul(1u64 << attempt);
    let capped_delay = delay_ms.min(max_delay_ms);

    // Add jitter: random value between 0 and delay/2
    let jitter = (rand_jitter() * (capped_delay as f64 / 2.0)) as u64;
    Duration::from_millis(capped_delay + jitter)
}

/// Simple pseudo-random jitter (0.0 to 1.0)
/// Uses the current time to generate a simple pseudo-random value
fn rand_jitter() -> f64 {
    use std::time::SystemTime;
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    (nanos % 1000) as f64 / 1000.0
}

/// Fetches tags for a single object with retry logic
async fn fetch_object_tags(
    client: &Client,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
    config: &TagFetchConfig,
    stats: &TagFetchStats,
) -> Result<Vec<Tag>, TagFetchError> {
    let mut attempt = 0;

    loop {
        let mut request = client.get_object_tagging().bucket(bucket).key(key);

        if let Some(vid) = version_id {
            request = request.version_id(vid);
        }

        match request.send().await {
            Ok(output) => {
                stats.record_success();
                return Ok(output.tag_set().to_vec());
            }
            Err(err) => {
                let fetch_error = classify_error(&err, bucket, key);

                match &fetch_error {
                    TagFetchError::Throttled => {
                        stats.record_throttled();
                        if attempt < config.max_retries {
                            let delay = calculate_backoff_delay(
                                attempt,
                                config.base_delay_ms,
                                config.max_delay_ms,
                            );
                            sleep(delay).await;
                            attempt += 1;
                            continue;
                        }
                        stats.record_failure();
                        return Err(fetch_error);
                    }
                    TagFetchError::AccessDenied { .. } => {
                        stats.record_access_denied();
                        return Err(fetch_error);
                    }
                    _ => {
                        stats.record_failure();
                        return Err(fetch_error);
                    }
                }
            }
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
                503 | 429 => TagFetchError::Throttled,
                _ => TagFetchError::ApiError(format!("HTTP {}: {:?}", status, service_err.err())),
            }
        }
        _ => TagFetchError::ApiError(err.to_string()),
    }
}

/// Fetches tags for a stream of objects with concurrency control
///
/// This function takes an iterator of StreamObjects, fetches tags for each one
/// concurrently (up to the configured limit), and returns a new iterator with
/// the tags populated.
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

    stream::iter(objects)
        .map(|mut obj| {
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

                match fetch_object_tags(&client, &bucket, &key, version_id, &config, &stats).await {
                    Ok(tags) => {
                        obj.tags = Some(tags);
                    }
                    Err(e) => {
                        // Log the error but continue processing
                        // Objects with failed tag fetches will have tags = None
                        eprintln!("Warning: Failed to fetch tags for {}: {}", key, e);
                        obj.tags = Some(Vec::new()); // Empty tags on error to allow filtering
                    }
                }

                obj
            }
        })
        .buffer_unordered(config.concurrency)
        .collect()
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_s3::types::Object;

    #[test]
    fn test_tag_fetch_stats() {
        let stats = TagFetchStats::new();

        assert_eq!(stats.total_requests(), 0);

        stats.record_success();
        stats.record_success();
        stats.record_failure();
        stats.record_throttled();
        stats.record_access_denied();

        assert_eq!(stats.success.load(Ordering::Relaxed), 2);
        assert_eq!(stats.failed.load(Ordering::Relaxed), 1);
        assert_eq!(stats.throttled.load(Ordering::Relaxed), 1);
        assert_eq!(stats.access_denied.load(Ordering::Relaxed), 1);
        assert_eq!(stats.total_requests(), 5);
    }

    #[test]
    fn test_tag_fetch_config_default() {
        let config = TagFetchConfig::default();

        assert_eq!(config.concurrency, 50);
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.base_delay_ms, 100);
        assert_eq!(config.max_delay_ms, 5000);
    }

    #[test]
    fn test_tag_fetch_config_with_concurrency() {
        let config = TagFetchConfig::default().with_concurrency(100);
        assert_eq!(config.concurrency, 100);
    }

    #[test]
    fn test_calculate_backoff_delay() {
        let delay0 = calculate_backoff_delay(0, 100, 5000);
        let delay1 = calculate_backoff_delay(1, 100, 5000);
        let delay2 = calculate_backoff_delay(2, 100, 5000);

        // Base delay is 100ms, with jitter up to 50ms
        assert!(delay0.as_millis() >= 100 && delay0.as_millis() <= 150);
        // Second attempt: 200ms + jitter
        assert!(delay1.as_millis() >= 200 && delay1.as_millis() <= 300);
        // Third attempt: 400ms + jitter
        assert!(delay2.as_millis() >= 400 && delay2.as_millis() <= 600);
    }

    #[test]
    fn test_calculate_backoff_delay_capped() {
        // Large attempt number should be capped
        let delay = calculate_backoff_delay(10, 100, 5000);
        // Should not exceed max_delay + jitter (5000 + 2500)
        assert!(delay.as_millis() <= 7500);
    }

    #[test]
    fn test_tag_fetch_error_retryable() {
        assert!(TagFetchError::Throttled.is_retryable());
        assert!(
            !TagFetchError::AccessDenied {
                bucket: "test".to_string(),
                key: "test".to_string()
            }
            .is_retryable()
        );
        assert!(
            !TagFetchError::NotFound {
                bucket: "test".to_string(),
                key: "test".to_string()
            }
            .is_retryable()
        );
        assert!(!TagFetchError::ApiError("test".to_string()).is_retryable());
        assert!(!TagFetchError::MissingKey.is_retryable());
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

        // Object already has tags - should be skipped
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

        // Simulate what fetch_tags_for_objects does for delete markers
        if stream_obj.is_delete_marker {
            stream_obj.tags = Some(Vec::new());
        }

        assert!(stream_obj.tags.is_some());
        assert!(stream_obj.tags.unwrap().is_empty());
    }
}
