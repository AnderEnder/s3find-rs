use aws_sdk_s3::Client;
use futures::Future;
use futures::stream::Stream;
use futures::stream::StreamExt;
use std::sync::Arc;

use crate::command::{FindStat, StreamObject};
use crate::error::S3FindResult;
use crate::filter::TagFilterList;
use crate::tag_fetcher::{TagFetchConfig, TagFetchStats, fetch_tags_for_objects};

const CHUNK: usize = 1000;

/// Batch size for tag fetching operations.
/// A value of 100 balances memory usage with API efficiency and works well with
/// the default `tag_concurrency` (50), allowing up to 50 concurrent tag fetches
/// per batch without overwhelming S3 API limits.
const TAG_FETCH_BATCH_SIZE: usize = 100;

pub async fn list_filter_execute<P, F, Fut, Fut2>(
    iterator: impl Stream<Item = S3FindResult<Vec<StreamObject>>>,
    limit: Option<usize>,
    stats: Option<FindStat>,
    p: P,
    f: &mut F,
) -> S3FindResult<Option<FindStat>>
where
    P: FnMut(&StreamObject) -> Fut,
    Fut: Future<Output = bool>,
    F: FnMut(Option<FindStat>, Vec<StreamObject>) -> Fut2,
    Fut2: Future<Output = S3FindResult<Option<FindStat>>>,
{
    match limit {
        Some(limit) => list_filter_limit_execute(iterator, limit, stats, p, f).await,
        None => list_filter_unlimited_execute(iterator, stats, p, f).await,
    }
}

/// Context for tag-aware filtering operations.
/// Bundles S3 client and tag filtering configuration.
pub struct TagFilterContext {
    pub client: Client,
    pub bucket: String,
    pub filters: TagFilterList,
    pub config: TagFetchConfig,
    pub stats: Arc<TagFetchStats>,
}

/// Two-phase filtering with tag support.
///
/// Phase 1: Apply cheap filters (name, size, mtime, etc.)
/// Phase 2: Fetch tags for passing objects and apply tag filters
///
/// This function is called when tag filters are configured. It fetches tags
/// only for objects that pass the cheap filters, minimizing API calls.
pub async fn list_filter_execute_with_tags<P, F, Fut, Fut2>(
    iterator: impl Stream<Item = S3FindResult<Vec<StreamObject>>>,
    limit: Option<usize>,
    stats: Option<FindStat>,
    tag_ctx: TagFilterContext,
    mut cheap_filter: P,
    f: &mut F,
) -> S3FindResult<Option<FindStat>>
where
    P: FnMut(&StreamObject) -> Fut,
    Fut: Future<Output = bool>,
    F: FnMut(Option<FindStat>, Vec<StreamObject>) -> Fut2,
    Fut2: Future<Output = S3FindResult<Option<FindStat>>>,
{
    let mut remaining_limit = limit;
    let mut current_stats = stats;

    // Process chunks from the stream
    let mut stream = Box::pin(iterator);

    // Collect objects in batches for tag fetching
    let mut batch: Vec<StreamObject> = Vec::with_capacity(TAG_FETCH_BATCH_SIZE);

    while let Some(batch_result) = stream.next().await {
        let objects = batch_result?;

        for obj in objects {
            // Check limit first
            if remaining_limit == Some(0) {
                break;
            }

            // Phase 1: Apply cheap filter
            if !cheap_filter(&obj).await {
                continue;
            }

            batch.push(obj);

            // When batch is full, process it
            if batch.len() >= TAG_FETCH_BATCH_SIZE {
                let (processed, new_stats) = process_tag_batch(
                    std::mem::take(&mut batch),
                    &tag_ctx,
                    remaining_limit,
                    current_stats,
                    f,
                )
                .await;

                current_stats = new_stats?;

                if let Some(ref mut remaining) = remaining_limit {
                    *remaining = remaining.saturating_sub(processed);
                    if *remaining == 0 {
                        break;
                    }
                }

                batch = Vec::with_capacity(TAG_FETCH_BATCH_SIZE);
            }
        }

        if remaining_limit == Some(0) {
            break;
        }
    }

    // Process remaining objects in the last batch
    if !batch.is_empty() {
        let (_processed, new_stats) =
            process_tag_batch(batch, &tag_ctx, remaining_limit, current_stats, f).await;
        current_stats = new_stats?;
    }

    Ok(current_stats)
}

/// Process a batch of objects: fetch tags and apply tag filters
async fn process_tag_batch<F, Fut2>(
    objects: Vec<StreamObject>,
    tag_ctx: &TagFilterContext,
    limit: Option<usize>,
    stats: Option<FindStat>,
    f: &mut F,
) -> (usize, S3FindResult<Option<FindStat>>)
where
    F: FnMut(Option<FindStat>, Vec<StreamObject>) -> Fut2,
    Fut2: Future<Output = S3FindResult<Option<FindStat>>>,
{
    // Fetch tags for all objects in the batch
    let objects_with_tags = fetch_tags_for_objects(
        tag_ctx.client.clone(),
        tag_ctx.bucket.clone(),
        objects,
        tag_ctx.config.clone(),
        Arc::clone(&tag_ctx.stats),
    )
    .await;

    // Apply tag filters and collect matching objects.
    // Objects where tags couldn't be fetched (None) or failed to fetch (empty)
    // won't match any tag filter, ensuring we only include verified matches.
    let matching: Vec<StreamObject> = objects_with_tags
        .into_iter()
        .filter(|obj| {
            // Apply tag filter - treat None (tags not fetched) as no match
            tag_ctx.filters.matches(obj).unwrap_or(false)
        })
        .collect();

    // Apply limit if needed
    let matching = if let Some(limit) = limit {
        matching.into_iter().take(limit).collect()
    } else {
        matching
    };

    let count = matching.len();

    // Execute command on matching objects
    let new_stats = if !matching.is_empty() {
        f(stats, matching).await
    } else {
        Ok(stats)
    };

    (count, new_stats)
}

#[inline]
async fn list_filter_limit_execute<P, F, Fut, Fut2>(
    iterator: impl Stream<Item = S3FindResult<Vec<StreamObject>>>,
    limit: usize,
    stats: Option<FindStat>,
    p: P,
    f: &mut F,
) -> S3FindResult<Option<FindStat>>
where
    P: FnMut(&StreamObject) -> Fut,
    Fut: Future<Output = bool>,
    F: FnMut(Option<FindStat>, Vec<StreamObject>) -> Fut2,
    Fut2: Future<Output = S3FindResult<Option<FindStat>>>,
{
    list_filter_execute_internal(iterator, Some(limit), stats, p, f).await
}

#[inline]
async fn list_filter_unlimited_execute<P, F, Fut, Fut2>(
    iterator: impl Stream<Item = S3FindResult<Vec<StreamObject>>>,
    stats: Option<FindStat>,
    p: P,
    f: &mut F,
) -> S3FindResult<Option<FindStat>>
where
    P: FnMut(&StreamObject) -> Fut,
    Fut: Future<Output = bool>,
    F: FnMut(Option<FindStat>, Vec<StreamObject>) -> Fut2,
    Fut2: Future<Output = S3FindResult<Option<FindStat>>>,
{
    list_filter_execute_internal(iterator, None, stats, p, f).await
}

async fn list_filter_execute_internal<P, F, Fut, Fut2>(
    iterator: impl Stream<Item = S3FindResult<Vec<StreamObject>>>,
    mut remaining_limit: Option<usize>,
    mut stats: Option<FindStat>,
    mut predicate: P,
    f: &mut F,
) -> S3FindResult<Option<FindStat>>
where
    P: FnMut(&StreamObject) -> Fut,
    Fut: Future<Output = bool>,
    F: FnMut(Option<FindStat>, Vec<StreamObject>) -> Fut2,
    Fut2: Future<Output = S3FindResult<Option<FindStat>>>,
{
    let mut stream = Box::pin(iterator);
    let mut chunk = Vec::with_capacity(CHUNK);

    while let Some(batch_result) = stream.next().await {
        let batch = batch_result?;

        for obj in batch {
            if remaining_limit == Some(0) {
                return flush_chunk(stats, chunk, f).await;
            }

            if !predicate(&obj).await {
                continue;
            }

            chunk.push(obj);

            if let Some(ref mut remaining) = remaining_limit {
                *remaining = remaining.saturating_sub(1);
                if *remaining == 0 {
                    return flush_chunk(stats, chunk, f).await;
                }
            }

            if chunk.len() >= CHUNK {
                stats = f(stats, std::mem::take(&mut chunk)).await?;
            }
        }
    }

    flush_chunk(stats, chunk, f).await
}

async fn flush_chunk<F, Fut2>(
    stats: Option<FindStat>,
    chunk: Vec<StreamObject>,
    f: &mut F,
) -> S3FindResult<Option<FindStat>>
where
    F: FnMut(Option<FindStat>, Vec<StreamObject>) -> Fut2,
    Fut2: Future<Output = S3FindResult<Option<FindStat>>>,
{
    if chunk.is_empty() {
        return Ok(stats);
    }

    f(stats, chunk).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{S3FindError, S3FindResult};
    use anyhow::anyhow;
    use aws_sdk_s3::types::Object;
    use futures::stream;
    use std::cell::Cell;
    use std::future::{Ready, ready};

    /// Helper to create the standard stats accumulator closure used in tests.
    fn make_stats_accumulator()
    -> impl FnMut(Option<FindStat>, Vec<StreamObject>) -> Ready<S3FindResult<Option<FindStat>>>
    {
        |acc, list| {
            let objects: Vec<_> = list.iter().map(|so| so.object.clone()).collect();
            ready(Ok(acc
                .map(|stat| stat + &objects)
                .or_else(|| Some(FindStat::default() + &objects))))
        }
    }

    fn always_true(_: &StreamObject) -> Ready<bool> {
        ready(true)
    }

    thread_local! {
        static COUNTING_TRUE_CALLS: Cell<usize> = const { Cell::new(0) };
    }

    fn reset_counting_true_calls() {
        COUNTING_TRUE_CALLS.with(|count| count.set(0));
    }

    fn counting_true(_: &StreamObject) -> Ready<bool> {
        COUNTING_TRUE_CALLS.with(|count| count.set(count.get() + 1));
        ready(true)
    }

    fn counting_true_calls() -> usize {
        COUNTING_TRUE_CALLS.with(Cell::get)
    }

    fn ok_iterator(
        batches: Vec<Vec<StreamObject>>,
    ) -> impl futures::Stream<Item = S3FindResult<Vec<StreamObject>>> {
        stream::iter(batches.into_iter().map(Ok))
    }

    fn make_stream_objects(keys: &[&str]) -> Vec<StreamObject> {
        keys.iter()
            .map(|k| StreamObject::from_object(Object::builder().key(*k).build()))
            .collect()
    }

    // Mock-based test utilities
    use aws_config::BehaviorVersion;
    use aws_sdk_s3::types::Tag;
    use aws_smithy_runtime::client::http::test_util::{ReplayEvent, StaticReplayClient};
    use aws_smithy_types::body::SdkBody;
    use http::{HeaderValue, StatusCode};

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

    fn make_stream_objects_with_tags(keys_and_tags: &[(&str, Vec<Tag>)]) -> Vec<StreamObject> {
        keys_and_tags
            .iter()
            .map(|(k, tags)| {
                let mut obj = StreamObject::from_object(Object::builder().key(*k).build());
                obj.tags = Some(tags.clone());
                obj
            })
            .collect()
    }

    #[tokio::test]
    async fn test_list_filter_execute_with_limit() {
        reset_counting_true_calls();
        let stream_objects = make_stream_objects(&["object1", "object2", "object3"]);

        let iterator = ok_iterator(vec![stream_objects]);
        let limit = Some(2);
        let stats = None;

        let result = list_filter_execute(
            iterator,
            limit,
            stats,
            counting_true,
            &mut make_stats_accumulator(),
        )
        .await;

        assert_eq!(result.unwrap().unwrap().total_files, 2);
        assert_eq!(counting_true_calls(), 2);
    }

    #[tokio::test]
    async fn test_list_filter_execute_with_zero_limit_skips_predicate() {
        reset_counting_true_calls();
        let stream_objects = make_stream_objects(&["object1"]);
        let iterator = ok_iterator(vec![stream_objects]);

        let result = list_filter_execute(
            iterator,
            Some(0),
            None,
            counting_true,
            &mut make_stats_accumulator(),
        )
        .await;

        assert!(result.unwrap().is_none());
        assert_eq!(counting_true_calls(), 0);
    }

    #[tokio::test]
    async fn test_list_filter_execute_without_limit() {
        let stream_objects = make_stream_objects(&["object1", "object2", "object3"]);

        let iterator = ok_iterator(vec![stream_objects]);
        let limit = None;
        let stats = None;

        let result = list_filter_execute(
            iterator,
            limit,
            stats,
            always_true,
            &mut make_stats_accumulator(),
        )
        .await;

        assert_eq!(result.unwrap().unwrap().total_files, 3);
    }

    #[tokio::test]
    async fn test_list_filter_execute_skips_non_matching_objects() {
        let stream_objects = make_stream_objects(&["object1", "object2"]);
        let iterator = ok_iterator(vec![stream_objects]);

        let result = list_filter_execute(
            iterator,
            None,
            None,
            |_: &StreamObject| ready(false),
            &mut make_stats_accumulator(),
        )
        .await;

        assert!(result.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_list_filter_limit_execute() {
        let stream_objects = make_stream_objects(&["object1", "object2", "object3"]);

        let iterator = ok_iterator(vec![stream_objects]);
        let limit = 2;
        let stats = None;

        let result = list_filter_limit_execute(
            iterator,
            limit,
            stats,
            always_true,
            &mut make_stats_accumulator(),
        )
        .await;

        assert_eq!(result.unwrap().unwrap().total_files, 2);
    }

    #[tokio::test]
    async fn test_list_filter_execute_flushes_full_chunk() {
        let stream_objects: Vec<_> = (0..=CHUNK)
            .map(|i| StreamObject::from_object(Object::builder().key(format!("object{i}")).build()))
            .collect();
        let iterator = ok_iterator(vec![stream_objects]);
        let chunk_sizes = Arc::new(std::sync::Mutex::new(Vec::new()));
        let chunk_sizes_for_accumulator = Arc::clone(&chunk_sizes);

        let result =
            list_filter_execute(iterator, None, None, always_true, &mut move |acc, list| {
                chunk_sizes_for_accumulator.lock().unwrap().push(list.len());
                let objects: Vec<_> = list.iter().map(|so| so.object.clone()).collect();
                ready(Ok(acc
                    .map(|stat| stat + &objects)
                    .or_else(|| Some(FindStat::default() + &objects))))
            })
            .await;

        assert_eq!(result.unwrap().unwrap().total_files, CHUNK + 1);
        assert_eq!(chunk_sizes.lock().unwrap().as_slice(), &[CHUNK, 1]);
    }

    #[tokio::test]
    async fn test_list_filter_unlimited_execute() {
        let stream_objects = make_stream_objects(&["object1", "object2", "object3"]);

        let iterator = ok_iterator(vec![stream_objects]);
        let stats = None;

        let result = list_filter_unlimited_execute(
            iterator,
            stats,
            always_true,
            &mut make_stats_accumulator(),
        )
        .await;

        assert_eq!(result.unwrap().unwrap().total_files, 3);
    }

    #[tokio::test]
    async fn test_list_filter_execute_propagates_iterator_errors() {
        let iterator = stream::iter(vec![Err(S3FindError::list_objects(anyhow!(
            "listing failed"
        )))]);

        let result = list_filter_execute(
            iterator,
            None,
            None,
            always_true,
            &mut make_stats_accumulator(),
        )
        .await;

        assert!(matches!(result, Err(S3FindError::ListObjects { .. })));
    }

    #[tokio::test]
    async fn test_list_filter_execute_propagates_command_errors() {
        let stream_objects = make_stream_objects(&["object1"]);
        let iterator = ok_iterator(vec![stream_objects]);

        let result = list_filter_execute(iterator, None, None, always_true, &mut |_, _| {
            ready(Err(S3FindError::command_execution(anyhow!(
                "command failed"
            ))))
        })
        .await;

        assert!(matches!(result, Err(S3FindError::CommandExecution { .. })));
    }

    // Tests for list_filter_execute_with_tags and process_tag_batch

    #[tokio::test]
    async fn test_list_filter_execute_with_tags_basic() {
        // Test basic tag filtering with pre-cached tags (no API calls needed)
        use crate::arg::TagFilter;
        use crate::filter::TagFilterList;

        let tag = Tag::builder().key("env").value("prod").build().unwrap();
        let stream_objects = make_stream_objects_with_tags(&[
            ("file1.txt", vec![tag.clone()]),
            ("file2.txt", vec![tag.clone()]),
        ]);

        let client = make_test_client(vec![]);
        let tag_stats = Arc::new(TagFetchStats::new());

        // Create a tag filter that matches env=prod
        let tag_filters = TagFilterList::with_filters(
            vec![TagFilter {
                key: "env".to_string(),
                value: "prod".to_string(),
            }],
            vec![],
        );

        let tag_ctx = TagFilterContext {
            client,
            bucket: "test-bucket".to_string(),
            filters: tag_filters,
            config: TagFetchConfig::default().with_concurrency(1),
            stats: Arc::clone(&tag_stats),
        };

        let iterator = ok_iterator(vec![stream_objects]);

        let result = list_filter_execute_with_tags(
            iterator,
            None, // No limit
            None, // No initial stats
            tag_ctx,
            always_true, // Cheap filter passes all
            &mut make_stats_accumulator(),
        )
        .await;

        let stats = result.unwrap();
        assert!(stats.is_some());
        assert_eq!(stats.unwrap().total_files, 2);
    }

    #[tokio::test]
    async fn test_list_filter_execute_with_tags_with_limit() {
        use crate::arg::TagFilter;
        use crate::filter::TagFilterList;

        let tag = Tag::builder().key("env").value("prod").build().unwrap();
        let stream_objects = make_stream_objects_with_tags(&[
            ("file1.txt", vec![tag.clone()]),
            ("file2.txt", vec![tag.clone()]),
            ("file3.txt", vec![tag.clone()]),
        ]);

        let client = make_test_client(vec![]);
        let stats = Arc::new(TagFetchStats::new());

        let tag_filters = TagFilterList::with_filters(
            vec![TagFilter {
                key: "env".to_string(),
                value: "prod".to_string(),
            }],
            vec![],
        );

        let tag_ctx = TagFilterContext {
            client,
            bucket: "test-bucket".to_string(),
            filters: tag_filters,
            config: TagFetchConfig::default().with_concurrency(1),
            stats: Arc::clone(&stats),
        };

        let iterator = ok_iterator(vec![stream_objects]);

        let result = list_filter_execute_with_tags(
            iterator,
            Some(2), // Limit to 2
            None,
            tag_ctx,
            always_true,
            &mut make_stats_accumulator(),
        )
        .await;

        let stats = result.unwrap();
        assert!(stats.is_some());
        assert_eq!(stats.unwrap().total_files, 2);
    }

    #[tokio::test]
    async fn test_list_filter_execute_with_tags_limit_keeps_traversal_order() {
        use crate::arg::TagFilter;
        use crate::filter::TagFilterList;

        let tag = Tag::builder().key("env").value("prod").build().unwrap();
        let stream_objects = make_stream_objects_with_tags(&[
            ("file1.txt", vec![tag.clone()]),
            ("file2.txt", vec![tag.clone()]),
            ("file3.txt", vec![tag]),
        ]);

        let client = make_test_client(vec![]);
        let stats = Arc::new(TagFetchStats::new());
        let seen_keys = Arc::new(std::sync::Mutex::new(Vec::new()));

        let tag_filters = TagFilterList::with_filters(
            vec![TagFilter {
                key: "env".to_string(),
                value: "prod".to_string(),
            }],
            vec![],
        );

        let tag_ctx = TagFilterContext {
            client,
            bucket: "test-bucket".to_string(),
            filters: tag_filters,
            config: TagFetchConfig::default().with_concurrency(3),
            stats: Arc::clone(&stats),
        };

        let iterator = ok_iterator(vec![stream_objects]);
        let seen_keys_for_accumulator = Arc::clone(&seen_keys);

        let result = list_filter_execute_with_tags(
            iterator,
            Some(2),
            None,
            tag_ctx,
            always_true,
            &mut move |acc, list| {
                seen_keys_for_accumulator
                    .lock()
                    .unwrap()
                    .extend(list.iter().filter_map(|obj| obj.key().map(str::to_string)));

                let objects: Vec<_> = list.iter().map(|so| so.object.clone()).collect();
                ready(Ok(acc
                    .map(|stat| stat + &objects)
                    .or_else(|| Some(FindStat::default() + &objects))))
            },
        )
        .await;

        let stats = result.unwrap();
        assert!(stats.is_some());
        assert_eq!(stats.unwrap().total_files, 2);
        assert_eq!(
            seen_keys.lock().unwrap().as_slice(),
            ["file1.txt", "file2.txt"]
        );
    }

    #[tokio::test]
    async fn test_list_filter_execute_with_tags_cheap_filter_rejects() {
        use crate::arg::TagFilter;
        use crate::filter::TagFilterList;

        let tag = Tag::builder().key("env").value("prod").build().unwrap();
        let stream_objects = make_stream_objects_with_tags(&[
            ("file1.txt", vec![tag.clone()]),
            ("file2.txt", vec![tag.clone()]),
        ]);

        let client = make_test_client(vec![]);
        let stats = Arc::new(TagFetchStats::new());

        let tag_filters = TagFilterList::with_filters(
            vec![TagFilter {
                key: "env".to_string(),
                value: "prod".to_string(),
            }],
            vec![],
        );

        let tag_ctx = TagFilterContext {
            client,
            bucket: "test-bucket".to_string(),
            filters: tag_filters,
            config: TagFetchConfig::default().with_concurrency(1),
            stats: Arc::clone(&stats),
        };

        let iterator = ok_iterator(vec![stream_objects]);

        let result = list_filter_execute_with_tags(
            iterator,
            None,
            None,
            tag_ctx,
            |_: &StreamObject| ready(false), // Cheap filter rejects all
            &mut make_stats_accumulator(),
        )
        .await;

        // No objects should match since cheap filter rejects all
        assert!(result.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_list_filter_execute_with_tags_tag_filter_rejects() {
        use crate::arg::TagFilter;
        use crate::filter::TagFilterList;

        let tag = Tag::builder().key("env").value("dev").build().unwrap();
        let stream_objects = make_stream_objects_with_tags(&[
            ("file1.txt", vec![tag.clone()]),
            ("file2.txt", vec![tag.clone()]),
        ]);

        let client = make_test_client(vec![]);
        let stats = Arc::new(TagFetchStats::new());

        // Filter expects env=prod but objects have env=dev
        let tag_filters = TagFilterList::with_filters(
            vec![TagFilter {
                key: "env".to_string(),
                value: "prod".to_string(),
            }],
            vec![],
        );

        let tag_ctx = TagFilterContext {
            client,
            bucket: "test-bucket".to_string(),
            filters: tag_filters,
            config: TagFetchConfig::default().with_concurrency(1),
            stats: Arc::clone(&stats),
        };

        let iterator = ok_iterator(vec![stream_objects]);

        let result = list_filter_execute_with_tags(
            iterator,
            None,
            None,
            tag_ctx,
            always_true, // Cheap filter passes all
            &mut make_stats_accumulator(),
        )
        .await;

        // No objects should match since tag filter rejects all
        assert!(result.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_list_filter_execute_with_tags_with_api_calls() {
        use crate::arg::TagFilter;
        use crate::filter::TagFilterList;

        // Create objects WITHOUT pre-cached tags - will need API calls
        let stream_objects = make_stream_objects(&["file1.txt", "file2.txt"]);

        // Set up mock responses for GetObjectTagging
        let events = vec![
            make_tag_response("file1.txt", &[("env", "prod")]),
            make_tag_response("file2.txt", &[("env", "prod")]),
        ];

        let client = make_test_client(events);
        let tag_stats = Arc::new(TagFetchStats::new());

        let tag_filters = TagFilterList::with_filters(
            vec![TagFilter {
                key: "env".to_string(),
                value: "prod".to_string(),
            }],
            vec![],
        );

        let tag_ctx = TagFilterContext {
            client,
            bucket: "test-bucket".to_string(),
            filters: tag_filters,
            config: TagFetchConfig::default().with_concurrency(1),
            stats: Arc::clone(&tag_stats),
        };

        let iterator = ok_iterator(vec![stream_objects]);

        let result = list_filter_execute_with_tags(
            iterator,
            None,
            None,
            tag_ctx,
            always_true,
            &mut make_stats_accumulator(),
        )
        .await;

        let stats = result.unwrap();
        assert!(stats.is_some());
        assert_eq!(stats.unwrap().total_files, 2);
        assert_eq!(
            tag_stats.success.load(std::sync::atomic::Ordering::Relaxed),
            2
        );
    }

    #[tokio::test]
    async fn test_list_filter_execute_with_tags_empty_stream() {
        use crate::arg::TagFilter;
        use crate::filter::TagFilterList;

        let client = make_test_client(vec![]);
        let stats = Arc::new(TagFetchStats::new());

        let tag_filters = TagFilterList::with_filters(
            vec![TagFilter {
                key: "env".to_string(),
                value: "prod".to_string(),
            }],
            vec![],
        );

        let tag_ctx = TagFilterContext {
            client,
            bucket: "test-bucket".to_string(),
            filters: tag_filters,
            config: TagFetchConfig::default().with_concurrency(1),
            stats: Arc::clone(&stats),
        };

        // Empty stream
        let iterator = ok_iterator(Vec::new());

        let result = list_filter_execute_with_tags(
            iterator,
            None,
            None,
            tag_ctx,
            always_true,
            &mut make_stats_accumulator(),
        )
        .await;

        // No results from empty stream
        assert!(result.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_list_filter_execute_with_tags_limit_zero() {
        use crate::arg::TagFilter;
        use crate::filter::TagFilterList;

        let tag = Tag::builder().key("env").value("prod").build().unwrap();
        let stream_objects = make_stream_objects_with_tags(&[("file1.txt", vec![tag.clone()])]);

        let client = make_test_client(vec![]);
        let stats = Arc::new(TagFetchStats::new());

        let tag_filters = TagFilterList::with_filters(
            vec![TagFilter {
                key: "env".to_string(),
                value: "prod".to_string(),
            }],
            vec![],
        );

        let tag_ctx = TagFilterContext {
            client,
            bucket: "test-bucket".to_string(),
            filters: tag_filters,
            config: TagFetchConfig::default().with_concurrency(1),
            stats: Arc::clone(&stats),
        };

        let iterator = ok_iterator(vec![stream_objects]);

        let result = list_filter_execute_with_tags(
            iterator,
            Some(0), // Limit of 0 - should return nothing
            None,
            tag_ctx,
            always_true,
            &mut make_stats_accumulator(),
        )
        .await;

        // With limit 0, nothing should be processed
        assert!(result.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_list_filter_execute_with_tags_key_exists_filter() {
        use crate::arg::TagExistsFilter;
        use crate::filter::TagFilterList;

        let tag = Tag::builder().key("owner").value("team-a").build().unwrap();
        let stream_objects = make_stream_objects_with_tags(&[
            ("file1.txt", vec![tag.clone()]),
            ("file2.txt", vec![]), // No tags
        ]);

        let client = make_test_client(vec![]);
        let stats = Arc::new(TagFetchStats::new());

        // Filter for objects that have an "owner" tag (any value)
        let tag_filters = TagFilterList::with_filters(
            vec![],
            vec![TagExistsFilter {
                key: "owner".to_string(),
            }],
        );

        let tag_ctx = TagFilterContext {
            client,
            bucket: "test-bucket".to_string(),
            filters: tag_filters,
            config: TagFetchConfig::default().with_concurrency(1),
            stats: Arc::clone(&stats),
        };

        let iterator = ok_iterator(vec![stream_objects]);

        let result = list_filter_execute_with_tags(
            iterator,
            None,
            None,
            tag_ctx,
            always_true,
            &mut make_stats_accumulator(),
        )
        .await;

        let stats = result.unwrap();
        assert!(stats.is_some());
        // Only file1.txt has owner tag
        assert_eq!(stats.unwrap().total_files, 1);
    }

    #[tokio::test]
    async fn test_list_filter_execute_with_tags_mixed_results() {
        use crate::arg::TagFilter;
        use crate::filter::TagFilterList;

        let prod_tag = Tag::builder().key("env").value("prod").build().unwrap();
        let dev_tag = Tag::builder().key("env").value("dev").build().unwrap();
        let stream_objects = make_stream_objects_with_tags(&[
            ("file1.txt", vec![prod_tag.clone()]),
            ("file2.txt", vec![dev_tag]),
            ("file3.txt", vec![prod_tag.clone()]),
        ]);

        let client = make_test_client(vec![]);
        let stats = Arc::new(TagFetchStats::new());

        // Filter for env=prod
        let tag_filters = TagFilterList::with_filters(
            vec![TagFilter {
                key: "env".to_string(),
                value: "prod".to_string(),
            }],
            vec![],
        );

        let tag_ctx = TagFilterContext {
            client,
            bucket: "test-bucket".to_string(),
            filters: tag_filters,
            config: TagFetchConfig::default().with_concurrency(1),
            stats: Arc::clone(&stats),
        };

        let iterator = ok_iterator(vec![stream_objects]);

        let result = list_filter_execute_with_tags(
            iterator,
            None,
            None,
            tag_ctx,
            always_true,
            &mut make_stats_accumulator(),
        )
        .await;

        let stats = result.unwrap();
        assert!(stats.is_some());
        // file1.txt and file3.txt have env=prod
        assert_eq!(stats.unwrap().total_files, 2);
    }

    #[tokio::test]
    async fn test_list_filter_execute_with_tags_with_initial_stats() {
        use crate::arg::TagFilter;
        use crate::filter::TagFilterList;

        let tag = Tag::builder().key("env").value("prod").build().unwrap();
        let stream_objects = make_stream_objects_with_tags(&[("file1.txt", vec![tag.clone()])]);

        let client = make_test_client(vec![]);
        let stats = Arc::new(TagFetchStats::new());

        let tag_filters = TagFilterList::with_filters(
            vec![TagFilter {
                key: "env".to_string(),
                value: "prod".to_string(),
            }],
            vec![],
        );

        let tag_ctx = TagFilterContext {
            client,
            bucket: "test-bucket".to_string(),
            filters: tag_filters,
            config: TagFetchConfig::default().with_concurrency(1),
            stats: Arc::clone(&stats),
        };

        // Start with some initial stats
        let initial_stats = FindStat {
            total_files: 5,
            total_space: 1000,
            ..FindStat::default()
        };

        let iterator = ok_iterator(vec![stream_objects]);

        let result = list_filter_execute_with_tags(
            iterator,
            None,
            Some(initial_stats),
            tag_ctx,
            always_true,
            &mut make_stats_accumulator(),
        )
        .await;

        let stats = result.unwrap();
        assert!(stats.is_some());
        // Initial 5 files + 1 new file = 6
        assert_eq!(stats.unwrap().total_files, 6);
    }

    #[tokio::test]
    async fn test_list_filter_execute_with_tags_large_batch() {
        // Test with more than TAG_FETCH_BATCH_SIZE (100) objects to trigger batch processing
        use crate::arg::TagFilter;
        use crate::filter::TagFilterList;

        let tag = Tag::builder().key("env").value("prod").build().unwrap();

        // Create 150 objects with pre-cached tags
        let stream_objects: Vec<StreamObject> = (0..150)
            .map(|i| {
                let mut obj = StreamObject::from_object(
                    Object::builder().key(format!("file{}.txt", i)).build(),
                );
                obj.tags = Some(vec![tag.clone()]);
                obj
            })
            .collect();

        let client = make_test_client(vec![]);
        let stats = Arc::new(TagFetchStats::new());

        let tag_filters = TagFilterList::with_filters(
            vec![TagFilter {
                key: "env".to_string(),
                value: "prod".to_string(),
            }],
            vec![],
        );

        let tag_ctx = TagFilterContext {
            client,
            bucket: "test-bucket".to_string(),
            filters: tag_filters,
            config: TagFetchConfig::default().with_concurrency(1),
            stats: Arc::clone(&stats),
        };

        let iterator = ok_iterator(vec![stream_objects]);

        let result = list_filter_execute_with_tags(
            iterator,
            None,
            None,
            tag_ctx,
            always_true,
            &mut make_stats_accumulator(),
        )
        .await;

        let stats = result.unwrap();
        assert!(stats.is_some());
        // All 150 objects should match
        assert_eq!(stats.unwrap().total_files, 150);
    }

    #[tokio::test]
    async fn test_list_filter_execute_with_tags_large_batch_with_limit() {
        // Test batch processing with limit that triggers early termination
        use crate::arg::TagFilter;
        use crate::filter::TagFilterList;

        let tag = Tag::builder().key("env").value("prod").build().unwrap();

        // Create 150 objects with pre-cached tags
        let stream_objects: Vec<StreamObject> = (0..150)
            .map(|i| {
                let mut obj = StreamObject::from_object(
                    Object::builder().key(format!("file{}.txt", i)).build(),
                );
                obj.tags = Some(vec![tag.clone()]);
                obj
            })
            .collect();

        let client = make_test_client(vec![]);
        let stats = Arc::new(TagFetchStats::new());

        let tag_filters = TagFilterList::with_filters(
            vec![TagFilter {
                key: "env".to_string(),
                value: "prod".to_string(),
            }],
            vec![],
        );

        let tag_ctx = TagFilterContext {
            client,
            bucket: "test-bucket".to_string(),
            filters: tag_filters,
            config: TagFetchConfig::default().with_concurrency(1),
            stats: Arc::clone(&stats),
        };

        let iterator = ok_iterator(vec![stream_objects]);

        let result = list_filter_execute_with_tags(
            iterator,
            Some(50), // Limit to 50 - should terminate during first batch
            None,
            tag_ctx,
            always_true,
            &mut make_stats_accumulator(),
        )
        .await;

        let stats = result.unwrap();
        assert!(stats.is_some());
        // Only 50 objects should be returned due to limit
        assert_eq!(stats.unwrap().total_files, 50);
    }

    #[tokio::test]
    async fn test_list_filter_execute_with_tags_batch_limit_exhaustion() {
        // Test that triggers the limit exhaustion break path after batch processing
        use crate::arg::TagFilter;
        use crate::filter::TagFilterList;

        let tag = Tag::builder().key("env").value("prod").build().unwrap();

        // Create exactly 100 objects to fill one batch, then 50 more
        let stream_objects: Vec<StreamObject> = (0..150)
            .map(|i| {
                let mut obj = StreamObject::from_object(
                    Object::builder().key(format!("file{}.txt", i)).build(),
                );
                obj.tags = Some(vec![tag.clone()]);
                obj
            })
            .collect();

        let client = make_test_client(vec![]);
        let stats = Arc::new(TagFetchStats::new());

        let tag_filters = TagFilterList::with_filters(
            vec![TagFilter {
                key: "env".to_string(),
                value: "prod".to_string(),
            }],
            vec![],
        );

        let tag_ctx = TagFilterContext {
            client,
            bucket: "test-bucket".to_string(),
            filters: tag_filters,
            config: TagFetchConfig::default().with_concurrency(1),
            stats: Arc::clone(&stats),
        };

        let iterator = ok_iterator(vec![stream_objects]);

        // Limit to exactly 100 - should exhaust limit after first batch
        let result = list_filter_execute_with_tags(
            iterator,
            Some(100),
            None,
            tag_ctx,
            always_true,
            &mut make_stats_accumulator(),
        )
        .await;

        let stats = result.unwrap();
        assert!(stats.is_some());
        // Exactly 100 objects should match (first batch)
        assert_eq!(stats.unwrap().total_files, 100);
    }
}
