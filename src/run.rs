use aws_sdk_s3::Client;
use futures::Future;
use futures::stream::Stream;
use futures::stream::StreamExt;
use std::sync::Arc;

use crate::command::{FindStat, StreamObject};
use crate::filter::TagFilterList;
use crate::tag_fetcher::{TagFetchConfig, TagFetchStats, fetch_tags_for_objects};

const CHUNK: usize = 1000;
const TAG_FETCH_BATCH_SIZE: usize = 100;

pub async fn list_filter_execute<P, F, Fut, Fut2>(
    iterator: impl Stream<Item = Vec<StreamObject>>,
    limit: Option<usize>,
    stats: Option<FindStat>,
    p: P,
    f: &mut F,
) -> Option<FindStat>
where
    P: FnMut(&StreamObject) -> Fut,
    Fut: Future<Output = bool>,
    F: FnMut(Option<FindStat>, Vec<StreamObject>) -> Fut2,
    Fut2: Future<Output = Option<FindStat>>,
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
    iterator: impl Stream<Item = Vec<StreamObject>>,
    limit: Option<usize>,
    stats: Option<FindStat>,
    tag_ctx: TagFilterContext,
    mut cheap_filter: P,
    f: &mut F,
) -> Option<FindStat>
where
    P: FnMut(&StreamObject) -> Fut,
    Fut: Future<Output = bool>,
    F: FnMut(Option<FindStat>, Vec<StreamObject>) -> Fut2,
    Fut2: Future<Output = Option<FindStat>>,
{
    let mut remaining_limit = limit;
    let mut current_stats = stats;

    // Process chunks from the stream
    let mut stream = Box::pin(
        iterator
            .map(|x| futures::stream::iter(x.into_iter()))
            .flatten(),
    );

    // Collect objects in batches for tag fetching
    let mut batch: Vec<StreamObject> = Vec::with_capacity(TAG_FETCH_BATCH_SIZE);

    while let Some(obj) = stream.next().await {
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

            current_stats = new_stats;

            if let Some(ref mut remaining) = remaining_limit {
                *remaining = remaining.saturating_sub(processed);
                if *remaining == 0 {
                    break;
                }
            }

            batch = Vec::with_capacity(TAG_FETCH_BATCH_SIZE);
        }
    }

    // Process remaining objects in the last batch
    if !batch.is_empty() {
        let (_processed, new_stats) =
            process_tag_batch(batch, &tag_ctx, remaining_limit, current_stats, f).await;
        current_stats = new_stats;
    }

    current_stats
}

/// Process a batch of objects: fetch tags and apply tag filters
async fn process_tag_batch<F, Fut2>(
    objects: Vec<StreamObject>,
    tag_ctx: &TagFilterContext,
    limit: Option<usize>,
    stats: Option<FindStat>,
    f: &mut F,
) -> (usize, Option<FindStat>)
where
    F: FnMut(Option<FindStat>, Vec<StreamObject>) -> Fut2,
    Fut2: Future<Output = Option<FindStat>>,
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

    // Apply tag filters and collect matching objects
    let matching: Vec<StreamObject> = objects_with_tags
        .into_iter()
        .filter(|obj| {
            // Apply tag filter - treat None (tags not fetched) as false
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
        stats
    };

    (count, new_stats)
}

#[inline]
async fn list_filter_limit_execute<P, F, Fut, Fut2>(
    iterator: impl Stream<Item = Vec<StreamObject>>,
    limit: usize,
    stats: Option<FindStat>,
    p: P,
    f: &mut F,
) -> Option<FindStat>
where
    P: FnMut(&StreamObject) -> Fut,
    Fut: Future<Output = bool>,
    F: FnMut(Option<FindStat>, Vec<StreamObject>) -> Fut2,
    Fut2: Future<Output = Option<FindStat>>,
{
    iterator
        .map(|x| futures::stream::iter(x.into_iter()))
        .flatten()
        .filter(p)
        .take(limit)
        .chunks(CHUNK)
        .fold(stats, f)
        .await
}

#[inline]
async fn list_filter_unlimited_execute<P, F, Fut, Fut2>(
    iterator: impl Stream<Item = Vec<StreamObject>>,
    stats: Option<FindStat>,
    p: P,
    f: &mut F,
) -> Option<FindStat>
where
    P: FnMut(&StreamObject) -> Fut,
    Fut: Future<Output = bool>,
    F: FnMut(Option<FindStat>, Vec<StreamObject>) -> Fut2,
    Fut2: Future<Output = Option<FindStat>>,
{
    iterator
        .map(|x| futures::stream::iter(x.into_iter()))
        .flatten()
        .filter(p)
        .chunks(CHUNK)
        .fold(stats, f)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_s3::types::Object;
    use futures::stream;
    use std::future::ready;

    fn make_stream_objects(keys: &[&str]) -> Vec<StreamObject> {
        keys.iter()
            .map(|k| StreamObject::from_object(Object::builder().key(*k).build()))
            .collect()
    }

    #[tokio::test]
    async fn test_list_filter_execute_with_limit() {
        let stream_objects = make_stream_objects(&["object1", "object2", "object3"]);

        let iterator = stream::iter(vec![stream_objects]);
        let limit = Some(2);
        let stats = None;

        let result = list_filter_execute(
            iterator,
            limit,
            stats,
            |_: &StreamObject| ready(true),
            &mut |acc, list| {
                let objects: Vec<_> = list.iter().map(|so| so.object.clone()).collect();
                ready(
                    acc.map(|stat| stat + &objects)
                        .or_else(|| Some(FindStat::default() + &objects)),
                )
            },
        )
        .await;

        assert_eq!(result.unwrap().total_files, 2);
    }

    #[tokio::test]
    async fn test_list_filter_execute_without_limit() {
        let stream_objects = make_stream_objects(&["object1", "object2", "object3"]);

        let iterator = stream::iter(vec![stream_objects]);
        let limit = None;
        let stats = None;

        let result = list_filter_execute(
            iterator,
            limit,
            stats,
            |_: &StreamObject| ready(true),
            &mut |acc, list| {
                let objects: Vec<_> = list.iter().map(|so| so.object.clone()).collect();
                ready(
                    acc.map(|stat| stat + &objects)
                        .or_else(|| Some(FindStat::default() + &objects)),
                )
            },
        )
        .await;

        assert_eq!(result.unwrap().total_files, 3);
    }

    #[tokio::test]
    async fn test_list_filter_limit_execute() {
        let stream_objects = make_stream_objects(&["object1", "object2", "object3"]);

        let iterator = stream::iter(vec![stream_objects]);
        let limit = 2;
        let stats = None;

        let result = list_filter_limit_execute(
            iterator,
            limit,
            stats,
            |_: &StreamObject| ready(true),
            &mut |acc, list| {
                let objects: Vec<_> = list.iter().map(|so| so.object.clone()).collect();
                ready(
                    acc.map(|stat| stat + &objects)
                        .or_else(|| Some(FindStat::default() + &objects)),
                )
            },
        )
        .await;

        assert_eq!(result.unwrap().total_files, 2);
    }

    #[tokio::test]
    async fn test_list_filter_unlimited_execute() {
        let stream_objects = make_stream_objects(&["object1", "object2", "object3"]);

        let iterator = stream::iter(vec![stream_objects]);
        let stats = None;

        let result = list_filter_unlimited_execute(
            iterator,
            stats,
            |_: &StreamObject| ready(true),
            &mut |acc, list| {
                let objects: Vec<_> = list.iter().map(|so| so.object.clone()).collect();
                ready(
                    acc.map(|stat| stat + &objects)
                        .or_else(|| Some(FindStat::default() + &objects)),
                )
            },
        )
        .await;

        assert_eq!(result.unwrap().total_files, 3);
    }
}
