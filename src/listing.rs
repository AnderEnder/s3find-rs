//! Turns an S3 path into a stream of objects, one listing strategy per mode:
//! a flat ListObjectsV2 walk, a depth-limited walk using the `/` delimiter, or
//! a ListObjectVersions walk that includes every version and delete marker.

use std::cmp::Ordering;
use std::collections::VecDeque;
use std::iter;
use std::pin::pin;

use async_stream::{stream, try_stream};
use futures::FutureExt;
use futures::channel::mpsc;
use futures::stream::{self, BoxStream, FuturesUnordered, Stream, StreamExt, TryStreamExt};

use crate::adapters::aws::{ListObjectsRequest, ListVersionsRequest, ObjectsPage, S3Client};
use crate::arg::{FindOpt, S3Path};
use crate::error::{S3FindError, S3FindResult};
use crate::object::StreamObject;

/// Delimiter used for hierarchical S3 listing.
const S3_PATH_DELIMITER: &str = "/";

/// Subdirectory walks started ahead of the one being read in a `--maxdepth`
/// listing, so wide trees aren't listed one request at a time.
const SUBDIRECTORY_PREFETCH: usize = 8;

/// Pages of a flat listing read one after another before the rest is split
/// into ranges, so small listings and `--limit` runs that stop early make no
/// extra requests.
const SEQUENTIAL_PAGES: usize = 2;

/// Most ranges the rest of a big flat listing is split into.
const MAX_RANGES: usize = 32;

/// Ranges of a flat listing walked at once.
const RANGE_CONCURRENCY: usize = 8;

/// Pages each range walk may read ahead of the one being read.
const RANGE_READ_AHEAD: usize = 2;

/// Directory levels searched for points to split a flat listing at.
const MAX_SPLIT_DEPTH: usize = 3;

/// Directories requested per level when looking for split points: one call
/// that finds as many as S3 returns, whatever the page size.
const SPLIT_MAX_KEYS: i32 = 1000;

/// What to list and how.
#[derive(Debug, Clone, PartialEq)]
pub struct ListingOptions {
    pub path: S3Path,
    /// Keys requested per list call (1..=1000).
    pub page_size: u16,
    /// Most '/' allowed in a key after the prefix; `None` is unlimited.
    pub maxdepth: Option<usize>,
    /// List every version and delete marker. Takes precedence over `maxdepth`.
    pub all_versions: bool,
}

impl ListingOptions {
    pub fn from_opts(opts: &FindOpt) -> Self {
        Self {
            path: opts.path.clone(),
            page_size: opts.page_size,
            maxdepth: opts.maxdepth,
            all_versions: opts.all_versions,
        }
    }

    fn prefix(&self) -> &str {
        self.path.prefix.as_deref().unwrap_or_default()
    }

    fn max_keys(&self) -> i32 {
        i32::from(self.page_size)
    }
}

/// Lists objects in traversal order. The first error ends the stream.
pub fn list_objects<'a, C: S3Client>(
    client: &'a C,
    options: &'a ListingOptions,
) -> BoxStream<'a, S3FindResult<StreamObject>> {
    match (options.all_versions, options.maxdepth) {
        (true, _) => list_versions(client, options),
        (false, Some(maxdepth)) => {
            // Depth counts from the prefix without a trailing '/', so the
            // objects directly inside `logs/` are at depth 1.
            let depth = usize::from(options.prefix().ends_with(S3_PATH_DELIMITER));
            if depth > maxdepth {
                return stream::empty().boxed();
            }
            let bucket = &options.path.bucket;
            let max_keys = options.max_keys();
            let pages = directory_pages(client, bucket, options.prefix().to_owned(), max_keys);
            list_to_depth(client, bucket, pages, depth, maxdepth, max_keys)
        }
        (false, None) => list_flat(client, options),
    }
}

/// Every object under the prefix, in key order.
///
/// A listing longer than [`SEQUENTIAL_PAGES`] pages is split at the
/// directories after the last key read, and up to [`RANGE_CONCURRENCY`] of
/// those ranges are listed at once; their objects are still yielded in key
/// order.
fn list_flat<'a, C: S3Client>(
    client: &'a C,
    options: &'a ListingOptions,
) -> BoxStream<'a, S3FindResult<StreamObject>> {
    let bucket = &options.path.bucket;
    let prefix = options.prefix();
    let max_keys = options.max_keys();

    let pages = try_stream! {
        let mut pages = pin!(object_pages(client, bucket, prefix.to_owned(), None, None, max_keys));
        let mut split_after = None;
        let mut read = 0;
        while let Some(page) = pages.next().await {
            let page = page?;
            read += 1;
            let last_key = page.objects.last().and_then(|object| object.key.clone());
            let more = page.next_continuation_token.is_some();
            yield page;
            if more && read >= SEQUENTIAL_PAGES && last_key.is_some() {
                split_after = last_key;
                break;
            }
        }

        if let Some(after) = split_after {
            let boundaries = split_points(client, bucket, prefix, &after).await?;
            if boundaries.is_empty() {
                while let Some(page) = pages.next().await {
                    yield page?;
                }
            } else {
                let walks = ranges(after, boundaries).map(|(after, until)| {
                    range_pages(client, bucket, prefix, after, until, max_keys).boxed()
                });
                let mut ranged = pin!(concat_concurrently(walks, RANGE_CONCURRENCY, RANGE_READ_AHEAD));
                while let Some(page) = ranged.next().await {
                    yield page?;
                }
            }
        }
    };

    pages
        .map_ok(|page: ObjectsPage| {
            stream::iter(
                page.objects
                    .into_iter()
                    .map(|object| Ok(StreamObject::from_object(object))),
            )
        })
        .try_flatten()
        .boxed()
}

/// Pages of a ListObjectsV2 walk, following continuation tokens; only the
/// last page has no `next_continuation_token`.
fn object_pages<'a, C: S3Client>(
    client: &'a C,
    bucket: &'a str,
    prefix: String,
    delimiter: Option<&'a str>,
    start_after: Option<String>,
    max_keys: i32,
) -> impl Stream<Item = S3FindResult<ObjectsPage>> + Send + 'a {
    try_stream! {
        let mut token: Option<String> = None;
        loop {
            let request = ListObjectsRequest {
                bucket,
                prefix: &prefix,
                delimiter,
                continuation_token: token.as_deref(),
                start_after: start_after.as_deref(),
                max_keys,
            };
            let mut page = client
                .list_objects(request)
                .await
                .map_err(S3FindError::list_objects)?;

            // Like the SDK paginator, stop on a missing, empty or repeated token.
            page.next_continuation_token = page
                .next_continuation_token
                .take()
                .filter(|next| !next.is_empty() && Some(next) != token.as_ref());
            let next = page.next_continuation_token.clone();

            yield page;

            match next {
                Some(next) => token = Some(next),
                None => break,
            }
        }
    }
}

/// Keys to split the rest of a listing after `after` at: the directories
/// after it, from a delimiter listing of the prefix. While a level has fewer
/// than two, the directory holding `after` is searched too. Spread to at most
/// `MAX_RANGES - 1` keys, in order.
async fn split_points<C: S3Client>(
    client: &C,
    bucket: &str,
    prefix: &str,
    after: &str,
) -> S3FindResult<Vec<String>> {
    // Directories found at each level; deeper ones sort before shallower ones.
    let mut levels = Vec::new();
    let mut level = prefix.to_owned();
    for _ in 0..MAX_SPLIT_DEPTH {
        let request = ListObjectsRequest {
            bucket,
            prefix: &level,
            delimiter: Some(S3_PATH_DELIMITER),
            continuation_token: None,
            start_after: Some(after),
            max_keys: SPLIT_MAX_KEYS,
        };
        let page = client
            .list_objects(request)
            .await
            .map_err(S3FindError::list_objects)?;

        let (holding, later): (Vec<_>, Vec<_>) = page
            .common_prefixes
            .into_iter()
            .partition(|dir| after.starts_with(dir.as_str()));
        let enough = later.len() >= 2;
        levels.push(later);
        match holding.into_iter().next() {
            Some(dir) if !enough => level = dir,
            _ => break,
        }
    }

    let boundaries: Vec<_> = levels.into_iter().rev().flatten().collect();
    Ok(spread(boundaries, MAX_RANGES - 1))
}

/// At most `count` of `items`, evenly spaced and in order.
fn spread<T: Clone>(items: Vec<T>, count: usize) -> Vec<T> {
    if items.len() <= count {
        return items;
    }
    (1..=count)
        .map(|i| items[i * items.len() / (count + 1)].clone())
        .collect()
}

/// Ranges `(after, until]` covering every key after `after`, split at
/// `boundaries`; the last range has no end.
fn ranges(
    after: String,
    boundaries: Vec<String>,
) -> impl Iterator<Item = (String, Option<String>)> + Send {
    let starts: Vec<_> = iter::once(after)
        .chain(boundaries.iter().cloned())
        .collect();
    let ends = boundaries.into_iter().map(Some).chain(iter::once(None));
    starts.into_iter().zip(ends)
}

/// Pages of the keys in `(after, until]`, or of every key after `after`
/// without `until`.
fn range_pages<'a, C: S3Client>(
    client: &'a C,
    bucket: &'a str,
    prefix: &'a str,
    after: String,
    until: Option<String>,
    max_keys: i32,
) -> impl Stream<Item = S3FindResult<ObjectsPage>> + Send + 'a {
    let pages = object_pages(
        client,
        bucket,
        prefix.to_owned(),
        None,
        Some(after),
        max_keys,
    );
    try_stream! {
        let mut pages = pin!(pages);
        while let Some(page) = pages.next().await {
            let mut page = page?;
            let past_end = until.as_deref().and_then(|until| {
                page.objects
                    .iter()
                    .position(|object| object.key().is_some_and(|key| key > until))
            });
            if let Some(end) = past_end {
                page.objects.truncate(end);
                yield page;
                break;
            }
            yield page;
        }
    }
}

/// The items of `walks`, one walk after another, with up to `concurrency`
/// walks running at once, each a few items ahead of the one being read.
/// Unlike `buffered`, every running walk keeps going while an earlier one is
/// read.
fn concat_concurrently<'a, T: Send + 'a>(
    walks: impl Iterator<Item = BoxStream<'a, T>> + Send + 'a,
    concurrency: usize,
    read_ahead: usize,
) -> impl Stream<Item = T> + Send + 'a {
    stream! {
        let mut walks = walks;
        let mut running = FuturesUnordered::new();
        let mut readers = VecDeque::new();
        loop {
            while readers.len() < concurrency
                && let Some(walk) = walks.next()
            {
                let (sender, receiver) = mpsc::channel(read_ahead);
                running.push(walk.map(Ok).forward(sender).map(drop));
                readers.push_back(receiver);
            }
            let Some(reader) = readers.front_mut() else {
                break;
            };

            // Wait for the walk being read while driving every running walk.
            let item = loop {
                tokio::select! {
                    item = reader.next() => break item,
                    Some(()) = running.next() => {}
                }
            };
            match item {
                Some(item) => yield item,
                None => {
                    readers.pop_front();
                }
            }
        }
    }
}

/// Depth-first walk: each page's objects, then each of its subdirectories.
/// Objects found at `depth` have that many '/' in their key after the
/// starting prefix; subdirectories are only entered while `depth < maxdepth`.
fn list_to_depth<'a, C: S3Client>(
    client: &'a C,
    bucket: &'a str,
    pages: BoxStream<'a, S3FindResult<ObjectsPage>>,
    depth: usize,
    maxdepth: usize,
    max_keys: i32,
) -> BoxStream<'a, S3FindResult<StreamObject>> {
    Box::pin(try_stream! {
        let mut pages = pages;
        while let Some(page) = pages.next().await {
            let page = page?;

            for object in page.objects {
                yield StreamObject::from_object(object);
            }

            if depth < maxdepth {
                let mut children = pin!(subdirectories(
                    client,
                    bucket,
                    page.common_prefixes,
                    depth + 1,
                    maxdepth,
                    max_keys,
                ));
                while let Some(child) = children.next().await {
                    yield child?;
                }
            }
        }
    })
}

/// The walks of `prefixes`, one after another. The first pages of up to
/// [`SUBDIRECTORY_PREFETCH`] of them are fetched ahead while earlier ones are
/// read; a walk only descends further once it is the one being read, so at
/// most that many listings per level are in flight.
fn subdirectories<'a, C: S3Client>(
    client: &'a C,
    bucket: &'a str,
    prefixes: Vec<String>,
    depth: usize,
    maxdepth: usize,
    max_keys: i32,
) -> impl Stream<Item = S3FindResult<StreamObject>> + Send + 'a {
    stream::iter(prefixes)
        .map(move |prefix| {
            let mut pages = directory_pages(client, bucket, prefix, max_keys);
            async move {
                let first = pages.next().await;
                let pages = stream::iter(first).chain(pages).boxed();
                list_to_depth(client, bucket, pages, depth, maxdepth, max_keys)
            }
        })
        .buffered(SUBDIRECTORY_PREFETCH)
        .flatten()
}

/// Pages of one directory level: objects plus subdirectories as common prefixes.
fn directory_pages<'a, C: S3Client>(
    client: &'a C,
    bucket: &'a str,
    prefix: String,
    max_keys: i32,
) -> BoxStream<'a, S3FindResult<ObjectsPage>> {
    object_pages(
        client,
        bucket,
        prefix,
        Some(S3_PATH_DELIMITER),
        None,
        max_keys,
    )
    .boxed()
}

fn list_versions<'a, C: S3Client>(
    client: &'a C,
    options: &'a ListingOptions,
) -> BoxStream<'a, S3FindResult<StreamObject>> {
    Box::pin(try_stream! {
        let mut marker = None;
        loop {
            let request = ListVersionsRequest {
                bucket: &options.path.bucket,
                prefix: options.prefix(),
                marker: marker.as_ref(),
                max_keys: options.max_keys(),
            };
            let page = client
                .list_object_versions(request)
                .await
                .map_err(S3FindError::list_object_versions)?;

            let mut objects: Vec<StreamObject> = page
                .versions
                .into_iter()
                .map(StreamObject::from_version)
                .chain(page.delete_markers.into_iter().map(StreamObject::from_delete_marker))
                .collect();
            objects.sort_by(newest_version_first);

            for object in objects {
                yield object;
            }

            match page.next_marker {
                Some(next) => marker = Some(next),
                None => break,
            }
        }
    })
}

/// Orders by key, then newest first, so each key's versions read top-down.
fn newest_version_first(a: &StreamObject, b: &StreamObject) -> Ordering {
    a.key()
        .cmp(&b.key())
        .then_with(|| b.object.last_modified().cmp(&a.object.last_modified()))
}

#[cfg(test)]
mod tests;
