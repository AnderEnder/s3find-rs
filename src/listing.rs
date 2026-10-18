//! Turns an S3 path into a stream of objects, one listing strategy per mode:
//! a flat ListObjectsV2 walk, a depth-limited walk using the `/` delimiter, or
//! a ListObjectVersions walk that includes every version and delete marker.

use std::cmp::Ordering;

use async_stream::try_stream;
use futures::stream::{BoxStream, Stream, StreamExt, TryStreamExt};

use crate::adapters::aws::{ListObjectsRequest, ListVersionsRequest, ObjectsPage, S3Client};
use crate::arg::{FindOpt, S3Path};
use crate::error::{S3FindError, S3FindResult};
use crate::object::StreamObject;

/// Delimiter used for hierarchical S3 listing.
const S3_PATH_DELIMITER: &str = "/";

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
pub fn list_objects<'a>(
    client: &'a dyn S3Client,
    options: &'a ListingOptions,
) -> BoxStream<'a, S3FindResult<StreamObject>> {
    match (options.all_versions, options.maxdepth) {
        (true, _) => list_versions(client, options),
        (false, Some(maxdepth)) => list_to_depth(
            client,
            &options.path.bucket,
            options.prefix().to_owned(),
            0,
            maxdepth,
            options.max_keys(),
        ),
        (false, None) => list_flat(client, options),
    }
}

/// Every object under the prefix, in key order.
fn list_flat<'a>(
    client: &'a dyn S3Client,
    options: &'a ListingOptions,
) -> BoxStream<'a, S3FindResult<StreamObject>> {
    object_pages(
        client,
        &options.path.bucket,
        options.prefix().to_owned(),
        None,
        options.max_keys(),
    )
    .map_ok(|page| {
        futures::stream::iter(
            page.objects
                .into_iter()
                .map(|object| Ok(StreamObject::from_object(object))),
        )
    })
    .try_flatten()
    .boxed()
}

/// Pages of a ListObjectsV2 walk, following continuation tokens.
fn object_pages<'a>(
    client: &'a dyn S3Client,
    bucket: &'a str,
    prefix: String,
    delimiter: Option<&'a str>,
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
                max_keys,
            };
            let mut page = client
                .list_objects(request)
                .await
                .map_err(S3FindError::list_objects)?;

            // Like the SDK paginator, stop on a missing, empty or repeated token.
            let next = page
                .next_continuation_token
                .take()
                .filter(|next| !next.is_empty() && Some(next) != token.as_ref());

            yield page;

            match next {
                Some(next) => token = Some(next),
                None => break,
            }
        }
    }
}

/// Depth-first walk: each page's objects, then each of its subdirectories.
/// Objects found at `depth` have that many '/' in their key after the
/// starting prefix; subdirectories are only entered while `depth < maxdepth`.
fn list_to_depth<'a>(
    client: &'a dyn S3Client,
    bucket: &'a str,
    prefix: String,
    depth: usize,
    maxdepth: usize,
    max_keys: i32,
) -> BoxStream<'a, S3FindResult<StreamObject>> {
    Box::pin(try_stream! {
        let mut pages = std::pin::pin!(object_pages(
            client,
            bucket,
            prefix,
            Some(S3_PATH_DELIMITER),
            max_keys,
        ));

        while let Some(page) = pages.next().await {
            let page = page?;

            for object in page.objects {
                yield StreamObject::from_object(object);
            }

            if depth < maxdepth {
                for subdirectory in page.common_prefixes {
                    let mut children =
                        list_to_depth(client, bucket, subdirectory, depth + 1, maxdepth, max_keys);
                    while let Some(child) = children.next().await {
                        yield child?;
                    }
                }
            }
        }
    })
}

fn list_versions<'a>(
    client: &'a dyn S3Client,
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
