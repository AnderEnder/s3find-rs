//! Copying one object: with CopyObject, or part by part when it is too big
//! for CopyObject.

use std::ops::RangeInclusive;

use anyhow::Error;
use aws_sdk_s3::types::{CompletedPart, MetadataDirective, StorageClass};
use futures::{Stream, StreamExt, TryStreamExt, stream};

use crate::adapters::aws::{MultipartUpload, ObjectHead, PartCopy, S3Client};
use crate::object::StreamObject;

const MIB: i64 = 1024 * 1024;

/// The biggest object CopyObject copies.
const MAX_COPY_OBJECT_SIZE: i64 = 5 * 1024 * MIB;

/// Size of each part of a multipart copy, raised for objects that would
/// otherwise need more than [`MAX_PARTS`] parts.
const PART_SIZE: i64 = 512 * MIB;

/// Most parts S3 accepts in one upload.
const MAX_PARTS: u64 = 10_000;

/// Parts of one object copied at once.
const PART_CONCURRENCY: usize = 8;

/// One object to copy to `bucket/key`.
pub(super) struct ObjectCopy<'a> {
    pub(super) source_bucket: &'a str,
    pub(super) object: &'a StreamObject,
    /// The URL-encoded `bucket/key[?versionId=]` S3 copies from.
    pub(super) copy_source: &'a str,
    pub(super) bucket: &'a str,
    pub(super) key: &'a str,
    pub(super) storage_class: Option<StorageClass>,
    pub(super) metadata_directive: Option<MetadataDirective>,
}

impl ObjectCopy<'_> {
    pub(super) async fn run(self, client: &impl S3Client) -> Result<(), Error> {
        match self.object.object.size {
            Some(size) if size > MAX_COPY_OBJECT_SIZE => self.copy_in_parts(client).await,
            _ => {
                client
                    .copy_object(
                        self.bucket,
                        self.key,
                        self.copy_source,
                        self.storage_class,
                        self.metadata_directive,
                    )
                    .await
            }
        }
    }

    /// Copies with a multipart upload, carrying over the metadata and tags
    /// CopyObject would copy. A failed copy is aborted, so S3 keeps none of
    /// its parts.
    async fn copy_in_parts(&self, client: &impl S3Client) -> Result<(), Error> {
        let source_key = self.object.require_key()?;
        let version_id = self.object.version_id.as_deref();
        let head = client
            .head_object(self.source_bucket, source_key, version_id)
            .await?;
        // Reuse tags already fetched for tag filtering.
        let tags = match &self.object.tags {
            Some(tags) => tags.clone(),
            None => {
                client
                    .get_object_tagging(self.source_bucket, source_key, version_id)
                    .await?
            }
        };

        let upload_id = client
            .create_multipart_upload(
                self.bucket,
                self.key,
                &head,
                self.storage_class.clone(),
                &tags,
            )
            .await?;
        let upload = MultipartUpload {
            bucket: self.bucket,
            key: self.key,
            upload_id: &upload_id,
        };

        let copied = async {
            let parts = copy_parts(client, upload, self.copy_source, &head)
                .try_collect()
                .await?;
            client.complete_multipart_upload(upload, parts).await
        }
        .await;
        if copied.is_err() {
            // The copy's error matters more than the abort's.
            let _ = client.abort_multipart_upload(upload).await;
        }
        copied
    }
}

/// Copies every part of the source, up to [`PART_CONCURRENCY`] at a time,
/// yielding them in part order.
fn copy_parts<'a>(
    client: &'a impl S3Client,
    upload: MultipartUpload<'a>,
    copy_source: &'a str,
    head: &'a ObjectHead,
) -> impl Stream<Item = Result<CompletedPart, Error>> + Send + 'a {
    stream::iter(part_ranges(head.size).zip(1..))
        .map(move |(range, part_number)| {
            let part = PartCopy {
                part_number,
                copy_source,
                range,
                source_e_tag: head.e_tag.as_deref(),
            };
            client.upload_part_copy(upload, part)
        })
        .buffered(PART_CONCURRENCY)
}

/// The byte ranges of the parts of a `size`-byte object.
fn part_ranges(size: i64) -> impl Iterator<Item = RangeInclusive<i64>> {
    let part_size = PART_SIZE.max(size.cast_unsigned().div_ceil(MAX_PARTS).cast_signed());
    (0..size)
        .step_by(part_size as usize)
        .map(move |start| start..=(start + part_size).min(size) - 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_part_ranges_cover_the_object_in_order() {
        let size = MAX_COPY_OBJECT_SIZE + 1;
        let ranges: Vec<_> = part_ranges(size).collect();

        assert_eq!(ranges.len(), 11);
        assert_eq!(ranges[0], 0..=PART_SIZE - 1);
        assert_eq!(ranges[10], size - 1..=size - 1);
        assert!(ranges.windows(2).all(|w| *w[1].start() == w[0].end() + 1));
    }

    #[test]
    fn test_part_ranges_stay_within_the_part_limit() {
        let five_tib = 5 * 1024 * 1024 * MIB;
        let ranges: Vec<_> = part_ranges(five_tib).collect();

        assert!(ranges.len() as u64 <= MAX_PARTS);
        assert_eq!(*ranges.last().unwrap().end(), five_tib - 1);
    }
}
