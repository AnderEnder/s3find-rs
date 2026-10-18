use std::borrow::Cow;
use std::fmt;
use std::pin::pin;

use anyhow::Error;
use aws_sdk_s3::types::Tag;
use futures::TryStreamExt;
use serde::Serialize;

use crate::adapters::aws::S3Client;
use crate::arg::{ListTags, SetTags};
use crate::object::StreamObject;

use super::report::{ObjectRef, TagList, tag_map, write_record};
use super::{CommandContext, RunCommand, in_order, live_objects};

/// Tags set on an object.
#[derive(Serialize)]
struct TagsSet<'a> {
    action: &'static str,
    #[serde(flatten)]
    object: ObjectRef<'a>,
    #[serde(serialize_with = "tag_map")]
    tags: &'a [Tag],
}

impl fmt::Display for TagsSet<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "tags are set for: {}", self.object)
    }
}

impl RunCommand for SetTags {
    async fn execute<C: S3Client>(
        &self,
        ctx: &mut CommandContext<'_, C>,
        objects: &[StreamObject],
    ) -> Result<(), Error> {
        let tags = self
            .tags
            .iter()
            .map(|x| {
                Tag::builder()
                    .key(x.key.clone())
                    .value(x.value.clone())
                    .build()
            })
            .collect::<Result<Vec<_>, _>>()?;

        let (client, bucket, tags) = (ctx.client, &ctx.path.bucket, &tags);
        let dry_run = ctx.options.dry_run;
        ctx.for_each_object(live_objects(objects), |object| async move {
            if !dry_run {
                client
                    .put_object_tagging(
                        bucket,
                        object.require_key()?,
                        object.version_id.as_deref(),
                        tags.clone(),
                    )
                    .await?;
            }
            Ok(TagsSet {
                action: "tags",
                object: ObjectRef::new(bucket, object),
                tags,
            })
        })
        .await
    }
}

/// An object with its tags.
#[derive(Serialize)]
struct TaggedObject<'a> {
    #[serde(flatten)]
    object: ObjectRef<'a>,
    #[serde(serialize_with = "tag_map")]
    tags: Cow<'a, [Tag]>,
}

impl fmt::Display for TaggedObject<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.object, TagList(&self.tags))
    }
}

impl RunCommand for ListTags {
    async fn execute<C: S3Client>(
        &self,
        ctx: &mut CommandContext<'_, C>,
        objects: &[StreamObject],
    ) -> Result<(), Error> {
        let (client, bucket) = (ctx.client, &ctx.path.bucket);
        let mut tagged = pin!(in_order(
            bucket,
            live_objects(objects),
            ctx.options.concurrency,
            |object| async move {
                // Reuse tags already fetched for tag filtering.
                let tags = match &object.tags {
                    Some(cached) => Cow::Borrowed(cached.as_slice()),
                    None => Cow::Owned(
                        client
                            .get_object_tagging(
                                bucket,
                                object.require_key()?,
                                object.version_id.as_deref(),
                            )
                            .await?,
                    ),
                };
                Ok(TaggedObject {
                    object: ObjectRef::new(bucket, object),
                    tags,
                })
            },
        ));

        // Read-only, so never marked as a dry run.
        while let Some(record) = tagged.try_next().await? {
            write_record(ctx.out, &record, ctx.options.json, false)?;
        }
        Ok(())
    }
}
