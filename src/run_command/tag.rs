use anyhow::Error;
use aws_sdk_s3::types::Tag;

use crate::adapters::aws::S3Client;
use crate::arg::{ListTags, SetTags};
use crate::object::StreamObject;

use super::{CommandContext, RunCommand, live_objects};

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

        for object in live_objects(objects) {
            ctx.client
                .put_object_tagging(
                    &ctx.path.bucket,
                    object.require_key()?,
                    object.version_id.as_deref(),
                    tags.clone(),
                )
                .await?;

            writeln!(
                ctx.out,
                "tags are set for: s3://{}/{}",
                ctx.path.bucket,
                object.display_key()
            )?;
        }
        Ok(())
    }
}

fn format_tags(tags: &[Tag]) -> String {
    tags.iter()
        .map(|tag| format!("{}:{}", tag.key(), tag.value()))
        .collect::<Vec<_>>()
        .join(",")
}

impl RunCommand for ListTags {
    async fn execute<C: S3Client>(
        &self,
        ctx: &mut CommandContext<'_, C>,
        objects: &[StreamObject],
    ) -> Result<(), Error> {
        for object in live_objects(objects) {
            // Reuse tags already fetched for tag filtering.
            let tags = match &object.tags {
                Some(cached) => format_tags(cached),
                None => {
                    let fetched = ctx
                        .client
                        .get_object_tagging(
                            &ctx.path.bucket,
                            object.require_key()?,
                            object.version_id.as_deref(),
                        )
                        .await?;
                    format_tags(&fetched)
                }
            };

            writeln!(
                ctx.out,
                "s3://{}/{} {}",
                ctx.path.bucket,
                object.display_key(),
                tags,
            )?;
        }
        Ok(())
    }
}
