use anyhow::Error;
use async_trait::async_trait;
use aws_sdk_s3::types::Tag;

use crate::adapters::aws::CommandS3Client;
use crate::arg::{ListTags, S3Path, SetTags};
use crate::command::StreamObject;

use super::RunCommand;

#[async_trait]
impl RunCommand for SetTags {
    async fn execute(
        &self,
        client: &dyn CommandS3Client,
        path: &S3Path,
        list: &[StreamObject],
    ) -> Result<(), Error> {
        for stream_obj in list {
            if stream_obj.is_delete_marker {
                continue;
            }

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

            let key = stream_obj
                .object
                .key
                .as_deref()
                .ok_or(crate::error::FunctionError::ObjectFieldError)?;

            client
                .put_object_tagging(&path.bucket, key, stream_obj.version_id.as_deref(), tags)
                .await?;

            println!(
                "tags are set for: s3://{}/{}",
                &path.bucket,
                stream_obj.display_key()
            );
        }
        Ok(())
    }
}

#[async_trait]
impl RunCommand for ListTags {
    async fn execute(
        &self,
        client: &dyn CommandS3Client,
        path: &S3Path,
        list: &[StreamObject],
    ) -> Result<(), Error> {
        for stream_obj in list {
            if stream_obj.is_delete_marker {
                continue;
            }

            let tags: String = if let Some(ref cached_tags) = stream_obj.tags {
                cached_tags
                    .iter()
                    .map(|x| format!("{}:{}", x.key(), x.value()))
                    .collect::<Vec<String>>()
                    .join(",")
            } else {
                let key = stream_obj
                    .object
                    .key
                    .as_deref()
                    .ok_or(crate::error::FunctionError::ObjectFieldError)?;

                client
                    .get_object_tagging(&path.bucket, key, stream_obj.version_id.as_deref())
                    .await?
                    .into_iter()
                    .map(|x| format!("{}:{}", x.key(), x.value()))
                    .collect::<Vec<String>>()
                    .join(",")
            };

            println!(
                "s3://{}/{} {}",
                &path.bucket,
                stream_obj.display_key(),
                tags,
            );
        }
        Ok(())
    }
}
