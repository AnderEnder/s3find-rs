use anyhow::Error;
use async_trait::async_trait;

use crate::adapters::aws::{CommandS3Client, DeleteObjectRequest};
use crate::arg::{MultipleDelete, S3Path};
use crate::command::StreamObject;

use super::RunCommand;

#[async_trait]
impl RunCommand for MultipleDelete {
    async fn execute(
        &self,
        client: &dyn CommandS3Client,
        path: &S3Path,
        list: &[StreamObject],
    ) -> Result<(), Error> {
        let key_list: Vec<_> = list
            .iter()
            .filter_map(|stream_obj| {
                stream_obj
                    .object
                    .key
                    .clone()
                    .map(|key| DeleteObjectRequest {
                        key,
                        version_id: stream_obj.version_id.clone(),
                    })
            })
            .collect();

        for object in client.delete_objects(&path.bucket, key_list).await? {
            let version_info = object
                .version_id
                .as_ref()
                .map(|v| format!(" (version: {})", v))
                .unwrap_or_default();
            println!(
                "deleted: s3://{}/{}{}",
                &path.bucket,
                object.key.as_deref().unwrap_or(""),
                version_info
            );
        }

        Ok(())
    }
}
