use anyhow::Error;
use async_trait::async_trait;
use aws_sdk_s3::types::ObjectStorageClass;

use crate::adapters::aws::{CommandS3Client, RestoreObjectStatus};
use crate::arg::{Restore, S3Path};
use crate::command::StreamObject;

use super::RunCommand;

#[async_trait]
impl RunCommand for Restore {
    async fn execute(
        &self,
        client: &dyn CommandS3Client,
        path: &S3Path,
        stream_objects: &[StreamObject],
    ) -> Result<(), Error> {
        for stream_obj in stream_objects {
            if stream_obj.is_delete_marker {
                continue;
            }

            let object = &stream_obj.object;
            let is_glacier = object.storage_class == Some(ObjectStorageClass::Glacier)
                || object.storage_class == Some(ObjectStorageClass::DeepArchive);

            if is_glacier && object.key.is_some() {
                let key = object.key.as_deref().unwrap();
                match client
                    .restore_object(
                        &path.bucket,
                        key,
                        stream_obj.version_id.as_deref(),
                        self.days,
                        self.tier.clone(),
                    )
                    .await?
                {
                    RestoreObjectStatus::Started => {
                        println!("Restore initiated for: {}", stream_obj.display_key())
                    }
                    RestoreObjectStatus::AlreadyInProgress => {
                        println!(
                            "Restore already in progress for: {}",
                            stream_obj.display_key()
                        )
                    }
                    RestoreObjectStatus::InvalidObjectState => {
                        println!(
                            "Object is not in Glacier storage or already restored: {}",
                            stream_obj.display_key()
                        )
                    }
                }
            }
        }
        Ok(())
    }
}
