use anyhow::Error;
use aws_sdk_s3::types::ObjectStorageClass;

use crate::adapters::aws::{RestoreObjectStatus, S3Client};
use crate::arg::Restore;
use crate::object::StreamObject;

use super::{CommandContext, RunCommand, live_objects};

fn is_archived(object: &StreamObject) -> bool {
    matches!(
        object.object.storage_class,
        Some(ObjectStorageClass::Glacier | ObjectStorageClass::DeepArchive)
    )
}

impl RunCommand for Restore {
    async fn execute<C: S3Client>(
        &self,
        ctx: &mut CommandContext<'_, C>,
        objects: &[StreamObject],
    ) -> Result<(), Error> {
        for object in live_objects(objects).filter(|object| is_archived(object)) {
            let status = ctx
                .client
                .restore_object(
                    &ctx.path.bucket,
                    object.require_key()?,
                    object.version_id.as_deref(),
                    self.days,
                    self.tier.clone(),
                )
                .await?;

            let message = match status {
                RestoreObjectStatus::Started => "Restore initiated for",
                RestoreObjectStatus::AlreadyInProgress => "Restore already in progress for",
                RestoreObjectStatus::InvalidObjectState => {
                    "Object is not in Glacier storage or already restored"
                }
            };
            writeln!(ctx.out, "{message}: {}", object.display_key())?;
        }
        Ok(())
    }
}
