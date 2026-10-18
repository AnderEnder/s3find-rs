use anyhow::Error;

use crate::adapters::aws::{DeleteObjectRequest, S3Client};
use crate::arg::MultipleDelete;
use crate::object::StreamObject;

use super::{CommandContext, RunCommand};

impl RunCommand for MultipleDelete {
    async fn execute<C: S3Client>(
        &self,
        ctx: &mut CommandContext<'_, C>,
        objects: &[StreamObject],
    ) -> Result<(), Error> {
        // Delete markers are included: deleting one by version id undeletes the object.
        let key_list = objects
            .iter()
            .map(|object| {
                Ok(DeleteObjectRequest {
                    key: object.require_key()?.to_owned(),
                    version_id: object.version_id.clone(),
                })
            })
            .collect::<Result<Vec<_>, Error>>()?;

        if key_list.is_empty() {
            return Ok(());
        }

        for deleted in ctx
            .client
            .delete_objects(&ctx.path.bucket, key_list)
            .await?
        {
            let version_info = deleted
                .version_id
                .as_ref()
                .map(|v| format!(" (version: {})", v))
                .unwrap_or_default();
            writeln!(
                ctx.out,
                "deleted: s3://{}/{}{}",
                ctx.path.bucket,
                deleted.key.as_deref().unwrap_or(""),
                version_info
            )?;
        }

        Ok(())
    }
}
