use std::fmt;

use anyhow::Error;
use aws_sdk_s3::types::ObjectStorageClass;
use serde::Serialize;

use crate::adapters::aws::{RestoreObjectStatus, S3Client};
use crate::arg::Restore;
use crate::object::StreamObject;

use super::report::ObjectRef;
use super::{CommandContext, RunCommand, live_objects};

fn is_archived(object: &StreamObject) -> bool {
    matches!(
        object.object.storage_class,
        Some(ObjectStorageClass::Glacier | ObjectStorageClass::DeepArchive)
    )
}

/// A restore requested for an archived object; `status` is `None` in a dry run.
#[derive(Serialize)]
struct RestoreRequested<'a> {
    action: &'static str,
    #[serde(flatten)]
    object: ObjectRef<'a>,
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<RestoreObjectStatus>,
}

impl fmt::Display for RestoreRequested<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self.status {
            None | Some(RestoreObjectStatus::Started) => "Restore initiated for",
            Some(RestoreObjectStatus::AlreadyInProgress) => "Restore already in progress for",
            Some(RestoreObjectStatus::InvalidObjectState) => {
                "Object is not in Glacier storage or already restored"
            }
        };
        write!(f, "{message}: {}", self.object.object.display_key())
    }
}

impl RunCommand for Restore {
    async fn execute<C: S3Client>(
        &self,
        ctx: &mut CommandContext<'_, C>,
        objects: &[StreamObject],
    ) -> Result<(), Error> {
        let (client, bucket) = (ctx.client, &ctx.path.bucket);
        let dry_run = ctx.options.dry_run;
        let archived = live_objects(objects).filter(|object| is_archived(object));
        ctx.for_each_object(archived, |object| async move {
            let status = if dry_run {
                None
            } else {
                let status = client
                    .restore_object(
                        bucket,
                        object.require_key()?,
                        object.version_id.as_deref(),
                        self.days,
                        self.tier.into(),
                    )
                    .await?;
                Some(status)
            };
            Ok(RestoreRequested {
                action: "restore",
                object: ObjectRef::new(bucket, object),
                status,
            })
        })
        .await
    }
}
