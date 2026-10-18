use std::fmt;

use anyhow::Error;
use serde::Serialize;

use crate::adapters::aws::{DeleteObjectRequest, S3Client};
use crate::arg::MultipleDelete;
use crate::object::StreamObject;

use super::report::S3Url;
use super::{CommandContext, RunCommand};

/// A deleted object version or delete marker.
#[derive(Serialize)]
struct Deleted<'a> {
    action: &'static str,
    bucket: &'a str,
    key: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    version_id: Option<&'a str>,
    url: S3Url<'a>,
}

impl<'a> Deleted<'a> {
    fn new(bucket: &'a str, key: &'a str, version_id: Option<&'a str>) -> Self {
        Self {
            action: "delete",
            bucket,
            key,
            version_id,
            url: S3Url { bucket, key },
        }
    }
}

impl fmt::Display for Deleted<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "deleted: {}", self.url)?;
        if let Some(version_id) = self.version_id {
            write!(f, " (version: {version_id})")?;
        }
        Ok(())
    }
}

impl RunCommand for MultipleDelete {
    async fn execute<C: S3Client>(
        &self,
        ctx: &mut CommandContext<'_, C>,
        objects: &[StreamObject],
    ) -> Result<(), Error> {
        let bucket = &ctx.path.bucket;

        if ctx.options.dry_run {
            for object in objects {
                let record =
                    Deleted::new(bucket, object.require_key()?, object.version_id.as_deref());
                ctx.report(&record)?;
            }
            return Ok(());
        }

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

        // Report what was deleted even when some keys weren't.
        let outcome = ctx.client.delete_objects(bucket, key_list).await?;
        for deleted in &outcome.deleted {
            let record = Deleted::new(
                bucket,
                deleted.key.as_deref().unwrap_or(""),
                deleted.version_id.as_deref(),
            );
            ctx.report(&record)?;
        }

        outcome.check()
    }
}
