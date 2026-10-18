use anyhow::Error;

use crate::adapters::aws::S3Client;
use crate::arg::SetPublic;
use crate::object::StreamObject;

use super::{CommandContext, RunCommand, live_objects};

#[inline]
pub(crate) fn generate_s3_url(region: &str, bucket: &str, key: &str) -> String {
    let encoded_key = key
        .split('/')
        .map(|segment| urlencoding::encode(segment).into_owned())
        .collect::<Vec<_>>()
        .join("/");

    match region {
        "us-east-1" => format!("https://{bucket}.s3.amazonaws.com/{encoded_key}"),
        _ => format!("https://{bucket}.s3-{region}.amazonaws.com/{encoded_key}"),
    }
}

impl RunCommand for SetPublic {
    async fn execute<C: S3Client>(
        &self,
        ctx: &mut CommandContext<'_, C>,
        objects: &[StreamObject],
    ) -> Result<(), Error> {
        let region = ctx
            .client
            .region()
            .unwrap_or_else(|| "us-east-1".to_string());

        for object in live_objects(objects) {
            let key = object.require_key()?;

            ctx.client
                .put_object_acl_public_read(&ctx.path.bucket, key, object.version_id.as_deref())
                .await?;

            let url = generate_s3_url(&region, &ctx.path.bucket, key);
            writeln!(ctx.out, "{} {}", object.display_key(), url)?;
        }
        Ok(())
    }
}
