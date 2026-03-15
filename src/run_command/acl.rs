use anyhow::Error;
use async_trait::async_trait;

use crate::adapters::aws::CommandS3Client;
use crate::arg::{S3Path, SetPublic};
use crate::command::StreamObject;

use super::RunCommand;

#[inline]
pub(crate) fn generate_s3_url(region: &str, bucket: &str, key: &str) -> String {
    let encoded_key = key
        .split('/')
        .map(|segment| urlencoding::encode(segment).into_owned())
        .collect::<Vec<_>>()
        .join("/");

    match region {
        "us-east-1" => format!("https://{}.s3.amazonaws.com/{}", bucket, encoded_key),
        _ => format!(
            "https://{}.s3-{}.amazonaws.com/{}",
            bucket, region, encoded_key
        ),
    }
}

#[async_trait]
impl RunCommand for SetPublic {
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

            let key = stream_obj
                .object
                .key
                .as_deref()
                .ok_or(crate::error::FunctionError::ObjectFieldError)?;

            client
                .put_object_acl_public_read(&path.bucket, key, stream_obj.version_id.as_deref())
                .await?;

            let region = client.region().unwrap_or_else(|| "us-east-1".to_string());
            let url = generate_s3_url(&region, &path.bucket, key);
            println!("{} {}", stream_obj.display_key(), url);
        }
        Ok(())
    }
}
