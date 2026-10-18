use std::fs;
use std::fs::File;
use std::io::Write;
use std::path::Path;

use anyhow::Error;
use async_trait::async_trait;
use aws_sdk_s3::types::{MetadataDirective, StorageClass};
use indicatif::{ProgressBar, ProgressStyle};

use crate::adapters::aws::DeleteObjectRequest;
use crate::arg::{ChangeStorage, Download, S3Copy, S3Move, S3Path};
use crate::error::FunctionError;
use crate::object::StreamObject;
use crate::utils::combine_keys;

use super::{CommandContext, RunCommand, build_copy_source, live_objects};

#[async_trait]
impl RunCommand for Download {
    async fn execute(
        &self,
        ctx: &mut CommandContext<'_>,
        objects: &[StreamObject],
    ) -> Result<(), Error> {
        for object in live_objects(objects) {
            let key = object.require_key()?;

            let file_path = match &object.version_id {
                Some(vid) => Path::new(&self.destination).join(format!("{}.v{}", key, vid)),
                None => Path::new(&self.destination).join(key),
            };
            let dir_path = file_path.parent().ok_or(FunctionError::ParentPathParse)?;
            let file_name = file_path
                .to_str()
                .ok_or(FunctionError::FileNameParseError)?;

            if file_path.exists() && !self.force {
                eprintln!("skipping existing file: {file_name}");
                continue;
            }

            writeln!(
                ctx.out,
                "downloading: s3://{}/{} => {}",
                ctx.path.bucket,
                object.display_key(),
                file_name
            )?;

            let mut stream = ctx
                .client
                .get_object(&ctx.path.bucket, key, object.version_id.as_deref())
                .await?;

            fs::create_dir_all(dir_path)?;
            let mut output = File::create(&file_path)?;

            let pb = ProgressBar::new(object.object.size.unwrap_or_default() as u64);
            pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta})")?
            .progress_chars("#>-"));

            while let Some(bytes) = stream.try_next().await? {
                output.write_all(&bytes)?;
                pb.inc(bytes.len() as u64);
            }
        }
        Ok(())
    }
}

/// Copies live objects under `destination`, printing `verb: source => target`.
async fn copy_objects(
    ctx: &mut CommandContext<'_>,
    objects: &[StreamObject],
    destination: &S3Path,
    flat: bool,
    storage_class: Option<&StorageClass>,
    metadata_directive: Option<MetadataDirective>,
    verb: &str,
) -> Result<(), Error> {
    for object in live_objects(objects) {
        let key = object.require_key()?;
        let target = combine_keys(flat, key, &destination.prefix);
        let source_path = build_copy_source(&ctx.path.bucket, key, object.version_id.as_deref());

        writeln!(
            ctx.out,
            "{verb}: s3://{} => s3://{}/{}",
            source_path, destination.bucket, target,
        )?;

        ctx.client
            .copy_object(
                &destination.bucket,
                &target,
                &source_path,
                storage_class.cloned(),
                metadata_directive.clone(),
            )
            .await?;
    }
    Ok(())
}

#[async_trait]
impl RunCommand for S3Copy {
    async fn execute(
        &self,
        ctx: &mut CommandContext<'_>,
        objects: &[StreamObject],
    ) -> Result<(), Error> {
        copy_objects(
            ctx,
            objects,
            &self.destination,
            self.flat,
            self.storage_class.as_ref(),
            None,
            "copying",
        )
        .await
    }
}

#[async_trait]
impl RunCommand for S3Move {
    async fn execute(
        &self,
        ctx: &mut CommandContext<'_>,
        objects: &[StreamObject],
    ) -> Result<(), Error> {
        copy_objects(
            ctx,
            objects,
            &self.destination,
            self.flat,
            self.storage_class.as_ref(),
            Some(MetadataDirective::Copy),
            "moving",
        )
        .await?;

        let key_list = live_objects(objects)
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

        ctx.client
            .delete_objects(&ctx.path.bucket, key_list)
            .await?;
        Ok(())
    }
}

#[async_trait]
impl RunCommand for ChangeStorage {
    async fn execute(
        &self,
        ctx: &mut CommandContext<'_>,
        objects: &[StreamObject],
    ) -> Result<(), Error> {
        for object in live_objects(objects) {
            let key = object.require_key()?;

            writeln!(
                ctx.out,
                "Changing storage class for s3://{}/{} to {:?}",
                ctx.path.bucket,
                object.display_key(),
                self.storage_class
            )?;

            let source_path =
                build_copy_source(&ctx.path.bucket, key, object.version_id.as_deref());

            ctx.client
                .copy_object(
                    &ctx.path.bucket,
                    key,
                    &source_path,
                    Some(self.storage_class.clone()),
                    Some(MetadataDirective::Copy),
                )
                .await?;
        }
        Ok(())
    }
}
