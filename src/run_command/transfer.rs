use std::fs;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::pin::pin;

use anyhow::Error;
use aws_sdk_s3::types::{MetadataDirective, StorageClass};
use futures::TryStreamExt;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

use crate::adapters::aws::{DeleteObjectRequest, S3Client};
use crate::arg::{ChangeStorage, Download, S3Copy, S3Move, S3Path, StorageClassName};
use crate::error::FunctionError;
use crate::object::StreamObject;
use crate::utils::combine_keys;

use super::{CommandContext, RunCommand, build_copy_source, in_order, live_objects};

impl RunCommand for Download {
    async fn execute<C: S3Client>(
        &self,
        ctx: &mut CommandContext<'_, C>,
        objects: &[StreamObject],
    ) -> Result<(), Error> {
        let (client, bucket) = (ctx.client, &ctx.path.bucket);
        let bars = MultiProgress::new();
        let style = ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta}) {wide_msg}")?
            .progress_chars("#>-");

        let mut downloads = pin!(in_order(
            bucket,
            live_objects(objects),
            ctx.concurrency,
            |object| self.download(client, bucket, object, &bars, &style),
        ));
        while let Some(line) = downloads.try_next().await? {
            if let Some(line) = line {
                bars.suspend(|| writeln!(ctx.out, "{line}"))?;
            }
        }
        Ok(())
    }
}

impl Download {
    /// Downloads one object, showing its progress among `bars`. Returns the
    /// line to print, or `None` when an existing file was skipped.
    async fn download(
        &self,
        client: &impl S3Client,
        bucket: &str,
        object: &StreamObject,
        bars: &MultiProgress,
        style: &ProgressStyle,
    ) -> Result<Option<String>, Error> {
        let key = object.require_key()?;

        let file_path = match &object.version_id {
            Some(vid) => Path::new(&self.destination).join(format!("{key}.v{vid}")),
            None => Path::new(&self.destination).join(key),
        };
        let dir_path = file_path.parent().ok_or(FunctionError::ParentPathParse)?;
        let file_name = file_path
            .to_str()
            .ok_or(FunctionError::FileNameParseError)?;

        if file_path.exists() && !self.force {
            bars.suspend(|| eprintln!("skipping existing file: {file_name}"));
            return Ok(None);
        }

        let mut stream = client
            .get_object(bucket, key, object.version_id.as_deref())
            .await?;

        fs::create_dir_all(dir_path)?;
        let mut output = File::create(&file_path)?;

        let size = object.object.size.unwrap_or_default() as u64;
        let bar = bars.add(
            ProgressBar::new(size)
                .with_style(style.clone())
                .with_message(file_name.to_owned()),
        );
        while let Some(bytes) = stream.try_next().await? {
            output.write_all(&bytes)?;
            bar.inc(bytes.len() as u64);
        }
        bar.finish_and_clear();

        Ok(Some(format!(
            "downloading: s3://{bucket}/{} => {file_name}",
            object.display_key()
        )))
    }
}

/// Copies live objects under `destination`, printing `verb: source => target`.
async fn copy_objects<C: S3Client>(
    ctx: &mut CommandContext<'_, C>,
    objects: &[StreamObject],
    destination: &S3Path,
    flat: bool,
    storage_class: Option<&StorageClassName>,
    metadata_directive: Option<MetadataDirective>,
    verb: &str,
) -> Result<(), Error> {
    let (client, source_bucket) = (ctx.client, &ctx.path.bucket);
    let metadata_directive = &metadata_directive;
    ctx.for_each_object(live_objects(objects), |object| async move {
        let key = object.require_key()?;
        let target = combine_keys(flat, key, destination.prefix.as_deref());
        let source_path = build_copy_source(source_bucket, key, object.version_id.as_deref());

        client
            .copy_object(
                &destination.bucket,
                &target,
                &source_path,
                storage_class.map(StorageClass::from),
                metadata_directive.clone(),
            )
            .await?;

        Ok(format!(
            "{verb}: s3://{source_path} => s3://{}/{target}",
            destination.bucket
        ))
    })
    .await
}

impl RunCommand for S3Copy {
    async fn execute<C: S3Client>(
        &self,
        ctx: &mut CommandContext<'_, C>,
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

impl RunCommand for S3Move {
    async fn execute<C: S3Client>(
        &self,
        ctx: &mut CommandContext<'_, C>,
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

impl RunCommand for ChangeStorage {
    async fn execute<C: S3Client>(
        &self,
        ctx: &mut CommandContext<'_, C>,
        objects: &[StreamObject],
    ) -> Result<(), Error> {
        let (client, bucket) = (ctx.client, &ctx.path.bucket);
        let storage_class = &self.storage_class;
        ctx.for_each_object(live_objects(objects), |object| async move {
            let key = object.require_key()?;
            let source_path = build_copy_source(bucket, key, object.version_id.as_deref());

            client
                .copy_object(
                    bucket,
                    key,
                    &source_path,
                    Some(StorageClass::from(storage_class)),
                    Some(MetadataDirective::Copy),
                )
                .await?;

            Ok(format!(
                "Changing storage class for s3://{bucket}/{} to {storage_class}",
                object.display_key()
            ))
        })
        .await
    }
}
