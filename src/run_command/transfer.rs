use std::fmt;
use std::fs;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::pin::pin;

use anyhow::Error;
use aws_sdk_s3::types::{MetadataDirective, StorageClass};
use futures::TryStreamExt;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use serde::Serialize;

use crate::adapters::aws::{DeleteObjectRequest, S3Client};
use crate::arg::{ChangeStorage, Download, S3Copy, S3Move, S3Path, StorageClassName};
use crate::error::FunctionError;
use crate::object::StreamObject;
use crate::utils::combine_keys;

use super::report::ObjectRef;
use super::{CommandContext, RunCommand, build_copy_source, in_order, live_objects};

/// A downloaded object, or one skipped because its file already exists.
#[derive(Serialize)]
struct Downloaded<'a> {
    action: &'static str,
    #[serde(flatten)]
    object: ObjectRef<'a>,
    path: String,
    skipped: bool,
}

impl fmt::Display for Downloaded<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "downloading: {} => {}", self.object, self.path)
    }
}

impl RunCommand for Download {
    async fn execute<C: S3Client>(
        &self,
        ctx: &mut CommandContext<'_, C>,
        objects: &[StreamObject],
    ) -> Result<(), Error> {
        let (client, bucket) = (ctx.client, &ctx.path.bucket);
        let dry_run = ctx.options.dry_run;
        let bars = MultiProgress::new();
        let style = ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta}) {wide_msg}")?
            .progress_chars("#>-");

        let mut downloads = pin!(in_order(
            bucket,
            live_objects(objects),
            ctx.options.concurrency,
            |object| self.download(client, bucket, object, dry_run, &bars, &style),
        ));
        while let Some(record) = downloads.try_next().await? {
            if record.skipped && !ctx.options.json {
                bars.suspend(|| eprintln!("skipping existing file: {}", record.path));
            } else {
                bars.suspend(|| ctx.report(&record))?;
            }
        }
        Ok(())
    }
}

impl Download {
    /// Downloads one object, showing its progress among `bars`, unless its
    /// file exists and `--force` wasn't given, or this is a dry run.
    async fn download<'a>(
        &self,
        client: &impl S3Client,
        bucket: &'a str,
        object: &'a StreamObject,
        dry_run: bool,
        bars: &MultiProgress,
        style: &ProgressStyle,
    ) -> Result<Downloaded<'a>, Error> {
        let key = object.require_key()?;

        let file_path = match &object.version_id {
            Some(vid) => Path::new(&self.destination).join(format!("{key}.v{vid}")),
            None => Path::new(&self.destination).join(key),
        };
        let dir_path = file_path.parent().ok_or(FunctionError::ParentPathParse)?;
        let file_name = file_path
            .to_str()
            .ok_or(FunctionError::FileNameParseError)?;

        let record = Downloaded {
            action: "download",
            object: ObjectRef::new(bucket, object),
            path: file_name.to_owned(),
            skipped: file_path.exists() && !self.force,
        };
        if record.skipped || dry_run {
            return Ok(record);
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

        Ok(record)
    }
}

/// A copied or moved object.
#[derive(Serialize)]
struct Copied<'a> {
    action: &'static str,
    #[serde(flatten)]
    object: ObjectRef<'a>,
    destination: String,
    /// The copy source sent to S3, shown in the text line.
    #[serde(skip)]
    source: String,
    #[serde(skip)]
    verb: &'static str,
}

impl fmt::Display for Copied<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}: s3://{} => {}",
            self.verb, self.source, self.destination
        )
    }
}

/// How `copy_objects` copies.
struct CopyOptions<'a> {
    destination: &'a S3Path,
    flat: bool,
    storage_class: Option<&'a StorageClassName>,
    metadata_directive: Option<MetadataDirective>,
    action: &'static str,
    verb: &'static str,
}

/// Copies live objects under `options.destination`.
async fn copy_objects<C: S3Client>(
    ctx: &mut CommandContext<'_, C>,
    objects: &[StreamObject],
    options: CopyOptions<'_>,
) -> Result<(), Error> {
    let (client, source_bucket) = (ctx.client, &ctx.path.bucket);
    let dry_run = ctx.options.dry_run;
    let options = &options;
    ctx.for_each_object(live_objects(objects), |object| async move {
        let key = object.require_key()?;
        let destination = options.destination;
        let target = combine_keys(options.flat, key, destination.prefix.as_deref());
        let source = build_copy_source(source_bucket, key, object.version_id.as_deref());

        if !dry_run {
            client
                .copy_object(
                    &destination.bucket,
                    &target,
                    &source,
                    options.storage_class.map(StorageClass::from),
                    options.metadata_directive.clone(),
                )
                .await?;
        }

        Ok(Copied {
            action: options.action,
            object: ObjectRef::new(source_bucket, object),
            destination: format!("s3://{}/{target}", destination.bucket),
            source,
            verb: options.verb,
        })
    })
    .await
}

impl RunCommand for S3Copy {
    async fn execute<C: S3Client>(
        &self,
        ctx: &mut CommandContext<'_, C>,
        objects: &[StreamObject],
    ) -> Result<(), Error> {
        let options = CopyOptions {
            destination: &self.destination,
            flat: self.flat,
            storage_class: self.storage_class.as_ref(),
            metadata_directive: None,
            action: "copy",
            verb: "copying",
        };
        copy_objects(ctx, objects, options).await
    }
}

impl RunCommand for S3Move {
    async fn execute<C: S3Client>(
        &self,
        ctx: &mut CommandContext<'_, C>,
        objects: &[StreamObject],
    ) -> Result<(), Error> {
        let options = CopyOptions {
            destination: &self.destination,
            flat: self.flat,
            storage_class: self.storage_class.as_ref(),
            metadata_directive: Some(MetadataDirective::Copy),
            action: "move",
            verb: "moving",
        };
        copy_objects(ctx, objects, options).await?;

        let key_list = live_objects(objects)
            .map(|object| {
                Ok(DeleteObjectRequest {
                    key: object.require_key()?.to_owned(),
                    version_id: object.version_id.clone(),
                })
            })
            .collect::<Result<Vec<_>, Error>>()?;

        if key_list.is_empty() || ctx.options.dry_run {
            return Ok(());
        }

        ctx.client
            .delete_objects(&ctx.path.bucket, key_list)
            .await?;
        Ok(())
    }
}

/// An object whose storage class was changed.
#[derive(Serialize)]
struct StorageChanged<'a> {
    action: &'static str,
    #[serde(flatten)]
    object: ObjectRef<'a>,
    storage_class: &'a str,
}

impl fmt::Display for StorageChanged<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Changing storage class for {} to {}",
            self.object, self.storage_class
        )
    }
}

impl RunCommand for ChangeStorage {
    async fn execute<C: S3Client>(
        &self,
        ctx: &mut CommandContext<'_, C>,
        objects: &[StreamObject],
    ) -> Result<(), Error> {
        let (client, bucket) = (ctx.client, &ctx.path.bucket);
        let dry_run = ctx.options.dry_run;
        let storage_class = &self.storage_class;
        ctx.for_each_object(live_objects(objects), |object| async move {
            if !dry_run {
                let key = object.require_key()?;
                let source = build_copy_source(bucket, key, object.version_id.as_deref());
                client
                    .copy_object(
                        bucket,
                        key,
                        &source,
                        Some(StorageClass::from(storage_class)),
                        Some(MetadataDirective::Copy),
                    )
                    .await?;
            }

            Ok(StorageChanged {
                action: "change-storage",
                object: ObjectRef::new(bucket, object),
                storage_class: storage_class.as_str(),
            })
        })
        .await
    }
}
