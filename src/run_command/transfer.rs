use std::borrow::Cow;
use std::collections::hash_map::Entry;
use std::fmt;
use std::fs;
use std::io::{self, Write};
use std::path::{Component, Path, PathBuf};
use std::pin::pin;

use anyhow::{Error, anyhow, bail};
use aws_sdk_s3::types::{MetadataDirective, StorageClass};
use futures::TryStreamExt;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use serde::Serialize;
use tempfile::NamedTempFile;

use crate::adapters::aws::{DeleteObjectRequest, S3Client};
use crate::arg::{ChangeStorage, Download, S3Copy, S3Move, S3Path, StorageClassName};
use crate::error::FunctionError;
use crate::object::StreamObject;
use crate::utils::{combine_keys, copies_are_listed};

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
    /// file exists and `--force` wasn't given, or this is a dry run. A folder
    /// marker (a key ending in `/`, as the S3 console creates) becomes a
    /// directory.
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
        let is_folder = key.ends_with('/');
        let file_path = local_path(&self.destination, key, object.version_id.as_deref())?;
        let file_name = file_path
            .to_str()
            .ok_or(FunctionError::FileNameParseError)?;

        let record = Downloaded {
            action: "download",
            object: ObjectRef::new(bucket, object),
            path: file_name.to_owned(),
            skipped: !is_folder && file_path.exists() && !self.force,
        };
        if record.skipped || dry_run {
            return Ok(record);
        }
        if is_folder {
            fs::create_dir_all(&file_path)?;
            return Ok(record);
        }

        let mut stream = client
            .get_object(bucket, key, object.version_id.as_deref())
            .await?;

        let dir_path = file_path.parent().ok_or(FunctionError::ParentPathParse)?;
        fs::create_dir_all(dir_path)?;
        // Renamed into place once complete, so a failed or cancelled download
        // never leaves a partial file that a later run would skip as existing.
        let mut output = partial_file(dir_path)?;

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
        // Keep only the io::Error, so the temporary file is dropped and deleted.
        output.persist(&file_path).map_err(|err| err.error)?;
        bar.finish_and_clear();

        Ok(record)
    }
}

/// Where `key` downloads to under `destination`; versions get a `.v{id}`
/// suffix. Keys that would land outside `destination`, through `..` or an
/// absolute path, are refused.
fn local_path(destination: &str, key: &str, version_id: Option<&str>) -> Result<PathBuf, Error> {
    let relative = match version_id {
        Some(vid) if !key.ends_with('/') => Cow::Owned(format!("{key}.v{vid}")),
        _ => Cow::Borrowed(key),
    };
    let stays_inside = Path::new(relative.as_ref())
        .components()
        .all(|component| matches!(component, Component::Normal(_) | Component::CurDir));
    if !stays_inside {
        bail!("refusing to download outside {destination}");
    }
    Ok(Path::new(destination).join(relative.as_ref()))
}

/// A temporary file in `dir` for a download in progress; dropping it deletes it.
fn partial_file(dir: &Path) -> io::Result<NamedTempFile> {
    let mut builder = tempfile::Builder::new();
    builder.prefix(".s3find-").suffix(".part");
    // Like `File::create`: permissions follow the umask instead of owner-only.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        builder.permissions(fs::Permissions::from_mode(0o666));
    }
    builder.tempfile_in(dir)
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

impl CopyOptions<'_> {
    /// The key `key` is copied to in the destination bucket.
    fn target(&self, key: &str) -> String {
        combine_keys(self.flat, key, self.destination.prefix.as_deref())
    }
}

/// Copies live objects under `options.destination` and returns the objects
/// it copied (see [`objects_to_copy`]).
async fn copy_objects<'o, C: S3Client>(
    ctx: &mut CommandContext<'_, C>,
    objects: &'o [StreamObject],
    options: &CopyOptions<'_>,
) -> Result<Vec<&'o StreamObject>, Error> {
    let copies = objects_to_copy(ctx, objects, options)?;

    let (client, source_bucket) = (ctx.client, &ctx.path.bucket);
    let dry_run = ctx.options.dry_run;
    ctx.for_each_object(copies.iter().copied(), |object| async move {
        let key = object.require_key()?;
        let destination = options.destination;
        let target = options.target(key);
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
    .await?;
    Ok(copies)
}

/// The live objects of a batch to copy, checked before anything is copied.
///
/// Skips an object already where its copy would go. When the destination is
/// inside the searched path, the copies are listed too, so without `--flat`
/// objects already under the destination are skipped as well: copying them
/// would nest the destination in itself again and again.
///
/// With `--flat`, two different keys copied to the same key are an error, in
/// this batch or an earlier one: only one copy would survive, and `move` would
/// delete both sources.
fn objects_to_copy<'o, C>(
    ctx: &mut CommandContext<'_, C>,
    objects: &'o [StreamObject],
    options: &CopyOptions<'_>,
) -> Result<Vec<&'o StreamObject>, Error> {
    let source_bucket = &ctx.path.bucket;
    let destination = options.destination;
    let same_bucket = *source_bucket == destination.bucket;
    let destination_dir = combine_keys(false, "", destination.prefix.as_deref());
    let skip_nested = !options.flat && copies_are_listed(ctx.path, destination);

    let mut copies = Vec::new();
    for object in live_objects(objects) {
        let key = object.require_key()?;
        let target = options.target(key);
        let in_place = same_bucket && target == key;
        if in_place || (skip_nested && key.starts_with(destination_dir.as_str())) {
            continue;
        }

        if options.flat {
            match ctx.flat_destinations.entry(target) {
                // Another version of the same key.
                Entry::Occupied(entry) if entry.get() == key => {}
                Entry::Occupied(entry) => {
                    return Err(anyhow!(
                        "s3://{source_bucket}/{} and s3://{source_bucket}/{key} would both be copied to s3://{}/{}",
                        entry.get(),
                        destination.bucket,
                        entry.key(),
                    ));
                }
                Entry::Vacant(entry) => {
                    entry.insert(key.to_owned());
                }
            }
        }
        copies.push(object);
    }
    Ok(copies)
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
        copy_objects(ctx, objects, &options).await?;
        Ok(())
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
        // Only what was copied is deleted: never an object moved onto itself.
        let copied = copy_objects(ctx, objects, &options).await?;
        if copied.is_empty() || ctx.options.dry_run {
            return Ok(());
        }

        let key_list = copied
            .into_iter()
            .map(|object| {
                Ok(DeleteObjectRequest {
                    key: object.require_key()?.to_owned(),
                    version_id: object.version_id.clone(),
                })
            })
            .collect::<Result<Vec<_>, Error>>()?;
        ctx.client
            .delete_objects(&ctx.path.bucket, key_list)
            .await?
            .check()
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
