use std::fs;
use std::fs::File;
use std::io::Write;
use std::path::Path;

use anyhow::Error;
use async_trait::async_trait;
use aws_sdk_s3::types::MetadataDirective;
use indicatif::{ProgressBar, ProgressStyle};

use crate::adapters::aws::{CommandS3Client, DeleteObjectRequest};
use crate::arg::{ChangeStorage, Download, S3Copy, S3Move, S3Path};
use crate::command::StreamObject;
use crate::error::FunctionError;
use crate::utils::combine_keys;

use super::RunCommand;
use super::shared::build_copy_source;

#[async_trait]
impl RunCommand for Download {
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
                .as_ref()
                .ok_or(FunctionError::ObjectFieldError)?;

            let size = stream_obj.object.size.unwrap_or_default() as u64;
            let file_path = if let Some(ref vid) = stream_obj.version_id {
                Path::new(&self.destination).join(format!("{}.v{}", key, vid))
            } else {
                Path::new(&self.destination).join(key)
            };
            let dir_path = file_path.parent().ok_or(FunctionError::ParentPathParse)?;

            let mut count: u64 = 0;
            let pb = ProgressBar::new(size);
            pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta})")?
            .progress_chars("#>-"));

            if file_path.exists() && !self.force {
                eprintln!(
                    "skipping existing file: {}",
                    file_path
                        .to_str()
                        .ok_or(FunctionError::FileNameParseError)?
                );
                continue;
            }

            println!(
                "downloading: s3://{}/{} => {}",
                &path.bucket,
                stream_obj.display_key(),
                file_path
                    .to_str()
                    .ok_or(FunctionError::FileNameParseError)?
            );

            let mut stream = client
                .get_object(&path.bucket, key, stream_obj.version_id.as_deref())
                .await?;

            fs::create_dir_all(dir_path)?;
            let mut output = File::create(&file_path)?;

            while let Some(bytes) = stream.try_next().await? {
                output.write_all(&bytes)?;
                count += bytes.len() as u64;
                pb.set_position(count);
            }
        }
        Ok(())
    }
}

#[async_trait]
impl RunCommand for S3Copy {
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
                .clone()
                .ok_or(FunctionError::ObjectFieldError)?;

            let target = combine_keys(self.flat, &key, &self.destination.prefix);
            let source_path =
                build_copy_source(&path.bucket, &key, stream_obj.version_id.as_deref());

            println!(
                "copying: s3://{} => s3://{}/{}",
                &source_path, &self.destination.bucket, target,
            );

            client
                .copy_object(
                    &self.destination.bucket,
                    &target,
                    &source_path,
                    self.storage_class.clone(),
                    None,
                )
                .await?;
        }
        Ok(())
    }
}

#[async_trait]
impl RunCommand for S3Move {
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
                .clone()
                .ok_or(FunctionError::ObjectFieldError)?;

            let target = combine_keys(self.flat, &key, &self.destination.prefix);
            let source_path =
                build_copy_source(&path.bucket, &key, stream_obj.version_id.as_deref());

            println!(
                "moving: s3://{} => s3://{}/{}",
                &source_path, &self.destination.bucket, target,
            );

            client
                .copy_object(
                    &self.destination.bucket,
                    &target,
                    &source_path,
                    self.storage_class.clone(),
                    Some(MetadataDirective::Copy),
                )
                .await?;
        }

        let key_list: Vec<_> = list
            .iter()
            .filter(|stream_obj| !stream_obj.is_delete_marker)
            .filter_map(|stream_obj| {
                stream_obj
                    .object
                    .key
                    .clone()
                    .map(|key| DeleteObjectRequest {
                        key,
                        version_id: stream_obj.version_id.clone(),
                    })
            })
            .collect();

        if key_list.is_empty() {
            return Ok(());
        }

        client.delete_objects(&path.bucket, key_list).await?;
        Ok(())
    }
}

#[async_trait]
impl RunCommand for ChangeStorage {
    async fn execute(
        &self,
        client: &dyn CommandS3Client,
        path: &S3Path,
        stream_objects: &[StreamObject],
    ) -> Result<(), Error> {
        for stream_obj in stream_objects {
            if stream_obj.is_delete_marker {
                continue;
            }

            if let Some(key) = &stream_obj.object.key {
                println!(
                    "Changing storage class for s3://{}/{} to {:?}",
                    path.bucket,
                    stream_obj.display_key(),
                    self.storage_class
                );

                let source_path =
                    build_copy_source(&path.bucket, key, stream_obj.version_id.as_deref());

                client
                    .copy_object(
                        &path.bucket,
                        key,
                        &source_path,
                        Some(self.storage_class.clone()),
                        Some(MetadataDirective::Copy),
                    )
                    .await?;
            }
        }
        Ok(())
    }
}
