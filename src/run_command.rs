use std::fs;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::process::Command;
use std::process::ExitStatus;

use anyhow::Error;
use async_trait::async_trait;
use aws_sdk_s3::types::MetadataDirective;
use csv::WriterBuilder;
use indicatif::{ProgressBar, ProgressStyle};
use serde::Deserialize;
use serde::Serialize;

use aws_sdk_s3::Client;
use aws_sdk_s3::types::{Delete, ObjectCannedAcl, ObjectIdentifier, Tag, Tagging};
use aws_sdk_s3::types::{GlacierJobParameters, ObjectStorageClass, RestoreRequest};
use aws_smithy_runtime_api::client::result::SdkError;
use aws_smithy_types::date_time::Format;

use crate::arg::*;
use crate::command::StreamObject;
use crate::error::*;
use crate::utils::combine_keys;

/// Build a properly URL-encoded copy_source path for S3 CopyObject operations.
/// S3 keys can contain special characters that need encoding in the copy_source parameter.
fn build_copy_source(bucket: &str, key: &str, version_id: Option<&str>) -> String {
    let encoded_key = urlencoding::encode(key);
    match version_id {
        Some(vid) => {
            let encoded_vid = urlencoding::encode(vid);
            format!("{}/{}?versionId={}", bucket, encoded_key, encoded_vid)
        }
        None => format!("{}/{}", bucket, encoded_key),
    }
}

impl Cmd {
    pub fn downcast(self) -> Box<dyn RunCommand> {
        match self {
            Cmd::Print(l) => Box::new(l),
            Cmd::Ls(l) => Box::new(l),
            Cmd::Exec(l) => Box::new(l),
            Cmd::Delete(l) => Box::new(l),
            Cmd::Download(l) => Box::new(l),
            Cmd::Tags(l) => Box::new(l),
            Cmd::LsTags(l) => Box::new(l),
            Cmd::Public(l) => Box::new(l),
            Cmd::Copy(l) => Box::new(l),
            Cmd::Move(l) => Box::new(l),
            Cmd::Nothing(l) => Box::new(l),
            Cmd::Restore(l) => Box::new(l),
            Cmd::ChangeStorage(l) => Box::new(l),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct ExecStatus {
    pub status: ExitStatus,
    pub runcommand: String,
}

#[async_trait]
pub trait RunCommand {
    async fn execute(
        &self,
        client: &Client,
        path: &S3Path,
        list: &[StreamObject],
    ) -> Result<(), Error>;
}

impl FastPrint {
    #[inline]
    fn print_stream_object<I: Write>(
        &self,
        io: &mut I,
        bucket: &str,
        stream_obj: &StreamObject,
    ) -> std::io::Result<()> {
        // Use display_key() which includes version info when present
        writeln!(io, "s3://{}/{}", bucket, stream_obj.display_key())
    }
}

#[async_trait]
impl RunCommand for FastPrint {
    async fn execute(
        &self,
        _c: &Client,
        path: &S3Path,
        list: &[StreamObject],
    ) -> Result<(), Error> {
        let mut stdout = std::io::stdout();
        for x in list {
            self.print_stream_object(&mut stdout, &path.bucket, x)?
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct ObjectRecord {
    e_tag: String,
    owner: String,
    size: i64,
    last_modified: String,
    key: String,
    storage_class: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    version_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    is_latest: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    is_delete_marker: Option<bool>,
}

impl From<&StreamObject> for ObjectRecord {
    fn from(stream_obj: &StreamObject) -> Self {
        let object = &stream_obj.object;
        ObjectRecord {
            e_tag: object.e_tag.clone().unwrap_or_default(),
            owner: object
                .owner
                .clone()
                .and_then(|x| x.display_name.clone())
                .unwrap_or_default(),
            size: object.size.unwrap_or_default(),
            last_modified: object
                .last_modified
                .and_then(|x| x.fmt(Format::DateTime).ok())
                .unwrap_or_default(),
            key: object.key.clone().unwrap_or_default(),
            storage_class: object
                .storage_class
                .clone()
                .map(|x| x.to_string())
                .unwrap_or_default(),
            version_id: stream_obj.version_id.clone(),
            is_latest: stream_obj.is_latest,
            is_delete_marker: if stream_obj.is_delete_marker {
                Some(true)
            } else {
                None
            },
        }
    }
}

impl AdvancedPrint {
    #[inline]
    fn print_stream_object<I: Write>(
        &self,
        io: &mut I,
        bucket: &str,
        stream_obj: &StreamObject,
    ) -> std::io::Result<()> {
        let object = &stream_obj.object;
        // Use display_key() which includes version info when present
        writeln!(
            io,
            "{0} {1} {2} {3} \"s3://{4}/{5}\" {6}",
            object.e_tag.as_ref().unwrap_or(&"NoEtag".to_string()),
            object
                .owner
                .as_ref()
                .and_then(|x| x.display_name.as_ref())
                .unwrap_or(&"None".to_string()),
            object.size.unwrap_or_default(),
            object
                .last_modified
                .and_then(|x| x.fmt(Format::DateTime).ok())
                .unwrap_or("None".to_string()),
            bucket,
            stream_obj.display_key(),
            object
                .storage_class
                .as_ref()
                .map(|x| x.as_str())
                .unwrap_or("NONE"),
        )
    }

    #[inline]
    fn print_json_stream_object<I: Write>(
        &self,
        io: &mut I,
        stream_obj: &StreamObject,
    ) -> Result<(), Error> {
        let record: ObjectRecord = stream_obj.into();
        writeln!(io, "{}", serde_json::to_string(&record)?)?;
        Ok(())
    }

    #[inline]
    fn print_csv_stream_objects<I: Write>(
        &self,
        io: &mut I,
        stream_objects: &[StreamObject],
    ) -> Result<(), Error> {
        let mut wtr = WriterBuilder::new().has_headers(false).from_writer(io);
        for stream_obj in stream_objects {
            let record: ObjectRecord = stream_obj.into();
            wtr.serialize(record)?;
        }
        wtr.flush()?;
        Ok(())
    }
}

#[async_trait]
impl RunCommand for AdvancedPrint {
    async fn execute(
        &self,
        _c: &Client,
        path: &S3Path,
        list: &[StreamObject],
    ) -> Result<(), Error> {
        let mut stdout = std::io::stdout();

        match self.format {
            PrintFormat::Json => {
                for x in list {
                    self.print_json_stream_object(&mut stdout, x)?;
                }
            }
            PrintFormat::Text => {
                for x in list {
                    self.print_stream_object(&mut stdout, &path.bucket, x)?;
                }
            }
            PrintFormat::Csv => {
                self.print_csv_stream_objects(&mut stdout, list)?;
            }
        }
        Ok(())
    }
}

impl Exec {
    #[inline]
    fn exec<I: Write>(&self, io: &mut I, key: &str) -> Result<ExecStatus, Error> {
        let command_str = self.utility.replace("{}", key);
        let split: Vec<_> = command_str.split(' ').collect();

        let (command_name, command_args) = match &*split {
            [command_name, command_args @ ..] => (*command_name, command_args),
            _ => return Err(FunctionError::CommandlineParse.into()),
        };

        let mut command = Command::new(command_name);
        for arg in command_args {
            command.arg(arg);
        }

        let output = command.output()?;
        let output_str = String::from_utf8_lossy(&output.stdout);
        writeln!(io, "{}", &output_str)?;

        Ok(ExecStatus {
            status: output.status,
            runcommand: command_str.clone(),
        })
    }
}

#[async_trait]
impl RunCommand for Exec {
    async fn execute(&self, _: &Client, path: &S3Path, list: &[StreamObject]) -> Result<(), Error> {
        let mut stdout = std::io::stdout();
        for x in list {
            // Use display_key() for the {} placeholder (includes version info)
            let s3_path = format!("s3://{}/{}", &path.bucket, x.display_key());
            self.exec(&mut stdout, &s3_path)?;
        }
        Ok(())
    }
}

#[async_trait]
impl RunCommand for MultipleDelete {
    async fn execute(
        &self,
        client: &Client,
        path: &S3Path,
        list: &[StreamObject],
    ) -> Result<(), Error> {
        let key_list: Vec<_> = list
            .iter()
            .filter_map(|stream_obj| {
                ObjectIdentifier::builder()
                    .set_key(stream_obj.object.key.clone())
                    // Pass version_id for version-aware deletes
                    .set_version_id(stream_obj.version_id.clone())
                    .build()
                    .ok()
            })
            .collect();

        let objects = Delete::builder().set_objects(Some(key_list)).build()?;

        client
            .delete_objects()
            .bucket(path.bucket.to_owned())
            .delete(objects)
            .send()
            .await
            .map_or_else(
                |e| {
                    eprintln!("{}", e);
                    Ok(())
                },
                |r| {
                    if let Some(deleted_list) = r.deleted {
                        for object in deleted_list {
                            let version_info = object
                                .version_id
                                .as_ref()
                                .map(|v| format!(" (version: {})", v))
                                .unwrap_or_default();
                            println!(
                                "deleted: s3://{}/{}{}",
                                &path.bucket,
                                object.key.as_ref().unwrap_or(&"".to_string()),
                                version_info
                            );
                        }
                    }
                    Ok(())
                },
            )
    }
}

#[async_trait]
impl RunCommand for SetTags {
    async fn execute(
        &self,
        client: &Client,
        path: &S3Path,
        list: &[StreamObject],
    ) -> Result<(), Error> {
        for stream_obj in list {
            // Skip delete markers - they have no content and cannot have tags
            if stream_obj.is_delete_marker {
                continue;
            }

            let tags = self
                .tags
                .iter()
                .filter_map(|x| {
                    Tag::builder()
                        .key(x.key.clone())
                        .value(x.value.clone())
                        .build()
                        .ok()
                })
                .collect();

            let tagging = Tagging::builder().set_tag_set(Some(tags)).build().ok();

            let mut request = client
                .put_object_tagging()
                .bucket(path.bucket.to_owned())
                .set_key(stream_obj.object.key.clone())
                .set_tagging(tagging);

            // Pass version_id if present
            if let Some(ref vid) = stream_obj.version_id {
                request = request.version_id(vid);
            }

            request.send().await?;

            println!(
                "tags are set for: s3://{}/{}",
                &path.bucket,
                stream_obj.display_key()
            );
        }
        Ok(())
    }
}

#[async_trait]
impl RunCommand for ListTags {
    async fn execute(
        &self,
        client: &Client,
        path: &S3Path,
        list: &[StreamObject],
    ) -> Result<(), Error> {
        for stream_obj in list {
            // Skip delete markers - they have no content and cannot have tags
            if stream_obj.is_delete_marker {
                continue;
            }

            // Use cached tags if available (from tag filtering), otherwise fetch
            let tags: String = if let Some(ref cached_tags) = stream_obj.tags {
                cached_tags
                    .iter()
                    .map(|x| format!("{}:{}", x.key(), x.value()))
                    .collect::<Vec<String>>()
                    .join(",")
            } else {
                // Fetch tags via API
                let mut request = client
                    .get_object_tagging()
                    .bucket(path.bucket.clone())
                    .set_key(stream_obj.object.key.clone());

                // Pass version_id if present
                if let Some(ref vid) = stream_obj.version_id {
                    request = request.version_id(vid);
                }

                let tag_output = request.send().await?;

                tag_output
                    .tag_set
                    .into_iter()
                    .map(|x| format!("{}:{}", x.key(), x.value()))
                    .collect::<Vec<String>>()
                    .join(",")
            };

            println!(
                "s3://{}/{} {}",
                &path.bucket,
                stream_obj.display_key(),
                tags,
            );
        }
        Ok(())
    }
}

#[inline]
fn generate_s3_url(region: &str, bucket: &str, key: &str) -> String {
    match region {
        "us-east-1" => format!("https://{}.s3.amazonaws.com/{}", bucket, key),
        _ => format!("https://{}.s3-{}.amazonaws.com/{}", bucket, region, key),
    }
}

#[async_trait]
impl RunCommand for SetPublic {
    async fn execute(
        &self,
        client: &Client,
        path: &S3Path,
        list: &[StreamObject],
    ) -> Result<(), Error> {
        for stream_obj in list {
            // Skip delete markers - they have no content and cannot have ACLs
            if stream_obj.is_delete_marker {
                continue;
            }

            let mut request = client
                .put_object_acl()
                .bucket(path.bucket.to_owned())
                .set_key(stream_obj.object.key.clone())
                .acl(ObjectCannedAcl::PublicRead);

            // Pass version_id if present
            if let Some(ref vid) = stream_obj.version_id {
                request = request.version_id(vid);
            }

            request.send().await?;

            if let Some(key) = stream_obj.object.key.as_ref() {
                let region = client
                    .config()
                    .region()
                    .map(|x| x.as_ref())
                    .unwrap_or("us-east-1");
                let url = generate_s3_url(region, &path.bucket, key);
                println!("{} {}", stream_obj.display_key(), url);
            } else {
                println!("No key found for object");
            }
        }
        Ok(())
    }
}

#[async_trait]
impl RunCommand for Download {
    async fn execute(
        &self,
        client: &Client,
        path: &S3Path,
        list: &[StreamObject],
    ) -> Result<(), Error> {
        for stream_obj in list {
            // Skip delete markers - they have no content to download
            if stream_obj.is_delete_marker {
                continue;
            }

            let key = stream_obj
                .object
                .key
                .as_ref()
                .ok_or(FunctionError::ObjectFieldError)?;

            let size = stream_obj.object.size.unwrap_or_default() as u64;
            // Include version_id in file path to avoid overwrites when downloading multiple versions
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

            println!(
                "downloading: s3://{}/{} => {}",
                &path.bucket,
                stream_obj.display_key(),
                file_path
                    .to_str()
                    .ok_or(FunctionError::FileNameParseError)?
            );

            if file_path.exists() && !self.force {
                return Ok(());
            }

            let mut request = client.get_object().bucket(&path.bucket).key(key);

            // Pass version_id if present for version-aware downloads
            if let Some(ref vid) = stream_obj.version_id {
                request = request.version_id(vid);
            }

            let mut stream = request.send().await?.body;

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
        client: &Client,
        path: &S3Path,
        list: &[StreamObject],
    ) -> Result<(), Error> {
        for stream_obj in list {
            // Skip delete markers - they have no content to copy
            if stream_obj.is_delete_marker {
                continue;
            }

            let key = stream_obj
                .object
                .key
                .clone()
                .ok_or(FunctionError::ObjectFieldError)?;

            let target = combine_keys(self.flat, &key, &self.destination.prefix);

            // Build URL-encoded copy_source path
            let source_path =
                build_copy_source(&path.bucket, &key, stream_obj.version_id.as_deref());

            println!(
                "copying: s3://{} => s3://{}/{}",
                &source_path, &self.destination.bucket, target,
            );

            client
                .copy_object()
                .bucket(&self.destination.bucket)
                .key(target)
                .copy_source(&source_path)
                .set_storage_class(self.storage_class.clone())
                .send()
                .await?;
        }
        Ok(())
    }
}

#[async_trait]
impl RunCommand for S3Move {
    async fn execute(
        &self,
        client: &Client,
        path: &S3Path,
        list: &[StreamObject],
    ) -> Result<(), Error> {
        for stream_obj in list {
            // Skip delete markers - they have no content to copy/move
            if stream_obj.is_delete_marker {
                continue;
            }

            let key = stream_obj
                .object
                .key
                .clone()
                .ok_or(FunctionError::ObjectFieldError)?;

            let target = combine_keys(self.flat, &key, &self.destination.prefix);

            // Build URL-encoded copy_source path
            let source_path =
                build_copy_source(&path.bucket, &key, stream_obj.version_id.as_deref());

            println!(
                "moving: s3://{} => s3://{}/{}",
                &source_path, &self.destination.bucket, target,
            );

            client
                .copy_object()
                .bucket(&self.destination.bucket)
                .key(target)
                .copy_source(&source_path)
                .set_storage_class(self.storage_class.clone())
                .metadata_directive(MetadataDirective::Copy)
                .send()
                .await?;
        }

        // Delete original objects with version_id if present (skip delete markers)
        let key_list: Vec<_> = list
            .iter()
            .filter(|stream_obj| !stream_obj.is_delete_marker)
            .filter_map(|stream_obj| {
                ObjectIdentifier::builder()
                    .set_key(stream_obj.object.key.clone())
                    .set_version_id(stream_obj.version_id.clone())
                    .build()
                    .ok()
            })
            .collect();

        let delete = Delete::builder().set_objects(Some(key_list)).build().ok();

        client
            .delete_objects()
            .bucket(path.bucket.clone())
            .set_delete(delete)
            .send()
            .await?;
        Ok(())
    }
}

#[async_trait]
impl RunCommand for DoNothing {
    async fn execute(&self, _c: &Client, _p: &S3Path, _l: &[StreamObject]) -> Result<(), Error> {
        Ok(())
    }
}

#[async_trait]
impl RunCommand for Restore {
    async fn execute(
        &self,
        client: &Client,
        path: &S3Path,
        stream_objects: &[StreamObject],
    ) -> Result<(), Error> {
        for stream_obj in stream_objects {
            // Skip delete markers - they have no content to restore
            if stream_obj.is_delete_marker {
                continue;
            }

            let object = &stream_obj.object;
            let is_glacier = object.storage_class == Some(ObjectStorageClass::Glacier)
                || object.storage_class == Some(ObjectStorageClass::DeepArchive);

            if is_glacier && object.key.is_some() {
                let key = object.key.as_ref().unwrap();
                let restore_request = RestoreRequest::builder()
                    .days(self.days)
                    .set_glacier_job_parameters(Some(
                        GlacierJobParameters::builder()
                            .tier(self.tier.clone())
                            .build()?,
                    ))
                    .build();

                let mut request = client
                    .restore_object()
                    .bucket(&path.bucket)
                    .key(key)
                    .restore_request(restore_request);

                // Pass version_id if present
                if let Some(ref vid) = stream_obj.version_id {
                    request = request.version_id(vid);
                }

                let result = request.send().await;

                match result {
                    Ok(_) => println!("Restore initiated for: {}", stream_obj.display_key()),
                    Err(e) => match e {
                        SdkError::ServiceError(err)
                            if err.err().meta().code() == Some("RestoreAlreadyInProgress") =>
                        {
                            println!(
                                "Restore already in progress for: {}",
                                stream_obj.display_key()
                            )
                        }
                        SdkError::ServiceError(err)
                            if err.err().meta().code() == Some("InvalidObjectState") =>
                        {
                            println!(
                                "Object is not in Glacier storage or already restored: {}",
                                stream_obj.display_key()
                            )
                        }
                        err => return Err(err.into()),
                    },
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl RunCommand for ChangeStorage {
    async fn execute(
        &self,
        client: &Client,
        path: &S3Path,
        stream_objects: &[StreamObject],
    ) -> Result<(), Error> {
        for stream_obj in stream_objects {
            // Skip delete markers - they have no content and cannot change storage class
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

                // Build URL-encoded copy_source path
                let source_path =
                    build_copy_source(&path.bucket, key, stream_obj.version_id.as_deref());

                client
                    .copy_object()
                    .copy_source(&source_path)
                    .bucket(&path.bucket)
                    .key(key)
                    .storage_class(self.storage_class.clone())
                    .metadata_directive(MetadataDirective::Copy)
                    .send()
                    .await?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_config::BehaviorVersion;
    use aws_sdk_s3::{
        primitives::DateTime,
        types::{Object, ObjectStorageClass, StorageClass},
    };
    use aws_smithy_runtime::client::http::test_util::{ReplayEvent, StaticReplayClient};
    use aws_smithy_types::body::SdkBody;
    use aws_smithy_types::date_time::Format;
    use http::{HeaderValue, StatusCode};
    use std::path::PathBuf;
    use tempfile::tempdir;

    #[test]
    fn test_advanced_print_object() -> Result<(), Error> {
        let mut buf = Vec::new();
        let cmd = AdvancedPrint::default();
        let bucket = "test";

        let object = Object::builder()
            .e_tag("9d48114aa7c18f9d68aa20086dbb7756")
            .key("somepath/otherpath")
            .size(4_997_288)
            .storage_class(ObjectStorageClass::Standard)
            .last_modified(DateTime::from_str(
                "2017-07-19T19:04:17.000Z",
                Format::DateTime,
            )?)
            .build();
        let stream_obj = StreamObject::from_object(object);

        cmd.print_stream_object(&mut buf, bucket, &stream_obj)?;
        let out = std::str::from_utf8(&buf)?;

        println!("{}", out);
        assert!(out.contains("9d48114aa7c18f9d68aa20086dbb7756"));
        assert!(out.contains("None"));
        assert!(out.contains("4997288"));
        assert!(out.contains("2017-07-19T19:04:17Z"));
        assert!(out.contains("s3://test/somepath/otherpath"));
        assert!(out.contains("STANDARD"));
        Ok(())
    }

    #[test]
    fn test_advanced_print_object_with_owner_no_date_no_storage() -> Result<(), Error> {
        let mut buf = Vec::new();
        let cmd = AdvancedPrint::default();
        let bucket = "test";

        let owner = aws_sdk_s3::types::Owner::builder()
            .display_name("test-owner")
            .id("owner-id-123456789")
            .build();

        let object = Object::builder()
            .e_tag("abc123456789")
            .key("some/test/file.txt")
            .size(1_234_567)
            .set_owner(Some(owner))
            .build();
        let stream_obj = StreamObject::from_object(object);

        cmd.print_stream_object(&mut buf, bucket, &stream_obj)?;
        let out = std::str::from_utf8(&buf)?;

        println!("Output with owner, no date, no storage: {}", out);
        assert!(out.contains("abc123456789"));
        assert!(out.contains("test-owner"));
        assert!(out.contains("1234567"));
        assert!(out.contains("None"));
        assert!(out.contains("s3://test/some/test/file.txt"));
        assert!(out.contains("None"));
        Ok(())
    }

    #[test]
    fn test_fast_print_object() -> Result<(), Error> {
        let mut buf = Vec::new();
        let cmd = FastPrint {};
        let bucket = "test";

        let object = Object::builder()
            .e_tag("9d48114aa7c18f9d68aa20086dbb7756")
            .key("somepath/otherpath")
            .size(4_997_288)
            .storage_class(ObjectStorageClass::Standard)
            .last_modified(DateTime::from_str(
                "2017-07-19T19:04:17.000Z",
                Format::DateTime,
            )?)
            .build();
        let stream_obj = StreamObject::from_object(object);

        cmd.print_stream_object(&mut buf, bucket, &stream_obj)?;
        let out = std::str::from_utf8(&buf)?;

        assert!(out.contains("s3://test/somepath/otherpath"));
        Ok(())
    }

    #[test]
    fn test_exec() -> Result<(), Error> {
        let mut buf = Vec::new();
        let cmd = Exec {
            utility: "echo test {}".to_owned(),
        };

        let path = "s3://test/somepath/otherpath";
        cmd.exec(&mut buf, path)?;
        let out = std::str::from_utf8(&buf)?;

        assert!(out.contains("test"));
        assert!(out.contains("s3://test/somepath/otherpath"));
        Ok(())
    }

    #[test]
    fn test_advanced_print_json() -> Result<(), Error> {
        let object = Object::builder()
            .e_tag("9d48114aa7c18f9d68aa20086dbb7756")
            .key("somepath/otherpath")
            .size(4_997_288)
            .storage_class(ObjectStorageClass::Standard)
            .last_modified(DateTime::from_str(
                "2017-07-19T19:04:17.000Z",
                Format::DateTime,
            )?)
            .build();
        let stream_obj = StreamObject::from_object(object);

        let mut buf = Vec::<u8>::new();
        let cmd = AdvancedPrint {
            format: PrintFormat::Json,
        };

        cmd.print_json_stream_object(&mut buf, &stream_obj)?;

        let output = String::from_utf8(buf).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&output)?;

        assert_eq!(
            parsed["e_tag"].as_str().unwrap(),
            "9d48114aa7c18f9d68aa20086dbb7756"
        );
        assert_eq!(parsed["key"].as_str().unwrap(), "somepath/otherpath");
        assert_eq!(parsed["size"].as_i64().unwrap(), 4_997_288);
        assert_eq!(parsed["storage_class"].as_str().unwrap(), "STANDARD");
        assert_eq!(
            parsed["last_modified"].as_str().unwrap(),
            "2017-07-19T19:04:17Z"
        );
        assert_eq!(parsed["owner"].as_str().unwrap(), "");

        Ok(())
    }

    #[test]
    fn test_advanced_print_csv() -> Result<(), Error> {
        let object1 = Object::builder()
            .e_tag("9d48114aa7c18f9d68aa20086dbb7756")
            .key("somepath/otherpath")
            .size(4_997_288)
            .storage_class(ObjectStorageClass::Standard)
            .last_modified(DateTime::from_str(
                "2017-07-19T19:04:17.000Z",
                Format::DateTime,
            )?)
            .build();
        let stream_obj1 = StreamObject::from_object(object1);

        let object2 = Object::builder()
            .e_tag("abcdef1234567890")
            .key("another/path")
            .size(1024)
            .storage_class(ObjectStorageClass::ReducedRedundancy)
            .last_modified(DateTime::from_str(
                "2020-01-01T12:30:45.000Z",
                Format::DateTime,
            )?)
            .build();
        let stream_obj2 = StreamObject::from_object(object2);

        let mut buf = Vec::<u8>::new();
        let cmd = AdvancedPrint {
            format: PrintFormat::Csv,
        };

        cmd.print_csv_stream_objects(&mut buf, &[stream_obj1.clone(), stream_obj2.clone()])?;

        let output = String::from_utf8(buf.clone()).unwrap();
        println!("CSV Output:\n{}", output);

        let record1: ObjectRecord = (&stream_obj1).into();
        let record2: ObjectRecord = (&stream_obj2).into();

        let mut reader = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(&*buf);

        let records: Vec<ObjectRecord> = reader
            .deserialize()
            .collect::<Result<Vec<ObjectRecord>, _>>()?;

        assert_eq!(records.len(), 2, "Should have exactly two records");

        assert_eq!(records[0].e_tag, record1.e_tag);
        assert_eq!(records[0].key, record1.key);
        assert_eq!(records[0].size, record1.size);
        assert_eq!(records[0].storage_class, record1.storage_class);
        assert_eq!(records[0].last_modified, record1.last_modified);

        assert_eq!(records[1].e_tag, record2.e_tag);
        assert_eq!(records[1].key, record2.key);
        assert_eq!(records[1].size, record2.size);
        assert_eq!(records[1].storage_class, record2.storage_class);
        assert_eq!(records[1].last_modified, record2.last_modified);

        Ok(())
    }

    #[tokio::test]
    async fn test_advanced_print() -> Result<(), Error> {
        let object = Object::builder()
            .e_tag("9d48114aa7c18f9d68aa20086dbb7756")
            .key("somepath/otherpath")
            .size(4_997_288)
            .storage_class(ObjectStorageClass::Standard)
            .last_modified(DateTime::from_str(
                "2017-07-19T19:04:17.000Z",
                Format::DateTime,
            )?)
            .build();
        let stream_obj = StreamObject::from_object(object);

        let cmd = Cmd::Print(AdvancedPrint::default()).downcast();
        let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
        let client = Client::new(&config);

        let path = S3Path {
            bucket: "test".to_owned(),
            prefix: None,
        };

        cmd.execute(&client, &path, &[stream_obj]).await?;
        Ok(())
    }

    #[tokio::test]
    async fn smoke_advanced_print_json_execute() -> Result<(), Error> {
        let object1 = Object::builder()
            .e_tag("test-etag-1")
            .key("folder/file1.txt")
            .size(100)
            .storage_class(ObjectStorageClass::Standard)
            .last_modified(
                DateTime::from_str("2023-01-01T00:00:00.000Z", Format::DateTime).unwrap(),
            )
            .build();
        let stream_obj1 = StreamObject::from_object(object1);

        let object2 = Object::builder()
            .e_tag("test-etag-2")
            .key("folder/file2.txt")
            .size(200)
            .storage_class(ObjectStorageClass::IntelligentTiering)
            .last_modified(
                DateTime::from_str("2023-02-15T12:30:45.000Z", Format::DateTime).unwrap(),
            )
            .build();
        let stream_obj2 = StreamObject::from_object(object2);

        let cmd = Cmd::Print(AdvancedPrint {
            format: PrintFormat::Json,
        })
        .downcast();

        let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
        let client = Client::new(&config);
        let path = S3Path {
            bucket: "test-bucket".to_owned(),
            prefix: None,
        };

        cmd.execute(&client, &path, &[stream_obj1, stream_obj2])
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn smoke_advanced_print_csv_execute() -> Result<(), Error> {
        let object1 = Object::builder()
            .e_tag("csv-etag-1")
            .key("data/report.csv")
            .size(5000)
            .storage_class(ObjectStorageClass::Standard)
            .last_modified(
                DateTime::from_str("2023-03-10T09:15:30.000Z", Format::DateTime).unwrap(),
            )
            .build();
        let stream_obj1 = StreamObject::from_object(object1);

        let object2 = Object::builder()
            .e_tag("csv-etag-2")
            .key("data/archive.zip")
            .size(10000)
            .storage_class(ObjectStorageClass::DeepArchive)
            .last_modified(
                DateTime::from_str("2023-04-20T14:25:10.000Z", Format::DateTime).unwrap(),
            )
            .build();
        let stream_obj2 = StreamObject::from_object(object2);

        let cmd = Cmd::Print(AdvancedPrint {
            format: PrintFormat::Csv,
        })
        .downcast();

        let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
        let client = Client::new(&config);
        let path = S3Path {
            bucket: "test-bucket".to_owned(),
            prefix: None,
        };

        cmd.execute(&client, &path, &[stream_obj1, stream_obj2])
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_fastprint() -> Result<(), Error> {
        let object = Object::builder()
            .e_tag("9d48114aa7c18f9d68aa20086dbb7756")
            .key("somepath/otherpath")
            .size(4_997_288)
            .storage_class(ObjectStorageClass::Standard)
            .last_modified(DateTime::from_str(
                "2017-07-19T19:04:17.000Z",
                Format::DateTime,
            )?)
            .build();
        let stream_obj = StreamObject::from_object(object);

        let cmd = Cmd::Ls(FastPrint {}).downcast();
        let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
        let client = Client::new(&config);

        let path = S3Path {
            bucket: "test".to_owned(),
            prefix: None,
        };

        cmd.execute(&client, &path, &[stream_obj]).await?;
        Ok(())
    }

    #[tokio::test]
    async fn smoke_donothing() -> Result<(), Error> {
        let object = Object::builder()
            .e_tag("9d48114aa7c18f9d68aa20086dbb7756")
            .key("somepath/otherpath")
            .size(4_997_288)
            .storage_class(ObjectStorageClass::Standard)
            .last_modified(DateTime::from_str(
                "2017-07-19T19:04:17.000Z",
                Format::DateTime,
            )?)
            .build();
        let stream_obj = StreamObject::from_object(object);

        let cmd = Cmd::Nothing(DoNothing {}).downcast();
        let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
        let client = Client::new(&config);

        let path = S3Path {
            bucket: "test".to_owned(),
            prefix: None,
        };

        cmd.execute(&client, &path, &[stream_obj]).await
    }

    #[tokio::test]
    async fn smoke_exec() -> Result<(), Error> {
        let object = Object::builder()
            .e_tag("9d48114aa7c18f9d68aa20086dbb7756")
            .key("somepath/otherpath")
            .size(4_997_288)
            .storage_class(ObjectStorageClass::Standard)
            .last_modified(DateTime::from_str(
                "2017-07-19T19:04:17.000Z",
                Format::DateTime,
            )?)
            .build();
        let stream_obj = StreamObject::from_object(object);

        let cmd = Cmd::Exec(Exec {
            utility: "echo {}".to_owned(),
        })
        .downcast();

        let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
        let client = Client::new(&config);

        let path = S3Path {
            bucket: "test".to_owned(),
            prefix: None,
        };

        cmd.execute(&client, &path, &[stream_obj]).await
    }

    #[test]
    fn test_generate_s3_url() {
        assert_eq!(
            &generate_s3_url("us-east-1", "test-bucket", "somepath/somekey"),
            "https://test-bucket.s3.amazonaws.com/somepath/somekey",
        );
        assert_eq!(
            &generate_s3_url("eu-west-1", "test-bucket", "somepath/somekey"),
            "https://test-bucket.s3-eu-west-1.amazonaws.com/somepath/somekey",
        );
    }

    fn make_s3_test_credentials() -> aws_sdk_s3::config::Credentials {
        aws_sdk_s3::config::Credentials::new(
            "ATESTCLIENT",
            "astestsecretkey",
            Some("atestsessiontoken".to_string()),
            None,
            "",
        )
    }

    #[tokio::test]
    async fn test_download_with_replay_client() -> Result<(), Error> {
        let temp_dir = tempdir()?;
        let temp_path = temp_dir.path().to_string_lossy().to_string();

        let key = "test/file.txt";
        let object = Object::builder()
            .e_tag("test-etag")
            .key(key)
            .size(14) // Size of "Test content\n"
            .storage_class(ObjectStorageClass::Standard)
            .last_modified(
                DateTime::from_str("2023-01-01T00:00:00.000Z", Format::DateTime).unwrap(),
            )
            .build();
        let stream_obj = StreamObject::from_object(object);

        let content = "Test content\n";

        let req = http::Request::builder()
            .method("GET")
            .uri("https://test-bucket.s3.amazonaws.com/test/file.txt")
            .body(SdkBody::empty())
            .unwrap();

        let resp = http::Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", HeaderValue::from_static("text/plain"))
            .header("ETag", HeaderValue::from_static("\"test-etag\""))
            .header("Content-Length", HeaderValue::from_static("13"))
            .body(SdkBody::from(content))
            .unwrap();

        let events = vec![ReplayEvent::new(req, resp)];

        let replay_client = StaticReplayClient::new(events);

        let client: aws_sdk_s3::Client = aws_sdk_s3::Client::from_conf(
            aws_sdk_s3::Config::builder()
                .behavior_version(BehaviorVersion::latest())
                .credentials_provider(make_s3_test_credentials())
                .region(aws_sdk_s3::config::Region::new("us-east-1"))
                .http_client(replay_client.clone())
                .build(),
        );

        let cmd = Cmd::Download(Download {
            destination: temp_path.clone(),
            force: true,
        })
        .downcast();

        let path = S3Path {
            bucket: "test-bucket".to_owned(),
            prefix: None,
        };

        cmd.execute(&client, &path, &[stream_obj]).await?;

        let file_path = PathBuf::from(temp_path).join(key);
        assert!(file_path.exists(), "Downloaded file should exist");

        let downloaded_content = fs::read_to_string(&file_path)?;
        assert_eq!(downloaded_content, "Test content\n");

        Ok(())
    }

    #[tokio::test]
    async fn test_set_public_with_replay_client() -> Result<(), Error> {
        let key1 = "test/file1.txt";
        let key2 = "test/file2.txt";

        let object1 = Object::builder()
            .e_tag("test-etag-1")
            .key(key1)
            .size(100)
            .storage_class(ObjectStorageClass::Standard)
            .last_modified(
                DateTime::from_str("2023-01-01T00:00:00.000Z", Format::DateTime).unwrap(),
            )
            .build();
        let stream_obj1 = StreamObject::from_object(object1);

        let object2 = Object::builder()
            .e_tag("test-etag-2")
            .key(key2)
            .size(200)
            .storage_class(ObjectStorageClass::Standard)
            .last_modified(
                DateTime::from_str("2023-01-01T00:00:00.000Z", Format::DateTime).unwrap(),
            )
            .build();
        let stream_obj2 = StreamObject::from_object(object2);

        let req1 = http::Request::builder()
            .method("PUT")
            .uri("https://test-bucket.s3.amazonaws.com/test/file1.txt?acl")
            .body(SdkBody::empty())
            .unwrap();

        let resp1 = http::Response::builder()
            .status(StatusCode::OK)
            .body(SdkBody::empty())
            .unwrap();

        let req2 = http::Request::builder()
            .method("PUT")
            .uri("https://test-bucket.s3.amazonaws.com/test/file2.txt?acl")
            .body(SdkBody::empty())
            .unwrap();

        let resp2 = http::Response::builder()
            .status(StatusCode::OK)
            .body(SdkBody::empty())
            .unwrap();

        let events = vec![ReplayEvent::new(req1, resp1), ReplayEvent::new(req2, resp2)];

        let replay_client = StaticReplayClient::new(events);

        let client: aws_sdk_s3::Client = aws_sdk_s3::Client::from_conf(
            aws_sdk_s3::Config::builder()
                .behavior_version(BehaviorVersion::latest())
                .credentials_provider(make_s3_test_credentials())
                .region(aws_sdk_s3::config::Region::new("us-east-1"))
                .http_client(replay_client.clone())
                .build(),
        );

        let cmd = Cmd::Public(SetPublic {}).downcast();

        let path = S3Path {
            bucket: "test-bucket".to_owned(),
            prefix: None,
        };

        cmd.execute(&client, &path, &[stream_obj1, stream_obj2])
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_delete_with_replay_client() -> Result<(), Error> {
        let key1 = "test/file1.txt";
        let key2 = "test/file2.txt";

        let object1 = Object::builder()
            .e_tag("test-etag-1")
            .key(key1)
            .size(100)
            .storage_class(ObjectStorageClass::Standard)
            .last_modified(
                DateTime::from_str("2023-01-01T00:00:00.000Z", Format::DateTime).unwrap(),
            )
            .build();

        let object2 = Object::builder()
            .e_tag("test-etag-2")
            .key(key2)
            .size(200)
            .storage_class(ObjectStorageClass::Standard)
            .last_modified(
                DateTime::from_str("2023-01-01T00:00:00.000Z", Format::DateTime).unwrap(),
            )
            .build();

        let req = http::Request::builder()
            .method("POST")
            .uri("https://test-bucket.s3.amazonaws.com/?delete")
            .body(SdkBody::empty()) // In a real request this would contain XML with keys to delete
            .unwrap();

        let resp_body = r#"<?xml version="1.0" encoding="UTF-8"?>
        <DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Deleted>
                <Key>test/file1.txt</Key>
            </Deleted>
            <Deleted>
                <Key>test/file2.txt</Key>
            </Deleted>
        </DeleteResult>"#;

        let resp = http::Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", HeaderValue::from_static("application/xml"))
            .body(SdkBody::from(resp_body))
            .unwrap();

        let events = vec![ReplayEvent::new(req, resp)];

        let replay_client = StaticReplayClient::new(events);

        let client: aws_sdk_s3::Client = aws_sdk_s3::Client::from_conf(
            aws_sdk_s3::Config::builder()
                .behavior_version(BehaviorVersion::latest())
                .credentials_provider(make_s3_test_credentials())
                .region(aws_sdk_s3::config::Region::new("us-east-1"))
                .http_client(replay_client.clone())
                .build(),
        );

        let cmd = Cmd::Delete(MultipleDelete {}).downcast();

        let path = S3Path {
            bucket: "test-bucket".to_owned(),
            prefix: None,
        };

        let stream_obj1 = StreamObject::from_object(object1);
        let stream_obj2 = StreamObject::from_object(object2);
        cmd.execute(&client, &path, &[stream_obj1, stream_obj2])
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_set_tags_with_replay_client() -> Result<(), Error> {
        let key1 = "test/file1.txt";
        let key2 = "test/file2.txt";

        let object1 = Object::builder()
            .e_tag("test-etag-1")
            .key(key1)
            .size(100)
            .storage_class(ObjectStorageClass::Standard)
            .last_modified(
                DateTime::from_str("2023-01-01T00:00:00.000Z", Format::DateTime).unwrap(),
            )
            .build();

        let object2 = Object::builder()
            .e_tag("test-etag-2")
            .key(key2)
            .size(200)
            .storage_class(ObjectStorageClass::Standard)
            .last_modified(
                DateTime::from_str("2023-01-01T00:00:00.000Z", Format::DateTime).unwrap(),
            )
            .build();

        let req1 = http::Request::builder()
            .method("PUT")
            .uri("https://test-bucket.s3.amazonaws.com/test/file1.txt?tagging")
            .body(SdkBody::empty()) // In a real request this would contain XML with tags
            .unwrap();

        let resp1 = http::Response::builder()
            .status(StatusCode::OK)
            .body(SdkBody::empty())
            .unwrap();

        let req2 = http::Request::builder()
            .method("PUT")
            .uri("https://test-bucket.s3.amazonaws.com/test/file2.txt?tagging")
            .body(SdkBody::empty())
            .unwrap();

        let resp2 = http::Response::builder()
            .status(StatusCode::OK)
            .body(SdkBody::empty())
            .unwrap();

        let events = vec![ReplayEvent::new(req1, resp1), ReplayEvent::new(req2, resp2)];

        let replay_client = StaticReplayClient::new(events);

        let client: aws_sdk_s3::Client = aws_sdk_s3::Client::from_conf(
            aws_sdk_s3::Config::builder()
                .behavior_version(BehaviorVersion::latest())
                .credentials_provider(make_s3_test_credentials())
                .region(aws_sdk_s3::config::Region::new("us-east-1"))
                .http_client(replay_client.clone())
                .build(),
        );

        let tags = vec![
            FindTag {
                key: "tag1".to_owned(),
                value: "value1".to_owned(),
            },
            FindTag {
                key: "tag2".to_owned(),
                value: "value2".to_owned(),
            },
        ];

        let cmd = Cmd::Tags(SetTags { tags }).downcast();

        let path = S3Path {
            bucket: "test-bucket".to_owned(),
            prefix: None,
        };

        let stream_obj1 = StreamObject::from_object(object1);
        let stream_obj2 = StreamObject::from_object(object2);
        cmd.execute(&client, &path, &[stream_obj1, stream_obj2])
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_list_tags_with_replay_client() -> Result<(), Error> {
        let key1 = "test/file1.txt";
        let key2 = "test/file2.txt";

        let object1 = Object::builder()
            .e_tag("test-etag-1")
            .key(key1)
            .size(100)
            .storage_class(ObjectStorageClass::Standard)
            .last_modified(
                DateTime::from_str("2023-01-01T00:00:00.000Z", Format::DateTime).unwrap(),
            )
            .build();

        let object2 = Object::builder()
            .e_tag("test-etag-2")
            .key(key2)
            .size(200)
            .storage_class(ObjectStorageClass::Standard)
            .last_modified(
                DateTime::from_str("2023-01-01T00:00:00.000Z", Format::DateTime).unwrap(),
            )
            .build();

        let req1 = http::Request::builder()
            .method("GET")
            .uri("https://test-bucket.s3.amazonaws.com/test/file1.txt?tagging")
            .body(SdkBody::empty())
            .unwrap();

        let resp_body1 = r#"<?xml version="1.0" encoding="UTF-8"?>
        <Tagging xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <TagSet>
                <Tag>
                    <Key>key1</Key>
                    <Value>value1</Value>
                </Tag>
                <Tag>
                    <Key>key2</Key>
                    <Value>value2</Value>
                </Tag>
            </TagSet>
        </Tagging>"#;

        let resp1 = http::Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", HeaderValue::from_static("application/xml"))
            .body(SdkBody::from(resp_body1))
            .unwrap();

        let req2 = http::Request::builder()
            .method("GET")
            .uri("https://test-bucket.s3.amazonaws.com/test/file2.txt?tagging")
            .body(SdkBody::empty())
            .unwrap();

        let resp_body2 = r#"<?xml version="1.0" encoding="UTF-8"?>
        <Tagging xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <TagSet>
                <Tag>
                    <Key>category</Key>
                    <Value>documents</Value>
                </Tag>
            </TagSet>
        </Tagging>"#;

        let resp2 = http::Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", HeaderValue::from_static("application/xml"))
            .body(SdkBody::from(resp_body2))
            .unwrap();

        let events = vec![ReplayEvent::new(req1, resp1), ReplayEvent::new(req2, resp2)];

        let replay_client = StaticReplayClient::new(events);

        let client: aws_sdk_s3::Client = aws_sdk_s3::Client::from_conf(
            aws_sdk_s3::Config::builder()
                .behavior_version(BehaviorVersion::latest())
                .credentials_provider(make_s3_test_credentials())
                .region(aws_sdk_s3::config::Region::new("us-east-1"))
                .http_client(replay_client.clone())
                .build(),
        );

        let cmd = Cmd::LsTags(ListTags {}).downcast();

        let path = S3Path {
            bucket: "test-bucket".to_owned(),
            prefix: None,
        };

        let stream_obj1 = StreamObject::from_object(object1);
        let stream_obj2 = StreamObject::from_object(object2);
        cmd.execute(&client, &path, &[stream_obj1, stream_obj2])
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_list_tags_with_cached_tags() -> Result<(), Error> {
        // Test that ListTags uses cached tags when available (no API calls needed)
        use aws_sdk_s3::types::Tag;

        let key1 = "test/file1.txt";
        let key2 = "test/file2.txt";

        let object1 = Object::builder()
            .e_tag("test-etag-1")
            .key(key1)
            .size(100)
            .storage_class(ObjectStorageClass::Standard)
            .last_modified(
                DateTime::from_str("2023-01-01T00:00:00.000Z", Format::DateTime).unwrap(),
            )
            .build();

        let object2 = Object::builder()
            .e_tag("test-etag-2")
            .key(key2)
            .size(200)
            .storage_class(ObjectStorageClass::Standard)
            .last_modified(
                DateTime::from_str("2023-01-01T00:00:00.000Z", Format::DateTime).unwrap(),
            )
            .build();

        // No HTTP events needed - tags are cached
        let replay_client = StaticReplayClient::new(vec![]);

        let client: aws_sdk_s3::Client = aws_sdk_s3::Client::from_conf(
            aws_sdk_s3::Config::builder()
                .behavior_version(BehaviorVersion::latest())
                .credentials_provider(make_s3_test_credentials())
                .region(aws_sdk_s3::config::Region::new("us-east-1"))
                .http_client(replay_client.clone())
                .build(),
        );

        let cmd = Cmd::LsTags(ListTags {}).downcast();

        let path = S3Path {
            bucket: "test-bucket".to_owned(),
            prefix: None,
        };

        // Create StreamObjects with pre-cached tags
        let mut stream_obj1 = StreamObject::from_object(object1);
        stream_obj1.tags = Some(vec![
            Tag::builder().key("env").value("prod").build().unwrap(),
            Tag::builder().key("team").value("data").build().unwrap(),
        ]);

        let mut stream_obj2 = StreamObject::from_object(object2);
        stream_obj2.tags = Some(vec![
            Tag::builder()
                .key("category")
                .value("documents")
                .build()
                .unwrap(),
        ]);

        // Execute - should use cached tags without making API calls
        cmd.execute(&client, &path, &[stream_obj1, stream_obj2])
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_s3_copy_with_replay_client() -> Result<(), Error> {
        let key1 = "test/file1.txt";
        let key2 = "test/file2.txt";

        let object1 = Object::builder()
            .e_tag("test-etag-1")
            .key(key1)
            .size(100)
            .storage_class(ObjectStorageClass::Standard)
            .last_modified(
                DateTime::from_str("2023-01-01T00:00:00.000Z", Format::DateTime).unwrap(),
            )
            .build();

        let object2 = Object::builder()
            .e_tag("test-etag-2")
            .key(key2)
            .size(200)
            .storage_class(ObjectStorageClass::Standard)
            .last_modified(
                DateTime::from_str("2023-01-01T00:00:00.000Z", Format::DateTime).unwrap(),
            )
            .build();

        let req1 = http::Request::builder()
            .method("PUT")
            .uri("https://test-bucket.s3.amazonaws.com/dest/file1.txt")
            .header("x-amz-copy-source", "test-bucket/test/file1.txt")
            .body(SdkBody::empty())
            .unwrap();

        let resp_body1 = r#"<?xml version="1.0" encoding="UTF-8"?>
        <CopyObjectResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <LastModified>2023-01-02T00:00:00Z</LastModified>
            <ETag>"new-etag-1"</ETag>
        </CopyObjectResult>"#;

        let resp1 = http::Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", HeaderValue::from_static("application/xml"))
            .body(SdkBody::from(resp_body1))
            .unwrap();

        let req2 = http::Request::builder()
            .method("PUT")
            .uri("https://test-bucket.s3.amazonaws.com/dest/file2.txt")
            .header("x-amz-copy-source", "test-bucket/test/file2.txt")
            .body(SdkBody::empty())
            .unwrap();

        let resp_body2 = r#"<?xml version="1.0" encoding="UTF-8"?>
        <CopyObjectResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <LastModified>2023-01-02T00:00:00Z</LastModified>
            <ETag>"new-etag-2"</ETag>
        </CopyObjectResult>"#;

        let resp2 = http::Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", HeaderValue::from_static("application/xml"))
            .body(SdkBody::from(resp_body2))
            .unwrap();

        let events = vec![ReplayEvent::new(req1, resp1), ReplayEvent::new(req2, resp2)];

        let replay_client = StaticReplayClient::new(events);

        let client: aws_sdk_s3::Client = aws_sdk_s3::Client::from_conf(
            aws_sdk_s3::Config::builder()
                .behavior_version(BehaviorVersion::latest())
                .credentials_provider(make_s3_test_credentials())
                .region(aws_sdk_s3::config::Region::new("us-east-1"))
                .http_client(replay_client.clone())
                .build(),
        );

        let destination = S3Path {
            bucket: "test-bucket".to_owned(),
            prefix: Some("dest/".to_owned()),
        };

        let cmd = Cmd::Copy(S3Copy {
            destination,
            flat: true,
            storage_class: None,
        })
        .downcast();

        let path = S3Path {
            bucket: "test-bucket".to_owned(),
            prefix: None,
        };

        let stream_obj1 = StreamObject::from_object(object1);
        let stream_obj2 = StreamObject::from_object(object2);
        cmd.execute(&client, &path, &[stream_obj1, stream_obj2])
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_s3_copy_with_storage_class() -> Result<(), Error> {
        let key = "test/file_to_copy.txt";

        let object = Object::builder()
            .e_tag("test-etag")
            .key(key)
            .size(1000)
            .storage_class(ObjectStorageClass::Standard)
            .last_modified(
                DateTime::from_str("2023-01-01T00:00:00.000Z", Format::DateTime).unwrap(),
            )
            .build();

        let req = http::Request::builder()
            .method("PUT")
            .uri("https://test-bucket.s3.amazonaws.com/archive/file_to_copy.txt")
            .header("x-amz-copy-source", "test-bucket/test/file_to_copy.txt")
            .header("x-amz-storage-class", "GLACIER")
            .body(SdkBody::empty())
            .unwrap();

        let resp_body = r#"<?xml version="1.0" encoding="UTF-8"?>
        <CopyObjectResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <LastModified>2023-01-02T00:00:00Z</LastModified>
            <ETag>"new-etag"</ETag>
        </CopyObjectResult>"#;

        let resp = http::Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", HeaderValue::from_static("application/xml"))
            .body(SdkBody::from(resp_body))
            .unwrap();

        let events = vec![ReplayEvent::new(req, resp)];
        let replay_client = StaticReplayClient::new(events);

        let client: aws_sdk_s3::Client = aws_sdk_s3::Client::from_conf(
            aws_sdk_s3::Config::builder()
                .behavior_version(BehaviorVersion::latest())
                .credentials_provider(make_s3_test_credentials())
                .region(aws_sdk_s3::config::Region::new("us-east-1"))
                .http_client(replay_client.clone())
                .build(),
        );

        let destination = S3Path {
            bucket: "test-bucket".to_owned(),
            prefix: Some("archive/".to_owned()),
        };

        let cmd = Cmd::Copy(S3Copy {
            destination,
            flat: true,
            storage_class: Some(StorageClass::Glacier),
        })
        .downcast();

        let path = S3Path {
            bucket: "test-bucket".to_owned(),
            prefix: None,
        };

        let stream_obj = StreamObject::from_object(object);
        cmd.execute(&client, &path, &[stream_obj]).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_s3_move_with_storage_class() -> Result<(), Error> {
        let key = "test/file_to_move.txt";

        let object = Object::builder()
            .e_tag("test-etag")
            .key(key)
            .size(1000)
            .storage_class(ObjectStorageClass::Standard)
            .last_modified(
                DateTime::from_str("2023-01-01T00:00:00.000Z", Format::DateTime).unwrap(),
            )
            .build();

        let req_copy = http::Request::builder()
            .method("PUT")
            .uri("https://test-bucket.s3.amazonaws.com/archive/file_to_move.txt")
            .header("x-amz-copy-source", "test-bucket/test/file_to_move.txt")
            .header("x-amz-storage-class", "INTELLIGENT_TIERING")
            .body(SdkBody::empty())
            .unwrap();

        let resp_copy_body = r#"<?xml version="1.0" encoding="UTF-8"?>
        <CopyObjectResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <LastModified>2023-01-02T00:00:00Z</LastModified>
            <ETag>"new-etag"</ETag>
        </CopyObjectResult>"#;

        let resp_copy = http::Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", HeaderValue::from_static("application/xml"))
            .body(SdkBody::from(resp_copy_body))
            .unwrap();

        let req_delete = http::Request::builder()
            .method("POST")
            .uri("https://test-bucket.s3.amazonaws.com/?delete")
            .body(SdkBody::empty())
            .unwrap();

        let resp_delete_body = r#"<?xml version="1.0" encoding="UTF-8"?>
        <DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Deleted>
                <Key>test/file_to_move.txt</Key>
            </Deleted>
        </DeleteResult>"#;

        let resp_delete = http::Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", HeaderValue::from_static("application/xml"))
            .body(SdkBody::from(resp_delete_body))
            .unwrap();

        let events = vec![
            ReplayEvent::new(req_copy, resp_copy),
            ReplayEvent::new(req_delete, resp_delete),
        ];

        let replay_client = StaticReplayClient::new(events);

        let client: aws_sdk_s3::Client = aws_sdk_s3::Client::from_conf(
            aws_sdk_s3::Config::builder()
                .behavior_version(BehaviorVersion::latest())
                .credentials_provider(make_s3_test_credentials())
                .region(aws_sdk_s3::config::Region::new("us-east-1"))
                .http_client(replay_client.clone())
                .build(),
        );

        let destination = S3Path {
            bucket: "test-bucket".to_owned(),
            prefix: Some("archive/".to_owned()),
        };

        let cmd = Cmd::Move(S3Move {
            destination,
            flat: true,
            storage_class: Some(StorageClass::IntelligentTiering),
        })
        .downcast();

        let path = S3Path {
            bucket: "test-bucket".to_owned(),
            prefix: None,
        };

        let stream_obj = StreamObject::from_object(object);
        cmd.execute(&client, &path, &[stream_obj]).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_s3_move_with_replay_client() -> Result<(), Error> {
        let key1 = "test/file1.txt";
        let key2 = "test/file2.txt";

        let object1 = Object::builder()
            .e_tag("test-etag-1")
            .key(key1)
            .size(100)
            .storage_class(ObjectStorageClass::Standard)
            .last_modified(
                DateTime::from_str("2023-01-01T00:00:00.000Z", Format::DateTime).unwrap(),
            )
            .build();

        let object2 = Object::builder()
            .e_tag("test-etag-2")
            .key(key2)
            .size(200)
            .storage_class(ObjectStorageClass::Standard)
            .last_modified(
                DateTime::from_str("2023-01-01T00:00:00.000Z", Format::DateTime).unwrap(),
            )
            .build();

        let req_copy1 = http::Request::builder()
            .method("PUT")
            .uri("https://test-bucket.s3.amazonaws.com/dest/file1.txt")
            .header("x-amz-copy-source", "test-bucket/test/file1.txt")
            .body(SdkBody::empty())
            .unwrap();

        let resp_copy_body1 = r#"<?xml version="1.0" encoding="UTF-8"?>
        <CopyObjectResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <LastModified>2023-01-02T00:00:00Z</LastModified>
            <ETag>"new-etag-1"</ETag>
        </CopyObjectResult>"#;

        let resp_copy1 = http::Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", HeaderValue::from_static("application/xml"))
            .body(SdkBody::from(resp_copy_body1))
            .unwrap();

        let req_copy2 = http::Request::builder()
            .method("PUT")
            .uri("https://test-bucket.s3.amazonaws.com/dest/file2.txt")
            .header("x-amz-copy-source", "test-bucket/test/file2.txt")
            .body(SdkBody::empty())
            .unwrap();

        let resp_copy_body2 = r#"<?xml version="1.0" encoding="UTF-8"?>
        <CopyObjectResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <LastModified>2023-01-02T00:00:00Z</LastModified>
            <ETag>"new-etag-2"</ETag>
        </CopyObjectResult>"#;

        let resp_copy2 = http::Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", HeaderValue::from_static("application/xml"))
            .body(SdkBody::from(resp_copy_body2))
            .unwrap();

        let req_delete = http::Request::builder()
            .method("POST")
            .uri("https://test-bucket.s3.amazonaws.com/?delete")
            .body(SdkBody::empty())
            .unwrap();

        let resp_delete_body = r#"<?xml version="1.0" encoding="UTF-8"?>
        <DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Deleted>
                <Key>test/file1.txt</Key>
            </Deleted>
            <Deleted>
                <Key>test/file2.txt</Key>
            </Deleted>
        </DeleteResult>"#;

        let resp_delete = http::Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", HeaderValue::from_static("application/xml"))
            .body(SdkBody::from(resp_delete_body))
            .unwrap();

        let events = vec![
            ReplayEvent::new(req_copy1, resp_copy1),
            ReplayEvent::new(req_copy2, resp_copy2),
            ReplayEvent::new(req_delete, resp_delete),
        ];

        let replay_client = StaticReplayClient::new(events);

        let client: aws_sdk_s3::Client = aws_sdk_s3::Client::from_conf(
            aws_sdk_s3::Config::builder()
                .behavior_version(BehaviorVersion::latest())
                .credentials_provider(make_s3_test_credentials())
                .region(aws_sdk_s3::config::Region::new("us-east-1"))
                .http_client(replay_client.clone())
                .build(),
        );

        let destination = S3Path {
            bucket: "test-bucket".to_owned(),
            prefix: Some("dest/".to_owned()),
        };

        let cmd = Cmd::Move(S3Move {
            destination,
            flat: true,
            storage_class: None,
        })
        .downcast();

        let path = S3Path {
            bucket: "test-bucket".to_owned(),
            prefix: None,
        };

        let stream_obj1 = StreamObject::from_object(object1);
        let stream_obj2 = StreamObject::from_object(object2);
        cmd.execute(&client, &path, &[stream_obj1, stream_obj2])
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_restore_with_replay_client() -> Result<(), Error> {
        let key1 = "test/archive1.dat";
        let key2 = "test/archive2.dat";
        let key3 = "test/already_in_progress.dat";
        let key4 = "test/invalid_state.dat";

        let glacier_object = Object::builder()
            .e_tag("glacier-etag-1")
            .key(key1)
            .size(5000)
            .storage_class(ObjectStorageClass::Glacier)
            .last_modified(
                DateTime::from_str("2023-03-10T09:15:30.000Z", Format::DateTime).unwrap(),
            )
            .build();

        let deeparchive_object = Object::builder()
            .e_tag("glacier-etag-2")
            .key(key2)
            .size(10000)
            .storage_class(ObjectStorageClass::DeepArchive)
            .last_modified(
                DateTime::from_str("2023-04-20T14:25:10.000Z", Format::DateTime).unwrap(),
            )
            .build();

        let restored_object = Object::builder()
            .e_tag("glacier-etag-3")
            .key(key3)
            .size(15000)
            .storage_class(ObjectStorageClass::Glacier)
            .last_modified(
                DateTime::from_str("2023-05-15T11:30:20.000Z", Format::DateTime).unwrap(),
            )
            .build();

        let invalid_state_object = Object::builder()
            .e_tag("glacier-etag-4")
            .key(key4)
            .size(20000)
            .storage_class(ObjectStorageClass::Glacier)
            .last_modified(
                DateTime::from_str("2023-06-25T16:45:30.000Z", Format::DateTime).unwrap(),
            )
            .build();

        let glacier_request = http::Request::builder()
            .method("POST")
            .uri("https://test-bucket.s3.amazonaws.com/test/archive1.dat?restore")
            .body(SdkBody::empty()) // In a real request this would contain XML with restore parameters
            .unwrap();

        let glacier_response = http::Response::builder()
            .status(StatusCode::OK)
            .body(SdkBody::empty())
            .unwrap();

        let deeparchive_request = http::Request::builder()
            .method("POST")
            .uri("https://test-bucket.s3.amazonaws.com/test/archive2.dat?restore")
            .body(SdkBody::empty())
            .unwrap();

        let deeparchive_response = http::Response::builder()
            .status(StatusCode::OK)
            .body(SdkBody::empty())
            .unwrap();

        let request_with_inprogress = http::Request::builder()
            .method("POST")
            .uri("https://test-bucket.s3.amazonaws.com/test/already_in_progress.dat?restore")
            .body(SdkBody::empty())
            .unwrap();

        let inprogress_response_body = r#"<?xml version="1.0" encoding="UTF-8"?>
        <Error>
            <Code>RestoreAlreadyInProgress</Code>
            <Message>Object restore is already in progress</Message>
            <Resource>/test-bucket/test/already_in_progress.dat</Resource>
            <RequestId>EXAMPLE123456789</RequestId>
        </Error>"#;

        let response_with_inprogress_error = http::Response::builder()
            .status(StatusCode::CONFLICT)
            .header("Content-Type", HeaderValue::from_static("application/xml"))
            .body(SdkBody::from(inprogress_response_body))
            .unwrap();

        let request_with_objectstate_error = http::Request::builder()
            .method("POST")
            .uri("https://test-bucket.s3.amazonaws.com/test/invalid_state.dat?restore")
            .body(SdkBody::empty())
            .unwrap();

        let invalid_objectstate_response_body = r#"<?xml version="1.0" encoding="UTF-8"?>
        <Error>
            <Code>InvalidObjectState</Code>
            <Message>The operation is not valid for the object's storage class</Message>
            <Resource>/test-bucket/test/invalid_state.dat</Resource>
            <RequestId>EXAMPLE987654321</RequestId>
        </Error>"#;

        let response_with_invalide_objectstate = http::Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .header("Content-Type", HeaderValue::from_static("application/xml"))
            .body(SdkBody::from(invalid_objectstate_response_body))
            .unwrap();

        let events = vec![
            ReplayEvent::new(glacier_request, glacier_response),
            ReplayEvent::new(deeparchive_request, deeparchive_response),
            ReplayEvent::new(request_with_inprogress, response_with_inprogress_error),
            ReplayEvent::new(
                request_with_objectstate_error,
                response_with_invalide_objectstate,
            ),
        ];

        let replay_client = StaticReplayClient::new(events);

        let client: aws_sdk_s3::Client = aws_sdk_s3::Client::from_conf(
            aws_sdk_s3::Config::builder()
                .behavior_version(BehaviorVersion::latest())
                .credentials_provider(make_s3_test_credentials())
                .region(aws_sdk_s3::config::Region::new("us-east-1"))
                .http_client(replay_client.clone())
                .build(),
        );

        let cmd = Cmd::Restore(Restore {
            days: 7,
            tier: aws_sdk_s3::types::Tier::Standard,
        })
        .downcast();

        let path = S3Path {
            bucket: "test-bucket".to_owned(),
            prefix: None,
        };

        cmd.execute(
            &client,
            &path,
            &[
                StreamObject::from_object(glacier_object),
                StreamObject::from_object(deeparchive_object),
                StreamObject::from_object(restored_object),
                StreamObject::from_object(invalid_state_object),
            ],
        )
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_restore_with_standard_tier() -> Result<(), Error> {
        let key = "test/standard_retrieval.dat";

        let glacier_object = Object::builder()
            .e_tag("glacier-etag")
            .key(key)
            .size(5000)
            .storage_class(ObjectStorageClass::Glacier)
            .last_modified(
                DateTime::from_str("2023-03-10T09:15:30.000Z", Format::DateTime).unwrap(),
            )
            .build();

        let request_successful = http::Request::builder()
            .method("POST")
            .uri("https://test-bucket.s3.amazonaws.com/test/standard_retrieval.dat?restore")
            .body(SdkBody::empty())
            .unwrap();

        let resp = http::Response::builder()
            .status(StatusCode::OK)
            .body(SdkBody::empty())
            .unwrap();

        let events = vec![ReplayEvent::new(request_successful, resp)];
        let replay_client = StaticReplayClient::new(events);

        let client: aws_sdk_s3::Client = aws_sdk_s3::Client::from_conf(
            aws_sdk_s3::Config::builder()
                .behavior_version(BehaviorVersion::latest())
                .credentials_provider(make_s3_test_credentials())
                .region(aws_sdk_s3::config::Region::new("us-east-1"))
                .http_client(replay_client.clone())
                .build(),
        );

        let restore_cmd = Restore {
            days: 7,
            tier: aws_sdk_s3::types::Tier::Standard,
        };

        let path = S3Path {
            bucket: "test-bucket".to_owned(),
            prefix: None,
        };

        let result = restore_cmd
            .execute(&client, &path, &[StreamObject::from_object(glacier_object)])
            .await;
        assert!(result.is_ok(), "Standard tier restore should succeed");

        Ok(())
    }

    #[tokio::test]
    async fn test_restore_with_expedited_tier() -> Result<(), Error> {
        let key = "test/expedited_retrieval.dat";

        let glacier_object = Object::builder()
            .e_tag("glacier-etag")
            .key(key)
            .size(5000)
            .storage_class(ObjectStorageClass::Glacier)
            .last_modified(
                DateTime::from_str("2023-03-10T09:15:30.000Z", Format::DateTime).unwrap(),
            )
            .build();

        let req = http::Request::builder()
            .method("POST")
            .uri("https://test-bucket.s3.amazonaws.com/test/expedited_retrieval.dat?restore")
            .body(SdkBody::empty())
            .unwrap();

        let resp = http::Response::builder()
            .status(StatusCode::OK)
            .body(SdkBody::empty())
            .unwrap();

        let events = vec![ReplayEvent::new(req, resp)];
        let replay_client = StaticReplayClient::new(events);

        let client: aws_sdk_s3::Client = aws_sdk_s3::Client::from_conf(
            aws_sdk_s3::Config::builder()
                .behavior_version(BehaviorVersion::latest())
                .credentials_provider(make_s3_test_credentials())
                .region(aws_sdk_s3::config::Region::new("us-east-1"))
                .http_client(replay_client.clone())
                .build(),
        );

        let restore_cmd = Restore {
            days: 2,
            tier: aws_sdk_s3::types::Tier::Expedited,
        };

        let path = S3Path {
            bucket: "test-bucket".to_owned(),
            prefix: None,
        };

        let result = restore_cmd
            .execute(&client, &path, &[StreamObject::from_object(glacier_object)])
            .await;
        assert!(result.is_ok(), "Expedited tier restore should succeed");

        Ok(())
    }

    #[tokio::test]
    async fn test_restore_with_bulk_tier() -> Result<(), Error> {
        let key = "test/bulk_retrieval.dat";

        let deep_archive_object = Object::builder()
            .e_tag("deep-archive-etag")
            .key(key)
            .size(5000)
            .storage_class(ObjectStorageClass::DeepArchive)
            .last_modified(
                DateTime::from_str("2023-03-10T09:15:30.000Z", Format::DateTime).unwrap(),
            )
            .build();

        let req = http::Request::builder()
            .method("POST")
            .uri("https://test-bucket.s3.amazonaws.com/test/bulk_retrieval.dat?restore")
            .body(SdkBody::empty()) // In a real request this would contain XML with Bulk tier
            .unwrap();

        let resp = http::Response::builder()
            .status(StatusCode::OK)
            .body(SdkBody::empty())
            .unwrap();

        let events = vec![ReplayEvent::new(req, resp)];
        let replay_client = StaticReplayClient::new(events);

        let client: aws_sdk_s3::Client = aws_sdk_s3::Client::from_conf(
            aws_sdk_s3::Config::builder()
                .behavior_version(BehaviorVersion::latest())
                .credentials_provider(make_s3_test_credentials())
                .region(aws_sdk_s3::config::Region::new("us-east-1"))
                .http_client(replay_client.clone())
                .build(),
        );

        let restore_cmd = Restore {
            days: 30,
            tier: aws_sdk_s3::types::Tier::Bulk,
        };

        let path = S3Path {
            bucket: "test-bucket".to_owned(),
            prefix: None,
        };

        let result = restore_cmd
            .execute(
                &client,
                &path,
                &[StreamObject::from_object(deep_archive_object)],
            )
            .await;
        assert!(result.is_ok(), "Bulk tier restore should succeed");

        Ok(())
    }

    #[tokio::test]
    async fn test_restore_with_non_glacier_objects() -> Result<(), Error> {
        let key = "test/standard_object.dat";

        let standard_object = Object::builder()
            .e_tag("standard-etag")
            .key(key)
            .size(5000)
            .storage_class(ObjectStorageClass::Standard) // Standard storage, not Glacier
            .last_modified(
                DateTime::from_str("2023-03-10T09:15:30.000Z", Format::DateTime).unwrap(),
            )
            .build();

        let events: Vec<ReplayEvent> = vec![];
        let replay_client = StaticReplayClient::new(events);

        let client: aws_sdk_s3::Client = aws_sdk_s3::Client::from_conf(
            aws_sdk_s3::Config::builder()
                .behavior_version(BehaviorVersion::latest())
                .credentials_provider(make_s3_test_credentials())
                .region(aws_sdk_s3::config::Region::new("us-east-1"))
                .http_client(replay_client.clone())
                .build(),
        );

        let restore_cmd = Restore {
            days: 7,
            tier: aws_sdk_s3::types::Tier::Standard,
        };

        let path = S3Path {
            bucket: "test-bucket".to_owned(),
            prefix: None,
        };

        let result = restore_cmd
            .execute(
                &client,
                &path,
                &[StreamObject::from_object(standard_object)],
            )
            .await;
        assert!(
            result.is_ok(),
            "Non-Glacier objects should be skipped without error"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_restore_with_maximum_days() -> Result<(), Error> {
        let key = "test/long_term_retrieval.dat";

        let glacier_object = Object::builder()
            .e_tag("glacier-etag")
            .key(key)
            .size(5000)
            .storage_class(ObjectStorageClass::Glacier)
            .last_modified(
                DateTime::from_str("2023-03-10T09:15:30.000Z", Format::DateTime).unwrap(),
            )
            .build();

        let req = http::Request::builder()
            .method("POST")
            .uri("https://test-bucket.s3.amazonaws.com/test/long_term_retrieval.dat?restore")
            .body(SdkBody::empty())
            .unwrap();

        let resp = http::Response::builder()
            .status(StatusCode::OK)
            .body(SdkBody::empty())
            .unwrap();

        let events = vec![ReplayEvent::new(req, resp)];
        let replay_client = StaticReplayClient::new(events);

        let client: aws_sdk_s3::Client = aws_sdk_s3::Client::from_conf(
            aws_sdk_s3::Config::builder()
                .behavior_version(BehaviorVersion::latest())
                .credentials_provider(make_s3_test_credentials())
                .region(aws_sdk_s3::config::Region::new("us-east-1"))
                .http_client(replay_client.clone())
                .build(),
        );

        let restore_cmd = Restore {
            days: 365,
            tier: aws_sdk_s3::types::Tier::Standard,
        };

        let path = S3Path {
            bucket: "test-bucket".to_owned(),
            prefix: None,
        };

        let result = restore_cmd
            .execute(&client, &path, &[StreamObject::from_object(glacier_object)])
            .await;
        assert!(result.is_ok(), "Restore with maximum days should succeed");

        Ok(())
    }

    #[tokio::test]
    async fn test_change_storage_class() -> Result<(), Error> {
        let key1 = "test/file1.txt";
        let key2 = "test/file2.txt";

        let object1 = Object::builder()
            .e_tag("test-etag-1")
            .key(key1)
            .size(100)
            .storage_class(ObjectStorageClass::Standard)
            .last_modified(
                DateTime::from_str("2023-01-01T00:00:00.000Z", Format::DateTime).unwrap(),
            )
            .build();

        let object2 = Object::builder()
            .e_tag("test-etag-2")
            .key(key2)
            .size(200)
            .storage_class(ObjectStorageClass::Standard)
            .last_modified(
                DateTime::from_str("2023-01-01T00:00:00.000Z", Format::DateTime).unwrap(),
            )
            .build();

        let req1 = http::Request::builder()
            .method("PUT")
            .uri("https://test-bucket.s3.amazonaws.com/test/file1.txt")
            .header("x-amz-copy-source", "test-bucket/test/file1.txt")
            .header("x-amz-metadata-directive", "COPY")
            .header("x-amz-storage-class", "GLACIER")
            .body(SdkBody::empty())
            .unwrap();

        let resp_body1 = r#"<?xml version="1.0" encoding="UTF-8"?>
        <CopyObjectResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <LastModified>2023-01-02T00:00:00Z</LastModified>
            <ETag>"new-etag-1"</ETag>
        </CopyObjectResult>"#;

        let resp1 = http::Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", HeaderValue::from_static("application/xml"))
            .body(SdkBody::from(resp_body1))
            .unwrap();

        let req2 = http::Request::builder()
            .method("PUT")
            .uri("https://test-bucket.s3.amazonaws.com/test/file2.txt")
            .header("x-amz-copy-source", "test-bucket/test/file2.txt")
            .header("x-amz-metadata-directive", "COPY")
            .header("x-amz-storage-class", "GLACIER")
            .body(SdkBody::empty())
            .unwrap();

        let resp_body2 = r#"<?xml version="1.0" encoding="UTF-8"?>
        <CopyObjectResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <LastModified>2023-01-02T00:00:00Z</LastModified>
            <ETag>"new-etag-2"</ETag>
        </CopyObjectResult>"#;

        let resp2 = http::Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", HeaderValue::from_static("application/xml"))
            .body(SdkBody::from(resp_body2))
            .unwrap();

        let events = vec![ReplayEvent::new(req1, resp1), ReplayEvent::new(req2, resp2)];
        let replay_client = StaticReplayClient::new(events);

        let client: aws_sdk_s3::Client = aws_sdk_s3::Client::from_conf(
            aws_sdk_s3::Config::builder()
                .behavior_version(BehaviorVersion::latest())
                .credentials_provider(make_s3_test_credentials())
                .region(aws_sdk_s3::config::Region::new("us-east-1"))
                .http_client(replay_client.clone())
                .build(),
        );

        let cmd = Cmd::ChangeStorage(ChangeStorage {
            storage_class: StorageClass::Glacier,
        })
        .downcast();

        let path = S3Path {
            bucket: "test-bucket".to_owned(),
            prefix: None,
        };

        let stream_obj1 = StreamObject::from_object(object1);
        let stream_obj2 = StreamObject::from_object(object2);
        cmd.execute(&client, &path, &[stream_obj1, stream_obj2])
            .await?;

        Ok(())
    }

    #[test]
    fn test_object_record_with_version_fields() {
        use aws_sdk_s3::types::ObjectVersion;

        // Create a versioned object using StreamObject::from_version
        let version = ObjectVersion::builder()
            .key("versioned-file.txt")
            .version_id("ver123")
            .is_latest(true)
            .size(500)
            .build();

        let stream_obj = StreamObject::from_version(version);
        let record: ObjectRecord = (&stream_obj).into();

        assert_eq!(record.key, "versioned-file.txt");
        assert_eq!(record.version_id, Some("ver123".to_string()));
        assert_eq!(record.is_latest, Some(true));
        assert_eq!(record.is_delete_marker, None);
    }

    #[test]
    fn test_object_record_with_delete_marker() {
        use aws_sdk_s3::types::DeleteMarkerEntry;

        let marker = DeleteMarkerEntry::builder()
            .key("deleted-file.txt")
            .version_id("del456")
            .is_latest(true)
            .build();

        let stream_obj = StreamObject::from_delete_marker(marker);
        let record: ObjectRecord = (&stream_obj).into();

        assert_eq!(record.key, "deleted-file.txt");
        assert_eq!(record.version_id, Some("del456".to_string()));
        assert_eq!(record.is_latest, Some(true));
        assert_eq!(record.is_delete_marker, Some(true));
    }

    #[test]
    fn test_advanced_print_json_with_version_fields() -> Result<(), Error> {
        use aws_sdk_s3::types::ObjectVersion;
        use aws_sdk_s3::types::ObjectVersionStorageClass;

        let version = ObjectVersion::builder()
            .e_tag("ver-etag")
            .key("data/versioned.txt")
            .version_id("v1234")
            .is_latest(false)
            .size(1024)
            .storage_class(ObjectVersionStorageClass::Standard)
            .last_modified(
                DateTime::from_str("2023-06-15T10:30:00.000Z", Format::DateTime).unwrap(),
            )
            .build();

        let stream_obj = StreamObject::from_version(version);

        let mut buf = Vec::<u8>::new();
        let cmd = AdvancedPrint {
            format: PrintFormat::Json,
        };

        cmd.print_json_stream_object(&mut buf, &stream_obj)?;

        let output = String::from_utf8(buf).unwrap();
        println!("JSON with version: {}", output);

        // Verify version fields are present in JSON output
        assert!(output.contains("\"version_id\":\"v1234\""));
        assert!(output.contains("\"is_latest\":false"));
        assert!(!output.contains("is_delete_marker")); // Should be omitted when false

        Ok(())
    }

    #[test]
    fn test_advanced_print_json_with_delete_marker() -> Result<(), Error> {
        use aws_sdk_s3::types::DeleteMarkerEntry;

        let marker = DeleteMarkerEntry::builder()
            .key("data/deleted.txt")
            .version_id("dm789")
            .is_latest(true)
            .last_modified(
                DateTime::from_str("2023-07-20T14:00:00.000Z", Format::DateTime).unwrap(),
            )
            .build();

        let stream_obj = StreamObject::from_delete_marker(marker);

        let mut buf = Vec::<u8>::new();
        let cmd = AdvancedPrint {
            format: PrintFormat::Json,
        };

        cmd.print_json_stream_object(&mut buf, &stream_obj)?;

        let output = String::from_utf8(buf).unwrap();
        println!("JSON with delete marker: {}", output);

        // Verify version and delete marker fields are present
        assert!(output.contains("\"version_id\":\"dm789\""));
        assert!(output.contains("\"is_latest\":true"));
        assert!(output.contains("\"is_delete_marker\":true"));

        Ok(())
    }

    #[tokio::test]
    async fn test_delete_versioned_object() -> Result<(), Error> {
        use aws_sdk_s3::types::ObjectVersion;

        let version = ObjectVersion::builder()
            .e_tag("ver-etag")
            .key("test/versioned-file.txt")
            .version_id("v123abc")
            .is_latest(true)
            .size(100)
            .build();

        let stream_obj = StreamObject::from_version(version);

        // The delete request should include versionId
        let req = http::Request::builder()
            .method("POST")
            .uri("https://test-bucket.s3.amazonaws.com/?delete")
            .body(SdkBody::empty())
            .unwrap();

        let resp_body = r#"<?xml version="1.0" encoding="UTF-8"?>
        <DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Deleted>
                <Key>test/versioned-file.txt</Key>
                <VersionId>v123abc</VersionId>
            </Deleted>
        </DeleteResult>"#;

        let resp = http::Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", HeaderValue::from_static("application/xml"))
            .body(SdkBody::from(resp_body))
            .unwrap();

        let events = vec![ReplayEvent::new(req, resp)];
        let replay_client = StaticReplayClient::new(events);

        let client: aws_sdk_s3::Client = aws_sdk_s3::Client::from_conf(
            aws_sdk_s3::Config::builder()
                .behavior_version(BehaviorVersion::latest())
                .credentials_provider(make_s3_test_credentials())
                .region(aws_sdk_s3::config::Region::new("us-east-1"))
                .http_client(replay_client.clone())
                .build(),
        );

        let cmd = Cmd::Delete(MultipleDelete {}).downcast();

        let path = S3Path {
            bucket: "test-bucket".to_owned(),
            prefix: None,
        };

        cmd.execute(&client, &path, &[stream_obj]).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_copy_versioned_object() -> Result<(), Error> {
        use aws_sdk_s3::types::ObjectVersion;

        let version = ObjectVersion::builder()
            .e_tag("ver-etag")
            .key("source/file.txt")
            .version_id("srcver123")
            .is_latest(true)
            .size(500)
            .build();

        let stream_obj = StreamObject::from_version(version);

        // Copy request should include versionId in x-amz-copy-source
        let req = http::Request::builder()
            .method("PUT")
            .uri("https://dest-bucket.s3.amazonaws.com/dest/file.txt")
            .header(
                "x-amz-copy-source",
                "source-bucket/source/file.txt?versionId=srcver123",
            )
            .body(SdkBody::empty())
            .unwrap();

        let resp_body = r#"<?xml version="1.0" encoding="UTF-8"?>
        <CopyObjectResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <LastModified>2023-01-02T00:00:00Z</LastModified>
            <ETag>"new-etag"</ETag>
        </CopyObjectResult>"#;

        let resp = http::Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", HeaderValue::from_static("application/xml"))
            .body(SdkBody::from(resp_body))
            .unwrap();

        let events = vec![ReplayEvent::new(req, resp)];
        let replay_client = StaticReplayClient::new(events);

        let client: aws_sdk_s3::Client = aws_sdk_s3::Client::from_conf(
            aws_sdk_s3::Config::builder()
                .behavior_version(BehaviorVersion::latest())
                .credentials_provider(make_s3_test_credentials())
                .region(aws_sdk_s3::config::Region::new("us-east-1"))
                .http_client(replay_client.clone())
                .build(),
        );

        let destination = S3Path {
            bucket: "dest-bucket".to_owned(),
            prefix: Some("dest/".to_owned()),
        };

        let cmd = Cmd::Copy(S3Copy {
            destination,
            flat: true,
            storage_class: None,
        })
        .downcast();

        let path = S3Path {
            bucket: "source-bucket".to_owned(),
            prefix: None,
        };

        cmd.execute(&client, &path, &[stream_obj]).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_download_versioned_object() -> Result<(), Error> {
        use aws_sdk_s3::types::ObjectVersion;

        let version = ObjectVersion::builder()
            .e_tag("ver-etag")
            .key("downloads/file.txt")
            .version_id("dlver456")
            .is_latest(false)
            .size(100)
            .build();

        let stream_obj = StreamObject::from_version(version);

        // Download request should include versionId parameter
        let req = http::Request::builder()
            .method("GET")
            .uri("https://test-bucket.s3.amazonaws.com/downloads/file.txt?versionId=dlver456")
            .body(SdkBody::empty())
            .unwrap();

        let resp = http::Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", HeaderValue::from_static("text/plain"))
            .header("Content-Length", HeaderValue::from_static("13"))
            .body(SdkBody::from("file contents"))
            .unwrap();

        let events = vec![ReplayEvent::new(req, resp)];
        let replay_client = StaticReplayClient::new(events);

        let client: aws_sdk_s3::Client = aws_sdk_s3::Client::from_conf(
            aws_sdk_s3::Config::builder()
                .behavior_version(BehaviorVersion::latest())
                .credentials_provider(make_s3_test_credentials())
                .region(aws_sdk_s3::config::Region::new("us-east-1"))
                .http_client(replay_client.clone())
                .build(),
        );

        // Create a temp directory for download
        let temp_dir = std::env::temp_dir().join("s3find_test_versioned_download");
        std::fs::create_dir_all(&temp_dir)?;

        let cmd = Cmd::Download(Download {
            destination: temp_dir.to_string_lossy().to_string(),
            force: true,
        })
        .downcast();

        let path = S3Path {
            bucket: "test-bucket".to_owned(),
            prefix: None,
        };

        cmd.execute(&client, &path, &[stream_obj]).await?;

        // Clean up
        let _ = std::fs::remove_dir_all(&temp_dir);

        Ok(())
    }
}
