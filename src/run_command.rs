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
use aws_sdk_s3::types::{Delete, Object, ObjectCannedAcl, ObjectIdentifier, Tag, Tagging};
use aws_sdk_s3::types::{GlacierJobParameters, ObjectStorageClass, RestoreRequest};
use aws_smithy_runtime_api::client::result::SdkError;
use aws_smithy_types::date_time::Format;

use crate::arg::*;
use crate::error::*;
use crate::utils::combine_keys;

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
    async fn execute(&self, client: &Client, path: &S3Path, list: &[Object]) -> Result<(), Error>;
}

impl FastPrint {
    #[inline]
    fn print_object<I: Write>(
        &self,
        io: &mut I,
        bucket: &str,
        object: &Object,
    ) -> std::io::Result<()> {
        writeln!(
            io,
            "s3://{}/{}",
            bucket,
            object.key.clone().unwrap_or_default()
        )
    }
}

#[async_trait]
impl RunCommand for FastPrint {
    async fn execute(&self, _c: &Client, path: &S3Path, list: &[Object]) -> Result<(), Error> {
        let mut stdout = std::io::stdout();
        for x in list {
            self.print_object(&mut stdout, &path.bucket, x)?
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
}

impl From<&Object> for ObjectRecord {
    fn from(object: &Object) -> Self {
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
        }
    }
}

impl AdvancedPrint {
    #[inline]
    fn print_object<I: Write>(
        &self,
        io: &mut I,
        bucket: &str,
        object: &Object,
    ) -> std::io::Result<()> {
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
            object.key.as_ref().unwrap_or(&"".to_string()),
            object
                .storage_class
                .as_ref()
                .map(|x| x.as_str())
                .unwrap_or("NONE"),
        )
    }

    #[inline]
    fn print_json_object<I: Write>(&self, io: &mut I, object: &Object) -> Result<(), Error> {
        let record: ObjectRecord = object.into();
        writeln!(io, "{}", serde_json::to_string(&record)?)?;
        Ok(())
    }

    #[inline]
    fn print_csv_objects<I: Write>(&self, io: &mut I, objects: &[Object]) -> Result<(), Error> {
        let mut wtr = WriterBuilder::new().has_headers(false).from_writer(io);
        for object in objects {
            let record: ObjectRecord = object.into();
            wtr.serialize(record)?;
        }
        wtr.flush()?;
        Ok(())
    }
}

#[async_trait]
impl RunCommand for AdvancedPrint {
    async fn execute(&self, _c: &Client, path: &S3Path, list: &[Object]) -> Result<(), Error> {
        let mut stdout = std::io::stdout();

        match self.format {
            PrintFormat::Json => {
                for x in list {
                    self.print_json_object(&mut stdout, x)?;
                }
            }
            PrintFormat::Text => {
                for x in list {
                    self.print_object(&mut stdout, &path.bucket, x)?;
                }
            }
            PrintFormat::Csv => {
                self.print_csv_objects(&mut stdout, list)?;
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
    async fn execute(&self, _: &Client, path: &S3Path, list: &[Object]) -> Result<(), Error> {
        let mut stdout = std::io::stdout();
        for x in list {
            let key = x.key.as_deref().unwrap_or("");
            let path = format!("s3://{}/{}", &path.bucket, key);
            self.exec(&mut stdout, &path)?;
        }
        Ok(())
    }
}

#[async_trait]
impl RunCommand for MultipleDelete {
    async fn execute(&self, client: &Client, path: &S3Path, list: &[Object]) -> Result<(), Error> {
        let key_list: Vec<_> = list
            .iter()
            .filter_map(|x| {
                ObjectIdentifier::builder()
                    .set_key(x.key.clone())
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
                            println!(
                                "deleted: s3://{}/{}",
                                &path.bucket,
                                object.key.as_ref().unwrap_or(&"".to_string())
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
    async fn execute(&self, client: &Client, path: &S3Path, list: &[Object]) -> Result<(), Error> {
        for object in list {
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

            client
                .put_object_tagging()
                .bucket(path.bucket.to_owned())
                .set_key(object.key.clone())
                .set_tagging(tagging)
                .send()
                .await?;

            println!(
                "tags are set for: s3://{}/{}",
                &path.bucket,
                &object.key.clone().unwrap()
            );
        }
        Ok(())
    }
}

#[async_trait]
impl RunCommand for ListTags {
    async fn execute(&self, client: &Client, path: &S3Path, list: &[Object]) -> Result<(), Error> {
        for object in list {
            let tag_output = client
                .get_object_tagging()
                .bucket(path.bucket.clone())
                .set_key(object.key.clone())
                .send()
                .await?;

            let tags: String = tag_output
                .tag_set
                .into_iter()
                .map(|x| format!("{}:{}", x.key, x.value))
                .collect::<Vec<String>>()
                .join(",");

            println!(
                "s3://{}/{} {}",
                &path.bucket,
                object.key.as_ref().unwrap_or(&"".to_string()),
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
    async fn execute(&self, client: &Client, path: &S3Path, list: &[Object]) -> Result<(), Error> {
        for object in list {
            client
                .put_object_acl()
                .bucket(path.bucket.to_owned())
                .set_key(object.key.clone())
                .acl(ObjectCannedAcl::PublicRead)
                .send()
                .await?;

            if let Some(key) = object.key.as_ref() {
                let region = client
                    .config()
                    .region()
                    .map(|x| x.as_ref())
                    .unwrap_or("us-east-1");
                let url = generate_s3_url(region, &path.bucket, key);
                println!("{} {}", key, url);
            } else {
                println!("No key found for object");
            }
        }
        Ok(())
    }
}

#[async_trait]
impl RunCommand for Download {
    async fn execute(&self, client: &Client, path: &S3Path, list: &[Object]) -> Result<(), Error> {
        for object in list {
            let key = object.key.as_ref().ok_or(FunctionError::ObjectFieldError)?;

            let size = object.size.unwrap_or_default() as u64;
            let file_path = Path::new(&self.destination).join(key);
            let dir_path = file_path.parent().ok_or(FunctionError::ParentPathParse)?;

            let mut count: u64 = 0;
            let pb = ProgressBar::new(size);
            pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta})")?
            .progress_chars("#>-"));

            println!(
                "downloading: s3://{}/{} => {}",
                &path.bucket,
                &key,
                file_path
                    .to_str()
                    .ok_or(FunctionError::FileNameParseError)?
            );

            if file_path.exists() && !self.force {
                return Ok(());
            }

            let mut stream = client
                .get_object()
                .bucket(&path.bucket)
                .key(key)
                .send()
                .await?
                .body;

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
    async fn execute(&self, client: &Client, path: &S3Path, list: &[Object]) -> Result<(), Error> {
        for object in list {
            let key = object.key.clone().ok_or(FunctionError::ObjectFieldError)?;

            let target = combine_keys(self.flat, &key, &self.destination.prefix);
            let source_path = format!("{0}/{1}", &path.bucket, key);

            println!(
                "copying: s3://{0} => s3://{1}/{2}",
                source_path, &self.destination.bucket, target,
            );

            client
                .copy_object()
                .bucket(&path.bucket)
                .key(target)
                .copy_source(source_path)
                .set_storage_class(self.storage_class.clone())
                .send()
                .await?;
        }
        Ok(())
    }
}

#[async_trait]
impl RunCommand for S3Move {
    async fn execute(&self, client: &Client, path: &S3Path, list: &[Object]) -> Result<(), Error> {
        for object in list {
            let key = object.key.clone().ok_or(FunctionError::ObjectFieldError)?;

            let target = combine_keys(self.flat, &key, &self.destination.prefix);
            let source_path = format!("{0}/{1}", &path.bucket, key);

            println!(
                "moving: s3://{0} => s3://{1}/{2}",
                source_path, &self.destination.bucket, target,
            );

            client
                .copy_object()
                .bucket(&path.bucket)
                .key(target)
                .copy_source(source_path)
                .set_storage_class(self.storage_class.clone())
                .send()
                .await?;
        }

        let key_list: Vec<_> = list
            .iter()
            .filter_map(|x| {
                ObjectIdentifier::builder()
                    .set_key(x.key.clone())
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
    async fn execute(&self, _c: &Client, _p: &S3Path, _l: &[Object]) -> Result<(), Error> {
        Ok(())
    }
}

#[async_trait]
impl RunCommand for Restore {
    async fn execute(
        &self,
        client: &Client,
        path: &S3Path,
        objects: &[Object],
    ) -> Result<(), Error> {
        for object in objects {
            if object.storage_class == Some(ObjectStorageClass::Glacier)
                || object.storage_class == Some(ObjectStorageClass::DeepArchive)
            {
                if let Some(key) = &object.key {
                    let restore_request = RestoreRequest::builder()
                        .days(self.days)
                        .set_glacier_job_parameters(Some(
                            GlacierJobParameters::builder()
                                .tier(self.tier.clone())
                                .build()?,
                        ))
                        .build();

                    let result = client
                        .restore_object()
                        .bucket(&path.bucket)
                        .key(key)
                        .restore_request(restore_request)
                        .send()
                        .await;

                    match result {
                        Ok(_) => println!("Restore initiated for: {}", key),
                        Err(e) => match e {
                            SdkError::ServiceError(err)
                                if err.err().meta().code() == Some("RestoreAlreadyInProgress") =>
                            {
                                println!("Restore already in progress for: {}", key)
                            }
                            SdkError::ServiceError(err)
                                if err.err().meta().code() == Some("InvalidObjectState") =>
                            {
                                println!(
                                    "Object is not in Glacier storage or already restored: {}",
                                    key
                                )
                            }
                            err => return Err(err.into()),
                        },
                    }
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
        objects: &[Object],
    ) -> Result<(), Error> {
        for object in objects {
            if let Some(key) = &object.key {
                println!(
                    "Changing storage class for s3://{}/{} to {:?}",
                    path.bucket, key, self.storage_class
                );

                let source_path = format!("{}/{}", path.bucket, key);

                client
                    .copy_object()
                    .copy_source(source_path)
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
        types::{ObjectStorageClass, StorageClass},
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

        cmd.print_object(&mut buf, bucket, &object)?;
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

        cmd.print_object(&mut buf, bucket, &object)?;
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

        cmd.print_object(&mut buf, bucket, &object)?;
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

        let mut buf = Vec::<u8>::new();
        let cmd = AdvancedPrint {
            format: PrintFormat::Json,
        };

        cmd.print_json_object(&mut buf, &object)?;

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

        let mut buf = Vec::<u8>::new();
        let cmd = AdvancedPrint {
            format: PrintFormat::Csv,
        };

        cmd.print_csv_objects(&mut buf, &[object1.clone(), object2.clone()])?;

        let output = String::from_utf8(buf.clone()).unwrap();
        println!("CSV Output:\n{}", output);

        let record1: ObjectRecord = (&object1).into();
        let record2: ObjectRecord = (&object2).into();

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

        let cmd = Cmd::Print(AdvancedPrint::default()).downcast();
        let config = aws_config::load_defaults(BehaviorVersion::v2025_01_17()).await;
        let client = Client::new(&config);

        let path = S3Path {
            bucket: "test".to_owned(),
            prefix: None,
        };

        cmd.execute(&client, &path, &[object]).await?;
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

        let object2 = Object::builder()
            .e_tag("test-etag-2")
            .key("folder/file2.txt")
            .size(200)
            .storage_class(ObjectStorageClass::IntelligentTiering)
            .last_modified(
                DateTime::from_str("2023-02-15T12:30:45.000Z", Format::DateTime).unwrap(),
            )
            .build();

        let cmd = Cmd::Print(AdvancedPrint {
            format: PrintFormat::Json,
        })
        .downcast();

        let config = aws_config::load_defaults(BehaviorVersion::v2025_01_17()).await;
        let client = Client::new(&config);
        let path = S3Path {
            bucket: "test-bucket".to_owned(),
            prefix: None,
        };

        cmd.execute(&client, &path, &[object1, object2]).await?;
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

        let object2 = Object::builder()
            .e_tag("csv-etag-2")
            .key("data/archive.zip")
            .size(10000)
            .storage_class(ObjectStorageClass::DeepArchive)
            .last_modified(
                DateTime::from_str("2023-04-20T14:25:10.000Z", Format::DateTime).unwrap(),
            )
            .build();

        let cmd = Cmd::Print(AdvancedPrint {
            format: PrintFormat::Csv,
        })
        .downcast();

        let config = aws_config::load_defaults(BehaviorVersion::v2025_01_17()).await;
        let client = Client::new(&config);
        let path = S3Path {
            bucket: "test-bucket".to_owned(),
            prefix: None,
        };

        cmd.execute(&client, &path, &[object1, object2]).await?;
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

        let cmd = Cmd::Ls(FastPrint {}).downcast();
        let config = aws_config::load_defaults(BehaviorVersion::v2025_01_17()).await;
        let client = Client::new(&config);

        let path = S3Path {
            bucket: "test".to_owned(),
            prefix: None,
        };

        cmd.execute(&client, &path, &[object]).await?;
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

        let cmd = Cmd::Nothing(DoNothing {}).downcast();
        let config = aws_config::load_defaults(BehaviorVersion::v2025_01_17()).await;
        let client = Client::new(&config);

        let path = S3Path {
            bucket: "test".to_owned(),
            prefix: None,
        };

        cmd.execute(&client, &path, &[object]).await
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

        let cmd = Cmd::Exec(Exec {
            utility: "echo {}".to_owned(),
        })
        .downcast();

        let config = aws_config::load_defaults(BehaviorVersion::v2025_01_17()).await;
        let client = Client::new(&config);

        let path = S3Path {
            bucket: "test".to_owned(),
            prefix: None,
        };

        cmd.execute(&client, &path, &[object]).await
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

        cmd.execute(&client, &path, &[object]).await?;

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

        cmd.execute(&client, &path, &[object1, object2]).await?;

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

        cmd.execute(&client, &path, &[object1, object2]).await?;

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

        cmd.execute(&client, &path, &[object1, object2]).await?;

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

        cmd.execute(&client, &path, &[object1, object2]).await?;

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

        cmd.execute(&client, &path, &[object1, object2]).await?;

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

        cmd.execute(&client, &path, &[object]).await?;

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

        cmd.execute(&client, &path, &[object]).await?;

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

        cmd.execute(&client, &path, &[object1, object2]).await?;

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
                glacier_object,
                deeparchive_object,
                restored_object,
                invalid_state_object,
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

        let result = restore_cmd.execute(&client, &path, &[glacier_object]).await;
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

        let result = restore_cmd.execute(&client, &path, &[glacier_object]).await;
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
            .execute(&client, &path, &[deep_archive_object])
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
            .execute(&client, &path, &[standard_object])
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

        let result = restore_cmd.execute(&client, &path, &[glacier_object]).await;
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

        cmd.execute(&client, &path, &[object1, object2]).await?;

        Ok(())
    }
}
