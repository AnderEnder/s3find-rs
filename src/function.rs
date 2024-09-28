use std::fs;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::process::Command;
use std::process::ExitStatus;

use anyhow::Error;
use async_trait::async_trait;
use aws_smithy_types::date_time::Format;
use indicatif::{ProgressBar, ProgressStyle};

use aws_sdk_s3::types::{Delete, Object, ObjectCannedAcl, ObjectIdentifier, Tag, Tagging};
use aws_sdk_s3::Client;

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
            // _ => Box::new(FastPrint {}),
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
            "{0} {1:?} {2} {3:?} s3://{4}/{5} {6:?}",
            object.e_tag.as_ref().unwrap_or(&"NoEtag".to_string()),
            object.owner.as_ref().map(|x| x.display_name.as_ref()),
            object.size.unwrap_or_default(),
            object.last_modified.unwrap().fmt(Format::DateTime),
            bucket,
            object.key.as_ref().unwrap_or(&"".to_string()),
            object.storage_class,
        )
    }
}

#[async_trait]
impl RunCommand for AdvancedPrint {
    async fn execute(&self, _c: &Client, path: &S3Path, list: &[Object]) -> Result<(), Error> {
        let mut stdout = std::io::stdout();
        for x in list {
            self.print_object(&mut stdout, &path.bucket, x)?
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
            [command_name, ref command_args @ ..] => (*command_name, command_args),
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

            let key = object.key.clone().unwrap();
            let url = generate_s3_url(path.region.as_ref(), &path.bucket, &key);
            println!("{} {}", key, url);
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
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta})")
            .unwrap()
            .progress_chars("#>-"));

            println!(
                "downloading: s3://{}/{} => {}",
                &path.bucket,
                &key,
                file_path
                    .to_str()
                    .ok_or(FunctionError::FileNameParseError)
                    .unwrap()
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
                output.write_all(&bytes).unwrap();
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

#[cfg(test)]
mod tests {
    use super::*;
    use aws_config::BehaviorVersion;
    use aws_sdk_s3::{primitives::DateTime, types::ObjectStorageClass};
    use aws_smithy_types::date_time::Format;
    use aws_types::region::Region;

    // use std::fs::File;
    // use std::io::prelude::*;
    // use tempfile::Builder;

    #[test]
    fn test_advanced_print_object() -> Result<(), Error> {
        let mut buf = Vec::new();
        let cmd = AdvancedPrint {};
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
        assert!(out.contains("Standard"));
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

        let cmd = Cmd::Print(AdvancedPrint {}).downcast();
        let config = aws_config::load_defaults(BehaviorVersion::v2024_03_28()).await;
        let client = Client::new(&config);

        let path = S3Path {
            bucket: "test".to_owned(),
            prefix: None,
            region: Region::from_static("us-east-1"),
        };

        cmd.execute(&client, &path, &[object]).await?;
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
        let config = aws_config::load_defaults(BehaviorVersion::v2024_03_28()).await;
        let client = Client::new(&config);

        let path = S3Path {
            bucket: "test".to_owned(),
            prefix: None,
            region: Region::from_static("us-east-1"),
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
        let config = aws_config::load_defaults(BehaviorVersion::v2024_03_28()).await;
        let client = Client::new(&config);

        let path = S3Path {
            bucket: "test".to_owned(),
            prefix: None,
            region: Region::from_static("us-east-1"),
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

        let config = aws_config::load_defaults(BehaviorVersion::v2024_03_28()).await;
        let client = Client::new(&config);

        let path = S3Path {
            bucket: "test".to_owned(),
            prefix: None,
            region: Region::from_static("us-east-1"),
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
}
