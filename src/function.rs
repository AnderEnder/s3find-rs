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

use aws_sdk_s3::Client;
use aws_sdk_s3::types::{Delete, Object, ObjectCannedAcl, ObjectIdentifier, Tag, Tagging};
use aws_smithy_types::byte_stream::ByteStream;

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
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct ExecStatus {
    pub status: ExitStatus,
    pub runcommand: String,
}

#[async_trait]
pub trait S3Client: Send + Sync {
    async fn put_object_acl(
        &self,
        bucket: String,
        key: String,
        acl: ObjectCannedAcl,
    ) -> Result<(), Error>;

    async fn get_object(&self, bucket: String, key: String) -> Result<ByteStream, Error>;

    async fn get_object_tagging(&self, bucket: String, key: String) -> Result<Vec<Tag>, Error>;

    async fn copy_object(
        &self,
        source_bucket: String,
        source_key: String,
        dest_bucket: String,
        dest_key: String,
    ) -> Result<(), Error>;

    async fn delete_objects(&self, bucket: String, keys: Vec<String>) -> Result<(), Error>;

    async fn put_object_tagging(
        &self,
        bucket: String,
        key: String,
        tagging: Option<Tagging>,
    ) -> Result<(), Error>;
}

#[async_trait]
impl S3Client for Client {
    async fn put_object_acl(
        &self,
        bucket: String,
        key: String,
        acl: ObjectCannedAcl,
    ) -> Result<(), Error> {
        self.put_object_acl()
            .bucket(bucket)
            .key(key)
            .acl(acl)
            .send()
            .await?;
        Ok(())
    }

    async fn get_object(&self, bucket: String, key: String) -> Result<ByteStream, Error> {
        let response = self.get_object().bucket(bucket).key(key).send().await?;
        Ok(response.body)
    }

    async fn get_object_tagging(&self, bucket: String, key: String) -> Result<Vec<Tag>, Error> {
        let response = self
            .get_object_tagging()
            .bucket(bucket)
            .key(key)
            .send()
            .await?;
        Ok(response.tag_set)
    }

    async fn copy_object(
        &self,
        source_bucket: String,
        source_key: String,
        dest_bucket: String,
        dest_key: String,
    ) -> Result<(), Error> {
        self.copy_object()
            .copy_source(format!("{}/{}", source_bucket, source_key))
            .bucket(dest_bucket)
            .key(dest_key)
            .send()
            .await?;
        Ok(())
    }

    async fn delete_objects(&self, bucket: String, keys: Vec<String>) -> Result<(), Error> {
        let objects = keys
            .into_iter()
            .map(|key| ObjectIdentifier::builder().key(key).build())
            .collect::<Result<Vec<_>, _>>()?;

        let delete = Delete::builder().set_objects(Some(objects)).build()?;

        self.delete_objects()
            .bucket(bucket)
            .delete(delete)
            .send()
            .await?;
        Ok(())
    }

    async fn put_object_tagging(
        &self,
        bucket: String,
        key: String,
        tagging: Option<Tagging>,
    ) -> Result<(), Error> {
        self.put_object_tagging()
            .bucket(bucket)
            .key(key)
            .set_tagging(tagging)
            .send()
            .await?;
        Ok(())
    }
}

#[async_trait]
pub trait RunCommand {
    async fn execute(
        &self,
        client: &dyn S3Client,
        path: &S3Path,
        list: &[Object],
    ) -> Result<(), Error>;
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
    async fn execute(
        &self,
        _c: &dyn S3Client,
        path: &S3Path,
        list: &[Object],
    ) -> Result<(), Error> {
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
    async fn execute(
        &self,
        _c: &dyn S3Client,
        path: &S3Path,
        list: &[Object],
    ) -> Result<(), Error> {
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
    async fn execute(&self, _: &dyn S3Client, path: &S3Path, list: &[Object]) -> Result<(), Error> {
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
    async fn execute(
        &self,
        client: &dyn S3Client,
        path: &S3Path,
        list: &[Object],
    ) -> Result<(), Error> {
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

        let keys: Vec<String> = objects
            .objects()
            .iter()
            .map(|obj| obj.key().to_string())
            .collect();

        client.delete_objects(path.bucket.to_owned(), keys).await?;
        Ok(())
    }
}

#[async_trait]
impl RunCommand for SetTags {
    async fn execute(
        &self,
        client: &dyn S3Client,
        path: &S3Path,
        list: &[Object],
    ) -> Result<(), Error> {
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
                .put_object_tagging(path.bucket.to_owned(), object.key.clone().unwrap(), tagging)
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
    async fn execute(
        &self,
        client: &dyn S3Client,
        path: &S3Path,
        list: &[Object],
    ) -> Result<(), Error> {
        for object in list {
            let tag_output = client
                .get_object_tagging(path.bucket.clone(), object.key.clone().unwrap())
                .await?;

            let tags: String = tag_output
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
    async fn execute(
        &self,
        client: &dyn S3Client,
        path: &S3Path,
        list: &[Object],
    ) -> Result<(), Error> {
        for object in list {
            client
                .put_object_acl(
                    path.bucket.to_owned(),
                    object.key.clone().unwrap(),
                    ObjectCannedAcl::PublicRead,
                )
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
    async fn execute(
        &self,
        client: &dyn S3Client,
        path: &S3Path,
        list: &[Object],
    ) -> Result<(), Error> {
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
                .get_object(path.bucket.clone(), key.to_string())
                .await?;

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
    async fn execute(
        &self,
        client: &dyn S3Client,
        path: &S3Path,
        list: &[Object],
    ) -> Result<(), Error> {
        for object in list {
            let key = object.key.clone().ok_or(FunctionError::ObjectFieldError)?;

            let target = combine_keys(self.flat, &key, &self.destination.prefix);
            let source_path = format!("{0}/{1}", &path.bucket, key);

            println!(
                "copying: s3://{0} => s3://{1}/{2}",
                source_path, &self.destination.bucket, target,
            );

            client
                .copy_object(
                    path.bucket.clone(),
                    key,
                    self.destination.bucket.clone(),
                    target,
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
        client: &dyn S3Client,
        path: &S3Path,
        list: &[Object],
    ) -> Result<(), Error> {
        for object in list {
            let key = object.key.clone().ok_or(FunctionError::ObjectFieldError)?;

            let target = combine_keys(self.flat, &key, &self.destination.prefix);
            let source_path = format!("{0}/{1}", &path.bucket, key);

            println!(
                "moving: s3://{0} => s3://{1}/{2}",
                source_path, &self.destination.bucket, target,
            );

            client
                .copy_object(
                    path.bucket.clone(),
                    key,
                    self.destination.bucket.clone(),
                    target,
                )
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

        // let delete = Delete::builder().set_objects(Some(key_list)).build().ok();

        client
            .delete_objects(
                path.bucket.clone(),
                key_list.into_iter().map(|o| o.key.to_string()).collect(),
            )
            .await?;
        Ok(())
    }
}

#[async_trait]
impl RunCommand for DoNothing {
    async fn execute(&self, _c: &dyn S3Client, _p: &S3Path, _l: &[Object]) -> Result<(), Error> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use aws_sdk_s3::types::ObjectCannedAcl;
    use aws_smithy_types::DateTime;
    use aws_types::region::Region;
    use std::sync::{Arc, Mutex};

    struct MockS3Client {
        pub put_object_acl_called: Arc<Mutex<bool>>,
        pub get_object_called: Arc<Mutex<bool>>,
        pub get_object_tagging_called: Arc<Mutex<bool>>,
        pub put_object_tagging_called: Arc<Mutex<bool>>,
        pub copy_object_called: Arc<Mutex<bool>>,
        pub delete_objects_called: Arc<Mutex<bool>>,
    }

    impl MockS3Client {
        fn new() -> Self {
            MockS3Client {
                put_object_acl_called: Arc::new(Mutex::new(false)),
                get_object_called: Arc::new(Mutex::new(false)),
                get_object_tagging_called: Arc::new(Mutex::new(false)),
                put_object_tagging_called: Arc::new(Mutex::new(false)),
                copy_object_called: Arc::new(Mutex::new(false)),
                delete_objects_called: Arc::new(Mutex::new(false)),
            }
        }
    }

    #[async_trait]
    impl S3Client for MockS3Client {
        async fn put_object_acl(
            &self,
            _bucket: String,
            _key: String,
            _acl: ObjectCannedAcl,
        ) -> Result<(), Error> {
            *self.put_object_acl_called.lock().unwrap() = true;
            Ok(())
        }

        async fn get_object(&self, _bucket: String, _key: String) -> Result<ByteStream, Error> {
            *self.get_object_called.lock().unwrap() = true;
            Ok(ByteStream::from_static(b"test data"))
        }

        async fn get_object_tagging(
            &self,
            _bucket: String,
            _key: String,
        ) -> Result<Vec<Tag>, Error> {
            *self.get_object_tagging_called.lock().unwrap() = true;
            Ok(vec![
                Tag::builder().key("tag1").value("value1").build()?,
                Tag::builder().key("tag2").value("value2").build()?,
            ])
        }

        async fn copy_object(
            &self,
            _source_bucket: String,
            _source_key: String,
            _dest_bucket: String,
            _dest_key: String,
        ) -> Result<(), Error> {
            *self.copy_object_called.lock().unwrap() = true;
            Ok(())
        }

        async fn delete_objects(&self, _bucket: String, _keys: Vec<String>) -> Result<(), Error> {
            *self.delete_objects_called.lock().unwrap() = true;
            Ok(())
        }

        async fn put_object_tagging(
            &self,
            _bucket: String,
            _key: String,
            _tagging: Option<Tagging>,
        ) -> Result<(), Error> {
            *self.put_object_tagging_called.lock().unwrap() = true;
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_set_public() -> Result<(), Error> {
        let mock_client = MockS3Client::new();
        let object = Object::builder().key("test-key").build();

        let cmd = Cmd::Public(SetPublic {}).downcast();
        let path = S3Path {
            bucket: "test".to_owned(),
            prefix: None,
            region: Region::from_static("us-east-1"),
        };

        cmd.execute(&mock_client, &path, &[object]).await?;
        assert!(*mock_client.put_object_acl_called.lock().unwrap());
        Ok(())
    }

    #[tokio::test]
    async fn test_download() -> Result<(), Error> {
        let mock_client = MockS3Client::new();
        let temp_dir = tempfile::tempdir()?;
        let object = Object::builder().key("test-key").size(10).build();

        let cmd = Cmd::Download(Download {
            destination: temp_dir.path().to_str().unwrap().to_string(),
            force: true,
        })
        .downcast();

        let path = S3Path {
            bucket: "test".to_owned(),
            prefix: None,
            region: Region::from_static("us-east-1"),
        };

        cmd.execute(&mock_client, &path, &[object]).await?;
        assert!(*mock_client.get_object_called.lock().unwrap());
        Ok(())
    }

    #[tokio::test]
    async fn test_list_tags() -> Result<(), Error> {
        let mock_client = MockS3Client::new();
        let object = Object::builder().key("test-key").build();

        let cmd = Cmd::LsTags(ListTags {}).downcast();
        let path = S3Path {
            bucket: "test".to_owned(),
            prefix: None,
            region: Region::from_static("us-east-1"),
        };

        cmd.execute(&mock_client, &path, &[object]).await?;
        assert!(*mock_client.get_object_tagging_called.lock().unwrap());
        Ok(())
    }

    #[tokio::test]
    async fn test_s3_move() -> Result<(), Error> {
        let mock_client = MockS3Client::new();
        let object = Object::builder().key("test-key").build();

        let cmd = Cmd::Move(S3Move {
            destination: S3Path {
                bucket: "dest-bucket".to_owned(),
                prefix: Some("dest-prefix".to_owned()),
                region: Region::from_static("us-east-1"),
            },
            flat: false,
        })
        .downcast();

        let path = S3Path {
            bucket: "test".to_owned(),
            prefix: None,
            region: Region::from_static("us-east-1"),
        };

        cmd.execute(&mock_client, &path, &[object]).await?;
        assert!(*mock_client.copy_object_called.lock().unwrap());
        assert!(*mock_client.delete_objects_called.lock().unwrap());
        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_delete() -> Result<(), Error> {
        let mock_client = MockS3Client::new();
        let objects = vec![
            Object::builder().key("test-key1").build(),
            Object::builder().key("test-key2").build(),
        ];

        let cmd = Cmd::Delete(MultipleDelete {}).downcast();
        let path = S3Path {
            bucket: "test".to_owned(),
            prefix: None,
            region: Region::from_static("us-east-1"),
        };

        cmd.execute(&mock_client, &path, &objects).await?;
        assert!(*mock_client.delete_objects_called.lock().unwrap());
        Ok(())
    }

    #[tokio::test]
    async fn test_set_tags() -> Result<(), Error> {
        let mock_client = MockS3Client::new();
        let object = Object::builder().key("test-key").build();

        let tags: Vec<FindTag> = vec![
            FindTag {
                key: "tag1".to_string(),
                value: "value1".to_string(),
            },
            FindTag {
                key: "tag2".to_string(),
                value: "value2".to_string(),
            },
        ];

        let cmd = Cmd::Tags(SetTags { tags }).downcast();
        let path = S3Path {
            bucket: "test".to_owned(),
            prefix: None,
            region: Region::from_static("us-east-1"),
        };

        cmd.execute(&mock_client, &path, &[object]).await?;
        assert!(*mock_client.put_object_tagging_called.lock().unwrap());
        Ok(())
    }

    #[tokio::test]
    async fn test_s3_copy() -> Result<(), Error> {
        let mock_client = MockS3Client::new();
        let object = Object::builder().key("test-key").build();

        let cmd = Cmd::Copy(S3Copy {
            destination: S3Path {
                bucket: "dest-bucket".to_owned(),
                prefix: Some("dest-prefix".to_owned()),
                region: Region::from_static("us-east-1"),
            },
            flat: false,
        })
        .downcast();

        let path = S3Path {
            bucket: "test".to_owned(),
            prefix: None,
            region: Region::from_static("us-east-1"),
        };

        cmd.execute(&mock_client, &path, &[object]).await?;
        assert!(*mock_client.copy_object_called.lock().unwrap());
        Ok(())
    }

    #[tokio::test]
    async fn test_advanced_print() -> Result<(), Error> {
        let object = Object::builder()
            .e_tag("9d48114aa7c18f9d68aa20086dbb7756")
            .key("somepath/otherpath")
            .size(4_997_288)
            .last_modified(DateTime::from_str(
                "2017-07-19T19:04:17.000Z",
                Format::DateTime,
            )?)
            .build();

        let cmd = Cmd::Print(AdvancedPrint {}).downcast();
        let mock_client = MockS3Client::new();

        let path = S3Path {
            bucket: "test".to_owned(),
            prefix: None,
            region: Region::from_static("us-east-1"),
        };

        cmd.execute(&mock_client, &path, &[object]).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_fastprint() -> Result<(), Error> {
        let object = Object::builder().key("somepath/otherpath").build();

        let cmd = Cmd::Ls(FastPrint {}).downcast();
        let mock_client = MockS3Client::new();

        let path = S3Path {
            bucket: "test".to_owned(),
            prefix: None,
            region: Region::from_static("us-east-1"),
        };

        cmd.execute(&mock_client, &path, &[object]).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_exec() -> Result<(), Error> {
        let object = Object::builder().key("somepath/otherpath").build();

        let cmd = Cmd::Exec(Exec {
            utility: "echo {}".to_owned(),
        })
        .downcast();

        let mock_client = MockS3Client::new();
        let path = S3Path {
            bucket: "test".to_owned(),
            prefix: None,
            region: Region::from_static("us-east-1"),
        };

        cmd.execute(&mock_client, &path, &[object]).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_donothing() -> Result<(), Error> {
        let object = Object::builder().key("somepath/otherpath").build();

        let cmd = Cmd::Nothing(DoNothing {}).downcast();
        let mock_client = MockS3Client::new();

        let path = S3Path {
            bucket: "test".to_owned(),
            prefix: None,
            region: Region::from_static("us-east-1"),
        };

        cmd.execute(&mock_client, &path, &[object]).await?;
        Ok(())
    }
}
