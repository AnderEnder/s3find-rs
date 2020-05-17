use async_trait::async_trait;
use futures::future;
use futures::stream::StreamExt;
use rusoto_s3::{
    CopyObjectRequest, Delete, DeleteObjectsRequest, GetObjectRequest, GetObjectTaggingRequest,
    Object, ObjectIdentifier, PutObjectAclRequest, PutObjectTaggingRequest, S3Client, Tagging, S3,
};
use std::process::Command;
use std::process::ExitStatus;

use std::fs;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::Error;
use dyn_clone::DynClone;
use indicatif::{ProgressBar, ProgressStyle};

use crate::arg::*;
use crate::error::*;

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

pub trait RunCommand: DynClone {
    async fn execute(
        &self,
        client: &S3Client,
        region: &str,
        path: &S3Path,
        list: &[Object],
    ) -> Result<(), Error>;
}

dyn_clone::clone_trait_object!(RunCommand);

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
            object.key.as_ref().unwrap_or(&"".to_string())
        )
    }
}

#[async_trait]
impl RunCommand for FastPrint {
    async fn execute(
        &self,
        _c: &S3Client,
        _r: &str,
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
            "{0} {1:?} {2} {3} s3://{4}/{5} {6}",
            object.e_tag.as_ref().unwrap_or(&"NoEtag".to_string()),
            object.owner.as_ref().map(|x| x.display_name.as_ref()),
            object.size.as_ref().unwrap_or(&0),
            object
                .last_modified
                .as_ref()
                .unwrap_or(&"NoTime".to_string()),
            bucket,
            object.key.as_ref().unwrap_or(&"".to_string()),
            object
                .storage_class
                .as_ref()
                .unwrap_or(&"NoStorage".to_string()),
        )
    }
}

#[async_trait]
impl RunCommand for AdvancedPrint {
    async fn execute(
        &self,
        _c: &S3Client,
        _r: &str,
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

        let mut command_args = command_str.split(' ');
        let command_name = command_args.next().ok_or(FunctionError::CommandlineParse)?;

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
    async fn execute(
        &self,
        _: &S3Client,
        _r: &str,
        path: &S3Path,
        list: &[Object],
    ) -> Result<(), Error> {
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
        client: &S3Client,
        _r: &str,
        path: &S3Path,
        list: &[Object],
    ) -> Result<(), Error> {
        let key_list: Vec<_> = list
            .iter()
            .flat_map(|x| match x.key.as_ref() {
                Some(key) => Some(ObjectIdentifier {
                    key: key.to_string(),
                    version_id: None,
                }),
                _ => None,
            })
            .collect();

        let request = DeleteObjectsRequest {
            bucket: path.bucket.to_string(),
            delete: Delete {
                objects: key_list,
                quiet: None,
            },
            ..Default::default()
        };

        let result = client.delete_objects(request).await;

        match result {
            Ok(r) => {
                if let Some(deleted_list) = r.deleted {
                    for object in deleted_list {
                        println!(
                            "deleted: s3://{}/{}",
                            &path.bucket,
                            object.key.as_ref().unwrap_or(&"".to_string())
                        );
                    }
                }
            }
            Err(e) => eprintln!("{}", e),
        }
        Ok(())
    }
}

#[async_trait]
impl RunCommand for SetTags {
    async fn execute(
        &self,
        client: &S3Client,
        _r: &str,
        path: &S3Path,
        list: &[Object],
    ) -> Result<(), Error> {
        for object in list {
            let key = object.key.as_ref().ok_or(FunctionError::ObjectFieldError)?;

            let tags = Tagging {
                tag_set: self.tags.iter().map(|x| x.clone().into()).collect(),
            };

            let request = PutObjectTaggingRequest {
                bucket: path.bucket.to_owned(),
                key: key.to_owned(),
                tagging: tags,
                ..Default::default()
            };

            client.put_object_tagging(request).await?;
            println!("tags are set for: s3://{}/{}", &path.bucket, &key);
        }
        Ok(())
    }
}

#[async_trait]
impl RunCommand for ListTags {
    async fn execute(
        &self,
        client: &S3Client,
        _r: &str,
        path: &S3Path,
        list: &[Object],
    ) -> Result<(), Error> {
        for object in list {
            let key = object.key.as_ref().ok_or(FunctionError::ObjectFieldError)?;

            let request = GetObjectTaggingRequest {
                bucket: path.bucket.to_string(),
                key: key.to_owned(),
                ..Default::default()
            };

            let tag_output = client.get_object_tagging(request).await?;

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

#[async_trait]
impl RunCommand for SetPublic {
    async fn execute(
        &self,
        client: &S3Client,
        region: &str,
        path: &S3Path,
        list: &[Object],
    ) -> Result<(), Error> {
        for object in list {
            let key = object.key.as_ref().ok_or(FunctionError::ObjectFieldError)?;

            let request = PutObjectAclRequest {
                bucket: path.bucket.to_owned(),
                key: key.to_owned(),
                acl: Some("public-read".to_string()),
                ..Default::default()
            };

            client.put_object_acl(request).await?;

            let url = match region {
                "us-east-1" => format!("http://{}.s3.amazonaws.com/{}", &path.bucket, key),
                _ => format!(
                    "http://{}.s3-{}.amazonaws.com/{}",
                    &path.bucket, region, key
                ),
            };
            println!("{} {}", &key, url);
        }
        Ok(())
    }
}

#[async_trait]
impl RunCommand for Download {
    async fn execute(
        &self,
        client: &S3Client,
        _r: &str,
        path: &S3Path,
        list: &[Object],
    ) -> Result<(), Error> {
        for object in list {
            let key = object.key.as_ref().ok_or(FunctionError::ObjectFieldError)?;

            let request = GetObjectRequest {
                bucket: path.bucket.to_owned(),
                key: key.to_owned(),
                ..Default::default()
            };

            let size = (*object
                .size
                .as_ref()
                .ok_or(FunctionError::ObjectFieldError)
                .unwrap()) as u64;
            let file_path = Path::new(&self.destination).join(key);
            let dir_path = file_path.parent().ok_or(FunctionError::ParentPathParse)?;

            let mut count: u64 = 0;
            let pb = ProgressBar::new(size);
            pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta})")
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

            let stream = client
                .get_object(request)
                .await?
                .body
                .ok_or(FunctionError::S3FetchBodyError)?;

            fs::create_dir_all(&dir_path)?;
            let mut output = File::create(&file_path)?;

            stream
                .for_each(|buf| {
                    let b = buf.unwrap();
                    output.write_all(&b).unwrap();
                    count += b.len() as u64;
                    pb.set_position(count);
                    future::ready(())
                })
                .await;
        }
        Ok(())
    }
}

#[async_trait]
impl RunCommand for S3Copy {
    async fn execute(
        &self,
        client: &S3Client,
        _r: &str,
        path: &S3Path,
        list: &[Object],
    ) -> Result<(), Error> {
        for object in list {
            let key = object.key.as_ref().ok_or(FunctionError::ObjectFieldError)?;

            let key2 = if self.flat {
                Path::new(key)
                    .file_name()
                    .ok_or(FunctionError::PathConverError)?
                    .to_str()
                    .ok_or(FunctionError::PathConverError)?
            } else {
                key
            };

            let target_key = if let Some(ref r) = self.destination.prefix {
                Path::new(r).join(key2)
            } else {
                PathBuf::from(key2)
            };

            let target_key_str = target_key.to_str().ok_or(FunctionError::PathConverError)?;
            let source_path = format!("{0}/{1}", &path.bucket, key);

            println!(
                "copying: s3://{0} => s3://{1}/{2}",
                source_path, &self.destination.bucket, target_key_str,
            );

            let request = CopyObjectRequest {
                bucket: self.destination.bucket.clone(),
                key: target_key_str.to_owned(),
                copy_source: source_path,
                ..Default::default()
            };

            client.copy_object(request).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl RunCommand for S3Move {
    async fn execute(
        &self,
        client: &S3Client,
        _r: &str,
        path: &S3Path,
        list: &[Object],
    ) -> Result<(), Error> {
        for object in list {
            let key = object.key.as_ref().ok_or(FunctionError::ObjectFieldError)?;

            let key2 = if self.flat {
                Path::new(key)
                    .file_name()
                    .ok_or(FunctionError::PathConverError)?
                    .to_str()
                    .ok_or(FunctionError::PathConverError)?
            } else {
                key
            };

            let target_key = if let Some(ref r) = self.destination.prefix {
                Path::new(r).join(key2)
            } else {
                PathBuf::from(key2)
            };

            let target_key_str = target_key.to_str().ok_or(FunctionError::PathConverError)?;
            let source_path = format!("{0}/{1}", &path.bucket, key);

            println!(
                "moving: s3://{0} => s3://{1}/{2}",
                source_path, &self.destination.bucket, target_key_str,
            );

            let request = CopyObjectRequest {
                bucket: self.destination.bucket.to_owned(),
                key: target_key_str.to_owned(),
                copy_source: source_path,
                ..Default::default()
            };

            client.copy_object(request).await?;
        }

        let key_list: Vec<_> = list
            .iter()
            .flat_map(|x| match x.key.as_ref() {
                Some(key) => Some(ObjectIdentifier {
                    key: key.to_string(),
                    version_id: None,
                }),
                _ => None,
            })
            .collect();

        let request = DeleteObjectsRequest {
            bucket: path.bucket.clone(),
            delete: Delete {
                objects: key_list,
                quiet: None,
            },
            ..Default::default()
        };

        client.delete_objects(request).await?;
        Ok(())
    }
}

#[async_trait]
impl RunCommand for DoNothing {
    async fn execute(
        &self,
        _c: &S3Client,
        _r: &str,
        _p: &S3Path,
        _l: &[Object],
    ) -> Result<(), Error> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use remove_dir_all::remove_dir_all;
    use rusoto_core::Region;
    use rusoto_mock::*;
    use std::fs::File;
    use std::io::prelude::*;
    use tempfile::Builder;

    #[test]
    fn test_advanced_print_object() -> Result<(), Error> {
        let mut buf = Vec::new();
        let cmd = AdvancedPrint {};
        let bucket = "test";

        let object = Object {
            e_tag: Some("9d48114aa7c18f9d68aa20086dbb7756".to_string()),
            key: Some("somepath/otherpath".to_string()),
            last_modified: Some("2017-07-19T19:04:17.000Z".to_string()),
            owner: None,
            size: Some(4_997_288),
            storage_class: Some("STANDARD".to_string()),
        };

        cmd.print_object(&mut buf, bucket, &object)?;
        let out = std::str::from_utf8(&buf)?;

        assert!(out.contains("9d48114aa7c18f9d68aa20086dbb7756"));
        assert!(out.contains("None"));
        assert!(out.contains("4997288"));
        assert!(out.contains("2017-07-19T19:04:17.000Z"));
        assert!(out.contains("s3://test/somepath/otherpath"));
        assert!(out.contains("STANDARD"));
        Ok(())
    }

    #[test]
    fn test_fast_print_object() -> Result<(), Error> {
        let mut buf = Vec::new();
        let cmd = FastPrint {};
        let bucket = "test";

        let object = Object {
            e_tag: Some("9d48114aa7c18f9d68aa20086dbb7756".to_string()),
            key: Some("somepath/otherpath".to_string()),
            last_modified: Some("2017-07-19T19:04:17.000Z".to_string()),
            owner: None,
            size: Some(4_997_288),
            storage_class: Some("STANDARD".to_string()),
        };

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
        let object = Object {
            e_tag: Some("9d48114aa7c18f9d68aa20086dbb7756".to_string()),
            key: Some("somepath/otherpath".to_string()),
            last_modified: Some("2017-07-19T19:04:17.000Z".to_string()),
            owner: None,
            size: Some(4_997_288),
            storage_class: Some("STANDARD".to_string()),
        };

        let cmd = AdvancedPrint {};
        let region = "us-east-1";
        let client = S3Client::new(Region::UsEast1);
        let path = S3Path {
            bucket: "test".to_owned(),
            prefix: None,
        };

        cmd.execute(&client, region, &path, &[object]).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_fastprint() -> Result<(), Error> {
        let object = Object {
            e_tag: Some("9d48114aa7c18f9d68aa20086dbb7756".to_string()),
            key: Some("somepath/otherpath".to_string()),
            last_modified: Some("2017-07-19T19:04:17.000Z".to_string()),
            owner: None,
            size: Some(4_997_288),
            storage_class: Some("STANDARD".to_string()),
        };

        let cmd = FastPrint {};
        let region = "us-east-1";
        let client = S3Client::new(Region::UsEast1);
        let path = S3Path {
            bucket: "test".to_owned(),
            prefix: None,
        };

        cmd.execute(&client, region, &path, &[object]).await?;
        Ok(())
    }

    #[tokio::test]
    async fn smoke_donothing() -> Result<(), Error> {
        let object = Object {
            e_tag: Some("9d48114aa7c18f9d68aa20086dbb7756".to_string()),
            key: Some("somepath/otherpath".to_string()),
            last_modified: Some("2017-07-19T19:04:17.000Z".to_string()),
            owner: None,
            size: Some(4_997_288),
            storage_class: Some("STANDARD".to_string()),
        };

        let cmd = DoNothing {};
        let region = "us-east-1";
        let client = S3Client::new(Region::UsEast1);
        let path = S3Path {
            bucket: "test".to_owned(),
            prefix: None,
        };

        cmd.execute(&client, region, &path, &[object]).await
    }

    #[tokio::test]
    async fn smoke_exec() -> Result<(), Error> {
        let object = Object {
            e_tag: Some("9d48114aa7c18f9d68aa20086dbb7756".to_string()),
            key: Some("somepath/otherpath".to_string()),
            last_modified: Some("2017-07-19T19:04:17.000Z".to_string()),
            owner: None,
            size: Some(4_997_288),
            storage_class: Some("STANDARD".to_string()),
        };

        let cmd = Exec {
            utility: "echo {}".to_owned(),
        };
        let region = "us-east-1";
        let client = S3Client::new(Region::UsEast1);
        let path = S3Path {
            bucket: "test".to_owned(),
            prefix: None,
        };

        cmd.execute(&client, region, &path, &[object]).await
    }

    #[tokio::test]
    async fn smoke_s3_delete() -> Result<(), Error> {
        let mock = MockRequestDispatcher::with_status(200).with_body(
            r#"
<?xml version="1.0" encoding="UTF8"?>
<DeleteResult xmlns="http://s3.amazonaws.com/doc/20060301/">
  <Deleted>
    <Key>sample1.txt</Key>
  </Deleted>
  <Deleted>
    <Key>sample2.txt</Key>
  </Deleted>
</DeleteResult>"#,
        );

        let client = S3Client::new_with(mock, MockCredentialsProvider, Region::UsEast1);

        let objects = &[
            Object {
                e_tag: Some("9d48114aa7c18f9d68aa20086dbb7756".to_string()),
                key: Some("sample1.txt".to_string()),
                last_modified: Some("2017-07-19T19:04:17.000Z".to_string()),
                owner: None,
                size: Some(4997288),
                storage_class: Some("STANDARD".to_string()),
            },
            Object {
                e_tag: Some("9d48114aa7c18f9d68aa20086dbb7756".to_string()),
                key: Some("sample2.txt".to_string()),
                last_modified: Some("2017-07-19T19:04:17.000Z".to_string()),
                owner: None,
                size: Some(4997288),
                storage_class: Some("STANDARD".to_string()),
            },
        ];

        let cmd = MultipleDelete {};
        let path = "s3://testbucket".parse()?;

        let res = cmd.execute(&client, "us-east-1", &path, objects).await;
        assert!(res.is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn smoke_s3_set_public() -> Result<(), Error> {
        let mock = MockRequestDispatcher::with_status(200);
        let client = S3Client::new_with(mock, MockCredentialsProvider, Region::UsEast1);

        let objects = &[
            Object {
                e_tag: Some("9d48114aa7c18f9d68aa20086dbb7756".to_string()),
                key: Some("sample1.txt".to_string()),
                last_modified: Some("2017-07-19T19:04:17.000Z".to_string()),
                owner: None,
                size: Some(4997288),
                storage_class: Some("STANDARD".to_string()),
            },
            Object {
                e_tag: Some("9d48114aa7c18f9d68aa20086dbb7756".to_string()),
                key: Some("sample2.txt".to_string()),
                last_modified: Some("2017-07-19T19:04:17.000Z".to_string()),
                owner: None,
                size: Some(4997288),
                storage_class: Some("STANDARD".to_string()),
            },
        ];

        let cmd = SetPublic {};
        let path = "s3://testbucket".parse()?;

        let res = cmd.execute(&client, "us-east-1", &path, objects).await;
        assert!(res.is_ok());

        Ok(())
    }

    #[tokio::test]
    async fn smoke_s3_download() -> Result<(), Error> {
        let test_data = "testdata";
        let mock = MockRequestDispatcher::with_status(200).with_body(test_data);
        let client = S3Client::new_with(mock, MockCredentialsProvider, Region::UsEast1);
        let filename = "sample1.txt";

        let objects = &[Object {
            e_tag: Some("9d48114aa7c18f9d68aa20086dbb7756".to_string()),
            key: Some(filename.to_string()),
            last_modified: Some("2017-07-19T19:04:17.000Z".to_string()),
            owner: None,
            size: Some(4997288),
            storage_class: Some("STANDARD".to_string()),
        }];

        let target = Builder::new()
            .prefix("s3_download")
            .tempdir()
            .unwrap()
            .path()
            .to_path_buf();

        let cmd = Download {
            destination: target.to_str().unwrap().to_owned(),
            force: false,
        };

        let path = "s3://testbucket".parse()?;

        let res = cmd.execute(&client, "us-east-1", &path, objects).await;
        assert!(res.is_ok());

        let file = target.join(&filename);

        let mut f = File::open(file).expect("file not found");
        let mut contents = String::new();
        f.read_to_string(&mut contents)
            .expect("something went wrong reading the file");

        assert_eq!(contents, test_data);

        remove_dir_all(&target).unwrap();
        Ok(())
    }

    #[tokio::test]
    async fn smoke_s3_set_tags() -> Result<(), Error> {
        let mock = MockRequestDispatcher::with_status(200);
        let client = S3Client::new_with(mock, MockCredentialsProvider, Region::UsEast1);

        let objects = &[
            Object {
                e_tag: Some("9d48114aa7c18f9d68aa20086dbb7756".to_string()),
                key: Some("sample1.txt".to_string()),
                last_modified: Some("2017-07-19T19:04:17.000Z".to_string()),
                owner: None,
                size: Some(4997288),
                storage_class: Some("STANDARD".to_string()),
            },
            Object {
                e_tag: Some("9d48114aa7c18f9d68aa20086dbb7756".to_string()),
                key: Some("sample2.txt".to_string()),
                last_modified: Some("2017-07-19T19:04:17.000Z".to_string()),
                owner: None,
                size: Some(4997288),
                storage_class: Some("STANDARD".to_string()),
            },
        ];

        let tags = vec![
            FindTag {
                key: "testkey1".to_owned(),
                value: "testvalue1".to_owned(),
            },
            FindTag {
                key: "testkey2".to_owned(),
                value: "testvalue2".to_owned(),
            },
        ];

        let cmd = SetTags { tags };
        let path = "s3://testbucket".parse()?;

        let res = cmd.execute(&client, "us-east-1", &path, objects).await;
        assert!(res.is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn smoke_s3_list_tags() -> Result<(), Error> {
        let mock = MockRequestDispatcher::with_status(200);
        let client = S3Client::new_with(mock, MockCredentialsProvider, Region::UsEast1);

        let objects = &[
            Object {
                e_tag: Some("9d48114aa7c18f9d68aa20086dbb7756".to_string()),
                key: Some("sample1.txt".to_string()),
                last_modified: Some("2017-07-19T19:04:17.000Z".to_string()),
                owner: None,
                size: Some(4997288),
                storage_class: Some("STANDARD".to_string()),
            },
            Object {
                e_tag: Some("9d48114aa7c18f9d68aa20086dbb7756".to_string()),
                key: Some("sample2.txt".to_string()),
                last_modified: Some("2017-07-19T19:04:17.000Z".to_string()),
                owner: None,
                size: Some(4997288),
                storage_class: Some("STANDARD".to_string()),
            },
        ];

        let cmd = ListTags {};
        let path = "s3://testbucket".parse()?;

        let res = cmd.execute(&client, "us-east-1", &path, objects).await;
        assert!(res.is_ok());

        Ok(())
    }

    #[tokio::test]
    async fn smoke_s3_copy() -> Result<(), Error> {
        let mock = MockRequestDispatcher::with_status(200);
        let client = S3Client::new_with(mock, MockCredentialsProvider, Region::UsEast1);

        let object = Object {
            e_tag: Some("9d48114aa7c18f9d68aa20086dbb7756".to_string()),
            key: Some("key".to_string()),
            last_modified: Some("2017-07-19T19:04:17.000Z".to_string()),
            owner: None,
            size: Some(4997288),
            storage_class: Some("STANDARD".to_string()),
        };

        let cmd = S3Copy {
            destination: ("s3://test/1").parse()?,
            flat: false,
        };

        let copy_path = "s3://test/1".parse()?;

        let res = cmd
            .execute(&client, "us-east-1", &copy_path, &[object])
            .await;
        assert!(res.is_ok());
        Ok(())
    }

    #[tokio::test]
    async fn smoke_s3_move() -> Result<(), Error> {
        let mock = MockRequestDispatcher::with_status(200);
        let client = S3Client::new_with(mock, MockCredentialsProvider, Region::UsEast1);

        let object = Object {
            e_tag: Some("9d48114aa7c18f9d68aa20086dbb7756".to_string()),
            key: Some("key".to_string()),
            last_modified: Some("2017-07-19T19:04:17.000Z".to_string()),
            owner: None,
            size: Some(4997288),
            storage_class: Some("STANDARD".to_string()),
        };

        let cmd = S3Move {
            destination: ("s3://test/1").parse()?,
            flat: false,
        };

        let copy_path = "s3://test/1".parse()?;

        let res = cmd
            .execute(&client, "us-east-1", &copy_path, &[object])
            .await;
        assert!(res.is_ok());
        Ok(())
    }
}
