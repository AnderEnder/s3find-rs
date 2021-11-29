use std::fs;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::process::Command;
use std::process::ExitStatus;

use anyhow::Error;
use async_trait::async_trait;
use futures::future;
use futures::stream::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};

use aws_sdk_s3::model::{Delete, Object, ObjectCannedAcl, ObjectIdentifier, Tag, Tagging};
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
            object.size,
            object.last_modified,
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
            .map(|x| ObjectIdentifier::builder().set_key(x.key.clone()).build())
            .collect();

        let objects = Delete::builder().set_objects(Some(key_list)).build();

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
                .map(|x| {
                    Tag::builder()
                        .key(x.key.clone())
                        .value(x.value.clone())
                        .build()
                })
                .collect();

            let tagging = Tagging::builder().set_tag_set(Some(tags)).build();

            client
                .put_object_tagging()
                .bucket(path.bucket.to_owned())
                .set_key(object.key.clone())
                .set_tagging(Some(tagging))
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
                .map(|tags| {
                    tags.into_iter()
                        .map(|x| format!("{}:{}", x.key.unwrap(), x.value.unwrap()))
                        .collect::<Vec<String>>()
                        .join(",")
                })
                .unwrap_or_default();

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

            let size = object.size as u64;
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
                .get_object()
                .bucket(&path.bucket)
                .key(key)
                .send()
                .await?
                .body;

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
            .map(|x| ObjectIdentifier::builder().set_key(x.key.clone()).build())
            .collect();

        let delete = Delete::builder().set_objects(Some(key_list)).build();

        client
            .delete_objects()
            .bucket(path.bucket.clone())
            .set_delete(Some(delete))
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
    // use rusoto_core::Region;
    // use rusoto_mock::*;
    // use std::fs::File;
    // use std::io::prelude::*;
    // use tempfile::Builder;

    // #[test]
    // fn test_advanced_print_object() -> Result<(), Error> {
    //     let mut buf = Vec::new();
    //     let cmd = AdvancedPrint {};
    //     let bucket = "test";

    //     let object = Object {
    //         e_tag: Some("9d48114aa7c18f9d68aa20086dbb7756".to_string()),
    //         key: Some("somepath/otherpath".to_string()),
    //         last_modified: Some("2017-07-19T19:04:17.000Z".to_string()),
    //         owner: None,
    //         size: Some(4_997_288),
    //         storage_class: Some("STANDARD".to_string()),
    //     };

    //     cmd.print_object(&mut buf, bucket, &object)?;
    //     let out = std::str::from_utf8(&buf)?;

    //     assert!(out.contains("9d48114aa7c18f9d68aa20086dbb7756"));
    //     assert!(out.contains("None"));
    //     assert!(out.contains("4997288"));
    //     assert!(out.contains("2017-07-19T19:04:17.000Z"));
    //     assert!(out.contains("s3://test/somepath/otherpath"));
    //     assert!(out.contains("STANDARD"));
    //     Ok(())
    // }

    // #[test]
    // fn test_fast_print_object() -> Result<(), Error> {
    //     let mut buf = Vec::new();
    //     let cmd = FastPrint {};
    //     let bucket = "test";

    //     let object = Object {
    //         e_tag: Some("9d48114aa7c18f9d68aa20086dbb7756".to_string()),
    //         key: Some("somepath/otherpath".to_string()),
    //         last_modified: Some("2017-07-19T19:04:17.000Z".to_string()),
    //         owner: None,
    //         size: Some(4_997_288),
    //         storage_class: Some("STANDARD".to_string()),
    //     };

    //     cmd.print_object(&mut buf, bucket, &object)?;
    //     let out = std::str::from_utf8(&buf)?;

    //     assert!(out.contains("s3://test/somepath/otherpath"));
    //     Ok(())
    // }

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

    // #[tokio::test]
    // async fn test_advanced_print() -> Result<(), Error> {
    //     let object = Object {
    //         e_tag: Some("9d48114aa7c18f9d68aa20086dbb7756".to_string()),
    //         key: Some("somepath/otherpath".to_string()),
    //         last_modified: Some("2017-07-19T19:04:17.000Z".to_string()),
    //         owner: None,
    //         size: Some(4_997_288),
    //         storage_class: Some("STANDARD".to_string()),
    //     };

    //     let cmd = Cmd::Print(AdvancedPrint {}).downcast();
    //     let region = "us-east-1";
    //     let client = S3Client::new(Region::UsEast1);
    //     let path = S3Path {
    //         bucket: "test".to_owned(),
    //         prefix: None,
    //     };

    //     cmd.execute(&client, region, &path, &[object]).await?;
    //     Ok(())
    // }
    // #[tokio::test]
    // async fn test_fastprint() -> Result<(), Error> {
    //     let object = Object {
    //         e_tag: Some("9d48114aa7c18f9d68aa20086dbb7756".to_string()),
    //         key: Some("somepath/otherpath".to_string()),
    //         last_modified: Some("2017-07-19T19:04:17.000Z".to_string()),
    //         owner: None,
    //         size: Some(4_997_288),
    //         storage_class: Some("STANDARD".to_string()),
    //     };

    //     let cmd = Cmd::Ls(FastPrint {}).downcast();
    //     let region = "us-east-1";
    //     let client = S3Client::new(Region::UsEast1);
    //     let path = S3Path {
    //         bucket: "test".to_owned(),
    //         prefix: None,
    //     };

    //     cmd.execute(&client, region, &path, &[object]).await?;
    //     Ok(())
    // }

    // #[tokio::test]
    // async fn smoke_donothing() -> Result<(), Error> {
    //     let object = Object {
    //         e_tag: Some("9d48114aa7c18f9d68aa20086dbb7756".to_string()),
    //         key: Some("somepath/otherpath".to_string()),
    //         last_modified: Some("2017-07-19T19:04:17.000Z".to_string()),
    //         owner: None,
    //         size: Some(4_997_288),
    //         storage_class: Some("STANDARD".to_string()),
    //     };

    //     let cmd = Cmd::Nothing(DoNothing {}).downcast();
    //     let region = "us-east-1";
    //     let client = S3Client::new(Region::UsEast1);
    //     let path = S3Path {
    //         bucket: "test".to_owned(),
    //         prefix: None,
    //     };

    //     cmd.execute(&client, region, &path, &[object]).await
    // }

    //     #[tokio::test]
    //     async fn smoke_exec() -> Result<(), Error> {
    //         let object = Object {
    //             e_tag: Some("9d48114aa7c18f9d68aa20086dbb7756".to_string()),
    //             key: Some("somepath/otherpath".to_string()),
    //             last_modified: Some("2017-07-19T19:04:17.000Z".to_string()),
    //             owner: None,
    //             size: Some(4_997_288),
    //             storage_class: Some("STANDARD".to_string()),
    //         };

    //         let cmd = Cmd::Exec(Exec {
    //             utility: "echo {}".to_owned(),
    //         })
    //         .downcast();
    //         let region = "us-east-1";
    //         let client = S3Client::new(Region::UsEast1);
    //         let path = S3Path {
    //             bucket: "test".to_owned(),
    //             prefix: None,
    //         };

    //         cmd.execute(&client, region, &path, &[object]).await
    //     }

    //     #[tokio::test]
    //     async fn smoke_s3_delete() -> Result<(), Error> {
    //         let mock = MockRequestDispatcher::with_status(200).with_body(
    //             r#"
    // <?xml version="1.0" encoding="UTF8"?>
    // <DeleteResult xmlns="http://s3.amazonaws.com/doc/20060301/">
    //   <Deleted>
    //     <Key>sample1.txt</Key>
    //   </Deleted>
    //   <Deleted>
    //     <Key>sample2.txt</Key>
    //   </Deleted>
    // </DeleteResult>"#,
    //         );

    //         let client = S3Client::new_with(mock, MockCredentialsProvider, Region::UsEast1);

    //         let objects = &[
    //             Object {
    //                 e_tag: Some("9d48114aa7c18f9d68aa20086dbb7756".to_string()),
    //                 key: Some("sample1.txt".to_string()),
    //                 last_modified: Some("2017-07-19T19:04:17.000Z".to_string()),
    //                 owner: None,
    //                 size: Some(4997288),
    //                 storage_class: Some("STANDARD".to_string()),
    //             },
    //             Object {
    //                 e_tag: Some("9d48114aa7c18f9d68aa20086dbb7756".to_string()),
    //                 key: Some("sample2.txt".to_string()),
    //                 last_modified: Some("2017-07-19T19:04:17.000Z".to_string()),
    //                 owner: None,
    //                 size: Some(4997288),
    //                 storage_class: Some("STANDARD".to_string()),
    //             },
    //         ];

    //         let cmd = Cmd::Delete(MultipleDelete {}).downcast();
    //         let path = "s3://testbucket".parse()?;

    //         let res = cmd.execute(&client, "us-east-1", &path, objects).await;
    //         assert!(res.is_ok());
    //         Ok(())
    //     }

    //     #[tokio::test]
    //     async fn smoke_s3_set_public() -> Result<(), Error> {
    //         let mock = MockRequestDispatcher::with_status(200);
    //         let client = S3Client::new_with(mock, MockCredentialsProvider, Region::UsEast1);

    //         let objects = &[
    //             Object {
    //                 e_tag: Some("9d48114aa7c18f9d68aa20086dbb7756".to_string()),
    //                 key: Some("sample1.txt".to_string()),
    //                 last_modified: Some("2017-07-19T19:04:17.000Z".to_string()),
    //                 owner: None,
    //                 size: Some(4997288),
    //                 storage_class: Some("STANDARD".to_string()),
    //             },
    //             Object {
    //                 e_tag: Some("9d48114aa7c18f9d68aa20086dbb7756".to_string()),
    //                 key: Some("sample2.txt".to_string()),
    //                 last_modified: Some("2017-07-19T19:04:17.000Z".to_string()),
    //                 owner: None,
    //                 size: Some(4997288),
    //                 storage_class: Some("STANDARD".to_string()),
    //             },
    //         ];

    //         let cmd = Cmd::Public(SetPublic {}).downcast();
    //         let path = "s3://testbucket".parse()?;

    //         let res = cmd.execute(&client, "us-east-1", &path, objects).await;
    //         assert!(res.is_ok());

    //         Ok(())
    //     }

    //     #[tokio::test]
    //     async fn smoke_s3_download() -> Result<(), Error> {
    //         let test_data = "testdata";
    //         let mock = MockRequestDispatcher::with_status(200).with_body(test_data);
    //         let client = S3Client::new_with(mock, MockCredentialsProvider, Region::UsEast1);
    //         let filename = "sample1.txt";

    //         let objects = &[Object {
    //             e_tag: Some("9d48114aa7c18f9d68aa20086dbb7756".to_string()),
    //             key: Some(filename.to_string()),
    //             last_modified: Some("2017-07-19T19:04:17.000Z".to_string()),
    //             owner: None,
    //             size: Some(4997288),
    //             storage_class: Some("STANDARD".to_string()),
    //         }];

    //         let tmp_target = Builder::new().prefix("s3_download").tempdir().unwrap();
    //         let target = tmp_target.path().to_path_buf();

    //         let cmd = Cmd::Download(Download {
    //             destination: target.to_str().unwrap().to_owned(),
    //             force: false,
    //         })
    //         .downcast();

    //         let path = "s3://testbucket".parse()?;

    //         let res = cmd.execute(&client, "us-east-1", &path, objects).await;
    //         assert!(res.is_ok());

    //         let file = target.join(&filename);

    //         let mut f = File::open(file).expect("file not found");
    //         let mut contents = String::new();
    //         f.read_to_string(&mut contents)
    //             .expect("something went wrong reading the file");

    //         assert_eq!(contents, test_data);
    //         tmp_target.close().unwrap();

    //         Ok(())
    //     }

    //     #[tokio::test]
    //     async fn smoke_s3_set_tags() -> Result<(), Error> {
    //         let mock = MockRequestDispatcher::with_status(200);
    //         let client = S3Client::new_with(mock, MockCredentialsProvider, Region::UsEast1);

    //         let objects = &[
    //             Object {
    //                 e_tag: Some("9d48114aa7c18f9d68aa20086dbb7756".to_string()),
    //                 key: Some("sample1.txt".to_string()),
    //                 last_modified: Some("2017-07-19T19:04:17.000Z".to_string()),
    //                 owner: None,
    //                 size: Some(4997288),
    //                 storage_class: Some("STANDARD".to_string()),
    //             },
    //             Object {
    //                 e_tag: Some("9d48114aa7c18f9d68aa20086dbb7756".to_string()),
    //                 key: Some("sample2.txt".to_string()),
    //                 last_modified: Some("2017-07-19T19:04:17.000Z".to_string()),
    //                 owner: None,
    //                 size: Some(4997288),
    //                 storage_class: Some("STANDARD".to_string()),
    //             },
    //         ];

    //         let tags = vec![
    //             FindTag {
    //                 key: "testkey1".to_owned(),
    //                 value: "testvalue1".to_owned(),
    //             },
    //             FindTag {
    //                 key: "testkey2".to_owned(),
    //                 value: "testvalue2".to_owned(),
    //             },
    //         ];

    //         let cmd = Cmd::Tags(SetTags { tags }).downcast();
    //         let path = "s3://testbucket".parse()?;

    //         let res = cmd.execute(&client, "us-east-1", &path, objects).await;
    //         assert!(res.is_ok());
    //         Ok(())
    //     }

    //     #[tokio::test]
    //     async fn smoke_s3_list_tags() -> Result<(), Error> {
    //         let mock = MockRequestDispatcher::with_status(200);
    //         let client = S3Client::new_with(mock, MockCredentialsProvider, Region::UsEast1);

    //         let objects = &[
    //             Object {
    //                 e_tag: Some("9d48114aa7c18f9d68aa20086dbb7756".to_string()),
    //                 key: Some("sample1.txt".to_string()),
    //                 last_modified: Some("2017-07-19T19:04:17.000Z".to_string()),
    //                 owner: None,
    //                 size: Some(4997288),
    //                 storage_class: Some("STANDARD".to_string()),
    //             },
    //             Object {
    //                 e_tag: Some("9d48114aa7c18f9d68aa20086dbb7756".to_string()),
    //                 key: Some("sample2.txt".to_string()),
    //                 last_modified: Some("2017-07-19T19:04:17.000Z".to_string()),
    //                 owner: None,
    //                 size: Some(4997288),
    //                 storage_class: Some("STANDARD".to_string()),
    //             },
    //         ];

    //         let cmd = Cmd::LsTags(ListTags {}).downcast();
    //         let path = "s3://testbucket".parse()?;

    //         let res = cmd.execute(&client, "us-east-1", &path, objects).await;
    //         assert!(res.is_ok());

    //         Ok(())
    //     }

    //     #[tokio::test]
    //     async fn smoke_s3_copy() -> Result<(), Error> {
    //         let mock = MockRequestDispatcher::with_status(200);
    //         let client = S3Client::new_with(mock, MockCredentialsProvider, Region::UsEast1);

    //         let object = [Object {
    //             e_tag: Some("9d48114aa7c18f9d68aa20086dbb7756".to_string()),
    //             key: Some("path/key".to_string()),
    //             last_modified: Some("2017-07-19T19:04:17.000Z".to_string()),
    //             owner: None,
    //             size: Some(4997288),
    //             storage_class: Some("STANDARD".to_string()),
    //         }];

    //         let cmd = Cmd::Copy(S3Copy {
    //             destination: ("s3://test/1").parse()?,
    //             flat: false,
    //         })
    //         .downcast();

    //         let copy_path = "s3://test/1".parse()?;

    //         let res = cmd.execute(&client, "us-east-1", &copy_path, &object).await;
    //         assert!(res.is_ok());

    //         let cmd_flat = S3Copy {
    //             destination: ("s3://test/1").parse()?,
    //             flat: true,
    //         };

    //         let res = cmd_flat
    //             .execute(&client, "us-east-1", &copy_path, &object)
    //             .await;
    //         assert!(res.is_ok());
    //         Ok(())
    //     }

    //     #[tokio::test]
    //     async fn smoke_s3_move() -> Result<(), Error> {
    //         let mock = MockRequestDispatcher::with_status(200);
    //         let client = S3Client::new_with(mock, MockCredentialsProvider, Region::UsEast1);

    //         let object = [Object {
    //             e_tag: Some("9d48114aa7c18f9d68aa20086dbb7756".to_string()),
    //             key: Some("key".to_string()),
    //             last_modified: Some("2017-07-19T19:04:17.000Z".to_string()),
    //             owner: None,
    //             size: Some(4997288),
    //             storage_class: Some("STANDARD".to_string()),
    //         }];

    //         let cmd = Cmd::Move(S3Move {
    //             destination: ("s3://test/1").parse()?,
    //             flat: false,
    //         })
    //         .downcast();

    //         let copy_path = "s3://test/1".parse()?;

    //         let res = cmd.execute(&client, "us-east-1", &copy_path, &object).await;
    //         assert!(res.is_ok());

    //         let cmd_flat = S3Move {
    //             destination: ("s3://test/1").parse()?,
    //             flat: true,
    //         };

    //         let res = cmd_flat
    //             .execute(&client, "us-east-1", &copy_path, &object)
    //             .await;
    //         assert!(res.is_ok());
    //         Ok(())
    //     }

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
