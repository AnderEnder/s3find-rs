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

use failure::Error;
use futures::stream::Stream;
use futures::Future;

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
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct ExecStatus {
    pub status: ExitStatus,
    pub runcommand: String,
}

pub trait RunCommand {
    fn execute(
        &self,
        client: &S3Client,
        region: &str,
        path: &S3path,
        list: &[&Object],
    ) -> Result<(), Error>;
}

impl RunCommand for FastPrint {
    fn execute(
        &self,
        _c: &S3Client,
        _r: &str,
        path: &S3path,
        list: &[&Object],
    ) -> Result<(), Error> {
        for x in list {
            println!(
                "s3://{}/{}",
                &path.bucket,
                x.key.as_ref().unwrap_or(&"".to_string())
            );
        }
        Ok(())
    }
}

impl RunCommand for AdvancedPrint {
    fn execute(
        &self,
        _c: &S3Client,
        _r: &str,
        path: &S3path,
        list: &[&Object],
    ) -> Result<(), Error> {
        for x in list {
            println!(
                "{0} {1:?} {2} {3} s3://{4}/{5} {6}",
                x.e_tag.as_ref().unwrap_or(&"NoEtag".to_string()),
                x.owner.as_ref().map(|x| x.display_name.as_ref()),
                x.size.as_ref().unwrap_or(&0),
                x.last_modified.as_ref().unwrap_or(&"NoTime".to_string()),
                &path.bucket,
                x.key.as_ref().unwrap_or(&"".to_string()),
                x.storage_class.as_ref().unwrap_or(&"NoStorage".to_string()),
            );
        }
        Ok(())
    }
}

impl Exec {
    pub fn exec(&self, key: &str) -> Result<ExecStatus, Error> {
        let scommand = self.utility.replace("{}", key);

        let mut command_args = scommand.split(' ');
        let command_name = command_args.next().ok_or(FunctionError::CommandlineParse)?;

        let mut rcommand = Command::new(command_name);
        for arg in command_args {
            rcommand.arg(arg);
        }

        let output = rcommand.output()?;
        let output_str = String::from_utf8_lossy(&output.stdout).to_string();
        print!("{}", &output_str);

        Ok(ExecStatus {
            status: output.status,
            runcommand: scommand.clone(),
        })
    }
}

impl RunCommand for Exec {
    fn execute(
        &self,
        _: &S3Client,
        _r: &str,
        path: &S3path,
        list: &[&Object],
    ) -> Result<(), Error> {
        for x in list {
            let key = x.key.as_ref().map(String::as_str).unwrap_or("");
            let path = format!("s3://{}/{}", &path.bucket, key);
            self.exec(&path)?;
        }
        Ok(())
    }
}

impl RunCommand for MultipleDelete {
    fn execute(
        &self,
        client: &S3Client,
        _r: &str,
        path: &S3path,
        list: &[&Object],
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

        let result = client.delete_objects(request).sync();

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

impl RunCommand for SetTags {
    fn execute(
        &self,
        client: &S3Client,
        _r: &str,
        path: &S3path,
        list: &[&Object],
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

            client.put_object_tagging(request).sync()?;

            println!("tags are set for: s3://{}/{}", &path.bucket, &key);
        }
        Ok(())
    }
}

impl RunCommand for ListTags {
    fn execute(
        &self,
        client: &S3Client,
        _r: &str,
        path: &S3path,
        list: &[&Object],
    ) -> Result<(), Error> {
        for object in list {
            let key = object.key.as_ref().ok_or(FunctionError::ObjectFieldError)?;

            let request = GetObjectTaggingRequest {
                bucket: path.bucket.to_string(),
                key: key.to_owned(),
                ..Default::default()
            };

            let tag_output = client.get_object_tagging(request).sync()?;

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

impl RunCommand for SetPublic {
    fn execute(
        &self,
        client: &S3Client,
        region: &str,
        path: &S3path,
        list: &[&Object],
    ) -> Result<(), Error> {
        // let region_str = self.0.name();
        for object in list {
            let key = object.key.as_ref().ok_or(FunctionError::ObjectFieldError)?;

            let request = PutObjectAclRequest {
                bucket: path.bucket.to_owned(),
                key: key.to_owned(),
                acl: Some("public-read".to_string()),
                ..Default::default()
            };

            client.put_object_acl(request).sync()?;

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

impl RunCommand for Download {
    fn execute(
        &self,
        client: &S3Client,
        _r: &str,
        path: &S3path,
        list: &[&Object],
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

            let result = client.get_object(request).sync()?;

            let stream = result.body.ok_or(FunctionError::S3FetchBodyError)?;

            fs::create_dir_all(&dir_path)?;
            let mut output = File::create(&file_path)?;

            let _r = stream
                .for_each(|buf| {
                    output.write_all(&buf)?;
                    count += buf.len() as u64;
                    pb.set_position(count);
                    Ok(())
                })
                .wait()?;
        }
        Ok(())
    }
}

impl RunCommand for S3Copy {
    fn execute(
        &self,
        client: &S3Client,
        _r: &str,
        path: &S3path,
        list: &[&Object],
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

            client.copy_object(request).sync()?;
        }
        Ok(())
    }
}

impl RunCommand for S3Move {
    fn execute(
        &self,
        client: &S3Client,
        _r: &str,
        path: &S3path,
        list: &[&Object],
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

            client.copy_object(request).sync()?;
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

        client.delete_objects(request).sync()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusoto_core::Region;

    #[test]
    fn advanced_print_test() -> Result<(), Error> {
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
        let path = S3path {
            bucket: "test".to_owned(),
            prefix: None,
        };

        cmd.execute(&client, region, &path, &[&object])
    }

    #[test]
    fn fastprint_test() -> Result<(), Error> {
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
        let path = S3path {
            bucket: "test".to_owned(),
            prefix: None,
        };

        cmd.execute(&client, region, &path, &[&object])
    }
}
