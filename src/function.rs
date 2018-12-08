use rusoto_s3::{
    CopyObjectRequest, Delete, DeleteObjectsRequest, GetObjectRequest, GetObjectTaggingRequest,
    Object, ObjectIdentifier, PutObjectAclRequest, PutObjectTaggingRequest, S3Client, Tagging, S3,
};
use std::process::Command;
use std::process::ExitStatus;

use std::fs;
use std::fs::File;
use std::io::Write;
use std::path::Path;

use rusoto_core::Region;

use failure::Error;
use futures::stream::Stream;
use futures::Future;

use indicatif::{ProgressBar, ProgressStyle};

use crate::error::*;

pub fn fprint(bucket: &str, item: &Object) {
    println!(
        "s3://{}/{}",
        bucket,
        item.key.as_ref().unwrap_or(&"".to_string())
    );
}

pub fn advanced_print(bucket: &str, item: &Object) {
    println!(
        "{} {:?} {} {} s3://{}/{} {}",
        item.e_tag.as_ref().unwrap_or(&"NoEtag".to_string()),
        item.owner.as_ref().map(|x| x.display_name.as_ref()),
        item.size.as_ref().unwrap_or(&0),
        item.last_modified.as_ref().unwrap_or(&"NoTime".to_string()),
        bucket,
        item.key.as_ref().unwrap_or(&"".to_string()),
        item.storage_class
            .as_ref()
            .unwrap_or(&"NoStorage".to_string()),
    );
}

#[derive(Debug, PartialEq, Clone)]
pub struct ExecStatus {
    pub status: ExitStatus,
    pub runcommand: String,
}

pub fn exec(command: &str, key: &str) -> Result<ExecStatus, Error> {
    let scommand = command.replace("{}", key);

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

pub fn s3_delete(client: &S3Client, bucket: &str, list: &[&Object]) -> Result<(), Error> {
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
        bucket: bucket.to_string(),
        delete: Delete {
            objects: key_list,
            quiet: None,
        },
        ..Default::default()
    };

    let result = client.delete_objects(request).sync()?;

    if let Some(deleted_list) = result.deleted {
        for object in deleted_list {
            println!(
                "deleted: s3://{}/{}",
                bucket,
                object.key.as_ref().unwrap_or(&"".to_string())
            );
        }
    }

    Ok(())
}

pub fn s3_download(
    client: &S3Client,
    bucket: &str,
    list: &[&Object],
    target: &str,
    force: bool,
) -> Result<(), Error> {
    for object in list {
        let key = object.key.as_ref().ok_or(FunctionError::ObjectFieldError)?;
        let request = GetObjectRequest {
            bucket: bucket.to_owned(),
            key: key.to_owned(),
            ..Default::default()
        };

        let size = (*object
            .size
            .as_ref()
            .ok_or(FunctionError::ObjectFieldError)?) as u64;
        let file_path = Path::new(target).join(key);
        let dir_path = file_path.parent().ok_or(FunctionError::ParentPathParse)?;

        let mut count: u64 = 0;
        let pb = ProgressBar::new(size);
        pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta})")
            .progress_chars("#>-"));
        println!(
            "downloading: s3://{}/{} => {}",
            bucket,
            &key,
            file_path
                .to_str()
                .ok_or(FunctionError::FileNameParseError)?
        );

        if file_path.exists() && !force {
            return Err(FunctionError::PresentFileError.into());
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
            .wait();
    }

    Ok(())
}

pub fn s3_set_tags(
    client: &S3Client,
    bucket: &str,
    list: &[&Object],
    tags: &Tagging,
) -> Result<(), Error> {
    for object in list {
        let key = object.key.as_ref().ok_or(FunctionError::ObjectFieldError)?;

        let request = PutObjectTaggingRequest {
            bucket: bucket.to_owned(),
            key: key.to_owned(),
            tagging: tags.clone(),
            ..Default::default()
        };

        client.put_object_tagging(request).sync()?;

        println!("tags are set for: s3://{}/{}", bucket, &key);
    }

    Ok(())
}

pub fn s3_list_tags(client: &S3Client, bucket: &str, list: &[&Object]) -> Result<(), Error> {
    for object in list {
        let key = object.key.as_ref().ok_or(FunctionError::ObjectFieldError)?;

        let request = GetObjectTaggingRequest {
            bucket: bucket.to_owned(),
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
            bucket,
            object.key.as_ref().unwrap_or(&"".to_string()),
            tags,
        );
    }

    Ok(())
}

fn s3_public_url(key: &str, bucket: &str, region: &str) -> String {
    match region {
        "us-east-1" => format!("http://{}.s3.amazonaws.com/{}", bucket, key),
        _ => format!("http://{}.s3-{}.amazonaws.com/{}", bucket, region, key),
    }
}

pub fn s3_set_public(
    client: &S3Client,
    bucket: &str,
    list: &[&Object],
    region: &Region,
) -> Result<(), Error> {
    let region_str = region.name();
    for object in list {
        let key = object.key.as_ref().ok_or(FunctionError::ObjectFieldError)?;

        let request = PutObjectAclRequest {
            bucket: bucket.to_owned(),
            key: key.to_owned(),
            acl: Some("public-read".to_string()),
            ..Default::default()
        };

        client.put_object_acl(request).sync()?;
        let url = s3_public_url(key, bucket, region_str);
        println!("{} {}", &key, url);
    }

    Ok(())
}

pub fn s3_copy(
    client: &S3Client,
    bucket: &str,
    list: &[&Object],
    target_bucket: &str,
    target_path: &str,
    flat: bool,
    delete: bool,
) -> Result<(), Error> {
    let action = if delete { "moving" } else { "copying" };

    for object in list {
        let key = object.key.as_ref().ok_or(FunctionError::ObjectFieldError)?;

        let key2 = if flat {
            Path::new(key)
                .file_name()
                .ok_or(FunctionError::PathConverError)?
                .to_str()
                .ok_or(FunctionError::PathConverError)?
        } else {
            key
        };

        let target_key = Path::new(target_path).join(key2);
        let target_key_str = target_key.to_str().ok_or(FunctionError::PathConverError)?;
        let source_path = format!("{}/{}", bucket, key);

        println!(
            "{}: s3://{} => s3://{}/{}",
            action, source_path, target_bucket, target_key_str,
        );

        let request = CopyObjectRequest {
            bucket: target_bucket.to_owned(),
            key: target_key_str.to_owned(),
            copy_source: source_path,
            ..Default::default()
        };

        client.copy_object(request).sync()?;
    }

    if delete {
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
            bucket: bucket.to_string(),
            delete: Delete {
                objects: key_list,
                quiet: None,
            },
            ..Default::default()
        };

        let _result = client.delete_objects(request).sync()?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use rusoto_mock::*;
    use tempfile::Builder;
    use super::*;
    use rusoto_core::Region;
    use rusoto_s3::Tag;

    use std::fs::remove_dir_all;
    use std::fs::File;
    use std::io::prelude::*;

    #[test]
    fn exec_true() {
        let exec_status = exec("true", "").unwrap();
        assert!(exec_status.status.success(), "Exit code of true is 0");
    }

    #[test]
    fn exec_false() {
        let exec_status = exec("false", "");
        assert!(
            !exec_status.unwrap().status.success(),
            "Exit code of false is 1"
        );
    }

    #[test]
    fn exec_echo_multiple() {
        let exec_status = exec("echo Hello world1", "").unwrap();

        assert!(exec_status.status.success(), "Exit code of echo is 0");
        assert_eq!(
            exec_status.runcommand, "echo Hello world1",
            "Output of echo is 'Hello world1'"
        );
    }

    #[test]
    #[should_panic]
    fn exec_incorrect_command() {
        let exec_status = exec("jbrwuDxPy4ck", "");
        assert!(
            !exec_status.unwrap().status.success(),
            "Exit code should not be success"
        );
    }

    #[test]
    fn exec_echo_interpolation() {
        let exec_status = exec("echo Hello {}", "world2").unwrap();

        assert!(exec_status.status.success(), "Exit code of echo is 0");
        assert_eq!(
            exec_status.runcommand, "echo Hello world2",
            "String should interpolated and printed"
        );
    }

    #[test]
    fn advanced_print_test() {
        let object = Object {
            e_tag: Some("9d48114aa7c18f9d68aa20086dbb7756".to_string()),
            key: Some("somepath/otherpath".to_string()),
            last_modified: Some("2017-07-19T19:04:17.000Z".to_string()),
            owner: None,
            size: Some(4997288),
            storage_class: Some("STANDARD".to_string()),
        };

        advanced_print("bucket", &object);
    }

    #[test]
    fn fprint_test() {
        let object = Object {
            e_tag: Some("9d48114aa7c18f9d68aa20086dbb7756".to_string()),
            key: Some("somepath/otherpath".to_string()),
            last_modified: Some("2017-07-19T19:04:17.000Z".to_string()),
            owner: None,
            size: Some(4997288),
            storage_class: Some("STANDARD".to_string()),
        };

        fprint("bucket", &object);
    }

    #[test]
    fn smoke_s3_delete() {
        let mock = MockRequestDispatcher::with_status(200).with_body(
            r#"
<?xml version="1.0" encoding="UTF-8"?>
<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Deleted>
    <Key>sample1.txt</Key>
  </Deleted>
  <Deleted>
    <Key>sample2.txt</Key>
  </Deleted>
</DeleteResult>"#,
        );

        let client = S3Client::new_with(mock, MockCredentialsProvider, Region::UsEast1);

        let objects: &[&Object] = &[
            &Object {
                e_tag: Some("9d48114aa7c18f9d68aa20086dbb7756".to_string()),
                key: Some("sample1.txt".to_string()),
                last_modified: Some("2017-07-19T19:04:17.000Z".to_string()),
                owner: None,
                size: Some(4997288),
                storage_class: Some("STANDARD".to_string()),
            },
            &Object {
                e_tag: Some("9d48114aa7c18f9d68aa20086dbb7756".to_string()),
                key: Some("sample2.txt".to_string()),
                last_modified: Some("2017-07-19T19:04:17.000Z".to_string()),
                owner: None,
                size: Some(4997288),
                storage_class: Some("STANDARD".to_string()),
            },
        ];

        let res = s3_delete(&client, "bucket", objects);

        assert!(res.is_ok());
    }

    #[test]
    fn smoke_s3_download() {
        let test_data = "testdata";
        let mock = MockRequestDispatcher::with_status(200).with_body(test_data);
        let client = S3Client::new_with(mock, MockCredentialsProvider, Region::UsEast1);
        let filename = "sample1.txt";

        let objects: &[&Object] = &[&Object {
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

        let target_str = target.to_str().unwrap();

        let res = s3_download(&client, "bucket", objects, target_str, false);
        assert!(res.is_ok());

        let file = target.join(&filename);

        let mut f = File::open(file).expect("file not found");
        let mut contents = String::new();
        f.read_to_string(&mut contents)
            .expect("something went wrong reading the file");

        assert_eq!(contents, test_data);

        remove_dir_all(&target).unwrap();
    }

    #[test]
    fn smoke_s3_set_public() {
        let mock = MockRequestDispatcher::with_status(200);
        let client = S3Client::new_with(mock, MockCredentialsProvider, Region::UsEast1);

        let objects: &[&Object] = &[
            &Object {
                e_tag: Some("9d48114aa7c18f9d68aa20086dbb7756".to_string()),
                key: Some("sample1.txt".to_string()),
                last_modified: Some("2017-07-19T19:04:17.000Z".to_string()),
                owner: None,
                size: Some(4997288),
                storage_class: Some("STANDARD".to_string()),
            },
            &Object {
                e_tag: Some("9d48114aa7c18f9d68aa20086dbb7756".to_string()),
                key: Some("sample2.txt".to_string()),
                last_modified: Some("2017-07-19T19:04:17.000Z".to_string()),
                owner: None,
                size: Some(4997288),
                storage_class: Some("STANDARD".to_string()),
            },
        ];

        let res = s3_set_public(&client, "testbucket", objects, &Region::UsEast1);
        assert!(res.is_ok());
    }

    #[test]
    fn smoke_s3_set_tags() {
        let mock = MockRequestDispatcher::with_status(200);
        let client = S3Client::new_with(mock, MockCredentialsProvider, Region::UsEast1);

        let objects: &[&Object] = &[
            &Object {
                e_tag: Some("9d48114aa7c18f9d68aa20086dbb7756".to_string()),
                key: Some("sample1.txt".to_string()),
                last_modified: Some("2017-07-19T19:04:17.000Z".to_string()),
                owner: None,
                size: Some(4997288),
                storage_class: Some("STANDARD".to_string()),
            },
            &Object {
                e_tag: Some("9d48114aa7c18f9d68aa20086dbb7756".to_string()),
                key: Some("sample2.txt".to_string()),
                last_modified: Some("2017-07-19T19:04:17.000Z".to_string()),
                owner: None,
                size: Some(4997288),
                storage_class: Some("STANDARD".to_string()),
            },
        ];

        let tags = Tagging {
            tag_set: vec![
                Tag {
                    key: "testkey1".to_owned(),
                    value: "testvalue1".to_owned(),
                },
                Tag {
                    key: "testkey2".to_owned(),
                    value: "testvalue2".to_owned(),
                },
            ],
        };

        let res = s3_set_tags(&client, "testbucket", objects, &tags);
        assert!(res.is_ok());
    }

    #[test]
    fn smoke_s3_list_tags() {
        let mock = MockRequestDispatcher::with_status(200);
        let client = S3Client::new_with(mock, MockCredentialsProvider, Region::UsEast1);

        let objects: &[&Object] = &[
            &Object {
                e_tag: Some("9d48114aa7c18f9d68aa20086dbb7756".to_string()),
                key: Some("sample1.txt".to_string()),
                last_modified: Some("2017-07-19T19:04:17.000Z".to_string()),
                owner: None,
                size: Some(4997288),
                storage_class: Some("STANDARD".to_string()),
            },
            &Object {
                e_tag: Some("9d48114aa7c18f9d68aa20086dbb7756".to_string()),
                key: Some("sample2.txt".to_string()),
                last_modified: Some("2017-07-19T19:04:17.000Z".to_string()),
                owner: None,
                size: Some(4997288),
                storage_class: Some("STANDARD".to_string()),
            },
        ];

        let res = s3_list_tags(&client, "testbucket", objects);
        assert!(res.is_ok());
    }

    #[test]
    fn s3_public_url_test() {
        assert_eq!(
            s3_public_url("key", "bucket", "us-east-1"),
            "http://bucket.s3.amazonaws.com/key"
        );
        assert_eq!(
            s3_public_url("key", "bucket", "us-west-1"),
            "http://bucket.s3-us-west-1.amazonaws.com/key"
        );
    }

    #[test]
    fn smoke_s3_copy() {
        let mock = MockRequestDispatcher::with_status(200);
        let client = S3Client::new_with(mock, MockCredentialsProvider, Region::UsEast1);

        let objects: &[&Object] = &[&Object {
            e_tag: Some("9d48114aa7c18f9d68aa20086dbb7756".to_string()),
            key: Some("key".to_string()),
            last_modified: Some("2017-07-19T19:04:17.000Z".to_string()),
            owner: None,
            size: Some(4997288),
            storage_class: Some("STANDARD".to_string()),
        }];

        let res = s3_copy(
            &client,
            "bucket",
            objects,
            "newbucket",
            "newpath",
            true,
            true,
        );
        assert!(res.is_ok());
    }
}
