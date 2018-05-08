#[macro_use]
extern crate structopt;

#[macro_use]
extern crate failure;

extern crate chrono;
extern crate clap;
extern crate futures;
extern crate glob;
extern crate regex;
extern crate rusoto_core;
extern crate rusoto_credential;
extern crate rusoto_s3;

use structopt::StructOpt;
use structopt::clap::AppSettings;
use std::process::*;
use std::process::Command;
use std::fs;
use std::fs::File;
use std::io::Write;
use std::path::Path;

use futures::stream::Stream;
use futures::Future;

use regex::Regex;

use rusoto_core::request::*;
use rusoto_core::Region;
use rusoto_core::ProvideAwsCredentials;
use rusoto_core::reactor::RequestDispatcher;

use rusoto_s3::*;

mod credentials;
use credentials::*;

mod types;
use types::*;

#[derive(StructOpt, Debug, Clone)]
#[structopt(name = "s3find", about = "walk a s3 path hierarchy",
            raw(global_settings = "&[AppSettings::ColoredHelp, AppSettings::NeedsLongHelp, AppSettings::NeedsSubcommandHelp]"))]
pub struct FindOpt {
    #[structopt(name = "path", raw(index = r#"1"#))]
    path: S3path,
    #[structopt(name = "aws_access_key", long = "aws-access-key",
                help = "AWS key to access to S3, unrequired",
                raw(requires_all = r#"&["aws_secret_key"]"#))]
    aws_access_key: Option<String>,
    #[structopt(name = "aws_secret_key", long = "aws-secret-key",
                help = "AWS secret key to access to S3, unrequired",
                raw(requires_all = r#"&["aws_access_key"]"#))]
    aws_secret_key: Option<String>,
    #[structopt(name = "aws_region", long = "aws-region",
                help = "AWS region to access to S3, unrequired")]
    aws_region: Option<Region>,
    #[structopt(name = "npatern", long = "name", help = "match by glob shell pattern",
                raw(number_of_values = "1"))]
    name: Vec<NameGlob>,
    #[structopt(name = "ipatern", long = "iname",
                help = "match by glob shell pattern, case insensitive",
                raw(number_of_values = "1"))]
    iname: Vec<InameGlob>,
    #[structopt(name = "rpatern", long = "regex",
                help = "match by regex pattern, case insensitive", raw(number_of_values = "1"))]
    regex: Vec<Regex>,
    #[structopt(name = "time", long = "mtime",
                help = "the difference between the file last modification time",
                raw(number_of_values = "1", allow_hyphen_values = "true"))]
    mtime: Vec<FindTime>,
    #[structopt(name = "bytes_size", long = "size", help = "file size",
                raw(number_of_values = "1", allow_hyphen_values = "true"))]
    size: Vec<FindSize>,
    #[structopt(subcommand)]
    cmd: Option<Cmd>,
}

#[derive(StructOpt, Debug, PartialEq, Clone)]
pub enum Cmd {
    #[structopt(name = "-exec", help = "exec any shell comand with every key")]
    Exec {
        #[structopt(name = "utility")]
        utility: String,
    },
    #[structopt(name = "-print", help = "extended print with detail information")]
    Print,
    #[structopt(name = "-delete", help = "delete filtered keys")]
    Delete,
    #[structopt(name = "-download", help = "download filtered keys")]
    Download {
        #[structopt(name = "destination")]
        destination: String,
    },
    #[structopt(name = "-ls", help = "list of filtered keys")]
    Ls,
}

impl FindOpt {
    fn command<P, D>(&self, client: &S3Client<P, D>, bucket: &str, list: Vec<&Object>) -> Result<()>
    where
        P: ProvideAwsCredentials + 'static,
        D: DispatchSignedRequest + 'static,
    {
        match self.cmd {
            Some(Cmd::Print) => {
                let _nlist: Vec<_> = list.iter().map(|x| advanced_print(bucket, x)).collect();
            }
            Some(Cmd::Ls) => {
                let _nlist: Vec<_> = list.iter().map(|x| fprint(bucket, x)).collect();
            }
            Some(Cmd::Exec { utility: ref p }) => {
                let _nlist: Vec<_> = list.iter()
                    .map(|x| {
                        let key = x.key.as_ref().unwrap();
                        let path = format!("s3://{}/{}", bucket, key);
                        exec(&p, &path)
                    })
                    .collect();
            }
            Some(Cmd::Delete) => s3_delete(client, bucket, list)?,
            Some(Cmd::Download { destination: ref d }) => s3_download(client, bucket, list, d)?,
            None => {
                let _nlist: Vec<_> = list.iter().map(|x| fprint(bucket, x)).collect();
            }
        }
        Ok(())
    }
}

struct FilterList(Vec<Box<Filter>>);

impl FilterList {
    fn new(opts: &FindOpt) -> FilterList {
        let mut list: Vec<Box<Filter>> = Vec::new();

        for name in opts.name.iter() {
            list.push(Box::new(name.clone()));
        }

        for iname in opts.iname.iter() {
            list.push(Box::new(iname.clone()));
        }

        for regex in opts.regex.iter() {
            list.push(Box::new(regex.clone()));
        }

        for size in opts.size.iter() {
            list.push(Box::new(size.clone()));
        }

        for mtime in opts.mtime.iter() {
            list.push(Box::new(mtime.clone()));
        }

        FilterList(list)
    }

    fn filters(&self, object: &Object) -> bool {
        for item in self.0.iter() {
            if !item.filter(object) {
                return false;
            }
        }

        true
    }
}

#[derive(Debug, PartialEq, Clone)]
struct ExecStatus {
    status: ExitStatus,
    runcommand: String,
}

fn fprint(bucket: &str, item: &Object) {
    println!(
        "s3://{}/{}",
        bucket,
        item.key.as_ref().unwrap_or(&"".to_string())
    );
}

fn advanced_print(bucket: &str, item: &Object) {
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

fn exec(command: &str, key: &str) -> Result<ExecStatus> {
    let scommand = command.replace("{}", key);

    let mut command_args = scommand.split(" ");
    let command_name = command_args.next().ok_or(FindError::CommandlineParse)?;

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

fn s3_delete<P, D>(client: &S3Client<P, D>, bucket: &str, list: Vec<&Object>) -> Result<()>
where
    P: ProvideAwsCredentials + 'static,
    D: DispatchSignedRequest + 'static,
{
    let key_list: Vec<_> = list.iter()
        .map(|x| ObjectIdentifier {
            key: x.key.as_ref().unwrap().to_string(),
            version_id: None,
        })
        .collect();

    let request = DeleteObjectsRequest {
        bucket: bucket.to_string(),
        delete: Delete {
            objects: key_list,
            quiet: None,
        },
        mfa: None,
        request_payer: None,
    };

    let result = client.delete_objects(&request).sync()?;

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

fn s3_download<P, D>(
    client: &S3Client<P, D>,
    bucket: &str,
    list: Vec<&Object>,
    target: &str,
) -> Result<()>
where
    P: ProvideAwsCredentials + 'static,
    D: DispatchSignedRequest + 'static,
{
    for object in list.iter() {
        let key = object.key.as_ref().unwrap();
        let request = GetObjectRequest {
            bucket: bucket.to_owned(),
            key: key.to_owned(),
            ..Default::default()
        };

        let file_path = format!("{}/{}", target, key);
        let dir_path = Path::new(&file_path)
            .parent()
            .ok_or(FindError::ParentPathParse)?;

        fs::create_dir_all(&dir_path)?;

        let result = client.get_object(&request).sync()?;

        let mut output = File::create(&file_path)?;
        let mut input = result.body.unwrap().concat2().wait().unwrap();

        output.write(&input)?;

        println!("downloaded: s3://{}/{} to {}", bucket, &key, &file_path);
    }

    Ok(())
}

fn s3_tags<P, D>(
    client: &S3Client<P, D>,
    bucket: &str,
    list: Vec<&Object>,
    tags: Tagging,
) -> Result<()>
where
    P: ProvideAwsCredentials + 'static,
    D: DispatchSignedRequest + 'static,
{
    for object in list.iter() {
        let key = object.key.as_ref().unwrap();

        let request = PutObjectTaggingRequest {
            bucket: bucket.to_owned(),
            key: key.to_owned(),
            tagging: tags.clone(),
            ..Default::default()
        };

        let result = client.put_object_tagging(&request).sync()?;

        println!("tags are set for: s3://{}/{}", bucket, &key);
    }

    Ok(())
}

fn real_main() -> Result<()> {
    let status = FindOpt::from_args();
    let s3path = status.path.clone();

    let filter = FilterList::new(&status);

    let region = status.aws_region.clone().unwrap_or(Region::default());
    let provider =
        CombinedProvider::new(status.aws_access_key.clone(), status.aws_secret_key.clone());
    let client = S3Client::new(RequestDispatcher::default(), provider, region);

    let mut request = ListObjectsV2Request {
        bucket: s3path.bucket.clone(),
        continuation_token: None,
        delimiter: None,
        encoding_type: None,
        fetch_owner: None,
        max_keys: Some(10000),
        prefix: s3path.prefix,
        request_payer: None,
        start_after: None,
    };

    loop {
        let output = client.list_objects_v2(&request).sync()?;
        match output.contents {
            Some(klist) => {
                let flist: Vec<_> = klist.iter().filter(|x| filter.filters(x)).collect();
                status.command(&client, &s3path.bucket, flist)?;

                match output.next_continuation_token {
                    Some(token) => request.continuation_token = Some(token),
                    None => break,
                }
            }
            None => {
                println!("No keys!");
                break;
            }
        }
    }
    Ok(())
}

fn main() {
    if let Err(error) = real_main() {
        eprintln!("Error - {:#?}", error);
        ::std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use rusoto_s3::*;
    use exec;
    use advanced_print;

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

}
