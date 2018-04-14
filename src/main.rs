#[macro_use]
extern crate structopt;

use structopt::StructOpt;

extern crate clap;
extern crate glob;
extern crate regex;
extern crate rusoto_core;
extern crate rusoto_s3;

use std::process::*;
use std::process::Command;
use glob::Pattern;
use regex::Regex;

use rusoto_core::DefaultCredentialsProvider;
use rusoto_core::default_tls_client;

use rusoto_core::ProvideAwsCredentials;
use rusoto_s3::*;
use rusoto_core::request::*;
use rusoto_core::default_region;

#[derive(StructOpt, Debug, Clone)]
#[structopt(name = "s3find", about = "walk a s3 path hierarchy")]
pub struct FindOpt {
    #[structopt(name = "path")]
    path: String,
    #[structopt(name = "aws_key", long = "aws-key", help = "AWS key to access to S3, unrequired")]
    aws_key: Option<String>,
    #[structopt(name = "aws_secret_key", long = "aws-secret-key",
                help = "AWS secret key to access to S3, unrequired")]
    aws_secret_key: Option<String>,
    #[structopt(name = "aws_region", long = "aws-region",
                help = "AWS region to access to S3, unrequired")]
    aws_region: Option<String>,
    #[structopt(name = "npatern", long = "name", help = "match by glob shell pattern")]
    name: Option<Pattern>,
    #[structopt(name = "ipatern", long = "iname",
                help = "match by glob shell pattern, case insensitive")]
    iname: Option<Pattern>,
    #[structopt(name = "rpatern", long = "regex",
                help = "match by regex pattern, case insensitive")]
    regex: Option<Regex>,
    #[structopt(name = "time", long = "mtime",
                help = "the difference between the file last modification time")]
    mtime: Option<String>,
    #[structopt(subcommand)]
    cmd: Option<Cmd>,
}

#[derive(StructOpt, Debug, PartialEq, Clone)]
pub enum Cmd {
    #[structopt(name = "-exec")]
    Exec {
        #[structopt(name = "utility")]
        utility: String,
    },
    #[structopt(name = "-print")]
    Print,
    #[structopt(name = "-delete")]
    Delete,
    #[structopt(name = "-ls")]
    Ls,
}

impl FindOpt {
    fn matches(&self, str: &str) -> bool {
        self.name.as_ref().unwrap().matches(str)
    }

    fn is_match(&self, str: &str) -> bool {
        self.regex.as_ref().unwrap().is_match(str)
    }

    fn filters(&self, object: &Object) -> bool {
        if self.name.is_some() {
            return self.matches(object.key.as_ref().unwrap())
        }

        if self.regex.is_some() {
            return self.is_match(object.key.as_ref().unwrap())
        }

        return true
    }

    fn command<P, D>(&self, client: &S3Client<P, D>, bucket: &str, list: Vec<&Object>)
    where
        P: ProvideAwsCredentials,
        D: DispatchSignedRequest,
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
                        exec(&p, &path);
                    })
                    .collect();
            }
            Some(Cmd::Delete) => s3_delete(client, bucket, list),
            None => {
                let _nlist: Vec<_> = list.iter().map(|x| fprint(bucket, x)).collect();
            }
        }
    }
}

fn fprint(bucket: &str, item: &Object) {
    println!("s3://{}/{}", bucket, item.key.as_ref().unwrap());
}

fn msg_print(bucket: &str, item: &Object, msg: &str) {
    println!("{}: s3://{}/{}", msg, bucket, item.key.as_ref().unwrap());
}

fn advanced_print(bucket: &str, item: &Object) {
    println!(
        "{} {:?} {} {} s3://{}/{} {}",
        item.e_tag.as_ref().unwrap(),
        item.owner.as_ref().map(|x| x.display_name.as_ref()),
        item.size.as_ref().unwrap(),
        item.last_modified.as_ref().unwrap(),
        bucket,
        item.key.as_ref().unwrap(),
        item.storage_class.as_ref().unwrap(),
    );
}

fn exec(command: &str, key: &str) -> (ExitStatus, String) {
    let scommand = command.replace("{}", key);

    let mut command_args = scommand.split(" ");
    let command_name = command_args.next().unwrap();

    let mut rcommand = Command::new(command_name);
    for arg in command_args {
        rcommand.arg(arg);
    }

    let output = rcommand.output().expect("failed to execute process");
    let output_str = String::from_utf8_lossy(&output.stdout).to_string();

    print!("{}", &output_str);
    return (output.status, output_str);
}

fn split_validate_s3(path: &str) -> (bool, &str, Option<&str>) {
    let s3_vec: Vec<&str> = path.split("/").collect();
    let s3bucket = s3_vec.get(2).unwrap_or(&"");
    let prefix = s3_vec.get(3).map(|x| x.to_owned());

    let is_validated =
        (s3_vec.get(0) == Some(&"s3:")) && (s3_vec.get(1) == Some(&"")) && (s3bucket != &"");

    return (is_validated, s3bucket, prefix);
}

fn s3_delete<P, D>(client: &S3Client<P, D>, bucket: &str, list: Vec<&Object>)
where
    P: ProvideAwsCredentials,
    D: DispatchSignedRequest,
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

    let _result = client.delete_objects(&request);
    for l in list.iter() {
        msg_print(bucket, l, "deleted");
    }
}

fn main() {
    let status = FindOpt::from_args();
    let s3path = status.path.as_ref();

    let (is_validated, s3bucket, prefix) = split_validate_s3(s3path);

    if !is_validated {
        eprintln!("S3PATH is not correct");
        exit(1);
    }

    let provider = DefaultCredentialsProvider::new().unwrap();
    let dispatcher = default_tls_client().unwrap();
    let region = default_region();
    let client = S3Client::new(dispatcher, provider, region);

    let mut request = ListObjectsV2Request {
        bucket: s3bucket.to_string(),
        continuation_token: None,
        delimiter: None,
        encoding_type: None,
        fetch_owner: None,
        max_keys: Some(10000),
        prefix: prefix.map(|x| (*x).to_string()),
        request_payer: None,
        start_after: None,
    };

    loop {
        match client.list_objects_v2(&request) {
            Ok(output) => match output.contents {
                Some(klist) => {
                    let flist: Vec<_> = klist.iter().filter(|x| status.filters(x)).collect();
                    status.command(&client, &s3bucket, flist);

                    match output.next_continuation_token {
                        Some(token) => request.continuation_token = Some(token),
                        None => break,
                    }
                }
                None => {
                    println!("No keys!");
                    break;
                }
            },
            Err(error) => {
                println!("Error - {}", error);
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use rusoto_s3::*;
    use split_validate_s3;
    use exec;
    use advanced_print;

    #[test]
    fn split_validate_s3_corect() {
        let url = "s3://testbucket/";
        let (is_validated, bucket, path) = split_validate_s3(url);
        assert!(is_validated, "This s3 url should be validated posivitely");
        assert_eq!(bucket, "testbucket", "This should be 'testbucket'");
        assert_eq!(path, Some(""), "This should be empty path");
    }

    #[test]
    fn split_validate_s3_correct_full() {
        let url = "s3://testbucket/path";
        let (is_validated, bucket, path) = split_validate_s3(url);
        assert!(is_validated, "This s3 url should be validated posivitely");
        assert_eq!(bucket, "testbucket", "This should be 'testbucket'");
        assert_eq!(path, Some("path"), "This should be 'path'");
    }

    #[test]
    fn split_validate_s3_correct_short() {
        let url = "s3://testbucket";
        let (is_validated, bucket, path) = split_validate_s3(url);
        assert!(is_validated, "This s3 url should be validated posivitely");
        assert_eq!(bucket, "testbucket", "This should be 'testbucket'");
        assert_eq!(path, None, "This should be None");
    }

    #[test]
    fn split_validate_s3_only_bucket() {
        let url = "testbucket";
        let (is_validated, bucket, path) = split_validate_s3(url);
        assert!(
            !is_validated,
            "This s3 url should not be validated posivitely"
        );
        assert_eq!(bucket, "");
        assert_eq!(path, None);
    }

    #[test]
    fn split_validate_s3_without_bucket() {
        let url = "s3://";
        let (is_validated, bucket, path) = split_validate_s3(url);
        assert!(
            !is_validated,
            "This s3 url should not be validated posivitely"
        );
        assert_eq!(bucket, "");
        assert_eq!(path, None);
    }

    #[test]
    fn split_validate_s3_without_2_slash() {
        let url = "s3:/testbucket";
        let (is_validated, bucket, path) = split_validate_s3(url);
        assert!(
            !is_validated,
            "This s3 url should not be validated posivitely"
        );
        assert_eq!(bucket, "");
        assert_eq!(path, None);
    }

    #[test]
    fn exec_true() {
        let (status, _) = exec("true", "");
        assert!(status.success(), "Exit code of true is 0");
    }

    #[test]
    fn exec_false() {
        let (status, _) = exec("false", "");
        assert!(!status.success(), "Exit code of false is 1");
    }

    #[test]
    fn exec_echo_multiple() {
        let (status, output) = exec("echo Hello world1", "");
        assert!(status.success(), "Exit code of echo is 0");
        assert_eq!(
            output, "Hello world1\n",
            "Output of echo is 'Hello world1\n'"
        );
    }

    #[test]
    #[should_panic]
    fn exec_incorrect_command() {
        let (status, _) = exec("jbrwuDxPy4ck", "");
        assert!(!status.success(), "Exit code should not be success");
    }

    #[test]
    fn exec_echo_interpolation() {
        let (status, output) = exec("echo Hello {}", "world2");
        assert!(status.success(), "Exit code of echo is 0");
        assert_eq!(
            output, "Hello world2\n",
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
