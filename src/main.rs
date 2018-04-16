#[macro_use]
extern crate structopt;

use structopt::StructOpt;

#[macro_use]
extern crate failure;

extern crate clap;
extern crate glob;
extern crate regex;
extern crate rusoto_core;
extern crate rusoto_s3;

use std::process::*;
use std::process::Command;
use std::str::FromStr;
use std::num::ParseIntError;

use glob::Pattern;
use glob::MatchOptions;
use regex::Regex;

use rusoto_core::DefaultCredentialsProvider;
use rusoto_core::default_tls_client;
use rusoto_core::ProvideAwsCredentials;
use rusoto_core::request::*;
use rusoto_core::default_region;

use rusoto_s3::*;

#[derive(Fail, Debug)]
enum FindError {
    #[fail(display = "Invalid s3 path")]
    S3Parse,
    #[fail(display = "Invalid size value")]
    SizeParse(#[cause] ParseIntError),
}

#[derive(Debug, Clone, PartialEq)]
struct S3path {
    bucket: String,
    prefix: Option<String>,
}

impl S3path {
    fn new(path: &str) -> Result<S3path, FindError> {
        let s3_vec: Vec<&str> = path.split("/").collect();
        let bucket = s3_vec.get(2).unwrap_or(&"");
        let prefix = s3_vec.get(3).map(|x| x.to_owned());

        let is_validated =
            (s3_vec.get(0) == Some(&"s3:")) && (s3_vec.get(1) == Some(&"")) && (bucket != &"");

        if is_validated {
            Ok(S3path {
                bucket: bucket.to_string(),
                prefix: prefix.map(|x| (*x).to_string()),
            })
        } else {
            Err(FindError::S3Parse)
        }
    }
}

impl FromStr for S3path {
    type Err = FindError;

    fn from_str(s: &str) -> Result<S3path, FindError> {
        S3path::new(s)
    }
}

#[derive(Debug, Clone, PartialEq)]
enum FindRelation {
    Equal,
    Upper,
    Lower,
}

#[derive(Debug, Clone, PartialEq)]
struct FindSize {
    relation: FindRelation,
    size: u64,
}

impl FindSize {
    fn new(size_str: &str) -> Result<FindSize, FindError> {
        let (relation, size_str_processed) = match size_str.chars().next().unwrap() {
            '+' => (FindRelation::Upper, &((*size_str)[1..])),
            '-' => (FindRelation::Lower, &((*size_str)[1..])),
            _ => (FindRelation::Equal, size_str),
        };

        let size = size_str_processed.parse().map_err(FindError::SizeParse)?;

        Ok(FindSize {
            relation: relation,
            size: size,
        })
    }
}

impl FromStr for FindSize {
    type Err = FindError;

    fn from_str(s: &str) -> Result<FindSize, FindError> {
        FindSize::new(s)
    }
}

#[derive(StructOpt, Debug, Clone)]
#[structopt(name = "s3find", about = "walk a s3 path hierarchy")]
pub struct FindOpt {
    #[structopt(name = "path")]
    path: S3path,
    #[structopt(name = "aws_access_key", long = "aws_access_key",
                help = "AWS key to access to S3, unrequired",
                raw(requires_all = r#"&["aws_secret_key", "aws_region"]"#))]
    aws_access_key: Option<String>,
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
    #[structopt(name = "bytes_size", long = "size", help = "file size")]
    size: Option<FindSize>,
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
    fn glob_match(&self, str: &str) -> bool {
        self.name.as_ref().unwrap().matches(str)
    }

    fn ci_glob_match(&self, str: &str) -> bool {
        self.iname.as_ref().unwrap().matches_with(
            str,
            &MatchOptions {
                case_sensitive: false,
                require_literal_separator: false,
                require_literal_leading_dot: false,
            },
        )
    }

    fn regex_match(&self, str: &str) -> bool {
        self.regex.as_ref().unwrap().is_match(str)
    }

    fn filters(&self, object: &Object) -> bool {
        if self.name.is_some() {
            return self.glob_match(object.key.as_ref().unwrap());
        }

        if self.iname.is_some() {
            return self.ci_glob_match(object.key.as_ref().unwrap());
        }

        if self.regex.is_some() {
            return self.regex_match(object.key.as_ref().unwrap());
        }

        return true;
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
    let s3path = status.path.clone();

    let provider = DefaultCredentialsProvider::new().unwrap();
    let dispatcher = default_tls_client().unwrap();
    let region = default_region();
    let client = S3Client::new(dispatcher, provider, region);

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
        match client.list_objects_v2(&request) {
            Ok(output) => match output.contents {
                Some(klist) => {
                    let flist: Vec<_> = klist.iter().filter(|x| status.filters(x)).collect();
                    status.command(&client, &s3path.bucket, flist);

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
    use exec;
    use advanced_print;
    use S3path;
    use FindSize;
    use FindRelation;

    #[test]
    fn s3path_corect() {
        let url = "s3://testbucket/";
        let path = S3path::new(url).unwrap();
        assert_eq!(path.bucket, "testbucket", "This should be 'testbucket'");
        assert_eq!(
            path.prefix,
            Some("".to_string()),
            "This should be empty path"
        );
    }

    #[test]
    fn s3path_correct_full() {
        let url = "s3://testbucket/path";
        let path = S3path::new(url).unwrap();
        assert_eq!(path.bucket, "testbucket", "This should be 'testbucket'");
        assert_eq!(
            path.prefix,
            Some("path".to_string()),
            "This should be 'path'"
        );
    }

    #[test]
    fn s3path_correct_short() {
        let url = "s3://testbucket";
        let path = S3path::new(url).unwrap();
        assert_eq!(path.bucket, "testbucket", "This should be 'testbucket'");
        assert_eq!(path.prefix, None, "This should be None");
    }

    #[test]
    fn s3path_only_bucket() {
        let url = "testbucket";
        let path = S3path::new(url);
        assert!(
            path.is_err(),
            "This s3 url should not be validated posivitely"
        );
    }

    #[test]
    fn s3path_without_bucket() {
        let url = "s3://";
        let path = S3path::new(url);
        assert!(
            path.is_err(),
            "This s3 url should not be validated posivitely"
        );
    }

    #[test]
    fn s3path_without_2_slash() {
        let url = "s3:/testbucket";
        let path = S3path::new(url);
        assert!(
            path.is_err(),
            "This s3 url should not be validated posivitely"
        );
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

    #[test]
    fn size_corect() {
        let size_str = "1111";
        let size = FindSize::new(size_str).unwrap();

        assert_eq!(size.size, 1111, "");
        assert_eq!(
            size.relation,
            FindRelation::Equal,
            "should be equal"
        );
    }

    #[test]
    fn size_corect_positive() {
        let size_str = "+1111";
        let size = FindSize::new(size_str).unwrap();

        assert_eq!(size.size, 1111, "");
        assert_eq!(
            size.relation,
            FindRelation::Upper,
            "should be upper"
        );
    }

    #[test]
    fn size_corect_negative() {
        let size_str = "-1111";
        let size = FindSize::new(size_str).unwrap();

        assert_eq!(size.size, 1111, "");
        assert_eq!(
            size.relation,
            FindRelation::Lower,
            "should be lower"
        );
    }
}
