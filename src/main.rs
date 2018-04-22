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

use failure::Error;

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
    #[fail(display = "Empty size value")]
    SizeEmpty,
    #[fail(display = "Empty time value")]
    TimeEmpty,
}

type Result<T> = ::std::result::Result<T, Error>;

#[derive(Debug, Clone, PartialEq)]
struct S3path {
    bucket: String,
    prefix: Option<String>,
}

impl S3path {
    fn new(path: &str) -> Result<S3path> {
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
            Err(FindError::S3Parse.into())
        }
    }
}

impl FromStr for S3path {
    type Err = Error;

    fn from_str(s: &str) -> Result<S3path> {
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
    size: i64,
}

impl FindSize {
    fn new(size_str: &str) -> Result<FindSize> {
        let (relation, size_str_processed) = match size_str.chars().next() {
            Some('+') => (FindRelation::Upper, &((*size_str)[1..])),
            Some('-') => (FindRelation::Lower, &((*size_str)[1..])),
            Some(_) => (FindRelation::Equal, size_str),
            None => return Err(FindError::TimeEmpty.into()),
        };

        let size = size_str_processed.parse()?;

        Ok(FindSize {
            relation: relation,
            size: size,
        })
    }
}

impl FromStr for FindSize {
    type Err = Error;

    fn from_str(s: &str) -> Result<FindSize> {
        FindSize::new(s)
    }
}

#[derive(Debug, Clone, PartialEq)]
enum FindTime {
    Upper(u64),
    Lower(u64),
}

impl FromStr for FindTime {
    type Err = Error;

    fn from_str(s: &str) -> Result<FindTime> {
        match s.chars().next() {
            Some('+') => Ok(FindTime::Upper(((*s)[1..]).parse()?)),
            Some('-') => Ok(FindTime::Lower(((*s)[1..]).parse()?)),
            Some(_) => Ok(FindTime::Upper(s.parse()?)),
            None => Err(FindError::SizeEmpty.into()),
        }
    }
}

type NameGlob = Pattern;

#[derive(Debug, Clone, PartialEq)]
struct InameGlob(Pattern);

impl FromStr for InameGlob {
    type Err = Error;

    fn from_str(s: &str) -> Result<InameGlob> {
        let pattern = Pattern::from_str(s)?;
        Ok(InameGlob(pattern))
    }
}

trait Filter {
    fn filter(&self, object: &Object) -> bool;
}

impl Filter for NameGlob {
    fn filter(&self, object: &Object) -> bool {
        let object_key = object.key.as_ref().unwrap();
        self.matches(object_key)
    }
}

impl Filter for InameGlob {
    fn filter(&self, object: &Object) -> bool {
        let object_key = object.key.as_ref().unwrap();
        self.0.matches_with(
            object_key,
            &MatchOptions {
                case_sensitive: false,
                require_literal_separator: false,
                require_literal_leading_dot: false,
            },
        )
    }
}

impl Filter for Regex {
    fn filter(&self, object: &Object) -> bool {
        let object_key = object.key.as_ref().unwrap();
        self.is_match(object_key)
    }
}

impl Filter for FindSize {
    fn filter(&self, object: &Object) -> bool {
        let object_size = object.size.as_ref().unwrap();
        match self.relation {
            FindRelation::Upper => object_size > &self.size,
            FindRelation::Lower => object_size < &self.size,
            FindRelation::Equal => object_size == &self.size,
        }
    }
}

#[derive(StructOpt, Debug, Clone)]
#[structopt(name = "s3find", about = "walk a s3 path hierarchy")]
pub struct FindOpt {
    #[structopt(name = "path", raw(index = r#"1"#))]
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

fn real_main() -> Result<()> {
    let status = FindOpt::from_args();
    let s3path = status.path.clone();

    let filter = FilterList::new(&status);
    let provider = DefaultCredentialsProvider::new()?;
    let dispatcher = default_tls_client()?;
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
        let output = client.list_objects_v2(&request)?;
        match output.contents {
            Some(klist) => {
                let flist: Vec<_> = klist.iter().filter(|x| filter.filters(x)).collect();
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
    use S3path;
    use FindSize;
    use FindRelation;
    use FindTime;

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
        assert_eq!(size.relation, FindRelation::Equal, "should be equal");
    }

    #[test]
    fn size_corect_positive() {
        let size_str = "+1111";
        let size = FindSize::new(size_str).unwrap();

        assert_eq!(size.size, 1111, "");
        assert_eq!(size.relation, FindRelation::Upper, "should be upper");
    }

    #[test]
    fn size_corect_negative() {
        let size_str = "-1111";
        let size = FindSize::new(size_str).unwrap();

        assert_eq!(size.size, 1111, "");
        assert_eq!(size.relation, FindRelation::Lower, "should be lower");
    }

    #[test]
    fn size_incorect_negative() {
        let size_str = "-";
        let size = FindSize::new(size_str);

        assert!(size.is_err(), "Should be error");
    }

    #[test]
    fn time_corect() {
        let time_str = "1111";
        let time: FindTime = time_str.parse().unwrap();

        assert_eq!(time, FindTime::Upper(1111), "Should be upper");
    }

    #[test]
    fn time_corect_positive() {
        let time_str = "+1111";
        let time: FindTime = time_str.parse().unwrap();

        assert_eq!(time, FindTime::Upper(1111), "Should be upper");
    }

    #[test]
    fn time_corect_negative() {
        let time_str = "-1111";
        let time: FindTime = time_str.parse().unwrap();

        assert_eq!(time, FindTime::Lower(1111), "Should be lower");
    }

    #[test]
    fn time_incorect_negative() {
        let time_str = "-";
        let time = time_str.parse::<FindTime>();

        assert!(time.is_err(), "Should be error");
    }
}
