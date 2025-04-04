use aws_types::region::Region;
use clap::{Args, Error, Parser, Subcommand, ValueEnum};
use glob::Pattern;
use regex::Regex;
use std::str::FromStr;
use thiserror::Error;

fn region(s: &str) -> std::result::Result<Region, Error> {
    Ok(Region::new(s.to_owned()))
}

/// Walk an Amazon S3 path hierarchy
#[derive(Parser)]
#[command(
    name = "s3find",
    arg_required_else_help = true,
    version,
    long_about(
        r#"
Walk an Amazon S3 path hierarchy

The authorization flow is the following chain:
  * use credentials from arguments provided by users
  * use environment variable credentials: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
  * use credentials via aws file profile.
    Profile can be set via environment variable AWS_PROFILE
    Profile file can be set via environment variable AWS_SHARED_CREDENTIALS_FILE
  * use AWS instance IAM profile
  * use AWS container IAM profile
"#
    )
)]
pub struct FindOpt {
    /// S3 path to walk through. It should be s3://bucket/path
    #[arg(name = "path")]
    pub path: S3Path,

    /// AWS access key. Unrequired.
    #[arg(
        name = "aws-access-key",
        long = "aws-access-key",
        requires_all = &["aws-secret-key"]
    )]
    pub aws_access_key: Option<String>,

    /// AWS secret key from AWS credential pair. Required only for the credential based authentication.
    #[arg(
        name = "aws-secret-key",
        long = "aws-secret-key",
        requires_all = &["aws-access-key"]
    )]
    pub aws_secret_key: Option<String>,

    // The region to use. Default value is us-east-1
    #[arg(name = "aws-region", long, default_value = "us-east-1", value_parser=region)]
    pub aws_region: Region,

    /// Glob pattern for match, can be multiple
    #[arg(name = "pattern", long = "name", number_of_values = 1)]
    pub name: Vec<NameGlob>,

    /// Case-insensitive glob pattern for match, can be multiple
    #[arg(name = "ipattern", long = "iname", number_of_values = 1)]
    pub iname: Vec<InameGlob>,

    /// Regex pattern for match, can be multiple
    #[arg(name = "rpattern", long = "regex", number_of_values = 1)]
    pub regex: Vec<Regex>,

    /// Modification time for match
    #[arg(
        name = "time",
        long = "mtime",
        number_of_values = 1,
        allow_hyphen_values = true,
        long_help = r#"Modification time for match, a time period:
    -5d - for period from now-5d to now
    +5d - for period before now-5d

Possible time units are as follows:
    s - seconds
    m - minutes
    h - hours
    d - days
    w - weeks

Can be multiple, but should be overlaping"#
    )]
    pub mtime: Vec<FindTime>,

    /// File size for match
    #[arg(
        name = "bytes-size",
        long,
        number_of_values = 1,
        allow_hyphen_values = true,
        long_help = r#"File size for match:
    5k - exact match 5k,
    +5k - bigger than 5k,
    -5k - smaller than 5k,

Possible file size units are as follows:
    k - kilobytes (1024 bytes)
    M - megabytes (1024 kilobytes)
    G - gigabytes (1024 megabytes)
    T - terabytes (1024 gigabytes)
    P - petabytes (1024 terabytes)"#
    )]
    pub size: Vec<FindSize>,

    /// Limit result
    #[arg(name = "limit", long)]
    pub limit: Option<usize>,

    /// The number of results to return in each response to a list operation.
    #[arg(
        name = "number",
        long,
        default_value = "1000",
        long_help = r#"The number of results to return in each response to a
list operation. The default value is 1000 (the maximum
allowed). Using a lower value may help if an operation
times out."#
    )]
    pub page_size: i64,

    /// Print summary statistic
    #[arg(name = "summarize", long, short)]
    pub summarize: bool,

    /// Action to be ran with matched list of paths
    #[command(subcommand)]
    pub cmd: Option<Cmd>,
}

#[derive(Subcommand, Clone)]
pub enum Cmd {
    /// Exec any shell program with every key
    #[command(name = "exec")]
    Exec(Exec),

    /// Extended print with detail information
    #[command(name = "print")]
    Print(AdvancedPrint),

    /// Delete matched keys
    #[command(name = "delete")]
    Delete(MultipleDelete),

    /// Download matched keys
    #[command(name = "download")]
    Download(Download),

    /// Copy matched keys to a s3 destination
    #[command(name = "copy")]
    Copy(S3Copy),

    /// Move matched keys to a s3 destination
    #[command(name = "move")]
    Move(S3Move),

    /// Print the list of matched keys
    #[command(name = "ls")]
    Ls(FastPrint),

    /// Print the list of matched keys with tags
    #[command(name = "lstags")]
    LsTags(ListTags),

    /// Set the tags(overwrite) for the matched keys
    #[command(name = "tags")]
    Tags(SetTags),

    /// Make the matched keys public available (readonly)
    #[command(name = "public")]
    Public(SetPublic),

    /// Do not do anything with keys, do not print them as well
    #[command(name = "nothing")]
    Nothing(DoNothing),
}

impl Default for Cmd {
    fn default() -> Self {
        Cmd::Ls(FastPrint {})
    }
}

#[derive(Args, Clone)]
pub struct FastPrint {}

#[derive(ValueEnum, Clone, Default)]
pub enum PrintFormat {
    /// default human-readable format with all object metadata
    #[default]
    Text,
    /// JSON format with all object metadata
    Json,
    /// CSV format with all object metadata
    Csv,
}

#[derive(Args, Clone, Default)]
pub struct AdvancedPrint {
    /// format for print subcommand
    #[arg(name = "format", long, default_value = "text")]
    pub format: PrintFormat,
}

#[derive(Args, Clone)]
pub struct MultipleDelete {}

#[derive(Args, Clone)]
pub struct ListTags {}

// region ?
#[derive(Args, Clone)]
pub struct SetPublic {}

#[derive(Args, Clone)]
pub struct Exec {
    /// Utility(program) to run
    #[arg(name = "utility", long)]
    pub utility: String,
}

#[derive(Args, Clone)]
pub struct Download {
    /// Force download files(overwrite) even if the target files are already present
    #[arg(long = "force")]
    pub force: bool,

    /// Directory destination to download files to
    #[arg(name = "destination")]
    pub destination: String,
}

#[derive(Args, Clone)]
pub struct S3Copy {
    /// S3 path destination to copy files to
    #[arg(name = "destination")]
    pub destination: S3Path,

    /// Copy keys like files
    #[arg(long = "flat")]
    pub flat: bool,
}

#[derive(Args, Clone)]
pub struct S3Move {
    /// S3 path destination to copy files to
    #[arg(name = "destination")]
    pub destination: S3Path,

    /// Copy keys like files
    #[arg(long = "flat")]
    pub flat: bool,
}

#[derive(Args, Clone)]
pub struct SetTags {
    /// List of the tags to set
    #[arg(name = "key:value", number_of_values = 1)]
    pub tags: Vec<FindTag>,
}

#[derive(Args, Clone)]
pub struct DoNothing {}

#[derive(Error, Debug)]
pub enum FindError {
    #[error("Invalid s3 path")]
    S3Parse,
    #[error("Invalid size parameter")]
    SizeParse,
    #[error("Invalid mtime parameter")]
    TimeParse,
    #[error("Cannot parse tag")]
    TagParseError,
    #[error("Cannot parse tag key")]
    TagKeyParseError,
    #[error("Cannot parse tag value")]
    TagValueParseError,
}

#[derive(Debug, Clone, PartialEq)]
pub struct S3Path {
    pub bucket: String,
    pub prefix: Option<String>,
    pub region: Region,
}

impl FromStr for S3Path {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, anyhow::Error> {
        let regex = Regex::new(r#"^s3://([\d\w _-]+)(/([\d\w/ _-]*))?"#)?;
        let captures = regex.captures(s).ok_or(FindError::S3Parse)?;

        let bucket = captures
            .get(1)
            .map(|x| x.as_str().to_owned())
            .ok_or(FindError::S3Parse)?;
        let prefix = captures.get(3).map(|x| x.as_str().to_owned());

        Ok(S3Path {
            bucket,
            prefix,
            region: Region::from_static("us-east-1"),
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum FindSize {
    Equal(i64),
    Bigger(i64),
    Lower(i64),
}

impl FromStr for FindSize {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, anyhow::Error> {
        let re = Regex::new(r"^([+-]?)(\d*)([kMGTP]?)$")?;
        let m = re.captures(s).ok_or(FindError::SizeParse)?;

        let sign = m
            .get(1)
            .ok_or(FindError::SizeParse)?
            .as_str()
            .chars()
            .next();
        let number: i64 = m.get(2).ok_or(FindError::SizeParse)?.as_str().parse()?;
        let metric = m
            .get(3)
            .ok_or(FindError::SizeParse)?
            .as_str()
            .chars()
            .next();

        let bytes = match metric {
            None => number,
            Some('k') => number * 1024,
            Some('M') => number * 1024_i64.pow(2),
            Some('G') => number * 1024_i64.pow(3),
            Some('T') => number * 1024_i64.pow(4),
            Some('P') => number * 1024_i64.pow(5),
            Some(_) => return Err(FindError::SizeParse.into()),
        };

        match sign {
            Some('+') => Ok(FindSize::Bigger(bytes)),
            Some('-') => Ok(FindSize::Lower(bytes)),
            None => Ok(FindSize::Equal(bytes)),
            Some(_) => Err(FindError::SizeParse.into()),
        }
    }
}

// Filter time range: 0__<time>__<now>
#[derive(Debug, Clone, PartialEq)]
pub enum FindTime {
    // time range <time>__<now>
    Upper(i64),
    // time range 0__<time>
    Lower(i64),
}

impl FromStr for FindTime {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, anyhow::Error> {
        let re = Regex::new(r"^([+-]?)(\d*)([smhdw]?)$")?;
        let m = re.captures(s).ok_or(FindError::TimeParse)?;

        let sign = m
            .get(1)
            .ok_or(FindError::TimeParse)?
            .as_str()
            .chars()
            .next();
        let number: i64 = m.get(2).ok_or(FindError::TimeParse)?.as_str().parse()?;
        let metric = m
            .get(3)
            .ok_or(FindError::TimeParse)?
            .as_str()
            .chars()
            .next();

        let seconds = match metric {
            None => number,
            Some('s') => number,
            Some('m') => number * 60,
            Some('h') => number * 3600,
            Some('d') => number * 3600 * 24,
            Some('w') => number * 3600 * 24 * 7,
            Some(_) => return Err(FindError::TimeParse.into()),
        };

        match sign {
            Some('-') => Ok(FindTime::Upper(seconds)),
            Some('+') => Ok(FindTime::Lower(seconds)),
            None => Ok(FindTime::Lower(seconds)),
            Some(_) => Err(FindError::TimeParse.into()),
        }
    }
}

pub type NameGlob = Pattern;

#[derive(Debug, Clone, PartialEq)]
pub struct InameGlob(pub Pattern);

impl FromStr for InameGlob {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, anyhow::Error> {
        let pattern = Pattern::from_str(s)?;
        Ok(InameGlob(pattern))
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct FindTag {
    pub key: String,
    pub value: String,
}

impl FromStr for FindTag {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, anyhow::Error> {
        let re = Regex::new(r"^(\w+):(\w+)$")?;
        let m = re.captures(s).ok_or(FindError::TagParseError)?;

        let key = m.get(1).ok_or(FindError::TagKeyParseError)?.as_str();
        let value = m.get(2).ok_or(FindError::TagValueParseError)?.as_str();

        Ok(FindTag {
            key: key.to_string(),
            value: value.to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn region_correct() {
        let region_result = region("us-west-2");
        assert!(region_result.is_ok());

        let r = region_result.unwrap();
        assert_eq!(r.as_ref(), "us-west-2");

        let region_result = region("eu-central-1");
        assert!(region_result.is_ok());

        let r = region_result.unwrap();
        assert_eq!(r.as_ref(), "eu-central-1");
    }

    #[test]
    fn s3path_correct() {
        assert_eq!(
            "s3://testbucket/".parse().ok(),
            Some(S3Path {
                bucket: "testbucket".to_owned(),
                prefix: Some("".to_owned()),
                region: Region::from_static("us-east-1"),
            })
        );

        assert_eq!(
            "s3://testbucket/path".parse().ok(),
            Some(S3Path {
                bucket: "testbucket".to_owned(),
                prefix: Some("path".to_owned()),
                region: Region::from_static("us-east-1"),
            })
        );

        assert_eq!(
            "s3://testbucket/multi/path".parse().ok(),
            Some(S3Path {
                bucket: "testbucket".to_owned(),
                prefix: Some("multi/path".to_owned()),
                region: Region::from_static("us-east-1"),
            })
        );

        assert_eq!(
            "s3://testbucket".parse().ok(),
            Some(S3Path {
                bucket: "testbucket".to_owned(),
                prefix: None,
                region: Region::from_static("us-east-1"),
            })
        );
    }

    #[test]
    fn s3path_incorrect() {
        assert!("testbucket".parse::<S3Path>().is_err());
        assert!("s3://".parse::<S3Path>().is_err());
        assert!("s3:/testbucket".parse::<S3Path>().is_err());
        assert!("://testbucket".parse::<S3Path>().is_err());
        assert!("as3://testbucket".parse::<S3Path>().is_err());
    }

    #[test]
    fn size_corect() {
        assert_eq!("11".parse().ok(), Some(FindSize::Equal(11)));
        assert_eq!("11k".parse().ok(), Some(FindSize::Equal(11 * 1024)));
        assert_eq!(
            "11M".parse().ok(),
            Some(FindSize::Equal(11 * 1024_i64.pow(2)))
        );
        assert_eq!(
            "11G".parse().ok(),
            Some(FindSize::Equal(11 * 1024_i64.pow(3)))
        );
        assert_eq!(
            "11T".parse().ok(),
            Some(FindSize::Equal(11 * 1024_i64.pow(4)))
        );
        assert_eq!(
            "11P".parse().ok(),
            Some(FindSize::Equal(11 * 1024_i64.pow(5)))
        );
        assert_eq!("+11".parse().ok(), Some(FindSize::Bigger(11)));
        assert_eq!("+11k".parse().ok(), Some(FindSize::Bigger(11 * 1024)));
        assert_eq!("-11".parse().ok(), Some(FindSize::Lower(11)));
        assert_eq!("-11k".parse().ok(), Some(FindSize::Lower(11 * 1024)));
    }

    #[test]
    fn size_incorect() {
        assert!("-".parse::<FindSize>().is_err());
        assert!("-123w".parse::<FindSize>().is_err());
        assert!(FindSize::from_str("").is_err());
        assert!(FindSize::from_str("*5").is_err());
        assert!(FindSize::from_str("+k").is_err());
        assert!(FindSize::from_str("10Z").is_err());
        assert!(FindSize::from_str("10a5k").is_err());
    }

    #[test]
    fn time_corect() {
        assert_eq!("11".parse().ok(), Some(FindTime::Lower(11)));
        assert_eq!("11s".parse().ok(), Some(FindTime::Lower(11)));
        assert_eq!("11m".parse().ok(), Some(FindTime::Lower(11 * 60)));
        assert_eq!("11h".parse().ok(), Some(FindTime::Lower(11 * 3600)));
        assert_eq!("11d".parse().ok(), Some(FindTime::Lower(11 * 3600 * 24)));
        assert_eq!(
            "11w".parse().ok(),
            Some(FindTime::Lower(11 * 3600 * 24 * 7))
        );
        assert_eq!("+11".parse().ok(), Some(FindTime::Lower(11)));
        assert_eq!("+11m".parse().ok(), Some(FindTime::Lower(11 * 60)));
        assert_eq!("-11m".parse().ok(), Some(FindTime::Upper(11 * 60)));
        assert_eq!("-11".parse().ok(), Some(FindTime::Upper(11)));
    }

    #[test]
    fn time_incorect() {
        assert!("-".parse::<FindTime>().is_err());
        assert!("-10t".parse::<FindTime>().is_err());
        assert!("+".parse::<FindTime>().is_err());
        assert!("+10t".parse::<FindTime>().is_err());
        assert!(FindTime::from_str("").is_err());
        assert!(FindTime::from_str("*5").is_err());
        assert!(FindTime::from_str("+d").is_err());
        assert!(FindTime::from_str("10y").is_err());
        assert!(FindTime::from_str("10a5d").is_err());
    }

    #[test]
    fn tag_ok() {
        assert_eq!(
            "tag1:value2".parse().ok(),
            Some(FindTag {
                key: "tag1".to_owned(),
                value: "value2".to_owned()
            })
        );
    }

    #[test]
    fn tag_incorect() {
        assert!("tag1value2".parse::<FindTag>().is_err());
        assert!("tag1:value2:".parse::<FindTag>().is_err());
        assert!(":".parse::<FindTag>().is_err());
        assert!(FindTag::from_str(":value").is_err());
        assert!(FindTag::from_str("key:").is_err());
        assert!(FindTag::from_str("key-:value").is_err());
        assert!(FindTag::from_str("key:value-").is_err());
        assert!(FindTag::from_str("key:value:extra").is_err(),);
        assert!(FindTag::from_str("").is_err());
        assert!(FindTag::from_str("keyvalue").is_err());
    }

    #[test]
    fn iname_glob_correct() {
        let glob = InameGlob::from_str("*.txt").unwrap();
        assert!(glob.0.matches("file.txt"));
        assert!(!glob.0.matches("file.png"));

        let glob = InameGlob::from_str("file-?.txt").unwrap();
        assert!(glob.0.matches("file-1.txt"));
        assert!(!glob.0.matches("file-12.txt"));
    }

    #[test]
    fn iname_glob_incorrect() {
        // Test for invalid bracket expression which is a syntax error in glob patterns
        assert!(InameGlob::from_str("[a-").is_err());
        assert!(InameGlob::from_str("[!").is_err());
    }
}
