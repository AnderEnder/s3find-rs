use failure::Error;
use glob::Pattern;
use regex::Regex;
use rusoto_core::Region;
use std::str::FromStr;
use structopt::clap::AppSettings;

/// Walk a s3 path hierarchy
#[derive(StructOpt, Debug, Clone)]
#[structopt(
    name = "s3find",
    raw(
        global_settings = "&[AppSettings::ColoredHelp, AppSettings::NeedsLongHelp, AppSettings::NeedsSubcommandHelp]"
    ),
    after_help = r#"
The authorization flow is the following chain:
  * use credentials from arguments provided by users
  * use environment variable credentials: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
  * use credentials via aws file profile.
    Profile can be set via environment variable AWS_PROFILE
    Profile file can be set via environment variable AWS_SHARED_CREDENTIALS_FILE
  * use AWS instance IAM profile
  * use AWS container IAM profile
"#
)]
pub struct FindOpt {
    /// S3 path to walk through. It should be s3://bucket/path
    #[structopt(name = "path")] //, raw(index = r#"1"#))]
    pub path: S3path,

    /// AWS access key. Unrequired.
    #[structopt(
        name = "aws_access_key",
        long = "aws-access-key",
        raw(requires_all = r#"&["aws_secret_key"]"#)
    )]
    pub aws_access_key: Option<String>,

    /// AWS secret key. Unrequired
    #[structopt(
        name = "aws_secret_key",
        long = "aws-secret-key",
        raw(requires_all = r#"&["aws_access_key"]"#)
    )]
    pub aws_secret_key: Option<String>,

    /// The region to use. Default value is us-east-1
    #[structopt(name = "aws_region", long = "aws-region", default_value="us-east-1")]
    pub aws_region: Region,

    /// Glob pattern for match, can be multiple
    #[structopt(name = "npatern", long = "name", raw(number_of_values = "1"))]
    pub name: Vec<NameGlob>,

    /// Case-insensitive glob pattern for match, can be multiple
    #[structopt(name = "ipatern", long = "iname", raw(number_of_values = "1"))]
    pub iname: Vec<InameGlob>,

    /// Regex pattern for match, can be multiple
    #[structopt(name = "rpatern", long = "regex", raw(number_of_values = "1"))]
    pub regex: Vec<Regex>,

    /// Modification time for match
    #[structopt(
        name = "time",
        long = "mtime",
        raw(number_of_values = "1", allow_hyphen_values = "true"),
        long_help = r#"Modification time for match, a time period:
    +5d - for period from now-5d to now
    -5d - for period  before now-5d

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
    #[structopt(
        name = "bytes_size",
        long = "size",
        raw(number_of_values = "1", allow_hyphen_values = "true"),
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

    //  /// Action to be ran with matched list of paths
    #[structopt(subcommand)]
    pub cmd: Option<Cmd>,
}

#[derive(StructOpt, Debug, PartialEq, Clone)]
pub enum Cmd {
    /// Exec any shell program with every key
    #[structopt(name = "-exec")]
    Exec {
        /// Utility(program) to run
        #[structopt(name = "utility")]
        utility: String,
    },

    /// Extended print with detail information
    #[structopt(name = "-print")]
    Print,

    /// Delete matched keys
    #[structopt(name = "-delete")]
    Delete,

    /// Download matched keys
    #[structopt(name = "-download")]
    Download {
        /// Force download files(overwrite) even if the target files are already present
        #[structopt(long = "force", short = "f")]
        force: bool,

        /// Directory destination to download files to
        #[structopt(name = "destination")]
        destination: String,
    },

    /// Print the list of matched keys
    #[structopt(name = "-ls")]
    Ls,

    /// Print the list of matched keys with tags
    #[structopt(name = "-lstags")]
    LsTags,

    /// Set the tags(overwrite) for the matched keys
    #[structopt(name = "-tags")]
    Tags {
        /// List of the tags to set
        #[structopt(name = "key:value", raw(min_values = "1"))]
        tags: Vec<FindTag>,
    },

    /// Make the matched keys public available (readonly)
    #[structopt(name = "-public")]
    Public,
}

#[derive(Fail, Debug)]
pub enum FindError {
    #[fail(display = "Invalid s3 path")]
    S3Parse,
    #[fail(display = "Invalid size parameter")]
    SizeParse,
    #[fail(display = "Invalid mtime parameter")]
    TimeParse,
    #[fail(display = "Cannot parse tag")]
    TagParseError,
    #[fail(display = "Cannot parse tag key")]
    TagKeyParseError,
    #[fail(display = "Cannot parse tag value")]
    TagValueParseError,
}

#[derive(Debug, Clone, PartialEq)]
pub struct S3path {
    pub bucket: String,
    pub prefix: Option<String>,
}

impl FromStr for S3path {
    type Err = Error;

    fn from_str(s: &str) -> Result<S3path, Error> {
        let regex = Regex::new(r#"s3://([\d\w _-]+)(/([\d\w _-]*))?"#).unwrap();
        let captures = regex.captures(s).ok_or(FindError::S3Parse)?;

        let bucket = captures.get(1).map(|x| x.as_str().to_owned()).ok_or(FindError::S3Parse)?;
        let prefix = captures.get(3).map(|x|x.as_str().to_owned());

        Ok(S3path {
            bucket,
            prefix,
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
    type Err = Error;

    fn from_str(s: &str) -> Result<FindSize, Error> {
        let re = Regex::new(r"([+-]?)(\d*)([kMGTP]?)$")?;
        let m = re.captures(s).unwrap();

        let sign = m.get(1).unwrap().as_str().chars().next();
        let number: i64 = m.get(2).unwrap().as_str().parse()?;
        let metric = m.get(3).unwrap().as_str().chars().next();

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

#[derive(Debug, Clone, PartialEq)]
pub enum FindTime {
    Upper(i64),
    Lower(i64),
}

impl FromStr for FindTime {
    type Err = Error;

    fn from_str(s: &str) -> Result<FindTime, Error> {
        let re = Regex::new(r"([+-]?)(\d*)([smhdw]?)$")?;
        let m = re.captures(s).unwrap();

        let sign = m.get(1).unwrap().as_str().chars().next();
        let number: i64 = m.get(2).unwrap().as_str().parse()?;
        let metric = m.get(3).unwrap().as_str().chars().next();

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
            Some('+') => Ok(FindTime::Upper(seconds)),
            Some('-') => Ok(FindTime::Lower(seconds)),
            None => Ok(FindTime::Upper(seconds)),
            Some(_) => Err(FindError::TimeParse.into()),
        }
    }
}

pub type NameGlob = Pattern;

#[derive(Debug, Clone, PartialEq)]
pub struct InameGlob(pub Pattern);

impl FromStr for InameGlob {
    type Err = Error;

    fn from_str(s: &str) -> Result<InameGlob, Error> {
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
    type Err = Error;

    fn from_str(s: &str) -> Result<FindTag, Error> {
        let re = Regex::new(r"(\w+):(\w+)$")?;
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
    fn s3path_correct() {
        assert_eq!(
            "s3://testbucket/".parse().ok(),
            Some(S3path {
                bucket: "testbucket".to_owned(),
                prefix: Some("".to_owned()),
            })
        );

        assert_eq!(
            "s3://testbucket/path".parse().ok(),
            Some(S3path {
                bucket: "testbucket".to_owned(),
                prefix: Some("path".to_owned())
            })
        );

        assert_eq!(
            "s3://testbucket".parse().ok(),
            Some(S3path {
                bucket: "testbucket".to_owned(),
                prefix: None
            })
        );
    }

    #[test]
    fn s3path_incorrect() {
        assert!("testbucket".parse::<S3path>().is_err());
        assert!("s3://".parse::<S3path>().is_err());
        assert!("s3:/testbucket".parse::<S3path>().is_err());
        assert!("://testbucket".parse::<S3path>().is_err());
    }

    #[test]
    fn size_corect() {
        assert_eq!("11".parse().ok(), Some(FindSize::Equal(11)));
        assert_eq!("11k".parse().ok(), Some(FindSize::Equal(11 * 1024)));
        assert_eq!("11M".parse().ok(), Some(FindSize::Equal(11 * 1024_i64.pow(2))));
        assert_eq!("11G".parse().ok(), Some(FindSize::Equal(11 * 1024_i64.pow(3))));
        assert_eq!("11T".parse().ok(), Some(FindSize::Equal(11 * 1024_i64.pow(4))));
        assert_eq!("11P".parse().ok(), Some(FindSize::Equal(11 * 1024_i64.pow(5))));
        assert_eq!("+11".parse().ok(), Some(FindSize::Bigger(11)));
        assert_eq!("+11k".parse().ok(), Some(FindSize::Bigger(11 * 1024)));
        assert_eq!("-11".parse().ok(), Some(FindSize::Lower(11)));
        assert_eq!("-11k".parse().ok(), Some(FindSize::Lower(11 * 1024)));
    }

    #[test]
    fn size_incorect() {
        assert!("-".parse::<FindSize>().is_err());
        assert!("-123w".parse::<FindSize>().is_err());
    }

    #[test]
    fn time_corect() {
        assert_eq!("11".parse().ok(), Some(FindTime::Upper(11)));
        assert_eq!("11s".parse().ok(), Some(FindTime::Upper(11)));
        assert_eq!("11m".parse().ok(), Some(FindTime::Upper(11 * 60)));
        assert_eq!("11h".parse().ok(), Some(FindTime::Upper(11 * 3600)));
        assert_eq!("11d".parse().ok(), Some(FindTime::Upper(11 * 3600 * 24)));
        assert_eq!("11w".parse().ok(), Some(FindTime::Upper(11 * 3600 * 24 * 7)));
        assert_eq!("+11".parse().ok(), Some(FindTime::Upper(11)));
        assert_eq!("+11m".parse().ok(), Some(FindTime::Upper(11 * 60)));
        assert_eq!("-11m".parse().ok(), Some(FindTime::Lower(11 * 60)));
        assert_eq!("-11".parse().ok(), Some(FindTime::Lower(11)));

    }

    #[test]
    fn time_incorect() {
        assert!("-".parse::<FindTime>().is_err());
        assert!("-10t".parse::<FindTime>().is_err());
        assert!("+".parse::<FindTime>().is_err());
        assert!("+10t".parse::<FindTime>().is_err());
    }

    #[test]
    fn tag_ok() {
        assert_eq!(
            "tag1:value2".parse().ok(),
            Some(FindTag { key: "tag1".to_owned(), value: "value2".to_owned() })
        );
    }

    #[test]
    fn tag_incorect() {
        assert!("tag1value2".parse::<FindTag>().is_err());
        assert!("tag1:value2:".parse::<FindTag>().is_err());
        assert!(":".parse::<FindTag>().is_err());
    }
}
