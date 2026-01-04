use aws_sdk_s3::types::ObjectStorageClass;
use aws_sdk_s3::types::StorageClass;
use aws_sdk_s3::types::Tier;
use aws_types::region::Region;
use clap::{Args, Parser, Subcommand, ValueEnum, error::ErrorKind};
use glob::Pattern;
use lazy_static::lazy_static;
use regex::Regex;
use std::str::FromStr;
use thiserror::Error;

const MIN_BUCKET_LENGTH: usize = 3;
const MAX_BUCKET_LENGTH: usize = 63;

const INVALID_PREFIXES: &[&str] = &["sthree-.", "amzn-s3-demo-", "xn--"];
const INVALID_SUFFIXES: &[&str] = &["-s3alias", "--ol-s3", ".mrap", "--x-s3", "--table-s3"];

lazy_static! {
    static ref S3_PATH_REGEX: Regex =
        Regex::new(r#"^s3://([a-z0-9][a-z0-9.-]+[a-z0-9])(/(.*))?$"#,).unwrap();
}

/// Validates an S3 bucket name according to AWS rules:
/// https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html
fn validate_bucket_name(bucket: &str) -> bool {
    if bucket.len() < MIN_BUCKET_LENGTH || bucket.len() > MAX_BUCKET_LENGTH {
        return false;
    }

    let first_char = bucket.chars().next().unwrap();
    let last_char = bucket.chars().last().unwrap();
    if !first_char.is_ascii_lowercase() && !first_char.is_ascii_digit()
        || !last_char.is_ascii_alphanumeric()
    {
        return false;
    }

    if bucket.contains("..")
        || bucket.contains(".-")
        || bucket.contains("-.")
        || bucket.contains("__")
        || bucket.contains(' ')
        || INVALID_PREFIXES
            .iter()
            .any(|&prefix| bucket.starts_with(prefix))
        || INVALID_SUFFIXES
            .iter()
            .any(|&suffix| bucket.ends_with(suffix))
        || bucket
            .chars()
            .any(|c| !c.is_ascii_alphanumeric() && c != '-' && c != '.')
    {
        return false;
    }

    if bucket.split('.').all(|part| part.parse::<u8>().is_ok()) {
        return false;
    }

    true
}

fn region(s: &str) -> std::result::Result<Region, clap::Error> {
    Ok(Region::new(s.to_owned()))
}

fn parse_restore_days(s: &str) -> std::result::Result<i32, clap::Error> {
    let days = s
        .parse::<i32>()
        .map_err(|_| clap::Error::raw(ErrorKind::InvalidValue, "Invalid number"))?;
    if !(1..=365).contains(&days) {
        return Err(clap::Error::raw(
            ErrorKind::InvalidValue,
            "Days should be between 1 and 365",
        ));
    }
    Ok(days)
}

/// Walk an Amazon S3 path hierarchy
#[derive(Parser, Clone)]
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

    // The region to use. If not specified, will use the region from AWS profile or default to us-east-1
    #[arg(name = "aws-region", long, value_parser=region)]
    pub aws_region: Option<Region>,

    /// Custom S3 endpoint URL for non-AWS S3-compatible services (e.g., MinIO, Ceph)
    #[arg(name = "endpoint-url", long)]
    pub endpoint_url: Option<String>,

    /// Force path-style bucket addressing (bucket.endpoint/key becomes endpoint/bucket/key)
    #[arg(name = "force-path-style", long)]
    pub force_path_style: bool,

    /// Glob pattern for match, can be multiple
    #[arg(name = "pattern", long = "name", number_of_values = 1)]
    pub name: Vec<NameGlob>,

    /// Case-insensitive glob pattern for match, can be multiple
    #[arg(name = "ipattern", long = "iname", number_of_values = 1)]
    pub iname: Vec<InameGlob>,

    /// Regex pattern for match, can be multiple
    #[arg(name = "rpattern", long = "regex", number_of_values = 1)]
    pub regex: Vec<Regex>,

    /// Object storage class for match
    #[arg(
        name = "storage-class",
        long = "storage-class",
        long_help = r#"Object storage class for match
Valid values are:
    DEEP_ARCHIVE
    EXPRESS_ONEZONE
    GLACIER
    GLACIER_IR
    INTELLIGENT_TIERING
    ONEZONE_IA
    OUTPOSTS
    REDUCED_REDUNDANCY
    SNOW
    STANDARD
    STANDARD_IA
    Unknown values are also supported"#
    )]
    pub storage_class: Option<ObjectStorageClass>,

    /// Filter objects by tag key=value pair (requires GetObjectTagging permission)
    #[arg(
        name = "tag",
        long = "tag",
        number_of_values = 1,
        long_help = r#"Filter objects by tag key-value pair.
Format: KEY=VALUE

Multiple --tag flags use AND logic (all must match).
This filter requires the s3:GetObjectTagging permission.

Note: Tag filtering requires an API call per object that passes other filters.
For large result sets, this can be slow and incur costs (~$0.0004 per 1,000 objects).
Apply other filters (--name, --mtime, --bytes-size) first to minimize API calls.

Examples:
    --tag environment=production
    --tag team=data-science --tag project=ml-pipeline"#
    )]
    pub tag: Vec<TagFilter>,

    /// Filter objects that have a specific tag key (any value)
    #[arg(
        name = "tag-exists",
        long = "tag-exists",
        number_of_values = 1,
        long_help = r#"Filter objects that have a specific tag key, regardless of value.

Multiple --tag-exists flags use AND logic (all keys must exist).
This filter requires the s3:GetObjectTagging permission.

Examples:
    --tag-exists environment
    --tag-exists owner --tag-exists project"#
    )]
    pub tag_exists: Vec<TagExistsFilter>,

    /// Maximum concurrent tag fetch requests (default: 50)
    #[arg(
        name = "tag-concurrency",
        long = "tag-concurrency",
        default_value = "50",
        long_help = r#"Maximum number of concurrent GetObjectTagging API requests.

Higher values increase throughput but may cause throttling.
AWS S3 supports ~3,500 requests/second per prefix.
Default: 50 (conservative, suitable for most workloads)"#
    )]
    pub tag_concurrency: usize,

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

    /// Include all object versions (uses ListObjectVersions API)
    #[arg(
        name = "all-versions",
        long,
        long_help = r#"Include all versions of objects, not just the current version.

When enabled, s3find uses the ListObjectVersions API instead of ListObjectsV2.
This shows all versions of each object, including delete markers.

Note: This option is not compatible with --maxdepth. If both are specified,
--all-versions takes precedence and --maxdepth is ignored.

Example:
  s3find s3://bucket --all-versions ls           # List all versions
  s3find s3://bucket --all-versions --name "*.log" ls
  s3find s3://bucket --all-versions print --format json"#
    )]
    pub all_versions: bool,

    /// Maximum depth to descend (uses S3 delimiter for efficient traversal)
    #[arg(
        name = "maxdepth",
        long,
        long_help = r#"Descend at most N levels of subdirectories below the starting prefix.

Depth is measured by subdirectory levels using S3's hierarchical structure:
  - maxdepth 0: Only objects at the prefix level (no subdirectories)
  - maxdepth 1: Prefix level + one subdirectory level
  - maxdepth 2: Prefix level + two subdirectory levels

Example: s3find s3://bucket/logs/ --maxdepth 1 ls
  logs/file.txt          → depth 0 (included, at prefix level)
  logs/2024/file.txt     → depth 1 (included, one subdirectory deep)
  logs/2024/01/file.txt  → depth 2 (excluded, two subdirectories deep)

Performance: Uses S3's Delimiter parameter for server-side filtering,
significantly reducing data transfer for deep hierarchies compared to
fetching all objects and filtering client-side."#
    )]
    pub maxdepth: Option<usize>,

    /// Action to be ran with matched list of paths
    #[command(subcommand)]
    pub cmd: Option<Cmd>,
}

#[derive(Subcommand, Clone, PartialEq, Debug)]
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

    /// Restore objects from Glacier and Deep Archive storage
    #[command(name = "restore")]
    Restore(Restore),

    /// Change storage class of matched objects and move objects to Glacier or Deep Archive
    #[command(name = "change-storage")]
    ChangeStorage(ChangeStorage),

    /// Do not do anything with keys, do not print them as well
    #[command(name = "nothing")]
    Nothing(DoNothing),
}

impl Default for Cmd {
    fn default() -> Self {
        Cmd::Ls(FastPrint {})
    }
}

#[derive(Args, Clone, PartialEq, Debug)]
pub struct FastPrint {}

#[derive(ValueEnum, Clone, Default, PartialEq, Debug)]
pub enum PrintFormat {
    /// default human-readable format with all object metadata
    #[default]
    Text,
    /// JSON format with all object metadata
    Json,
    /// CSV format with all object metadata
    Csv,
}

#[derive(Args, Clone, Default, PartialEq, Debug)]
pub struct AdvancedPrint {
    /// format for print subcommand
    #[arg(name = "format", long, default_value = "text")]
    pub format: PrintFormat,
}

#[derive(Args, Clone, PartialEq, Debug)]
pub struct MultipleDelete {}

#[derive(Args, Clone, PartialEq, Debug)]
pub struct ListTags {}

// region ?
#[derive(Args, Clone, PartialEq, Debug)]
pub struct SetPublic {}

#[derive(Args, Clone, PartialEq, Debug)]
pub struct Exec {
    /// Utility(program) to run
    #[arg(name = "utility", long)]
    pub utility: String,
}

#[derive(Args, Clone, PartialEq, Debug)]
pub struct Download {
    /// Force download files(overwrite) even if the target files are already present
    #[arg(long = "force")]
    pub force: bool,

    /// Directory destination to download files to
    #[arg(name = "destination")]
    pub destination: String,
}

#[derive(Args, Clone, PartialEq, Debug)]
pub struct S3Copy {
    /// S3 path destination to copy files to
    #[arg(name = "destination")]
    pub destination: S3Path,

    /// Copy keys like files
    #[arg(long = "flat")]
    pub flat: bool,

    /// Storage class for the copied objects
    #[arg(long = "storage-class")]
    pub storage_class: Option<StorageClass>,
}

#[derive(Args, Clone, PartialEq, Debug)]
pub struct S3Move {
    /// S3 path destination to copy files to
    #[arg(name = "destination")]
    pub destination: S3Path,

    /// Copy keys like files
    #[arg(long = "flat")]
    pub flat: bool,

    /// Storage class for the moved objects
    #[arg(long = "storage-class")]
    pub storage_class: Option<StorageClass>,
}

#[derive(Args, Clone, PartialEq, Debug)]
pub struct SetTags {
    /// List of the tags to set
    #[arg(name = "key:value", number_of_values = 1)]
    pub tags: Vec<FindTag>,
}

#[derive(Args, Clone, PartialEq, Debug)]
pub struct DoNothing {}

#[derive(Args, Clone, PartialEq, Debug)]
pub struct Restore {
    /// Number of days to keep the restored objects
    #[arg(long, default_value = "1", value_parser = parse_restore_days)]
    pub days: i32,

    /// Retrieval tier for restoring objects
    #[arg(long, default_value = "Standard")]
    pub tier: Tier,
}

#[derive(Args, Clone, PartialEq, Debug)]
pub struct ChangeStorage {
    /// New storage class to apply to objects
    #[arg(name = "class")]
    pub storage_class: StorageClass,
}

impl Default for Restore {
    fn default() -> Self {
        Self {
            days: 1,
            tier: Tier::Standard,
        }
    }
}

#[derive(Error, Debug)]
pub enum FindError {
    #[error("Invalid s3 path: {0}")]
    S3Parse(String),
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
    #[error("Invalid days value: it should be in between 1 and 365")]
    RestoreDaysParseError,
    #[error("Invalid tag filter format. Expected KEY=VALUE, got: {0}")]
    TagFilterParseError(String),
    #[error("Tag filter key cannot be empty")]
    TagFilterEmptyKey,
}

#[derive(Debug, Clone, PartialEq)]
pub struct S3Path {
    pub bucket: String,
    pub prefix: Option<String>,
}

impl FromStr for S3Path {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, anyhow::Error> {
        let captures = S3_PATH_REGEX
            .captures(s)
            .ok_or(FindError::S3Parse(s.to_string()))?;

        let bucket = captures
            .get(1)
            .map(|x| x.as_str().to_owned())
            .ok_or(FindError::S3Parse(s.to_string()))?;

        if !validate_bucket_name(&bucket) {
            return Err(FindError::S3Parse(s.to_string()).into());
        }

        let prefix = captures.get(3).map(|x| {
            let s = x.as_str();
            // Strip trailing slashes for consistent behavior
            let trimmed = s.trim_end_matches('/');
            trimmed.to_owned()
        });

        Ok(S3Path { bucket, prefix })
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

/// Filter for matching objects by tag key-value pairs.
/// Format: KEY=VALUE
#[derive(Debug, PartialEq, Clone)]
pub struct TagFilter {
    pub key: String,
    pub value: String,
}

impl FromStr for TagFilter {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, anyhow::Error> {
        let parts: Vec<&str> = s.splitn(2, '=').collect();
        if parts.len() != 2 {
            return Err(FindError::TagFilterParseError(s.to_string()).into());
        }

        let key = parts[0].trim();
        let value = parts[1].trim();

        if key.is_empty() {
            return Err(FindError::TagFilterEmptyKey.into());
        }

        Ok(TagFilter {
            key: key.to_string(),
            value: value.to_string(),
        })
    }
}

/// Filter for matching objects that have a specific tag key (regardless of value).
#[derive(Debug, PartialEq, Clone)]
pub struct TagExistsFilter {
    pub key: String,
}

impl FromStr for TagExistsFilter {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, anyhow::Error> {
        let key = s.trim();
        if key.is_empty() {
            return Err(FindError::TagFilterEmptyKey.into());
        }

        Ok(TagExistsFilter {
            key: key.to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

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
            })
        );

        assert_eq!(
            "s3://testbucket/path".parse().ok(),
            Some(S3Path {
                bucket: "testbucket".to_owned(),
                prefix: Some("path".to_owned()),
            })
        );

        assert_eq!(
            "s3://testbucket/multi/path".parse().ok(),
            Some(S3Path {
                bucket: "testbucket".to_owned(),
                prefix: Some("multi/path".to_owned()),
            })
        );

        assert_eq!(
            "s3://testbucket".parse().ok(),
            Some(S3Path {
                bucket: "testbucket".to_owned(),
                prefix: None,
            })
        );
    }

    #[test]
    fn s3path_trailing_slash_handling() {
        // Trailing slashes should be stripped for consistent behavior
        assert_eq!(
            "s3://testbucket/path/".parse().ok(),
            Some(S3Path {
                bucket: "testbucket".to_owned(),
                prefix: Some("path".to_owned()),
            })
        );

        assert_eq!(
            "s3://testbucket/multi/path/".parse().ok(),
            Some(S3Path {
                bucket: "testbucket".to_owned(),
                prefix: Some("multi/path".to_owned()),
            })
        );

        // Multiple trailing slashes should also be stripped
        assert_eq!(
            "s3://testbucket/path///".parse().ok(),
            Some(S3Path {
                bucket: "testbucket".to_owned(),
                prefix: Some("path".to_owned()),
            })
        );

        // s3://bucket/ should still result in empty prefix
        assert_eq!(
            "s3://testbucket/".parse().ok(),
            Some(S3Path {
                bucket: "testbucket".to_owned(),
                prefix: Some("".to_owned()),
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
    fn s3path_validation_bypass() {
        let invalid_cases = vec![
            "s3://bucket..name",      // Double dots not allowed
            "s3://bucket.-name",      // Can't start segment with dash
            "s3://bucket-.name",      // Can't end segment with dash
            "s3://my..bucket",        // Double dots between segments
            "s3://UPPERCASE-BUCKET",  // Uppercase not allowed in new buckets
            "s3://bucket_name",       // Underscores not allowed in new buckets
            "s3://bucket.with space", // Spaces not allowed
            "s3://192.168.1.1",       // IP address format not allowed
            "s3://bucket.",           // Can't end with dot
            "s3://bu",                // Too short (min 3 chars)
            "s3://bucket@name",       // Special chars not allowed
            "s3://xn--bucket",        // ACE/Punycode prefixes not allowed
            "s3://aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", // Too long (64 chars)
        ];

        for case in invalid_cases {
            assert!(
                case.parse::<S3Path>().is_err(),
                "Should reject invalid bucket name: {}",
                case
            );
        }

        // Test valid cases
        let valid_cases = vec![
            "s3://my-bucket-name/path",
            "s3://my.bucket.name/path",
            "s3://mybucket123/path",
            "s3://123-bucket-name/",
        ];

        for case in valid_cases {
            let r = case.parse::<S3Path>();
            assert!(
                r.is_ok(),
                "Should accept valid bucket name: {}, error: {}",
                case,
                r.unwrap_err()
            );
        }
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

    #[test]
    fn test_basic_path_parsing() {
        let args = FindOpt::parse_from(["s3find", "s3://mybucket/path"]);
        assert_eq!(args.path.bucket, "mybucket");
        assert_eq!(args.path.prefix, Some("path".to_string()));

        // Default values
        assert_eq!(args.aws_region, None);
        assert_eq!(args.endpoint_url, None);
        assert!(!args.force_path_style);
        assert_eq!(args.page_size, 1000);
        assert!(!args.summarize);
        assert!(args.cmd.is_none());
    }

    #[test]
    fn test_aws_credentials() {
        let args = FindOpt::parse_from([
            "s3find",
            "s3://mybucket",
            "--aws-access-key",
            "AKIAIOSFODNN7EXAMPLE",
            "--aws-secret-key",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        ]);

        assert_eq!(
            args.aws_access_key,
            Some("AKIAIOSFODNN7EXAMPLE".to_string())
        );
        assert_eq!(
            args.aws_secret_key,
            Some("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string())
        );
    }

    #[test]
    fn test_aws_region() {
        let args = FindOpt::parse_from(["s3find", "s3://mybucket", "--aws-region", "eu-west-1"]);

        assert_eq!(
            args.aws_region.as_ref().map(|r| r.as_ref()),
            Some("eu-west-1")
        );
    }

    #[test]
    fn test_name_filters() {
        let args = FindOpt::parse_from([
            "s3find",
            "s3://mybucket",
            "--name",
            "*.txt",
            "--name",
            "file?.doc",
        ]);

        assert_eq!(args.name.len(), 2);
        assert!(args.name[0].matches("file.txt"));
        assert!(!args.name[0].matches("file.jpg"));
        assert!(args.name[1].matches("file1.doc"));
        assert!(!args.name[1].matches("file12.doc"));
    }

    #[test]
    fn test_iname_filters() {
        let args = FindOpt::parse_from(["s3find", "s3://mybucket", "--iname", "*.txt"]);

        assert_eq!(args.iname.len(), 1);
        assert!(args.iname[0].0.matches("file.txt"));
        assert!(!args.iname[0].0.matches("file.jpg"));
    }

    #[test]
    fn test_regex_filters() {
        let args = FindOpt::parse_from(["s3find", "s3://mybucket", "--regex", r"^file_\d+\.txt$"]);

        assert_eq!(args.regex.len(), 1);
        assert!(args.regex[0].is_match("file_123.txt"));
        assert!(!args.regex[0].is_match("document_123.txt"));
    }

    #[test]
    fn test_storage_class_filter() {
        let args = FindOpt::parse_from(["s3find", "s3://mybucket", "--storage-class", "STANDARD"]);

        assert_eq!(args.storage_class, Some(ObjectStorageClass::Standard));
    }

    #[test]
    fn test_mtime_filter() {
        let args = FindOpt::parse_from([
            "s3find",
            "s3://mybucket",
            "--mtime",
            "-7d",
            "--mtime",
            "+1h",
        ]);

        assert_eq!(args.mtime.len(), 2);
        assert_eq!(args.mtime[0], FindTime::Upper(7 * 24 * 3600)); // -7d
        assert_eq!(args.mtime[1], FindTime::Lower(3600)); // +1h
    }

    #[test]
    fn test_size_filter() {
        let args = FindOpt::parse_from([
            "s3find",
            "s3://mybucket",
            "--bytes-size",
            "+1M",
            "--bytes-size",
            "-10k",
        ]);

        assert_eq!(args.size.len(), 2);
        assert_eq!(args.size[0], FindSize::Bigger(1024 * 1024)); // +1M
        assert_eq!(args.size[1], FindSize::Lower(10 * 1024)); // -10k
    }

    #[test]
    fn test_limit_and_page_size() {
        let args = FindOpt::parse_from([
            "s3find",
            "s3://mybucket",
            "--limit",
            "100",
            "--number",
            "500",
        ]);

        assert_eq!(args.limit, Some(100));
        assert_eq!(args.page_size, 500);
    }

    #[test]
    fn test_summarize_flag() {
        let args = FindOpt::parse_from(["s3find", "s3://mybucket", "--summarize"]);

        assert!(args.summarize);

        // Also test short form
        let args = FindOpt::parse_from(["s3find", "s3://mybucket", "-s"]);

        assert!(args.summarize);
    }

    #[test]
    fn test_ls_subcommand() {
        let args = FindOpt::parse_from(["s3find", "s3://mybucket", "ls"]);
        assert_eq!(args.cmd, Some(Cmd::Ls(FastPrint {})));
    }

    #[test]
    fn test_print_subcommand() {
        let args = FindOpt::parse_from(["s3find", "s3://mybucket", "print", "--format", "json"]);

        assert_eq!(
            args.cmd,
            Some(Cmd::Print(AdvancedPrint {
                format: PrintFormat::Json
            }))
        );
    }

    #[test]
    fn test_copy_subcommand() {
        let args = FindOpt::parse_from([
            "s3find",
            "s3://mybucket/source",
            "copy",
            "s3://otherbucket/dest",
            "--flat",
        ]);

        assert_eq!(
            args.cmd,
            Some(Cmd::Copy(S3Copy {
                destination: S3Path {
                    bucket: "otherbucket".to_string(),
                    prefix: Some("dest".to_string()),
                },
                flat: true,
                storage_class: None,
            }))
        );
    }

    #[test]
    fn test_copy_with_storage_class() {
        let args = FindOpt::parse_from([
            "s3find",
            "s3://mybucket/source",
            "copy",
            "s3://otherbucket/dest",
            "--storage-class",
            "GLACIER",
        ]);

        assert_eq!(
            args.cmd,
            Some(Cmd::Copy(S3Copy {
                destination: S3Path {
                    bucket: "otherbucket".to_string(),
                    prefix: Some("dest".to_string()),
                },
                flat: false,
                storage_class: Some(StorageClass::Glacier),
            }))
        );
    }

    #[test]
    fn test_delete_subcommand() {
        let args = FindOpt::parse_from(["s3find", "s3://mybucket", "delete"]);

        assert_eq!(args.cmd, Some(Cmd::Delete(MultipleDelete {})));
    }

    #[test]
    fn test_download_subcommand() {
        let args = FindOpt::parse_from([
            "s3find",
            "s3://mybucket",
            "download",
            "/tmp/downloads",
            "--force",
        ]);

        assert_eq!(
            args.cmd,
            Some(Cmd::Download(Download {
                destination: "/tmp/downloads".to_string(),
                force: true,
            }))
        );
    }

    #[test]
    fn test_exec_subcommand() {
        let args = FindOpt::parse_from(["s3find", "s3://mybucket", "exec", "--utility", "echo {}"]);

        assert_eq!(
            args.cmd,
            Some(Cmd::Exec(Exec {
                utility: "echo {}".to_string(),
            }))
        );
    }

    #[test]
    fn test_restore_subcommand() {
        let args_defaults = FindOpt::parse_from(["s3find", "s3://mybucket", "restore"]);

        assert_eq!(
            args_defaults.cmd,
            Some(Cmd::Restore(Restore {
                days: 1,
                tier: Tier::Standard,
            }))
        );

        let args = FindOpt::parse_from([
            "s3find",
            "s3://mybucket",
            "restore",
            "--days",
            "7",
            "--tier",
            "Bulk",
        ]);

        assert_eq!(
            args.cmd,
            Some(Cmd::Restore(Restore {
                days: 7,
                tier: Tier::Bulk,
            }))
        );

        let args = FindOpt::parse_from(["s3find", "s3://mybucket", "restore", "--days", "365"]);

        assert_eq!(
            args.cmd,
            Some(Cmd::Restore(Restore {
                days: 365,
                tier: Tier::Standard, // Default tier
            }))
        );

        let args =
            FindOpt::parse_from(["s3find", "s3://mybucket", "restore", "--tier", "Expedited"]);

        assert_eq!(
            args.cmd,
            Some(Cmd::Restore(Restore {
                days: 1, // Default days
                tier: Tier::Expedited,
            }))
        );

        let args = FindOpt::parse_from([
            "s3find",
            "s3://mybucket/glacier-objects",
            "--storage-class",
            "GLACIER",
            "--name",
            "*.archive",
            "restore",
            "--days",
            "30",
            "--tier",
            "Standard",
        ]);

        assert_eq!(args.storage_class, Some(ObjectStorageClass::Glacier));
        assert_eq!(args.name.len(), 1);
        assert_eq!(
            args.cmd,
            Some(Cmd::Restore(Restore {
                days: 30,
                tier: Tier::Standard,
            }))
        );
    }

    #[test]
    fn test_restore_default() {
        let restore_default = Restore::default();

        assert_eq!(restore_default.days, 1, "Default days should be 1");
        assert_eq!(
            restore_default.tier,
            Tier::Standard,
            "Default tier should be Standard"
        );

        let default_trait = <Restore as Default>::default();
        assert_eq!(
            restore_default, default_trait,
            "Both default methods should be equivalent"
        );

        let clone = restore_default.clone();
        assert_eq!(clone, restore_default, "Clone should equal original");
    }

    #[test]
    fn test_complex_command() {
        let args = FindOpt::parse_from([
            "s3find",
            "s3://mybucket/logs",
            "--name",
            "*.log",
            "--mtime",
            "-30d",
            "--bytes-size",
            "+1M",
            "--limit",
            "50",
            "--summarize",
            "copy",
            "s3://archivebucket/logs/2023",
        ]);

        assert_eq!(args.path.bucket, "mybucket");
        assert_eq!(args.path.prefix, Some("logs".to_string()));
        assert_eq!(args.name.len(), 1);
        assert_eq!(args.mtime.len(), 1);
        assert_eq!(args.size.len(), 1);
        assert_eq!(args.limit, Some(50));
        assert!(args.summarize);
        assert_eq!(
            args.cmd,
            Some(Cmd::Copy(S3Copy {
                destination: S3Path {
                    bucket: "archivebucket".to_string(),
                    prefix: Some("logs/2023".to_string()),
                },
                flat: false,
                storage_class: None,
            }))
        );
    }

    #[test]
    fn test_restore_with_multiple_filters() {
        let args = FindOpt::parse_from([
            "s3find",
            "s3://mybucket/archives",
            "--name",
            "*.bak",
            "--regex",
            r"^backup_\d{8}\.bak$",
            "--storage-class",
            "DEEP_ARCHIVE",
            "--mtime",
            "-90d",
            "--bytes-size",
            "+100M",
            "--limit",
            "10",
            "restore",
            "--days",
            "14",
            "--tier",
            "Standard",
        ]);

        assert_eq!(args.path.bucket, "mybucket");
        assert_eq!(args.path.prefix, Some("archives".to_string()));
        assert_eq!(args.name.len(), 1);
        assert_eq!(args.regex.len(), 1);
        assert_eq!(args.storage_class, Some(ObjectStorageClass::DeepArchive));
        assert_eq!(args.mtime.len(), 1);
        assert_eq!(args.mtime[0], FindTime::Upper(90 * 24 * 3600)); // -90d
        assert_eq!(args.size.len(), 1);
        assert_eq!(args.size[0], FindSize::Bigger(100 * 1024 * 1024)); // +100M
        assert_eq!(args.limit, Some(10));

        assert_eq!(
            args.cmd,
            Some(Cmd::Restore(Restore {
                days: 14,
                tier: Tier::Standard,
            }))
        );
    }

    #[test]
    fn test_parse_restore_days() {
        assert_eq!(parse_restore_days("1").unwrap(), 1);
        assert_eq!(parse_restore_days("7").unwrap(), 7);
        assert_eq!(parse_restore_days("30").unwrap(), 30);
        assert_eq!(parse_restore_days("365").unwrap(), 365);

        assert!(parse_restore_days("0").is_err(), "0 days should be invalid");
        assert!(
            parse_restore_days("366").is_err(),
            "366 days should be invalid"
        );
        assert!(
            parse_restore_days("-1").is_err(),
            "Negative days should be invalid"
        );
        assert!(
            parse_restore_days("abc").is_err(),
            "Non-numeric input should be invalid"
        );
        assert!(
            parse_restore_days("").is_err(),
            "Empty string should be invalid"
        );
    }

    #[test]
    fn test_move_with_storage_class() {
        let args = FindOpt::parse_from([
            "s3find",
            "s3://mybucket/source",
            "move",
            "s3://otherbucket/dest",
            "--storage-class",
            "INTELLIGENT_TIERING",
        ]);

        assert_eq!(
            args.cmd,
            Some(Cmd::Move(S3Move {
                destination: S3Path {
                    bucket: "otherbucket".to_string(),
                    prefix: Some("dest".to_string()),
                },
                flat: false,
                storage_class: Some(StorageClass::IntelligentTiering),
            }))
        );
    }

    #[test]
    fn test_change_storage_class_command() {
        let args = FindOpt::parse_from([
            "s3find",
            "s3://mybucket/data",
            "--mtime",
            "-30d",
            "change-storage",
            "GLACIER",
        ]);

        assert_eq!(
            args.cmd,
            Some(Cmd::ChangeStorage(ChangeStorage {
                storage_class: StorageClass::Glacier,
            }))
        );

        let args = FindOpt::parse_from([
            "s3find",
            "s3://mybucket/data",
            "change-storage",
            "DEEP_ARCHIVE",
        ]);

        assert_eq!(
            args.cmd,
            Some(Cmd::ChangeStorage(ChangeStorage {
                storage_class: StorageClass::DeepArchive,
            }))
        );
    }

    #[test]
    fn test_bucket_name_validation_correct() {
        assert!(validate_bucket_name("my-bucket-name"));
        assert!(validate_bucket_name("my.bucket.name"));
        assert!(validate_bucket_name("mybucket"));
        assert!(validate_bucket_name("my-bucket-123"));
        assert!(validate_bucket_name("123-bucket-name"));
    }

    #[test]
    fn test_bucket_name_validation_incorrect() {
        assert!(!validate_bucket_name("bu")); // Too short
        assert!(!validate_bucket_name("a".repeat(64).as_str())); // Too long
        assert!(!validate_bucket_name("my..bucket")); // Consecutive periods
        assert!(!validate_bucket_name("my.-bucket")); // Period next to hyphen
        assert!(!validate_bucket_name("my-.bucket")); // Hyphen next to period
        assert!(!validate_bucket_name("my__bucket")); // Consecutive underscores
        assert!(!validate_bucket_name("my bucket")); // Contains space
        assert!(!validate_bucket_name("xn--bucket")); // ACE/Punycode prefix
        assert!(!validate_bucket_name("my@bucket")); // Invalid character
        assert!(!validate_bucket_name("192.168.1.1")); // IP address format
        assert!(!validate_bucket_name("-mybucket")); // Starts with hyphen
        assert!(!validate_bucket_name("mybucket-")); // Ends with hyphen
        assert!(!validate_bucket_name(".mybucket")); // Starts with period
        assert!(!validate_bucket_name("mybucket.")); // Ends with period
    }

    #[test]
    fn test_bucket_name_prefix_suffix_validation() {
        for &prefix in INVALID_PREFIXES {
            let test_bucket = format!("{}bucket", prefix);
            assert!(
                !validate_bucket_name(&test_bucket),
                "Should reject bucket name with invalid prefix: {}",
                prefix
            );

            let test_bucket = format!("{}valid-bucket-name", prefix);
            assert!(
                !validate_bucket_name(&test_bucket),
                "Should reject bucket name with invalid prefix even with valid suffix: {}",
                prefix
            );
        }

        for &suffix in INVALID_SUFFIXES {
            let test_bucket = format!("bucket{}", suffix);
            assert!(
                !validate_bucket_name(&test_bucket),
                "Should reject bucket name with invalid suffix: {}",
                suffix
            );

            let test_bucket = format!("valid-bucket-name{}", suffix);
            assert!(
                !validate_bucket_name(&test_bucket),
                "Should reject bucket name with invalid suffix even with valid prefix: {}",
                suffix
            );
        }

        let valid_cases = vec![
            "mythree-bucket",        // Similar to "sthree-." but valid
            "my-s3-demo-bucket",     // Similar to "amzn-s3-demo-" but valid
            "bucket-s3",             // Similar to "-s3alias" but valid
            "bucket-ol-s3",          // Similar to "--ol-s3" but valid
            "bucket-wrap",           // Similar to ".mrap" but valid
            "bucket-x-s3-extra",     // Similar to "--x-s3" but valid
            "bucket-table-s3-extra", // Similar to "--table-s3" but valid
            "xn-bucket",             // Similar to "xn--" but valid
        ];

        for case in valid_cases {
            assert!(
                validate_bucket_name(case),
                "Should accept bucket name that is similar but valid: {}",
                case
            );
        }
    }

    #[test]
    fn test_validate_bucket_name_edge_cases() {
        assert!(!validate_bucket_name(""));

        assert!(!validate_bucket_name(&"a".repeat(64)));
        assert!(validate_bucket_name(&"a".repeat(3)));
        assert!(validate_bucket_name(&"a".repeat(63)));

        assert!(!validate_bucket_name("-bucket"));
        assert!(!validate_bucket_name("bucket-"));
        assert!(!validate_bucket_name(".bucket"));
        assert!(!validate_bucket_name("bucket."));

        assert!(!validate_bucket_name("bucket..name"));
        assert!(!validate_bucket_name("bucket.-name"));
        assert!(!validate_bucket_name("bucket-.name"));
        assert!(!validate_bucket_name("bucket__name"));

        assert!(!validate_bucket_name("192.168.1.1"));
        assert!(!validate_bucket_name("10.0.0.0"));
        assert!(!validate_bucket_name("172.16.0.1"));

        assert!(validate_bucket_name("1bucket2"));
        assert!(validate_bucket_name("bucket.name"));
        assert!(validate_bucket_name("bucket-name"));
        assert!(validate_bucket_name("b.u.c.k.e.t"));
        assert!(validate_bucket_name("b-u-c-k-e-t"));
    }

    #[test]
    fn test_endpoint_url_option() {
        let args = FindOpt::parse_from([
            "s3find",
            "s3://mybucket",
            "--endpoint-url",
            "http://localhost:9000",
        ]);

        assert_eq!(args.endpoint_url, Some("http://localhost:9000".to_string()));
        assert!(!args.force_path_style);
    }

    #[test]
    fn test_force_path_style_option() {
        let args = FindOpt::parse_from(["s3find", "s3://mybucket", "--force-path-style"]);

        assert!(args.force_path_style);
        assert_eq!(args.endpoint_url, None);
    }

    #[test]
    fn test_minio_configuration() {
        let args = FindOpt::parse_from([
            "s3find",
            "s3://mybucket/path",
            "--endpoint-url",
            "http://minio.local:9000",
            "--force-path-style",
            "--aws-access-key",
            "minioadmin",
            "--aws-secret-key",
            "minioadmin",
            "--aws-region",
            "us-east-1",
        ]);

        assert_eq!(
            args.endpoint_url,
            Some("http://minio.local:9000".to_string())
        );
        assert!(args.force_path_style);
        assert_eq!(args.aws_access_key, Some("minioadmin".to_string()));
        assert_eq!(args.aws_secret_key, Some("minioadmin".to_string()));
        assert_eq!(
            args.aws_region.as_ref().map(|r| r.as_ref()),
            Some("us-east-1")
        );
        assert_eq!(args.path.bucket, "mybucket");
        assert_eq!(args.path.prefix, Some("path".to_string()));
    }

    #[test]
    fn test_ceph_configuration() {
        let args = FindOpt::parse_from([
            "s3find",
            "s3://mybucket",
            "--endpoint-url",
            "https://ceph.example.com",
            "--force-path-style",
            "ls",
        ]);

        assert_eq!(
            args.endpoint_url,
            Some("https://ceph.example.com".to_string())
        );
        assert!(args.force_path_style);
        assert_eq!(args.cmd, Some(Cmd::Ls(FastPrint {})));
    }

    #[test]
    fn tag_filter_correct() {
        assert_eq!(
            "environment=production".parse().ok(),
            Some(TagFilter {
                key: "environment".to_owned(),
                value: "production".to_owned()
            })
        );

        // Key with value containing special characters
        assert_eq!(
            "project=my-project-123".parse().ok(),
            Some(TagFilter {
                key: "project".to_owned(),
                value: "my-project-123".to_owned()
            })
        );

        // Empty value is allowed
        assert_eq!(
            "status=".parse().ok(),
            Some(TagFilter {
                key: "status".to_owned(),
                value: "".to_owned()
            })
        );

        // Value with equals sign (splitn handles this)
        assert_eq!(
            "equation=a=b+c".parse().ok(),
            Some(TagFilter {
                key: "equation".to_owned(),
                value: "a=b+c".to_owned()
            })
        );

        // Whitespace is trimmed
        assert_eq!(
            "  key  =  value  ".parse().ok(),
            Some(TagFilter {
                key: "key".to_owned(),
                value: "value".to_owned()
            })
        );
    }

    #[test]
    fn tag_filter_incorrect() {
        // Missing equals sign
        assert!(TagFilter::from_str("environmentproduction").is_err());

        // Empty key
        assert!(TagFilter::from_str("=value").is_err());

        // Whitespace only key
        assert!(TagFilter::from_str("   =value").is_err());

        // Empty string
        assert!(TagFilter::from_str("").is_err());
    }

    #[test]
    fn tag_exists_filter_correct() {
        assert_eq!(
            "environment".parse().ok(),
            Some(TagExistsFilter {
                key: "environment".to_owned()
            })
        );

        // With dashes and numbers
        assert_eq!(
            "my-tag-123".parse().ok(),
            Some(TagExistsFilter {
                key: "my-tag-123".to_owned()
            })
        );

        // Whitespace is trimmed
        assert_eq!(
            "  key  ".parse().ok(),
            Some(TagExistsFilter {
                key: "key".to_owned()
            })
        );
    }

    #[test]
    fn tag_exists_filter_incorrect() {
        // Empty string
        assert!(TagExistsFilter::from_str("").is_err());

        // Whitespace only
        assert!(TagExistsFilter::from_str("   ").is_err());
    }

    #[test]
    fn test_tag_filter_cli_flags() {
        let args = FindOpt::parse_from([
            "s3find",
            "s3://mybucket",
            "--tag",
            "env=prod",
            "--tag",
            "team=data",
            "--tag-exists",
            "owner",
            "--tag-concurrency",
            "100",
        ]);

        assert_eq!(args.tag.len(), 2);
        assert_eq!(
            args.tag[0],
            TagFilter {
                key: "env".to_string(),
                value: "prod".to_string()
            }
        );
        assert_eq!(
            args.tag[1],
            TagFilter {
                key: "team".to_string(),
                value: "data".to_string()
            }
        );
        assert_eq!(args.tag_exists.len(), 1);
        assert_eq!(
            args.tag_exists[0],
            TagExistsFilter {
                key: "owner".to_string()
            }
        );
        assert_eq!(args.tag_concurrency, 100);
    }

    #[test]
    fn test_tag_concurrency_default() {
        let args = FindOpt::parse_from(["s3find", "s3://mybucket"]);
        assert_eq!(args.tag_concurrency, 50);
    }
}
