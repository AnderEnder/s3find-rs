use structopt::clap::AppSettings;

use regex::Regex;
use rusoto_core::Region;

use types::*;

/// Walk a s3 path hierarchy
#[derive(StructOpt, Debug, Clone)]
#[structopt(
    name = "s3find",
    raw(
        global_settings = "&[AppSettings::ColoredHelp, AppSettings::NeedsLongHelp, AppSettings::NeedsSubcommandHelp]"
    )
)]
pub struct FindOpt {
    /// S3 path to walk through. It should be s3://bucket/path
    #[structopt(name = "path")] //, raw(index = r#"1"#))]
    pub path: S3path,

    /// AWS access key. Unrequired
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
    #[structopt(name = "aws_region", long = "aws-region")]
    pub aws_region: Option<Region>,

    /// Glob pattern for match, can be multiple
    #[structopt(name = "npatern", long = "name", raw(number_of_values = "1"))]
    pub name: Vec<NameGlob>,

    /// Case-insensitive glob pattern for match, can be multiple
    #[structopt(name = "ipatern", long = "iname", raw(number_of_values = "1"))]
    pub iname: Vec<InameGlob>,

    /// Regex pattern for match, can be multiple
    #[structopt(name = "rpatern", long = "regex", raw(number_of_values = "1"))]
    pub regex: Vec<Regex>,

    #[structopt(
        name = "time",
        long = "mtime",
        raw(number_of_values = "1", allow_hyphen_values = "true"),
        help = r#"Modification time for match, a time period:
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

    #[structopt(
        name = "bytes_size",
        long = "size",
        raw(number_of_values = "1", allow_hyphen_values = "true"),
        help = r#"File size for match:
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
