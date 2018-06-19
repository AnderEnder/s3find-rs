#[macro_use]
extern crate structopt;

#[macro_use]
extern crate failure;

extern crate chrono;
extern crate clap;
extern crate futures;
extern crate glob;
extern crate indicatif;
extern crate regex;
extern crate rusoto_core;
extern crate rusoto_credential;
extern crate rusoto_s3;

mod commands;
mod credentials;
mod functions;
mod types;

use structopt::clap::AppSettings;
use structopt::StructOpt;

use regex::Regex;

use rusoto_core::reactor::RequestDispatcher;
use rusoto_core::Region;
use rusoto_s3::*;

use commands::*;
use credentials::*;
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
    path: S3path,

    /// AWS access key. Unrequired
    #[structopt(
        name = "aws_access_key",
        long = "aws-access-key",
        raw(requires_all = r#"&["aws_secret_key"]"#)
    )]
    aws_access_key: Option<String>,

    /// AWS secret key. Unrequired
    #[structopt(
        name = "aws_secret_key",
        long = "aws-secret-key",
        raw(requires_all = r#"&["aws_access_key"]"#)
    )]
    aws_secret_key: Option<String>,

    /// The region to use. Default value is us-east-1
    #[structopt(name = "aws_region", long = "aws-region")]
    aws_region: Option<Region>,

    /// Glob pattern for match, can be multiple
    #[structopt(name = "npatern", long = "name", raw(number_of_values = "1"))]
    name: Vec<NameGlob>,

    /// Case-insensitive glob pattern for match, can be multiple
    #[structopt(name = "ipatern", long = "iname", raw(number_of_values = "1"))]
    iname: Vec<InameGlob>,

    /// Regex pattern for match, can be multiple
    #[structopt(name = "rpatern", long = "regex", raw(number_of_values = "1"))]
    regex: Vec<Regex>,

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
    mtime: Vec<FindTime>,

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
    size: Vec<FindSize>,

    //  /// Action to be ran with matched list of paths
    #[structopt(subcommand)]
    cmd: Option<Cmd>,
}

impl From<FindOpt> for FindCommand {
    fn from(opts: FindOpt) -> FindCommand {
        let region = opts.aws_region.clone().unwrap_or(Region::default());
        let provider =
            CombinedProvider::new(opts.aws_access_key.clone(), opts.aws_secret_key.clone());
        let client = S3Client::new(RequestDispatcher::default(), provider, region);

        FindCommand {
            path: opts.path.clone(),
            client: client,
            filters: opts.clone().into(),
            command: opts.cmd.clone(),
        }
    }
}

impl From<FindOpt> for FilterList {
    fn from(opts: FindOpt) -> FilterList {
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
}

fn main() -> Result<()> {
    let status_opts = FindOpt::from_args();

    let status: FindCommand = status_opts.clone().into();
    let mut request = status.list_request();

    loop {
        let output = status.client.list_objects_v2(&request).sync()?;
        match output.contents {
            Some(klist) => {
                let flist: Vec<_> = klist.iter().filter(|x| status.filters.filters(x)).collect();
                status.exec(flist)?;

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

#[cfg(test)]
mod tests {}
