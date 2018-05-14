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

use structopt::StructOpt;
use structopt::clap::AppSettings;

use regex::Regex;

use rusoto_core::Region;
use rusoto_core::reactor::RequestDispatcher;
use rusoto_s3::*;

use commands::*;
use credentials::*;
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
