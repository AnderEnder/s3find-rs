extern crate rusoto_core;
extern crate rusoto_s3;

use rusoto_s3::S3Client;
use rusoto_core::reactor::RequestDispatcher;

use types::*;
use credentials::*;

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

pub struct FilterList(pub Vec<Box<Filter>>);

pub struct FindCommand {
    pub client: S3Client<CombinedProvider, RequestDispatcher>,
    pub path: S3path,
    pub filters: FilterList,
    pub command: Option<Cmd>,
}
