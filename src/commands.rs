extern crate rusoto_core;
extern crate rusoto_s3;

use rusoto_core::reactor::RequestDispatcher;
use rusoto_s3::{ListObjectsV2Request, Object, S3Client, Tagging};

use credentials::*;
use functions::*;
use types::*;

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
    #[structopt(name = "-lstags", help = "list of filtered keys with tags")]
    LsTags,
    #[structopt(name = "-tags", help = "set tags for the keys")]
    Tags {
        #[structopt(name = "key:value", raw(min_values = "1"))]
        tags: Vec<FindTag>,
    },
}

pub struct FilterList(pub Vec<Box<Filter>>);

impl FilterList {
    pub fn filters(&self, object: &Object) -> bool {
        for item in self.0.iter() {
            if !item.filter(object) {
                return false;
            }
        }

        true
    }
}

pub struct FindCommand {
    pub client: S3Client<CombinedProvider, RequestDispatcher>,
    pub path: S3path,
    pub filters: FilterList,
    pub command: Option<Cmd>,
}

impl FindCommand {
    pub fn exec(&self, list: Vec<&Object>) -> Result<()> {
        match (*self).command {
            Some(Cmd::Print) => {
                let _nlist: Vec<_> = list.iter()
                    .map(|x| advanced_print(&self.path.bucket, x))
                    .collect();
            }
            Some(Cmd::Ls) => {
                let _nlist: Vec<_> = list.iter().map(|x| fprint(&self.path.bucket, x)).collect();
            }
            Some(Cmd::Exec { utility: ref p }) => {
                let _nlist: Vec<_> = list.iter()
                    .map(|x| {
                        let key = x.key.as_ref().unwrap();
                        let path = format!("s3://{}/{}", &self.path.bucket, key);
                        exec(&p, &path)
                    })
                    .collect();
            }
            Some(Cmd::Delete) => s3_delete(&self.client, &self.path.bucket, list)?,
            Some(Cmd::Download { destination: ref d }) => {
                s3_download(&self.client, &self.path.bucket, list, d)?
            }
            Some(Cmd::Tags { tags: ref t }) => {
                let tags = Tagging {
                    tag_set: t.into_iter().map(|x| (*x).clone().into()).collect(),
                };
                s3_set_tags(&self.client, &self.path.bucket, list, tags)?
            }
            Some(Cmd::LsTags) => s3_list_tags(&self.client, &self.path.bucket, list)?,
            None => {
                let _nlist: Vec<_> = list.iter().map(|x| fprint(&self.path.bucket, x)).collect();
            }
        }
        Ok(())
    }

    pub fn list_request(&self) -> ListObjectsV2Request {
        ListObjectsV2Request {
            bucket: self.path.bucket.clone(),
            continuation_token: None,
            delimiter: None,
            encoding_type: None,
            fetch_owner: None,
            max_keys: Some(10000),
            prefix: self.path.prefix.clone(),
            request_payer: None,
            start_after: None,
        }
    }
}
