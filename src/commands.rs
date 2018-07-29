extern crate rusoto_core;
extern crate rusoto_s3;

use rusoto_core::reactor::RequestDispatcher;
use rusoto_core::Region;
use rusoto_s3::{ListObjectsV2Request, Object, S3Client, Tagging};

use credentials::*;
use functions::*;
use types::*;

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

pub struct FilterList(pub Vec<Box<Filter>>);

impl FilterList {
    pub fn filters(&self, object: &Object) -> bool {
        for item in &self.0 {
            if !item.filter(object) {
                return false;
            }
        }

        true
    }
}

pub struct FindCommand {
    pub client: S3Client<CombinedProvider, RequestDispatcher>,
    pub region: Region,
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
            Some(Cmd::Delete) => s3_delete(&self.client, &self.path.bucket, &list)?,
            Some(Cmd::Download {
                destination: ref d,
                force: ref f,
            }) => s3_download(&self.client, &self.path.bucket, &list, d, f.to_owned())?,
            Some(Cmd::Tags { tags: ref t }) => {
                let tags = Tagging {
                    tag_set: t.into_iter().map(|x| (*x).clone().into()).collect(),
                };
                s3_set_tags(&self.client, &self.path.bucket, &list, tags)?
            }
            Some(Cmd::LsTags) => s3_list_tags(&self.client, &self.path.bucket, list)?,
            Some(Cmd::Public) => {
                s3_set_public(&self.client, &self.path.bucket, &list, &self.region)?
            }
            Some(_) => println!("Not implemented"),
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
