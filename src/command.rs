use failure::Error;
use rusoto_core::request::HttpClient;
use rusoto_core::Region;
use rusoto_s3::{ListObjectsV2Request, Object, S3Client, Tag, Tagging};

use arg::*;
use credential::*;
use filter::Filter;
use function::*;

pub struct FilterList(pub Vec<Box<Filter>>);

impl FilterList {
    pub fn test_match(&self, object: &Object) -> bool {
        for item in &self.0 {
            if !item.filter(object) {
                return false;
            }
        }

        true
    }
}

pub struct FindCommand {
    pub client: S3Client,
    pub region: Region,
    pub path: S3path,
    pub filters: FilterList,
    pub command: Option<Cmd>,
}

impl FindCommand {
    #![allow(unreachable_patterns)]
    pub fn exec(&self, list: &[&Object]) -> Result<(), Error> {
        match (*self).command {
            Some(Cmd::Print) => {
                let _nlist: Vec<_> = list
                    .iter()
                    .map(|x| advanced_print(&self.path.bucket, x))
                    .collect();
            }
            Some(Cmd::Ls) => {
                let _nlist: Vec<_> = list.iter().map(|x| fprint(&self.path.bucket, x)).collect();
            }
            Some(Cmd::Exec { utility: ref p }) => {
                let _nlist: Vec<_> = list
                    .iter()
                    .map(|x| {
                        let key = x.key.as_ref().map(|x| x.as_str()).unwrap_or("");
                        let path = format!("s3://{}/{}", &self.path.bucket, key);
                        exec(&p, &path)
                    }).collect();
            }
            Some(Cmd::Delete) => s3_delete(&self.client, &self.path.bucket, list)?,
            Some(Cmd::Download {
                destination: ref d,
                force: ref f,
            }) => s3_download(&self.client, &self.path.bucket, &list, d, f.to_owned())?,
            Some(Cmd::Tags { tags: ref t }) => {
                let tags = Tagging {
                    tag_set: t.into_iter().map(|x| (*x).clone().into()).collect(),
                };
                s3_set_tags(&self.client, &self.path.bucket, &list, &tags)?
            }
            Some(Cmd::LsTags) => s3_list_tags(&self.client, &self.path.bucket, list)?,
            Some(Cmd::Public) => {
                s3_set_public(&self.client, &self.path.bucket, list, &self.region)?
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

impl From<FindOpt> for FindCommand {
    fn from(opts: FindOpt) -> FindCommand {
        let region = opts.aws_region.clone();
        let provider =
            CombinedProvider::new(opts.aws_access_key.clone(), opts.aws_secret_key.clone());

        let dispatcher = HttpClient::new().unwrap();

        let client = S3Client::new_with(dispatcher, provider, region.clone());

        FindCommand {
            path: opts.path.clone(),
            client,
            region,
            filters: opts.clone().into(),
            command: opts.cmd.clone(),
        }
    }
}

impl From<FindOpt> for FilterList {
    fn from(opts: FindOpt) -> FilterList {
        let mut list: Vec<Box<Filter>> = Vec::new();

        for name in &opts.name {
            list.push(Box::new(name.clone()));
        }

        for iname in &opts.iname {
            list.push(Box::new(iname.clone()));
        }

        for regex in &opts.regex {
            list.push(Box::new(regex.clone()));
        }

        for size in &opts.size {
            list.push(Box::new(size.clone()));
        }

        for mtime in &opts.mtime {
            list.push(Box::new(mtime.clone()));
        }

        FilterList(list)
    }
}

impl From<FindTag> for Tag {
    fn from(tag: FindTag) -> Tag {
        Tag {
            key: tag.key,
            value: tag.value,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use regex::Regex;
    use std::str::FromStr;

    #[test]
    fn from_findtag() -> Result<(), Error> {
        let tag: Tag = FindTag {
            key: "tag".to_owned(),
            value: "val".to_owned(),
        }.into();

        assert_eq!(
            tag,
            Tag {
                key: "tag".to_owned(),
                value: "val".to_owned(),
            }
        );
        Ok(())
    }

    #[test]
    fn from_findopt_to_findcommand() {
        let find: FindCommand = FindOpt {
            path: S3path {
                bucket: "bucket".to_owned(),
                prefix: Some("prefix".to_owned()),
            },
            aws_access_key: Some("access".to_owned()),
            aws_secret_key: Some("secret".to_owned()),
            aws_region: Region::UsEast1,
            name: vec![NameGlob::from_str("*ref*").unwrap()],
            iname: vec![InameGlob::from_str("Pre*").unwrap()],
            regex: vec![Regex::from_str("^pre").unwrap()],
            mtime: Vec::new(),
            size: vec![FindSize::Lower(1000)],
            cmd: Some(Cmd::Ls),
        }.into();

        assert_eq!(
            find.path,
            S3path {
                bucket: "bucket".to_owned(),
                prefix: Some("prefix".to_owned()),
            }
        );
        assert_eq!(find.region, Region::UsEast1);
        assert_eq!(find.command, Some(Cmd::Ls));

        let object_ok = Object {
            key: Some("pref".to_owned()),
            size: Some(10),
            ..Default::default()
        };
        assert!(find.filters.test_match(&object_ok));

        let object_fail = Object {
            key: Some("Refer".to_owned()),
            size: Some(10),
            ..Default::default()
        };
        assert!(!find.filters.test_match(&object_fail));
    }
}
