use failure::Error;
use rusoto_core::request::HttpClient;
use rusoto_core::Region;
use rusoto_s3::{ListObjectsV2Request, Object, S3Client, Tag};

use crate::arg::*;
use crate::credential::*;
use crate::filter::Filter;
use crate::function::*;

pub struct FilterList(pub Vec<Box<dyn Filter>>);

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

pub struct Find {
    pub client: S3Client,
    pub region: Region,
    pub path: S3path,
    pub filters: FilterList,
    pub limit: Option<usize>,
    pub page_size: i64,
    pub stats: bool,
    pub command: Box<dyn RunCommand>,
}

impl Find {
    #![allow(unreachable_patterns)]
    pub fn exec(&self, list: &[&Object], acc: Option<FindStat>) -> Result<Option<FindStat>, Error> {
        let status = match acc {
            Some(stat) => Some(stat.add(list)),
            None => None,
        };

        let region = &self.region.name();
        self.command.execute(&self.client, region, &self.path, list)?;

        Ok(status)
    }

    pub fn list_request(&self) -> ListObjectsV2Request {
        ListObjectsV2Request {
            bucket: self.path.bucket.clone(),
            continuation_token: None,
            delimiter: None,
            encoding_type: None,
            fetch_owner: None,
            max_keys: Some(self.page_size),
            prefix: self.path.prefix.clone(),
            request_payer: None,
            start_after: None,
        }
    }

    pub fn stats(&self) -> Option<FindStat> {
        if self.stats {
            Some(FindStat::default())
        } else {
            None
        }
    }
}

impl From<FindOpt> for Find {
    fn from(opts: FindOpt) -> Self {
        let region = opts.aws_region.clone();
        let provider =
            CombinedProvider::new(opts.aws_access_key.clone(), opts.aws_secret_key.clone());

        let dispatcher = HttpClient::new().unwrap();

        let client = S3Client::new_with(dispatcher, provider, region.clone());

        Find {
            path: opts.path.clone(),
            client,
            region,
            filters: opts.clone().into(),
            command: opts.cmd.unwrap_or_default().downcast(),
            page_size: opts.page_size,
            stats: opts.stats,
            limit: opts.limit,
        }
    }
}

impl From<FindOpt> for FilterList {
    fn from(opts: FindOpt) -> Self {
        let mut list: Vec<Box<dyn Filter>> = Vec::new();

        let FindOpt {
            name,
            iname,
            regex,
            size,
            mtime,
            ..
        } = opts;

        for name in name {
            list.push(Box::new(name));
        }

        for iname in iname {
            list.push(Box::new(iname));
        }

        for regex in regex {
            list.push(Box::new(regex));
        }

        for size in size {
            list.push(Box::new(size));
        }

        for mtime in mtime {
            list.push(Box::new(mtime));
        }

        FilterList(list)
    }
}

impl From<FindTag> for Tag {
    fn from(tag: FindTag) -> Self {
        Tag {
            key: tag.key,
            value: tag.value,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FindStat {
    pub count: usize,
    pub size: i64,
    pub max: i64,
    pub min: i64,
    pub max_key: String,
    pub min_key: String,
}

impl FindStat {
    pub fn add(mut self: FindStat, list: &[&Object]) -> FindStat {
        for x in list {
            self.count += 1;
            let size = x.size.as_ref().unwrap_or(&0);
            self.size += size;

            if self.max < *size {
                self.max = *size;
                self.max_key = x.key.clone().unwrap_or_default();
            }

            if self.min > *size {
                self.min = *size;
                self.min_key = x.key.clone().unwrap_or_default();
            }
        }
        self
    }
}

impl Default for FindStat {
    fn default() -> Self {
        FindStat {
            count: 0,
            size: 0,
            max: 0,
            min: i64::max_value(),
            max_key: "".to_owned(),
            min_key: "".to_owned(),
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
        }
        .into();

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
        let find: Find = FindOpt {
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
            limit: None,
            page_size: 1000,
            cmd: Some(Cmd::Ls(FastPrint {})),
            stats: false,
        }
        .into();

        assert_eq!(
            find.path,
            S3path {
                bucket: "bucket".to_owned(),
                prefix: Some("prefix".to_owned()),
            }
        );
        assert_eq!(find.region, Region::UsEast1);

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
