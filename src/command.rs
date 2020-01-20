use failure::Error;
use humansize::{file_size_opts as options, FileSize};
use rusoto_core::request::HttpClient;
use rusoto_core::Region;
use rusoto_credential::{DefaultCredentialsProvider, StaticProvider};
use rusoto_s3::*;
use rusoto_s3::{ListObjectsV2Request, Object, S3Client, Tag};
use std::fmt;
use std::ops::Add;

use crate::arg::*;
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
    pub summarize: bool,
    pub command: Box<dyn RunCommand>,
}

impl Find {
    #![allow(unreachable_patterns)]
    pub fn exec(&self, list: &[&Object], acc: Option<FindStat>) -> Result<Option<FindStat>, Error> {
        let status = match acc {
            Some(stat) => Some(stat + list),
            None => None,
        };

        let region = &self.region.name();
        self.command
            .execute(&self.client, region, &self.path, list)?;
        Ok(status)
    }

    pub fn stats(&self) -> Option<FindStat> {
        if self.summarize {
            Some(FindStat::default())
        } else {
            None
        }
    }

    pub fn iter(&self) -> FindIter {
        FindIter {
            client: self.client.clone(),
            path: self.path.clone(),
            token: None,
            page_size: self.page_size,
            initial: true,
        }
    }
}

#[derive(Clone)]
pub struct FindIter {
    pub client: S3Client,
    pub path: S3path,
    pub token: Option<String>,
    pub page_size: i64,
    pub initial: bool,
}

impl Iterator for FindIter {
    type Item = Result<Vec<Object>, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.initial && self.token == None {
            return None;
        }

        let request = ListObjectsV2Request {
            bucket: self.path.bucket.clone(),
            continuation_token: self.token.clone(),
            delimiter: None,
            encoding_type: None,
            fetch_owner: None,
            max_keys: Some(self.page_size),
            prefix: self.path.prefix.clone(),
            request_payer: None,
            start_after: None,
        };

        self.initial = false;
        self.token = None;

        self.client
            .list_objects_v2(request)
            .sync()
            .map_err(|e| e.into())
            .map(|x| {
                self.token = x.next_continuation_token;
                x.contents
            })
            .transpose()
    }
}

impl From<FindOpt> for Find {
    fn from(opts: FindOpt) -> Self {
        let filters = opts.clone().into();

        let FindOpt {
            aws_access_key,
            aws_secret_key,
            aws_region,
            path,
            cmd,
            page_size,
            summarize,
            limit,
            ..
        } = opts;

        let region = aws_region.clone();
        let client = get_client(aws_access_key, aws_secret_key, aws_region);
        let command = cmd.unwrap_or_default().downcast();

        Find {
            client,
            filters,
            region,
            path,
            command,
            page_size,
            summarize,
            limit,
            stats: summarize,
        }
    }
}

fn get_client(
    aws_access_key: Option<String>,
    aws_secret_key: Option<String>,
    region: Region,
) -> S3Client {
    let dispatcher = HttpClient::new().unwrap();
    match (aws_access_key, aws_secret_key) {
        (Some(aws_access_key), Some(aws_secret_key)) => {
            let provider = StaticProvider::new(aws_access_key, aws_secret_key, None, None);
            S3Client::new_with(dispatcher, provider, region)
        }
        _ => {
            let provider = DefaultCredentialsProvider::new().unwrap();
            S3Client::new_with(dispatcher, provider, region)
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
    pub total_files: usize,
    pub total_space: i64,
    pub max_size: Option<i64>,
    pub min_size: Option<i64>,
    pub max_key: String,
    pub min_key: String,
    pub average_size: i64,
}

impl Add<&[&Object]> for FindStat {
    type Output = FindStat;

    #[allow(clippy::suspicious_arithmetic_impl)]
    fn add(mut self: FindStat, list: &[&Object]) -> Self {
        for x in list {
            self.total_files += 1;
            let size = x.size.as_ref().unwrap_or(&0);
            self.total_space += size;

            match self.max_size {
                None => {
                    self.max_size = Some(*size);
                    self.max_key = x.key.clone().unwrap_or_default();
                }
                Some(max_size) if max_size <= *size => {
                    self.max_size = Some(*size);
                    self.max_key = x.key.clone().unwrap_or_default();
                }
                _ => {}
            }

            match self.min_size {
                None => {
                    self.min_size = Some(*size);
                    self.min_key = x.key.clone().unwrap_or_default();
                }
                Some(min_size) if min_size > *size => {
                    self.min_size = Some(*size);
                    self.min_key = x.key.clone().unwrap_or_default();
                }
                _ => {}
            }

            self.average_size = self.total_space / (self.total_files as i64);
        }
        self
    }
}

impl Default for FindStat {
    fn default() -> Self {
        FindStat {
            total_files: 0,
            total_space: 0,
            max_size: None,
            min_size: None,
            max_key: "".to_owned(),
            min_key: "".to_owned(),
            average_size: 0,
        }
    }
}

impl fmt::Display for FindStat {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f)?;
        writeln!(f, "Summary")?;
        writeln!(f, "{:19} {}", "Total files:", &self.total_files)?;
        writeln!(
            f,
            "Total space:        {}",
            &self
                .total_space
                .file_size(options::CONVENTIONAL)
                .map_err(|_| fmt::Error)?
        )?;
        writeln!(f, "{:19} {}", "Largest file:", &self.max_key)?;
        writeln!(
            f,
            "{:19} {}",
            "Largest file size:",
            &self
                .max_size
                .unwrap_or_default()
                .file_size(options::CONVENTIONAL)
                .map_err(|_| fmt::Error)?
        )?;
        writeln!(f, "{:19} {}", "Smallest file:", &self.min_key)?;
        writeln!(
            f,
            "{:19} {}",
            "Smallest file size:",
            &self
                .min_size
                .unwrap_or_default()
                .file_size(options::CONVENTIONAL)
                .map_err(|_| fmt::Error)?
        )?;
        writeln!(
            f,
            "{:19} {}",
            "Average file size:",
            &self
                .average_size
                .file_size(options::CONVENTIONAL)
                .map_err(|_| fmt::Error)?
        )?;
        Ok(())
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
            summarize: false,
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
