use futures::Stream;
use glob::Pattern;
use humansize::{file_size_opts as options, FileSize};
use regex::Regex;
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

pub struct AWSPair {
    access: Option<String>,
    secret: Option<String>,
}

pub struct FilterList(pub Vec<Box<dyn Filter>>);

impl FilterList {
    pub fn new(
        name: Vec<Pattern>,
        iname: Vec<InameGlob>,
        regex: Vec<Regex>,
        size: Vec<FindSize>,
        mtime: Vec<FindTime>,
    ) -> Self {
        let mut list: Vec<Box<dyn Filter>> = Vec::new();

        for filter in name {
            list.push(Box::new(filter));
        }

        for filter in iname {
            list.push(Box::new(filter));
        }

        for filter in regex {
            list.push(Box::new(filter));
        }

        for filter in size {
            list.push(Box::new(filter));
        }

        for filter in mtime {
            list.push(Box::new(filter));
        }

        FilterList(list)
    }

    pub async fn test_match(&self, object: Object) -> bool {
        for item in &self.0 {
            if !item.filter(&object) {
                return false;
            }
        }

        true
    }
}

pub struct Find {
    pub client: S3Client,
    pub region: Region,
    pub path: S3Path,
    pub limit: Option<usize>,
    pub page_size: i64,
    pub stats: bool,
    pub summarize: bool,
    pub command: Box<dyn RunCommand>,
}

impl Find {
    pub fn new(
        aws_credentials: AWSPair,
        aws_region: Region,
        cmd: Option<Cmd>,
        path: S3Path,
        page_size: i64,
        summarize: bool,
        limit: Option<usize>,
    ) -> Self {
        let region = aws_region.clone();
        let client = get_client(aws_credentials.access, aws_credentials.secret, aws_region);
        let command = cmd.unwrap_or_default().downcast();

        Find {
            client,
            region,
            path,
            command,
            page_size,
            summarize,
            limit,
            stats: summarize,
        }
    }

    pub async fn exec(&self, acc: Option<FindStat>, list: Vec<Object>) -> Option<FindStat> {
        let status = match acc {
            Some(stat) => Some(stat + &list),
            None => None,
        };

        let region = &self.region.name();
        self.command
            .execute(&self.client, region, &self.path, &list)
            .await
            .unwrap();
        status
    }

    pub fn to_stream(&self) -> FindStream {
        FindStream {
            client: self.client.clone(),
            path: self.path.clone(),
            token: None,
            page_size: self.page_size,
            initial: true,
        }
    }
}

impl From<FindOpt> for (Find, FilterList) {
    fn from(opts: FindOpt) -> Self {
        let FindOpt {
            aws_access_key,
            aws_secret_key,
            aws_region,
            path,
            cmd,
            page_size,
            summarize,
            limit,
            name,
            iname,
            regex,
            size,
            mtime,
            ..
        } = opts;

        let find = Find::new(
            AWSPair {
                access: aws_access_key,
                secret: aws_secret_key,
            },
            aws_region,
            cmd,
            path,
            page_size,
            summarize,
            limit,
        );
        let filters = FilterList::new(name, iname, regex, size, mtime);

        (find, filters)
    }
}

pub fn default_stats(summarize: bool) -> Option<FindStat> {
    if summarize {
        Some(FindStat::default())
    } else {
        None
    }
}

pub struct FindStream {
    pub client: S3Client,
    pub path: S3Path,
    pub token: Option<String>,
    pub page_size: i64,
    pub initial: bool,
}

impl FindStream {
    async fn list(mut self) -> Option<(Vec<Object>, Self)> {
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
            expected_bucket_owner: None,
        };

        self.initial = false;
        self.token = None;

        let (token, objects) = self
            .client
            .list_objects_v2(request)
            .await
            .map(|x| (x.next_continuation_token, x.contents))
            .unwrap();

        self.token = token;
        objects.map(|x| (x, self))
    }

    pub fn stream(self) -> impl Stream<Item = Vec<Object>> {
        futures::stream::unfold(self, |s| async { s.list().await })
    }
}

impl PartialEq for FindStream {
    fn eq(&self, other: &Self) -> bool {
        self.path == other.path
            && self.token == other.token
            && self.page_size == other.page_size
            && self.initial == other.initial
    }
}

impl fmt::Debug for FindStream {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "\
FindStream {{
    client,
    path: {:?},
    token: {:?},
    page_size: {},
    initial: {},
}}",
            self.path, self.token, self.page_size, self.initial
        )
    }
}

#[inline]
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

impl Add<&[Object]> for FindStat {
    type Output = FindStat;

    #[allow(clippy::suspicious_arithmetic_impl)]
    fn add(mut self: FindStat, list: &[Object]) -> Self {
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

    use anyhow::Error;
    use regex::Regex;
    use rusoto_mock::*;
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

    #[tokio::test]
    async fn from_findopt_to_findcommand() {
        let (find, filters) = FindOpt {
            path: S3Path {
                bucket: "bucket".to_owned(),
                prefix: Some("prefix".to_owned()),
            },
            aws_access_key: Some("access".to_owned()),
            aws_secret_key: Some("secret".to_owned()),
            aws_region: Region::UsEast1,
            name: vec![NameGlob::from_str("*ref*").unwrap()],
            iname: vec![InameGlob::from_str("Pre*").unwrap()],
            regex: vec![Regex::from_str("^pre").unwrap()],
            mtime: vec![FindTime::Lower(10)],
            size: vec![FindSize::Lower(1000)],
            limit: None,
            page_size: 1000,
            cmd: Some(Cmd::Ls(FastPrint {})),
            summarize: false,
        }
        .into();

        assert_eq!(
            find.path,
            S3Path {
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
        assert!(filters.test_match(object_ok).await);

        let object_fail = Object {
            key: Some("Refer".to_owned()),
            size: Some(10),
            ..Default::default()
        };
        assert!(!filters.test_match(object_fail).await);
    }

    #[test]
    fn filestat_add() -> Result<(), Error> {
        let objects = [
            Object {
                e_tag: Some("9d48114aa7c18f9d68aa20086dbb7756".to_string()),
                key: Some("sample2.txt".to_string()),
                last_modified: Some("2017-07-19T19:04:17.000Z".to_string()),
                owner: None,
                size: Some(20),
                storage_class: Some("STANDARD".to_string()),
            },
            Object {
                e_tag: Some("9d48114aa7c18f9d68aa20086dbb7756".to_string()),
                key: Some("sample1.txt".to_string()),
                last_modified: Some("2017-07-19T19:04:17.000Z".to_string()),
                owner: None,
                size: Some(10),
                storage_class: Some("STANDARD".to_string()),
            },
            Object {
                e_tag: Some("9d48114aa7c18f9d68aa20086dbb7756".to_string()),
                key: Some("sample3.txt".to_string()),
                last_modified: Some("2017-07-19T19:04:15.000Z".to_string()),
                owner: None,
                size: Some(30),
                storage_class: Some("STANDARD".to_string()),
            },
        ];

        let stat = FindStat::default() + &objects;

        let expected = FindStat {
            total_files: 3,
            total_space: 60,
            max_size: Some(30),
            min_size: Some(10),
            max_key: "sample3.txt".to_owned(),
            min_key: "sample1.txt".to_owned(),
            average_size: 20,
        };

        assert_eq!(stat, expected);

        Ok(())
    }

    #[tokio::test]
    async fn findstream_list() -> Result<(), Error> {
        let mock = MockRequestDispatcher::with_status(200).with_body(
            r#"
            <?xml version="1.0" encoding="UTF-8"?>
            <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
              <Name>test</Name>
              <Prefix></Prefix>
              <Marker></Marker>
              <MaxKeys>1</MaxKeys>
              <IsTruncated>false</IsTruncated>
              <Contents>
                <Key>key1</Key>
                <LastModified>2013-01-10T21:45:09.000Z</LastModified>
                <ETag>&quot;1d921b22129502cbbe5cbaf2c8bac682&quot;</ETag>
                <Size>10000</Size>
                <Owner>
                  <ID>1936a5d8a2b189cda450d1d1d514f3861b3adc2df515</ID>
                  <DisplayName>aws</DisplayName>
                </Owner>
                <StorageClass>STANDARD</StorageClass>
              </Contents>
              <Contents>
                <Key>key2</Key>
                <LastModified>2013-01-10T22:45:09.000Z</LastModified>
                <ETag>&quot;1d921b22129502cbbe5cbaf2c8bac682&quot;</ETag>
                <Size>1234</Size>
                <Owner>
                  <ID>1936a5d8a2b189cda450d1d1d514f3861b3adc2df515</ID>
                  <DisplayName>aws</DisplayName>
                </Owner>
                <StorageClass>STANDARD</StorageClass>
              </Contents>
            </ListBucketResult>"#,
        );
        let client = S3Client::new_with(mock, MockCredentialsProvider, Region::UsEast1);
        let path = S3Path {
            bucket: "test".to_owned(),
            prefix: None,
        };

        let command = FindStream {
            client,
            path,
            token: None,
            page_size: 1,
            initial: true,
        };

        let (objects, command2) = command.list().await.unwrap();
        assert_eq!(command2.token, None);
        assert_eq!(command2.initial, false);
        assert_eq!(objects[0].key, Some("key1".to_owned()));
        assert_eq!(objects[1].key, Some("key2".to_owned()));

        Ok(())
    }

    #[test]
    fn test_default_stats() {
        assert_eq!(default_stats(false), None);
        assert_eq!(default_stats(true), Some(Default::default()));
    }

    #[tokio::test]
    async fn test_find_exec() {
        let mock = MockRequestDispatcher::with_status(200);
        let client = S3Client::new_with(mock, MockCredentialsProvider, Region::UsEast1);
        let command: Box<dyn RunCommand> = Box::new(DoNothing {});

        let find = Find {
            client,
            region: Region::UsEast1,
            path: "s3://test/path".parse().unwrap(),
            limit: None,
            page_size: 1000,
            stats: true,
            summarize: false,
            command,
        };

        assert_eq!(find.exec(None, Vec::new()).await, None);
        assert_eq!(
            find.exec(Some(Default::default()), Vec::new()).await,
            Some(Default::default())
        );

        let objects = vec![
            Object {
                e_tag: Some("9d48114aa7c18f9d68aa20086dbb7756".to_string()),
                key: Some("sample1.txt".to_string()),
                last_modified: Some("2017-07-19T19:04:17.000Z".to_string()),
                owner: None,
                size: Some(10),
                storage_class: Some("STANDARD".to_string()),
            },
            Object {
                e_tag: Some("9d48114aa7c18f9d68aa20086dbb7756".to_string()),
                key: Some("sample2.txt".to_string()),
                last_modified: Some("2017-07-19T19:04:17.000Z".to_string()),
                owner: None,
                size: Some(20),
                storage_class: Some("STANDARD".to_string()),
            },
            Object {
                e_tag: Some("9d48114aa7c18f9d68aa20086dbb7756".to_string()),
                key: Some("sample3.txt".to_string()),
                last_modified: Some("2017-07-19T19:04:15.000Z".to_string()),
                owner: None,
                size: Some(30),
                storage_class: Some("STANDARD".to_string()),
            },
        ];

        let stat = find.exec(Some(Default::default()), objects).await;
        assert_eq!(
            stat,
            Some(FindStat {
                total_files: 3,
                total_space: 60,
                max_size: Some(30),
                min_size: Some(10),
                max_key: "sample3.txt".to_owned(),
                min_key: "sample1.txt".to_owned(),
                average_size: 20
            })
        );
    }

    #[test]
    fn test_find_findstat() {
        let stat = FindStat {
            total_files: 3,
            total_space: 60,
            max_size: Some(30),
            min_size: Some(10),
            max_key: "sample3.txt".to_owned(),
            min_key: "sample1.txt".to_owned(),
            average_size: 20,
        };
        // smoke debug
        let stat_str = stat.to_string();
        assert!(stat_str.contains("sample1.txt"));
        assert!(stat_str.contains("sample3.txt"));
    }

    #[test]
    fn test_find_stream() {
        let mock = MockRequestDispatcher::with_status(200);
        let client = S3Client::new_with(mock, MockCredentialsProvider, Region::UsEast1);
        let command: Box<dyn RunCommand> = Box::new(DoNothing {});

        let find = Find {
            client: client.clone(),
            region: Region::UsEast1,
            path: "s3://test/path".parse().unwrap(),
            limit: None,
            page_size: 1000,
            stats: true,
            summarize: false,
            command,
        };

        let stream = find.to_stream();
        assert_eq!(
            stream,
            FindStream {
                client,
                path: "s3://test/path".parse().unwrap(),
                token: None,
                page_size: 1000,
                initial: true,
            }
        );
    }

    #[test]
    fn test_stream_debug() {
        let mock = MockRequestDispatcher::with_status(200);
        let client = S3Client::new_with(mock, MockCredentialsProvider, Region::UsEast1);

        let stream = FindStream {
            client,
            path: "s3://test/path".parse().unwrap(),
            token: None,
            page_size: 1000,
            initial: true,
        };

        let stream_str = format!("{:?}", stream);
        assert!(stream_str.contains("FindStream"));
        assert!(stream_str.contains("S3Path"));
        assert!(stream_str.contains("test"));
        assert!(stream_str.contains("path"));
        assert!(stream_str.contains("1000"));
    }
}
