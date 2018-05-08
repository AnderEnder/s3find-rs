extern crate chrono;
extern crate failure;
extern crate glob;
extern crate regex;
extern crate rusoto_s3;

use failure::Error;
use std::str::FromStr;
use rusoto_s3::*;
use chrono::prelude::*;
use glob::Pattern;
use glob::MatchOptions;
use regex::Regex;

#[derive(Fail, Debug)]
pub enum FindError {
    #[fail(display = "Invalid s3 path")]
    S3Parse,
    #[fail(display = "Invalid size parameter")]
    SizeParse,
    #[fail(display = "Invalid mtime parameter")]
    TimeParse,
    #[fail(display = "Invalid command line value")]
    CommandlineParse,
    #[fail(display = "Invalid path value")]
    ParentPathParse,
}

pub type Result<T> = ::std::result::Result<T, Error>;

pub trait Filter {
    fn filter(&self, object: &Object) -> bool;
}

#[derive(Debug, Clone, PartialEq)]
pub struct S3path {
    pub bucket: String,
    pub prefix: Option<String>,
}

impl FromStr for S3path {
    type Err = Error;

    fn from_str(s: &str) -> Result<S3path> {
        let s3_vec: Vec<&str> = s.split("/").collect();
        let bucket = s3_vec.get(2).unwrap_or(&"");
        let prefix = s3_vec.get(3).map(|x| x.to_owned());

        let is_validated =
            (s3_vec.get(0) == Some(&"s3:")) && (s3_vec.get(1) == Some(&"")) && (bucket != &"");

        if is_validated {
            Ok(S3path {
                bucket: bucket.to_string(),
                prefix: prefix.map(|x| (*x).to_string()),
            })
        } else {
            Err(FindError::S3Parse.into())
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum FindSize {
    Equal(i64),
    Bigger(i64),
    Lower(i64),
}

impl FromStr for FindSize {
    type Err = Error;

    fn from_str(s: &str) -> Result<FindSize> {
        let re = Regex::new(r"([+-]?)(\d*)([kMGTP]?)$")?;
        let m = re.captures(s).unwrap();

        let sign = m.get(1).unwrap().as_str().chars().next();
        let number: i64 = m.get(2).unwrap().as_str().parse()?;
        let metric = m.get(3).unwrap().as_str().chars().next();

        let bytes = match metric {
            None => number,
            Some('k') => number * 1024,
            Some('M') => number * 1024_i64.pow(2),
            Some('G') => number * 1024_i64.pow(3),
            Some('T') => number * 1024_i64.pow(4),
            Some('P') => number * 1024_i64.pow(5),
            Some(_) => return Err(FindError::SizeParse.into()),
        };

        match sign {
            Some('+') => Ok(FindSize::Bigger(bytes)),
            Some('-') => Ok(FindSize::Lower(bytes)),
            None => Ok(FindSize::Equal(bytes)),
            Some(_) => Err(FindError::SizeParse.into()),
        }
    }
}

impl Filter for FindSize {
    fn filter(&self, object: &Object) -> bool {
        let object_size = object.size.as_ref().unwrap_or(&0);
        match *self {
            FindSize::Bigger(size) => *object_size >= size,
            FindSize::Lower(size) => *object_size <= size,
            FindSize::Equal(size) => *object_size == size,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum FindTime {
    Upper(i64),
    Lower(i64),
}

impl FromStr for FindTime {
    type Err = Error;

    fn from_str(s: &str) -> Result<FindTime> {
        let re = Regex::new(r"([+-]?)(\d*)([smhdw]?)$")?;
        let m = re.captures(s).unwrap();

        let sign = m.get(1).unwrap().as_str().chars().next();
        let number: i64 = m.get(2).unwrap().as_str().parse()?;
        let metric = m.get(3).unwrap().as_str().chars().next();

        let seconds = match metric {
            None => number,
            Some('s') => number,
            Some('m') => number * 60,
            Some('h') => number * 3600,
            Some('d') => number * 3600 * 24,
            Some('w') => number * 3600 * 24 * 7,
            Some(_) => return Err(FindError::TimeParse.into()),
        };

        match sign {
            Some('+') => Ok(FindTime::Upper(seconds)),
            Some('-') => Ok(FindTime::Lower(seconds)),
            None => Ok(FindTime::Upper(seconds)),
            Some(_) => Err(FindError::TimeParse.into()),
        }
    }
}

impl Filter for FindTime {
    fn filter(&self, object: &Object) -> bool {
        let object_time = object.last_modified.as_ref().unwrap();
        let last_modified_time = object_time.parse::<DateTime<Utc>>().unwrap().timestamp();
        let now = Utc::now().timestamp();

        match *self {
            FindTime::Upper(seconds) => (now - last_modified_time) >= seconds,
            FindTime::Lower(seconds) => (now - last_modified_time) <= seconds,
        }
    }
}

pub type NameGlob = Pattern;

impl Filter for NameGlob {
    fn filter(&self, object: &Object) -> bool {
        let object_key = object.key.as_ref().map(|x| x.as_ref()).unwrap_or_default();
        self.matches(&object_key)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct InameGlob(Pattern);

impl FromStr for InameGlob {
    type Err = Error;

    fn from_str(s: &str) -> Result<InameGlob> {
        let pattern = Pattern::from_str(s)?;
        Ok(InameGlob(pattern))
    }
}

impl Filter for InameGlob {
    fn filter(&self, object: &Object) -> bool {
        let object_key = object.key.as_ref().map(|x| x.as_ref()).unwrap_or_default();
        self.0.matches_with(
            &object_key,
            &MatchOptions {
                case_sensitive: false,
                require_literal_separator: false,
                require_literal_leading_dot: false,
            },
        )
    }
}

impl Filter for Regex {
    fn filter(&self, object: &Object) -> bool {
        let object_key = object.key.as_ref().map(|x| x.as_ref()).unwrap_or_default();
        self.is_match(&object_key)
    }
}

#[cfg(test)]
mod tests {

    use S3path;
    use FindSize;
    use FindTime;
    use failure::Error;

    #[test]
    fn s3path_corect() {
        let url = "s3://testbucket/";
        let path: S3path = url.parse().unwrap();
        assert_eq!(path.bucket, "testbucket", "This should be 'testbucket'");
        assert_eq!(
            path.prefix,
            Some("".to_string()),
            "This should be empty path"
        );
    }

    #[test]
    fn s3path_correct_full() {
        let url = "s3://testbucket/path";
        let path: S3path = url.parse().unwrap();
        assert_eq!(path.bucket, "testbucket", "This should be 'testbucket'");
        assert_eq!(
            path.prefix,
            Some("path".to_string()),
            "This should be 'path'"
        );
    }

    #[test]
    fn s3path_correct_short() {
        let url = "s3://testbucket";
        let path: S3path = url.parse().unwrap();
        assert_eq!(path.bucket, "testbucket", "This should be 'testbucket'");
        assert_eq!(path.prefix, None, "This should be None");
    }

    #[test]
    fn s3path_only_bucket() {
        let url = "testbucket";
        let path: Result<S3path, Error> = url.parse();
        assert!(
            path.is_err(),
            "This s3 url should not be validated posivitely"
        );
    }

    #[test]
    fn s3path_without_bucket() {
        let url = "s3://";
        let path: Result<S3path, Error> = url.parse();
        assert!(
            path.is_err(),
            "This s3 url should not be validated posivitely"
        );
    }

    #[test]
    fn s3path_without_2_slash() {
        let url = "s3:/testbucket";
        let path: Result<S3path, Error> = url.parse();
        assert!(
            path.is_err(),
            "This s3 url should not be validated posivitely"
        );
    }

    #[test]
    fn size_corect() {
        let size_str = "1111";
        let size = size_str.parse::<FindSize>();

        assert_eq!(size.ok(), Some(FindSize::Equal(1111)), "should be equal");
    }

    #[test]
    fn size_corect_k() {
        let size_str = "1111k";
        let size = size_str.parse::<FindSize>();

        assert_eq!(
            size.ok(),
            Some(FindSize::Equal(1111 * 1024)),
            "should be equal"
        );
    }

    #[test]
    fn size_corect_positive() {
        let size_str = "+1111";
        let size = size_str.parse::<FindSize>();

        assert_eq!(size.ok(), Some(FindSize::Bigger(1111)), "should be upper");
    }

    #[test]
    fn size_corect_positive_k() {
        let size_str = "+1111k";
        let size = size_str.parse::<FindSize>();

        assert_eq!(
            size.ok(),
            Some(FindSize::Bigger(1111 * 1024)),
            "should be upper"
        );
    }

    #[test]
    fn size_corect_negative() {
        let size_str = "-1111";
        let size = size_str.parse::<FindSize>();

        assert_eq!(size.ok(), Some(FindSize::Lower(1111)), "should be lower");
    }

    #[test]
    fn size_corect_negative_k() {
        let size_str = "-1111k";
        let size = size_str.parse::<FindSize>();

        assert_eq!(
            size.ok(),
            Some(FindSize::Lower(1111 * 1024)),
            "should be lower"
        );
    }

    #[test]
    fn size_incorect_negative() {
        let size_str = "-";
        let size = size_str.parse::<FindSize>();

        assert!(size.is_err(), "Should be error");
    }

    #[test]
    fn size_incorect_negative_s() {
        let size_str = "-123w";
        let size = size_str.parse::<FindSize>();

        assert!(size.is_err(), "Should be error");
    }

    #[test]
    fn time_corect() {
        let time_str = "1111";
        let time = time_str.parse::<FindTime>();

        assert!(time.is_ok(), "Should be ok");
        assert_eq!(time.ok(), Some(FindTime::Upper(1111)), "Should be upper");
    }

    #[test]
    fn time_corect_m() {
        let time_str = "10m";
        let time = time_str.parse::<FindTime>();

        assert!(time.is_ok(), "Should be ok");
        assert_eq!(time.ok(), Some(FindTime::Upper(600)), "Should be upper");
    }

    #[test]
    fn time_corect_positive() {
        let time_str = "+1111";
        let time = time_str.parse::<FindTime>();

        assert!(time.is_ok(), "Should be ok");
        assert_eq!(time.ok(), Some(FindTime::Upper(1111)), "Should be upper");
    }

    #[test]
    fn time_corect_positive_m() {
        let time_str = "+10m";
        let time = time_str.parse::<FindTime>();

        assert!(time.is_ok(), "Should be ok");
        assert_eq!(time.ok(), Some(FindTime::Upper(600)), "Should be upper");
    }

    #[test]
    fn time_corect_negative_m() {
        let time_str = "-10m";
        let time = time_str.parse::<FindTime>();

        assert!(time.is_ok(), "Should be ok");
        assert_eq!(time.ok(), Some(FindTime::Lower(600)), "Should be upper");
    }

    #[test]
    fn time_corect_negative() {
        let time_str = "-1111";
        let time = time_str.parse::<FindTime>();

        assert!(time.is_ok(), "Should be ok");
        assert_eq!(time.ok(), Some(FindTime::Lower(1111)), "Should be lower");
    }

    #[test]
    fn time_incorect_negative() {
        let time_str = "-";
        let time = time_str.parse::<FindTime>();
        assert!(time.is_err(), "Should be error");
    }

    #[test]
    fn time_incorect_negative_t() {
        let time_str = "-10t";
        let time = time_str.parse::<FindTime>();
        assert!(time.is_err(), "Should be error");
    }

    #[test]
    fn time_incorect_positive() {
        let time_str = "+";
        let time = time_str.parse::<FindTime>();
        assert!(time.is_err(), "Should be error");
    }

    #[test]
    fn time_incorect_positive_t() {
        let time_str = "+10t";
        let time = time_str.parse::<FindTime>();
        assert!(time.is_err(), "Should be error");
    }
}
