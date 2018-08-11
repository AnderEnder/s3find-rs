use failure::Error;
use glob::Pattern;
use regex::Regex;
use std::str::FromStr;

use error::*;

pub type Result<T> = ::std::result::Result<T, Error>;

#[derive(Debug, Clone, PartialEq)]
pub struct S3path {
    pub bucket: String,
    pub prefix: Option<String>,
}

impl FromStr for S3path {
    type Err = Error;

    fn from_str(s: &str) -> Result<S3path> {
        let s3_vec: Vec<&str> = s.split('/').collect();
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

pub type NameGlob = Pattern;

#[derive(Debug, Clone, PartialEq)]
pub struct InameGlob(pub Pattern);

impl FromStr for InameGlob {
    type Err = Error;

    fn from_str(s: &str) -> Result<InameGlob> {
        let pattern = Pattern::from_str(s)?;
        Ok(InameGlob(pattern))
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct FindTag {
    pub key: String,
    pub value: String,
}

impl FromStr for FindTag {
    type Err = Error;

    fn from_str(s: &str) -> Result<FindTag> {
        let re = Regex::new(r"(\w+):(\w+)$")?;
        let m = re.captures(s).ok_or(FindError::TagParseError)?;

        let key = m.get(1).ok_or(FindError::TagKeyParseError)?.as_str();
        let value = m.get(2).ok_or(FindError::TagValueParseError)?.as_str();

        Ok(FindTag {
            key: key.to_string(),
            value: value.to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let path: Result<S3path> = url.parse();
        assert!(
            path.is_err(),
            "This s3 url should not be validated posivitely"
        );
    }

    #[test]
    fn s3path_without_bucket() {
        let url = "s3://";
        let path: Result<S3path> = url.parse();
        assert!(
            path.is_err(),
            "This s3 url should not be validated posivitely"
        );
    }

    #[test]
    fn s3path_without_2_slash() {
        let url = "s3:/testbucket";
        let path: Result<S3path> = url.parse();
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

    #[test]
    fn tag_ok() {
        let str = "tag1:value2";
        let tag = str.parse::<FindTag>();
        assert!(tag.is_ok(), "Should be ok");
        let ftag = tag.unwrap();
        assert_eq!(ftag.key, "tag1", "Should be tag1");
        assert_eq!(ftag.value, "value2", "Should be value2");
    }

    #[test]
    fn tag_incorect_1() {
        let str = "tag1value2";
        let time = str.parse::<FindTag>();
        assert!(time.is_err(), "Should not be parsed");
    }

    #[test]
    fn tag_incorect_2() {
        let str = "tag1:value2:";
        let time = str.parse::<FindTag>();
        assert!(time.is_err(), "Should not be parsed");
    }

    #[test]
    fn tag_incorect_3() {
        let str = ":";
        let time = str.parse::<FindTag>();
        assert!(time.is_err(), "Should not be parsed");
    }
}
