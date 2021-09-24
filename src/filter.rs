use chrono::prelude::*;
use glob::MatchOptions;
use regex::Regex;
use rusoto_s3::Object;
use std::convert::AsRef;

use crate::arg::*;

pub trait Filter {
    fn filter(&self, object: &Object) -> bool;
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

impl Filter for FindTime {
    fn filter(&self, object: &Object) -> bool {
        let last_modified_time = match object.last_modified.as_ref() {
            Some(object_time) => match object_time.parse::<DateTime<Utc>>() {
                Ok(mtime) => mtime.timestamp(),
                Err(_) => return false,
            },
            None => 0,
        };

        let now = Utc::now().timestamp();

        match *self {
            FindTime::Lower(seconds) => (now - last_modified_time) >= seconds,
            FindTime::Upper(seconds) => (now - last_modified_time) <= seconds,
        }
    }
}

impl Filter for NameGlob {
    fn filter(&self, object: &Object) -> bool {
        let object_key = object.key.as_ref().map(AsRef::as_ref).unwrap_or_default();
        self.matches(object_key)
    }
}

impl Filter for InameGlob {
    fn filter(&self, object: &Object) -> bool {
        let object_key = object.key.as_ref().map(AsRef::as_ref).unwrap_or_default();
        self.0.matches_with(
            object_key,
            MatchOptions {
                case_sensitive: false,
                require_literal_separator: false,
                require_literal_leading_dot: false,
            },
        )
    }
}

impl Filter for Regex {
    fn filter(&self, object: &Object) -> bool {
        let object_key = object.key.as_ref().map(AsRef::as_ref).unwrap_or_default();
        self.is_match(object_key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use std::str::FromStr;

    #[test]
    fn findsize_filter() {
        let object = Object {
            size: Some(10),
            ..Default::default()
        };

        assert!(FindSize::Bigger(5).filter(&object));
        assert!(FindSize::Lower(11).filter(&object));
        assert!(FindSize::Equal(10).filter(&object));

        assert!(!FindSize::Bigger(11).filter(&object));
        assert!(!FindSize::Lower(5).filter(&object));
        assert!(!FindSize::Equal(11).filter(&object));
    }

    #[test]
    fn findtime_filter() {
        let current = Utc::now().checked_sub_signed(Duration::hours(1)).unwrap();
        let time = format!("{:?}", current);
        let object = Object {
            last_modified: Some(time),
            ..Default::default()
        };

        assert!(FindTime::Lower(10).filter(&object));
        assert!(FindTime::Upper(4000).filter(&object));

        assert!(!FindTime::Lower(4000).filter(&object));
        assert!(!FindTime::Upper(10).filter(&object));
    }

    #[test]
    fn nameglob_filter() {
        let object = Object {
            key: Some("some_key".to_owned()),
            ..Default::default()
        };

        assert!(NameGlob::from_str("*ome*").unwrap().filter(&object));
        assert!(NameGlob::from_str("some_key").unwrap().filter(&object));

        assert!(!NameGlob::from_str("ome*").unwrap().filter(&object));
        assert!(!NameGlob::from_str("other").unwrap().filter(&object));
        assert!(!NameGlob::from_str("*Ome*").unwrap().filter(&object));
        assert!(!NameGlob::from_str("some_Key").unwrap().filter(&object));
    }

    #[test]
    fn inameglob_filter() {
        let object = Object {
            key: Some("some_key".to_owned()),
            ..Default::default()
        };

        assert!(InameGlob::from_str("*ome*").unwrap().filter(&object));
        assert!(InameGlob::from_str("some_key").unwrap().filter(&object));
        assert!(InameGlob::from_str("*Ome*").unwrap().filter(&object));
        assert!(InameGlob::from_str("some_Key").unwrap().filter(&object));

        assert!(!InameGlob::from_str("ome*").unwrap().filter(&object));
        assert!(!InameGlob::from_str("other").unwrap().filter(&object));
    }

    #[test]
    fn regex_filter() {
        let object = Object {
            key: Some("some_key".to_owned()),
            ..Default::default()
        };

        assert!(Regex::from_str("^some_key").unwrap().filter(&object));
        assert!(Regex::from_str("some_key$").unwrap().filter(&object));
        assert!(Regex::from_str("key").unwrap().filter(&object));
        assert!(Regex::from_str("key$").unwrap().filter(&object));

        assert!(!Regex::from_str("^Some").unwrap().filter(&object));
        assert!(!Regex::from_str("^key").unwrap().filter(&object));
        assert!(!Regex::from_str("some&").unwrap().filter(&object));
        assert!(!Regex::from_str("other").unwrap().filter(&object));
        assert!(!Regex::from_str("Ome").unwrap().filter(&object));
        assert!(!Regex::from_str("some_Key").unwrap().filter(&object));
    }
}
