use chrono::prelude::*;
use glob::MatchOptions;
use regex::Regex;
use rusoto_s3::Object;

use opts::*;

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
        let object_time = object.last_modified.as_ref().unwrap();
        let last_modified_time = object_time.parse::<DateTime<Utc>>().unwrap().timestamp();
        let now = Utc::now().timestamp();

        match *self {
            FindTime::Upper(seconds) => (now - last_modified_time) >= seconds,
            FindTime::Lower(seconds) => (now - last_modified_time) <= seconds,
        }
    }
}

impl Filter for NameGlob {
    fn filter(&self, object: &Object) -> bool {
        let object_key = object.key.as_ref().map(|x| x.as_ref()).unwrap_or_default();
        self.matches(&object_key)
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
