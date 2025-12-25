use aws_sdk_s3::types::{Object, ObjectStorageClass};
use chrono::prelude::*;
use glob::MatchOptions;
use regex::Regex;

use crate::arg::*;

pub trait Filter {
    fn filter(&self, object: &Object) -> bool;
}

impl Filter for FindSize {
    fn filter(&self, object: &Object) -> bool {
        let object_size = object.size.unwrap_or_default();
        match *self {
            FindSize::Bigger(size) => object_size >= size,
            FindSize::Lower(size) => object_size <= size,
            FindSize::Equal(size) => object_size == size,
        }
    }
}

impl Filter for FindTime {
    fn filter(&self, object: &Object) -> bool {
        let last_modified_time = object.last_modified.map(|x| x.secs()).unwrap_or_default();

        let now = Utc::now().timestamp();

        match *self {
            FindTime::Lower(seconds) => (now - last_modified_time) >= seconds,
            FindTime::Upper(seconds) => (now - last_modified_time) <= seconds,
        }
    }
}

impl Filter for NameGlob {
    fn filter(&self, object: &Object) -> bool {
        let object_key = object.key.clone().unwrap_or_default();
        self.matches(&object_key)
    }
}

impl Filter for InameGlob {
    fn filter(&self, object: &Object) -> bool {
        let object_key = object.key.clone().unwrap_or_default();
        self.0.matches_with(
            &object_key,
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
        let object_key = object.key.clone().unwrap_or_default();
        self.is_match(&object_key)
    }
}

impl Filter for ObjectStorageClass {
    fn filter(&self, object: &Object) -> bool {
        Some(self) == object.storage_class()
    }
}

impl Filter for FindDepth {
    fn filter(&self, object: &Object) -> bool {
        let object_key = object.key.clone().unwrap_or_default();
        let depth = self.calculate_depth(&object_key);

        // Check if depth is within the specified range
        let meets_min = self.mindepth.map_or(true, |min| depth >= min);
        let meets_max = self.maxdepth.map_or(true, |max| depth <= max);

        meets_min && meets_max
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{str::FromStr, time::Duration};

    #[test]
    fn findsize_filter() {
        let object = Object::builder().size(10).build();

        assert!(FindSize::Bigger(5).filter(&object));
        assert!(FindSize::Lower(11).filter(&object));
        assert!(FindSize::Equal(10).filter(&object));

        assert!(!FindSize::Bigger(11).filter(&object));
        assert!(!FindSize::Lower(5).filter(&object));
        assert!(!FindSize::Equal(11).filter(&object));
    }

    #[test]
    fn findtime_filter() {
        let current = std::time::SystemTime::now()
            .checked_sub(Duration::from_secs(60))
            .unwrap();
        let object = Object::builder().last_modified(current.into()).build();

        assert!(FindTime::Lower(10).filter(&object));
        assert!(FindTime::Upper(4000).filter(&object));

        assert!(!FindTime::Lower(4000).filter(&object));
        assert!(!FindTime::Upper(10).filter(&object));
    }

    #[test]
    fn nameglob_filter() {
        let object = Object::builder().key("some_key").build();

        assert!(NameGlob::from_str("*ome*").unwrap().filter(&object));
        assert!(NameGlob::from_str("some_key").unwrap().filter(&object));

        assert!(!NameGlob::from_str("ome*").unwrap().filter(&object));
        assert!(!NameGlob::from_str("other").unwrap().filter(&object));
        assert!(!NameGlob::from_str("*Ome*").unwrap().filter(&object));
        assert!(!NameGlob::from_str("some_Key").unwrap().filter(&object));
    }

    #[test]
    fn inameglob_filter() {
        let object = Object::builder().key("some_key").build();

        assert!(InameGlob::from_str("*ome*").unwrap().filter(&object));
        assert!(InameGlob::from_str("some_key").unwrap().filter(&object));
        assert!(InameGlob::from_str("*Ome*").unwrap().filter(&object));
        assert!(InameGlob::from_str("some_Key").unwrap().filter(&object));

        assert!(!InameGlob::from_str("ome*").unwrap().filter(&object));
        assert!(!InameGlob::from_str("other").unwrap().filter(&object));
    }

    #[test]
    fn regex_filter() {
        let object = Object::builder().key("some_key").build();

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

    #[test]
    fn object_storage_class_filter() {
        let standard_object = Object::builder()
            .storage_class(ObjectStorageClass::Standard)
            .build();

        let glacier_object = Object::builder()
            .storage_class(ObjectStorageClass::Glacier)
            .build();

        assert!(ObjectStorageClass::Standard.filter(&standard_object));
        assert!(ObjectStorageClass::Glacier.filter(&glacier_object));

        assert!(!ObjectStorageClass::Standard.filter(&glacier_object));
        assert!(!ObjectStorageClass::Glacier.filter(&standard_object));

        let no_class_object = Object::builder().build();
        assert!(!ObjectStorageClass::Standard.filter(&no_class_object));
    }

    #[test]
    fn finddepth_filter_maxdepth() {
        use crate::arg::FindDepth;

        // Test with prefix "logs"
        let depth_filter = FindDepth::new(Some("logs".to_string()), Some(2), None);

        let depth1_object = Object::builder().key("logs/file.txt").build();
        let depth2_object = Object::builder().key("logs/2024/file.txt").build();
        let depth3_object = Object::builder().key("logs/2024/01/file.txt").build();

        assert!(depth_filter.filter(&depth1_object), "depth 1 should pass maxdepth 2");
        assert!(depth_filter.filter(&depth2_object), "depth 2 should pass maxdepth 2");
        assert!(!depth_filter.filter(&depth3_object), "depth 3 should not pass maxdepth 2");
    }

    #[test]
    fn finddepth_filter_mindepth() {
        use crate::arg::FindDepth;

        let depth_filter = FindDepth::new(Some("logs".to_string()), None, Some(2));

        let depth1_object = Object::builder().key("logs/file.txt").build();
        let depth2_object = Object::builder().key("logs/2024/file.txt").build();
        let depth3_object = Object::builder().key("logs/2024/01/file.txt").build();

        assert!(!depth_filter.filter(&depth1_object), "depth 1 should not pass mindepth 2");
        assert!(depth_filter.filter(&depth2_object), "depth 2 should pass mindepth 2");
        assert!(depth_filter.filter(&depth3_object), "depth 3 should pass mindepth 2");
    }

    #[test]
    fn finddepth_filter_both() {
        use crate::arg::FindDepth;

        let depth_filter = FindDepth::new(Some("logs".to_string()), Some(3), Some(2));

        let depth1_object = Object::builder().key("logs/file.txt").build();
        let depth2_object = Object::builder().key("logs/2024/file.txt").build();
        let depth3_object = Object::builder().key("logs/2024/01/file.txt").build();
        let depth4_object = Object::builder().key("logs/2024/01/02/file.txt").build();

        assert!(!depth_filter.filter(&depth1_object), "depth 1 should not pass mindepth 2");
        assert!(depth_filter.filter(&depth2_object), "depth 2 should pass mindepth 2 and maxdepth 3");
        assert!(depth_filter.filter(&depth3_object), "depth 3 should pass mindepth 2 and maxdepth 3");
        assert!(!depth_filter.filter(&depth4_object), "depth 4 should not pass maxdepth 3");
    }

    #[test]
    fn finddepth_filter_no_prefix() {
        use crate::arg::FindDepth;

        // Test with no prefix (root of bucket)
        let depth_filter = FindDepth::new(None, Some(1), None);

        let depth1_object = Object::builder().key("file.txt").build();
        let depth2_object = Object::builder().key("dir/file.txt").build();

        assert!(depth_filter.filter(&depth1_object), "depth 1 should pass maxdepth 1");
        assert!(!depth_filter.filter(&depth2_object), "depth 2 should not pass maxdepth 1");
    }

    #[test]
    fn finddepth_calculate_depth() {
        use crate::arg::FindDepth;

        let depth_filter = FindDepth::new(Some("logs".to_string()), None, None);

        assert_eq!(depth_filter.calculate_depth("logs/file.txt"), 1);
        assert_eq!(depth_filter.calculate_depth("logs/2024/file.txt"), 2);
        assert_eq!(depth_filter.calculate_depth("logs/2024/01/file.txt"), 3);
        assert_eq!(depth_filter.calculate_depth("logs/2024/01/02/file.txt"), 4);
    }
}
