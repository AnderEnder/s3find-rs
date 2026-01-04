use aws_sdk_s3::types::{Object, ObjectStorageClass};
use chrono::prelude::*;
use glob::MatchOptions;
use regex::Regex;

use crate::arg::*;
use crate::command::StreamObject;

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

/// Trait for filters that require tag information.
/// Unlike Filter which works on Object, TagAwareFilter works on StreamObject
/// which contains cached tag data.
pub trait TagAwareFilter {
    /// Returns true if the object matches this filter.
    /// Returns None if tags haven't been fetched yet (caller should fetch tags first).
    fn filter_with_tags(&self, object: &StreamObject) -> Option<bool>;

    /// Returns true if this filter requires tags to be fetched.
    fn requires_tags(&self) -> bool {
        true
    }
}

impl TagAwareFilter for TagFilter {
    fn filter_with_tags(&self, object: &StreamObject) -> Option<bool> {
        let tags = object.tags.as_ref()?;
        Some(
            tags.iter()
                .any(|t| t.key() == self.key && t.value() == self.value),
        )
    }
}

impl TagAwareFilter for TagExistsFilter {
    fn filter_with_tags(&self, object: &StreamObject) -> Option<bool> {
        let tags = object.tags.as_ref()?;
        Some(tags.iter().any(|t| t.key() == self.key))
    }
}

/// A collection of tag-aware filters that uses AND logic.
/// All filters must match for an object to pass.
#[derive(Debug, Default)]
pub struct TagFilterList {
    tag_filters: Vec<TagFilter>,
    tag_exists_filters: Vec<TagExistsFilter>,
}

impl TagFilterList {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_opts(opts: &FindOpt) -> Self {
        Self {
            tag_filters: opts.tag.clone(),
            tag_exists_filters: opts.tag_exists.clone(),
        }
    }

    /// Creates a TagFilterList with the specified filters.
    /// This is useful for testing or programmatic filter construction.
    pub fn with_filters(
        tag_filters: Vec<TagFilter>,
        tag_exists_filters: Vec<TagExistsFilter>,
    ) -> Self {
        Self {
            tag_filters,
            tag_exists_filters,
        }
    }

    /// Returns true if there are any tag filters configured.
    pub fn has_filters(&self) -> bool {
        !self.tag_filters.is_empty() || !self.tag_exists_filters.is_empty()
    }

    /// Returns the total number of tag filters.
    pub fn len(&self) -> usize {
        self.tag_filters.len() + self.tag_exists_filters.len()
    }

    /// Returns true if there are no tag filters.
    pub fn is_empty(&self) -> bool {
        self.tag_filters.is_empty() && self.tag_exists_filters.is_empty()
    }

    /// Tests if the object matches all tag filters (AND logic).
    /// Returns None if tags haven't been fetched yet.
    /// Returns Some(true) if all filters match.
    /// Returns Some(false) if any filter doesn't match.
    pub fn matches(&self, object: &StreamObject) -> Option<bool> {
        if self.is_empty() {
            return Some(true);
        }

        // Check all TagFilter filters
        for filter in &self.tag_filters {
            match filter.filter_with_tags(object) {
                None => return None,               // Tags not fetched
                Some(false) => return Some(false), // Short-circuit on first non-match
                Some(true) => continue,
            }
        }

        // Check all TagExistsFilter filters
        for filter in &self.tag_exists_filters {
            match filter.filter_with_tags(object) {
                None => return None,               // Tags not fetched
                Some(false) => return Some(false), // Short-circuit on first non-match
                Some(true) => continue,
            }
        }

        Some(true)
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
    fn tag_filter_with_tags() {
        use aws_sdk_s3::types::Tag;

        let object = Object::builder().key("test.txt").build();
        let tags = vec![
            Tag::builder().key("env").value("prod").build().unwrap(),
            Tag::builder().key("team").value("data").build().unwrap(),
        ];
        let stream_obj = StreamObject {
            object,
            version_id: None,
            is_latest: None,
            is_delete_marker: false,
            tags: Some(tags),
        };

        // Matching tag
        let filter = TagFilter {
            key: "env".to_string(),
            value: "prod".to_string(),
        };
        assert_eq!(filter.filter_with_tags(&stream_obj), Some(true));

        // Non-matching value
        let filter = TagFilter {
            key: "env".to_string(),
            value: "dev".to_string(),
        };
        assert_eq!(filter.filter_with_tags(&stream_obj), Some(false));

        // Non-existent key
        let filter = TagFilter {
            key: "nonexistent".to_string(),
            value: "value".to_string(),
        };
        assert_eq!(filter.filter_with_tags(&stream_obj), Some(false));
    }

    #[test]
    fn tag_filter_without_tags() {
        let object = Object::builder().key("test.txt").build();
        let stream_obj = StreamObject {
            object,
            version_id: None,
            is_latest: None,
            is_delete_marker: false,
            tags: None,
        };

        let filter = TagFilter {
            key: "env".to_string(),
            value: "prod".to_string(),
        };
        assert_eq!(filter.filter_with_tags(&stream_obj), None);
    }

    #[test]
    fn tag_exists_filter_with_tags() {
        use aws_sdk_s3::types::Tag;

        let object = Object::builder().key("test.txt").build();
        let tags = vec![
            Tag::builder().key("env").value("prod").build().unwrap(),
            Tag::builder().key("team").value("data").build().unwrap(),
        ];
        let stream_obj = StreamObject {
            object,
            version_id: None,
            is_latest: None,
            is_delete_marker: false,
            tags: Some(tags),
        };

        // Existing key
        let filter = TagExistsFilter {
            key: "env".to_string(),
        };
        assert_eq!(filter.filter_with_tags(&stream_obj), Some(true));

        // Non-existent key
        let filter = TagExistsFilter {
            key: "nonexistent".to_string(),
        };
        assert_eq!(filter.filter_with_tags(&stream_obj), Some(false));
    }

    #[test]
    fn tag_filter_list_empty() {
        let object = Object::builder().key("test.txt").build();
        let stream_obj = StreamObject {
            object,
            version_id: None,
            is_latest: None,
            is_delete_marker: false,
            tags: None,
        };

        let filter_list = TagFilterList::new();
        assert!(filter_list.is_empty());
        assert!(!filter_list.has_filters());
        assert_eq!(filter_list.len(), 0);
        // Empty filter list always matches
        assert_eq!(filter_list.matches(&stream_obj), Some(true));
    }

    #[test]
    fn tag_filter_list_and_logic() {
        use aws_sdk_s3::types::Tag;

        let object = Object::builder().key("test.txt").build();
        let tags = vec![
            Tag::builder().key("env").value("prod").build().unwrap(),
            Tag::builder().key("team").value("data").build().unwrap(),
        ];
        let stream_obj = StreamObject {
            object,
            version_id: None,
            is_latest: None,
            is_delete_marker: false,
            tags: Some(tags),
        };

        // Both filters match
        let filter_list = TagFilterList {
            tag_filters: vec![TagFilter {
                key: "env".to_string(),
                value: "prod".to_string(),
            }],
            tag_exists_filters: vec![TagExistsFilter {
                key: "team".to_string(),
            }],
        };
        assert!(filter_list.has_filters());
        assert_eq!(filter_list.len(), 2);
        assert_eq!(filter_list.matches(&stream_obj), Some(true));

        // One filter doesn't match (AND logic - should fail)
        let filter_list = TagFilterList {
            tag_filters: vec![TagFilter {
                key: "env".to_string(),
                value: "dev".to_string(), // Wrong value
            }],
            tag_exists_filters: vec![TagExistsFilter {
                key: "team".to_string(),
            }],
        };
        assert_eq!(filter_list.matches(&stream_obj), Some(false));

        // Missing tag key (AND logic - should fail)
        let filter_list = TagFilterList {
            tag_filters: vec![],
            tag_exists_filters: vec![TagExistsFilter {
                key: "nonexistent".to_string(),
            }],
        };
        assert_eq!(filter_list.matches(&stream_obj), Some(false));
    }

    #[test]
    fn tag_filter_list_without_tags() {
        let object = Object::builder().key("test.txt").build();
        let stream_obj = StreamObject {
            object,
            version_id: None,
            is_latest: None,
            is_delete_marker: false,
            tags: None,
        };

        let filter_list = TagFilterList {
            tag_filters: vec![TagFilter {
                key: "env".to_string(),
                value: "prod".to_string(),
            }],
            tag_exists_filters: vec![],
        };

        // Should return None when tags not fetched
        assert_eq!(filter_list.matches(&stream_obj), None);
    }
}
