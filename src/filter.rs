use std::fmt;
use std::time::SystemTime;

use aws_sdk_s3::primitives::DateTime;
use aws_sdk_s3::types::ObjectStorageClass;
use glob::MatchOptions;
use regex::Regex;

use crate::arg::*;
use crate::object::StreamObject;

/// A predicate over listed objects; a [`FilterList`] combines filters with AND.
///
/// To add a filter, implement this trait (usually for the type clap parses the
/// option into) and register it in [`FilterList::from_opts`], or in
/// [`FilterList::tag_filters_from_opts`] if it needs object tags.
pub trait Filter: Send + Sync {
    fn filter(&self, object: &StreamObject) -> bool;
}

#[inline]
fn key(object: &StreamObject) -> &str {
    object.key().unwrap_or_default()
}

impl Filter for FindSize {
    fn filter(&self, object: &StreamObject) -> bool {
        let object_size = object.object.size.unwrap_or_default();
        match *self {
            FindSize::Bigger(size) => object_size > size,
            FindSize::Lower(size) => object_size < size,
            FindSize::Equal(size) => object_size == size,
        }
    }
}

/// `--mtime` evaluated against a fixed "now", so every object in a run is
/// compared to the same point in time.
#[derive(Debug, Clone, PartialEq)]
pub struct MtimeFilter {
    pub time: FindTime,
    /// Unix timestamp, in seconds, that ages are measured from.
    pub now: i64,
}

impl MtimeFilter {
    pub fn new(time: FindTime, now: i64) -> Self {
        Self { time, now }
    }
}

impl Filter for MtimeFilter {
    fn filter(&self, object: &StreamObject) -> bool {
        let last_modified = object
            .object
            .last_modified
            .map(|time| time.secs())
            .unwrap_or_default();
        let age = self.now - last_modified;

        match self.time {
            FindTime::Lower(seconds) => age >= seconds,
            FindTime::Upper(seconds) => age <= seconds,
        }
    }
}

impl Filter for NameGlob {
    fn filter(&self, object: &StreamObject) -> bool {
        self.matches(key(object))
    }
}

const CASE_INSENSITIVE: MatchOptions = MatchOptions {
    case_sensitive: false,
    require_literal_separator: false,
    require_literal_leading_dot: false,
};

impl Filter for InameGlob {
    fn filter(&self, object: &StreamObject) -> bool {
        self.0.matches_with(key(object), CASE_INSENSITIVE)
    }
}

impl Filter for Regex {
    fn filter(&self, object: &StreamObject) -> bool {
        self.is_match(key(object))
    }
}

impl Filter for ObjectStorageClass {
    fn filter(&self, object: &StreamObject) -> bool {
        Some(self) == object.object.storage_class()
    }
}

/// Needs fetched tags: objects whose tags are unknown never match.
impl Filter for TagFilter {
    fn filter(&self, object: &StreamObject) -> bool {
        object.get_tag(&self.key) == Some(self.value.as_str())
    }
}

/// Needs fetched tags: objects whose tags are unknown never match.
impl Filter for TagExistsFilter {
    fn filter(&self, object: &StreamObject) -> bool {
        object.has_tag_key(&self.key)
    }
}

/// Filters that must all match (AND logic). An empty list matches everything.
#[derive(Default)]
pub struct FilterList(Vec<Box<dyn Filter>>);

impl FilterList {
    pub fn new() -> Self {
        Self::default()
    }

    /// Filters answerable from listing metadata, cheapest checks first.
    pub fn from_opts(opts: &FindOpt) -> Self {
        let now = DateTime::from(SystemTime::now()).secs();

        let mut filters = Self::new();
        filters.extend(opts.storage_class.as_ref().map(ObjectStorageClass::from));
        filters.extend(opts.size.iter().cloned());
        filters.extend(
            opts.mtime
                .iter()
                .map(|time| MtimeFilter::new(time.clone(), now)),
        );
        filters.extend(opts.name.iter().cloned());
        filters.extend(opts.iname.iter().cloned());
        filters.extend(opts.regex.iter().cloned());
        filters
    }

    /// Filters that need object tags, i.e. a GetObjectTagging call per object.
    pub fn tag_filters_from_opts(opts: &FindOpt) -> Self {
        let mut filters = Self::new();
        filters.extend(opts.tag.iter().cloned());
        filters.extend(opts.tag_exists.iter().cloned());
        filters
    }

    pub fn push(&mut self, filter: impl Filter + 'static) {
        self.0.push(Box::new(filter));
    }

    pub fn with(mut self, filter: impl Filter + 'static) -> Self {
        self.push(filter);
        self
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn matches(&self, object: &StreamObject) -> bool {
        self.0.iter().all(|filter| filter.filter(object))
    }
}

impl<F: Filter + 'static> Extend<F> for FilterList {
    fn extend<I: IntoIterator<Item = F>>(&mut self, filters: I) {
        for filter in filters {
            self.push(filter);
        }
    }
}

impl fmt::Debug for FilterList {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FilterList({} filters)", self.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::opts;
    use aws_sdk_s3::types::{Object, Tag};
    use std::{str::FromStr, time::Duration};

    fn stream_object(object: Object) -> StreamObject {
        StreamObject::from_object(object)
    }

    fn keyed(key: &str) -> StreamObject {
        stream_object(Object::builder().key(key).build())
    }

    fn tagged(tags: &[(&str, &str)]) -> StreamObject {
        let mut object = keyed("test.txt");
        object.tags = Some(
            tags.iter()
                .map(|(key, value)| Tag::builder().key(*key).value(*value).build().unwrap())
                .collect(),
        );
        object
    }

    fn tag_filter(key: &str, value: &str) -> TagFilter {
        TagFilter {
            key: key.to_string(),
            value: value.to_string(),
        }
    }

    fn tag_exists(key: &str) -> TagExistsFilter {
        TagExistsFilter {
            key: key.to_string(),
        }
    }

    #[test]
    fn findsize_filter() {
        let object = stream_object(Object::builder().size(10).build());

        assert!(FindSize::Bigger(5).filter(&object));
        assert!(FindSize::Lower(11).filter(&object));
        assert!(FindSize::Equal(10).filter(&object));

        assert!(!FindSize::Bigger(10).filter(&object));
        assert!(!FindSize::Lower(10).filter(&object));
        assert!(!FindSize::Bigger(11).filter(&object));
        assert!(!FindSize::Lower(5).filter(&object));
        assert!(!FindSize::Equal(11).filter(&object));
    }

    #[test]
    fn mtime_filter() {
        let modified = std::time::SystemTime::now()
            .checked_sub(Duration::from_secs(60))
            .unwrap();
        let now = DateTime::from(std::time::SystemTime::now()).secs();
        let object = stream_object(Object::builder().last_modified(modified.into()).build());

        assert!(MtimeFilter::new(FindTime::Lower(10), now).filter(&object));
        assert!(MtimeFilter::new(FindTime::Upper(4000), now).filter(&object));

        assert!(!MtimeFilter::new(FindTime::Lower(4000), now).filter(&object));
        assert!(!MtimeFilter::new(FindTime::Upper(10), now).filter(&object));
    }

    #[test]
    fn mtime_filter_uses_fixed_now() {
        let object = stream_object(
            Object::builder()
                .last_modified(DateTime::from_secs(1_000))
                .build(),
        );

        // 100 seconds old at now=1_100
        assert!(MtimeFilter::new(FindTime::Lower(100), 1_100).filter(&object));
        assert!(!MtimeFilter::new(FindTime::Lower(101), 1_100).filter(&object));
        assert!(MtimeFilter::new(FindTime::Upper(100), 1_100).filter(&object));
        assert!(!MtimeFilter::new(FindTime::Upper(99), 1_100).filter(&object));
    }

    #[test]
    fn nameglob_filter() {
        let object = keyed("some_key");

        assert!(NameGlob::from_str("*ome*").unwrap().filter(&object));
        assert!(NameGlob::from_str("some_key").unwrap().filter(&object));

        assert!(!NameGlob::from_str("ome*").unwrap().filter(&object));
        assert!(!NameGlob::from_str("other").unwrap().filter(&object));
        assert!(!NameGlob::from_str("*Ome*").unwrap().filter(&object));
        assert!(!NameGlob::from_str("some_Key").unwrap().filter(&object));
    }

    #[test]
    fn inameglob_filter() {
        let object = keyed("some_key");

        assert!(InameGlob::from_str("*ome*").unwrap().filter(&object));
        assert!(InameGlob::from_str("some_key").unwrap().filter(&object));
        assert!(InameGlob::from_str("*Ome*").unwrap().filter(&object));
        assert!(InameGlob::from_str("some_Key").unwrap().filter(&object));

        assert!(!InameGlob::from_str("ome*").unwrap().filter(&object));
        assert!(!InameGlob::from_str("other").unwrap().filter(&object));
    }

    #[test]
    fn regex_filter() {
        let object = keyed("some_key");

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
    fn key_filters_treat_missing_key_as_empty() {
        let object = stream_object(Object::builder().build());

        assert!(NameGlob::from_str("*").unwrap().filter(&object));
        assert!(Regex::from_str("^$").unwrap().filter(&object));
        assert!(!Regex::from_str(".").unwrap().filter(&object));
    }

    #[test]
    fn object_storage_class_filter() {
        let standard_object = stream_object(
            Object::builder()
                .storage_class(ObjectStorageClass::Standard)
                .build(),
        );
        let glacier_object = stream_object(
            Object::builder()
                .storage_class(ObjectStorageClass::Glacier)
                .build(),
        );

        assert!(ObjectStorageClass::Standard.filter(&standard_object));
        assert!(ObjectStorageClass::Glacier.filter(&glacier_object));

        assert!(!ObjectStorageClass::Standard.filter(&glacier_object));
        assert!(!ObjectStorageClass::Glacier.filter(&standard_object));

        let no_class_object = stream_object(Object::builder().build());
        assert!(!ObjectStorageClass::Standard.filter(&no_class_object));
    }

    #[test]
    fn tag_filter_with_tags() {
        let object = tagged(&[("env", "prod"), ("team", "data")]);

        assert!(tag_filter("env", "prod").filter(&object));
        assert!(!tag_filter("env", "dev").filter(&object));
        assert!(!tag_filter("nonexistent", "value").filter(&object));
    }

    #[test]
    fn tag_filters_without_fetched_tags_do_not_match() {
        let object = keyed("test.txt");

        assert!(!tag_filter("env", "prod").filter(&object));
        assert!(!tag_exists("env").filter(&object));
    }

    #[test]
    fn tag_exists_filter_with_tags() {
        let object = tagged(&[("env", "prod"), ("team", "data")]);

        assert!(tag_exists("env").filter(&object));
        assert!(!tag_exists("nonexistent").filter(&object));
        assert!(!tag_exists("owner").filter(&tagged(&[])));
    }

    #[test]
    fn filter_list_uses_and_logic() {
        struct Const(bool);
        impl Filter for Const {
            fn filter(&self, _: &StreamObject) -> bool {
                self.0
            }
        }

        let object = keyed("test-object.txt");

        assert!(FilterList::new().matches(&object), "empty list matches all");
        assert!(
            FilterList::new()
                .with(Const(true))
                .with(Const(true))
                .matches(&object)
        );
        assert!(
            !FilterList::new()
                .with(Const(true))
                .with(Const(false))
                .matches(&object)
        );
    }

    #[test]
    fn filter_list_with_tag_filters() {
        let object = tagged(&[("env", "prod"), ("team", "data")]);

        let both_match = FilterList::new()
            .with(tag_filter("env", "prod"))
            .with(tag_exists("team"));
        assert_eq!(both_match.len(), 2);
        assert!(both_match.matches(&object));

        let one_mismatch = FilterList::new()
            .with(tag_filter("env", "dev"))
            .with(tag_exists("team"));
        assert!(!one_mismatch.matches(&object));
        assert!(!both_match.matches(&keyed("untagged.txt")));
    }

    #[test]
    fn filter_list_from_opts() {
        let filters = FilterList::from_opts(&opts(&["s3://bucket/prefix"]));
        assert!(filters.is_empty(), "no options, no filters");

        let filters = FilterList::from_opts(&opts(&[
            "s3://bucket/prefix",
            "--name",
            "*.txt",
            "--iname",
            "*.TXT",
            "--regex",
            r"test.*\.txt",
            "--bytes-size",
            "+100",
            "--mtime",
            "+1d",
        ]));
        assert_eq!(filters.len(), 5);

        let filters = FilterList::from_opts(&opts(&[
            "s3://bucket/prefix",
            "--name",
            "*.txt",
            "--storage-class",
            "STANDARD",
        ]));
        assert_eq!(filters.len(), 2);

        assert!(
            filters.matches(&stream_object(
                Object::builder()
                    .key("a.txt")
                    .storage_class(ObjectStorageClass::Standard)
                    .build()
            ))
        );
        assert!(
            !filters.matches(&stream_object(
                Object::builder()
                    .key("a.txt")
                    .storage_class(ObjectStorageClass::Glacier)
                    .build()
            ))
        );
    }

    #[test]
    fn filter_list_from_opts_ignores_tag_filters() {
        let opts = opts(&[
            "s3://bucket",
            "--tag",
            "env=prod",
            "--tag",
            "team=data",
            "--tag-exists",
            "owner",
        ]);

        assert!(FilterList::from_opts(&opts).is_empty());

        let tag_filters = FilterList::tag_filters_from_opts(&opts);
        assert_eq!(tag_filters.len(), 3);
        assert!(tag_filters.matches(&tagged(&[
            ("env", "prod"),
            ("team", "data"),
            ("owner", "me")
        ])));
        assert!(!tag_filters.matches(&tagged(&[("env", "prod"), ("team", "data")])));
    }

    #[test]
    fn filter_list_debug() {
        let filters = FilterList::new().with(FindSize::Equal(1));
        assert_eq!(format!("{filters:?}"), "FilterList(1 filters)");
    }
}
