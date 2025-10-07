use aws_sdk_s3::types::Object;

use crate::arg::*;
use crate::filter::Filter;

pub struct FilterList<'a>(pub Vec<&'a dyn Filter>);

impl<'a> FilterList<'a> {
    pub fn from_opts(opts: &'a FindOpt) -> Self {
        let FindOpt {
            name,
            iname,
            regex,
            size,
            mtime,
            storage_class,
            ..
        } = opts;

        let mut filters = FilterList::new()
            .add_filters(name)
            .add_filters(iname)
            .add_filters(mtime)
            .add_filters(regex)
            .add_filters(size);

        if let Some(storage_class) = storage_class {
            filters.add_single_filter(storage_class);
        }
        filters
    }

    pub async fn test_match(&self, object: Object) -> bool {
        for item in &self.0 {
            if !item.filter(&object) {
                return false;
            }
        }

        true
    }

    #[inline]
    pub fn add_filter(mut self, filter: &'a dyn Filter) -> Self {
        self.0.push(filter);
        self
    }

    #[inline]
    pub fn add_single_filter(&mut self, filter: &'a dyn Filter) {
        self.0.push(filter);
    }

    #[inline]
    pub fn add_filters<F: Filter>(mut self, filters: &'a [F]) -> Self {
        for filter in filters {
            self.0.push(filter);
        }
        self
    }

    pub fn new() -> FilterList<'a> {
        FilterList(Vec::new())
    }
}

impl Default for FilterList<'_> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use crate::{command::default_stats, filter::Filter};

    use super::*;

    use aws_config::Region;
    use aws_sdk_s3::types::ObjectStorageClass;
    use glob::Pattern;
    use regex::Regex;
    use std::str::FromStr;

    #[tokio::test]
    async fn test_filter_list_test_match() {
        let object = Object::builder().key("test-object.txt").size(100).build();

        struct AlwaysTrueFilter;
        impl Filter for AlwaysTrueFilter {
            fn filter(&self, _: &Object) -> bool {
                true
            }
        }

        struct AlwaysFalseFilter;
        impl Filter for AlwaysFalseFilter {
            fn filter(&self, _: &Object) -> bool {
                false
            }
        }

        let true_filter = AlwaysTrueFilter;
        let filter_list = FilterList::new()
            .add_filter(&true_filter)
            .add_filter(&true_filter);

        assert!(
            filter_list.test_match(object.clone()).await,
            "all true filters failed"
        );

        let false_filter = AlwaysFalseFilter;
        let filter_list = FilterList::new()
            .add_filter(&true_filter)
            .add_filter(&false_filter);

        assert!(
            !filter_list.test_match(object.clone()).await,
            "one false filter failed"
        );
    }

    #[test]
    fn test_filter_list_new() {
        let name_patterns = vec![Pattern::new("*.txt").unwrap()];
        let iname_globs = vec![InameGlob(Pattern::new("*.TXT").unwrap())];
        let regexs = vec![Regex::new(r"test.*\.txt").unwrap()];
        let sizes = vec![FindSize::Bigger(100)];
        let mtimes = vec![FindTime::Lower(3600 * 24)];

        let filter_list = FilterList::new()
            .add_filters(&name_patterns)
            .add_filters(&iname_globs)
            .add_filters(&regexs)
            .add_filters(&sizes)
            .add_filters(&mtimes);

        assert_eq!(filter_list.0.len(), 5, "it should contains 5 filters");
    }

    #[test]
    fn test_default_stats() {
        let stats = default_stats(true);
        assert!(stats.is_some());

        let stats = default_stats(false);
        assert!(stats.is_none());
    }

    #[test]
    fn test_filter_list_default() {
        let filter_list = FilterList::default();

        assert_eq!(
            filter_list.0.len(),
            0,
            "default filter list should be empty"
        );

        let test_filter = Pattern::new("*.txt").unwrap();
        let filter_list = filter_list.add_filter(&test_filter);

        assert_eq!(
            filter_list.0.len(),
            1,
            "should be able to add filters to default list"
        );
    }

    #[test]
    fn test_filter_list_from_opts() {
        let opts = FindOpt {
            path: S3Path::from_str("s3://bucket/prefix").unwrap(),
            aws_access_key: Some("test_access_key".to_owned()),
            aws_secret_key: Some("test_secret_key".to_owned()),
            aws_region: Some(Region::from_static("us-east-2")),
            storage_class: None,
            page_size: 100,
            limit: None,
            summarize: false,
            name: Vec::new(),
            iname: Vec::new(),
            regex: Vec::new(),
            size: Vec::new(),
            mtime: Vec::new(),
            cmd: None,
        };

        let filter_list = FilterList::from_opts(&opts);
        assert_eq!(
            filter_list.0.len(),
            0,
            "empty options should create empty filter list"
        );

        let mut opts = FindOpt {
            name: vec![Pattern::new("*.txt").unwrap()],
            iname: vec![InameGlob(Pattern::new("*.TXT").unwrap())],
            regex: vec![Regex::new(r"test.*\.txt").unwrap()],
            size: vec![FindSize::Bigger(100)],
            mtime: vec![FindTime::Lower(3600 * 24)],
            cmd: None,
            ..opts
        };

        let filter_list = FilterList::from_opts(&opts);
        assert_eq!(filter_list.0.len(), 5, "should contain 5 filters");

        let storage_class_filter = ObjectStorageClass::Standard;
        opts.storage_class = Some(storage_class_filter);

        let filter_list = FilterList::from_opts(&opts);
        assert_eq!(
            filter_list.0.len(),
            6,
            "should contain 6 filters including storage class"
        );
    }
}
