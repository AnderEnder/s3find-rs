use std::fmt;

use aws_sdk_s3::types::Object;
use humansize::{BINARY, make_format};
use serde::ser::{Serialize, SerializeStruct, Serializer};

use crate::tag_fetcher::TagFetchStats;

/// Summary statistics over the objects a command was executed on.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct FindStat {
    pub total_files: usize,
    pub total_space: i64,
    pub max_size: Option<i64>,
    pub min_size: Option<i64>,
    pub max_key: String,
    pub min_key: String,
}

impl FindStat {
    /// Adds one object to the statistics. Objects without a size count as empty.
    pub fn record(&mut self, object: &Object) {
        let size = object.size.unwrap_or_default();
        let key = object.key().unwrap_or_default();

        self.total_files += 1;
        self.total_space += size;

        // On ties the last largest and the first smallest object win.
        if self.max_size.is_none_or(|max| max <= size) {
            self.max_size = Some(size);
            key.clone_into(&mut self.max_key);
        }
        if self.min_size.is_none_or(|min| min > size) {
            self.min_size = Some(size);
            key.clone_into(&mut self.min_key);
        }
    }

    pub fn average_size(&self) -> i64 {
        match self.total_files {
            0 => 0,
            files => self.total_space / files as i64,
        }
    }
}

impl Serialize for FindStat {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut stat = serializer.serialize_struct("FindStat", 7)?;
        stat.serialize_field("total_files", &self.total_files)?;
        stat.serialize_field("total_space", &self.total_space)?;
        stat.serialize_field("max_size", &self.max_size)?;
        stat.serialize_field("max_key", &self.max_key)?;
        stat.serialize_field("min_size", &self.min_size)?;
        stat.serialize_field("min_key", &self.min_key)?;
        stat.serialize_field("average_size", &self.average_size())?;
        stat.end()
    }
}

/// `--summarize` output with `--json`: the statistics, plus tag fetch counters
/// when tags were fetched.
#[derive(serde::Serialize)]
pub struct Summary<'a> {
    #[serde(flatten)]
    pub stats: &'a FindStat,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tag_fetch: Option<&'a TagFetchStats>,
}

impl fmt::Display for FindStat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let file_size = make_format(BINARY);
        let total_space = file_size(self.total_space as u64);
        let max_size = file_size(self.max_size.unwrap_or_default() as u64);
        let min_size = file_size(self.min_size.unwrap_or_default() as u64);
        let average_size = file_size(self.average_size() as u64);

        writeln!(f)?;
        writeln!(f, "Summary")?;
        writeln!(f, "{:19} {}", "Total files:", self.total_files)?;
        writeln!(f, "Total space:        {total_space}")?;
        writeln!(f, "{:19} {}", "Largest file:", self.max_key)?;
        writeln!(f, "{:19} {}", "Largest file size:", max_size)?;
        writeln!(f, "{:19} {}", "Smallest file:", self.min_key)?;
        writeln!(f, "{:19} {}", "Smallest file size:", min_size)?;
        writeln!(f, "{:19} {}", "Average file size:", average_size)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record_all(stat: &mut FindStat, objects: &[Object]) {
        for object in objects {
            stat.record(object);
        }
    }

    #[test]
    fn test_find_stat_display() {
        let find_stat = FindStat {
            total_files: 42,
            total_space: 1234567890,
            max_size: Some(987654321),
            min_size: Some(123),
            max_key: "largest-file.txt".to_owned(),
            min_key: "smallest-file.txt".to_owned(),
        };

        let display_output = find_stat.to_string();

        assert!(display_output.contains("Summary"), "Missing Summary header");
        assert!(
            display_output.contains("Total files:        42"),
            "Incorrect total files count"
        );
        assert!(
            display_output.contains("Total space:"),
            "Missing total space"
        );
        assert!(
            display_output.contains("Largest file:       largest-file.txt"),
            "Incorrect largest file name"
        );
        assert!(
            display_output.contains("Largest file size:"),
            "Missing largest file size"
        );
        assert!(
            display_output.contains("Smallest file:      smallest-file.txt"),
            "Incorrect smallest file name"
        );
        let file_size = make_format(BINARY);
        let expected_smallest_size = format!("{:19} {}", "Smallest file size:", file_size(123_u64));
        assert!(
            display_output.contains(&expected_smallest_size),
            "Incorrect smallest file size line"
        );
        let expected_average_size =
            format!("{:19} {}", "Average file size:", file_size(29394474_u64));
        assert!(
            display_output.contains(&expected_average_size),
            "Incorrect average file size line"
        );
        assert!(
            display_output.contains("GiB"),
            "Expected binary size format (MiB/GiB)"
        );
    }

    #[test]
    fn test_find_stat_record() {
        let mut stat = FindStat::default();

        record_all(
            &mut stat,
            &[
                Object::builder().key("file1.txt").size(100).build(),
                Object::builder().key("file2.txt").size(200).build(),
                Object::builder().key("file3.txt").size(50).build(),
            ],
        );

        assert_eq!(stat.total_files, 3);
        assert_eq!(stat.total_space, 350);
        assert_eq!(stat.max_size, Some(200));
        assert_eq!(stat.max_key, "file2.txt");
        assert_eq!(stat.min_size, Some(50));
        assert_eq!(stat.min_key, "file3.txt");
        assert_eq!(stat.average_size(), 116);

        record_all(
            &mut stat,
            &[
                Object::builder().key("file4.txt").size(500).build(),
                Object::builder().key("file5.txt").size(10).build(),
            ],
        );

        assert_eq!(stat.total_files, 5);
        assert_eq!(stat.total_space, 860);
        assert_eq!(stat.max_size, Some(500));
        assert_eq!(stat.max_key, "file4.txt");
        assert_eq!(stat.min_size, Some(10));
        assert_eq!(stat.min_key, "file5.txt");
        assert_eq!(stat.average_size(), 172);
    }

    #[test]
    fn test_find_stat_object_without_size_counts_as_empty() {
        let mut stat = FindStat::default();
        record_all(
            &mut stat,
            &[
                Object::builder().key("sized.txt").size(10).build(),
                Object::builder().key("no-size.txt").build(),
            ],
        );

        assert_eq!(stat.total_files, 2);
        assert_eq!(stat.total_space, 10);
        assert_eq!(stat.min_size, Some(0));
        assert_eq!(stat.min_key, "no-size.txt");
    }

    #[test]
    fn test_find_stat_ties() {
        let mut stat = FindStat::default();
        record_all(
            &mut stat,
            &[
                Object::builder().key("first").size(7).build(),
                Object::builder().key("second").size(7).build(),
            ],
        );

        assert_eq!(stat.max_key, "second");
        assert_eq!(stat.min_key, "first");
    }

    #[test]
    fn test_summary_json() {
        let mut stat = FindStat::default();
        record_all(
            &mut stat,
            &[
                Object::builder().key("a").size(10).build(),
                Object::builder().key("b").size(30).build(),
            ],
        );
        let tag_fetch = TagFetchStats::new();
        tag_fetch.record_success();

        let summary = |tag_fetch| {
            serde_json::to_value(Summary {
                stats: &stat,
                tag_fetch,
            })
            .unwrap()
        };

        let expected = serde_json::json!({
            "total_files": 2,
            "total_space": 40,
            "max_size": 30,
            "max_key": "b",
            "min_size": 10,
            "min_key": "a",
            "average_size": 20,
        });
        assert_eq!(summary(None), expected);

        let mut with_tags = expected;
        with_tags["tag_fetch"] =
            serde_json::json!({"success": 1, "failed": 0, "access_denied": 0, "excluded": 0});
        assert_eq!(summary(Some(&tag_fetch)), with_tags);
    }

    #[test]
    fn test_find_stat_empty() {
        let stat = FindStat::default();
        assert_eq!(stat.average_size(), 0);
        assert_eq!(stat.max_size, None);
        assert_eq!(stat.min_size, None);
    }
}
