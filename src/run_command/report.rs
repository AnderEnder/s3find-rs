//! What commands write about each object: a line of text, or with `--json` one
//! JSON object per line.

use std::fmt;
use std::io::Write;

use anyhow::Error;
use aws_sdk_s3::types::Tag;
use serde::ser::SerializeMap;
use serde::{Serialize, Serializer};

use crate::object::StreamObject;

/// Writes `record` as its text line, or as one JSON object with `json`.
///
/// `preview` marks a record that only describes what `--dry-run` would do:
/// the text gets a `[dry-run] ` prefix and the JSON a `"dry_run": true` field.
pub(crate) fn write_record<R>(
    out: &mut (dyn Write + Send),
    record: &R,
    json: bool,
    preview: bool,
) -> Result<(), Error>
where
    R: fmt::Display + Serialize + ?Sized,
{
    match (json, preview) {
        (true, _) => {
            serde_json::to_writer(&mut *out, &Previewed { record, preview })?;
            writeln!(out)?;
        }
        (false, true) => writeln!(out, "[dry-run] {record}")?,
        (false, false) => writeln!(out, "{record}")?,
    }
    Ok(())
}

#[derive(Serialize)]
struct Previewed<'r, R: ?Sized> {
    #[serde(flatten)]
    record: &'r R,
    #[serde(rename = "dry_run", skip_serializing_if = "std::ops::Not::not")]
    preview: bool,
}

/// `s3://bucket/key`, formatted without allocating.
#[derive(Debug, Clone, Copy)]
pub(crate) struct S3Url<'a> {
    pub(crate) bucket: &'a str,
    pub(crate) key: &'a str,
}

impl fmt::Display for S3Url<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "s3://{}/{}", self.bucket, self.key)
    }
}

impl Serialize for S3Url<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

/// An object in the searched bucket.
///
/// Displays as `s3://bucket/key` with version details; serializes, usually
/// flattened into a record, as its `bucket`, `key`, `version_id` and `url`.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ObjectRef<'a> {
    pub(crate) bucket: &'a str,
    pub(crate) object: &'a StreamObject,
}

impl<'a> ObjectRef<'a> {
    pub(crate) fn new(bucket: &'a str, object: &'a StreamObject) -> Self {
        Self { bucket, object }
    }

    fn key(&self) -> &'a str {
        self.object.key().unwrap_or_default()
    }
}

impl fmt::Display for ObjectRef<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "s3://{}/{}", self.bucket, self.object.display_key())
    }
}

impl Serialize for ObjectRef<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let key = self.key();
        let mut map = serializer.serialize_map(None)?;
        map.serialize_entry("bucket", self.bucket)?;
        map.serialize_entry("key", key)?;
        if let Some(version_id) = &self.object.version_id {
            map.serialize_entry("version_id", version_id)?;
        }
        let url = S3Url {
            bucket: self.bucket,
            key,
        };
        map.serialize_entry("url", &url)?;
        map.end()
    }
}

/// An object's `s3://` URL for other programs: with `?versionId=` when listed
/// by version, but none of the labels `ls` adds. What `exec` passes for `{}`
/// and `ls --print0` prints.
pub(crate) struct ObjectUrl<'a> {
    pub(crate) bucket: &'a str,
    pub(crate) object: &'a StreamObject,
}

impl fmt::Display for ObjectUrl<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let key = self.object.key().unwrap_or_default();
        write!(f, "s3://{}/{key}", self.bucket)?;
        if let Some(version_id) = &self.object.version_id {
            write!(f, "?versionId={version_id}")?;
        }
        Ok(())
    }
}

/// Serializes tags as one JSON object, `{"key": "value", ...}`.
pub(crate) fn tag_map<S: Serializer>(tags: &[Tag], serializer: S) -> Result<S::Ok, S::Error> {
    serializer.collect_map(tags.iter().map(|tag| (tag.key(), tag.value())))
}

/// Formats tags as `key:value` pairs joined by commas.
pub(crate) struct TagList<'a>(pub(crate) &'a [Tag]);

impl fmt::Display for TagList<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, tag) in self.0.iter().enumerate() {
            if i > 0 {
                f.write_str(",")?;
            }
            write!(f, "{}:{}", tag.key(), tag.value())?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_s3::types::Object;

    #[derive(Serialize)]
    struct Example<'a> {
        action: &'static str,
        #[serde(flatten)]
        object: ObjectRef<'a>,
    }

    impl fmt::Display for Example<'_> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}: {}", self.action, self.object)
        }
    }

    fn written(record: &Example<'_>, json: bool, preview: bool) -> String {
        let mut out = Vec::new();
        write_record(&mut out, record, json, preview).unwrap();
        String::from_utf8(out).unwrap()
    }

    #[test]
    fn test_write_record_formats() {
        let mut object = StreamObject::from_object(Object::builder().key("dir/a b.txt").build());
        let record = Example {
            action: "delete",
            object: ObjectRef::new("bucket", &object),
        };

        assert_eq!(
            written(&record, false, false),
            "delete: s3://bucket/dir/a b.txt\n"
        );
        assert_eq!(
            written(&record, false, true),
            "[dry-run] delete: s3://bucket/dir/a b.txt\n"
        );
        assert_eq!(
            written(&record, true, false),
            r#"{"action":"delete","bucket":"bucket","key":"dir/a b.txt","url":"s3://bucket/dir/a b.txt"}"#
                .to_owned()
                + "\n"
        );

        object.version_id = Some("v1".to_owned());
        let record = Example {
            action: "delete",
            object: ObjectRef::new("bucket", &object),
        };
        assert_eq!(
            written(&record, true, true),
            r#"{"action":"delete","bucket":"bucket","key":"dir/a b.txt","version_id":"v1","url":"s3://bucket/dir/a b.txt","dry_run":true}"#
                .to_owned()
                + "\n"
        );
    }

    #[test]
    fn test_tags_format() {
        let tags = [
            Tag::builder().key("env").value("prod").build().unwrap(),
            Tag::builder().key("team").value("data").build().unwrap(),
        ];

        assert_eq!(TagList(&tags).to_string(), "env:prod,team:data");
        assert_eq!(TagList(&[]).to_string(), "");

        let json = tag_map(&tags, serde_json::value::Serializer).unwrap();
        assert_eq!(json, serde_json::json!({"env": "prod", "team": "data"}));
    }
}
