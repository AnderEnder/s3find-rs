use std::borrow::Cow;
use std::io::Write;

use anyhow::Error;
use aws_sdk_s3::primitives::DateTimeFormat;
use csv::WriterBuilder;
use serde::{Deserialize, Serialize};

use crate::adapters::aws::S3Client;
use crate::arg::{AdvancedPrint, FastPrint, PrintFormat};
use crate::object::StreamObject;

use super::report::S3Url;
use super::{CommandContext, RunCommand};

impl FastPrint {
    #[inline]
    pub(crate) fn print_stream_object<I: Write + ?Sized>(
        &self,
        io: &mut I,
        bucket: &str,
        stream_obj: &StreamObject,
    ) -> std::io::Result<()> {
        writeln!(io, "s3://{}/{}", bucket, stream_obj.display_key())
    }
}

impl RunCommand for FastPrint {
    async fn execute<C: S3Client>(
        &self,
        ctx: &mut CommandContext<'_, C>,
        objects: &[StreamObject],
    ) -> Result<(), Error> {
        let bucket = &ctx.path.bucket;
        for object in objects {
            if ctx.options.json {
                print_json_object(ctx.out, bucket, object)?;
            } else {
                self.print_stream_object(ctx.out, bucket, object)?;
            }
        }
        Ok(())
    }

    fn buffered_output(&self) -> bool {
        true
    }
}

/// One object in JSON output (`--json`, or `print --format json`): its bucket
/// and URL, followed by the same fields as in CSV.
#[derive(Serialize)]
struct JsonObject<'a> {
    bucket: &'a str,
    url: S3Url<'a>,
    #[serde(flatten)]
    record: ObjectRecord<'a>,
}

/// Writes one object as a line of JSON.
fn print_json_object<I: Write + ?Sized>(
    io: &mut I,
    bucket: &str,
    stream_obj: &StreamObject,
) -> Result<(), Error> {
    let record = JsonObject {
        bucket,
        url: S3Url {
            bucket,
            key: stream_obj.key().unwrap_or_default(),
        },
        record: ObjectRecord::from(stream_obj),
    };
    serde_json::to_writer(&mut *io, &record)?;
    writeln!(io)?;
    Ok(())
}

/// One object in `print --format json|csv` output. Strings borrow from the
/// listed object; deserializing (in tests) produces owned ones.
#[derive(Serialize, Deserialize)]
pub(crate) struct ObjectRecord<'a> {
    pub(crate) e_tag: Cow<'a, str>,
    pub(crate) owner: Cow<'a, str>,
    pub(crate) size: i64,
    pub(crate) last_modified: String,
    pub(crate) key: Cow<'a, str>,
    pub(crate) storage_class: Cow<'a, str>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub(crate) version_id: Option<Cow<'a, str>>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub(crate) is_latest: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub(crate) is_delete_marker: Option<bool>,
}

impl<'a> From<&'a StreamObject> for ObjectRecord<'a> {
    fn from(stream_obj: &'a StreamObject) -> Self {
        let object = &stream_obj.object;
        ObjectRecord {
            e_tag: object.e_tag().unwrap_or_default().into(),
            owner: object
                .owner()
                .and_then(|owner| owner.display_name())
                .unwrap_or_default()
                .into(),
            size: object.size.unwrap_or_default(),
            last_modified: object
                .last_modified
                .and_then(|x| x.fmt(DateTimeFormat::DateTime).ok())
                .unwrap_or_default(),
            key: object.key().unwrap_or_default().into(),
            storage_class: object
                .storage_class()
                .map(|class| class.as_str())
                .unwrap_or_default()
                .into(),
            version_id: stream_obj.version_id.as_deref().map(Cow::from),
            is_latest: stream_obj.is_latest,
            is_delete_marker: stream_obj.is_delete_marker.then_some(true),
        }
    }
}

impl AdvancedPrint {
    #[inline]
    pub(crate) fn print_stream_object<I: Write + ?Sized>(
        &self,
        io: &mut I,
        bucket: &str,
        stream_obj: &StreamObject,
    ) -> std::io::Result<()> {
        let object = &stream_obj.object;
        writeln!(
            io,
            "{0} {1} {2} {3} \"s3://{4}/{5}\" {6}",
            object.e_tag.as_deref().unwrap_or("NoEtag"),
            object
                .owner
                .as_ref()
                .and_then(|x| x.display_name.as_deref())
                .unwrap_or("None"),
            object.size.unwrap_or_default(),
            object
                .last_modified
                .and_then(|x| x.fmt(DateTimeFormat::DateTime).ok())
                .unwrap_or_else(|| "None".to_owned()),
            bucket,
            stream_obj.display_key(),
            object
                .storage_class
                .as_ref()
                .map(|x| x.as_str())
                .unwrap_or("NONE"),
        )
    }

    #[inline]
    pub(crate) fn print_json_stream_object<I: Write + ?Sized>(
        &self,
        io: &mut I,
        bucket: &str,
        stream_obj: &StreamObject,
    ) -> Result<(), Error> {
        print_json_object(io, bucket, stream_obj)
    }

    #[inline]
    pub(crate) fn print_csv_stream_objects<I: Write + ?Sized>(
        &self,
        io: &mut I,
        stream_objects: &[StreamObject],
    ) -> Result<(), Error> {
        let mut wtr = WriterBuilder::new().has_headers(false).from_writer(io);
        for stream_obj in stream_objects {
            wtr.serialize(ObjectRecord::from(stream_obj))?;
        }
        wtr.flush()?;
        Ok(())
    }
}

impl RunCommand for AdvancedPrint {
    async fn execute<C: S3Client>(
        &self,
        ctx: &mut CommandContext<'_, C>,
        objects: &[StreamObject],
    ) -> Result<(), Error> {
        let bucket = &ctx.path.bucket;
        // --json overrides --format.
        match (ctx.options.json, &self.format) {
            (true, _) | (false, PrintFormat::Json) => {
                for object in objects {
                    self.print_json_stream_object(ctx.out, bucket, object)?;
                }
            }
            (false, PrintFormat::Text) => {
                for object in objects {
                    self.print_stream_object(ctx.out, bucket, object)?;
                }
            }
            (false, PrintFormat::Csv) => {
                self.print_csv_stream_objects(ctx.out, objects)?;
            }
        }
        Ok(())
    }

    fn buffered_output(&self) -> bool {
        true
    }
}
