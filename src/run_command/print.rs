use std::io::Write;

use anyhow::Error;
use async_trait::async_trait;
use aws_smithy_types::date_time::Format;
use csv::WriterBuilder;
use serde::{Deserialize, Serialize};

use crate::adapters::aws::CommandS3Client;
use crate::arg::{AdvancedPrint, FastPrint, PrintFormat, S3Path};
use crate::command::StreamObject;

use super::RunCommand;

impl FastPrint {
    #[inline]
    pub(crate) fn print_stream_object<I: Write>(
        &self,
        io: &mut I,
        bucket: &str,
        stream_obj: &StreamObject,
    ) -> std::io::Result<()> {
        writeln!(io, "s3://{}/{}", bucket, stream_obj.display_key())
    }
}

#[async_trait]
impl RunCommand for FastPrint {
    async fn execute(
        &self,
        _c: &dyn CommandS3Client,
        path: &S3Path,
        list: &[StreamObject],
    ) -> Result<(), Error> {
        let mut stdout = std::io::stdout();
        for x in list {
            self.print_stream_object(&mut stdout, &path.bucket, x)?
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
pub(crate) struct ObjectRecord {
    pub(crate) e_tag: String,
    pub(crate) owner: String,
    pub(crate) size: i64,
    pub(crate) last_modified: String,
    pub(crate) key: String,
    pub(crate) storage_class: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub(crate) version_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub(crate) is_latest: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub(crate) is_delete_marker: Option<bool>,
}

impl From<&StreamObject> for ObjectRecord {
    fn from(stream_obj: &StreamObject) -> Self {
        let object = &stream_obj.object;
        ObjectRecord {
            e_tag: object.e_tag.clone().unwrap_or_default(),
            owner: object
                .owner
                .clone()
                .and_then(|x| x.display_name.clone())
                .unwrap_or_default(),
            size: object.size.unwrap_or_default(),
            last_modified: object
                .last_modified
                .and_then(|x| x.fmt(Format::DateTime).ok())
                .unwrap_or_default(),
            key: object.key.clone().unwrap_or_default(),
            storage_class: object
                .storage_class
                .clone()
                .map(|x| x.to_string())
                .unwrap_or_default(),
            version_id: stream_obj.version_id.clone(),
            is_latest: stream_obj.is_latest,
            is_delete_marker: if stream_obj.is_delete_marker {
                Some(true)
            } else {
                None
            },
        }
    }
}

impl AdvancedPrint {
    #[inline]
    pub(crate) fn print_stream_object<I: Write>(
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
                .and_then(|x| x.fmt(Format::DateTime).ok())
                .unwrap_or("None".to_string()),
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
    pub(crate) fn print_json_stream_object<I: Write>(
        &self,
        io: &mut I,
        stream_obj: &StreamObject,
    ) -> Result<(), Error> {
        let record: ObjectRecord = stream_obj.into();
        writeln!(io, "{}", serde_json::to_string(&record)?)?;
        Ok(())
    }

    #[inline]
    pub(crate) fn print_csv_stream_objects<I: Write>(
        &self,
        io: &mut I,
        stream_objects: &[StreamObject],
    ) -> Result<(), Error> {
        let mut wtr = WriterBuilder::new().has_headers(false).from_writer(io);
        for stream_obj in stream_objects {
            let record: ObjectRecord = stream_obj.into();
            wtr.serialize(record)?;
        }
        wtr.flush()?;
        Ok(())
    }
}

#[async_trait]
impl RunCommand for AdvancedPrint {
    async fn execute(
        &self,
        _c: &dyn CommandS3Client,
        path: &S3Path,
        list: &[StreamObject],
    ) -> Result<(), Error> {
        let mut stdout = std::io::stdout();

        match self.format {
            PrintFormat::Json => {
                for x in list {
                    self.print_json_stream_object(&mut stdout, x)?;
                }
            }
            PrintFormat::Text => {
                for x in list {
                    self.print_stream_object(&mut stdout, &path.bucket, x)?;
                }
            }
            PrintFormat::Csv => {
                self.print_csv_stream_objects(&mut stdout, list)?;
            }
        }
        Ok(())
    }
}
