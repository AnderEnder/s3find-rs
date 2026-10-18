//! Commands run on each batch of matched objects.
//!
//! To add a command: add a `Cmd` variant and its argument struct in `arg.rs`,
//! implement [`RunCommand`] for that struct in a module here, and map the
//! variant in [`Cmd::into_command`].

use std::io::Write;
use std::process::ExitStatus;

use anyhow::Error;
use async_trait::async_trait;

use crate::adapters::aws::S3Client;
use crate::arg::*;
use crate::object::StreamObject;

mod acl;
mod delete;
mod exec;
mod print;
mod restore;
mod tag;
mod transfer;

#[cfg(test)]
use self::acl::generate_s3_url;
#[cfg(test)]
use self::print::ObjectRecord;

impl Cmd {
    pub fn into_command(self) -> Box<dyn RunCommand> {
        match self {
            Cmd::Print(l) => Box::new(l),
            Cmd::Ls(l) => Box::new(l),
            Cmd::Exec(l) => Box::new(l),
            Cmd::Delete(l) => Box::new(l),
            Cmd::Download(l) => Box::new(l),
            Cmd::Tags(l) => Box::new(l),
            Cmd::LsTags(l) => Box::new(l),
            Cmd::Public(l) => Box::new(l),
            Cmd::Copy(l) => Box::new(l),
            Cmd::Move(l) => Box::new(l),
            Cmd::Nothing(l) => Box::new(l),
            Cmd::Restore(l) => Box::new(l),
            Cmd::ChangeStorage(l) => Box::new(l),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct ExecStatus {
    pub status: ExitStatus,
    pub runcommand: String,
}

/// What a command needs to run: S3 access, the searched path and an output sink.
pub struct CommandContext<'a> {
    pub client: &'a dyn S3Client,
    /// The path being searched; every object in a batch lives in `path.bucket`.
    pub path: &'a S3Path,
    /// Where results and progress go (stdout in the CLI).
    pub out: &'a mut (dyn Write + Send),
}

#[async_trait]
pub trait RunCommand: Send + Sync {
    /// Runs the command on one batch of matched objects, in traversal order.
    async fn execute(
        &self,
        ctx: &mut CommandContext<'_>,
        objects: &[StreamObject],
    ) -> Result<(), Error>;

    /// Whether output may be block-buffered.
    ///
    /// Commands that only print (like `ls`) return `true` for much faster
    /// output on large listings. Commands that do slow work per object keep the
    /// default so each progress line shows up as soon as it is written.
    fn buffered_output(&self) -> bool {
        false
    }
}

/// Objects that have content to act on; delete markers have no data, tags or ACL.
fn live_objects(objects: &[StreamObject]) -> impl Iterator<Item = &StreamObject> {
    objects.iter().filter(|object| !object.is_delete_marker)
}

/// Build a properly URL-encoded copy_source path for S3 CopyObject operations.
/// S3 keys can contain special characters that need encoding in the copy_source parameter.
fn build_copy_source(bucket: &str, key: &str, version_id: Option<&str>) -> String {
    let encoded_key = urlencoding::encode(key);
    match version_id {
        Some(vid) => {
            let encoded_vid = urlencoding::encode(vid);
            format!("{}/{}?versionId={}", bucket, encoded_key, encoded_vid)
        }
        None => format!("{}/{}", bucket, encoded_key),
    }
}

#[async_trait]
impl RunCommand for DoNothing {
    async fn execute(&self, _: &mut CommandContext<'_>, _: &[StreamObject]) -> Result<(), Error> {
        Ok(())
    }

    fn buffered_output(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests;
