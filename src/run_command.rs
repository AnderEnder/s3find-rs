//! Commands run on each batch of matched objects.
//!
//! To add a command: add a `Cmd` variant and its argument struct in `arg.rs`,
//! implement [`RunCommand`] for that struct in a module here, and add the
//! variant to the `for_each_command!` list below.

use std::io::Write;
use std::pin::pin;
use std::process::ExitStatus;

use anyhow::{Context, Error};
use futures::{FutureExt, Stream, StreamExt, TryStreamExt, stream};

use crate::adapters::aws::S3Client;
use crate::arg::{Cmd, DoNothing, S3Path};
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

#[derive(Debug, PartialEq, Clone)]
pub struct ExecStatus {
    pub status: ExitStatus,
    pub runcommand: String,
}

/// Default for `--concurrency`.
pub const DEFAULT_CONCURRENCY: usize = 32;

/// What a command needs to run: S3 access, the searched path and an output sink.
pub struct CommandContext<'a, C> {
    pub client: &'a C,
    /// The path being searched; every object in a batch lives in `path.bucket`.
    pub path: &'a S3Path,
    /// Where results and progress go (stdout in the CLI).
    pub out: &'a mut (dyn Write + Send),
    /// Most requests to keep in flight for commands that make one per object.
    pub concurrency: usize,
}

impl<C: S3Client> CommandContext<'_, C> {
    /// Runs `request` on each object with up to `concurrency` in flight and
    /// writes the line each one returns, in input order.
    ///
    /// The first error stops the batch and is reported with the object it
    /// came from; requests already in flight are dropped.
    async fn for_each_object<'o, Fut>(
        &mut self,
        objects: impl Iterator<Item = &'o StreamObject> + Send + 'o,
        request: impl FnMut(&'o StreamObject) -> Fut + Send + 'o,
    ) -> Result<(), Error>
    where
        Fut: Future<Output = Result<String, Error>> + Send + 'o,
        Self: 'o,
    {
        let mut lines = pin!(in_order(
            &self.path.bucket,
            objects,
            self.concurrency,
            request
        ));
        while let Some(line) = lines.try_next().await? {
            writeln!(self.out, "{line}")?;
        }
        Ok(())
    }
}

/// Runs `request` on each object with up to `concurrency` in flight, yielding
/// the results in input order. Errors name the object they came from.
///
/// This is a plain fn returning an opaque `Send` stream because rustc can't
/// prove the same stream `Send` when it is built inline in an `async fn`
/// (rust-lang/rust#110338).
fn in_order<'o, T, Fut>(
    bucket: &'o str,
    objects: impl Iterator<Item = &'o StreamObject> + Send + 'o,
    concurrency: usize,
    mut request: impl FnMut(&'o StreamObject) -> Fut + Send + 'o,
) -> impl Stream<Item = Result<T, Error>> + Send + 'o
where
    T: Send + 'o,
    Fut: Future<Output = Result<T, Error>> + Send + 'o,
{
    stream::iter(objects)
        .map(move |object| {
            request(object).map(move |result| {
                result.with_context(|| format!("s3://{bucket}/{}", object.display_key()))
            })
        })
        .buffered(concurrency.max(1))
}

pub trait RunCommand: Send + Sync {
    /// Runs the command on one batch of matched objects, in traversal order.
    fn execute<C: S3Client>(
        &self,
        ctx: &mut CommandContext<'_, C>,
        objects: &[StreamObject],
    ) -> impl Future<Output = Result<(), Error>> + Send;

    /// Whether output may be block-buffered.
    ///
    /// Commands that only print (like `ls`) return `true` for much faster
    /// output on large listings. Commands that do slow work per object keep the
    /// default so each progress line shows up as soon as it is written.
    fn buffered_output(&self) -> bool {
        false
    }
}

/// Evaluates `$body` with `$command` bound to the arguments of whichever
/// command `$cmd` holds.
macro_rules! for_each_command {
    ($cmd:expr, $command:ident => $body:expr) => {
        match $cmd {
            Cmd::Exec($command) => $body,
            Cmd::Print($command) => $body,
            Cmd::Delete($command) => $body,
            Cmd::Download($command) => $body,
            Cmd::Copy($command) => $body,
            Cmd::Move($command) => $body,
            Cmd::Ls($command) => $body,
            Cmd::LsTags($command) => $body,
            Cmd::Tags($command) => $body,
            Cmd::Public($command) => $body,
            Cmd::Restore($command) => $body,
            Cmd::ChangeStorage($command) => $body,
            Cmd::Nothing($command) => $body,
        }
    };
}

impl RunCommand for Cmd {
    async fn execute<C: S3Client>(
        &self,
        ctx: &mut CommandContext<'_, C>,
        objects: &[StreamObject],
    ) -> Result<(), Error> {
        for_each_command!(self, command => command.execute(ctx, objects).await)
    }

    fn buffered_output(&self) -> bool {
        for_each_command!(self, command => command.buffered_output())
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
            format!("{bucket}/{encoded_key}?versionId={encoded_vid}")
        }
        None => format!("{bucket}/{encoded_key}"),
    }
}

impl RunCommand for DoNothing {
    async fn execute<C: S3Client>(
        &self,
        _: &mut CommandContext<'_, C>,
        _: &[StreamObject],
    ) -> Result<(), Error> {
        Ok(())
    }

    fn buffered_output(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests;
