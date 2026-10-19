use std::fmt;

use anyhow::{Error, anyhow};
use serde::Serialize;
use tokio::process::Command;

use crate::adapters::aws::S3Client;
use crate::arg::Exec;
use crate::error::FunctionError;
use crate::object::StreamObject;

use super::report::{ObjectRef, ObjectUrl};
use super::{CommandContext, RunCommand};

/// A program run for an object; `stdout` is `None` in a dry run.
#[derive(Serialize)]
struct Executed<'a> {
    action: &'static str,
    #[serde(flatten)]
    object: ObjectRef<'a>,
    command: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    stdout: Option<String>,
}

impl fmt::Display for Executed<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.stdout.as_deref().unwrap_or(&self.command))
    }
}

impl Exec {
    /// The program and arguments to run for `target`: the utility split into
    /// words like a shell would, then `{}` replaced in each word, so a target
    /// with spaces or quotes stays inside its word.
    pub(crate) fn command(&self, target: &str) -> Result<Vec<String>, Error> {
        let words = shlex::split(&self.utility).ok_or(FunctionError::CommandlineParse)?;
        if words.is_empty() {
            return Err(FunctionError::CommandlineParse.into());
        }
        Ok(words
            .iter()
            .map(|word| word.replace("{}", target))
            .collect())
    }
}

/// `command` as a shell would need it typed, for output and errors.
fn command_line(command: &[String]) -> String {
    shlex::try_join(command.iter().map(String::as_str)).unwrap_or_else(|_| command.join(" "))
}

/// Runs a program and returns what it wrote to stdout. A non-zero exit is an
/// error that includes what it wrote to stderr.
pub(crate) async fn run(command: &[String]) -> Result<String, Error> {
    let (program, args) = command
        .split_first()
        .ok_or(FunctionError::CommandlineParse)?;

    let output = Command::new(program).args(args).output().await?;
    if !output.status.success() {
        let command_line = command_line(command);
        let status = output
            .status
            .code()
            .map(|code| code.to_string())
            .unwrap_or_else(|| "terminated by signal".to_owned());
        let stderr = String::from_utf8_lossy(&output.stderr);
        let message = match stderr.trim() {
            "" => format!("command `{command_line}` failed with status {status}"),
            stderr => format!("command `{command_line}` failed with status {status}: {stderr}"),
        };
        return Err(anyhow!(message));
    }

    Ok(String::from_utf8_lossy_owned(output.stdout))
}

impl RunCommand for Exec {
    async fn execute<C: S3Client>(
        &self,
        ctx: &mut CommandContext<'_, C>,
        objects: &[StreamObject],
    ) -> Result<(), Error> {
        // One at a time, like `find -exec`: the utility may not be safe to run
        // concurrently.
        for object in objects {
            if ctx.is_interrupted() {
                break;
            }
            let target = ObjectUrl {
                bucket: &ctx.path.bucket,
                object,
            };
            let command = self.command(&target.to_string())?;
            let stdout = if ctx.options.dry_run {
                None
            } else {
                Some(run(&command).await?)
            };
            ctx.report(&Executed {
                action: "exec",
                object: ObjectRef::new(&ctx.path.bucket, object),
                command: command_line(&command),
                stdout,
            })?;
        }
        Ok(())
    }
}
