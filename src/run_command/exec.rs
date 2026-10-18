use std::fmt;

use anyhow::{Error, anyhow};
use serde::Serialize;
use tokio::process::Command;

use crate::adapters::aws::S3Client;
use crate::arg::Exec;
use crate::error::FunctionError;
use crate::object::StreamObject;

use super::report::ObjectRef;
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
    /// The utility's command line with `{}` replaced by `target`.
    pub(crate) fn command_line(&self, target: &str) -> String {
        self.utility.replace("{}", target)
    }
}

/// Runs a command line and returns what it wrote to stdout. A non-zero exit
/// is an error that includes what it wrote to stderr.
pub(crate) async fn run(command_line: &str) -> Result<String, Error> {
    let split = shlex::split(command_line).ok_or(FunctionError::CommandlineParse)?;
    let (command_name, command_args) =
        split.split_first().ok_or(FunctionError::CommandlineParse)?;

    let output = Command::new(command_name)
        .args(command_args)
        .output()
        .await?;
    if !output.status.success() {
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
            let object = ObjectRef::new(&ctx.path.bucket, object);
            let command = self.command_line(&object.to_string());
            let stdout = if ctx.options.dry_run {
                None
            } else {
                Some(run(&command).await?)
            };
            ctx.report(&Executed {
                action: "exec",
                object,
                command,
                stdout,
            })?;
        }
        Ok(())
    }
}
