use std::io::Write;

use anyhow::{Error, anyhow};
use tokio::process::Command;

use crate::adapters::aws::S3Client;
use crate::arg::Exec;
use crate::error::FunctionError;
use crate::object::StreamObject;

use super::{CommandContext, ExecStatus, RunCommand};

impl Exec {
    #[inline]
    pub(crate) async fn exec<I: Write + Send + ?Sized>(
        &self,
        io: &mut I,
        key: &str,
    ) -> Result<ExecStatus, Error> {
        let command_str = self.utility.replace("{}", key);
        let split = shlex::split(&command_str).ok_or(FunctionError::CommandlineParse)?;
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
                "" => format!("command `{command_str}` failed with status {status}"),
                stderr => format!("command `{command_str}` failed with status {status}: {stderr}"),
            };
            return Err(anyhow!(message));
        }

        writeln!(io, "{}", String::from_utf8_lossy_owned(output.stdout))?;

        Ok(ExecStatus {
            status: output.status,
            runcommand: command_str,
        })
    }
}

impl RunCommand for Exec {
    async fn execute<C: S3Client>(
        &self,
        ctx: &mut CommandContext<'_, C>,
        objects: &[StreamObject],
    ) -> Result<(), Error> {
        for object in objects {
            let s3_path = format!("s3://{}/{}", ctx.path.bucket, object.display_key());
            self.exec(ctx.out, &s3_path).await?;
        }
        Ok(())
    }
}
