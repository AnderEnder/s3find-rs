use std::io::Write;
use std::process::Command;

use anyhow::{Error, anyhow};
use async_trait::async_trait;

use crate::adapters::aws::CommandS3Client;
use crate::arg::{Exec, S3Path};
use crate::command::StreamObject;
use crate::error::FunctionError;

use super::{ExecStatus, RunCommand};

impl Exec {
    #[inline]
    pub(crate) fn exec<I: Write>(&self, io: &mut I, key: &str) -> Result<ExecStatus, Error> {
        let command_str = self.utility.replace("{}", key);
        let split = shlex::split(&command_str).ok_or(FunctionError::CommandlineParse)?;
        let (command_name, command_args) =
            split.split_first().ok_or(FunctionError::CommandlineParse)?;

        let mut command = Command::new(command_name);
        command.args(command_args);

        let output = command.output()?;
        if !output.status.success() {
            let status = output
                .status
                .code()
                .map(|code| code.to_string())
                .unwrap_or_else(|| "terminated by signal".to_string());
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            let message = if stderr.is_empty() {
                format!("command `{command_str}` failed with status {status}")
            } else {
                format!("command `{command_str}` failed with status {status}: {stderr}")
            };
            return Err(anyhow!(message));
        }

        let output_str = String::from_utf8_lossy(&output.stdout);
        writeln!(io, "{}", &output_str)?;

        Ok(ExecStatus {
            status: output.status,
            runcommand: command_str,
        })
    }
}

#[async_trait]
impl RunCommand for Exec {
    async fn execute(
        &self,
        _: &dyn CommandS3Client,
        path: &S3Path,
        list: &[StreamObject],
    ) -> Result<(), Error> {
        let mut stdout = std::io::stdout();
        for x in list {
            let s3_path = format!("s3://{}/{}", &path.bucket, x.display_key());
            self.exec(&mut stdout, &s3_path)?;
        }
        Ok(())
    }
}
