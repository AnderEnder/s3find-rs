use std::io::Write;
use std::process::Command;

use anyhow::Error;
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
        let split: Vec<_> = command_str.split(' ').collect();

        let (command_name, command_args) = match &*split {
            [command_name, command_args @ ..] => (*command_name, command_args),
            _ => return Err(FunctionError::CommandlineParse.into()),
        };

        let mut command = Command::new(command_name);
        for arg in command_args {
            command.arg(arg);
        }

        let output = command.output()?;
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
