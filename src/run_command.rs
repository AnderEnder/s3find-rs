use std::process::ExitStatus;

use anyhow::Error;
use async_trait::async_trait;

use crate::adapters::aws::CommandS3Client;
use crate::arg::*;
use crate::command::StreamObject;

mod acl;
mod delete;
mod exec;
mod noop;
mod print;
mod restore;
mod shared;
mod tag;
mod transfer;

#[cfg(test)]
use self::acl::generate_s3_url;
#[cfg(test)]
use self::print::ObjectRecord;

impl Cmd {
    pub fn downcast(self) -> Box<dyn RunCommand> {
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

#[async_trait]
pub trait RunCommand {
    async fn execute(
        &self,
        client: &dyn CommandS3Client,
        path: &S3Path,
        list: &[StreamObject],
    ) -> Result<(), Error>;
}

#[cfg(test)]
mod tests;
