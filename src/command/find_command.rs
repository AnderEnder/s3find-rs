use aws_sdk_s3::Client;
use aws_sdk_s3::types::Object;

use crate::arg::{Cmd, FindOpt, S3Path};
use crate::error::{S3FindError, S3FindResult};
use crate::run_command::RunCommand;

use super::{FindStat, StreamObject};

pub struct FindCommand {
    pub client: Client,
    pub path: S3Path,
    pub command: Box<dyn RunCommand>,
}

impl FindCommand {
    pub fn new(cmd: Option<Cmd>, path: S3Path, client: Client) -> Self {
        let command = cmd.unwrap_or_default().downcast();

        FindCommand {
            client,
            path,
            command,
        }
    }

    pub async fn exec(
        &self,
        acc: Option<FindStat>,
        list: Vec<StreamObject>,
    ) -> S3FindResult<Option<FindStat>> {
        let objects: Vec<Object> = list.iter().map(|so| so.object.clone()).collect();
        let status = acc.map(|stat| stat + &objects);

        self.command
            .execute(&self.client, &self.path, &list)
            .await
            .map_err(S3FindError::command_execution)?;
        Ok(status)
    }

    pub fn from_opts(opts: &FindOpt, client: Client) -> FindCommand {
        let FindOpt { path, cmd, .. } = opts;
        let path = S3Path { ..path.clone() };
        FindCommand::new(cmd.clone(), path, client)
    }
}
