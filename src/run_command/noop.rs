use anyhow::Error;
use async_trait::async_trait;

use crate::adapters::aws::CommandS3Client;
use crate::arg::{DoNothing, S3Path};
use crate::command::StreamObject;

use super::RunCommand;

#[async_trait]
impl RunCommand for DoNothing {
    async fn execute(
        &self,
        _c: &dyn CommandS3Client,
        _p: &S3Path,
        _l: &[StreamObject],
    ) -> Result<(), Error> {
        Ok(())
    }
}
