use std::pin::Pin;

use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Error;
use aws_smithy_runtime_api::http::Response;

use futures::Stream;

// Type aliases for cleaner signatures
type BoxedStream<'a, T> = Pin<Box<dyn Stream<Item = T> + Send + 'a>>;
type S3ListResult<T> = Result<T, SdkError<ListObjectsV2Error, Response>>;

// Delimiter used for hierarchical S3 listing
const S3_PATH_DELIMITER: &str = "/";

mod find_command;
mod model;
mod stats;
mod stream;

pub use find_command::FindCommand;
pub use model::StreamObject;
pub use stats::{FindStat, default_stats};
pub use stream::FindStream;

#[cfg(test)]
mod tests;
