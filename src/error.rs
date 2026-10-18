use anyhow::Error as AnyhowError;
use thiserror::Error;

pub type S3FindResult<T> = Result<T, S3FindError>;

#[derive(Error, Debug)]
pub enum FunctionError {
    #[error("Invalid command line value")]
    CommandlineParse,
    #[error("Invalid path value")]
    ParentPathParse,
    #[error("Cannot parse filename")]
    FileNameParseError,
    #[error("S3 Object is not complete")]
    ObjectFieldError,
}

/// Why fetching an object's tags failed.
#[derive(Error, Debug)]
pub enum TagFetchError {
    #[error("Access denied for s3:GetObjectTagging on {bucket}/{key}")]
    AccessDenied { bucket: String, key: String },

    #[error("Object not found: {bucket}/{key}")]
    NotFound { bucket: String, key: String },

    #[error("S3 API error: {0}")]
    ApiError(String),
}

#[derive(Error, Debug)]
pub enum S3FindError {
    #[error("failed to list S3 objects: {source}")]
    ListObjects {
        #[source]
        source: AnyhowError,
    },
    #[error("failed to list S3 object versions: {source}")]
    ListObjectVersions {
        #[source]
        source: AnyhowError,
    },
    #[error("failed to execute command: {source:#}")]
    CommandExecution {
        #[source]
        source: AnyhowError,
    },
}

impl S3FindError {
    pub fn list_objects<E>(source: E) -> Self
    where
        E: Into<AnyhowError>,
    {
        Self::ListObjects {
            source: source.into(),
        }
    }

    pub fn list_object_versions<E>(source: E) -> Self
    where
        E: Into<AnyhowError>,
    {
        Self::ListObjectVersions {
            source: source.into(),
        }
    }

    pub fn command_execution<E>(source: E) -> Self
    where
        E: Into<AnyhowError>,
    {
        Self::CommandExecution {
            source: source.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use anyhow::anyhow;

    use super::{S3FindError, TagFetchError};

    #[test]
    fn test_display_includes_source_error() {
        assert_eq!(
            S3FindError::list_objects(anyhow!("list failed")).to_string(),
            "failed to list S3 objects: list failed"
        );
        assert_eq!(
            S3FindError::list_object_versions(anyhow!("version failed")).to_string(),
            "failed to list S3 object versions: version failed"
        );
        assert_eq!(
            S3FindError::command_execution(anyhow!("command failed")).to_string(),
            "failed to execute command: command failed"
        );
    }

    #[test]
    fn test_command_error_display_includes_context_chain() {
        assert_eq!(
            S3FindError::command_execution(anyhow!("AccessDenied").context("s3://bucket/key"))
                .to_string(),
            "failed to execute command: s3://bucket/key: AccessDenied"
        );
    }

    #[test]
    fn test_tag_fetch_error_display() {
        let err = TagFetchError::AccessDenied {
            bucket: "my-bucket".to_string(),
            key: "my-key".to_string(),
        };
        assert_eq!(
            err.to_string(),
            "Access denied for s3:GetObjectTagging on my-bucket/my-key"
        );

        let err = TagFetchError::NotFound {
            bucket: "bucket".to_string(),
            key: "key".to_string(),
        };
        assert_eq!(err.to_string(), "Object not found: bucket/key");

        let err = TagFetchError::ApiError("Custom error".to_string());
        assert_eq!(err.to_string(), "S3 API error: Custom error");
    }
}
