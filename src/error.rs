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
    #[error("Cannot convert path to string")]
    PathConverError,
    #[error("Cannot fetch body from s3 response")]
    S3FetchBodyError,
    #[error("File is already present")]
    PresentFileError,
    #[error("S3 Object is not complete")]
    ObjectFieldError,
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
    #[error("failed to execute command: {source}")]
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

    use super::S3FindError;

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
}
