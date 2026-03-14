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
    #[error("failed to list S3 objects")]
    ListObjects {
        #[source]
        source: AnyhowError,
    },
    #[error("failed to list S3 object versions")]
    ListObjectVersions {
        #[source]
        source: AnyhowError,
    },
    #[error("failed to execute command")]
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
