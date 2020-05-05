use thiserror::Error;

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
