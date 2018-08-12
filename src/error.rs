#[derive(Fail, Debug)]
pub enum FunctionError {
    #[fail(display = "Invalid command line value")]
    CommandlineParse,
    #[fail(display = "Invalid path value")]
    ParentPathParse,
    #[fail(display = "Cannot parse tag")]
    TagParseError,
    #[fail(display = "Cannot parse tag key")]
    TagKeyParseError,
    #[fail(display = "Cannot parse tag value")]
    TagValueParseError,
    #[fail(display = "Cannot parse filename")]
    FileNameParseError,
    #[fail(display = "Cannot fetch body from s3 response")]
    S3FetchBodyError,
    #[fail(display = "File is already present")]
    PresentFileError,
}
