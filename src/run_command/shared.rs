/// Build a properly URL-encoded copy_source path for S3 CopyObject operations.
/// S3 keys can contain special characters that need encoding in the copy_source parameter.
pub(super) fn build_copy_source(bucket: &str, key: &str, version_id: Option<&str>) -> String {
    let encoded_key = urlencoding::encode(key);
    match version_id {
        Some(vid) => {
            let encoded_vid = urlencoding::encode(vid);
            format!("{}/{}?versionId={}", bucket, encoded_key, encoded_vid)
        }
        None => format!("{}/{}", bucket, encoded_key),
    }
}
