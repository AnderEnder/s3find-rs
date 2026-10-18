use std::collections::HashMap;
use std::fmt;
use std::ops::RangeInclusive;

use anyhow::{Error, anyhow};
use aws_config::{BehaviorVersion, Region, meta::credentials::CredentialsProviderChain};
use aws_sdk_s3::{
    Client,
    config::Credentials,
    error::SdkError,
    operation::get_object_tagging::GetObjectTaggingError,
    primitives::{ByteStream, DateTime, DateTimeFormat},
    types::{
        CompletedMultipartUpload, CompletedPart, Delete, DeleteMarkerEntry, GlacierJobParameters,
        MetadataDirective, Object, ObjectCannedAcl, ObjectIdentifier, ObjectStorageClass,
        ObjectVersion, RestoreRequest, StorageClass, Tag, Tagging, Tier,
    },
};

use crate::arg::{self, FindOpt, RestoreTier, StorageClassName};
use crate::error::TagFetchError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteObjectRequest {
    pub key: String,
    pub version_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeletedObjectInfo {
    pub key: Option<String>,
    pub version_id: Option<String>,
}

/// What one DeleteObjects call did: S3 deletes the keys it can and reports
/// the rest.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DeleteOutcome {
    pub deleted: Vec<DeletedObjectInfo>,
    pub failed: Vec<DeleteFailure>,
}

impl DeleteOutcome {
    /// An error naming every key that wasn't deleted, if any.
    pub fn check(&self) -> Result<(), Error> {
        if self.failed.is_empty() {
            return Ok(());
        }
        let details = self
            .failed
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        Err(anyhow!(
            "failed to delete {} of the objects: {details}",
            self.failed.len()
        ))
    }
}

/// A key S3 didn't delete, and why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteFailure {
    pub key: String,
    pub version_id: Option<String>,
    pub code: String,
    pub message: String,
}

impl fmt::Display for DeleteFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.key)?;
        if let Some(version_id) = &self.version_id {
            write!(f, "?versionId={version_id}")?;
        }
        write!(f, ": {} ({})", self.code, self.message)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RestoreObjectStatus {
    Started,
    AlreadyInProgress,
    InvalidObjectState,
}

/// What a multipart copy carries over from its source. CopyObject copies
/// these itself; a multipart upload has to be given them.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ObjectHead {
    pub size: i64,
    /// Every part is copied only if the source still has this ETag, so a
    /// source that changes midway fails the copy instead of mixing versions.
    pub e_tag: Option<String>,
    pub cache_control: Option<String>,
    pub content_disposition: Option<String>,
    pub content_encoding: Option<String>,
    pub content_language: Option<String>,
    pub content_type: Option<String>,
    pub expires: Option<DateTime>,
    pub website_redirect_location: Option<String>,
    pub metadata: HashMap<String, String>,
}

/// A multipart upload in progress.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MultipartUpload<'a> {
    pub bucket: &'a str,
    pub key: &'a str,
    pub upload_id: &'a str,
}

/// One UploadPartCopy call: bytes `range` of `copy_source` as part
/// `part_number`, if the source still has `source_e_tag`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartCopy<'a> {
    pub part_number: i32,
    pub copy_source: &'a str,
    pub range: RangeInclusive<i64>,
    pub source_e_tag: Option<&'a str>,
}

/// One ListObjectsV2 call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListObjectsRequest<'a> {
    pub bucket: &'a str,
    pub prefix: &'a str,
    /// Groups keys sharing a prefix up to the delimiter into `common_prefixes`.
    pub delimiter: Option<&'a str>,
    pub continuation_token: Option<&'a str>,
    pub max_keys: i32,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ObjectsPage {
    pub objects: Vec<Object>,
    pub common_prefixes: Vec<String>,
    /// Token for the next page, `None` on the last page.
    pub next_continuation_token: Option<String>,
}

/// One ListObjectVersions call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListVersionsRequest<'a> {
    pub bucket: &'a str,
    pub prefix: &'a str,
    pub marker: Option<&'a VersionMarker>,
    pub max_keys: i32,
}

/// Position to resume a version listing from.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VersionMarker {
    pub key_marker: String,
    pub version_id_marker: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct VersionsPage {
    pub versions: Vec<ObjectVersion>,
    pub delete_markers: Vec<DeleteMarkerEntry>,
    /// Where the next page starts, `None` on the last page.
    pub next_marker: Option<VersionMarker>,
}

impl From<&StorageClassName> for StorageClass {
    fn from(name: &StorageClassName) -> Self {
        Self::from(name.as_str())
    }
}

impl From<&StorageClassName> for ObjectStorageClass {
    fn from(name: &StorageClassName) -> Self {
        Self::from(name.as_str())
    }
}

impl From<RestoreTier> for Tier {
    fn from(tier: RestoreTier) -> Self {
        match tier {
            RestoreTier::Standard => Self::Standard,
            RestoreTier::Bulk => Self::Bulk,
            RestoreTier::Expedited => Self::Expedited,
        }
    }
}

/// The S3 operations s3find uses.
///
/// Listing, tag fetching and commands all go through this trait, so every layer
/// can be tested against an in-memory fake instead of a real or replayed S3.
/// Implementations write these as plain `async fn`s; the trait spells out the
/// returned futures so callers can rely on them being `Send`.
pub trait S3Client: Send + Sync {
    fn region(&self) -> Option<String>;

    fn list_objects(
        &self,
        request: ListObjectsRequest<'_>,
    ) -> impl Future<Output = Result<ObjectsPage, Error>> + Send;

    fn list_object_versions(
        &self,
        request: ListVersionsRequest<'_>,
    ) -> impl Future<Output = Result<VersionsPage, Error>> + Send;

    /// Deletes up to 1000 keys. Keys S3 refuses to delete are reported in the
    /// outcome, not as an error.
    fn delete_objects(
        &self,
        bucket: &str,
        objects: Vec<DeleteObjectRequest>,
    ) -> impl Future<Output = Result<DeleteOutcome, Error>> + Send;

    fn put_object_tagging(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        tags: Vec<Tag>,
    ) -> impl Future<Output = Result<(), Error>> + Send;

    fn get_object_tagging(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> impl Future<Output = Result<Vec<Tag>, TagFetchError>> + Send;

    fn put_object_acl_public_read(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> impl Future<Output = Result<(), Error>> + Send;

    fn get_object(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> impl Future<Output = Result<ByteStream, Error>> + Send;

    fn copy_object(
        &self,
        bucket: &str,
        key: &str,
        copy_source: &str,
        storage_class: Option<StorageClass>,
        metadata_directive: Option<MetadataDirective>,
    ) -> impl Future<Output = Result<(), Error>> + Send;

    fn restore_object(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        days: i32,
        tier: Tier,
    ) -> impl Future<Output = Result<RestoreObjectStatus, Error>> + Send;

    fn head_object(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> impl Future<Output = Result<ObjectHead, Error>> + Send;

    /// Starts a multipart upload with `source`'s metadata and `tags`, and
    /// returns its upload id.
    fn create_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        source: &ObjectHead,
        storage_class: Option<StorageClass>,
        tags: &[Tag],
    ) -> impl Future<Output = Result<String, Error>> + Send;

    fn upload_part_copy(
        &self,
        upload: MultipartUpload<'_>,
        part: PartCopy<'_>,
    ) -> impl Future<Output = Result<CompletedPart, Error>> + Send;

    fn complete_multipart_upload(
        &self,
        upload: MultipartUpload<'_>,
        parts: Vec<CompletedPart>,
    ) -> impl Future<Output = Result<(), Error>> + Send;

    fn abort_multipart_upload(
        &self,
        upload: MultipartUpload<'_>,
    ) -> impl Future<Output = Result<(), Error>> + Send;
}

/// Converts an SDK error into one whose message includes its causes, such as
/// the S3 error code, instead of the SDK's bare "service error".
fn sdk_error<E, R>(err: SdkError<E, R>) -> Error
where
    E: std::error::Error + Send + Sync + 'static,
    R: std::fmt::Debug + Send + Sync + 'static,
{
    anyhow!(error_chain(&err))
}

/// An error's message followed by those of its sources, joined by ": ".
fn error_chain(err: &dyn std::error::Error) -> String {
    let mut message = err.to_string();
    let mut source = err.source();
    while let Some(cause) = source {
        let cause_message = cause.to_string();
        if !message.ends_with(&cause_message) {
            message.push_str(": ");
            message.push_str(&cause_message);
        }
        source = cause.source();
    }
    message
}

fn classify_tagging_error(
    err: SdkError<GetObjectTaggingError>,
    bucket: &str,
    key: &str,
) -> TagFetchError {
    let status = match &err {
        SdkError::ServiceError(service_err) => service_err.raw().status().as_u16(),
        _ => return TagFetchError::ApiError(error_chain(&err)),
    };

    match status {
        403 => TagFetchError::AccessDenied {
            bucket: bucket.to_string(),
            key: key.to_string(),
        },
        404 => TagFetchError::NotFound {
            bucket: bucket.to_string(),
            key: key.to_string(),
        },
        _ => TagFetchError::ApiError(format!("HTTP {status}: {}", error_chain(&err))),
    }
}

impl S3Client for Client {
    fn region(&self) -> Option<String> {
        self.config()
            .region()
            .map(|region| region.as_ref().to_string())
    }

    async fn list_objects(&self, request: ListObjectsRequest<'_>) -> Result<ObjectsPage, Error> {
        let output = self
            .list_objects_v2()
            .bucket(request.bucket)
            .prefix(request.prefix)
            .set_delimiter(request.delimiter.map(str::to_owned))
            .set_continuation_token(request.continuation_token.map(str::to_owned))
            .max_keys(request.max_keys)
            .send()
            .await
            .map_err(sdk_error)?;

        Ok(ObjectsPage {
            objects: output.contents.unwrap_or_default(),
            common_prefixes: output
                .common_prefixes
                .unwrap_or_default()
                .into_iter()
                .filter_map(|common_prefix| common_prefix.prefix)
                .collect(),
            next_continuation_token: output.next_continuation_token,
        })
    }

    async fn list_object_versions(
        &self,
        request: ListVersionsRequest<'_>,
    ) -> Result<VersionsPage, Error> {
        let output = self
            .list_object_versions()
            .bucket(request.bucket)
            .prefix(request.prefix)
            .max_keys(request.max_keys)
            .set_key_marker(request.marker.map(|marker| marker.key_marker.clone()))
            .set_version_id_marker(
                request
                    .marker
                    .and_then(|marker| marker.version_id_marker.clone()),
            )
            .send()
            .await
            .map_err(sdk_error)?;

        // A truncated page without a key marker cannot be resumed; treat it as
        // the last page rather than restarting the listing forever.
        let next_marker = match (output.is_truncated, output.next_key_marker) {
            (Some(true), Some(key_marker)) => Some(VersionMarker {
                key_marker,
                version_id_marker: output.next_version_id_marker,
            }),
            _ => None,
        };

        Ok(VersionsPage {
            versions: output.versions.unwrap_or_default(),
            delete_markers: output.delete_markers.unwrap_or_default(),
            next_marker,
        })
    }

    async fn delete_objects(
        &self,
        bucket: &str,
        objects: Vec<DeleteObjectRequest>,
    ) -> Result<DeleteOutcome, Error> {
        if objects.is_empty() {
            return Ok(DeleteOutcome::default());
        }

        let objects = objects
            .into_iter()
            .map(|object| {
                ObjectIdentifier::builder()
                    .key(object.key)
                    .set_version_id(object.version_id)
                    .build()
            })
            .collect::<Result<Vec<_>, _>>()?;

        let delete = Delete::builder().set_objects(Some(objects)).build()?;

        let response = self
            .delete_objects()
            .bucket(bucket)
            .delete(delete)
            .send()
            .await
            .map_err(sdk_error)?;

        let deleted = response
            .deleted
            .unwrap_or_default()
            .into_iter()
            .map(|object| DeletedObjectInfo {
                key: object.key,
                version_id: object.version_id,
            })
            .collect();
        let failed = response
            .errors
            .unwrap_or_default()
            .into_iter()
            .map(|error| DeleteFailure {
                key: error.key.unwrap_or_else(|| "<unknown>".to_owned()),
                version_id: error.version_id,
                code: error.code.unwrap_or_else(|| "Unknown".to_owned()),
                message: error
                    .message
                    .unwrap_or_else(|| "unknown delete error".to_owned()),
            })
            .collect();
        Ok(DeleteOutcome { deleted, failed })
    }

    async fn put_object_tagging(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        tags: Vec<Tag>,
    ) -> Result<(), Error> {
        let tagging = Tagging::builder().set_tag_set(Some(tags)).build()?;

        self.put_object_tagging()
            .bucket(bucket)
            .key(key)
            .set_version_id(version_id.map(str::to_owned))
            .tagging(tagging)
            .send()
            .await
            .map_err(sdk_error)?;
        Ok(())
    }

    async fn get_object_tagging(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> Result<Vec<Tag>, TagFetchError> {
        let output = self
            .get_object_tagging()
            .bucket(bucket)
            .key(key)
            .set_version_id(version_id.map(str::to_owned))
            .send()
            .await
            .map_err(|err| classify_tagging_error(err, bucket, key))?;

        Ok(output.tag_set)
    }

    async fn put_object_acl_public_read(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> Result<(), Error> {
        self.put_object_acl()
            .bucket(bucket)
            .key(key)
            .set_version_id(version_id.map(str::to_owned))
            .acl(ObjectCannedAcl::PublicRead)
            .send()
            .await
            .map_err(sdk_error)?;
        Ok(())
    }

    async fn get_object(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> Result<ByteStream, Error> {
        let output = self
            .get_object()
            .bucket(bucket)
            .key(key)
            .set_version_id(version_id.map(str::to_owned))
            .send()
            .await
            .map_err(sdk_error)?;

        Ok(output.body)
    }

    async fn copy_object(
        &self,
        bucket: &str,
        key: &str,
        copy_source: &str,
        storage_class: Option<StorageClass>,
        metadata_directive: Option<MetadataDirective>,
    ) -> Result<(), Error> {
        self.copy_object()
            .bucket(bucket)
            .key(key)
            .copy_source(copy_source)
            .set_storage_class(storage_class)
            .set_metadata_directive(metadata_directive)
            .send()
            .await
            .map_err(sdk_error)?;
        Ok(())
    }

    async fn restore_object(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        days: i32,
        tier: Tier,
    ) -> Result<RestoreObjectStatus, Error> {
        let restore_request = RestoreRequest::builder()
            .days(days)
            .glacier_job_parameters(GlacierJobParameters::builder().tier(tier).build()?)
            .build();

        let result = self
            .restore_object()
            .bucket(bucket)
            .key(key)
            .set_version_id(version_id.map(str::to_owned))
            .restore_request(restore_request)
            .send()
            .await;

        match result {
            Ok(_) => Ok(RestoreObjectStatus::Started),
            Err(SdkError::ServiceError(err))
                if err.err().meta().code() == Some("RestoreAlreadyInProgress") =>
            {
                Ok(RestoreObjectStatus::AlreadyInProgress)
            }
            Err(SdkError::ServiceError(err))
                if err.err().meta().code() == Some("InvalidObjectState") =>
            {
                Ok(RestoreObjectStatus::InvalidObjectState)
            }
            Err(err) => Err(sdk_error(err)),
        }
    }

    async fn head_object(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> Result<ObjectHead, Error> {
        let output = self
            .head_object()
            .bucket(bucket)
            .key(key)
            .set_version_id(version_id.map(str::to_owned))
            .send()
            .await
            .map_err(sdk_error)?;

        let expires = output
            .expires_string
            .as_deref()
            .and_then(|value| DateTime::from_str(value, DateTimeFormat::HttpDate).ok());
        Ok(ObjectHead {
            size: output.content_length.unwrap_or_default(),
            e_tag: output.e_tag,
            cache_control: output.cache_control,
            content_disposition: output.content_disposition,
            content_encoding: output.content_encoding,
            content_language: output.content_language,
            content_type: output.content_type,
            expires,
            website_redirect_location: output.website_redirect_location,
            metadata: output.metadata.unwrap_or_default(),
        })
    }

    async fn create_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        source: &ObjectHead,
        storage_class: Option<StorageClass>,
        tags: &[Tag],
    ) -> Result<String, Error> {
        let tagging = tags
            .iter()
            .map(|tag| {
                format!(
                    "{}={}",
                    urlencoding::encode(tag.key()),
                    urlencoding::encode(tag.value())
                )
            })
            .collect::<Vec<_>>()
            .join("&");

        let output = self
            .create_multipart_upload()
            .bucket(bucket)
            .key(key)
            .set_storage_class(storage_class)
            .set_cache_control(source.cache_control.clone())
            .set_content_disposition(source.content_disposition.clone())
            .set_content_encoding(source.content_encoding.clone())
            .set_content_language(source.content_language.clone())
            .set_content_type(source.content_type.clone())
            .set_expires(source.expires)
            .set_website_redirect_location(source.website_redirect_location.clone())
            .set_metadata((!source.metadata.is_empty()).then(|| source.metadata.clone()))
            .set_tagging((!tagging.is_empty()).then_some(tagging))
            .send()
            .await
            .map_err(sdk_error)?;

        output
            .upload_id
            .ok_or_else(|| anyhow!("CreateMultipartUpload returned no upload id"))
    }

    async fn upload_part_copy(
        &self,
        upload: MultipartUpload<'_>,
        part: PartCopy<'_>,
    ) -> Result<CompletedPart, Error> {
        let output = self
            .upload_part_copy()
            .bucket(upload.bucket)
            .key(upload.key)
            .upload_id(upload.upload_id)
            .part_number(part.part_number)
            .copy_source(part.copy_source)
            .copy_source_range(format!("bytes={}-{}", part.range.start(), part.range.end()))
            .set_copy_source_if_match(part.source_e_tag.map(str::to_owned))
            .send()
            .await
            .map_err(sdk_error)?;

        let e_tag = output.copy_part_result.and_then(|result| result.e_tag);
        Ok(CompletedPart::builder()
            .part_number(part.part_number)
            .set_e_tag(e_tag)
            .build())
    }

    async fn complete_multipart_upload(
        &self,
        upload: MultipartUpload<'_>,
        parts: Vec<CompletedPart>,
    ) -> Result<(), Error> {
        self.complete_multipart_upload()
            .bucket(upload.bucket)
            .key(upload.key)
            .upload_id(upload.upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .set_parts(Some(parts))
                    .build(),
            )
            .send()
            .await
            .map_err(sdk_error)?;
        Ok(())
    }

    async fn abort_multipart_upload(&self, upload: MultipartUpload<'_>) -> Result<(), Error> {
        self.abort_multipart_upload()
            .bucket(upload.bucket)
            .key(upload.key)
            .upload_id(upload.upload_id)
            .send()
            .await
            .map_err(sdk_error)?;
        Ok(())
    }
}

pub async fn setup_client(args: &arg::FindOpt) -> Client {
    let FindOpt {
        aws_access_key,
        aws_secret_key,
        aws_region,
        endpoint_url,
        force_path_style,
        ..
    } = args;

    let region_provider = match aws_region {
        Some(region) => {
            aws_config::meta::region::RegionProviderChain::first_try(Region::new(region.clone()))
        }
        None => aws_config::meta::region::RegionProviderChain::default_provider(),
    };

    let loader = aws_config::ConfigLoader::default()
        .behavior_version(BehaviorVersion::latest())
        .region(region_provider);

    let shared_config = match (aws_access_key, aws_secret_key) {
        (Some(aws_access_key), Some(aws_secret_key)) => {
            let credentials_provider =
                Credentials::new(aws_access_key, aws_secret_key, None, None, "static");
            loader
                .credentials_provider(credentials_provider)
                .load()
                .await
        }
        _ => {
            let credentials_provider = CredentialsProviderChain::default_provider().await;
            loader
                .credentials_provider(credentials_provider)
                .load()
                .await
        }
    };

    // Create S3 client with custom configuration for non-AWS S3 services
    let mut s3_config_builder = aws_sdk_s3::config::Builder::from(&shared_config);

    // Set custom endpoint if provided (for MinIO, Ceph, etc.)
    if let Some(endpoint) = endpoint_url {
        s3_config_builder = s3_config_builder.endpoint_url(endpoint);
    }

    // Enable path-style addressing if requested (required for some S3-compatible services)
    if *force_path_style {
        s3_config_builder = s3_config_builder.force_path_style(true);
    }

    Client::from_conf(s3_config_builder.build())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{opts, replay_s3_client};

    use aws_sdk_s3::primitives::SdkBody;
    use aws_smithy_runtime::client::http::test_util::ReplayEvent;

    use aws_config::Region;
    use aws_smithy_runtime::client::http::test_util::StaticReplayClient;

    fn region_of(client: &Client) -> Option<String> {
        S3Client::region(client)
    }

    #[tokio::test]
    async fn test_get_s3_client() {
        let with_creds = setup_client(&opts(&[
            "s3://mock-bucket/mock-prefix",
            "--aws-access-key",
            "mock_access",
            "--aws-secret-key",
            "mock_secret",
            "--aws-region",
            "mock-region",
        ]))
        .await;
        let without_creds = setup_client(&opts(&[
            "s3://mock-bucket/mock-prefix",
            "--aws-region",
            "mock-region",
        ]))
        .await;

        assert_eq!(region_of(&with_creds).as_deref(), Some("mock-region"));
        assert_eq!(region_of(&without_creds).as_deref(), Some("mock-region"));
    }

    #[tokio::test]
    async fn test_client_with_custom_endpoint_and_path_style() {
        for args in [
            &[
                "s3://test-bucket",
                "--aws-access-key",
                "minio_access",
                "--aws-secret-key",
                "minio_secret",
                "--aws-region",
                "us-east-1",
                "--endpoint-url",
                "http://localhost:9000",
            ][..],
            &[
                "s3://test-bucket",
                "--aws-access-key",
                "test_access",
                "--aws-secret-key",
                "test_secret",
                "--aws-region",
                "us-west-2",
                "--force-path-style",
            ],
            &[
                "s3://ceph-bucket/data",
                "--aws-access-key",
                "ceph_access",
                "--aws-secret-key",
                "ceph_secret",
                "--aws-region",
                "default",
                "--endpoint-url",
                "https://ceph.example.com",
                "--force-path-style",
            ],
            &[
                "s3://test-bucket/prefix",
                "--aws-region",
                "us-east-1",
                "--endpoint-url",
                "http://localhost:9000",
                "--force-path-style",
            ],
        ] {
            let opts = opts(args);
            let client = setup_client(&opts).await;

            assert_eq!(
                client.config().region().map(|region| region.as_ref()),
                opts.aws_region.as_deref(),
                "{args:?}"
            );
        }
    }

    #[test]
    fn test_option_types_convert_to_sdk_types() {
        let standard_ia = StorageClassName::from("STANDARD_IA");
        assert_eq!(StorageClass::from(&standard_ia), StorageClass::StandardIa);
        assert_eq!(
            ObjectStorageClass::from(&standard_ia),
            ObjectStorageClass::StandardIa
        );

        let custom = StorageClassName::from("COLD");
        assert_eq!(StorageClass::from(&custom).as_str(), "COLD");

        assert_eq!(Tier::from(RestoreTier::Standard), Tier::Standard);
        assert_eq!(Tier::from(RestoreTier::Bulk), Tier::Bulk);
        assert_eq!(Tier::from(RestoreTier::Expedited), Tier::Expedited);
    }

    #[tokio::test]
    async fn test_delete_objects_empty_list_is_noop() {
        let replay_client = StaticReplayClient::new(vec![]);
        let client = Client::from_conf(
            aws_sdk_s3::Config::builder()
                .behavior_version(BehaviorVersion::latest())
                .credentials_provider(Credentials::new("mock", "mock", None, None, "mock"))
                .region(Region::new("us-east-1"))
                .http_client(replay_client.clone())
                .build(),
        );

        let outcome = S3Client::delete_objects(&client, "test-bucket", Vec::new())
            .await
            .unwrap();

        assert_eq!(outcome, DeleteOutcome::default());
        replay_client.assert_requests_match(&[]);
    }

    fn event(
        method: &str,
        uri: &str,
        headers: &[(&str, &str)],
        body: &str,
        response: http::Response<SdkBody>,
    ) -> ReplayEvent {
        let mut request = http::Request::builder().method(method).uri(uri);
        for (name, value) in headers {
            request = request.header(*name, *value);
        }
        ReplayEvent::new(request.body(SdkBody::from(body)).unwrap(), response)
    }

    fn response(headers: &[(&str, &str)], body: &str) -> http::Response<SdkBody> {
        let mut response = http::Response::builder().status(200);
        for (name, value) in headers {
            response = response.header(*name, *value);
        }
        response.body(SdkBody::from(body)).unwrap()
    }

    #[tokio::test]
    async fn test_multipart_copy_requests() {
        let base = "https://test-bucket.s3.us-east-1.amazonaws.com";
        let (client, replay) = replay_s3_client(vec![
            event(
                "HEAD",
                &format!("{base}/big.bin?versionId=v1"),
                &[],
                "",
                response(
                    &[
                        ("content-length", "6442450944"),
                        ("etag", "\"abc\""),
                        ("content-type", "video/mp4"),
                        ("cache-control", "max-age=60"),
                        ("expires", "Wed, 21 Oct 2026 07:28:00 GMT"),
                        ("x-amz-meta-owner", "data-team"),
                        ("x-amz-website-redirect-location", "/other"),
                    ],
                    "",
                ),
            ),
            event(
                "POST",
                &format!("{base}/copy.bin?uploads"),
                &[
                    ("x-amz-storage-class", "GLACIER"),
                    ("content-type", "video/mp4"),
                    ("cache-control", "max-age=60"),
                    ("expires", "Wed, 21 Oct 2026 07:28:00 GMT"),
                    ("x-amz-meta-owner", "data-team"),
                    ("x-amz-website-redirect-location", "/other"),
                    ("x-amz-tagging", "env=prod&team=data%20eng"),
                ],
                "",
                response(
                    &[],
                    "<InitiateMultipartUploadResult><Bucket>test-bucket</Bucket>\
                     <Key>copy.bin</Key><UploadId>up-1</UploadId></InitiateMultipartUploadResult>",
                ),
            ),
            event(
                "PUT",
                &format!("{base}/copy.bin?x-id=UploadPartCopy&partNumber=1&uploadId=up-1"),
                &[
                    ("x-amz-copy-source", "test-bucket/big.bin?versionId=v1"),
                    ("x-amz-copy-source-range", "bytes=0-99"),
                    ("x-amz-copy-source-if-match", "\"abc\""),
                ],
                "",
                response(&[], "<CopyPartResult><ETag>\"p1\"</ETag></CopyPartResult>"),
            ),
            event(
                "POST",
                &format!("{base}/copy.bin?uploadId=up-1"),
                &[],
                r#"<CompleteMultipartUpload xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Part><ETag>&quot;p1&quot;</ETag><PartNumber>1</PartNumber></Part></CompleteMultipartUpload>"#,
                response(
                    &[],
                    "<CompleteMultipartUploadResult></CompleteMultipartUploadResult>",
                ),
            ),
            event(
                "DELETE",
                &format!("{base}/copy.bin?x-id=AbortMultipartUpload&uploadId=up-1"),
                &[],
                "",
                response(&[], ""),
            ),
        ]);

        let head = S3Client::head_object(&client, "test-bucket", "big.bin", Some("v1"))
            .await
            .unwrap();
        assert_eq!(head.size, 6 * 1024 * 1024 * 1024);
        assert_eq!(head.e_tag.as_deref(), Some("\"abc\""));
        assert_eq!(head.metadata["owner"], "data-team");
        assert!(head.expires.is_some());

        let tags = [
            Tag::builder().key("env").value("prod").build().unwrap(),
            Tag::builder()
                .key("team")
                .value("data eng")
                .build()
                .unwrap(),
        ];
        let upload_id = S3Client::create_multipart_upload(
            &client,
            "test-bucket",
            "copy.bin",
            &head,
            Some(StorageClass::Glacier),
            &tags,
        )
        .await
        .unwrap();
        assert_eq!(upload_id, "up-1");

        let upload = MultipartUpload {
            bucket: "test-bucket",
            key: "copy.bin",
            upload_id: &upload_id,
        };
        let part = PartCopy {
            part_number: 1,
            copy_source: "test-bucket/big.bin?versionId=v1",
            range: 0..=99,
            source_e_tag: head.e_tag.as_deref(),
        };
        let part = S3Client::upload_part_copy(&client, upload, part)
            .await
            .unwrap();
        assert_eq!(part.e_tag(), Some("\"p1\""));

        S3Client::complete_multipart_upload(&client, upload, vec![part])
            .await
            .unwrap();
        S3Client::abort_multipart_upload(&client, upload)
            .await
            .unwrap();
        replay.assert_requests_match(&[]);
    }

    #[test]
    fn test_delete_outcome_names_each_failed_key() {
        let failure = |key: &str, version_id: Option<&str>| DeleteFailure {
            key: key.to_owned(),
            version_id: version_id.map(str::to_owned),
            code: "AccessDenied".to_owned(),
            message: "Access Denied".to_owned(),
        };
        assert!(DeleteOutcome::default().check().is_ok());

        let outcome = DeleteOutcome {
            deleted: Vec::new(),
            failed: vec![failure("a.txt", None), failure("b.txt", Some("v1"))],
        };
        assert_eq!(
            outcome.check().unwrap_err().to_string(),
            "failed to delete 2 of the objects: a.txt: AccessDenied (Access Denied), \
             b.txt?versionId=v1: AccessDenied (Access Denied)"
        );
    }

    #[test]
    fn test_error_chain_joins_each_cause_once() {
        #[derive(Debug, thiserror::Error)]
        #[error("inner")]
        struct Inner;

        #[derive(Debug, thiserror::Error)]
        #[error("outer")]
        struct Outer(#[source] Inner);

        #[derive(Debug, thiserror::Error)]
        #[error("outer: inner")]
        struct AlreadyIncludesCause(#[source] Inner);

        assert_eq!(error_chain(&Inner), "inner");
        assert_eq!(error_chain(&Outer(Inner)), "outer: inner");
        assert_eq!(error_chain(&AlreadyIncludesCause(Inner)), "outer: inner");
    }

    #[test]
    fn test_classify_tagging_error_non_service_error() {
        let timeout_err: SdkError<GetObjectTaggingError> = SdkError::timeout_error(Box::new(
            std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout"),
        ));

        let result = classify_tagging_error(timeout_err, "bucket", "key");

        assert!(
            matches!(&result, TagFetchError::ApiError(message) if message.contains("timeout")),
            "Expected ApiError, got: {result:?}"
        );
    }
}
