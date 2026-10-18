use anyhow::{Error, anyhow};
use aws_config::{BehaviorVersion, Region, meta::credentials::CredentialsProviderChain};
use aws_sdk_s3::{
    Client,
    config::Credentials,
    error::SdkError,
    operation::get_object_tagging::GetObjectTaggingError,
    primitives::ByteStream,
    types::{
        Delete, DeleteMarkerEntry, GlacierJobParameters, MetadataDirective, Object,
        ObjectCannedAcl, ObjectIdentifier, ObjectStorageClass, ObjectVersion, RestoreRequest,
        StorageClass, Tag, Tagging, Tier,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestoreObjectStatus {
    Started,
    AlreadyInProgress,
    InvalidObjectState,
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

    fn delete_objects(
        &self,
        bucket: &str,
        objects: Vec<DeleteObjectRequest>,
    ) -> impl Future<Output = Result<Vec<DeletedObjectInfo>, Error>> + Send;

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
    ) -> Result<Vec<DeletedObjectInfo>, Error> {
        if objects.is_empty() {
            return Ok(Vec::new());
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

        let delete_errors = response.errors.unwrap_or_default();
        if !delete_errors.is_empty() {
            let details = delete_errors
                .iter()
                .map(|error| {
                    let key = error.key().unwrap_or("<unknown>");
                    let version = error
                        .version_id()
                        .map(|value| format!("?versionId={value}"))
                        .unwrap_or_default();
                    let code = error.code().unwrap_or("Unknown");
                    let message = error.message().unwrap_or("unknown delete error");
                    format!("{key}{version}: {code} ({message})")
                })
                .collect::<Vec<_>>()
                .join(", ");
            return Err(anyhow!("delete_objects partially failed: {details}"));
        }

        Ok(response
            .deleted
            .unwrap_or_default()
            .into_iter()
            .map(|object| DeletedObjectInfo {
                key: object.key,
                version_id: object.version_id,
            })
            .collect())
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
    use crate::testing::opts;

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

        let deleted = S3Client::delete_objects(&client, "test-bucket", Vec::new())
            .await
            .unwrap();

        assert!(deleted.is_empty());
        replay_client.assert_requests_match(&[]);
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
