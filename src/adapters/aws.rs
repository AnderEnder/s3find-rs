use anyhow::{Error, anyhow};
use async_trait::async_trait;
use aws_config::{BehaviorVersion, meta::credentials::CredentialsProviderChain};
use aws_sdk_s3::{
    Client,
    config::Credentials,
    primitives::ByteStream,
    types::{
        Delete, GlacierJobParameters, MetadataDirective, ObjectCannedAcl, ObjectIdentifier,
        RestoreRequest, StorageClass, Tag, Tagging, Tier,
    },
};
use aws_smithy_runtime_api::client::result::SdkError;

use crate::arg::{self, FindOpt};

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

#[async_trait]
pub trait CommandS3Client: Send + Sync {
    fn region(&self) -> Option<String>;

    async fn delete_objects(
        &self,
        bucket: &str,
        objects: Vec<DeleteObjectRequest>,
    ) -> Result<Vec<DeletedObjectInfo>, Error>;

    async fn put_object_tagging(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        tags: Vec<Tag>,
    ) -> Result<(), Error>;

    async fn get_object_tagging(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> Result<Vec<Tag>, Error>;

    async fn put_object_acl_public_read(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> Result<(), Error>;

    async fn get_object(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> Result<ByteStream, Error>;

    async fn copy_object(
        &self,
        bucket: &str,
        key: &str,
        copy_source: &str,
        storage_class: Option<StorageClass>,
        metadata_directive: Option<MetadataDirective>,
    ) -> Result<(), Error>;

    async fn restore_object(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        days: i32,
        tier: Tier,
    ) -> Result<RestoreObjectStatus, Error>;
}

#[async_trait]
impl CommandS3Client for Client {
    fn region(&self) -> Option<String> {
        self.config()
            .region()
            .map(|region| region.as_ref().to_string())
    }

    async fn delete_objects(
        &self,
        bucket: &str,
        objects: Vec<DeleteObjectRequest>,
    ) -> Result<Vec<DeletedObjectInfo>, Error> {
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
            .await?;

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

        let mut request = self
            .put_object_tagging()
            .bucket(bucket)
            .key(key)
            .set_tagging(Some(tagging));

        if let Some(version_id) = version_id {
            request = request.version_id(version_id);
        }

        request.send().await?;
        Ok(())
    }

    async fn get_object_tagging(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> Result<Vec<Tag>, Error> {
        let mut request = self.get_object_tagging().bucket(bucket).key(key);

        if let Some(version_id) = version_id {
            request = request.version_id(version_id);
        }

        Ok(request.send().await?.tag_set)
    }

    async fn put_object_acl_public_read(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> Result<(), Error> {
        let mut request = self
            .put_object_acl()
            .bucket(bucket)
            .key(key)
            .acl(ObjectCannedAcl::PublicRead);

        if let Some(version_id) = version_id {
            request = request.version_id(version_id);
        }

        request.send().await?;
        Ok(())
    }

    async fn get_object(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> Result<ByteStream, Error> {
        let mut request = self.get_object().bucket(bucket).key(key);

        if let Some(version_id) = version_id {
            request = request.version_id(version_id);
        }

        Ok(request.send().await?.body)
    }

    async fn copy_object(
        &self,
        bucket: &str,
        key: &str,
        copy_source: &str,
        storage_class: Option<StorageClass>,
        metadata_directive: Option<MetadataDirective>,
    ) -> Result<(), Error> {
        let mut request = self
            .copy_object()
            .bucket(bucket)
            .key(key)
            .copy_source(copy_source)
            .set_storage_class(storage_class);

        if let Some(metadata_directive) = metadata_directive {
            request = request.metadata_directive(metadata_directive);
        }

        request.send().await?;
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
            .set_glacier_job_parameters(Some(GlacierJobParameters::builder().tier(tier).build()?))
            .build();

        let mut request = self
            .restore_object()
            .bucket(bucket)
            .key(key)
            .restore_request(restore_request);

        if let Some(version_id) = version_id {
            request = request.version_id(version_id);
        }

        match request.send().await {
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
            Err(err) => Err(anyhow!(err)),
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
        Some(region) => aws_config::meta::region::RegionProviderChain::first_try(region.to_owned()),
        None => aws_config::meta::region::RegionProviderChain::default_provider(),
    };

    let shared_config = match (aws_access_key, aws_secret_key) {
        (Some(aws_access_key), Some(aws_secret_key)) => {
            let credentials_provider =
                Credentials::new(aws_access_key, aws_secret_key, None, None, "static");
            aws_config::ConfigLoader::default()
                .behavior_version(BehaviorVersion::latest())
                .region(region_provider)
                .credentials_provider(credentials_provider)
                .load()
                .await
        }
        _ => {
            let credentials_provider = CredentialsProviderChain::default_provider().await;
            aws_config::ConfigLoader::default()
                .behavior_version(BehaviorVersion::latest())
                .region(region_provider)
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
    use crate::arg::S3Path;

    use aws_config::Region;

    use std::str::FromStr;

    #[tokio::test]
    async fn test_get_s3_client() {
        let args = FindOpt {
            aws_access_key: Some("mock_access".to_string()),
            aws_secret_key: Some("mock_secret".to_string()),
            aws_region: Some(Region::new("mock-region")),
            endpoint_url: None,
            force_path_style: false,
            path: S3Path::from_str("s3://mock-bucket/mock-prefix").unwrap(),
            limit: Some(100),
            summarize: false,
            name: Vec::new(),
            iname: Vec::new(),
            mtime: Vec::new(),
            regex: Vec::new(),
            size: Default::default(),
            page_size: 100,
            cmd: Default::default(),
            storage_class: None,
            maxdepth: None,
            all_versions: false,
            tag: Vec::new(),
            tag_exists: Vec::new(),
            tag_concurrency: 50,
        };
        let client_with_creds = setup_client(&args).await;

        let args_without_creds = FindOpt {
            aws_access_key: None,
            aws_secret_key: None,
            aws_region: Some(Region::new("mock-region")),
            ..args
        };
        let client_without_creds = setup_client(&args_without_creds).await;

        assert!(client_with_creds.config().region().is_some());
        assert!(client_without_creds.config().region().is_some());
    }

    #[tokio::test]
    async fn test_client_with_custom_endpoint() {
        let args = FindOpt {
            aws_access_key: Some("minio_access".to_string()),
            aws_secret_key: Some("minio_secret".to_string()),
            aws_region: Some(Region::new("us-east-1")),
            endpoint_url: Some("http://localhost:9000".to_string()),
            force_path_style: false,
            path: S3Path::from_str("s3://test-bucket/test-prefix").unwrap(),
            limit: None,
            summarize: false,
            name: Vec::new(),
            iname: Vec::new(),
            mtime: Vec::new(),
            regex: Vec::new(),
            size: Default::default(),
            page_size: 1000,
            cmd: Default::default(),
            storage_class: None,
            maxdepth: None,
            all_versions: false,
            tag: Vec::new(),
            tag_exists: Vec::new(),
            tag_concurrency: 50,
        };

        let client = setup_client(&args).await;

        // Verify the client was created successfully
        assert!(client.config().region().is_some());
        assert_eq!(client.config().region().unwrap().as_ref(), "us-east-1");
    }

    #[tokio::test]
    async fn test_client_with_force_path_style() {
        let args = FindOpt {
            aws_access_key: Some("test_access".to_string()),
            aws_secret_key: Some("test_secret".to_string()),
            aws_region: Some(Region::new("us-west-2")),
            endpoint_url: None,
            force_path_style: true,
            path: S3Path::from_str("s3://test-bucket/test-prefix").unwrap(),
            limit: None,
            summarize: false,
            name: Vec::new(),
            iname: Vec::new(),
            mtime: Vec::new(),
            regex: Vec::new(),
            size: Default::default(),
            page_size: 1000,
            cmd: Default::default(),
            storage_class: None,
            maxdepth: None,
            all_versions: false,
            tag: Vec::new(),
            tag_exists: Vec::new(),
            tag_concurrency: 50,
        };

        let client = setup_client(&args).await;

        // Verify the client was created successfully
        assert!(client.config().region().is_some());
        assert_eq!(client.config().region().unwrap().as_ref(), "us-west-2");
    }

    #[tokio::test]
    async fn test_client_with_endpoint_and_path_style() {
        let args = FindOpt {
            aws_access_key: Some("ceph_access".to_string()),
            aws_secret_key: Some("ceph_secret".to_string()),
            aws_region: Some(Region::new("default")),
            endpoint_url: Some("https://ceph.example.com".to_string()),
            force_path_style: true,
            path: S3Path::from_str("s3://ceph-bucket/data").unwrap(),
            limit: None,
            summarize: false,
            name: Vec::new(),
            iname: Vec::new(),
            mtime: Vec::new(),
            regex: Vec::new(),
            size: Default::default(),
            page_size: 1000,
            cmd: Default::default(),
            storage_class: None,
            maxdepth: None,
            all_versions: false,
            tag: Vec::new(),
            tag_exists: Vec::new(),
            tag_concurrency: 50,
        };

        let client = setup_client(&args).await;

        // Verify the client was created successfully with both options
        assert!(client.config().region().is_some());
        assert_eq!(client.config().region().unwrap().as_ref(), "default");
    }

    #[tokio::test]
    async fn test_client_without_credentials_with_endpoint() {
        let args = FindOpt {
            aws_access_key: None,
            aws_secret_key: None,
            aws_region: Some(Region::new("us-east-1")),
            endpoint_url: Some("http://localhost:9000".to_string()),
            force_path_style: true,
            path: S3Path::from_str("s3://test-bucket/prefix").unwrap(),
            limit: None,
            summarize: false,
            name: Vec::new(),
            iname: Vec::new(),
            mtime: Vec::new(),
            regex: Vec::new(),
            size: Default::default(),
            page_size: 1000,
            cmd: Default::default(),
            storage_class: None,
            maxdepth: None,
            all_versions: false,
            tag: Vec::new(),
            tag_exists: Vec::new(),
            tag_concurrency: 50,
        };

        let client = setup_client(&args).await;

        // Verify the client was created with default credentials provider
        assert!(client.config().region().is_some());
    }
}
