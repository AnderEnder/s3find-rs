use aws_config::{BehaviorVersion, meta::credentials::CredentialsProviderChain};
use aws_sdk_s3::{Client, config::Credentials};

use crate::arg::{self, FindOpt};

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
        };

        let client = setup_client(&args).await;

        // Verify the client was created with default credentials provider
        assert!(client.config().region().is_some());
    }
}
