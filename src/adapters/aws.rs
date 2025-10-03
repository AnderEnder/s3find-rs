use aws_config::{BehaviorVersion, meta::credentials::CredentialsProviderChain};
use aws_sdk_s3::{Client, config::Credentials};

use crate::arg::{self, FindOpt};

pub async fn setup_client(args: &arg::FindOpt) -> Client {
    let FindOpt {
        aws_access_key,
        aws_secret_key,
        aws_region,
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

    Client::new(&shared_config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arg::S3Path;

    use aws_config::Region;

    use std::str::FromStr;

    async fn test_get_s3_client() {
        let args = FindOpt {
            aws_access_key: Some("mock_access".to_string()),
            aws_secret_key: Some("mock_secret".to_string()),
            aws_region: Some(Region::new("mock-region")),
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
}
