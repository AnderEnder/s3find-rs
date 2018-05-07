extern crate futures;
extern crate rusoto_core;
extern crate rusoto_credential;

use rusoto_credential::{AwsCredentials, CredentialsError, ProvideAwsCredentials, StaticProvider};
use rusoto_core::reactor::CredentialsProviderFuture;
use rusoto_core::reactor::CredentialsProvider;
use futures::{Future, Poll};
use futures::future::FutureResult;

pub enum CombinedProvider {
    Static(StaticProvider),
    Simple(CredentialsProvider),
}

impl CombinedProvider {
    pub fn new(access_key: Option<String>, secret_key: Option<String>) -> CombinedProvider {
        match (access_key, secret_key) {
            (Some(aws_access_key), Some(aws_secret_key)) => {
                CombinedProvider::with_credentials(aws_access_key, aws_secret_key)
            }
            _ => CombinedProvider::with_default(),
        }
    }

    pub fn with_credentials(aws_access_key: String, aws_secret_key: String) -> CombinedProvider {
        CombinedProvider::Static(StaticProvider::new(
            aws_access_key,
            aws_secret_key,
            None,
            None,
        ))
    }

    pub fn with_default() -> CombinedProvider {
        CombinedProvider::Simple(CredentialsProvider::default())
    }
}

pub enum CombinedProviderFuture {
    Static(FutureResult<AwsCredentials, CredentialsError>),
    Simple(CredentialsProviderFuture),
}

impl Future for CombinedProviderFuture {
    type Item = AwsCredentials;
    type Error = CredentialsError;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match *self {
            CombinedProviderFuture::Static(ref mut f) => f.poll(),
            CombinedProviderFuture::Simple(ref mut f) => f.poll(),
        }
    }
}

impl ProvideAwsCredentials for CombinedProvider {
    type Future = CombinedProviderFuture;

    fn credentials(&self) -> Self::Future {
        match *self {
            CombinedProvider::Static(ref p) => p.credentials().into(),
            CombinedProvider::Simple(ref p) => p.credentials().into(),
        }
    }
}

impl From<CredentialsProviderFuture> for CombinedProviderFuture {
    fn from(future: CredentialsProviderFuture) -> CombinedProviderFuture {
        CombinedProviderFuture::Simple(future)
    }
}

impl From<FutureResult<AwsCredentials, CredentialsError>> for CombinedProviderFuture {
    fn from(future: FutureResult<AwsCredentials, CredentialsError>) -> CombinedProviderFuture {
        CombinedProviderFuture::Static(future)
    }
}
