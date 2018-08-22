use futures::future::FutureResult;
use futures::{Future, Poll};
use rusoto_credential::{AwsCredentials, CredentialsError, ProvideAwsCredentials, StaticProvider};
use rusoto_credential::{DefaultCredentialsProvider, DefaultCredentialsProviderFuture};

pub enum CombinedProvider {
    Static(StaticProvider),
    Simple(DefaultCredentialsProvider),
}

impl CombinedProvider {
    pub fn new(access_key: Option<String>, secret_key: Option<String>) -> CombinedProvider {
        match (access_key, secret_key) {
            (Some(aws_access_key), Some(aws_secret_key)) =>
                CombinedProvider::with_credentials(aws_access_key, aws_secret_key),
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
        CombinedProvider::Simple(DefaultCredentialsProvider::new().unwrap())
    }
}

pub enum CombinedProviderFuture {
    Static(FutureResult<AwsCredentials, CredentialsError>),
    Simple(DefaultCredentialsProviderFuture),
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

impl From<DefaultCredentialsProviderFuture> for CombinedProviderFuture {
    fn from(future: DefaultCredentialsProviderFuture) -> CombinedProviderFuture {
        CombinedProviderFuture::Simple(future)
    }
}

impl From<FutureResult<AwsCredentials, CredentialsError>> for CombinedProviderFuture {
    fn from(future: FutureResult<AwsCredentials, CredentialsError>) -> CombinedProviderFuture {
        CombinedProviderFuture::Static(future)
    }
}
