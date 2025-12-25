//! Integration tests for s3find CLI using LocalStack
//!
//! These tests use testcontainers to automatically start a LocalStack container,
//! create test data in S3, and verify CLI behavior.
//!
//! ## Running these tests
//!
//! Run only integration tests:
//! ```bash
//! cargo test --test localstack_integration
//! ```
//!
//! Run all tests (unit + integration):
//! ```bash
//! cargo test
//! ```
//!
//! Run only unit tests (exclude integration):
//! ```bash
//! cargo test --lib
//! ```
//!
//! ## CI/CD Usage
//!
//! In CI pipelines, you can run these as a separate stage:
//! ```yaml
//! - name: Run integration tests
//!   run: cargo test --test localstack_integration
//! ```
//!
//! ## Test Isolation
//!
//! Each test creates its own bucket with a unique timestamp-based name to prevent
//! test pollution. This ensures:
//! - Tests can run in parallel without interfering with each other
//! - Reusing a manually-started LocalStack container is safe across multiple test runs
//! - Old test data doesn't affect new test results
//!
//! Example bucket names: `test-ls-basic-1735152341234`, `test-print-1735152341567`
//!
//! ## Container Reuse & Cleanup
//!
//! **Manual LocalStack Reuse:**
//! Tests can detect and reuse a manually-started LocalStack container on port 4566.
//! Note: Containers started by testcontainers use random ports and won't be detected.
//! To manually start LocalStack for faster repeated test runs:
//! ```bash
//! # Start LocalStack on port 4566 (tests will detect and reuse it)
//! docker run -d -p 4566:4566 --name localstack localstack/localstack:4.12
//!
//! # Run tests multiple times (instant startup!)
//! cargo test --test localstack_integration
//! ```
//!
//! **Cleanup:**
//! Containers started by tests persist after completion. To clean up:
//! ```bash
//! # Docker
//! docker rm -f $(docker ps -aq --filter ancestor=localstack/localstack)
//!
//! # Podman
//! podman rm -f $(podman ps -aq --filter ancestor=localstack/localstack)
//! ```
//!
//! ## Using with Podman
//!
//! These tests work with Podman as well as Docker. Podman needs to expose a
//! Docker-compatible socket:
//!
//! **Linux:**
//! ```bash
//! # Enable Podman socket (systemd)
//! systemctl --user enable --now podman.socket
//!
//! # Set DOCKER_HOST environment variable
//! export DOCKER_HOST=unix:///run/user/$UID/podman/podman.sock
//!
//! # Run tests
//! cargo test --test localstack_integration
//! ```
//!
//! **macOS/Windows (Podman Machine):**
//! ```bash
//! # Initialize and start Podman machine
//! podman machine init
//! podman machine start
//!
//! # Podman automatically sets DOCKER_HOST
//! cargo test --test localstack_integration
//! ```
//!
//! Note: Docker or Podman must be available for testcontainers to work.

use assert_cmd::assert::OutputAssertExt;
use aws_config::BehaviorVersion;
use aws_sdk_s3::{Client, primitives::ByteStream};
use predicates::prelude::*;
use std::{process::Command, time::Duration};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{ContainerPort, WaitFor},
    runners::AsyncRunner,
};
use tokio::sync::OnceCell;

/// Global LocalStack container shared across all tests
static LOCALSTACK: OnceCell<SharedLocalStack> = OnceCell::const_new();

struct SharedLocalStack {
    _container: Option<ContainerAsync<GenericImage>>,
    endpoint: String,
}

/// LocalStack container configuration
const LOCALSTACK_IMAGE: &str = "localstack/localstack";
const LOCALSTACK_TAG: &str = "4.12";
const LOCALSTACK_PORT: u16 = 4566;

/// Check if LocalStack is already running on the default port 4566
/// Note: This only detects manually-started LocalStack containers.
/// Containers started by testcontainers use random ports and won't be detected here.
async fn is_localstack_running() -> Option<String> {
    let endpoint = format!("http://localhost:{}", LOCALSTACK_PORT);

    // Try to connect to LocalStack on port 4566
    match tokio::time::timeout(
        Duration::from_secs(1),
        tokio::net::TcpStream::connect(format!("localhost:{}", LOCALSTACK_PORT)),
    )
    .await
    {
        Ok(Ok(_)) => Some(endpoint),
        _ => None,
    }
}

/// Wait for LocalStack to be ready by polling the health endpoint
async fn wait_for_localstack_ready(endpoint: &str, max_wait: Duration) -> Result<(), String> {
    let start = std::time::Instant::now();

    // Derive the host:port from the endpoint (e.g., "http://host:port[/...]" -> "host:port")
    let addr = {
        let without_scheme = endpoint
            .split_once("://")
            .map(|(_, rest)| rest)
            .unwrap_or(endpoint);
        without_scheme
            .split('/')
            .next()
            .unwrap_or(without_scheme)
            .to_string()
    };

    while start.elapsed() < max_wait {
        // Try to make a simple TCP connection to the derived address
        match tokio::time::timeout(
            Duration::from_secs(2),
            tokio::net::TcpStream::connect(&addr),
        )
        .await
        {
            Ok(Ok(_)) => {
                eprintln!("LocalStack health check passed");
                return Ok(());
            }
            _ => {
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }

    Err(format!(
        "LocalStack did not become ready within {:?}",
        max_wait
    ))
}

/// Get or initialize the shared LocalStack container
async fn get_localstack() -> &'static SharedLocalStack {
    LOCALSTACK
        .get_or_init(|| async {
            // First, check if LocalStack is already running
            if let Some(endpoint) = is_localstack_running().await {
                eprintln!("Reusing existing LocalStack at {}", endpoint);
                return SharedLocalStack {
                    _container: None,
                    endpoint,
                };
            }

            eprintln!("Starting new LocalStack container...");

            // Start LocalStack container once for all tests
            let container = GenericImage::new(LOCALSTACK_IMAGE, LOCALSTACK_TAG)
                .with_exposed_port(ContainerPort::Tcp(LOCALSTACK_PORT))
                .with_wait_for(WaitFor::message_on_stdout("Ready."))
                .with_env_var("SERVICES", "s3")
                .start()
                .await
                .expect("Failed to start shared LocalStack container");

            let host_port = container
                .get_host_port_ipv4(LOCALSTACK_PORT)
                .await
                .expect("Failed to get container port");

            let endpoint = format!("http://localhost:{}", host_port);

            // Wait for LocalStack to be fully ready with health check
            // Longer timeout for CI environments where image pull may be slow
            wait_for_localstack_ready(&endpoint, Duration::from_secs(120))
                .await
                .expect("LocalStack failed to become ready");

            eprintln!("LocalStack ready at {}", endpoint);

            SharedLocalStack {
                _container: Some(container),
                endpoint,
            }
        })
        .await
}

/// Test fixture that manages S3 client and test bucket
struct LocalStackFixture {
    endpoint: String,
    client: Client,
    bucket: String,
}

/// Generate a unique bucket name with timestamp suffix to prevent test pollution
fn unique_bucket_name(base_name: &str) -> String {
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    format!("{}-{}", base_name, timestamp)
}

impl LocalStackFixture {
    /// Creates a new test fixture with its own bucket in the shared LocalStack
    async fn new(bucket_name: &str) -> Self {
        let localstack = get_localstack().await;
        let endpoint = localstack.endpoint.clone();

        // Create S3 client pointing to LocalStack with path-style addressing
        let config = aws_config::defaults(BehaviorVersion::latest())
            .endpoint_url(&endpoint)
            .region("us-east-1")
            .credentials_provider(aws_sdk_s3::config::Credentials::new(
                "test",
                "test",
                None,
                None,
                "localstack",
            ))
            .load()
            .await;

        let s3_config = aws_sdk_s3::config::Builder::from(&config)
            .force_path_style(true)
            .build();

        let client = Client::from_conf(s3_config);

        // Create test bucket with retry for robustness
        let mut retries = 3;
        loop {
            match client.create_bucket().bucket(bucket_name).send().await {
                Ok(_) => {
                    eprintln!("Created bucket: {}", bucket_name);
                    break;
                }
                Err(e) => {
                    let error_str = e.to_string();
                    // Bucket already exists - this is fine, we can reuse it
                    if error_str.contains("BucketAlreadyOwnedByYou")
                        || error_str.contains("BucketAlreadyExists")
                    {
                        eprintln!("Bucket already exists, reusing: {}", bucket_name);
                        break;
                    }
                    // Other errors - retry
                    if retries > 0 {
                        eprintln!(
                            "Bucket creation failed, retrying... ({} attempts left): {}",
                            retries, e
                        );
                        retries -= 1;
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    } else {
                        panic!("Failed to create bucket after retries: {}", e);
                    }
                }
            }
        }

        Self {
            endpoint,
            client,
            bucket: bucket_name.to_string(),
        }
    }

    /// Upload an object to the test bucket
    async fn put_object(&self, key: &str, body: &[u8]) {
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(ByteStream::from(body.to_vec()))
            .send()
            .await
            .expect("Failed to put object");
    }

    /// Upload an object with metadata
    async fn put_object_with_storage_class(
        &self,
        key: &str,
        body: &[u8],
        storage_class: aws_sdk_s3::types::StorageClass,
    ) {
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(ByteStream::from(body.to_vec()))
            .storage_class(storage_class)
            .send()
            .await
            .expect("Failed to put object");
    }

    /// Get the S3 path for use with s3find CLI
    fn s3_path(&self, prefix: &str) -> String {
        format!("s3://{}/{}", self.bucket, prefix)
    }

    /// Create a Command configured to use this LocalStack instance
    fn s3find_command(&self) -> Command {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_s3find"));
        cmd.env("AWS_ACCESS_KEY_ID", "test")
            .env("AWS_SECRET_ACCESS_KEY", "test")
            .env("AWS_DEFAULT_REGION", "us-east-1")
            .arg("--endpoint-url")
            .arg(&self.endpoint)
            .arg("--force-path-style");
        cmd
    }
}

#[tokio::test]
async fn test_ls_basic() {
    let bucket_name = unique_bucket_name("test-ls-basic");
    let fixture = LocalStackFixture::new(&bucket_name).await;

    // Create test data
    fixture.put_object("file1.txt", b"content1").await;
    fixture.put_object("file2.txt", b"content2").await;
    fixture.put_object("dir/file3.txt", b"content3").await;

    // Run s3find ls command
    let mut cmd = fixture.s3find_command();
    cmd.arg(fixture.s3_path("")).arg("ls");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("file1.txt"))
        .stdout(predicate::str::contains("file2.txt"))
        .stdout(predicate::str::contains("dir/file3.txt"));
}

#[tokio::test]
async fn test_ls_with_name_filter() {
    let bucket_name = unique_bucket_name("test-ls-name-filter");
    let fixture = LocalStackFixture::new(&bucket_name).await;

    // Create test data
    fixture.put_object("document.txt", b"content").await;
    fixture.put_object("image.png", b"image").await;
    fixture.put_object("archive.txt", b"archive").await;
    fixture.put_object("data.json", b"{}").await;

    // Run s3find with --name filter for *.txt files
    let mut cmd = fixture.s3find_command();
    cmd.arg(fixture.s3_path(""))
        .arg("--name")
        .arg("*.txt")
        .arg("ls");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("document.txt"))
        .stdout(predicate::str::contains("archive.txt"))
        .stdout(predicate::str::contains("image.png").not())
        .stdout(predicate::str::contains("data.json").not());
}

#[tokio::test]
async fn test_ls_with_iname_filter() {
    let bucket_name = unique_bucket_name("test-ls-iname-filter");
    let fixture = LocalStackFixture::new(&bucket_name).await;

    // Create test data with mixed case
    fixture.put_object("Document.TXT", b"content").await;
    fixture.put_object("IMAGE.PNG", b"image").await;
    fixture.put_object("readme.txt", b"readme").await;

    // Run s3find with --iname (case-insensitive) filter
    let mut cmd = fixture.s3find_command();
    cmd.arg(fixture.s3_path(""))
        .arg("--iname")
        .arg("*.txt")
        .arg("ls");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("Document.TXT"))
        .stdout(predicate::str::contains("readme.txt"))
        .stdout(predicate::str::contains("IMAGE.PNG").not());
}

#[tokio::test]
async fn test_ls_with_regex_filter() {
    let bucket_name = unique_bucket_name("test-ls-regex");
    let fixture = LocalStackFixture::new(&bucket_name).await;

    // Create test data
    fixture.put_object("file001.txt", b"content").await;
    fixture.put_object("file002.txt", b"content").await;
    fixture.put_object("document.txt", b"content").await;

    // Run s3find with regex filter for files with numbers
    let mut cmd = fixture.s3find_command();
    cmd.arg(fixture.s3_path(""))
        .arg("--regex")
        .arg(r"file\d+\.txt")
        .arg("ls");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("file001.txt"))
        .stdout(predicate::str::contains("file002.txt"))
        .stdout(predicate::str::contains("document.txt").not());
}

#[tokio::test]
async fn test_print_command() {
    let bucket_name = unique_bucket_name("test-print");
    let fixture = LocalStackFixture::new(&bucket_name).await;

    let test_content = b"Hello from LocalStack!";
    fixture.put_object("test.txt", test_content).await;

    // Run s3find print command (shows metadata, not file contents)
    let mut cmd = fixture.s3find_command();
    cmd.arg(fixture.s3_path("test.txt")).arg("print");

    // Check for the file path (with timestamp-based bucket name) and storage class
    let expected_path = format!("s3://{}/test.txt", bucket_name);
    cmd.assert()
        .success()
        .stdout(predicate::str::contains(&expected_path))
        .stdout(predicate::str::contains("STANDARD"));
}

#[tokio::test]
async fn test_size_filter() {
    let bucket_name = unique_bucket_name("test-size-filter");
    let fixture = LocalStackFixture::new(&bucket_name).await;

    // Create files of different sizes
    fixture.put_object("small.txt", b"small").await; // 5 bytes
    fixture
        .put_object("medium.txt", b"medium file content")
        .await; // 19 bytes
    fixture.put_object("large.txt", &[b'x'; 1000]).await; // 1000 bytes

    // Find files larger than 100 bytes
    let mut cmd = fixture.s3find_command();
    cmd.arg(fixture.s3_path(""))
        .arg("--bytes-size")
        .arg("+100")
        .arg("ls");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("large.txt"))
        .stdout(predicate::str::contains("small.txt").not())
        .stdout(predicate::str::contains("medium.txt").not());
}

#[tokio::test]
async fn test_storage_class_filter() {
    let bucket_name = unique_bucket_name("test-storage-class");
    let fixture = LocalStackFixture::new(&bucket_name).await;

    // Create files with different storage classes
    fixture
        .put_object_with_storage_class(
            "standard.txt",
            b"standard",
            aws_sdk_s3::types::StorageClass::Standard,
        )
        .await;
    fixture
        .put_object_with_storage_class(
            "glacier.txt",
            b"glacier",
            aws_sdk_s3::types::StorageClass::Glacier,
        )
        .await;

    // Find only GLACIER files
    let mut cmd = fixture.s3find_command();
    cmd.arg(fixture.s3_path(""))
        .arg("--storage-class")
        .arg("GLACIER")
        .arg("ls");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("glacier.txt"))
        .stdout(predicate::str::contains("standard.txt").not());
}

#[tokio::test]
async fn test_ls_with_prefix() {
    let bucket_name = unique_bucket_name("test-ls-prefix");
    let fixture = LocalStackFixture::new(&bucket_name).await;

    // Create files in different directories
    fixture.put_object("root.txt", b"root").await;
    fixture.put_object("docs/readme.txt", b"readme").await;
    fixture.put_object("docs/guide.txt", b"guide").await;
    fixture.put_object("images/logo.png", b"logo").await;

    // List only files under docs/ prefix
    let mut cmd = fixture.s3find_command();
    cmd.arg(fixture.s3_path("docs/")).arg("ls");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("readme.txt"))
        .stdout(predicate::str::contains("guide.txt"))
        .stdout(predicate::str::contains("root.txt").not())
        .stdout(predicate::str::contains("logo.png").not());
}

#[tokio::test]
async fn test_combined_filters() {
    let bucket_name = unique_bucket_name("test-combined-filters");
    let fixture = LocalStackFixture::new(&bucket_name).await;

    // Create various test files
    fixture.put_object("data001.txt", &[b'x'; 200]).await;
    fixture.put_object("data002.txt", &[b'x'; 50]).await;
    fixture.put_object("info001.txt", &[b'x'; 300]).await;
    fixture.put_object("readme.md", &[b'x'; 250]).await;

    // Find .txt files matching data* pattern that are larger than 100 bytes
    let mut cmd = fixture.s3find_command();
    cmd.arg(fixture.s3_path(""))
        .arg("--name")
        .arg("data*.txt")
        .arg("--bytes-size")
        .arg("+100")
        .arg("ls");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("data001.txt"))
        .stdout(predicate::str::contains("data002.txt").not())
        .stdout(predicate::str::contains("info001.txt").not())
        .stdout(predicate::str::contains("readme.md").not());
}

#[tokio::test]
async fn test_empty_bucket() {
    let bucket_name = unique_bucket_name("test-empty-bucket");
    let fixture = LocalStackFixture::new(&bucket_name).await;

    // Run s3find on empty bucket
    let mut cmd = fixture.s3find_command();
    cmd.arg(fixture.s3_path("")).arg("ls");

    // Should succeed with no output (or just headers)
    cmd.assert().success();
}

#[tokio::test]
async fn test_nonexistent_prefix() {
    let bucket_name = unique_bucket_name("test-nonexistent");
    let fixture = LocalStackFixture::new(&bucket_name).await;

    fixture.put_object("exists.txt", b"content").await;

    // List with a prefix that doesn't exist
    let mut cmd = fixture.s3find_command();
    cmd.arg(fixture.s3_path("nonexistent/")).arg("ls");

    // Should succeed with no results
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("exists.txt").not());
}
