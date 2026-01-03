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

    /// Enable versioning on the bucket
    async fn enable_versioning(&self) {
        self.client
            .put_bucket_versioning()
            .bucket(&self.bucket)
            .versioning_configuration(
                aws_sdk_s3::types::VersioningConfiguration::builder()
                    .status(aws_sdk_s3::types::BucketVersioningStatus::Enabled)
                    .build(),
            )
            .send()
            .await
            .expect("Failed to enable versioning");
    }

    /// Delete an object (creates a delete marker when versioning is enabled)
    async fn delete_object(&self, key: &str) {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .expect("Failed to delete object");
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

#[tokio::test]
async fn test_maxdepth_zero() {
    let bucket_name = unique_bucket_name("test-maxdepth-zero");
    let fixture = LocalStackFixture::new(&bucket_name).await;

    // Create hierarchical structure
    fixture.put_object("root.txt", b"root").await;
    fixture.put_object("dir1/file.txt", b"level1").await;
    fixture.put_object("dir2/file.txt", b"level1").await;
    fixture.put_object("dir1/subdir/deep.txt", b"level2").await;

    // maxdepth 0 should only return objects at root level (no subdirectories)
    let mut cmd = fixture.s3find_command();
    cmd.arg(fixture.s3_path(""))
        .arg("--maxdepth")
        .arg("0")
        .arg("ls");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("root.txt"))
        .stdout(predicate::str::contains("dir1/file.txt").not())
        .stdout(predicate::str::contains("dir2/file.txt").not())
        .stdout(predicate::str::contains("dir1/subdir/deep.txt").not());
}

#[tokio::test]
async fn test_maxdepth_one() {
    let bucket_name = unique_bucket_name("test-maxdepth-one");
    let fixture = LocalStackFixture::new(&bucket_name).await;

    // Create hierarchical structure
    fixture.put_object("root.txt", b"root").await;
    fixture.put_object("dir1/file.txt", b"level1").await;
    fixture.put_object("dir2/file.txt", b"level1").await;
    fixture.put_object("dir1/subdir/deep.txt", b"level2").await;
    fixture
        .put_object("dir2/subdir/another.txt", b"level2")
        .await;

    // maxdepth 1 should return root + one level of subdirectories
    let mut cmd = fixture.s3find_command();
    cmd.arg(fixture.s3_path(""))
        .arg("--maxdepth")
        .arg("1")
        .arg("ls");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("root.txt"))
        .stdout(predicate::str::contains("dir1/file.txt"))
        .stdout(predicate::str::contains("dir2/file.txt"))
        .stdout(predicate::str::contains("dir1/subdir/deep.txt").not())
        .stdout(predicate::str::contains("dir2/subdir/another.txt").not());
}

#[tokio::test]
async fn test_maxdepth_two() {
    let bucket_name = unique_bucket_name("test-maxdepth-two");
    let fixture = LocalStackFixture::new(&bucket_name).await;

    // Create deep hierarchical structure
    fixture.put_object("root.txt", b"root").await;
    fixture.put_object("dir1/file.txt", b"level1").await;
    fixture.put_object("dir1/subdir/deep.txt", b"level2").await;
    fixture
        .put_object("dir1/subdir/deeper/verydeep.txt", b"level3")
        .await;

    // maxdepth 2 should return up to two levels of subdirectories
    let mut cmd = fixture.s3find_command();
    cmd.arg(fixture.s3_path(""))
        .arg("--maxdepth")
        .arg("2")
        .arg("ls");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("root.txt"))
        .stdout(predicate::str::contains("dir1/file.txt"))
        .stdout(predicate::str::contains("dir1/subdir/deep.txt"))
        .stdout(predicate::str::contains("dir1/subdir/deeper/verydeep.txt").not());
}

#[tokio::test]
async fn test_maxdepth_with_prefix() {
    let bucket_name = unique_bucket_name("test-maxdepth-prefix");
    let fixture = LocalStackFixture::new(&bucket_name).await;

    // Create structure under a prefix
    fixture.put_object("data/file.txt", b"data").await;
    fixture
        .put_object("data/subdir/nested.txt", b"nested")
        .await;
    fixture
        .put_object("data/subdir/deep/verydeep.txt", b"deep")
        .await;
    fixture.put_object("other/file.txt", b"other").await;

    // Depth is counted from bucket root, so from data/ prefix:
    // - data/ is at depth 1
    // - data/file.txt is at depth 1 (under data/)
    // - data/subdir/nested.txt is at depth 2 (under data/subdir/)
    // maxdepth 2 from data/ prefix includes up to depth 2
    let mut cmd = fixture.s3find_command();
    cmd.arg(fixture.s3_path("data/"))
        .arg("--maxdepth")
        .arg("2")
        .arg("ls");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("data/file.txt"))
        .stdout(predicate::str::contains("data/subdir/nested.txt"))
        .stdout(predicate::str::contains("data/subdir/deep/verydeep.txt").not())
        .stdout(predicate::str::contains("other/file.txt").not());
}

#[tokio::test]
async fn test_maxdepth_with_name_filter() {
    let bucket_name = unique_bucket_name("test-maxdepth-filter");
    let fixture = LocalStackFixture::new(&bucket_name).await;

    // Create hierarchical structure with mixed file types
    fixture.put_object("root.txt", b"root").await;
    fixture.put_object("root.log", b"log").await;
    fixture.put_object("dir1/file.txt", b"level1").await;
    fixture.put_object("dir1/file.log", b"level1").await;
    fixture.put_object("dir1/subdir/deep.txt", b"level2").await;

    // maxdepth 1 + name filter for *.txt
    let mut cmd = fixture.s3find_command();
    cmd.arg(fixture.s3_path(""))
        .arg("--maxdepth")
        .arg("1")
        .arg("--name")
        .arg("*.txt")
        .arg("ls");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("root.txt"))
        .stdout(predicate::str::contains("dir1/file.txt"))
        .stdout(predicate::str::contains("root.log").not())
        .stdout(predicate::str::contains("dir1/file.log").not())
        .stdout(predicate::str::contains("dir1/subdir/deep.txt").not());
}

#[tokio::test]
async fn test_maxdepth_empty_subdirectories() {
    let bucket_name = unique_bucket_name("test-maxdepth-empty");
    let fixture = LocalStackFixture::new(&bucket_name).await;

    // Create structure where some subdirectories are "empty" at certain depths
    // (i.e., they only contain deeper objects, not objects at their level)
    fixture.put_object("root.txt", b"root").await;
    fixture
        .put_object("empty_at_level1/subdir/file.txt", b"deep")
        .await;
    fixture.put_object("has_files/file.txt", b"level1").await;

    // maxdepth 1 should still traverse empty_at_level1/ but find no objects there
    let mut cmd = fixture.s3find_command();
    cmd.arg(fixture.s3_path(""))
        .arg("--maxdepth")
        .arg("1")
        .arg("ls");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("root.txt"))
        .stdout(predicate::str::contains("has_files/file.txt"))
        .stdout(predicate::str::contains("empty_at_level1/subdir/file.txt").not());
}

#[tokio::test]
async fn test_all_versions_basic() {
    let bucket_name = unique_bucket_name("test-versions-basic");
    let fixture = LocalStackFixture::new(&bucket_name).await;

    // Enable versioning on the bucket
    fixture.enable_versioning().await;

    // Create multiple versions of the same object
    fixture.put_object("file.txt", b"version 1").await;
    fixture.put_object("file.txt", b"version 2").await;
    fixture.put_object("file.txt", b"version 3").await;

    // Without --all-versions, should only see the latest version (1 entry)
    let mut cmd = fixture.s3find_command();
    cmd.arg(fixture.s3_path("")).arg("ls");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("file.txt"));

    // With --all-versions, should see all 3 versions
    let mut cmd_versions = fixture.s3find_command();
    cmd_versions
        .arg(fixture.s3_path(""))
        .arg("--all-versions")
        .arg("ls");

    let output = cmd_versions.output().expect("Failed to execute command");
    let stdout = String::from_utf8_lossy(&output.stdout);

    // Count occurrences of file.txt in output (should be exactly 3 versions)
    let count = stdout.matches("file.txt").count();
    assert_eq!(
        3, count,
        "Expected exactly 3 versions, found {} occurrences of file.txt in output:\n{}",
        count, stdout
    );

    // Verify that version IDs are present and distinct
    let version_ids: Vec<&str> = stdout
        .lines()
        .filter_map(|line| {
            if line.contains("file.txt") && line.contains("versionId=") {
                // Extract versionId from line like "file.txt?versionId=abc123"
                line.split("versionId=")
                    .nth(1)
                    .and_then(|s| s.split_whitespace().next())
            } else {
                None
            }
        })
        .collect();

    assert_eq!(
        3,
        version_ids.len(),
        "Expected 3 version IDs, found {}: {:?}",
        version_ids.len(),
        version_ids
    );

    // Check that all version IDs are distinct
    let mut unique_ids = version_ids.clone();
    unique_ids.sort();
    unique_ids.dedup();
    assert_eq!(
        version_ids.len(),
        unique_ids.len(),
        "Version IDs should be distinct, found duplicates: {:?}",
        version_ids
    );
}

#[tokio::test]
async fn test_all_versions_with_delete_markers() {
    let bucket_name = unique_bucket_name("test-versions-delete");
    let fixture = LocalStackFixture::new(&bucket_name).await;

    // Enable versioning
    fixture.enable_versioning().await;

    // Create object and then delete it (creates delete marker)
    fixture.put_object("deleted.txt", b"original").await;
    fixture.delete_object("deleted.txt").await;

    // Also create another object with multiple versions
    fixture.put_object("kept.txt", b"v1").await;
    fixture.put_object("kept.txt", b"v2").await;

    // Without --all-versions, deleted.txt should not appear (hidden by delete marker)
    let mut cmd = fixture.s3find_command();
    cmd.arg(fixture.s3_path("")).arg("ls");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("kept.txt"))
        .stdout(predicate::str::contains("deleted.txt").not());

    // With --all-versions, should see deleted.txt (both original and delete marker)
    let mut cmd_versions = fixture.s3find_command();
    cmd_versions
        .arg(fixture.s3_path(""))
        .arg("--all-versions")
        .arg("ls");

    cmd_versions
        .assert()
        .success()
        .stdout(predicate::str::contains("kept.txt"))
        .stdout(predicate::str::contains("deleted.txt"));
}

#[tokio::test]
async fn test_all_versions_with_name_filter() {
    let bucket_name = unique_bucket_name("test-versions-filter");
    let fixture = LocalStackFixture::new(&bucket_name).await;

    // Enable versioning
    fixture.enable_versioning().await;

    // Create versioned objects with different extensions
    fixture.put_object("doc.txt", b"v1").await;
    fixture.put_object("doc.txt", b"v2").await;
    fixture.put_object("image.png", b"v1").await;
    fixture.put_object("image.png", b"v2").await;

    // With --all-versions and --name filter
    let mut cmd = fixture.s3find_command();
    cmd.arg(fixture.s3_path(""))
        .arg("--all-versions")
        .arg("--name")
        .arg("*.txt")
        .arg("ls");

    let output = cmd.output().expect("Failed to execute command");
    let stdout = String::from_utf8_lossy(&output.stdout);

    // Should see doc.txt versions but not image.png
    assert!(
        stdout.contains("doc.txt"),
        "Expected doc.txt in output:\n{}",
        stdout
    );
    assert!(
        !stdout.contains("image.png"),
        "Expected no image.png in output:\n{}",
        stdout
    );

    // Count doc.txt occurrences (should be at least 2)
    let count = stdout.matches("doc.txt").count();
    assert!(
        count >= 2,
        "Expected at least 2 versions of doc.txt, found {}",
        count
    );
}

#[tokio::test]
async fn test_all_versions_with_maxdepth_warning() {
    let bucket_name = unique_bucket_name("test-versions-maxdepth");
    let fixture = LocalStackFixture::new(&bucket_name).await;

    // Enable versioning
    fixture.enable_versioning().await;

    // Create versioned objects at different depths
    fixture.put_object("root.txt", b"v1").await;
    fixture.put_object("root.txt", b"v2").await;
    fixture.put_object("dir/nested.txt", b"v1").await;
    fixture.put_object("dir/nested.txt", b"v2").await;

    // When both --all-versions and --maxdepth are used, --all-versions takes precedence
    // and a warning should be printed to stderr
    let mut cmd = fixture.s3find_command();
    cmd.arg(fixture.s3_path(""))
        .arg("--all-versions")
        .arg("--maxdepth")
        .arg("0")
        .arg("ls");

    let output = cmd.output().expect("Failed to execute command");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    // Should see all versions at all depths (maxdepth ignored)
    assert!(
        stdout.contains("root.txt"),
        "Expected root.txt in output:\n{}",
        stdout
    );
    assert!(
        stdout.contains("dir/nested.txt"),
        "Expected dir/nested.txt in output (maxdepth should be ignored):\n{}",
        stdout
    );

    // Should see warning in stderr
    assert!(
        stderr.contains("--maxdepth is ignored when --all-versions is used"),
        "Expected warning about maxdepth being ignored in stderr:\n{}",
        stderr
    );

    // Count versions - should have at least 4 entries (2 versions each of 2 files)
    let root_count = stdout.matches("root.txt").count();
    let nested_count = stdout.matches("dir/nested.txt").count();
    assert!(
        root_count >= 2 && nested_count >= 2,
        "Expected at least 2 versions of each file, found root.txt={}, nested.txt={}",
        root_count,
        nested_count
    );
}
