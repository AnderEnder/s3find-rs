# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

s3find is a Rust command-line utility that provides `find`-like functionality for Amazon S3. It allows users to search, filter, and perform operations on S3 objects using glob patterns, regex, size filters, time filters, and storage class filters.

## Development Commands

### Build and Test
- `cargo build --release` - Build optimized release binary
- `cargo test --lib` - Run unit tests only
- `cargo test --test localstack_integration` - Run integration tests only (requires Docker)
- `cargo test --no-fail-fast --verbose --all -- --nocapture` - Run all tests with verbose output
- `cargo install --path .` - Install from local source

### Code Quality
- `cargo clippy` - Run Clippy linter for code quality checks
- `cargo fmt` - Format code according to Rust style guidelines
- `cargo fmt --check` - Check formatting without making changes

### Development Build
- `cargo build` - Build in development mode (debug symbols disabled in dev profile)
- `cargo run -- --help` - Run the application with help flag to see usage

## Architecture

### Core Modules Structure
- `src/lib.rs` - Library root with module declarations
- `src/bin/s3find.rs` - Main CLI binary entry point
- `src/arg.rs` - Command-line argument parsing and validation
- `src/command.rs` - Command definitions and execution logic
- `src/error.rs` - Error types and handling
- `src/filter.rs` - Object filtering logic (name, size, time, storage class)
- `src/filter_list.rs` - Filter list management and operations
- `src/run.rs` - Core application runtime logic
- `src/run_command.rs` - Command execution orchestration
- `src/utils.rs` - Utility functions and helpers
- `src/adapters/` - AWS S3 SDK integration layer
  - `src/adapters/aws.rs` - AWS S3 client implementation
  - `src/adapters/mod.rs` - Adapter module declarations

### Key Dependencies
- **AWS SDK**: `aws-sdk-s3`, `aws-config`, `aws-types` for S3 operations
- **CLI**: `clap` for argument parsing
- **Async**: `tokio` with full features for async runtime
- **Pattern Matching**: `glob` and `regex` for file matching
- **Serialization**: `serde` and `serde_json` for data handling
- **Progress**: `indicatif` for progress bars
- **Error Handling**: `anyhow` and `thiserror` for error management

### AWS Authentication Flow
The application supports multiple authentication methods in priority order:
1. Command-line credentials (`--aws-access-key`, `--aws-secret-key`)
2. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
3. AWS profile credentials file (via `AWS_PROFILE` and `AWS_SHARED_CREDENTIALS_FILE`)
4. AWS instance IAM profile
5. AWS container IAM profile

### Non-AWS S3 Support
The application supports self-hosted and third-party S3-compatible services:
- `--endpoint-url` - Custom S3 endpoint URL (for MinIO, Ceph, etc.)
- `--force-path-style` - Enable path-style bucket addressing (required for most non-AWS S3 services)

### Command Architecture
The application follows a command pattern where each operation (ls, print, delete, download, copy, move, etc.) is implemented as a separate command with its own logic. Commands are defined in `src/command.rs` and executed through `src/run_command.rs`.

### Filter System
Filters are composable and can be combined:
- Name filters: glob patterns (`--name`) and case-insensitive (`--iname`)
- Regex patterns (`--regex`)
- Size filters (`--bytes-size`) with units (k, M, G, T, P)
- Time filters (`--mtime`) with units (s, m, h, d, w)
- Storage class filters (`--storage-class`)

### Build Configuration
- Uses Rust 2024 edition
- Release builds are optimized with LTO and strip for size
- Development builds have debug symbols disabled for faster compilation
- Build script generates shell completions via `clap_complete`

## Testing

The project includes two types of tests that can be run separately:

### Unit Tests
Located in individual source files under `#[cfg(test)]` modules. Run with:
```bash
cargo test --lib
```

Unit tests cover:
- Core functionality (filters, commands, argument parsing)
- AWS SDK mock utilities via `aws-smithy-runtime` test features
- Utility functions and helpers

### Integration Tests
Located in `tests/localstack_integration.rs`. These tests use LocalStack (a local AWS cloud emulator) to test the complete CLI workflow against a real S3-compatible service.

**Prerequisites:**
- Docker must be installed and running
- Tests automatically start/stop LocalStack via testcontainers

**Running integration tests:**
```bash
# Run only integration tests
cargo test --test localstack_integration

# Run a specific integration test
cargo test --test localstack_integration test_ls_basic

# Run with output
cargo test --test localstack_integration -- --nocapture
```

**Test coverage includes:**
- Basic listing (ls command)
- Name and regex filters (--name, --iname, --regex)
- Size filters (--size)
- Storage class filters (--storage-class)
- Print command
- Combined filters
- Edge cases (empty buckets, nonexistent prefixes)

**CI/CD Integration:**
Integration tests can be run as a separate CI stage:
```yaml
- name: Run integration tests
  run: cargo test --test localstack_integration
```

**Manual LocalStack Testing:**
If you prefer to run LocalStack manually and have integration tests reuse it:
```bash
# Start LocalStack on default port (tests will detect and reuse it)
docker run -d -p 4566:4566 --name localstack localstack/localstack:latest

# Run integration tests (will reuse the running container)
cargo test --test localstack_integration

# Run s3find against LocalStack manually
s3find ls s3://bucket/path \
  --endpoint-url http://localhost:4566 \
  --force-path-style \
  --aws-access-key test \
  --aws-secret-key test

# Stop LocalStack when done
docker stop localstack && docker rm localstack
```

**Note:** Integration tests will automatically detect and reuse a LocalStack container running on port 4566. If no container is found, tests will start their own (which persists after tests complete for reuse in subsequent runs).

**Using with Podman:**
The integration tests work with Podman as a Docker alternative:

Linux:
```bash
# Enable Podman socket
systemctl --user enable --now podman.socket
export DOCKER_HOST=unix:///run/user/$UID/podman/podman.sock

# Run tests
cargo test --test localstack_integration
```

macOS/Windows (Podman Machine):
```bash
# Start Podman machine (automatically sets DOCKER_HOST)
podman machine init
podman machine start

# Run tests
cargo test --test localstack_integration
```

## CI/CD

The project uses GitHub Actions for:
- **Build**: Multi-platform builds (Linux, macOS, Windows, ARM)
- **Code Quality**: Clippy linting and rustfmt formatting checks
- **Coverage**: Code coverage reporting via codecov
- **Release**: Automated releases with cross-platform binaries