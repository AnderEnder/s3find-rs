# s3find
[![build status](https://github.com/AnderEnder/s3find-rs/workflows/Build/badge.svg)](https://github.com/AnderEnder/s3find-rs/actions)
[![release status](https://github.com/AnderEnder/s3find-rs/workflows/Release/badge.svg)](https://github.com/AnderEnder/s3find-rs/actions)
[![codecov](https://codecov.io/gh/AnderEnder/s3find-rs/branch/master/graph/badge.svg)](https://codecov.io/gh/AnderEnder/s3find-rs)
[![crates.io](https://img.shields.io/crates/v/s3find.svg)](https://crates.io/crates/s3find)

A powerful command line utility to walk an Amazon S3 hierarchy. Think of it as the `find` command but specifically designed for Amazon S3.

## Table of Contents
- [Installation](#installation)
  - [Pre-built Binaries](#pre-built-binaries)
  - [Build from Source](#build-from-source)
- [Development](#development)
  - [Checks](#checks)
  - [Behavior Notes](#behavior-notes)
- [Usage](#usage)
  - [Basic Syntax](#basic-syntax)
  - [Authentication Methods](#authentication-methods)
  - [Using with Non-AWS S3-Compatible Services](#using-with-non-aws-s3-compatible-services)
  - [Scripting and AI Agents](#scripting-and-ai-agents)
- [Examples](#examples)
  - [Finding Files](#finding-files-by-glob-pattern)
  - [Filter by Size](#find-path-by-size)
  - [Filter by Time](#find-path-by-time)
  - [Filter by Storage Class](#object-storage-class-filter)
  - [Filter by Tags](#filter-by-tags)
  - [Multiple Filters](#multiple-filters)
  - [Actions and Operations](#actions-and-operations)
- [Advanced Options](#additional-control)
  - [Depth Control](#depth-control)
  - [Object Versioning](#object-versioning)
  - [Tag Filtering Performance](#tag-filtering-performance)

## Installation

### Pre-built Binaries

Github Release page provides ready-to-use binaries for:

* Windows (x86_64)
* Linux (x86_64 and ARM), in two flavors:
  * `s3find-<arch>-linux.tar.gz`: a static musl binary that runs on any Linux, including Alpine and scratch containers
  * `s3find-<arch>-linux-gnu.tar.gz`: a glibc binary for distributions with glibc 2.17 or newer (RHEL/CentOS 7, Ubuntu 14.04, Debian 8 and later)
* macOS (x86_64 and ARM)
* FreeBSD 14 or newer (x86_64)

Binaries for both architectures allow you to run s3find natively on Intel-based and ARM-based machines (like Apple M1/M2/M3/M4, AWS Graviton, and Raspberry Pi).

### Build from Source

Requirements: Rust 1.99 or newer (`rustup update stable`)

```sh
# Build
cargo build --release

# Install from local source
cargo install --path .

# Install latest from git
cargo install --git https://github.com/AnderEnder/s3find-rs

# Install from crates.io
cargo install s3find
```

The release workflow builds the Linux and FreeBSD binaries with [cargo-zigbuild](https://github.com/rust-cross/cargo-zigbuild), which needs [zig](https://ziglang.org/):

```sh
# Static musl binary; the mimalloc feature replaces musl's slow allocator
cargo zigbuild --release --target x86_64-unknown-linux-musl --features mimalloc

# glibc binary that runs on glibc 2.17 or newer
cargo zigbuild --release --target x86_64-unknown-linux-gnu.2.17

# FreeBSD 14 or newer
cargo zigbuild --release --target x86_64-unknown-freebsd
```

## Development

### Checks

```sh
# Fast unit and module tests
cargo test --lib -- --nocapture

# Full default test suite (without opt-in LocalStack integration tests)
cargo test --all -- --nocapture

# LocalStack integration tests (opt-in; requires Docker or Podman socket)
cargo test --features localstack-tests --test localstack_integration -- --nocapture

# Lint
cargo clippy --all-targets --all-features -- -D warnings
```

### Behavior Notes

- Listing failures and subcommand failures return a non-zero exit code instead of succeeding with partial output.
- `download` skips existing destination files and continues with the remaining objects unless `--force` is set.
- With tag filters enabled, result order stays aligned with S3 traversal order, and `--limit` applies to that ordered match set.
- `--summarize` prints normal summary statistics and, when tag filters are used, tag fetch statistics including `excluded` objects.
- Commands that make one S3 request per object (`copy`, `move`, `download`, `tags`, `lstags`, `public`, `restore`, `change-storage`) keep up to `--concurrency` requests in flight (default: 32) and still print results in traversal order. On an error they stop, but requests already in flight may have completed; `--concurrency 1` processes one object at a time. `exec` always runs one command at a time.
- Listing runs up to one batch (1000 objects) ahead of the command, and `--maxdepth` fetches the first page of up to 8 subdirectories per level at once.

## Usage

### Basic Syntax

```sh
s3find [OPTIONS] <s3-path> [COMMAND]
```

Where:
- `<s3-path>` is formatted as `s3://bucket/path`
- `[OPTIONS]` are filters and controls
- `[COMMAND]` is the action to perform on matched objects

### Authentication Methods

s3find supports multiple AWS authentication methods in the following priority:

1. Command-line credentials (`--aws-access-key` and `--aws-secret-key`)
2. Environment variables (`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`)
3. AWS profile credentials file (configurable via `AWS_PROFILE` and `AWS_SHARED_CREDENTIALS_FILE`)
4. AWS instance IAM profile
5. AWS container IAM profile

### Using with Non-AWS S3-Compatible Services

s3find supports self-hosted and third-party S3-compatible services such as MinIO, Ceph, and others. Use the following options to connect to these services:

- `--endpoint-url <URL>` - Custom S3 endpoint URL
- `--force-path-style` - Use path-style bucket addressing (required for most non-AWS S3 services)

#### MinIO Example

```sh
s3find 's3://mybucket/path' \
  --endpoint-url 'http://localhost:9000' \
  --force-path-style \
  --aws-access-key 'minioadmin' \
  --aws-secret-key 'minioadmin' \
  --aws-region 'us-east-1' \
  ls
```

#### Ceph Example

```sh
s3find 's3://mybucket/data' \
  --endpoint-url 'https://ceph.example.com' \
  --force-path-style \
  --aws-access-key 'your-access-key' \
  --aws-secret-key 'your-secret-key' \
  --name '*.log' \
  print
```

### Command Line Reference

Use `s3find --help` for the top-level reference and `s3find <path> <command> --help` for subcommand syntax. Commonly used commands:

```sh
s3find --help
s3find s3://example-bucket/path print --help
s3find s3://example-bucket/path copy --help
```

### Scripting and AI Agents

s3find is built to be driven by scripts and AI agents as well as by people:

- **Results on stdout, everything else on stderr.** Each result is one line; warnings, progress bars and errors go to stderr. Progress bars are hidden when stderr isn't a terminal.
- **`--json`** writes [JSON Lines](https://jsonlines.org/): one JSON object per result.
- **`--dry-run`** shows what a command would change without changing anything.
- **Exit status:** `0` on success (including when nothing matched), `1` when listing or a command failed, `2` for invalid arguments.
- `--json` and `--dry-run` may be given before or after the command, and `--limit` caps how many objects are processed.

```sh
# Preview a cleanup, then run it
s3find 's3://example-bucket/tmp' --name '*.tmp' --mtime +7d delete --dry-run
s3find 's3://example-bucket/tmp' --name '*.tmp' --mtime +7d delete

# Machine-readable listing
s3find 's3://example-bucket/logs' --limit 100 --json ls | jq -r .url
```

With `--json`, `ls` and `print` write one object per line:

```json
{"bucket":"example-bucket","url":"s3://example-bucket/logs/app.log","e_tag":"\"9d48114aa7c18f9d68aa20086dbb7756\"","owner":"","size":4997288,"last_modified":"2024-07-19T19:04:17Z","key":"logs/app.log","storage_class":"STANDARD"}
```

Versioned listings (`--all-versions`) add `version_id`, `is_latest` and, for delete markers, `is_delete_marker`. `lstags` writes `bucket`, `key`, `url` and `tags` (an object such as `{"env":"prod"}`).

Commands that change objects write one line per object with an `action` field, the object's `bucket`, `key`, `url` (and `version_id` when listing versions), plus:

| `action` | Extra fields |
| --- | --- |
| `delete` | |
| `copy`, `move` | `destination` |
| `download` | `path`, `skipped` (the file existed and `--force` wasn't given) |
| `tags` | `tags` |
| `public` | `public_url` |
| `restore` | `status`: `started`, `already_in_progress` or `invalid_object_state` |
| `change-storage` | `storage_class` |
| `exec` | `command`, `stdout` |

```json
{"action":"copy","bucket":"example-bucket","key":"data/a.csv","url":"s3://example-bucket/data/a.csv","destination":"s3://backup-bucket/data/a.csv"}
```

Under `--dry-run`, these lines have `"dry_run": true` (text lines start with `[dry-run] `), and `restore` has no `status` and `exec` no `stdout`. With `--summarize`, the last line is `{"summary": {"total_files": ..., "total_space": ..., "max_size": ..., "max_key": ..., "min_size": ..., "min_key": ..., "average_size": ...}}`, plus `tag_fetch` counters when filtering on tags. Errors are written to stderr as `{"error": "..."}`.

## Examples

### Finding Files by Glob Pattern

Use the `--name` option with glob patterns to match objects:

```sh
# Find all objects in a path
s3find 's3://example-bucket/example-path' --name '*' ls

# Find objects with specific extension
s3find 's3://example-bucket/example-path' --name '*.json' ls
```

### Output Formats

The `print` command supports different output formats:

```sh
# Default format
s3find 's3://example-bucket/example-path' --name '*' print

# Text format
s3find 's3://example-bucket/example-path' --name '*' print --format text

# JSON format
s3find 's3://example-bucket/example-path' --name '*' print --format json

# CSV format
s3find 's3://example-bucket/example-path' --name '*' print --format csv
```

### Case Insensitive Search

Use `--iname` for case-insensitive glob pattern matching:

```sh
s3find 's3://example-bucket/example-path' --iname '*data*' ls
```

### Regex Pattern Matching

Use `--regex` for regular expression pattern matching:

```sh
# Find objects ending with a number
s3find 's3://example-bucket/example-path' --regex '[0-9]$' print
```

### Find Path by Size

```sh
# Exact match - files exactly 0 bytes
s3find 's3://example-bucket/example-path' --bytes-size 0 print

# Strictly larger than 10 megabytes
s3find 's3://example-bucket/example-path' --bytes-size +10M print

# Strictly smaller than 10 kilobytes
s3find 's3://example-bucket/example-path' --bytes-size -10k print
```

### Find Path by Time

```sh
# Files modified in the last 10 seconds
s3find 's3://example-bucket/example-path' --mtime -10s print

# Files modified more than 10 minutes ago
s3find 's3://example-bucket/example-path' --mtime +10m print

# Files modified in the last 10 hours
s3find 's3://example-bucket/example-path' --mtime -10h print
```

### Object Storage Class Filter

Filter objects by their storage class:

```bash
# Find objects in STANDARD storage class
s3find 's3://example-bucket/example-path' --storage-class STANDARD print

# Find objects in GLACIER storage class
s3find 's3://example-bucket/example-path' --storage-class GLACIER print
```

### Filter by Tags

Filter objects based on their S3 tags using `--tag` for key-value matching or `--tag-exists` for key presence:

```bash
# Find objects with a specific tag key and value
s3find 's3://example-bucket/example-path' --tag 'environment=production' ls

# Find objects with multiple tags (AND logic - all must match)
s3find 's3://example-bucket/example-path' --tag 'environment=production' --tag 'team=data' ls

# Find objects that have a specific tag key (any value)
s3find 's3://example-bucket/example-path' --tag-exists 'owner' ls

# Combine tag filters with other filters
s3find 's3://example-bucket/example-path' --name '*.log' --tag 'retention=long-term' ls

# Control concurrency for tag fetching (default: 50)
s3find 's3://example-bucket/example-path' --tag 'env=prod' --tag-concurrency 100 ls
```

**Required IAM Permissions:**
Tag filtering requires the `s3:GetObjectTagging` permission on the objects being searched.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket",
        "s3:GetObjectTagging"
      ],
      "Resource": [
        "arn:aws:s3:::example-bucket",
        "arn:aws:s3:::example-bucket/*"
      ]
    }
  ]
}
```

### Multiple Filters

Combine filters to create more specific queries:

```sh
# Files between 10 and 20 bytes
s3find 's3://example-bucket/example-path' --bytes-size +10 --bytes-size -20 print

# Combine different filter types
s3find 's3://example-bucket/example-path' --bytes-size +10M --name '*backup*' print
```

### Actions and Operations

#### Delete Objects

```sh
s3find 's3://example-bucket/example-path' --name '*.tmp' delete
```

#### List Objects and Tags

```sh
# List objects
s3find 's3://example-bucket/example-path' --name '*' ls

# List objects with their tags
s3find 's3://example-bucket/example-path' --name '*' lstags
```

#### Execute Commands on Objects

```sh
s3find 's3://example-bucket/example-path' --name '*' exec --utility 'echo {}'
```

#### Download Objects

```sh
s3find 's3://example-bucket/example-path' --name '*.pdf' download ./downloads
```

#### Copy and Move Operations

```sh
# Copy files to another location
s3find 's3://example-bucket/example-path' --name '*.dat' copy 's3://example-bucket/example-path2'

# Move files to another location
s3find 's3://example-bucket/example-path' --name '*.dat' move 's3://example-bucket/example-path2'

# Flatten keys while copying
s3find 's3://example-bucket/example-path' --name '*.dat' copy --flat 's3://example-bucket/example-path2'

# Copy with up to 100 requests in flight (default: 32)
s3find 's3://example-bucket/example-path' --concurrency 100 copy 's3://example-bucket/example-path2'
```

#### Tag Management

```sh
s3find 's3://example-bucket/example-path' --name '*archive*' tags 'key:value' 'env:staging'
```

#### Make Objects Public

```sh
s3find 's3://example-bucket/example-path' --name '*.public.html' public
```

## Additional Control

Control the number of results and request behavior:

```sh
# Limit to first 10 matching objects
s3find 's3://example-bucket/example-path' --name '*' --limit 10 ls

# Control page size for S3 API requests
s3find 's3://example-bucket/example-path' --name '*' --number 100 ls
```

### Depth Control

Limit how deep s3find descends into the object hierarchy:

```sh
# Only objects at the bucket root level (no subdirectories)
s3find 's3://example-bucket/' --maxdepth 0 ls

# Objects up to one subdirectory level deep
s3find 's3://example-bucket/' --maxdepth 1 ls

# Objects directly inside data/ and one subdirectory below it
s3find 's3://example-bucket/data/' --maxdepth 2 ls
```

An object's depth is the number of `/` in its key after the prefix. Trailing slashes in the path are ignored, so for `s3://example-bucket/data/` the objects directly inside `data/` are at depth 1 and `data/2024/report.csv` is at depth 2.

The `--maxdepth` option uses S3's delimiter-based traversal for efficient server-side filtering, avoiding the need to fetch objects beyond the specified depth.

### Object Versioning

List all versions of objects in versioned buckets:

```sh
# List all versions of all objects
s3find 's3://example-bucket/' --all-versions ls

# List all versions matching a pattern
s3find 's3://example-bucket/' --all-versions --name '*.log' ls

# Print all versions in JSON format
s3find 's3://example-bucket/' --all-versions print --format json
```

When `--all-versions` is enabled:
- Uses the S3 ListObjectVersions API instead of ListObjectsV2
- Shows all versions of each object, not just the current version
- Includes delete markers (shown with size 0)
- Each entry displays the version ID alongside the key (e.g., `file.txt?versionId=abc123`); the underlying S3 key remains unchanged
- The latest version is marked with `(latest)`
- Delete markers are marked with `(delete marker)`

**Version-aware operations:** When using `--all-versions`, operations work on specific versions:
- `delete` - Deletes specific object versions (not just the current version)
- `copy`/`move` - Copies or moves specific versions to the destination
- `download` - Downloads specific versions of objects

**Note:** Delete markers are automatically skipped for operations that don't support them (copy, move, download, tags, restore, change-storage, public) since they have no content.

**Note:** `--all-versions` is not compatible with `--maxdepth`. If both are specified, `--all-versions` takes precedence and `--maxdepth` is ignored.

### Tag Filtering Performance

Tag filtering requires an individual `GetObjectTagging` API call for each object that passes the other filters. To optimize performance and cost:

1. **Apply other filters first**: Use `--name`, `--mtime`, `--bytes-size`, or `--storage-class` filters to reduce the number of objects before tag filtering is applied.

2. **Use `--limit`**: When testing or exploring, use `--limit` to cap the number of objects processed.

3. **Adjust concurrency**: Use `--tag-concurrency` to tune parallel API calls (default: 50). Higher values increase throughput but may cause throttling.

4. **Cost awareness**: `GetObjectTagging` requests incur S3 API charges that vary by region. For large buckets with millions of objects, apply filters to reduce the number of tag fetch operations. Refer to the [AWS S3 pricing documentation](https://aws.amazon.com/s3/pricing/) for current rates.

**Example: Optimized tag filtering**

```bash
# Bad: Fetches tags for ALL objects (expensive for large buckets)
s3find 's3://large-bucket/' --tag 'env=prod' ls

# Good: Apply cheap filters first, then tag filter
s3find 's3://large-bucket/' --name '*.log' --mtime -7d --tag 'env=prod' ls

# Good: Use limit when exploring
s3find 's3://large-bucket/' --tag 'env=prod' --limit 100 ls
```

When `--summarize` is enabled, tag fetch statistics are displayed showing success, failed, throttled, access denied, and excluded counts.

For more information, see the [GitHub repository](https://github.com/AnderEnder/s3find-rs).
