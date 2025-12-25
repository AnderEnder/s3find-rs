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
- [Usage](#usage)
  - [Basic Syntax](#basic-syntax)
  - [Authentication Methods](#authentication-methods)
  - [Using with Non-AWS S3-Compatible Services](#using-with-non-aws-s3-compatible-services)
- [Examples](#examples)
  - [Finding Files](#finding-files-by-glob-pattern)
  - [Filter by Size](#find-path-by-size)
  - [Filter by Time](#find-path-by-time)
  - [Filter by Storage Class](#object-storage-class-filter)
  - [Multiple Filters](#multiple-filters)
  - [Actions and Operations](#actions-and-operations)
- [Advanced Options](#additional-control)
  - [Depth Control](#depth-control)
  - [Object Versioning](#object-versioning)

## Installation

### Pre-built Binaries

Github Release page provides ready-to-use binaries for:

* Windows (x86_64)
* Linux (x86_64 and ARM)
* macOS (x86_64 and ARM)

Binaries for both architectures allow you to run s3find natively on Intel-based and ARM-based machines (like Apple M1/M2/M3/M4, AWS Graviton, and Raspberry Pi).

### Build from Source

Requirements: Rust and Cargo

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

```sh
Walk an Amazon S3 path hierarchy

The authorization flow is the following chain:
  * use credentials from arguments provided by users
  * use environment variable credentials: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
  * use credentials via aws file profile.
    Profile can be set via environment variable AWS_PROFILE
    Profile file can be set via environment variable AWS_SHARED_CREDENTIALS_FILE
  * use AWS instance IAM profile
  * use AWS container IAM profile


Usage: s3find [OPTIONS] <path> [COMMAND]

Commands:
  exec      Exec any shell program with every key
  print     Extended print with detail information
  delete    Delete matched keys
  download  Download matched keys
  copy      Copy matched keys to a s3 destination
  move      Move matched keys to a s3 destination
  ls        Print the list of matched keys
  lstags    Print the list of matched keys with tags
  tags      Set the tags(overwrite) for the matched keys
  public    Make the matched keys public available (readonly)
  restore   Restore objects from Glacier and Deep Archive storage
  change-storage  Change storage class of matched objects and move objects to Glacier or Deep Archive
  nothing   Do not do anything with keys, do not print them as well
  help      Print this message or the help of the given subcommand(s)

Arguments:
  <path>
          S3 path to walk through. It should be s3://bucket/path

Options:
      --aws-access-key <aws-access-key>
          AWS access key. Unrequired

      --aws-secret-key <aws-secret-key>
          AWS secret key from AWS credential pair. Required only for the credential based authentication

      --aws-region <aws-region>
          [default: us-east-1]

      --name <pattern>
          Glob pattern for match, can be multiple

      --iname <ipattern>
          Case-insensitive glob pattern for match, can be multiple

      --regex <rpattern>
          Regex pattern for match, can be multiple

      --storage-class <storage-class>
          Object storage class for match
          Valid values are:
              DEEP_ARCHIVE
              EXPRESS_ONEZONE
              GLACIER
              GLACIER_IR
              INTELLIGENT_TIERING
              ONEZONE_IA
              OUTPOSTS
              REDUCED_REDUNDANCY
              SNOW
              STANDARD
              STANDARD_IA
              Unknown values are also supported

      --mtime <time>
          Modification time for match, a time period:
              -5d - for period from now-5d to now
              +5d - for period before now-5d

          Possible time units are as follows:
              s - seconds
              m - minutes
              h - hours
              d - days
              w - weeks

          Can be multiple, but should be overlaping

      --bytes-size <bytes-size>
          File size for match:
              5k - exact match 5k,
              +5k - bigger than 5k,
              -5k - smaller than 5k,

          Possible file size units are as follows:
              k - kilobytes (1024 bytes)
              M - megabytes (1024 kilobytes)
              G - gigabytes (1024 megabytes)
              T - terabytes (1024 gigabytes)
              P - petabytes (1024 terabytes)

      --limit <limit>
          Limit result

      --number <number>
          The number of results to return in each response to a
          list operation. The default value is 1000 (the maximum
          allowed). Using a lower value may help if an operation
          times out.

          [default: 1000]

  -s, --summarize
          Print summary statistic

  -h, --help
          Print help (see a summary with '-h')

  -V, --version
          Print version
```

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
s3find 's3://example-bucket/example-path' --size 0 print

# Larger than 10 megabytes
s3find 's3://example-bucket/example-path' --size +10M print

# Smaller than 10 kilobytes
s3find 's3://example-bucket/example-path' --size -10k print
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

### Multiple Filters

Combine filters to create more specific queries:

```sh
# Files between 10 and 20 bytes
s3find 's3://example-bucket/example-path' --size +10 --size -20 print

# Combine different filter types
s3find 's3://example-bucket/example-path' --size +10M --name '*backup*' print
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
s3find 's3://example-bucket/example-path' --name '*' exec 'echo {}'
```

#### Download Objects

```sh
s3find 's3://example-bucket/example-path' --name '*.pdf' download
```

#### Copy and Move Operations

```sh
# Copy files to another location
s3find 's3://example-bucket/example-path' --name '*.dat' copy -f 's3://example-bucket/example-path2'

# Move files to another location
s3find 's3://example-bucket/example-path' --name '*.dat' move -f 's3://example-bucket/example-path2'
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

# Objects up to two levels deep
s3find 's3://example-bucket/data/' --maxdepth 2 ls
```

The `--maxdepth` option uses S3's delimiter-based traversal for efficient server-side filtering, avoiding the need to fetch objects beyond the specified depth.

### Object Versioning

List all versions of objects in versioned buckets:

```sh
# List all versions of all objects
s3find 's3://example-bucket/' --all-versions ls

# List all versions matching a pattern
s3find 's3://example-bucket/' --all-versions --name '*.log' ls

# Print detailed version information
s3find 's3://example-bucket/' --all-versions print --format json
```

When `--all-versions` is enabled:
- Uses the S3 ListObjectVersions API instead of ListObjectsV2
- Shows all versions of each object, not just the current version
- Includes delete markers (shown with size 0)

**Note:** `--all-versions` is not compatible with `--maxdepth`. If both are specified, `--all-versions` takes precedence and `--maxdepth` is ignored.

For more information, see the [GitHub repository](https://github.com/AnderEnder/s3find-rs).
