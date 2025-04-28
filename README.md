# s3find
[![build status](https://github.com/AnderEnder/s3find-rs/workflows/Build/badge.svg)](https://github.com/AnderEnder/s3find-rs/actions)
[![release status](https://github.com/AnderEnder/s3find-rs/workflows/Release/badge.svg)](https://github.com/AnderEnder/s3find-rs/actions)
[![codecov](https://codecov.io/gh/AnderEnder/s3find-rs/branch/master/graph/badge.svg)](https://codecov.io/gh/AnderEnder/s3find-rs)
[![crates.io](https://img.shields.io/crates/v/s3find.svg)](https://crates.io/crates/s3find)


A command line utility to walk an Amazon S3 hierarchy. An analog of find for Amazon S3.

## Distribution

### Release page distributions

Github Release page provides binaries for:

* Windows
* Linux
* macOS

## Usage

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

### Find path by glob pattern

#### Print keys with extended information and different formats

```sh
s3find 's3://example-bucket/example-path' --name '*' print
```

```sh
s3find 's3://example-bucket/example-path' --name '*' print --format text
```

```sh
s3find 's3://example-bucket/example-path' --name '*' print --format json
```

```sh
s3find 's3://example-bucket/example-path' --name '*' print --format csv
```

#### Delete

```sh
s3find 's3://example-bucket/example-path' --name '*' delete
```

#### List

```sh
s3find 's3://example-bucket/example-path' --name '*' ls
```

#### List keys with tags

```sh
s3find 's3://example-bucket/example-path' --name '*' lstags
```

#### Exec

```sh
s3find 's3://example-bucket/example-path' --name '*' exec 'echo {}'
```

#### Download

```sh
s3find 's3://example-bucket/example-path' --name '*' download
```

#### Copy files to another s3 location

```sh
s3find 's3://example-bucket/example-path' --name '*.dat' copy -f 's3://example-bucket/example-path2'
```

#### Move files to another s3 location

```sh
s3find 's3://example-bucket/example-path' --name '*.dat' move -f 's3://example-bucket/example-path2'
```

#### Set tags

```sh
s3find 's3://example-bucket/example-path' --name '*9*' tags 'key:value' 'env:staging'
```

#### Make public available

```sh
s3find 's3://example-bucket/example-path' --name '*9*' public
```

### Find path by case insensitive glob pattern

```sh
s3find 's3://example-bucket/example-path' --iname '*s*' ls
```

### Find path by regex pattern

```sh
s3find 's3://example-bucket/example-path' --regex '1$' print
```

### Find path by size

#### Exact match

```sh
s3find 's3://example-bucket/example-path' --size 0 print
```

#### Larger

```sh
s3find 's3://example-bucket/example-path' --size +10M print
```

#### Smaller

```sh
s3find 's3://example-bucket/example-path' --size -10k print
```

### Find path by time

#### Files modified for the period before last 10 seconds

```sh
s3find 's3://example-bucket/example-path' --mtime 10 print
```

#### Files modified for the period before last 10 minutes

```sh
s3find 's3://example-bucket/example-path' --mtime +10m print
```

#### Files modified since last 10 hours

```sh
s3find 's3://example-bucket/example-path' --mtime -10h print
```

### Object Storage Class Filter

You can filter objects based on their storage class using the `storage-class` filter. This allows you to match objects stored in specific S3 storage classes, such as `STANDARD` or `GLACIER`.

```bash
s3find 's3://example-bucket/example-path' --storage-class STANDARD print
s3find 's3://example-bucket/example-path' --storage-class GLACIER print
```

This feature is useful for identifying objects stored in different storage tiers and managing them accordingly.

### Multiple filters

#### Same filters

Files with size between 10 and 20 bytes

```sh
s3find 's3://example-bucket/example-path' --size +10 --size -20 print
```

#### Different filters

```sh
s3find 's3://example-bucket/example-path' --size +10 --name '*file*' print
```

### Additional control

#### Select limited number of keys

```sh
s3find 's3://example-bucket/example-path' --name '*' --limit 10
```

#### Limit page size of the request

```sh
s3find 's3://example-bucket/example-path' --name '*' --page-size 100
```

## How to build and install

Requirements: rust and cargo

```sh
# Build
cargo build --release

# Install from local source
cargo install

# Install latest from git
cargo install --git https://github.com/AnderEnder/s3find-rs

# Install from crate package
cargo install s3find
```
