# s3find
[![build status](https://travis-ci.org/AnderEnder/s3find-rs.svg?branch=master)](https://travis-ci.org/AnderEnder/s3find-rs)
[![codecov](https://codecov.io/gh/AnderEnder/s3find-rs/branch/master/graph/badge.svg)](https://codecov.io/gh/AnderEnder/s3find-rs)
[![Crates.io](https://img.shields.io/crates/v/s3find.svg)](https://crates.io/crates/s3find)


Utility to walk a S3 hierarchy. An analog of find for AWS S3.

## Usage

```sh
USAGE:
    s3find [OPTIONS] <path> [SUBCOMMAND]

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
        --aws-access-key <aws_access_key>    AWS access key. Unrequired
        --aws-region <aws_region>            The region to use. Default value is us-east-1
        --aws-secret-key <aws_secret_key>    AWS secret key. Unrequired
        --size <bytes_size>...               File size for match:
                                                 5k - exact match 5k,
                                                 +5k - bigger than 5k,
                                                 -5k - smaller than 5k,

                                             Possible file size units are as follows:
                                                 k - kilobytes (1024 bytes)
                                                 M - megabytes (1024 kilobytes)
                                                 G - gigabytes (1024 megabytes)
                                                 T - terabytes (1024 gigabytes)
                                                 P - petabytes (1024 terabytes)
        --iname <ipatern>...                 Case-insensitive glob pattern for match, can be multiple
        --name <npatern>...                  Glob pattern for match, can be multiple
        --regex <rpatern>...                 Regex pattern for match, can be multiple
        --mtime <time>...                    Modification time for match, a time period:
                                                 +5d - for period from now-5d to now
                                                 -5d - for period  before now-5d

                                             Possible time units are as follows:
                                                 s - seconds
                                                 m - minutes
                                                 h - hours
                                                 d - days
                                                 w - weeks

                                             Can be multiple, but should be overlaping

ARGS:
    <path>    S3 path to walk through. It should be s3://bucket/path

SUBCOMMANDS:
    -delete      Delete matched keys
    -download    Download matched keys
    -exec        Exec any shell program with every key
    -ls          Print the list of matched keys
    -lstags      Print the list of matched keys with tags
    -print       Extended print with detail information
    -public      Make the matched keys public available (readonly)
    -tags        Set the tags(overwrite) for the matched keys
    help         Prints this message or the help of the given subcommand(s)
```

## Examples

### Find path by glob pattern

#### Print

```sh
s3find 's3://example-bucket/example-path' --name '*' -print
```

#### Delete

```sh
s3find 's3://example-bucket/example-path' --name '*' -delete
```

#### List

```sh
s3find 's3://example-bucket/example-path' --name '*' -ls
```

#### List keys with tags

```sh
s3find 's3://example-bucket/example-path' --name '*' -lstags
```

#### Exec

```sh
s3find 's3://example-bucket/example-path' --name '*' -exec 'echo {}'

```

#### Download

```sh
s3find 's3://example-bucket/example-path' --name '*' -download

```

#### Set tags

```sh
s3find 's3://example-bucket/example-path' --name '*9*' -tags 'key:value' 'env:staging'

```

#### Make public available

```sh
s3find 's3://example-bucket/example-path' --name '*9*' -public

```

### Find path by case insensitive glob pattern

```sh
s3find 's3://example-bucket/example-path' --iname '*s*' -ls
```

### Find path by regex pattern

```sh
s3find 's3://example-bucket/example-path' --regex '1$' -print
```

### Find path by size

#### Exact match

```sh
s3find 's3://example-bucket/example-path' --size 0 -print
```

#### Larger

```sh
s3find 's3://example-bucket/example-path' --size +10M -print
```

#### Smaller

```sh
s3find 's3://example-bucket/example-path' --size -10k -print
```

### Find path by time

#### Files modified since last 10 seconds

```sh
s3find 's3://example-bucket/example-path' --time 10 -print
```

#### Files modified for the period before last 10 minutes

```sh
s3find 's3://example-bucket/example-path' --time +10m -print
```

#### Files modified since last 10 hours

```sh
s3find 's3://example-bucket/example-path' --time -10h -print
```

### Multiple filters

#### Same filters

Files with size between 10 and 20 bytes

```sh
s3find 's3://example-bucket/example-path' --size +10 --size -20 -print
```

#### Different filters

```sh
s3find 's3://example-bucket/example-path' --size +10 --name '*file*' -print
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
