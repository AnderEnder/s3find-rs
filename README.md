# s3find
[![build status](https://github.com/AnderEnder/s3find-rs/workflows/Build/badge.svg)](https://github.com/AnderEnder/s3find-rs/actions)
[![freebsd build status](https://api.cirrus-ci.com/github/AnderEnder/s3find-rs.svg)](https://cirrus-ci.com/github/AnderEnder/s3find-rs/>)
[![codecov](https://codecov.io/gh/AnderEnder/s3find-rs/branch/master/graph/badge.svg)](https://codecov.io/gh/AnderEnder/s3find-rs)
[![crates.io](https://img.shields.io/crates/v/s3find.svg)](https://crates.io/crates/s3find)
[![docker image](https://img.shields.io/docker/cloud/build/anderender/s3find.svg)](https://hub.docker.com/r/anderender/s3find)


A command line utility to walk an Amazon S3 hierarchy. An analog of find for Amazon S3.

## Distribution

### Release page distributions

Github Release page provides binaries for:

* Windows
* Linux
* macOS

### Docker

Docker image on docker hub:

* develop: `anderender/s3find:latest`
* release: `anderender/s3find:<version>`

## Usage

```sh
USAGE:
    s3find [FLAGS] [OPTIONS] <path> [SUBCOMMAND]

FLAGS:
    -h, --help
            Prints help information

        --summarize
            Print summary statistic

    -V, --version
            Prints version information


OPTIONS:
        --aws-access-key <aws-access-key>
            AWS access key. Unrequired.

        --aws-region <aws-region>
            The region to use. Default value is us-east-1 [default: us-east-1]

        --aws-secret-key <aws-secret-key>
            AWS secret key. Unrequired

        --size <bytes-size>...
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
        --iname <ipatern>...
            Case-insensitive glob pattern for match, can be multiple

        --limit <limit>
            Limit result

        --name <npatern>...
            Glob pattern for match, can be multiple

        --page-size <number>
            The number of results to return in each response to a
            list operation. The default value is 1000 (the maximum
            allowed). Using a lower value may help if an operation
            times out. [default: 1000]
        --regex <rpatern>...
            Regex pattern for match, can be multiple

        --mtime <time>...
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

ARGS:
    <path>
            S3 path to walk through. It should be s3://bucket/path


SUBCOMMANDS:
    copy        Copy matched keys to a s3 destination
    delete      Delete matched keys
    download    Download matched keys
    exec        Exec any shell program with every key
    help        Prints this message or the help of the given subcommand(s)
    ls          Print the list of matched keys
    lstags      Print the list of matched keys with tags
    move        Move matched keys to a s3 destination
    nothing     Do not do anything with keys, do not print them as well
    print       Extended print with detail information
    public      Make the matched keys public available (readonly)
    tags        Set the tags(overwrite) for the matched keys


The authorization flow is the following chain:
  * use credentials from arguments provided by users
  * use environment variable credentials: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
  * use credentials via aws file profile.
    Profile can be set via environment variable AWS_PROFILE
    Profile file can be set via environment variable AWS_SHARED_CREDENTIALS_FILE
  * use AWS instance IAM profile
  * use AWS container IAM profile
```

## Examples

### Find path by glob pattern

#### Print

```sh
s3find 's3://example-bucket/example-path' --name '*' print
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
