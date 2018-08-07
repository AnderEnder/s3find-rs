# s3find
[![build status](https://travis-ci.org/AnderEnder/s3find-rs.svg?branch=master)](https://travis-ci.org/AnderEnder/s3find-rs)
[![codecov](https://codecov.io/gh/AnderEnder/s3find-rs/branch/master/graph/badge.svg)](https://codecov.io/gh/AnderEnder/s3find-rs)

Utility to walk a S3 hierarchy. An analog of find for AWS S3.

## Examples

### Find path by glob pattern

#### Print

```sh
s3find 's3://s3example/s3test' --name '*' -print
```

#### Delete

```sh
s3find 's3://s3example/s3test' --name '*' -delete
```

#### List

```sh
s3find 's3://s3example/s3test' --name '*' -ls
```

#### List keys with tags

```sh
s3find 's3://s3example/s3test' --name '*' -lstags
```

#### Exec

```sh
s3find 's3://s3example/s3test' --name '*' -exec 'echo {}'

```

#### Download(simple implementation)

```sh
s3find 's3://s3example/s3test' --name '*' -download

```

#### Set tags

```sh
s3find 's3://s3example/s3test' --name '*9*' -tags 'key:value' 'env:staging'

```

### Find path by case insensitive glob pattern

```sh
s3find 's3://s3example/s3test' --iname '*s*' -ls
```

### Find path by regex pattern

```sh
s3find 's3://s3example/s3test' --regex '1$' -print
```

### Find path by size

#### Exact match

```sh
s3find 's3://s3example/s3test' --size 0 -print
```

#### Larger

```sh
s3find 's3://s3example/s3test' --size +10M -print
```

#### Smaller

```sh
s3find 's3://s3example/s3test' --size -10k -print
```

### Find path by time

#### Files modified since last 10 seconds

```sh
s3find 's3://s3example/s3test' --time 10 -print
```

#### Files modified for the period before last 10 minutes

```sh
s3find 's3://s3example/s3test' --time +10m -print
```

#### Files modified since last 10 hours

```sh
s3find 's3://s3example/s3test' --time -10h -print
```

### Multiple filters

#### Same filters

Files with size between 10 and 20 bytes

```sh
s3find 's3://s3example/s3test' --size +10 --size -20 -print
```

#### Different filters

```sh
s3find 's3://s3example/s3test' --size +10 --name '*file*' -print
```

## How to build and install

Requirements: rust and cargo

```sh
# Build
cargo build --release

# Install from local source
cargo install

# Install from git
cargo install --git https://github.com/AnderEnder/s3find-rs
```
