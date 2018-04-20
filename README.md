# s3find
[![build status](https://travis-ci.org/AnderEnder/s3find-rs.svg?branch=master)](https://travis-ci.org/AnderEnder/s3find-rs)

Utility to walk a S3 hierarchy. An analog of find for AWS S3.

## Examples

### Find path by glob pattern

#### Print

```sh
s3find --name='*' 's3://s3example/s3test' -print
```

#### Delete

```sh
s3find --name='*' 's3://s3example/s3test' -delete
```

#### List

```sh
s3find --name='*' 's3://s3example/s3test' -ls
```

#### Exec

```sh
s3find --name='*' 's3://s3example/s3test' -exec 'echo {}'

```

### Find path by case insensitive glob pattern

```sh
s3find --iname='*s*' 's3://s3example/s3test' -ls
```

### Find path by regex pattern

```sh
s3find --regex='1$' 's3://s3example/s3test' -print
```

### Find path by size

#### Exact match

```sh
s3find --size=0 's3://s3example/s3test' -print
```

#### Larger

```sh
s3find --size=+10 's3://s3example/s3test' -print
```

#### Smaller

```sh
s3find --size=-10 's3://s3example/s3test' -print
```

### Multiple filters

#### Same filters

```sh
s3find --size=+10  --size=-20 's3://s3example/s3test' -print
```

#### Different filters

```sh
s3find --size=+10 --name='*file*' 's3://s3example/s3test' -print
```
