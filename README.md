# s3find

Utility to walk a S3 hierarchy. An analog of find for AWS S3.

## Examples

### Find path by glob pattern

#### Print

```sh
s3find --name '*' 's3://s3example/s3test' -print
```

#### Delete

```sh
s3find --name '*' 's3://s3example/s3test' -delete
```

#### List

```sh
s3find --name '*' 's3://s3example/s3test' -ls
```

#### Exec

```sh
s3find --name '*' 's3://s3example/s3test' -exec 'echo {}'

```

### Find path by case insensitive glob pattern

```sh
s3find --iname '*s*' 's3://s3example/s3test' -ls
```

### Find path by regex pattern

```sh
s3find --regex '1$' 's3://s3example/s3test' -print
```
