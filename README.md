# s3find

Utility to walk a S3 hierarchy. An analog of find for AWS S3.

## Examples

### Find and print by glob pattern

```sh
s3find --name '*' 's3://s3example/s3test' -print
```

### Find and delete by glob pattern

```sh
s3find --name '*' 's3://s3example/s3test' -delete
```

### Find and list by glob pattern

```sh
s3find --name '*' 's3://s3example/s3test' -ls
```

### Find by glob pattern and execute command echo with it

```sh
s3find --name '*' 's3://s3example/s3test' -exec 'echo {}'
```
