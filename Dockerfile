FROM anderender/muslrust as cargo-build
WORKDIR /usr/src
COPY . /usr/src
RUN cargo build --release

FROM alpine:latest
COPY --from=cargo-build  /usr/src/target/x86_64-unknown-linux-musl/release/s3find /usr/bin/
ENTRYPOINT ["s3find"]
