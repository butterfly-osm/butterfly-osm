FROM rust:alpine AS builder

# Install build dependencies
RUN apk add --no-cache musl-dev pkgconfig openssl-dev openssl-libs-static

WORKDIR /app

# Copy manifests
COPY Cargo.toml ./

# Create dummy source to cache dependencies
RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN cargo build --release
RUN rm -rf src target/release/deps/geofabrik_downloader*

# Copy source and build
COPY src ./src
RUN touch src/main.rs
RUN cargo build --release

# Runtime stage - from scratch for minimal image
FROM scratch

# Copy CA certificates for HTTPS
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# Copy binary
COPY --from=builder /app/target/release/geofabrik-downloader /usr/local/bin/geofabrik-downloader

ENTRYPOINT ["geofabrik-downloader"]