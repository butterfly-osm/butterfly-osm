FROM rust:alpine AS builder

# Install build dependencies
RUN apk add --no-cache musl-dev

WORKDIR /app

# Copy manifests
COPY Cargo.toml ./

# Create dummy source to cache dependencies
RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN cargo build --release
RUN rm -rf src

# Copy source and build
COPY src ./src
RUN touch src/main.rs
RUN cargo build --release

# Runtime stage
FROM alpine:latest

RUN apk add --no-cache ca-certificates

WORKDIR /app

# Copy binary
COPY --from=builder /app/target/release/geofabrik-downloader /usr/local/bin/

# Create data directory
RUN mkdir -p /data/pbf

ENTRYPOINT ["geofabrik-downloader"]