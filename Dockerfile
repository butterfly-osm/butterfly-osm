# Multi-stage build for butterfly-route
# Stage 1: Build
# Pin to bookworm for reproducibility. For exact reproducibility, pin to a SHA digest.
FROM rust:1.84-bookworm AS builder

WORKDIR /build

# Copy workspace manifests first for layer caching
COPY Cargo.toml Cargo.lock ./
COPY butterfly-common/Cargo.toml butterfly-common/Cargo.toml
COPY tools/butterfly-dl/Cargo.toml tools/butterfly-dl/Cargo.toml
COPY tools/butterfly-route/Cargo.toml tools/butterfly-route/Cargo.toml

# Create dummy source files for dependency caching
RUN mkdir -p butterfly-common/src tools/butterfly-dl/src tools/butterfly-route/src tools/butterfly-route/src/bench && \
    echo "fn main() {}" > butterfly-common/src/lib.rs && \
    echo "fn main() {}" > tools/butterfly-dl/src/lib.rs && \
    echo "fn main() {}" > tools/butterfly-route/src/lib.rs && \
    echo "fn main() {}" > tools/butterfly-route/src/main.rs && \
    echo "fn main() {}" > tools/butterfly-route/src/bench/main.rs

# Build dependencies only (cached layer)
RUN cargo build --release -p butterfly-route 2>/dev/null || true

# Copy actual source code
COPY butterfly-common/ butterfly-common/
COPY tools/butterfly-dl/ tools/butterfly-dl/
COPY tools/butterfly-route/ tools/butterfly-route/

# Touch source files to invalidate the build cache for actual code
RUN touch butterfly-common/src/lib.rs tools/butterfly-dl/src/lib.rs \
    tools/butterfly-route/src/lib.rs tools/butterfly-route/src/main.rs

# Build release binary
RUN cargo build --release -p butterfly-route

# Stage 2: Runtime
# Pin to bookworm for reproducibility. For exact reproducibility, pin to a SHA digest.
FROM debian:bookworm-20250203-slim

# curl needed for Docker HEALTHCHECK; ca-certificates for HTTPS
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy the built binary
COPY --from=builder /build/target/release/butterfly-route /usr/local/bin/butterfly-route

# Run as non-root user for security
RUN groupadd -r butterfly && useradd -r -g butterfly butterfly
USER butterfly

# Data volume
VOLUME /data

EXPOSE 8080

# JSON logging by default in containers
ENV RUST_LOG=info,tower_http=debug

HEALTHCHECK --interval=30s --timeout=5s --start-period=25s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

ENTRYPOINT ["butterfly-route"]
CMD ["serve", "--data-dir", "/data", "--port", "8080", "--log-format", "json"]
