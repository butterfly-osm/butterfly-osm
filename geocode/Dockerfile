# syntax=docker/dockerfile:1
# Multi-stage build for butterfly-geocode.
#
# Stage 1: Build the binary against the workspace.
# Stage 2: Run from a thin debian:trixie-slim base.
#
# The trixie tags are pinned by name only — for byte-identical
# reproducibility, swap in a `@sha256:...` digest. Trixie is the
# active Debian stable as of this writing and matches the runtime
# used by butterfly-route's production Dockerfile.

###########
# Builder #
###########
FROM rust:1.95-trixie AS builder

# Build inputs:
#   - protoc: required transitively by `gtfs-rt` (prost-build) when the
#     full workspace is compiled. Cheap to install, future-proofs the
#     image if butterfly-geocode ever pulls in a protobuf-using crate.
#   - pkg-config + libssl-dev: required by `reqwest`'s rustls config in
#     dev-deps. Builder-only; not needed at runtime since the binary
#     statically links rustls.
RUN apt-get update && apt-get install -y --no-install-recommends \
    protobuf-compiler \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy workspace manifests first for layer caching. A change to any
# `Cargo.toml` invalidates the dependency-build layer below; a change
# to *only* application source code reuses it.
COPY Cargo.toml Cargo.lock ./
COPY butterfly-common/Cargo.toml butterfly-common/Cargo.toml
COPY dl/Cargo.toml dl/Cargo.toml
COPY route/Cargo.toml route/Cargo.toml
COPY geocode/Cargo.toml geocode/Cargo.toml

# Stub source files so cargo can resolve and pre-build dependencies
# without the actual application code. This is a standard Rust
# Docker idiom; the touched timestamps below force a rebuild once
# the real sources land.
RUN mkdir -p \
        butterfly-common/src \
        dl/src \
        route/src route/src/bench \
        geocode/src \
    && echo "fn main() {}" > butterfly-common/src/lib.rs \
    && echo "fn main() {}" > dl/src/lib.rs \
    && echo "fn main() {}" > route/src/lib.rs \
    && echo "fn main() {}" > route/src/main.rs \
    && echo "fn main() {}" > route/src/bench/main.rs \
    && echo "fn main() {}" > geocode/src/lib.rs \
    && echo "fn main() {}" > geocode/src/main.rs

# Pre-build deps. Suppress non-zero exit because the stub workspace
# may fail to link — we only care about populating the dep cache.
RUN cargo build --release -p butterfly-geocode 2>/dev/null || true

# Now copy the real workspace sources.
COPY butterfly-common/ butterfly-common/
COPY dl/ dl/
COPY route/ route/
COPY geocode/ geocode/

# Touch source files so cargo rebuilds them rather than reusing
# the stub artifacts.
RUN find butterfly-common/src dl/src route/src geocode/src -name '*.rs' -exec touch {} +

# Build the release binary. Workspace lints (warnings as errors) run
# during this step, so a clippy regression upstream will surface here
# before the image ships.
RUN cargo build --release -p butterfly-geocode

###########
# Runtime #
###########
FROM debian:trixie-slim

# curl: drives the HEALTHCHECK below.
# ca-certificates: the geocoder doesn't make outbound HTTPS calls, but
#   ship them anyway so an operator can `docker exec` and `curl
#   https://...` without surprises.
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/butterfly-geocode /usr/local/bin/butterfly-geocode

# Drop privileges. The binary only needs read access to the shard
# files mounted at /data and write access to stdout/stderr; running
# as root is a needless attack surface.
RUN groupadd -r butterfly && useradd -r -g butterfly butterfly
USER butterfly

# Mount point for the shard file. Operators bind-mount their
# `<region>.bfgs` here (e.g. `-v /host/path/belgium.bfgs:/data/shard.bfgs`)
# or the entire `regions/` directory if multi-shard support is added
# later (#96).
VOLUME /data

EXPOSE 8080

# JSON logs by default in containers — operators with a log
# aggregator (Loki/Elasticsearch/Datadog) get parseable structured
# events without touching the CMD.
ENV RUST_LOG=info,butterfly_geocode=info

# Liveness probe via the enriched /health endpoint. `start-period` is
# generous because shard mmap + parser-backend init dominates cold
# start (the heuristic backend is fast; the optional neural backend
# loads ~5 MB of weights at boot).
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD curl -fsS http://localhost:8080/health || exit 1

ENTRYPOINT ["butterfly-geocode"]
CMD ["serve", \
     "--shard", "/data/shard.bfgs", \
     "--port", "8080", \
     "--host", "0.0.0.0", \
     "--log-format", "json"]
