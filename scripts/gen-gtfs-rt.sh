#!/usr/bin/env bash
# ============================================================================
# Regenerate `route/src/transit/gtfs_realtime.rs` from the vendored
# `route/src/transit/gtfs-realtime.proto` (#574).
#
# The generated code is COMMITTED. Nothing in the normal build runs this:
# butterfly-route has no `build.rs`, no `prost-build` dependency and needs no
# `protoc` on the build host or in either container image. Run this script by
# hand — and only — when the upstream GTFS-Realtime spec moves and you have
# refreshed the `.proto` beside it.
#
# Requirements: `protoc` on PATH, network access for the pinned prost-build.
#
# Usage: bash scripts/gen-gtfs-rt.sh
# ============================================================================
set -euo pipefail

# Must track `prost` in route/Cargo.toml: prost-build emits code against a
# specific prost derive/runtime version.
PROST_BUILD_VERSION="0.14"

cd "$(git rev-parse --show-toplevel)"
PROTO_DIR="$PWD/route/src/transit"
OUT_FILE="$PROTO_DIR/gtfs_realtime.rs"

command -v protoc >/dev/null || { echo "protoc not found on PATH" >&2; exit 1; }

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

mkdir -p "$WORK/gen/src" "$WORK/out"
cat > "$WORK/gen/Cargo.toml" <<EOF
[package]
name = "gtfs-rt-codegen"
version = "0.0.0"
edition = "2021"

[dependencies]
prost-build = "$PROST_BUILD_VERSION"
EOF

cat > "$WORK/gen/src/main.rs" <<'EOF'
fn main() {
    let proto_dir = std::env::var("PROTO_DIR").expect("PROTO_DIR");
    let out_dir = std::env::var("GEN_OUT_DIR").expect("GEN_OUT_DIR");
    let mut cfg = prost_build::Config::new();
    cfg.out_dir(&out_dir);
    cfg.default_package_filename("gtfs_realtime");
    cfg.compile_protos(
        &[format!("{proto_dir}/gtfs-realtime.proto")],
        &[proto_dir.as_str()],
    )
    .expect("prost-build codegen");
}
EOF

PROTO_DIR="$PROTO_DIR" GEN_OUT_DIR="$WORK/out" \
    cargo run --quiet --manifest-path "$WORK/gen/Cargo.toml"

mapfile -t PRODUCED < <(find "$WORK/out" -name '*.rs' -type f | sort)
if [ "${#PRODUCED[@]}" -ne 1 ]; then
    echo "expected exactly one generated .rs, got: ${PRODUCED[*]:-none}" >&2
    exit 1
fi
GENERATED="${PRODUCED[0]}"
[ -s "$GENERATED" ] || { echo "codegen produced an empty file" >&2; exit 1; }

{
    cat <<'HEADER'
// GENERATED FILE — DO NOT EDIT BY HAND.
//
// Rust bindings for the GTFS-Realtime protobuf schema, produced from the
// sibling `gtfs-realtime.proto` by `scripts/gen-gtfs-rt.sh` (prost-build).
// Regenerate with that script when the upstream spec moves; see #574 for
// why the code is committed rather than built by a `build.rs`.
//
// The schema (`gtfs-realtime.proto`) is Copyright 2015 The GTFS
// Specifications Authors, licensed under the Apache License, Version 2.0.
// This file is a mechanical translation of it.
//
// prost's output is machine-written: it does not follow our hand-written
// style rules (long single-line doc comments carried over from the proto,
// `Option<i32>` scalars, etc.), so the module — and only this module —
// opts out of the workspace clippy/rustdoc pedantry rather than being
// hand-patched after every regeneration.
#![allow(clippy::all, clippy::pedantic, rustdoc::all)]

HEADER
    cat "$GENERATED"
} > "$OUT_FILE"

rustfmt --edition 2024 "$OUT_FILE"
echo "wrote $OUT_FILE"
