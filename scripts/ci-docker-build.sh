#!/usr/bin/env bash
# ============================================================================
# Build BOTH image targets and print their sizes (#565, #573).
#
# Why this is a CI step: the builder stage used to pre-build dependencies with
# `cargo build … 2>/dev/null || true`, so a failed dependency layer looked
# green; and nothing anywhere built the images, so a broken manifest only
# surfaced at deploy time. The `|| true` is gone and the two manifests are now
# one — this step is what proves both stay true.
#
# Called by scripts/ci-steps.sh. Skips loudly (exit 0) when there is no Docker
# daemon available, so the rest of the step list still runs on a machine
# without one.
#
# Usage: bash scripts/ci-docker-build.sh
# ============================================================================
set -uo pipefail
cd "$(git rev-parse --show-toplevel)" || exit 1

if ! command -v docker >/dev/null 2>&1; then
    echo "[docker] SKIPPED — no docker binary on PATH"
    exit 0
fi
if ! docker info >/dev/null 2>&1; then
    echo "[docker] SKIPPED — docker daemon not reachable"
    exit 0
fi

# BuildKit is required for the builder stage's `RUN --mount=type=cache`.
export DOCKER_BUILDKIT=1

status=0
for target in tools runtime; do
    tag="butterfly-ci-$target"
    echo "[docker] building --target $target"
    started=$(date +%s)
    if ! docker build --target "$target" -t "$tag" .; then
        echo "[docker] FAILED: --target $target"
        status=1
        continue
    fi
    elapsed=$(( $(date +%s) - started ))
    size=$(docker images --format '{{.Size}}' "$tag" | head -n 1)
    echo "[docker] $target ok — ${elapsed}s, image size ${size:-unknown} ($tag)"
done

# The deprecated shim has to keep building too, for as long as it exists —
# it is what deploy tooling outside this repo still points at.
echo "[docker] building the deprecated -f Dockerfile.tools shim"
if ! docker build -f Dockerfile.tools -t butterfly-ci-tools-shim .; then
    echo "[docker] FAILED: -f Dockerfile.tools"
    status=1
fi

exit "$status"
