#!/bin/bash
#
# Build butterfly-osm Docker images.
# Infrastructure-agnostic: tags images with the git SHA and (optionally)
# a registry prefix. No pushes here — see deploy.sh.
#
# Usage:
#   ./build.sh                       # builds butterfly-route:<sha> + butterfly-tools:<sha>
#   ./build.sh --registry foo.bar    # also tags as foo.bar/butterfly-{route,tools}:<sha>
#   ./build.sh --server-only         # skip the tools image
#   ./build.sh --tools-only          # skip the server image
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

REGISTRY=""
BUILD_SERVER=1
BUILD_TOOLS=1

while [[ $# -gt 0 ]]; do
    case "$1" in
        --registry)    REGISTRY="$2"; shift 2 ;;
        --server-only) BUILD_TOOLS=0; shift ;;
        --tools-only)  BUILD_SERVER=0; shift ;;
        -h|--help)
            sed -n '2,12p' "$0"; exit 0 ;;
        *) echo "Unknown arg: $1" >&2; exit 2 ;;
    esac
done

log() { echo -e "\033[0;32m[build]\033[0m $*"; }

SHA=$(git rev-parse --short HEAD)
DIRTY=""
if ! git diff --quiet || ! git diff --cached --quiet; then
    DIRTY="-dirty"
fi
TAG="${SHA}${DIRTY}"

log "git sha: $TAG"

tag_args() {
    local name="$1"
    local args=(-t "${name}:${TAG}" -t "${name}:latest")
    if [[ -n "$REGISTRY" ]]; then
        args+=(-t "${REGISTRY}/${name}:${TAG}" -t "${REGISTRY}/${name}:latest")
    fi
    printf '%s\n' "${args[@]}"
}

if [[ "$BUILD_SERVER" -eq 1 ]]; then
    log "Building butterfly-route (server) @ $TAG"
    mapfile -t SERVER_TAGS < <(tag_args butterfly-route)
    DOCKER_BUILDKIT=1 docker build -f Dockerfile "${SERVER_TAGS[@]}" .
fi

if [[ "$BUILD_TOOLS" -eq 1 ]]; then
    log "Building butterfly-tools @ $TAG"
    mapfile -t TOOLS_TAGS < <(tag_args butterfly-tools)
    DOCKER_BUILDKIT=1 docker build -f Dockerfile.tools "${TOOLS_TAGS[@]}" .
fi

log "Done. SHA=$TAG"
if [[ -n "$DIRTY" ]]; then
    log "WARNING: working tree dirty — images tagged '${TAG}'"
fi
