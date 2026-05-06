#!/usr/bin/env bash
# Boot the geocode server for benchmarking against Nominatim.
#
# Defaults:
#   - port 3003
#   - belgium shard (single-country)
#   - neural parser with belgium-prod model
#   - admission disabled (bench is single-client, single-IP, exceeds
#     the 25/s default)
#
# Usage:
#   PORT=3003 MODEL=geocode/data/models/belgium-prod.safetensors \
#     scripts/geocode_serve_bench.sh

set -euo pipefail

cd "$(dirname "$0")/.."

PORT="${PORT:-3003}"
SHARD="${SHARD:-geocode/regions/belgium.bfgs}"
MODEL="${MODEL:-geocode/data/models/belgium-prod.safetensors}"
PARSER="${PARSER:-neural}"
HOST="${HOST:-127.0.0.1}"

# CUDA env (no-op on CPU-only builds).
export PATH="/usr/local/cuda/bin:${PATH:-}"
export LD_LIBRARY_PATH="/usr/local/cuda/lib64:${LD_LIBRARY_PATH:-}"

if [[ ! -f "$SHARD" ]]; then
    echo "ERROR: shard $SHARD not found" >&2
    exit 1
fi

if [[ "$PARSER" == "neural" && ! -f "$MODEL" ]]; then
    echo "ERROR: model $MODEL not found (parser=neural). Either train it first or set PARSER=heuristic." >&2
    exit 1
fi

echo "[serve] PORT=$PORT shard=$SHARD parser=$PARSER model=${MODEL:-N/A}"

ARGS=(
    serve
    --shard "$SHARD"
    --port "$PORT"
    --host "$HOST"
    --parser "$PARSER"
    --admission-disable
    --rate-limit-per-sec 100000
    --rate-limit-burst 200000
    --transport rest
)
if [[ "$PARSER" == "neural" ]]; then
    ARGS+=(--model "$MODEL")
fi

exec ./target/release/butterfly-geocode "${ARGS[@]}"
