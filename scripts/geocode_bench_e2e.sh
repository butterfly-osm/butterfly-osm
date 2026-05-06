#!/bin/bash
# End-to-end multi-country geocode benchmark.
#
# Boots a butterfly-geocode server with all 15 shards, the multi-country
# tagger, and the multi-country rerank GBDT. Runs the per-country bench
# against Butterfly + (optionally) Nominatim. Tears down the server.
#
# Usage:
#   bash scripts/geocode_bench_e2e.sh \
#       <model.safetensors> <rerank.gbdt> <out-dir> [--countries BE,FR,...]

set -euo pipefail

MODEL="${1:?model path required}"
RERANK="${2:?rerank GBDT path required}"
OUT_DIR="${3:?output dir required}"
shift 3 || true
COUNTRIES="AT,AU,BE,BR,CH,DE,ES,FR,GB,IN,IT,JP,LU,NL,US"
if [ "${1:-}" = "--countries" ]; then
  COUNTRIES="$2"
  shift 2
fi

WORKTREE="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${WORKTREE}/target/release/butterfly-geocode"
SHARDS="/home/snape/projects/butterfly-osm/geocode/data/shards"
PORT=31000
NOMINATIM_BASE="${NOMINATIM_BASE:-http://localhost:8080}"

mkdir -p "$OUT_DIR"
echo "[bench-e2e] model=$MODEL rerank=$RERANK shards=$SHARDS port=$PORT"

# Start server (background).
SERVER_LOG="$OUT_DIR/server.log"
echo "[bench-e2e] starting server (log: $SERVER_LOG)..."
"$BIN" serve \
  --shard-dir "$SHARDS" \
  --rest-port "$PORT" \
  --transport rest \
  --parser neural \
  --model "$MODEL" \
  --rerank-model "$RERANK" \
  --admission-disable \
  --request-timeout-secs 30 \
  --max-body-bytes 65536 \
  > "$SERVER_LOG" 2>&1 &
SERVER_PID=$!
echo "[bench-e2e] server PID=$SERVER_PID"

# Wait until /health returns 200.
echo "[bench-e2e] waiting for /health..."
for i in $(seq 1 60); do
  if curl -s --max-time 2 "http://localhost:${PORT}/health" >/dev/null 2>&1; then
    echo "[bench-e2e] server ready after ${i}s"
    break
  fi
  sleep 1
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    echo "[bench-e2e] server died during startup; tail of log:"
    tail -50 "$SERVER_LOG"
    exit 1
  fi
done

# Run bench.
trap 'echo "[bench-e2e] stopping server"; kill "$SERVER_PID" 2>/dev/null; wait "$SERVER_PID" 2>/dev/null; true' EXIT

python3 "${WORKTREE}/scripts/geocode_multi_country_bench.py" \
  --queries-dir "${WORKTREE}/bench/geocode/queries" \
  --out-dir "$OUT_DIR" \
  --butterfly-base "http://localhost:${PORT}" \
  --nominatim-base "$NOMINATIM_BASE" \
  --countries "$COUNTRIES" \
  --concurrency 4 \
  --limit 5 \
  --radius-m 100

echo "[bench-e2e] done. Results: $OUT_DIR"
