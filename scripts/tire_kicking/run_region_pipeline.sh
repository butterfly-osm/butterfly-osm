#!/bin/bash
# Build a region's CCH from PBF — full pipeline (steps 1-8) using subset modes.
#
# Usage:
#   scripts/tire_kicking/run_region_pipeline.sh <region_name> <pbf_path>
#
# Must be invoked from the repository root, or via its absolute path —
# the script derives the repo root from its own location so it works
# from any CWD (CI, dev machine, alternate checkout).
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "Usage: $0 <region_name> <pbf_path>" >&2
  echo "  region_name   directory name under data/ (e.g. luxembourg)" >&2
  echo "  pbf_path      path to the OSM PBF input (e.g. data/luxembourg.pbf)" >&2
  exit 2
fi

REGION="$1"
PBF="$2"

# Derive the repo root from this script's location: scripts/tire_kicking/ ⇒ ../..
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/../.." && pwd)
cd "$REPO_ROOT"

BIN="./target/release/butterfly-route"
DATA="data/${REGION}"
MODES_DIR="models"
MODES=(car bike foot)

if [[ ! -x "$BIN" ]]; then
  echo "Error: $BIN not found. Run 'cargo build --release -p butterfly-route' first." >&2
  exit 1
fi
if [[ ! -f "$PBF" ]]; then
  echo "Error: PBF input not found: $PBF" >&2
  exit 1
fi

mkdir -p "$DATA"/step{1,2,3,4,5,6,7,8}

echo "=== step1 ==="
time "$BIN" step1-ingest --input "$PBF" --outdir "$DATA/step1"

echo "=== step2 ==="
time "$BIN" step2-profile \
  --ways "$DATA/step1/ways.raw" \
  --relations "$DATA/step1/relations.raw" \
  --models-dir "$MODES_DIR" \
  --outdir "$DATA/step2"

# Build --way-attrs / --turn-rules argv arrays so spaces in REGION /
# alternate checkouts don't trip word-splitting.
WA_ARGS=()
TR_ARGS=()
for m in "${MODES[@]}"; do
  WA_ARGS+=(--way-attrs  "$m=$DATA/step2/way_attrs.$m.bin")
  TR_ARGS+=(--turn-rules "$m=$DATA/step2/turn_rules.$m.bin")
done

echo "=== step3 ==="
time "$BIN" step3-nbg \
  --nodes "$DATA/step1/nodes.sa" \
  --ways "$DATA/step1/ways.raw" \
  "${WA_ARGS[@]}" \
  --outdir "$DATA/step3"

echo "=== step4 ==="
time "$BIN" step4-ebg \
  --nbg-csr "$DATA/step3/nbg.csr" \
  --nbg-geo "$DATA/step3/nbg.geo" \
  --nbg-node-map "$DATA/step3/nbg.node_map" \
  --node-signals "$DATA/step1/node_signals.bin" \
  "${WA_ARGS[@]}" \
  "${TR_ARGS[@]}" \
  --outdir "$DATA/step4"

echo "=== step5 ==="
time "$BIN" step5-weights \
  --ebg-nodes "$DATA/step4/ebg.nodes" \
  --ebg-csr "$DATA/step4/ebg.csr" \
  --turn-table "$DATA/step4/ebg.turn_table" \
  --nbg-geo "$DATA/step3/nbg.geo" \
  "${WA_ARGS[@]}" \
  --outdir "$DATA/step5"

echo "=== step6 ==="
for m in "${MODES[@]}"; do
  time "$BIN" step6-order \
    --filtered-ebg "$DATA/step5/filtered.$m.ebg" \
    --ebg-nodes "$DATA/step4/ebg.nodes" \
    --nbg-geo "$DATA/step3/nbg.geo" \
    --mode "$m" \
    --outdir "$DATA/step6"
done

echo "=== step7 ==="
for m in "${MODES[@]}"; do
  time "$BIN" step7-contract \
    --filtered-ebg "$DATA/step5/filtered.$m.ebg" \
    --order "$DATA/step6/order.$m.ebg" \
    --weights "$DATA/step5/w.$m.u32" \
    --turns "$DATA/step5/t.$m.u32" \
    --mode "$m" \
    --outdir "$DATA/step7"
done

echo "=== step8 ==="
for m in "${MODES[@]}"; do
  time "$BIN" step8-customize \
    --cch-topo "$DATA/step7/cch.$m.topo" \
    --filtered-ebg "$DATA/step5/filtered.$m.ebg" \
    --order "$DATA/step6/order.$m.ebg" \
    --weights "$DATA/step5/w.$m.u32" \
    --turns "$DATA/step5/t.$m.u32" \
    --ebg-nodes "$DATA/step4/ebg.nodes" \
    --mode "$m" \
    --outdir "$DATA/step8"
done

echo "DONE $REGION"
