#!/bin/bash
#
# Build a region's CCH from PBF — idempotent step1 → step8 pipeline
# meant to run inside the butterfly-tools image (in a Kubernetes
# initContainer, an Argo Workflow, or locally).
#
# Skips the rebuild if every step8 output exists and is newer than the
# source PBF. Each step writes into <data-dir>/stepN/.
#
# Usage:
#   butterfly-build-pipeline <data-dir> <pbf-path> [modes...]
#
# Defaults:
#   modes = car bike foot
#   models dir = $BUTTERFLY_MODELS_DIR or /opt/butterfly/models
#
# Environment overrides:
#   BUTTERFLY_BIN          path to butterfly-route (default: butterfly-route on PATH)
#   BUTTERFLY_MODELS_DIR   models directory
#   BUTTERFLY_FORCE_REBUILD=1  ignore freshness check
#

set -euo pipefail

if [[ $# -lt 2 ]]; then
    echo "Usage: $0 <data-dir> <pbf-path> [modes...]" >&2
    exit 2
fi

DATA="$1"; shift
PBF="$1"; shift
if [[ $# -gt 0 ]]; then
    MODES=("$@")
else
    MODES=(car bike foot)
fi

BIN="${BUTTERFLY_BIN:-butterfly-route}"
MODELS_DIR="${BUTTERFLY_MODELS_DIR:-/opt/butterfly/models}"

if ! command -v "$BIN" >/dev/null 2>&1; then
    echo "Error: $BIN not on PATH (set BUTTERFLY_BIN)" >&2
    exit 1
fi
if [[ ! -f "$PBF" ]]; then
    echo "Error: PBF not found: $PBF" >&2
    exit 1
fi
if [[ ! -d "$MODELS_DIR" ]]; then
    echo "Error: models dir not found: $MODELS_DIR" >&2
    exit 1
fi

log() { echo -e "\033[0;32m[pipeline]\033[0m $*"; }

# --- Freshness check: skip if every step8 output is fresher than the PBF
need_rebuild=0
if [[ "${BUTTERFLY_FORCE_REBUILD:-0}" == "1" ]]; then
    log "BUTTERFLY_FORCE_REBUILD=1 — forcing full rebuild"
    need_rebuild=1
else
    for m in "${MODES[@]}"; do
        OUT="$DATA/step8/cch.w.${m}.u32"
        if [[ ! -f "$OUT" ]] || [[ "$PBF" -nt "$OUT" ]]; then
            need_rebuild=1
            log "Missing or stale: $OUT — will rebuild"
            break
        fi
    done
fi

if [[ "$need_rebuild" -eq 0 ]]; then
    log "All step8 outputs are fresh vs PBF — skipping rebuild"
    exit 0
fi

mkdir -p "$DATA"/step{1,2,3,4,5,6,7,8}

log "step1-ingest"
time "$BIN" step1-ingest --input "$PBF" --outdir "$DATA/step1"

log "step2-profile"
time "$BIN" step2-profile \
  --ways "$DATA/step1/ways.raw" \
  --relations "$DATA/step1/relations.raw" \
  --models-dir "$MODELS_DIR" \
  --outdir "$DATA/step2"

WA_ARGS=()
TR_ARGS=()
for m in "${MODES[@]}"; do
    WA_ARGS+=(--way-attrs  "${m}=$DATA/step2/way_attrs.${m}.bin")
    TR_ARGS+=(--turn-rules "${m}=$DATA/step2/turn_rules.${m}.bin")
done

log "step3-nbg"
time "$BIN" step3-nbg \
  --nodes "$DATA/step1/nodes.sa" \
  --ways "$DATA/step1/ways.raw" \
  "${WA_ARGS[@]}" \
  --outdir "$DATA/step3"

log "step4-ebg"
time "$BIN" step4-ebg \
  --nbg-csr "$DATA/step3/nbg.csr" \
  --nbg-geo "$DATA/step3/nbg.geo" \
  --nbg-node-map "$DATA/step3/nbg.node_map" \
  --node-signals "$DATA/step1/node_signals.bin" \
  "${WA_ARGS[@]}" \
  "${TR_ARGS[@]}" \
  --outdir "$DATA/step4"

log "step5-weights"
time "$BIN" step5-weights \
  --ebg-nodes "$DATA/step4/ebg.nodes" \
  --ebg-csr "$DATA/step4/ebg.csr" \
  --turn-table "$DATA/step4/ebg.turn_table" \
  --nbg-geo "$DATA/step3/nbg.geo" \
  "${WA_ARGS[@]}" \
  --outdir "$DATA/step5"

for m in "${MODES[@]}"; do
    log "step6-order $m"
    time "$BIN" step6-order \
      --filtered-ebg "$DATA/step5/filtered.${m}.ebg" \
      --ebg-nodes "$DATA/step4/ebg.nodes" \
      --nbg-geo "$DATA/step3/nbg.geo" \
      --mode "$m" \
      --outdir "$DATA/step6"
done

for m in "${MODES[@]}"; do
    log "step7-contract $m"
    time "$BIN" step7-contract \
      --filtered-ebg "$DATA/step5/filtered.${m}.ebg" \
      --order "$DATA/step6/order.${m}.ebg" \
      --weights "$DATA/step5/w.${m}.u32" \
      --turns "$DATA/step5/t.${m}.u32" \
      --mode "$m" \
      --outdir "$DATA/step7"
done

for m in "${MODES[@]}"; do
    log "step8-customize $m"
    time "$BIN" step8-customize \
      --cch-topo "$DATA/step7/cch.${m}.topo" \
      --filtered-ebg "$DATA/step5/filtered.${m}.ebg" \
      --order "$DATA/step6/order.${m}.ebg" \
      --weights "$DATA/step5/w.${m}.u32" \
      --turns "$DATA/step5/t.${m}.u32" \
      --ebg-nodes "$DATA/step4/ebg.nodes" \
      --mode "$m" \
      --outdir "$DATA/step8"
done

log "pipeline DONE — artefacts in $DATA/step8/"
