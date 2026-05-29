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

CONTAINER="$DATA/belgium.butterfly"

# #412: pre-build the ULTRA transfer graph cache (transit/transfers.bin)
# so the serving pod loads it in seconds instead of paying the
# multi-minute build on its boot path. Idempotent: the subcommand
# cache-HITs and no-ops if the cache is already fresh. Skipped (with a
# warning, non-fatal) if no transit feeds are present — the server can
# still build it lazily at boot in that case.
prebuild_transfers() {
    if [[ ! -d "$DATA/transit" ]]; then
        log "no transit/ dir — skipping transfer prebuild (road-only)"
        return 0
    fi
    log "transit-build-transfers (foot)"
    if time "$BIN" transit-build-transfers --data "$CONTAINER" --modes foot; then
        log "transfer-graph cache built: $DATA/transit/transfers.bin"
    else
        log "WARN: transfer prebuild failed — serve will rebuild lazily on boot"
    fi
}

# Freshness model. Uses the PBF SHA-256 sidecar (`<pbf>.sha256` written
# atomically by butterfly-dl) as the source-of-truth identity for the
# PBF rather than mtime — mtime gets bumped every time the PBF is
# re-downloaded byte-identically.
#
# We persist the sidecar into `<data>/last-source.sha256` after a
# successful pack. CRITICAL distinction: "PBF changed" (last-source
# EXISTS and differs from the current sidecar) forces a rebuild, but
# "PBF identity unknown" (last-source ABSENT — e.g. a PVC packed by an
# older build path that never wrote it) does NOT — we ADOPT the existing
# container rather than burning ~2 h on a needless full rebuild. The
# step8 outputs / container on the PVC are trusted unless we have
# positive evidence the source changed.
LAST_SOURCE="$DATA/last-source.sha256"
PBF_SHA_FILE="${PBF}.sha256"

# pbf_changed=1 ONLY when we have a stored identity AND it differs.
pbf_changed=0
if [[ -f "$LAST_SOURCE" ]]; then
    if [[ ! -f "$PBF_SHA_FILE" ]] || ! cmp -s "$LAST_SOURCE" "$PBF_SHA_FILE"; then
        pbf_changed=1
    fi
fi

all_step8_present=1
for m in "${MODES[@]}"; do
    if [[ ! -f "$DATA/step8/cch.w.${m}.u32" ]]; then
        all_step8_present=0
        break
    fi
done

need_pipeline=0
need_pack=0
if [[ "${BUTTERFLY_FORCE_REBUILD:-0}" == "1" ]]; then
    log "BUTTERFLY_FORCE_REBUILD=1 — forcing full rebuild"
    need_pipeline=1
    need_pack=1
elif [[ "$pbf_changed" -eq 1 ]]; then
    # Positive evidence the PBF changed since the last pack → full rebuild.
    log "PBF changed vs $LAST_SOURCE — full rebuild"
    need_pipeline=1
    need_pack=1
elif [[ ! -f "$CONTAINER" ]]; then
    # Container missing but PBF unchanged/unknown. Pack from step8 if
    # present; only run the full pipeline if step8 is also missing.
    need_pack=1
    if [[ "$all_step8_present" -eq 0 ]]; then
        log "Container + step8 missing — full pipeline"
        need_pipeline=1
    else
        log "Container missing, step8 present, PBF unchanged → packing only"
    fi
fi
# else: container present and PBF unchanged-or-unknown → adopt it.

# Persist the current PBF identity so subsequent runs have a baseline
# (adopting an old PVC writes it for the first time).
adopt_source() {
    if [[ -f "$PBF_SHA_FILE" ]]; then
        cp "$PBF_SHA_FILE" "$LAST_SOURCE"
    fi
}

if [[ "$need_pipeline" -eq 0 ]] && [[ "$need_pack" -eq 0 ]]; then
    log "Container $CONTAINER present and PBF unchanged — adopting"
    adopt_source
    # Still run the transfer prebuild: it is idempotent AND self-healing.
    # transit-build-transfers cache-HITs and returns fast when
    # transfers.bin is already fresh, but REBUILDS when the cache is stale
    # for a reason the PBF SHA can't see — a TRANSFER_ALGO_VERSION bump,
    # or a feeds change that didn't move the PBF. Doing this in the (root,
    # writable) init container guarantees the serving pod never discovers
    # a stale cache and rebuilds on its boot path.
    prebuild_transfers
    log "pipeline DONE — container: $CONTAINER"
    exit 0
fi

if [[ "$need_pipeline" -eq 0 ]]; then
    log "pack -> $CONTAINER"
    time "$BIN" pack --data-dir "$DATA" --out "$CONTAINER" --region BE
    # Persist source identity so the next restart fast-skips correctly.
    if [[ -f "$PBF_SHA_FILE" ]]; then
        cp "$PBF_SHA_FILE" "$LAST_SOURCE"
    fi
    prebuild_transfers
    log "pipeline DONE — container: $CONTAINER"
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

log "pack -> $CONTAINER"
time "$BIN" pack --data-dir "$DATA" --out "$CONTAINER" --region BE

if [[ -f "$PBF_SHA_FILE" ]]; then
    cp "$PBF_SHA_FILE" "$LAST_SOURCE"
fi

prebuild_transfers

log "pipeline DONE — container: $CONTAINER"
