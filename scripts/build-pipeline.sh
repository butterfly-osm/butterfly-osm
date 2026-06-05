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

# #433: car traffic is no longer baked at build time — the build ships a
# provider-clean single legal-limit car and the engine recustomizes it at
# serve boot from a runtime observed_speeds.parquet (see the step8 section
# below). The build-time bake (#392), rush_hour variant (#415) and
# observed-speeds calibration (#388) are gone, so TRAFFIC_DIR / the per-role
# profile names are no longer consulted here.
#
# Bumping RECIPE_VERSION forces a one-time rebuild on the next deploy so the
# existing traffic-BAKED container on the PVC is replaced with the clean one
# (the fingerprint below folds this in).
RECIPE_VERSION="car-clean-legal-limit+serve-boot-recustomize-v1"

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

# #424/#433 recipe fingerprint: per-mode model selection lives in the
# *.model.json files, so changing one must force a full rebuild even when the
# PBF is byte-identical. #433 dropped build-time traffic (bake/variant/observed
# calibration), so the traffic profiles + observed-speeds table no longer affect
# the artifact and are NO LONGER folded in — but RECIPE_VERSION bumped, so a PVC
# holding the old traffic-baked container rebuilds once into the clean one.
LAST_RECIPE="$DATA/last-recipe"
recipe_fingerprint() {
    # Content-only fingerprint over the model files, in a locale-stable
    # (LC_ALL=C) order. `sha256sum < file` emits the hash with no path, so the
    # fingerprint is path- and readdir-order-independent; the basename is
    # included so a mode rename still counts as a change.
    {
        echo "$RECIPE_VERSION"
        # Emit "basename <content-hash>" per model file, then sort by that line —
        # ordering is determined by basename + content only, NOT directory path,
        # so MODELS_DIR location can't change the fingerprint.
        {
            for f in "$MODELS_DIR"/*.model.json; do
                [[ -f "$f" ]] || continue
                printf '%s %s\n' "$(basename "$f")" "$(sha256sum <"$f" | cut -d' ' -f1)"
            done
        } | LC_ALL=C sort
    } | sha256sum | cut -d' ' -f1
}
RECIPE_FP="$(recipe_fingerprint)"
# #424: the fingerprint covers the *.model.json files, so a model edit forces a
# rebuild for ANY mode set (computed unconditionally — not gated on whether car
# is in the build, which previously let a foot/bike build silently adopt a stale
# container after a model edit).
recipe_changed=0
if [[ ! -f "$LAST_RECIPE" ]] || [[ "$(cat "$LAST_RECIPE" 2>/dev/null)" != "$RECIPE_FP" ]]; then
    recipe_changed=1
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
elif [[ "$recipe_changed" -eq 1 ]]; then
    # Model recipe or RECIPE_VERSION changed (e.g. #433 clean-car bump, or a
    # PVC holding the old traffic-baked container) → full rebuild.
    log "recipe changed (fp=$RECIPE_FP) — full rebuild"
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

# Persist the current PBF identity + recipe fingerprint so subsequent runs
# fast-skip correctly (adopting an old PVC writes them for the first time).
adopt_source() {
    [[ -f "$PBF_SHA_FILE" ]] && cp "$PBF_SHA_FILE" "$LAST_SOURCE"
    echo "$RECIPE_FP" >"$LAST_RECIPE"
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
    # Persist source identity + recipe fingerprint for the next restart.
    adopt_source
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

# #424: steps 6/7/8 run sequentially per mode ON PURPOSE — do NOT fan the modes
# out concurrently. Each invocation is already internally rayon-parallel AND holds
# a multi-GB working set (filtered EBG + order + weighted_adj/atomic weight Vecs);
# running modes in parallel would multiply peak RSS by the mode count and blow the
# 78 GB staging budget. step7 also writes a non-mode-keyed shortcuts.tmp that
# concurrent runs would clobber. Keep these loops sequential.
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

# #433: car traffic calibration moved from BUILD-time to SERVE-BOOT, so this
# pipeline now ships a PROVIDER-CLEAN, single legal-limit car (the step8 loop
# above is the whole story for car). The build no longer:
#   - reads BUTTERFLY_OBSERVED_SPEEDS / runs calibrate-traffic (#388),
#   - bakes a 'realistic' friction profile into base car (#392),
#   - builds a car_rush_hour variant (#415).
# Instead, at serve startup the engine fits ONE car profile from a runtime
# `observed_speeds.parquet` staged on the data volume by a deploy init
# container, and recustomizes the car CCH weights in memory — see
# `ServerState::recustomize_car_from_observed`. The artifact carries no
# provider-derived data; `pack` already ships the step4-7 car inputs
# (way_attrs / filtered_ebg / node_weights.turn / ebg.nodes / nbg.geo) the boot
# recustomize re-reads, so this stays a single clean legal-limit build.

log "pack -> $CONTAINER"
time "$BIN" pack --data-dir "$DATA" --out "$CONTAINER" --region BE

adopt_source

prebuild_transfers

log "pipeline DONE — container: $CONTAINER"
