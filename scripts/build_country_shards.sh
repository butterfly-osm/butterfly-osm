#!/usr/bin/env bash
# Build production address shards for #81 from OpenAddresses + OSM PBF.
#
# Per-country source strategy (default "auto"):
#   - OpenAddresses single feed:   build OA shard only
#   - OpenAddresses multi-region:  per-region fragments + merge into OA shard
#   - OSM PBF only (no OA feed):   build OSM shard
#   - OA + OSM PBF both available: build BOTH then merge into final shard
#                                   (OA wins on dedup; #81 BE/LU validated)
#
# The OA+OSM merge closes the recall gap on POI-light address corpora by
# adding the ~3-4M OSM `addr:*` records that OpenAddresses misses
# (verified: BE OA=7.93M, OSM=4.03M, merged=10.49M / 1.46M deduped).
#
# Override per-country via `--source-strategy`:
#   auto      (default) merge OA+OSM when both feeds exist
#   oa-only   force OA-only build (skips OSM merge even if PBF present)
#   osm-only  force OSM-only build (skips OA, useful for IN/IT/GB)
#
# Source URLs and per-country fragment lists are pinned via the
# `dl/regions/*.toml` indexes (verified live 2026-05-05). Re-run this
# after `butterfly-dl <country> --only addresses` has staged the
# inputs under `data/<country>[-addr]/addresses/`.
#
# Usage:
#   scripts/build_country_shards.sh                           # build every supported country, auto strategy
#   scripts/build_country_shards.sh be lu fr                  # build a subset
#   scripts/build_country_shards.sh --source-strategy oa-only be   # opt out of OSM merge for BE
set -euo pipefail

cd "$(dirname "$0")/.."
BIN="./target/release/butterfly-geocode"
OUT_DIR="geocode/data/shards"
mkdir -p "$OUT_DIR"

STRATEGY="auto"

# -------- arg parsing --------
ARGS=()
while [ $# -gt 0 ]; do
    case "$1" in
        --source-strategy)
            STRATEGY="${2:-auto}"; shift 2 ;;
        --source-strategy=*)
            STRATEGY="${1#--source-strategy=}"; shift ;;
        -h|--help)
            sed -n '2,30p' "$0"; exit 0 ;;
        --) shift; while [ $# -gt 0 ]; do ARGS+=("$1"); shift; done; break ;;
        -*) echo "unknown flag: $1" >&2; exit 2 ;;
        *) ARGS+=("$1"); shift ;;
    esac
done

case "$STRATEGY" in
    auto|oa-only|osm-only) ;;
    *) echo "invalid --source-strategy '$STRATEGY' (auto|oa-only|osm-only)" >&2; exit 2 ;;
esac

# Per-country OSM PBF mapping (data/<file>.pbf paths). Empty string
# means no PBF available — those countries fall back to OA-only.
osm_pbf_for() {
    case "${1,,}" in
        be) echo "data/belgium.pbf" ;;
        lu) echo "data/luxembourg.pbf" ;;
        nl) echo "data/netherlands.pbf" ;;
        fr) echo "data/france.pbf" ;;
        de) echo "data/germany.pbf" ;;
        ch) echo "data/switzerland.pbf" ;;
        at) echo "data/austria.pbf" ;;
        gb) echo "data/great-britain.pbf" ;;
        es) echo "data/spain.pbf" ;;
        it) echo "data/italy.pbf" ;;
        au) echo "data/australia.pbf" ;;
        br) echo "data/brazil.pbf" ;;
        jp) echo "data/japan.pbf" ;;
        in) echo "data/india.pbf" ;;
        us) echo "" ;;  # no countrywide US PBF in repo (us-northeast.pbf only)
        *)  echo "" ;;
    esac
}

# -------- build helpers --------
# Build a single OA shard. Output path is the third arg.
build_oa_single() {
    local iso2="$1" src="$2" out="$3"
    [ -f "$src" ] || { echo "[skip] $iso2: $src not found"; return 1; }
    "$BIN" build-shard --csv "$src" --source openaddresses \
        --country "$iso2" --out "$out"
}

# Build an OA shard from multiple regional feeds. Output path is the second arg.
build_oa_multi() {
    local iso2="$1" out="$2"; shift 2
    local missing=0
    for s in "$@"; do
        [ -f "$s" ] || { echo "[skip] $iso2: missing $s"; missing=1; }
    done
    if [ $missing -eq 1 ]; then return 1; fi
    ./scripts/build_multi_region_shard.sh "$iso2" "$out" "$@"
}

# Build an OSM-only shard from a country PBF. Output path is the third arg.
build_osm_only() {
    local iso2="$1" pbf="$2" out="$3"
    [ -f "$pbf" ] || { echo "[skip] $iso2: $pbf not found"; return 1; }
    "$BIN" build-shard --pbf "$pbf" --source osm \
        --country "$iso2" --out "$out"
}

# Merge two existing shards (OA + OSM) into a final shard.
merge_oa_osm() {
    local iso2="$1" oa_shard="$2" osm_shard="$3" out="$4"
    "$BIN" build-shard \
        --merge "$oa_shard" --merge "$osm_shard" \
        --country "$iso2" --out "$out"
}

# OA-build dispatcher: writes <iso2>.oa.bfgs to OUT_DIR. Returns 0 on success.
build_oa_for() {
    local iso2="$1" out="$2"
    case "${iso2,,}" in
        lu) build_oa_single LU data/luxembourg-addr/addresses/oa-lu-countrywide.geojson.gz "$out" ;;
        at) build_oa_single AT data/austria/addresses/oa-at-countrywide.geojson.gz "$out" ;;
        ch) build_oa_single CH data/switzerland/addresses/oa-ch-countrywide.geojson.gz "$out" ;;
        fr) build_oa_single FR data/france/addresses/oa-fr-countrywide.geojson.gz "$out" ;;
        nl) build_oa_single NL data/netherlands/addresses/oa-nl-countrywide.geojson.gz "$out" ;;
        au) build_oa_single AU data/australia-addr/addresses/oa-au-countrywide.geojson.gz "$out" ;;
        es) build_oa_single ES data/spain-addr/addresses/oa-es-countrywide.geojson.gz "$out" ;;
        be) build_oa_multi BE "$out" data/belgium-addr/addresses/oa-be-bru-fr.geojson.gz \
                                     data/belgium-addr/addresses/oa-be-bru-nl.geojson.gz \
                                     data/belgium-addr/addresses/oa-be-vlg-fr.geojson.gz \
                                     data/belgium-addr/addresses/oa-be-vlg-nl.geojson.gz \
                                     data/belgium-addr/addresses/oa-be-wal-fr.geojson.gz \
                                     data/belgium-addr/addresses/oa-be-wal-de.geojson.gz ;;
        de) build_oa_multi DE "$out" data/germany/addresses/oa-de-bb.geojson.gz \
                                     data/germany/addresses/oa-de-bw.geojson.gz \
                                     data/germany/addresses/oa-de-hb.geojson.gz \
                                     data/germany/addresses/oa-de-he.geojson.gz \
                                     data/germany/addresses/oa-de-hh.geojson.gz \
                                     data/germany/addresses/oa-de-mv.geojson.gz \
                                     data/germany/addresses/oa-de-ni.geojson.gz \
                                     data/germany/addresses/oa-de-nw.geojson.gz \
                                     data/germany/addresses/oa-de-rp.geojson.gz \
                                     data/germany/addresses/oa-de-sh.geojson.gz \
                                     data/germany/addresses/oa-de-sl.geojson.gz \
                                     data/germany/addresses/oa-de-sn.geojson.gz \
                                     data/germany/addresses/oa-de-st.geojson.gz \
                                     data/germany/addresses/oa-de-th.geojson.gz ;;
        jp) build_oa_multi JP "$out" data/japan-addr/addresses/oa-jp-aichi.geojson.gz \
                                     data/japan-addr/addresses/oa-jp-chiba.geojson.gz \
                                     data/japan-addr/addresses/oa-jp-fukuoka.geojson.gz \
                                     data/japan-addr/addresses/oa-jp-gifu.geojson.gz \
                                     data/japan-addr/addresses/oa-jp-hyogo.geojson.gz \
                                     data/japan-addr/addresses/oa-jp-ibaraki.geojson.gz \
                                     data/japan-addr/addresses/oa-jp-saitama.geojson.gz \
                                     data/japan-addr/addresses/oa-jp-shizuoka.geojson.gz ;;
        us) build_oa_multi US "$out" data/us-addr/addresses/oa-us-az.geojson.gz \
                                     data/us-addr/addresses/oa-us-dc.geojson.gz \
                                     data/us-addr/addresses/oa-us-de.geojson.gz \
                                     data/us-addr/addresses/oa-us-in.geojson.gz \
                                     data/us-addr/addresses/oa-us-nc.geojson.gz \
                                     data/us-addr/addresses/oa-us-nj.geojson.gz \
                                     data/us-addr/addresses/oa-us-ny.geojson.gz \
                                     data/us-addr/addresses/oa-us-ri.geojson.gz \
                                     data/us-addr/addresses/oa-us-tx.geojson.gz ;;
        br) build_oa_multi BR "$out" data/brazil-addr/addresses/oa-br-ce.geojson.gz \
                                     data/brazil-addr/addresses/oa-br-mg.geojson.gz \
                                     data/brazil-addr/addresses/oa-br-pe.geojson.gz \
                                     data/brazil-addr/addresses/oa-br-pr.geojson.gz \
                                     data/brazil-addr/addresses/oa-br-rj.geojson.gz \
                                     data/brazil-addr/addresses/oa-br-rs.geojson.gz \
                                     data/brazil-addr/addresses/oa-br-sc.geojson.gz \
                                     data/brazil-addr/addresses/oa-br-sp.geojson.gz ;;
        in|gb|it) return 1 ;;  # no OA feed shipped — caller picks osm-only
        *) return 1 ;;
    esac
}

build_country() {
    local iso2="$1"
    local lc="${iso2,,}"
    local final_out="$OUT_DIR/${lc}.bfgs"
    local pbf
    pbf="$(osm_pbf_for "$lc")"

    # Resolve effective strategy. `auto` upgrades to merge when both
    # OA + PBF are available; falls back to oa-only / osm-only otherwise.
    local strat="$STRATEGY"
    case "$lc" in
        in|gb|it) strat="osm-only" ;;  # no OA feed
    esac
    if [ "$strat" = "auto" ]; then
        if [ -n "$pbf" ] && [ -f "$pbf" ]; then
            strat="merge"
        else
            strat="oa-only"
        fi
    fi

    case "$strat" in
        oa-only)
            echo "[strategy] $iso2: oa-only -> $final_out" >&2
            build_oa_for "$lc" "$final_out"
            ;;
        osm-only)
            if [ -z "$pbf" ]; then
                echo "[FAIL] $iso2: osm-only requested but no PBF mapped" >&2
                return 1
            fi
            echo "[strategy] $iso2: osm-only -> $final_out" >&2
            build_osm_only "$lc" "$pbf" "$final_out"
            ;;
        merge)
            echo "[strategy] $iso2: merge OA+OSM -> $final_out" >&2
            local oa_tmp="$OUT_DIR/${lc}.oa.bfgs"
            local osm_tmp="$OUT_DIR/${lc}.osm.bfgs"
            if ! build_oa_for "$lc" "$oa_tmp"; then
                echo "[FAIL] $iso2: OA build failed in merge strategy" >&2
                return 1
            fi
            if ! build_osm_only "$lc" "$pbf" "$osm_tmp"; then
                echo "[FAIL] $iso2: OSM build failed in merge strategy" >&2
                return 1
            fi
            merge_oa_osm "$lc" "$oa_tmp" "$osm_tmp" "$final_out"
            # Keep intermediates around for proof-file generation; the
            # rebuild tooling (`gen_shard_proof.sh`) reads them.
            ;;
    esac
}

if [ ${#ARGS[@]} -eq 0 ]; then
    set -- be lu nl fr de ch at gb us es it au br jp in
else
    set -- "${ARGS[@]}"
fi

for c in "$@"; do
    echo ""
    echo "=== building $c (strategy=$STRATEGY) ==="
    if build_country "$c"; then
        echo "[ok] $c"
    else
        echo "[FAIL] $c — see logs above"
    fi
done
