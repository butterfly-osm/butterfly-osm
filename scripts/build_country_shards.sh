#!/usr/bin/env bash
# Build production address shards for #81 from OpenAddresses + OSM PBF.
#
# Per-country strategy:
#   - OpenAddresses (single national feed): direct `build-shard --csv`.
#   - OpenAddresses (multi-region feed):    per-region fragments + merge.
#   - No OA available:                      fall back to `build-shard --pbf`.
#
# Source URLs and per-country fragment lists are pinned via the
# `dl/regions/*.toml` indexes (verified live 2026-05-05). Re-run this
# after `butterfly-dl <country> --only addresses` has staged the
# inputs under `data/<country>[-addr]/addresses/`.
#
# Usage:
#   scripts/build_country_shards.sh                 # build every supported country
#   scripts/build_country_shards.sh be lu fr        # build a subset
set -euo pipefail

cd "$(dirname "$0")/.."
BIN="./target/release/butterfly-geocode"
OUT_DIR="geocode/data/shards"
mkdir -p "$OUT_DIR"

build_oa_single() {
    local iso2="$1" src="$2"
    [ -f "$src" ] || { echo "[skip] $iso2: $src not found"; return 1; }
    "$BIN" build-shard --csv "$src" --source openaddresses \
        --country "$iso2" --out "$OUT_DIR/${iso2,,}.bfgs"
}

build_oa_multi() {
    local iso2="$1"; shift
    local missing=0
    for s in "$@"; do
        [ -f "$s" ] || { echo "[skip] $iso2: missing $s"; missing=1; }
    done
    if [ $missing -eq 1 ]; then return 1; fi
    ./scripts/build_multi_region_shard.sh "$iso2" "$OUT_DIR/${iso2,,}.bfgs" "$@"
}

build_pbf() {
    local iso2="$1" pbf="$2"
    [ -f "$pbf" ] || { echo "[skip] $iso2: $pbf not found"; return 1; }
    "$BIN" build-shard --pbf "$pbf" --source osm \
        --country "$iso2" --out "$OUT_DIR/${iso2,,}.bfgs"
}

build_country() {
    local iso2="$1"
    case "${iso2,,}" in
        lu) build_oa_single LU data/luxembourg-addr/addresses/oa-lu-countrywide.geojson.gz ;;
        at) build_oa_single AT data/austria/addresses/oa-at-countrywide.geojson.gz ;;
        ch) build_oa_single CH data/switzerland/addresses/oa-ch-countrywide.geojson.gz ;;
        fr) build_oa_single FR data/france/addresses/oa-fr-countrywide.geojson.gz ;;
        nl) build_oa_single NL data/netherlands/addresses/oa-nl-countrywide.geojson.gz ;;
        au) build_oa_single AU data/australia-addr/addresses/oa-au-countrywide.geojson.gz ;;
        es) build_oa_single ES data/spain-addr/addresses/oa-es-countrywide.geojson.gz ;;
        be) build_oa_multi BE data/belgium-addr/addresses/oa-be-bru-fr.geojson.gz \
                              data/belgium-addr/addresses/oa-be-bru-nl.geojson.gz \
                              data/belgium-addr/addresses/oa-be-vlg-fr.geojson.gz \
                              data/belgium-addr/addresses/oa-be-vlg-nl.geojson.gz \
                              data/belgium-addr/addresses/oa-be-wal-fr.geojson.gz \
                              data/belgium-addr/addresses/oa-be-wal-de.geojson.gz ;;
        de) build_oa_multi DE data/germany/addresses/oa-de-bb.geojson.gz \
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
        jp) build_oa_multi JP data/japan-addr/addresses/oa-jp-aichi.geojson.gz \
                              data/japan-addr/addresses/oa-jp-chiba.geojson.gz \
                              data/japan-addr/addresses/oa-jp-fukuoka.geojson.gz \
                              data/japan-addr/addresses/oa-jp-gifu.geojson.gz \
                              data/japan-addr/addresses/oa-jp-hyogo.geojson.gz \
                              data/japan-addr/addresses/oa-jp-ibaraki.geojson.gz \
                              data/japan-addr/addresses/oa-jp-saitama.geojson.gz \
                              data/japan-addr/addresses/oa-jp-shizuoka.geojson.gz ;;
        us) build_oa_multi US data/us-addr/addresses/oa-us-az.geojson.gz \
                              data/us-addr/addresses/oa-us-dc.geojson.gz \
                              data/us-addr/addresses/oa-us-de.geojson.gz \
                              data/us-addr/addresses/oa-us-in.geojson.gz \
                              data/us-addr/addresses/oa-us-nc.geojson.gz \
                              data/us-addr/addresses/oa-us-nj.geojson.gz \
                              data/us-addr/addresses/oa-us-ny.geojson.gz \
                              data/us-addr/addresses/oa-us-ri.geojson.gz \
                              data/us-addr/addresses/oa-us-tx.geojson.gz ;;
        br) build_oa_multi BR data/brazil-addr/addresses/oa-br-ce.geojson.gz \
                              data/brazil-addr/addresses/oa-br-mg.geojson.gz \
                              data/brazil-addr/addresses/oa-br-pe.geojson.gz \
                              data/brazil-addr/addresses/oa-br-pr.geojson.gz \
                              data/brazil-addr/addresses/oa-br-rj.geojson.gz \
                              data/brazil-addr/addresses/oa-br-rs.geojson.gz \
                              data/brazil-addr/addresses/oa-br-sc.geojson.gz \
                              data/brazil-addr/addresses/oa-br-sp.geojson.gz ;;
        in) build_pbf IN data/india.pbf ;;
        gb) build_pbf GB data/great-britain.pbf ;;
        it) build_pbf IT data/italy.pbf ;;
        *) echo "unknown country: $iso2" >&2; return 1 ;;
    esac
}

if [ $# -eq 0 ]; then
    set -- be lu nl fr de ch at gb us es it au br jp in
fi

for c in "$@"; do
    echo ""
    echo "=== building $c ==="
    if build_country "$c"; then
        echo "[ok] $c"
    else
        echo "[FAIL] $c — see logs above"
    fi
done
