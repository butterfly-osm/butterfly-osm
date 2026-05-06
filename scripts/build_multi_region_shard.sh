#!/usr/bin/env bash
# Build a multi-region country shard by ingesting each OA file as a per-region
# shard, then merging into the country shard. Used for countries where OA
# publishes one feed per state/prefecture (DE, JP, US, BR).
#
# Usage: build_multi_region_shard.sh <ISO2> <OUT_BFGS> <SRC1> <SRC2> ...
set -euo pipefail
iso2="$1"; shift
out="$1"; shift
tmp=$(mktemp -d)
trap "rm -rf '$tmp'" EXIT

frag_args=()
i=0
for src in "$@"; do
  stem=$(basename "$src" .geojson.gz)
  frag="$tmp/$(printf '%02d' "$i")-$stem.bfgs"
  echo "[+] frag: $stem -> $frag" >&2
  ./target/release/butterfly-geocode build-shard \
    --csv "$src" --source openaddresses \
    --country "$iso2" --out "$frag" 2>&1 | tail -2 >&2
  frag_args+=(--merge "$frag")
  i=$((i+1))
done

echo "[+] merging $i fragments into $out" >&2
./target/release/butterfly-geocode build-shard \
  "${frag_args[@]}" --country "$iso2" --out "$out" 2>&1 | tail -3 >&2
