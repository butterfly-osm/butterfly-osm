#!/usr/bin/env bash
# Emit a proof file documenting a single shard's provenance and a
# 3-query smoke test against it. Used by #81 to ship per-country proof
# files alongside `geocode/data/shards/*.bfgs`.
#
# Usage: gen_shard_proof.sh <ISO2> <SHARD_PATH> <SOURCE_LABEL> <PROOF_OUT> "query1" "query2" "query3"
#
# Source label is freeform (e.g. "OpenAddresses oa-be-* (6 regional feeds)"
# or "Geofabrik OSM PBF addr:* tags").
set -euo pipefail

iso2="$1"
shard="$2"
source_label="$3"
proof="$4"
shift 4

mkdir -p "$(dirname "$proof")"

# Boot a single-shard server on a random high port so multiple proofs can
# run concurrently without colliding.
port=$((RANDOM % 10000 + 30000))
log=$(mktemp)
./target/release/butterfly-geocode serve \
    --shard "$shard" \
    --rest-port "$port" --grpc-port "$((port + 1))" \
    --transport rest \
    --log-format text > "$log" 2>&1 &
pid=$!
trap "kill $pid 2>/dev/null || true; rm -f $log" EXIT

# Wait for /health to come up.
for _ in $(seq 1 60); do
    if curl -fsS "http://127.0.0.1:$port/health" > /dev/null 2>&1; then
        break
    fi
    sleep 0.5
done

if ! curl -fsS "http://127.0.0.1:$port/health" > /dev/null 2>&1; then
    echo "ERROR: server did not come up in 30s" >&2
    cat "$log" >&2
    exit 1
fi

size=$(stat -c%s "$shard")
size_mb=$(awk "BEGIN{printf \"%.1f\", $size/1048576}")
sha=$(sha256sum "$shard" | awk '{print $1}')
date_utc=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

{
    echo "# Shard proof — $iso2"
    echo "# Generated $date_utc"
    echo "#"
    echo "# Path:        $shard"
    echo "# Size:        ${size_mb} MB ($size bytes)"
    echo "# SHA-256:     $sha"
    echo "# Source:      $source_label"
    echo "# Format:      BFGS v5 (CRC-verified at server load)"
    echo ""
    echo "## /health"
    curl -fsS "http://127.0.0.1:$port/health" | python3 -m json.tool
    echo ""
    for q in "$@"; do
        encoded=$(python3 -c "import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1]))" "$q")
        echo "## GET /geocode?q=${q}&country=${iso2}"
        curl -fsS "http://127.0.0.1:$port/geocode?q=${encoded}&country=${iso2}&limit=3" \
            | python3 -m json.tool
        echo ""
    done
} > "$proof"

echo "wrote $proof"
