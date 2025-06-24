#!/bin/bash
# Simple benchmark: butterfly-dl vs curl
# Usage: ./bench.sh <country>
# Example: ./bench.sh monaco

set -e

if [[ $# -ne 1 ]]; then
    echo "Usage: $0 <country>"
    echo "Examples:"
    echo "  $0 monaco"
    echo "  $0 europe/belgium"
    echo "  $0 europe/france"
    exit 1
fi

COUNTRY="$1"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
DOWNLOAD_DIR="$SCRIPT_DIR"

# Build butterfly-dl
echo "Building butterfly-dl..."
cd "$PROJECT_ROOT"
cargo build --release --quiet

echo ""
echo "=== Benchmarking: $COUNTRY ==="
echo ""

# Clean up any existing files
SAFE_NAME=$(echo "$COUNTRY" | tr '/' '_')
rm -f "$DOWNLOAD_DIR/${SAFE_NAME}_butterfly.pbf"
rm -f "$DOWNLOAD_DIR/${SAFE_NAME}_curl.pbf"

# Test butterfly-dl
echo "Testing butterfly-dl..."
start_time=$(date +%s.%N)
"$PROJECT_ROOT/target/release/butterfly-dl" "$COUNTRY" "$DOWNLOAD_DIR/${SAFE_NAME}_butterfly.pbf"
end_time=$(date +%s.%N)
butterfly_duration=$(echo "$end_time - $start_time" | bc -l)

if [[ -f "$DOWNLOAD_DIR/${SAFE_NAME}_butterfly.pbf" ]]; then
    butterfly_size=$(stat -c%s "$DOWNLOAD_DIR/${SAFE_NAME}_butterfly.pbf")
    butterfly_size_mb=$(echo "scale=2; $butterfly_size / 1024 / 1024" | bc -l)
    butterfly_speed=$(echo "scale=2; $butterfly_size_mb / $butterfly_duration" | bc -l)
    echo "  Duration: ${butterfly_duration}s"
    echo "  Size: ${butterfly_size_mb} MB"
    echo "  Speed: ${butterfly_speed} MB/s"
else
    echo "  FAILED"
    exit 1
fi

echo ""

# Test curl
echo "Testing curl..."
URL="https://download.geofabrik.de/${COUNTRY}-latest.osm.pbf"
start_time=$(date +%s.%N)
curl --progress-bar -L -o "$DOWNLOAD_DIR/${SAFE_NAME}_curl.pbf" "$URL"
end_time=$(date +%s.%N)
curl_duration=$(echo "$end_time - $start_time" | bc -l)

if [[ -f "$DOWNLOAD_DIR/${SAFE_NAME}_curl.pbf" ]]; then
    curl_size=$(stat -c%s "$DOWNLOAD_DIR/${SAFE_NAME}_curl.pbf")
    curl_size_mb=$(echo "scale=2; $curl_size / 1024 / 1024" | bc -l)
    curl_speed=$(echo "scale=2; $curl_size_mb / $curl_duration" | bc -l)
    echo "  Duration: ${curl_duration}s"
    echo "  Size: ${curl_size_mb} MB"
    echo "  Speed: ${curl_speed} MB/s"
else
    echo "  FAILED"
    exit 1
fi

echo ""
echo "=== COMPARISON ==="

# Compare times
if (( $(echo "$butterfly_duration < $curl_duration" | bc -l) )); then
    improvement=$(echo "scale=1; ($curl_duration - $butterfly_duration) / $curl_duration * 100" | bc -l)
    echo "⚡ Butterfly-dl is ${improvement}% faster"
else
    slower=$(echo "scale=1; ($butterfly_duration - $curl_duration) / $curl_duration * 100" | bc -l)
    echo "🐌 Butterfly-dl is ${slower}% slower"
fi

# Compare speeds
if (( $(echo "$butterfly_speed > $curl_speed" | bc -l) )); then
    improvement=$(echo "scale=1; ($butterfly_speed - $curl_speed) / $curl_speed * 100" | bc -l)
    echo "🚀 Butterfly-dl has ${improvement}% higher throughput"
else
    lower=$(echo "scale=1; ($curl_speed - $butterfly_speed) / $curl_speed * 100" | bc -l)
    echo "📉 Butterfly-dl has ${lower}% lower throughput"
fi

ratio=$(echo "scale=2; $curl_duration / $butterfly_duration" | bc -l)
echo "📊 Time ratio (curl/butterfly): ${ratio}x"

echo ""

# Clean up test files
echo "Cleaning up test files..."
rm -f "$DOWNLOAD_DIR/${SAFE_NAME}_butterfly.pbf"
rm -f "$DOWNLOAD_DIR/${SAFE_NAME}_curl.pbf"
echo "✅ Test files removed"