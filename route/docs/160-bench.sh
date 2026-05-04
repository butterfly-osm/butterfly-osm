#!/usr/bin/env bash
# Boot-timing harness for #160 lazy CRC verification.
#
# Usage:
#   bash route/docs/160-bench.sh <eager|lazy|warmup> [container_path]
#
# Captures: time-to-/health-ready, RSS timeline (RSS_CHECKPOINT lines),
# /health snapshot, post-100-route RSS.
#
# The script does NOT drop page caches — that requires root. For a
# fully cold first-run measurement, the operator should `sync; echo 3 |
# sudo tee /proc/sys/vm/drop_caches` between runs. The unmodified
# behaviour against a warm page cache is still informative because the
# difference between eager and lazy in that regime is dominated by CPU
# (the CRC walk runs entirely from page cache, but still touches every
# byte through the user-space CRC routine).

set -euo pipefail

MODE="${1:?eager|lazy|warmup required}"
DATA="${2:-/home/snape/projects/butterfly-osm/data/belgium-155.butterfly}"

case "$MODE" in
  eager) FLAGS=(--eager-verify) ;;
  lazy) FLAGS=() ;;
  warmup) FLAGS=(--warmup-on-boot) ;;
  *) echo "unknown mode: $MODE" >&2 ; exit 2 ;;
esac

# Pick a free port deterministically per mode so two runs don't collide.
case "$MODE" in
  eager) PORT=8160 ;;
  lazy) PORT=8161 ;;
  warmup) PORT=8162 ;;
esac

LOG=/tmp/butterfly-160-${MODE}.log
echo "Starting butterfly-route ${MODE}, port ${PORT}, log ${LOG}"

./target/release/butterfly-route serve \
  --data "$DATA" \
  --port "$PORT" \
  --transport rest \
  --rss-checkpoints \
  --log-format text \
  "${FLAGS[@]}" \
  > "$LOG" 2>&1 &
PID=$!

trap "kill $PID 2>/dev/null || true; wait 2>/dev/null || true" EXIT

# Poll /health for first-success time. We poll /health rather than
# parsing the log because the listener bind happens after every blocking
# init step (LoadOptions::eager_verify path included).
deadline=$(( $(date +%s) + 600 ))
while true; do
  if curl -sf "http://127.0.0.1:${PORT}/health" >/dev/null 2>&1; then
    break
  fi
  if (( $(date +%s) > deadline )); then
    echo "/health did not become ready within 600 s" >&2
    tail -n 50 "$LOG" >&2
    exit 1
  fi
  sleep 0.1
done

echo "--- /health (post-boot) ---"
curl -s "http://127.0.0.1:${PORT}/health" | head -c 4000
echo
echo
echo "--- /proc/${PID}/smaps_rollup (post-boot) ---"
cat /proc/${PID}/smaps_rollup | head -20

# Run 100 routes to generate steady-state working set.
echo "--- running 100 routes ---"
PYBIN=$(command -v python3 || true)
if [[ -n "$PYBIN" ]]; then
  $PYBIN <<EOF
import urllib.request
import json
import random
random.seed(42)
n=100
ok=0
for i in range(n):
    # Random points inside Belgium bbox (loose).
    lon1 = random.uniform(2.5, 6.4)
    lat1 = random.uniform(49.5, 51.5)
    lon2 = random.uniform(2.5, 6.4)
    lat2 = random.uniform(49.5, 51.5)
    url = f"http://127.0.0.1:${PORT}/route?src_lon={lon1}&src_lat={lat1}&dst_lon={lon2}&dst_lat={lat2}&mode=car"
    try:
        req = urllib.request.urlopen(url, timeout=20)
        if req.status == 200:
            ok += 1
    except Exception:
        pass
print(f"{ok}/{n} route requests OK")
EOF
fi

echo "--- /proc/${PID}/smaps_rollup (post-100-route) ---"
cat /proc/${PID}/smaps_rollup | head -20

echo "--- /metrics excerpts (verify counters) ---"
curl -s "http://127.0.0.1:${PORT}/metrics" 2>/dev/null | grep -E "butterfly_route_section|verify" | head -30

echo "--- RSS_CHECKPOINT lines ---"
grep RSS_CHECKPOINT "$LOG" | tail -20

echo "--- elapsed timestamps from log ---"
head -30 "$LOG" | grep -E "elapsed_s=|Step 9|loading server" | head -10

# Also wait briefly to give a warmup-on-boot's background pass a chance
# to complete before we shut down.
if [[ "$MODE" == "warmup" ]]; then
  echo "--- waiting 90 s for background warmup to complete ---"
  sleep 90
  echo "--- /health (post-warmup-wait) ---"
  curl -s "http://127.0.0.1:${PORT}/health" | head -c 2000
  echo
  echo "--- /proc/${PID}/smaps_rollup (post-warmup-wait) ---"
  cat /proc/${PID}/smaps_rollup | head -20
fi

echo "--- shutting down ---"
kill -TERM $PID 2>/dev/null || true
wait $PID 2>/dev/null || true
echo "done"
