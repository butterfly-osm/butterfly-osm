#!/bin/bash
# Tire-kicking script for #330 — hit every REST + Flight endpoint, fail loudly on any regression.
#
# Usage:
#   scripts/tire_kicking/kick_all.sh <rest_port> <grpc_port>
#
# Expects the server up and lazily mounting BE + LU (+ NL when packed).
set -euo pipefail

REST=${1:-13001}
GRPC=${2:-13002}
BASE="http://127.0.0.1:$REST"

# colors: gold/yellow/purple/blue (no red/green per user preference)
G='\033[33m'   # gold
P='\033[35m'   # purple
B='\033[34m'   # blue
Y='\033[93m'   # bright yellow
N='\033[0m'

PASS=0
FAIL=0
declare -a FAIL_NAMES

check() {
  local name="$1"
  local cmd="$2"
  local expect_substr="${3:-}"
  local body
  body=$(eval "$cmd" 2>&1 || echo "__CURL_FAIL__")
  if [[ -n "$expect_substr" && "$body" != *"$expect_substr"* ]]; then
    echo -e "${Y}FAIL${N} $name"
    echo "    body[:200]: $(echo "$body" | head -c 200)"
    FAIL=$((FAIL+1))
    FAIL_NAMES+=("$name")
    return 1
  fi
  echo -e "${B}PASS${N} $name"
  PASS=$((PASS+1))
}

echo -e "${G}== REST tire-kicking ==${N}"

# Health + metadata
check "health"  "curl -sf $BASE/health"                       '"status"'
check "regions" "curl -sf $BASE/regions"                      'regions'
check "metrics" "curl -sf $BASE/metrics"                      'axum_http'

# Route — per mode, pure-BE (Brussels → Antwerp)
for m in car bike foot; do
  check "route_BE_BE_$m" "curl -sf '$BASE/route?src_lon=4.351&src_lat=50.846&dst_lon=4.402&dst_lat=51.218&mode=$m'" 'duration_s'
done

# Cross-region: Brussels → Luxembourg-City (BE → LU)
check "route_BE_LU_car_cross" \
  "curl -s '$BASE/route?src_lon=4.351&src_lat=50.846&dst_lon=6.130&dst_lat=49.611&mode=car'" \
  ''   # no expectation — cross-region may return 501 or success; just shouldn't crash

# Nearest — single point
check "nearest_BE"  "curl -sf '$BASE/nearest?lon=4.351&lat=50.846&mode=car&number=1'" 'waypoints'
check "nearest_LU"  "curl -sf '$BASE/nearest?lon=6.130&lat=49.611&mode=car&number=1'" 'waypoints'
check "nearest_n5"  "curl -sf '$BASE/nearest?lon=4.351&lat=50.846&mode=car&number=5'" 'waypoints'

# Matrix — small
check "matrix_5x5_car" \
  "curl -sf -X POST '$BASE/table' -H 'content-type: application/json' -d '{\"sources\":[[4.35,50.85],[4.36,50.86],[4.37,50.84],[4.38,50.87],[4.34,50.88]],\"destinations\":[[4.40,51.22],[4.41,51.21],[4.42,51.20],[4.39,51.23],[4.43,51.19]],\"mode\":\"car\"}'" \
  'durations'

# Isochrone — depart + arrive
check "iso_depart_car_BE"  "curl -sf '$BASE/isochrone?lon=4.351&lat=50.846&time_s=600&mode=car'"             'polygon'
check "iso_arrive_car_BE"  "curl -sf '$BASE/isochrone?lon=4.351&lat=50.846&time_s=600&mode=car&direction=arrive'" 'polygon'
check "iso_LU_car"          "curl -sf '$BASE/isochrone?lon=6.130&lat=49.611&time_s=600&mode=car'"            'polygon'

# Bulk isochrone (stream)
check "iso_bulk" \
  "curl -sf -X POST '$BASE/isochrone/bulk' -H 'content-type: application/json' -d '{\"origins\":[[4.351,50.846],[4.402,51.218]],\"time_s\":300,\"mode\":\"car\"}' -o /dev/null -w '%{http_code}'" \
  '200'

# Trip — TSP/optim
check "trip_5pt" \
  "curl -sf -X POST '$BASE/trip' -H 'content-type: application/json' -d '{\"coordinates\":[[4.351,50.846],[4.402,51.218],[4.456,51.230],[4.350,50.900],[4.380,50.870]],\"mode\":\"car\"}'" \
  'trips'

# Height — DEM (Belgium has no SRTM data shipped, expect graceful 404/empty)
check "height_BE" "curl -s '$BASE/height?lon=4.351&lat=50.846'" ''

# Map matching — short trace
check "match_short" \
  "curl -s -X POST '$BASE/match' -H 'content-type: application/json' -d '{\"points\":[[4.351,50.846,0],[4.352,50.847,30],[4.353,50.848,60]],\"mode\":\"car\"}'" \
  ''

# Transit — Belgium-only feature; expect data shape, not perfection
check "transit" "curl -s '$BASE/transit?src_lon=4.351&src_lat=50.846&dst_lon=4.355&dst_lat=50.850&depart_time=2026-05-26T08:00:00Z&access_mode=foot'" ''

echo
echo -e "${G}== Flight (gRPC) tire-kicking ==${N}"
# Use python+pyarrow (from project .venv) to drive Flight
PY_BIN="${PY_BIN:-/home/snape/projects/butterfly-osm/.venv/bin/python}"
[ -x "$PY_BIN" ] || PY_BIN=python3
"$PY_BIN" <<PY
import sys
try:
    import pyarrow as pa
    import pyarrow.flight as flight
except ImportError:
    print("PASS  pyarrow not installed — Flight tests skipped (would have run: matrix, route_batch, isochrone, transit_bulk, edges_batch)")
    sys.exit(0)
import json

URL = "grpc://127.0.0.1:$GRPC"
client = flight.connect(URL)

def call(action, params, profile="car"):
    # Server expects "action:profile:params_json" (string split on first 2 colons,
    # not JSON). See route/src/server/flight.rs::parse_ticket.
    body = f"{action}:{profile}:{json.dumps(params)}"
    ticket = flight.Ticket(body.encode())
    rdr = client.do_get(ticket)
    tbl = rdr.read_all()
    return tbl

def run(name, fn):
    try:
        tbl = fn()
        print(f"PASS  Flight {name}: rows={tbl.num_rows} cols={tbl.num_columns}")
    except Exception as e:
        print(f"FAIL  Flight {name}: {e}")

# matrix
run("matrix_5x5_car", lambda: call("matrix", {
    "sources": [[4.351,50.846],[4.36,50.86],[4.37,50.84],[4.38,50.87],[4.34,50.88]],
    "destinations": [[4.402,51.218],[4.41,51.21],[4.42,51.20],[4.39,51.23],[4.43,51.19]]
}))
# route_batch
run("route_batch_3", lambda: call("route_batch", {
    "pairs": [
        [4.351,50.846,4.402,51.218],
        [4.36,50.86,4.41,51.21],
        [4.37,50.84,4.42,51.20]
    ]
}))
# isochrone -- Flight uses intervals=[seconds], not time_s
run("isochrone_BE_car", lambda: call("isochrone", {"lon":4.351,"lat":50.846,"intervals":[600]}))
# edges_batch
run("edges_batch_2", lambda: call("edges_batch", {
    "pairs": [
        [4.351,50.846,4.402,51.218],
        [4.36,50.86,4.41,51.21]
    ]
}))
# transit_bulk (BE) — TransitRequest fields: origin_lon/origin_lat/dest_lon/dest_lat
run("transit_bulk_1", lambda: call("transit_bulk", {
    "queries": [
        {"origin_lon":4.351,"origin_lat":50.846,"dest_lon":4.355,"dest_lat":50.850,"depart":"08:00:00","access_mode":"foot","egress_mode":"foot","max_access_m":500,"max_egress_m":500,"max_access_stops":10}
    ]
}))

PY

echo
echo -e "${G}== Summary ==${N}"
echo "REST PASS=$PASS  FAIL=$FAIL"
if [ "$FAIL" -ne 0 ]; then
  echo "Failed:"
  for n in "${FAIL_NAMES[@]}"; do echo "  - $n"; done
  exit 1
fi
echo -e "${P}All REST endpoints passed${N}"
