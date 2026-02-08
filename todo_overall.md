# Butterfly-route Overall Invariants (Must Hold)

## Global input invariants
- Coordinates must be finite WGS84 values: lon `[-180,180]`, lat `[-90,90]`.
- Mode endpoints must accept only `car|bike|foot` (case-insensitive if kept), with documented whitespace behavior.
- Endpoint-specific numeric limits must be enforced without panic:
  - `nearest.number <= 100`
  - `isochrone.time_s in [1,7200]`
  - `match.coordinates <= 500`
  - `trip.coordinates in [2,100]`
  - `isochrone.bulk.origins <= 10000`
  - `height.coordinates <= 10000`
  - `table` cells `<= 10,000,000`

## Routing correctness invariants (`/route`, `/trip`, `/match`)
- `distance >= 0`, `duration >= 0`, never NaN/Inf.
- `route(A,A)` should be zero or near-zero with explicit semantics.
- On mostly symmetric roads: `dist(A,B)` close to `dist(B,A)` (within tolerance).
- Triangle inequality should hold approximately for sampled triplets.
- If `steps=true`, step sums should approximately equal route totals.
- Alternative routes must be geometrically distinct and not better than primary unless explicitly documented.
- `trip` with two points and `round_trip=false` should match `/route`.
- `match` on clean traces should snap to the known road corridor.

## Matrix invariants (`/table`, `/table/stream`)
- Unreachable values must remain explicit (`null` in JSON; `u32::MAX` in Arrow stream).
- `table[i][j]` should closely match `/route(source_i, dest_j)` for same mode.
- Streamed table values must equal non-stream table after unit conversion (`ms` vs `s`).
- Diagonal entries should be zero/near-zero for identical snapped points.

## Isochrone invariants (`/isochrone`, `/isochrone/bulk`)
- Polygon must contain origin snap point (within raster tolerance).
- Monotonicity: larger `time_s` should not produce smaller reachable area.
- Bulk and single isochrone results should match for same origin/time/mode.
- Bulk framing and headers must exactly match payload reality.

## Geometry/format invariants
- GeoJSON coordinate order is always `[lon,lat]`.
- `polyline6`, `geojson`, and `points` representations are coordinate-equivalent within precision tolerance.
- GeoJSON polygon rings must be closed; orientation policy must be consistent (outer CCW, holes CW where applicable).
- Optional fields should follow stable conventions: omitted vs `null` vs empty arrays.

## Operational invariants
- `/health` should be consistently 200 under normal operation.
- Concurrency limits and timeout behavior must protect process stability (no panic/OOM).
- Cancellation paths for streaming endpoints should stop wasted compute quickly.

## Mode-specific invariants
- Every mode endpoint must be exercised for `car`, `bike`, and `foot`.
- Same query across modes should produce mode-appropriate differences (car fastest on roads, foot accesses pedestrian-only links, etc.).
- Mode-inaccessible snaps should fail clearly (not silently degrade to bogus metrics).

## Industry-aligned API behavior targets
- Prefer consistent 4xx domain errors with machine-readable codes (`InvalidValue`, `NoSegment`, `NoRoute`, `NoTable`, `NoMatch`, `TooBig`).
- Keep matrix semantics OSRM/GraphHopper-like (`null` for unreachable per cell).
- Avoid mixing plain-byte and JSON error envelopes across endpoints.

## Current top risk summaries
1. `/route` alternatives path can cause large per-request memory spikes (mitigated: concurrency limit 32, timeout 120s).
2. ~~`/trip` currently encodes unreachable legs as zero duration/distance under `code=Partial`.~~ **FIXED**: unreachable legs now `null`.
3. `/isochrone/bulk` silently drops failed origins while returning HTTP 200 (by design: reflected in headers).
4. ~~Error-envelope inconsistency across endpoints complicates client safety checks.~~ **FIXED**: all endpoints now return JSON errors.
