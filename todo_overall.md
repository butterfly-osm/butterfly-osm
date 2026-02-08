# Butterfly-route Overall Plan

## Competitive Feature Matrix (vs OSRM / Valhalla / GraphHopper)

### Endpoints / Services

| Feature | Butterfly | OSRM | Valhalla | GraphHopper |
|---------|:---------:|:----:|:--------:|:-----------:|
| P2P Routing | `/route` | `/route` | `/route` | `/route` |
| Nearest snap | `/nearest` | `/nearest` | `/locate` | (inline) |
| Distance matrix | `/table` | `/table` | `/sources_to_targets` | `/matrix` |
| Streaming matrix | `/table/stream` (Arrow) | - | - | - |
| Isochrone | `/isochrone` | - | `/isochrone` | `/isochrone` |
| Bulk isochrone | `/isochrone/bulk` | - | - | - |
| Map matching | `/match` | `/match` | `/trace_route` | `/match` |
| Trip / TSP | `/trip` | `/trip` | `/optimized_route` | `/route` (optimize) |
| Elevation | `/height` | - | `/height` | (inline) |
| Health | `/health` | - | `/status` | `/health` |
| Swagger/OpenAPI | `/swagger-ui` | - | - | - |
| Prometheus metrics | `/metrics` | - | - | - |
| VRP (multi-vehicle) | - | - | - | `/optimization` |
| Geocoding | - | - | - | `/geocode` |
| Clustering | - | - | - | `/cluster` |
| Debug tiles | - | `/tile` (MVT) | - | - |
| Graph metadata | - | - | `/locate` (rich) | - |
| Expansion viz | - | - | `/expansion` (GeoJSON) | - |
| Centroid | - | - | `/centroid` | - |
| Trace attributes | - | - | `/trace_attributes` | - |

### Transport Modes

| Mode | Butterfly | OSRM | Valhalla | GraphHopper |
|------|:---------:|:----:|:--------:|:-----------:|
| Car | car | car | auto | car |
| Bicycle | bike | bicycle | bicycle | bike |
| Pedestrian | foot | foot | pedestrian | foot |
| Truck | - | - | truck | truck, small_truck |
| Bus | - | - | bus | bus |
| Motorcycle | - | - | motorcycle | motorcycle |
| Scooter | - | - | motor_scooter | scooter |
| E-cargo bike | - | - | - | ecargobike |
| Transit | - | - | multimodal | - |
| Custom profiles | - | Lua scripts | costing options | custom models (JSON) |

### Routing Features

| Feature | Butterfly | OSRM | Valhalla | GraphHopper |
|---------|:---------:|:----:|:--------:|:-----------:|
| Turn-by-turn steps | yes | yes | yes | yes |
| Road names in steps | yes | yes | yes | yes |
| Alternatives | yes (max 5) | yes | yes | yes |
| Turn restrictions | exact (edge-based) | approx (node-based) | yes | yes |
| Avoid areas (polygons) | `avoid_polygons=` | - | avoid_polygons | custom model |
| Avoid tolls/ferries/highways | `exclude=toll,ferry,motorway` | - | costing options | custom model |
| Time-dependent routing | - | - | date_time param | - |
| Bearing hints per waypoint | `bearings=angle,range` | yes (per waypoint) | - | heading/heading_penalty |
| Per-edge annotations | `annotations=speed,distance,...` | speed/weight/nodes | - | path_details |
| Elevation in route | - | - | - | elevation=true |
| Isodistance | `distance_m=` | - | - | distance_limit |
| Multiple isochrone contours | `contours=300,600,1200` | N/A | multiple contours | 1 per request |

### Matrix Features

| Feature | Butterfly | OSRM | Valhalla | GraphHopper |
|---------|:---------:|:----:|:--------:|:-----------:|
| Duration matrix | yes | yes | yes | yes |
| Distance matrix | yes | yes | - | yes |
| Arrow IPC streaming | yes (50k×50k) | - | - | - |
| Max scale | unlimited (streaming) | ~10k | ~5k | ~10k |
| Unreachable = null | yes | yes | yes | yes (fail_fast=false) |

### Output Formats

| Format | Butterfly | OSRM | Valhalla | GraphHopper |
|--------|:---------:|:----:|:--------:|:-----------:|
| JSON | yes | yes | yes | yes |
| Polyline6 | yes | yes | yes | - |
| GeoJSON | yes | yes | yes | yes |
| WKB | yes (isochrone) | - | - | - |
| Arrow IPC | yes (matrix) | - | - | - |
| FlatBuffers | - | yes | - | - |
| GPX | - | - | - | yes |

---

## Where Butterfly Already Wins

- **Streaming matrix** — only engine that does 50k×50k via Arrow IPC
- **Bulk isochrone** — parallel batch endpoint, nobody else has this
- **Exact turn restrictions** — edge-based CCH, OSRM approximates with node-based CH
- **Integrated observability** — Prometheus metrics + structured logging out of the box
- **OpenAPI/Swagger** — self-documenting API, others lack this
- **WKB isochrone** — compact binary format, unique to Butterfly
- **Avoid polygons** — per-request avoid areas via CCH recustomization with R-tree spatial index
- **Exclude toll/ferry/motorway** — runtime CCH recustomization, no profile rebuild needed
- **Isodistance** — distance-based reachability polygons (GraphHopper only other engine with this)
- **Multiple contours** — single PHAST run, multiple threshold polygons
- **Per-edge annotations** — speed/duration/distance/nodes per route segment
- **Bearing hints** — OSRM-compatible bearing-filtered snap

---

## P-Sprint: Feature Parity Implementation Plan

**Status: ALL implemented features COMPLETE (P1-P4, P6-P7). P5 deferred (requires pipeline re-run).**

### P1: Avoid tolls/ferries/highways ✅
**Files:** `api.rs`, `exclude.rs` (new), `state.rs`
**Implementation:** `exclude=toll,ferry,motorway` query param. At query time, recustomizes CCH weights with 100x penalty on flagged edges. Sparse triangle relaxation for efficient weight propagation. Works on `/route`, `/trip`, `/table`, `/isochrone`, `/match`, `/isochrone/bulk`, `/table/stream`.
**API:** `GET /route?...&exclude=toll,ferry`
**Performance:** ~16s recustomization for Belgium graph (sparse triangle relax optimization).

### P2: Multiple isochrone contours ✅
**Files:** `api.rs`, `range/contour.rs`
**Implementation:** `contours=300,600,1200` param. Single PHAST run, generates contour polygons at each threshold. Returns GeoJSON FeatureCollection with `contour_value` property per feature.
**API:** `GET /isochrone?lon=&lat=&contours=300,600,1200&mode=car`

### P3: Isodistance ✅
**Files:** `api.rs`, `range/phast.rs`
**Implementation:** `distance_m=` param as alternative to `time_s`. Uses distance CCH weights for PHAST. Same contour pipeline.
**API:** `GET /isochrone?lon=&lat=&distance_m=5000&mode=foot`

### P4: Per-edge annotations in route response ✅
**Files:** `api.rs`, `geometry.rs`
**Implementation:** `annotations=speed,duration,distance,nodes` param on `/route`. Returns per-edge metadata arrays matching geometry segments.
**API:** `GET /route?...&annotations=speed,distance`

### P5: Truck profile (Deferred)
**Reason:** Requires full pipeline re-run (steps 2-8) for truck mode. Low priority compared to other features.

### P6: Avoid polygon areas ✅
**Files:** `api.rs`, `avoid.rs` (new), `exclude.rs`
**Implementation:** `avoid_polygons=` query param accepting JSON polygon rings. R-tree spatial index finds edges inside polygons, then recustomizes CCH weights with AVOID_BIT penalty. Works on all routing endpoints.
**API:** `GET /route?...&avoid_polygons=[[lon,lat],...]` or multiple polygons `[[[lon,lat],...],[[lon,lat],...]]`
**Performance:** ~16s P2P (time-only recustomization), ~28s PHAST endpoints (full recustomization). Sparse triangle relaxation converges in ~30 passes.
**Optimization:** Time-only variant for P2P routes skips distance weights + flat adjacencies.

### P7: Bearing hints for waypoint snapping ✅
**Files:** `api.rs`, `spatial.rs`
**Implementation:** `bearings=angle,range` pairs per waypoint. Filters snap candidates by bearing compatibility. OSRM-compatible format.
**API:** `GET /route?...&bearings=0,90;180,45`

---

## Out of Scope (Document Only)

### Full VRP Optimization
Multi-vehicle routing with time windows, capacity constraints, pickup/delivery pairs. GraphHopper uses [jsprit](https://github.com/graphhopper/jsprit). Recommendation: users should use OR-Tools or jsprit externally, feeding our `/table` matrix.

### Geocoding
Address-to-coordinate and reverse. Use [Nominatim](https://nominatim.org/) or [Pelias](https://github.com/pelias/pelias) externally.

### Transit / Multimodal
Requires GTFS data, entirely different graph model. Valhalla is the only OSS engine that does this well.

### Time-dependent Routing
Requires time-varying speed profiles (traffic data). Large scope, different weight model.

### Custom Profiles (Lua/JSON DSL)
OSRM uses Lua scripts, GraphHopper uses JSON custom models. Our Rust profiles are compiled in. Lower priority — truck/bus cover main use cases.

---

## Invariants (Must Hold)

### Global input invariants
- Coordinates must be finite WGS84: lon `[-180,180]`, lat `[-90,90]`.
- Mode: `car|bike|foot|truck` (case-insensitive, no whitespace trimming).
- Endpoint limits enforced without panic:
  - `nearest.number in [1,100]`
  - `isochrone.time_s in [1,7200]`, `isochrone.distance_m in [1,100000]`
  - `match.coordinates <= 500`
  - `trip.coordinates in [2,100]`
  - `isochrone.bulk.origins <= 10000`
  - `height.coordinates <= 10000`
  - `table` cells `<= 10,000,000`
  - `exclude` tokens must be valid: `toll,ferry,motorway`
  - `annotations` tokens must be valid: `duration,distance,speed,nodes`

### Routing correctness
- `distance >= 0`, `duration >= 0`, never NaN/Inf.
- `route(A,A)` zero or near-zero.
- Approximate symmetry on two-way roads: `dist(A,B)` within 20% of `dist(B,A)`.
- Triangle inequality approximately holds.
- Steps sum ≈ route total.
- Alternatives geometrically distinct.
- `trip` with 2 points and `round_trip=false` matches `/route`.
- `match` on clean traces snaps to known corridor.

### Matrix invariants
- Unreachable: `null` in JSON, `u32::MAX` in Arrow.
- `table[i][j]` ≈ `/route(src_i, dst_j)`.
- Stream and non-stream consistent after unit conversion.

### Isochrone invariants
- Polygon contains snapped origin.
- Monotonicity: larger threshold → larger area.
- Multiple contours: area(t1) ≤ area(t2) when t1 < t2.
- Bulk matches single for same origin/time.

### Geometry/format
- GeoJSON: `[lon,lat]` always.
- Polyline6/GeoJSON/points coordinate-equivalent.
- Polygon rings closed, outer CCW, holes CW.
- No NaN/Infinity in JSON numbers.

### Operational
- `/health` always 200.
- Concurrency limits + timeouts protect stability.
- Streaming cancellation stops wasted compute.
