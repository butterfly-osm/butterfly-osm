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
| Truck | truck | - | truck | truck, small_truck |
| Bus | (add model JSON) | - | bus | bus |
| Motorcycle | (add model JSON) | - | motorcycle | motorcycle |
| Scooter | (add model JSON) | - | motor_scooter | scooter |
| E-cargo bike | (add model JSON) | - | - | ecargobike |
| Transit | - | - | multimodal | - |
| Custom profiles | JSON models (declarative) | Lua scripts | costing options | custom models (JSON) |

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

**Status: ALL features COMPLETE (P1-P7). P5 (truck) implemented via Q-Sprint declarative model system.**

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

### P5: Truck profile ✅ (via Q-Sprint)
**Implementation:** `truck.model.json` with HGV speeds, `hgv=no/private` denial, `restriction:hgv` support. Pipeline re-run with `--way-attrs truck=...` builds truck data. Zero Rust code changes needed.

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

### Custom Profiles (Lua/JSON DSL) — DONE (Q-Sprint)
OSRM uses Lua scripts, GraphHopper uses JSON custom models. **Butterfly now uses declarative JSON model files** (`*.model.json`). Adding a new mode = drop a JSON file, re-run pipeline. Zero Rust code changes. Car, bike, foot, truck models included. Bus/motorcycle/scooter can be added by creating model files.

---

## Invariants (Must Hold)

### Global input invariants
- Coordinates must be finite WGS84: lon `[-180,180]`, lat `[-90,90]`.
- Mode: any discovered mode name (case-insensitive, no whitespace trimming). Default models: car, bike, foot, truck.
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

---

## Q-Sprint: Declarative Custom Model System (2026-02-08) — COMPLETE

Replace all hardcoded Rust profiles with JSON model files. Adding new transport modes requires zero Rust code changes.

### Architecture
- `models/*.model.json` — declarative model definitions (car, bike, foot, truck)
- `src/model/` — schema.rs, compile.rs, evaluate.rs, mod.rs
- `Mode(pub u8)` — pure index wrapper, no hardcoded constants
- Deterministic mode indexing: sorted alphabetically by name (bike=0, car=1, foot=2, truck=3)
- `TurnEntry.penalty_ds: [u32; MAX_MODES=8]` — dynamic per-mode penalties
- CLI Steps 3/4/5 use `--way-attrs MODE=PATH` repeatable args
- Server auto-discovers modes from `way_attrs.*.bin` files in data directory

### What was deleted
- `src/profiles/` directory (car.rs, bike.rs, foot.rs, mod.rs, tag_lookup.rs)
- `Profile` trait, all `Mode::Car/Bike/Foot` constants
- All `Mode::all()`, `Mode::name()`, `Mode::from_name()` methods

### Adding a new mode (e.g. bus)
1. Create `models/bus.model.json` (speeds, access, turn penalties, restrictions)
2. Re-run pipeline: `step2-profile` discovers new model, `step4-ebg --way-attrs bus=... --turn-rules bus=...`
3. Server auto-discovers bus mode on startup — all endpoints serve bus routes

### Status
- 52 files changed, 280 Mode enum usages replaced
- 234 tests pass, zero clippy warnings, clean release build

---

## R-Sprint: Architecture Cleanup (2026-03-20) — COMPLETE

Purely mechanical restructuring — zero behavioral changes. Fixes opaque `stepN` module names, 5,911-line `api.rs` monolith, and dead experimental code.

### Phase 1: Delete Dead Code (~1,930 LOC) ✅
- Deleted `hybrid/` module (1,505 LOC) — 5 files, 5 CLI commands removed
- Deleted `analysis/` module (428 LOC) — 2 files, 1 CLI command removed
- Kept `formats/hybrid_state.rs` (used by Step6/7/8 hybrid pipeline variant)
- Kept Step6Hybrid/Step7Hybrid/Step8Hybrid CLI commands (use `formats::hybrid_state`, not `hybrid::`)

### Phase 2: Rename Step Modules ✅
| Old | New | LOC |
|-----|-----|-----|
| `step5.rs` | `weights.rs` | 383 |
| `step6.rs` | `ordering.rs` | 1,939 |
| `step6_lifted.rs` | `ordering_lifted.rs` | 162 |
| `step7.rs` | `contraction.rs` | 1,593 |
| `step8.rs` | `customization.rs` | 997 |

Also renamed `validate/step5.rs` → `validate/weights.rs`, `validate/step6.rs` → `validate/ordering.rs`, `validate/step7.rs` → `validate/contraction.rs`.

### Phase 3: Merge `profile_abi.rs` into `model/types.rs` ✅
- Content moved to `model/types.rs`
- `profile_abi.rs` is now a thin re-export shim (`pub use crate::model::types::*`)
- All 33 import sites continue to work unchanged
- Unified duplicate `MAX_MODES` constant

### Phase 4: Rename `step9/` → `server/` + Split `api.rs` ✅
- Directory renamed from `step9/` to `server/`
- All external references updated (cli.rs, bench/main.rs, matrix/bucket_ch.rs)
- `api.rs` split from 5,911 lines into 11 focused modules (largest: 1,221 lines)
- New modules: `types.rs`, `route.rs`, `nearest.rs`, `table.rs`, `isochrone_handler.rs`, `matching.rs`, `health_handler.rs`, `height_handler.rs`, `debug.rs`, `api_tests.rs`

### Phase 5: Merge `profile/` into `model/profiling.rs` ✅
- Content moved to `model/profiling.rs`
- `profile/mod.rs` is now a thin re-export shim (`pub use crate::model::profiling::*`)
- All 4 import sites continue to work unchanged

### Final Module Layout
```
lib.rs: cli, ebg, formats, ingest, matrix, model, nbg, nbg_ch, range,
        weights, ordering, ordering_lifted, contraction, customization,
        server, profile (shim), profile_abi (shim), validate

model/: compile, evaluate, profiling, schema, types
server/: api (router+OpenApi, 153 LOC), types, route, nearest, table,
         isochrone_handler, matching, health_handler, height_handler,
         debug, api_tests, avoid, elevation, exclude, geometry,
         map_match, query, spatial, state, trip, unpack
```

---

## Remaining Work

### Pipeline Status (2026-02-08)
- Steps 2-8: DONE for all 4 modes (bike, car, foot, truck)
- Server verification with truck: PENDING (data exists, needs live test)

### Deferred
- Two-resolution isochrone mask (D8) — WONTFIX
- Hybrid pipeline removal (~1,100 LOC in ordering/contraction/customization + 3 CLI commands) — separate decision
- Remove re-export shims (`profile_abi.rs`, `profile/mod.rs`) once all imports migrated
