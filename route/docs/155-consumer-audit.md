# Issue #155 — `nbg.geo` polyline consumer audit

**Branch base:** `work-154` (commit `6685125`).
**Goal:** enumerate every read site for per-edge polylines, classify hot/cold, identify the response fields each one feeds. This drives the migration plan in #155.

`nbg.geo` carries two payloads:

1. `edges: Vec<NbgEdge>` — fixed-size 36-byte records (`u_node`, `v_node`, `length_mm`,
   `bearing_deci_deg`, `n_poly_pts`, `poly_off`, `first_osm_way_id`, `flags`). Already
   flat. ~4 M × 36 B ≈ 140 MB on Belgium. Hot serve consumer is just
   `first_osm_way_id` → road-name lookup.
2. `polylines: Vec<PolyLine>` — **nested** heap allocation. Each `PolyLine` owns two
   `Vec<i32>` (lat/lon i32-e7 arrays). ~4 M outer Vecs (24 B each = 96 MB just for
   headers) + ~30 M points × 8 B = ~240 MB body. Cache-hostile, not zero-copy.

This ticket flattens (2) only — the `edges` vector stays put for now (see "out of
scope" below).

## Methodology

```
grep -rn "polylines\[" route/src --include='*.rs'
grep -rn "PolyLine\|polylines" route/src --include='*.rs'
grep -rn "nbg_geo\." route/src --include='*.rs'
```

## Polyline consumers (`nbg_geo.polylines[geom_idx]`)

### Serve path (HOT — must migrate)

Every site below is reached from a request handler. Migrate to the new
`EdgeGeometry::polyline(geom_idx)` accessor. Numbers cite `route/src/...` paths
relative to repo root.

| Path | Purpose | Endpoint feeding | Hot/cold | Notes |
|---|---|---|---|---|
| `server/geometry.rs:140-150` (`build_raw_points`) | Concat polyline points → `Vec<Point>` for response | `GET /route` (geometry), GPX export | **HOT** | One per requested route. Per-call vec growth bounded by route length. |
| `server/geometry.rs:262-265, 272-277, 305-340` (`build_isochrone_geometry_sparse`, `extract_partial_polyline`) | Stamp reachable edges into raster grid; cut frontier edges at fraction | `GET /isochrone` (default polygon), `POST /isochrone/bulk` | **HOT** | Iterates ALL settled nodes (~hundreds of thousands at 30-min car); reads the polyline for each. Largest per-call polyline-read fan-out in the codebase. |
| `server/route.rs:1018-1027` (`get_edge_start_location`) | First lon/lat of edge polyline | `/route` turn-by-turn `location` field | HOT | One per turn. |
| `server/route.rs:1031-1047` (`get_edge_end_location`) | Last lon/lat of edge polyline | `/route` turn-by-turn `location` for arrive | HOT | Once per route. |
| `server/route.rs:1050-1072` (`get_edge_bearing`) | First or last segment bearing | `/route` turn-by-turn `bearing_before` / `bearing_after` | HOT | Two reads per turn (start + end bearings of adjacent segments). |
| `server/route.rs:1108-1129` (`build_edge_geometry`) | Single-edge `RouteGeometry` for a turn step | `/route` per-step `geometry` field | HOT | One per turn step. |
| `server/route.rs:1132-1157` (`build_multi_edge_geometry`) | Multi-edge concatenated geometry for a turn step | `/route` per-step `geometry` field | HOT | One per turn step covering N edges; same pattern as `build_raw_points` for a slice. |
| `server/types.rs:78-87` (`get_node_location`) | First lon/lat of edge polyline | Various — `/table`, `/trip`, transit | HOT | Equivalent of `get_edge_start_location` but for a generic node. |
| `server/trip.rs:807-820` (`get_node_location`) | First lon/lat of edge polyline | `POST /trip` waypoint locations | HOT | Per waypoint. |
| `server/isochrone_handler.rs:949-1015` (`build_network_geometry`) | Per-edge polyline → coordinate array (`include=network`) | `GET /isochrone?include=network` | HOT | Iterates ALL settled nodes. Used when caller asks for the full primal network as polylines. |
| `server/transit_handler.rs:1208-1212` (calls `build_raw_points`) | Per-leg geometry for transit routing | `GET /transit`, `POST /transit/bulk` | HOT | Reused via `geometry::build_raw_points`. |
| `server/catchment.rs:323` (calls `build_raw_points`) | Catchment polygon geometry | Flight `catchment` | HOT | Reused via `geometry::build_raw_points`. |
| `server/flight.rs:630, 777` (calls `build_raw_points`, also reads polyline directly) | WKB polyline for `route_batch`, `edges_batch` Flight actions | gRPC Flight | HOT | Reused via `geometry::build_raw_points`. |
| `server/map_match.rs:235-279` (`project_onto_edge`) | Per-edge segment projection for HMM emission | `POST /match` | HOT | Iterates polyline vertices for closest-point projection. |
| `server/snap_index.rs:101-157` (`build_snap_index`) | Boot-time snap-point derivation (#154) | server boot | **COLD-BOOT** | Walks all polylines once at startup. Could keep using owning `NbgGeo` if fallback path retained — easier to migrate too. |
| `server/spatial.rs:106-110, 360-369` (`SpatialIndex::build_inner`, `get_coords`) | Boot-time legacy rstar build (back-compat fallback) | server boot only when container pre-dates #154 | **COLD-BOOT** | Only fired on old containers. Can keep reading `NbgGeo.polylines` directly via the legacy fallback. |
| `server/isochrone_test.rs:291` | Internal isochrone consistency test | dev-only | cold | Test harness. |
| `server/consistency_test.rs:492, 844, 869, 901, 1107, 1224, 1237, 1293` | Internal consistency / regression tests | dev-only | cold | Test harness. |
| `server/matching.rs:288, 297, 324` | Map-match callers (delegating to `project_onto_edge`) | `POST /match` | HOT | Same fan-out as `map_match.rs`. |
| `server/state.rs:198, 279, 453, 543` | Boot — load `NbgGeo`, then build packed snap index | server boot | **COLD-BOOT** | Boot path; we'll dispatch on whether the new sections exist. |
| `range/frontier.rs:306-415` (`FrontierExtractor::*`) | CLI-only frontier extraction tool | CLI tool | cold | Loads NbgGeo from a file path — outside the serve path. |

### Build path (do NOT migrate this ticket)

These run on the build pipeline (steps 3/4/5/6/7/8 + validate). They consume the
full `NbgGeo` from disk. Per the spec, this ticket is serve-side only.

| Path | Purpose |
|---|---|
| `nbg_ch/ordering.rs`, `nbg_ch/contraction.rs`, `nbg_ch/validate.rs` | Build-time NBG-CH ordering/contraction/validation. |
| `ordering.rs`, `ordering_lifted.rs` | EBG ordering build steps. |
| `contraction.rs` | CCH contraction build step. |
| `weights.rs`, `validate/weights.rs` | Build-time weight derivation / validation. |
| `validate/step4.rs`, `validate/step3.rs` | Build-time lock-condition checks. |
| `ebg/mod.rs`, `ebg/turn_processor.rs` | EBG construction. |
| `range/batched_isochrone.rs` (path-loading constructors) | Bench harness path-load constructors. |
| `pack.rs` | Pack-time aggregation — reads NbgGeo to derive sections (here we'll **add** edge-geometry derivation). |

These keep loading the legacy file format from `step3/nbg.geo`. No migration
needed; the serve path is where #155 saves RSS.

## Edge metadata consumers (`nbg_geo.edges[geom_idx]`)

The **edges** array itself is flat `Vec<NbgEdge>` (36-byte records, contiguous).
Already cache-friendly. The only serve-side read is:

| Path | Field | Endpoint |
|---|---|---|
| `server/route.rs:861-862` (`lookup_road_name`) | `first_osm_way_id` | `/route` turn-by-turn `name` field |

This is **out of scope** for #155 (the issue focuses on the nested polyline
heap). Keeping `NbgGeo.edges` in its current shape costs ~140 MB on Belgium —
material, but disjoint from the polyline win, and it'd require its own design
work (e.g. flat `[NbgEdgeRecord]` mmap-backed with `bytemuck::Pod`). That can
land as a follow-up; for #155 we keep `NbgGeo.edges` heap-loaded but drop the
polyline `Vec<Vec<_>>`.

## Surface area summary

- **15 distinct serve hot-path call sites** read polyline vertices.
- **3 cold-boot serve sites** (snap-index build, rstar fallback build, boot
  load).
- All hot-path sites can migrate to a single `EdgeGeometry::polyline(edge_id)`
  accessor that returns either an iterator-yielding (`f64, f64)` or a
  borrowed `&[i32]` interleaved view — choice driven by what each consumer
  needs.

## Migration shape (preview — full design in `155-design.md`)

```rust
// state.rs
pub struct ServerState {
    // ... existing fields ...
    pub edge_geom: EdgeGeometry, // NEW — replaces nbg_geo.polylines on the serve path
    pub nbg_geo: NbgGeo,         // kept for `edges` (way_id, length_mm, ...);
                                  // .polylines is empty on the new boot path
                                  // and back-filled only on legacy fallback
}

pub struct EdgeGeometry {
    offsets: Cow<'static, [u32]>,    // length n_edges + 1 (cumulative POINT counts)
    points:  Cow<'static, [i32]>,    // interleaved (lon_e7, lat_e7) pairs
}

impl EdgeGeometry {
    pub fn polyline(&self, edge_id: u32) -> EdgePolyline<'_> { ... }
    pub fn n_edges(&self) -> u32 { ... }
    pub fn is_empty(&self) -> bool { ... }
}

pub struct EdgePolyline<'a> {
    points_lon_lat_e7: &'a [i32], // 2N entries: [lon0, lat0, lon1, lat1, ...]
}

impl<'a> EdgePolyline<'a> {
    pub fn len(&self) -> usize { self.points_lon_lat_e7.len() / 2 }
    pub fn is_empty(&self) -> bool { self.points_lon_lat_e7.is_empty() }

    /// (lon_e7, lat_e7) at vertex `i`. Cheap.
    pub fn at_e7(&self, i: usize) -> (i32, i32) { ... }
    /// (lon, lat) at vertex `i`. Converts on the fly.
    pub fn at(&self, i: usize) -> (f64, f64) { ... }

    /// Lazy iterator over `(lon, lat)` pairs in f64 degrees.
    pub fn iter(&self) -> impl Iterator<Item = (f64, f64)> + '_ { ... }
    /// Lazy iterator over `(lon_e7, lat_e7)` pairs in i32-e7.
    pub fn iter_e7(&self) -> impl Iterator<Item = (i32, i32)> + '_ { ... }

    /// One-shot owned vec for callers that need it (fallback only).
    pub fn into_vec_f64(self) -> Vec<(f64, f64)> { ... }
}
```

The migration replaces every `let poly = &nbg_geo.polylines[geom_idx]; poly.lat_fxp[i]; poly.lon_fxp[i];` access with the equivalent `EdgeGeometry::polyline(geom_idx)` view.

## Ordering of migration commits (preview)

1. Format file + tests — synthetic data only.
2. Pack-side derivation — reads `NbgGeo` once, emits sections.
3. Access type (`server/edge_geom.rs`) — the `EdgeGeometry` wrapper.
4. Migrate hot-path consumers — `geometry.rs`, `route.rs`, `types.rs`,
   `trip.rs`, `isochrone_handler.rs`, `map_match.rs`, `transit_handler.rs`,
   `catchment.rs`, `flight.rs`.
5. Migrate cold-boot consumers — `state.rs`, fallback `snap_index.rs` builder
   stays as-is (still consumes full `NbgGeo`).
6. Server load path — wire `try_load_edge_geom` + back-compat fallback.
7. Measurement.
