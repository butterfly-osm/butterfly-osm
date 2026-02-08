# Butterfly-route QA Adversarial Plan (Endpoint x Mode)

## Scope and code evidence reviewed
- `tools/butterfly-route/src/step9/api.rs`
- `tools/butterfly-route/src/step9/geometry.rs`
- `tools/butterfly-route/src/step9/trip.rs`
- `tools/butterfly-route/src/step9/elevation.rs`
- `tools/butterfly-route/src/step9/map_match.rs`
- `tools/butterfly-route/src/step9/query.rs`
- `tools/butterfly-route/src/step9/state.rs`
- `tools/butterfly-route/src/range/contour.rs`
- `tools/butterfly-route/src/matrix/bucket_ch.rs`

Additional behavior-critical reads:
- `tools/butterfly-route/src/step9/spatial.rs` (snap distance/radius behavior)
- `tools/butterfly-route/src/range/wkb_stream.rs` (ring orientation/closure for WKB)

## Mode coverage policy
For every mode-dependent endpoint, run each test with mode in `{car, bike, foot}` unless the test explicitly targets invalid mode handling.

Naming convention in this document:
- `..._{mode}` means generate 3 tests: `_car`, `_bike`, `_foot`.

## Cross-cutting implementation facts (must inform expected results)
- Coordinate validation allows only lon `[-180,180]`, lat `[-90,90]`, rejects NaN (`api.rs:2916`).
- Mode parsing is case-insensitive (`to_lowercase`) but does not trim whitespace (`api.rs:2936`, `trip.rs:705`).
- Snap cutoff is hard-limited to 5000m in spatial index (`spatial.rs:7-10`, `spatial.rs:82-101`).
- `u32::MAX` is unreachable sentinel in routing/matrix internals (`query.rs`, `bucket_ch.rs`).
- Regular routes are limited to 32 concurrent + 120s timeout (`api.rs:121-125`), stream endpoints to 4 concurrent + 600s (`api.rs:133-137`).
- Stream endpoints accept up to 256MB request body (`api.rs:132`).
- `CatchPanicLayer` converts panics to HTTP 500 instead of crashing (`api.rs:144`).

---

## 1) GET `/route`

### High-risk findings
- `src==dst` only returns zero route when both snap to same rank, geometry is a single snapped source point (`api.rs:412-435`).
- `alternatives` silently clamped to max 5 (`api.rs:341`).
- Alternative generation clones full CCH weights per request (comment says ~200MB) (`api.rs:497-501`) -> memory DoS vector.
- No-snap is 400, no-path is 404 (`api.rs:359-366`, `api.rs:481-488`) unlike OSRM-style 400 error codes.

### Tests
- `test_route_coord_exact_bounds_{mode}`
  Input: `src=(-180,-90) dst=(180,90)`.
  OSRM/Valhalla: valid coordinates accepted syntactically; likely no segment/no data error.
  SHOULD: 400 with explicit no-snap/no-data code, never panic.
  Type: validation.

- `test_route_coord_just_outside_bounds_{mode}`
  Input: `src_lon=-180.001`, `dst_lat=90.001`.
  OSRM/Valhalla: 400 invalid value/location.
  SHOULD: 400 with coordinate-range error.
  Type: validation.

- `test_route_poles_antimeridian_{mode}`
  Input: `(180,0)->(-180,0)`, `(0,90)->(0,89.999)`.
  OSRM/Valhalla: usually 400 no segment/no data.
  SHOULD: deterministic 400, no overflow/NaN.
  Type: validation/regression.

- `test_route_nan_inf_negzero_{mode}`
  Input: query `src_lon=NaN`, `src_lon=inf`, `src_lon=-0.0`.
  OSRM: invalid value (400). Valhalla: failed parse location (400).
  SHOULD: NaN/Inf -> 400; `-0.0` should behave same as `0.0`.
  Type: validation.

- `test_route_ocean_no_roads_{mode}`
  Input: `(0,0)->(0.1,0.1)`.
  OSRM: `NoSegment` 400.
  SHOULD: 400 "could not snap" without internal error leakage.
  Type: validation.

- `test_route_src_equals_dst_same_snap_{mode}`
  Input: identical coordinate on dense urban road.
  OSRM/Valhalla: typically 200 with zero/near-zero route.
  SHOULD: 200, `duration_s=0`, `distance_m=0`, no alternatives, steps empty if requested.
  Type: invariant/regression.

- `test_route_src_equals_dst_different_snap_{mode}`
  Input: same raw coordinate near parallel carriageways to force different snaps.
  OSRM/Valhalla: may produce tiny non-zero route.
  SHOULD: stable deterministic behavior, never negative metrics.
  Type: regression.

- `test_route_very_close_points_{mode}`
  Input: delta ~1e-8 degrees.
  OSRM/Valhalla: valid 200 or no-segment if snap ambiguity.
  SHOULD: no precision crash, non-negative duration/distance.
  Type: invariant.

- `test_route_far_across_belgium_{mode}`
  Input: west-east Belgium pair.
  OSRM/Valhalla: 200 if connected.
  SHOULD: 200 within timeout, no overflow, realistic distance.
  Type: invariant/perf.

- `test_route_missing_required_params_{mode}`
  Input: omit `src_lon` or `mode`.
  OSRM: 400 invalid query.
  SHOULD: 400 extractor error body, not 500.
  Type: validation.

- `test_route_mode_empty_case_whitespace`
  Input: `mode=""`, `mode="Car"`, `mode=" car "`.
  OSRM profile is path segment (strict). Valhalla costing strict strings.
  SHOULD: `Car` accepted, empty/whitespace rejected 400.
  Type: validation/regression.

- `test_route_unknown_query_params_ignored_{mode}`
  Input: add `foo=bar&debug2=true`.
  OSRM usually rejects unknown options.
  SHOULD: decide contract; current behavior ignores extras. Lock expected behavior explicitly.
  Type: regression.

- `test_route_geometry_format_variants_{mode}`
  Input: `geometries=polyline6|geojson|points|INVALID`.
  OSRM supports `polyline|polyline6|geojson`.
  SHOULD: 3 known formats succeed; invalid returns 400.
  Type: validation.

- `test_route_alternatives_bounds_{mode}`
  Input: `alternatives=0,1,5,6,4294967295`.
  OSRM supports bool/number with service limits.
  SHOULD: clamp at 5, no OOM, no panic.
  Type: regression/resource.

- `test_route_steps_sum_consistency_{mode}`
  Input: `steps=true` on multi-turn route.
  OSRM/Valhalla: leg/step sums approx total.
  SHOULD: `sum(step.distance_m)` and `sum(step.duration_s)` approximately total route values.
  Type: invariant.

- `test_route_alternative_distinctness_{mode}`
  Input: `alternatives=3` where alternatives exist.
  OSRM: alternatives differ in geometry; primary best.
  SHOULD: each alternative geometry differs from primary; alt duration >= primary (or document if not guaranteed).
  Type: invariant/regression.

- `test_route_symmetry_on_symmetric_corridor_{mode}`
  Input: A<->B on likely two-way arterial.
  OSRM/Valhalla: distances close, durations may differ slightly.
  SHOULD: `distance(A,B)` within 20% of `distance(B,A)` unless one-way constraints dominate.
  Type: invariant.

- `test_route_triangle_inequality_local_triplet_{mode}`
  Input: A,B,C in same city.
  OSRM/Valhalla: should hold approximately.
  SHOULD: `dur(A,C) <= dur(A,B)+dur(B,C)+epsilon`.
  Type: invariant.

---

## 2) GET `/nearest`

### High-risk findings
- `number=0` is silently clamped to 1 (`api.rs:983`), not rejected.
- Max enforced is 100, but struct comment says "max 10" (`api.rs:911` vs `api.rs:965`).

### Tests
- `test_nearest_coord_bounds_{mode}`
  Input: exact/min/max bounds and out-of-range.
  OSRM nearest: invalid -> 400.
  SHOULD: strict 400 for out-of-range.
  Type: validation.

- `test_nearest_nan_inf_negzero_{mode}`
  Input: `lon=NaN|inf|-inf|-0.0`.
  OSRM: invalid value 400.
  SHOULD: NaN/Inf rejected, `-0.0` equivalent to `0.0`.
  Type: validation.

- `test_nearest_number_limits_{mode}`
  Input: `number=0,1,100,101,4294967295`.
  OSRM expects integer >=1.
  SHOULD: either reject 0 or keep documented clamp; reject >100.
  Type: validation/regression.

- `test_nearest_outside_map_{mode}`
  Input: ocean point.
  OSRM: `NoSegment` 400.
  SHOULD: 400 with clear no-road-within-snap message.
  Type: validation.

- `test_nearest_result_sorting_{mode}`
  Input: urban point with `number=10`.
  OSRM: sorted by distance.
  SHOULD: non-decreasing distances.
  Type: invariant.

- `test_nearest_snap_distance_cap_{mode}`
  Input: point >5km from any road.
  OSRM: no segment.
  SHOULD: fail; never return waypoint with distance >5000m.
  Type: invariant/regression.

- `test_nearest_mode_case_and_whitespace`
  Input: `mode=Car`, `mode= car`.
  SHOULD: case-insensitive yes, whitespace no.
  Type: validation.

- `test_nearest_unknown_query_ignored_{mode}`
  Input: `&foo=x`.
  SHOULD: lock behavior (ignore vs reject).
  Type: regression.

---

## 3) POST `/table`

### High-risk findings
- Off-network points become `null` cells with HTTP 200, not hard error (`api.rs:1195-1258`, `api.rs:1351-1358`).
- `annotations` unknown values silently default to duration (`api.rs:1159-1164`).
- Hard cap: 10,000,000 cells (`api.rs:1129`).

### Tests
- `test_table_empty_sources_{mode}`
  Input: `sources=[]`, valid destinations.
  OSRM table: 400 invalid value.
  SHOULD: 400.
  Type: validation.

- `test_table_empty_destinations_{mode}`
  Input: valid sources, `destinations=[]`.
  SHOULD: 400.
  Type: validation.

- `test_table_matrix_too_large_{mode}`
  Input: dimensions just above 10,000,000 cells.
  OSRM: `TooBig` 400.
  SHOULD: 400 with actionable hint to `/table/stream`.
  Type: validation/resource.

- `test_table_coord_bounds_and_nan_{mode}`
  Input: one source with out-of-range/NaN.
  SHOULD: 400 with offending index in message.
  Type: validation.

- `test_table_off_network_null_cells_{mode}`
  Input: valid WGS84 but outside Belgium.
  OSRM: table supports `null` per-cell.
  SHOULD: 200 with `null` cells for invalid snaps/unreachable.
  Type: invariant.

- `test_table_unknown_annotations_fallback_{mode}`
  Input: `annotations="foo"`.
  OSRM would reject invalid options.
  SHOULD: pick one policy and enforce consistently. Current behavior silently returns durations.
  Type: regression.

- `test_table_annotations_duration_distance_{mode}`
  Input: duration-only, distance-only, both.
  SHOULD: return only requested matrices.
  Type: invariant.

- `test_table_missing_mode_or_fields`
  Input: malformed/missing JSON keys.
  SHOULD: 400 extractor error.
  Type: validation.

- `test_table_malformed_json`
  Input: truncated JSON/body as text.
  OSRM/Valhalla: 400 parse error.
  SHOULD: 400, not 500.
  Type: validation.

- `test_table_route_consistency_{mode}`
  Input: small matrix 3x3; compare each cell against `/route`.
  SHOULD: durations/distances close to `/route` outputs after unit conversion.
  Type: invariant.

- `test_table_diagonal_zeroish_{mode}`
  Input: same points in sources/destinations.
  OSRM: diagonal generally 0 when same snapped location.
  SHOULD: diagonal values zero or tiny snap-induced values; never negative/non-finite.
  Type: invariant.

---

## 4) POST `/table/stream` (Arrow IPC)

### High-risk findings
- No hard point-count limit by design; only body size and tile processing constraints (`api.rs:1456-1459`).
- Returns only durations in milliseconds with `u32::MAX` sentinel for unreachable (`api.rs:1665-1669`).
- Errors are JSON before stream starts; mid-stream failures surface as stream errors.

### Tests
- `test_table_stream_empty_arrays_{mode}`
  Input: empty `sources` or `destinations`.
  SHOULD: 400.
  Type: validation.

- `test_table_stream_tile_size_clamping_{mode}`
  Input: `src_tile_size=0`, `dst_tile_size=0`, and huge values.
  SHOULD: clamps to `[1, min(n,65535)]` without crash.
  Type: regression.

- `test_table_stream_overflow_matrix_dims_{mode}`
  Input: artificial huge lengths (fuzzed JSON) to trigger checked_mul overflow path.
  SHOULD: 400 "matrix dimensions overflow".
  Type: validation/regression.

- `test_table_stream_off_network_all_unreachable_{mode}`
  Input: all coordinates valid but outside data.
  SHOULD: 200 stream; all payload durations are `u32::MAX`.
  Type: invariant.

- `test_table_stream_headers_present_{mode}`
  Input: valid small request.
  SHOULD: `X-Total-*` headers coherent with payload.
  Type: invariant.

- `test_table_stream_matches_table_{mode}`
  Input: same data as `/table`.
  OSRM/Valhalla analog: matrix should be deterministic.
  SHOULD: `table_stream_ms / 1000 == table_duration_s` (within rounding), sentinel/null mapping consistent.
  Type: invariant.

- `test_table_stream_malformed_json`
  Input: bad JSON.
  SHOULD: 400.
  Type: validation.

- `test_table_stream_body_too_large`
  Input: payload >256MB.
  SHOULD: 413 payload too large.
  Type: validation/resource.

- `test_table_stream_unknown_fields_ignored_{mode}`
  Input: extra JSON keys.
  SHOULD: lock contract (currently ignored).
  Type: regression.

- `test_table_stream_tile_order_non_deterministic`
  Input: moderate matrix with multiple blocks.
  SHOULD: client must reconstruct by `(src_block_start,dst_block_start)`, not order.
  Type: regression.

---

## 5) GET `/isochrone`

### High-risk findings
- `time_s` hard-limited to 1..7200 (`api.rs:1814`).
- `direction` is case-sensitive strict (`api.rs:1838-1849`).
- GeoJSON output ring is explicitly CCW and closed (`api.rs:1973-1988`), but points/polyline output do not enforce closure/orientation.
- Accept header `application/octet-stream` returns WKB; empty polygon returns 204 (`api.rs:1945-1954`).

### Tests
- `test_isochrone_coord_bounds_{mode}`
  Input: exact and out-of-range bounds.
  SHOULD: valid WGS84 accepted, invalid rejected 400.
  Type: validation.

- `test_isochrone_time_bounds_{mode}`
  Input: `time_s=0,1,7200,7201,4294967295`.
  SHOULD: only 1..7200 accepted.
  Type: validation.

- `test_isochrone_direction_case_{mode}`
  Input: `depart`, `arrive`, `Depart`.
  Valhalla uses explicit enums.
  SHOULD: either case-normalize or keep strict and document.
  Type: validation/regression.

- `test_isochrone_invalid_geometry_format_{mode}`
  Input: `geometries=INVALID`.
  SHOULD: 400.
  Type: validation.

- `test_isochrone_center_off_network_{mode}`
  Input: ocean point.
  SHOULD: 400 no-snap.
  Type: validation.

- `test_isochrone_origin_containment_{mode}`
  Input: city center, 600s.
  SHOULD: polygon contains snapped origin (point-in-polygon).
  Type: invariant.

- `test_isochrone_monotonicity_{mode}`
  Input: same origin, 600s vs 1800s.
  SHOULD: area(600) subset/<= area(1800) up to raster tolerance.
  Type: invariant.

- `test_isochrone_geojson_ring_properties_{mode}`
  Input: `geometries=geojson`.
  SHOULD: first==last, ring CCW.
  Type: response-format invariant.

- `test_isochrone_points_polyline_roundtrip_{mode}`
  Input: compare `polyline6`, `points`, `geojson`.
  SHOULD: equivalent coordinates up to precision.
  Type: response-format invariant.

- `test_isochrone_include_network_{mode}`
  Input: `include=network`.
  SHOULD: network segments returned with valid lon/lat ordering and partial clipping sane.
  Type: invariant/regression.

- `test_isochrone_accept_wkb_{mode}`
  Input: `Accept: application/octet-stream`.
  SHOULD: binary WKB or 204 if empty; never JSON when binary requested.
  Type: response-format.

---

## 6) POST `/isochrone/bulk`

### High-risk findings
- Max origins 10,000 (`api.rs:2118`).
- Unsnappable/unreachable origins are silently dropped from results and only reflected in headers (`api.rs:2157-2163`, `api.rs:2218-2223`).
- Validation errors return raw bytes, not JSON envelope (`api.rs:2112-2147`).
- Builds whole response in-memory, not true incremental stream (`api.rs:2204-2212`) -> memory pressure.

### Tests
- `test_isochrone_bulk_empty_origins_{mode}`
  Input: `origins=[]`.
  SHOULD: 400.
  Type: validation.

- `test_isochrone_bulk_max_origins_{mode}`
  Input: 10,000 and 10,001 origins.
  SHOULD: 10,000 accepted, 10,001 rejected 400.
  Type: validation/resource.

- `test_isochrone_bulk_coord_bounds_{mode}`
  Input: one invalid origin coordinate.
  SHOULD: 400 with index context.
  Type: validation.

- `test_isochrone_bulk_time_bounds_{mode}`
  Input: 0 and 7201.
  SHOULD: 400.
  Type: validation.

- `test_isochrone_bulk_partial_success_headers_{mode}`
  Input: mixed valid + ocean origins.
  OSRM/Valhalla analog for bulk services: either per-item errors or full failure.
  SHOULD: if partial success is intended, must be explicit and deterministic; headers and record count must match.
  Type: regression.

- `test_isochrone_bulk_matches_single_{mode}`
  Input: same origin/time queried both endpoints.
  SHOULD: decoded WKB polygon approximately equal to single `/isochrone` WKB.
  Type: invariant.

- `test_isochrone_bulk_binary_framing_{mode}`
  Input: parse output as `[origin_idx:u32][len:u32][wkb bytes]...`.
  SHOULD: framing never desyncs; each frame length valid.
  Type: response-format.

- `test_isochrone_bulk_malformed_json`
  Input: malformed body.
  SHOULD: 400.
  Type: validation.

- `test_isochrone_bulk_memory_pressure_{mode}`
  Input: 10k origins with large contours.
  SHOULD: bounded memory or explicit admission control; no OOM.
  Type: resource/regression.

---

## 7) POST `/match`

### High-risk findings
- Coordinate count hard-limited to 500 (`api.rs:2689-2698`).
- `gps_accuracy` must be `(0,100]` and non-NaN (`api.rs:2711-2721`).
- Map matching splits traces at gaps >2000m (`map_match.rs:29`, `map_match.rs:326-335`).
- No matched segments returns 404 `NoMatch` (`api.rs:2825-2831`).

### Tests
- `test_match_minimum_points_{mode}`
  Input: 0 or 1 coordinate.
  Valhalla: parse/validation error 400.
  SHOULD: 400.
  Type: validation.

- `test_match_maximum_points_{mode}`
  Input: 500 and 501 coordinates.
  SHOULD: 500 accepted, 501 rejected.
  Type: validation/resource.

- `test_match_coord_bounds_{mode}`
  Input: out-of-range coordinate inside list.
  SHOULD: 400 with index.
  Type: validation.

- `test_match_gps_accuracy_bounds_{mode}`
  Input: `gps_accuracy=0,-1,101,NaN,10`.
  SHOULD: only positive <=100 allowed.
  Type: validation.

- `test_match_invalid_geometry_format_{mode}`
  Input: `geometry="INVALID"`.
  SHOULD: 400.
  Type: validation.

- `test_match_off_network_trace_{mode}`
  Input: all points in ocean.
  Valhalla: map-match failure code.
  SHOULD: 404 NoMatch.
  Type: validation.

- `test_match_gap_splitting_{mode}`
  Input: trace with >2km gap in middle.
  SHOULD: multiple `matchings` with tracepoint nulls/discontinuity semantics documented.
  Type: invariant/regression.

- `test_match_duplicate_points_{mode}`
  Input: repeated identical coordinates.
  SHOULD: stable behavior, no NaN confidence, no panic.
  Type: regression.

- `test_match_known_road_snapping_{mode}`
  Input: noisy samples along known road.
  OSRM/Valhalla: should snap to plausible road.
  SHOULD: snapped trace follows target corridor.
  Type: invariant.

- `test_match_steps_sum_consistency_{mode}`
  Input: `steps=true`.
  SHOULD: steps sums approximately matching total matching duration/distance.
  Type: invariant.

- `test_match_geometry_format_equivalence_{mode}`
  Input: `polyline6`, `geojson`, `points`.
  SHOULD: coordinate-equivalent outputs.
  Type: response-format invariant.

---

## 8) POST `/trip`

### High-risk findings
- Waypoints count 2..100 (`trip.rs:491-511`).
- Any unsnappable waypoint fails whole request with 400 `NoSegment` (`trip.rs:561-574`).
- Unreachable legs become `duration=0` and optional `distance=0`, response `code="Partial"` (`trip.rs:624-683`) -> can mislead consumers.
- `annotations` unknown values are not rejected; silently behaves as duration-only unless `distance` token appears (`trip.rs:513-517`).

### Tests
- `test_trip_minimum_waypoints_{mode}`
  Input: 0/1 waypoint.
  OSRM trip: needs >=2.
  SHOULD: 400.
  Type: validation.

- `test_trip_maximum_waypoints_{mode}`
  Input: 100 and 101 waypoints.
  SHOULD: 100 accepted, 101 rejected.
  Type: validation/resource.

- `test_trip_coord_bounds_nan_inf_{mode}`
  Input: out-of-range and NaN coordinates.
  SHOULD: 400.
  Type: validation.

- `test_trip_unsnappable_waypoint_fails_{mode}`
  Input: one ocean waypoint among city points.
  SHOULD: 400 NoSegment (all-or-nothing policy).
  Type: validation/regression.

- `test_trip_two_points_matches_route_round_trip_false_{mode}`
  Input: two points, `round_trip=false`.
  OSRM/Valhalla: should align with route.
  SHOULD: leg metrics approximately equal `/route`.
  Type: invariant.

- `test_trip_two_points_round_trip_true_{mode}`
  Input: two points, `round_trip=true`.
  SHOULD: two legs (A->B, B->A) semantics explicit.
  Type: invariant.

- `test_trip_duplicate_waypoints_{mode}`
  Input: repeated points.
  SHOULD: no panic; deterministic order and sane totals.
  Type: regression.

- `test_trip_unreachable_partial_semantics_{mode}`
  Input: disconnected components.
  SHOULD: if `code=Partial`, avoid zero-as-valid-time ambiguity (prefer null/unreachable marker).
  Type: regression.

- `test_trip_annotations_variants_{mode}`
  Input: `duration`, `distance`, `duration,distance`, `foo`.
  SHOULD: explicit validation or strict documented fallback.
  Type: validation/regression.

- `test_trip_waypoint_index_mapping_{mode}`
  Input: 6+ waypoints.
  SHOULD: `waypoints[].waypoint_index` is a true inverse permutation of optimized order.
  Type: invariant.

- `test_trip_table_consistency_{mode}`
  Input: same waypoint set in `/table`.
  SHOULD: trip leg durations come from same matrix cells.
  Type: invariant.

---

## 9) GET `/height`

### High-risk findings
- Coordinate parser accepts any `f64` parseable token then range-checks (`elevation.rs:524-569`).
- Max 10,000 coordinates (`elevation.rs:582-587`).
- Out-of-coverage returns `null` elevation, not hard error (`elevation.rs:592-596`).
- If SRTM not loaded, endpoint returns 503 (`api.rs:2866-2876`).

### Tests
- `test_height_missing_coordinates_param`
  Input: no `coordinates` query.
  SHOULD: 400 extractor error.
  Type: validation.

- `test_height_empty_coordinates`
  Input: `coordinates=`.
  SHOULD: 400 "coordinates parameter is empty".
  Type: validation.

- `test_height_malformed_pair_syntax`
  Input: `coordinates=4.3|5.1,50.8`.
  SHOULD: 400 parse error with index.
  Type: validation.

- `test_height_bounds_nan_inf_negzero`
  Input: `NaN,50`, `inf,50`, `-0.0,0.0`.
  SHOULD: NaN/Inf rejected; `-0.0` accepted.
  Type: validation.

- `test_height_exact_boundary_values`
  Input: `-180,-90|180,90|0,0`.
  SHOULD: accepted syntactically; likely null elevations depending on coverage.
  Type: validation.

- `test_height_too_many_coordinates`
  Input: 10001 pairs.
  SHOULD: 400.
  Type: validation/resource.

- `test_height_outside_coverage_nulls`
  Input: mixed in-coverage and outside coverage points.
  SHOULD: 200 with per-point `elevation: null` where unavailable.
  Type: invariant.

- `test_height_duplicate_coordinates`
  Input: repeated same coordinate.
  SHOULD: stable repeated outputs.
  Type: invariant.

- `test_height_service_unavailable_without_srtm`
  Input: any request on server without SRTM.
  SHOULD: 503 with actionable message.
  Type: regression.

---

## 10) GET `/health`

### High-risk findings
- No input validation path, always returns JSON object (`api.rs:2899-2911`).
- Includes internal metadata (`data_dir`, counts) that may be sensitive in some deployments.

### Tests
- `test_health_always_200`
  Input: repeated calls under load.
  Valhalla status API also 200 by default.
  SHOULD: always 200 unless process unhealthy.
  Type: invariant.

- `test_health_response_shape`
  Input: single call.
  SHOULD: keys `status`, `version`, `uptime_s`, modes, data stats present and valid types.
  Type: response-format invariant.

- `test_health_unknown_query_ignored`
  Input: `/health?foo=bar`.
  SHOULD: still 200.
  Type: regression.

---

## Response-format invariants (cross-endpoint)

- `test_geojson_lon_lat_order_all_endpoints`
  Input: all geojson-capable endpoints.
  SHOULD: always `[lon, lat]` ordering (`geometry.rs:68`, `api.rs:1982`).
  Type: response-format invariant.

- `test_polyline6_roundtrip_route_match_isochrone`
  Input: decode polyline6 and compare with points/geojson.
  SHOULD: equivalent within 1e-6 precision.
  Type: response-format invariant.

- `test_isochrone_ring_closed_and_orientation`
  Input: geojson and WKB isochrones.
  SHOULD: outer closed; outer CCW, holes CW (for WKB path in `wkb_stream.rs`).
  Type: response-format invariant.

- `test_no_nan_or_infinity_in_json_outputs`
  Input: fuzz valid requests.
  SHOULD: JSON numeric fields never NaN/Inf.
  Type: invariant.

- `test_null_vs_missing_key_conventions`
  Input: endpoints with optional sections.
  SHOULD: keep consistent semantics:
  - `table`: matrix keys omitted when not requested, cells `null` for unreachable.
  - `route`: optional fields omitted unless requested.
  - `match`: `tracepoints` entries can be `null`.
  Type: regression.

---

## Concurrency / resource attack plan

- `test_concurrency_limit_regular_endpoints`
  Input: >32 concurrent `/route`/`/match` requests.
  SHOULD: requests above limit rejected/throttled (not crash), latency bounded.
  Type: resource/regression.

- `test_concurrency_limit_stream_endpoints`
  Input: >4 concurrent `/table/stream` and `/isochrone/bulk`.
  SHOULD: controlled rejection/backpressure, no OOM.
  Type: resource.

- `test_regular_timeout_120s`
  Input: deliberately expensive `/match` or `/trip`.
  SHOULD: 408 after 120s.
  Type: resource/regression.

- `test_stream_timeout_600s`
  Input: massive `/table/stream`.
  SHOULD: 408 at timeout boundary, no leaked workers.
  Type: resource.

- `test_route_alternatives_memory_spike`
  Input: many concurrent `alternatives=5` requests.
  SHOULD: no OOM despite per-request weight clone (~200MB comment).
  Type: resource/regression.

- `test_isochrone_bulk_memory_spike`
  Input: 10k origins with large polygons.
  SHOULD: bounded memory or explicit admission control.
  Type: resource/regression.

- `test_table_json_large_but_valid`
  Input: near-maximum 10M-cell `/table` request.
  SHOULD: no panic, predictable memory usage, response eventually returned or timed out cleanly.
  Type: resource.

---

## Industry comparison (OSRM / Valhalla / GraphHopper)

### OSRM (project-osrm.org docs)
- General error model: HTTP 400 for errors, HTTP 200 for success; body has `code` (`InvalidValue`, `NoSegment`, `TooBig`, etc.).
- Route-specific error: `NoRoute`.
- Table-specific error: `NoTable`; but per-cell unreachable can be `null` in successful table responses.
- Outside loaded map / no nearby edge: `NoSegment`.
- Unreachable destinations in table: `null` cells.
- Source=destination: generally valid and expected to succeed.
- Max waypoints: service-specific limits -> `TooBig` instead of one universal constant.

### Valhalla (valhalla.github.io docs)
- Uses HTTP semantics: 2xx success, 4xx request/data problems, 5xx server issues.
- Typical route/matrix failures are HTTP 400 with descriptive messages (e.g., "No suitable edges near location", "No path could be found for input").
- Internal code list includes constraints such as exceeded max locations/time/contours and too many shape points.
- Status endpoint returns 200 by default and can act as health endpoint.
- Outside map / unreachable generally surfaces as 400 with explicit failure reason.

### GraphHopper (docs.graphhopper.com)
- Routing endpoint documents 200/400/401/429/500.
- Error body pattern: `message` and optional `hints`.
- Matrix API has `fail_fast` (default true). With `fail_fast=false`, unresolved connections return `null` and `hints` include invalid points/pairs.
- GET route docs: max number of points depends on plan (not one hard universal number).
- Mentions HTTP 413 for oversized request entities in optimization guidance.

### What Butterfly-route SHOULD do for compatibility
- Standardize one error envelope across all endpoints (including `/isochrone/bulk` and stream preflight errors).
- Prefer consistent 400-class domain errors with machine-readable codes (`InvalidValue`, `NoSegment`, `NoRoute`, `TooBig`, `NoMatch`).
- Preserve per-cell null semantics for matrix-like outputs instead of converting unreachable legs to zero.
- Keep health endpoint always cheap and reliably 200 unless explicit degraded mode is implemented.

---

## Mean regression list (highest priority)

1. `test_route_alternatives_memory_spike` (possible OOM via ~200MB/request clone) — mitigated by concurrency limit 32 + timeout 120s.
2. ~~`test_trip_unreachable_partial_semantics_{mode}` (0 duration for unreachable legs)~~ **FIXED (O-Sprint)**: TripLeg.duration/distance now `Option<f64>`, unreachable → `null`.
3. `test_isochrone_bulk_partial_success_headers_{mode}` (silent data loss behind HTTP 200) — by design: reflected in X-Total-* headers.
4. `test_table_stream_matches_table_{mode}` (unit/sentinel mismatches) — verified: stream=ms, table=seconds, u32::MAX=null mapping documented.
5. `test_isochrone_geojson_ring_properties_{mode}` and `test_polyline6_roundtrip_route_match_isochrone` (format correctness drift) — verified: CCW rings, closed, round-trips clean.
6. ~~`test_nearest_number_limits_{mode}` (code/doc mismatch max10 vs max100)~~ **FIXED (O-Sprint)**: doc updated to "max 100", number=0 now rejected.
7. ~~`test_mode_whitespace_rejection`~~ **Already correct**: `parse_mode` uses `to_lowercase()` without trimming — " car " is rejected.

### Additional O-Sprint fixes (2026-02-08)
- isochrone direction case-insensitive (`.to_lowercase()`)
- table/trip annotations: validate tokens, reject unknown values
- isochrone/bulk errors: JSON ErrorResponse envelope (was raw bytes)
- 35 new unit tests added (189 total, up from 154)

