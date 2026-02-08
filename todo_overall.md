# Butterfly-Route: Architecture & Roadmap

## Goal

Build a routing engine with **exact turn-aware isochrones** and **OSRM-class speed** using:
- Edge-based graph (state = directed edge ID)
- Per-mode CCH preprocessing on filtered edge-based graphs
- Exact bounded Dijkstra on the hierarchy for all query types

**Key principle:** One graph, one hierarchy per mode, one query engine. Routes, matrices, and isochrones use identical cost semantics.

---

## Pipeline (All Steps Complete)

| Step | Output | Description |
|------|--------|-------------|
| 1 | `nodes.sa`, `nodes.si`, `ways.raw`, `relations.raw` | PBF ingest |
| 2 | `way_attrs.*.bin`, `turn_rules.*.bin` | Per-mode profiling (car/bike/foot) |
| 3 | `nbg.csr`, `nbg.geo`, `nbg.node_map` | Node-Based Graph (intermediate) |
| 4 | `ebg.nodes`, `ebg.csr`, `ebg.turn_table` | Edge-Based Graph (THE routing graph) |
| 5 | `w.*.u32`, `t.*.u32`, `mask.*.bitset`, `filtered.*.ebg` | Per-mode weights, masks, filtered EBGs |
| 6 | `order.{mode}.ebg` | Per-mode CCH ordering on filtered EBG |
| 7 | `cch.{mode}.topo` | Per-mode CCH contraction (shortcuts topology) |
| 8 | `cch.w.{mode}.u32`, `cch.d.{mode}.u32` | Per-mode customized weights (duration + distance) |
| 9 | HTTP server | Query server with all endpoints |

---

## Architecture: Per-Mode Filtered CCH

Each transport mode has its own CCH built on a **filtered subgraph** containing only mode-accessible nodes:

```
Original EBG (5M nodes)
    |
FilteredEbg (per mode)
    - Car:  2.4M nodes (49%)
    - Bike: 4.8M nodes (95%)
    - Foot: 4.9M nodes (98%)
    |
Per-mode CCH ordering -> order.{mode}.ebg
    |
Per-mode CCH topology -> cch.{mode}.topo
    |
Per-mode weights -> cch.w.{mode}.u32
```

**Why per-mode CCH?** A shared CCH on all nodes fails when some modes can't access certain nodes.

---

## Query Server Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /route` | P2P routing with geometry (polyline6/geojson/points), steps with road names, alternatives |
| `GET /nearest` | Snap to nearest road segments with distance |
| `POST /table` | Distance matrix with duration and/or distance (bucket M2M) |
| `POST /table/stream` | Arrow IPC streaming for large matrices (50k+) |
| `GET /isochrone` | Areal polygon + optional network roads, `direction=depart|arrive`, WKB via Accept header |
| `POST /isochrone/bulk` | Parallel batch isochrones (WKB stream) |
| `POST /trip` | TSP/trip optimization (nearest-neighbor + 2-opt + or-opt) |
| `POST /match` | GPS trace map matching (HMM + Viterbi, local Dijkstra + CCH P2P transitions) |
| `GET /height` | Elevation lookup from SRTM DEM tiles |
| `GET /health` | Health check with uptime, node/edge counts, modes |
| `GET /metrics` | Prometheus metrics (per-endpoint latency histograms) |
| `GET /swagger-ui/` | OpenAPI documentation |

### Production Infrastructure

| Feature | Implementation |
|---------|---------------|
| Structured logging | `tracing` + `tracing-subscriber` (text/JSON via `--log-format`) |
| Graceful shutdown | SIGINT + SIGTERM handling |
| Request timeouts | 120s API, 600s streaming |
| Response compression | gzip + brotli (API routes only) |
| Input validation | Coordinate bounds, time_s 1-7200, number max 100 |
| Panic recovery | `CatchPanicLayer` (returns 500 JSON) |
| Docker | Multi-stage build (`rust:bookworm` -> `debian:bookworm-slim`) |

### Algorithm Selection

```
if query_type == isochrone:
    use PHAST (need all reachable nodes)
elif n_sources * n_targets <= 10_000:
    use Bucket M2M (sparse, low latency)
else:
    use K-lane batched PHAST + Arrow streaming (throughput)
```

---

## Performance (Belgium)

### Build Times

| Step | Time | Output Size |
|------|------|-------------|
| Step 6 (ordering) | ~3s per mode | 9-19 MB |
| Step 7 (contraction) | ~23s per mode | 200-350 MB |
| Step 8 (customization) | ~5s per mode | 180-230 MB |

### Query Performance

| Operation | Latency |
|-----------|---------|
| Server startup | ~25s (loading all data + 754K road names) |
| P2P route | < 10ms |
| Isochrone (30min, car) | 5ms p50 |
| Bulk isochrones | 1,526 iso/sec |
| Matrix 100x100 | 164ms |
| Matrix 10k x 10k (Arrow) | 18.2s (**1.8x FASTER than OSRM**) |

### vs OSRM (Fair HTTP Comparison)

| Size | OSRM CH | Butterfly | Ratio |
|------|---------|-----------|-------|
| 100x100 | 55ms | 164ms | 3x slower |
| 1000x1000 | 0.68s | 1.55s | 2.3x slower |
| 5000x5000 | 8.0s | 11.1s | 1.38x slower |
| 10000x10000 | 32.9s | **18.2s** | **1.8x FASTER** |

**Key insight:** Edge-based CCH has 2.5x more states than node-based (exact turn handling). The overhead is acceptable for small queries. **Butterfly wins at scale** due to Arrow streaming + parallel tiling.

### vs Valhalla (Isochrones)

| Threshold | Valhalla | Butterfly | Speedup |
|-----------|----------|-----------|---------|
| 5 min | 36ms | 4ms | **9.5x faster** |
| 10 min | 63ms | 8ms | **7.9x faster** |
| 30 min | 260ms | 78ms | **3.3x faster** |
| 60 min | 737ms | 302ms | **2.4x faster** |

---

## What NOT to Do

- Do not use node-based graphs for routing/isochrones
- Do not share a CCH across all modes (causes orphaned nodes)
- Do not approximate range queries
- Do not use different backends for different query types
- Do not snap differently for different APIs

---

## Deferred / Future Work

| Feature | Complexity | Notes |
|---------|------------|-------|
| Map matching (GPS trace -> route) | High | HMM-based, needs Viterbi on CCH |
| ~~Two-resolution isochrone mask~~ | ~~Medium~~ | WONTFIX — 30m grid + simplification pass is sufficient |
| Truck routing (dimensions) | High | Needs vehicle profile system |
| Time-dependent routing | Very High | Needs time-expanded graph |
| Hybrid exact turn model | Abandoned | Equivalence-class hybrid incompatible with CCH separator quality |

---

## CLI Commands

```bash
# Build pipeline
butterfly-route step1-ingest -i map.osm.pbf -o ./build/
butterfly-route step2-profile --ways ./build/ways.raw --relations ./build/relations.raw -o ./build/
butterfly-route step3-nbg ... -o ./build/
butterfly-route step4-ebg ... -o ./build/
butterfly-route step5-weights ... -o ./build/

# Per-mode CCH pipeline
butterfly-route step6-order --filtered-ebg ./build/filtered.car.ebg --mode car -o ./build/
butterfly-route step7-contract --filtered-ebg ./build/filtered.car.ebg --order ./build/order.car.ebg --mode car -o ./build/
butterfly-route step8-customize --cch-topo ./build/cch.car.topo --mode car -o ./build/

# Query server (local)
butterfly-route serve --data-dir ./build/ --port 8080

# Query server (Docker, recommended)
docker build -t butterfly-route .
docker run -d --name butterfly -p 3001:8080 -v "${PWD}/data/belgium:/data" butterfly-route
```

---

## Audit Findings (2026-02-07)

Combined findings from Codex (gpt-5.3-codex) and Gemini (gemini-2.5-pro) repo-wide audits. Originally filed at commit `05554e9`. All CRITICALs and HIGHs remediated as of J-Sprint (2026-02-08).

### CRITICAL

| # | Finding | Component | Location | Source |
|---|---------|-----------|----------|--------|
| C1 | ~~**CI benchmark job is broken.**~~ **FIXED** (J-Sprint). Replaced with real release build + binary verification. Also added c-bindings tests (H3) and fixed cache (L2). | CI | `.github/workflows/ci.yml` | Gemini |
| C2 | ~~**FFI: Unhandled panics across `extern "C"` boundary.**~~ **FIXED** (J-Sprint). All 5 FFI functions wrapped in `catch_unwind`. `RUNTIME` returns `Option`. `butterfly_version()` uses static byte string. | butterfly-dl | `tools/butterfly-dl/src/ffi.rs` | Gemini |
| C3 | ~~**FFI: Use-after-free in progress callback.**~~ **FALSE POSITIVE**. `block_on()` blocks the calling thread — `user_data` is guaranteed valid. Added explicit safety comments. | butterfly-dl | `tools/butterfly-dl/src/ffi.rs` | Gemini |

### HIGH

| # | Finding | Component | Location | Source |
|---|---------|-----------|----------|--------|
| H1 | ~~**FFI: Lossy error handling.**~~ **FIXED** (J-Sprint). Added thread-local `LAST_ERROR` + `butterfly_last_error_message()`. Full `Display` output stored on every error. | butterfly-dl | `tools/butterfly-dl/src/ffi.rs` | Gemini |
| H2 | ~~**FFI: Naive threading model.**~~ **BY DESIGN** (J-Sprint). `block_on` is correct for sync C callers. Added module-level documentation explaining concurrency model. | butterfly-dl | `tools/butterfly-dl/src/ffi.rs` | Gemini |
| H3 | ~~**Feature-specific tests not in CI.**~~ **FIXED** (J-Sprint, bundled with C1). Added `cargo test -p butterfly-dl --features c-bindings` to CI test matrix. | CI | `.github/workflows/ci.yml` | Gemini |
| H4 | ~~**Hardcoded static source list.**~~ **DOCUMENTED** (J-Sprint). Added design rationale: static list avoids Geofabrik API dependency, covers ~120 regions. Dynamic loading deferred. | butterfly-common | `butterfly-common/src/error.rs` | Gemini |
| H5 | ~~**Root Makefile is misleading.**~~ **FIXED** (J-Sprint). Moved to `tools/butterfly-dl/Makefile`. All cargo commands now use `-p butterfly-dl`. | Build | `tools/butterfly-dl/Makefile` | Gemini |
| H6 | ~~**source_idx stored as `u16` in bucket M2M.**~~ **FIXED** (commit `845bbcc`). Widened to `u32` across `bucket_ch.rs` and `nbg_ch/query.rs`. Zero memory cost. | butterfly-route | `tools/butterfly-route/src/matrix/bucket_ch.rs` | Codex |
| H7 | ~~**`unwrap()` calls in production API code paths.**~~ **FIXED** (commit `845bbcc`). 2x `unwrap_or_else`, 2x `get_or_insert_with`. | butterfly-route | `tools/butterfly-route/src/step9/api.rs` | Codex |
| H8 | ~~**8 `unsafe` blocks in step7 parallel edge filling.**~~ **FIXED** (commit `845bbcc`). 8 `debug_assert!` bounds checks added. Zero cost in release. | butterfly-route | `tools/butterfly-route/src/step7.rs` | Codex |
| H9 | ~~**Fuzzy matching uses unexplained magic numbers.**~~ **FIXED** (J-Sprint). Added inline comments explaining all weights: JW/Lev split, prefix/substring/length bonuses, anti-bias penalty, threshold. | butterfly-common | `butterfly-common/src/error.rs` | Gemini |

### MEDIUM

| # | Finding | Component | Location | Source |
|---|---------|-----------|----------|--------|
| M1 | ~~**Overly broad `unsafe` scopes in FFI.**~~ **FIXED** (K-Sprint). Added SAFETY comments to all unsafe blocks. Function-level `unsafe` is required by FFI ABI; inner blocks are already minimal. | butterfly-dl | `tools/butterfly-dl/src/ffi.rs` | Gemini |
| M2 | ~~**Non-UTF8 path handling in FFI.**~~ **DOCUMENTED** (K-Sprint). UTF-8 requirement documented in function-level docs with M2 reference. Returns descriptive error for non-UTF8 inputs. | butterfly-dl | `tools/butterfly-dl/src/ffi.rs` | Gemini |
| M3 | ~~**Step 6 minimum-degree ordering is O(n^2).**~~ **FIXED** (K-Sprint). Replaced linear scan with `BinaryHeap` + lazy deletion → O(n log n). Refactored 3 copies into generic `minimum_degree_order_generic<G: CsrAdjacency>()`. | butterfly-route | `tools/butterfly-route/src/step6.rs` | Codex |
| M4 | ~~**Step 6 uses `HashSet<usize>` adjacency.**~~ **FIXED** (K-Sprint). Replaced with sorted `Vec<usize>` + `binary_search` for cache-friendly access. Part of M3 refactor. | butterfly-route | `tools/butterfly-route/src/step6.rs` | Codex |
| M5 | ~~**`anyhow` used in library-level code.**~~ **DOCUMENTED** (K-Sprint). Added rationale comment in Cargo.toml: `anyhow` deliberate for app boundary, `thiserror` for library errors. | butterfly-dl, butterfly-route | `Cargo.toml` files | Gemini |
| M6 | ~~**Contour holes vector always empty.**~~ **FIXED** (K-Sprint). `marching_squares_with_holes()` detects hole components via flood-fill, traces boundaries, populates `ContourResult.holes`. WKB encoder handles CW holes. 2 unit tests added. | butterfly-route | `tools/butterfly-route/src/range/contour.rs` | Codex |
| M7 | ~~**No rate limiting or request size limits.**~~ **FIXED** (K-Sprint). Added `ConcurrencyLimitLayer`: max 32 for API routes, max 4 for streaming routes. Added `tower` dependency. | butterfly-route | `tools/butterfly-route/src/step9/api.rs` | Codex |
| M8 | ~~**Cross-compilation setup could be more robust.**~~ **FIXED** (K-Sprint). Replaced manual gcc-aarch64 + cargo config with `cross build` for containerized cross-compilation. | CI | `.github/workflows/ci.yml` | Gemini |
| M9 | ~~**`Makefile` install target modifies system dirs with no `sudo` warning.**~~ **FIXED** (K-Sprint). Added root privileges warning comment and `mkdir -p` error message. | Build | `Makefile` | Gemini |

### LOW

| # | Finding | Component | Location | Source |
|---|---------|-----------|----------|--------|
| L1 | ~~**Windows DLL naming.**~~ **REMOVED** (L-Sprint). No Windows/macOS builds — Linux/Docker only. | Build | `Makefile` | Gemini |
| L2 | ~~**CI target dir caching.**~~ **FIXED** (J-Sprint, bundled with C1). Now caches only `~/.cargo/registry` + `~/.cargo/git`. | CI | `.github/workflows/ci.yml` | Gemini |
| L3 | ~~**Simplify tolerance conversion is approximate.**~~ **FIXED** (J-Sprint). Simplification now runs in grid coordinates (Mercator space) before WGS84 conversion. Eliminates lat/lon distortion entirely. | butterfly-route | `tools/butterfly-route/src/range/contour.rs` | Codex |
| L4 | ~~**Pre-existing failing test in butterfly-dl.**~~ **FIXED** (K-Sprint). Widened assertion to accept "Could not determine file size" and "HTTP error" (Geofabrik doesn't always return 404 for invalid paths). | butterfly-dl | `tools/butterfly-dl/tests/integration_tests.rs` | Known |
| L5 | ~~**`version = "2.0"` semver ambiguity.**~~ **FIXED** (L-Sprint). Changed to `"2.0.0"`. | butterfly-dl | `tools/butterfly-dl/Cargo.toml` | Gemini |

### Remediation Status

**ALL CRITICALs, HIGHs, and MEDIUMs RESOLVED.**

**I-Sprint (commit `845bbcc`, 2026-02-07):** H6, H7, H8 — butterfly-route HIGHs
**J-Sprint (2026-02-08):** C1, C2, C3, H1, H2, H3, H4, H5, H9, L2, L3 — CRITICALs + remaining HIGHs + LOWs
**K-Sprint (2026-02-08):** M1-M9, L4 — All MEDIUMs + pre-existing test failure
**L-Sprint (2026-02-08):** Codex-H2, Codex-H8 — deferred architectural HIGHs (thread-local CCH query state, CRC validation across all format readers)

**Remaining backlog:** None (L1 removed — no Windows builds, L5 fixed).

---

## Codex Audit (2026-02-08)

### CRITICAL (correctness bugs, data corruption, wrong results)

1. **Isochrone geometry hardcodes `Mode::Car`**
   - `tools/butterfly-route/src/step9/geometry.rs:187` calls `build_isochrone_geometry_sparse(..., Mode::Car)`
   - Expected: geometry config must respect request mode. Actual: bike/foot isochrone geometry is generated with car contour parameters.

2. **Self-contradictory route output when source == target edge**
   - `tools/butterfly-route/src/step9/query.rs:125-131` returns distance `0` when `source == target`.
   - `tools/butterfly-route/src/step9/unpack.rs:22` always returns path starting with `source`.
   - `tools/butterfly-route/src/step9/api.rs:405-414` builds geometry from that path and reports `distance_m = geometry.distance_m`, `duration_s = result.distance / 10`.
   - Actual failure mode: same snapped edge can return `duration=0` with non-zero geometry distance (and full-edge geometry).

3. **Trip endpoint returns `code: "Ok"` with unreachable legs containing NaN**
   - `tools/butterfly-route/src/step9/trip.rs:583-639` — per-leg unreachable values become `NaN`, while totals still accumulate only reachable legs.
   - Expected: explicit failure/degraded status. Actual: partial-invalid trip returned as success.

### HIGH (performance bottlenecks, algorithmic inefficiency, security)

1. **CCH query runs both searches to exhaustion with no early termination**
   - `tools/butterfly-route/src/step9/query.rs:164-166`
   - Direct throughput killer for `/route` and catastrophic for map-matching transition matrices.

2. ~~**CCH query allocates O(|V|) vectors per call**~~ **FIXED** (L-Sprint). Thread-local generation-stamped state eliminates O(|V|) allocation per query. Distance/parent arrays allocated once per thread, reused via version stamps. O(1) query init.
   - `tools/butterfly-route/src/step9/query.rs`

3. **Map matching transition distance ignores snapped offsets on source/target edges**
   - `tools/butterfly-route/src/step9/map_match.rs:475-531`
   - Sums full edge lengths over unpacked edge states, ignoring partial coverage at start/end edges. This biases HMM transitions and can pick wrong paths.

4. **Map matching can emit disconnected paths**
   - `tools/butterfly-route/src/step9/map_match.rs:575-578`
   - On missing sub-path, code appends target edge anyway (`full_path.push(cand.ebg_id)`), producing invalid topology.

5. **Sparse contour discards real components and holes**
   - `tools/butterfly-route/src/range/sparse_contour.rs:700-704` returns `holes: vec![]`
   - `tools/butterfly-route/src/range/sparse_contour.rs:747-751` keeps only largest contour.
   - API/WKB pipeline also drops holes: `api.rs:1804-1807`, `api.rs:2028-2031`
   - Under-reports reachable area and destroys multi-component correctness.

6. **Integer overflow in streamed matrix distance conversion**
   - `tools/butterfly-route/src/step9/api.rs:1543`: `d * 100` can wrap in release mode.

7. **Tile-size truncation bug in streaming matrix**
   - `api.rs:1481-1482`, `1551-1552` casts tile dimensions to `u16`
   - `matrix/arrow_stream.rs:39-41`, `69-70`
   - Missing validation for `src_tile_size/dst_tile_size <= u16::MAX` can silently truncate dimensions and corrupt output.

8. ~~**Binary format readers skip CRC/magic/version validation**~~ **FIXED** (L-Sprint). CRC64 validation added to ALL 17 binary format readers. Pattern A (single CRC): cch_topo, cch_weights, ebg_csr, ebg_nodes, ebg_turn_table, filtered_ebg, hybrid_state, nbg_csr, nbg_geo, nbg_node_map, order_ebg. Pattern B (body+file CRC): mod_mask, mod_weights, mod_turns, turn_rules, way_attrs. Round-trip + corruption tests added.

9. **Malformed-file panic/DoS risk in deserializers**
   - `formats/ways.rs:228-254`, `formats/relations.rs:290-327`
   - Use `all_bytes[pos..pos+N]` inside count-driven loops without prior bounds checks.

10. **`/table` lacks matrix-size guardrails**
    - `api.rs:1035-1097`, `1182-1287`
    - Response memory can explode for large `sources × destinations`.

11. **`/debug/compare` has a correctness bug and is mounted by default**
    - `api.rs:2994` passes filtered IDs to `query_with_debug`, which expects rank IDs.
    - Endpoint is mounted by default at `api.rs:97`.

### MEDIUM (code quality, maintainability, missing tests)

1. **`ModMask` sets accessibility bit before zero-speed rejection**
   - `tools/butterfly-route/src/step5.rs:341-352`
   - Zero-speed edges can remain marked accessible in mask.

2. **CCH customization non-convergence is only logged, not failed**
   - `tools/butterfly-route/src/step8.rs:503-506`
   - Pipeline may ship non-converged weights.

3. **`unpack` silently drops missing decomposition edges**
   - `tools/butterfly-route/src/step9/unpack.rs:63-70`, `87-94`
   - Missing link during shortcut expansion returns partial path with no error.

4. **Large-latitude assumptions hardcoded into snapping/map-matching**
   - `spatial.rs:11-13`, `map_match.rs:32-35`, `272-274`
   - Distances/projections are tuned to ~50°N and degrade globally.

5. **Sparse contour simplification uses fixed meter→degree conversion**
   - `range/sparse_contour.rs:695` — latitude-dependent distortion remains.

6. **Tests missing for regression-prone paths**
   - No tests for: isochrone mode propagation, unreachable-trip API semantics, streaming matrix overflow/truncation, map-matching disconnected fallback, sparse multi-component/holes.

7. **Provenance hashing is stubbed with zero bytes**
   - `step6_lifted.rs:135`, `nbg/mod.rs:436`: `inputs_sha: [0u8; 32] // TODO`
   - Pipeline reproducibility and mismatch detection weakened.

8. **Tag lookup is O(dict_size) per lookup due to reverse scan**
   - `profiles/tag_lookup.rs:61`: `key_dict.iter().find(|(_, v)| v.as_str() == key)`
   - Avoidable hot-path overhead in profile processing.

9. **Map-matching fallback candidate emits `(0,0,inf)` and is not filtered out**
   - `map_match.rs:202`, `map_match.rs:297`
   - Invalid state pollution increases transition work and instability.

### LOW (style, documentation, minor improvements)

1. **Documentation/claims mismatch**
   - `README.md:132` claims audit-clean status, but unresolved correctness and integrity gaps remain.

2. **Placeholder integrity metadata still present**
   - `step6_lifted.rs:135`, `nbg/mod.rs:436`: `inputs_sha` TODO
   - `validate/step5.rs:142-154`: hash fields set to `"TODO"`.

3. **Comment drift**
   - `nbg_geo.rs:140` says "40 bytes each" but record layout in code is 36 bytes.

4. **Potential divide-by-zero in stats output**
   - `hybrid/equiv_builder.rs:120`, `125`: percentages divide by `n_reachable_nbg` without zero guard.

5. **Matrix tile dimensions truncated to `u16` without explicit request-size guard**
   - `api.rs:1551`, `matrix/arrow_stream.rs:79`, `83`

---

## Second-Pass Audit Findings (2026-02-08, Codex gpt-5.3-codex)

Full repo-wide audit with read-only sandbox access. Findings are net-new relative to the first audit section above. Items that overlap existing findings are marked **(see above #ID)**.

### CRITICAL

| # | Finding | Location | Notes |
|---|---------|----------|-------|
| Codex-C1 | **Parallel downloader path violates <1GB memory guarantee.** Entire chunk payloads buffered concurrently before ordered writeback. | `tools/butterfly-dl/src/core/downloader.rs:388,416,421,429` | Can allocate multi-GB under concurrent chunks. |
| Codex-C2 | **Resumable download does not validate HTTP range response semantics.** Server may respond `200` (full body) instead of `206` (partial), silently corrupting output by prepending already-downloaded data. | `tools/butterfly-dl/src/core/downloader.rs:271-274`, `stream.rs:76` | Must check for `206 Partial Content` status. |
| Codex-C3 | **C FFI header declares `butterfly_has_s3_support()` with no implementation.** Linker error for any C consumer including this function. ABI/header drift. | `tools/butterfly-dl/include/butterfly.h:180`, `src/ffi.rs` (absent) | S3 was removed but header not updated. |
| Codex-C4 | **`/isochrone/bulk` is unbounded — no origin count limit, fully buffers all results in memory before responding.** A single request with 100K origins will OOM the server. | `tools/butterfly-route/src/step9/api.rs:2015,2044,2098,2103,2115` | Only checks for empty, no max. |
| Codex-C5 | **`/debug/compare` is mounted in production router.** Expensive diagnostic endpoint with no auth, no rate limit. | `tools/butterfly-route/src/step9/api.rs:97` | See also existing HIGH #11. |

### HIGH

| # | Finding | Location | Notes |
|---|---------|----------|-------|
| Codex-H1 | **`/match` allows up to 10K GPS points — worst-case compute explosion.** Viterbi transition work scales with O(n * candidates^2 * P2P queries). 10K points = minutes of CPU per request. | `api.rs:2730`, `map_match.rs:15,402,419,482` | Needs hard cap (~200 points). |
| Codex-H3 | **CPU-heavy graph algorithms execute inline on async Tokio handlers.** Route computation, matrix, isochrone, trip all block the async runtime. Under load, async task starvation. | `api.rs:470,1231,1818,2785`, `trip.rs:537` | Should use `spawn_blocking` or rayon. |
| Codex-H4 | **`/table/stream` lacks hard request-size guard.** `/table` has a 100K cap but `/table/stream` has none — a million-point request is unchecked. | `api.rs:1364,1446` vs `api.rs:1092,1095` | |
| Codex-H5 | **`/route` alternatives clone full CCH weights per request.** Each alternative request deep-clones the weight array for penalty application. | `api.rs:475` | ~100MB clone per alt request. |
| Codex-H6 | **Dataset step directory selection is nondeterministic.** `read_dir` iteration order is filesystem-dependent — different data directories could load different step versions on different systems. | `state.rs:190-195` | Should sort or use explicit version selection. |
| Codex-H7 | **CORS is fully permissive (`allow_origin(Any)`, all methods, all headers).** For a public-facing production API, this is a security concern. | `api.rs:79-82` | Should be configurable or restricted. |

### MEDIUM

| # | Finding | Location | Notes |
|---|---------|----------|-------|
| Codex-M1 | **Ingest `threads` config field is unused; ingestion uses Mutex-heavy pattern.** `IngestConfig.threads` is declared but never passed to the parallel parser. Tight loops push to Mutex-protected Vecs. | `ingest/mod.rs:15,31,137,145,208,270` | |
| Codex-M2 | **Step5 lock condition D (graph-level parity check) is explicitly skipped.** | `validate/step5.rs:125,127` | |
| Codex-M3 | **Step5 lock hashes are `"TODO"` placeholders.** All weight/mask SHA fields hardcoded to `"TODO"` string. | `validate/step5.rs:142-154` | See also existing LOW #2. |
| Codex-M4 | **NBG edge flags always zero (`flags: 0, // TODO`).** | `nbg/mod.rs:381` | |
| Codex-M5 | **Step9 module header docs list only 5 endpoints; actual router has 12+.** | `step9/mod.rs:7-9` vs `api.rs:89-121` | |
| Codex-M6 | **OpenAPI spec omits public endpoints.** `/trip`, `/match`, `/height`, `/debug/compare`, `/isochrone/bulk`, `/table/stream` missing from `#[openapi(paths(...))]`. | `api.rs:45` | Community users can't discover full API. |
| Codex-M7 | **`api.rs` tests only cover bearing/distance math helpers — zero HTTP handler tests.** No integration tests for any endpoint. | `api.rs:3833-4007` | |
| Codex-M8 | **`nodes.si` validation is shallow.** Only checks magic and sample ordering — no CRC, no record-count cross-check against `nodes.sa`. | `validate/mod.rs:228-246` | Only format file without CRC. |
| Codex-M9 | **User-facing docs are stale/misleading.** README lists non-existent tools (butterfly-shrink, butterfly-tile, butterfly-serve). Trip example uses `locations` but code expects `coordinates`. `/table/v1/driving` example doesn't exist. CONTRIBUTING references unavailable tools. | `README.md:20-22,221,224`, `CONTRIBUTING.md:25-28` | Embarrassing for open-source launch. |
| Codex-M10 | **Docker packaging gaps.** Floating base tags (no pinned hash). Runtime runs as root (no user drop). Expanded runtime package surface (deb packages installed in slim image). | `Dockerfile:3,37-42,58-59` | |
| Codex-M11 | **Public metadata still claims S3 support.** lib.rs docstring, butterfly.h header, .pc.in description all mention S3 but implementation is HTTP-only. | `lib.rs:8`, `butterfly.h:5,8,174`, `butterfly-dl.pc.in:7` | Misleading for library consumers. |
| Codex-M12 | **Potential unchecked multiplication overflow in streamed matrix accounting.** `n_sources * n_destinations` can overflow `usize` for very large inputs. | `api.rs:1446` | See also existing HIGH #6. |

### LOW

| # | Finding | Location | Notes |
|---|---------|----------|-------|
| Codex-L1 | **`find_free_port` has panic path + TOCTOU race.** `.expect()` on bind failure, and freed port can be taken before actual bind. | `step9/mod.rs:67-73,113` | |
| Codex-L2 | **Dead legacy helpers in production API module.** `bounded_dijkstra`, `debug_compare` helper funcs, `#[allow(dead_code)]` annotations. | `api.rs:2126,2539,3482` | |
| Codex-L3 | **Downloader test coverage misses resume-integrity edge cases.** Tests cover happy path and retry timing but not resume with wrong HTTP status (200 vs 206). | `downloader.rs:271-274,596,680` | |

### COSMETIC

| # | Finding | Location | Notes |
|---|---------|----------|-------|
| Codex-X1 | **README CI badge points to GitHub Actions workflow that no longer exists.** `.github/` directory was deleted. | `README.md:3` | Dead badge link. |
