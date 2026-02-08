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
| Two-resolution isochrone mask | Medium | Better boundary accuracy |
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
| M1 | **Overly broad `unsafe` scopes in FFI.** Entire functions marked `unsafe` rather than minimizing to pointer dereferences. | butterfly-dl | `tools/butterfly-dl/src/ffi.rs` | Gemini |
| M2 | **Non-UTF8 path handling in FFI.** Code assumes C file paths are valid UTF-8. Fails on valid non-UTF8 paths (common on Linux). | butterfly-dl | `tools/butterfly-dl/src/ffi.rs` | Gemini |
| M3 | **Step 6 minimum-degree ordering is O(n^2).** Linear scan per elimination step in `minimum_degree_order()`. Should use a priority queue for O(n log n). Acceptable for small components but may bottleneck on large leaf partitions. | butterfly-route | `tools/butterfly-route/src/step6.rs:597-690` | Codex |
| M4 | **Step 6 uses `HashSet<usize>` adjacency.** High memory overhead per node. `Vec` + sort for neighbor lookup would be more cache-friendly for the elimination game. | butterfly-route | `tools/butterfly-route/src/step6.rs:613` | Codex |
| M5 | **`anyhow` used in library-level code.** `anyhow::Result` obscures specific error types. Should use typed errors (`thiserror`) in algorithmic code; reserve `anyhow` for application boundaries. | butterfly-dl, butterfly-route | `Cargo.toml` files | Gemini |
| M6 | **Contour holes vector always empty.** `ContourResult.holes` is always `vec![]`. Multi-polygon support (e.g., islands within isochrone) is absent. | butterfly-route | `tools/butterfly-route/src/range/contour.rs:96,181,205` | Codex |
| M7 | **No rate limiting or request size limits on non-streaming API routes.** A single client can overwhelm the server with expensive concurrent matrix computations. | butterfly-route | `tools/butterfly-route/src/step9/api.rs` | Codex |
| M8 | **Cross-compilation setup could be more robust.** Using `cross` for containerized toolchains instead of raw `cargo build --target`. | CI | `.github/workflows/ci.yml` | Gemini |
| M9 | **`Makefile` install target modifies system dirs with no `sudo` warning.** | Build | `Makefile` | Gemini |

### LOW

| # | Finding | Component | Location | Source |
|---|---------|-----------|----------|--------|
| L1 | **Windows DLL naming.** `butterfly_dl.dll` uses underscores; `butterfly-dl.dll` would match Cargo convention. | Build | `Makefile` | Gemini |
| L2 | ~~**CI target dir caching.**~~ **FIXED** (J-Sprint, bundled with C1). Now caches only `~/.cargo/registry` + `~/.cargo/git`. | CI | `.github/workflows/ci.yml` | Gemini |
| L3 | ~~**Simplify tolerance conversion is approximate.**~~ **FIXED** (J-Sprint). Simplification now runs in grid coordinates (Mercator space) before WGS84 conversion. Eliminates lat/lon distortion entirely. | butterfly-route | `tools/butterfly-route/src/range/contour.rs` | Codex |
| L4 | **Pre-existing failing test in butterfly-dl.** `test_invalid_continent_fails_gracefully` fails consistently. Should be `#[ignore]` with issue link or fixed. | butterfly-dl | `tools/butterfly-dl/src/core/downloader.rs` | Known |
| L5 | **`version = "2.0"` semver ambiguity.** butterfly-dl depends on butterfly-common `"2.0"` — should be `"2.0.0"` for precision. | butterfly-dl | `tools/butterfly-dl/Cargo.toml` | Gemini |

### Remediation Status

**ALL CRITICALs and HIGHs RESOLVED.**

**I-Sprint (commit `845bbcc`, 2026-02-07):** H6, H7, H8 — butterfly-route HIGHs
**J-Sprint (2026-02-08):** C1, C2, C3, H1, H2, H3, H4, H5, H9, L2, L3 — CRITICALs + remaining HIGHs + LOWs

**Remaining backlog (MEDIUM/LOW only):**
1. M3/M4: Optimize step6 elimination game (PQ + Vec adjacency)
2. M6: Implement multi-polygon contour support (holes)
3. M7: Add concurrency limiting / rate limiting middleware
4. L1: Windows DLL naming convention
5. L4: Fix or ignore pre-existing butterfly-dl test
6. L5: Semver precision in Cargo.toml
