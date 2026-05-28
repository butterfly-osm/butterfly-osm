# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Run (Docker-First)

Docker is the primary build and deployment method. All builds, tests, and server runs use Docker.

```bash
# Build the Docker image
docker build -t butterfly-route .

# Run the server (Belgium data on port 3001)
docker run -d --name butterfly \
  -p 3001:8080 \
  -v "${PWD}/data/belgium:/data" \
  butterfly-route

# Run with text logging (default is JSON)
docker run -d --name butterfly \
  -p 3001:8080 \
  -v "${PWD}/data/belgium:/data" \
  butterfly-route serve --data-dir /data --port 8080 --log-format text

# Run with debug logging
docker run -d --name butterfly \
  -p 3001:8080 \
  -v "${PWD}/data/belgium:/data" \
  -e RUST_LOG=debug \
  butterfly-route

# View logs
docker logs -f butterfly

# Stop gracefully (SIGTERM → graceful shutdown)
docker stop butterfly

# Health check
curl http://localhost:3001/health

# Prometheus metrics
curl http://localhost:3001/metrics
```

## Local Development (cargo)

For iterating on code without Docker rebuild:

```bash
# Build entire workspace
cargo build --workspace

# Build release
cargo build --workspace --release

# Run all tests
cargo test --workspace

# Run tests for specific package
cargo test -p butterfly-dl
cargo test -p butterfly-route
cargo test -p butterfly-common

# Run single test
cargo test -p butterfly-route test_name

# Lint and format (warnings are enforced as errors via workspace lints)
cargo clippy --workspace --all-targets --all-features
cargo fmt --all -- --check
cargo fmt --all  # auto-fix
```

## Architecture Overview

**Butterfly-OSM** is a high-performance OSM toolkit built in Rust, organized as a Cargo workspace.

### Workspace Structure

```
butterfly-osm/
├── butterfly-common/    # Shared error handling and utilities
├── dl/                  # butterfly-dl: OSM data downloader (production-ready)
└── route/               # butterfly-route: road router + transit engine (production-ready)
```

### butterfly-dl

Memory-efficient OSM downloader (<1GB RAM for any file size). Provides both CLI and library APIs.

- **Core modules**: `src/core/` - Downloader, source routing, streaming
- **CLI**: `src/cli/` - Progress display, argument parsing
- **Verified primitive**: `src/verified.rs` - magic-byte + min-bytes + SHA-256 sidecar checks
- **Region indexes**: `src/regions.rs` + `regions/<region>.toml` - parallel multi-file fetch

Key API: `butterfly_dl::get()`, `butterfly_dl::get_stream()`, `butterfly_dl::get_with_options()` (progress callbacks pass through `DownloadOptions.progress`)

### butterfly-route

High-performance road router **and** multimodal transit engine. Edge-based CCH for exact turn-aware driving/walking/cycling (sections below), plus a full RAPTOR-based public transport stack with multi-feed merging (GTFS + NeTEx-EPIP for STIB), ULTRA transfer-graph preprocessing, and both REST + gRPC Flight interfaces. See the **Transit Subsystem** subsection further down for the transit side.

#### Core Principle

**Edge-based graph is the single source of truth.** All queries (P2P routing, distance matrices, isochrones) use the same EBG-based CCH hierarchy. This ensures:
- Turn restrictions are exact
- Penalties applied identically for all query types
- Routes, matrices, and isochrones are internally consistent

#### Pipeline

| Step | Command | Status | Purpose |
|------|---------|--------|---------|
| 1 | `step1-ingest` | ✅ | Parse PBF → `nodes.sa`, `nodes.si`, `ways.raw`, `relations.raw` |
| 2 | `step2-profile` | ✅ | Per-mode attributes → `way_attrs.*.bin`, `turn_rules.*.bin` |
| 3 | `step3-nbg` | ✅ | Node-Based Graph (build-time intermediate only) |
| 4 | `step4-ebg` | ✅ | Edge-Based Graph → `ebg.nodes`, `ebg.csr`, `ebg.turn_table` |
| 5 | `step5-weights` | ✅ | Per-mode weights → `w.*.u32`, `t.*.u32`, `mask.*.bitset` |
| 6 | `step6-order` | ✅ | ND ordering on EBG |
| 7 | `step7-contract` | ✅ | CCH contraction on EBG |
| 8 | `step8-customize` | ✅ | Apply weights to shortcuts |

**Important:** NBG is a build-time intermediate. The ND ordering and CCH contraction must operate on the EBG because:
- Routing state = directed edge ID
- Turn costs are transitions between edges: `cost(e_in → e_out)`
- NBG ordering cannot be directly mapped to EBG

#### Key Modules

- `src/formats/` - Binary file format readers/writers (CRC-verified)
- `src/model/` - Declarative JSON model system (schema, compile, evaluate, profiling, types)
- `src/ebg/` - Edge-Based Graph construction (THE routing graph)
- `src/nbg/` - Node-Based Graph (intermediate only)
- `src/weights.rs` - Step 5: per-mode weights & masks
- `src/ordering.rs` - Step 6: CCH ordering via nested dissection
- `src/contraction.rs` - Step 7: CCH contraction
- `src/customization.rs` - Step 8: weight customization
- `src/validate/` - Lock condition verification per step
- `src/range/` - PHAST-based range queries for isochrones
- `src/matrix/` - K-lane batched PHAST for bulk distance matrices
- `src/server/` - HTTP query server (Axum + Utoipa)
- `src/bench/` - Benchmark harness (`butterfly-bench` binary)

#### Query Server API

The query server (`butterfly-route serve`) exposes **two transports**:

**REST (Axum, port 3001)** — human-friendly JSON:
- `GET /route` — Point-to-point routing with geometry, turn-by-turn steps with road names, alternatives
- `GET /nearest` — Snap to nearest road segments with distance
- `GET /matrix` — One-to-many distance matrix (duration and/or distance)
- `POST /matrix/bulk` — Bulk many-to-many matrix (K-lane batched PHAST, Arrow streaming)
- `POST /table/stream` — Arrow IPC streaming for large matrices (50k×50k, cooperative cancellation on disconnect)
- `GET /isochrone` — Reachability polygon (GeoJSON/WKB, CCW outer rings, `direction=depart|arrive`)
- `POST /isochrone/bulk` — Parallel batch isochrones (WKB stream)
- `POST /trip` — TSP/trip optimization (nearest-neighbor + 2-opt + or-opt)
- `GET /height` — Elevation lookup from SRTM DEM tiles
- `GET /transit` — Single multimodal transit journey (access CCH + RAPTOR + egress CCH, returns JSON legs)
- `POST /transit/bulk` — Batch multimodal transit (origin-grouped, rayon `par_iter`, up to 100k queries/call)
- `GET /health` — Health check
- Swagger UI at `/swagger-ui`

**gRPC Flight (tonic, port 3002)** — machine-facing Arrow IPC, no transport mixing:
- `matrix` — Distance/duration matrix action
- `route_batch` — Batch P2P routing with WKB polyline
- `isochrone` — Reachability polygons as WKB
- `catchment` — Catchment hulls via DoExchange (store_id → polygon)
- `transit_bulk` — Multimodal batch routing with per-query metadata columns + JSON legs (up to 500k queries/call)
- `edges_batch` — **Unnested per-edge path output** with OSM node ids (flow analytics / traffic assignment / emissions inventory)

**Architectural rule**: REST stays JSON, Flight stays Arrow. No hybrid Arrow-over-HTTP endpoints on the Axum server. `POST /table/stream` is a legacy exception from the pre-Flight era; new bulk Arrow endpoints land on Flight.

#### Performance Optimizations

**PHAST (PHAst Shortest-path Trees)**:
- Upward phase: PQ-based search on UP edges (~5ms)
- Downward phase: Linear rank-order scan on DOWN edges (~90-270ms)
- 5-19x faster than naive Dijkstra

**K-Lane Batching** (K=8):
- Process 8 sources in one downward scan
- Amortizes memory access cost (80-87% cache miss rate)
- 2.24x speedup for matrices, 2.63x for isochrones

**Active-Set Gating** (rPHAST-lite):
- Skip nodes with dist > threshold
- Up to 68% relaxation reduction for bounded queries
- 2.79x speedup when reachable set is <30% of graph

**Current Throughput** (Belgium):
| Query Type | Throughput | Latency |
|------------|------------|---------|
| Isochrone (10-min, car) | ~50/sec | 20ms |
| Matrix 50×50 | ~11/sec | 93ms |
| Matrix 100×100 | ~6/sec | 173ms |

**Isochrone Geometry Pipeline**:
```
PHAST → Near-frontier filter → Sparse tile stamp → Moore boundary trace → Simplify
```
- Near-frontier stamping: only stamp edges with dist >= 60% of threshold
- Skips interior edges that don't affect boundary shape
- Consistency test: 1.2% violation rate (snapped road point semantics)

### Transit Subsystem (RAPTOR + CCH)

butterfly-route ships a **full multimodal transit engine** alongside the road router. The two share the same `ServerState` — road and transit queries run on the same process, the same foot CCH, and the same spatial index.

#### Pipeline shape

```
origin → access CCH 1-to-N (foot/bike/car) → RAPTOR rounds → egress CCH 1-to-N (foot) → response
```

- **Access leg**: `CchQuery::distances_one_to_many` on the selected access mode. Bounded by `max_access_m` radius + `max_access_stops` cap.
- **RAPTOR rounds**: round-based earliest-arrival over the merged `Timetable`. Thread-local `RaptorState` with generation-stamped scratch arrays — O(1) per-query init.
- **Transfer graph**: ULTRA-preprocessed, stop-to-stop, pure foot. Built once at startup via bounded multi-source Dijkstra over the foot CCH. Cached in `transit/transfers.bin` with provenance hashing (CCH fingerprint + feed hash + algo version).
- **Egress leg**: same as access but destination-side, foot mode only.

#### Multi-feed merge + format dispatch

`transit::load_from_disk` dispatches on `FeedConfig.format`:
- `Gtfs` → `gtfs::load_into_builder` (SNCB, De Lijn, TEC)
- `NetexEpip` → `netex_epip::load_into_builder` (STIB — streaming `quick-xml` parser, Lambert-93 → WGS84 via `proj4rs`, per-pattern SSP dedup by quantised coordinate)

Both loaders write into a **shared** `TimetableBuilder` so GTFS and NeTEx feeds merge into one `Timetable` with namespaced stop ids (`sncb:8814001`, `stib:FR:ScheduledStopPoint:...`).

Cross-feed equivalence bridges (same-place different-operator, e.g. SNCB Brussels-Midi ↔ STIB Bruxelles-Midi metro) are injected into the transfer graph with a 30 s fixed cost; same-station child-pair bridges (multi-platform stations) with 60 s. Both run **before** ULTRA dominance restriction, so the restriction drops them cleanly if a shorter real walking transfer dominates.

#### Key modules

- `src/transit/timetable.rs` — `Timetable` + `TimetableBuilder` (SoA `stop_times` split is filed as #126)
- `src/transit/gtfs.rs` — GTFS loader
- `src/transit/netex_epip.rs` — NeTEx-EPIP streaming loader (STIB)
- `src/transit/raptor.rs` — Round-based earliest-arrival with thread-local state
- `src/transit/transfers.rs` — ULTRA transfer graph build (v7 — zero-cost edges preserved, see `ultra_restriction_keeps_zero_cost_cluster` test)
- `src/transit/transfers_cache.rs` — Streaming on-disk cache with provenance
- `src/transit/stop_index.rs` — R-tree spatial index over stops for O(log n) candidate selection
- `src/server/transit_handler.rs` — REST `/transit` + `/transit/bulk` handlers, origin grouping for bulk
- `src/server/flight.rs::do_transit_bulk` — Flight `transit_bulk` action

#### Calendar handling

NeTEx-EPIP publications can be weeks stale. `netex_epip::compute_active_day_types` tries today's date first; if the active set is empty (every period's `FromDate..ToDate` in the past), it remaps today to **the same weekday in the latest published period** so Tuesday-today maps to Tuesday-in-window. Preserves weekday/weekend semantics.

GTFS calendar is applied normally via `ServiceFilter`.

#### Performance (Belgium, 4 feeds merged)

| Query | Metric |
|---|---|
| Single `/transit` warm | 35 ms p50 |
| `/transit/bulk` 20 same-origin | 150 ms (7× vs serial) |
| `/transit/bulk` 1000 varied | 311 q/s sustained |
| Transfer graph | 66 512 stops, 668 k edges |

#### Not in transit yet

- Real-time (GTFS-RT in `realtime.rs` is plumbed but the statistical p50/p90 path in #122/#123 is deferred)
- RAPTOR SoA/SIMD/delta-encoded stop_times (#126/#127/#128)
- Source-batched `queries` shape for edges_batch / transit_bulk (MVP ships the flat shape)

### Binary File Formats

All formats use:
- Magic number headers for type identification
- CRC64 checksums (body + file)
- Fixed-size records for memory-mapped access
- Little-endian encoding

Each step produces a `stepN.lock.json` with SHA-256 checksums for reproducibility.

## Implementation Plan

Planning lives in **GitHub issues**. Open tickets are the canonical list of in-flight and upcoming work — no `todo_*.md` sidekick files. The deleted historical ones (`todo_immediate.md`, `todo_overall.md`) were made obsolete by P/Q/R sprints landing and by the transit subsystem shipping; their useful content was either architectural invariants (merged into this file) or competitive analysis (merged into `competitive_landscape.md`).

Pipeline / architecture specifics are in the sections above. Pending work items:

```
gh issue list --state open
```

Tickets are grouped informally by area:
- **Transit perf:** #126 (SoA stop_times), #127 (SIMD earliest_trip), #128 (trip-table delta compression)
- **Transit data:** #122 (GTFS-RT archive), #123 (statistical p50/p90 synthesis)
- **Infrastructure:** #100 (consolidate HTTP into butterfly-dl)

## Testing

**Belgium is the ONLY test dataset. No other countries.**

- File: `belgium.pbf` from Geofabrik
- ~1.9M NBG nodes, ~4M edges → ~5M EBG nodes
- All tests, benchmarks, and validation MUST run on Belgium
- Do NOT create test data for Monaco, Luxembourg, or any other region
- Run the full pipeline on Belgium before considering any step complete

## Code Quality Requirements

**ABSOLUTE REQUIREMENTS — NO EXCEPTIONS:**

1. **No placeholders** — Every function must be fully implemented
2. **No code shortcuts** — No "TODO: implement later", no stub functions
3. **No sloppiness** — Code must be correct, not "good enough"
4. **Prove it works** — Run on belgium.pbf and verify lock conditions pass
5. **No assumptions** — If uncertain, investigate; don't guess
6. **Never stop if you have a backlog** — In `/loop` dynamic mode or any
   autonomous task, do NOT schedule long waits while open tickets,
   pending PR reviews, or unstarted follow-up work exist. Pick up the
   next backlog item and start it. Scheduling fallback waits is for
   truly idle external blockers (CI, deploys, remote queues) — not for
   "I'm tired" or "I've done enough today". The user is explicit:
   *"I want this done better than anyone else"*.
7. **Never call a perf gap "structural" without asking codex** —
   Dismissing residual performance gaps as irreducible (e.g. "edge-based
   CCH has 2.5× more states than node-based") is laziness disguised as
   analysis. Before claiming a gap is structural, invoke codex (no
   timeouts, no leading) with full context and let it find the wins.

**If code cannot be completed correctly, stop and ask rather than writing incomplete code.**

## Development Principles

**XP Pair Programming Rules**:
- Test-first: Write failing test, then implement
- KISS: Always choose minimal abstraction
- Atomic commits: One logical change per commit
- Conventional Commits: `feat(module): ...`, `fix(module): ...`

**Key constraints**:
- Memory-efficient streaming (fixed-size buffers)
- Deterministic outputs (byte-for-byte reproducible)
- Lock conditions must pass before proceeding to next step
- **One graph, one hierarchy, one query engine** — no separate backends for different query types

## AI Code Review Process

**Goal: Beat OSRM in every respect.** Profile relentlessly and never assume.

### When to Consult AI Reviewers

Always consult with Gemini and Codex **before making drastic changes** to:
- Algorithm selection or implementation
- Data structure changes
- Performance-critical code paths
- CCH/CH search semantics

### How to Use AI Reviewers

1. **Check availability**:
   ```bash
   which gemini && which codex
   ```

2. **Run in parallel** (never sequential):
   ```bash
   timeout 300 gemini -m gemini-2.5-pro -p "prompt" &
   timeout 300 codex -q "prompt" &
   wait
   ```

3. **Structure your review request**:
   - **Explain the problem fully** — context, constraints, current behavior
   - **Do NOT lead** — don't suggest solutions, let them find issues
   - **Provide ALL relevant file paths** — even remotely involved files
   - **Request focus areas**: correctness, efficiency (CPU, RAM, disk), smart algorithms first, parallelism second

4. **After review**:
   - Reviewers file GitHub issues for findings (`gh issue create`)
   - Implement fixes based on reviewer consensus
   - If reviewers disagree or you're stuck, **keep looping and iterating**

### Review Request Template

```
Problem: [Describe what you're trying to achieve]

Current behavior: [What happens now, including benchmark numbers]

Expected behavior: [Target metrics, e.g., "50×50 matrix < 100ms"]

Relevant files:
- route/src/matrix/bucket_ch.rs
- route/src/server/query.rs
- [... all potentially involved files]

Please review for:
1. Correctness (especially directed graph semantics)
2. Algorithmic efficiency (smart algorithms > parallelism)
3. Memory efficiency (allocation patterns, cache locality)
4. CPU efficiency (branch prediction, vectorization opportunities)

Do not assume anything works. Prove it with analysis.
```

---

## Algorithm Strategy (Empirically Validated)

**State-of-the-art routing engine architecture:**

### 1. PHAST (Parallel Hierarchical Approximate Shortest-path Trees)
**Use for:**
- Exact distance fields (one-to-ALL)
- Isochrones (need all reachable nodes)
- Batched throughput (K-lane amortization)

**Why:** PHAST computes ALL distances in one linear scan. For isochrones/reachability, this is optimal.

### 2. Bucket Many-to-Many CH
**Use for:**
- Sparse matrices (specific source-target pairs)
- Small N×M queries (N×M ≤ 10,000)
- Low-latency table API

**Why:** Only explores paths to REQUESTED targets. For 50×50 matrix, explores ~5% of graph vs PHAST's 100%.

**Critical for directed graphs:**
```
d(s → t) = min over m: d(s → m) + d(m → t)

- Source phase: forward UP search → d(s → m)
- Target phase: REVERSE search (DownReverseAdj) → d(m → t)

WARNING: d(t → m) ≠ d(m → t) in directed graphs!
```

### 3. Rank-Aligned CCH
**Foundation for both algorithms:**
- Shared topology (UP/DOWN edge structure)
- Identical cost semantics across all query types
- Internal consistency (routes = matrices = isochrones)
- Cache-friendly memory access (node_id == rank)

### Algorithm Selection Logic

```
if query_type == isochrone:
    use PHAST (need all reachable nodes)
elif n_sources * n_targets <= 10_000:
    use Bucket M2M (sparse, low latency)
else:
    use K-lane batched PHAST + Arrow streaming (throughput)
```

---

## Key File Paths by Component

### CCH Core
- `route/src/formats/cch_topo.rs` — CCH topology (UP/DOWN edges)
- `route/src/formats/cch_weights.rs` — Customized edge weights
- `route/src/contraction.rs` — CCH contraction
- `route/src/customization.rs` — Weight customization

### Query Engine
- `route/src/server/query.rs` — Bidirectional P2P search
- `route/src/server/state.rs` — Server state, DownReverseAdj
- `route/src/server/api.rs` — HTTP endpoints (router + handler modules)

### PHAST / Isochrones
- `route/src/range/phast.rs` — Single-source PHAST
- `route/src/range/batched_isochrone.rs` — K-lane batched PHAST
- `route/src/range/frontier.rs` — Frontier extraction
- `route/src/range/contour.rs` — Polygon generation

### Matrix / Many-to-Many
- `route/src/matrix/bucket_ch.rs` — Bucket M2M algorithm
- `route/src/matrix/batched_phast.rs` — K-lane PHAST for matrices
- `route/src/matrix/arrow_stream.rs` — Arrow IPC streaming

### Benchmarking
- `route/src/bench/main.rs` — Benchmark harness (`butterfly-bench` binary)
- `bench/` (top-level) — regression suite + competitor harnesses (OSRM, Valhalla); orchestrates runs against the live server

### Planning Documents
- GitHub issues (`gh issue list`) — canonical list of open/closed work
- `competitive_landscape.md` — competitor analysis, feature matrix, and gaps
- `CHANGELOG.md` — release notes

---

## Strategic Status (snapshot)

**All Core Features Complete** ✅

- ✅ Exact turn-aware single truth model (edge-based CCH)
- ✅ Matrices: **1.8x FASTER than OSRM at 10k+** scale
- ✅ Trust Package (routes): OSRM parity 0.98 correlation
- ✅ Bulk-First APIs with progress headers
- ✅ Isochrone geometry: CCW polygons, 5-decimal precision, ring closure
- ✅ Road names in turn-by-turn instructions (754K named roads loaded from `ways.raw`)
- ✅ Arrow streaming with cooperative cancellation on client disconnect
- ✅ Reverse isochrones (`direction=arrive`)
- ✅ TSP/trip optimization (`POST /trip`)
- ✅ Elevation/DEM integration (`GET /height`)
- ✅ Traffic-aware routing via CCH recustomization (#84)

### Traffic recustomization (#84)

Step 8 supports an optional `--traffic <profile.traffic.json>` flag that
applies per-density-class speed factors to time weights. By default it emits
a separate `cch.w.<mode>_<variant>.u32` file and the server registers the
result as a synthetic mode `<base>_<variant>` (e.g. `car_rush_hour`). With
`--bake-as-base` the customised weights overwrite the BASE `cch.w.<mode>.u32`
so `?mode=<mode>` returns the friction profile directly — used to make
"realistic" the implicit default car mode without exposing a second name.

#392 (2026-05-27): Belgium ships ONE car friction profile baked into base
(`?mode=car` = realistic) plus ONE variant for peak congestion
(`?mode=car_rush_hour`). Earlier freeflow + offpeak variants were dropped:
freeflow became identical to the post-#390 legal-limit base (so the
realistic-baked-as-base swap made it doubly redundant), and offpeak
overlapped semantically with realistic.

- **Density classes** (5 buckets: urban_high, urban_medium, urban_low,
  suburban, rural) are assigned per way during step 2 by an OSM-tag
  classifier (`route/src/density.rs`) and stored in `way_attrs.<mode>.bin`
  v2. The classifier is deterministic, no spatial pass, O(n_ways).
  `--density-classifier cdis-parquet` is reserved as a plug-in seam for the
  proprietary Sirius CDIS sector data; not implemented in this repo.
- **Profiles** live in `traffic/*.traffic.json`; ship 2 samples
  (`car_realistic` baked into base car, `rush_hour` as variant). Strict
  schema validation: all 5 density keys required, factors in `[0.1, 1.5]`.
- **Wall time** (Belgium, car, ≈5M EBG nodes / ≈34M shortcuts): ~35-40 s.
  Bottom-up alone is 0.5 s; the rest is parallel triangle relaxation,
  which is **correctness-critical** — skipping it produces over-estimated
  paths (Brussels-Antwerp went 5583 s / 77 km without relax vs the
  correct 1947 s / 45 km on legal-limit base). The `--skip-triangle-relax`
  flag is hidden, dev-only.
- **Smoke** (post-#392, Brussels → Antwerp HTTP /route): `car` (realistic
  baked into base) = 7120 s / 58.1 km; `car_rush_hour` = 7482 s / 58.1 km.
  Realistic vs legal-limit baseline: +26% slower (urban friction). The
  legal-limit weights are no longer served — base `cch.w.car.u32` now
  always carries the realistic profile after #392.

**Production Hardening (H-Sprint) Complete** ✅

- ✅ Upgraded all dependencies (axum 0.8, tower-http 0.6, rand 0.9, geo 0.29, arrow 54, utoipa 5)
- ✅ Structured logging with `tracing` (text/JSON, `--log-format` flag)
- ✅ Graceful shutdown (SIGINT/SIGTERM)
- ✅ Request timeouts (120s API, 600s streaming)
- ✅ Response compression (gzip + brotli)
- ✅ Input validation (coordinate bounds, time_s 1-7200, number max 100)
- ✅ Prometheus metrics (`GET /metrics`)
- ✅ Enhanced health endpoint (uptime, node/edge counts)
- ✅ Panic recovery (`CatchPanicLayer`)
- ✅ Dockerfile (multi-stage, `debian:bookworm-slim`)
- ✅ Workspace lints: warnings enforced as errors (`[workspace.lints]`)
- ✅ ~300+ clippy lints fixed across ~50 files

**Feature Parity (P-Sprint) Complete** ✅

- ✅ P1: Exclude toll/ferry/motorway (`exclude=toll,ferry,motorway`)
- ✅ P2: Multiple isochrone contours (`contours=300,600,1200`)
- ✅ P3: Isodistance (`distance_m=5000`)
- ✅ P4: Per-edge annotations (`annotations=speed,duration,distance,nodes`)
- ✅ P6: Avoid polygon areas (`avoid_polygons=[[lon,lat],...]`) — R-tree + sparse CCH recustomization
- ✅ P7: Bearing hints (`bearings=angle,range`) — OSRM-compatible format

**Remaining deferred items:** Truck profile (P5), two-resolution isochrone mask (D8)

---

## Benchmark Reference (Belgium, 2026-02-01)

**Fair HTTP Comparison (same methodology, single requests):**
| Size | OSRM CH | Butterfly | Ratio | Notes |
|------|---------|-----------|-------|-------|
| 1000×1000 | 0.5s | 1.5s | 3.0x | HTTP overhead dominates |
| 2000×2000 | 1.5s | 3.2s | 2.1x | Gap closing |
| 3000×3000 | 3.1s | 5.3s | 1.7x | Gap closing |
| 5000×5000 | 8.0s | 11.1s | **1.38x** | Near convergence |
| 10000×10000 | ~32s | ~44s | **~1.4x** | Extrapolated |

**Arrow Streaming (POST /table/stream) - Large Scale:**
| Size | Butterfly | OSRM | Winner |
|------|-----------|------|--------|
| 10k×10k (100M) | **24s** | 33s | **Butterfly 28% faster** |
| 50k×50k (2.5B) | **9.5 min** | ❌ crashes | **Butterfly only** |

- Throughput: **4.4M distances/sec** sustained
- RAM overhead: Only **2.4 GB** above baseline (tile-by-tile streaming)
- OSRM cannot handle 50k×50k (URL length limits, no streaming)

**Key finding:** At large scale, Butterfly is only **1.4x slower** than OSRM despite:
- Edge-based CCH (2.5x more nodes than OSRM's node-based CH)
- Exact turn handling (OSRM ignores turn restrictions in matrix)

The gap closes at scale because fixed overhead (HTTP, coordination) is amortized.

**Matrix Performance (2026-05-27, Belgium, post-#395, HTTP wall, 20-core):**
| Size | OSRM CH (HTTP) | Butterfly | Ratio |
|------|---------|-----------|-------|
| 10×10 | **3.1ms** | 20.5ms | **0.15× (OSRM faster)** |
| 25×25 | 7.3ms | **11.7ms** | 0.62× (closing — was 0.17× pre-#395) |
| 50×50 | 14.4ms | **15.2ms** | **0.95× (essentially tied)** |
| 100×100 | 31.7ms | **23.0ms** | **1.38× FASTER** |
| 200×200 | URL-limit | **42ms** | OSRM can't issue via GET |
| 500×500 | URL-limit | **105ms** | — |
| 1000×1000 | 684ms (batched) | **260ms** | ~**2.7× FASTER** |
| 2000×2000 | 1.5s | **840ms** | ~**1.79× FASTER** |
| 5000×5000 | 8.0s | **5.5s** | **1.45× FASTER** |
| 10000×10000 | 32.9s | **18.3s** | **1.8× FASTER** |

**Small-N progress (#395, 2026-05-27)**: The /table handler was
hand-dispatching `use_parallel = cells >= 2500` and calling
`table_bucket_full_flat` directly for small N — that function did a
fresh `SearchState::new(n_nodes)` per call (~60 MB malloc on Belgium)
and dominated 10×10 / 25×25 latency. Fix: always delegate to
`table_bucket_parallel` / `_parallel_len_along_time`, which already had
the correct internal dispatch routing small-N to a pooled thread-local
`BucketM2MEngine`. Added the same fast-path delegation inside the
2-channel parallel wrapper plus a `SEQ_STATE_LAT` /
`SEQ_BUCKETS_LAT` thread-local pool for the sequential 2-channel path.
Result: 25×25 dropped 49 → 12 ms (4× faster), 10×10 dropped 30 → 20 ms
(33% faster). 100×100+ unchanged (already went through the
parallel path).

**Remaining 10×10 gap (6× slower)**: per-Dijkstra cost on edge-based
CCH (5 M EBG nodes vs OSRM's ~1.9 M NBG nodes) means each forward /
backward walk visits more states. OSRM additionally uses
stall-on-demand to prune ~40 % of visits. /route P2P p50 = 3.7 ms; a
10×10 sequential bucket-M2M = 20 Dijkstras × ~1 ms each. To match
OSRM we need stall-on-demand inside the bucket-M2M Dijkstras
(estimated 30-40 % visit reduction → 12-14 ms). Tracked as #396.

**Key insight:** Butterfly BEATS OSRM across the useful-N range (50–10000). Small sizes (10–25) still lose to OSRM's sequential shape because rayon thread-dispatch overhead isn't amortised over 100–625 cells — partial mitigation in 8eb4799 (≤100-cell fast path, ~14% gain at 10×10), residual gap is graph-architectural. 10k×10k bench-only number was previously stuck at the DRAM-bandwidth floor (~33 s monolithic) — fixed in #190 by L3-aware source-tiling inside `table_bucket_parallel`: when the single-pass `PrefixSumBuckets` working set would blow shared L3, the source dimension is split into ~2500-source tiles (sized via runtime cache-topology detection, see `route/src/matrix/tile_geometry.rs`) so each backward sweep stays L3-resident. 10k×10k drops 25.6 s → 18.3 s on the dev host (20-core, 30 MiB L3, single NUMA node). Production `/table/stream` was already tiled at 1000×1000 by `server/table.rs` and is unaffected.

**Optimizations Implemented:**
| Optimization | Effect | Status |
|--------------|--------|--------|
| Flat reverse adjacency (embedded weights) | Eliminates indirection | ✅ |
| 4-ary heap with decrease-key | 0% stale pops | ✅ |
| Version-stamped distances | O(1) search init | ✅ |
| O(1) prefix-sum bucket lookup | -7% time | ✅ |
| Bound-aware join pruning | -41% joins, -10% time | ✅ |
| SoA bucket layout | -24% time | ✅ |
| Thread-local PHAST state | O(1) per-query init | ✅ |
| Thread-local bucket M2M state (parallel fwd+bwd) | 6x at 100×100, 5.5x at 1000×1000 | ✅ |
| Block-gated downward scan (C1) | 18x isochrone speedup | ✅ |
| L3-aware source tiling (#190) | 10k×10k 25.6s → 18.3s (28% faster) | ✅ |
| Software prefetch on matrix writes (#190) | Gated on matrix ≥ 8 MiB (avoids small-N regression) | ✅ |

**Combined improvement:** 51s → 32.4s (algorithm time) = **36% faster**, HTTP comparison: 1.4x slower than OSRM at scale

**Algorithm Selection:**
- **Bucket M2M**: for `/table` (sparse S×T matrices)
- **PHAST**: for `/isochrone` (need full distance field)

**Isochrone Performance (30-min threshold, after all optimizations):**
| Endpoint | Throughput | Latency |
|----------|------------|---------|
| `/isochrone` (JSON) | 815/sec | 5ms p50 |
| `/isochrone/wkb` (binary) | 814/sec | 5ms p50, 55% smaller |
| `/isochrone/bulk` (batch) | **1526 iso/sec** | - |

**Improvements:**
- Block-gated PHAST: 90ms → 5ms p50 (**18x faster**)
- Bulk endpoint: 1.9x throughput over individual requests

**Thread Scaling (Matrix 1000×1000):**
- 4 threads: 3.2x speedup (80% efficiency)
- 8 threads: 4.1x speedup (51% efficiency)
- Beyond 8: no improvement (memory bandwidth limited)

**Run benchmarks:**
```bash
# OSRM (must be running on port 5050)
python3 scripts/osrm_matrix_bench.py

# Butterfly
./target/release/butterfly-bench bucket-m2m --data-dir ./data/belgium --sizes 10000 --parallel
```

---

## Profiling Results (2026-02-01)

### Matrix 10k×10k - Source-Block Fix ✅

**Before fix (forward repeated 10x):**
- 25.3s, 3.96M distances/sec
- `forward_fill_buckets_flat`: 92% CPU

**After fix (forward computed once per source block):**
- **16.2s, 6.1M distances/sec**
- **1.56x speedup**

**Fix:** New API in `bucket_ch.rs`:
- `forward_build_buckets()` - Forward phase only
- `backward_join_with_buckets()` - Backward with prebuilt buckets

### Isochrones 100K - Thread-Local PHAST ✅

**Before fix (9.6MB allocation per query):**
- 1370/sec, 21.1ms avg latency
- 68-71% cache miss rate

**After fix (thread-local state + generation stamping):**
- **1471/sec, 19.5ms avg latency**
- **1.07x speedup**

**Remaining bottleneck:** Downward scan still iterates 2.4M nodes

### Optimization Summary

| Fix | Speedup | Status |
|-----|---------|--------|
| A1: Source-block outer loop (matrix) | **1.56x** | ✅ Done |
| B1: Thread-local PHAST (isochrones) | **1.07x** | ✅ Done |
| C1: Block-gated downward | TBD | Pending |
| A2: Bucket structure optimization | TBD | Pending |

---

## Benchmark Comparison Policy

**ALWAYS compare to external baselines when benchmarking on Belgium:**

| API Endpoint | Compare Against | How to Run |
|--------------|----------------|------------|
| `/table` (distance matrix) | **OSRM CH** (docker, port 5050) | `python3 scripts/osrm_matrix_bench.py` |
| `/isochrone` | **Valhalla** | See below |

### OSRM Setup (for /table comparison)
```bash
# One-time setup
docker pull osrm/osrm-backend
docker run -t -v "${PWD}/data:/data" osrm/osrm-backend osrm-extract -p /opt/car.lua /data/belgium.osm.pbf
docker run -t -v "${PWD}/data:/data" osrm/osrm-backend osrm-partition /data/belgium.osrm
docker run -t -v "${PWD}/data:/data" osrm/osrm-backend osrm-customize /data/belgium.osrm

# Run (CH profile)
docker run -t -i -p 5050:5000 -v "${PWD}/data:/data" osrm/osrm-backend osrm-routed --algorithm ch /data/belgium.osrm
```

### Valhalla Setup (for /isochrone comparison)
```bash
# One-time setup with valhalla docker
docker pull ghcr.io/gis-ops/valhalla:latest
mkdir -p valhalla_tiles
# Create valhalla config and build tiles from belgium.pbf
# See: https://github.com/valhalla/valhalla/blob/master/docs/api/isochrone/api-reference.md

# Run Valhalla
docker run -d -p 8002:8002 -v "${PWD}/valhalla_tiles:/custom_files" ghcr.io/gis-ops/valhalla:latest
```

### Comparison Benchmarks
```bash
# /table comparison: Butterfly vs OSRM CH
./target/release/butterfly-bench bucket-m2m --data-dir ./data/belgium --sizes 10,25,50,100

# /isochrone comparison: Butterfly vs Valhalla
./target/release/butterfly-bench pathological-origins --data-dir ./data/belgium --mode car
./target/release/butterfly-bench e2e-isochrone --data-dir ./data/belgium --mode car
```

**Targets:**
- `/table`: Within 2-3x of OSRM CH for same matrix sizes
- `/isochrone`: Faster than Valhalla for equivalent thresholds (Valhalla typically 200-500ms for 30-min)

---

## OSRM Algorithm Analysis (many_to_many_ch.cpp)

**CRITICAL: OSRM uses NO PARALLELISM in core matrix algorithm.**

### Fundamental Architecture Difference

| Aspect | OSRM | Butterfly |
|--------|------|-----------|
| **Graph type** | Node-based | Edge-based (bidirectional edges) |
| **State** | Node ID | Directed edge ID |
| **Turn costs** | Approximated/ignored | Exact (edge→edge transitions) |
| **Graph size** | ~1.9M nodes | ~5M edge-states |
| **CH complexity** | Simpler | ~2.5x more states |

**This matters!** Edge-based CH has inherently more work:
- More nodes to contract (~5M vs ~1.9M)
- More edges in hierarchy
- More bucket items per search

**Goal: Be FASTER than OSRM despite the extra complexity. No excuses.**

### Algorithm Structure
1. **Backward phase FIRST**: Sequential Dijkstra from each target
2. **Collect NodeBuckets**: Store (node, target_idx, dist) in flat vector
3. **Sort buckets** once by node ID
4. **Forward phase**: Sequential Dijkstra from each source
5. **Join via binary search**: `std::equal_range` for O(log n) bucket lookup

### Key Implementation Details

**Heap**: d-ary heap with proper DecreaseKey (NOT lazy reinsert)
```cpp
// OSRM uses boost::heap::d_ary_heap with index storage
heap.Insert(to, to_weight, parent);  // or
heap.DecreaseKey(*toHeapNode);       // proper decrease-key
```

**Stall-on-Demand**: Check OPPOSITE direction edges
```cpp
// In forward search, check backward edges for stalling
for (edge in opposite_direction_edges) {
    if (neighbor_in_heap && neighbor.dist + edge.weight < current.dist) {
        return true;  // stall this node
    }
}
```

**Index Storage**: O(1) "was node visited" lookup
```cpp
// ArrayStorage for overlay nodes (dense)
// UnorderedMapStorage for base nodes (sparse)
Key peek_index(NodeID node) const { return positions[node]; }
```

### Why OSRM is Fast
1. **No parallel overhead** for small matrices
2. **Proper heap** with O(log n) decrease-key, not O(n) lazy duplicates
3. **O(1) visited check** via index storage
4. **Stalling** reduces search space by 20-40%
5. **Binary search** for bucket lookup is cache-efficient

### Remaining Issues to Fix (Priority Order)

1. **75% stale heap entries** - 4x more heap operations than OSRM
   - Don't use positions array (9.6MB, causes cache misses)
   - Instead: 4-ary heap with lazy reinsertion (better cache, fewer ops)
   - Or: reduce duplicates via stricter relax condition

2. **Per-query array allocation** - O(n_nodes) bucket offset build is 15ms
   - Fix: Reuse count/offset buffers across queries in `SearchState`
   - Amortize allocation cost over multiple queries

3. **Binary heap vs 4-ary heap** - Cache behavior matters
   - 4-ary heap: 4 children per node, shallower tree
   - Better cache utilization, fewer memory accesses

### Critical Finding: 75% Stale Heap Entries

Profiling revealed that lazy reinsertion causes massive overhead:
```
pushes=205K, pops=??, stale=75%
```
- For 10×10: ~20K nodes visited, but ~80K heap operations
- OSRM uses decrease-key or strict duplicate prevention
- We push duplicates freely → 4x wasted work

**Solutions (pick one):**
1. **4-ary heap + lazy reinsertion** - Better cache, same semantics
2. **Strict relax** - Only push if `new_dist < best_seen[u]` (already doing this)
3. **Small positions array** - Only for visited nodes (sparse), not all 2.4M

The issue is that even with strict relax, the graph structure causes natural re-relaxations.

---

## Performance Optimization Philosophy

1. **Profile first** — Never optimize without data
2. **Smart algorithms > parallelism** — Right algorithm beats threads
3. **Memory locality > raw speed** — Cache misses dominate
4. **Correctness > performance** — Wrong fast is still wrong
5. **Iterate relentlessly** — Small gains compound

### Optimization Priority Order

1. **Algorithm selection** (e.g., Bucket M2M vs PHAST)
2. **Data structure design** (e.g., flat arena vs per-node vectors)
3. **Memory layout** (e.g., SoA vs AoS, rank-alignment)
4. **Allocation reduction** (e.g., buffer reuse)
5. **Parallelism** (only after above are exhausted)
