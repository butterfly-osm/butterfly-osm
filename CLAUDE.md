# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Test Commands

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

# Lint and format
cargo clippy --workspace --all-targets --all-features -- -D warnings -A clippy::redundant_closure
cargo fmt --all -- --check
cargo fmt --all  # auto-fix

# Build specific tool
cargo build --release -p butterfly-dl
cargo build --release -p butterfly-route
```

## Architecture Overview

**Butterfly-OSM** is a high-performance OSM toolkit built in Rust, organized as a Cargo workspace.

### Workspace Structure

```
butterfly-osm/
├── butterfly-common/        # Shared error handling and utilities
├── tools/
│   ├── butterfly-dl/        # OSM data downloader (production-ready)
│   └── butterfly-route/     # Routing engine (in development)
```

### butterfly-dl

Memory-efficient OSM downloader (<1GB RAM for any file size). Provides both CLI and library APIs.

- **Core modules**: `src/core/` - Downloader, source routing, streaming
- **CLI**: `src/cli/` - Progress display, argument parsing
- **FFI**: `src/ffi.rs` - C-compatible bindings (optional feature `c-bindings`)

Key API: `butterfly_dl::get()`, `butterfly_dl::get_stream()`, `butterfly_dl::get_with_progress()`

### butterfly-route

High-performance routing engine using **edge-based CCH** (Customizable Contraction Hierarchies).

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
- `src/profiles/` - Routing profiles (car, bike, foot)
- `src/ebg/` - Edge-Based Graph construction (THE routing graph)
- `src/nbg/` - Node-Based Graph (intermediate only)
- `src/validate/` - Lock condition verification per step
- `src/range/` - PHAST-based range queries for isochrones
- `src/matrix/` - K-lane batched PHAST for bulk distance matrices
- `src/step9/` - HTTP query server (Axum + Utoipa)
- `src/bench/` - Benchmark harness (`butterfly-bench` binary)

#### Query Server API

The Step 9 query server (`butterfly-route serve`) provides:
- `GET /route` - Point-to-point routing with geometry
- `GET /matrix` - One-to-many distance matrix
- `POST /matrix/bulk` - Bulk many-to-many matrix (K-lane batched PHAST, Arrow streaming)
- `POST /table/stream` - Arrow IPC streaming for large matrices (handles 50k×50k = 2.5B distances)
- `GET /isochrone` - Reachability polygon for time threshold
- `POST /isochrone/batch` - K-lane batched isochrones (up to 8 origins per request)
- `GET /health` - Health check
- Swagger UI at `/swagger-ui`

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

**Current Throughput** (Belgium, after C1 optimization):
| Query Type | Throughput | Latency |
|------------|------------|---------|
| Isochrone (30-min, car) | **815/sec** | **5ms p50** |
| Matrix 50×50 | ~11/sec | 93ms |
| Matrix 100×100 | ~6/sec | 173ms |

### Binary File Formats

All formats use:
- Magic number headers for type identification
- CRC64 checksums (body + file)
- Fixed-size records for memory-mapped access
- Little-endian encoding

Each step produces a `stepN.lock.json` with SHA-256 checksums for reproducibility.

## Implementation Plan

See **[todo_overall.md](todo_overall.md)** for the overall implementation plan, including:
- Step-by-step pipeline specification
- Algorithm details and lock conditions
- Performance targets
- "What NOT to do" constraints

See **[todo_immediate.md](todo_immediate.md)** for immediate bugs and fixes that must be addressed before continuing.

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
   - Reviewers update `todo_overall.md` and `todo_immediate.md` with findings
   - Implement fixes based on reviewer consensus
   - If reviewers disagree or you're stuck, **keep looping and iterating**

### Review Request Template

```
Problem: [Describe what you're trying to achieve]

Current behavior: [What happens now, including benchmark numbers]

Expected behavior: [Target metrics, e.g., "50×50 matrix < 100ms"]

Relevant files:
- tools/butterfly-route/src/matrix/bucket_ch.rs
- tools/butterfly-route/src/step9/query.rs
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
- `tools/butterfly-route/src/formats/cch_topo.rs` — CCH topology (UP/DOWN edges)
- `tools/butterfly-route/src/formats/cch_weights.rs` — Customized edge weights
- `tools/butterfly-route/src/step7.rs` — CCH contraction
- `tools/butterfly-route/src/step8.rs` — Weight customization

### Query Engine
- `tools/butterfly-route/src/step9/query.rs` — Bidirectional P2P search
- `tools/butterfly-route/src/step9/state.rs` — Server state, DownReverseAdj
- `tools/butterfly-route/src/step9/api.rs` — HTTP endpoints

### PHAST / Isochrones
- `tools/butterfly-route/src/range/phast.rs` — Single-source PHAST
- `tools/butterfly-route/src/range/batched_isochrone.rs` — K-lane batched PHAST
- `tools/butterfly-route/src/range/frontier.rs` — Frontier extraction
- `tools/butterfly-route/src/range/contour.rs` — Polygon generation

### Matrix / Many-to-Many
- `tools/butterfly-route/src/matrix/bucket_ch.rs` — Bucket M2M algorithm
- `tools/butterfly-route/src/matrix/batched_phast.rs` — K-lane PHAST for matrices
- `tools/butterfly-route/src/matrix/arrow_stream.rs` — Arrow IPC streaming

### Benchmarking
- `tools/butterfly-route/src/bench/main.rs` — Benchmark harness

### Planning Documents
- `todo_overall.md` — Architecture and roadmap
- `todo_immediate.md` — Current sprint tasks

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

**Small Matrix (HTTP API) - After C1 Optimization:**
| Size | OSRM CH | Butterfly | Gap |
|------|---------|-----------|-----|
| 10×10 | 4.5ms | 26ms | 5.8x |
| 25×25 | 8.7ms | 48ms | 5.5x |
| 50×50 | 18.9ms | 85ms | 4.5x |
| 100×100 | 35ms | 160ms | 4.6x |

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
| Block-gated downward scan (C1) | 18x isochrone speedup | ✅ |

**Combined improvement:** 51s → 32.4s (algorithm time) = **36% faster**, HTTP comparison: 1.4x slower than OSRM at scale

**Algorithm Selection:**
- **Bucket M2M**: for `/table` (sparse S×T matrices)
- **PHAST**: for `/isochrone` (need full distance field)

**Isochrone Performance (30-min threshold, after C1 block-gated PHAST):**
| Metric | Value |
|--------|-------|
| Mean latency | 8.3ms |
| P50 latency | **5ms** |
| P99 latency | 53ms |
| Throughput (8 threads) | **827 queries/sec** |

**Improvement:** 90ms → 5ms p50 latency (**18x faster**) via block-gated downward scan

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
