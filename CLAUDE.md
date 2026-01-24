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

**Current Throughput** (Belgium):
| Query Type | Throughput | Latency |
|------------|------------|---------|
| Single isochrone (car) | 10.8/sec | 90ms |
| K-lane isochrones | 25.6/sec | 39ms/iso |
| K-lane matrix | 25.8 queries/sec | - |

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
