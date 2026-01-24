# Immediate Roadmap: Correctness → Isochrones → Speed

## Current Status

CCH validated with 75k+ queries across all modes (0 mismatches).
Regression test suite in place (87 edge cases per mode).
Invariant validation complete for all modes (car/bike/foot).

---

## Phase 1: Lock Down Invariants ✅ DONE

### 1.1 `validate-invariants` Command ✅

Fast-fail validation for graph/weight correctness:

- [x] **Non-negative weights**: All edge weights ≥ 0 (required for Dijkstra)
- [x] **No overflow**: Check u32 additions don't wrap (use saturating_add or detect)
- [x] **Deterministic tie-breaking**: When costs equal, pick consistent predecessor (by node_id)
- [x] **CSR structure validity**: Offsets monotonic, targets in range
- [x] **Hierarchy property**: UP edges go to higher rank, DOWN to lower

```bash
butterfly-route validate-invariants \
  --cch-topo cch.car.topo \
  --cch-weights cch.w.car.u32 \
  --order order.car.ebg \
  --mode car
```

### 1.2 Weight Domain Checks ✅

- [x] Weights are in expected range (0 to reasonable max, e.g., 24h in ms)
- [x] No INF weights on edges that should be reachable
- [x] Isolated nodes detected (nodes with all INF outgoing edges)

---

## Phase 2: Exact Isochrone Core (Range Query) ✅ DONE

### 2.1 `range-cch` Command ✅

Bounded Dijkstra in edge-based state space:

- [x] Input: snapped origin, threshold T (milliseconds)
- [x] Output: all settled states with dist ≤ T
- [x] Uses same CCH backend as P2P queries
- [x] Returns: dist map + parent pointers for path reconstruction
- [x] Frontier edge extraction for polygon construction

```bash
butterfly-route range-cch \
  --cch-topo cch.car.topo \
  --cch-weights cch.w.car.u32 \
  --order order.car.ebg \
  --origin-node 12345 \
  --threshold-ms 600000 \
  --mode car
```

### 2.2 Range Query Tests ✅

- [x] **Monotonicity**: reachable(T1) ⊆ reachable(T2) for T1 < T2
- [x] **Equivalence**: dist(target) ≤ T iff target in reachable set
- [x] **Edge frontier**: Frontier edges satisfy inside/outside correctly
- [x] **Consistency with P2P**: For any target in range, dist matches P2P query

All 3 modes (car/bike/foot) pass validation.

---

## Phase 3: Frontier Extraction + Polygonization ✅ DONE

**Critical insight**: CCH frontier edges (160M+) are shortcuts, not real roads.
Must compute frontier on **base graph edges** with actual geometry.

### 3.0 Base Graph Frontier ✅ DONE

- [x] Map PHAST distances (CCH node IDs) → base EBG edge distances
- [x] `extract_frontier_base_edges(dist, T)` → boundary cut points on real edges
- [x] For each base edge (u→v) with cost w: if `dist[u] ≤ T < dist[u] + w`, emit cut point
- [x] Interpolate position along polyline at cut fraction
- [x] Fixed critical units bug: PHAST distances in deciseconds, threshold in milliseconds

Results (after unit fix):
| Mode | Threshold | Frontier | Interior | Total | Unique Cells |
|------|-----------|----------|----------|-------|--------------|
| Car  | 5 min     | 232      | 3599     | 3831  | 923          |
| Bike | 5 min     | 73       | 1081     | 1154  | 424          |
| Foot | 5 min     | 38       | 246      | 284   | 137          |

### 3.1 Frontier Tests ✅

- [x] Every cut point lies on edge that crosses T (by construction)
- [x] No cut points on edges fully inside or outside (by construction)
- [x] Frontier count is manageable (<500 points after unit fix)

### 3.2 Polygon Construction ✅ DONE

**Problem**: Convex hull bridges across unreachable space (water, parks). Need road-following concave envelope.

**Solution**: Grid fill + marching squares with segment stamping

Pipeline:
1. **Build metric grid** ✅
   - [x] Project to Web Mercator (meters)
   - [x] Compute bounding box with margin (5 cells)
   - [x] Create boolean raster grid

2. **Stamp reachable road segments onto grid** ✅
   - [x] For each base edge: compute reachable sub-segment using PHAST dist + cut fraction
   - [x] Stamp polyline segments onto grid cells using Bresenham's line algorithm
   - [x] Cell is "inside" if touched by reachable road geometry

3. **Morphological closing** (dilate then erode) ✅
   - [x] Asymmetric closing: 3+ dilations, 1 erosion (to connect linear road features)
   - [x] Deterministic, stable across runs

4. **Marching squares contour extraction** ✅
   - [x] Flood fill from corners to identify exterior cells
   - [x] Extract outer boundary from filled boolean grid
   - [x] Proper case handling for all 16 configurations

5. **Simplify polygon** ✅
   - [x] Douglas-Peucker with tolerance (≈ cell size)
   - [x] Valid GeoJSON Polygon output

Grid settings per mode:
- Car: 100m grid, 1 closing iter, 75m simplify
- Bike: 50m grid, 1 closing iter, 50m simplify
- Foot: 25m grid, 1 closing iter, 25m simplify

### 3.3 Polygon Tests ✅ (Basic)

- [x] Output <10KB GeoJSON, <500 vertices
- [x] Concave shape follows road network (not convex)
- [x] All modes work: car, bike, foot
- [x] Valid GeoJSON (parses correctly)

```bash
butterfly-route isochrone \
  --cch-topo cch.bike.topo \
  --cch-weights cch.w.bike.u32 \
  --order order.bike.ebg \
  --filtered-ebg filtered.bike.ebg \
  --ebg-nodes ebg.nodes \
  --nbg-geo nbg.geo \
  --base-weights w.bike.u32 \
  --origin-node 2000000 \
  --threshold-ms 300000 \
  --mode bike \
  --output isochrone.geojson
```

Results (Belgium):
| Mode | Threshold | Time   | Vertices | Size   | Dimensions      |
|------|-----------|--------|----------|--------|-----------------|
| Foot | 10 min    | 280ms  | 57       | 1.5 KB | 2.1km × 1.3km   |
| Bike | 10 min    | 270ms  | 141      | 3.4 KB | 8.0km × 5.0km   |
| Bike | 15 min    | 270ms  | 179      | 4.3 KB | 11.5km × 8.1km  |
| Car  | 10 min    | 90ms   | 321      | 7.7 KB | 26.3km × 14.5km |

---

## Phase 4: Performance Optimization ✅ DONE

### 4.1 PHAST Implementation ✅

Replaced naive PQ-based Dijkstra with PHAST (PHAst Shortest-path Trees):
- **Upward phase**: PQ-based, UP edges only (~5ms)
- **Downward phase**: Linear scan in rank order, DOWN edges (~280ms)

Results:
| Mode | Threshold | Old (Naive) | New (PHAST) | Speedup |
|------|-----------|-------------|-------------|---------|
| Car  | 5 min     | 455ms       | 92ms        | 5x      |
| Bike | 5 min     | 3100ms      | 261ms       | 12x     |
| Foot | 10 min    | 5560ms      | 290ms       | 19x     |

All queries now under 300ms - production viable.

### 4.2 Future Optimizations (Not Needed Now)

- [ ] Restricted PHAST (skip unreachable nodes in downward scan)
- [ ] SIMD-accelerated weight lookups
- [ ] Memory-mapped files
- [ ] Reuse allocations across queries

---

## Phase 5: Bulk Performance Optimization (4/5 Milestones Done)

Target: **huge matrices** (100k×100k) and **millions of isochrones**

### Reality Checks

- 100k × 100k dense matrix = 10¹⁰ cells = 40 GB → must tile/stream
- Millions of isochrones → can't do one PHAST scan per origin

---

### Milestone 1: Profiling Infrastructure ✅ DONE

**Goal**: Repeatable benchmarks + flamegraphs to guide optimization

#### 1.1 Benchmark Harness (`bench/` binary) ✅ DONE

- [x] Load Belgium data once
- [x] Run workloads: `phast-only`, `isochrone`, `isochrone-batch`
- [x] Output: p50/p90/p95/p99 time via hdrhistogram
- [x] Counters per run:
  - Upward PQ pushes/pops, relaxations, settled
  - Downward relaxations, improvements
  - Frontier segments
  - Grid cells filled
  - Contour vertices

**Baseline Results (Belgium, 10-min isochrone, 50 random origins)**:

| Mode | PHAST p50 | Relaxations | Throughput |
|------|-----------|-------------|------------|
| Car | 89ms | 18.2M | 10.8/sec |
| Bike | 266ms | 71.8M | 3.7/sec |

**Time breakdown** (bike):
- PHAST: 266ms (98.3%)
- Frontier: 2.7ms (1.0%)
- Contour: 1.1ms (0.4%)

#### 1.2 Flamegraph / Perf Setup ✅ DONE

- [x] Wall-clock flamegraph (`cargo flamegraph`)
- [x] `perf stat` (cache misses, branch misses, LLC-load-misses)
- [x] Annotated hot spots in downward scan

**perf stat results** (bike, 20 queries):
- Cache miss rate: **80-87%** - extremely memory-bound
- LLC miss rate: 27-42% - main memory accesses
- Branch misses: 0.04-0.4% - excellent prediction
- IPC: 2.8-3.2 - CPU not starved but memory-limited

**Annotated hot spots** (downward scan inner loop):
| Instruction | % | Operation |
|-------------|---|-----------|
| Load weight | **23.76%** | `mov (%r8,%r14,4),%ebx` |
| Load offset | **11.83%** | `mov (%r9,%rbp,8),%r13` |
| Compare dist | **9.31%** | `cmp (%r9,%rbp,4),%ebx` |
| Load value | **6.59%** | `mov -0x4(%r9,%r14,4),%ebp` |
| **Total memory ops** | **~58%** | Inner loop is memory-bound |

#### 1.3 Baseline Report ✅ DONE

| Workload | p50 | p99 | Relaxations | Key Insight |
|----------|-----|-----|-------------|-------------|
| Bike PHAST | 266ms | 274ms | 71.8M | Memory-bound scan |
| Car PHAST | 89ms | 94ms | 18.2M | Smaller graph |
| Bike isochrone | 270ms | 300ms | - | PHAST dominates |

**Optimization targets** (in priority order):
1. ~~K-lane batching to amortize memory access cost~~ ✅ Done (2.24x)
2. **Blocked relaxation** to fix 80-87% cache miss rate ← CRITICAL
3. SoA data layout for dist arrays
4. SIMD vectorization (only after cache is fixed)

**Root cause analysis**:
- Downward scan is 98% of runtime
- Cache miss rate: 80-87%
- Problem: random writes to `dist[v]` for each edge relaxation
- Solution: block updates by destination rank to improve locality

---

### Milestone 2: Arrow Streaming for Matrix Tiles ✅ PARTIAL

**Goal**: Streaming output for long-running bulk queries

- [x] Arrow IPC module implemented (`matrix/arrow_stream.rs`)
- [x] Tiled block schema:
  ```
  src_block_start: u32
  dst_block_start: u32
  src_block_len: u16
  dst_block_len: u16
  durations_ms: Binary (packed row-major u32)
  ```
- [x] `ArrowMatrixWriter` for streaming writes
- [x] Unit tests for tile creation and Arrow serialization
- [x] HTTP endpoint `POST /matrix/bulk` with JSON and Arrow format support
- [ ] Bounded channel between compute and writer (for streaming large matrices)
- [ ] Cancellation token on client disconnect

---

### Milestone 3: K-Lane Batched PHAST for Matrices ✅ DONE

**Goal**: Single most important algorithmic optimization

**Implementation** (`matrix/batched_phast.rs`):
- [x] K-lane downward scan (K=8 lanes)
- [x] K sources processed in one downward pass
- [x] Upward: K sequential searches (per batch)
- [x] Downward: iterate ranks once, update `dist[K]` per node
- [x] Reduces `O(N × #sources)` → `O(N × #sources/K)`

**Benchmark Results** (Belgium, car mode):
```
64 sources × 1000 targets:
  Single-source: 5.57s (11.5 queries/sec)
  K-lane batched: 2.48s (25.8 queries/sec)
  Speedup: 2.24x
  Correctness: ✅ All 64,000 distances match
```

**Analysis**:
- 2.24x speedup vs theoretical 8x (K lanes)
- Speedup limited by sequential upward phases (95ms of 2.48s)
- Downward phase benefits fully from K-lane amortization
- Next step: parallelize upward phases with rayon

Tasks:
- [x] Implement K-lane downward scan
- [x] Benchmark correctness verification
- [x] HTTP endpoint `POST /matrix/bulk` with Arrow output
- [ ] Parallelize upward phases with rayon (potential 4x further speedup)

---

### Milestone 4: Batched Isochrones ✅ DONE

**Two modes**:

1. **K-lane PHAST + per-origin frontier/raster**
   - Same K-lane scan computes K distance fields
   - Then per origin: frontier → raster → contour
   - Good for many origins, similar thresholds

2. **Active-set gating (rPHAST-lite)** ✅ DONE
   - Skip `dist[v] > T` nodes in downward scan
   - Maintain `active` bitset for nodes with finite dist ≤ T
   - Skip nodes not active
   - Good for single-origin latency

**Active-Set Gating Results** (Belgium):

| Mode | Threshold | Naive | Active-Set | Speedup | Relaxation Drop |
|------|-----------|-------|------------|---------|-----------------|
| Car  | 30 sec    | 85ms  | 33ms       | 2.57x   | 68.3%           |
| Bike | 2 min     | 269ms | 97ms       | 2.79x   | 68.8%           |
| Bike | 5 min     | 267ms | 250ms      | 1.07x   | 15.9%           |
| Car  | 3 min     | 88ms  | 100ms      | 0.88x   | 0%              |

**Insight**: Active-set gating is effective when the reachable set is small (<30% of graph).
For large thresholds where most of the graph is reachable, the bitset overhead negates benefits.

**K-Lane Batched Isochrone Results** (Belgium, car, 2-min threshold):

| Method | Time (32 origins) | Throughput | Speedup |
|--------|-------------------|------------|---------|
| Single-source | 3.29s | 9.7 iso/s | baseline |
| K-lane (K=8) | 1.25s | 25.6 iso/s | 2.63x |

✅ Vertex counts match exactly (100% correctness)

Tasks:
- [x] Implement active-set gating in PHAST (`query_active_set()` in phast.rs)
- [x] Measure relaxations drop (up to 68% for bounded queries)
- [x] Implement K-lane isochrone batch mode (`range/batched_isochrone.rs`)
- [ ] Reusable grid buffer per worker (deferred - contour is only 1-2% of time)

---

### Milestone 5: Cache-Friendly Edge Processing ✅ DONE (partial)

**Problem**: 80-87% cache miss rate in downward scan due to random writes to `dist[v]`

**Original hypothesis**: Blocked relaxation (buffer writes, flush in batches) would improve cache efficiency.

**Finding**: Blocked relaxation doesn't work for PHAST because:
- PHAST requires strict source rank ordering (decreasing)
- When processing node u, `dist[u]` must already have all updates from higher-ranked nodes
- Buffering writes breaks this invariant (updates to u may be sitting in buffers)
- Even bucket-based processing by destination block violates global rank order

**Alternative implemented**: Pre-sorted edge processing
- Sort DOWN edges at construction by (source_rank DESC, target ASC)
- Process edges in pre-sorted order
- Maintains PHAST correctness while improving write locality

#### 5.1 Results

| Method | Downward Time | Speedup | Cache Miss Rate |
|--------|---------------|---------|-----------------|
| Baseline | 2161ms | 1.00x | 57-85% |
| Pre-sorted edges | 1816ms | 1.19x | 57-85% |

**Analysis**: Modest 1.19x improvement on downward phase. Cache miss rate still high because:
- Writes are now sequential (sorted by target) ✅
- Reads (`dist[src]`) are still random (different sources each edge) ❌
- Read misses dominate the cache behavior

Tasks:
- [x] Implement pre-sorted edge order at construction
- [x] Benchmark cache miss rate with `perf stat`
- [x] Verify correctness (all distances match)
- [ ] Try SoA layout to improve read locality (Milestone 6)

---

### Milestone 6: SoA Layout + SIMD ← After cache is fixed

**Prerequisites**: Milestone 5 complete (cache miss rate < 50%)

#### 6.1 Structure of Arrays Layout
- [ ] Convert `dist[K][N]` to `dist_lane0[N], dist_lane1[N], ...`
- [ ] Align arrays to cache line boundaries (64 bytes)
- [ ] Benchmark improvement

#### 6.2 AVX2 SIMD Inner Loop
- [ ] Implement SIMD min/saturating_add for K lanes
- [ ] Handle u32::MAX (INF) correctly
- [ ] Benchmark with and without SIMD

**Expected gain**: 1.5-3x after cache is fixed

---

### Milestone 7: Scale Testing

- [ ] Run 10M isochrones offline, validate throughput + memory
- [ ] Run matrix build as distributed tiles
- [ ] Document SLO targets (p95 latency, throughput)

---

## Phase 6: Production Hardening (Deferred)

### 6.1 Snapping

- [ ] Snapper as first-class component
- [ ] Same snapping for route/matrix/isochrone
- [ ] Edge snapping (not just node snapping)

### 6.2 Concurrency

- [ ] Per-request scratch buffers
- [ ] No global mutable state
- [ ] Thread-safe query engine

---

## Completed Phases ✅

- [x] Phase 1: Lock Down Invariants
- [x] Phase 2: Exact Isochrone Core (Range Query)
- [x] Phase 3: Frontier Extraction + Polygonization
- [x] Phase 4: PHAST Performance Optimization

**Current Performance** (Belgium):
| Mode | Threshold | Time   | Vertices | Dimensions      |
|------|-----------|--------|----------|-----------------|
| Foot | 10 min    | 280ms  | 57       | 2.1km × 1.3km   |
| Bike | 10 min    | 270ms  | 141      | 8.0km × 5.0km   |
| Bike | 15 min    | 270ms  | 179      | 11.5km × 8.1km  |
| Car  | 10 min    | 90ms   | 321      | 26.3km × 14.5km |
