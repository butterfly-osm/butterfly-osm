# Butterfly-Route: Edge-Based CCH Implementation Plan

## Goal

Build a routing engine with **exact turn-aware isochrones** and **OSRM-class speed** using:
- Edge-based graph (state = directed edge ID)
- Per-mode CCH preprocessing on filtered edge-based graphs
- Exact bounded Dijkstra on the hierarchy for all query types

**Key principle:** One graph, one hierarchy per mode, one query engine. Routes, matrices, and isochrones use identical cost semantics.

---

## Pipeline Status

### All Steps Complete ✅

| Step | Output | Description | Status |
|------|--------|-------------|--------|
| 1 | `nodes.sa`, `nodes.si`, `ways.raw`, `relations.raw` | PBF ingest | ✅ |
| 2 | `way_attrs.*.bin`, `turn_rules.*.bin` | Per-mode profiling (car/bike/foot) | ✅ |
| 3 | `nbg.csr`, `nbg.geo`, `nbg.node_map` | Node-Based Graph (intermediate) | ✅ |
| 4 | `ebg.nodes`, `ebg.csr`, `ebg.turn_table` | Edge-Based Graph (THE routing graph) | ✅ |
| 5 | `w.*.u32`, `t.*.u32`, `mask.*.bitset`, `filtered.*.ebg` | Per-mode weights, masks, filtered EBGs | ✅ |
| 6 | `order.{mode}.ebg` | Per-mode CCH ordering on filtered EBG | ✅ |
| 7 | `cch.{mode}.topo` | Per-mode CCH contraction (shortcuts topology) | ✅ |
| 8 | `cch.w.{mode}.u32` | Per-mode customized weights | ✅ |
| 9 | HTTP server | Query server with /route, /matrix, /isochrone | ✅ |

---

## Architecture: Per-Mode Filtered CCH

Each transport mode has its own CCH built on a **filtered subgraph** containing only mode-accessible nodes:

```
Original EBG (5M nodes)
    ↓
FilteredEbg (per mode)
    - Car:  2.4M nodes (49%)
    - Bike: 4.8M nodes (95%)
    - Foot: 4.9M nodes (98%)
    ↓
Per-mode CCH ordering → order.{mode}.ebg
    ↓
Per-mode CCH topology → cch.{mode}.topo
    ↓
Per-mode weights → cch.w.{mode}.u32
```

**Why per-mode CCH?** A shared CCH on all nodes fails when some modes can't access certain nodes—those nodes become orphaned in the hierarchy (no finite paths up/down).

---

## Step 9: Query Engine ✅

### Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /route` | P2P routing with geometry |
| `GET /matrix` | One-to-many distance matrix |
| `GET /isochrone` | Reachable area within time limit |
| `GET /health` | Server health check |
| `GET /swagger-ui/` | OpenAPI documentation |

### Components

- **Spatial Index**: R-tree on EBG nodes for snapping (rstar crate)
- **CCH Query**: Bidirectional Dijkstra on UP/DOWN edges
- **Shortcut Unpacking**: Recursive expansion to original EBG edges
- **Geometry Reconstruction**: Map EBG path to NBG coordinates

### Query Flow

1. Snap coordinates → original EBG node ID
2. Convert to filtered ID via `original_to_filtered`
3. Run CCH query in filtered space
4. Convert results to original IDs for geometry
5. Build GeoJSON response

---

## Performance (Belgium)

### Build Times

| Step | Time | Output Size |
|------|------|-------------|
| Step 6 (ordering) | ~3s per mode | 9-19 MB |
| Step 7 (contraction) | ~23s per mode | 200-350 MB |
| Step 8 (customization) | ~5s per mode | 180-230 MB |

### Query Performance

| Operation | Measured |
|-----------|----------|
| Server startup | ~25s (loading all data) |
| P2P query | < 10ms |
| **Matrix (Bucket M2M, HTTP)** | |
| - 10×10 | 30ms (OSRM: 6ms = 5x) |
| - 100×100 | 171ms (OSRM: 30ms = 5.7x) |
| - 200×200 | 300ms (OSRM: 61ms = 4.9x) |
| - 1000×1000 | ~1.5s (OSRM: 0.5s = 3x) |
| - 5000×5000 | ~11s (OSRM: 8s = 1.38x) |

*Note: Gap closes at scale. Edge-based CCH has 2.7x more edge relaxations than node-based CH.*
| **Isochrone (PHAST)** | |
| - Car 5 min | 3.3ms (306 iso/sec) |
| - Bike 5 min | 4.3ms (233 iso/sec) |
| - Foot 5 min | 2.8ms (356 iso/sec) |

### Isochrone Pipeline ✅

**Sparse Contour with Moore-Neighbor Boundary Tracing (2026-01-25):**
```
PHAST distances → Base graph frontier → Sparse tile stamp → Boundary trace → Simplify
     (6-7ms)         (73-232 points)     (O(segments))     (O(perimeter))   (D-P)
```

**Key Optimization**: Replaced dense grid marching squares with O(perimeter) boundary tracing.
- No densification step needed (works directly on sparse tile map)
- Contour extraction: 47μs (was 67ms) = **1426x speedup** (car 30-min)
- Bike 30-min: 43μs (was 217ms) = **5070x speedup**
- End-to-end: 7.4ms per isochrone (was 80ms) = **10.8x faster**
- Throughput: **134.5 isochrones/sec** (car 30-min), 95.6/sec (bike 30-min)
- Contour is now <1% of total time; PHAST dominates at 89%

### CCH Statistics (Belgium)

| Mode | Filtered Nodes | UP Edges | DOWN Edges | Shortcut Ratio |
|------|----------------|----------|------------|----------------|
| Car  | 2,447,122 | 10.3M | 9.1M | 2.5x |
| Bike | 4,770,739 | 23.8M | 22.0M | 2.4x |
| Foot | 4,932,592 | 25.0M | 23.3M | 2.3x |

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
butterfly-route step6-order --filtered-ebg ./build/filtered.car.ebg --ebg-nodes ./build/ebg.nodes --nbg-geo ./build/nbg.geo --mode car -o ./build/
butterfly-route step7-contract --filtered-ebg ./build/filtered.car.ebg --order ./build/order.car.ebg --mode car -o ./build/
butterfly-route step8-customize --cch-topo ./build/cch.car.topo --filtered-ebg ./build/filtered.car.ebg --weights ./build/w.car.u32 --turns ./build/t.car.u32 --order ./build/order.car.ebg --mode car -o ./build/

# Query server
butterfly-route serve --data-dir ./build/ --port 8080
```

---

## What NOT to Do

- ❌ Use node-based graphs for routing/isochrones
- ❌ Shared CCH for all modes (causes orphaned nodes)
- ❌ Approximate range queries
- ❌ Different backends for different query types
- ❌ Snap differently for different APIs

---

## Bulk Performance Optimization

### Current Status

**Profiling revealed the bottleneck**: 80-87% cache miss rate in downward scan.
- Downward phase = 98% of runtime
- Problem: random writes to `dist[v]` for each edge relaxation
- K-lane batching alone gives only 2.24x (not 8x) due to memory bottleneck

### Reality Checks

- **100k × 100k dense matrix** = 10¹⁰ cells = 40 GB @ 4 bytes/cell
  → Must tile/stream/distribute; never materialize in one request
- **Millions of isochrones** = can't do one PHAST per origin
  → Need K-lane batched PHAST or restricted scan

### Phase A: Measurement Infrastructure ✅ DONE

| Task | Status |
|------|--------|
| Benchmark harness (`bench/` binary) | ✅ |
| Flamegraph + perf scripts | ✅ |
| Counters: upward settled, downward scanned, relaxations, frontier edges | ✅ |
| Baseline report per workload | ✅ |

**Key finding**: 80-87% cache miss rate, 98% time in downward scan

### Phase B: Batch Compute ✅ DONE

| Task | Status | Result |
|------|--------|--------|
| K-lane downward scan (K=8) | ✅ | 2.24x speedup |
| Matrix tile computation | ✅ | Arrow streaming |
| Batched isochrones (K origins) | ✅ | 2.63x speedup |
| Active-set gating (rPHAST-lite) | ✅ | 2.79x for bounded |

### Phase C: Arrow Streaming Output ✅ PARTIAL

| Task | Status |
|------|--------|
| Content negotiation (JSON default, Arrow for bulk) | ✅ |
| Tiled block schema for matrices | ✅ |
| Backpressure + cancellation (bounded channel) | ⬜ |
| Streaming writer for long-running queries | ✅ |

### Phase D: Cache-Friendly Memory Access ✅ DONE

**Finding**: Rank-aligned CCH eliminates the "permute penalty".

| Task | Status | Actual Gain |
|------|--------|-------------|
| **Rank-aligned node renumbering** | ✅ | node_id == rank |
| CCH Topology Version 2 | ✅ | rank_to_filtered mapping |
| PHAST simplified (no inv_perm) | ✅ | Sequential dist[rank] access |
| K-lane batching | ✅ | 1.91x speedup |

**Performance** (Belgium, car mode):
| Metric | Value |
|--------|-------|
| Single PHAST | 39ms (25.5 queries/sec) |
| K-lane batched (K=8) | 20.7ms effective (48.3 queries/sec) |
| Batching speedup | 1.91x |
| Correctness | 100% (0 mismatches) |

### Phase E: Bulk Engine Optimizations ✅ DONE

| Task | Status | Result |
|------|--------|--------|
| SoA dist layout (cache-line aligned) | ✅ | Implemented in batched PHAST |
| Block-level active gating | ✅ | 2.58x for bounded queries |
| Arrow IPC streaming for matrices | ✅ | Backpressure + cancellation |
| K-lane block-gated PHAST | ✅ | Adaptive switching |

### Phase F: Many-to-Many CH for Matrix Queries ✅ ALGORITHM CORRECT

**Problem**: PHAST computes one-to-ALL, wastes 99.996% work for sparse matrices.
- Root cause: algorithmic mismatch, not implementation

**Solution**: Bucket-based many-to-many CH with parallel forward + sorted buckets

| Task | Status | Result |
|------|--------|--------|
| Bucket M2M algorithm | ✅ | Core algorithm correct |
| Parallel forward phase (rayon) | ✅ | Thread parallelism for sources |
| Sorted flat buckets + offsets | ✅ | Replaced HashMap |
| Combined dist+version struct | ✅ | Better cache locality |
| Parallel backward phase | ✅ | Thread parallelism for targets |
| Versioned search state | ✅ | O(1) search init instead of O(N) |

**Performance** (Belgium, car mode, 2026-02-01, fair HTTP comparison):

| Size | OSRM CH | Butterfly | Ratio |
|------|---------|-----------|-------|
| 1000×1000 | 0.5s | 1.5s | 3.0x |
| 5000×5000 | 8.0s | 11.1s | 1.38x |
| 10000×10000 | ~32s | ~44s | **~1.4x** |

**Key finding:** At large scale, Butterfly is only **1.4x slower** than OSRM CH despite:
- Edge-based CCH with 2.5x more nodes
- Exact turn handling (OSRM ignores turn restrictions in matrix queries)

**Optimizations Implemented:**
| Optimization | Effect | Time | Joins |
|--------------|--------|------|-------|
| Baseline (binary search) | - | 51.0s | 103B |
| O(1) prefix-sum bucket lookup | -7% | 47.5s | 103B |
| Bound-aware join pruning | -10% | 42.9s | 61B (-41%) |
| SoA bucket layout | **-24%** | **32.4s** | 61B |
| **Total improvement** | **-36%** | **32.4s** | **61B** |

Gap went from 1.39x slower → **0.88x faster** (12% faster than OSRM).

**Algorithm** (directed graph aware):

The key formula: `d(s → t) = min over m: d(s → m) + d(m → t)`

1. **Source phase (parallel forward)**: Dijkstra on UP graph, collect (node, src_idx, dist)
2. **Build prefix-sum buckets**: O(1) lookup by node ID
3. **Target phase (parallel reverse)**: Dijkstra via DownReverseAdj + join with buckets

**Critical**: For directed graphs, must use reverse search from targets (DownReverseAdj + down_weights)
to get `d(m → t)`. Using forward UP search from targets would give `d(t → m)` which is WRONG.

**Query Type Routing** (validated):
- **Matrices**: Bucket many-to-many CH
- **Isochrones**: PHAST/range (all reachable nodes needed)

---

## CRITICAL PATH: Hybrid Exact Turn Model (2026-01-25)

### The Problem

Current edge-based CCH has **2.6x state expansion** vs node-based:
- NBG nodes: 1,907,111
- EBG nodes: 5,018,890 (2.6x)
- This directly causes the 3-6x performance gap vs OSRM

### The Solution

**Only 0.30% of intersections require edge-based state!**

| Category | Count | Percentage |
|----------|-------|------------|
| Complex intersections (turn restrictions) | 5,726 | 0.30% |
| Simple intersections (no restrictions) | 1,901,385 | 99.70% |

### Hybrid State Graph

Build a mixed-state graph:
- **Simple nodes** → 1 node-state per directed graph node
- **Complex nodes** → edge-states (one per incoming edge) as before

This is **exact** (not an approximation):
- Turn costs only matter where they vary by incoming edge
- Collapsing states at simple nodes doesn't change shortest paths

### Expected Impact

| Metric | Current | Hybrid | Improvement |
|--------|---------|--------|-------------|
| State count | 5.0M | ~1.9M | 2.6x reduction |
| Edge count | ~18M | ~7M | ~2.5x reduction |
| Table gap vs OSRM | 6.4x | ~2.5x | Within striking distance |

### Implementation Plan

1. **Node Classification** (`is_complex(node)`)
   - Check turn_rules table for restrictions at this node
   - Complex if: any turn restriction, conditional access, angle-dependent penalty

2. **Build Hybrid State Graph** (new step between 4 and 5)
   - Map original EBG → hybrid states
   - Simple destination → node-state
   - Complex destination → edge-state

3. **Re-run CCH Pipeline** (Steps 6-8 unchanged)
   - Works on hybrid state graph instead of full EBG
   - Query code unchanged (operates on "state graph")

4. **Validate Correctness**
   - P2P queries: compare vs full EBG queries
   - Isochrones: verify reachable sets match
   - Matrix: compare vs reference

### Why This Beats OSRM

OSRM uses node-based CH (~1.9M nodes) and **ignores most turn restrictions**.
We use hybrid (~1.9M states) and **handle all turn restrictions exactly**.

Same state count + exact turn semantics = faster AND more correct.

---

## CRITICAL PATH: Turn Penalty Cost Model (2026-01-31) ✅ IMPLEMENTED

### Summary

Implemented OSRM-compatible turn penalties using the exact sigmoid formula from car.lua.

**Results:**
| Metric | Before | After |
|--------|--------|-------|
| Brussels-Antwerp | 29 min | 33.7 min |
| Gap vs OSRM | 27% faster | **16% faster** |
| Route distance | 37.6 km | 32.9 km |

The router now properly avoids left turns (which cross traffic in right-hand drive countries).

**OSRM car.lua formula:**
```
penalty = turn_penalty / (1 + exp(-((13/turn_bias) * -angle/180 - 6.5*turn_bias)))
```

With parameters:
- `turn_penalty = 7.5s` (max penalty)
- `turn_bias = 1.075` (right turns cheaper)
- `u_turn_penalty = 20s` (additional)

### Previous Issues (Fixed)

### Arc Filtering Fix (Completed)

**Bug fixed**: `FilteredEbg::build()` only checked NODE accessibility, not ARC accessibility.

| Metric | Before | After |
|--------|--------|-------|
| Car arcs included | 5,871,801 | 3,750,954 |
| Arcs correctly filtered | 0 | 2,120,847 (36%) |

**Files changed**:
- `tools/butterfly-route/src/formats/filtered_ebg.rs` - New `build_with_arc_filter()` function
- `tools/butterfly-route/src/step5.rs` - Extract mode_masks and pass to arc filter

### Turn Penalty Implementation Plan

#### 1. Turn Geometry Calculation

```rust
struct TurnGeometry {
    from_bearing: f32,      // 0-360° heading of incoming edge
    to_bearing: f32,        // 0-360° heading of outgoing edge
    angle: f32,             // Signed turn angle (-180 to +180)
    classification: TurnClass,
}

enum TurnClass {
    Straight,   // |angle| < 30°
    SlightRight, SlightLeft,
    Right, Left,
    SharpRight, SharpLeft,
    UTurn,      // |angle| > 150°
}
```

**Bearing calculation**: Use NBG geometry (lat/lon pairs) to compute edge bearings at intersection.

#### 2. Turn Penalty Formula

```
turn_penalty = base_angle_cost(angle)
             + signal_delay(has_traffic_signal)
             + intersection_complexity(degree, road_class_diff)
```

**Angle-based costs** (car mode):
| Turn Type | Angle Range | Penalty |
|-----------|-------------|---------|
| Straight | |θ| < 30° | 0s |
| Slight turn | 30° ≤ |θ| < 60° | 2s |
| Right turn | 60° ≤ θ < 120° | 5s |
| Left turn | -120° < θ ≤ -60° | 8s |
| Sharp turn | 120° ≤ |θ| < 150° | 10s |
| U-turn | |θ| ≥ 150° | 30s |

**Signal delays** (✅ IMPLEMENTED 2026-01-31):
| Mode | Delay at Signal |
|------|-----------------|
| Car | 8s |
| Bike | 5s |
| Foot | 4s |

12,979 traffic signals extracted from Belgium PBF (`node_signals.bin`).
| Stop sign | 5s |
| Give way | 2s |
| No control | 0s |

**Intersection complexity**:
| Factor | Delay |
|--------|-------|
| High-degree intersection (>4 ways) | +3s |
| Road class change (highway→residential) | +2s |
| Crossing major road | +5s |

#### 3. Files to Modify

| File | Change |
|------|--------|
| `src/profiles/mod.rs` | Add turn penalty calculation |
| `src/step4.rs` | Compute turn geometry, store penalties |
| `src/formats/turn_table.rs` | Store computed penalties |
| `src/step5.rs` | Read penalties into turn costs |

#### 4. Validation

After implementation:
- [ ] Re-run OSRM comparison benchmark
- [ ] Expect routes to be 0-10% slower than OSRM (not 27% faster)
- [ ] Verify realistic left-turn avoidance in urban areas
- [ ] Check highway-to-local transition penalties

### Expected Impact

| Metric | Current | Expected |
|--------|---------|----------|
| Route speed vs OSRM | 27% faster | ~5% (margin of error) |
| Turn delays enforced | None | Realistic |
| Left turn avoidance | None | Yes (urban) |
| Signal delay modeling | None | 8s/signal (✅ implemented) |

**Actual Results (Brussels→Antwerp):**
- Without turn penalties: 29 min
- With turn penalties: 33.7 min
- With turn penalties + signals: 34.9 min
- OSRM reference: ~40 min
- **Gap: ~12.8% faster than OSRM**

---

## Future Features

- [ ] Alternative routes
- [ ] Traffic-aware routing (live weight updates)
- [ ] Multi-modal routing (car + foot)
- [ ] Turn-by-turn instructions

## Correctness

- [ ] Validate routes against reference (OSRM/Valhalla)
- [ ] Stress test with random queries
- [ ] Edge case handling (ferries, toll roads, restricted areas)

---

## OSRM Algorithm Analysis (2026-01-25)

### Key Finding: OSRM Uses NO PARALLELISM

The OSRM many-to-many CH implementation (`many_to_many_ch.cpp`) is **purely sequential**.
Parallelism is NOT why OSRM is fast. Smart algorithms are.

### OSRM's Bucket M2M Structure

```
Phase 1: Backward searches (SEQUENTIAL)
  for each target:
    backward_dijkstra(target)
    append NodeBucket(node, target_idx, dist) to buckets[]

Phase 2: Sort buckets by node_id (ONCE)
  std::sort(buckets)

Phase 3: Forward searches (SEQUENTIAL)
  for each source:
    forward_dijkstra(source)
    for each settled node:
      binary_search(buckets, node)  // std::equal_range
      update matrix[source][target]
```

### Why OSRM is Fast

1. **d-ary heap with DecreaseKey** (not lazy reinsert)
2. **O(1) visited check** via position array
3. **Stall-on-demand** checks opposite-direction edges
4. **Binary search** for bucket lookup (not offset arrays)
5. **No parallel overhead**

### Butterfly vs OSRM Performance Gap ✅ 1.4x AT SCALE (2026-02-01)

**Fair HTTP Comparison (same methodology):**
| Size | OSRM | Butterfly | Gap |
|------|------|-----------|-----|
| 1000×1000 | 0.5s | 1.5s | 3.0x |
| 5000×5000 | 8.0s | 11.1s | 1.38x |
| 10000×10000 | ~32s | ~44s | **~1.4x** |

**Optimizations implemented:**
1. ✅ O(1) prefix-sum bucket lookup (not binary search)
2. ✅ Bound-aware join pruning (-41% joins)
3. ✅ SoA bucket layout (-24% backward time)
4. ✅ 4-ary heap with decrease-key (0% stale)
5. ✅ Version-stamped distances (O(1) init)

**Result:** Edge-based CCH with exact turn handling is **1.4x slower** than OSRM at scale.
This is excellent given we have 2.5x more nodes and exact turn restrictions.

---

## Arrow Streaming for Large Matrices (2026-02-01) ✅ COMPLETE

### The Problem

OSRM's GET-based API cannot handle matrices larger than ~10k×10k (URL length limits).
Butterfly needed a streaming solution for massive matrices (50k×50k = 2.5B distances).

### Solution: Tile-by-Tile Arrow IPC Streaming

**Endpoint:** `POST /table/stream`

**Architecture:**
```
Request (50k sources, 50k destinations)
    ↓
Rayon parallel tile computation (1000×1000 tiles)
    ↓
Each tile → Arrow IPC → HTTP chunked transfer
    ↓
Client receives tiles as they complete
```

**Key Design Decisions:**
1. Compute tiles in parallel using `rayon::par_iter().for_each()`
2. Stream each tile immediately (no memory accumulation)
3. Use `tokio::sync::mpsc` channel for backpressure (depth 8)
4. 256MB body limit for large coordinate payloads

### Benchmark Results

| Size | Distances | Time | Throughput | First Byte | Data |
|------|-----------|------|------------|------------|------|
| 10k×10k | 100M | 24s | 4.1M/sec | 3.6s | 381 MB |
| 50k×50k | 2.5B | 9.5 min | 4.4M/sec | 3.6s | 9.5 GB |

### vs OSRM

| Size | Butterfly | OSRM | Result |
|------|-----------|------|--------|
| 10k×10k | 24s | 33.6s | **Butterfly 28% faster** |
| 50k×50k | 9.5 min | CRASHES | **Butterfly only option** |

### Memory Efficiency

- Baseline (server loaded): 11.6 GB
- During 50k×50k query: 14 GB
- **Query overhead: only 2.4 GB** (tiles streamed, not accumulated)

### Files Changed

- `tools/butterfly-route/src/step9/api.rs` - `table_stream` endpoint
- `tools/butterfly-route/src/matrix/arrow_stream.rs` - `MatrixTile`, `tiles_to_record_batch`
