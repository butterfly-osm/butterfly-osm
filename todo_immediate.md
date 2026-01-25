# Immediate Roadmap: Bulk Engine Optimization

## Current Status

Rank-aligned CCH (Version 2) implemented and validated:
- **Single PHAST**: 39ms per query (25.5 queries/sec)
- **K-lane batched (K=8)**: 20.7ms effective (48.3 queries/sec)
- **Batching speedup**: 1.91x
- **CCH validation**: 0 mismatches (100% correct)

---

## Phase 5: Cache-Friendly PHAST via Rank-Aligned Memory ✅ DONE

### 5.1 Rank-Aligned CCH (Version 2) ✅

Implemented rank-aligned node renumbering where `node_id == rank`:

```
// Before: inv_perm[rank] gives random node → random memory access
// After:  node_id == rank → sequential memory access

dist[rank] is now sequential!
```

Changes:
- [x] CCH Topology Version 2 with `rank_to_filtered` mapping
- [x] `remap_to_rank_space()` in Step 7 contraction
- [x] Step 8 customization updated for rank-aligned indices
- [x] PHAST/BatchedPHAST simplified (no inv_perm lookup needed)
- [x] Validation passes (0 mismatches across 100+ queries)

**Performance Impact** (Belgium, car mode):
| Metric | Before | After |
|--------|--------|-------|
| Single PHAST | ~40ms | 39ms |
| Batched K=8 | ~40ms/query | 20.7ms effective |
| Batching speedup | ~1x | 1.91x |

---

## Phase 6: Bulk Engine ← CURRENT PRIORITY

Target: Best-in-class for one-to-many on CPU, scalable to bulk matrices + millions of isochrones

### Milestone 6.1: Generalize Beyond "car Belgium" ← DO IMMEDIATELY

Verify rank-alignment win isn't dataset/mode-specific.

- [ ] Run benchmarks for **bike** mode (larger graph: 4.8M nodes)
- [ ] Run benchmarks for **foot** mode (largest graph: 4.9M nodes)
- [ ] Track metrics:
  - Downward relaxations per query
  - LLC miss rate (perf stat)
  - Effective queries/sec for K=1,4,8,16
- [ ] Identify new bottleneck after rank-alignment

**Goal**: Confirm rank-alignment is dominant win everywhere.

---

### Milestone 6.2: SoA Layout for Batched PHAST ← HIGH ROI

Current K=8 batching gives 1.91x; should get closer to 3-6x.

**Root cause**: AoS layout `dist[lane][node]` causes poor cache utilization for K-lane inner loop.

#### Implementation
- [ ] Switch dist layout to **SoA**: `dist_lane0[N], dist_lane1[N], ...`
- [ ] Align arrays to 64 bytes (cache line)
- [ ] Tighten hot loop (autovectorization friendly):
  ```rust
  // Load K-lane distances from u
  let du: [u32; K] = load_aligned(dist_lanes, u);
  // Compute candidates: du + w
  let cand: [u32; K] = simd_saturating_add(du, w);
  // Compare and update: min(dv, cand)
  let dv: [u32; K] = load_aligned(dist_lanes, v);
  let new_dv = simd_min(dv, cand);
  store_aligned(dist_lanes, v, new_dv);
  ```

**Expected**: 2-4x additional speedup over current batched PHAST.

---

### Milestone 6.3: Block-Level Active Gating for Isochrones ✅ IMPLEMENTED

For bounded queries (isochrones), skip scanning most ranks when only small region reachable.

#### Implementation ✅ Complete

1. **Rank blocks** ✅
   - [x] `BLOCK_SIZE = 4096` ranks per block
   - [x] `block = rank / BLOCK_SIZE`

2. **Active block bitset** ✅
   - [x] Block active if contains node with `dist ≤ T` or can receive updates ≤T
   - [x] Bitset size: ~75 bytes for foot (1205 blocks)

3. **Scan only active blocks** ✅
   - [x] Outer loop iterates blocks in descending order
   - [x] Skip inactive blocks entirely

4. **Within a block** ✅
   - [x] For each node: load `du`
   - [x] If `du == INF` or `du > T`: continue (early exit)

5. **Mark blocks when relaxing** ✅
   - [x] When `cand ≤ T`, mark `block(v)` active

#### Validation ✅ Passed
- [x] Compare against active-set PHAST (0 mismatches)
- [x] Tested on 6 origins × 4 thresholds for car, foot, bike modes

#### Benchmark Results (Belgium, 100 random origins)

| Mode | Threshold | p95 | Blocks Skipped | Speedup vs Active-Set |
|------|-----------|-----|----------------|----------------------|
| Foot | 1min | **16ms** | 97.8% | **1.18x** |
| Foot | 5min | 47ms | 83.5% | 1.06x |
| Bike | 10min | 130ms | 1.2% | 0.99x |

#### Analysis

Block-level gating provides significant benefit for **small thresholds** (≤1-2 min):
- At 1 min, 97.8% blocks skipped → 1.18x speedup
- At 5 min, only 83.5% skipped (graph becomes well-connected)
- At 10 min bike, almost entire graph reachable → no benefit

**Targets NOT met** for 5/10 min thresholds:
- Foot 5min: p95=47ms (target was 20-40ms) ❌
- Bike 10min: p95=130ms (target was 50-80ms) ❌

**Root cause**: PHAST downward scan is O(edges_from_reachable_nodes). Block gating reduces node iteration but NOT edge relaxations. For large thresholds, almost all edges are still relaxed.

#### Completed Optimizations

### Milestone 6.3.1: Adaptive Gating Strategy ✅ DONE

Prevent gating overhead from hurting performance when skip% is low.

**Implementation:**
- [x] Add heuristic switch: if `active_blocks / total_blocks > 0.25`, run plain PHAST (no gating)
- [x] Add lane-mask adjacency skip for batched: if all K lanes have `dist > T`, skip adjacency entirely
- [x] Benchmark: show no regressions, improved p95 where gating helps

**Results** (single PHAST adaptive):
| Mode | Threshold | Plain | Block-Gated | Adaptive | Speedup |
|------|-----------|-------|-------------|----------|---------|
| Foot | 1min | 138ms | 10ms | 10ms | 13.8x |
| Foot | 5min | 138ms | 68ms | 64ms | 2.2x |
| Bike | 1min | 128ms | 18ms | 18ms | 7x |

---

### Milestone 6.3.2: K-Lane Block-Gated PHAST ✅ DONE

The only CPU-friendly way to shrink per-isochrone cost is to share relaxation work across K queries.

**Implementation:**
- [x] Maintain one active bitset per lane (or per small group)
- [x] Block is active if **any lane** has it active
- [x] At each node, compute lane mask of `dist_lane <= T`; if mask=0, skip adjacency
- [x] Relax edges updating only lanes in mask
- [x] SoA layout for cache-friendly vectorization
- [x] Heuristic early-exit for large thresholds (>= 5 min): skip gating, use plain batched PHAST

**Results** (K=8 batched PHAST, effective per-query time):
| Mode | Threshold | Regular | K-Lane Gated | Speedup | Blocks Skipped |
|------|-----------|---------|--------------|---------|----------------|
| Foot | 1 min | 82.9ms | 32.1ms | **2.58x** | 84% |
| Foot | 2 min | 83.6ms | 37.4ms | **2.23x** | 70% |
| Foot | 5 min | 82.4ms | 82.3ms | **1.00x** | (plain path) |
| Bike | 2 min | 74.5ms | 70.8ms | **1.05x** | 9% |
| Bike | 10 min | 74.5ms | 74.0ms | **1.01x** | (plain path) |

**Key insights:**
- 2-2.6x speedup for small thresholds (1-2 min)
- No regression for large thresholds (>= 5 min) due to heuristic early-exit
- Block gating benefits diminish as reachable area grows

---

### Milestone 6.3.3: rPHAST Decision ✅ ANALYZED

Instrumented reachability metrics to decide whether rPHAST is worth implementing.

**Reachability Analysis Results (Belgium, 20 random origins):**

| Mode | Threshold | Edges Reachable | rPHAST Decision |
|------|-----------|-----------------|-----------------|
| Foot | 1 min | 1.1% | ✅ Recommended |
| Foot | 2 min | 2.7% | ✅ Recommended |
| Foot | 5 min | 11.2% | ✅ Recommended |
| Foot | 10 min | 36.3% | ✅ Recommended |
| Bike | 1 min | 6.2% | ✅ Recommended |
| Bike | 2 min | 22.4% | ✅ Recommended |
| Bike | 5 min | 84.2% | ⚠️ Marginal |
| Bike | 10 min | 99.8% | ❌ Not recommended |

**Key Insights:**
- **Foot mode**: Even at 10 min, only 36.3% edges reachable → rPHAST preprocessing could help significantly
- **Bike mode**: At 5 min already 84% reachable, at 10 min ~100% → K-lane batching is the better approach
- **Production pattern dependent**:
  - For bulk foot isochrones (any threshold): Consider rPHAST
  - For bulk bike isochrones: K-lane batching is optimal
  - For interactive single queries: Current adaptive PHAST is sufficient

**Decision:** rPHAST implementation deferred in favor of Arrow IPC streaming (Milestone 6.4).
- Current K-lane block-gated PHAST provides good throughput for small thresholds
- rPHAST would add complexity for diminishing returns on bike mode
- May revisit for foot-specific bulk workloads if needed

---

### The Uncomfortable Truth (Validated)

For "single query, large T" we're near CPU roofline:
- Bike/foot 10min touches 83-92M edges
- Memory-bandwidth streaming relaxation is close to optimal on CPU

**Current status:**
- ✅ Bulk throughput targets met via K-lane batching + block gating
- ✅ No regression for large thresholds
- ✅ 32ms effective per-query for foot 1min (was 82ms before optimization)

---

### Milestone 6.4: Arrow IPC Streaming for Matrix Tiles ✅ IMPLEMENTED

Productize the bulk path for 100k×100k matrices.

#### A) Define Matrix Output as Tiled Blocks (NOT long format) ✅

Schema implemented in `arrow_stream.rs`:
```
src_block_start: u32
dst_block_start: u32
src_block_len: u16
dst_block_len: u16
durations_ms: Binary (packed row-major u32)
```
- Unreachable sentinel: `u32::MAX`

#### B) Stream Tiles Over HTTP ✅

Implemented `/matrix/stream` endpoint:
- [x] `Content-Type: application/vnd.apache.arrow.stream`
- [x] Write RecordBatches as tiles complete
- [x] Bounded channel for backpressure (4 tiles in flight)
- [x] Cancellation via channel drop on client disconnect

#### C) Tile Sizes Matching K-Lane ✅

- [x] `src_tile_size` defaults to 8 (matches K_LANES), configurable
- [x] `dst_tile_size` defaults to 256, configurable
- [x] Source batches aligned to K_LANES for efficient computation

#### D) Scheduling ✅

- [x] Outer loop over src tiles (sequential for memory stability)
- [x] Compute once per src tile, emit multiple dst tiles
- [x] Backpressure prevents unbounded memory growth

#### Implementation Details

**Endpoint**: `POST /matrix/stream`

**Request**:
```json
{
  "sources": [1000, 2000, ...],
  "targets": [4000, 5000, ...],
  "mode": "car",
  "src_tile_size": 8,
  "dst_tile_size": 256
}
```

**Response**: Arrow IPC stream with tiles as RecordBatches

**Key Features**:
- Async streaming via `tokio::spawn_blocking` + `mpsc::channel`
- True backpressure: computation pauses if client is slow (channel depth 4)
- Cancellation: compute task exits early if client disconnects
- Memory-stable: only 1 src batch + 4 buffered tiles in memory at a time

#### Benchmark Results (Belgium, car mode)

| Matrix Size | Time | Cells/sec | PHAST QPS |
|-------------|------|-----------|-----------|
| 100 × 100 | 2.1s | 4,720 | 47.2 |
| 1k × 1k | 21s | 47,810 | 47.8 |
| 10k × 10k | 210s | 475,579 | 47.6 |

#### OSRM Comparison (CRITICAL)

| Matrix Size | OSRM | Butterfly | Ratio |
|-------------|------|-----------|-------|
| 100 × 100 | 32ms | 2,112ms | **66x slower** |
| 1k × 1k (batched) | 7.5s | 21s | 2.8x slower |

**Root Cause**: ALGORITHMIC MISMATCH

1. **OSRM Table**: Uses CH bidirectional queries per source-target pair
   - Only explores paths to REQUESTED targets
   - For 100 targets out of 2.4M nodes → tiny graph fraction

2. **Butterfly PHAST**: Computes one-to-ALL nodes (full SSSP)
   - 100 sources × 2.4M nodes = 240M distances computed
   - Extracts 100 targets → **discards 99.996% of work**

**Conclusion**: PHAST is correct for isochrones (dense, all reachable nodes).
PHAST is WRONG for sparse distance matrices (few specific targets).

For competitive matrix performance, need:
- CH bidirectional P2P queries (per source-target pair), OR
- Bucket-based many-to-many CH, OR
- RPHAST with target-specific preprocessing

**P2P Query Comparison** (100 random pairs):

| Metric | OSRM CH | Butterfly CCH | Ratio |
|--------|---------|---------------|-------|
| Avg latency | 18.8ms | 41.5ms | 2.2x slower |
| p50 latency | 13.2ms | 34.8ms | 2.6x slower |
| QPS | 53 | 24 | 2.2x slower |

→ P2P performance is reasonable (2.2x gap due to turn-awareness overhead)

**Many-to-Many Comparison** (50×50 matrix):

| Method | Time | Cells/sec | vs OSRM |
|--------|------|-----------|---------|
| OSRM Table | 65ms | 38,301 | 1.0x |
| Butterfly PHAST | 2,100ms | 1,190 | 32x slower |
| Butterfly P2P (seq) | 66,500ms | 38 | 1000x slower |
| Butterfly P2P (10t) | 8,200ms | 305 | 125x slower |

OSRM's Table uses bucket-based many-to-many CH (O(|V|+|E|)), not N×M P2P queries.

**Current Status**:
- Streaming infrastructure complete ✅
- Isochrone performance acceptable ✅ (PHAST is correct algorithm)
- Matrix performance needs many-to-many CH algorithm ❌

**Recommended Next Steps**:
1. Implement bucket-based many-to-many CH for matrix queries
2. Keep PHAST for isochrone/reachability (dense queries)
3. Route to appropriate algorithm based on query type

#### Done-ness Bar
- [x] Streaming endpoint implemented with backpressure
- [x] 10k×10k matrix benchmark with bounded memory ✅
- [ ] Python Arrow consumer verification
- [x] Sustained throughput benchmark documented ✅
- [x] OSRM comparison completed ✅

---

## Phase 7: Many-to-Many CH for Matrix Queries ⚠️ OPTIMIZED - GAP EXPLAINED

### The Problem

PHAST computes one-to-ALL distances, which is correct for isochrones but wasteful for sparse matrix queries where we only need specific source-target pairs.

### Current State (2026-01-25): Reusable Engine with Buffer Reuse ✅

**Constraint**: No parallelism - comparing apples-to-apples with OSRM's sequential algorithm.

| Query | OSRM CH | Butterfly Bucket M2M | Gap | Notes |
|-------|---------|----------------------|-----|-------|
| 10×10 | 6ms | **21.2ms** | 3.5x slower | Was 44ms originally |
| 25×25 | 10ms | **52.9ms** | 5.3x slower | |
| 50×50 | 17ms | **108.7ms** | 6.4x slower | |
| 100×100 | 35ms | **225.2ms** | 6.4x slower | |

**Time Breakdown** (100×100):
- Forward phase (UP edges): 88ms (39%)
- Sort buckets: 2ms (1%)
- Backward phase (reversed DOWN): 132ms (59%)
- Overhead: 3.2ms (1%)

**Key Achievements**:
- 0% stale heap entries (4-ary heap with DecreaseKey)
- Reusable BucketM2MEngine avoids per-call allocations
- Flat DownReverseAdjFlat with embedded weights

---

### Milestone 7.1: Bucket Many-to-Many Algorithm ✅ CORRECT

#### Algorithm (Verified for Directed Graphs)

For directed graphs: **d(s → t) = min over m: d(s → m) + d(m → t)**

- **Source Phase**: Forward UP search → store `(source_idx, d(s→m))` in bucket[m]
- **Target Phase**: Reverse search via DownReverseAdj → join with buckets

#### Implementation

- [x] **Combined dist+version struct** - Single cache line for locality
- [x] **Forward search** - Dijkstra on UP graph, populate buckets
- [x] **Backward search** - Dijkstra on reversed DOWN graph via DownReverseAdj
- [x] **Binary search buckets** - partition_point for O(log n) lookup
- [x] **Versioned search state** - O(1) init instead of O(N)
- [x] **Lazy heap reinsertion** - Despite 75% stale rate, faster than indexed heap

#### Validation ✅

- [x] Compared 5×5 M2M vs P2P queries: **All 25 queries match**

---

### Milestone 7.2: Optimizations Implemented ✅

| Optimization | Result | Verdict |
|--------------|--------|---------|
| Combined dist+version struct | 47ms → 44ms (-6%) | ✅ Keep |
| Flat reverse adjacency (embedded weights) | Eliminates 1 indirection | ✅ Keep |
| Sorted buckets (binary search) | Efficient for sparse matrices | ✅ Keep |
| **4-ary heap with DecreaseKey (OSRM-style)** | **32ms → 23.7ms (-26%)** | ✅ **KEY WIN** |
| **BucketM2MEngine (buffer reuse)** | **23.7ms → 21.2ms (-11%)** | ✅ **KEY WIN** |
| Lazy reinsertion | 75% stale entries | ❌ Replaced with DecreaseKey |
| Flat UP adjacency (pre-filtered INF) | +3ms overhead (0 INF edges to filter) | ❌ No benefit |
| Prefix-sum bucket layout (stamped) | No improvement over binary search | ❌ Binary search already fast |
| Merged NodeEntry (dist+version+handle) | +1.2ms overhead (16 bytes vs 12) | ❌ Worse cache locality |
| Lazy deletion heap | +10ms, 75% stale | ❌ Much worse than DecreaseKey |
| Indexed heap (fixed array) | 57ms (+30%) | ❌ Overhead > savings |
| Indexed heap (HashMap) | 65ms (+48%) | ❌ Hash overhead worse |
| Stall-on-demand (forward search) | 45ms (2x slower), **0% stall rate** | ❌ **NOT APPLICABLE to edge-based CCH** |
| Swapped direction (bwd→fwd) | Incorrect | ❌ Wrong semantics for directed |
| Early-exit pruning (global min_found) | BREAKS CORRECTNESS | ❌ Wrong algorithm |

**Key Discovery (2026-01-25):**
- CCH weights have **0 INF edges** after step8 customization
- INF check in forward loop has near-zero overhead (branch perfectly predicted)
- Early-exit pruning with global `min_found` is INCORRECT (breaks correctness)
  - Tracks min to ANY source, but we might still need paths to OTHER sources
  - Correct early-exit requires per-source upper bounds (complex)
- **Stall-on-demand NOT applicable to edge-based CCH** (2026-01-25):
  - OSRM uses stall-on-demand for node-based CH (~1M nodes)
  - Our edge-based CCH has ~2.4M "nodes" (actually directed edges)
  - The stall condition (better path via incoming UP edge) never triggers
  - **0% stall rate** observed across all matrix sizes
  - Overhead of checking incoming UP edges makes it 2x slower
  - Root cause: Edge-based hierarchy has different structure from node-based CH

**Improvements achieved:**
- 44ms → 21.2ms for 10×10 (52% improvement total)
- Original: 44ms (lazy heap, no buffer reuse)
- After DecreaseKey: 23.7ms (26% faster)
- After BucketM2MEngine: 21.2ms (11% faster)
- **0% stale heap entries** (was 75%)

---

### Milestone 7.3: Root Cause Analysis ✅ UNDERSTOOD

**Why we're 4-7x slower than OSRM:**

| Aspect | OSRM | Butterfly | Impact |
|--------|------|-----------|--------|
| Graph type | Node-based CH | Edge-based CCH | - |
| Nodes (Belgium) | ~1.9M | ~2.4M | +26% |
| Edges/node | ~7 | ~15 | +114% |
| Total edges | ~13M | ~37M | +185% |

**The math:**
- 2.4M/1.9M = 1.26x more nodes
- 15/7 = 2.14x more edges per node
- Combined: **1.26 × 2.14 ≈ 2.7x more edge relaxations per search**

**Current gap analysis (10×10):**
- Expected from graph size: 2.7x slower
- Actual: 4.0x slower (23.7ms vs 6ms)
- Unexplained overhead: 1.5x (likely cache effects from larger working set)

**This is largely fundamental architecture overhead**, not algorithmic inefficiency.
Edge-based CCH provides exact turn costs but at 2.7-4x computational cost.

**Progress from optimization:**
- Started: 32ms (5.3x slower than OSRM)
- After 4-ary heap with DecreaseKey: 23.7ms (4.0x slower)
- **Improvement: 26% faster, closed 25% of the gap**

---

### Milestone 7.4: Hybrid Exact Turn Model ⚠️ REQUIRES REDESIGN

**CONFIRMED Analysis Result (2026-01-25):**
- **Complex intersections: 5,719 / 1,907,139 = 0.30%**
- **Simple intersections: 99.70%**
- Current EBG: 5,019,010 nodes (2.63x expansion from NBG)
- **Hybrid state graph BUILT and VALIDATED:**
  - Node-states: 1,901,420 (99.1%)
  - Edge-states: 16,311 (0.9%)
  - Total: 1,917,731 hybrid states
  - **State reduction: 2.62x** (5.0M → 1.9M)
  - **Arc reduction: 2.93x** (14.6M → 4.9M arcs)

**FIXED (2026-01-25): CCH Contraction Now Works**

Initial failure was due to broken coordinate extraction in Step 6:
- Bug: All states got the same coordinate → spatial partitioning failed → bad ordering
- Fix: Properly map each hybrid state to its NBG node's coordinate

After fix:
- **Regular EBG CCH**: 30M shortcuts, max_degree=966, 2.4M nodes
- **Hybrid CCH**: 35M shortcuts, max_degree=1338, 1.9M nodes ← NOW WORKS

**However: Performance is 10% SLOWER, not faster!**

| Metric | Regular | Hybrid | Change |
|--------|---------|--------|--------|
| Nodes | 2.4M | 1.9M | -21% ✓ |
| Edges | 37M | 40M | +8% ✗ |
| Edges/node | 15.4 | 21.0 | **+36%** ✗ |
| 100×100 matrix | 227ms | 250ms | +10% ✗ |

**Root Cause:**
Collapsing edges to node-states **increases edges-per-node** because:
- Each node-state inherits ALL outgoing edges from ALL collapsed incoming edges
- This cancels out the benefit of fewer nodes

---

### Three Smart Fixes for Hybrid Exactness

#### Fix 1: Two-Level Overlay Design (MLD-style)

Instead of one CH over hybrid graph, build:
- **Base layer**: Keep exact edge-based graph for correctness
- **Overlay layer**: Node-based "simple-intersection" overlay

For simple-only regions:
- Identify maximal chains/regions of simple nodes
- Replace each region with bounded-degree overlay via exact multi-source Dijkstra
- Boundary states = transitions entering/exiting the region

**Result**: Reduces CH search states without creating hubs.

#### Fix 2: Equivalence-Class Hybrid (RECOMMENDED)

Never collapse to ONE node-state per node. Use **bounded equivalence classes**:

Current (broken):
```
State = node_id  (all incoming edges → one state)
```

Fixed:
```
State = (node_id, incoming_class)  where class ∈ {0..K-1}, K small (2-8)
```

Classes based on exact equivalence:
- Incoming edges grouped by identical restriction sets + penalty function
- Or by: road class, approach angle bucket (8-16 bins), carriageway type

**Key insight**: Classes are exact if they have same allowed outgoing set + same penalties.

**Result**: Prevents single-hub effect while reducing states vs full edge-based.

#### Fix 3: Degree-Constrained Contraction Ordering

Even without changing state model, prevent explosion via ordering:

**A) Degree-aware importance:**
- Any state with degree > threshold (16-32) gets very high importance
- High-degree hubs become apex nodes (contracted last) instead of cascade generators

**B) Cap shortcut growth:**
- Monitor shortcut count per rank
- If growth rate explodes, force remaining high-degree nodes to top of hierarchy

---

### Recommended Path Forward

**CRITICAL INSIGHT (2026-01-25)**: The naive hybrid result proves exactly WHY equivalence-class hybrid is necessary.

**Why Naive Hybrid Failed (Mathematically Inevitable)**

CH/CCH query cost is dominated by:
```
#relaxations ≈ Σ(deg(u)) over visited u
```

Naive hybrid: reduced nodes by 21% but increased degree by 36%
→ Total relaxations INCREASED → Query time INCREASED

This is not a bug - it's the **provably worst possible collapse strategy**.

**Why Equivalence-Class Hybrid is Fundamentally Different**

| Approach | States/node | Edges/state | Total edges |
|----------|-------------|-------------|-------------|
| Edge-based | indeg(v) | outdeg(v) | indeg × outdeg |
| Naive node-state | 1 | sum(all outs) | indeg × outdeg (worse locality) |
| Equivalence-class | K (small) | outdeg(v) | **K × outdeg** where K ≪ indeg |

The key invariant for equivalence-class collapse:
> Two incoming edges e1, e2 can share a state IFF they have:
> 1. Identical allowed outgoing transitions (restriction mask)
> 2. Identical turn penalties to each outgoing edge

If this holds, collapsing is **exact** and does **NOT increase degree**.

**Implementation Plan:**

1. ✅ **FIRST: Measure K(node) distribution (BEFORE coding anything)** ← DONE

   **Results (Belgium, car mode, 2026-01-25):**
   ```
   Nodes analyzed:      1,907,139
   Total indeg:         5,019,010
   Total K:             1,961,816
   Reduction ratio:     2.56x

   K(node) distribution:
     p50: 1    ← EXCELLENT!
     p90: 1
     p95: 1
     p99: 2
     max: 10

   Indeg distribution (comparison):
     p50: 3
     p90: 4
     p99: 4
     max: 12

   Node breakdown:
     Fully collapsed (K=1):     1,869,701 (98.0%)
     Partial reduction (K<indeg): 1,579,879 (82.8%)
     No benefit (K=indeg):        327,260 (17.2%)

   VERDICT: ✅ Equivalence-class hybrid WILL HELP
   ```

   **Key Insight**: 98% of nodes have K=1, meaning ALL incoming edges at these nodes
   have identical behavior signatures. This is the best possible result - we can
   collapse to nearly NBG size (1.96M states) while maintaining exact turn semantics.

2. ✅ **Group incoming edges by identical signature → class_id** ← DONE
   - Each class has EXACTLY the outgoing edges of ONE member
   - This guarantees edges-per-state = outdeg (not union)
   - **VERIFIED**: Out-degree ratio 0.89x, In-degree ratio 0.89x

3. ✅ **Create hybrid states as `(node, class_id)`** ← DONE
   - States: 5.0M → 1.96M (2.56x reduction)
   - Arcs: 14.6M → 5.08M (2.88x reduction)
   - Degree: 2.92 → 2.59 (0.89x - LOWER than EBG!)

4. ⚠️ **Build CCH on equivalence-class graph** ← ORDERING MISMATCH DISCOVERED
   - Geometry-based ND ordering FAILED:
     - Shortcuts per original arc: EBG 5.25x → Hybrid **17.11x** (3.2x worse)
     - Up/Down balance: EBG 0.96x → Hybrid **3.39x** (severely unbalanced)
   - **Root cause**: ND assumes "connectivity ≈ spatial proximity"
   - Hybrid graph breaks this: equivalence classes create non-local connections
   - **This is NOT a fundamental limit** - it's an ordering mismatch!

**Key Metrics from Equivalence-Class Hybrid:**
- K(node) distribution: median=1, p99=2 → ✅ EXCELLENT
- Input graph degree ratio: 0.89x → ✅ LOWER than EBG (preserved invariant)
- CCH fill-in: 17.11x → ❌ BAD (due to geometry-based ordering, not topology)
- Up/Down balance: 3.39x → ❌ BAD (smoking gun for ordering mismatch)

---

### Milestone 7.4.1: Graph-Based Ordering for Hybrid CCH ✅ TESTED - BFS FAILED

**Problem**: Geometry-based ND fails because hybrid graph connectivity ≠ spatial proximity.

**Hypothesis**: Graph-based partitioning would produce better separators.

**Experiment Results (2026-01-25): BFS Bisection Ordering**

Implemented BFS-based graph partitioning (no coordinates):
- Pseudo-diameter heuristic to find two peripheral nodes
- Bidirectional BFS from seeds to partition nodes
- Boundary nodes become separators

**Results: CATASTROPHIC FAILURE**

| Metric | Geometry-Based | BFS-Based (at 18%) |
|--------|----------------|-------------------|
| Shortcuts | 86.9M (final) | 365M (and climbing) |
| Max degree | ~966 | 2769 |
| Projected total | 86.9M | Billions |

**BFS contraction was stopped at 18% due to runaway fill-in.**

**Root Cause Analysis:**

BFS ordering has two CH-specific pathologies:
1. **Layering effect**: Creates huge contiguous rank bands with similar structural role
2. **No fill-awareness**: Doesn't account for shortcut creation cost

The hybrid graph has **densifier nodes** (high in×out product) that create local bicliques.
BFS contracts these early → cascade of fill-in.

**Key Insight**: This doesn't prove "graph-based ordering is fundamentally worse".
It proves **naive BFS is incompatible with CCH on densifier-heavy graphs**.

Geometry-based ND works better because separators naturally end up late in the order.

---

### Milestone 7.4.2: Constrained Ordering with Densifier Delay ✅ TESTED - NO IMPROVEMENT

**Hypothesis**: Delaying high in×out states would reduce fill-in.

**Experiment Results (2026-01-25):**

1. ✅ **Densifier distribution analysis**
   - Max in×out: 144 (only 7 states above 100)
   - 86 states with in×out > 50 (0.004%)
   - This is actually a VERY LOW densifier count

2. ✅ **Constrained geometry ND**
   - Added `--densifier-threshold=50` option
   - Forces 86 high in×out states to late ranks

3. ✅ **Results: MADE THINGS WORSE**

| Metric | No Delay | With Delay (50) | Change |
|--------|----------|-----------------|--------|
| Shortcuts | 86.9M | 89.1M | **+2.5% WORSE** |
| Up edges | 71.1M | 73.1M | +2.8% |
| Down edges | 20.9M | 21.1M | +0.9% |
| Up/Down ratio | 3.40x | 3.46x | Slightly worse |

**Conclusion**: The 86 densifiers are NOT the root cause.

---

### Milestone 7.4.3: Hybrid CCH ❌ ABANDONED

**Final Verdict (2026-01-25):**

Equivalence-class hybrid is **structurally incompatible with CCH**.

**Evidence:**
- Geometry-based ND: 86.9M shortcuts (17.1x per arc)
- BFS ordering: 365M+ shortcuts (catastrophic)
- Densifier delay: 89.1M shortcuts (worse)
- Regular EBG CCH: 30.9M shortcuts (5.25x per arc)

**Root Cause Analysis:**

Road networks have good CCH separators because they're **nearly planar**.
Equivalence-class collapse destroys this property:
- Creates non-local connections (edges skip over spatial regions)
- Cross-cutting arcs force shortcuts to bridge remote hierarchy levels
- Result: 3x more shortcuts than uncollapsed EBG

The problem is NOT:
- ❌ Individual high-degree densifiers (max in×out=144, only 7 above 100)
- ❌ Ordering algorithm choice (both geometry and BFS failed)
- ❌ Contraction being too aggressive

The problem IS:
- ✅ The collapse transformation fundamentally changes graph structure
- ✅ Non-planar connections created by collapse
- ✅ Separator quality destroyed regardless of ordering

**Impact on Project:**

| Approach | Shortcuts/arc | Status |
|----------|---------------|--------|
| Regular EBG CCH | 5.25x | ✅ Use this |
| Naive hybrid | - | ❌ Abandoned (degree +36%) |
| Equiv-class hybrid | 17.11x | ❌ Abandoned (fill-in 3x worse) |

**Path Forward:**
1. Keep full edge-based CCH for turn-exactness
2. Accept 4-7x OSRM gap as "cost of exact turns"
3. Close gap with **parallelism** (Milestone 7.5)
4. Can still achieve sub-20ms for 10×10 with 4-8 parallel searches

---

### Milestone 7.5: Parallel Bucket M2M ✅ IMPLEMENTED

**Implementation (2026-01-25):**

Added `table_bucket_parallel()` using rayon:
- Parallel forward phase: thread-local buckets merged at end
- Parallel sort: `par_sort_unstable_by_key`
- Parallel backward phase: atomic min updates to shared matrix

**Benchmark Results (Belgium, car mode, 20 threads):**

| Size | Sequential | Parallel | Speedup |
|------|------------|----------|---------|
| 10×10 | 20.8ms | 21.8ms | 0.95x (slower) |
| 25×25 | 51.8ms | 55.1ms | 0.94x (slower) |
| 50×50 | 107ms | 113ms | 0.95x (slower) |
| 100×100 | 221ms | **174ms** | **1.27x faster** |

**Analysis:**
- Small matrices hurt by thread-local SearchState allocation (19MB per thread)
- 100×100 shows 27% speedup from parallelism
- Overhead dominates for N×M < 2500

**TODO for better small-matrix performance:**
1. Thread pool with pre-allocated SearchState per worker
2. Only use parallel for N×M > threshold (e.g., 2500)
3. Fallback to sequential for small matrices

**Current Status:**
- Sequential 10×10: 20.8ms (vs OSRM 6ms) = 3.5x gap
- Sequential 100×100: 221ms (vs OSRM 35ms) = 6.3x gap
- With parallel 100×100: 174ms = 5.0x gap

**Parallel speedup is low due to:**
- Too fine-grained tasks (per-source, not per-block)
- Per-task SearchState allocation (~19MB per thread)
- Should chunk by source/target blocks (512-2048)
- Thread-local arenas for heaps, dist arrays, buckets

---

### Milestone 7.6: Node-Based CH + Junction Expansion ← NEXT PRIORITY

**Key Insight (2026-01-25):**

The 3-6x gap is NOT fundamentally unavoidable. It exists because we contract the full edge-state graph.

**The Smart Solution: Don't contract edge-states at all.**

Turns only matter at **junctions**. Between junctions, travel is edge-weight only.

**Architecture:**
1. Build **node-based CH** on NBG (1.9M nodes, not 5M edge-states)
2. At query time, handle exact turns via **local expansion at junctions**

**Two Exact Approaches:**

**A) Junction Expansion (recommended)**
- State = (node, incoming_edge) only at turn-relevant junctions
- Between junctions: simple node-to-node CH search
- At junction: expand to consider all legal outgoing edges with penalties
- Exact because CH distances are between correct entry/exit states

**B) Turn-Patch Overlay**
- Precompute node-based shortest paths ignoring turns
- Apply exact penalties via small correction graph at junctions
- Works when penalties are local (they are)

**Why This Will Work:**
- Node-based CH: 1.9M nodes, ~7 edges/node
- Edge-based CCH: 2.4M nodes, ~15 edges/node
- **2.7x less work per search** (matching OSRM architecture)
- Turns handled exactly via junction expansion (no approximation)

**Implementation Plan:**

1. ✅ **Analyze turn model** (COMPLETED 2026-01-25)
   - CLI command: `turn-model-analysis`
   - Analysis module: `src/analysis/turn_model.rs`

   **Key Findings (Belgium):**
   ```
   Turn Restriction Rules (from OSM relations):
     Car:  7,052 rules
     Bike: 3 rules
     Foot: 0 rules

   NBG Junction Analysis:
     Total NBG nodes:        1,907,139
     Multi-way (degree > 2): 1,267,821 (66.48%)
     With explicit restrict: 5,726 (0.30%)

   Turn Table Entries (deduplicated):
     Total: 15 entries (Ban: 5, Only: 3, Penalty: 0, None: 7)

   EBG Arc Analysis:
     Total arcs:        14,644,223
     With ban:          5,663 (0.04%)
     With penalty:      0 (0.00%)

   === CRITICAL INSIGHT ===
   TRUE turn-relevant junctions: 5,726 (0.30%)
   Turn-free junctions:          1,901,413 (99.70%)

   Note: U-turn bans (66.48% of junctions) are NOT turn-relevant!
   - U-turns are handled by search policy (don't reverse)
   - Only EXPLICIT OSM restrictions need junction expansion
   ```

   **Verdict: EXCELLENT for Node-Based CH + Junction Expansion**
   - Only 0.30% of junctions need expansion (5,726 / 1.9M)
   - Expected overhead: minimal (most searches never hit restricted junctions)
   - This validates the junction expansion approach!

2. 🔄 **Build node-based CH** ← IN PROGRESS
   - Module created: `src/nbg_ch/` (ordering.rs, contraction.rs, weights.rs)
   - CLI command: `build-nbg-ch`
   - Uses existing NBG from Step 3 (1.9M nodes, 2.5M edges)
   - **Issue**: Naive contraction (no witness search) is too slow
   - **Need**: Implement proper witness search to avoid unnecessary shortcuts
   - Expected: ~10M shortcuts with witness search (vs ~30M for EBG CCH)

3. ⬜ **Implement junction expansion**
   - At bucket M2M query time:
     - Forward search: at turn-relevant node, expand to (node, in_edge) states
     - Backward search: same expansion
   - Only expand at junctions with non-trivial turn costs

4. ⬜ **Benchmark**
   - Target: 10×10 < 10ms (match OSRM)
   - Target: 100×100 < 50ms

**This is the single most promising way to close the architectural gap without approximation.**

---

### Current Ordering Implementation (Step 6)

- `--graph-partition` flag added for BFS-based ordering (proven ineffective)
- Geometry-based inertial partitioning is current default
- Need to add `--force-late` constraint for densifier handling

---

### Completed Steps (Naive Hybrid - Proves Why Equivalence Classes Are Needed)

1. ✅ **Node Classification** (`is_complex(node)`):
   - 5,719 nodes classified as complex (0.30%)

2. ✅ **Build Naive Hybrid State Graph**:
   - Collapsed ALL incoming edges to single node-state per simple node
   - 1.9M states (2.62x reduction), but edges-per-node +36%
   - This proves naive collapse is provably the WORST strategy

3. ✅ **Step 6/7/8 Hybrid Pipeline** (infrastructure complete):
   - Coordinate extraction fixed
   - Contraction works (35M shortcuts)
   - Customization works (0% unreachable)
   - BUT 10% slower due to degree explosion

**Key Learning**: The naive hybrid result is NOT a failure - it's **scientific validation** that:
- Collapsing without behavior-equivalence invariant increases degree
- Degree increase dominates node reduction
- Equivalence-class hybrid is the ONLY viable approach

**Infrastructure ready for equivalence-class hybrid:**
- hybrid/builder.rs, hybrid/state_graph.rs
- formats/hybrid_state.rs
- Step 6/7/8 CLI commands
- Just need to change the collapse criterion from "same node" to "same behavior signature"

**Bug fixes applied (still valid):**
- Fixed weight indexing: use `weights[tgt_ebg]` not `weights[arc_idx]`
- Fixed coordinate extraction: map states to proper NBG node coordinates

---

### Milestone 7.5: Remaining Options (After Hybrid)

**After hybrid implementation:**
1. **Parallelism** - Linear speedup on already-efficient sequential algorithm
2. **CCH edge deduplication** - Remove dominated parallel arcs during customization
3. **Better contraction order** - Quality ordering → fewer shortcuts

**Deprioritized:**
- Stalling heuristics (0% stall rate, not applicable to edge-based)
- Hub labels (different paradigm, significant complexity)

---

## CRITICAL: OSRM Algorithm Analysis (2026-01-25)

**OSRM uses NO PARALLELISM in core matrix algorithm. We must match apples-to-apples.**

### Fundamental Architecture Difference

| Aspect | OSRM | Butterfly |
|--------|------|-----------|
| **Graph type** | Node-based | Edge-based (bidirectional edges) |
| **State** | Node ID | Directed edge ID |
| **Turn costs** | Approximated/ignored | Exact (edge→edge transitions) |
| **Graph size (Belgium)** | ~1.9M nodes | ~5M edge-states |
| **CH complexity** | Simpler | ~2.5x more states to search |

**This matters!** Edge-based CH has inherently more work per search.
We must be FASTER than OSRM despite the extra complexity. No excuses.

### OSRM many_to_many_ch.cpp Structure

```
1. Backward phase FIRST (sequential):
   for each target:
     run backward Dijkstra on CH
     store NodeBucket(node, target_idx, dist) in flat vector

2. Sort buckets by node ID (once)

3. Forward phase (sequential):
   for each source:
     run forward Dijkstra on CH
     for each popped node:
       binary_search buckets (std::equal_range)
       update matrix cells
```

### Why OSRM is Fast (NO parallelism needed)

1. **d-ary heap with DecreaseKey** - not lazy reinsert
   ```cpp
   heap.Insert(to, weight, parent);
   heap.DecreaseKey(*toHeapNode);  // O(log n), not O(n) duplicates
   ```

2. **O(1) visited check** via index storage
   ```cpp
   positions[node]  // direct array lookup, not version comparison
   ```

3. **Stall-on-demand** - checks OPPOSITE direction edges
   ```cpp
   // In forward search, check backward edges
   if (backward_neighbor.dist + edge.weight < current.dist)
       return true;  // stall
   ```

4. **Binary search** for bucket lookup
   ```cpp
   std::equal_range(buckets, node);  // O(log n)
   ```

### Current Butterfly Performance Gap

| Size | OSRM | Butterfly | Gap | Root Cause |
|------|------|-----------|-----|------------|
| 10×10 | 6ms | 19ms | 3.2x | Parallel overhead, O(n) offset build |
| 25×25 | 10ms | 14ms | 1.4x | Parallel overhead |
| 50×50 | 17ms | 21ms | 1.2x | Lazy heap, no proper stalling |
| 100×100 | 35ms | 36ms | 1.0x | At scale, algorithms converge |

### What We're Doing Wrong

1. **Parallelism as crutch** - adds overhead for small inputs
2. **Lazy reinsert heap** - duplicates in PQ waste time
3. **Version-based visited** - cache miss on every check
4. **Forward-then-backward** - OSRM does backward-then-forward
5. **O(n_nodes) offset array** - should use binary search like OSRM

### Immediate Fixes (Priority Order)

1. ✅ **Remove parallelism** - go fully sequential like OSRM
2. ✅ **Binary search buckets** - replace offset array with partition_point
3. ❌ **Backward-then-forward** - not needed, forward-then-backward is equivalent
4. ⚠️ **Proper heap** - d-ary heap with decrease-key ← **CRITICAL**
5. **Index storage** - O(1) visited lookup

### Critical Finding: 73% Stale Heap Entries → FIXED ✅

**Before (lazy reinsertion):**
```
pops=9891, stale=7276 (73%), pushes=9890
```
- 73% of heap pops were wasted on stale duplicates
- 4x more heap operations than necessary

**After (4-ary heap with DecreaseKey):**
```
pops=2500, stale=0 (0%), pushes=~20000
```
- 0% stale entries
- Each node inserted at most once
- DecreaseKey updates priority in-place

**Performance improvement:**
| Size | Before | After | Improvement |
|------|--------|-------|-------------|
| 10×10 | 32ms | 23.7ms | 26% faster |
| 100×100 | 328ms | 240ms | 27% faster |

**Remaining gap to OSRM (4x) is now mostly explained by edge-based CCH overhead (2.7x theoretical).**

---

#### Legacy Optimization Ideas (Deprioritized)

- [ ] **SIMD join**: Vectorize bucket join operations
- [ ] **Collective backward search**: Single pass instead of |T| Dijkstra runs

---

### Milestone 7.2: Integration & Strategy Switch

#### A) Add to `/matrix/bulk` endpoint

- [ ] Strategy selection based on N×M size:
  - **N×M ≤ 10,000**: Use bucket many-to-many (latency mode)
  - **N×M > 10,000**: Use tiled PHAST streaming (throughput mode)

- [ ] Response header indicating algorithm used

#### B) Performance monitoring

- [ ] Add counters:
  - Visited nodes per source (forward phase)
  - Visited nodes per target (backward phase)
  - Total bucket items
  - Total join operations

#### C) Memory controls

- [ ] Max bucket items limit (fail gracefully if exceeded)
- [ ] Max N×M in latency mode (switch to streaming if exceeded)

---

### Milestone 7.3: Parallel Processing (if needed)

#### A) Parallel forward phase

- Thread-local bucket arenas
- Merge after all sources processed (or keep separate and check all on read)

#### B) Parallel backward phase

- Process targets in parallel
- Each target updates disjoint matrix columns (no sync needed)

---

### Key Design Decisions

1. **Edge-based states**: Buckets keyed by rank-aligned node ID (which is edge-state in our CCH)

2. **Bucket storage**: Flat arena with offsets, not per-node vectors
   - Avoids 2.4M tiny allocations
   - Cache-friendly iteration

3. **Memory estimate** (100 sources):
   - ~10k visited nodes per source typical for CH
   - ~1M bucket items total
   - ~8MB for bucket arena (manageable)

---

### Milestone 6.5: Memory-Stable Isochrone Pipeline

For millions of isochrones, allocations must be constant.

#### A) Reuse buffers aggressively
- [ ] Grid buffer per worker (not per query)
- [ ] Flood-fill queue reused
- [ ] Simplification scratch reused

#### B) Batch origins
- [ ] Process origins in blocks of K (K-lane engine)
- [ ] Frontier + raster + contour per origin, constant allocations

#### C) Output format
- [ ] GeoJSON for human (single isochrone)
- [ ] Arrow for bulk: polygons in WKB + metadata
- [ ] OR raster masks for analytics use case

---

### Milestone 6.6: Correctness Guardrails at Scale

Protect against "fast but subtly wrong".

- [ ] Nightly validation runs:
  - Random origins (100+ per mode)
  - Random thresholds
  - All modes (car/bike/foot)
- [ ] Invariant tests:
  - Monotonic reachable sets: reachable(T1) ⊆ reachable(T2) for T1 < T2
  - Polygon contains reachable samples (within grid tolerance)
- [ ] CI integration for regression detection

---

## Deliverables for Phase 6

1. **Matrix tile throughput benchmark** (cells/sec)
2. **Isochrone batch benchmark** (isochrones/sec) for foot/bike/car with 5/10/15 min
3. **Memory bound guarantees** (max RSS under load)

---

## What NOT to Rush Into

- ❌ GPU acceleration (separate program, high complexity)
- ❌ Hub labeling (different algorithm entirely)
- ❌ Handwritten AVX2 (only after SoA/autovec and active gating)

---

## Completed Phases ✅

- [x] Phase 1: Lock Down Invariants
- [x] Phase 2: Exact Isochrone Core (Range Query)
- [x] Phase 3: Frontier Extraction + Polygonization
- [x] Phase 4: PHAST Performance Optimization
- [x] Phase 5: Rank-Aligned CCH (Cache-Friendly PHAST)

**Current Performance** (Belgium, car mode):
| Metric | Value |
|--------|-------|
| Single PHAST query | 39ms |
| K-lane batched (K=8) | 20.7ms effective |
| Batching speedup | 1.91x |
| CCH validation | 100% correct |
