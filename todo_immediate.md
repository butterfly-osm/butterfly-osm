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

## Phase 7: Many-to-Many CH for Matrix Queries ✅ IMPLEMENTED

### The Problem (Solved)

PHAST computes one-to-ALL distances, which is correct for isochrones but wasteful for sparse matrix queries where we only need specific source-target pairs.

| Query | OSRM | Butterfly PHAST | Butterfly Bucket M2M | Status |
|-------|------|-----------------|----------------------|--------|
| 50×50 | 65ms | 2,100ms | **100ms** | ✅ Target met |
| 100×100 | ~100ms | 2,112ms | **213ms** | ⚠️ Slightly over |

**21x improvement over PHAST, 1.5-2x gap to OSRM.**

---

### Milestone 7.1: Bucket Many-to-Many Algorithm ✅ COMPLETE

#### Algorithm (Verified Correct for Directed Graphs)

For directed graphs: **d(s → t) = min over m: d(s → m) + d(m → t)**

- **Source Phase**: Forward UP search → store `(source_idx, d(s→m))` in bucket[m]
- **Target Phase**: Reverse search via DownReverseAdj → join with buckets

#### Implementation

- [x] **SparseBuckets** - HashMap-based storage (no fixed capacity overflow)
- [x] **Forward search** - Dijkstra on UP graph, populate buckets
- [x] **Backward search** - Dijkstra on reversed DOWN graph via DownReverseAdj
- [x] **Parallel backward phase** - rayon parallel processing of targets
- [x] **Cache-friendly local columns** - each target writes to local Vec, merged at end

#### Validation ✅

- [x] Compared 5×5 M2M vs P2P queries: **All 25 queries match**
- [x] No bucket overflows with sparse HashMap approach

#### Benchmark Results (Belgium, car mode)

| Size | Time | Cells/sec | Target | Status |
|------|------|-----------|--------|--------|
| 10×10 | **18ms** | 5,438 | - | ✅ |
| 25×25 | **48ms** | 13,063 | - | ✅ |
| 50×50 | **87ms** | 28,693 | <100ms | ✅ **Met!** |
| 100×100 | **176ms** | 56,643 | <200ms | ✅ **Met!** |

---

### Milestone 7.2: Optimizations Applied ✅

#### Versioned Search State ✅ DONE

Avoid O(N) dist array initialization per backward search:
- Each thread has a VersionedSearchState with version counter
- Node is "unvisited" if its version doesn't match current search
- Start search is O(1) instead of O(N)
- **Result: 1.2-2.3x speedup**

#### Remaining Gap to OSRM

| Size | OSRM | Butterfly | Gap |
|------|------|-----------|-----|
| 50×50 | 65ms | **87ms** | 1.3x |
| 100×100 | ~100ms | **176ms** | 1.8x |

**Analysis**: Remaining gap is likely due to:
- Edge-based CCH (more complex than node-based)
- Turn-awareness overhead
- Some gap is inherent to the architecture

#### Further Optimization Ideas (Not Implemented)

- [ ] **Stalling**: OSRM uses stalling to prune search space
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
