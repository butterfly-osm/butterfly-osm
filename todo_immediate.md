# Immediate Roadmap: Bulk Engine Optimization

## Current Status

**Table API Location Fix (2026-02-01):** ✅ FIXED
- `/table` endpoint was returning `[0.0, 0.0]` for source/destination locations
- Bug: `get_node_location()` was using `poly_off` as an index, but polylines are indexed by edge index
- Fix: Use `edge_idx` directly to index into `polylines` Vec
- Now returns correct snapped coordinates (e.g., `[4.3498, 50.8503]` for Brussels)

---

**API Normalization Fix (2026-01-25):** ✅ CRITICAL FIX

The API endpoints were NOT using the optimized PHAST code due to coordinate space bugs:

**Before fix:**
- `/matrix` used N separate P2P queries (not PHAST)
- `/isochrone` used naive inline Dijkstra (not PHAST)
- `/route` passed filtered IDs to rank-indexed CCH (wrong results!)
- Matrix and route endpoints returned DIFFERENT distances for same origin-destination!

**After fix:**
- All endpoints properly convert: original → filtered → rank space
- `/matrix` now uses `compute_batched_matrix()` (K-lane PHAST)
- `/isochrone` now uses `run_phast_bounded()` (PHAST with threshold)
- `/route` correctly converts to rank space before CCH query
- `/matrix/bulk` and `/matrix/stream` also fixed for rank-aligned CCH

**Verification:**
```
Brussels → Leuven:
  Matrix endpoint:  22.0 min ✓
  Route endpoint:   22.0 min ✓ (was 38.4 min before fix!)
  OSRM:            30.3 min
```

---

**Turn Restriction Arc Filtering Fix (2026-01-25):** ✅ CRITICAL FIX

**Problem discovered:**
Butterfly was ~27-30% faster than OSRM on same routes. Investigation revealed:

1. Turn table has 15 unique entries with different `mode_mask` values
2. Arc-level mode_mask analysis (Belgium):
   - CAR allowed arcs: 4,336,787 (29.6%)
   - CAR banned arcs: 10,307,436 (70.4%) ← These are turns cars cannot make
3. **BUG**: `FilteredEbg::build()` only checked NODE accessibility, not ARC accessibility
   - If source and target nodes were car-accessible, the arc was included
   - But the arc's turn itself might be banned for cars!

**Fix implemented:**
- Added `FilteredEbg::build_with_arc_filter()` that checks BOTH:
  1. Source and target node accessibility (from node mask)
  2. Arc (turn) accessibility for this mode (from turn table mode_mask)
- Updated step5.rs to extract mode_masks from turn table and pass to new function

**Files changed:**
- `tools/butterfly-route/src/formats/filtered_ebg.rs` - New `build_with_arc_filter()` function
- `tools/butterfly-route/src/step5.rs` - Pass turn_idx and arc_mode_masks

**Verification:**
After rebuilding step5/6/7/8, turn restrictions are now properly enforced.

---

**Turn Penalty Cost Model (2026-01-31):** ✅ IMPLEMENTED

Implemented OSRM-compatible turn penalties using the exact sigmoid formula from car.lua.

**Results:**
- Brussels-Antwerp: 33.7 min (was 29 min without penalties)
- Gap vs OSRM: **~16% faster** (was 27% faster)
- Route distance: 32.9 km (was 37.6 km) - router now avoids left turns

**OSRM Configuration (from car.lua):**
- `turn_penalty = 7.5s` (max penalty for 180° turns via sigmoid)
- `turn_bias = 1.075` (right turns cheaper in right-hand traffic)
- `u_turn_penalty = 20s` (additional penalty for U-turns)
- Penalties only at intersections (degree >= 3)

**Files changed:**
- `src/ebg/turn_penalty.rs` - OSRM sigmoid formula implementation
- `src/ebg/mod.rs` - Integration into EBG construction
- `src/step5.rs` - Turn penalties applied from turn table
- `src/validate/step5.rs` - Updated validation

**Current state:**
- ✅ OSM turn restrictions (no_left_turn, only_straight_on, etc.) - ENFORCED
- ✅ U-turn bans at non-dead-ends - ENFORCED
- ✅ Mode-specific road access - ENFORCED
- ✅ Turn angle penalties (OSRM sigmoid) - ENFORCED
- ✅ Left/right asymmetry (turn_bias) - ENFORCED
- ✅ Traffic signal delays - IMPLEMENTED (8s for cars at signalized intersections)
- ✅ Road class transition penalties - IMPLEMENTED (0.5s per class diff, max 3s)

**Results with traffic signals (Brussels → Antwerp):**
- Duration: 34.9 min (was 33.7 min without signals)
- Distance: 32.9 km
- Gap vs OSRM: ~12.8% faster (OSRM ~40 min)

---

### Implementation Plan: Geometry-Based Turn Penalties

#### Step 1: Compute Turn Geometry (during EBG construction in step4)

For each arc/turn (u → v → w):

```rust
struct TurnGeometry {
    angle_deg: i16,       // Signed delta: wrap_to_180(bearing_out - bearing_in)
    turn_type: TurnType,  // Straight/Right/Left/UTurn (2 bits)
    via_has_signal: bool, // traffic_signals tag at via node
    via_degree: u8,       // in_degree + out_degree at via node
}

enum TurnType { Straight = 0, Right = 1, Left = 2, UTurn = 3 }
```

**Classification thresholds:**
- Straight: `|Δ| <= 30°`
- Right: `Δ > 30°` (right-hand traffic)
- Left: `Δ < -30°`
- U-turn: `|Δ| >= 170°`

**Bearing calculation:**
```rust
fn bearing(from: (f64, f64), to: (f64, f64)) -> f64 {
    let (lon1, lat1) = from;
    let (lon2, lat2) = to;
    let dlon = (lon2 - lon1).to_radians();
    let lat1 = lat1.to_radians();
    let lat2 = lat2.to_radians();
    let y = dlon.sin() * lat2.cos();
    let x = lat1.cos() * lat2.sin() - lat1.sin() * lat2.cos() * dlon.cos();
    y.atan2(x).to_degrees()
}

fn wrap_to_180(deg: f64) -> f64 {
    ((deg + 180.0) % 360.0) - 180.0
}
```

#### Step 2: Apply Turn Penalty Function (in step5)

**Turn penalty formula (seconds):**

```rust
fn turn_penalty_seconds(geom: &TurnGeometry) -> f64 {
    let angle = geom.angle_deg.abs() as f64;

    // Base by turn type
    let mut penalty = match geom.turn_type {
        TurnType::Straight => 0.0,
        TurnType::Right => 2.0 + 2.0 * ((angle - 30.0) / 120.0).clamp(0.0, 1.0),
        TurnType::Left => 6.0 + 4.0 * ((angle - 30.0) / 120.0).clamp(0.0, 1.0),
        TurnType::UTurn => 25.0,
    };

    // Traffic signal delay
    if geom.via_has_signal {
        penalty += 8.0;
    }

    // High-complexity intersection
    if geom.via_degree >= 6 {
        penalty += match geom.turn_type {
            TurnType::Left => 2.0,
            TurnType::Right => 1.0,
            _ => 0.0,
        };
    }

    penalty
}
```

**Expected penalties:**
| Turn Type | Base | Angle Adj | Signal | Complex | Total Range |
|-----------|------|-----------|--------|---------|-------------|
| Straight  | 0s   | 0         | +8s    | 0       | 0-8s        |
| Right     | 2s   | +0-2s     | +8s    | +1s     | 2-13s       |
| Left      | 6s   | +0-4s     | +8s    | +2s     | 6-20s       |
| U-turn    | 25s  | 0         | +8s    | 0       | 25-33s      |

#### Step 3: Data Flow

**Where to store turn geometry:**
- Option A: Extend `TurnEntry` in `ebg.turn_table` with geometry fields
- Option B: Separate `ebg.turn_geometry` file indexed by arc

**Where to apply:**
- In step5 when computing `penalties[arc_idx]`
- Turn penalty belongs on the **arc** (turn transition), not the node

#### Step 4: Profile Constants

Add to car profile (tunable):
```rust
pub struct TurnCostConfig {
    pub right_base_s: f64,     // 2.0
    pub left_base_s: f64,      // 6.0
    pub uturn_base_s: f64,     // 25.0
    pub signal_delay_s: f64,   // 8.0
    pub straight_threshold: f64, // 30.0 degrees
    pub uturn_threshold: f64,  // 170.0 degrees
}
```

---

### Files to Modify

1. **step4 (EBG construction)**: Compute turn angles from NBG geo coordinates
2. **formats/ebg_turn_table.rs**: Add `angle_deg`, `turn_type`, `via_has_signal`, `via_degree`
3. **step5.rs**: Apply `turn_penalty_seconds()` instead of just returning 0
4. **profiles/car.rs**: Add turn cost configuration constants

---

### Expected Impact

This should close most of the ~30% gap in urban routing:
- Turn costs dominate in cities (many intersections)
- Highway routes will see less impact (few turns)
- Signal delays add 8s per signalized intersection

**What this won't fix (last ~5-10%):**
- Profile-specific speed tables
- Surface/tracktype penalties
- Sliproad/link handling quirks

---

### Validation Plan

After implementation:
1. Re-run Brussels→Antwerp, Brussels→Leuven comparisons
2. Target: within 10% of OSRM (not 30%)
3. Urban routes should improve more than highway routes

---

Rank-aligned CCH (Version 2) implemented and validated:
- **Single PHAST**: 39ms per query (25.5 queries/sec)
- **K-lane batched (K=8)**: 20.7ms effective (48.3 queries/sec)
- **Batching speedup**: 1.91x
- **CCH validation**: 0 mismatches (100% correct)

**Sparse Contour Optimization (2026-01-25):** ✅ COMPLETE
- Moore-neighbor boundary tracing: O(perimeter) instead of O(area)
- Contour extraction: 1426x speedup (car), 5070x speedup (bike)
- End-to-end isochrone: 10.8x faster (80ms → 7.4ms for car 30-min)
- Contour now <1% of total time (was ~90%), PHAST dominates (89%)

**Early-Stop Upward Phase (2026-01-25):** ✅ COMPLETE

When heap minimum > threshold, stop upward search (exact, no approximation).

| Mode | Threshold | Single iso/sec | Notes |
|------|-----------|----------------|-------|
| **Car** | 5 min | **680/sec** | 1.5ms/query |
| **Car** | 10 min | **543/sec** | 1.8ms/query |
| **Car** | 30 min | **168/sec** | 6ms/query |
| **Bike** | 5 min | **351/sec** | 2.8ms/query |
| **Bike** | 30 min | **188/sec** | 5.3ms/query |

**Critical Finding: Early-stop changes optimal strategy**

| Threshold | Single (early-stop) | Batched K=8 | Winner |
|-----------|---------------------|-------------|--------|
| 5 min | 275/sec | 45/sec | **Single** |
| 30 min | 13/sec | 23/sec | **Batched** |

- Small thresholds: Single-source + early-stop wins (most upward work skipped)
- Large thresholds: K-lane batching wins (downward amortization helps)
- ✅ Early-stop added to batched PHAST (see below)

For 1M 5-min car isochrones: ~1 hour on single core, ~3 min with 20 cores.

**Batched Early-Stop (2026-01-25):** ✅ COMPLETE

Added `query_batch_bounded()` to BatchedPhastEngine with:
- Per-lane early-stop in upward phase (8 separate heaps, each stops when min > threshold)
- Active block tracking per lane (bitset marks which rank blocks have reachable nodes)
- Lane masking in downward scan (skip inactive lanes per block)

| Mode | Threshold | Single iso/sec | Batched iso/sec | Winner |
|------|-----------|----------------|-----------------|--------|
| **Car** | 5 min | 680/sec | 129/sec | **Single** |
| **Car** | 30 min | 168/sec | 200+/sec | **Batched** |

**Adaptive Isochrone Engine (2026-01-25):** ✅ COMPLETE

Created `AdaptiveIsochroneEngine` that auto-selects optimal algorithm:
- `ADAPTIVE_THRESHOLD_DS = 10000` (~17 min crossover point)
- Below threshold: Uses single-source PHAST with early-stop (Mode A)
- Above threshold: Uses K-lane batched PHAST with early-stop (Mode B)

```rust
impl AdaptiveIsochroneEngine {
    pub fn query_many(&self, origins: &[u32], threshold_ds: u32) -> Result<Vec<ContourResult>> {
        if threshold_ds < ADAPTIVE_THRESHOLD_DS {
            // Small threshold: single-source is faster
            self.query_single_batch(origins, threshold_ds)
        } else {
            // Large threshold: K-lane batching is faster
            self.query_batched(origins, threshold_ds)
        }
    }
}
```

**WKB Streaming (2026-01-25):** ✅ COMPLETE

Created `wkb_stream.rs` module for high-throughput isochrone output:
- `encode_polygon_wkb()`: Standard WKB polygon format (byte order, type, rings, points as f64)
- `IsochroneRecord`: Combines origin_id, threshold_ds, wkb, n_vertices, elapsed_us
- `IsochroneBatch`: Columnar storage for Arrow-friendly output
- `write_ndjson()`: Newline-delimited JSON with base64-encoded WKB

Output formats ready for GIS tools (PostGIS, QGIS, GeoPandas, Shapely).

**End-to-End Validation (2026-01-25):** ✅ COMPLETE

Full pipeline benchmark (compute + contour + WKB + serialize):

| Mode | Threshold | p50 | p95 | p99 | Throughput |
|------|-----------|-----|-----|-----|------------|
| **Car** | 5 min | 3.3ms | 7.6ms | 12ms | **260/sec** |
| **Car** | 30 min | 83ms | 177ms | 197ms | 11/sec |
| **Bike** | 10 min | 5.2ms | 23ms | 39ms | 137/sec |
| **Foot** | 5 min | 3.0ms | 4.3ms | 10ms | **314/sec** |

Time budget breakdown:
- Compute (PHAST + contour): **100%**
- WKB encoding: **<0.1%** (negligible)
- Serialization overhead: **~0%**

---

## Phase 8: Production Hardening ✅ MOSTLY COMPLETE

### 8.1 Pathological Origins Validation ✅ COMPLETE (2026-01-25)

Test worst-case scenarios across all modes - **all locations tested, all under 500ms**:

**Car Mode (2.4M filtered nodes):**
| Location | 5 min | 10 min | 30 min | 60 min | Vertices |
|----------|-------|--------|--------|--------|----------|
| Brussels Center | 7ms | 6ms | 65ms | 333ms | 53 |
| Antwerp Center | 3ms | 8ms | 54ms | 204ms | 16 |
| Ghent Center | 4ms | 8ms | 115ms | 419ms | 46 |
| Liège Center | 3ms | 17ms | 143ms | **473ms** | 58 |
| Charleroi | 5ms | 10ms | 68ms | 212ms | 17 |
| E40/E19 Junction | 3ms | 6ms | 46ms | 190ms | 21 |
| Ring Brussels S | 3ms | 5ms | 78ms | 332ms | 25 |
| Near Netherlands | 3ms | 5ms | 54ms | 209ms | 16 |
| Near France | 3ms | 7ms | 83ms | 343ms | 5 |

**Bike Mode (4.8M filtered nodes):**
| Location | 5 min | 10 min | 30 min | 60 min | Vertices |
|----------|-------|--------|--------|--------|----------|
| Brussels Center | 15ms | 17ms | 144ms | **264ms** | 23 |
| Antwerp Center | 3ms | 3ms | 18ms | 33ms | 12 |
| Ghent Center | 6ms | 14ms | 61ms | 100ms | 13 |
| Liège Center | 3ms | 3ms | 18ms | 45ms | 28 |
| Near Germany | 6ms | 8ms | 32ms | 72ms | 8 |
| Near France | 3ms | 5ms | 33ms | 111ms | 10 |

**Foot Mode (4.9M filtered nodes):**
| Location | 5 min | 10 min | 30 min | 60 min | Vertices |
|----------|-------|--------|--------|--------|----------|
| Brussels Center | 9ms | 4ms | 19ms | 23ms | 12 |
| Antwerp Center | 2ms | 2ms | 13ms | 15ms | 10 |
| Ghent Center | 2ms | 3ms | 16ms | 22ms | 30 |
| Charleroi | 3ms | 5ms | 23ms | **38ms** | 5 |
| Near Germany | 2ms | 2ms | 17ms | 26ms | 8 |

**Summary:**
| Mode | Worst Case | Threshold |
|------|------------|-----------|
| Car | 473ms | 60 min (Liège) |
| Bike | 264ms | 60 min (Brussels) |
| Foot | 38ms | 60 min (Charleroi) ✅ All <200ms |

**Valhalla Comparison (2026-01-25):**

| Threshold | Valhalla | Butterfly | Speedup |
|-----------|----------|-----------|---------|
| 5 min | 36ms | **4ms** | **9.5x faster** |
| 10 min | 63ms | **8ms** | **7.9x faster** |
| 30 min | 260ms | **78ms** | **3.3x faster** |
| 60 min | 737ms | **302ms** | **2.4x faster** |

**Butterfly beats Valhalla at all thresholds!**

Run comparison:
```bash
docker run -d --name valhalla_belgium -p 8002:8002 \
  -v "/home/snape/projects/routing/valhalla_tiles:/custom_files/valhalla_tiles" \
  ghcr.io/gis-ops/docker-valhalla/valhalla:latest
python3 scripts/valhalla_isochrone_bench.py
```

Run commands:
```bash
./target/release/butterfly-bench pathological-origins --data-dir ./data/belgium --mode car
./target/release/butterfly-bench pathological-origins --data-dir ./data/belgium --mode bike
./target/release/butterfly-bench pathological-origins --data-dir ./data/belgium --mode foot
```

### 8.2 Bulk Pipeline (10K Isochrones) ✅ COMPLETE (2026-01-25)

**Results (10,000 random origins, 5-min threshold, car mode):**
- Time: 33.5s
- Rate: **299 isochrones/sec**
- Valid: 100% (10,000/10,000)
- Total vertices: 64,718
- WKB output: 1,294 KB
- RSS growth: +19MB (stable, no memory leak)

Run command:
```bash
./target/release/butterfly-bench bulk-pipeline --data-dir ./data/belgium --mode car --threshold-ms 300000 --n-origins 10000
```

### 8.3 Polygon Output Stability ⬜ TODO

Deterministic output for production:
- [ ] Fixed epsilon simplification (meters, not adaptive)
- [ ] Consistent ring orientation (CCW outer, CW holes)
- [ ] Hole handling policy (configurable keep/remove)

**KNOWN ISSUE: Polygon Vertex Count (2026-01-25)**

Our sparse contour approach produces far fewer vertices than Valhalla:
- Butterfly: ~50-100 vertices (30-min car isochrone)
- Valhalla: ~3000-4000 vertices

Root cause: Grid-based rasterization fundamentally limits vertex count.
- We stamp roads on a binary grid, run morphology, trace boundary
- Boundary vertices only occur at grid cell corners (direction changes)
- Large isochrones form smooth blobs with few direction changes

Attempted solutions (none worked well):
- Smaller cells (5m): Only 94 vertices, 570ms compute time
- No morphology: Fewer vertices (blob doesn't form)
- Concave hull on frontier: Star-shaped, wrong topology
- Radial polygon: Urchin-shaped (crosses unreachable areas)

**What Valhalla does differently:**
- Computes travel time at grid points (continuous, not binary)
- Uses marching squares to interpolate contour positions
- Interpolation gives detailed vertices even with coarse grids

**Potential fix:** Implement marching squares on distance field
- Requires nearest-road lookup per grid point (expensive)
- Deferred for now, polygon quality acceptable for most use cases

### 8.4 Monotonicity Test ✅ COMPLETE (2026-01-25)

Automated regression tests:
- [x] Monotonicity: T1 < T2 ⇒ reachable(T1) ⊆ reachable(T2) **VERIFIED**
  - Tested: 100 origins × 6 thresholds (1.7min to 30min)
  - Tests: 500 threshold pairs
  - Violations: **0** (100% pass rate)

Run command:
```bash
./target/release/butterfly-bench monotonicity-test --data-dir ./data/belgium --mode car --n-origins 100
```

- [ ] Boundary points correspond to base-edge cutpoints within grid tolerance
- [ ] Cross-mode consistency checks

### 8.5 Throughput Scaling ✅ MEASURED (2026-02-01)

**Matrix 1000×1000 Scaling (bucket M2M):**
| Threads | Forward | Backward | Total | Speedup | Efficiency |
|---------|---------|----------|-------|---------|------------|
| 1 | 2128ms | 3753ms | 5916ms | 1.0x | 100% |
| 2 | 1239ms | 1976ms | 3249ms | 1.82x | 91% |
| 4 | 754ms | 1053ms | 1841ms | 3.21x | 80% |
| 8 | 661ms | 761ms | 1456ms | 4.06x | 51% |
| 12 | 745ms | 789ms | 1562ms | 3.79x | 32% |
| 16 | 745ms | 796ms | 1568ms | 3.77x | 24% |

**Key findings:**
- Good scaling up to 4 threads (80% efficiency)
- Diminishing returns beyond 8 threads (memory bandwidth limited)
- Optimal thread count: 4-8 for this workload
- Beyond 8 threads, cache contention hurts performance

**Isochrone Throughput (5-min threshold):**
- Single isochrone: ~3ms (306 iso/sec sequential)
- Bulk pipeline: 324 iso/sec
- No scaling benefit from more threads (each isochrone already fast)

**Memory Usage:**
- RSS delta for 1000 isochrones: ~19MB (stable, no leak)

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

## Phase 6: Bulk Engine ✅ CORE COMPLETE

Target: Best-in-class for one-to-many on CPU, scalable to bulk matrices + millions of isochrones

### Milestone 6.1: Generalize Beyond "car Belgium" ✅ VERIFIED (2026-02-01)

All modes work with rank-aligned CCH:

| Mode | Graph Size | Mean Latency | Throughput |
|------|------------|--------------|------------|
| Car | 2.4M nodes | 3.3ms | 306 iso/sec |
| Bike | 4.8M nodes | 4.3ms | 233 iso/sec |
| Foot | 4.9M nodes | 2.8ms | 356 iso/sec |

Rank-alignment benefits all modes. Foot is fastest despite largest graph
(simpler network, fewer edges per node).

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

## Phase 7: Many-to-Many CH for Matrix Queries ✅ COMPLETE - 1.4x vs OSRM

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

### Milestone 7.6: Node-Based CH + Junction Expansion ⚠️ DEPRIORITIZED

**Update (2026-02-01):** With SoA optimization, edge-based CCH is now **12% faster than OSRM**.
This milestone was designed to close the performance gap, but we've already exceeded OSRM.
NBG CH remains available for approximate queries (ignoring 0.3% turn restrictions).

**Original Key Insight (2026-01-25):**

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

2. ✅ **Build node-based CH** (COMPLETED 2026-01-25)
   - Module: `src/nbg_ch/` (ordering.rs, contraction.rs)
   - CLI command: `build-nbg-ch`
   - **Fully parallelized**: 2.4 seconds total build time
     - Ordering: 1.3s (parallel nested dissection)
     - Contraction: 0.7s (parallel batched shortcuts)

   **Results (Belgium):**
   ```
   NBG CH (node-based):
     Nodes:     1,907,139 (vs 5M for EBG)
     Shortcuts: 1,758,274 (vs ~30M for EBG CCH!)
     UP edges:  3,243,447
     DOWN edges: 3,276,342

   Comparison:
     EBG CCH: 5M nodes, ~30M shortcuts
     NBG CH:  1.9M nodes, 1.8M shortcuts
     → 2.6x fewer nodes, 17x fewer shortcuts!
   ```

   **Initial Benchmark (no optimizations):**
   ```
   NBG CH Bucket M2M (Belgium):
     10×10:   20ms (OSRM: 4ms)  - 5x gap
     25×25:   27ms (OSRM: 9ms)  - 3x gap
     50×50:   37ms (OSRM: 19ms) - 2x gap
     100×100: 56ms (OSRM: 35ms) - 1.6x gap
   ```

3. ✅ **Optimize NBG CH queries** (COMPLETED 2026-01-25)
   - Applied optimizations:
     - Flat adjacency structure (cache-friendly)
     - Version-stamped distances (O(1) reset)
     - Sorted buckets with binary search
     - Reusable search state (zero allocation)

   **🎉 WE BEAT OSRM BY 3-4x! 🎉**
   ```
   NBG CH Optimized (Belgium):
     10×10:   <1ms  (OSRM: 4ms)   - 4x FASTER!
     25×25:   2ms   (OSRM: 9ms)   - 4.5x FASTER!
     50×50:   6ms   (OSRM: 19ms)  - 3x FASTER!
     100×100: 9ms   (OSRM: 35ms)  - 4x FASTER!

   Journey:
     EBG CCH: 9-10x SLOWER than OSRM
     NBG CH:  3-4x FASTER than OSRM!
   ```

4. ⚠️ **Junction expansion infrastructure** ← IN PROGRESS
   - ✅ `TurnRestrictionIndex`: Loads turn rules, maps OSM→compact node IDs
   - ✅ `NbgEdgeWayMap`: Maps NBG edges (tail,head) to way_id for restriction checking
   - ✅ `is_turn_allowed()`: Checks ban/only restrictions at junctions
   - ✅ Unit tests passing

   **Challenge discovered**: CH shortcuts hide intermediate nodes
   - A shortcut u→w might pass through turn-relevant node v
   - Without unpacking, we can't check the turn at v
   - Full unpacking defeats the speed advantage

   **Current recommendation**:
   - NBG CH: Fast (3-4x faster than OSRM), approximate (ignores 0.3% restricted junctions)
   - EBG CCH: Slower (4-7x slower than OSRM), exact turn handling
   - Choose based on use case: analytics vs navigation

5. ⬜ **Full junction expansion** (optional future work)
   - Mark shortcuts that pass through restricted nodes during CH construction
   - Split such shortcuts at query time
   - Or use MLD-style approach with cell-level turn handling

**NBG CH already beats OSRM by 3-4x without junction expansion. The 0.3% restriction rate means
approximation error is acceptable for many use cases (logistics, analytics, coverage).**

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

---

## Benchmark Results (2026-01-31)

### 10000×10000 Matrix (100M distances)

| System | Algorithm | Time | Throughput | vs OSRM CH |
|--------|-----------|------|------------|------------|
| OSRM CH | Bucket M2M (node-based, tiled HTTP) | 41.6s | 2.4M/s | 1.0x |
| **Butterfly** | **Bucket M2M (edge-based, single)** | **50.2s** | **2.0M/s** | **1.2x slower** |
| OSRM MLD | MLD (tiled HTTP) | ~680s est | 0.15M/s | 16x slower |

### 500×500 Matrix (250K distances)

| System | Algorithm | Time | Throughput | vs OSRM CH |
|--------|-----------|------|------------|------------|
| OSRM CH | Bucket M2M | 215ms | 1.16M/s | 1.0x |
| Butterfly | Bucket M2M | 1,356ms | 184K/s | 6.3x slower |
| OSRM MLD | MLD | 5,224ms | 48K/s | 24x slower |

**Key finding**: At large scale (10k×10k), Butterfly bucket M2M is only 1.2x slower than OSRM CH.
The gap narrows because per-query overhead is amortized.


### Resource Comparison (10000×10000)

| System | Time | Throughput | CPU | RAM |
|--------|------|------------|-----|-----|
| OSRM CH | 36.7s | 2.73M/s | 758% | 928MB |
| Butterfly bucket M2M | 51.0s | 1.96M/s | 1538% | 2787MB |

**Analysis:**
- Performance gap: 1.39x slower
- CPU efficiency: OSRM achieves higher throughput with half the CPU cores
- RAM efficiency: OSRM uses 3x less memory
- Butterfly's backward phase (bucket joining) dominates: 44s of 51s total

**PHAST vs Bucket M2M:**
- PHAST (10000 single-source): 327s (just the upward/downward scans)
- Bucket M2M: 51s for full 10000×10000 matrix
- Bucket M2M is **~60x faster** than PHAST for matrix queries

**Conclusion:** Bucket M2M is the correct algorithm for /table. The 1.4x gap vs OSRM CH
is acceptable given exact turn handling. PHAST should only be used for isochrones.

---

### HTTP API: OSRM-Compatible Table Endpoint ✅ IMPLEMENTED (2026-01-31)

Replaced PHAST-based matrix endpoints with bucket M2M and OSRM-compatible routes.

**New Endpoints:**

1. **GET `/table/v1/{profile}/{coordinates}`** - OSRM-compatible format
   ```
   GET /table/v1/car/4.35,50.85;4.40,50.86;4.38,50.84?sources=0;1&destinations=2
   ```

2. **POST `/table`** - Alternative for large coordinate lists
   ```json
   {
     "sources": [[lon, lat], ...],
     "destinations": [[lon, lat], ...],
     "mode": "car"
   }
   ```

**Response format (OSRM-compatible):**
```json
{
  "code": "Ok",
  "durations": [[seconds or null, ...], ...],
  "sources": [{"location": [lon, lat], "name": ""}, ...],
  "destinations": [{"location": [lon, lat], "name": ""}, ...]
}
```

**Removed (PHAST-based, wrong algorithm for matrices):**
- `/matrix` - legacy one-to-many PHAST
- `/matrix/bulk` - batched PHAST
- `/matrix/stream` - streaming PHAST

**HTTP Benchmark Results (OSRM-compatible format):**

| Size | OSRM CH | Butterfly | Ratio |
|------|---------|-----------|-------|
| 10×10 | 16ms | 28ms | 1.7x slower |
| 25×25 | 25ms | 53ms | 2.1x slower |
| 50×50 | 43ms | 87ms | 2.0x slower |
| 100×100 | 72ms | 165ms | 2.3x slower |

**Analysis:**
- ~2x slower than OSRM CH is expected due to edge-based CCH overhead
- Much better than the previous 60x gap when using PHAST
- Algorithm selection is now correct:
  - **Bucket M2M** for matrices (sparse S×T queries)
  - **PHAST** for isochrones (need full distance field)

**Files changed:**
- `tools/butterfly-route/src/step9/api.rs` - New OSRM-compatible endpoints, removed PHAST matrix code
- `tools/butterfly-route/src/step9/state.rs` - Added flat adjacencies to ModeData

---

## Efficiency Optimization: Backward Join (2026-02-01) ✅ COMPLETE

### The Problem

We're at 1.39x slower than OSRM CH while being turn-exact. That's algorithmically excellent.
But we're paying **2x CPU** and **3x RAM** to get within 40%. That screams "memory traffic + join layout".

**Backward phase = 44s of 51s total (87% of time)**

The issue is NOT the algorithm choice - bucket M2M is correct. The issue is **efficiency**:
- 103 billion join operations for 10000×10000
- Binary search per settled node is O(log n) per lookup
- Scattered matrix writes cause cache thrashing
- Thread-local SearchState allocates 2.4M entries per target

### Root Cause Found: Binary Search Instead of O(1) Lookup

Current code (`table_bucket_parallel`):
```rust
// Line 1493: Uses SortedBuckets with binary search
let buckets = SortedBuckets::from_sorted(bucket_items);

// Line 1563: O(log n) per settled node!
for (source_idx, dist_to_source) in buckets.get(u) {
```

There's already a `PrefixSumBuckets` with O(1) lookup that's **not being used**:
```rust
fn get(&self, node: u32) -> &[BucketEntry] {
    // O(1) - direct array access
    let start = self.offsets[n] as usize;
    let len = self.counts[n] as usize;
    &self.items[start..start + len]
}
```

### Optimization Checklist

#### 1. Switch to PrefixSumBuckets (O(1) lookup) ✅ DONE
- [x] `PrefixSumBuckets` already implemented (lines 514-625)
- [x] `backward_join_prefix` already implemented (lines 1381-1426)
- [x] Changed `table_bucket_parallel` to use `PrefixSumBuckets`
- [x] Changed `table_bucket_full_flat` similarly

**Results (10000×10000):**
| Optimization | Time | Joins | vs Original |
|--------------|------|-------|-------------|
| Original (binary search) | 51.0s | 103B | baseline |
| + O(1) prefix-sum lookup | 47.5s | 103B | **-7%** |
| + Bound-aware pruning | **42.9s** | **61B** | **-16%** |

**Final breakdown (10000×10000):**
- Forward: 7.1s (unchanged)
- Build: 0.16s (prefix-sum)
- Backward: ~35s (was 44s)

**Results (100×100):**
| Before | After | Change |
|--------|-------|--------|
| 330ms | 187ms | **-43%** |

Bound-aware pruning skips 41% of joins by checking if `current_best <= entry.dist`
before computing the full distance. This is a significant win because we avoid
both the distance computation and the atomic min update for paths that can't improve.

#### 2. Structure-of-Arrays (SoA) for Buckets ✅ DONE
Original: 8 bytes per entry (4 bytes dist + 2 bytes source_idx + 2 bytes padding)

Changed to SoA layout:
```rust
struct PrefixSumBuckets {
    offsets: Vec<u32>,
    counts: Vec<u16>,
    dists: Vec<u32>,          // Contiguous distances
    source_indices: Vec<u16>, // Contiguous source indices
}
```

**Results (10000×10000):**
| Optimization | Time | vs OSRM | Change |
|--------------|------|---------|--------|
| Before SoA | 42.9s | 1.16x slower | baseline |
| **After SoA** | **32.4s** | **0.88x (12% FASTER!)** | **-24%** |

**Breakdown:**
- Forward: 7.0s (unchanged)
- Sort: 290ms
- Backward: 24.8s (was ~35s, **-29%**)

SoA improves cache efficiency in backward join loop:
- Distances are contiguous → better prefetching
- Source indices accessed separately → no cache pollution from padding

#### 3. Block result writes (cache optimization) ⬜ Deprioritized
Current: Scattered writes to `matrix[source_idx * n_targets + target_idx]`
Fix: Accumulate results in cache-friendly tiles, write blocked
**Status: Optional - 1.4x gap is acceptable for turn-exact routing**

#### 4. Reduce thread-local allocation ⬜ Deprioritized
Current: Each thread allocates 2.4M × 8 = 19.2MB for SearchState
Fix: Pool SearchState objects, reuse across targets within same thread
**Status: Optional - 1.4x gap is acceptable for turn-exact routing**

### Current Status ✅ 1.4x SLOWER THAN OSRM AT SCALE

**Fair HTTP Comparison (2026-02-01):**
| Size | OSRM CH | Butterfly | Ratio |
|------|---------|-----------|-------|
| 1000×1000 | 0.5s | 1.5s | 3.0x |
| 5000×5000 | 8.0s | 11.1s | **1.38x** |
| 10000×10000 | ~32s | ~44s | **~1.4x** |

**Optimization journey:**
- Original: 51s algorithm time (1.39x slower than OSRM)
- + O(1) prefix-sum lookup: -7%
- + Bound-aware pruning: -16%
- + SoA bucket layout: -24%
- **Final: 32.4s algorithm, ~44s via HTTP (1.4x slower than OSRM)**

**Key insight:** The gap closes at scale! At 5000×5000, Butterfly is only 1.38x slower.
This is excellent for an edge-based CCH with exact turn handling (vs OSRM's node-based CH).

**Correctness verified:** Sequential and parallel paths produce identical results, matching P2P queries.
