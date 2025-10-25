# How to Beat OSRM: The PHAST Architecture

**Target:** 2x faster than OSRM (<3ms vs 6ms) at planet scale with complete feature parity.

**Status:** Research phase - architecture designed, implementation not started.

**Research Date:** 2025-10-25

**Sources:** Brainstorming session with Gemini AI, analysis of OSRM/Valhalla/GraphHopper research docs.

---

## The Challenge

**Current Performance (Belgium, v0.5):**
- Single route: 750ms (125x slower than OSRM's 6ms)
- Matrix NxN: Not implemented
- Isochrones: Not implemented
- Memory: 800MB for Belgium

**The Gap:**
- Need to be **250x faster** to beat OSRM by 2x
- Need to implement missing features (matrix, isochrones)
- Need to work at planet scale (<64GB RAM)

**Why Simple Optimizations Won't Work:**
- Bloom filter for restrictions: 750ms → 500ms (1.5x)
- Bidirectional A*: 500ms → 200ms (2-3x)
- Better heuristics: 200ms → 150ms (1.3x)
- **Total: ~6x improvement = still 125ms** ❌
- Still 20x slower than OSRM!

**The Hard Truth:** Incremental optimizations can't close a 125x gap. We need a **fundamentally different architecture**.

---

## The Solution: PHAST

**PHAST** = **P**ublic **H**ierarchy **A**nd **S**mall-world **T**iles

A multi-level, tiled architecture that uses different algorithms at different scales.

### Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│  L2: Super-Core (Hub Labeling)                          │
│  - 50,000 nodes (continental backbone)                  │
│  - Hub Labels for instant lookups                       │
│  - <0.1ms queries between continents                    │
│  - Memory: ~21MB                                        │
└─────────────────────────────────────────────────────────┘
                         ▲
                         │
┌─────────────────────────────────────────────────────────┐
│  L1: Transit Network (Contraction Hierarchies)          │
│  - Major roads only (motorways, trunks, primary)        │
│  - 1-5% of planet graph                                 │
│  - CH preprocessing                                     │
│  - 1-2ms queries between cities                         │
│  - Memory: ~7.5GB                                       │
└─────────────────────────────────────────────────────────┘
                         ▲
                         │
┌─────────────────────────────────────────────────────────┐
│  L0: Local Tiles (Bidirectional A*)                     │
│  - 1×1 degree geographic tiles                          │
│  - Complete OSM data (all roads)                        │
│  - 100k-500k nodes per tile                             │
│  - Simple A* for intra-tile routing                     │
│  - 0.5-1ms local searches                               │
│  - Memory: Mapped on-demand                             │
└─────────────────────────────────────────────────────────┘
```

### Query Flow

**Example: Brussels → Antwerp (50km)**

1. **L0 Local Search (Start → Transit):** 0.5-1.0ms
   - Find nearest L1 transit nodes from Brussels
   - Bidirectional A* in small Brussels tile
   - Only needs to reach highway on-ramps

2. **L1 CH Query (Transit → Transit):** 1.0-2.0ms
   - Route between transit nodes on highway-only graph
   - CH query on graph that's 5% the size of planet
   - Dramatic speedup due to smaller search space

3. **L0 Local Search (Transit → End):** 0.5-1.0ms
   - Find path from highway exit to Antwerp destination
   - Bidirectional A* in small Antwerp tile

4. **Stitching:** <0.5ms
   - Combine the three path segments
   - Just data concatenation, not routing

**Total: 2.5-4.5ms** ✓ Beats OSRM's 6ms!

---

## Why This Beats OSRM

### OSRM's Approach
- **Single monolithic CH** on entire planet graph
- Query searches ONE giant contracted graph
- Implicit hierarchy via shortcuts
- Still needs to explore many nodes at all levels

### PHAST's Advantage
- **Explicit multi-level hierarchy** with specialized graphs
- Different algorithms optimized for each scale
- Separation of concerns

**The Key Insight:** CH query time is NOT linear with graph size.

A CH query on a graph that's 5% the original size is **MORE than 20x faster** (super-linear speedup).

PHAST runs CH on a 5% graph (L1), not the full 100% graph like OSRM.

### Fundamental Differences

| Aspect | OSRM | PHAST |
|--------|------|-------|
| **Hierarchy** | Implicit (shortcuts in one graph) | Explicit (separate graphs per level) |
| **Algorithm** | CH everywhere | A* (L0), CH (L1), HL (L2) |
| **Search Space** | Full contracted planet | Tiny tiles + 5% highway graph |
| **Optimization** | One-size-fits-all | Specialized per level |
| **Memory** | Monolithic | Tiled, on-demand |

---

## Feature Performance Targets

### 1. Single Route Queries

**Target:** <3ms (2x faster than OSRM's 6ms)

**Expected Performance:**
```
Local routes (<10km):     1-2ms   (mostly L0)
Regional routes (10-50km): 2.5-4.5ms (L0 + L1)
Long routes (>100km):     3-5ms   (L0 + L1 + L2)
Continental (>1000km):    2-4ms   (L1 + L2, L2 dominates)
```

**Status:** Credible based on theory and calculations.

### 2. Matrix NxN Queries

**Target:** <5s for 100×100 matrix (competitive with OSRM's 2s)

**Algorithm (NOT 10,000 independent queries!):**

1. **Origin Searches:** 100 × 1ms = 100ms
   - For each origin, find all reachable L1 transit nodes
   - Store (transit_node, distance) pairs

2. **Destination Searches:** 100 × 1ms = 100ms
   - For each destination, find all L1 transit nodes that can reach it
   - Store (transit_node, distance) pairs

3. **Core Matrix Calculation:** ~1.0s
   - **Many-to-many CH algorithm** on L1 graph
   - NOT 10k separate queries - specialized batched algorithm
   - Computes all transit-to-transit distances efficiently

4. **Combination:** <50ms
   - For each (origin, destination) pair:
   - Result = origin_to_transit + transit_to_transit + transit_to_destination
   - Simple array lookups

**Total: ~1.2s** ✓ Competitive with OSRM!

**Critical Optimization:** Batching and many-to-many algorithms. The naive approach (10k independent queries) would take 20-30 seconds.

### 3. Isochrones

**Target:** ~100-150ms for 30min reachability

**Algorithm:** (Needs refinement - Gemini hit rate limits)

Likely approach:
- Start Dijkstra expansion from origin
- Expand through L0 tile until hitting L1 transit nodes
- Continue expansion through L1 network
- Load additional L0 tiles on-demand as boundaries expand
- Stop when 30min budget exhausted

**Challenge:** Loading 10+ tiles during expansion
**Solution:** Memory-mapped tiles with prefetch/caching

**Status:** Theoretical, needs verification.

---

## Memory Budget (Planet Scale)

**Target:** <64GB RAM for full planet routing

**Breakdown:**

```
L2 Hub Labeling (Super-Core):
- 50,000 nodes
- ~35 hub labels per node (average)
- 12 bytes per label (node_id + distance)
- 50k × 35 × 12 = 21 MB
                        ✓ Minimal!

L1 Contraction Hierarchies (Transit Network):
- Planet raw graph: ~100GB
- L1 is 1-5% of planet: 5GB
- CH shortcuts add ~50% overhead: +2.5GB
- Total L1: 7.5 GB
                        ✓ Manageable!

L0 Tiles (Local Graphs):
- Memory-mapped on demand
- Keep ~10-20 hot tiles in RAM
- Typical tile: 50-200MB
- Hot set: 2-4 GB
                        ✓ Reasonable!

Total Core Memory: ~10-12GB
Total with hot tiles: ~14-18GB
                        ✓ Well under 64GB!
```

**Key Technique:** Memory-mapped L0 tiles. Only load what's needed.

---

## Algorithm Details

### RPHAST for Matrix Calculations

**RPHAST** = **R**obust **PHAST** - optimized variant for many-to-many distance matrices.

#### Core Optimization: Search Tree Reuse

Instead of running 10,000 independent CH queries, RPHAST leverages the fact that many queries share common structure:

**Standard CH approach (naive batching):**
```
For i in 1..100:
  For j in 1..100:
    CH_query(origin[i], dest[j])  // 10,000 separate searches
```

**RPHAST approach (search tree sharing):**
```
# Phase 1: Forward searches from all origins
forward_trees = []
For each origin in origins:
  tree = bidirectional_CH_search(origin, upward_only=True, no_target)
  forward_trees.append(tree)

# Phase 2: Backward searches from all destinations
backward_trees = []
For each dest in destinations:
  tree = bidirectional_CH_search(dest, downward_only=True, no_target)
  backward_trees.append(tree)

# Phase 3: Find meeting points
For i in 1..100:
  For j in 1..100:
    distance[i][j] = min_meeting_point(forward_trees[i], backward_trees[j])
```

**Key insight:** The forward search from an origin explores ALL paths upward through the hierarchy. This single search can be reused for ALL destinations.

#### Integration with Multi-Level Architecture

**For our L0/L1/L2 PHAST architecture:**

1. **L0 Batched Searches** (not RPHAST, just batching):
   - 100 origin → transit searches: `100 × 1ms = 100ms`
   - 100 dest → transit searches: `100 × 1ms = 100ms`
   - Produces: `origin_transit_distances[][]` and `dest_transit_distances[][]`

2. **L1 RPHAST on Transit Network**:
   - Build forward search trees from all origin transit nodes
   - Build backward search trees to all destination transit nodes
   - Find meeting points for all combinations
   - Time: `~500-800ms` (faster than 1000ms due to search reuse)

3. **Combination**:
   - `distance[i][j] = min over all transit pairs of:`
   - `  origin_to_transit[i][t1] + L1_distance[t1][t2] + transit_to_dest[t2][j]`
   - Time: `~50ms`

**Total: ~750ms-1.0s** (vs naive 20-30s!)

#### Performance vs Standard Batching

| Approach | 100×100 Matrix | 1000×1000 Matrix |
|----------|----------------|------------------|
| Naive (10k queries) | 20-30s | 200-300s |
| Batched L0+L1 | ~1.2s | ~15-20s |
| RPHAST | ~0.8-1.0s | ~8-12s |

RPHAST provides 20-30% speedup over batched approach through search tree reuse.

### Isochrone Polygonization

**Goal:** Convert discrete set of reachable nodes into continuous polygon.

#### Step 1: Compute Reachable Set (PHAST-based expansion)

Modified Dijkstra with time budget:

```
reachable = set()
pq = PriorityQueue()
pq.push((0, start_node))

while pq not empty:
  (time, node) = pq.pop()

  if time > time_budget:
    break

  if node in visited:
    continue

  visited.add(node)
  reachable.add(node)

  # Expand to neighbors (across L0 tiles and L1 network)
  for (neighbor, edge_time) in neighbors(node):
    new_time = time + edge_time
    if new_time <= time_budget:
      pq.push((new_time, neighbor))

      # Load tile on-demand if neighbor is in different tile
      ensure_tile_loaded(neighbor)
```

**Multi-level expansion:**
- Start in L0 tile
- When reaching L1 transit nodes, expand through L1 network quickly
- When L1 expansion hits new tiles, load those L0 tiles on-demand
- Continue until time budget exhausted

**Performance:**
- L0 expansion (local): ~20-30ms
- L1 expansion (highway network): ~30-50ms
- Tile loading overhead: ~10-20ms (if cold)
- **Total: ~60-100ms** for reachable set

#### Step 2: Marching Squares Polygonization

Convert discrete node set → continuous polygon boundary.

**Algorithm:**

1. **Create Grid:** Overlay 2D grid on map
   - Grid cell size: `250-500m` (tunable for accuracy vs speed)
   - For 30min driving (~50km radius): `200×200 cells = 40,000 cells`

2. **Classify Cells:** For each cell, determine if it's "inside" or "outside"
   ```
   for each cell in grid:
     center = cell.center_coords
     nearest_node = spatial_index.nearest(center)

     if nearest_node in reachable:
       cell.value = 1  // inside
     else:
       cell.value = 0  // outside
   ```
   - Using R-tree: `~1M lookups/sec`
   - 40,000 cells: `~40ms`

3. **March Squares:** For each 2×2 block of cells
   ```
   for each 2×2 block:
     # Create 4-bit index from corner values
     index = (NW << 3) | (NE << 2) | (SE << 1) | SW

     # Lookup contour line shape from table (16 cases)
     line_segments = marching_squares_table[index]

     # Add to polygon
     polygon.add_segments(line_segments)
   ```

4. **Interpolate:** Use linear interpolation to smooth polygon edges
   - For cells on boundary, estimate exact crossing point
   - Makes polygon less "blocky"

**Marching Squares Cases:**

```
Case 0 (0000): ····  (all outside, no line)
Case 1 (0001): ╰──   (SW corner inside)
Case 2 (0010): ──╯   (SE corner inside)
Case 3 (0011): ════  (S edge inside)
Case 15 (1111): ████ (all inside, no line)
... (16 total cases)
```

**Performance:**
- Cell classification: `~40ms` (40k cells, R-tree lookups)
- Marching squares: `~10ms` (simple table lookups)
- Polygon assembly: `~5ms`
- **Total: ~55ms**

#### Combined Isochrone Performance

```
Reachable set computation:  ~70ms  (PHAST expansion with tile loading)
Grid classification:        ~40ms  (R-tree nearest neighbor lookups)
Marching squares:          ~10ms  (contour line generation)
Polygon assembly:          ~5ms   (connect line segments)
──────────────────────────────────
TOTAL:                     ~125ms ✓ Within 100-150ms target!
```

**Optimizations:**
- Coarser grid (500m cells) for 30min isochrones: ~10,000 cells → ~60ms total polygonization
- Finer grid (100m cells) for 5min local isochrones: better accuracy, similar time (fewer cells)
- Parallel cell classification (trivially parallelizable)
- Adaptive grid resolution (fine near boundaries, coarse in interior)

**Grid Size Trade-offs:**

| Cell Size | 50km Radius Cells | Classification | Accuracy |
|-----------|-------------------|----------------|----------|
| 100m | 1,000,000 | ~1000ms | Excellent |
| 250m | 160,000 | ~160ms | Good |
| 500m | 40,000 | ~40ms | Acceptable |
| 1000m | 10,000 | ~10ms | Rough |

**Recommended:** 250m cells for good balance (~125ms total).

### Why Multi-Level Helps Isochrones

**Naive single-level approach:**
- Expand through entire OSM graph
- Load full Belgium dataset into RAM
- Explore all 8M nodes within time budget
- Slow due to massive search space

**PHAST multi-level approach:**
- **L0 local expansion**: Only load tiles as needed (not all 8M nodes!)
- **L1 highway expansion**: Quickly reach distant areas via highway network
- **Hybrid**: Expand locally in L0, jump between regions via L1
- Much smaller active search space at any time

**Example: 30min isochrone from Brussels:**
1. Expand locally in Brussels tile (L0): ~5km radius, 50k nodes
2. Hit highway entry points, expand via L1 network: 50km range, 200k nodes
3. Load destination tiles on-demand: Only ~10 tiles, not all of Belgium
4. Total nodes visited: ~250k (vs 8M if loading full dataset)

**Performance gain: ~30x fewer nodes explored**

---

## Implementation Roadmap

**Total Estimate: 4-6 months of focused work**

### Phase 1: Tiling System (2-3 weeks)
- Implement 1×1 degree tile partitioning
- PBF parser to extract tile data
- Tile serialization format
- Memory-mapped tile loading

**Deliverable:** Can partition Belgium into tiles, load on-demand

### Phase 2: L0 Local Routing (2 weeks)
- Bidirectional A* within tiles
- Tile boundary handling
- R-tree spatial index per tile

**Deliverable:** Intra-tile routing works, ~1-2ms for local queries

### Phase 3: L1 Transit Network (4-6 weeks)
**This is the hard part!**

- Transit node selection algorithm
  - Identify major highways (motorway, trunk, primary)
  - Select important junctions/intersections
  - Build L1-only graph (1-5% of full graph)

- Contraction Hierarchies implementation
  - Node ordering heuristics
  - Edge difference calculation
  - Lazy witness search
  - Shortcut creation
  - Contracted graph storage

- Turn restrictions integration
  - Apply restrictions during preprocessing
  - Remove illegal edges from L1 graph

**Deliverable:** L1 CH working, queries in 1-2ms on highway network

### Phase 4: L0/L1 Integration (2-3 weeks)
- Identify L0→L1 connection points
- Multi-level query algorithm
- Path stitching logic
- End-to-end testing

**Deliverable:** Brussels → Antwerp in 2.5-4.5ms

### Phase 5: Matrix NxN (2 weeks)
- Batched origin/destination searches
- Many-to-many CH algorithm
- Result combination logic

**Deliverable:** 100×100 matrix in <5s

### Phase 6: Isochrones (2-3 weeks)
- Multi-level Dijkstra expansion
- Tile loading during expansion
- Polygon generation from reachable nodes

**Deliverable:** 30min isochrone in ~100-150ms

### Phase 7: L2 Hub Labeling (3-4 weeks) *Optional*
- Landmark selection for hub labels
- Label preprocessing
- Hub label query algorithm
- Integration with L1

**Deliverable:** Continental queries <1ms

### Phase 8: Multi-Modal (3-4 weeks) *Future*
- Bike/foot speed profiles
- Mode-specific routing
- GTFS transit integration

**Deliverable:** Car, bike, foot, transit routing

### Phase 9: Optimization & Testing (4-6 weeks)
- Performance profiling
- SIMD optimizations
- Cache-friendly data structures
- Planet-scale testing
- Comparison benchmarks vs OSRM/Valhalla/GraphHopper

**Deliverable:** Production-ready, documented, benchmarked

---

## Technical Deep Dives

### Why L1 CH is Faster than Full CH

**OSRM:** CH on 1 billion nodes
- Contraction creates shortcuts
- Query explores contracted hierarchy
- Still visits thousands of nodes
- Search space: Full planet

**PHAST L1:** CH on 50 million nodes (5% of planet)
- Same CH algorithm
- But 95% smaller graph!
- Search space: Just highways

**Speedup is super-linear:**
- Graph half the size ≠ 2x faster
- Graph half the size ≈ 4-5x faster (due to smaller search cones)
- Graph 5% the size ≈ 50-100x faster!

But we don't get the full 100x because we still need L0 searches (adds ~1-2ms overhead).

Net result: ~2-3x faster than OSRM's monolithic CH.

### Why Tiling Helps Memory

**Monolithic approach:**
- Load entire planet graph: 100GB RAM
- Keep everything in memory
- Fast but requires huge RAM

**Tiled approach:**
- Core hierarchy (L1+L2): ~8GB
- L0 tiles: Load on-demand
- Keep 10-20 hot tiles: ~3-5GB
- Total: ~12-15GB

**Trade-off:**
- Tile loading adds latency (~10-50ms for cold tile)
- But 99% of queries hit hot tiles
- Enables planet-scale on normal workstation

### Matrix Batching Math

**Naive approach:**
```
For each of 100 origins:
  For each of 100 destinations:
    L0 search (origin → transit): 0.5ms
    L1 CH query: 1.0ms
    L0 search (transit → destination): 0.5ms

Total: 10,000 × 2ms = 20 seconds ❌
```

**Batched approach:**
```
# Phase 1: Origins
For each of 100 origins (parallel):
  L0 search to find ALL reachable transit nodes: 1ms
  Store: {transit_node: distance} map
Total: 100ms

# Phase 2: Destinations
For each of 100 destinations (parallel):
  L0 search to find ALL transit nodes that can reach it: 1ms
  Store: {transit_node: distance} map
Total: 100ms

# Phase 3: Core
Many-to-many CH on L1:
  Input: Set of origin transit nodes, set of destination transit nodes
  Output: Full distance matrix between them
  Algorithm: Modified bidirectional CH, shares search trees
Total: 1000ms

# Phase 4: Combine
For i in 1..100, j in 1..100:
  distance[i][j] = origin_map[i][transit] + core_matrix[transit][transit] + dest_map[transit][j]
  (Find minimum over all transit node combinations)
Total: 50ms

Grand Total: 1.25 seconds ✓
```

The 16x speedup comes from:
1. Amortizing L0 searches (100 instead of 10,000)
2. Shared search trees in many-to-many algorithm

---

## Open Questions

### 1. Isochrone Implementation Details
- Exact algorithm for multi-level expansion
- Tile loading performance during expansion
- Polygon generation from mixed L0/L1 expansion

**Need to research:** How does Valhalla handle tiled isochrones?

### 2. L1 Transit Node Selection
- What percentage of nodes should be transit nodes?
- Highway-only vs highway + major primary roads?
- How to handle dense urban areas?

**Need to research:** OSRM's node selection heuristics.

### 3. Optimal Tile Size
- 1×1 degree tiles = ~100-500k nodes
- Smaller tiles: Less memory, more tile boundaries
- Larger tiles: More memory, fewer boundaries

**Need to test:** Belgium with different tile sizes.

### 4. Turn Restrictions in Multi-Level Graph
- Apply during L1 preprocessing?
- Check at query time?
- Store at tile boundaries?

**Need to design:** Turn restriction handling in L0/L1 integration.

### 5. Real-World Performance
- Will L0 searches really be <1ms on real hardware?
- How much does memory-mapped tile loading slow things down?
- What's the actual cache hit rate for hot tiles?

**Need to benchmark:** Prototype and measure.

---

## Alternative Approaches Considered

### 1. Single-Level CH (Like OSRM)
- **Pros:** Simpler to implement, proven
- **Cons:** Matches OSRM, doesn't beat it
- **Verdict:** Gets us to parity, not superiority

### 2. Pure Hub Labeling (No Tiling)
- **Pros:** 0.5-2ms queries, extremely fast
- **Cons:** 60-80GB RAM for planet, complex preprocessing
- **Verdict:** Memory requirements too high for our goal

### 3. Transit Node Routing Only
- **Pros:** <1ms for long routes
- **Cons:** Poor for local routes, needs fallback
- **Verdict:** Good for L2, not complete solution

### 4. Valhalla-Style Tiling + Bidirectional A*
- **Pros:** Simpler than CH, good features
- **Cons:** 8-20ms queries, doesn't beat OSRM
- **Verdict:** Feature-rich but not fast enough

**Conclusion:** PHAST combines the best of all approaches.

---

## Success Criteria

### Must Have (v1.0)
- ✅ Single route queries: <3ms average (2x faster than OSRM)
- ✅ Matrix 100×100: <5s
- ✅ Isochrones 30min: <200ms
- ✅ Planet scale: Works on 64GB workstation
- ✅ Multi-modal: Car, bike, foot

### Nice to Have (v1.1+)
- ✅ Matrix 1000×1000: <2 minutes
- ✅ Alternative routes (k-shortest paths)
- ✅ Turn-by-turn instructions
- ✅ GTFS transit integration
- ✅ Traffic updates (dynamic weights)

### Stretch Goals (v2.0)
- ✅ GPU acceleration for matrix queries
- ✅ Distributed deployment (multiple machines)
- ✅ Real-time traffic integration
- ✅ Elevation-aware routing
- ✅ <1ms queries with L2 optimizations

---

## Risk Assessment

### High Risk
1. **L1 CH may not be fast enough**
   - Mitigation: Aggressive node ordering optimization
   - Fallback: Accept 4-5ms instead of 3ms

2. **Memory-mapped tiles may be too slow**
   - Mitigation: Prefetching, larger hot cache
   - Fallback: Require more RAM (32GB → 64GB)

3. **Isochrone tile loading overhead**
   - Mitigation: Smart prefetch based on expansion direction
   - Fallback: Accept 200ms instead of 100ms

### Medium Risk
1. **Turn restrictions in multi-level graph**
   - Mitigation: Apply during preprocessing
   - Fallback: Check at query time (slower but correct)

2. **Tile boundary handling**
   - Mitigation: Careful stitching logic
   - Fallback: Larger tiles with more overlap

3. **Many-to-many CH algorithm complexity**
   - Mitigation: Study OSRM's implementation
   - Fallback: Optimized batching of individual queries

### Low Risk
1. **L2 Hub Labeling implementation**
   - It's optional, we can skip it
   - L1 alone should get us to target

2. **Multi-modal routing**
   - Nice to have, not critical for beating OSRM
   - Can be added later

---

## Decision Points

### Now (Phase 1)
**Decision:** Start with tiling system
- **Why:** Foundation for everything else
- **Risk:** Low - well understood problem
- **Timeline:** 2-3 weeks

### Month 2 (Phase 3)
**Decision:** L1 transit node selection strategy
- **Options:** Highway-only vs highway + major primary
- **Test:** Belgium prototype with both approaches
- **Choose:** Based on performance/memory trade-off

### Month 3 (Phase 4)
**Decision:** Proceed with L2 Hub Labeling?
- **Criteria:** Are we already at <3ms without it?
- **If Yes:** Skip L2, focus on features (matrix, isochrones)
- **If No:** Implement L2 for the final speedup

### Month 4-5 (Phase 8)
**Decision:** Optimization strategy
- **Options:** SIMD, GPU, better data structures
- **Choose:** Based on profiling results
- **Goal:** Close remaining gap to 3ms target

---

## Conclusion

**The PHAST architecture provides a credible path to beating OSRM by 2x.**

**Key innovations:**
1. Explicit multi-level hierarchy (not implicit like OSRM)
2. Different algorithms per level (A*, CH, HL)
3. Super-linear speedup from 95% graph reduction
4. Memory-efficient tiling for planet scale

**Expected results:**
- Single queries: 2.5-4.5ms (vs OSRM's 6ms) ✓
- Matrix 100×100: ~1.2s (vs OSRM's 2s) ✓
- Memory: ~12-15GB (vs OSRM's ~30-40GB) ✓
- Planet scale: ✓

**Timeline:** 4-6 months to full implementation

**Risk:** Medium - requires correct implementation of complex algorithms, but theory is sound

**Next step:** Build tiling system and validate assumptions with Belgium prototype.

---

**References:**
- OSRM.md - Research on Contraction Hierarchies and MLD
- Valhalla.md - Research on tiled architecture and bidirectional A*
- GraphHopper.md - Research on Landmarks/ALT and flexible routing
- Gemini brainstorming session (2025-10-25)

**Last Updated:** 2025-10-25
