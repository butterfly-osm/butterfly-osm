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
| Matrix (1×3) | < 30ms |
| **Isochrone (PHAST)** | |
| - Car 5 min | 92ms |
| - Bike 5 min | 261ms |
| - Foot 10 min | 290ms |

### Isochrone Pipeline ✅

```
PHAST distances → Base graph frontier → Stamp segments → Grid fill → Marching squares → Simplify
     (92-287ms)      (73-232 points)      (rasterize)     (close)       (contour)        (D-P)
```

Road-following concave envelope via grid + marching squares (not convex hull).

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

## Bulk Performance Optimization (Current Focus)

### Reality Checks

- **100k × 100k dense matrix** = 10¹⁰ cells = 40 GB @ 4 bytes/cell
  → Must tile/stream/distribute; never materialize in one request
- **Millions of isochrones** = can't do one PHAST per origin
  → Need K-lane batched PHAST or restricted scan

### Phase A: Measurement Infrastructure ← CURRENT

| Task | Status |
|------|--------|
| Benchmark harness (`bench/` binary) | ⬜ |
| Flamegraph + perf scripts | ⬜ |
| Counters: upward settled, downward scanned, relaxations, frontier edges | ⬜ |
| Baseline report per workload | ⬜ |

### Phase B: Batch Compute (Big Win)

**K-lane batched PHAST** (single most important optimization):
- Upward: K parallel searches
- Downward: one scan updating `dist[K]` vector per node
- Reduces `O(N × #sources)` scans to `O(N × #sources/K)`
- K = 8/16/32/64 (tune for cache/SIMD)

| Task | Status |
|------|--------|
| K-lane downward scan implementation | ⬜ |
| Matrix tile computation (64×256 blocks) | ⬜ |
| Batched isochrones (K origins per scan) | ⬜ |
| Active-set gating (rPHAST-lite) | ⬜ |

### Phase C: Arrow Streaming Output

| Task | Status |
|------|--------|
| Content negotiation (JSON default, Arrow for bulk) | ⬜ |
| Tiled block schema for matrices | ⬜ |
| Backpressure + cancellation (bounded channel) | ⬜ |
| Streaming writer for long-running queries | ⬜ |

### Phase D: Low-Level Optimizations

| Task | Status |
|------|--------|
| SoA layout for down-edges (`to[]`, `weight[]`, `first_out[]`) | ⬜ |
| Early termination: skip if `dist[v] > bound` | ⬜ |
| Reusable grid buffer with generation counters | ⬜ |
| Batch polygon simplification | ⬜ |

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
