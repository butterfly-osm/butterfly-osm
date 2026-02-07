# Butterfly-Route: Architecture & Roadmap

## Goal

Build a routing engine with **exact turn-aware isochrones** and **OSRM-class speed** using:
- Edge-based graph (state = directed edge ID)
- Per-mode CCH preprocessing on filtered edge-based graphs
- Exact bounded Dijkstra on the hierarchy for all query types

**Key principle:** One graph, one hierarchy per mode, one query engine. Routes, matrices, and isochrones use identical cost semantics.

---

## Pipeline (All Steps Complete)

| Step | Output | Description |
|------|--------|-------------|
| 1 | `nodes.sa`, `nodes.si`, `ways.raw`, `relations.raw` | PBF ingest |
| 2 | `way_attrs.*.bin`, `turn_rules.*.bin` | Per-mode profiling (car/bike/foot) |
| 3 | `nbg.csr`, `nbg.geo`, `nbg.node_map` | Node-Based Graph (intermediate) |
| 4 | `ebg.nodes`, `ebg.csr`, `ebg.turn_table` | Edge-Based Graph (THE routing graph) |
| 5 | `w.*.u32`, `t.*.u32`, `mask.*.bitset`, `filtered.*.ebg` | Per-mode weights, masks, filtered EBGs |
| 6 | `order.{mode}.ebg` | Per-mode CCH ordering on filtered EBG |
| 7 | `cch.{mode}.topo` | Per-mode CCH contraction (shortcuts topology) |
| 8 | `cch.w.{mode}.u32`, `cch.d.{mode}.u32` | Per-mode customized weights (duration + distance) |
| 9 | HTTP server | Query server with all endpoints |

---

## Architecture: Per-Mode Filtered CCH

Each transport mode has its own CCH built on a **filtered subgraph** containing only mode-accessible nodes:

```
Original EBG (5M nodes)
    |
FilteredEbg (per mode)
    - Car:  2.4M nodes (49%)
    - Bike: 4.8M nodes (95%)
    - Foot: 4.9M nodes (98%)
    |
Per-mode CCH ordering -> order.{mode}.ebg
    |
Per-mode CCH topology -> cch.{mode}.topo
    |
Per-mode weights -> cch.w.{mode}.u32
```

**Why per-mode CCH?** A shared CCH on all nodes fails when some modes can't access certain nodes.

---

## Query Server Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /route` | P2P routing with geometry (polyline6/geojson/points), steps with road names, alternatives |
| `GET /nearest` | Snap to nearest road segments with distance |
| `POST /table` | Distance matrix with duration and/or distance (bucket M2M) |
| `POST /table/stream` | Arrow IPC streaming for large matrices (50k+) |
| `GET /isochrone` | Areal polygon + optional network roads, `direction=depart|arrive`, WKB via Accept header |
| `POST /isochrone/bulk` | Parallel batch isochrones (WKB stream) |
| `POST /trip` | TSP/trip optimization (nearest-neighbor + 2-opt + or-opt) |
| `GET /height` | Elevation lookup from SRTM DEM tiles |
| `GET /health` | Health check with uptime, node/edge counts, modes |
| `GET /metrics` | Prometheus metrics (per-endpoint latency histograms) |
| `GET /swagger-ui/` | OpenAPI documentation |

### Production Infrastructure

| Feature | Implementation |
|---------|---------------|
| Structured logging | `tracing` + `tracing-subscriber` (text/JSON via `--log-format`) |
| Graceful shutdown | SIGINT + SIGTERM handling |
| Request timeouts | 120s API, 600s streaming |
| Response compression | gzip + brotli (API routes only) |
| Input validation | Coordinate bounds, time_s 1-7200, number max 100 |
| Panic recovery | `CatchPanicLayer` (returns 500 JSON) |
| Docker | Multi-stage build (`rust:bookworm` -> `debian:bookworm-slim`) |

### Algorithm Selection

```
if query_type == isochrone:
    use PHAST (need all reachable nodes)
elif n_sources * n_targets <= 10_000:
    use Bucket M2M (sparse, low latency)
else:
    use K-lane batched PHAST + Arrow streaming (throughput)
```

---

## Performance (Belgium)

### Build Times

| Step | Time | Output Size |
|------|------|-------------|
| Step 6 (ordering) | ~3s per mode | 9-19 MB |
| Step 7 (contraction) | ~23s per mode | 200-350 MB |
| Step 8 (customization) | ~5s per mode | 180-230 MB |

### Query Performance

| Operation | Latency |
|-----------|---------|
| Server startup | ~25s (loading all data + 754K road names) |
| P2P route | < 10ms |
| Isochrone (30min, car) | 5ms p50 |
| Bulk isochrones | 1,526 iso/sec |
| Matrix 100x100 | 164ms |
| Matrix 10k x 10k (Arrow) | 18.2s (**1.8x FASTER than OSRM**) |

### vs OSRM (Fair HTTP Comparison)

| Size | OSRM CH | Butterfly | Ratio |
|------|---------|-----------|-------|
| 100x100 | 55ms | 164ms | 3x slower |
| 1000x1000 | 0.68s | 1.55s | 2.3x slower |
| 5000x5000 | 8.0s | 11.1s | 1.38x slower |
| 10000x10000 | 32.9s | **18.2s** | **1.8x FASTER** |

**Key insight:** Edge-based CCH has 2.5x more states than node-based (exact turn handling). The overhead is acceptable for small queries. **Butterfly wins at scale** due to Arrow streaming + parallel tiling.

### vs Valhalla (Isochrones)

| Threshold | Valhalla | Butterfly | Speedup |
|-----------|----------|-----------|---------|
| 5 min | 36ms | 4ms | **9.5x faster** |
| 10 min | 63ms | 8ms | **7.9x faster** |
| 30 min | 260ms | 78ms | **3.3x faster** |
| 60 min | 737ms | 302ms | **2.4x faster** |

---

## What NOT to Do

- Do not use node-based graphs for routing/isochrones
- Do not share a CCH across all modes (causes orphaned nodes)
- Do not approximate range queries
- Do not use different backends for different query types
- Do not snap differently for different APIs

---

## Deferred / Future Work

| Feature | Complexity | Notes |
|---------|------------|-------|
| Map matching (GPS trace -> route) | High | HMM-based, needs Viterbi on CCH |
| Two-resolution isochrone mask | Medium | Better boundary accuracy |
| Truck routing (dimensions) | High | Needs vehicle profile system |
| Time-dependent routing | Very High | Needs time-expanded graph |
| Hybrid exact turn model | Abandoned | Equivalence-class hybrid incompatible with CCH separator quality |

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
butterfly-route step6-order --filtered-ebg ./build/filtered.car.ebg --mode car -o ./build/
butterfly-route step7-contract --filtered-ebg ./build/filtered.car.ebg --order ./build/order.car.ebg --mode car -o ./build/
butterfly-route step8-customize --cch-topo ./build/cch.car.topo --mode car -o ./build/

# Query server (local)
butterfly-route serve --data-dir ./build/ --port 8080

# Query server (Docker, recommended)
docker build -t butterfly-route .
docker run -d --name butterfly -p 3001:8080 -v "${PWD}/data/belgium:/data" butterfly-route
```
