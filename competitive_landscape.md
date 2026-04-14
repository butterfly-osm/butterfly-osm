# Competitive Landscape: Routing Engines

> Compiled 2026-03-20. Goal: understand every competitor to supersede them all.

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Primary Competitors (Big 3)](#primary-competitors)
   - [OSRM](#1-osrm-open-source-routing-machine)
   - [Valhalla](#2-valhalla)
   - [GraphHopper](#3-graphhopper)
3. [Feature Matrix: Butterfly vs Big 3](#feature-matrix)
4. [Secondary Open-Source Engines](#secondary-open-source-engines)
5. [Commercial Routing APIs](#commercial-routing-apis)
6. [VRP & Optimization Engines](#vrp--optimization-engines)
7. [Transit/Multimodal Engines](#transitmultimodal-engines)
8. [Algorithm Landscape](#algorithm-landscape)
9. [Performance Benchmarks](#performance-benchmarks)
10. [Strategic Gap Analysis](#strategic-gap-analysis)

---

## Executive Summary

The routing engine ecosystem has three tiers:

**Tier 1 — Direct competitors** (open-source, self-hosted road routing):
- **OSRM** (C++, CH/MLD) — fastest P2P queries, largest community, de facto standard
- **Valhalla** (C++, bidirectional A*) — most flexible, best mode coverage, tiled architecture
- **GraphHopper** (Java, CH/LM) — best commercial ecosystem, VRP, custom models

**Tier 2 — Adjacent engines** (specialized or complementary):
- OpenRouteService (ORS) — GraphHopper fork, most complete all-in-one
- VROOM / OR-Tools / jsprit — VRP optimization (consume our matrices)
- OpenTripPlanner / MOTIS / R5 — transit/multimodal (different problem domain)
- pgRouting / BRouter / Routino — niche use cases

**Tier 3 — Commercial APIs** (proprietary data moats):
- Google Maps, HERE, TomTom — live traffic, toll costs, EV routing, massive matrix scale
- Mapbox — OSRM-based with traffic layer and mobile SDKs
- Azure Maps, AWS Location Service — cloud platform integration

**Butterfly's unique position:** The only engine with edge-based CCH (exact turn-aware routing), Arrow-streamed 50k x 50k matrices, PHAST-based isochrones at 5ms/query, and declarative JSON model profiles — all in Rust with zero GC overhead.

---

## Primary Competitors

### 1. OSRM (Open Source Routing Machine)

| Attribute | Detail |
|-----------|--------|
| **URL** | https://github.com/Project-OSRM/osrm-backend |
| **License** | BSD-2-Clause |
| **Language** | C++14, Boost, TBB |
| **GitHub Stars** | ~6,500 |
| **Latest** | v5.27.1 (2024) |

#### Architecture
- **Graph type**: Node-based (intersections as vertices, road segments as edges)
- **Two algorithms**:
  - **CH (Contraction Hierarchies)**: Fastest queries (~0.1ms Europe), but non-customizable. Full re-preprocess for weight changes.
  - **MLD (Multi-Level Dijkstra)**: Customizable, supports live traffic updates, slightly slower queries (~0.3ms). Based on CRP (Customizable Route Planning).
- **Shared memory**: `osrm-datastore` loads data once, multiple processes share via shared memory segments
- **Profiles**: Lua scripts define speed, access, turn penalties. Extremely flexible but requires full re-extract for profile changes.

#### API Endpoints
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/route/v1/{profile}/{coords}` | GET | Point-to-point routing |
| `/table/v1/{profile}/{coords}` | GET | Distance/duration matrix |
| `/nearest/v1/{profile}/{coords}` | GET | Snap to nearest road |
| `/match/v1/{profile}/{coords}` | GET | Map matching (HMM-based) |
| `/trip/v1/{profile}/{coords}` | GET | TSP round-trip optimization |
| `/tile/v1/{profile}/{z}/{x}/{y}` | GET | Mapbox Vector Tiles of road network |

#### Features
- **Turn-by-turn**: Full maneuver instructions with road names, refs, exit numbers
- **Alternatives**: Up to 3 alternative routes
- **Annotations**: Speed, duration, distance, weight, nodes, datasources per segment
- **Bearings**: Per-waypoint bearing filter (angle,range)
- **Radiuses**: Per-waypoint snap radius
- **Approaches**: `curb` or `unrestricted` per waypoint
- **Exclude**: Classes defined in profile (e.g., `motorway`, `toll`, `ferry`)
- **Waypoints**: Max 100 for route, unlimited for nearest
- **Geometry**: Polyline (5 or 6 precision), GeoJSON
- **Response codes**: `Ok`, `InvalidValue`, `NoSegment`, `NoRoute`, `TooBig`

#### Matrix Capabilities
- **Algorithm**: Bucket CH with d-ary heap, proper decrease-key, stall-on-demand, binary search bucket lookup
- **Performance**: ~32s for 10k x 10k on Belgium (node-based CH)
- **Limitation**: GET request with coordinates in URL — practical limit ~10k points due to URL length
- **No streaming**: Entire result in single JSON response
- **No parallelism**: Core algorithm is sequential
- **Turn restrictions**: Ignored in matrix computation (node-based CH)

#### Isochrone Support
- **None natively**. Requires external tools (e.g., `galton`, `osrm-isochrone`, or Valhalla).

#### Custom Profiles (Lua)
```lua
-- Example: car.lua
function way_function(way, result)
  local highway = way:get_value_by_key("highway")
  if highway == "motorway" then
    result.forward_speed = 90
    result.backward_speed = 90
  end
end
```
- Full access to all OSM tags
- Can define: speeds, access, turn penalties, classes, restrictions
- Changing profile requires full re-extract + re-contract

#### Traffic / Time-Dependent
- **CH**: No support. Full re-preprocess required.
- **MLD**: Can update edge weights via `osrm-customize` without full re-preprocess. Re-customization takes seconds.
- No built-in traffic data ingestion — users must provide updated weight files.

#### Preprocessing (Planet scale)
| Phase | CH | MLD |
|-------|-----|-----|
| Extract | ~3h | ~3h |
| Contract/Partition | ~5h | ~minutes |
| Customize | N/A | ~seconds |
| **Total** | **~8h** | **~3.5h** |
| Disk | ~250 GB | ~220 GB |
| RAM | ~50 GB | ~50 GB |

#### Strengths
- Fastest P2P queries of any open-source engine
- Fastest small-scale matrices (Bucket CH is optimal for N*M < 10k)
- Largest community, most battle-tested in production
- Lua profiles are extremely flexible
- MLD enables live traffic updates
- MVT tile endpoint for visualization

#### Weaknesses
- **No isochrones** — critical gap
- **Node-based CH**: Turn restrictions are approximated or ignored in matrix
- **No streaming/bulk APIs** — can't handle 50k x 50k matrices
- **GET-only API**: URL length limits for large requests
- **No Arrow/binary output formats** — JSON only (plus polyline)
- **Preprocessing is monolithic**: CH requires hours for any weight change
- **No declarative profiles**: Lua requires programming skill
- **Memory hungry**: Planet preprocessing needs ~50 GB RAM, 250 GB disk
- **C++ complexity**: Contributing is difficult, build system is complex

---

### 2. Valhalla

| Attribute | Detail |
|-----------|--------|
| **URL** | https://github.com/valhalla/valhalla |
| **License** | MIT |
| **Language** | C++17, Protocol Buffers |
| **GitHub Stars** | ~4,600 |
| **Latest** | Active development (no versioned releases) |

#### Architecture
- **Modular design**: Named after Norse concepts — Midgard (geometry), Baldr (graph tiles), Sif (costing), Thor (pathfinding), Odin (narrative), Loki (snapping), Meili (map matching), Mjolnir (tile building), Skadi (elevation), Tyr (API gateway)
- **Tiled hierarchical graph**: 3 road levels + 1 transit level
  - Level 0 (Highway): 4-degree tiles — motorway, trunk, primary
  - Level 1 (Arterial): 1-degree tiles — secondary, tertiary
  - Level 2 (Local): 0.25-degree tiles — residential, service (densest)
  - Level 3 (Transit): GTFS-only edges
- **No contraction hierarchies**: Uses bidirectional A* with hierarchy pruning. Deliberate trade-off: **flexibility over speed**. Costs never baked into graph.
- **Dynamic costing**: `DynamicCost` plugin per mode, 4 virtual methods: `Allowed(node)`, `Allowed(edge)`, `EdgeCost()`, `TransitionCost()`
- **Tile format**: Custom binary (`.gph`), LZ4 compressed, lazy decompression. Median tile ~100KB. GraphId: 4-byte compact `tileid:22|level:3|id:7`. LRU cache (default 1GB). Supports HTTP remote tile fetching.
- **Planet tiles**: 70-140 GB uncompressed. Germany: ~4.6 GB.

#### Algorithms
- **Bidirectional A***: Default for P2P routing
- **Unidirectional A***: For time-dependent routing (bidirectional invalid with time-varying weights)
- **Dijkstra expansion**: For isochrones
- **Matrix**: Time-distance matrix via Dijkstra-based expansion (no CH speedup)

#### Transport Modes (Most Extensive)
| Mode | Costing Model |
|------|---------------|
| `auto` | Standard car |
| `truck` | HGV with height/width/weight/length/hazmat |
| `taxi` | Taxi with HOV access |
| `bus` | Bus with bus lane access |
| `bicycle` | Bicycle with surface/grade awareness |
| `bikeshare` | Bicycle with docking stations |
| `motor_scooter` | Motorized scooter |
| `motorcycle` | Motorcycle |
| `pedestrian` | Walking |
| `low_speed_vehicle` | Golf cart / NEV |
| `multimodal` | Transit + walk/bike (GTFS integration) |

#### API Endpoints
| Endpoint | Description |
|----------|-------------|
| `/route` | P2P routing with OSRM-compatible or Valhalla-native output |
| `/sources_to_targets` | Distance/time matrix |
| `/isochrone` | Reachability polygons (time/distance, multiple contours) |
| `/trace_route` | Map matching (Meili algorithm) |
| `/trace_attributes` | Extract road attributes along GPS trace |
| `/locate` | Snap to road with rich metadata |
| `/height` | Elevation lookup (SRTM/ASTER) |
| `/expansion` | Visualize routing expansion as GeoJSON |
| `/centroid` | Find optimal meeting point for N locations |
| `/optimized_route` | TSP optimization |
| `/status` | Health check |

#### Features
- **Costing options**: Per-request JSON overrides for speed, penalties, restrictions
  - `use_highways`, `use_tolls`, `use_ferry` (0.0-1.0 preference)
  - `height`, `width`, `length`, `weight`, `axle_load` for trucks
  - `hazmat`, `tunnel_category` for hazmat routing
  - `top_speed`, `use_hills` for bicycles
  - `walking_speed`, `walkway_factor` for pedestrians
- **Time-dependent routing**: `date_time` parameter with departure/arrival time
- **Avoid areas**: GeoJSON polygons to avoid
- **Elevation**: Built-in SRTM/ASTER integration, grade-weighted routing for bikes
- **OSRM-compatible output**: Can produce OSRM-format JSON responses (`format=osrm`)
- **Expansion API**: Visualize ALL edges visited during pathfinding as GeoJSON (unique, can be hundreds of MB)
- **Centroid API**: Find optimal meeting point for N locations (unique)
- **Trace attributes**: 50+ per-edge attributes extractable from GPS traces (way_id, speed_limit, surface, lanes, grade, etc.)
- **Locate API**: Rich graph metadata at coordinates (node/edge IDs, access per mode, admin info)
- **Lane guidance**: `turn_lanes` with direction/valid/active bitmasks
- **OpenLR references**: Base64-encoded linear references per edge
- **Recostings**: Calculate time under alternative costing parameters without re-routing
- **GeoTIFF isochrone output**: Raster grid format
- **Output formats**: JSON, OSRM JSON, GPX, Protocol Buffers, GeoJSON, GeoTIFF
- **30+ languages** for turn-by-turn instructions
- **Speed data**: 4 tiers — current (real-time), predicted (weekly patterns, 5-min buckets DCT-II compressed), freeflow, constrained

#### Isochrone Capabilities
- **Algorithm**: Dijkstra expansion on tiled graph, "isotile" grid approach
- **Contours**: Multiple time/distance contours in single request
- **Output**: GeoJSON polygons or linestrings
- **Performance**: ~200-500ms typical (much slower than PHAST-based approaches)
- **Reverse**: Supports arrive-by isochrones

#### Matrix
- **Two algorithms**: CostMatrix (bidirectional, for auto/truck) and TimeDistanceMatrix (unidirectional Dijkstra, for ped/bike)
- **Default limit**: 2,500 location pairs (configurable)
- **Performance**: 33x1 matrix: Valhalla ~2,095ms vs OSRM ~10ms (**210x slower**). 175x175 exceeds 60s timeout on Germany. 3,855x3,855 takes ~10 min. 12k+ crashes.
- **Memory**: 100 locations across Germany peaks at 6.8 GB, 400 locations at 13 GB. Memory not released after large queries.
- **Maintainer acknowledged**: "achieving OSRM-level matrix speeds remains unachievable" — realistic improvement estimate "20-30%"
- **Advantage**: Supports all costing models including time-dependent

#### Transit / Multimodal
- **GTFS integration**: Can route with public transit schedules
- **Multimodal**: Combine transit legs with walking/cycling access
- **Real-time**: Supports GTFS-RT for live departure updates

#### Strengths
- **Most transport modes** of any open-source engine (12+)
- **Most flexible** costing system — per-request customization with zero preprocessing
- **Time-dependent routing** — the only major OSS engine with real departure-time support
- **Isochrone support** — native, multi-contour, time and distance
- **Transit routing** — GTFS + real-time
- **Elevation-aware** — grade-weighted bicycle/pedestrian routing
- **Expansion API** — unique debugging/visualization tool
- **Tiled architecture** — designed for mobile/offline use
- **OSRM-compatible output** — drop-in replacement for some use cases

#### Weaknesses
- **No contraction hierarchies**: Queries are 10-100x slower than OSRM/GraphHopper/Butterfly for long routes
- **Slow matrices**: 210x slower than OSRM at 33x1. Memory leaks: 572x572 consumes 12+ GB, not released
- **No streaming APIs**: Can't handle very large matrices (12k+ crashes)
- **No Arrow/binary output**: JSON only (plus GPX/PBF)
- **BidirectionalAStar does NOT correctly handle time-dependent traffic** — only UnidirectionalAStar works
- **Complex codebase**: ~145K lines of C++, steep contribution barrier. 766 open issues, ~100 open PRs
- **Isochrones are slow**: Full Dijkstra expansion (~200-500ms) vs Butterfly's PHAST (5ms)
- **Memory per request**: +1.2 GB per concurrent matrix query on Germany
- **Startup time**: 10+ minutes for large datasets
- **Planet preprocessing**: 1.5-2 days
- **No custom tag interpretation**: Costing options are predefined C++ classes — adding new OSM tag logic requires C++ changes
- **Scaling**: Maintainers acknowledge "struggling with the scaling problem since the project's creation"

---

### 3. GraphHopper

| Attribute | Detail |
|-----------|--------|
| **URL** | https://github.com/graphhopper/graphhopper |
| **License** | Apache 2.0 (open source), commercial for Directions API |
| **Language** | Java 17+ |
| **GitHub Stars** | ~4,200 |
| **Latest** | v11.0 (October 2025) |

#### Architecture
- **Graph**: Node-based with tower nodes (junctions) and pillar nodes (geometry). Adjacency as linked list per node.
- **Three routing modes**:
  - **Speed mode (CH)**: Contraction Hierarchies. Fastest queries (~1ms), no per-request flexibility.
  - **Hybrid mode (LM)**: Landmarks + A* + Triangle Inequality. Moderate speed (~10-50ms), per-request custom model adjustments (restricted: multiply_by in [0,1]).
  - **Flexible mode**: Dijkstra/A*. Full flexibility, slowest (~100-500ms+).
- **Encoded values**: Road attributes packed into compact flag structures. Extensive: `road_class`, `surface`, `smoothness`, `toll`, `max_speed`, `max_weight`, `hazmat`, `hazmat_tunnel`, `lanes`, `curvature`, `hike_rating`, `mtb_rating`, etc.
- **Turn costs**: Node-based CH (fast, no turn costs) or edge-based CH (supports turn restrictions, ~8x slower preprocessing).

#### Transport Modes (Open Source)
`car`, `bike`, `racingbike`, `mtb`, `foot`, `hike`, `truck`, `bus`, `motorcycle`, `car4wd`, `wheelchair`

Commercial adds: `small_truck`, `scooter`, `ecargobike`

#### API Endpoints

**Open Source:**
| Endpoint | Description |
|----------|-------------|
| `/route` | P2P routing (GET/POST) |
| `/isochrone` | Reachability polygons |
| `/spt` | Shortest path tree (raw points) |
| `/match` | Map matching (GPX input, HMM/Viterbi) |
| `/navigate` | Navigation with custom models (v11+) |
| `/info` | Health/version/profiles |

**Commercial only:**
| Endpoint | Description |
|----------|-------------|
| `/matrix` | Distance/time matrix |
| `/optimize` | VRP (jsprit-based) |
| `/cluster` | Capacity clustering |
| `/geocode` | Forward/reverse geocoding |

#### Custom Models (JSON) — "Still Beta"
```json
{
  "speed": [
    {"if": "road_class == MOTORWAY", "multiply_by": 0.8},
    {"if": "surface == GRAVEL", "limit_to": 30}
  ],
  "priority": [
    {"if": "toll == ALL", "multiply_by": 0.1}
  ],
  "distance_influence": 70,
  "areas": { "type": "FeatureCollection", "features": [...] }
}
```
- Requires `ch.disable: true` (POST only)
- LM mode: `multiply_by` restricted to [0, 1], `distance_influence` cannot decrease
- Flexible mode: Full unrestricted

#### Matrix (Commercial Only)
- **Algorithm**: CH-based
- **Performance**: 10k x 10k in <5 minutes, >350k routes/sec
- **Limits**: Free: 5 locations, Basic: 30, Standard: 80, Premium: 200, Custom: 10,000

#### Isochrone
- Algorithm: Shortest Path Tree (Dijkstra) + triangulation for polygon
- Multiple rings via `buckets` parameter
- `reverse_flow` for arrive-by

#### VRP / Route Optimization (Commercial Only)
- Based on **jsprit** (ruin-and-recreate metaheuristic)
- Capabilities: TSP, CVRP, VRPTW, PDP, multi-depot, driver skills, time windows, multi-dimensional capacity
- Vehicle types: cargo bikes, scooters, delivery trucks, large trucks

#### Preprocessing (Planet scale)
| Operation | Time | Memory |
|-----------|------|--------|
| Base graph import | ~1 hour | — |
| CH (car, with turn restrictions) | ~25 hours | 120 GB heap |
| CH (car, no turn restrictions) | ~3 hours | — |
| LM (car) | ~3.5 hours | 60 GB heap |

#### Strengths
- **Richest commercial ecosystem**: VRP, clustering, geocoding, traffic (TomTom)
- **Custom models**: JSON-based per-request customization (similar to Butterfly's approach)
- **Most transport modes** in open source (11+)
- **Extensive encoded values**: curvature, hike_rating, mtb_rating, lanes, hazmat
- **Map matching**: HMM/Viterbi (same algorithm as Butterfly)
- **Path details**: Per-segment road class, speed limits, surface, etc.
- **Java**: Easier to extend than C++ for most developers
- **Motorcycle routing**: Kurviger-powered profile that prefers curves/slopes

#### Weaknesses
- **Matrix is commercial-only** — huge gap for open-source users
- **Edge-based CH is extremely slow**: ~25 hours for planet (vs ~3 hours node-based)
- **Java GC overhead**: Latency spikes, 120 GB heap for planet
- **Custom models are beta**: Potential breaking changes
- **LM restrictions**: Per-request adjustments limited (multiply_by [0,1] only)
- **CH mode inflexible**: No headings, no pass_through, no custom models
- **Node-based graph**: Same turn restriction limitations as OSRM
- **No streaming APIs**: JSON only, no Arrow
- **No binary output formats**

---

## Feature Matrix: Butterfly vs Big 3

| Feature | Butterfly | OSRM | Valhalla | GraphHopper |
|---------|:---------:|:----:|:--------:|:-----------:|
| **Core Algorithm** | Edge-based CCH | Node-based CH/MLD | Bidirectional A* | Node-based CH/LM |
| **Turn Restrictions** | Exact (edge-based) | Approx (node-based) | Native (in A*) | Edge-based CH (slow) |
| **P2P Query** | ~0.14ms | ~0.11ms | ~10-100ms | ~1-5ms (CH) |
| **Language** | Rust | C++ | C++ | Java |
| **License** | — | BSD-2 | MIT | Apache 2.0 |
| | | | | |
| **P2P Routing** | Yes | Yes | Yes | Yes |
| **Distance Matrix** | Yes (OSS) | Yes | Yes (slow) | Commercial only |
| **Streaming Matrix** | 50k x 50k Arrow | No | No | No |
| **Isochrone** | Yes (5ms PHAST) | No | Yes (200-500ms) | Yes (Dijkstra) |
| **Bulk Isochrone** | Yes (1526/sec) | No | No | No |
| **Map Matching** | Yes (HMM) | Yes (HMM) | Yes (Meili) | Yes (HMM) |
| **TSP/Trip** | Yes | Yes | Yes | Commercial |
| **Nearest** | Yes | Yes | Yes | — |
| **Elevation** | Yes (SRTM) | No | Yes (SRTM) | Yes (multi-source) |
| **Health/Metrics** | Yes + Prometheus | No | /status | /info |
| **OpenAPI/Swagger** | Yes | No | No | Yes (commercial) |
| | | | | |
| **Car** | Yes | Yes | Yes | Yes |
| **Bike** | Yes | Yes | Yes | Yes (4 variants) |
| **Foot** | Yes | Yes | Yes | Yes (2 variants) |
| **Truck** | Yes (model) | No | Yes | Yes |
| **Bus** | Add model JSON | No | Yes | Yes |
| **Motorcycle** | Add model JSON | No | Yes | Yes |
| **Scooter** | Add model JSON | No | Yes | Commercial |
| **Wheelchair** | No | No | No | Yes |
| **Transit** | No | No | Yes (GTFS) | Yes (GTFS, limited) |
| | | | | |
| **Custom Profiles** | JSON models | Lua scripts | Costing options | JSON models (beta) |
| **Avoid Areas** | Yes (R-tree + CCH) | No | Yes | Yes (custom model) |
| **Exclude toll/ferry** | Yes (CCH reconfig) | Via profile | Costing options | Custom model |
| **Bearing Hints** | Yes | Yes | No | Hybrid/Flex only |
| **Alternatives** | Yes (max 5) | Yes (max 3) | Yes | Yes |
| **Annotations** | speed/dist/dur/nodes | speed/weight/nodes/etc | No | path_details |
| **Steps/Instructions** | Yes + road names | Yes | Yes (2 formats) | Yes (45 languages) |
| **Multiple Contours** | Yes | N/A | Yes | Yes (buckets) |
| **Isodistance** | Yes | N/A | No | Yes |
| **Reverse Isochrone** | Yes | N/A | Yes | Yes |
| **Network Isochrone** | Yes (primal) | N/A | No | No |
| | | | | |
| **Time-Dependent** | No | MLD only | Yes | Commercial (TomTom) |
| **Live Traffic** | CCH recustomize (~1s) | MLD customize | Runtime costing | Commercial |
| **Transit/Multimodal** | No | No | Yes | Limited |
| **Toll Costs** | No | No | No | No |
| **EV Routing** | No | No | No | No |
| | | | | |
| **Output: JSON** | Yes | Yes | Yes | Yes |
| **Output: Polyline6** | Yes | Yes | Yes | Yes (encoded) |
| **Output: GeoJSON** | Yes | Yes | Yes | Yes |
| **Output: WKB** | Yes (isochrone) | No | No | No |
| **Output: Arrow IPC** | Yes (matrix) | No | No | No |
| **Output: GPX** | No | No | No | Yes |
| | | | | |
| **Concurrency Limits** | Yes (32 regular, 4 stream) | No | No | No |
| **Request Timeouts** | Yes (120s/600s) | No | No | Configurable |
| **Compression** | gzip + brotli | No | No | No |
| **Graceful Shutdown** | Yes (SIGTERM) | No | No | No |
| **Prometheus Metrics** | Yes | No | No | No |
| **Structured Logging** | Yes (JSON/text) | No | No | Yes (Dropwizard) |

---

## Secondary Open-Source Engines

### OpenRouteService (ORS)
- **Based on**: GraphHopper 4.0 fork (Java)
- **Stars**: 1,800 | **License**: GPL-3.0 | **Status**: Very active (University of Heidelberg)
- **Unique**: Wheelchair routing, green routing (favors parks), avoid-countries, QGIS plugin
- **Profiles**: car, truck, bike (road/mountain/electric), foot, hiking, wheelchair
- **APIs**: directions, isochrones, matrix, snap, export + VROOM integration for VRP
- **Limitation**: 128 GB+ RAM per profile recommended. Inherits GraphHopper CH restrictions.

### BRouter
- **Focus**: Bicycle routing with unmatched profile customization
- **Stars**: 635 | **License**: MIT | **Language**: Java
- **Unique**: Custom `.brf` profile scripting, kinetic energy model with elevation, offline Android
- **Limitation**: No CH/CCH. No matrices. No isochrones. Slow for long routes.

### pgRouting
- **Focus**: In-database routing via PostgreSQL/PostGIS
- **Stars**: 1,400 | **License**: GPL-2.0 | **Language**: C++
- **Unique**: SQL-defined cost functions, no preprocessing, instant data changes, works with any network type
- **Algorithms**: Dijkstra, A*, K-shortest paths, TSP, driving distance
- **Limitation**: Slow without preprocessing. No HTTP API. No turn-by-turn.

### Routino
- **Focus**: Lightweight self-contained routing
- **Stars**: N/A (SVN) | **License**: AGPL | **Language**: C
- **Unique**: Pure C, no dependencies, 10 transport modes including wheelchair/HGV/PSV
- **Limitation**: Basic A*/Dijkstra, solo developer, no matrices/isochrones.

### RoutingKit
- **Focus**: Academic reference implementation of CH/CCH
- **Stars**: 412 | **License**: BSD-2 | **Language**: C++
- **Unique**: THE reference CCH implementation from KIT (inventors of CCH). Clean API. InertialFlowCutter integration.
- **Limitation**: Library only, no HTTP server, no profiles. Low maintenance.

### rust_road_router (KIT)
- **Focus**: Rust CCH research code
- **Stars**: 38 | **License**: BSD-3 | **Language**: Rust
- **Unique**: Only other Rust CCH implementation. Time-dependent routing (TDPOT). From KIT researchers.
- **Limitation**: Dead since October 2022. No documentation. 38 stars.

### fast_paths
- **Stars**: ~200 | **License**: Apache 2.0 | **Language**: Rust
- **Unique**: Clean, simple CH library in Rust. By GraphHopper developer.
- **Limitation**: CH only (no CCH). Library only.

### Itinero
- **Stars**: 232 | **License**: Apache 2.0 | **Language**: C#
- **Unique**: Only .NET routing library. Lua profiles. Transit via Linked Connections.
- **Limitation**: Small community. No formal releases. .NET only.

### libosmscout
- **Stars**: 297 | **License**: LGPL | **Language**: C++20
- **Unique**: Combined map rendering + routing. Multi-platform (Android, iOS, Linux, Windows).
- **Limitation**: Basic routing algorithms. Primarily a rendering library.

### OsmAnd
- **Stars**: 5,400 | **License**: GPL | **Language**: Java
- **Unique**: Premier offline mobile navigation app. Custom Highway Hierarchy (100x speedup over bidi A*).
- **Limitation**: Mobile app, not a server-side engine. No API for external integration.

---

## Commercial Routing APIs

### Cross-Cutting Comparison

| Feature | Google | HERE | TomTom | Mapbox | Azure | AWS | Stadia | Geoapify |
|---------|--------|------|--------|--------|-------|-----|--------|----------|
| **Car** | Yes | Yes | Yes | Yes | Yes | Yes | Yes | Yes |
| **Truck** | No | Yes | Yes | No | Yes | Yes | Yes | Yes (6 types) |
| **Bicycle** | Beta | Yes | No | Yes | No | No | Yes | Yes |
| **Transit** | Yes | Yes | No | No | No | No | No | Yes |
| **Motorcycle** | Beta | Yes | Yes | No | No | Yes | Yes | Yes |
| **Matrix max** | 625 | 100M | 100M | 625 | 50K | 122K | 10K | 1K |
| **Isochrone** | No | Yes | Yes | Yes | Yes | Yes | Yes | Yes |
| **Live traffic** | Yes (ML) | Yes | Yes | Yes | Yes | Yes | No | No |
| **Toll costs** | No | Yes | No | No | No | Yes | No | No |
| **EV routing** | No | Yes | Yes | Beta | No | No | No | Yes |
| **Hazmat** | No | Yes | Yes | No | Yes | Partial | Yes | Yes |

### Pricing (per 1,000 basic route requests)

| Provider | Free Tier | Cost/1K |
|----------|-----------|---------|
| Google (Essentials) | 10K/month | $5.00 |
| HERE (basic) | 30K/month | $0.75 |
| TomTom | 2.5K/day | $0.75 |
| Mapbox | ~100K elements/mo | ~$2-5 |
| Azure Maps | 5K free | $0.50-4.50 |
| Geoapify | 3K credits/day | $59-860/month flat |
| Self-hosted (OSRM/Valhalla/Butterfly) | Unlimited | Infrastructure only |

### Features Only in Commercial APIs (Not in Any Open-Source Engine)

1. **Live traffic** (Google/HERE/TomTom) — probe data from billions of devices
2. **Predictive ETAs** (Google DeepMind GNNs) — >97% accuracy
3. **Toll cost calculation** (HERE/AWS) — exact costs by vehicle class, time, payment method
4. **EV routing with consumption models** (HERE/TomTom) — battery SoC, charging curves, temperature
5. **Fuel consumption estimation** (HERE/TomTom)
6. **Transit with real-time departures** (Google/HERE)
7. **Speed limit database** (HERE/TomTom/AWS) — near-complete coverage
8. **Weather-aware routing** (HERE)
9. **Vignette avoidance** (TomTom) — country-level toll sticker routing

---

## VRP & Optimization Engines

These consume distance matrices from routing engines. Potential consumers of Butterfly's `/table/stream`.

| Engine | Language | Stars | Algorithm | Key Capability |
|--------|----------|-------|-----------|----------------|
| **Google OR-Tools** | C++ | 13,200 | CP-SAT, VRP | Industry standard, backed by Google |
| **VROOM** | C++ | 1,700 | Ruin-recreate | Fast VRP solver, uses OSRM/Valhalla/ORS |
| **jsprit** | Java | 1,700 | Ruin-recreate | Used by GraphHopper commercial |
| **Timefold** | Java/Kotlin | ~1,000 | Incremental scoring | OptaPlanner successor |

---

## Transit/Multimodal Engines

Different problem domain from road routing. Included for completeness.

| Engine | Language | Stars | Algorithm | Focus |
|--------|----------|-------|-----------|-------|
| **OpenTripPlanner** | Java | 2,600 | RAPTOR + A* | Premier transit router. Deployed nationwide in Finland, Norway, Netherlands |
| **MOTIS** | C++ | 479 | RAPTOR | All-in-one: routing + geocoding + tiles. Powers Transitous |
| **Conveyal R5** | Java | 370 | Custom | Accessibility analysis. 1-to-many/many-to-many |
| **ULTRA** (KIT) | C++ | 80 | RAPTOR + CH | Most advanced academic multimodal router |
| **Transitous** | Lua/Python | 584 | Via MOTIS | Community-operated global transit service |

---

## Algorithm Landscape

### The Definitive Comparison (PTV Europe, 18M nodes)

From Blasius et al. 2025 — "Customizable Contraction Hierarchies — A Survey":

| Algorithm | Preprocess [s] | Customize [s] | Query [ms] | Customizable? |
|-----------|---------------:|---------------:|-----------:|:------------:|
| **Dijkstra** | — | — | 2,359 | N/A |
| **CH** | 109 | — | 0.11 | No |
| **CRP** | 654 | 1.05 | 1.65 | Yes |
| **CCH basic** | 367 | 0.58 | 0.30 | Yes |
| **CCH perfect** | 367 | 1.25 | 0.14 | Yes |

Key insight: **CCH queries are as fast as CH** and **10x faster than CRP**, while being fully customizable.

### Extended Performance Table

| Algorithm | Query Time | Speedup vs Dijkstra | Space | Trade-off |
|-----------|-----------|--------------------:|-------|-----------|
| Dijkstra | ~2,400 ms | 1x | O(n+m) | Baseline |
| A* | ~600 ms | 4x | O(n+m) | Heuristic guided |
| ALT | ~5 ms | 480x | O(n*k) | Landmarks |
| CRP/MLD | ~1.65 ms | 1,450x | Compact | Customizable, Bing Maps uses this |
| CCH | ~0.14-0.30 ms | 8,000-17,000x | 2-3x edges | **Customizable, turn-cost ready** |
| CH | ~0.11 ms | 21,000x | 2-3x edges | Non-customizable |
| Transit Nodes | ~4-5 us | 500,000x | 600 MB | Near-constant for long queries |
| Hub Labels | ~0.3-0.5 us | 5,000,000x | 18 GB | Fastest, enormous space |

### Edge-Based vs Node-Based (Buchhold et al. 2020)

| Aspect | Node-based | Edge-based |
|--------|-----------|------------|
| Vertices (Europe) | ~18M | ~42-50M |
| CCH customization overhead | 1x | ~3x |
| Turn restriction accuracy | Approximated | Exact |
| Recommended for production | If turn costs don't matter | If correctness matters |

### Key Academic References

| Paper | Year | Contribution |
|-------|------|-------------|
| Geisberger et al. | 2008 | Contraction Hierarchies (CH) |
| Delling et al. | 2011 | CRP (Bing Maps), PHAST, RPHAST |
| Abraham et al. | 2011 | Hub Labels (sub-microsecond queries) |
| Dibbelt, Strasser, Wagner | 2016 | CCH (three-phase customizable routing) |
| Gottesburen et al. | 2019 | InertialFlowCutter (best CCH orders) |
| Buchhold et al. | 2020 | CCH with Turn Costs (edge-based CCH) |
| Blasius et al. | 2025 | CCH Survey (definitive reference) |
| Wan et al. | 2025 | Parallel CH (23s preprocessing for 87M nodes) |
| Farhan et al. | 2025 | Customizable Hub Labels (tunable space-query) |

### Graph Partitioning Quality for CCH

| Algorithm | CCH Order Quality | Speed |
|-----------|:----------------:|:-----:|
| InertialFlowCutter | Best | Medium (~4 min Europe) |
| FlowCutter | Near-best | Slow |
| KaHIP | Good | Medium |
| InertialFlow | Good (variable) | Fast |
| METIS | Worst (by large margin) | Fastest |

### Production Engine Algorithm Choices

| Engine | Algorithm | Why |
|--------|-----------|-----|
| **OSRM** | CH (default) / MLD | Speed + maturity |
| **Valhalla** | Bidirectional A* | Flexibility over speed |
| **GraphHopper** | CH / LM / Flexible | Tiered speed-flexibility trade-off |
| **Butterfly** | Edge-based CCH | Speed + customizability + exact turns |
| **Google** | CH + traffic | Proprietary |
| **Bing** | CRP | Published in Transportation Science |

---

## Performance Benchmarks

### Point-to-Point Query Latency (Europe-scale graph)

| Engine | Algorithm | Query Time |
|--------|-----------|-----------|
| OSRM (CH) | Node-based CH | ~0.11 ms |
| **Butterfly** | Edge-based CCH | ~0.14 ms |
| GraphHopper (CH) | Node-based CH | ~1-5 ms |
| CRP (academic) | CRP | ~1.65 ms |
| Valhalla | Bidi A* (regional) | ~10-100 ms |
| GraphHopper (Flexible) | Dijkstra/A* | ~100-500 ms |
| Valhalla (continental) | Bidi A* | ~1-2 s |

### Matrix Performance (Belgium, fair HTTP comparison)

| Size | OSRM CH | Butterfly | Ratio |
|------|---------|-----------|:-----:|
| 100 x 100 | 55ms | 164ms | 3.0x slower |
| 1k x 1k | 684ms | 1.55s | 2.3x slower |
| 10k x 10k | 32.9s | 18.2s | **1.8x FASTER** |
| 50k x 50k | Crashes | **9.5 min** | **Butterfly only** |

### Isochrone Performance

| Engine | Algorithm | Latency |
|--------|-----------|---------|
| **Butterfly** | PHAST + block-gated | **5ms p50** |
| Valhalla | Dijkstra isotile | ~200-500ms |
| GraphHopper | SPT + triangulation | ~100-300ms |
| OSRM | N/A | No isochrone support |

### Preprocessing Time (Planet scale, approximate)

| Engine | Time | RAM |
|--------|------|-----|
| Valhalla | ~12h | 16 GB |
| OSRM (MLD) | ~3.5h | 50 GB |
| OSRM (CH) | ~8h | 50 GB |
| GraphHopper (CH, no turns) | ~4h | 60 GB |
| GraphHopper (CH, with turns) | ~25h | 120 GB |

---

## Strategic Gap Analysis

### Where Butterfly Already Wins

1. **Exact turn-aware routing** — only engine with edge-based CCH as single source of truth
2. **Matrix at scale** — 50k x 50k via Arrow IPC streaming, 1.8x faster than OSRM at 10k+
3. **Isochrone performance** — 5ms vs 200-500ms (Valhalla) — 40-100x faster
4. **Bulk isochrone** — 1,526 isochrones/sec, no competitor has bulk endpoint
5. **Observability** — Prometheus metrics, structured logging, health endpoint with stats
6. **OpenAPI/Swagger** — self-documenting API, none of the Big 3 have this
7. **Binary formats** — WKB isochrones, Arrow IPC matrices — unique
8. **Declarative profiles** — JSON model files, zero Rust changes for new modes
9. **Customizable weights** — CCH re-customization in ~1s (vs OSRM's full re-preprocess)
10. **Rust** — zero GC overhead, memory-safe, small binary, low resource footprint
11. **Multimodal transit built in** — full RAPTOR stack with multi-feed merging (GTFS + NeTEx-EPIP for STIB) in the same process as the road router, sharing one foot CCH. No separate OTP/MOTIS service required. Brussels→Antwerp multimodal query in 35 ms p50; 311 transit queries/sec sustained via `/transit/bulk`. Cross-operator stop bridges, same-station hierarchy, ULTRA transfer preprocessing all wired automatically.
12. **Unnested per-edge flow analytics** — the `edges_batch` Flight action emits `(query_idx, edge_seq, osm_node_from, osm_node_to, duration_ms, distance_m)` rows with continuity invariant. No other OSS router ships this as a first-class RPC. The primitive for traffic assignment, all-or-nothing loading, emissions inventory, edge betweenness, and network vulnerability analysis.
13. **Two-transport architecture** — REST (Axum) for JSON-shaped human traffic, gRPC Flight for Arrow-shaped analytics traffic. No transport mixing. Clients pick the right tool.

### Gaps to Close (ordered by impact)

| Gap | Impact | Competitor | Effort |
|-----|--------|-----------|--------|
| **Time-dependent routing** | Critical for logistics | Valhalla, commercial APIs | Very high — need piecewise-linear weight functions |
| **Real-time transit (statistical p90 timetables)** | High for reliability | None ship this correctly | Medium-high — tracked in #122 (GTFS-RT archive) + #123 (stats synthesis) |
| **More transport modes** | Medium-high | Valhalla (12 modes) | Low — add `.model.json` files |
| **Wheelchair routing** | Niche but unique | GraphHopper, ORS | Low — add model JSON |
| **GPX output** | Nice-to-have | GraphHopper | Low |
| **MVT tiles** | Visualization | OSRM | Medium |
| **Expansion visualization** | Debugging | Valhalla | Low-medium |
| **Centroid API** | Niche | Valhalla | Low |
| **Planet-scale validation** | Credibility | All | Medium — need hardware for planet preprocessing |
| **VRP integration** | Ecosystem | GraphHopper/VROOM | Medium — integrate VROOM as consumer of `/table/stream` or the Flight `edges_batch` action |

Transit/multimodal is no longer a gap: full RAPTOR + CCH multimodal with GTFS + NeTEx-EPIP merging shipped in 2026-04. See the **Where Butterfly Already Wins** list item 11 and `/src/transit/` for the implementation.

### Features That Require External Data (Commercial Moat)

These cannot be replicated with OSM data alone:

1. **Live traffic** — requires probe data partnerships or telemetry ingestion
2. **Predictive ETAs** — requires ML training on historical trip data
3. **Toll costs** — requires licensing toll databases
4. **EV consumption models** — requires vehicle-specific energy models
5. **Speed limit database** — OSM has partial coverage; commercial providers have near-complete
6. **Weather-aware routing** — requires weather data integration

### Recommended Priority for Butterfly

**Phase 1 — Low-hanging fruit (days):**
- Add `bus.model.json`, `motorcycle.model.json`, `scooter.model.json` profiles
- Add `wheelchair.model.json` with accessibility-focused speed/access rules
- Add GPX output format for `/route`
- Transit RAPTOR micro-optimisations (tickets #126 SoA `stop_times` → #127 SIMD `earliest_trip` → #128 trip-table delta compression). #126 is the smallest and unblocks the other two.

**Phase 2 — Ecosystem (weeks):**
- Statistical transit reliability (#122 GTFS-RT archive pipeline → #123 p50/p90 synthesis). Replaces the "real-time" framing with the correct "reproducible p90" model. No competitor ships this correctly.
- Document VROOM integration as VRP consumer of `/table/stream` or the Flight `edges_batch` action
- Consolidate HTTP download logic into butterfly-dl (#100)
- Expansion API endpoint (GeoJSON visualization of CCH search)
- Centroid API endpoint

**Phase 3 — Competitive edge (months):**
- Time-dependent routing via CCH with piecewise-linear weights
- Planet-scale preprocessing benchmark and validation
- Live traffic ingestion pipeline (open traffic data sources)

**Phase 4 — Moonshots (quarters):**
- ~~Transit/multimodal~~ **DONE 2026-04** — full RAPTOR + CCH stack, multi-feed GTFS + NeTEx-EPIP, Brussels→Antwerp 35 ms p50
- Toll cost integration (open toll databases)
- EV routing with consumption models

---

*Sources: GitHub repositories, official documentation, API references, academic papers (Blasius et al. 2025, Buchhold et al. 2020, Delling et al. 2011/2013, Geisberger et al. 2008, Gottesburen et al. 2019), web searches conducted March 2026.*
