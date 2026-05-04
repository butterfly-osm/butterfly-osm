<p align="center">
  <img src="images/butterfly_logo_900kb.jpg" width="280" alt="Butterfly-OSM logo" />
</p>

# Butterfly-OSM 🦋

Hurricane-fast drivetime engine and OSM toolkit built for modern performance requirements.

## Vision

**Goal**: 10x faster than state-of-the-art with minimal memory footprint and modern architecture.

Butterfly-OSM reimagines OpenStreetMap data processing from the ground up, leveraging Rust's performance and safety to create a new generation of geographic tools that are both lightning-fast and resource-efficient.

## What

A comprehensive ecosystem of OSM tools designed around **separation of concerns** and **composability**:

### Core Tools

- **butterfly-dl**: Memory-efficient OSM data downloader (<1GB RAM for any file size) with verified content checks (magic-byte prefix, min-bytes floor, SHA-256 sidecar) and region-indexed parallel downloads
- **butterfly-route**: High-performance road router **and** multimodal transit engine. Edge-based CCH for exact turn-aware driving/walking/cycling, PHAST isochrones, Arrow-streaming distance matrices, and a full RAPTOR-based public transport stack with multi-feed merging (SNCB, De Lijn, TEC, STIB via NeTEx-EPIP) and ULTRA transfer-graph preprocessing
- **butterfly-common**: Shared utilities, error handling, and geographic algorithms

## Why

### The Performance Problem

Current OSM tools suffer from fundamental limitations:
- **Memory inefficiency**: Tools that require 10GB+ RAM for large datasets
- **Single-threaded bottlenecks**: Missing modern parallelization opportunities  
- **Monolithic architecture**: Difficult to compose and extend
- **Legacy codebases**: Built before modern understanding of performance

### Our Approach

**Hurricane-fast through intelligent design:**
- **🔥 Rust performance**: Zero-cost abstractions with memory safety
- **🧠 Smart algorithms**: Geographic-aware data structures and caching
- **🚀 Modern async**: Tokio-based concurrency for I/O-bound operations
- **💎 Composable architecture**: Unix philosophy applied to OSM processing

### Target Performance

| Operation | Current Tools | Butterfly-OSM Target | Improvement |
|-----------|---------------|---------------------|-------------|
| Planet download | 2-4 hours | 20-40 minutes | **3-6x faster** |
| Matrix 10k×10k | 32.9s (OSRM) | 18.2s | **1.8x faster** |
| Memory usage | 4-16GB | <1GB | **4-16x less** |

## How

### Architecture Principles

#### 1. Separation of Concerns
Each tool has a single, well-defined responsibility:
```
butterfly-dl    → Data acquisition (download, streaming)
butterfly-route → Routing engine (CCH, isochrones, matrices)
butterfly-common → Shared utilities and algorithms
```

#### 2. Composable Design
Common patterns abstracted into `butterfly-common`:
- Geographic algorithms (bounding boxes, projections)
- Error handling with fuzzy matching
- Memory-efficient data structures
- Async I/O primitives

#### 3. Performance-First Design

**Memory efficiency:**
- Streaming architecture - process data without loading entirely into memory
- Fixed-size buffers - predictable memory usage regardless of input size
- Zero-copy operations where possible

**Compute efficiency:**
- Rust's zero-cost abstractions
- SIMD operations for geometric calculations
- Lock-free data structures for parallelism
- Modern async/await for I/O concurrency

**I/O efficiency:**  
- HTTP/2 with connection pooling
- Direct I/O for large sequential operations
- Intelligent caching strategies
- Range requests and resume capability

### Modern Language Benefits

**Rust advantages over C/C++:**
- Memory safety without garbage collection overhead
- Fearless concurrency with compile-time race condition prevention
- Modern package management with Cargo
- Excellent async ecosystem with Tokio

**Rust advantages over Python/JavaScript:**
- 10-100x performance improvement
- Predictable memory usage
- No GIL limitations for parallelism
- Compiled binaries with no runtime dependencies

## Ecosystem Status

### ✅ Available Now

- **butterfly-dl**: Production-ready OSM downloader
  - Handles files from 1MB to 81GB (planet.osm.pbf)
  - 79% faster than aria2, 3x faster than curl on medium files
  - <1GB memory usage regardless of file size

- **butterfly-route**: High-performance road router + multimodal transit engine
  - Exact turn-aware routing (edge-based CCH)
  - **1.8x FASTER than OSRM** at scale (10k×10k matrices)
  - Full RAPTOR transit stack: multi-feed merging (GTFS + NeTEx-EPIP), ULTRA transfer preprocessing, `/transit` + `/transit/bulk` REST, `transit_bulk` + `edges_batch` Flight actions
  - Production-hardened: structured logging, graceful shutdown, timeouts, compression, input validation, panic recovery, Prometheus metrics
  - Open work tracked in GitHub issues (`gh issue list`); `competitive_landscape.md` captures where Butterfly wins vs OSRM / Valhalla / GraphHopper
  - See [Routing Engine](#routing-engine-butterfly-route) below

## Routing Engine (butterfly-route)

High-performance routing engine with **exact turn-aware queries** using edge-based Customizable Contraction Hierarchies (CCH). Production-hardened with structured logging, graceful shutdown, request timeouts, response compression, input validation, panic recovery, and Prometheus metrics. Docker-ready.

### Performance

| Workload | Butterfly | OSRM | Result |
|----------|-----------|------|--------|
| Isochrones (bulk) | **1,526/sec** | - | Production-ready |
| Matrix 100×100 | 164ms | 55ms | 3x slower (acceptable) |
| Matrix 10k×10k | **18.2s** | 32.9s | **1.8x FASTER** |

**Key insight**: Butterfly wins at scale. Use bulk APIs for production workloads.

### Bulk APIs (Recommended for Production)

#### Bulk Isochrones

For computing many isochrones, use the bulk endpoint which processes origins in parallel:

```bash
# Compute 100 isochrones in one request
curl -X POST http://localhost:3001/isochrone/bulk \
  -H "Content-Type: application/json" \
  -d '{
    "origins": [[4.35, 50.85], [4.40, 50.86], ...],
    "time_s": 1800,
    "mode": "car"
  }' \
  --output isochrones.wkb

# Response: Length-prefixed WKB stream
# Format: [4B origin_idx][4B wkb_len][WKB polygon]...
```

**Throughput**: 1,526 isochrones/sec (vs 815/sec individual)

#### Bulk Distance Matrices

For large matrices (1000+ origins/destinations), use Arrow streaming:

```bash
# Compute 10,000 × 10,000 matrix via Arrow IPC
curl -X POST http://localhost:3001/table/stream \
  -H "Content-Type: application/json" \
  -d '{
    "sources": [[lon1,lat1], [lon2,lat2], ...],
    "destinations": [[lon1,lat1], [lon2,lat2], ...],
    "mode": "car"
  }' \
  --output matrix.arrow

# Process with PyArrow, DuckDB, or any Arrow-compatible tool
```

**Performance**: 18.2s for 10k×10k (1.8x faster than OSRM)

### Individual APIs

For single queries or small workloads:

```bash
# Single route with turn-by-turn steps (includes road names)
curl "http://localhost:3001/route?src_lon=4.35&src_lat=50.85&dst_lon=4.40&dst_lat=50.86&mode=car&steps=true"

# Single isochrone (GeoJSON, CCW polygons, 5-decimal precision)
curl "http://localhost:3001/isochrone?lon=4.35&lat=50.85&time_s=1800&mode=car"

# Reverse isochrone (where can people reach me FROM within 30 min?)
curl "http://localhost:3001/isochrone?lon=4.35&lat=50.85&time_s=1800&mode=car&direction=arrive"

# TSP/trip optimization
curl -X POST "http://localhost:3001/trip" -H "Content-Type: application/json" \
  -d '{"coordinates": [[4.35,50.85],[4.40,50.86],[4.45,50.87]], "mode": "car"}'
```

## Multimodal Transit (RAPTOR + CCH)

butterfly-route ships a production transit engine alongside the road router. Public transport queries thread a foot/bike/car **access leg** (CCH) + **RAPTOR rounds** over the merged timetable + foot **egress leg** (CCH), with ULTRA-preprocessed transfer graphs for sub-second stop-to-stop walking.

### Feed coverage

Out of the box on Belgium with **zero operator configuration**:

- **SNCB** (national rail) — GTFS via iRail
- **De Lijn** (Flanders bus + tram) — GTFS via iRail
- **TEC** (Wallonia bus) — GTFS
- **STIB** (Brussels metro/tram/bus) — **NeTEx-EPIP** via the Belgian National Access Point (no GTFS published — butterfly-route's streaming NeTEx parser handles the 720 MB EPIP file directly, reprojects Lambert-93 → WGS84, and merges with the GTFS feeds into one `Timetable`)

Cross-feed equivalence bridges wire physically co-located stops (SNCB ↔ STIB at Brussels-Midi, SNCB ↔ De Lijn at suburban hubs), and same-station parent-child transfers (multiple platforms of the same station) are injected automatically into the foot-CCH transfer graph.

### REST endpoints

```bash
# Single multimodal query (JSON)
curl "http://localhost:3001/transit?origin_lon=4.3517&origin_lat=50.8466\
&dest_lon=4.4025&dest_lat=51.2194&depart=08:00:00"

# Batch routing: 100k queries in one call, rayon-amortised access CCH
curl -X POST http://localhost:3001/transit/bulk \
  -H 'content-type: application/json' \
  -d '{"queries":[{"origin_lon":4.3517,"origin_lat":50.8466,"dest_lon":4.4025,"dest_lat":51.2194,"depart":"08:00:00"},...]}'

# Opt-in routed polylines for access / egress / middle walking legs
curl "http://localhost:3001/transit?...&geometry=full"
```

### Arrow Flight (gRPC, port 3002)

For high-throughput analytics pipelines the **standalone gRPC Flight server** exposes Arrow IPC streaming on a separate port — no Arrow-over-HTTP hybrid, REST stays JSON and Flight stays Arrow:

| Action | Ticket | Output shape |
|---|---|---|
| `matrix` | `matrix:<profile>:{"sources":[...],"destinations":[...]}` | One row per pair, duration + distance |
| `route_batch` | `route_batch:<profile>:{"pairs":[[src,dst],...]}` | Pair-level duration + WKB polyline |
| `isochrone` | `isochrone:<profile>:{"lon":...,"lat":...,"intervals":[...]}` | Polygons as WKB |
| `catchment` | via DoExchange | Catchment hulls per store |
| **`transit_bulk`** | `transit_bulk:<profile>:{"queries":[...]}` | Per-query row with metadata + JSON legs (up to 500 k queries/call) |
| **`edges_batch`** | `edges_batch:<profile>:{"pairs":[...]}` | **Unnested per-edge path output** with OSM node ids — the flow-analytics primitive no other OSS router ships |

The `edges_batch` action emits one Arrow row per traversed EBG edge with `(query_idx, target_idx, edge_seq, osm_node_from, osm_node_to, duration_ms, distance_m)` columns, unreachable pairs get a single row with null edge columns, and the continuity invariant `osm_node_to[i] == osm_node_from[i+1]` is enforced — ready to `GROUP BY osm_node_to` for traffic assignment, emissions inventory, or network vulnerability analysis.

### Transit-specific perf (Belgium, 4 feeds merged)

| Workload | Metric |
|---|---|
| Single `/transit` query, warm | **35 ms p50** |
| Bulk 20 same-origin queries | **150 ms** (7× speedup vs serial) |
| Bulk 1000 varied queries | **3.22 ms/query**, **311 queries/sec** |
| Merged transfer graph | 66 512 stops, 668 k edges |
| Cross-feed equivalence bridges | 986 post-ULTRA |
| Same-station child-pair coverage | 94 % of multi-child parents |

### Running the Routing Engine

```bash
# Build the Docker image
docker build -t butterfly-route .

# Run the server (Belgium data). Port 3001 = REST, 3002 = gRPC Flight.
docker run -d --name butterfly \
  -p 3001:8080 -p 3002:3002 \
  -v "${PWD}/data/belgium:/data" \
  butterfly-route

# Health check (REST)
curl http://localhost:3001/health

# Swagger UI at http://localhost:3001/swagger-ui/

# gRPC Flight server at 0.0.0.0:3002 — list available actions:
grpcurl -plaintext localhost:3002 arrow.flight.protocol.FlightService/ListActions
```

See [CLAUDE.md](CLAUDE.md) for detailed build instructions, algorithm documentation, and local development setup.

## Installation

### Quick Start
```bash
# Install from crates.io
cargo install butterfly-dl

# Download Belgium OSM data
butterfly-dl europe/belgium

# Verify installation
butterfly-dl --version
```

### Development Setup
```bash
# Clone the workspace
git clone https://github.com/butterfly-osm/butterfly-osm
cd butterfly-osm

# Docker (recommended)
docker build -t butterfly-route .

# Local build
cargo build --workspace --release

# Run tests
cargo test --workspace

# Install specific tool
cargo install --path dl
```

### Pre-built Binaries

Download optimized binaries for your platform:
- [Linux x86_64](https://github.com/butterfly-osm/butterfly-osm/releases/latest/download/butterfly-dl-latest-x86_64-unknown-linux-gnu.tar.gz)
- [Linux ARM64](https://github.com/butterfly-osm/butterfly-osm/releases/latest/download/butterfly-dl-latest-aarch64-unknown-linux-gnu.tar.gz)  
- [macOS Intel](https://github.com/butterfly-osm/butterfly-osm/releases/latest/download/butterfly-dl-latest-x86_64-apple-darwin.tar.gz)
- [macOS Apple Silicon](https://github.com/butterfly-osm/butterfly-osm/releases/latest/download/butterfly-dl-latest-aarch64-apple-darwin.tar.gz)
- [Windows x86_64](https://github.com/butterfly-osm/butterfly-osm/releases/latest/download/butterfly-dl-latest-x86_64-pc-windows-msvc.zip)

## Workspace Structure

```
butterfly-osm/
├── butterfly-common/     # Shared utilities and algorithms
├── dl/                   # butterfly-dl: OSM data downloader (production-ready)
├── route/                # butterfly-route: routing engine (production-ready)
├── scripts/              # Benchmarking and validation scripts
└── data/                 # Test data and examples
```

### Building Individual Tools

```bash
# Build specific tool
cargo build --release -p butterfly-dl

# Test specific tool  
cargo test -p butterfly-dl

# Install from workspace
cargo install --path dl
```

## Performance Benchmarks

### butterfly-route vs OSRM (Belgium)

| Workload | Butterfly | OSRM | Ratio |
|----------|-----------|------|-------|
| Single route | 2ms | 1ms | 2x slower |
| Isochrone (30min) | **5ms** | - | - |
| Bulk isochrones | **1,526/sec** | - | - |
| Matrix 100×100 | 164ms | 55ms | 3x slower |
| Matrix 1k×1k | 1.55s | 0.68s | 2.3x slower |
| Matrix 10k×10k | **18.2s** | 32.9s | **1.8x FASTER** |

**Key insight**: Edge-based CH has ~2.5x more states than node-based (exact turn handling).
The overhead is acceptable for small queries, and **Butterfly wins at scale**.

### Production Features

- **Structured logging**: `tracing` with text/JSON output (`--log-format json`)
- **Graceful shutdown**: SIGINT + SIGTERM handling
- **Request timeouts**: 120s for API routes, 600s for streaming
- **Concurrency limiting**: Max 32 concurrent API requests, max 4 streaming requests
- **Response compression**: gzip + brotli on API routes
- **Input validation**: Coordinate bounds, time limits, size limits
- **Panic recovery**: `CatchPanicLayer` returns 500 JSON instead of crashing
- **Prometheus metrics**: Per-endpoint latency histograms at `/metrics`, plus per-section CRC verification counters (`butterfly_route_sections_verified_total`, `butterfly_route_sections_verify_pending`, `butterfly_route_section_verify_duration_seconds{section}`, `butterfly_route_section_verify_failed_total{section}`)
- **Health check**: `/health` with uptime, node/edge counts, mode list, and per-section lazy-CRC verification status (`verify_status`, `verify.{n_sections,n_verified,n_unverified,n_verifying,n_failed,failed[]}`)
- **Swagger UI**: OpenAPI docs at `/swagger-ui/`
- **Lazy CRC verification (#160)**: When loaded from a `.butterfly` container (`--data <file>`), per-section CRC walks default to **on-first-access** rather than at boot — saves ~30 % wall-clock and ~30 % boot-transient peak RSS on Belgium. Use `--warmup-on-boot` for a background pass that walks every section in parallel after the listener binds (recommended for production). Use `--eager-verify` to restore the pre-#160 fail-fast-on-boot semantics.

### butterfly-dl vs Industry Standard

Real-world benchmarks on 43MB Luxembourg dataset:

```
Tool         Duration  Speed     Memory    Improvement
--------------------------------------------------------
butterfly-dl 3.037s    14.07MB/s ~215MB   Baseline
aria2c       5.447s    7.84MB/s  ~120MB   79% slower
curl         9.349s    4.57MB/s  ~10MB    208% slower
```

**Key insights:**
- **79% faster** than aria2 (industry standard)
- **3x faster** than curl
- Consistent ~215MB memory usage regardless of file size
- Smart connection scaling based on file size

### Memory Efficiency Verification

```bash
# Download 81GB planet file - uses <1GB RAM
butterfly-dl planet

# Memory usage remains constant
ps aux | grep butterfly-dl
# butterfly-dl   0.2  2.1  215MB  ...
```

## Contributing

We welcome contributions to the butterfly-osm ecosystem! 

### Development Philosophy
- **Performance first**: Every change should maintain or improve performance
- **Memory conscious**: Fixed memory usage patterns preferred
- **Test driven**: Comprehensive benchmarks for all performance claims  
- **Unix philosophy**: Small, composable tools that do one thing well

### Getting Started
1. Fork the repository
2. Create a feature branch
3. Add comprehensive tests and benchmarks
4. Ensure all existing tests pass
5. Submit a pull request

See [CONTRIBUTING.md](CONTRIBUTING.md) for detailed guidelines.

## License

AGPL-3.0-or-later — see [LICENSE](LICENSE) file for the canonical FSF text.

[![License: AGPL v3+](https://img.shields.io/badge/license-AGPL--3.0--or--later-blue.svg)](LICENSE)

Network-deployed forks must publish their source code under AGPL §13. By
submitting a pull request you agree your contribution is licensed under
AGPL-3.0-or-later (see [CONTRIBUTING.md](CONTRIBUTING.md)).

## Team

**Butterfly Project** - Built by Pierre <pierre@warnier.net> for the broader OpenStreetMap community.

---

**Mission**: Democratizing high-performance geographic computing through modern tools and architecture.

**butterfly-osm** - Hurricane-fast OSM processing for the modern era.