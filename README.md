# Butterfly-OSM 🦋

[![CI](https://github.com/butterfly-osm/butterfly-osm/workflows/CI/badge.svg)](https://github.com/butterfly-osm/butterfly-osm/actions/workflows/ci.yml)

Hurricane-fast drivetime engine and OSM toolkit built for modern performance requirements.

## Vision

**Goal**: 10x faster than state-of-the-art with minimal memory footprint and modern architecture.

Butterfly-OSM reimagines OpenStreetMap data processing from the ground up, leveraging Rust's performance and safety to create a new generation of geographic tools that are both lightning-fast and resource-efficient.

## What

A comprehensive ecosystem of OSM tools designed around **separation of concerns** and **composability**:

### Core Tools

- **🚀 butterfly-dl**: Memory-efficient OSM data downloader (<1GB RAM for any file size)
- **✂️ butterfly-shrink**: Polygon-based area extraction with geometric optimization  
- **🔧 butterfly-extract**: Advanced filtering and transformation engine
- **🌐 butterfly-serve**: High-performance HTTP tile server with caching

### Shared Foundation

- **📚 butterfly-common**: Shared utilities, error handling, and geographic algorithms
- **🧠 Unified Intelligence**: Geographic fuzzy matching and semantic understanding
- **⚡ Performance Primitives**: Memory-efficient data structures and async I/O

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
| Regional extraction | 5-15 minutes | 30-90 seconds | **10x faster** |
| Tile serving (QPS) | 100-500 | 5,000+ | **10-50x faster** |
| Memory usage | 4-16GB | <1GB | **4-16x less** |

## How

### Architecture Principles

#### 1. Separation of Concerns
Each tool has a single, well-defined responsibility:
```
butterfly-dl    → Data acquisition (download, streaming)
butterfly-shrink → Geometric operations (extraction, clipping)  
butterfly-extract → Data transformation (filtering, conversion)
butterfly-serve  → Data serving (HTTP, caching, tiles)
```

#### 2. Composable Pipeline Design
Tools work together via standard streams and file formats:
```bash
# Download → Extract → Serve pipeline
butterfly-dl planet - | \
butterfly-extract --bbox 2.0,46.0,8.0,49.0 - france.pbf && \
butterfly-serve france.pbf --port 8080
```

#### 3. Shared Intelligence
Common patterns abstracted into `butterfly-common`:
- Geographic algorithms (bounding boxes, projections)
- Error handling with fuzzy matching
- Memory-efficient data structures
- Async I/O primitives

#### 4. Performance-First Design

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

- **butterfly-route**: High-performance routing engine
  - Exact turn-aware routing (edge-based CCH)
  - **1.8x FASTER than OSRM** at scale (10k×10k matrices)
  - Production-hardened: structured logging, graceful shutdown, timeouts, compression, input validation, panic recovery, Prometheus metrics
  - Audit-remediated: all butterfly-route HIGH findings resolved (source_idx overflow, unwrap panics, unsafe bounds checks)
  - See [Routing Engine](#routing-engine-butterfly-route) below

### 🚧 In Development

- **butterfly-shrink**: Polygon-based extraction engine
- **butterfly-extract**: Advanced filtering and transformation
- **butterfly-serve**: High-performance tile server

### 🎯 Roadmap

**Phase 1 (Current)**: Core data acquisition and workspace foundation  
**Phase 2**: Geometric operations and extraction tools  
**Phase 3**: Advanced transformation and filtering capabilities  
**Phase 4**: High-performance serving and caching infrastructure  

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
  -d '{"locations": [[4.35,50.85],[4.40,50.86],[4.45,50.87]], "mode": "car"}'

# Small matrix (OSRM-compatible)
curl "http://localhost:3001/table/v1/driving/4.35,50.85;4.40,50.86;4.45,50.87"
```

### Running the Routing Engine

```bash
# Build the Docker image
docker build -t butterfly-route .

# Run the server (Belgium data)
docker run -d --name butterfly \
  -p 3001:8080 \
  -v "${PWD}/data/belgium:/data" \
  butterfly-route

# Health check
curl http://localhost:3001/health

# Swagger UI at http://localhost:3001/swagger-ui/
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
cargo install --path tools/butterfly-dl
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
├── tools/
│   ├── butterfly-dl/     # OSM data downloader (production-ready)
│   └── butterfly-route/  # Routing engine (production-ready)
├── scripts/             # Benchmarking and validation scripts
└── data/                # Test data and examples
```

**Note**: Additional tools (butterfly-shrink, butterfly-extract, butterfly-serve) are planned for future development.

### Building Individual Tools

```bash
# Build specific tool
cargo build --release -p butterfly-dl

# Test specific tool  
cargo test -p butterfly-dl

# Install from workspace
cargo install --path tools/butterfly-dl
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
- **Response compression**: gzip + brotli on API routes
- **Input validation**: Coordinate bounds, time limits, size limits
- **Panic recovery**: `CatchPanicLayer` returns 500 JSON instead of crashing
- **Prometheus metrics**: Per-endpoint latency histograms at `/metrics`
- **Health check**: `/health` with uptime, node/edge counts, mode list
- **Swagger UI**: OpenAPI docs at `/swagger-ui/`

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

MIT License - see [LICENSE](LICENSE) file for details.

## Team

**Butterfly Project** - Built by Pierre <pierre@warnier.net> for the broader OpenStreetMap community.

---

**Mission**: Democratizing high-performance geographic computing through modern tools and architecture.

**butterfly-osm** - Hurricane-fast OSM processing for the modern era.