# Butterfly-OSM 🦋

Hurricane-fast drive-time engine and OSM toolkit built for modern performance requirements.

## Vision

**Goal**: 10x faster than state-of-the-art with minimal memory footprint and modern architecture.

Butterfly-OSM reimagines OpenStreetMap data processing from the ground up, leveraging Rust's performance and safety to create a new generation of geographic tools that are both lightning-fast and resource-efficient.

## What

A comprehensive ecosystem of OSM tools designed around **separation of concerns** and **composability**:

### Core Tools

- **🚀 butterfly-dl**: Memory-efficient OSM data downloader (<1GB RAM for any file size)
- **📊 butterfly-extract**: PBF streaming processor with routing-relevant filtering and telemetry
- **🧠 butterfly-plan**: Adaptive memory planning with telemetry-driven parameter optimization
- **🌐 butterfly-serve**: REST API server with /telemetry, /probe/snap, and /graph debug endpoints for spatial metrics and QA

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

| Operation | Current Tools | Butterfly-OSM | Improvement |
|-----------|---------------|---------------|-------------|
| Planet download | 2-4 hours | 20-40 minutes | **3-6x faster** |
| Memory usage | 4-16GB | <1GB | **4-16x less** |

## How

### Architecture Principles

#### 1. Separation of Concerns
Each tool has a single, well-defined responsibility:
```
butterfly-dl → Data acquisition (download, streaming)
```

#### 2. Composable Pipeline Design
Tools work together via standard streams and file formats:
```bash
# Download OSM data
butterfly-dl europe/france
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

- **M1 - Telemetry & Adaptive Planning**: Complete foundation for intelligent OSM processing
  - PBF streaming with routing-relevant tag filtering
  - 125m spatial tile grid with junction/length/curvature metrics
  - REST API with /telemetry endpoint for spatial density analysis
  - Adaptive memory planning with urban/suburban/rural density classification
  - Comprehensive fuzzing with planet-scale distributions and safety invariants

- **M2 - Adaptive Coarsening**: Advanced topology preservation and optimization
  - Semantic breakpoints for routing-critical features (bridges, tunnels, named roads)
  - Curvature analysis with geometry-aware vertex retention and fast-path optimization
  - Node canonicalization using grid hash + union-find for collision-safe coordinate merging
  - Policy smoothing with 3×3 median filtering for tile boundary consistency  
  - /probe/snap API endpoint for QA/debugging of canonical mapping

- **M3 - Super-Edge Construction**: Advanced graph topology optimization with cross-tile consistency
  - **M3.1**: Canonical adjacency lists with bidirectional neighbor tracking and edge semantics
  - **M3.2**: Policy-aware degree-2 collapse creating super-edges with segment guards (4,096 pt / 1km limits)
  - **M3.3**: Border reconciliation using geometric analysis for global consistency across tiles
  - **M3.4**: Graph debug artifacts (nodes.bin, super_edges.bin, geom.temp) with REST APIs (/graph/stats, /graph/edge/{id})
  - Memory safety compliance for M5 geometry processing and production-ready tile boundary handling

### 🚧 Future Development

Future tools and capabilities will be added based on community needs and feedback.

### 🎯 Roadmap

**Phase 1 (Current)**: Core data acquisition and workspace foundation  
**Future Phases**: Additional tools and capabilities based on community needs  

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

# Build all tools
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
│   └── butterfly-dl/     # OSM data downloader
├── benchmarks/          # Performance benchmarks
├── examples/            # Usage examples and tutorials (planned)
```

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