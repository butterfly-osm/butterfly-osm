# Butterfly-OSM Ecosystem Changelog

All notable changes to the butterfly-osm ecosystem will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

For detailed tool-specific changes, see individual tool changelogs:
- [butterfly-dl](./tools/butterfly-dl/CHANGELOG.md) - OSM data downloader
- [butterfly-shrink](./tools/butterfly-shrink/CHANGELOG.md) - Polygon extraction tool (planned)
- [butterfly-extract](./tools/butterfly-extract/CHANGELOG.md) - Data transformation tool (planned)
- [butterfly-serve](./tools/butterfly-serve/CHANGELOG.md) - High-performance tile server (planned)

## [2.0.0] - 2025-06-27

### 🌟 Major Milestone: Ecosystem Foundation

**Transformation from single-tool to ecosystem workspace**

### Added
- **🏗️ Workspace Architecture**: Multi-tool Rust workspace with shared components
- **📚 butterfly-common**: Shared library for error handling, geographic algorithms, and utilities
- **🤖 Automated Release Process**: Modern GitHub Actions with multi-platform builds (5 platforms)
- **🔒 Security**: Automatic checksums and integrity verification for all releases
- **📋 Tool Templates**: Standardized structure for future butterfly tools
- **🌍 Enhanced Geographic Intelligence**: Advanced fuzzy matching with semantic understanding

### Changed
- **Repository Structure**: Organized as multi-tool workspace
- **Release Process**: Fully automated from tag push to published release (~4 minutes)
- **Performance**: Improved build times while maintaining runtime performance
- **Documentation**: Ecosystem-focused with tool-specific documentation

### Maintained
- **100% Backward Compatibility**: All v1.x APIs and CLI usage preserved
- **Performance**: Same runtime characteristics and memory efficiency
- **Features**: All existing functionality retained

### Performance
- **Build Efficiency**: Shared dependencies across tools
- **Release Speed**: 4-minute automated releases vs 30+ minute manual process
- **Platform Coverage**: 5 platforms (Linux x86_64/ARM64, macOS Intel/Apple Silicon, Windows x86_64)

---

## butterfly-dl Evolution (1.0.0 → 2.0.0)

*For detailed version history, see [butterfly-dl CHANGELOG](./tools/butterfly-dl/CHANGELOG.md)*

### Key Milestones

#### 🚀 **Performance Era** (1.4.x)
- Hurricane-fast downloads: **79% faster** than aria2, **3x faster** than curl
- Memory efficiency: **<1GB RAM** for any file size (including 81GB planet)
- Network resilience with intelligent retry and resume
- Beautiful progress displays with tqdm-style formatting

#### 🧠 **Intelligence Era** (1.2.x - 1.3.x)  
- Geographic-aware fuzzy matching: knows Belgium is in Europe, not Antarctica
- Dynamic source discovery from Geofabrik API
- Semantic error correction: "austrailia" → "australia-oceania" (not "austria")
- Real-time source updates, no hardcoded lists

#### 🏗️ **Architecture Era** (1.0.x - 1.1.x)
- Library + CLI architecture with C FFI bindings
- HTTP-only design for security and simplicity
- Smart connection scaling based on file size
- Comprehensive benchmarking against industry standards

#### 🛠️ **Foundation Era** (0.1.x)
- Multi-connection parallel downloads
- Docker-first development
- Convention over configuration approach
- Production-ready Geofabrik downloader

### Performance Achievements

| Metric | Achievement | Comparison |
|--------|-------------|------------|
| **Speed** | 14.07 MB/s | 79% faster than aria2 |
| **Memory** | <1GB fixed | 4-16x less than alternatives |
| **Reliability** | Smart resume | Network resilience with retry |
| **Intelligence** | Geographic fuzzy matching | Semantic understanding |

---

## Upcoming Tools

### 🔄 **Development Roadmap**

#### **Phase 2: Geometric Operations** 
- **butterfly-shrink**: Polygon-based extraction with GEOS integration
- Target: **10x faster** than osmium extract
- Memory limit: **<2GB** for planet-scale operations

#### **Phase 3: Data Transformation**
- **butterfly-extract**: Advanced filtering and transformation engine  
- Target: **5-10x faster** than osmosis
- Memory limit: **<1GB** for streaming operations

#### **Phase 4: High-Performance Serving**
- **butterfly-serve**: HTTP tile server with intelligent caching
- Target: **10-50x faster** QPS than existing solutions
- Memory limit: **<500MB** baseline + configurable caching

### 🎯 **Ecosystem Goals**

- **10x Performance**: Across all operations vs state-of-the-art
- **Minimal Memory**: Fixed memory usage regardless of data size  
- **Modern Architecture**: Rust's safety + async performance
- **Composable Design**: Unix philosophy applied to OSM processing

---

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md) for ecosystem development guidelines.

### Performance Standards
- All performance claims must be benchmarked
- Memory usage must be predictable and bounded
- Tools must compose via standard streams and formats

### Tool Development
- Each tool has a single, well-defined responsibility
- Shared functionality goes in butterfly-common
- Comprehensive test coverage including performance tests

---

**butterfly-osm** - Hurricane-fast OSM processing for the modern era.