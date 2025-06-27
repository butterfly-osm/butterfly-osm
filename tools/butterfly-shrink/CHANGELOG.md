# butterfly-shrink Changelog

All notable changes to butterfly-shrink (polygon extraction tool) will be documented in this file.

This is the detailed changelog for the butterfly-shrink tool. For ecosystem-level changes, see the [main CHANGELOG](../../CHANGELOG.md).

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **📖 PBF Reader Skeleton**: Implemented using osmpbf crate for reading OpenStreetMap PBF files
- **🔄 Echo Mode**: Read input PBF and write bitwise identical output (Issue #25)
- **🧪 Integration Tests**: Automated tests that download real OSM data (Monaco) using butterfly-dl
- **⚙️ Basic CLI**: Updated to accept input/output PBF file arguments

### Planned Features
- **🏗️ Polygon-based Extraction**: High-performance geometric operations for OSM data
- **⚡ Performance Target**: 10x faster than osmium extract
- **🧠 Memory Efficiency**: <2GB RAM for planet-scale operations
- **🔧 GEOS Integration**: Advanced geometric algorithms and spatial operations
- **📐 Smart Clipping**: Optimized polygon clipping with spatial indexing
- **🌍 Geographic Accuracy**: Precision handling of geographic boundaries

### Architecture Goals
- **Shared Foundation**: Built on butterfly-common for consistency
- **Streaming Design**: Process data without loading entire datasets
- **Composable Interface**: Works seamlessly with butterfly-dl and butterfly-extract
- **Modern Performance**: Leverage Rust's zero-cost abstractions and SIMD

## [2.0.0] - 2025-06-27

### Added
- **🚀 Initial Cargo Scaffold**: Created butterfly-shrink binary crate with CLI skeleton
- **📋 Project Planning**: Comprehensive GitHub issues roadmap (Issues #24-#38)
- **🔧 Development Foundation**: Basic clap CLI with --help functionality
- **✅ CI Integration**: Automated build, test, and clippy checks
- **📚 Documentation**: README, CHANGELOG, and project planning documents

### Development
- Created complete milestone-based development plan (M0-M4)
- Established dependency graph for 15 planned features
- Set up testing framework for future implementations

---

**Status**: In development as part of butterfly-osm Phase 2

For ecosystem updates, see the [main changelog](../../CHANGELOG.md).