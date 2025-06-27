# butterfly-serve Changelog

All notable changes to butterfly-serve (high-performance tile server) will be documented in this file.

This is the detailed changelog for the butterfly-serve tool. For ecosystem-level changes, see the [main CHANGELOG](../../CHANGELOG.md).

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Planned Features
- **🌐 High-Performance HTTP Server**: Hurricane-fast tile serving for OSM data
- **⚡ Performance Target**: 10-50x faster QPS than existing solutions
- **🧠 Memory Efficiency**: <500MB baseline + configurable caching
- **🗂️ Intelligent Caching**: Multi-layer caching with smart eviction policies
- **📡 Modern Protocols**: HTTP/2, compression, and connection pooling
- **🎯 Tile Generation**: On-demand tile generation with background pre-rendering

### Architecture Goals
- **Async Foundation**: Built on Tokio for maximum concurrency
- **Smart Caching**: Memory + disk caching with geographic awareness
- **Scalable Design**: Horizontal scaling with cluster support
- **Modern HTTP**: HTTP/2, WebSockets, and efficient compression

---

**Status**: Planned for butterfly-osm Phase 4

For ecosystem updates, see the [main changelog](../../CHANGELOG.md).