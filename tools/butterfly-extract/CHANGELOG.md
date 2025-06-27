# butterfly-extract Changelog

All notable changes to butterfly-extract (data transformation tool) will be documented in this file.

This is the detailed changelog for the butterfly-extract tool. For ecosystem-level changes, see the [main CHANGELOG](../../CHANGELOG.md).

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Planned Features
- **🔧 Advanced Filtering**: High-performance OSM data transformation and filtering
- **⚡ Performance Target**: 5-10x faster than osmosis
- **🧠 Memory Efficiency**: <1GB RAM for streaming operations
- **📊 Format Conversion**: Support for multiple input/output formats (PBF, XML, JSON)
- **🎯 Smart Filtering**: Tag-based filtering with geographic constraints
- **🔄 Pipeline Integration**: Seamless composition with other butterfly tools

### Architecture Goals
- **Streaming First**: Process massive datasets without memory bloat
- **Rule Engine**: Flexible, composable filtering rules
- **Format Agnostic**: Support multiple OSM data formats
- **Modern Performance**: Async I/O and parallel processing where beneficial

---

**Status**: Planned for butterfly-osm Phase 3

For ecosystem updates, see the [main changelog](../../CHANGELOG.md).