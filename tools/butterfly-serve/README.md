# butterfly-serve 🦋

High-performance HTTP server for OSM data and tiles.

## Overview

butterfly-serve is planned as the serving component of the butterfly-osm ecosystem, focused on delivering OSM data and tiles with exceptional performance.

## Performance Goals

- **⚡ Speed**: 10-50x faster QPS than existing solutions
- **🧠 Memory**: <500MB baseline + configurable caching
- **🎯 Focus**: High-performance data and tile serving

## Planned Core Function

Serve OSM data and tiles via HTTP while maintaining the butterfly-osm performance and memory efficiency standards.

## Development Status

**Current Phase**: Planned (Phase 4 of butterfly-osm roadmap)

### Known Requirements
- Must integrate with butterfly-common for consistent error handling
- Must maintain ecosystem performance standards (<500MB baseline memory)
- Must work with processed data from other butterfly tools
- Must be 10-50x faster than existing tile servers
- Must support HTTP protocols for web compatibility

### Design Constraints
- Memory usage must be predictable and bounded
- Must handle high concurrent connection loads
- Must provide both data and tile serving capabilities
- Response times must be consistently fast

## Contributing

See the main [CONTRIBUTING.md](../../CONTRIBUTING.md) for ecosystem development guidelines.

---

**Status**: Planned for butterfly-osm Phase 4

Part of the [butterfly-osm ecosystem](../../README.md) - hurricane-fast OSM tools.