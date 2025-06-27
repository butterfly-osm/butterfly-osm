# butterfly-extract 🦋

Data transformation and filtering tool for OSM data.

## Overview

butterfly-extract is planned as the data transformation component of the butterfly-osm ecosystem, focused on filtering and converting OSM data formats.

## Performance Goals

- **⚡ Speed**: 5-10x faster than osmosis
- **🧠 Memory**: <1GB RAM for streaming operations
- **🎯 Focus**: Data filtering and format conversion

## Planned Core Function

Filter and transform OSM data while maintaining the butterfly-osm performance and memory efficiency standards.

## Development Status

**Current Phase**: Planned (Phase 3 of butterfly-osm roadmap)

### Known Requirements
- Must integrate with butterfly-common for consistent error handling
- Must maintain ecosystem performance standards (<1GB memory usage)
- Must work with butterfly-dl and butterfly-shrink in pipeline workflows
- Must be 5-10x faster than existing tools like osmosis
- Must support multiple OSM data formats

### Design Constraints
- Memory usage must be predictable and bounded
- Must support standard input/output for pipeline composition
- Must maintain data integrity during transformations
- Must handle streaming operations efficiently

## Contributing

See the main [CONTRIBUTING.md](../../CONTRIBUTING.md) for ecosystem development guidelines.

---

**Status**: Planned for butterfly-osm Phase 3

Part of the [butterfly-osm ecosystem](../../README.md) - hurricane-fast OSM tools.