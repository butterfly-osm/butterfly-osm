# butterfly-shrink 🦋

Polygon-based area extraction tool for OSM data.

## Overview

butterfly-shrink is planned as the geometric operations component of the butterfly-osm ecosystem, focused on extracting OSM data within specified geographic boundaries.

## Performance Goals

- **⚡ Speed**: 10x faster than osmium extract
- **🧠 Memory**: <2GB RAM for planet-scale operations
- **🎯 Focus**: Area extraction with geometric boundaries

## Planned Core Function

Extract OSM data within polygon boundaries while maintaining the butterfly-osm performance and memory efficiency standards.

## Development Status

**Current Phase**: Planning (Phase 2 of butterfly-osm roadmap)

### Known Requirements
- Must integrate with butterfly-common for consistent error handling
- Must maintain ecosystem performance standards (<2GB memory usage)
- Must work with butterfly-dl in pipeline workflows
- Must be 10x faster than existing tools like osmium extract

### Design Constraints
- Memory usage must be predictable and bounded
- Must support standard input/output for pipeline composition
- Must maintain geometric accuracy

## Contributing

See the main [CONTRIBUTING.md](../../CONTRIBUTING.md) for ecosystem development guidelines.

---

**Status**: Planned for butterfly-osm Phase 2

Part of the [butterfly-osm ecosystem](../../README.md) - hurricane-fast OSM tools.