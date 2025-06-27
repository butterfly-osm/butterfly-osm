# butterfly-common 🦋

Shared utilities and error handling for the butterfly-osm ecosystem.

## Overview

butterfly-common provides foundational components used across all butterfly-osm tools, ensuring consistency and shared functionality throughout the ecosystem.

## Current Components

### Error Handling
- **Geographic-aware Errors**: Smart error messages with fuzzy matching
- **Semantic Understanding**: Context-aware suggestions for OSM sources  
- **Consistent Error Types**: Unified error handling across all tools

### Known Features (From butterfly-dl)
- **Source Validation**: Fuzzy matching for OSM source names
- **Geographic Intelligence**: Knows geographic relationships (Belgium is in Europe)
- **Semantic Correction**: "austrailia" → "australia-oceania" (not "austria")

## Current Usage

Based on existing butterfly-dl integration:

```rust
use butterfly_common::Error;

// Geographic-aware error correction
suggest_correction("austrailia") → Some("australia-oceania")
suggest_correction("luxemburg") → Some("europe/luxembourg")  
suggest_correction("plant") → Some("planet")
```

## Architecture Goals

### Performance Standards
- Memory usage must be predictable and bounded
- Must support the butterfly-osm performance targets
- Must integrate with Rust async ecosystem

### Consistency Requirements
- Unified error handling across all tools
- Shared patterns for common operations
- Consistent API design throughout ecosystem

## Development Guidelines

### Shared Library Constraints
- Changes affect all tools in the ecosystem
- Must maintain backward compatibility where possible
- Performance regressions affect entire ecosystem
- API changes require coordination across all tools

### Testing Requirements
- Comprehensive unit tests for all shared functionality
- Integration tests with all tools that use the library
- Performance benchmarks for critical paths

## Contributing

See the main [CONTRIBUTING.md](../CONTRIBUTING.md) for ecosystem development guidelines.

---

Part of the [butterfly-osm ecosystem](../README.md) - hurricane-fast OSM tools.