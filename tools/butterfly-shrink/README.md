# butterfly-shrink 🦋

`butterfly-shrink` is a high-performance, memory-efficient tool within the `butterfly-osm` ecosystem designed to optimize OpenStreetMap (OSM) PBF files for routing applications. It strips non-routing data and collapses nodes to a configurable grid resolution, maintaining compatibility with standard routing engines.

## Development Status

**Current Phase**: Production Ready - BCSI processor optimized for performance.

## Features

- ✅ Read/Write OpenStreetMap PBF files
- ✅ Grid-based node deduplication (5m default resolution)
- ✅ Highway tag filtering for routing applications
- ✅ BCSI (Block-Compressed Sorted Index) for memory-efficient processing
- ✅ Hard 4GB memory cap for cloud/embedded deployments
- ✅ O(1) block cache lookups for fast performance

## Usage

```bash
# Process with BCSI (recommended - fast, <4GB RAM)
butterfly-shrink --bcsi input.pbf output.pbf

# Emergency mode (guaranteed <4GB, slower)
butterfly-shrink --bcsi-emergency input.pbf output.pbf

# Two-pass mode (original, uses RocksDB)
butterfly-shrink --two-pass input.pbf output.pbf
```

## Performance

Processing Belgium (577MB input):
- **BCSI mode**: ~167s total, 1.7GB peak RAM
- **Output**: 378MB (40% reduction)
- **Nodes**: 69M → 53M (23% reduction via grid snapping)
- **Ways**: 11M → 798K (filtered to routing-relevant highways)

## Development

### Running Tests

Tests require a PBF file which will be automatically downloaded using butterfly-dl:

```bash
cargo test
```

The first test run will download Monaco (~500KB) for testing purposes.

## Contributing

See the main [CONTRIBUTING.md](../../CONTRIBUTING.md) for ecosystem development guidelines.

---

Part of the [butterfly-osm ecosystem](../../README.md) - hurricane-fast OSM tools.