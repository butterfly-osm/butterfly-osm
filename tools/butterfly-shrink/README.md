# butterfly-shrink 🦋

`butterfly-shrink` is a high-performance, memory-efficient tool within the `butterfly-osm` ecosystem designed to optimize OpenStreetMap (OSM) PBF files for routing applications. It strips non-routing data and collapses nodes to a configurable grid resolution, maintaining compatibility with standard routing engines.

## Development Status

**Current Phase**: M0 - Bootstrap. PBF reader and writer skeleton implemented.

## Features

- ✅ Read OpenStreetMap PBF files
- ✅ Echo mode: Copy PBF files (bitwise identical)
- 🚧 Filter non-routing data (coming soon)
- 🚧 Node collapsing to grid resolution (coming soon)

## Usage

```bash
# Echo a PBF file (creates bitwise identical copy)
butterfly-shrink input.pbf output.pbf
```

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