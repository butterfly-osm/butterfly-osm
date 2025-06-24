# Butterfly Project

A collection of tools and services for OpenStreetMap data processing and analysis.

## Geofabrik PBF Downloader

A Rust library and CLI tool for downloading OpenStreetMap PBF files from Geofabrik with Docker support.

## What

Downloads OSM data in PBF format from [Geofabrik](https://download.geofabrik.de/) with support for:
- Individual countries and continents
- Batch downloads of multiple regions
- List available regions  
- Dry-run mode for previewing downloads
- **Multi-connection parallel downloads** for faster speeds
- Dockerized execution with volume mounting
- Configuration files and environment variables
- Structured logging and error handling

## Why

Simplifies OSM data acquisition for mapping applications, routing engines, and geospatial analysis by providing a reliable, containerized download tool with comprehensive features.

## How

### Docker (Recommended)

```bash
# Build
make build

# List available regions
make run ARGS="list"
make run ARGS="list --filter countries"
make run ARGS="list --filter continents"

# Download individual regions
make run ARGS="country monaco"
make run ARGS="continent antarctica"

# Batch downloads
make run ARGS="countries monaco,andorra,malta"
make run ARGS="continents europe,africa"

# Dry-run mode (preview without downloading)
make run ARGS="--dry-run country monaco"
make run ARGS="--dry-run continent europe"
```

### Native

```bash
cargo build --release

# List regions
./target/release/geofabrik-downloader list

# Download examples
./target/release/geofabrik-downloader country monaco
./target/release/geofabrik-downloader continent europe
./target/release/geofabrik-downloader countries monaco,andorra
./target/release/geofabrik-downloader --dry-run country monaco
```

### Configuration

**Convention over configuration** - Uses sensible defaults, customize via environment variables:

Default settings:
- **8 parallel connections** for maximum speed
- **100MB chunks** for optimal performance  
- **Multi-connection enabled** by default
- **Auto-detection** of server capabilities

Environment variables (.env file):
```bash
# Basic settings
GEOFABRIK_DATA_DIR=/data
GEOFABRIK_LOG_LEVEL=info

# Multi-connection download settings (defaults optimized for performance)
GEOFABRIK_PARALLEL_CONNECTIONS=8
GEOFABRIK_CHUNK_SIZE=104857600  # 100MB
GEOFABRIK_ENABLE_PARALLEL_DOWNLOAD=true

# Logging
RUST_LOG=info
```

## Performance

The downloader automatically uses **multi-connection parallel downloads** when possible, significantly improving download speeds for large files:

- **Range request detection**: Automatically checks if the server supports partial downloads
- **Parallel chunks**: Downloads multiple chunks simultaneously (default: 8 connections)
- **Configurable**: Adjust connections and chunk size via configuration
- **Fallback**: Gracefully falls back to single-connection if server doesn't support ranges
- **Progress tracking**: Real-time progress with connection count display

Example speed improvements:
- Single connection: ~2-5 MB/s
- 4 parallel connections: ~8-20 MB/s  
- 8 parallel connections: ~15-40 MB/s (for large files on fast connections)

## File Structure

Downloaded files are organized as:
```
./data/pbf/
├── europe/
│   ├── monaco.pbf
│   └── andorra.pbf
└── africa/
    └── ...
```

## Development

Docker-first development with XP practices:

```bash
make build    # Build container
make test     # Run tests  
make clean    # Clean up
```

## Status

✅ **v0.1.0 Released** - First component of butterfly project complete
🚀 **Production Ready** - Geofabrik downloader fully functional
🎯 **Foundation Established** - Docker, testing, and development workflow ready
📋 **Future Components**: This is the first of many planned components in the butterfly ecosystem

## Contributing

This project follows XP pair programming with human + AI collaboration. See [CLAUDE.md](CLAUDE.md) for development guidelines.

## Who

**Butterfly Project** built by Pierre <pierre@warnier.net> for the broader OpenStreetMap community.

This is v0.1.0 - the first component in a larger ecosystem of OSM tools and services.
