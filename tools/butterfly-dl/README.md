# butterfly-dl 🦋

High-performance, memory-efficient OpenStreetMap data downloader with intelligent source routing and resilient networking.

## Overview

butterfly-dl is the data acquisition component of the butterfly-osm ecosystem, optimized for downloading large OSM files with minimal memory usage and maximum reliability.

## Key Features

- **🚀 Memory Efficient**: <1GB RAM usage regardless of file size (including 81GB planet.osm.pbf)
- **🛡️ Network Resilient**: Intelligent retry with exponential backoff and smart resume
- **🧠 Smart Routing**: HTTP with parallel downloads optimized by file size
- **🔍 Intelligent Errors**: Advanced fuzzy matching with geographic understanding
- **💧 Streaming**: Direct stdout streaming for pipeline integration
- **⚡ Performance**: Auto-tuning connections, Direct I/O for large files

## Installation

From the workspace root:
```bash
cargo install --path tools/butterfly-dl
```

Or install from crates.io:
```bash
cargo install butterfly-dl
```

## Usage

### Basic Examples

```bash
# Download planet file (81GB)
butterfly-dl planet

# Download continent 
butterfly-dl europe

# Download country/region
butterfly-dl europe/belgium

# Stream to stdout for processing
butterfly-dl europe/monaco - | gzip > monaco.pbf.gz
```

### Advanced Features

#### Smart Error Correction
```bash
butterfly-dl austrailia
# Error: Source 'austrailia' not found. Did you mean 'australia-oceania'?

butterfly-dl luxemburg
# Error: Source 'luxemburg' not found. Did you mean 'europe/luxembourg'?
```

#### Network Resilience
```bash
# Automatic retry and resume on network failures
butterfly-dl europe/germany
# ⚠️ Network error: Retrying in 1000ms...
# ⚠️ Stream interrupted at 300MB, resuming...
# ✅ Download completed!
```

## Architecture

### Memory Management
- **Fixed 64KB buffers**: Predictable memory usage
- **Ring buffer**: Maintains chunk ordering efficiently
- **Direct I/O**: Bypasses OS cache for large files (>1GB)
- **Streaming writes**: No intermediate accumulation

### Protocol Optimization
- **Single stream**: For maximum network utilization on large files
- **Parallel ranges**: Auto-tuned connections (2-16) based on file size
- **Graceful fallback**: Works with servers without range support

### Error Handling
Uses shared error handling from `butterfly-common` with:
- Geographic-aware fuzzy matching
- Semantic intent recognition
- Dynamic source discovery from Geofabrik API

## Performance

### Benchmarks (43MB file)
```
Tool         Speed     Memory    
------------------------
butterfly-dl 14.07MB/s ~215MB   ✅
aria2        7.84MB/s  ~120MB   
curl         4.57MB/s  ~10MB    
```

**79% faster than aria2, 3x faster than curl**

### Memory Usage
```
Connection buffers: 16 × 64KB = 1MB
Ring buffer:       64MB (max)
HTTP overhead:     ~50MB
Runtime:          ~50MB
Total:            ~215MB
```

## CLI Reference

```
butterfly-dl [OPTIONS] <SOURCE> [OUTPUT]

Arguments:
  <SOURCE>  Source: "planet", "europe", "europe/belgium"
  [OUTPUT]  Output file or "-" for stdout

Options:
  --dry-run     Show what would be downloaded
  -v, --verbose Enable verbose logging
  -h, --help    Print help
  -V, --version Print version
```

## Integration

butterfly-dl is designed to work seamlessly with other butterfly-osm tools:

```bash
# Download and extract in pipeline
butterfly-dl europe/belgium - | butterfly-extract --bbox 4.0,50.0,6.0,52.0 - processed.pbf

# Download and serve
butterfly-dl planet
butterfly-serve planet-latest.osm.pbf --port 8080
```

## Development

### Building
```bash
# From workspace root
cargo build --release -p butterfly-dl

# From this directory
cargo build --release
```

### Testing
```bash
cargo test -p butterfly-dl
```

---

Part of the [butterfly-osm ecosystem](../../README.md) - hurricane-fast OSM tools.