# Butterfly-dl 🦋

A high-performance, memory-efficient OpenStreetMap data downloader with intelligent source routing and curl-like behavior.

## Features

- **🚀 Optimized for Large Files**: <1GB RAM usage regardless of file size (including 81GB planet.osm.pbf)
- **🧠 Smart Source Routing**: S3 for planet files, HTTP with parallel downloads for regional extracts
- **📡 Multiple Protocols**: Native AWS S3 support + HTTP with range requests
- **💧 Streaming Support**: Direct stdout streaming for pipeline integration
- **⚡ Performance Optimized**: Auto-tuning connections, Direct I/O for large files
- **🔧 Curl-like Interface**: Simple positional arguments, stderr logging

## Installation

```bash
# Build from source
git clone https://github.com/username/butterfly
cd butterfly
cargo build --release
```

## Usage

### Basic Examples

```bash
# Download planet file from S3 (81GB)
butterfly-dl planet

# Download regional extract from Geofabrik
butterfly-dl europe/belgium

# Download continent
butterfly-dl europe

# Stream to stdout
butterfly-dl europe/monaco - | gzip > monaco.pbf.gz

# Save to specific file
butterfly-dl planet planet-backup.pbf

# Verbose output
butterfly-dl --verbose europe/belgium
```

### Source Resolution

| Input | Source | Description |
|-------|--------|-------------|
| `planet` | S3 | Planet file from `s3://osm-planet-eu-central-1/planet-latest.osm.pbf` |
| `europe` | HTTP | Continent from `https://download.geofabrik.de/europe-latest.osm.pbf` |
| `europe/belgium` | HTTP | Country from `https://download.geofabrik.de/europe/belgium-latest.osm.pbf` |

### Output Options

- **No output argument**: Auto-generated filename (e.g., `belgium-latest.osm.pbf`)
- **Filename**: Save to specified file
- **`-`**: Stream to stdout (logs go to stderr)

## Performance Features

### Memory Efficiency
- **Fixed 64KB buffers**: Memory usage independent of file size
- **Ring buffer ordering**: Small memory footprint for parallel downloads
- **Direct I/O**: Bypasses OS page cache for files >1GB (Unix systems)
- **Streaming writes**: No intermediate accumulation

### Download Optimization
- **S3**: Single optimized stream for maximum AWS backbone utilization
- **HTTP**: Auto-tuned parallel range requests (2-16 connections based on file size)
- **Fallback**: Graceful degradation for servers without range support
- **Progress tracking**: Real-time progress bars to stderr

### Intelligent Defaults
- **Connection scaling**: Based on file size and CPU count
- **Protocol selection**: Optimal source for each data type
- **Error handling**: Robust retry and fallback mechanisms

## Technical Architecture

### Memory Usage Breakdown
```
Connection buffers:    16 × 64KB = 1MB
Ring buffer:          64MB (max)
HTTP client overhead: ~50MB
S3 SDK:              ~100MB
Runtime:             ~50MB
Total:               ~265MB (well under 1GB limit)
```

### Direct I/O Support
Automatically enabled for files >1GB on Unix systems:
- Bypasses OS page cache
- Reduces memory pressure
- Optimizes large sequential writes
- Falls back gracefully if not available

## CLI Reference

```
butterfly-dl [OPTIONS] <SOURCE> [OUTPUT]

Arguments:
  <SOURCE>  Source identifier (e.g., "planet", "europe", "europe/belgium")
  [OUTPUT]  Output file path, or "-" for stdout

Options:
      --dry-run  Show what would be downloaded without downloading
  -v, --verbose  Enable verbose logging
  -h, --help     Print help
  -V, --version  Print version
```

## Examples

### Planet Download (81GB)
```bash
# Download planet file (uses S3, single stream, Direct I/O)
butterfly-dl planet

# Stream planet to compressed archive
butterfly-dl planet - | gzip > planet.pbf.gz
```

### Regional Downloads
```bash
# Download all of Europe (parallel HTTP ranges)
butterfly-dl europe

# Download specific country
butterfly-dl europe/germany

# Download to custom location
butterfly-dl asia/japan japan-$(date +%Y%m%d).pbf
```

### Pipeline Integration
```bash
# Stream and process immediately
butterfly-dl europe/monaco - | osmium fileinfo -

# Compress on the fly
butterfly-dl europe/switzerland - | bzip2 > switzerland.pbf.bz2

# Chain with other tools
butterfly-dl planet - | osmium extract --bbox 2.3,46.8,2.4,46.9 -o monaco-bbox.pbf -
```

## Development

### Building
```bash
cargo build --release
```

### Testing
```bash
# Run all tests
cargo test

# Run with verbose output
cargo test -- --nocapture
```

### Performance Testing
```bash
# Test with small file
time butterfly-dl europe/monaco

# Test streaming
butterfly-dl europe/monaco - | wc -c
```

## Architecture

- **Rust + Tokio**: Async/await for concurrent downloads
- **AWS SDK**: Native S3 integration with anonymous access
- **Reqwest**: HTTP client with connection pooling and range requests
- **Indicatif**: Progress bars to stderr
- **Ring buffer**: Maintains chunk ordering with minimal memory

## Comparison with Alternatives

| Tool | Memory (81GB file) | Parallel Downloads | S3 Support | Streaming |
|------|-------------------|-------------------|------------|-----------|
| `butterfly-dl` | ~265MB | Yes (HTTP) | Native | Yes |
| `curl` | ~10MB | No | No | Yes |
| `aws s3 cp` | ~100MB | No | Yes | Yes |
| `aria2c` | ~500MB+ | Yes | No | Limited |

## License

MIT License - see LICENSE file for details.

## Contributing

This project follows XP pair programming with human + AI collaboration. See [CLAUDE.md](CLAUDE.md) for development guidelines.

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## Who

**Butterfly Project** built by Pierre <pierre@warnier.net> for the broader OpenStreetMap community.

---

**Butterfly-dl**: The optimal tool for downloading large OpenStreetMap datasets efficiently.
