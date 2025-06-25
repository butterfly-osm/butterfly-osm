# Butterfly-dl 🦋

A high-performance, memory-efficient OpenStreetMap data downloader with intelligent source routing. Downloads one file at a time with optimal performance.

## Features

- **🚀 Optimized for Large Files**: <1GB RAM usage regardless of file size (including 81GB planet.osm.pbf)
- **🧠 Smart Source Routing**: HTTP with parallel downloads optimized by file size
- **🔍 Intelligent Error Messages**: Fuzzy matching with geographic accuracy for typos and misspellings
- **🌍 Dynamic Source Loading**: Automatically fetches latest available regions from Geofabrik
- **📡 HTTP Protocol**: Advanced HTTP with range requests and connection pooling
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
# Download planet file from HTTP (81GB) 
butterfly-dl planet

# Download continent from HTTP
butterfly-dl europe

# Download country/region from HTTP  
butterfly-dl europe/belgium

# Stream to stdout for processing
butterfly-dl europe/monaco - | gzip > monaco.pbf.gz

# Save to custom file name
butterfly-dl planet planet-backup.pbf

# Verbose output with source info
butterfly-dl --verbose europe/belgium
```

### Source Resolution

| Input | Source | Description |
|-------|--------|-------------|
| `planet` | HTTP | Planet file from `https://planet.openstreetmap.org/pbf/planet-latest.osm.pbf` |
| `europe` | HTTP | Continent from `https://download.geofabrik.de/europe-latest.osm.pbf` |
| `europe/belgium` | HTTP | Country from `https://download.geofabrik.de/europe/belgium-latest.osm.pbf` |

### Intelligent Error Handling

butterfly-dl includes smart error correction with fuzzy matching:

```bash
# Typo correction
butterfly-dl antartica
# Error: Source 'antartica' not found. Did you mean 'antarctica'?

# Geographic accuracy  
butterfly-dl antartica/belgium
# Error: Source 'antartica/belgium' not found. Did you mean 'europe/belgium'?

# Standalone country recognition
butterfly-dl luxembourg
# Error: Source 'luxembourg' not found. Did you mean 'europe/luxembourg'?

# Smart continent suggestions
butterfly-dl plant
# Error: Source 'plant' not found. Did you mean 'planet'?
```

**Features:**
- **Dynamic Source Discovery**: Automatically fetches available regions from Geofabrik JSON API
- **Fuzzy Matching**: Uses Levenshtein distance algorithm for typo detection
- **Geographic Intelligence**: Knows Belgium belongs to Europe, not Antarctica
- **Fallback Protection**: Works offline with comprehensive fallback region list

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
- **HTTP**: Single optimized stream for maximum network utilization
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
Runtime:             ~50MB
Total:               ~215MB (well under 1GB limit)
```

### Direct I/O Support
Automatically enabled for files >1GB on Unix systems:
- Bypasses OS page cache
- Reduces memory pressure
- Optimizes large sequential writes
- Falls back gracefully if not available

## CLI Reference

```
Downloads single OpenStreetMap files efficiently:
  butterfly-dl planet              # Download planet file (81GB) from HTTP
  butterfly-dl europe              # Download Europe continent from HTTP
  butterfly-dl europe/belgium      # Download Belgium from HTTP
  butterfly-dl europe/monaco -     # Stream Monaco to stdout

Usage: butterfly-dl [OPTIONS] <SOURCE> [OUTPUT]

Arguments:
  <SOURCE>  Source to download: "planet" (HTTP), "europe" (continent), or "europe/belgium" (country/region)
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
# Download planet file (uses HTTP, single stream, Direct I/O)
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

### Version Management

The project uses a centralized version management system to maintain consistency across all components:

**📄 Single Source of Truth:**
- **`VERSION`** file contains the current version number (e.g., `1.0.0`)
- All other files automatically read from this central location

**🔧 Automatic Version Propagation:**
- **CLI tool**: Uses `env!("BUTTERFLY_VERSION")` from build script
- **HTTP User-Agent**: Dynamically includes version in requests
- **Library exports**: Version available via build-time environment
- **C bindings**: pkg-config file includes correct version
- **Documentation**: Version stays in sync automatically

**🔄 Build Integration:**
- `build.rs` reads `VERSION` file and sets environment variables
- Any change to `VERSION` triggers automatic rebuild
- Build system tracks version file as dependency

**📝 Updating Version:**
```bash
# Update version for new release
echo "1.1.0" > VERSION

# Rebuild automatically picks up new version
cargo build --release

# All components now use 1.1.0
./target/release/butterfly-dl --version  # Shows 1.1.0
```

**Note:** `Cargo.toml` version must still be updated manually due to Cargo limitations.

## Architecture

- **Rust + Tokio**: Async/await for concurrent downloads
- **HTTP Client**: Advanced reqwest client with connection pooling
- **Reqwest**: HTTP client with connection pooling and range requests
- **Indicatif**: Progress bars to stderr
- **Ring buffer**: Maintains chunk ordering with minimal memory

## Comparison with Alternatives

| Tool | Memory (81GB file) | Parallel Downloads | HTTP Features | Streaming |
|------|-------------------|-------------------|------------|-----------|
| `butterfly-dl` | ~215MB | Yes (Smart) | Advanced | Yes |
| `curl` | ~10MB | No | Basic | Yes |
| `aria2c` | ~500MB+ | Yes | Basic | Limited |
| `wget` | ~10MB | No | Basic | No |

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
