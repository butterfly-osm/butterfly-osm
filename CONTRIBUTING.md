# Contributing to Butterfly-OSM

Welcome! We're building hurricane-fast OSM processing tools through **performance-first development** with human + AI collaboration.

## Mission

Create OSM tools that are **10x faster** than state-of-the-art while using minimal memory and modern architecture.

## Development Philosophy

### Core Principles
- **Performance first**: Every change should maintain or improve performance
- **Memory conscious**: Fixed memory usage patterns preferred over dynamic allocation
- **Test driven**: Comprehensive benchmarks for all performance claims
- **Unix philosophy**: Small, composable tools that do one thing exceptionally well
- **Modern architecture**: Leverage Rust's zero-cost abstractions and async ecosystem

### When in Doubt
**Measure first, optimize second. Performance claims require benchmarks.**

## Ecosystem Overview

### Tools Architecture
```
butterfly-dl    → Data acquisition (download, streaming)
butterfly-shrink → Geometric operations (extraction, clipping)  
butterfly-extract → Data transformation (filtering, conversion)
butterfly-serve  → Data serving (HTTP, caching, tiles)
```

### Shared Foundation
- **butterfly-common**: Geographic algorithms, error handling, performance primitives
- **Composable design**: Tools work via standard streams and file formats
- **Performance targets**: 10x improvement over existing solutions

## Getting Started

### Prerequisites
- Rust 1.70+ (for async/await and performance features)
- Git
- Docker (for integration testing)

### Setup
```bash
git clone https://github.com/butterfly-osm/butterfly-osm
cd butterfly-osm

# Build all tools
cargo build --workspace --release

# Run tests with benchmarks
cargo test --workspace

# Install for development
cargo install --path tools/butterfly-dl
```

## Development Workflow

### 1. Performance-Driven Development

```bash
# Always start with benchmarks
./benchmarks/bench.sh europe/luxembourg  # Baseline
# ... make changes ...
./benchmarks/bench.sh europe/luxembourg  # Verify improvement

# Memory testing
cargo build --release -p butterfly-dl
valgrind --tool=massif ./target/release/butterfly-dl europe/monaco
```

### 2. Workspace Development

```bash
# Work on specific tool
cd tools/butterfly-dl
cargo test
cargo build --release

# Test integration with other tools
echo "test data" | butterfly-dl - | butterfly-extract --filter tags
```

### 3. Branch Strategy
```bash
# Feature branches for new capabilities
git checkout -b feature/simd-geometric-ops

# Performance branches for optimizations  
git checkout -b perf/zero-copy-streaming

# Use descriptive commits
git commit -m "perf(dl): reduce memory allocation by 40% with ring buffer"
```

## Performance Standards

### Benchmarking Requirements

**All performance claims must be verified:**
```bash
# Required benchmarks for PRs
./benchmarks/bench.sh europe/monaco      # Small files
./benchmarks/bench.sh europe/luxembourg  # Medium files  
./benchmarks/bench.sh europe/belgium     # Large files

# Memory usage verification
cargo build --release -p butterfly-dl
time -v ./target/release/butterfly-dl europe/belgium
```

### Performance Targets

| Tool | Memory Usage | Speed Target | Improvement Goal |
|------|-------------|--------------|------------------|
| butterfly-dl | <1GB fixed | 10-20MB/s | 3-6x vs aria2 |
| butterfly-shrink | <2GB fixed | <30s extraction | 10x vs osmium |
| butterfly-extract | <1GB fixed | 50MB/s filtering | 5-10x vs osmosis |
| butterfly-serve | <500MB | 5000+ QPS | 10-50x vs existing |

### Code Quality Standards

#### Memory Management
```rust
// Prefer fixed-size buffers
const BUFFER_SIZE: usize = 64 * 1024; // 64KB
let mut buffer = [0u8; BUFFER_SIZE];

// Avoid dynamic allocation in hot paths
// BAD: Vec::new() in tight loops
// GOOD: Pre-allocated buffers with reuse
```

#### Error Handling
```rust
// Use shared error types from butterfly-common
use butterfly_common::Error;

// Provide geographic context in errors
Err(Error::SourceNotFound {
    input: "luxemburg".to_string(),
    suggestion: Some("europe/luxembourg".to_string()),
})
```

#### Async Patterns
```rust
// Use Tokio ecosystem consistently
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::fs::File;

// Prefer streaming over buffering
async fn process_stream<R: AsyncRead>(mut reader: R) -> Result<()> {
    // Process in chunks, don't load entire file
}
```

## Tool-Specific Guidelines

### butterfly-dl
- **Focus**: Network efficiency and resumable downloads
- **Memory limit**: <1GB regardless of file size
- **Key metrics**: Download speed, memory usage, reliability

### butterfly-shrink (planned)
- **Focus**: Geometric operations and polygon clipping
- **Memory limit**: <2GB for planet-scale operations
- **Key metrics**: Extraction speed, geometric accuracy

### butterfly-extract (planned)
- **Focus**: Data transformation and filtering
- **Memory limit**: <1GB for streaming operations
- **Key metrics**: Filtering speed, memory efficiency

### butterfly-serve (planned)
- **Focus**: High-performance HTTP serving
- **Memory limit**: <500MB baseline + caching
- **Key metrics**: QPS, latency, cache hit rate

## Testing Requirements

### Performance Tests
```bash
# Required for all PRs
cargo test --workspace --release

# Benchmark verification
./benchmarks/verify_performance.sh

# Memory testing
cargo test --features memory-profiling
```

### Unit Tests
- Test all public APIs
- Include error cases and edge conditions
- Use descriptive test names
- Mock external dependencies

### Integration Tests
- Test tool combinations and pipelines
- Verify file format compatibility
- Test with real OSM data samples

## Submitting Changes

### Pull Request Process

1. **Performance verification**: Include benchmark results
2. **Memory analysis**: Verify memory usage remains within limits
3. **Test coverage**: All new code must have tests
4. **Documentation**: Update tool-specific README if needed
5. **Commit format**: Use conventional commits

### Commit Message Format
```
type(tool): description

perf(dl): optimize HTTP connection pooling for 25% speed improvement
feat(shrink): add polygon clipping with GEOS integration
fix(common): resolve geographic fuzzy matching edge case
docs(readme): update performance benchmark results
```

Types: `feat`, `fix`, `perf`, `docs`, `test`, `refactor`

### PR Description Template
```markdown
## Performance Impact
- Speed: X% faster/slower than baseline
- Memory: X MB reduction/increase  
- Benchmark results: [link to results]

## Changes
- Brief description of what changed
- Why this change improves the ecosystem

## Testing
- [ ] Unit tests pass
- [ ] Integration tests pass  
- [ ] Performance benchmarks run
- [ ] Memory usage verified
```

## Local Development

### Quick Commands
```bash
# Build specific tool
cargo build --release -p butterfly-dl

# Run with profiling
cargo build --release -p butterfly-dl --features profiling
time ./target/release/butterfly-dl europe/monaco

# Memory analysis
valgrind --tool=massif ./target/release/butterfly-dl europe/monaco

# Benchmark comparison
./benchmarks/compare.sh baseline feature-branch
```

### Performance Debugging
```bash
# CPU profiling
cargo build --release -p butterfly-dl
perf record ./target/release/butterfly-dl europe/belgium
perf report

# Memory debugging
cargo build --features memory-debug
RUST_LOG=debug ./target/release/butterfly-dl europe/monaco
```

## Issue Reporting

### Performance Issues
Include:
- Benchmark comparison (before/after)
- Memory usage measurements
- Profiling data if available
- System specifications

### Bug Reports
Include:
- Steps to reproduce
- Expected vs actual behavior
- Performance impact (if any)
- Tool version and platform

## Release Process

### Performance Validation
Before any release:
1. **Full benchmark suite**: All tools tested
2. **Memory regression testing**: Ensure no memory leaks
3. **Cross-platform verification**: Linux, macOS, Windows
4. **Real-world testing**: Large dataset verification

### Automated Releases
- Tag-based releases trigger automated builds
- Multi-platform binaries with checksums
- Performance regression detection in CI
- Automated crates.io publication

## Getting Help

### Performance Optimization
- Use `cargo flamegraph` for CPU profiling
- Use `heaptrack` for memory analysis
- Consult Rust Performance Book
- Ask for review on performance-critical changes

### Project Resources
- [Rust Performance Book](https://nnethercote.github.io/perf-book/)
- [Tokio Documentation](https://tokio.rs/)
- [SIMD in Rust](https://doc.rust-lang.org/std/simd/)
- [Geographic Algorithms](https://en.wikipedia.org/wiki/Computational_geometry)

### Project Maintainer
Pierre <pierre@warnier.net>

---

**Philosophy**: We're not just building faster tools - we're demonstrating what's possible with modern language features and thoughtful architecture.

**butterfly-osm** - Hurricane-fast performance through intelligent design.