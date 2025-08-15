# butterfly-shrink: Single-Pass Routing-Optimized PBF Processor

This document describes the production-ready design for butterfly-shrink, a tool that strips OSM PBF files to their routing essentials while maintaining compatibility with standard routing engines.

## Overview

**Purpose**: Transform any OSM PBF file into a minimal routing-ready version by:
- Removing non-routing data  
- Collapsing nodes to a fixed grid resolution
- Maintaining full compatibility with OSRM, Valhalla, GraphHopper
- Supporting true single-pass streaming with parallel processing

**Key Design Decisions**:
- **Grid**: Fixed resolution (1/2/5/10m) - NOT dynamic/adaptive
- **Index**: RocksDB only - NOT custom binary format
- **Errors**: Fail-fast on corruption - NO forward reference buffering
- **Parallelism**: Multi-threaded processing with ordered output

**Key Constraints**:
- Memory usage: <200MB active RAM (plus RocksDB managed memory)
- Single-pass processing for streaming compatibility
- Standard PBF output readable by any OSM tool
- Optimized for pipeline use with butterfly-dl
- Multi-core utilization while maintaining stream ordering

---

## 1. Core Features

### 1.1 What It Does

1. **Single-pass streaming**: Process PBF data in order (nodes → ways → relations)
2. **Parallel processing**: Multi-threaded architecture for CPU utilization
3. **Fixed grid snapping**: Configurable resolution (1/2/5/10m) with latitude-aware scaling
4. **Routing data extraction**: Keep only highway ways, turn restrictions, and essential tags
5. **Node deduplication**: Merge nodes in same grid cell using consistent rules
6. **Flexible I/O**: Support file input/output and stdin/stdout streaming
7. **Direct I/O**: Optional O_DIRECT for large files (Unix only)
8. **Metadata preservation**: Add data lineage to output PBF header

### 1.2 Output Characteristics

- Standard PBF format with dense nodes and Zstd compression
- WGS84 coordinates (snapped to grid centers)
- Minimal tags: `highway`, `oneway`, turn restrictions
- 90-98% size reduction typical
- Full routing graph connectivity preserved
- Metadata header with `writingprogram` and `source` tags for lineage

---

## 2. Design Rationale

### 2.1 Fail-Fast on Malformed Data

**Challenge**: PBF format guarantees nodes appear before ways that reference them. Any violation indicates corruption.

**Solution**: Immediate failure with diagnostics
- No buffering of "forward references" 
- Clear error message with byte offset
- Prevents silent graph corruption
- Simplifies implementation

**Error handling**:
```
ERROR: Data corruption detected at byte 1234567890
  Way 123456789 references non-existent node 987654321
  This indicates a malformed PBF file
  Debug with: osmconvert --out-statistics input.pbf
  Alternative: osmium check-refs input.pbf
```

### 2.2 Fixed Grid with Latitude Scaling

**Solution**: Fixed resolution with proper geographic scaling
- User selects resolution: 1m (urban), 2m, 5m (default), or 10m (rural)
- Latitude-aware longitude scaling
- Center-of-cell snapping to minimize bias
- High-latitude protection (maintain precision up to ±89.9°)

**Grid snapping mathematics**:
```rust
fn snap_coordinate(lat: f64, lon: f64, grid_meters: f64) -> (i64, i64) {
    // Keep all nodes, including far northern regions (Svalbard, Alert, etc.)
    // Clamp latitude to valid range but don't drop
    let lat_clamped = lat.clamp(-89.9, 89.9);
    
    let lat_scale = grid_meters / 111_111.0;
    
    // Accurate longitude scaling: 111_320m × cos(lat) at equator
    // At extreme latitudes (>85°), grid cells become very narrow E-W
    // This is correct behavior - maintains proper distances
    let cos_lat = lat_clamped.to_radians().cos().max(0.001); // Min ~89.9°
    let lon_scale = grid_meters / (111_320.0 * cos_lat);
    
    // Snap to cell center (floor + 0.5)
    let lat_snapped = ((lat_clamped / lat_scale).floor() + 0.5) * lat_scale;
    let lon_snapped = ((lon / lon_scale).floor() + 0.5) * lon_scale;
    
    // Store as nanodegrees (OSM format)
    let lat_nano = (lat_snapped * 1e9).round() as i64;
    let lon_nano = (lon_snapped * 1e9).round() as i64;
    
    (lat_nano, lon_nano)
}
```

**High-latitude behavior**:
- Svalbard (78°N): Grid cells ~11m N-S, ~2.3m E-W for 5m setting
- Alert, Canada (82.5°N): Grid cells ~11m N-S, ~1.5m E-W for 5m setting
- This maintains road connectivity in Arctic settlements

**Benefits**:
- Guarantees intersection connectivity
- Simple, predictable behavior
- Valid at all latitudes
- No projection dependencies

### 2.3 RocksDB for Node Index

**Storage requirements**: 
- Planet: ~8B nodes → ~1-2B after deduplication
- Index size: ~15GB compressed on disk
- Memory usage: 128MB block cache (configurable)

**Configuration**:
```rust
let mut opts = rocksdb::Options::default();
opts.set_compression_type(DBCompressionType::Zstd);
opts.set_block_cache(&Cache::new_lru_cache(128_MB));
opts.set_bloom_filter(10.0); // 10 bits per key
opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(8));
opts.set_optimize_filters_for_hits(true); // Most lookups will find a node

// WAL tuning for temporary database
opts.set_wal_ttl_seconds(60); // Keep WAL for only 1 minute
opts.set_wal_size_limit_mb(256); // Limit WAL size
// Optional: opts.disable_wal(true) for maximum performance

// Parallel processing tuning
let num_cores = num_cpus::get();
opts.set_max_background_jobs((num_cores / 4).max(2));
opts.set_num_levels(4); // Support parallel writes

// Batch write optimization
opts.set_write_buffer_size(64_MB);
opts.set_max_write_buffer_number(3);
```

**Benefits**:
- Automatic compression (30-40% savings)
- Configurable memory usage
- Crash recovery built-in
- Proven reliability at scale

### 2.4 Memory Model

**Active RAM budget**:
- PBF decompression: 32MB
- RocksDB block cache: 128MB (tunable)
- Way/relation buffers: 20MB
- Overhead: ~20MB
- **Total: <200MB** (plus kernel page cache)

**Disk requirements**:
- Temp directory: ~15GB for planet (5m grid)
  - Warning: 1m grid can require 600GB+
- Must be disk-backed (not tmpfs/RAM)
- Auto-cleanup on exit
- Location: `$TMPDIR/butterfly-shrink-{uuid}/`

### 2.5 Parallel Processing Architecture

**Thread design**: Multi-threaded processing while maintaining stream ordering
- **Reader thread**: Decodes PBF blobs from input stream
- **Worker pool**: Processes elements (snapping, RocksDB operations)
- **Writer thread**: Outputs processed elements in correct order

**Channel architecture**:
```rust
// Bounded channels to control memory usage
let (decoded_tx, decoded_rx) = channel::bounded(1000);
let (processed_tx, processed_rx) = channel::bounded(1000);

// Reader -> Workers
Reader Thread -> decoded_tx -> Worker Pool -> processed_tx -> Writer Thread
```

**Ordering guarantee**:
- Elements tagged with sequence numbers
- Writer buffers and reorders as needed
- Maximum reorder window: 1000 elements

**Worker pool sizing**:
```rust
let worker_count = num_cpus::get().min(8); // Cap at 8 workers
```

**Benefits**:
- Better CPU utilization (2-3x speedup on multi-core)
- I/O operations don't block processing
- Maintains strict output ordering for PBF compliance

### 2.6 Highway Tag Configuration

**Built-in presets**:
```bash
--preset car   # Motorways, roads, service ways
--preset bike  # Adds cycleways, appropriate paths  
--preset foot  # All walkable ways
```

**Custom YAML override**:
```yaml
# custom.yaml
version: 1  # Schema version for stability

grid_size_m: 5  # Pin grid size in config

highway_tags:
  include:
    - motorway
    - trunk
    - primary
    - secondary
    - tertiary
    - unclassified
    - residential
    
restrictions:
  keep_turn_restrictions: true
```

**Configuration precedence**:
1. CLI flags (highest priority)
2. External YAML file (`--config`)
3. Built-in preset (`--preset`)
4. Defaults

---

## 3. Implementation Design

### 3.1 Initialization

1. **Validate inputs**: Check grid size (1-20m), verify paths
2. **Tmpfs detection**: Check if $TMPDIR is RAM-backed filesystem
3. **Disk space check**: Ensure adequate space in $TMPDIR
4. **Create RocksDB**: `$TMPDIR/butterfly-shrink-{uuid}/node_index/`
5. **Open streams**: PBF reader (file/stdin), writer (file/stdout)
6. **Load configuration**: Built-in preset or custom YAML
7. **Setup parallelism**: Initialize thread pool and channels
8. **Write metadata**: Add lineage info to output PBF header

**Tmpfs detection**:
```rust
fn check_tmpfs(path: &Path) -> bool {
    // Linux: Check /proc/mounts for tmpfs
    // macOS: Check mount output for tmpfs/ramfs
    // Warning if detected, but continue
}
```

**Output metadata**:
```rust
header.set_writingprogram("butterfly-shrink v0.1.0");
if let Some(source) = input_metadata.get("source") {
    header.set_source(source);
}
header.set_timestamp(SystemTime::now());
```

**Startup messages**:
```
Using RocksDB at /tmp/butterfly-shrink-a1b2c3d4/
Estimated space needed: ~15GB
Available space: 42.3GB
Grid resolution: 5m
Highway preset: car
Parallel workers: 8

WARNING: TMPDIR appears to be tmpfs (RAM-backed)
  Current: /tmp (tmpfs filesystem detected)
  Action: export TMPDIR=/mnt/ssd/tmp
  
Verbose mode: RocksDB using compression=zstd, cache=128MB, WAL=limited
```

### 3.2 Node Processing

**Single-threaded flow** (for reference):
```
For each node:
  1. Extract ID and coordinates
  2. Apply latitude-aware grid snapping
  3. Create grid cell key
  4. Check if cell already has representative:
     - Yes: Map to existing representative
     - No: This node becomes representative
  5. Store mapping in RocksDB
  6. Write node to output (snapped coordinates)
```

**Parallel flow**:
```
Reader thread:
  - Decode PBF blobs
  - Tag with sequence number
  - Send to worker pool via channel

Worker threads:
  - Receive batch of nodes
  - Apply grid snapping
  - Batch RocksDB operations (write_batch)
  - Send to writer via channel

Writer thread:
  - Buffer and reorder by sequence
  - Write dense node blocks
  - Maintain output ordering
```

**Batch optimization**:
```rust
// Collect node mappings in batches
let mut batch = WriteBatch::default();
for (original_id, representative_id) in mappings.iter() {
    batch.put(original_id.to_be_bytes(), representative_id.to_be_bytes());
}
db.write(batch)?; // Single write operation
```

**Representative selection**: First node in each cell wins (deterministic by input order)

### 3.3 Way Processing

```
For each way:
  1. Check highway tag against profile
  2. If not included: skip
  3. For each node_ref:
     - Lookup in RocksDB → get representative ID
     - If not found: log error, skip way
  4. Remove consecutive duplicate refs
  5. If <2 unique nodes: skip (too short)
  6. Keep minimal tags (highway, oneway)
  7. Write to output
```

### 3.4 Relation Processing

```
For each relation:
  1. If type=restriction:
     - Extract from/via/to members
     - Remap via node to representative
     - If multi-via: log warning, skip
     - Buffer in memory
  2. Else: skip

After all relations:
  - Write buffered restrictions with remapped IDs
```

**Turn restriction remapping**:
```
Original: from=way/123, via=node/456, to=way/789
After:    from=way/123, via=node/999, to=way/789
          (where 999 is representative of 456)
```

### 3.5 Finalization

1. Flush output buffers
2. Close RocksDB
3. Delete temp directory
4. Print statistics

---

## 4. Usage Examples

### Basic Usage

```bash
# Stream from butterfly-dl
butterfly-dl planet - | butterfly-shrink - planet-routing.pbf

# Process existing file
butterfly-shrink planet.pbf planet-routing.pbf

# Stream to stdout  
butterfly-shrink city.pbf - | osrm-extract -

# Warning for 1m planet processing
butterfly-shrink --grid 1 planet.pbf output.pbf
# WARNING: 1m grid on planet data requires ~600GB disk space
```

### Advanced Options

```bash
# Urban area with 1m precision
butterfly-shrink --grid 1 manhattan.pbf manhattan-routing.pbf

# Rural area with 10m grid
butterfly-shrink --grid 10 alaska.pbf alaska-routing.pbf

# Bicycle routing preset
butterfly-shrink --preset bike netherlands.pbf bike-routing.pbf

# Custom highway configuration
butterfly-shrink --config emergency.yaml city.pbf emergency-routing.pbf

# Machine-readable statistics
butterfly-shrink --stats-format json input.pbf output.pbf 2>stats.json

# Save dropped ways report (includes reason column)
butterfly-shrink --dropped-ways report.csv input.pbf output.pbf
# CSV columns: way_id,missing_nodes,highway_tag,reason

# Save skipped multi-via restrictions
butterfly-shrink --skipped-restrictions restrictions.csv input.pbf output.pbf
# CSV columns: relation_id,restriction_type,via_count,from_way,to_way

# Force Direct I/O (Unix only)
butterfly-shrink --direct-io large.pbf output.pbf

# Custom temp directory
TMPDIR=/fast/nvme butterfly-shrink planet.pbf planet-routing.pbf
```

---

## 5. Performance Characteristics

### Processing Speed (Expected)
- Nodes: 4-6M/second (with parallel processing)
- Ways: 800K-1M/second (with parallel lookups)
- Relations: 50-100K/second
- Planet processing: ~2-3 hours (multi-core system)

### Resource Usage
- Active RAM: <200MB constant
- RocksDB cache: 128MB (configurable)
- Temp disk: ~15GB for planet
- Output size: 2-10% of input

### I/O Patterns
- Input: Sequential read
- RocksDB: Random writes then reads
- Output: Sequential write
- Optimized for SSD

---

## 6. Configuration

### Grid Sizes

| Grid | Use Case | Node Reduction | Precision | Disk Space (Planet) |
|------|----------|----------------|-----------|--------------------|
| 1m   | Dense urban | ~90% | ±0.5m | ~600GB ⚠️ |
| 2m   | Urban | ~95% | ±1m | ~150GB |
| 5m   | General (default) | ~98% | ±2.5m | ~15GB |
| 10m  | Rural | ~99% | ±5m | ~4GB |

### Built-in Presets

**car** (default):
- motorway, trunk, primary, secondary, tertiary
- unclassified, residential, living_street, service
- All link types

**bike**:
- All from car preset
- Plus: cycleway, track, path (where bicycle!=no)

**foot**:
- All from bike preset  
- Plus: footway, pedestrian, steps, path

### Statistics Output

**Human format** (default):
```
butterfly-shrink statistics:
  Input:  8,145,923,421 nodes, 923,654,812 ways
  Output:   142,857,923 nodes,  84,923,142 ways
  Reduction: 98.2% nodes, 90.8% ways
  Grid: 5m (147,234,821 cells)
  Dropped ways: 423 (missing nodes)
  Failed restrictions: 234 (multi-via)
  Time: 4h 23m 17s
  Throughput: 2.34M nodes/sec
```

**JSON format** (`--stats-format json`):
```json
{
  "input_nodes": 8145923421,
  "output_nodes": 142857923,
  "node_reduction_percent": 98.2,
  "input_ways": 923654812,
  "output_ways": 84923142,
  "way_reduction_percent": 90.8,
  "grid_size_m": 5,
  "grid_cells": 147234821,
  "dropped_ways": 423,
  "failed_restrictions": 234,
  "multi_via_restrictions_skipped": 234,
  "duration_seconds": 15797,
  "throughput_nodes_per_sec": 2340000,
  "rocksdb_size_bytes": 15123456789,
  "rocksdb_write_mb_s": 142.3
}
```

---

## 7. Quality Assurance

### Connectivity Verification

1. **Graph connectivity**: Same number of strongly connected components
2. **Edge preservation**: Every routing edge has equivalent in output
3. **Turn restrictions**: 100% of valid restrictions preserved

### Validation Command

```bash
# Compare routing quality
butterfly-shrink --validate original.pbf shrunk.pbf

Output:
Connectivity: ✓ (1 component → 1 component)
Edges preserved: 84,923,142 / 84,923,150 (99.99%)
Turn restrictions: 486,766 / 487,000 (99.95%)
Directional edges: Forward 84.9M, Reverse 84.9M (balanced ✓)
Route deviation (1000 samples): avg 0.12%, max 0.8%
```

### Known Limitations

1. **Multi-via restrictions**: Not supported (affects <0.1% of restrictions)
   - IDs logged to stderr for OSM patching
2. **Complex relations**: Only turn restrictions preserved
3. **Elevation data**: Stripped (not used by most routers)
4. **Extreme latitudes**: Grid cells become very narrow E-W near poles (expected behavior)

---

## 8. Error Handling

### Fatal Errors (exit 1)
- Forward reference detected (corrupted input)
- Disk full in temp directory
- Invalid grid size
- Cannot open input/output

### Warnings (continue processing)
- Way with missing nodes (skip way)
- Multi-via restriction (skip restriction, log ID to stderr and optional CSV)
- Invalid highway tag (skip way)

### Error Messages

All errors include actionable context:
```
ERROR: Forward reference at byte 1234567890
  Way 123456789 references non-existent node 987654321
  This indicates a corrupted PBF file
  Action: Verify input with osmium check-refs
  Alternative: osmconvert --out-statistics input.pbf

ERROR: Disk full in /tmp
  Need ~15GB free space for planet processing (5m grid)
  Action: export TMPDIR=/path/to/larger/filesystem
  Note: 1m grid requires ~600GB

WARNING: TMPDIR appears to be tmpfs (RAM-backed)
  Current: /tmp (tmpfs filesystem detected)
  Action: export TMPDIR=/mnt/ssd/tmp
```

---

## 9. Integration

### With Routing Engines

```bash
# OSRM
butterfly-shrink --preset car planet.pbf - | osrm-extract -

# Valhalla  
butterfly-shrink planet.pbf planet-routing.pbf
valhalla_build_tiles -c valhalla.json planet-routing.pbf

# GraphHopper
butterfly-shrink --preset bike planet.pbf planet-routing.pbf
java -jar graphhopper.jar import planet-routing.pbf
```

### In Pipelines

```bash
# Full pipeline
butterfly-dl planet - | \
butterfly-shrink --grid 5 - - | \
osrm-extract --profile car -

# With compression
butterfly-shrink input.pbf - | gzip > output.pbf.gz

# Parallel processing
butterfly-shrink germany.pbf - | tee >(osrm-extract -) | valhalla_build_tiles -
```

---

**Summary**: butterfly-shrink provides single-pass, memory-efficient PBF optimization for routing through proven techniques: fixed-grid snapping with geographic awareness, RocksDB for reliable indexing, and strict data validation.