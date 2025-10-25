# butterfly-route: A-to-B Drivetime Calculator

## Status: MVP Complete ✅

Successfully calculates driving time between two coordinates using OSM PBF data.

## What Works (v0.1 MVP)

**Implemented:**
- ✅ PBF parsing (nodes + highway ways)
- ✅ In-memory graph building with speed-based travel times
- ✅ Dijkstra shortest-path routing
- ✅ Graph serialization/deserialization (bincode)
- ✅ CLI with `build` and `route` commands
- ✅ Haversine distance calculations
- ✅ Nearest node search (linear)
- ✅ Speed profiles based on highway tags
- ✅ `maxspeed` tag support
- ✅ One-way street support

**Performance (Monaco - 661KB PBF):**
```
Parse time:    0.07s
Graph build:   0.03s
Graph size:    1.2 MB (14,721 nodes, 29,094 edges)
Route queries: 0.003-0.014s
```

**Tested Routes:**
```bash
# 373m route
$ butterfly-route route monaco.graph --from "43.7384,7.4246" --to "43.7403,7.4268"
Route found in 0.003s
Distance: 373m
Time: 0.4 minutes

# 1989m route
$ butterfly-route route monaco.graph --from "43.73,7.42" --to "43.74,7.43"
Route found in 0.014s
Distance: 1989m
Time: 2.2 minutes
```

## Current Limitations

**Memory-bound:**
- Stores all nodes in HashMap (works for Monaco)
- Belgium (60M nodes) will exhaust RAM
- Planet (8B nodes) impossible

**Features not implemented:**
- Turn restrictions
- Multiple transport profiles (bike, foot)
- Grid snapping/deduplication
- Streaming input/output
- Spatial indexing (nearest node is O(n) linear search)

## Next Step: Scale to Belgium with RocksDB

### Problem
Belgium PBF (~635MB):
- 60M total nodes → ~8M highway nodes
- HashMap memory: ~8M × 24 bytes = ~192MB minimum
- Plus graph structure: ~500MB total
- **Will work but inefficient** - should use RocksDB for scalability

### Solution: RocksDB Node Storage

**Goal:** Handle Belgium (and eventually planet) without loading all nodes into RAM.

**Changes needed:**
1. Replace `HashMap<i64, (f64, f64)>` with RocksDB
2. Keep graph in memory (only highway nodes)
3. Store temp RocksDB in `$TMPDIR/butterfly-route-{uuid}/`

**New dependencies:**
```toml
rocksdb = "0.22"
uuid = "1.0"
```

**Architecture:**
```rust
// During parsing
let db = RocksDB::open(temp_dir)?;
for node in pbf_nodes {
    db.put(node.id, (lat, lon))?;
}

// During graph building
for way in ways {
    for node_id in way.nodes {
        let coord = db.get(node_id)?;  // Fetch from disk
        // Build graph...
    }
}

// After graph building
db.close();
std::fs::remove_dir_all(temp_dir)?;
```

**Implementation steps:**
1. Add RocksDB dependency
2. Create temp directory management
3. Replace HashMap in `parse.rs`
4. Update graph builder to query RocksDB
5. Add cleanup on success/failure
6. Test with Belgium dataset

**Expected performance (Belgium):**
- Parse: ~30s (RocksDB writes)
- Graph build: ~45s (RocksDB reads)
- Graph size: ~80MB serialized
- Route queries: <0.1s

## Future Enhancements (After RocksDB)

### 1. Spatial Indexing (High Impact)
**Problem:** Linear nearest_node search is O(n)
**Solution:** R-tree spatial index

```toml
rstar = "0.12"
```

Makes nearest_node O(log n) - crucial for Belgium/planet.

### 2. Turn Restrictions (Medium Impact)
Parse `type=restriction` relations:
- `restriction:from` (way)
- `restriction:via` (node)
- `restriction:to` (way)

Modify Dijkstra to respect forbidden turns.

### 3. Multiple Profiles (Low Impact)
```rust
enum Profile {
    Car,   // motorways, roads
    Bike,  // + cycleways, paths
    Foot,  // + footways, pedestrian
}
```

### 4. Grid Snapping (Memory Optimization)
Snap nodes to 5m grid → 98% node reduction
- Enables planet-scale routing
- Requires coordination system for grid cells

### 5. Streaming Pipeline
```bash
butterfly-dl belgium - | butterfly-route build - belgium.graph
```

## Memory Budget (Target)

| Dataset | Nodes (total) | Highway Nodes | RAM (HashMap) | RAM (RocksDB) | Temp Disk |
|---------|---------------|---------------|---------------|---------------|-----------|
| Monaco  | 41K          | 15K           | 1 MB          | 500 KB        | 2 MB      |
| Belgium | 60M          | 8M            | 200 MB        | 50 MB         | 1.5 GB    |
| Planet  | 8B           | 1B            | **120 GB**    | **500 MB**    | 20 GB     |

RocksDB makes planet-scale routing feasible.

## Dependencies

**Current (v0.1):**
```toml
[dependencies]
clap = { workspace = true, features = ["derive"] }
osmpbf = "0.3"          # PBF parsing
petgraph = "0.6"        # Graph + Dijkstra
geo = "0.28"            # Haversine distance
bincode = "1.3"         # Graph serialization
serde = { version = "1.0", features = ["derive"] }
anyhow = "1.0"          # Error handling
```

**Planned (v0.2 - RocksDB):**
```toml
rocksdb = "0.22"        # Persistent node storage
uuid = "1.0"            # Temp directory naming
```

**Future:**
```toml
rstar = "0.12"          # Spatial indexing
```

## File Structure

```
tools/butterfly-route/
├── Cargo.toml
├── PLAN.md (this file)
└── src/
    ├── main.rs          # CLI entry point
    ├── lib.rs           # Public API
    ├── parse.rs         # PBF -> nodes + ways
    ├── graph.rs         # Build graph from ways
    ├── route.rs         # Dijkstra wrapper
    └── geo.rs           # Haversine, nearest_node
```

## Speed Model (Current)

```rust
fn get_speed(highway_type: &str, maxspeed: Option<u32>) -> f64 {
    if let Some(speed) = maxspeed {
        return speed as f64;  // Use OSM maxspeed tag
    }

    match highway_type {
        "motorway" => 120.0,      // km/h
        "trunk" => 100.0,
        "primary" => 80.0,
        "secondary" => 60.0,
        "tertiary" => 50.0,
        "residential" => 30.0,
        "service" => 20.0,
        _ => 50.0,
    }
}
```

## Recommendation: Next Step

**Implement RocksDB node storage** to enable Belgium and planet-scale routing. This is the critical blocker for real-world use. Without it, we're limited to city-sized datasets.

**Why RocksDB first:**
1. Unblocks larger datasets (Belgium → planet)
2. Foundation for all future features
3. Relatively straightforward implementation
4. Immediate 10x scale improvement

**Timeline estimate:** 2-3 hours for RocksDB integration + Belgium testing
