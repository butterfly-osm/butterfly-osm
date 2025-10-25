# butterfly-route MVP: A-to-B Drivetime Calculator

## Goal
Calculate driving time between two coordinates using OSM PBF data. Simplest possible implementation.

## Scope: MVP Only

**What it does:**
- Reads a PBF file (monaco.pbf to start)
- Extracts driveable roads (highway tags)
- Builds in-memory graph
- Calculates route from A to B using Dijkstra
- Returns travel time in minutes

**What it does NOT do (future):**
- Grid snapping
- Turn restrictions
- Streaming/stdin/stdout
- RocksDB indexing
- Multiple transport modes
- Large files (planet.pbf)

## Dependencies

```toml
[dependencies]
osmpbf = "0.4"        # PBF parsing
petgraph = "0.6"      # Graph + Dijkstra
geo = "0.28"          # Haversine distance
clap = { version = "4.5", features = ["derive"] }
```

## Data Structures

### 1. Parse PBF
```rust
// Node: ID -> (lat, lon)
HashMap<i64, (f64, f64)>

// Way: List of node IDs + highway type
struct Way {
    nodes: Vec<i64>,
    highway: String,
    maxspeed: Option<u32>,  // km/h
    oneway: bool,
}
```

### 2. Build Graph
```rust
// Graph node = OSM node ID
// Graph edge = road segment with travel time (seconds)
petgraph::Graph<i64, f64>
```

### 3. Speed Model (Hardcoded)
```rust
fn get_speed(highway_type: &str) -> f64 {
    match highway_type {
        "motorway" => 120.0,
        "trunk" => 100.0,
        "primary" => 80.0,
        "secondary" => 60.0,
        "residential" => 30.0,
        _ => 50.0,
    }
}
```

## Algorithm

### Step 1: Parse PBF
```
For each element:
  - If Node: store ID -> (lat, lon)
  - If Way with highway tag:
    - Extract nodes, highway type, maxspeed, oneway
    - Store in ways list
```

### Step 2: Build Graph
```
For each way:
  For each pair of consecutive nodes (A, B):
    - Get coordinates from node map
    - Calculate distance using Haversine
    - Speed = maxspeed OR default from highway type
    - Time = distance / speed
    - Add edge A->B with weight=time
    - If NOT oneway: add edge B->A
```

### Step 3: Find Nearest Nodes
```
fn nearest_node(target: (lat, lon)) -> node_id:
  - Linear search through all nodes
  - Return closest by Haversine distance
```

### Step 4: Route
```
start_node = nearest_node(start_coord)
end_node = nearest_node(end_coord)
path = dijkstra(graph, start_node, end_node)
total_time = sum of edge weights
```

## CLI Interface

```bash
# Build graph from PBF
butterfly-route build monaco.pbf monaco.graph

# Query route
butterfly-route route monaco.graph \
  --from "43.7384,7.4246" \
  --to "43.7403,7.4268"

# Output:
# Distance: 342 meters
# Time: 1.2 minutes
# Path: node/123 -> node/456 -> node/789
```

## Implementation Order

1. Parse PBF (nodes only) - verify we can read coordinates
2. Parse ways with highway tags
3. Build in-memory graph with hardcoded speeds
4. Implement nearest_node search
5. Run Dijkstra between two hardcoded nodes
6. Add CLI for coordinates input
7. Serialize graph to disk (bincode or similar)

## Memory Constraints

**Monaco**: ~500KB PBF
- ~3,000 nodes
- ~500 ways
- Graph fits easily in RAM

**Belgium**: ~635MB PBF
- ~60M nodes
- ~8M ways
- **Will not fit in RAM** - need RocksDB later

**For MVP: Monaco only**

## Success Criteria

```bash
$ butterfly-route build data/monaco-latest.osm.pbf monaco.graph
Parsed 3,247 nodes
Parsed 521 highway ways
Built graph: 3,247 nodes, 1,842 edges
Saved to monaco.graph

$ butterfly-route route monaco.graph --from "43.7384,7.4246" --to "43.7403,7.4268"
Found route in 0.003s
Distance: 342m
Time: 1.2 minutes
Nodes: 8
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

## Next Steps After MVP

1. RocksDB for node storage (handle Belgium/planet)
2. Grid snapping for deduplication
3. Turn restrictions
4. Multiple speed profiles (bike, foot)
5. Streaming pipeline with butterfly-dl
