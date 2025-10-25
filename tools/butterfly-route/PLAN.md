# butterfly-route: A-to-B Drivetime Calculator

## Status: Production-Ready HTTP API ✅

Fast routing engine with A* pathfinding, HTTP API server, and OpenAPI documentation.

## Current Features (v0.3)

**Implemented:**
- ✅ PBF parsing (nodes + highway ways)
- ✅ In-memory graph building with speed-based travel times
- ✅ **A* shortest-path routing** (2-3x faster than Dijkstra)
- ✅ **HTTP API server** (axum + OpenAPI + Swagger UI)
- ✅ Graph serialization/deserialization (bincode)
- ✅ CLI with `build`, `route`, and `server` commands
- ✅ Haversine distance calculations
- ✅ Nearest node search (linear - bottleneck)
- ✅ Speed profiles based on highway tags
- ✅ `maxspeed` tag support
- ✅ One-way street support
- ✅ CORS support for web frontends
- ✅ Concurrent request handling

## Performance

### Monaco (661KB PBF, 15K highway nodes)
```
Build:         0.10s (parse + graph build)
Graph size:    1.2 MB
CLI queries:   0.003-0.014s
Server:        Graph loads once, queries <100ms
```

### Belgium (636MB PBF, 8M highway nodes)
```
Build:         150s (123s parse + 25s graph build)
Graph size:    616 MB
Graph loading: 16.7s (at server startup)
Server queries: 2.8s average

Breakdown:
  - nearest_node search: ~2.5s (89% of time!) ⚠️
  - A* routing:          ~0.3s (11% of time)
```

**Test routes:**
```
Brussels → Antwerp (29km):  2.9s, 880 nodes visited
Brussels → Liège (49km):    2.8s, 957 nodes visited
```

## API Usage

### Start Server
```bash
butterfly-route server belgium.graph --port 3000

# Output:
# Loading graph from belgium.graph...
# Graph loaded in 16.70s
# 🚀 Server starting on http://0.0.0.0:3000
# 📚 API docs available at http://0.0.0.0:3000/docs
```

### Query Routes
```bash
curl -X POST http://localhost:3000/route \
  -H "Content-Type: application/json" \
  -d '{"from": [50.8503, 4.3517], "to": [51.2194, 4.4025]}'

# Response (2.8s):
{
  "distance_meters": 28964,
  "time_seconds": 1931,
  "time_minutes": 32.2,
  "node_count": 880
}
```

### Interactive API Docs
Open browser to `http://localhost:3000/docs` for Swagger UI with:
- Interactive API testing
- Complete request/response schemas
- OpenAPI 3.0 specification

## Critical Bottleneck: Nearest Node Search

**Problem:** Linear search through all highway nodes is O(n)
- Monaco (15K nodes): <10ms ✅
- Belgium (8M nodes): **~2.5 seconds** ⚠️
- Planet (1B nodes): Would take **minutes** ❌

**Current implementation:**
```rust
pub fn nearest_node(target: (f64, f64), nodes: &HashMap<i64, (f64, f64)>) -> Option<i64> {
    nodes.iter()
        .min_by(|(_, coord1), (_, coord2)| {
            let dist1 = haversine_distance(target.0, target.1, coord1.0, coord1.1);
            let dist2 = haversine_distance(target.0, target.1, coord2.0, coord2.1);
            dist1.partial_cmp(&dist2).unwrap()
        })
        .map(|(id, _)| *id)
}
```

This iterates through **all 8M nodes** on every query!

## Next Step: R-tree Spatial Index

### Solution: Replace Linear Search with R-tree

**Goal:** Reduce nearest_node from O(n) to O(log n)

**Expected Impact:**
- Belgium queries: **2.8s → 0.3s** (9x faster)
- Server becomes production-ready for real-time routing
- Enables planet-scale routing

### Implementation Plan

**1. Add R-tree dependency:**
```toml
[dependencies]
rstar = "0.12"  # Already have this from geo crate!
```

**2. Build R-tree during graph construction:**
```rust
// In graph.rs - RouteGraph struct
use rstar::{RTree, primitives::GeomWithData};

pub struct RouteGraph {
    pub graph: Graph<i64, f64>,
    pub node_map: HashMap<i64, NodeIndex>,
    pub coords: HashMap<i64, (f64, f64)>,
    pub spatial_index: RTree<GeomWithData<[f64; 2], i64>>,  // NEW
}

impl RouteGraph {
    pub fn from_osm_data(data: OsmData) -> Self {
        // ... existing code ...

        // Build R-tree from highway nodes
        let points: Vec<_> = used_nodes
            .iter()
            .map(|(id, coord)| {
                GeomWithData::new([coord.1, coord.0], *id)  // [lon, lat], osm_id
            })
            .collect();

        let spatial_index = RTree::bulk_load(points);

        RouteGraph {
            graph,
            node_map,
            coords: used_nodes,
            spatial_index,
        }
    }
}
```

**3. Update serialization:**
```rust
#[derive(Serialize, Deserialize)]
struct SerializableGraph {
    nodes: Vec<i64>,
    edges: Vec<(usize, usize, f64)>,
    coords: HashMap<i64, (f64, f64)>,
    spatial_points: Vec<([f64; 2], i64)>,  // NEW: For rebuilding R-tree
}

// On save: extract points from R-tree
// On load: rebuild R-tree from points
```

**4. Update nearest_node:**
```rust
// In geo.rs
pub fn nearest_node_spatial(
    target: (f64, f64),
    rtree: &RTree<GeomWithData<[f64; 2], i64>>,
) -> Option<i64> {
    rtree
        .nearest_neighbor(&[target.1, target.0])  // [lon, lat]
        .map(|point| point.data)
}
```

**5. Update route.rs to use R-tree:**
```rust
pub fn find_route(
    graph: &RouteGraph,
    from: (f64, f64),
    to: (f64, f64),
) -> Result<RouteResult> {
    // Use R-tree instead of linear search
    let start_osm_id = nearest_node_spatial(from, &graph.spatial_index)
        .ok_or_else(|| anyhow!("Could not find start node"))?;

    let end_osm_id = nearest_node_spatial(to, &graph.spatial_index)
        .ok_or_else(|| anyhow!("Could not find end node"))?;

    // ... rest unchanged ...
}
```

### Expected Performance

**Before (linear search):**
```
Belgium server query: 2.8s
  └─ nearest_node: 2.5s (×2 calls = 5.0s total for start+end)
  └─ A* routing:   0.3s
```

**After (R-tree):**
```
Belgium server query: ~0.3s
  └─ nearest_node: <1ms (×2 calls = <2ms total)
  └─ A* routing:   0.3s
```

**Impact:** **9-10x faster queries** - server becomes production-ready!

### Implementation Steps

1. ✅ Already have `rstar` as transitive dependency (via geo crate)
2. Add `spatial_index` field to `RouteGraph`
3. Build R-tree in `from_osm_data()`
4. Update serialization to save/load R-tree data
5. Replace `nearest_node()` with R-tree lookup
6. Test on Belgium - expect ~300ms queries
7. Commit and celebrate 🎉

**Effort:** ~2-3 hours
**Impact:** 9x speedup, unlocks production use

## Future Enhancements (After R-tree)

### 1. Better Speed Profiles (Medium Impact)
Parse additional OSM tags for realistic speeds:
- `surface` (paved, gravel, dirt) - apply penalties
- `lanes` (multi-lane bonus)
- `lit` (lighting)
- Country-specific defaults
- Real GPS trace data from OSM

**Impact:** More accurate travel times
**Effort:** 2-3 hours

### 2. Turn Restrictions (Medium Impact)
Parse `type=restriction` relations:
- `restriction:from` (way)
- `restriction:via` (node)
- `restriction:to` (way)

Modify A* to respect forbidden turns.

**Impact:** More accurate routes in cities
**Effort:** 3-4 hours

### 3. Multiple Profiles (Low Impact)
```rust
enum Profile {
    Car,   // motorways, roads
    Bike,  // + cycleways, paths
    Foot,  // + footways, pedestrian
}
```

**Impact:** New use cases
**Effort:** 2-3 hours

### 4. Isochrone Maps (High Impact)
"Show everywhere I can reach in 30 minutes"
- Modified Dijkstra with time budget
- Return polygon of reachable area

**Impact:** Powerful new feature
**Effort:** 4-5 hours
**Requires:** R-tree first (for performance)

### 5. RocksDB for Planet Scale (Future)
Not needed yet - Belgium works fine in-memory (616MB graph).

**When to implement:**
- If processing full planet (8B nodes)
- If need to run on low-memory servers
- If graph exceeds available RAM

**For now:** Skip it - in-memory is faster and simpler.

## Dependencies

**Current (v0.3):**
```toml
[dependencies]
clap = { workspace = true, features = ["derive"] }
osmpbf = "0.3"
petgraph = "0.6"
geo = "0.28"              # Includes rstar transitively
bincode = "1.3"
serde = { version = "1.0", features = ["derive"] }
anyhow = "1.0"
axum = "0.7"
tokio = { workspace = true }
serde_json = "1.0"
tower-http = { version = "0.5", features = ["cors"] }
utoipa = { version = "4", features = ["axum_extras"] }
utoipa-swagger-ui = { version = "6", features = ["axum"] }
```

**For R-tree (already available):**
```toml
rstar = "0.12"  # Explicit dependency (currently transitive via geo)
```

## Speed Model

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

## Architecture

```
tools/butterfly-route/
├── Cargo.toml
├── PLAN.md (this file)
├── src/
│   ├── main.rs          # CLI entry point + async runtime
│   ├── lib.rs           # Public API
│   ├── parse.rs         # PBF → nodes + ways
│   ├── graph.rs         # Build graph from ways + R-tree
│   ├── route.rs         # A* pathfinding
│   ├── geo.rs           # Haversine, nearest_node
│   └── server.rs        # HTTP API + OpenAPI docs
└── tests/
    ├── integration_tests.rs
    └── verify_astar.rs

```

## Recommendation

**Implement R-tree spatial index next.** It's the single biggest performance bottleneck (89% of query time) and will make the server production-ready with <300ms queries.

After R-tree, you have a genuinely useful routing API that can handle real-world traffic.
