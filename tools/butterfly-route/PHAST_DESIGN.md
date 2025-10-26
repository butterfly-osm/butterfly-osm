# PHAST Architecture Design

**P**artitioned **H**ighway-centric **A***  **S**earch **T**echnique

## Overview

Two-level hierarchical routing system optimized for fast long-distance queries on planet-scale road networks.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Query Input                          │
│              (Brussels → Antwerp)                       │
└──────────────────────┬──────────────────────────────────┘
                       │
                ┌──────▼───────┐
                │  Classify    │
                │    Query     │
                └──────┬───────┘
                       │
        ┌──────────────┼──────────────┐
        │              │              │
   ┌────▼────┐   ┌────▼────┐   ┌────▼─────┐
   │  Local  │   │Regional │   │Long Dist │
   │ (1 tile)│   │(2 tiles)│   │(L0→L1→L0)│
   └────┬────┘   └────┬────┘   └────┬─────┘
        │              │              │
   ┌────▼────┐   ┌────▼────┐   ┌────▼─────┐
   │  A* on  │   │ A* cross│   │ 1. L0 A* │
   │  L0     │   │ boundary│   │    to    │
   └────┬────┘   └────┬────┘   │  highway │
        │              │        │ 2. L1 CH │
        │              │        │ 3. L0 A* │
        │              │        │   from   │
        │              │        │  highway │
        │              │        └────┬─────┘
        └──────────────┴─────────────┘
                       │
                  ┌────▼────┐
                  │  Result │
                  └─────────┘
```

## L0: Local Tile Layer

**Purpose:** Fast routing within geographic regions

**Structure:**
- Geographic partitioning (e.g., 10km × 10km tiles)
- ~50-100 tiles for Belgium
- ~50k-150k nodes per tile
- Each tile is independent, serialized separately

**Tile Contents:**
```rust
struct Tile {
    id: TileId,                    // (grid_x, grid_y)
    bounds: TileBounds,            // (min_lat, min_lon, max_lat, max_lon)
    graph: Graph<i64, f64>,        // Petgraph
    node_map: HashMap<i64, NodeIndex>,
    coords: HashMap<i64, (f64, f64)>,
    boundary_nodes: HashSet<i64>,  // Connect to other tiles
    restrictions: Vec<TurnRestriction>,
    spatial_index: RTree<...>,     // For nearest node
}
```

**Boundary Nodes:**
- Nodes that have edges crossing tile boundaries
- Marked during partitioning
- Included in L1 highway network

**Routing:**
- Within tile: Standard A* (50-200ms)
- Cross-tile: Load adjacent tiles, A* across boundary

## L1: Highway Network Layer

**Purpose:** Fast long-distance routing via highways

**Structure:**
- Filtered to motorway, trunk, primary roads
- ~100-200k nodes (vs 8M full graph)
- Includes ALL L0 boundary nodes
- Preprocessed with Contraction Hierarchies

**Highway Types (OSM tags):**
```rust
const HIGHWAY_TYPES: &[&str] = &[
    "motorway",
    "motorway_link",
    "trunk",
    "trunk_link",
    "primary",
    "primary_link",
];
```

**L1 Contents:**
```rust
struct HighwayNetwork {
    ch_graph: CHGraph,               // From ch.rs
    boundary_nodes: HashMap<i64, TileId>,  // OSM ID → which tile
    highway_entries: RTree<...>,     // Spatial index of entry points
}
```

**Why CH Works on L1:**
- Highway interchanges have low degree (typically 2-6 connections)
- Small graph size (~200k nodes)
- Preprocessing completes in 1-2 minutes
- Query time: ~1-5ms

## Query Classification

```rust
enum QueryType {
    Local {
        tile: TileId,
    },
    Regional {
        tiles: Vec<TileId>,
    },
    LongDistance {
        start_tile: TileId,
        end_tile: TileId,
    },
}

fn classify_query(start: (f64, f64), end: (f64, f64)) -> QueryType {
    let start_tile = coord_to_tile(start);
    let end_tile = coord_to_tile(end);

    if start_tile == end_tile {
        QueryType::Local { tile: start_tile }
    } else if tiles_adjacent(start_tile, end_tile) {
        QueryType::Regional { tiles: vec![start_tile, end_tile] }
    } else {
        QueryType::LongDistance { start_tile, end_tile }
    }
}
```

## Long-Distance Routing Pipeline

**Step 1: L0 → Highway Entry**
```
Start (Brussels center)
    ↓ A* on L0 tile
Highway Entry (E40 motorway)
```

**Step 2: L1 CH Query**
```
Entry Point (Brussels E40)
    ↓ CH query on L1 (~2ms)
Exit Point (Antwerp E19)
```

**Step 3: Highway Exit → L0**
```
Highway Exit (Antwerp E19)
    ↓ A* on L0 tile
End (Antwerp center)
```

## Tile Grid System

**Belgium bounds:**
- Lat: 49.5° to 51.5° (2° range)
- Lon: 2.5° to 6.4° (3.9° range)

**10km × 10km tiles:**
- ~0.09° per tile (lat)
- ~0.13° per tile (lon)
- Grid dimensions: ~22 × 30 = 660 tiles
- Most tiles empty (water, borders)
- ~100 tiles with road nodes

**Tile ID:**
```rust
type TileId = (u16, u16);  // (grid_x, grid_y)

fn coord_to_tile(coord: (f64, f64)) -> TileId {
    let (lat, lon) = coord;
    let grid_x = ((lon - MIN_LON) / TILE_SIZE_LON) as u16;
    let grid_y = ((lat - MIN_LAT) / TILE_SIZE_LAT) as u16;
    (grid_x, grid_y)
}
```

## File Structure

```
belgium-phast/
├── tiles/
│   ├── tile_00_00.bin
│   ├── tile_00_01.bin
│   ├── ...
│   └── tile_29_21.bin
├── highway_l1.bin           # L1 highway network with CH
└── metadata.json            # Bounds, tile size, stats
```

## Performance Expectations

**Preprocessing (Belgium):**
- L0 tiling: 5-7 minutes (parallelizable)
- L1 extraction: 1-2 minutes
- L1 CH: 1-2 minutes
- **Total: ~10 minutes**

**Query Performance:**
| Query Type | Distance | Expected Time | Algorithm |
|------------|----------|---------------|-----------|
| Local | <5km | 50-100ms | A* on L0 |
| Regional | 5-20km | 100-300ms | A* cross-tile |
| Long-distance | >20km | 5-10ms | L0→L1 CH→L0 |

**Memory:**
- L1 + CH: ~500MB (loaded always)
- L0 tiles: ~10MB each (load on-demand)
- Active set: ~3-5 tiles cached

## Implementation Phases

### Phase 1: L0 Tiling (1-2 weeks)
- [x] Define Tile struct
- [ ] Geographic partitioning
- [ ] Tile extraction
- [ ] Boundary node identification
- [ ] Serialization
- [ ] CLI: `build-tiles`

### Phase 2: L1 Highway (1 week)
- [ ] Highway extraction
- [ ] Include boundary nodes
- [ ] Run CH (reuse ch.rs!)
- [ ] Serialization
- [ ] CLI: `build-highway`

### Phase 3: Multi-Level Query (1-2 weeks)
- [ ] Query classification
- [ ] L0 local routing
- [ ] L0 cross-tile routing
- [ ] L0→L1→L0 pipeline
- [ ] Path reconstruction
- [ ] CLI: `route-phast`

### Phase 4: Optimization (1-2 weeks)
- [ ] Tile size tuning
- [ ] Tile caching
- [ ] Boundary optimization
- [ ] Benchmarking
- [ ] Documentation

## Success Criteria

**Correctness:**
- ✅ Routes within 5% of A* baseline
- ✅ Respects turn restrictions
- ✅ No crashes or errors

**Performance:**
- ✅ Long-distance queries <10ms (100x faster than A*)
- ✅ Preprocessing <15 minutes
- ✅ Memory <2GB

**Scalability:**
- ✅ Can extend to Europe-wide
- ✅ Parallel tile building
- ✅ On-demand tile loading
