use super::highway::HighwayNetwork;
use super::tile::{Tile, TileGrid, TileId};
use crate::route::astar_tile_route;
use petgraph::graph::NodeIndex;
use std::collections::HashMap;

/// Query type classification
#[derive(Debug, Clone, Copy)]
pub enum QueryType {
    /// Local query: both nodes in same tile
    Local { tile: TileId },
    /// Regional query: nodes in adjacent tiles
    Regional {
        start_tile: TileId,
        end_tile: TileId,
    },
    /// Long-distance query: use L0 → L1 → L0 pipeline
    LongDistance {
        start_tile: TileId,
        end_tile: TileId,
    },
}

/// Query classification based on start/end coordinates
pub fn classify_query(
    grid: &TileGrid,
    start: (f64, f64),
    end: (f64, f64),
) -> Option<QueryType> {
    let (start_lat, start_lon) = start;
    let (end_lat, end_lon) = end;

    let start_tile = grid.coord_to_tile(start_lat, start_lon)?;
    let end_tile = grid.coord_to_tile(end_lat, end_lon)?;

    if start_tile == end_tile {
        Some(QueryType::Local { tile: start_tile })
    } else if grid.tiles_adjacent(start_tile, end_tile) {
        Some(QueryType::Regional {
            start_tile,
            end_tile,
        })
    } else {
        Some(QueryType::LongDistance {
            start_tile,
            end_tile,
        })
    }
}

/// PHAST query result
#[derive(Debug)]
pub struct PhastResult {
    pub distance: f64,
    pub time: f64,
    pub path: Vec<i64>, // OSM node IDs
    pub query_type: QueryType,
}

/// PHAST query engine
pub struct PhastEngine {
    grid: TileGrid,
    tiles: HashMap<TileId, Tile>,
    highway_network: HighwayNetwork,
}

impl PhastEngine {
    pub fn new(grid: TileGrid, tiles: Vec<Tile>, highway_network: HighwayNetwork) -> Self {
        let tiles_map = tiles.into_iter().map(|t| (t.id, t)).collect();

        Self {
            grid,
            tiles: tiles_map,
            highway_network,
        }
    }

    /// Route between two coordinates using PHAST
    pub fn route(&self, start: (f64, f64), end: (f64, f64)) -> Result<PhastResult, String> {
        // Classify query
        let query_type = classify_query(&self.grid, start, end)
            .ok_or("Coordinates outside grid bounds")?;

        match query_type {
            QueryType::Local { tile } => self.route_local(tile, start, end, query_type),
            QueryType::Regional {
                start_tile,
                end_tile,
            } => self.route_regional(start_tile, end_tile, start, end, query_type),
            QueryType::LongDistance {
                start_tile,
                end_tile,
            } => self.route_long_distance(start_tile, end_tile, start, end, query_type),
        }
    }

    /// Local query: A* within single tile
    fn route_local(
        &self,
        tile_id: TileId,
        start: (f64, f64),
        end: (f64, f64),
        query_type: QueryType,
    ) -> Result<PhastResult, String> {
        let tile = self
            .tiles
            .get(&tile_id)
            .ok_or("Tile not found in cache")?;

        // Find nearest nodes in tile
        let start_osm = tile
            .nearest_node(start.0, start.1)
            .ok_or("No start node found in tile")?;
        let end_osm = tile
            .nearest_node(end.0, end.1)
            .ok_or("No end node found in tile")?;

        // Get NodeIndex
        let start_idx = *tile
            .node_map
            .get(&start_osm)
            .ok_or("Start node not in tile graph")?;
        let end_idx = *tile
            .node_map
            .get(&end_osm)
            .ok_or("End node not in tile graph")?;

        // Run A* on tile graph
        let (distance, time, path) =
            astar_tile_route(&tile.graph, &tile.coords, start_idx, end_idx, &tile.restrictions)?;

        Ok(PhastResult {
            distance,
            time,
            path,
            query_type,
        })
    }

    /// Regional query: A* across adjacent tiles
    fn route_regional(
        &self,
        start_tile_id: TileId,
        end_tile_id: TileId,
        start: (f64, f64),
        end: (f64, f64),
        query_type: QueryType,
    ) -> Result<PhastResult, String> {
        // Load both tiles
        let start_tile = self
            .tiles
            .get(&start_tile_id)
            .ok_or("Start tile not found")?;
        let end_tile = self.tiles.get(&end_tile_id).ok_or("End tile not found")?;

        // Merge tiles into a temporary combined graph
        // TODO: Implement graph merging and cross-tile A*
        // For now, fall back to long-distance routing

        self.route_long_distance(start_tile_id, end_tile_id, start, end, query_type)
    }

    /// Long-distance query: L0 → L1 CH → L0 pipeline
    fn route_long_distance(
        &self,
        start_tile_id: TileId,
        end_tile_id: TileId,
        start: (f64, f64),
        end: (f64, f64),
        query_type: QueryType,
    ) -> Result<PhastResult, String> {
        // Step 1: L0 A* from start to nearest highway entry point
        let start_tile = self
            .tiles
            .get(&start_tile_id)
            .ok_or("Start tile not found")?;

        let start_osm = start_tile
            .nearest_node(start.0, start.1)
            .ok_or("No start node found")?;

        // Find nearest highway entry point
        let highway_entry_osm = self
            .highway_network
            .nearest_entry_point(start.0, start.1)
            .ok_or("No highway entry point found near start")?;

        // Route from start to highway entry (if not already on highway)
        let (l0_start_distance, l0_start_time, l0_start_path) = if start_osm != highway_entry_osm {
            let start_idx = *start_tile
                .node_map
                .get(&start_osm)
                .ok_or("Start node not in tile")?;

            // Check if highway entry is in this tile
            if let Some(&entry_idx) = start_tile.node_map.get(&highway_entry_osm) {
                astar_tile_route(&start_tile.graph, &start_tile.coords, start_idx, entry_idx, &start_tile.restrictions)?
            } else {
                // Highway entry in different tile - fall back to full A*
                return Err("Highway entry in different tile - not yet implemented".to_string());
            }
        } else {
            (0.0, 0.0, vec![start_osm])
        };

        // Step 2: L1 CH query from highway entry to highway exit
        let highway_exit_osm = self
            .highway_network
            .nearest_entry_point(end.0, end.1)
            .ok_or("No highway exit point found near end")?;

        // Get NodeIndex in highway network
        let highway_entry_idx = *self
            .highway_network
            .ch_graph
            .node_map
            .get(&highway_entry_osm)
            .ok_or("Highway entry not in CH graph")?;
        let highway_exit_idx = *self
            .highway_network
            .ch_graph
            .node_map
            .get(&highway_exit_osm)
            .ok_or("Highway exit not in CH graph")?;

        // CH query
        let (l1_distance, l1_path_indices) = self
            .highway_network
            .ch_graph
            .query(highway_entry_idx, highway_exit_idx)?;

        // Convert to OSM IDs
        let l1_path: Vec<i64> = l1_path_indices
            .iter()
            .filter_map(|&idx| {
                self.highway_network
                    .ch_graph
                    .graph
                    .node_weight(idx)
                    .copied()
            })
            .collect();

        // Step 3: L0 A* from highway exit to end
        let end_tile = self.tiles.get(&end_tile_id).ok_or("End tile not found")?;

        let end_osm = end_tile
            .nearest_node(end.0, end.1)
            .ok_or("No end node found")?;

        let (l0_end_distance, l0_end_time, l0_end_path) = if highway_exit_osm != end_osm {
            if let Some(&exit_idx) = end_tile.node_map.get(&highway_exit_osm) {
                let end_idx = *end_tile.node_map.get(&end_osm).ok_or("End node not in tile")?;
                astar_tile_route(&end_tile.graph, &end_tile.coords, exit_idx, end_idx, &end_tile.restrictions)?
            } else {
                return Err("Highway exit in different tile - not yet implemented".to_string());
            }
        } else {
            (0.0, 0.0, vec![end_osm])
        };

        // Combine paths
        let mut full_path = Vec::new();
        full_path.extend(l0_start_path.iter().take(l0_start_path.len().saturating_sub(1)));
        full_path.extend(&l1_path);
        full_path.extend(l0_end_path.iter().skip(1));

        let total_distance = l0_start_distance + l1_distance + l0_end_distance;
        let total_time = l0_start_time + (l1_distance / 33.33) + l0_end_time; // Estimate L1 time

        Ok(PhastResult {
            distance: total_distance,
            time: total_time,
            path: full_path,
            query_type,
        })
    }
}
