use crate::parse::TurnRestriction;
use petgraph::graph::{Graph, NodeIndex};
use rstar::RTree;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// Tile identifier: (grid_x, grid_y)
pub type TileId = (u16, u16);

/// Geographic bounds of a tile
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TileBounds {
    pub min_lat: f64,
    pub min_lon: f64,
    pub max_lat: f64,
    pub max_lon: f64,
}

impl TileBounds {
    pub fn new(min_lat: f64, min_lon: f64, max_lat: f64, max_lon: f64) -> Self {
        Self {
            min_lat,
            min_lon,
            max_lat,
            max_lon,
        }
    }

    pub fn contains(&self, lat: f64, lon: f64) -> bool {
        lat >= self.min_lat
            && lat <= self.max_lat
            && lon >= self.min_lon
            && lon <= self.max_lon
    }
}

/// Point type for spatial index
#[derive(Debug, Clone)]
pub struct SpatialPoint {
    pub osm_id: i64,
    pub position: [f64; 2], // [lon, lat] for geographic coords
}

impl rstar::RTreeObject for SpatialPoint {
    type Envelope = rstar::AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        rstar::AABB::from_point(self.position)
    }
}

impl rstar::PointDistance for SpatialPoint {
    fn distance_2(&self, point: &[f64; 2]) -> f64 {
        let dx = self.position[0] - point[0];
        let dy = self.position[1] - point[1];
        dx * dx + dy * dy
    }
}

/// L0 Tile: Geographic partition for local routing
#[derive(Debug, Serialize, Deserialize)]
pub struct Tile {
    /// Tile identifier (grid_x, grid_y)
    pub id: TileId,

    /// Geographic bounds
    pub bounds: TileBounds,

    /// Road graph (OSM IDs as node weights, distances as edge weights)
    pub graph: Graph<i64, f64>,

    /// Mapping from OSM node ID to petgraph NodeIndex
    pub node_map: HashMap<i64, NodeIndex>,

    /// Node coordinates (OSM ID -> (lat, lon))
    pub coords: HashMap<i64, (f64, f64)>,

    /// Boundary nodes that connect to adjacent tiles
    pub boundary_nodes: HashSet<i64>,

    /// Turn restrictions within this tile
    pub restrictions: Vec<TurnRestriction>,

    /// Spatial index for nearest node queries
    #[serde(skip)]
    pub spatial_index: Option<RTree<SpatialPoint>>,
}

impl Tile {
    /// Create a new empty tile
    pub fn new(id: TileId, bounds: TileBounds) -> Self {
        Self {
            id,
            bounds,
            graph: Graph::new(),
            node_map: HashMap::new(),
            coords: HashMap::new(),
            boundary_nodes: HashSet::new(),
            restrictions: Vec::new(),
            spatial_index: None,
        }
    }

    /// Build spatial index from coords
    pub fn build_spatial_index(&mut self) {
        let points: Vec<SpatialPoint> = self
            .coords
            .iter()
            .map(|(&osm_id, &(lat, lon))| SpatialPoint {
                osm_id,
                position: [lon, lat],
            })
            .collect();

        self.spatial_index = Some(RTree::bulk_load(points));
    }

    /// Find nearest node to given coordinates
    pub fn nearest_node(&self, lat: f64, lon: f64) -> Option<i64> {
        if let Some(ref index) = self.spatial_index {
            index
                .nearest_neighbor(&[lon, lat])
                .map(|point| point.osm_id)
        } else {
            // Fallback: linear search
            self.coords
                .iter()
                .min_by(|(_, (lat1, lon1)), (_, (lat2, lon2))| {
                    let dist1 = (lat - lat1).powi(2) + (lon - lon1).powi(2);
                    let dist2 = (lat - lat2).powi(2) + (lon - lon2).powi(2);
                    dist1.partial_cmp(&dist2).unwrap()
                })
                .map(|(&osm_id, _)| osm_id)
        }
    }

    /// Check if a node is on the tile boundary
    pub fn is_boundary_node(&self, osm_id: i64) -> bool {
        self.boundary_nodes.contains(&osm_id)
    }

    /// Get number of nodes in tile
    pub fn node_count(&self) -> usize {
        self.graph.node_count()
    }

    /// Get number of edges in tile
    pub fn edge_count(&self) -> usize {
        self.graph.edge_count()
    }
}

/// Geographic partitioning configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TileGrid {
    /// Minimum latitude of region
    pub min_lat: f64,

    /// Minimum longitude of region
    pub min_lon: f64,

    /// Maximum latitude of region
    pub max_lat: f64,

    /// Maximum longitude of region
    pub max_lon: f64,

    /// Tile size in degrees (latitude)
    pub tile_size_lat: f64,

    /// Tile size in degrees (longitude)
    pub tile_size_lon: f64,

    /// Grid dimensions
    pub grid_width: u16,
    pub grid_height: u16,
}

impl TileGrid {
    /// Create grid for Belgium (10km × 10km tiles)
    pub fn belgium_10km() -> Self {
        // Belgium bounds
        let min_lat = 49.5;
        let max_lat = 51.5;
        let min_lon = 2.5;
        let max_lon = 6.4;

        // Approximate degrees per 10km
        let tile_size_lat = 0.09; // ~10km at Belgium latitude
        let tile_size_lon = 0.13; // ~10km at Belgium latitude

        let grid_width = ((max_lon - min_lon) / tile_size_lon).ceil() as u16;
        let grid_height = ((max_lat - min_lat) / tile_size_lat).ceil() as u16;

        Self {
            min_lat,
            min_lon,
            max_lat,
            max_lon,
            tile_size_lat,
            tile_size_lon,
            grid_width,
            grid_height,
        }
    }

    /// Convert coordinates to tile ID
    pub fn coord_to_tile(&self, lat: f64, lon: f64) -> Option<TileId> {
        if lat < self.min_lat
            || lat > self.max_lat
            || lon < self.min_lon
            || lon > self.max_lon
        {
            return None;
        }

        let grid_x = ((lon - self.min_lon) / self.tile_size_lon) as u16;
        let grid_y = ((lat - self.min_lat) / self.tile_size_lat) as u16;

        // Clamp to grid bounds
        let grid_x = grid_x.min(self.grid_width - 1);
        let grid_y = grid_y.min(self.grid_height - 1);

        Some((grid_x, grid_y))
    }

    /// Get bounds for a specific tile
    pub fn tile_bounds(&self, tile_id: TileId) -> TileBounds {
        let (grid_x, grid_y) = tile_id;

        let min_lon = self.min_lon + (grid_x as f64) * self.tile_size_lon;
        let min_lat = self.min_lat + (grid_y as f64) * self.tile_size_lat;
        let max_lon = (min_lon + self.tile_size_lon).min(self.max_lon);
        let max_lat = (min_lat + self.tile_size_lat).min(self.max_lat);

        TileBounds::new(min_lat, min_lon, max_lat, max_lon)
    }

    /// Check if two tiles are adjacent (including diagonals)
    pub fn tiles_adjacent(&self, tile1: TileId, tile2: TileId) -> bool {
        let (x1, y1) = tile1;
        let (x2, y2) = tile2;

        let dx = (x1 as i32 - x2 as i32).abs();
        let dy = (y1 as i32 - y2 as i32).abs();

        dx <= 1 && dy <= 1 && (dx + dy) > 0
    }

    /// Get all adjacent tiles (up to 8 neighbors)
    pub fn adjacent_tiles(&self, tile_id: TileId) -> Vec<TileId> {
        let (x, y) = tile_id;
        let mut neighbors = Vec::new();

        for dx in -1..=1 {
            for dy in -1..=1 {
                if dx == 0 && dy == 0 {
                    continue;
                }

                let nx = x as i32 + dx;
                let ny = y as i32 + dy;

                if nx >= 0
                    && nx < self.grid_width as i32
                    && ny >= 0
                    && ny < self.grid_height as i32
                {
                    neighbors.push((nx as u16, ny as u16));
                }
            }
        }

        neighbors
    }

    /// Total number of tiles in grid
    pub fn tile_count(&self) -> usize {
        (self.grid_width as usize) * (self.grid_height as usize)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_belgium_grid() {
        let grid = TileGrid::belgium_10km();

        // Brussels: 50.8503°N, 4.3517°E
        let brussels_tile = grid.coord_to_tile(50.8503, 4.3517).unwrap();
        assert!(brussels_tile.0 < grid.grid_width);
        assert!(brussels_tile.1 < grid.grid_height);

        // Antwerp: 51.2194°N, 4.4025°E
        let antwerp_tile = grid.coord_to_tile(51.2194, 4.4025).unwrap();
        assert!(antwerp_tile.0 < grid.grid_width);
        assert!(antwerp_tile.1 < grid.grid_height);

        // Should be in different tiles
        assert_ne!(brussels_tile, antwerp_tile);
    }

    #[test]
    fn test_tile_bounds() {
        let grid = TileGrid::belgium_10km();
        let tile_id = (10, 10);
        let bounds = grid.tile_bounds(tile_id);

        assert!(bounds.min_lat < bounds.max_lat);
        assert!(bounds.min_lon < bounds.max_lon);
        assert!(bounds.min_lat >= grid.min_lat);
        assert!(bounds.max_lat <= grid.max_lat);
    }

    #[test]
    fn test_adjacent_tiles() {
        let grid = TileGrid::belgium_10km();
        let tile = (5, 5);
        let neighbors = grid.adjacent_tiles(tile);

        // Should have 8 neighbors (not on edge)
        assert_eq!(neighbors.len(), 8);

        // All should be adjacent
        for neighbor in neighbors {
            assert!(grid.tiles_adjacent(tile, neighbor));
        }
    }

    #[test]
    fn test_tile_contains() {
        let bounds = TileBounds::new(50.0, 4.0, 51.0, 5.0);

        assert!(bounds.contains(50.5, 4.5));
        assert!(!bounds.contains(49.5, 4.5));
        assert!(!bounds.contains(50.5, 3.5));
    }
}
