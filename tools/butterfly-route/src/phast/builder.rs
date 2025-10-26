use super::tile::{Tile, TileBounds, TileGrid, TileId};
use crate::graph::RouteGraph;
use crate::parse::TurnRestriction;
use petgraph::graph::NodeIndex;
use std::collections::{HashMap, HashSet};
use std::path::Path;

/// Builder for L0 tile layer
pub struct TileBuilder {
    grid: TileGrid,
}

impl TileBuilder {
    /// Create a new tile builder with the specified grid
    pub fn new(grid: TileGrid) -> Self {
        Self { grid }
    }

    /// Extract all tiles from a full road graph
    pub fn build_tiles(&self, graph: &RouteGraph) -> Result<Vec<Tile>, String> {
        println!("Building tiles from graph with {} nodes, {} edges",
            graph.graph.node_count(),
            graph.graph.edge_count()
        );
        println!("Grid: {}x{} tiles ({} total)",
            self.grid.grid_width,
            self.grid.grid_height,
            self.grid.tile_count()
        );

        // Assign each node to its tile
        let mut node_to_tile: HashMap<i64, TileId> = HashMap::new();
        for (&osm_id, &(lat, lon)) in &graph.coords {
            if let Some(tile_id) = self.grid.coord_to_tile(lat, lon) {
                node_to_tile.insert(osm_id, tile_id);
            }
        }

        println!("Assigned {} nodes to tiles", node_to_tile.len());

        // Create empty tiles
        let mut tiles: HashMap<TileId, Tile> = HashMap::new();
        for y in 0..self.grid.grid_height {
            for x in 0..self.grid.grid_width {
                let tile_id = (x, y);
                let bounds = self.grid.tile_bounds(tile_id);
                tiles.insert(tile_id, Tile::new(tile_id, bounds));
            }
        }

        // Identify boundary nodes (nodes with edges crossing tile boundaries)
        let mut boundary_nodes: HashSet<i64> = HashSet::new();

        for edge in graph.graph.edge_references() {
            let source_idx = edge.source();
            let target_idx = edge.target();

            if let (Some(&source_osm), Some(&target_osm)) = (
                graph.graph.node_weight(source_idx),
                graph.graph.node_weight(target_idx),
            ) {
                if let (Some(&source_tile), Some(&target_tile)) = (
                    node_to_tile.get(&source_osm),
                    node_to_tile.get(&target_osm),
                ) {
                    // If edge crosses tile boundary, mark both nodes as boundary
                    if source_tile != target_tile {
                        boundary_nodes.insert(source_osm);
                        boundary_nodes.insert(target_osm);
                    }
                }
            }
        }

        println!("Identified {} boundary nodes", boundary_nodes.len());

        // Populate tiles with nodes and edges
        for (tile_id, tile) in tiles.iter_mut() {
            self.populate_tile(tile, graph, &node_to_tile, &boundary_nodes)?;
        }

        // Filter out empty tiles and build spatial indices
        let mut result: Vec<Tile> = tiles
            .into_iter()
            .filter(|(_, tile)| tile.node_count() > 0)
            .map(|(_, mut tile)| {
                tile.build_spatial_index();
                tile
            })
            .collect();

        result.sort_by_key(|tile| tile.id);

        println!("Created {} non-empty tiles", result.len());
        println!(
            "Average nodes per tile: {:.0}",
            result.iter().map(|t| t.node_count()).sum::<usize>() as f64 / result.len() as f64
        );
        println!(
            "Average edges per tile: {:.0}",
            result.iter().map(|t| t.edge_count()).sum::<usize>() as f64 / result.len() as f64
        );

        Ok(result)
    }

    /// Populate a single tile with nodes and edges from the full graph
    fn populate_tile(
        &self,
        tile: &mut Tile,
        graph: &RouteGraph,
        node_to_tile: &HashMap<i64, TileId>,
        boundary_nodes: &HashSet<i64>,
    ) -> Result<(), String> {
        // Add all nodes in this tile
        for (&osm_id, &tile_id) in node_to_tile {
            if tile_id == tile.id {
                // Add node to tile graph
                let node_idx = tile.graph.add_node(osm_id);
                tile.node_map.insert(osm_id, node_idx);

                // Copy coordinates
                if let Some(&coords) = graph.coords.get(&osm_id) {
                    tile.coords.insert(osm_id, coords);
                }

                // Mark boundary nodes
                if boundary_nodes.contains(&osm_id) {
                    tile.boundary_nodes.insert(osm_id);
                }
            }
        }

        // Add edges where both endpoints are in this tile
        // OR one endpoint is in this tile and the other is a boundary node in an adjacent tile
        for edge in graph.graph.edge_references() {
            let source_idx = edge.source();
            let target_idx = edge.target();
            let weight = *edge.weight();

            if let (Some(&source_osm), Some(&target_osm)) = (
                graph.graph.node_weight(source_idx),
                graph.graph.node_weight(target_idx),
            ) {
                let source_in_tile = tile.node_map.contains_key(&source_osm);
                let target_in_tile = tile.node_map.contains_key(&target_osm);

                // Include edge if both nodes are in this tile
                if source_in_tile && target_in_tile {
                    let src = *tile.node_map.get(&source_osm).unwrap();
                    let tgt = *tile.node_map.get(&target_osm).unwrap();
                    tile.graph.add_edge(src, tgt, weight);
                }
                // TODO: Handle cross-tile edges (add "virtual" boundary connection)
            }
        }

        // Copy relevant turn restrictions
        for restriction in &graph.raw_restrictions {
            // Include restriction if all nodes (from/via/to) are in this tile
            // We need to check the nodes, not ways - get nodes from way edges
            // For now, we'll use a heuristic: if via_node is in this tile, include it
            if let Some(&via_coord) = graph.coords.get(&restriction.via_node) {
                if let Some(via_tile) = self.grid.coord_to_tile(via_coord.0, via_coord.1) {
                    if via_tile == tile.id {
                        tile.restrictions.push(restriction.clone());
                    }
                }
            }
        }

        Ok(())
    }

    /// Save tiles to directory
    pub fn save_tiles(&self, tiles: &[Tile], output_dir: &Path) -> Result<(), String> {
        use std::fs;
        use std::io::Write;

        // Create output directory
        fs::create_dir_all(output_dir).map_err(|e| format!("Failed to create output directory: {}", e))?;

        // Save metadata
        let metadata = serde_json::json!({
            "grid": {
                "min_lat": self.grid.min_lat,
                "min_lon": self.grid.min_lon,
                "max_lat": self.grid.max_lat,
                "max_lon": self.grid.max_lon,
                "tile_size_lat": self.grid.tile_size_lat,
                "tile_size_lon": self.grid.tile_size_lon,
                "grid_width": self.grid.grid_width,
                "grid_height": self.grid.grid_height,
            },
            "tile_count": tiles.len(),
            "total_nodes": tiles.iter().map(|t| t.node_count()).sum::<usize>(),
            "total_edges": tiles.iter().map(|t| t.edge_count()).sum::<usize>(),
        });

        let metadata_path = output_dir.join("metadata.json");
        let mut metadata_file = fs::File::create(&metadata_path)
            .map_err(|e| format!("Failed to create metadata file: {}", e))?;
        metadata_file
            .write_all(serde_json::to_string_pretty(&metadata)?.as_bytes())
            .map_err(|e| format!("Failed to write metadata: {}", e))?;

        println!("Saved metadata to {}", metadata_path.display());

        // Create tiles subdirectory
        let tiles_dir = output_dir.join("tiles");
        fs::create_dir_all(&tiles_dir)
            .map_err(|e| format!("Failed to create tiles directory: {}", e))?;

        // Save each tile
        for tile in tiles {
            let tile_filename = format!("tile_{:02}_{:02}.bin", tile.id.0, tile.id.1);
            let tile_path = tiles_dir.join(tile_filename);

            let tile_data = bincode::serialize(tile)
                .map_err(|e| format!("Failed to serialize tile {:?}: {}", tile.id, e))?;

            fs::write(&tile_path, tile_data)
                .map_err(|e| format!("Failed to write tile {:?}: {}", tile.id, e))?;
        }

        println!("Saved {} tiles to {}", tiles.len(), tiles_dir.display());

        Ok(())
    }

    /// Load tiles from directory
    pub fn load_tiles(input_dir: &Path) -> Result<(TileGrid, Vec<Tile>), String> {
        use std::fs;

        // Load metadata
        let metadata_path = input_dir.join("metadata.json");
        let metadata_content = fs::read_to_string(&metadata_path)
            .map_err(|e| format!("Failed to read metadata: {}", e))?;
        let metadata: serde_json::Value = serde_json::from_str(&metadata_content)
            .map_err(|e| format!("Failed to parse metadata: {}", e))?;

        // Reconstruct grid
        let grid = TileGrid {
            min_lat: metadata["grid"]["min_lat"].as_f64().unwrap(),
            min_lon: metadata["grid"]["min_lon"].as_f64().unwrap(),
            max_lat: metadata["grid"]["max_lat"].as_f64().unwrap(),
            max_lon: metadata["grid"]["max_lon"].as_f64().unwrap(),
            tile_size_lat: metadata["grid"]["tile_size_lat"].as_f64().unwrap(),
            tile_size_lon: metadata["grid"]["tile_size_lon"].as_f64().unwrap(),
            grid_width: metadata["grid"]["grid_width"].as_u64().unwrap() as u16,
            grid_height: metadata["grid"]["grid_height"].as_u64().unwrap() as u16,
        };

        // Load all tiles
        let tiles_dir = input_dir.join("tiles");
        let mut tiles = Vec::new();

        for entry in fs::read_dir(&tiles_dir)
            .map_err(|e| format!("Failed to read tiles directory: {}", e))?
        {
            let entry = entry.map_err(|e| format!("Failed to read directory entry: {}", e))?;
            let path = entry.path();

            if path.extension().and_then(|s| s.to_str()) == Some("bin") {
                let tile_data = fs::read(&path)
                    .map_err(|e| format!("Failed to read tile file: {}", e))?;
                let mut tile: Tile = bincode::deserialize(&tile_data)
                    .map_err(|e| format!("Failed to deserialize tile: {}", e))?;

                // Rebuild spatial index
                tile.build_spatial_index();

                tiles.push(tile);
            }
        }

        tiles.sort_by_key(|tile| tile.id);

        println!("Loaded {} tiles from {}", tiles.len(), input_dir.display());

        Ok((grid, tiles))
    }
}
