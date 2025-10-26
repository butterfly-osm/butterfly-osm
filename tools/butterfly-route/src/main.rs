use anyhow::Result;
use butterfly_route::{find_route, RouteGraph, CHGraph};
use butterfly_route::parse::parse_pbf;
use butterfly_route::server::run_server;
use butterfly_route::phast::builder::TileBuilder;
use butterfly_route::phast::highway::HighwayNetwork;
use butterfly_route::phast::query::PhastEngine;
use butterfly_route::phast::tile::{TileGrid, TileBounds};
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use std::time::Instant;

#[derive(Parser)]
#[command(name = "butterfly-route")]
#[command(about = "A-to-B routing using OpenStreetMap data", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Build routing graph from PBF file
    Build {
        /// Input PBF file
        input: PathBuf,
        /// Output graph file
        output: PathBuf,
    },
    /// Find route between two coordinates
    Route {
        /// Graph file
        graph: PathBuf,
        /// Start coordinate (lat,lon)
        #[arg(long)]
        from: String,
        /// End coordinate (lat,lon)
        #[arg(long)]
        to: String,
    },
    /// Start HTTP API server with OpenAPI docs
    Server {
        /// Graph file
        graph: PathBuf,
        /// Port to listen on
        #[arg(short, long, default_value = "3000")]
        port: u16,
    },
    /// Build Contraction Hierarchies graph from regular graph
    BuildCh {
        /// Input graph file (regular RouteGraph)
        input: PathBuf,
        /// Output CH graph file
        output: PathBuf,
    },
    /// Find route using Contraction Hierarchies
    RouteCh {
        /// CH graph file
        graph: PathBuf,
        /// Start coordinate (lat,lon)
        #[arg(long)]
        from: String,
        /// End coordinate (lat,lon)
        #[arg(long)]
        to: String,
    },
    /// Build PHAST L0 tiles from graph
    BuildTiles {
        /// Input graph file
        input: PathBuf,
        /// Output tiles directory
        output: PathBuf,
        /// Number of tiles per dimension (default: 16x16)
        #[arg(long, default_value = "16")]
        grid_size: u16,
    },
    /// Build PHAST L1 highway network
    BuildHighway {
        /// Input graph file
        input: PathBuf,
        /// Input tiles directory
        tiles: PathBuf,
        /// Output highway network file
        output: PathBuf,
    },
    /// Find route using PHAST
    RoutePhast {
        /// Tiles directory
        tiles: PathBuf,
        /// Highway network file
        highway: PathBuf,
        /// Start coordinate (lat,lon)
        #[arg(long)]
        from: String,
        /// End coordinate (lat,lon)
        #[arg(long)]
        to: String,
    },
}

fn parse_coord(s: &str) -> Result<(f64, f64)> {
    let parts: Vec<&str> = s.split(',').collect();
    if parts.len() != 2 {
        anyhow::bail!("Coordinate must be in format 'lat,lon'");
    }
    let lat = parts[0].trim().parse::<f64>()?;
    let lon = parts[1].trim().parse::<f64>()?;
    Ok((lat, lon))
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Build { input, output } => {
            println!("Parsing PBF file: {}", input.display());
            let start = Instant::now();

            let data = parse_pbf(&input)?;
            println!("Parsing took {:.2}s", start.elapsed().as_secs_f64());

            println!("\nBuilding graph...");
            let graph_start = Instant::now();
            let graph = RouteGraph::from_osm_data(data);
            println!("Graph building took {:.2}s", graph_start.elapsed().as_secs_f64());

            println!("\nSaving to {}...", output.display());
            graph.save(&output)?;

            println!("\nTotal time: {:.2}s", start.elapsed().as_secs_f64());
            println!("Graph saved successfully!");
        }
        Commands::Route { graph, from, to } => {
            println!("Loading graph from {}...", graph.display());
            let route_graph = RouteGraph::load(&graph)?;

            let from_coord = parse_coord(&from)?;
            let to_coord = parse_coord(&to)?;

            println!("Finding route from {} to {}...", from, to);
            let start = Instant::now();

            let result = find_route(&route_graph, from_coord, to_coord)?;

            println!("\nRoute found in {:.3}s", start.elapsed().as_secs_f64());
            println!("Distance: {:.0}m", result.distance_meters);
            println!("Time: {:.1} minutes", result.time_seconds / 60.0);
            println!("Nodes visited: {}", result.node_count);
        }
        Commands::Server { graph, port } => {
            println!("Loading graph from {}...", graph.display());
            let start = Instant::now();
            let route_graph = RouteGraph::load(&graph)?;
            println!("Graph loaded in {:.2}s", start.elapsed().as_secs_f64());

            run_server(route_graph, port).await?;
        }
        Commands::BuildCh { input, output } => {
            println!("Loading graph from {}...", input.display());
            let route_graph = RouteGraph::load(&input)?;

            println!("\nBuilding Contraction Hierarchies...");
            let ch_graph = CHGraph::from_route_graph(&route_graph)?;

            println!("\nSaving CH graph to {}...", output.display());
            let save_start = Instant::now();
            ch_graph.save(&output)?;
            println!("✓ CH graph saved in {:.2}s", save_start.elapsed().as_secs_f64());
            println!("✓ CH preprocessing complete!");
        }
        Commands::RouteCh { graph, from, to } => {
            println!("Loading CH graph from {}...", graph.display());
            let load_start = Instant::now();
            let ch_graph = CHGraph::load(&graph)?;
            println!("CH graph loaded in {:.2}s", load_start.elapsed().as_secs_f64());

            let from_coord = parse_coord(&from)?;
            let to_coord = parse_coord(&to)?;

            // Find nearest nodes
            println!("\nFinding nearest nodes...");
            let start_osm = ch_graph.nearest_node(from_coord)
                .ok_or_else(|| anyhow::anyhow!("Could not find start node"))?;
            let goal_osm = ch_graph.nearest_node(to_coord)
                .ok_or_else(|| anyhow::anyhow!("Could not find goal node"))?;

            println!("Routing from node {} to node {}", start_osm, goal_osm);

            // Query CH graph
            let query_start = Instant::now();
            let result = ch_graph.query(start_osm, goal_osm)
                .ok_or_else(|| anyhow::anyhow!("No route found"))?;

            println!("\n=== CH Query Results ===");
            println!("Total query time: {:.3}s", query_start.elapsed().as_secs_f64());
            println!("Distance: {:.0}m ({:.1} km)", result.0, result.0 / 1000.0);
            println!("Time: {:.1} minutes", result.0 / 33.33); // Assuming average 120 km/h
            println!("Nodes in path: {}", result.1.len());
            println!("========================");
        }
        Commands::BuildTiles { input, output, grid_size } => {
            println!("Loading graph from {}...", input.display());
            let graph = RouteGraph::load(&input)?;

            // Determine grid bounds from graph coordinates
            let coords: Vec<_> = graph.coords.values().copied().collect();
            let min_lat = coords.iter().map(|c| c.0).fold(f64::INFINITY, f64::min);
            let max_lat = coords.iter().map(|c| c.0).fold(f64::NEG_INFINITY, f64::max);
            let min_lon = coords.iter().map(|c| c.1).fold(f64::INFINITY, f64::min);
            let max_lon = coords.iter().map(|c| c.1).fold(f64::NEG_INFINITY, f64::max);

            println!("\nGraph bounds:");
            println!("  Latitude:  {:.6} to {:.6}", min_lat, max_lat);
            println!("  Longitude: {:.6} to {:.6}", min_lon, max_lon);

            // Create grid
            let grid = TileGrid::new(
                min_lat, min_lon,
                max_lat, max_lon,
                grid_size, grid_size,
            );

            println!("\nBuilding {}x{} tile grid...", grid_size, grid_size);
            let builder = TileBuilder::new(grid.clone());
            let tiles = builder.build_tiles(&graph)
                .map_err(|e| anyhow::anyhow!("Failed to build tiles: {}", e))?;

            println!("\nSaving tiles to {}...", output.display());
            builder.save_tiles(&tiles, &output)
                .map_err(|e| anyhow::anyhow!("Failed to save tiles: {}", e))?;

            println!("✓ Tile extraction complete!");
        }
        Commands::BuildHighway { input, tiles, output } => {
            println!("Loading graph from {}...", input.display());
            let graph = RouteGraph::load(&input)?;

            println!("Loading tiles from {}...", tiles.display());
            let (grid, tile_vec) = TileBuilder::load_tiles(&tiles)
                .map_err(|e| anyhow::anyhow!("Failed to load tiles: {}", e))?;

            // Collect boundary nodes from tiles
            let mut boundary_nodes = std::collections::HashMap::new();
            for tile in &tile_vec {
                for &osm_id in &tile.boundary_nodes {
                    boundary_nodes.insert(osm_id, tile.id);
                }
            }

            println!("\nExtracting highway network...");
            let mut highway_network = HighwayNetwork::from_road_graph(&graph, boundary_nodes)
                .map_err(|e| anyhow::anyhow!("Failed to extract highway network: {}", e))?;

            // Build spatial index for highway entry points
            highway_network.build_entry_index();

            println!("\nSaving highway network to {}...", output.display());
            highway_network.save(output.to_str().unwrap())
                .map_err(|e| anyhow::anyhow!("Failed to save highway network: {}", e))?;

            println!("✓ Highway network preprocessing complete!");
        }
        Commands::RoutePhast { tiles, highway, from, to } => {
            println!("Loading tiles from {}...", tiles.display());
            let (grid, tile_vec) = TileBuilder::load_tiles(&tiles)
                .map_err(|e| anyhow::anyhow!("Failed to load tiles: {}", e))?;

            println!("Loading highway network from {}...", highway.display());
            let mut highway_network = HighwayNetwork::load(highway.to_str().unwrap())
                .map_err(|e| anyhow::anyhow!("Failed to load highway network: {}", e))?;

            // Build spatial index (not serialized)
            highway_network.build_entry_index();

            let from_coord = parse_coord(&from)?;
            let to_coord = parse_coord(&to)?;

            println!("\nInitializing PHAST engine...");
            let engine = PhastEngine::new(grid, tile_vec, highway_network);

            println!("Finding route from {} to {}...", from, to);
            let start = Instant::now();

            let result = engine.route(from_coord, to_coord)
                .map_err(|e| anyhow::anyhow!("Route failed: {}", e))?;

            println!("\n=== PHAST Query Results ===");
            println!("Query type: {:?}", result.query_type);
            println!("Total query time: {:.3}s", start.elapsed().as_secs_f64());
            println!("Distance: {:.0}m ({:.1} km)", result.distance, result.distance / 1000.0);
            println!("Time: {:.1} minutes", result.time / 60.0);
            println!("Nodes in path: {}", result.path.len());
            println!("===========================");
        }
    }

    Ok(())
}
