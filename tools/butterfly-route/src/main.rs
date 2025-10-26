use anyhow::Result;
use butterfly_route::{find_route, RouteGraph, CHGraph};
use butterfly_route::parse::parse_pbf;
use butterfly_route::server::run_server;
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
    }

    Ok(())
}
