use anyhow::Result;
use butterfly_route::{find_route, RouteGraph};
use butterfly_route::parse::parse_pbf;
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

fn main() -> Result<()> {
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
    }

    Ok(())
}
