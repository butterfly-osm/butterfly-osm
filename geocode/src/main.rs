//! butterfly-geocode CLI: build shards and serve the API.

#![deny(unsafe_code)]

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use tracing::{Level, info};
use tracing_subscriber::EnvFilter;

use butterfly_geocode::osm_extract::{ExtractProgress, extract_addresses};
use butterfly_geocode::server::{ServerState, build_router};
use butterfly_geocode::shard::builder::build_shard;
use butterfly_geocode::shard::reader::Shard;

#[derive(Parser, Debug)]
#[command(
    name = "butterfly-geocode",
    about = "Geocoder for the butterfly-osm toolkit"
)]
struct Cli {
    /// Logging format: `text` (default) or `json`.
    #[arg(long, default_value = "text", global = true)]
    log_format: String,

    #[command(subcommand)]
    cmd: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Build a shard from an OSM PBF.
    BuildShard {
        #[arg(long)]
        pbf: PathBuf,
        #[arg(long)]
        out: PathBuf,
    },
    /// Run the HTTP server.
    Serve {
        #[arg(long)]
        shard: PathBuf,
        #[arg(long, default_value_t = 3003)]
        port: u16,
        #[arg(long, default_value = "0.0.0.0")]
        host: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    init_logging(&cli.log_format);

    match cli.cmd {
        Command::BuildShard { pbf, out } => build_shard_cmd(&pbf, &out),
        Command::Serve { shard, port, host } => serve_cmd(&shard, &host, port).await,
    }
}

fn init_logging(format: &str) {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,butterfly_geocode=debug"));
    if format.eq_ignore_ascii_case("json") {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_max_level(Level::DEBUG)
            .json()
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_max_level(Level::DEBUG)
            .init();
    }
}

fn build_shard_cmd(pbf: &PathBuf, out: &PathBuf) -> Result<()> {
    info!(pbf = %pbf.display(), out = %out.display(), "extracting OSM addresses");
    let start = std::time::Instant::now();

    let addresses = extract_addresses(pbf, |evt| match evt {
        ExtractProgress::Phase { phase } => info!("phase: {phase}"),
        ExtractProgress::NodePass {
            nodes_seen,
            addresses_emitted,
        } => info!(nodes_seen, addresses_emitted, "nodes pass complete"),
        ExtractProgress::WayPass {
            ways_seen,
            addresses_emitted,
        } => info!(ways_seen, addresses_emitted, "ways pass complete"),
    })?;

    info!(
        count = addresses.len(),
        secs = start.elapsed().as_secs_f64(),
        "extracted addresses"
    );

    let stats = build_shard(out, addresses).context("writing shard")?;
    info!(
        records = stats.record_count,
        unique_postcodes = stats.unique_postcodes,
        unique_streets = stats.unique_streets,
        strings_bytes = stats.strings_bytes,
        records_bytes = stats.records_bytes,
        index_bytes = stats.index_bytes,
        secs = start.elapsed().as_secs_f64(),
        "shard built"
    );

    let _ = Shard::open(out).context("verifying shard CRC after build")?;
    info!("shard verified");

    Ok(())
}

async fn serve_cmd(shard_path: &PathBuf, host: &str, port: u16) -> Result<()> {
    info!(shard = %shard_path.display(), "loading shard");
    let shard = Shard::open(shard_path).context("opening shard")?;
    info!(record_count = shard.record_count(), "shard loaded");

    let state = Arc::new(ServerState::new(shard));
    let app = build_router(state);

    let addr = format!("{host}:{port}");
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .with_context(|| format!("binding {addr}"))?;
    info!(addr = %addr, "serving");

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal())
    .await
    .context("server crashed")?;
    Ok(())
}

async fn shutdown_signal() {
    use tokio::signal;
    let ctrl_c = async {
        signal::ctrl_c().await.expect("install Ctrl+C handler");
    };
    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    info!("shutdown signal received");
}
