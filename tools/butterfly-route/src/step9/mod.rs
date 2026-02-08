//! Step 9: Query Engine
//!
//! HTTP server providing routing queries on the CCH hierarchy.
//!
//! # Endpoints
//!
//! - `GET /route` - Point-to-point routing with geometry, steps, alternatives
//! - `GET /nearest` - Snap to nearest road segments
//! - `POST /table` - Distance matrix (bucket M2M)
//! - `POST /table/stream` - Arrow IPC streaming for large matrices
//! - `GET /isochrone` - Reachability polygon (GeoJSON/WKB)
//! - `POST /isochrone/bulk` - Parallel batch isochrones (WKB stream)
//! - `POST /trip` - TSP/trip optimization
//! - `POST /match` - GPS trace map matching (HMM + Viterbi)
//! - `GET /height` - Elevation lookup (SRTM DEM)
//! - `GET /health` - Health check with uptime and stats
//! - `GET /metrics` - Prometheus metrics
//! - `GET /swagger-ui/` - OpenAPI documentation
//!
//! # Architecture
//!
//! All queries use the same CCH hierarchy with edge-based state:
//! - Bidirectional Dijkstra on up/down graphs
//! - Shortcut unpacking for path reconstruction
//! - Geometry lookup via EBG -> NBG mapping

pub mod api;
pub mod elevation;
pub mod geometry;
pub mod map_match;
pub mod query;
pub mod spatial;
pub mod state;
pub mod trip;
pub mod unpack;

#[cfg(test)]
mod consistency_test;
#[cfg(test)]
mod isochrone_test;

use anyhow::Result;
use std::net::TcpListener;
use std::path::Path;
use std::sync::Arc;

pub use state::ServerState;

/// Initialize structured logging with tracing.
///
/// - `log_format`: "text" for human-readable, "json" for structured JSON lines.
/// - Respects RUST_LOG env var for filtering (default: `info,tower_http=debug`).
pub fn init_tracing(log_format: &str) {
    use tracing_subscriber::{fmt, EnvFilter};

    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,tower_http=debug"));

    match log_format {
        "json" => {
            fmt()
                .json()
                .with_env_filter(filter)
                .with_target(true)
                .init();
        }
        _ => {
            fmt().with_env_filter(filter).with_target(false).init();
        }
    }
}

/// Find a free port starting from the given port
pub fn find_free_port(start: u16) -> Result<u16> {
    for port in start..65535 {
        if TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return Ok(port);
        }
    }
    anyhow::bail!("No free port found starting from {}", start);
}

/// Shutdown signal: waits for SIGINT (Ctrl-C) or SIGTERM.
async fn shutdown_signal() {
    use tokio::signal;

    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    tracing::info!("shutdown signal received, starting graceful shutdown");
}

/// Load all data and start the server
pub async fn serve(data_dir: &Path, port: Option<u16>) -> Result<()> {
    tracing::info!("Step 9: Starting query server...");

    // Load server state
    let state = Arc::new(ServerState::load(data_dir)?);

    // Find free port
    let port = match port {
        Some(p) => p,
        None => find_free_port(8080)?,
    };

    // Build router
    let app = api::build_router(state);

    // Start server
    let addr = format!("0.0.0.0:{}", port);
    tracing::info!(port = port, "server listening on http://127.0.0.1:{}", port);
    tracing::info!(
        port = port,
        "Swagger UI: http://127.0.0.1:{}/swagger-ui/",
        port
    );

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    tracing::info!("server shut down gracefully");
    Ok(())
}
