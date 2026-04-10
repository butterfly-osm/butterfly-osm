//! Step 9: Query Engine
//!
//! Dual-transport server: Axum REST/JSON + Arrow Flight gRPC, sharing `Arc<ServerState>`.
//!
//! # REST Endpoints (Axum, `--port`)
//!
//! - `GET /route` - Point-to-point routing with geometry, steps, alternatives
//! - `GET /nearest` - Snap to nearest road segments
//! - `POST /table` - Distance matrix (bucket M2M)
//! - `GET /isochrone` - Reachability polygon (GeoJSON/WKB)
//! - `POST /isochrone/bulk` - Parallel batch isochrones (WKB stream)
//! - `POST /trip` - TSP/trip optimization
//! - `POST /match` - GPS trace map matching (HMM + Viterbi)
//! - `GET /height` - Elevation lookup (SRTM DEM)
//! - `GET /health` - Health check with uptime and stats
//! - `GET /metrics` - Prometheus metrics
//! - `GET /swagger-ui/` - OpenAPI documentation
//!
//! # Arrow Flight gRPC Endpoints (`--grpc-port`)
//!
//! All via `DoGet` with ticket format `action:profile:params_json`:
//! - `matrix` - Distance/duration matrix (Bucket M2M or PHAST tiling)
//! - `route_batch` - Batch P2P routing with WKB geometry
//! - `isochrone` - Reachability polygons as WKB per interval
//!
//! # Architecture
//!
//! All queries use the same CCH hierarchy with edge-based state:
//! - Bidirectional Dijkstra on up/down graphs
//! - Shortcut unpacking for path reconstruction
//! - Geometry lookup via EBG -> NBG mapping

pub mod api;
pub mod avoid;
pub mod catchment;
pub mod debug;
pub mod elevation;
pub mod exclude;
// tonic::Status is 176 bytes — the canonical gRPC error type.
// Every gRPC function returns Result<_, Status>; boxing adds indirection with no benefit.
#[allow(clippy::result_large_err)]
pub mod flight;
pub mod geometry;
pub mod health_handler;
pub mod height_handler;
pub mod isochrone_handler;
pub mod map_match;
pub mod matching;
pub mod nearest;
pub mod query;
pub mod route;
pub mod spatial;
pub mod state;
pub mod table;
pub mod trip;
pub mod types;
pub mod unpack;

#[cfg(test)]
mod api_tests;
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

/// Transport mode controlling which servers start
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transport {
    /// REST/JSON only (Axum HTTP)
    Rest,
    /// Arrow Flight gRPC only
    Grpc,
    /// Both REST and gRPC (default)
    Both,
}

impl Transport {
    pub fn parse(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "rest" => Ok(Transport::Rest),
            "grpc" => Ok(Transport::Grpc),
            "both" => Ok(Transport::Both),
            other => anyhow::bail!("Invalid transport '{}'. Use: rest, grpc, both", other),
        }
    }
}

/// Load all data and start the server(s)
pub async fn serve(
    data_dir: &Path,
    port: Option<u16>,
    grpc_port: Option<u16>,
    transport: Transport,
    mode_filter: Option<&[String]>,
) -> Result<()> {
    tracing::info!("Step 9: Starting query server...");

    // Load server state
    let state = Arc::new(ServerState::load(data_dir, mode_filter)?);

    // Find free ports
    let http_port = match port {
        Some(p) => p,
        None => find_free_port(8080)?,
    };
    let grpc_port = grpc_port.unwrap_or(http_port + 1);

    match transport {
        Transport::Rest => {
            start_rest_server(state, http_port).await?;
        }
        Transport::Grpc => {
            start_grpc_server(state, grpc_port).await?;
        }
        Transport::Both => {
            let state_rest = Arc::clone(&state);
            let state_grpc = Arc::clone(&state);

            let rest_handle =
                tokio::spawn(async move { start_rest_server(state_rest, http_port).await });
            let grpc_handle =
                tokio::spawn(async move { start_grpc_server(state_grpc, grpc_port).await });

            // Wait for either to finish (typically via shutdown signal)
            tokio::select! {
                res = rest_handle => {
                    if let Err(e) = res {
                        tracing::error!(error = %e, "REST server task failed");
                    }
                }
                res = grpc_handle => {
                    if let Err(e) = res {
                        tracing::error!(error = %e, "gRPC server task failed");
                    }
                }
            }
        }
    }

    tracing::info!("server shut down gracefully");
    Ok(())
}

/// Start only the Axum REST/JSON server
async fn start_rest_server(state: Arc<ServerState>, port: u16) -> Result<()> {
    let app = api::build_router(state);

    let addr = format!("0.0.0.0:{}", port);
    tracing::info!(
        port = port,
        "REST server listening on http://127.0.0.1:{}",
        port
    );
    tracing::info!(
        port = port,
        "Swagger UI: http://127.0.0.1:{}/swagger-ui/",
        port
    );

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    Ok(())
}

/// Start only the Arrow Flight gRPC server
async fn start_grpc_server(state: Arc<ServerState>, port: u16) -> Result<()> {
    let grpc_addr: std::net::SocketAddr = format!("0.0.0.0:{}", port).parse()?;
    tracing::info!(port = port, "gRPC Flight server listening on {}", grpc_addr);

    let flight_svc = flight::build_flight_server(state);

    tonic::transport::Server::builder()
        .add_service(flight_svc)
        .serve_with_shutdown(grpc_addr, shutdown_signal())
        .await?;

    Ok(())
}
