//! Step 9: Query Engine
//!
//! HTTP server providing routing queries on the CCH hierarchy.
//!
//! # Endpoints
//!
//! - `GET /route` - Point-to-point routing
//! - `GET /matrix` - Distance matrix (one-to-many)
//! - `GET /isochrone` - Reachability polygon
//! - `GET /health` - Health check
//! - `GET /swagger-ui` - OpenAPI documentation
//!
//! # Architecture
//!
//! All queries use the same CCH hierarchy with edge-based state:
//! - Bidirectional Dijkstra on up/down graphs
//! - Shortcut unpacking for path reconstruction
//! - Geometry lookup via EBG -> NBG mapping

pub mod api;
pub mod geometry;
pub mod query;
pub mod spatial;
pub mod state;
pub mod unpack;

use anyhow::Result;
use std::net::TcpListener;
use std::path::Path;
use std::sync::Arc;

pub use state::ServerState;

/// Find a free port starting from the given port
pub fn find_free_port(start: u16) -> u16 {
    for port in start..65535 {
        if TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return port;
        }
    }
    panic!("No free port found");
}

/// Load all data and start the server
pub async fn serve(data_dir: &Path, port: Option<u16>) -> Result<()> {
    println!("\n🚀 Step 9: Starting query server...\n");

    // Load server state
    let state = Arc::new(ServerState::load(data_dir)?);

    // Find free port
    let port = port.unwrap_or_else(|| find_free_port(8080));

    // Build router
    let app = api::build_router(state);

    // Start server
    let addr = format!("0.0.0.0:{}", port);
    println!("🌐 Server listening on http://127.0.0.1:{}", port);
    println!("📖 Swagger UI: http://127.0.0.1:{}/swagger-ui/", port);

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
