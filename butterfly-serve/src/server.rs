//! Main server implementation

use axum::{response::Json, Router};
use butterfly_extract::Extractor;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tower_http::cors::CorsLayer;
use crate::routes::{AppState, get_telemetry, probe_snap, graph_stats, graph_edge};

/// Main routing server
pub struct RoutingServer {
    addr: SocketAddr,
    extractor: Arc<Mutex<Extractor>>,
}

impl RoutingServer {
    pub fn new(addr: SocketAddr, extractor: Extractor) -> Self {
        Self { 
            addr,
            extractor: Arc::new(Mutex::new(extractor)),
        }
    }

    /// Create the router with all routes
    pub fn router(state: AppState) -> Router {
        Router::new()
            .route("/health", axum::routing::get(health_check))
            .route("/telemetry", axum::routing::get(get_telemetry))
            .route("/probe/snap", axum::routing::get(probe_snap))
            .route("/graph/stats", axum::routing::get(graph_stats))
            .route("/graph/edge/:id", axum::routing::get(graph_edge))
            .with_state(state)
            .layer(CorsLayer::permissive())
    }

    /// Start the server
    pub async fn serve(self) -> Result<(), Box<dyn std::error::Error>> {
        let state = AppState {
            extractor: self.extractor,
        };
        let app = Self::router(state);
        let listener = tokio::net::TcpListener::bind(self.addr).await?;
        println!("Server listening on {}", self.addr);
        println!("Available endpoints:");
        println!("  GET /health - Health check");
        println!("  GET /telemetry - Spatial density telemetry with bbox filtering");
        println!("  GET /probe/snap - Canonical mapping validation probe");
        println!("  GET /graph/stats - Graph statistics for debugging");
        println!("  GET /graph/edge/{{id}} - Edge details for debugging");
        axum::serve(listener, app).await?;
        Ok(())
    }
}

async fn health_check() -> Json<serde_json::Value> {
    Json(serde_json::json!({"status": "ok"}))
}
