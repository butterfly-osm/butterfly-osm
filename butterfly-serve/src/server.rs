//! Main server implementation

use axum::{response::Json, Router};
use std::net::SocketAddr;
use tower_http::cors::CorsLayer;

/// Main routing server
pub struct RoutingServer {
    addr: SocketAddr,
}

impl RoutingServer {
    pub fn new(addr: SocketAddr) -> Self {
        Self { addr }
    }

    /// Create the router with all routes
    pub fn router() -> Router {
        Router::new()
            .route("/health", axum::routing::get(health_check))
            .layer(CorsLayer::permissive())
    }

    /// Start the server
    pub async fn serve(self) -> Result<(), Box<dyn std::error::Error>> {
        let app = Self::router();
        let listener = tokio::net::TcpListener::bind(self.addr).await?;
        println!("Server listening on {}", self.addr);
        axum::serve(listener, app).await?;
        Ok(())
    }
}

async fn health_check() -> Json<serde_json::Value> {
    Json(serde_json::json!({"status": "ok"}))
}
