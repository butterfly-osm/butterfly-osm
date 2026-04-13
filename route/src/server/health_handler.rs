//! /health handler — health check endpoint

use axum::{Json, extract::State, response::IntoResponse};
use std::sync::Arc;

use super::state::ServerState;

/// Health check endpoint
#[utoipa::path(
    get,
    path = "/health",
    tag = "System",
    summary = "Health check",
    description = "Returns server status, uptime, loaded modes, and dataset statistics.",
    responses(
        (status = 200, description = "Server is healthy"),
    )
)]
pub async fn health_handler(State(state): State<Arc<ServerState>>) -> impl IntoResponse {
    let uptime = state.started_at.elapsed();
    Json(serde_json::json!({
        "status": "ok",
        "version": env!("CARGO_PKG_VERSION"),
        "uptime_s": uptime.as_secs(),
        "modes": state.mode_names,
        "data_dir": state.data_dir,
        "nodes_count": state.ebg_nodes.n_nodes,
        "edges_count": state.ebg_csr.n_arcs,
        "named_roads_count": state.way_names.len(),
    }))
}
