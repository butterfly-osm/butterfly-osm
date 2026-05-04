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
    description = "Returns server status, uptime, loaded modes, dataset statistics, and (when loaded from a container) per-section lazy-CRC verification status.",
    responses(
        (status = 200, description = "Server is healthy"),
    )
)]
pub async fn health_handler(State(state): State<Arc<ServerState>>) -> impl IntoResponse {
    let uptime = state.started_at.elapsed();

    // #160: aggregate lazy-CRC verification status. The `verify_status`
    // field is `ok` for non-container loads (no manifest), `verified`
    // once every section is `Verified`, `pending` while sections are
    // still unverified or in-flight, and `degraded` if any section is
    // `Failed`.
    let (verify_status, n_sections, n_verified, n_unverified, n_verifying, failed_sections) =
        if let Some(lazy) = &state.lazy {
            use crate::formats::lazy_verify::SectionVerifyState;
            let mut n_sections = 0usize;
            let mut n_verified = 0usize;
            let mut n_unverified = 0usize;
            let mut n_verifying = 0usize;
            let mut failed: Vec<serde_json::Value> = Vec::new();
            for (name, rt) in lazy.iter_runtimes() {
                n_sections += 1;
                match rt.state() {
                    SectionVerifyState::Verified => n_verified += 1,
                    SectionVerifyState::Unverified => n_unverified += 1,
                    SectionVerifyState::Verifying => n_verifying += 1,
                    SectionVerifyState::Failed => {
                        failed.push(serde_json::json!({
                            "name": name,
                            "reason": rt.failure_reason().unwrap_or_default(),
                        }));
                    }
                }
            }
            let status = if !failed.is_empty() {
                "degraded"
            } else if n_unverified == 0 && n_verifying == 0 {
                "verified"
            } else {
                "pending"
            };
            (
                status,
                n_sections,
                n_verified,
                n_unverified,
                n_verifying,
                failed,
            )
        } else {
            ("ok", 0, 0, 0, 0, Vec::new())
        };

    let n_failed = failed_sections.len();

    Json(serde_json::json!({
        "status": "ok",
        "version": env!("CARGO_PKG_VERSION"),
        "uptime_s": uptime.as_secs(),
        "modes": state.mode_names,
        "data_dir": state.data_dir,
        "nodes_count": state.ebg_nodes.n_nodes,
        "edges_count": state.ebg_csr.n_arcs,
        "named_roads_count": state.way_names.len(),
        "verify_status": verify_status,
        "verify": {
            "n_sections": n_sections,
            "n_verified": n_verified,
            "n_unverified": n_unverified,
            "n_verifying": n_verifying,
            "n_failed": n_failed,
            "failed": failed_sections,
        },
    }))
}
