//! /health handler — health check endpoint

use axum::{Json, extract::State, response::IntoResponse};
use std::sync::Arc;

use super::regions::RegionsState;

/// Health check endpoint
#[utoipa::path(
    get,
    path = "/health",
    tag = "System",
    summary = "Health check",
    description = "Returns server status, uptime, loaded modes, dataset statistics, \
                   and (when loaded from a container) per-section lazy-CRC verification \
                   status. In multi-region mode (#91), primary-region fields keep the \
                   original shape; `regions_count`, `regions`, `total_nodes_count`, \
                   and `total_edges_count` summarise the full multi-region state, \
                   and `/regions` returns the full per-region listing.",
    responses(
        (status = 200, description = "Server is healthy"),
    )
)]
pub async fn health_handler(State(regions): State<Arc<RegionsState>>) -> impl IntoResponse {
    let primary = regions.primary();
    let uptime = primary.started_at.elapsed();
    let total_nodes: u64 = regions
        .regions
        .iter()
        .map(|r| r.state.ebg_nodes.n_nodes as u64)
        .sum();
    let total_edges: u64 = regions.regions.iter().map(|r| r.state.ebg_csr.n_arcs).sum();

    // #160: aggregate lazy-CRC verification status across every loaded
    // region. The `verify_status` field is `ok` if no region has a
    // manifest, `verified` once every section in every region is
    // `Verified`, `pending` while any section is still Unverified or
    // Verifying, and `degraded` if any section is `Failed`.
    let (verify_status, n_sections, n_verified, n_unverified, n_verifying, failed_sections) = {
        use crate::formats::lazy_verify::SectionVerifyState;
        let mut n_sections = 0usize;
        let mut n_verified = 0usize;
        let mut n_unverified = 0usize;
        let mut n_verifying = 0usize;
        let mut failed: Vec<serde_json::Value> = Vec::new();
        let mut any_lazy = false;
        for region in regions.regions.iter() {
            if let Some(lazy) = &region.state.lazy {
                any_lazy = true;
                for (name, rt) in lazy.iter_runtimes() {
                    n_sections += 1;
                    match rt.state() {
                        SectionVerifyState::Verified => n_verified += 1,
                        SectionVerifyState::Unverified => n_unverified += 1,
                        SectionVerifyState::Verifying => n_verifying += 1,
                        SectionVerifyState::Failed => {
                            failed.push(serde_json::json!({
                                "region": region.id,
                                "name": name,
                                "reason": rt.failure_reason().unwrap_or_default(),
                            }));
                        }
                    }
                }
            }
        }
        let status = if !any_lazy {
            "ok"
        } else if !failed.is_empty() {
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
    };

    let n_failed = failed_sections.len();

    Json(serde_json::json!({
        "status": "ok",
        "version": env!("CARGO_PKG_VERSION"),
        "uptime_s": uptime.as_secs(),
        "modes": primary.mode_names,
        "data_dir": primary.data_dir,
        "nodes_count": primary.ebg_nodes.n_nodes,
        "edges_count": primary.ebg_csr.n_arcs,
        "named_roads_count": primary.way_names.len(),
        "regions_count": regions.len(),
        "regions": regions.region_ids(),
        "total_nodes_count": total_nodes,
        "total_edges_count": total_edges,
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
