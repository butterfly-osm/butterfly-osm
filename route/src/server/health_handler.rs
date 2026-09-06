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
    description = "Liveness probe: status, version, uptime, loaded modes and dataset \
                   counts — O(1), no section walk (#551). Per-region lazy-CRC \
                   verification status is on `/regions`; cache gauges on `/metrics`. \
                   `transit_feeds` names the operators the loaded timetable holds, the \
                   ones knowingly excluded with their reason, and any undeclared gap \
                   (#603); null when transit is not installed.",
    responses(
        (status = 200, description = "Server is healthy"),
    )
)]
pub async fn health_handler(State(regions): State<Arc<RegionsState>>) -> impl IntoResponse {
    // #292 Phase 3: use server-level started_at (set when RegionsState
    // was constructed) rather than primary.started_at, which would
    // force a lazy region load just to compute uptime.
    let uptime = regions.server_started_at.elapsed();
    // #292 Phase 3: only sum stats for regions that are already loaded.
    // Pending regions don't contribute to the totals (a lazy-boot
    // operator sees the total grow as queries pull regions in).
    let total_nodes: u64 = regions
        .regions
        .iter()
        .filter_map(|r| r.state_loaded().map(|s| s.ebg_nodes.n_nodes as u64))
        .sum();
    let total_edges: u64 = regions
        .regions
        .iter()
        .filter_map(|r| r.state_loaded().map(|s| s.ebg_csr.n_arcs))
        .sum();

    // #551: /health is the liveness probe — it must be O(1). The lazy-CRC
    // verification detail lives on /regions (`verify_status` per region) and
    // the avoid-cache gauges are exported by /metrics; walking every section
    // of every region here is what starved liveness under load (#539).
    // Primary-region stats only if already loaded (no lazy load from /health).
    let primary_loaded = regions.regions.first().and_then(|r| r.state_loaded());

    // #603: a merged timetable that is one operator short looks exactly
    // like a complete one from the outside. It says which operators it
    // holds, which are knowingly excluded (with the reason) and which are
    // an undeclared gap. O(1): three short id lists built at load.
    let transit = primary_loaded
        .as_ref()
        .and_then(|p| p.transit.as_ref())
        .map(|t| t.snapshot.feeds.clone());

    Json(serde_json::json!({
        "status": "ok",
        "version": env!("CARGO_PKG_VERSION"),
        "uptime_s": uptime.as_secs(),
        "modes": primary_loaded.as_ref().map(|p| {
            let mut m: Vec<String> = p.mode_lookup.keys().cloned().collect();
            m.sort();
            m
        }).unwrap_or_default(),
        "data_dir": primary_loaded.as_ref().map(|p| p.data_dir.clone()).unwrap_or_default(),
        "nodes_count": primary_loaded.as_ref().map(|p| p.ebg_nodes.n_nodes).unwrap_or(0),
        "edges_count": primary_loaded.as_ref().map(|p| p.ebg_csr.n_arcs).unwrap_or(0),
        "named_roads_count": primary_loaded.as_ref().map(|p| p.way_names.len()).unwrap_or(0),
        "regions_count": regions.len(),
        "regions": regions.region_ids(),
        "total_nodes_count": total_nodes,
        "total_edges_count": total_edges,
        "transit_feeds": transit,
    }))
}
