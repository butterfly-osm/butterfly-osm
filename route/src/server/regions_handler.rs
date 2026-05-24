//! `GET /regions` — multi-region introspection (#91).
//!
//! Lists every loaded region with its id, source container path, node
//! and edge counts, and the boot-time CRC verification status. Used
//! by operators and tests to confirm that `serve --data-dir` actually
//! mounted the regions they expected, and to sanity-check the
//! cross-region 501 path (the dispatcher only ever returns 501 between
//! pairs of region ids that appear here).

use axum::{Json, extract::State, response::IntoResponse};
use serde::Serialize;
use std::sync::Arc;
use utoipa::ToSchema;

use super::regions::RegionsState;

#[derive(Debug, Serialize, ToSchema)]
pub struct LoadedRegion {
    /// Region identifier (`BE`, `LU`, …) — same string used by the
    /// per-region metric labels and by 501 dispatch errors.
    pub id: String,
    /// File-system path of the source `*.butterfly` container, as
    /// loaded. Useful for operators verifying which region bundle a
    /// running process is actually serving.
    pub container: String,
    /// Number of EBG nodes in this region's graph.
    pub nodes: u64,
    /// Number of EBG arcs in this region's graph.
    pub edges: u64,
    /// Container CRC verification state. Currently always `"verified"`
    /// (boot is eager-CRC); will gain `"pending"` and `"failed"` when
    /// #160 lands.
    pub verify_status: &'static str,
    /// Number of named roads (OSM way names indexed) in this region.
    /// Operators use this as a coarse signal for "did the road-name
    /// section load" without parsing the full container.
    pub named_roads: usize,
    /// Sorted list of mode names available in this region.
    pub modes: Vec<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct RegionsResponse {
    /// All loaded regions, sorted by id. Empty list is impossible
    /// (constructor rejects zero-region states) — callers can rely on
    /// at least one entry being present.
    pub loaded: Vec<LoadedRegion>,
}

/// `GET /regions` handler.
///
/// Read-only metadata; no graph traversal, no allocation beyond the
/// JSON serialisation buffer. Safe to hammer at high QPS — sub-microsecond
/// in steady state.
#[utoipa::path(
    get,
    path = "/regions",
    tag = "System",
    summary = "List loaded regions and their metadata",
    description = "Lists every region currently mounted by the server, with the source container path, EBG node/edge counts, the boot-time CRC verification status, and the per-region mode list. Used by operators to confirm `--data-dir` discovery and by tests for the cross-region 501 path.",
    responses(
        (status = 200, description = "Loaded regions", body = RegionsResponse),
    )
)]
pub async fn regions_handler(State(regions): State<Arc<RegionsState>>) -> impl IntoResponse {
    let loaded: Vec<LoadedRegion> = regions
        .regions
        .iter()
        .map(|r| LoadedRegion {
            id: r.id.clone(),
            container: r.container.display().to_string(),
            nodes: r.state().ebg_nodes.n_nodes as u64,
            edges: r.state().ebg_csr.n_arcs,
            verify_status: r.verify_status.label(),
            named_roads: r.state().way_names.len(),
            modes: r.state().mode_names.clone(),
        })
        .collect();
    Json(RegionsResponse { loaded })
}
