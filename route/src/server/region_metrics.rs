//! Per-region Prometheus metrics (#91).
//!
//! Routed through the global `metrics` recorder installed by
//! `axum_prometheus::PrometheusMetricLayer::pair()` in
//! [`crate::server::api::build_router`]. The recorder's `/metrics`
//! handler renders the values; we only need to emit the right metric
//! macros from the dispatch path.
//!
//! Metric names + labels per the #91 spec:
//!
//! - `butterfly_route_region_nodes_total{region="..."}` — gauge,
//!   number of EBG nodes loaded for the region.
//! - `butterfly_route_region_edges_total{region="..."}` — gauge,
//!   number of EBG arcs loaded for the region.
//! - `butterfly_route_query_total{region="...",endpoint="..."}` —
//!   counter, incremented once per dispatched query.
//! - `butterfly_route_query_duration_seconds{region="...",endpoint="..."}`
//!   — histogram, observed once per dispatched query (in seconds).
//! - `butterfly_route_query_cross_region_total{src="...",dst="..."}` —
//!   counter, incremented when a query is rejected with 501 because
//!   the source and destination snapped into different regions. This
//!   is operationally interesting independent of `query_total`
//!   because cross-region queries never enter a region's per-region
//!   counter.

/// Register the per-region size gauges. Call once at boot for every
/// loaded region. These are gauges so they reflect "currently loaded"
/// and read sensibly across rolling restarts (Prometheus keeps the
/// last sample, no double-counting).
pub fn register_region_size(region: &str, nodes: u64, edges: u64) {
    metrics::gauge!(
        "butterfly_route_region_nodes_total",
        "region" => region.to_string()
    )
    .set(nodes as f64);
    metrics::gauge!(
        "butterfly_route_region_edges_total",
        "region" => region.to_string()
    )
    .set(edges as f64);
}

/// Record a successful per-region query: increments the counter for
/// `(region, endpoint)` and observes the duration histogram.
///
/// Endpoints are the human-friendly path tags (`route`, `nearest`,
/// `isochrone`, `table`, `trip`, `match`, `height`, `transit`).
pub fn record_query(region: &str, endpoint: &str, duration_s: f64) {
    metrics::counter!(
        "butterfly_route_query_total",
        "region" => region.to_string(),
        "endpoint" => endpoint.to_string()
    )
    .increment(1);
    metrics::histogram!(
        "butterfly_route_query_duration_seconds",
        "region" => region.to_string(),
        "endpoint" => endpoint.to_string()
    )
    .record(duration_s);
}

/// Record a cross-region rejection. Labels are sorted by the dispatcher
/// such that the `src` is the region the source snapped into and `dst`
/// is the region the destination snapped into. Both are real region ids
/// (not arbitrary strings), so the cardinality is bounded by the number
/// of loaded regions squared.
pub fn record_cross_region_reject(src: &str, dst: &str) {
    metrics::counter!(
        "butterfly_route_query_cross_region_total",
        "src" => src.to_string(),
        "dst" => dst.to_string()
    )
    .increment(1);
}
