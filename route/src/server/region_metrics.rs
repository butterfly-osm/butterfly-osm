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
//!   counter, incremented once per dispatched query. The endpoint
//!   label is one of the closed set wired by [`record_query`]:
//!   `route`, `nearest`, `isochrone`, `table`, `trip`, `match`,
//!   `height`, `transit`.
//! - `butterfly_route_query_duration_seconds{region="...",endpoint="..."}`
//!   — histogram, observed once per dispatched query (in seconds).
//!   Same endpoint set as `query_total`.
//! - `butterfly_route_query_cross_region_total{src="...",dst="..."}` —
//!   counter, incremented when a query is rejected with 501 because
//!   the source and destination snapped into different regions. This
//!   is operationally interesting independent of `query_total`
//!   because cross-region queries never enter a region's per-region
//!   counter.
//!
//! ## Pre-created handles (#91 review item)
//!
//! [`RegionMetrics`] owns one `metrics::Counter` / `Histogram` /
//! `Gauge` per (region, endpoint) tuple, allocated once at boot from
//! [`RegionMetrics::new`]. The hot path looks the handle up by
//! `&'static str` endpoint key (zero-allocation map lookup) and
//! increments / records on the existing handle. This avoids the
//! `region.to_string()` + `endpoint.to_string()` allocation pair the
//! old `metrics::counter!` macro forced on every dispatched query.

use metrics::{Counter, Gauge, Histogram};
use std::collections::HashMap;

/// The closed set of endpoint label values emitted by `/metrics`.
/// Drives both [`RegionMetrics::new`] (one Counter + Histogram per
/// entry) and the `record_query` typed wrapper. Adding a new endpoint
/// means adding it here and wiring the handler.
pub const ENDPOINTS: &[&str] = &[
    "route",
    "nearest",
    "isochrone",
    "table",
    "trip",
    "match",
    "height",
    "transit",
];

/// Pre-created per-region metric handles. Stored once per region in
/// [`crate::server::regions::RegionEntry::metrics`]. Lookup by
/// endpoint name is O(1) on the closed [`ENDPOINTS`] set.
pub struct RegionMetrics {
    pub region: String,
    pub query_total: HashMap<&'static str, Counter>,
    pub query_duration: HashMap<&'static str, Histogram>,
    pub nodes: Gauge,
    pub edges: Gauge,
}

impl RegionMetrics {
    /// Allocate every handle this region needs. The `region` string is
    /// stored once on the struct and reused for every metric label,
    /// so the recorder sees a single label string per (region,
    /// endpoint) tuple instead of an allocation per query.
    pub fn new(region: &str) -> Self {
        let region_str = region.to_string();
        let mut query_total = HashMap::with_capacity(ENDPOINTS.len());
        let mut query_duration = HashMap::with_capacity(ENDPOINTS.len());
        for ep in ENDPOINTS {
            query_total.insert(
                *ep,
                metrics::counter!(
                    "butterfly_route_query_total",
                    "region" => region_str.clone(),
                    "endpoint" => ep.to_string(),
                ),
            );
            query_duration.insert(
                *ep,
                metrics::histogram!(
                    "butterfly_route_query_duration_seconds",
                    "region" => region_str.clone(),
                    "endpoint" => ep.to_string(),
                ),
            );
        }
        let nodes = metrics::gauge!(
            "butterfly_route_region_nodes_total",
            "region" => region_str.clone(),
        );
        let edges = metrics::gauge!(
            "butterfly_route_region_edges_total",
            "region" => region_str.clone(),
        );
        Self {
            region: region_str,
            query_total,
            query_duration,
            nodes,
            edges,
        }
    }

    /// Set the per-region size gauges. Call once at boot.
    pub fn set_size(&self, nodes: u64, edges: u64) {
        self.nodes.set(nodes as f64);
        self.edges.set(edges as f64);
    }

    /// Record a query against a known endpoint. Returns silently if
    /// the endpoint isn't in [`ENDPOINTS`] (caller bug — wire the new
    /// endpoint into the constant). Hot path uses these handles
    /// directly so no per-call allocation is needed.
    pub fn record(&self, endpoint: &str, duration_s: f64) {
        if let Some(c) = self.query_total.get(endpoint) {
            c.increment(1);
        }
        if let Some(h) = self.query_duration.get(endpoint) {
            h.record(duration_s);
        }
    }
}

/// Register the per-region size gauges. Call once at boot for every
/// loaded region. Kept for back-compat with single-region call sites
/// that don't hold a [`RegionMetrics`] handle.
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
/// Endpoints are the human-friendly path tags listed in
/// [`ENDPOINTS`] (`route`, `nearest`, `isochrone`, `table`, `trip`,
/// `match`, `height`, `transit`). Each one is wired by a request
/// handler in `server/`; values outside this set will not be observed
/// by Prometheus dashboards but won't cause an error either.
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// A [`metrics::Recorder`] that records the *key* of every metric
    /// registered while it is installed, rendered the way the
    /// Prometheus exporter renders it: `name{label="value",...}` in
    /// emission order.
    ///
    /// #577: this is how "the `/metrics` label set is byte-identical"
    /// gets checked without booting a server.
    /// `QueryContext::record` is a verbatim forwarder to
    /// [`record_query`], so pinning the keys [`record_query`] emits
    /// pins what every migrated handler emits.
    struct KeyCapture(Mutex<Vec<String>>);

    impl KeyCapture {
        fn new() -> Self {
            Self(Mutex::new(Vec::new()))
        }

        fn push(&self, kind: &str, key: &metrics::Key) {
            let mut rendered = format!("{} {}", kind, key.name());
            let labels: Vec<String> = key
                .labels()
                .map(|l| format!("{}={:?}", l.key(), l.value()))
                .collect();
            if !labels.is_empty() {
                rendered.push('{');
                rendered.push_str(&labels.join(","));
                rendered.push('}');
            }
            self.0.lock().unwrap().push(rendered);
        }

        fn keys(&self) -> Vec<String> {
            self.0.lock().unwrap().clone()
        }
    }

    impl metrics::Recorder for KeyCapture {
        fn describe_counter(
            &self,
            _: metrics::KeyName,
            _: Option<metrics::Unit>,
            _: metrics::SharedString,
        ) {
        }
        fn describe_gauge(
            &self,
            _: metrics::KeyName,
            _: Option<metrics::Unit>,
            _: metrics::SharedString,
        ) {
        }
        fn describe_histogram(
            &self,
            _: metrics::KeyName,
            _: Option<metrics::Unit>,
            _: metrics::SharedString,
        ) {
        }
        fn register_counter(
            &self,
            key: &metrics::Key,
            _: &metrics::Metadata<'_>,
        ) -> metrics::Counter {
            self.push("counter", key);
            metrics::Counter::noop()
        }
        fn register_gauge(&self, key: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Gauge {
            self.push("gauge", key);
            metrics::Gauge::noop()
        }
        fn register_histogram(
            &self,
            key: &metrics::Key,
            _: &metrics::Metadata<'_>,
        ) -> metrics::Histogram {
            self.push("histogram", key);
            metrics::Histogram::noop()
        }
    }

    /// Run `f` with a capturing recorder installed on this thread and
    /// return the keys it registered.
    fn capture(f: impl FnOnce()) -> Vec<String> {
        let recorder = KeyCapture::new();
        metrics::with_local_recorder(&recorder, f);
        recorder.keys()
    }

    /// #577: the per-query metric keys. Every handler that moved to
    /// `QueryContext` still lands here with the same region id and the
    /// same endpoint literal, so this is the byte-level contract the
    /// `/metrics` scrape depends on.
    #[test]
    fn record_query_emits_the_documented_keys_and_labels() {
        let keys = capture(|| record_query("BE", "table", 0.25));
        assert_eq!(
            keys,
            vec![
                r#"counter butterfly_route_query_total{region="BE",endpoint="table"}"#.to_string(),
                r#"histogram butterfly_route_query_duration_seconds{region="BE",endpoint="table"}"#
                    .to_string(),
            ]
        );
    }

    /// The endpoint label is data, not a closed enum at the emit site:
    /// `/isochrone/bulk` and `/catchment` label themselves with values
    /// outside [`ENDPOINTS`] and must keep reaching the exporter.
    #[test]
    fn record_query_carries_endpoint_labels_outside_the_endpoints_constant() {
        for endpoint in ["isochrone_bulk", "catchment"] {
            assert!(
                !ENDPOINTS.contains(&endpoint),
                "{endpoint} is in ENDPOINTS now — update this test",
            );
            let keys = capture(|| record_query("BE", endpoint, 0.5));
            assert_eq!(
                keys,
                vec![
                    format!(
                        r#"counter butterfly_route_query_total{{region="BE",endpoint="{endpoint}"}}"#
                    ),
                    format!(
                        r#"histogram butterfly_route_query_duration_seconds{{region="BE",endpoint="{endpoint}"}}"#
                    ),
                ]
            );
        }
    }

    /// The cross-region rejection counter — bumped by the dispatcher,
    /// not by the handlers, and therefore untouched by #577.
    #[test]
    fn record_cross_region_reject_emits_the_documented_key_and_labels() {
        let keys = capture(|| record_cross_region_reject("BE", "LU"));
        assert_eq!(
            keys,
            vec![
                r#"counter butterfly_route_query_cross_region_total{src="BE",dst="LU"}"#
                    .to_string()
            ]
        );
    }

    /// The pre-created per-region handles must key exactly like the
    /// macro path in [`record_query`] — otherwise the same query
    /// counted through one route would land on a different time series
    /// than through the other.
    #[test]
    fn pre_created_handles_key_identically_to_record_query() {
        for ep in ENDPOINTS {
            let via_handles = capture(|| {
                let m = RegionMetrics::new("BE");
                m.record(ep, 0.25);
            });
            let via_macros = capture(|| record_query("BE", ep, 0.25));
            for expected in &via_macros {
                assert!(
                    via_handles.contains(expected),
                    "endpoint {ep}: handle path never registered {expected}; registered {via_handles:?}",
                );
            }
        }
    }

    /// The per-region size gauges keep their single `region` label.
    #[test]
    fn register_region_size_emits_the_documented_keys_and_labels() {
        let keys = capture(|| register_region_size("BE", 1, 2));
        assert_eq!(
            keys,
            vec![
                r#"gauge butterfly_route_region_nodes_total{region="BE"}"#.to_string(),
                r#"gauge butterfly_route_region_edges_total{region="BE"}"#.to_string(),
            ]
        );
    }

    #[test]
    fn region_metrics_pre_creates_handle_per_endpoint() {
        let m = RegionMetrics::new("BE");
        for ep in ENDPOINTS {
            assert!(
                m.query_total.contains_key(ep),
                "missing query_total handle for endpoint '{}'",
                ep
            );
            assert!(
                m.query_duration.contains_key(ep),
                "missing query_duration handle for endpoint '{}'",
                ep
            );
        }
        assert_eq!(m.region, "BE");
    }

    #[test]
    fn region_metrics_record_unknown_endpoint_is_silent() {
        // Caller passing an endpoint outside ENDPOINTS is a bug at the
        // call site, but the metric path must not panic — it's run on
        // the hot serve path under load.
        let m = RegionMetrics::new("BE");
        m.record("does-not-exist", 0.001); // no panic, no observation
    }
}
