//! Prometheus metrics for lazy CRC verification (#160).
//!
//! Routed through the global `metrics` recorder installed by
//! `axum_prometheus::PrometheusMetricLayer::pair()` in
//! [`crate::server::api::build_router`]. The recorder's `/metrics`
//! handler renders the values with no extra wiring on our side.
//!
//! The metric names match the spec in #160:
//!
//! - `butterfly_route_sections_verified_total` (counter, no labels)
//! - `butterfly_route_sections_verify_pending`  (gauge, no labels)
//! - `butterfly_route_section_verify_duration_seconds` (histogram, label `section`)
//! - `butterfly_route_section_verify_failed_total` (counter, label `section`)
//!
//! Pending is incremented when a section enters the verifier queue
//! (manifest-load time) and decremented on a terminal transition. The
//! gauge therefore approximates "unverified sections in flight" — the
//! ops-meaningful number for an operator monitoring lazy verification
//! progress.

use std::sync::atomic::{AtomicI64, Ordering};

/// Pending count, mirrored separately so we can keep `gauge!` writes
/// free of inter-thread serialisation. The `metrics` crate's gauge API
/// requires an absolute value; we maintain the count locally and push
/// the new absolute value on every change.
static PENDING: AtomicI64 = AtomicI64::new(0);

/// Register `n_sections` with the gauge; called once when the manifest
/// is loaded. After this call, the gauge reflects the number of
/// sections that have not yet reached a terminal state.
pub fn register_pending(n_sections: usize) {
    PENDING.store(n_sections as i64, Ordering::Relaxed);
    metrics::gauge!("butterfly_route_sections_verify_pending").set(n_sections as f64);
}

/// Record a successful section verification. Decrements the pending
/// gauge, increments the `verified_total` counter, and observes the
/// per-section duration histogram.
pub fn record_section_verified(section: &str, duration_s: f64) {
    let prev = PENDING.fetch_sub(1, Ordering::Relaxed);
    let new = (prev - 1).max(0);
    metrics::gauge!("butterfly_route_sections_verify_pending").set(new as f64);
    metrics::counter!("butterfly_route_sections_verified_total").increment(1);
    metrics::histogram!(
        "butterfly_route_section_verify_duration_seconds",
        "section" => section.to_string()
    )
    .record(duration_s);
}

/// Record a failed section verification. Decrements pending and
/// increments the per-section failure counter. The duration histogram
/// is intentionally NOT updated: a verification that did not produce
/// trustworthy bytes shouldn't pollute the success-distribution.
pub fn record_section_failed(section: &str) {
    let prev = PENDING.fetch_sub(1, Ordering::Relaxed);
    let new = (prev - 1).max(0);
    metrics::gauge!("butterfly_route_sections_verify_pending").set(new as f64);
    metrics::counter!(
        "butterfly_route_section_verify_failed_total",
        "section" => section.to_string()
    )
    .increment(1);
}

/// Snapshot of the pending count for tests / `/health`.
pub fn pending_count() -> i64 {
    PENDING.load(Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pending_register_then_record_decrements_to_zero() {
        // The global recorder may not be installed in unit-test
        // mode — that's fine; the metrics macros are no-ops when no
        // recorder is set, but our local `PENDING` counter must still
        // track correctly.
        PENDING.store(0, Ordering::Relaxed);
        register_pending(3);
        assert_eq!(pending_count(), 3);
        record_section_verified("a", 0.001);
        assert_eq!(pending_count(), 2);
        record_section_failed("b");
        assert_eq!(pending_count(), 1);
        record_section_verified("c", 0.002);
        assert_eq!(pending_count(), 0);
    }
}
