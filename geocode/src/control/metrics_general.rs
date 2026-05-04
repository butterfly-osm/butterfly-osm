//! General per-query monitoring (#97 §8).
//!
//! Counters and histograms surfaced at `/metrics` (Prometheus, via
//! `metrics-exporter-prometheus` already wired in `server/mod.rs`).
//!
//! Names use the `geocode_*` prefix to namespace cleanly off
//! butterfly-route's own metrics on the same exporter.
//!
//! # Metric vocabulary
//!
//! | Name | Type | Description |
//! |------|------|-------------|
//! | `geocode_admission_admitted_total` | counter | Requests admitted |
//! | `geocode_admission_rejected_total` | counter | Requests rejected (429) |
//! | `geocode_query_candidates` | histogram | Final candidate count emitted |
//! | `geocode_query_countries_explored` | histogram | Countries probed per query |
//! | `geocode_query_hypotheses_pre_dedup` | histogram | Hypotheses before recombination |
//! | `geocode_query_hypotheses_post_dedup` | histogram | Hypotheses after recombination |
//! | `geocode_query_budget_exhaustion_total` | counter | Queries that hit `max_total_candidates` |
//! | `geocode_query_per_country_fanout` | histogram | Candidates per country pair (label `country`) |
//! | `geocode_query_tier_total` | counter | Per-tier admission count (label `tier`) |

use metrics::{counter, histogram};

/// Cheap-clone bundle of the general counters / histograms.
///
/// All emission goes through this struct so the call sites do not
/// scatter `metric!()` macros across the codebase.
#[derive(Debug, Clone, Copy, Default)]
pub struct GeneralMetrics;

impl GeneralMetrics {
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    pub fn record_admitted(&self) {
        counter!("geocode_admission_admitted_total").increment(1);
    }

    pub fn record_rejected(&self) {
        counter!("geocode_admission_rejected_total").increment(1);
    }

    pub fn record_candidates(&self, n: u32) {
        histogram!("geocode_query_candidates").record(f64::from(n));
    }

    pub fn record_countries_explored(&self, n: u32) {
        histogram!("geocode_query_countries_explored").record(f64::from(n));
    }

    pub fn record_hypotheses(&self, pre: u32, post: u32) {
        histogram!("geocode_query_hypotheses_pre_dedup").record(f64::from(pre));
        histogram!("geocode_query_hypotheses_post_dedup").record(f64::from(post));
    }

    pub fn record_budget_exhaustion(&self) {
        counter!("geocode_query_budget_exhaustion_total").increment(1);
    }

    pub fn record_per_country_fanout(&self, country: &'static str, fanout: u32) {
        histogram!(
            "geocode_query_per_country_fanout",
            "country" => country,
        )
        .record(f64::from(fanout));
    }

    pub fn record_tier(&self, tier: &'static str) {
        counter!(
            "geocode_query_tier_total",
            "tier" => tier,
        )
        .increment(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_emit_without_panic() {
        let m = GeneralMetrics::new();
        m.record_admitted();
        m.record_rejected();
        m.record_candidates(7);
        m.record_countries_explored(2);
        m.record_hypotheses(3, 1);
        m.record_budget_exhaustion();
        m.record_per_country_fanout("BE", 42);
        m.record_tier("tight");
    }
}
