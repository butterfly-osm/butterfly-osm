//! Channel + retrieval-program metrics (#97 §7).
//!
//! Five sub-bundles, each with its own struct:
//!
//! - [`ChannelMetrics`] — channel behaviour (posting list sizes,
//!   hard-intersection success, downgrades)
//! - [`CostCalibrationMetrics`] — static / feedback cost ratio,
//!   feedback firing leaderboard
//! - [`RoleSmoothnessMetrics`] — dual-evaluation firing + divergence
//! - [`RecombinationMetrics`] — hypothesis-dedup collapse rate
//! - [`CleanQueryMetrics`] — Zero-Cost-on-Clean-Queries canary
//!
//! ## Zero-Cost canary (#96 NFR, #97 §7)
//!
//! `CleanQueryMetrics` emits two histograms and one gauge:
//!
//! - `geocode_cleanquery_overhead_seconds` (target: p99 ≤ 500 ns,
//!   p50 ≤ 100 ns)
//! - `geocode_cleanquery_alloc_count` (target: 0)
//! - `geocode_cleanquery_share` (gauge, fraction of traffic)
//!
//! The allocation count is captured per-call by the strict-allocator
//! wrapper used in tests (see
//! `tests/control_clean_query_alloc_test.rs`). At runtime the count
//! is reported via [`CleanQueryMetrics::record_clean`] from the
//! executor's clean-path entry/exit hooks; if a non-zero value ever
//! reaches the histogram, the alert (config keys in
//! [`MetricsAlertThresholds`]) fires.

use std::sync::atomic::{AtomicU64, Ordering};

use metrics::{counter, gauge, histogram};

/// Threshold knobs consumed downstream by an alerting layer.
#[derive(Debug, Clone, Copy)]
pub struct MetricsAlertThresholds {
    /// Alert if clean-query overhead p99 exceeds this duration.
    /// Default: 500 ns (#97 NFR target).
    pub clean_query_overhead_p99: std::time::Duration,
    /// Alert if any clean-query allocation count is reported above
    /// this threshold. Default: 0 (any heap allocation is a regression).
    pub clean_query_alloc_count_max: u64,
    /// Alert if static-vs-feedback cost ratio exceeds this value.
    /// Default: 2.0.
    pub static_vs_feedback_ratio_max: f64,
}

impl Default for MetricsAlertThresholds {
    fn default() -> Self {
        Self {
            clean_query_overhead_p99: std::time::Duration::from_nanos(500),
            clean_query_alloc_count_max: 0,
            static_vs_feedback_ratio_max: 2.0,
        }
    }
}

/// Channel-behaviour metrics.
#[derive(Debug, Clone, Copy, Default)]
pub struct ChannelMetrics;

impl ChannelMetrics {
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    pub fn record_posting_list_size(&self, channel: &'static str, country: &'static str, n: u32) {
        histogram!(
            "geocode_channel_posting_list_size",
            "channel" => channel,
            "country" => country,
        )
        .record(f64::from(n));
    }

    pub fn record_hard_intersection_success(&self, success: bool) {
        counter!(
            "geocode_channel_hard_intersection_total",
            "outcome" => if success { "success" } else { "downgrade" },
        )
        .increment(1);
    }

    pub fn record_fallback_to_soft_scoring(&self) {
        counter!("geocode_channel_fallback_to_soft_total").increment(1);
    }

    pub fn record_parallel_channel_count(&self, n: u8) {
        histogram!("geocode_channel_parallel_count").record(f64::from(n));
    }

    /// `reason` ∈ `empty_postings` | `oversized_postings` | `budget_exhausted`.
    pub fn record_downgrade_reason(&self, channel: &'static str, reason: &'static str) {
        counter!(
            "geocode_channel_downgrade_reason_total",
            "channel" => channel,
            "reason" => reason,
        )
        .increment(1);
    }
}

/// Cost-calibration metrics.
#[derive(Debug, Clone, Copy, Default)]
pub struct CostCalibrationMetrics;

impl CostCalibrationMetrics {
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    pub fn record_static_cost(&self, cost: f32) {
        histogram!("geocode_cost_static").record(f64::from(cost));
    }

    pub fn record_feedback_cost(&self, cost: f32) {
        histogram!("geocode_cost_feedback").record(f64::from(cost));
    }

    /// Ratio = observed feedback / pre-admission static estimate.
    /// Target ~1.0; ratio ≫ 1 = retrieval-program miscalibration.
    pub fn record_static_vs_feedback_ratio(&self, ratio: f64) {
        histogram!("geocode_cost_static_vs_feedback_ratio").record(ratio);
    }

    pub fn record_feedback_firing(&self, channel: &'static str, country: &'static str) {
        counter!(
            "geocode_cost_feedback_firing_total",
            "channel" => channel,
            "country" => country,
        )
        .increment(1);
    }
}

/// Role-smoothness metrics (#96 §Role-Smoothness Guarantee).
#[derive(Debug, Clone, Copy, Default)]
pub struct RoleSmoothnessMetrics;

impl RoleSmoothnessMetrics {
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    pub fn record_dual_evaluation_firing(&self) {
        counter!("geocode_role_dual_eval_firing_total").increment(1);
    }

    pub fn record_dual_evaluation_divergence(&self) {
        counter!("geocode_role_dual_eval_divergence_total").increment(1);
    }

    pub fn record_weak_preference_downgrade(&self) {
        counter!("geocode_role_weak_pref_downgrade_total").increment(1);
    }
}

/// Recombination metrics (#96 §Recombination Invariant).
#[derive(Debug, Clone, Copy, Default)]
pub struct RecombinationMetrics;

impl RecombinationMetrics {
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    /// `pre` = raw parser hypothesis count, `post` = post-canonical
    /// dedup. Emits both gauges for absolute counts plus the collapse
    /// rate.
    pub fn record_dedup(&self, pre: u32, post: u32) {
        histogram!("geocode_recomb_pre").record(f64::from(pre));
        histogram!("geocode_recomb_post").record(f64::from(post));
        let collapsed = pre.saturating_sub(post);
        let rate = if pre > 0 {
            f64::from(collapsed) / f64::from(pre)
        } else {
            0.0
        };
        histogram!("geocode_recomb_collapse_rate").record(rate);
    }
}

/// Clean-query overhead canary (#96 NFR, #97 §7).
///
/// Tracks cumulative counts so [`Self::share_gauge`] can be called
/// periodically to update the share-of-traffic gauge.
#[derive(Debug, Default)]
pub struct CleanQueryMetrics {
    clean_count: AtomicU64,
    total_count: AtomicU64,
}

impl CleanQueryMetrics {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Mark this query as clean-path; record overhead + (optional)
    /// allocation count.
    pub fn record_clean(&self, overhead: std::time::Duration, alloc_count: u64) {
        self.clean_count.fetch_add(1, Ordering::Relaxed);
        self.total_count.fetch_add(1, Ordering::Relaxed);
        histogram!("geocode_cleanquery_overhead_seconds").record(overhead.as_secs_f64());
        histogram!("geocode_cleanquery_alloc_count").record(alloc_count as f64);
        self.refresh_share();
    }

    /// Mark this query as non-clean-path. Updates the share gauge.
    pub fn record_non_clean(&self) {
        self.total_count.fetch_add(1, Ordering::Relaxed);
        self.refresh_share();
    }

    fn refresh_share(&self) {
        let total = self.total_count.load(Ordering::Relaxed);
        if total == 0 {
            return;
        }
        let clean = self.clean_count.load(Ordering::Relaxed);
        let share = clean as f64 / total as f64;
        gauge!("geocode_cleanquery_share").set(share);
    }

    /// Convenience accessor for tests / introspection.
    #[must_use]
    pub fn share(&self) -> f64 {
        let total = self.total_count.load(Ordering::Relaxed);
        if total == 0 {
            return 0.0;
        }
        let clean = self.clean_count.load(Ordering::Relaxed);
        clean as f64 / total as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn channel_metrics_emit() {
        let m = ChannelMetrics::new();
        m.record_posting_list_size("postcode", "BE", 64);
        m.record_hard_intersection_success(true);
        m.record_hard_intersection_success(false);
        m.record_fallback_to_soft_scoring();
        m.record_parallel_channel_count(3);
        m.record_downgrade_reason("postcode", "empty_postings");
    }

    #[test]
    fn cost_calibration_metrics_emit() {
        let m = CostCalibrationMetrics::new();
        m.record_static_cost(123.4);
        m.record_feedback_cost(45.6);
        m.record_static_vs_feedback_ratio(0.37);
        m.record_feedback_firing("postcode", "BE");
    }

    #[test]
    fn role_smoothness_metrics_emit() {
        let m = RoleSmoothnessMetrics::new();
        m.record_dual_evaluation_firing();
        m.record_dual_evaluation_divergence();
        m.record_weak_preference_downgrade();
    }

    #[test]
    fn recombination_collapse_rate() {
        let m = RecombinationMetrics::new();
        // 5 pre, 2 post → collapse 3/5 = 0.6
        m.record_dedup(5, 2);
        m.record_dedup(0, 0);
        m.record_dedup(1, 1);
    }

    #[test]
    fn clean_query_metrics_track_share() {
        let m = CleanQueryMetrics::new();
        m.record_clean(std::time::Duration::from_nanos(50), 0);
        m.record_clean(std::time::Duration::from_nanos(80), 0);
        m.record_non_clean();
        let s = m.share();
        assert!((s - 2.0 / 3.0).abs() < 1e-9, "share={s}");
    }

    #[test]
    fn alert_thresholds_have_strict_defaults() {
        let t = MetricsAlertThresholds::default();
        assert_eq!(t.clean_query_alloc_count_max, 0);
        assert_eq!(
            t.clean_query_overhead_p99,
            std::time::Duration::from_nanos(500)
        );
        assert!((t.static_vs_feedback_ratio_max - 2.0).abs() < 1e-9);
    }
}
