//! Fanout safeguards (#97 §5).
//!
//! Two classes of caps:
//!
//! ## Sequential fanout (per-query absolute ceilings)
//!
//! - `max_total_candidates` — already enforced in
//!   [`crate::geocoder::executor`]. The runtime tracker here exposes
//!   an explicit verdict so the executor can short-circuit and emit
//!   the right metric.
//! - `max_query_time_ms` — absolute wall-clock cap.
//! - `max_fuzzy_expansion_depth` — bound on `Strictness::Fuzzy` width.
//! - Strict-to-broad escalation only — the executor already does this;
//!   the config carries the boolean for completeness.
//!
//! ## Multi-channel / parallel fanout (#97 §5)
//!
//! - `max_field_channels_per_hypothesis`
//! - `max_parallel_retrieval_tasks_per_query`
//! - `max_posting_list_size_for_blocker` — past this, a Blocker must
//!   downgrade to Scorer (#96 Role-Smoothness).
//! - `max_hard_intersections_per_hypothesis`
//! - `max_blocker_empty_downgrades_per_query`
//! - `parallel_channel_concurrency_per_query`
//! - `max_feedback_operator_firings_per_query`
//!
//! Defaults are tuned to never fire on the Belgium MVP clean path,
//! but to clamp the worst pathological cases (`"123 Rue de la Gare"`
//! cross-shard).

use std::sync::atomic::{AtomicU32, Ordering};

/// Tunable fanout caps consumed at runtime by the executor.
///
/// All knobs default to values calibrated for Belgium MVP. Each
/// field documents its valid range and rationale.
#[derive(Debug, Clone, Copy)]
pub struct FanoutConfig {
    // -- Sequential ----------------------------------------------------
    /// Absolute candidate ceiling per query, regardless of budget tier.
    /// Range: 100 - 100_000. Default: 5_000.
    pub max_total_candidates: u32,
    /// Wall-clock cap per query, in milliseconds.
    /// Range: 10 - 60_000. Default: 500.
    pub max_query_time_ms: u32,
    /// Maximum number of fuzzy expansions emitted before aborting.
    /// Range: 1 - 1024. Default: 64.
    pub max_fuzzy_expansion_depth: u16,
    /// Whether the executor must escalate strict → broad rather than
    /// running broad-first. Default: true (mandatory per #97 §5).
    pub require_strict_to_broad_escalation: bool,
    /// Whether high-confidence strict hits short-circuit further
    /// fuzzy expansion. Default: true.
    pub early_terminate_on_high_confidence_strict: bool,

    // -- Multi-channel / parallel -------------------------------------
    /// Maximum field channels queried per hypothesis.
    /// Range: 1 - 6. Default: 4.
    pub max_field_channels_per_hypothesis: u8,
    /// Maximum concurrent retrieval tasks per query.
    /// Range: 1 - 32. Default: 6.
    pub max_parallel_retrieval_tasks_per_query: u8,
    /// Posting-list size at or above which a Blocker channel must
    /// downgrade to Scorer (Role-Smoothness, #96).
    /// Range: 100 - 10_000_000. Default: 100_000.
    pub max_posting_list_size_for_blocker: u32,
    /// Maximum hard intersections stacked per hypothesis before the
    /// remaining evidence flips to soft scoring.
    /// Range: 1 - 16. Default: 4.
    pub max_hard_intersections_per_hypothesis: u8,
    /// Maximum number of blocker-empty downgrades before the entire
    /// query is aborted (silent whole-shard scan would otherwise
    /// occur).
    /// Range: 1 - 64. Default: 6.
    pub max_blocker_empty_downgrades_per_query: u8,
    /// Hard cap on parallel-channel concurrency (worker tasks) per
    /// query.
    /// Range: 1 - 64. Default: 8.
    pub parallel_channel_concurrency_per_query: u8,
    /// Maximum number of feedback-operator firings (Downgrade /
    /// retry) before the query is aborted. Catches runaway
    /// staged-cost blowups.
    /// Range: 1 - 64. Default: 8.
    pub max_feedback_operator_firings_per_query: u8,
}

impl Default for FanoutConfig {
    fn default() -> Self {
        Self {
            max_total_candidates: 5_000,
            max_query_time_ms: 500,
            max_fuzzy_expansion_depth: 64,
            require_strict_to_broad_escalation: true,
            early_terminate_on_high_confidence_strict: true,
            max_field_channels_per_hypothesis: 4,
            max_parallel_retrieval_tasks_per_query: 6,
            max_posting_list_size_for_blocker: 100_000,
            max_hard_intersections_per_hypothesis: 4,
            max_blocker_empty_downgrades_per_query: 6,
            parallel_channel_concurrency_per_query: 8,
            max_feedback_operator_firings_per_query: 8,
        }
    }
}

/// Verdict returned by [`FanoutTracker`] on each cap check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FanoutVerdict {
    /// Within all configured caps; proceed.
    Ok,
    /// `max_total_candidates` reached.
    CandidateCeiling,
    /// `max_query_time_ms` exceeded.
    Timeout,
    /// `max_blocker_empty_downgrades_per_query` exceeded.
    BlockerDowngradeStorm,
    /// `max_feedback_operator_firings_per_query` exceeded.
    FeedbackStorm,
    /// `max_hard_intersections_per_hypothesis` exceeded.
    IntersectStackTooDeep,
    /// `max_fuzzy_expansion_depth` exceeded.
    FuzzyDepthExceeded,
}

/// Per-query fanout state. Cheap to construct (a handful of atomics
/// + a [`std::time::Instant`]).
///
/// Atomics are used so the multi-channel parallel path can update the
/// counters from rayon worker threads without taking a lock — but
/// the executor MVP path is single-threaded, in which case the
/// atomics are equivalent to plain integers.
#[derive(Debug)]
pub struct FanoutTracker {
    config: FanoutConfig,
    started_at: std::time::Instant,
    candidates_used: AtomicU32,
    blocker_empty_downgrades: AtomicU32,
    feedback_firings: AtomicU32,
    fuzzy_depth: AtomicU32,
}

impl FanoutTracker {
    #[must_use]
    pub fn new(config: FanoutConfig) -> Self {
        Self {
            config,
            started_at: std::time::Instant::now(),
            candidates_used: AtomicU32::new(0),
            blocker_empty_downgrades: AtomicU32::new(0),
            feedback_firings: AtomicU32::new(0),
            fuzzy_depth: AtomicU32::new(0),
        }
    }

    #[must_use]
    pub fn config(&self) -> &FanoutConfig {
        &self.config
    }

    /// Add `n` candidates to the cumulative count.
    pub fn add_candidates(&self, n: u32) -> FanoutVerdict {
        let total = self.candidates_used.fetch_add(n, Ordering::Relaxed) + n;
        if total >= self.config.max_total_candidates {
            FanoutVerdict::CandidateCeiling
        } else {
            FanoutVerdict::Ok
        }
    }

    /// Record one blocker-empty downgrade.
    pub fn record_blocker_downgrade(&self) -> FanoutVerdict {
        let total = self
            .blocker_empty_downgrades
            .fetch_add(1, Ordering::Relaxed)
            + 1;
        if total > u32::from(self.config.max_blocker_empty_downgrades_per_query) {
            FanoutVerdict::BlockerDowngradeStorm
        } else {
            FanoutVerdict::Ok
        }
    }

    /// Record one feedback-operator firing (Downgrade / retry).
    pub fn record_feedback_firing(&self) -> FanoutVerdict {
        let total = self.feedback_firings.fetch_add(1, Ordering::Relaxed) + 1;
        if total > u32::from(self.config.max_feedback_operator_firings_per_query) {
            FanoutVerdict::FeedbackStorm
        } else {
            FanoutVerdict::Ok
        }
    }

    /// Add one fuzzy-expansion step.
    pub fn add_fuzzy(&self, n: u32) -> FanoutVerdict {
        let total = self.fuzzy_depth.fetch_add(n, Ordering::Relaxed) + n;
        if total > u32::from(self.config.max_fuzzy_expansion_depth) {
            FanoutVerdict::FuzzyDepthExceeded
        } else {
            FanoutVerdict::Ok
        }
    }

    /// Check the wall-clock deadline.
    #[must_use]
    pub fn check_timeout(&self) -> FanoutVerdict {
        if self.started_at.elapsed().as_millis() as u64 >= u64::from(self.config.max_query_time_ms)
        {
            FanoutVerdict::Timeout
        } else {
            FanoutVerdict::Ok
        }
    }

    /// Validate a stack depth against `max_hard_intersections_per_hypothesis`.
    #[must_use]
    pub fn check_intersect_depth(&self, depth: u8) -> FanoutVerdict {
        if depth > self.config.max_hard_intersections_per_hypothesis {
            FanoutVerdict::IntersectStackTooDeep
        } else {
            FanoutVerdict::Ok
        }
    }

    /// Validate posting-list size against the Blocker downgrade
    /// threshold. Returns `true` iff the channel may stay as a
    /// Blocker; `false` signals the caller should downgrade the
    /// channel to a weaker role.
    #[must_use]
    pub fn check_blocker_size(&self, posting_list_size: u32) -> bool {
        posting_list_size < self.config.max_posting_list_size_for_blocker
    }

    /// Validate channel-fanout against `max_field_channels_per_hypothesis`.
    #[must_use]
    pub fn check_channel_fanout(&self, n_channels: u8) -> bool {
        n_channels <= self.config.max_field_channels_per_hypothesis
    }

    /// Snapshot the per-query counters for metric emission.
    #[must_use]
    pub fn snapshot(&self) -> FanoutSnapshot {
        FanoutSnapshot {
            elapsed_ms: self.started_at.elapsed().as_millis() as u64,
            candidates_used: self.candidates_used.load(Ordering::Relaxed),
            blocker_empty_downgrades: self.blocker_empty_downgrades.load(Ordering::Relaxed),
            feedback_firings: self.feedback_firings.load(Ordering::Relaxed),
            fuzzy_depth: self.fuzzy_depth.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct FanoutSnapshot {
    pub elapsed_ms: u64,
    pub candidates_used: u32,
    pub blocker_empty_downgrades: u32,
    pub feedback_firings: u32,
    pub fuzzy_depth: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_sensible() {
        let c = FanoutConfig::default();
        assert!(c.max_total_candidates >= 1_000);
        assert!(c.max_query_time_ms >= 100);
        assert!(c.max_field_channels_per_hypothesis <= 6);
        assert!(c.require_strict_to_broad_escalation);
    }

    #[test]
    fn candidate_ceiling_fires() {
        let cfg = FanoutConfig {
            max_total_candidates: 100,
            ..FanoutConfig::default()
        };
        let t = FanoutTracker::new(cfg);
        assert_eq!(t.add_candidates(50), FanoutVerdict::Ok);
        assert_eq!(t.add_candidates(60), FanoutVerdict::CandidateCeiling);
    }

    #[test]
    fn blocker_storm_fires() {
        let cfg = FanoutConfig {
            max_blocker_empty_downgrades_per_query: 2,
            ..FanoutConfig::default()
        };
        let t = FanoutTracker::new(cfg);
        assert_eq!(t.record_blocker_downgrade(), FanoutVerdict::Ok);
        assert_eq!(t.record_blocker_downgrade(), FanoutVerdict::Ok);
        assert_eq!(
            t.record_blocker_downgrade(),
            FanoutVerdict::BlockerDowngradeStorm
        );
    }

    #[test]
    fn feedback_storm_fires() {
        let cfg = FanoutConfig {
            max_feedback_operator_firings_per_query: 1,
            ..FanoutConfig::default()
        };
        let t = FanoutTracker::new(cfg);
        assert_eq!(t.record_feedback_firing(), FanoutVerdict::Ok);
        assert_eq!(t.record_feedback_firing(), FanoutVerdict::FeedbackStorm);
    }

    #[test]
    fn intersect_depth_check() {
        let t = FanoutTracker::new(FanoutConfig::default());
        assert_eq!(t.check_intersect_depth(1), FanoutVerdict::Ok);
        assert_eq!(
            t.check_intersect_depth(99),
            FanoutVerdict::IntersectStackTooDeep
        );
    }

    #[test]
    fn fuzzy_depth_check() {
        let cfg = FanoutConfig {
            max_fuzzy_expansion_depth: 8,
            ..FanoutConfig::default()
        };
        let t = FanoutTracker::new(cfg);
        assert_eq!(t.add_fuzzy(4), FanoutVerdict::Ok);
        assert_eq!(t.add_fuzzy(4), FanoutVerdict::Ok);
        assert_eq!(t.add_fuzzy(1), FanoutVerdict::FuzzyDepthExceeded);
    }

    #[test]
    fn blocker_size_threshold_downgrades() {
        let cfg = FanoutConfig {
            max_posting_list_size_for_blocker: 100,
            ..FanoutConfig::default()
        };
        let t = FanoutTracker::new(cfg);
        assert!(t.check_blocker_size(50));
        assert!(!t.check_blocker_size(200));
    }

    #[test]
    fn channel_fanout_threshold() {
        let cfg = FanoutConfig {
            max_field_channels_per_hypothesis: 3,
            ..FanoutConfig::default()
        };
        let t = FanoutTracker::new(cfg);
        assert!(t.check_channel_fanout(3));
        assert!(!t.check_channel_fanout(4));
    }

    #[test]
    fn timeout_fires() {
        let cfg = FanoutConfig {
            max_query_time_ms: 0,
            ..FanoutConfig::default()
        };
        let t = FanoutTracker::new(cfg);
        std::thread::sleep(std::time::Duration::from_millis(2));
        assert_eq!(t.check_timeout(), FanoutVerdict::Timeout);
    }

    #[test]
    fn snapshot_reflects_state() {
        let t = FanoutTracker::new(FanoutConfig::default());
        let _ = t.add_candidates(7);
        let _ = t.record_feedback_firing();
        let s = t.snapshot();
        assert_eq!(s.candidates_used, 7);
        assert_eq!(s.feedback_firings, 1);
    }
}
