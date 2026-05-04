//! Execution control plane (#97).
//!
//! Operational guardrails between the parser (heuristic today,
//! transformer in #98) and the executor:
//!
//! - [`budget`] — derive [`crate::types::ExecutionBudget`] from
//!   parser uncertainty + retrieval fanout + planned operator-tree
//!   static cost.
//! - [`admission`] — server-side admission control middleware
//!   (token-bucket rate limiting, queue, 429 with `Retry-After`).
//! - [`fanout`] — runtime fanout safeguards consumed by the executor.
//! - [`metrics_routing`] — first-class country-routing metrics.
//! - [`metrics_channels`] — channel + retrieval-program metrics
//!   (clean-query overhead canary, static-vs-feedback ratio).
//! - [`metrics_general`] — general per-query counters.
//!
//! ## Design
//!
//! Everything is **policy-as-data** — `BudgetPolicy`, `FanoutConfig`,
//! `AdmissionPolicy`, `MetricsThresholds` are plain `Copy` structs the
//! caller can build from defaults and tune per endpoint. No hidden
//! globals beyond the `metrics` crate's recorder (which the existing
//! Prometheus exporter already drives).
//!
//! ## Static-cost ceiling, two ways
//!
//! 1. The budget computation in [`budget::compute_budget`] writes
//!    `static_cost_ceiling` based on the planned cost.
//! 2. The executor entry calls [`budget::pre_execution_check`] which
//!    re-verifies — defense-in-depth per #97 §3.
//!
//! These are intentionally independent: a malformed `ParsedQuery`
//! (e.g. one constructed by an upstream that didn't run the budget)
//! still gets refused at admission/execution time.

pub mod admission;
pub mod budget;
pub mod fanout;
pub mod metrics_channels;
pub mod metrics_general;
pub mod metrics_routing;

pub use admission::{
    AdmissionError, AdmissionPolicy, AdmissionState, admission_middleware, mk_admission_layer,
};

// Re-export the type alias used by metrics emitters.
pub use budget::classify_tier;
pub use budget::{
    BudgetPolicy, BudgetTier, ParsedQueryStats, compute_budget, estimate_static_cost,
    pre_execution_check,
};
pub use fanout::{FanoutConfig, FanoutTracker, FanoutVerdict};
pub use metrics_channels::{
    ChannelMetrics, CleanQueryMetrics, CostCalibrationMetrics, MetricsAlertThresholds,
    RecombinationMetrics, RoleSmoothnessMetrics,
};
pub use metrics_general::GeneralMetrics;
pub use metrics_routing::{CountryRoutingMetrics, RoutingDirection, RoutingObservation};
