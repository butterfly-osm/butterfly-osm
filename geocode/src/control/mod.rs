//! Server execution-control plane (#205 simplified).
//!
//! What survives from the #97 control plane:
//!
//! - [`admission`] — server-side admission control middleware
//!   (token-bucket rate limiting, queue, 429 with `Retry-After`).
//!   Independent of the retrieval shape; carried over unchanged.
//! - [`metrics_general`] — general per-query counters (Prometheus
//!   exporter feeds off these).
//! - [`metrics_routing`] — first-class country-routing metrics.
//!
//! Deleted in #205 (channel-execution-bound, no longer apply):
//!
//! - `budget` — derived `ExecutionBudget` from `ParsedQuery`. The
//!   recall + rerank pipeline uses
//!   [`crate::geocoder::recall::RecallBudget`] instead.
//! - `fanout` — runtime fanout safeguards consumed by the legacy
//!   executor.
//! - `metrics_channels` — channel + retrieval-program metrics.

pub mod admission;
pub mod metrics_general;
pub mod metrics_routing;

pub use admission::{
    AdmissionError, AdmissionPolicy, AdmissionState, admission_middleware, mk_admission_layer,
};
pub use metrics_general::GeneralMetrics;
pub use metrics_routing::{CountryRoutingMetrics, RoutingDirection, RoutingObservation};
