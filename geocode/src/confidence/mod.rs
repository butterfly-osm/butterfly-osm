//! Confidence + GBDT inference layer (#205).
//!
//! - [`gbdt`] — generic GBDT inference wrapper used by the rerank
//!   step. Loads / saves boosted-tree ensembles produced by the
//!   `gbdt` crate.
//! - [`thresholds`] — `accept` / `caution` / `review` / `reject`
//!   tier mapping + machine-readable reason codes. Vocabulary is
//!   stable across the post-libpostal redesign so existing client
//!   code that branches on confidence does not break.
//!
//! The legacy `features` module (14-feature schema bound to the
//! channel-execution executor) and `training` module (parse-success
//! corpora) were deleted in #205. The replacement training pipeline
//! for the rerank GBDT lives in
//! [`crate::geocoder::rerank::training`](super::geocoder::rerank).

pub mod gbdt;
pub mod thresholds;

pub use gbdt::GbdtModel;
pub use thresholds::{
    Confidence, ConfidenceConfig, RC_BELOW_THRESHOLD, RC_COUNTRY_UNCERTAIN, RC_HIGH_CONFIDENCE,
    RC_LOW_CONFIDENCE, RC_POSTCODE_EXACT, RC_POSTCODE_MISMATCH, RC_RERANK_GBDT, RC_STREET_WEAK,
};
