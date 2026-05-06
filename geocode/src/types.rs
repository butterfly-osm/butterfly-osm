//! Public type contracts (#205).
//!
//! The legacy parse-then-geocode shape (`ParsedQuery`,
//! `ParseHypothesis`, `RetrievalPolicy`, `Channel`, `ChannelRole`,
//! `FieldMask`, `Strictness`, `RecoveryFlags`) was deleted. The
//! public boundary today is:
//!
//! - [`crate::geocoder::recall::TaggerSignals`] — soft priors emitted
//!   by the tagger.
//! - [`crate::geocoder::recall::Candidate`] — recall output.
//! - [`crate::geocoder::rerank::RankedResult`] — final ranked output.

// This module is intentionally empty; it exists as a stable
// re-export point in case downstream crates have `crate::types::...`
// imports they want to keep working through the transition. New
// public types live in `geocoder::recall` and `geocoder::rerank`.
