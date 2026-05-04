//! Query parser.
//!
//! ## What this module is
//!
//! Phase 0 baseline: a deterministic heuristic parser. Belgium-only
//! signals (postcode regex, leading/trailing numeric token), no model
//! inference. Single hypothesis, single country.
//!
//! ## What this module is NOT
//!
//! This is **NOT #98 Phase 1**. #98 Phase 1 is the retrieval-aware
//! beam search over byte-level transformer outputs. That work is
//! blocked on the trained transformer (#96 §Tagger). The
//! deterministic parser here is the baseline that #98 will eventually
//! replace, while keeping the same `ParsedQuery` output shape.

pub mod heuristic;
pub mod normalize;

pub use heuristic::parse_heuristic;
