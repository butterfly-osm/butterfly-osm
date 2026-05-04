//! butterfly-geocode — geocoder for the butterfly-osm toolkit.
//!
//! ## Status: MVP (Belgium-only, deterministic)
//!
//! Phase 0 baseline of the architecture in
//! [butterfly-osm#96](https://github.com/butterfly-osm/butterfly-osm/issues/96).
//! The architectural type contracts (`ParsedQuery`, `ParseHypothesis`,
//! `ExecutionBudget`, `Channel`, `ChannelRole`, `RetrievalPolicy`,
//! retrieval operators) are all implemented to the design spec so
//! that future phases (#97 execution control, #98 neural parser)
//! extend cleanly without churning the public surface.
//!
//! What ships:
//!
//! - Belgium address shard built from OSM `addr:*` tags
//! - Deterministic heuristic parser (regex postcode + numeric house
//!   extraction + remainder-as-street). Single hypothesis, single
//!   country. **This is NOT #98 Phase 1** — that is the transformer
//!   path. This is the deterministic baseline that #98's beam search
//!   replaces once the transformer is trained.
//! - Multi-channel executor with `lookup`, `intersect`, `cap`,
//!   `score` operators, the canonicalization-based **Recombination
//!   Invariant**, and the **Zero-Cost-on-Clean-Queries** NFR (the
//!   |hypotheses|==1 path does not re-canonicalize, dedup, or score
//!   estimate).
//! - REST API: `GET /geocode` (forward), `GET /geocode/reverse`,
//!   `GET /health`, `GET /metrics` with content negotiation via the
//!   `Accept` header (`application/json` default,
//!   `application/geo+json` for the GeoJSON variant) per the
//!   project's API design preference (Sirius Insight pattern).
//!
//! What's deferred (tracked in #96/#97/#98):
//!
//! - Byte-level transformer parser (#96 §Tagger, #98 Phase 2)
//! - GBDT confidence reranker (#96 §Confidence Model)
//! - Multi-country routing (#96 §Country Routing) — the `CountryId`
//!   enum is `non_exhaustive` for extension; only `BE` is wired.
//! - Cross-border shard co-location (#96 §Cross-Border Shard
//!   Co-location)
//! - Feedback operators (`Downgrade`, `TopkMerge`, `Sample`) — types
//!   defined per #96 but not invoked by the MVP executor.
//! - Admission-control fanout caps (#97 §5)

#![deny(unsafe_code)]
#![deny(missing_debug_implementations)]

pub mod confidence;
pub mod control;
pub mod geocoder;
pub mod osm_extract;
pub mod parser;
pub mod routing;
pub mod server;
pub mod shard;
pub mod types;

pub use confidence::{Confidence, ConfidenceConfig, Features, GbdtModel};
pub use geocoder::executor::{GeocodedResult, execute, execute_with_rerank};
pub use parser::heuristic::parse_heuristic;
pub use routing::{CountryId, classify_country};
pub use shard::reader::Shard;
pub use types::{
    ExecutionBudget, FieldMask, ParseHypothesis, ParsedQuery, RecoveryFlags, RetrievalPolicy,
    Strictness,
};
