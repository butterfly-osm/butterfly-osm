//! butterfly-geocode — geocoder for the butterfly-osm toolkit.
//!
//! ## Status: Belgium MVP + neural parser scaffold
//!
//! Implements the architecture in
//! [butterfly-osm#96](https://github.com/butterfly-osm/butterfly-osm/issues/96)
//! and Phase 1 of
//! [#98](https://github.com/butterfly-osm/butterfly-osm/issues/98)
//! (retrieval-aware decoding).
//!
//! Two parser backends ship side-by-side:
//!
//! - [`parser::heuristic`] — deterministic regex-driven baseline (Phase
//!   0 from PR #162). Single hypothesis, single country. Always
//!   available, no model file required.
//! - [`parser::neural`] — byte-level transformer ([`tagger`]) +
//!   retrieval-aware decoding ([`parser::decoding`]) implementing all
//!   five sub-deliverables of #98 Phase 1: canonicalization-based
//!   recombination, adaptive beam, country-router prior, anchor
//!   pruning with role-smoothness, and retrieval-utility scoring.
//!
//! The architectural type contracts (`ParsedQuery`, `ParseHypothesis`,
//! `ExecutionBudget`, `Channel`, `ChannelRole`, `RetrievalPolicy`,
//! retrieval operators) are common to both backends. The
//! [`parser::ParserBackend`] trait dispatches between them at runtime;
//! a missing model file falls back to the heuristic backend with a
//! warning.
//!
//! ## #96 invariants honored
//!
//! - **Recombination Invariant**: parser-side enforced in
//!   [`parser::decoding::decode`] which canonicalizes every program
//!   and dedups by canonical form before emitting `ParsedQuery`.
//! - **Role-Smoothness Guarantee**: anchor pruning downweights within
//!   ε of the boundary instead of hard-thresholding (see
//!   [`parser::anchor::ANCHOR_EPSILON`]).
//! - **Zero-Cost-on-Clean-Queries**: the `|hypotheses|==1` path in the
//!   executor still skips canonicalization, dedup, and dynamic
//!   dispatch — the neural parser produces a multi-hypothesis output
//!   so it takes the fully-canonicalizing path, but a heuristic-parsed
//!   single-hypothesis query is still O(1).
//!
//! ## What's deferred (tracked in #96/#97/#98)
//!
//! - **#98 Phase 2 (learned decoding objective)** — explicitly blocked
//!   on a labeled corpus; the spec itself defers it.
//! - **Production-quality trained model** — the shipped tiny model is
//!   a proof-of-life that the training loop converges and inference is
//!   wired correctly. A real model needs the #96 §Tagger
//!   shard-agnostic augmentation strategy.
//! - **GBDT confidence reranker** (#96 §Confidence Model)
//! - **Multi-country routing** (#96 §Country Routing) — the `CountryId`
//!   enum is `non_exhaustive` for extension; only `BE` is wired.
//! - **Cross-border shard co-location** (#96 §Cross-Border Shard
//!   Co-location)
//! - **Feedback operators** (`Downgrade`, `TopkMerge`, `Sample`) —
//!   types defined per #96 but not invoked by the MVP executor.
//! - **Admission-control fanout caps** (#97 §5)
//! - **LoRA / regional adapters** — hooks noted in #96 §Tagger.

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
pub mod sources;
pub mod tagger;
pub mod types;

pub use confidence::{Confidence, ConfidenceConfig, Features, GbdtModel};
pub use geocoder::executor::{GeocodedResult, execute, execute_across_shards, execute_with_rerank};
pub use parser::decoding::build_program_for_hypothesis;
pub use parser::heuristic::{parse_heuristic, parse_with_classifier};
pub use parser::neural::NeuralParser;
pub use parser::phase2_features::{
    AnchorSummary as Phase2AnchorSummary, BeamStats as Phase2BeamStats, Features as Phase2Features,
    ProgramFeatures as Phase2ProgramFeatures,
};
pub use parser::phase2_training::{
    EvalReport as Phase2EvalReport, LabeledRow as Phase2LabeledRow,
    TrainConfig as Phase2TrainConfig, evaluate as phase2_evaluate,
    load_labels as phase2_load_labels, save_labels as phase2_save_labels,
    split_train_eval as phase2_split_train_eval, train_pointwise as phase2_train_pointwise,
};
pub use parser::retrieval_utility::{HeuristicScorer, LearnedScorer, RetrievalUtilityScorer};
pub use parser::{HeuristicBackend, NeuralBackend, ParserBackend};
pub use routing::{CountryId, classify_country, country_for_point, supported_countries_for_point};
pub use shard::reader::Shard;
pub use shard::{AddressRecord, SourceTag};
pub use sources::{Source, SourceProgress, bosa::BosaCsvSource, merge_records, osm::OsmPbfSource};
pub use types::{
    ExecutionBudget, FieldMask, ParseHypothesis, ParsedQuery, RecoveryFlags, RetrievalPolicy,
    Strictness,
};
