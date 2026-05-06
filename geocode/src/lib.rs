//! butterfly-geocode — geocoder for the butterfly-osm toolkit.
//!
//! ## Architecture (#205)
//!
//! Two retrieval steps, no parse intermediate:
//!
//! 1. [`geocoder::recall`] — FST descent over per-country recall
//!    indexes. Cheap deterministic priors (postcode regex, country
//!    classifier, script detection) gate which country FSTs to
//!    descend into; tagger BIO logits weight prefix expansion.
//! 2. [`geocoder::rerank`] — GBDT scoring over recall candidates.
//!    Trained on perturbed OA gold + OSM-derived synthetic queries
//!    + bench query mix.
//!
//! [`geocoder::executor::recall_then_rerank`] is the orchestrator
//! the HTTP handler calls.
//!
//! ## #96/#97/#98 invariants — recast in retrieval terms
//!
//! - **Recombination invariant** → canonical-form indexing in the
//!   recall FST (duplicate addresses live at the same key).
//! - **Role-smoothness guarantee** → calibrated continuous GBDT
//!   scoring; no hard thresholds in either recall or rerank.
//! - **Zero-cost-on-clean-queries** → strong-prior queries hit
//!   recall at O(1) FST descent, no allocation, no model dispatch
//!   in the hot path. Reranker degrades to recall-score ordering
//!   when no model is loaded.
//! - **Cross-border shard co-location** → per-country recall FSTs
//!   in border clusters laid out contiguously on disk.
//! - **Country routing as first-class stage** → cheap classifier +
//!   neural fallback feed recall as the country prior.

#![deny(unsafe_code)]
#![deny(missing_debug_implementations)]

pub mod confidence;
pub mod control;
pub mod geocoder;
pub mod index;
pub mod osm_extract;
pub mod parser;
pub mod routing;
pub mod server;
pub mod shard;
pub mod sources;
pub mod tagger;
pub mod types;

pub use confidence::{Confidence, ConfidenceConfig, GbdtModel};
pub use geocoder::executor::{GeocodedResult, recall_then_rerank};
pub use geocoder::recall::{Candidate, N_BIO_LABELS, RecallBudget, Recaller, TaggerSignals};
pub use geocoder::rerank::{RankedResult, RerankFeatures, Reranker, SourcePriors};
pub use index::{BuildOptions, BuildReport, RecallIndex, ShardRecallStats, build_recall_index};
pub use parser::neural::NeuralParser;
pub use parser::{HeuristicBackend, NeuralBackend, ParserBackend};
pub use routing::{CountryId, classify_country, country_for_point, supported_countries_for_point};
pub use shard::reader::Shard;
pub use shard::{AddressRecord, SourceTag};
pub use sources::{
    Source, SourceProgress, merge_records, openaddresses::OpenAddressesSource, osm::OsmPbfSource,
};
