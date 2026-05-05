//! Confidence + GBDT reranking layer (#96 §Confidence Model).
//!
//! Three responsibilities:
//!
//! 1. **Feature extraction** ([`features`]) — convert
//!    `GeocodedResult`s into a fixed-shape numeric feature vector.
//! 2. **GBDT inference** ([`gbdt`]) — score candidates with a trained
//!    boosted-tree ensemble (~1.12 µs p50 per row).
//! 3. **Action thresholds** ([`thresholds`]) — turn a raw score into
//!    an `accept` / `caution` / `review` / `reject` action plus
//!    machine-readable reason codes.
//!
//! The training pipeline ([`training`]) is exposed through the
//! `butterfly-geocode train-rerank` CLI subcommand.
//!
//! ## No-model fallback
//!
//! When [`crate::server::ServerState::rerank_model`] is `None`, the
//! executor returns its raw scores untouched. The reranker is purely
//! additive — turning it on never *removes* candidates the executor
//! would have returned, except via the `Reject` threshold tier.

pub mod features;
pub mod gbdt;
pub mod thresholds;
pub mod training;

pub use features::{
    Features, FeaturesBatch, N_FEATURES, extract_features, extract_features_into,
    extract_features_into_per_hypothesis, extract_features_per_hypothesis,
};
pub use gbdt::{GbdtModel, rerank};
pub use thresholds::{
    Confidence, ConfidenceConfig, RC_BELOW_THRESHOLD, RC_COUNTRY_UNCERTAIN, RC_HIGH_CONFIDENCE,
    RC_LOW_CONFIDENCE, RC_POSTCODE_EXACT, RC_POSTCODE_MISMATCH, RC_RERANK_GBDT, RC_STREET_WEAK,
    apply_thresholds,
};
pub use training::{
    EvalReport, GoldAddress, LabeledQuery, POSITIVE_RADIUS_M, TrainConfig, build_training_groups,
    build_training_rows, dump_training_rows, evaluate, label_candidate, load_corpus,
    synthesise_corpus_from_shard, train_pointwise,
};
