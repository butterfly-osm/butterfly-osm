//! Wire format for Phase 2 corpus rows.
//!
//! A `Phase2Sample` is one (query, gold) pair. Multiple samples can
//! share the same `gold_record_id` — they are augmentations of the
//! same canonical query, all expected to retrieve the same gold
//! record. The shared `gold_record_id` is the **retrieval-success
//! invariant** the parser is supposed to learn (#96 §Shard-Agnostic
//! Augmentation).

use serde::{Deserialize, Serialize};

/// On-disk schema version for `Phase2Sample`. Bump when fields change.
pub const PHASE2_SAMPLE_SCHEMA_VERSION: u32 = 1;

/// Augmentation strategy used to produce `query`. Same family as
/// `corpus-gen/src/augment.rs` — kept independent because Phase 2
/// works on shard records (lat/lon known) rather than OSM addr-tagged
/// nodes, so the rendering set is slightly different.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AugmentationKind {
    /// `street housenumber postcode locality` — the canonical Belgian
    /// rendering when the country's postcode goes after the street.
    Canonical,
    /// `postcode locality street housenumber`.
    PostcodeFirst,
    /// `street housenumber locality` — postcode dropped.
    DropPostcode,
    /// `street housenumber postcode` — locality dropped.
    DropLocality,
    /// Apply abbreviation contraction: `Rue` → `R.`, `Boulevard` → `Bd`,
    /// etc.
    AbbrContract,
    /// Apply abbreviation expansion: `R.` → `Rue`, `Bd.` → `Boulevard`.
    AbbrExpand,
    /// All-uppercase.
    UpperCase,
    /// All-lowercase.
    LowerCase,
    /// Whitespace and punctuation noise (multiple spaces, comma
    /// permutations).
    WhitespaceNoise,
    /// One ASCII typo injected in the street.
    Typo,
}

impl AugmentationKind {
    /// Stable string tag for JSONL serialisation + provenance traces.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            AugmentationKind::Canonical => "canonical",
            AugmentationKind::PostcodeFirst => "postcode_first",
            AugmentationKind::DropPostcode => "drop_postcode",
            AugmentationKind::DropLocality => "drop_locality",
            AugmentationKind::AbbrContract => "abbr_contract",
            AugmentationKind::AbbrExpand => "abbr_expand",
            AugmentationKind::UpperCase => "upper_case",
            AugmentationKind::LowerCase => "lower_case",
            AugmentationKind::WhitespaceNoise => "ws_noise",
            AugmentationKind::Typo => "typo",
        }
    }
}

/// One row in the Phase 2 corpus JSONL.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Phase2Sample {
    /// Schema version. Validated on load.
    pub schema_version: u32,
    /// Free-text query the parser sees.
    pub query: String,
    /// Shard record id. The labeler verifies the executor's results
    /// against this id (and against `gold_lat/lon` when matching by
    /// distance is needed).
    pub gold_record_id: u32,
    pub gold_lat: f64,
    pub gold_lon: f64,
    /// Optional: gold housenumber, kept so the labeler can match
    /// stricter than just `record_id` when a record's id was reused
    /// across versions of the shard (rare but possible during
    /// dedup-merges).
    pub gold_housenumber: Option<String>,
    /// Augmentation that produced this query. `Canonical` is emitted
    /// once per gold record; the others can repeat.
    pub augmentation: AugmentationKind,
    /// ISO 3166-1 alpha-2 country code (for clean dispatch on the
    /// labeler side).
    pub country: String,
}
