//! Recall index — per-country FST over canonical address keys (#205).
//!
//! ## Design
//!
//! For each shard we emit a `<base>.recall.fst` sidecar plus a
//! `<base>.recall.postings` payload:
//!
//! - **FST** (`fst::Map`): keys are normalized canonical address
//!   strings, values are 64-bit packed `(offset_words << 24) | count`.
//!   Built via [`fst::MapBuilder`] over sorted-unique keys.
//! - **Postings** (binary): little-endian `u32`s. Each posting encodes
//!   the BFGS record id in the low 31 bits and the [`SourceTag`] in
//!   the high bit (`0=OSM`, `1=OpenAddresses`).
//!
//! ## Canonical keys
//!
//! Every shard record contributes up to **two** keys:
//!
//! 1. *Address key* — `street + " " + housenumber + " " + postcode + " " + locality`.
//!    Empty fields collapse to a single space; the whole string is
//!    normalized via [`crate::parser::normalize`]. Always emitted.
//! 2. *Place key* — `locality` alone (skipped if equal to the address
//!    key once normalized). Lets place-name only queries resolve to
//!    locality centroids without falling all the way through the
//!    fuzzy fallback.
//!
//! ## Stats sidecar (`<base>.recall.stats.json`)
//!
//! Per-shard summary stats consumed by `RecallBudget::adapt_to_stats`
//! to size top-K caps adaptively per country: vocab size, average key
//! length, p50/p95 posting-list size.
//!
//! ## Determinism
//!
//! Keys are sorted lex-ascending before being fed to the FST builder
//! — the on-disk FST bytes are byte-for-byte deterministic for a given
//! input shard.

pub mod build;
pub mod read;
pub mod stats;

pub use build::{BuildOptions, BuildReport, build_recall_index};
pub use read::{Posting, RecallIndex};
pub use stats::ShardRecallStats;

/// File suffix appended to the shard base path for the FST.
pub const FST_EXT: &str = "recall.fst";
/// File suffix for the postings payload.
pub const POSTINGS_EXT: &str = "recall.postings";
/// File suffix for the JSON stats sidecar.
pub const STATS_EXT: &str = "recall.stats.json";

/// High-bit flag in a posting word indicating an OpenAddresses record.
/// Cleared bit means OSM.
pub const POSTING_OA_FLAG: u32 = 1 << 31;
/// Mask for the BFGS record id portion of a posting word.
pub const POSTING_ID_MASK: u32 = !POSTING_OA_FLAG;
