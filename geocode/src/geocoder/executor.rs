//! Thin orchestrator — recall + rerank, in that order (#205).
//!
//! Replaces the legacy multi-channel program executor (#96 §Geocoder)
//! with a two-step pipeline:
//!
//! 1. [`Recaller::query`](super::recall::Recaller) — FST descent with
//!    soft tagger priors.
//! 2. [`Reranker::rank`](super::rerank::Reranker) — GBDT scoring.
//!
//! There is no parse intermediate. The handler builds
//! `TaggerSignals` from whatever model is loaded (or leaves it
//! neutral when no model is configured) and threads them straight
//! through.

use std::sync::Arc;

use super::recall::{RecallBudget, Recaller, TaggerSignals};
use super::rerank::{RankedResult, Reranker};
use crate::routing::CountryId;
use crate::shard::reader::Shard;

/// Result emitted by the orchestrator. Backwards-compatible alias
/// for [`RankedResult`] — the legacy `GeocodedResult` type was tied
/// to the multi-channel executor and has been removed.
pub use super::rerank::RankedResult as GeocodedResult;

/// Run recall + rerank in sequence and return the top-K results.
///
/// `shard_for` is a closure that hands back the [`Shard`] for a given
/// country. The reranker uses it to materialise candidate records
/// for feature extraction + the response payload.
#[allow(clippy::too_many_arguments)]
pub fn recall_then_rerank<F>(
    input: &str,
    signals: &TaggerSignals,
    countries: &[CountryId],
    recaller: &Recaller,
    reranker: &Reranker,
    budget: &RecallBudget,
    shard_for: F,
    limit: usize,
) -> Vec<RankedResult>
where
    F: Fn(CountryId) -> Option<Arc<Shard>>,
{
    let candidates = recaller.query(input, signals, countries, budget);
    if candidates.is_empty() {
        return Vec::new();
    }
    let mut ranked = reranker.rank(input, signals, &candidates, shard_for);
    ranked.truncate(limit);
    ranked
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::build::{BuildOptions, build_recall_index};
    use crate::index::read::RecallIndex;
    use crate::shard::AddressRecord;
    use crate::shard::SourceTag;
    use crate::shard::builder::build_shard;
    use std::path::PathBuf;
    use tempfile::tempdir;

    fn small_shard() -> (tempfile::TempDir, PathBuf, Arc<Shard>, RecallIndex) {
        let dir = tempdir().unwrap();
        let p = dir.path().join("be.bfgs");
        let addrs = vec![
            AddressRecord {
                street: "Rue Wayez".into(),
                housenumber: "122".into(),
                postcode: "1070".into(),
                locality: "Anderlecht".into(),
                lat: 50.834,
                lon: 4.314,
                source: SourceTag::OpenAddresses,
                ..Default::default()
            },
            AddressRecord {
                street: "Grote Markt".into(),
                housenumber: "1".into(),
                postcode: "2000".into(),
                locality: "Antwerpen".into(),
                lat: 51.221,
                lon: 4.401,
                source: SourceTag::Osm,
                ..Default::default()
            },
        ];
        build_shard(&p, CountryId::BE, addrs).unwrap();
        let shard = Arc::new(Shard::open(&p).unwrap());
        build_recall_index(&p, &shard, &BuildOptions::default()).unwrap();
        let idx = RecallIndex::open(&p).unwrap();
        (dir, p, shard, idx)
    }

    #[test]
    fn end_to_end_no_model() {
        let (_d, _p, shard, idx) = small_shard();
        let mut recaller = Recaller::new();
        recaller.insert(CountryId::BE, idx);
        let reranker = Reranker::new_no_model();
        let signals = TaggerSignals {
            global_confidence: 1.0,
            ..Default::default()
        };
        let shard_for = {
            let shard = shard.clone();
            move |c: CountryId| -> Option<Arc<Shard>> {
                if c == CountryId::BE {
                    Some(shard.clone())
                } else {
                    None
                }
            }
        };
        let results = recall_then_rerank(
            "Rue Wayez 122 1070 Anderlecht",
            &signals,
            &[CountryId::BE],
            &recaller,
            &reranker,
            &RecallBudget::default(),
            shard_for,
            5,
        );
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].postcode, "1070");
        assert_eq!(results[0].source, SourceTag::OpenAddresses);
    }
}
