//! End-to-end smoke tests against a Belgium shard (#205).
//!
//! These tests are `#[ignore]` by default because they require:
//!
//! 1. A built shard at `regions/belgium.bfgs` with sibling
//!    `regions/belgium.recall.fst` / `.recall.postings` /
//!    `.recall.stats.json` — the `build-shard` command emits all of
//!    them in one pass since #205.
//!
//! Run with:
//!
//! ```text
//! cargo test -p butterfly-geocode --release --test belgium_e2e -- --ignored
//! ```

use std::sync::Arc;

use butterfly_geocode::CountryId;
use butterfly_geocode::geocoder::executor::recall_then_rerank;
use butterfly_geocode::geocoder::recall::{RecallBudget, Recaller, TaggerSignals};
use butterfly_geocode::geocoder::rerank::Reranker;
use butterfly_geocode::index::RecallIndex;
use butterfly_geocode::shard::reader::Shard;

const SHARD_PATH: &str = "regions/belgium.bfgs";

fn open_setup() -> (Arc<Shard>, Recaller, Reranker) {
    let shard = Arc::new(Shard::open(SHARD_PATH).unwrap_or_else(|e| {
        panic!(
            "could not open {SHARD_PATH}: {e}\n\
             Build it first: cargo run --release -p butterfly-geocode -- build-shard \
             --pbf data/belgium.pbf --out geocode/regions/belgium.bfgs --country BE"
        );
    }));
    let idx = RecallIndex::open(SHARD_PATH.as_ref())
        .unwrap_or_else(|e| panic!("could not open recall index: {e}"));
    let mut recaller = Recaller::new();
    recaller.insert(CountryId::BE, idx);
    let reranker = Reranker::new_no_model();
    (shard, recaller, reranker)
}

#[test]
#[ignore]
fn recall_returns_candidates_for_known_address() {
    let (shard, recaller, reranker) = open_setup();
    let signals = TaggerSignals::default();
    let budget = RecallBudget::default();
    let shard_for = {
        let s = shard.clone();
        move |c: CountryId| {
            if c == CountryId::BE {
                Some(s.clone())
            } else {
                None
            }
        }
    };
    let results = recall_then_rerank(
        "Rue de la Loi 16 1000 Bruxelles",
        &signals,
        &[CountryId::BE],
        &recaller,
        &reranker,
        &budget,
        shard_for,
        10,
    );
    assert!(!results.is_empty(), "expected non-empty results");
    let top = &results[0];
    // Top result should be in Brussels (50.84, 4.36) within ~5km.
    assert!(
        (top.lat - 50.84).abs() < 0.05,
        "lat={} not near Brussels",
        top.lat
    );
    assert!(
        (top.lon - 4.36).abs() < 0.05,
        "lon={} not near Brussels",
        top.lon
    );
}

#[test]
#[ignore]
fn recall_top_k_is_bounded() {
    let (shard, recaller, reranker) = open_setup();
    let signals = TaggerSignals::default();
    let budget = RecallBudget {
        top_k: 5,
        ..RecallBudget::default()
    };
    let shard_for = {
        let s = shard.clone();
        move |c: CountryId| {
            if c == CountryId::BE {
                Some(s.clone())
            } else {
                None
            }
        }
    };
    let results = recall_then_rerank(
        "Avenue Louise",
        &signals,
        &[CountryId::BE],
        &recaller,
        &reranker,
        &budget,
        shard_for,
        100,
    );
    assert!(
        results.len() <= 5,
        "top_k=5 budget violated: {}",
        results.len()
    );
}
