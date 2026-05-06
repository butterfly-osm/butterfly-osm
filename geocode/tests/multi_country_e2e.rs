//! End-to-end smoke tests against multi-country shards (#205).
//!
//! Requires:
//!   1. Built shards at `regions/multi/<iso2>.bfgs` for BE plus
//!      one of NL / LU, each with sibling recall index sidecars
//!      (`build-shard` emits them since #205).

use std::path::PathBuf;
use std::sync::Arc;

use butterfly_geocode::CountryId;
use butterfly_geocode::geocoder::executor::recall_then_rerank;
use butterfly_geocode::geocoder::recall::{RecallBudget, TaggerSignals};
use butterfly_geocode::server::ServerState;
use butterfly_geocode::shard::reader::Shard;

fn shards_dir() -> PathBuf {
    PathBuf::from("regions/multi")
}

fn open_state() -> ServerState {
    ServerState::load_from_dir(shards_dir()).unwrap_or_else(|e| {
        panic!(
            "could not load shards from {}: {e}\n\
             Build them first via `butterfly-geocode build-shard --pbf data/<country>.pbf \
             --out geocode/regions/multi/<iso2>.bfgs --country <ISO2>`",
            shards_dir().display()
        );
    })
}

#[test]
#[ignore]
fn loads_multiple_country_shards() {
    let state = open_state();
    let countries = state.loaded_countries();
    assert!(countries.contains(&"BE"), "expected BE shard loaded");
    assert!(countries.len() >= 2, "expected ≥2 shards");
    assert!(state.total_record_count() > 0);
}

#[test]
#[ignore]
fn pinned_belgium_recall_rerank_returns_brussels() {
    let state = open_state();
    let signals = TaggerSignals::default();
    let budget = RecallBudget::default();
    let shard_for = {
        let shards = state.shards.clone();
        move |c: CountryId| -> Option<Arc<Shard>> { shards.get(&c).cloned() }
    };
    let results = recall_then_rerank(
        "Rue de la Loi 16 1000 Bruxelles",
        &signals,
        &[CountryId::BE],
        &state.recaller,
        &state.reranker,
        &budget,
        shard_for,
        5,
    );
    assert!(!results.is_empty(), "BE pinned query produced no hits");
    let top = &results[0];
    assert_eq!(top.country, CountryId::BE);
    assert!(
        (50.5..51.5).contains(&top.lat) && (4.1..4.6).contains(&top.lon),
        "top hit not in Brussels area: lat={} lon={}",
        top.lat,
        top.lon
    );
}
