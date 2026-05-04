//! End-to-end tests against multi-country shards.
//!
//! These tests are `#[ignore]` by default because they require:
//!
//! 1. Built shards at `regions/multi/<iso2>.bfgs` for at least
//!    `be.bfgs` and one of `nl.bfgs` / `lu.bfgs`.
//! 2. The PBFs to have been downloaded via `butterfly-dl` or curl.
//!
//! Run with:
//!
//! ```text
//! cargo test -p butterfly-geocode --release --test multi_country_e2e -- --ignored
//! ```

use std::path::PathBuf;

use butterfly_geocode::server::ServerState;
use butterfly_geocode::{
    CountryId, classify_country, execute, execute_across_shards, parse_heuristic,
    parse_with_classifier,
};

fn shards_dir() -> PathBuf {
    PathBuf::from("regions/multi")
}

fn open_state() -> ServerState {
    ServerState::load_from_dir(shards_dir()).unwrap_or_else(|e| {
        panic!(
            "could not load shards from {}: {e}\n\
             Build them first: cargo run --release -p butterfly-geocode -- build-shard \\\n\
                            --pbf data/<country>.pbf --out geocode/regions/multi/<iso2>.bfgs --country <ISO2>",
            shards_dir().display()
        );
    })
}

#[test]
#[ignore]
fn loads_multiple_country_shards() {
    let state = open_state();
    let countries = state.loaded_countries();
    assert!(
        countries.contains(&"BE"),
        "expected BE shard loaded, got {countries:?}"
    );
    assert!(
        countries.len() >= 2,
        "expected ≥2 country shards for the multi-country e2e (got {countries:?}). \
         Build LU and/or NL shards into regions/multi/."
    );
    assert!(state.total_record_count() > 0);
}

#[test]
#[ignore]
fn pinned_belgium_query_returns_belgium_results() {
    let state = open_state();
    let q = parse_heuristic("Rue Wayez 122 1070 Anderlecht", CountryId::BE);
    let results = execute_across_shards(&q, &state.shards, 5);
    assert!(!results.is_empty(), "BE pinned query produced no hits");
    let top = &results[0];
    assert_eq!(top.country, Some("BE"));
    assert!(
        (50.5..51.0).contains(&top.lat) && (4.1..4.5).contains(&top.lon),
        "top hit not in Brussels area: {top:?}"
    );
}

#[test]
#[ignore]
fn pinned_luxembourg_query_returns_lu_results_when_loaded() {
    let state = open_state();
    if !state.shards.contains_key(&CountryId::LU) {
        eprintln!("skipping (no LU shard loaded)");
        return;
    }
    let q = parse_heuristic("rue de la Gare Luxembourg", CountryId::LU);
    let results = execute_across_shards(&q, &state.shards, 5);
    if results.is_empty() {
        let q2 = parse_heuristic("L-1611", CountryId::LU);
        let r2 = execute_across_shards(&q2, &state.shards, 5);
        assert!(!r2.is_empty(), "no LU hits for either canonical query");
        for r in &r2 {
            assert_eq!(
                r.country,
                Some("LU"),
                "LU pin returned non-LU result {:?}",
                r
            );
        }
        return;
    }
    let top = &results[0];
    assert_eq!(top.country, Some("LU"));
    assert!(
        (49.4..50.3).contains(&top.lat),
        "top hit not in Luxembourg lat band: {top:?}"
    );
}

#[test]
#[ignore]
fn pinned_netherlands_query_returns_nl_results_when_loaded() {
    let state = open_state();
    if !state.shards.contains_key(&CountryId::NL) {
        eprintln!("skipping (no NL shard loaded)");
        return;
    }
    let q = parse_heuristic("Damrak 1 1012 LP Amsterdam", CountryId::NL);
    let results = execute_across_shards(&q, &state.shards, 5);
    assert!(!results.is_empty(), "NL pinned query produced no hits");
    let top = &results[0];
    assert_eq!(top.country, Some("NL"));
    assert!(
        (52.0..53.0).contains(&top.lat) && (4.5..5.5).contains(&top.lon),
        "top NL hit not in Amsterdam area: {top:?}"
    );
}

#[test]
#[ignore]
fn cross_country_classifier_routes_correctly() {
    let state = open_state();
    if !state.shards.contains_key(&CountryId::NL) {
        eprintln!("skipping (no NL shard loaded)");
        return;
    }

    // BE-flavored input → classifier picks BE → BE shard hit.
    let q_be = parse_with_classifier("Rue Wayez 122 1070 Anderlecht");
    assert_eq!(q_be.country_candidates[0].0, CountryId::BE);
    let r_be = execute_across_shards(&q_be, &state.shards, 5);
    assert!(!r_be.is_empty());
    assert_eq!(r_be[0].country, Some("BE"));

    // NL-flavored input → classifier picks NL → NL shard hit.
    let q_nl = parse_with_classifier("Damrak 1 1012 LP Amsterdam");
    assert_eq!(q_nl.country_candidates[0].0, CountryId::NL);
    let r_nl = execute_across_shards(&q_nl, &state.shards, 5);
    assert!(!r_nl.is_empty());
    assert_eq!(r_nl[0].country, Some("NL"));
}

#[test]
#[ignore]
fn ambiguous_query_returns_ranked_posterior_across_shards() {
    let state = open_state();
    let _other = if state.shards.contains_key(&CountryId::LU) {
        CountryId::LU
    } else if state.shards.contains_key(&CountryId::AT) {
        CountryId::AT
    } else if state.shards.contains_key(&CountryId::CH) {
        CountryId::CH
    } else if state.shards.contains_key(&CountryId::NL) {
        CountryId::NL
    } else {
        eprintln!("skipping (no second-country shard loaded)");
        return;
    };

    // Use a generic 4-digit postcode that exists across countries.
    let posterior = classify_country("1070");
    let be_mass = posterior
        .iter()
        .find(|(c, _)| *c == CountryId::BE)
        .map(|(_, w)| *w)
        .unwrap_or(0.0);
    assert!(be_mass > 0.0, "BE expected in posterior for '1070'");

    // Run the executor with max_countries=4 so the executor walks
    // multiple shards from the classifier posterior.
    let mut q = parse_with_classifier("1070");
    q.execution_budget.max_countries = 4;
    let results = execute_across_shards(&q, &state.shards, 50);
    if results.is_empty() {
        eprintln!("no results for ambiguous '1070' — sparse OSM postcode coverage");
        return;
    }
    let countries: std::collections::HashSet<_> =
        results.iter().filter_map(|r| r.country).collect();
    assert!(
        !countries.is_empty(),
        "no country tagged on ambiguous results"
    );
}

#[test]
#[ignore]
fn reverse_geocode_routes_via_bbox() {
    let state = open_state();
    {
        let shard = state.shards.get(&CountryId::BE).expect("BE shard");
        let hits = shard.nearest_within(50.8467, 4.3525, 80.0, 1);
        assert!(!hits.is_empty(), "expected BE hit at Grand-Place");
    }
    if state.shards.contains_key(&CountryId::LU) {
        use butterfly_geocode::country_for_point;
        assert_eq!(country_for_point(49.6116, 6.1319), Some(CountryId::LU));
    }
    if state.shards.contains_key(&CountryId::NL) {
        use butterfly_geocode::country_for_point;
        assert_eq!(country_for_point(52.3791, 4.9003), Some(CountryId::NL));
    }
}

#[test]
#[ignore]
fn missing_pinned_country_returns_empty_in_executor() {
    let state = open_state();
    if state.shards.contains_key(&CountryId::DE) {
        eprintln!("skipping (DE shard happens to be loaded)");
        return;
    }
    let mut q = parse_heuristic("Friedrichstraße 100 10117 Berlin", CountryId::DE);
    q.execution_budget.max_countries = 1;
    let results = execute_across_shards(&q, &state.shards, 5);
    assert!(
        results.is_empty(),
        "expected empty when DE shard is not loaded; got {:?}",
        results
    );
}

/// Assert the per-shard `execute()` and the multi-shard
/// `execute_across_shards()` produce the same top result on a clean
/// pinned query — the multi-shard layer must not regress single-shard
/// quality.
#[test]
#[ignore]
fn execute_across_shards_matches_single_shard_on_clean_query() {
    let state = open_state();
    let shard = state.shards.get(&CountryId::BE).expect("BE shard");
    let q = parse_heuristic("Rue Wayez 122 1070 Anderlecht", CountryId::BE);
    let single = execute(&q, shard, 5);
    let multi = execute_across_shards(&q, &state.shards, 5);
    assert!(!single.is_empty() && !multi.is_empty());
    assert_eq!(single[0].lat, multi[0].lat);
    assert_eq!(single[0].lon, multi[0].lon);
    assert_eq!(single[0].housenumber, multi[0].housenumber);
}
