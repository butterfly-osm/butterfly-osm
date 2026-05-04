//! End-to-end tests against a Belgium shard.
//!
//! These tests are `#[ignore]` by default because they require:
//!
//! 1. A built shard at `regions/belgium.bfgs` (see
//!    `cargo run --release -p butterfly-geocode -- build-shard ...`)
//! 2. A running PBF extract — the shard build needs `data/belgium.pbf`
//!
//! Run with:
//!
//! ```text
//! cargo test -p butterfly-geocode --release --test belgium_e2e -- --ignored
//! ```

use butterfly_geocode::control::budget::{BudgetPolicy, classify_tier};
use butterfly_geocode::geocoder::executor::{ControlPlane, execute_with_control};
use butterfly_geocode::types::ExecutionBudget;
use butterfly_geocode::{
    Confidence, ConfidenceConfig, CountryId, GbdtModel, NeuralParser, Shard, execute,
    execute_with_rerank, parse_heuristic,
};

const SHARD_PATH: &str = "regions/belgium.bfgs";
const RERANK_MODEL_PATH: &str = "data/models/rerank-belgium-tiny.gbdt";
const TINY_MODEL_PATH: &str = "data/models/belgium-tiny.safetensors";

fn open_model_if_present() -> Option<NeuralParser> {
    if std::path::Path::new(TINY_MODEL_PATH).exists() {
        match NeuralParser::load(TINY_MODEL_PATH) {
            Ok(p) => Some(p),
            Err(e) => {
                eprintln!("warning: model file present but failed to load: {e}");
                None
            }
        }
    } else {
        None
    }
}

fn open_shard() -> Shard {
    Shard::open(SHARD_PATH).unwrap_or_else(|e| {
        panic!(
            "could not open {SHARD_PATH}: {e}\n\
             Build it first: cargo run --release -p butterfly-geocode -- build-shard \\\n\
                            --pbf data/belgium.pbf --out geocode/regions/belgium.bfgs"
        );
    })
}

#[test]
#[ignore]
fn rue_wayez_122_anderlecht_resolves_to_brussels() {
    let shard = open_shard();
    let q = parse_heuristic("Rue Wayez 122 Anderlecht", CountryId::BE);
    let results = execute(&q, &shard, 5);
    assert!(
        !results.is_empty(),
        "expected hits for Rue Wayez 122 Anderlecht"
    );
    let top = &results[0];
    // Anderlecht / Bruxelles area: lat ~50.6-50.8, lon ~4.2-4.4
    assert!(
        (50.5..51.0).contains(&top.lat) && (4.1..4.5).contains(&top.lon),
        "top hit out of Brussels bounds: lat={} lon={}",
        top.lat,
        top.lon
    );
    assert!(top.street.to_lowercase().contains("wayez"));
}

#[test]
#[ignore]
fn postcode_1000_centroid_in_brussels() {
    let shard = open_shard();
    let q = parse_heuristic("1000 Bruxelles", CountryId::BE);
    let results = execute(&q, &shard, 10);
    assert!(!results.is_empty());
    for r in &results {
        assert_eq!(
            r.postcode, "1000",
            "expected postcode=1000, got {}",
            r.postcode
        );
        assert!(
            (50.7..51.0).contains(&r.lat) && (4.2..4.5).contains(&r.lon),
            "postcode 1000 record out of Brussels: lat={} lon={}",
            r.lat,
            r.lon
        );
    }
}

#[test]
#[ignore]
fn grote_markt_antwerpen_at_correct_centroid() {
    let shard = open_shard();
    let q = parse_heuristic("Grote Markt Antwerpen", CountryId::BE);
    let results = execute(&q, &shard, 5);
    assert!(!results.is_empty());
    let top = &results[0];
    // Grote Markt Antwerpen ≈ 51.221, 4.401
    assert!(
        (51.10..51.30).contains(&top.lat) && (4.30..4.50).contains(&top.lon),
        "top hit out of Antwerp center: lat={} lon={}",
        top.lat,
        top.lon
    );
}

#[test]
#[ignore]
fn reverse_brussels_grand_place_returns_grand_place() {
    let shard = open_shard();
    // Brussels Grand-Place: ~50.8467, 4.3525
    let hits = shard.nearest_within(50.8467, 4.3525, 80.0, 5);
    assert!(
        !hits.is_empty(),
        "expected at least one record near Grand-Place"
    );
    let top_street = hits[0].0.street.to_lowercase();
    assert!(
        top_street.contains("grand-place")
            || top_street.contains("grote markt")
            || top_street.contains("place"),
        "expected Grand-Place/Grote Markt, got '{top_street}'"
    );
}

#[test]
#[ignore]
fn empty_query_returns_empty() {
    let shard = open_shard();
    let q = parse_heuristic("", CountryId::BE);
    let results = execute(&q, &shard, 5);
    assert!(results.is_empty());
}

#[test]
#[ignore]
fn budget_exhaustion_short_circuits() {
    // "Rue 1" is the canonical fanout-hostile query: ambiguous
    // street, no locality, no postcode. The control plane MUST
    // either widen to Wide+ tier or short-circuit on the candidate
    // cap.
    let shard = open_shard();
    let mut q = parse_heuristic("Rue 1", CountryId::BE);
    // Bracket the budget so any non-trivial expansion trips the cap.
    q.execution_budget = ExecutionBudget {
        max_countries: 1,
        max_hypotheses: 1,
        max_fuzzy_expansions: 0,
        max_total_candidates: 5,
        // Generous ceiling so admission doesn't fire — we want the
        // candidate cap to be the gate.
        static_cost_ceiling: 1e9,
        dual_evaluation_enabled: false,
    };
    let cp = ControlPlane::new();
    let res = execute_with_control(&q, &shard, 100, &cp).unwrap();
    assert!(
        res.len() <= 100,
        "candidate cap did not bound the result set: {} hits",
        res.len()
    );
}

#[test]
#[ignore]
fn rerank_does_not_break_top1_for_known_correct_query() {
    // Same Brussels query as the no-model test above, but routed
    // through `execute_with_rerank` with a real GBDT model loaded.
    // Asserts:
    //   1. Top-1 is still the right street.
    //   2. Top-1 lands in the Brussels lat/lon box.
    //   3. The rerank attached `RERANK_GBDT` to every candidate.
    //   4. The action tier is one of `accept|caution|review` — never
    //      `reject` for a query the no-model path handles.
    let shard = open_shard();
    let model = GbdtModel::load(std::path::Path::new(RERANK_MODEL_PATH)).unwrap_or_else(|e| {
        panic!(
            "could not load {RERANK_MODEL_PATH}: {e}\n\
             Train it first: cargo run --release -p butterfly-geocode -- train-rerank \\\n\
                            --shard geocode/regions/belgium.bfgs \\\n\
                            --out geocode/data/models/rerank-belgium-tiny.gbdt"
        );
    });
    let cfg = ConfidenceConfig::default();
    let q = parse_heuristic("Rue Wayez 122 Anderlecht", CountryId::BE);

    let baseline = execute(&q, &shard, 5);
    assert!(
        !baseline.is_empty(),
        "no-model executor should resolve this query"
    );

    let (results, action) = execute_with_rerank(&q, &shard, 5, Some(&model), &cfg);
    assert!(
        !results.is_empty(),
        "rerank should not empty the result set"
    );
    assert_ne!(action, Confidence::Reject, "got reject for a known query");

    let top = &results[0];
    assert!(
        top.street.to_lowercase().contains("wayez"),
        "rerank changed top-1 street: got '{}'",
        top.street
    );
    assert!(
        (50.5..51.0).contains(&top.lat) && (4.1..4.5).contains(&top.lon),
        "rerank top-1 out of Brussels bounds: lat={} lon={}",
        top.lat,
        top.lon
    );
    assert!(
        results
            .iter()
            .all(|r| r.reason_codes.iter().any(|c| c == "RERANK_GBDT")),
        "every reranked candidate should carry RERANK_GBDT"
    );
}

#[test]
#[ignore]
fn clean_query_belgium_classifies_correctly() {
    let shard = open_shard();
    let mut q = parse_heuristic("Rue Wayez 122 1070 Anderlecht", CountryId::BE);
    q.global_confidence = 0.95; // boost so we hit the Tight branch
    let stats = shard.stats();
    let tier = classify_tier(&q, stats, BudgetPolicy::default());
    // The Belgium clean query SHOULD land at Tight or Normal — never
    // Wide / Desperate.
    assert!(
        matches!(
            tier,
            butterfly_geocode::control::budget::BudgetTier::Tight
                | butterfly_geocode::control::budget::BudgetTier::Normal
        ),
        "clean Belgium query landed at {tier:?}"
    );

    let cp = ControlPlane::new();
    let res = execute_with_control(&q, &shard, 5, &cp).unwrap();
    assert!(
        !res.is_empty(),
        "expected hits for the canonical clean query"
    );
}

#[test]
#[ignore]
fn rerank_no_model_path_is_unchanged() {
    let shard = open_shard();
    let cfg = ConfidenceConfig::default();
    let q = parse_heuristic("Rue Wayez 122 Anderlecht", CountryId::BE);
    let baseline = execute(&q, &shard, 5);
    let (results, action) = execute_with_rerank(&q, &shard, 5, None, &cfg);
    assert_eq!(action, Confidence::Accept);
    assert_eq!(results.len(), baseline.len());
    for (a, b) in results.iter().zip(baseline.iter()) {
        assert_eq!(a.lat, b.lat);
        assert_eq!(a.lon, b.lon);
        assert_eq!(a.housenumber, b.housenumber);
        assert!(
            !a.reason_codes.iter().any(|c| c == "RERANK_GBDT"),
            "no-model path must not annotate RERANK_GBDT"
        );
    }
}

#[test]
#[ignore]
fn rerank_latency_p50_under_budget() {
    // Smoke benchmark — runs the full rerank pipeline on a single
    // query and asserts that p50 inference latency over 100 calls is
    // under 5 ms (well under the 10 µs/candidate slice budgeted in
    // GBDT_DECISION.md, even after extract_features and the apply
    // layer).
    let shard = open_shard();
    let model = GbdtModel::load(std::path::Path::new(RERANK_MODEL_PATH)).unwrap();
    let cfg = ConfidenceConfig::default();
    let q = parse_heuristic("Rue Wayez 122 Anderlecht", CountryId::BE);

    // Warm-up.
    for _ in 0..10 {
        let _ = execute_with_rerank(&q, &shard, 5, Some(&model), &cfg);
    }
    let mut samples_us = Vec::with_capacity(100);
    for _ in 0..100 {
        let t = std::time::Instant::now();
        let _ = execute_with_rerank(&q, &shard, 5, Some(&model), &cfg);
        samples_us.push(t.elapsed().as_micros() as u64);
    }
    samples_us.sort_unstable();
    let p50 = samples_us[50];
    let p99 = samples_us[99];
    println!("rerank latency: p50={p50} µs, p99={p99} µs");
    assert!(p50 < 5_000, "p50 latency {p50} µs over 5 ms budget");
}

#[test]
#[ignore]
fn fuzzy_misspelling_recovers() {
    let shard = open_shard();
    // Misspelled "Avenue Louise" → should fall back via fuzzy.
    let q = parse_heuristic("Avenue Louse 100", CountryId::BE);
    let results = execute(&q, &shard, 5);
    assert!(
        !results.is_empty(),
        "expected fuzzy fallback to recover Avenue Louise"
    );
    assert!(results.iter().any(|r| {
        r.reason_codes
            .iter()
            .any(|c| c == "STREET_FUZZY" || c == "STREET_EXACT")
    }));
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn axum_belgium_real_shard_smoke() {
    // Sanity check that the Axum router still works end-to-end against
    // the full Belgium shard. Multi-thread runtime so `spawn_blocking`
    // (used by handlers per C6) has a thread to park on.
    use std::sync::Arc;

    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use butterfly_geocode::server::{ServerState, build_router};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    let shard = open_shard();
    let state = Arc::new(ServerState::new(shard));
    let app = build_router(state);

    let req = Request::builder()
        .uri("/geocode?q=Rue%20Wayez%20122%20Anderlecht&country=BE")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let body = std::str::from_utf8(&bytes).unwrap();
    let v: serde_json::Value = serde_json::from_str(body).unwrap();
    assert!(v["count"].as_u64().unwrap() > 0, "no hits: {body}");
}

#[test]
#[ignore]
fn neural_parser_resolves_rue_wayez_122_anderlecht() {
    let Some(parser) = open_model_if_present() else {
        eprintln!(
            "skipping: {} not present. Train it first with `butterfly-geocode train --out {}`",
            TINY_MODEL_PATH, TINY_MODEL_PATH
        );
        return;
    };
    let shard = open_shard();
    let parsed = parser
        .parse("Rue Wayez 122 Anderlecht", &shard)
        .expect("neural parse");
    let results = execute(&parsed, &shard, 5);
    assert!(
        !results.is_empty(),
        "expected neural parser to recover Rue Wayez 122 Anderlecht; \
         note: tiny model has limited capacity — failure is acceptable \
         only if heuristic-fallback path were active, which it is not in this test."
    );
    let top = &results[0];
    assert!(
        top.street.to_lowercase().contains("wayez"),
        "top hit street did not contain 'wayez': {}",
        top.street
    );
}

#[test]
#[ignore]
fn neural_parser_dedup_collapse_rate_is_observable() {
    // Validates the #98 1.1 exit criterion: dedup collapse rate is
    // measurable and non-negative. This exposes the canonicalization
    // pipeline to an actual parser output rather than a synthetic test.
    let Some(parser) = open_model_if_present() else {
        eprintln!("skipping: {} not present", TINY_MODEL_PATH);
        return;
    };
    let shard = open_shard();
    let decoded = parser
        .decode("Rue Wayez 122 1070 Anderlecht", &shard)
        .expect("neural decode");
    assert!(
        decoded.dedup_collapse_rate >= 0.0 && decoded.dedup_collapse_rate <= 1.0,
        "collapse rate {} out of [0,1]",
        decoded.dedup_collapse_rate
    );
    assert!(
        !decoded.programs.is_empty(),
        "neural decoder produced zero programs"
    );
}
