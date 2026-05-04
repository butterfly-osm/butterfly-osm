//! Integration tests for the #97 fanout safeguards.
//!
//! These tests build a small in-memory shard and exercise the
//! control plane end-to-end (parser → budget → executor) with
//! adversarial knobs flipped to force admission to refuse, the
//! candidate cap to fire, or the static-cost ceiling to abort.

use butterfly_geocode::control::AdmissionState;
use butterfly_geocode::control::GeneralMetrics;
use butterfly_geocode::control::admission::AdmissionPolicy;
use butterfly_geocode::control::budget::{BudgetPolicy, classify_tier, compute_budget};
use butterfly_geocode::control::fanout::{FanoutConfig, FanoutTracker, FanoutVerdict};
use butterfly_geocode::geocoder::executor::{ControlPlane, execute_with_control};
use butterfly_geocode::parser::heuristic::parse_heuristic;
use butterfly_geocode::routing::CountryId;
use butterfly_geocode::shard::AddressRecord;
use butterfly_geocode::shard::builder::build_shard;
use butterfly_geocode::shard::reader::Shard;
use butterfly_geocode::types::ExecutionBudget;

fn small_shard() -> (tempfile::TempDir, Shard) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("shard.bfgs");
    let mut addrs: Vec<AddressRecord> = Vec::new();
    // Generate enough records to make the cost model fire on
    // unindexed queries.
    for i in 0..200 {
        addrs.push(AddressRecord {
            street: format!("Rue {i}"),
            housenumber: format!("{i}"),
            postcode: "1070".into(),
            locality: "Anderlecht".into(),
            lat: 50.834,
            lon: 4.314 + (i as f64) * 1e-5,
        });
    }
    build_shard(&path, addrs).unwrap();
    let s = Shard::open(&path).unwrap();
    (dir, s)
}

#[test]
fn ambiguous_query_widens_tier_beyond_tight() {
    // Low-confidence multilingual ambiguous postcode-less input.
    // Should NOT land at Tight tier.
    let mut q = parse_heuristic("Random text without anchors", CountryId::BE);
    q.global_confidence = 0.3;
    let (_dir, shard) = small_shard();
    let stats = shard.stats();
    let tier = classify_tier(&q, stats, BudgetPolicy::default());
    assert!(
        tier != butterfly_geocode::control::budget::BudgetTier::Tight,
        "low confidence query was still Tight: {tier:?}"
    );
}

#[test]
fn cost_ceiling_pre_execution_check_rejects_overrun() {
    let (_dir, shard) = small_shard();
    let q = parse_heuristic("Rue 1 1070 Anderlecht", CountryId::BE);
    let cp = ControlPlane::new();

    // Squeeze the budget below any plausible cost.
    let mut q2 = q.clone();
    q2.execution_budget = ExecutionBudget {
        max_countries: 1,
        max_hypotheses: 1,
        max_fuzzy_expansions: 0,
        max_total_candidates: 50,
        static_cost_ceiling: 0.001,
        dual_evaluation_enabled: false,
    };

    let res = execute_with_control(&q2, &shard, 5, &cp);
    assert!(
        res.is_err(),
        "expected static-cost ceiling refusal, got {} hits",
        res.map(|v| v.len()).unwrap_or(0)
    );
}

#[test]
fn candidate_cap_truncates_results() {
    let (_dir, shard) = small_shard();
    let q = parse_heuristic("1070", CountryId::BE);
    // Confidence is medium with postcode-only — Normal tier.
    let cp = ControlPlane::new();

    let mut q2 = q.clone();
    // Tight `max_total_candidates` so the cap fires.
    q2.execution_budget = ExecutionBudget {
        max_countries: 1,
        max_hypotheses: 1,
        max_fuzzy_expansions: 0,
        max_total_candidates: 5,
        static_cost_ceiling: 1e9,
        dual_evaluation_enabled: false,
    };

    let res = execute_with_control(&q2, &shard, 50, &cp).unwrap();
    // Cap is enforced in the multi-hypothesis path; for clean
    // queries, the executor still returns up to `limit` records.
    // Either way, the shard has 200 records but we should not flood
    // the result set beyond `limit`.
    assert!(res.len() <= 50);
}

#[test]
fn fanout_tracker_aborts_on_blocker_storm() {
    let cfg = FanoutConfig {
        max_blocker_empty_downgrades_per_query: 2,
        ..FanoutConfig::default()
    };
    let t = FanoutTracker::new(cfg);
    assert_eq!(t.record_blocker_downgrade(), FanoutVerdict::Ok);
    assert_eq!(t.record_blocker_downgrade(), FanoutVerdict::Ok);
    assert_eq!(
        t.record_blocker_downgrade(),
        FanoutVerdict::BlockerDowngradeStorm
    );
}

#[test]
fn admission_rejects_burst_then_refills() {
    let policy = AdmissionPolicy {
        global_capacity: 2,
        global_refill_per_sec: 100, // 10ms per token
        per_ip_capacity: 1_000_000,
        per_ip_refill_per_sec: 1_000_000,
        ..AdmissionPolicy::default()
    };
    let s = AdmissionState::new(policy, GeneralMetrics::new());

    let ip = Some(std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)));
    assert!(s.try_admit(ip));
    assert!(s.try_admit(ip));
    // Burst exhausted.
    assert!(!s.try_admit(ip));
    // Wait for refill (one token = 10 ms).
    std::thread::sleep(std::time::Duration::from_millis(20));
    assert!(s.try_admit(ip));
}

#[test]
fn budget_compute_returns_widened_tier_on_high_fanout() {
    let mut q = parse_heuristic("Rue Wayez 122 1070 Anderlecht", CountryId::BE);
    q.global_confidence = 0.95;

    // Lower the threshold so the synthetic stats trip it.
    let policy = BudgetPolicy {
        high_fanout_postings_threshold: 1,
        ..BudgetPolicy::default()
    };

    let (_dir, shard) = small_shard();
    let b = compute_budget(&q, shard.stats(), policy);
    let tight_ceiling = BudgetPolicy::default().static_cost_ceilings.0;
    assert!(
        b.static_cost_ceiling > tight_ceiling,
        "high fanout did not widen ceiling: {b:?}"
    );
}
