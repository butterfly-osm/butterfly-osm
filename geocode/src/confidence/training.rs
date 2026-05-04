//! GBDT training pipeline for the confidence reranker.
//!
//! ## Pipeline
//!
//! 1. Ingest a labeled JSONL corpus of `(query, gold)` pairs.
//! 2. For each query, run the executor against the shard and collect
//!    its candidates.
//! 3. For each candidate compute features ([`super::features`]).
//! 4. Label each `(query, candidate)` pair as `1.0` if the candidate
//!    matches the gold address (within 30 m AND same housenumber when
//!    the gold has one), else `0.0`.
//! 5. Train a pointwise GBDT (logistic loss) on the resulting
//!    `(features, label)` rows.
//!
//! ## Pointwise vs pairwise
//!
//! We train **pointwise** (logistic loss). Pointwise training is what
//! the gbdt crate ships natively (`LogLikelyhood`), and at the
//! per-candidate scale here (≤ 50 candidates per query), the difference
//! between pointwise and pairwise ranking quality is dominated by
//! corpus size, not loss shape. When real telemetry arrives and the
//! corpus grows past ~10k labelled queries, we revisit per #98 Phase 2.
//!
//! ## Corpus shape
//!
//! Each line in the JSONL is a [`LabeledQuery`]:
//!
//! ```json
//! {"query": "Rue Wayez 122 Anderlecht", "gold": {"lat": 50.834, "lon": 4.314, "housenumber": "122", "postcode": "1070"}}
//! ```

use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;

use anyhow::{Context, Result, anyhow};
use gbdt::config::Config;
use gbdt::decision_tree::{Data, DataVec};
use gbdt::gradient_boost::GBDT;
use serde::{Deserialize, Serialize};

use super::features::{Features, N_FEATURES, extract_features};
use super::gbdt::GbdtModel;
use crate::geocoder::cost::ShardStats;
use crate::geocoder::executor::{GeocodedResult, execute};
use crate::parser::heuristic::parse_heuristic;
use crate::routing::CountryId;
use crate::shard::reader::{Shard, haversine_m};

/// Distance tolerance for "same address" — a candidate within this
/// radius of the gold lat/lon AND with matching housenumber (when the
/// gold provides one) is labelled positive.
pub const POSITIVE_RADIUS_M: f64 = 30.0;

/// One labelled query for offline training.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LabeledQuery {
    pub query: String,
    #[serde(default)]
    pub country: Option<String>,
    pub gold: GoldAddress,
}

/// Ground-truth address fields for label computation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GoldAddress {
    pub lat: f64,
    pub lon: f64,
    #[serde(default)]
    pub housenumber: Option<String>,
    #[serde(default)]
    pub postcode: Option<String>,
}

/// Training-time hyperparameters. These mirror the shape benchmarked in
/// `GBDT_DECISION.md` (100 trees, depth 6) so the per-candidate latency
/// matches the published numbers.
#[derive(Debug, Clone, Copy)]
pub struct TrainConfig {
    pub n_trees: usize,
    pub max_depth: u32,
    pub learning_rate: f32,
    pub feature_sample_ratio: f32,
    pub data_sample_ratio: f32,
}

impl Default for TrainConfig {
    fn default() -> Self {
        Self {
            n_trees: 100,
            max_depth: 6,
            learning_rate: 0.1,
            feature_sample_ratio: 1.0,
            data_sample_ratio: 1.0,
        }
    }
}

/// Read a JSONL corpus of [`LabeledQuery`] entries.
pub fn load_corpus(path: &Path) -> Result<Vec<LabeledQuery>> {
    let f = File::open(path).with_context(|| format!("opening corpus {}", path.display()))?;
    let mut out = Vec::new();
    for (lineno, line) in BufReader::new(f).lines().enumerate() {
        let line = line.with_context(|| format!("reading line {}", lineno + 1))?;
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let q: LabeledQuery = serde_json::from_str(trimmed)
            .with_context(|| format!("parsing JSONL line {}", lineno + 1))?;
        out.push(q);
    }
    Ok(out)
}

/// Write a `(features, label)` corpus as JSONL — one row per
/// (query, candidate) pair. Useful for ablation / external trainers.
pub fn dump_training_rows(rows: &[(Features, f32)], out: &Path) -> Result<()> {
    if let Some(parent) = out.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }
    let f = File::create(out)?;
    let mut w = BufWriter::new(f);
    for (feats, label) in rows {
        let mut v = serde_json::to_value(feats)?;
        if let serde_json::Value::Object(ref mut m) = v {
            m.insert("label".into(), serde_json::Value::from(*label));
            m.insert(
                "schema_version".into(),
                serde_json::Value::from(Features::SCHEMA_VERSION),
            );
        }
        writeln!(w, "{}", serde_json::to_string(&v)?)?;
    }
    w.flush()?;
    Ok(())
}

/// One training-row outcome — feature vector and binary label.
pub type TrainingRow = (Features, f32);

/// Class-balance summary returned by [`build_training_rows`].
#[derive(Debug, Clone, Copy, Default)]
pub struct LabelBalance {
    pub positives: usize,
    pub negatives: usize,
}

/// Generate labelled training rows by running the executor on each
/// query in the corpus.
///
/// Returns `(rows, balance)` so the caller can report class balance —
/// a critical sanity check when the corpus is small.
pub fn build_training_rows(
    shard: &Shard,
    corpus: &[LabeledQuery],
    limit_per_query: usize,
) -> Result<(Vec<TrainingRow>, LabelBalance)> {
    let mut rows: Vec<TrainingRow> = Vec::new();
    let mut balance = LabelBalance::default();
    let stats = shard.stats();

    for lq in corpus {
        let country = parse_country(lq.country.as_deref())?;
        let parsed = parse_heuristic(&lq.query, country);
        let cands = execute(&parsed, shard, limit_per_query);
        if cands.is_empty() {
            continue;
        }
        let static_costs = static_cost_per_candidate(&cands, stats);
        let feats = extract_features(&cands, &parsed, &static_costs);
        for (cand, feat) in cands.iter().zip(feats.iter()) {
            let label = label_candidate(cand, &lq.gold);
            if label > 0.5 {
                balance.positives += 1;
            } else {
                balance.negatives += 1;
            }
            rows.push((feat.clone(), label));
        }
    }
    Ok((rows, balance))
}

fn parse_country(s: Option<&str>) -> Result<CountryId> {
    match s {
        None => Ok(CountryId::BE),
        Some(s) => CountryId::from_iso2(s).ok_or_else(|| anyhow!("unknown country '{s}'")),
    }
}

/// Per-candidate static cost, used as a feature. We use a single shared
/// value derived from the shard stats — for clean queries the executor
/// only runs one program, so all candidates share the same cost. When
/// multi-program execution lands (#98), this becomes per-program.
fn static_cost_per_candidate(cands: &[GeocodedResult], stats: ShardStats) -> Vec<f32> {
    // Approximation: cost ∝ posting-list size of the strongest blocker
    // that fired. We can't observe the program from out here, so we
    // back out from reason codes (same heuristic as
    // features::posting_size_log_from_query_and_record) and normalize.
    let total = stats.total_addresses.max(1) as f32;
    cands
        .iter()
        .map(|c| {
            let raw = if c.reason_codes.iter().any(|r| r == "POSTCODE_EXACT")
                && c.reason_codes.iter().any(|r| r == "STREET_EXACT")
            {
                stats.avg_street_postings.min(8.0)
            } else if c.reason_codes.iter().any(|r| r == "STREET_EXACT") {
                stats.avg_street_postings
            } else if c.reason_codes.iter().any(|r| r == "POSTCODE_EXACT") {
                stats.avg_postcode_postings
            } else {
                stats.avg_locality_postings
            };
            (raw / total).clamp(0.0, 1.0)
        })
        .collect()
}

/// Compute a binary label — `1.0` if `cand` matches `gold`, else `0.0`.
///
/// Definition of "matches":
///
/// - Distance to gold lat/lon ≤ [`POSITIVE_RADIUS_M`].
/// - When `gold.housenumber` is present, candidate housenumber must
///   equal it (case-insensitive).
/// - When `gold.postcode` is present, candidate postcode must equal it
///   (eq, no normalisation — postcodes are numeric in BE).
pub fn label_candidate(cand: &GeocodedResult, gold: &GoldAddress) -> f32 {
    let d = haversine_m(cand.lat, cand.lon, gold.lat, gold.lon);
    if d > POSITIVE_RADIUS_M {
        return 0.0;
    }
    if let Some(gh) = gold.housenumber.as_deref()
        && !gh.is_empty()
        && !cand.housenumber.eq_ignore_ascii_case(gh)
    {
        return 0.0;
    }
    if let Some(gp) = gold.postcode.as_deref()
        && !gp.is_empty()
        && cand.postcode.as_str() != gp
    {
        return 0.0;
    }
    1.0
}

/// Train a GBDT model on the labelled rows. Pointwise logistic loss.
pub fn train_pointwise(rows: &[(Features, f32)], cfg: TrainConfig) -> Result<GbdtModel> {
    if rows.is_empty() {
        return Err(anyhow!("empty training set — nothing to fit"));
    }
    let mut data: DataVec = rows
        .iter()
        .map(|(f, label)| Data {
            feature: f.to_row(),
            target: *label,
            weight: 1.0,
            label: *label,
            residual: 0.0,
            initial_guess: 0.0,
        })
        .collect();

    let mut conf = Config::new();
    conf.set_feature_size(N_FEATURES);
    conf.set_max_depth(cfg.max_depth);
    conf.set_iterations(cfg.n_trees);
    conf.set_shrinkage(cfg.learning_rate);
    conf.set_loss("LogLikelyhood");
    conf.set_data_sample_ratio(cfg.data_sample_ratio as f64);
    conf.set_feature_sample_ratio(cfg.feature_sample_ratio as f64);
    conf.set_training_optimization_level(2);
    conf.set_debug(false);

    let mut g = GBDT::new(&conf);
    g.fit(&mut data);
    Ok(GbdtModel::from_inner(g))
}

/// Held-out evaluation. Runs `model.predict_one` on each row and
/// computes binary classification accuracy at threshold `0.5`,
/// plus a "rank-1 hit-rate" — for each query group, did the
/// highest-scored candidate receive a positive label.
///
/// The caller groups rows by query before passing them in; each group
/// is one `Vec<(Features, f32)>`. The "rank-1 hit-rate" only fires for
/// groups that have at least one positive label.
#[derive(Debug, Clone, Copy, Default)]
pub struct EvalReport {
    pub n_rows: usize,
    pub n_positive: usize,
    pub binary_accuracy: f32,
    pub n_groups: usize,
    pub n_groups_with_positive: usize,
    pub rank_1_hits: usize,
    pub rank_1_hit_rate: f32,
}

#[must_use]
pub fn evaluate(model: &GbdtModel, groups: &[Vec<(Features, f32)>]) -> EvalReport {
    let mut report = EvalReport::default();
    let mut correct = 0usize;
    for grp in groups {
        if grp.is_empty() {
            continue;
        }
        report.n_groups += 1;
        let any_pos = grp.iter().any(|(_, l)| *l > 0.5);
        if any_pos {
            report.n_groups_with_positive += 1;
        }

        let mut best_idx = 0usize;
        let mut best_score = f32::MIN;
        for (i, (f, l)) in grp.iter().enumerate() {
            let s = model.predict_one(f);
            let predicted_pos = s >= 0.5;
            let actual_pos = *l > 0.5;
            if predicted_pos == actual_pos {
                correct += 1;
            }
            if *l > 0.5 {
                report.n_positive += 1;
            }
            report.n_rows += 1;
            if s > best_score {
                best_score = s;
                best_idx = i;
            }
        }
        if any_pos && grp[best_idx].1 > 0.5 {
            report.rank_1_hits += 1;
        }
    }
    if report.n_rows > 0 {
        report.binary_accuracy = correct as f32 / report.n_rows as f32;
    }
    if report.n_groups_with_positive > 0 {
        report.rank_1_hit_rate = report.rank_1_hits as f32 / report.n_groups_with_positive as f32;
    }
    report
}

/// Group training rows by query — required to compute rank-1 hit-rate
/// in [`evaluate`]. The caller knows which rows came from which query
/// (we emit them in lockstep with `corpus` order in
/// [`build_training_rows`]) but [`build_training_rows`] returns them
/// flattened. This helper rebuilds the grouping from the raw corpus +
/// the executor's per-query candidate count.
pub fn build_training_groups(
    shard: &Shard,
    corpus: &[LabeledQuery],
    limit_per_query: usize,
) -> Result<Vec<Vec<(Features, f32)>>> {
    let mut out: Vec<Vec<(Features, f32)>> = Vec::with_capacity(corpus.len());
    let stats = shard.stats();
    for lq in corpus {
        let country = parse_country(lq.country.as_deref())?;
        let parsed = parse_heuristic(&lq.query, country);
        let cands = execute(&parsed, shard, limit_per_query);
        if cands.is_empty() {
            out.push(Vec::new());
            continue;
        }
        let static_costs = static_cost_per_candidate(&cands, stats);
        let feats = extract_features(&cands, &parsed, &static_costs);
        let grp: Vec<(Features, f32)> = cands
            .iter()
            .zip(feats.iter())
            .map(|(c, f)| (f.clone(), label_candidate(c, &lq.gold)))
            .collect();
        out.push(grp);
    }
    Ok(out)
}

/// Synthesise a tiny labelled corpus by sampling random records from
/// the shard. Each sampled record becomes a `(query, gold)` pair where
/// the query is the canonical text rendition. Used as a Phase-0
/// bootstrap when no real labelled data is available.
pub fn synthesise_corpus_from_shard(shard: &Shard, max_rows: usize) -> Vec<LabeledQuery> {
    let mut out = Vec::with_capacity(max_rows.min(shard.record_count()));
    // Use a deterministic LCG over record ids — no rand dep needed.
    let n = shard.record_count() as u64;
    if n == 0 {
        return out;
    }
    let mut x: u64 = 0x9E3779B97F4A7C15;
    let want = max_rows.min(shard.record_count()) as u64;
    let mut emitted = 0u64;
    let mut tries = 0u64;
    while emitted < want && tries < want * 8 {
        x = x
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        tries += 1;
        let idx = (x % n) as u32;
        let Some(rec) = shard.record(idx) else {
            continue;
        };
        if rec.street.is_empty() || rec.housenumber.is_empty() {
            continue;
        }
        let q = if !rec.postcode.is_empty() && !rec.locality.is_empty() {
            format!(
                "{} {} {} {}",
                rec.street, rec.housenumber, rec.postcode, rec.locality
            )
        } else if !rec.locality.is_empty() {
            format!("{} {} {}", rec.street, rec.housenumber, rec.locality)
        } else {
            format!("{} {}", rec.street, rec.housenumber)
        };
        out.push(LabeledQuery {
            query: q,
            country: Some("BE".into()),
            gold: GoldAddress {
                lat: rec.lat,
                lon: rec.lon,
                housenumber: Some(rec.housenumber.to_string()),
                postcode: if rec.postcode.is_empty() {
                    None
                } else {
                    Some(rec.postcode.to_string())
                },
            },
        });
        emitted += 1;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shard::AddressRecord;
    use crate::shard::builder::build_shard;

    fn small_shard() -> (tempfile::TempDir, Shard) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("shard.bfgs");
        let addrs: Vec<AddressRecord> = (0..50)
            .map(|i| AddressRecord {
                street: if i % 2 == 0 {
                    "Rue Wayez".into()
                } else {
                    "Grote Markt".into()
                },
                housenumber: format!("{}", i + 1),
                postcode: if i % 2 == 0 {
                    "1070".into()
                } else {
                    "2000".into()
                },
                locality: if i % 2 == 0 {
                    "Anderlecht".into()
                } else {
                    "Antwerpen".into()
                },
                lat: if i % 2 == 0 { 50.834 } else { 51.221 },
                lon: if i % 2 == 0 {
                    4.314 + i as f64 * 1e-4
                } else {
                    4.401 + i as f64 * 1e-4
                },
            })
            .collect();
        build_shard(&path, crate::routing::CountryId::BE, addrs).unwrap();
        (dir, Shard::open(&path).unwrap())
    }

    #[test]
    fn label_candidate_positive_within_radius() {
        let cand = GeocodedResult {
            lat: 50.834,
            lon: 4.314,
            street: "Rue Wayez".into(),
            housenumber: "122".into(),
            postcode: "1070".into(),
            locality: "Anderlecht".into(),
            score: 1.0,
            country: None,
            reason_codes: vec![],
        };
        let gold = GoldAddress {
            lat: 50.834,
            lon: 4.31405,
            housenumber: Some("122".into()),
            postcode: Some("1070".into()),
        };
        assert_eq!(label_candidate(&cand, &gold), 1.0);
    }

    #[test]
    fn label_candidate_negative_wrong_house() {
        let cand = GeocodedResult {
            lat: 50.834,
            lon: 4.314,
            street: "Rue Wayez".into(),
            housenumber: "122".into(),
            postcode: "1070".into(),
            locality: "Anderlecht".into(),
            score: 1.0,
            country: None,
            reason_codes: vec![],
        };
        let gold = GoldAddress {
            lat: 50.834,
            lon: 4.314,
            housenumber: Some("999".into()),
            postcode: Some("1070".into()),
        };
        assert_eq!(label_candidate(&cand, &gold), 0.0);
    }

    #[test]
    fn label_candidate_far_distance_negative() {
        let cand = GeocodedResult {
            lat: 50.834,
            lon: 4.314,
            street: "Rue Wayez".into(),
            housenumber: "122".into(),
            postcode: "1070".into(),
            locality: "Anderlecht".into(),
            score: 1.0,
            country: None,
            reason_codes: vec![],
        };
        // 100 km away.
        let gold = GoldAddress {
            lat: 51.5,
            lon: 5.0,
            housenumber: Some("122".into()),
            postcode: None,
        };
        assert_eq!(label_candidate(&cand, &gold), 0.0);
    }

    #[test]
    fn synthesise_corpus_returns_records() {
        let (_d, shard) = small_shard();
        let corpus = synthesise_corpus_from_shard(&shard, 10);
        assert!(!corpus.is_empty());
        for lq in &corpus {
            assert!(!lq.query.is_empty());
            assert!(lq.gold.housenumber.is_some());
        }
    }

    #[test]
    fn build_training_rows_smoke() {
        let (_d, shard) = small_shard();
        let corpus = synthesise_corpus_from_shard(&shard, 20);
        let (rows, balance) = build_training_rows(&shard, &corpus, 10).unwrap();
        assert!(!rows.is_empty());
        assert!(balance.positives + balance.negatives == rows.len());
        assert!(
            balance.positives > 0,
            "expected at least one positive label"
        );
    }

    #[test]
    fn train_smoke_100_synthetic_samples() {
        // Pure synthetic — independent of shard. Validates that training
        // converges to "reasonable" predictions: positives score higher
        // than negatives on average.
        let mut rows: Vec<(Features, f32)> = Vec::new();
        for i in 0..100 {
            let f = Features {
                parser_confidence: (i as f32) / 100.0,
                street_fuzzy_score: (i as f32) / 100.0,
                ..Features::default()
            };
            let label = if i >= 50 { 1.0 } else { 0.0 };
            rows.push((f, label));
        }
        let cfg = TrainConfig {
            n_trees: 10,
            max_depth: 3,
            ..TrainConfig::default()
        };
        let model = train_pointwise(&rows, cfg).unwrap();
        let hi = Features {
            parser_confidence: 0.9,
            street_fuzzy_score: 0.9,
            ..Features::default()
        };
        let lo = Features {
            parser_confidence: 0.1,
            street_fuzzy_score: 0.1,
            ..Features::default()
        };
        let s_hi = model.predict_one(&hi);
        let s_lo = model.predict_one(&lo);
        assert!(s_hi > s_lo, "expected hi > lo, got {s_hi} vs {s_lo}");
    }

    #[test]
    fn corpus_jsonl_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("c.jsonl");
        std::fs::write(
            &p,
            "{\"query\":\"Test\",\"gold\":{\"lat\":50.0,\"lon\":4.0,\"housenumber\":\"1\"}}\n# a comment\n\n",
        )
        .unwrap();
        let v = load_corpus(&p).unwrap();
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].query, "Test");
    }

    #[test]
    fn dump_training_rows_writes_jsonl_with_label() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("rows.jsonl");
        let rows = vec![(Features::default(), 1.0), (Features::default(), 0.0)];
        dump_training_rows(&rows, &p).unwrap();
        let s = std::fs::read_to_string(&p).unwrap();
        assert_eq!(s.lines().count(), 2);
        assert!(s.contains("\"label\":1.0"));
        assert!(s.contains("\"schema_version\":1"));
    }

    #[test]
    fn evaluate_groups_reports_metrics() {
        let mut rows: Vec<(Features, f32)> = Vec::new();
        for i in 0..40 {
            let f = Features {
                parser_confidence: (i as f32) / 40.0,
                street_fuzzy_score: (i as f32) / 40.0,
                ..Features::default()
            };
            let label = if i >= 20 { 1.0 } else { 0.0 };
            rows.push((f, label));
        }
        let cfg = TrainConfig {
            n_trees: 8,
            max_depth: 3,
            ..TrainConfig::default()
        };
        let model = train_pointwise(&rows, cfg).unwrap();

        let groups = vec![rows[..10].to_vec(), rows[20..30].to_vec()];
        let report = evaluate(&model, &groups);
        assert!(report.n_rows == 20);
        assert!(report.n_groups == 2);
        // Group 1 has only negatives, group 2 has only positives →
        // exactly one group has positives and rank-1 should hit.
        assert_eq!(report.n_groups_with_positive, 1);
        assert_eq!(report.rank_1_hits, 1);
    }
}
