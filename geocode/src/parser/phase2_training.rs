//! Phase 2 GBDT training pipeline (#98 §2.2).
//!
//! Read a JSONL file of `(features, label)` rows produced by the
//! `phase2-label` binary, train a pointwise GBDT against geocode
//! success, evaluate on a held-out split (AUC + Brier), persist the
//! model.
//!
//! ## Pointwise loss vs ranking loss
//!
//! Mirrors the `confidence::training` choice — we use pointwise log-
//! likelihood. Pairwise / listwise losses on Phase 2 don't help because
//! the per-query candidate set is the **parser's hypothesis set**
//! (typically ≤ 5 entries), not a ranked candidate list. We're
//! learning a calibrated success-probability for each (parse →
//! program) decision in isolation, then stacking probabilities at
//! decode time.
//!
//! ## Train/eval split
//!
//! Random 90/10 by index. Deterministic with `--seed`. The split is
//! NOT group-aware — Phase 2 rows from the same gold record are not
//! "the same query" the way candidate-rerank rows are; they're
//! independent (parse hypothesis, program) choices with potentially
//! different correct answers. Random split is correct.

use std::fs::{File, create_dir_all};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;

use anyhow::{Context, Result, anyhow};
use gbdt::config::Config;
use gbdt::decision_tree::{Data, DataVec};
use gbdt::gradient_boost::GBDT;
use rand::SeedableRng;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};

use super::phase2_features::{Features, N_FEATURES};
use super::retrieval_utility::LearnedScorer;

/// One labeled training row written by the labeler.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LabeledRow {
    /// Phase 2 schema version stamp. Validated at load time.
    pub schema_version: u32,
    /// Feature vector.
    #[serde(flatten)]
    pub features: Features,
    /// Binary label: 1.0 if the executed retrieval program landed on
    /// the gold record, 0.0 otherwise.
    pub label: f32,
}

/// Hyper-parameters for [`train_pointwise`]. Defaults are tuned for
/// the 30-feature schema and a 5M-row training set: 150 trees @ depth
/// 6 fits comfortably under the 1 MB on-disk budget while still
/// reaching diminishing-returns on AUC by epoch 100.
#[derive(Debug, Clone, Copy)]
pub struct TrainConfig {
    pub n_trees: usize,
    pub max_depth: u32,
    pub learning_rate: f32,
    pub feature_sample_ratio: f32,
    pub data_sample_ratio: f32,
    pub seed: u64,
    /// Eval split fraction in `[0, 1)`. `0.0` = no held-out eval.
    pub eval_split: f32,
}

impl Default for TrainConfig {
    fn default() -> Self {
        Self {
            n_trees: 150,
            max_depth: 6,
            learning_rate: 0.1,
            feature_sample_ratio: 1.0,
            data_sample_ratio: 1.0,
            seed: 0xB17EBAD0,
            eval_split: 0.1,
        }
    }
}

/// Held-out evaluation report.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct EvalReport {
    pub n_eval: usize,
    pub n_positive: usize,
    pub n_negative: usize,
    pub auc: f32,
    pub brier: f32,
    /// Binary accuracy at threshold 0.5.
    pub accuracy_at_half: f32,
}

/// Read a JSONL labels file. Each line is a [`LabeledRow`].
pub fn load_labels(path: &Path) -> Result<Vec<LabeledRow>> {
    let f = File::open(path).with_context(|| format!("opening labels {}", path.display()))?;
    let r = BufReader::new(f);
    let mut out: Vec<LabeledRow> = Vec::new();
    for (i, line) in r.lines().enumerate() {
        let line = line.with_context(|| format!("reading line {}", i + 1))?;
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let row: LabeledRow =
            serde_json::from_str(trimmed).with_context(|| format!("parsing line {}", i + 1))?;
        if row.schema_version != Features::SCHEMA_VERSION {
            return Err(anyhow!(
                "labels file schema_version {} does not match runtime {}; \
                 regenerate with `phase2-label` after the version bump",
                row.schema_version,
                Features::SCHEMA_VERSION
            ));
        }
        out.push(row);
    }
    Ok(out)
}

/// Write a JSONL labels file. Used by the `phase2-label` binary.
pub fn save_labels(rows: &[LabeledRow], path: &Path) -> Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        create_dir_all(parent)?;
    }
    let f = File::create(path)?;
    let mut w = BufWriter::new(f);
    for r in rows {
        let line = serde_json::to_string(r)?;
        w.write_all(line.as_bytes())?;
        w.write_all(b"\n")?;
    }
    w.flush()?;
    Ok(())
}

/// Split a row vector into `(train, eval)` using a deterministic
/// shuffle. When `cfg.eval_split == 0.0`, returns `(rows, &[])`.
pub fn split_train_eval(
    rows: &[LabeledRow],
    cfg: &TrainConfig,
) -> (Vec<LabeledRow>, Vec<LabeledRow>) {
    if cfg.eval_split <= 0.0 || rows.is_empty() {
        return (rows.to_vec(), Vec::new());
    }
    let mut idx: Vec<usize> = (0..rows.len()).collect();
    let mut rng = StdRng::seed_from_u64(cfg.seed);
    idx.shuffle(&mut rng);
    let n_eval = ((rows.len() as f32) * cfg.eval_split).round() as usize;
    let n_eval = n_eval.min(rows.len().saturating_sub(1)).max(1);
    let mut train: Vec<LabeledRow> = Vec::with_capacity(rows.len() - n_eval);
    let mut eval: Vec<LabeledRow> = Vec::with_capacity(n_eval);
    for (k, &i) in idx.iter().enumerate() {
        if k < n_eval {
            eval.push(rows[i].clone());
        } else {
            train.push(rows[i].clone());
        }
    }
    (train, eval)
}

/// Train a pointwise log-likelihood GBDT on the labeled rows.
///
/// Internally maps the labels from the wire `{0.0, 1.0}` representation
/// to gbdt's `LogLikelyhood` convention `{-1.0, +1.0}` (the loss
/// function treats the residual sign as the gradient direction; passing
/// 0.0 there causes the optimiser to never push predictions across
/// the 0.5 boundary). The model's `predict` output is still in
/// `[0, 1]` per the gbdt sigmoid wrapper.
pub fn train_pointwise(rows: &[LabeledRow], cfg: TrainConfig) -> Result<LearnedScorer> {
    if rows.is_empty() {
        return Err(anyhow!("empty Phase 2 training set"));
    }
    let mut data: DataVec = rows
        .iter()
        .map(|r| {
            let gbdt_label = if r.label > 0.5 { 1.0_f32 } else { -1.0_f32 };
            Data {
                feature: r.features.to_row(),
                target: gbdt_label,
                weight: 1.0,
                label: gbdt_label,
                residual: 0.0,
                initial_guess: 0.0,
            }
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
    Ok(LearnedScorer::from_inner(g))
}

/// Held-out evaluation: AUC + Brier + accuracy@0.5.
#[must_use]
pub fn evaluate(model: &LearnedScorer, eval: &[LabeledRow]) -> EvalReport {
    if eval.is_empty() {
        return EvalReport::default();
    }
    let mut report = EvalReport {
        n_eval: eval.len(),
        ..EvalReport::default()
    };

    // Predictions + labels.
    let mut pairs: Vec<(f32, f32)> = Vec::with_capacity(eval.len());
    let mut brier_sum = 0.0_f64;
    let mut correct_at_half = 0usize;
    for r in eval {
        let p = model.predict_p(&r.features);
        pairs.push((p, r.label));
        let d = (p - r.label) as f64;
        brier_sum += d * d;
        if r.label > 0.5 {
            report.n_positive += 1;
        } else {
            report.n_negative += 1;
        }
        let predicted_pos = p >= 0.5;
        let actual_pos = r.label > 0.5;
        if predicted_pos == actual_pos {
            correct_at_half += 1;
        }
    }
    report.brier = (brier_sum / pairs.len() as f64) as f32;
    report.accuracy_at_half = correct_at_half as f32 / pairs.len() as f32;
    report.auc = roc_auc(&pairs);
    report
}

/// ROC AUC via the Mann-Whitney U statistic. O(n log n) sort + O(n)
/// rank sum.
fn roc_auc(pairs: &[(f32, f32)]) -> f32 {
    let n_pos = pairs.iter().filter(|(_, l)| *l > 0.5).count();
    let n_neg = pairs.len() - n_pos;
    if n_pos == 0 || n_neg == 0 {
        // Degenerate case — undefined AUC; report 0.5 (no information).
        return 0.5;
    }
    // Sort ascending by score; assign mid-rank for ties.
    let mut sorted: Vec<(f32, f32)> = pairs.to_vec();
    sorted.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    // Rank assignment with tie averaging.
    let n = sorted.len();
    let mut ranks = vec![0.0_f64; n];
    let mut i = 0;
    while i < n {
        let mut j = i + 1;
        while j < n && sorted[j].0 == sorted[i].0 {
            j += 1;
        }
        let avg_rank = (i + j + 1) as f64 / 2.0; // ranks 1-based
        for slot in &mut ranks[i..j] {
            *slot = avg_rank;
        }
        i = j;
    }
    // Sum of ranks of positives.
    let mut sum_pos_rank = 0.0_f64;
    for (k, (_, l)) in sorted.iter().enumerate() {
        if *l > 0.5 {
            sum_pos_rank += ranks[k];
        }
    }
    let n_pos_f = n_pos as f64;
    let n_neg_f = n_neg as f64;
    // U_pos = sum_pos_rank - n_pos*(n_pos+1)/2
    let u = sum_pos_rank - n_pos_f * (n_pos_f + 1.0) / 2.0;
    let auc = u / (n_pos_f * n_neg_f);
    auc as f32
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::phase2_features::Features;

    fn row(label: f32, blocker: bool, cost: f32) -> LabeledRow {
        let f = Features {
            has_blocker: if blocker { 1.0 } else { 0.0 },
            static_cost_fraction: cost,
            role_postcode: if blocker { 0.0 } else { -1.0 },
            country_posterior: 0.95,
            hypothesis_logprob: -0.1,
            claims_postcode: if blocker { 1.0 } else { 0.0 },
            claims_street: 1.0,
            max_postings_log: 4.0,
            min_postings_log: 2.0,
            ..Features::default()
        };
        LabeledRow {
            schema_version: Features::SCHEMA_VERSION,
            features: f,
            label,
        }
    }

    #[test]
    fn auc_perfect_separation() {
        let pairs = vec![(0.1, 0.0), (0.2, 0.0), (0.8, 1.0), (0.9, 1.0)];
        let auc = roc_auc(&pairs);
        assert!((auc - 1.0).abs() < 1e-6, "got {auc}");
    }

    #[test]
    fn auc_random_is_half() {
        let pairs = vec![(0.5, 0.0), (0.5, 1.0), (0.5, 0.0), (0.5, 1.0)];
        let auc = roc_auc(&pairs);
        assert!((auc - 0.5).abs() < 1e-6, "got {auc}");
    }

    #[test]
    fn auc_degenerate_one_class() {
        let all_pos = vec![(0.5, 1.0), (0.7, 1.0)];
        let all_neg = vec![(0.5, 0.0), (0.7, 0.0)];
        assert!((roc_auc(&all_pos) - 0.5).abs() < 1e-6);
        assert!((roc_auc(&all_neg) - 0.5).abs() < 1e-6);
    }

    #[test]
    fn split_deterministic_with_seed() {
        let rows: Vec<LabeledRow> = (0..100)
            .map(|i| row(if i % 2 == 0 { 1.0 } else { 0.0 }, true, 0.1))
            .collect();
        let cfg = TrainConfig {
            eval_split: 0.2,
            seed: 7,
            ..TrainConfig::default()
        };
        let (t1, e1) = split_train_eval(&rows, &cfg);
        let (t2, e2) = split_train_eval(&rows, &cfg);
        assert_eq!(t1.len(), 80);
        assert_eq!(e1.len(), 20);
        // Determinism: same seed → same partition.
        let labels1: Vec<f32> = t1.iter().map(|r| r.label).collect();
        let labels2: Vec<f32> = t2.iter().map(|r| r.label).collect();
        assert_eq!(labels1, labels2);
        let labels_e1: Vec<f32> = e1.iter().map(|r| r.label).collect();
        let labels_e2: Vec<f32> = e2.iter().map(|r| r.label).collect();
        assert_eq!(labels_e1, labels_e2);
    }

    #[test]
    fn save_then_load_labels_round_trip() {
        let rows = vec![row(1.0, true, 0.1), row(0.0, false, 0.9)];
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("labels.jsonl");
        save_labels(&rows, &path).unwrap();
        let loaded = load_labels(&path).unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].label, 1.0);
        assert_eq!(loaded[1].label, 0.0);
    }

    #[test]
    fn load_rejects_wrong_schema() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.jsonl");
        let f = File::create(&path).unwrap();
        let mut w = BufWriter::new(f);
        // Hand-craft a row with schema_version=999 (not the runtime).
        let feat = Features {
            has_blocker: 1.0,
            ..Features::default()
        };
        let bad = LabeledRow {
            schema_version: 999,
            features: feat,
            label: 1.0,
        };
        let line = serde_json::to_string(&bad).unwrap();
        w.write_all(line.as_bytes()).unwrap();
        w.write_all(b"\n").unwrap();
        w.flush().unwrap();
        let err = load_labels(&path).unwrap_err();
        assert!(
            err.to_string().contains("schema_version"),
            "expected schema mismatch, got: {err}"
        );
    }

    #[test]
    fn train_tiny_corpus_separates_blockers() {
        // Synthetic corpus: 200 rows.
        //   - blocker=true, cost=0.05 → label=1.0 (good search direction)
        //   - blocker=false, cost=0.6 → label=0.0 (bad search direction)
        // The GBDT should achieve near-perfect AUC on this trivial split.
        //
        // We add jitter so the GBDT actually has multiple split points
        // — a perfectly-degenerate corpus (every positive identical,
        // every negative identical) gives the splitter no signal.
        use rand::Rng;
        let mut rng = StdRng::seed_from_u64(42);
        let mut rows: Vec<LabeledRow> = Vec::new();
        for _ in 0..200 {
            let mut r = row(1.0, true, 0.05 + rng.random::<f32>() * 0.1);
            r.features.country_posterior = 0.9 + rng.random::<f32>() * 0.05;
            r.features.hypothesis_logprob = -0.1 - rng.random::<f32>() * 0.1;
            r.features.max_postings_log = 4.0 + rng.random::<f32>();
            rows.push(r);
        }
        for _ in 0..200 {
            let mut r = row(0.0, false, 0.6 + rng.random::<f32>() * 0.2);
            r.features.country_posterior = 0.5 + rng.random::<f32>() * 0.1;
            r.features.hypothesis_logprob = -1.0 - rng.random::<f32>() * 0.5;
            r.features.max_postings_log = 11.0 + rng.random::<f32>();
            rows.push(r);
        }
        let cfg = TrainConfig {
            n_trees: 50,
            max_depth: 4,
            eval_split: 0.2,
            seed: 7,
            ..TrainConfig::default()
        };
        let (train, eval) = split_train_eval(&rows, &cfg);
        let model = train_pointwise(&train, cfg).unwrap();
        let report = evaluate(&model, &eval);
        assert!(
            report.auc > 0.85,
            "expected AUC > 0.85 on trivial split, got {}",
            report.auc
        );
        assert!(
            report.accuracy_at_half > 0.85,
            "expected acc > 0.85 on trivial split, got {} (AUC={})",
            report.accuracy_at_half,
            report.auc
        );
    }

    #[test]
    fn save_then_load_model() {
        // Round-trip a trained model file.
        let mut rows: Vec<LabeledRow> = Vec::new();
        for _ in 0..50 {
            rows.push(row(1.0, true, 0.05));
        }
        for _ in 0..50 {
            rows.push(row(0.0, false, 0.6));
        }
        let cfg = TrainConfig {
            n_trees: 10,
            max_depth: 3,
            eval_split: 0.0,
            ..TrainConfig::default()
        };
        let model = train_pointwise(&rows, cfg).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rerank.gbdt");
        model.save(&path).unwrap();
        let loaded = LearnedScorer::load(&path).unwrap();
        // Sanity: reload predicts approximately the same.
        let f = rows[0].features.clone();
        let p1 = model.predict_p(&f);
        let p2 = loaded.predict_p(&f);
        assert!((p1 - p2).abs() < 1e-4, "got p1={p1} p2={p2}");
    }
}
