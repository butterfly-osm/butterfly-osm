//! GBDT inference wrapper around the `gbdt` crate.
//!
//! The model is a 100-tree, depth-6 boosted ensemble producing a scalar
//! score in `[0, 1]` (logistic output if trained with `LogLikelyhood`).
//! Inference latency is ~1.12 µs p50 / 1.81 µs p99 for a single row on
//! commodity x86_64 (per `geocode-research/scratch-gbdt/`), comfortably
//! inside the per-query reranker budget.
//!
//! ## Model file format
//!
//! We use the gbdt crate's native `save_model` / `load_model` JSON-ish
//! format. Models are versioned in their directory name
//! (`rerank-belgium-tiny.gbdt`). When the schema changes
//! ([`crate::confidence::features::Features::SCHEMA_VERSION`] bump), we
//! drop the old file and retrain.
//!
//! ## Inference path
//!
//! [`GbdtModel::predict_one`] calls into `gbdt::GBDT::predict` with a
//! one-element `DataVec`, which is the path benchmarked in
//! `GBDT_DECISION.md`.

use std::path::Path;

use anyhow::{Context, Result};
use gbdt::decision_tree::{Data, DataVec};
use gbdt::gradient_boost::GBDT;

use super::features::Features;
use crate::geocoder::executor::GeocodedResult;

/// Owned trained reranker.
pub struct GbdtModel {
    inner: GBDT,
}

impl std::fmt::Debug for GbdtModel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GbdtModel").finish_non_exhaustive()
    }
}

impl GbdtModel {
    /// Wrap an already-trained `GBDT` instance. Used by the trainer.
    #[must_use]
    pub fn from_inner(inner: GBDT) -> Self {
        Self { inner }
    }

    /// Load a trained model from disk.
    pub fn load(path: &Path) -> Result<Self> {
        let s = path.to_str().context("model path is not valid UTF-8")?;
        let inner = GBDT::load_model(s)
            .map_err(|e| anyhow::anyhow!("loading GBDT model from {}: {}", path.display(), e))?;
        Ok(Self { inner })
    }

    /// Persist a trained model to disk.
    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating model directory {}", parent.display()))?;
        }
        let s = path.to_str().context("model path is not valid UTF-8")?;
        self.inner
            .save_model(s)
            .map_err(|e| anyhow::anyhow!("saving GBDT model to {}: {}", path.display(), e))?;
        Ok(())
    }

    /// Score a single feature row. Returns the GBDT raw output (sigmoid
    /// for `LogLikelyhood` training).
    #[must_use]
    pub fn predict_one(&self, f: &Features) -> f32 {
        let row = f.to_row();
        let datum = Data {
            feature: row,
            target: 0.0,
            weight: 1.0,
            label: 0.0,
            residual: 0.0,
            initial_guess: 0.0,
        };
        let out: Vec<f32> = self.inner.predict(&vec![datum]);
        out.first().copied().unwrap_or(0.0)
    }

    /// Score a batch of feature rows. Used in eval/training paths;
    /// the executor's hot path uses `predict_one` per candidate.
    #[must_use]
    pub fn predict_batch(&self, rows: &[Features]) -> Vec<f32> {
        let data: DataVec = rows
            .iter()
            .map(|f| Data {
                feature: f.to_row(),
                target: 0.0,
                weight: 1.0,
                label: 0.0,
                residual: 0.0,
                initial_guess: 0.0,
            })
            .collect();
        self.inner.predict(&data)
    }
}

/// Rerank `candidates` in place by GBDT score. Order is preserved for
/// ties (stable sort). Each candidate's `score` field is overwritten
/// with the GBDT score, and `RERANK_GBDT` is appended to its
/// `reason_codes`.
pub fn rerank(
    candidates: &mut [GeocodedResult],
    features_per_candidate: &[Features],
    model: &GbdtModel,
) {
    debug_assert_eq!(
        candidates.len(),
        features_per_candidate.len(),
        "candidates and features must align"
    );
    if candidates.is_empty() {
        return;
    }

    let mut indexed: Vec<(usize, f32)> = features_per_candidate
        .iter()
        .enumerate()
        .map(|(i, f)| (i, model.predict_one(f)))
        .collect();

    // Apply scores + reason code without reordering yet.
    for (i, score) in &indexed {
        let cand = &mut candidates[*i];
        cand.score = *score;
        if !cand.reason_codes.iter().any(|r| r == "RERANK_GBDT") {
            cand.reason_codes
                .push(std::borrow::Cow::Borrowed("RERANK_GBDT"));
        }
    }

    // Stable sort indices by score descending.
    indexed.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(a.0.cmp(&b.0))
    });

    // Permute candidates into score order using the index permutation.
    apply_permutation(candidates, &indexed);
}

fn apply_permutation(candidates: &mut [GeocodedResult], indexed: &[(usize, f32)]) {
    if candidates.len() <= 1 {
        return;
    }
    // Build a destination vector via take-and-replace. We allocate one
    // small `Vec<Option<GeocodedResult>>` here — this is OK because
    // rerank is only invoked when a model is configured AND the
    // candidate set is the multi-result path; the clean-zero-cost NFR
    // is preserved by skipping rerank entirely when the model is None.
    let n = candidates.len();
    let mut slots: Vec<Option<GeocodedResult>> =
        candidates.iter_mut().map(|c| Some(c.clone())).collect();
    let mut reordered: Vec<GeocodedResult> = Vec::with_capacity(n);
    for (src, _) in indexed {
        let item = slots[*src].take().expect("each index visited once");
        reordered.push(item);
    }
    for (dst, src) in candidates.iter_mut().zip(reordered) {
        *dst = src;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gbdt::config::Config;

    /// Tiny self-contained LCG so tests don't pull `rand` into the
    /// dep graph. Seeded determinism is sufficient for smoke tests.
    fn lcg(state: &mut u64) -> f32 {
        *state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        ((*state >> 33) as f32) / ((1u32 << 31) as f32)
    }

    fn synthetic_model() -> GbdtModel {
        let mut s: u64 = 0xBEE;
        let mut data: DataVec = (0..200)
            .map(|_| {
                let feats: Vec<f32> = (0..super::super::features::N_FEATURES)
                    .map(|_| lcg(&mut s))
                    .collect();
                // synthetic: positive iff first feature dominant
                let label = if feats[0] + feats[8] > 1.0 { 1.0 } else { 0.0 };
                Data {
                    feature: feats,
                    target: label,
                    weight: 1.0,
                    label,
                    residual: 0.0,
                    initial_guess: 0.0,
                }
            })
            .collect();
        let mut cfg = Config::new();
        cfg.set_feature_size(super::super::features::N_FEATURES);
        cfg.set_max_depth(4);
        cfg.set_iterations(20);
        cfg.set_shrinkage(0.1);
        cfg.set_loss("LogLikelyhood");
        cfg.set_data_sample_ratio(1.0);
        cfg.set_feature_sample_ratio(1.0);
        cfg.set_training_optimization_level(2);
        let mut g = GBDT::new(&cfg);
        g.fit(&mut data);
        GbdtModel::from_inner(g)
    }

    #[test]
    fn predict_one_returns_finite() {
        let model = synthetic_model();
        let f = Features::default();
        let s = model.predict_one(&f);
        assert!(s.is_finite(), "got {s}");
    }

    #[test]
    fn rerank_orders_by_predicted_score() {
        let model = synthetic_model();
        let mut feats = vec![Features::default(); 3];
        feats[0].parser_confidence = 0.0;
        feats[0].street_fuzzy_score = 0.0;
        feats[1].parser_confidence = 1.0;
        feats[1].street_fuzzy_score = 1.0;
        feats[2].parser_confidence = 0.5;
        feats[2].street_fuzzy_score = 0.5;
        let mut cands: Vec<GeocodedResult> = (0..3)
            .map(|i| GeocodedResult {
                lat: 50.0,
                lon: 4.0,
                street: format!("st{i}"),
                housenumber: "1".into(),
                postcode: "1000".into(),
                locality: "loc".into(),
                score: 0.5,
                country: None,
                reason_codes: vec![],
            })
            .collect();
        rerank(&mut cands, &feats, &model);
        // Each got a RERANK_GBDT code.
        for c in &cands {
            assert!(c.reason_codes.iter().any(|r| r == "RERANK_GBDT"));
        }
        // Scores are now monotone non-increasing.
        for w in cands.windows(2) {
            assert!(w[0].score >= w[1].score, "{} >= {}", w[0].score, w[1].score);
        }
    }

    #[test]
    fn save_load_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("m.gbdt");
        let model = synthetic_model();
        model.save(&path).unwrap();
        let loaded = GbdtModel::load(&path).unwrap();
        let f = Features::default();
        let a = model.predict_one(&f);
        let b = loaded.predict_one(&f);
        assert!((a - b).abs() < 1e-5, "{a} vs {b}");
    }

    #[test]
    fn empty_rerank_no_panic() {
        let model = synthetic_model();
        let mut cands: Vec<GeocodedResult> = vec![];
        let feats: Vec<Features> = vec![];
        rerank(&mut cands, &feats, &model);
        assert!(cands.is_empty());
    }
}
