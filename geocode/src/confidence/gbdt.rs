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
//! Models are wrapped in a small versioned envelope so a feature-schema
//! bump does not silently load with the wrong column semantics:
//!
//! ```text
//! [u32 LE: magic "BFGB"]
//! [u32 LE: schema_version (matches Features::SCHEMA_VERSION)]
//! [u32 LE: gbdt_payload_offset (bytes from start of file)]
//! [u32 LE: reserved, must be 0]
//! ... gbdt-crate native serialization at gbdt_payload_offset ...
//! ```
//!
//! [`GbdtModel::load`] refuses to load a file with a mismatched
//! `schema_version`. Older bare-payload files (no envelope) are
//! transparently accepted for backwards compatibility — the file is
//! treated as schema_version = `Features::SCHEMA_VERSION` if the
//! magic header is absent. Once the schema bumps for the first time
//! these legacy files become uninterpretable and must be retrained.
//!
//! ## Inference path
//!
//! [`GbdtModel::predict_one`] calls into `gbdt::GBDT::predict` with a
//! one-element `DataVec`, which is the path benchmarked in
//! `GBDT_DECISION.md`.

use std::io::{Read, Write};
use std::path::Path;

use anyhow::{Context, Result, anyhow};
use gbdt::decision_tree::{Data, DataVec};
use gbdt::gradient_boost::GBDT;

use super::features::Features;
use crate::geocoder::executor::GeocodedResult;

/// Magic bytes identifying a butterfly-geocode GBDT envelope.
/// Spelled `BFGB` (Butterfly Geocode Boost) in little-endian.
const ENVELOPE_MAGIC: [u8; 4] = *b"BFGB";

/// Total length of the on-disk envelope header.
///
/// Layout:
///   [0..4)  magic (`BFGB`)
///   [4..8)  schema_version (u32 LE)
///   [8..12) gbdt_payload_offset (u32 LE; always 16 in this version)
///  [12..16) reserved, must be zero
const ENVELOPE_HEADER_BYTES: usize = 16;

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
    ///
    /// Recognises both the modern envelope format (16-byte
    /// `BFGB`-prefixed header followed by the gbdt-crate native
    /// payload) and the legacy bare-payload format. A modern envelope
    /// with a mismatched `schema_version` is **rejected** to prevent
    /// loading a model trained against a different feature schema.
    pub fn load(path: &Path) -> Result<Self> {
        let mut f = std::fs::File::open(path)
            .with_context(|| format!("opening GBDT model {}", path.display()))?;
        let mut head = [0u8; ENVELOPE_HEADER_BYTES];
        let nread = read_up_to(&mut f, &mut head)?;
        let has_envelope = nread == ENVELOPE_HEADER_BYTES && head[0..4] == ENVELOPE_MAGIC;

        if has_envelope {
            let schema_version = u32::from_le_bytes(
                head[4..8]
                    .try_into()
                    .expect("4 bytes from a 16-byte buffer"),
            );
            let payload_offset = u32::from_le_bytes(
                head[8..12]
                    .try_into()
                    .expect("4 bytes from a 16-byte buffer"),
            );
            let reserved = u32::from_le_bytes(
                head[12..16]
                    .try_into()
                    .expect("4 bytes from a 16-byte buffer"),
            );
            if reserved != 0 {
                return Err(anyhow!(
                    "GBDT model {} has non-zero reserved header field ({reserved:#010x}); \
                     header bytes [12..16) must be zero — file is corrupt or from a future format",
                    path.display()
                ));
            }
            if schema_version != Features::SCHEMA_VERSION {
                return Err(anyhow!(
                    "GBDT model {} declares feature schema_version {} but this build expects {} — retrain the model",
                    path.display(),
                    schema_version,
                    Features::SCHEMA_VERSION
                ));
            }
            if payload_offset as usize != ENVELOPE_HEADER_BYTES {
                return Err(anyhow!(
                    "GBDT model {} has unexpected payload offset {}; expected {}",
                    path.display(),
                    payload_offset,
                    ENVELOPE_HEADER_BYTES
                ));
            }
            // Strip the envelope into a tempfile, then hand the payload
            // path to the gbdt crate's loader (it only accepts paths).
            let tmp = tempfile::NamedTempFile::new()
                .with_context(|| "creating tempfile for GBDT payload")?;
            let mut payload = Vec::new();
            f.read_to_end(&mut payload)
                .with_context(|| format!("reading GBDT payload from {}", path.display()))?;
            std::fs::write(tmp.path(), &payload)
                .with_context(|| "writing GBDT payload to tempfile")?;
            let s = tmp
                .path()
                .to_str()
                .context("tempfile path is not valid UTF-8")?;
            let inner = GBDT::load_model(s)
                .map_err(|e| anyhow!("loading GBDT model from {}: {}", path.display(), e))?;
            Ok(Self { inner })
        } else {
            // Legacy bare-payload format. Treated as the current schema
            // version. Once the schema bumps these will need retraining.
            let s = path.to_str().context("model path is not valid UTF-8")?;
            let inner = GBDT::load_model(s)
                .map_err(|e| anyhow!("loading GBDT model from {}: {}", path.display(), e))?;
            Ok(Self { inner })
        }
    }

    /// Persist a trained model to disk in the envelope format.
    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating model directory {}", parent.display()))?;
        }
        // Write the gbdt payload to a tempfile, then concatenate
        // [envelope header][payload] into the destination path.
        let tmp =
            tempfile::NamedTempFile::new().with_context(|| "creating tempfile for GBDT payload")?;
        let payload_path = tmp
            .path()
            .to_str()
            .context("tempfile path is not valid UTF-8")?;
        self.inner
            .save_model(payload_path)
            .map_err(|e| anyhow!("saving GBDT payload: {e}"))?;
        let payload = std::fs::read(tmp.path()).with_context(|| "reading GBDT payload tempfile")?;

        // Crash-safe write: temp file in same directory, fsync, then
        // atomic rename. A mid-write crash leaves either the old file
        // intact or no temp file at all — never a corrupt destination.
        let parent = path.parent().unwrap_or_else(|| Path::new("."));
        let file_name = path.file_name().and_then(|s| s.to_str()).unwrap_or("model");
        let tmp_name = format!("{file_name}.tmp.{}", std::process::id());
        let tmp_path = parent.join(tmp_name);

        let mut out = std::fs::File::create(&tmp_path)
            .with_context(|| format!("creating temp file {}", tmp_path.display()))?;
        let mut header = [0u8; ENVELOPE_HEADER_BYTES];
        header[0..4].copy_from_slice(&ENVELOPE_MAGIC);
        header[4..8].copy_from_slice(&Features::SCHEMA_VERSION.to_le_bytes());
        header[8..12].copy_from_slice(&(ENVELOPE_HEADER_BYTES as u32).to_le_bytes());
        // bytes 12..16 are reserved zeros (left as the zero-init from above).
        let write_result = (|| -> Result<()> {
            out.write_all(&header)
                .with_context(|| format!("writing envelope header to {}", tmp_path.display()))?;
            out.write_all(&payload)
                .with_context(|| format!("writing GBDT payload to {}", tmp_path.display()))?;
            out.sync_all()
                .with_context(|| format!("fsyncing {}", tmp_path.display()))?;
            Ok(())
        })();
        if let Err(e) = write_result {
            // Best-effort cleanup; ignore unlink failure (the temp file
            // is harmless, and the original path is still untouched).
            let _ = std::fs::remove_file(&tmp_path);
            return Err(e);
        }
        drop(out);
        std::fs::rename(&tmp_path, path).with_context(|| {
            format!(
                "atomically renaming {} -> {}",
                tmp_path.display(),
                path.display()
            )
        })?;
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

    /// Score a single raw feature row (any schema). Generic entry
    /// point used by the recall+rerank pipeline (#205) where the
    /// feature struct lives in `geocoder/rerank.rs` and is decoupled
    /// from the legacy [`Features`] schema.
    #[must_use]
    pub fn predict_one_raw(&self, row: &[f32]) -> f32 {
        let datum = Data {
            feature: row.to_vec(),
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

/// Read up to `buf.len()` bytes, tolerating short files. Returns the
/// actual number of bytes read.
fn read_up_to<R: Read>(r: &mut R, buf: &mut [u8]) -> Result<usize> {
    let mut total = 0;
    while total < buf.len() {
        match r.read(&mut buf[total..]) {
            Ok(0) => break,
            Ok(n) => total += n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e.into()),
        }
    }
    Ok(total)
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
    fn load_rejects_nonzero_reserved_bytes() {
        // Build a model file with a corrupted reserved field.
        let dir = tempfile::tempdir().unwrap();
        let good = dir.path().join("good.gbdt");
        let bad = dir.path().join("bad.gbdt");
        let model = synthetic_model();
        model.save(&good).unwrap();
        let mut bytes = std::fs::read(&good).unwrap();
        // Bytes [12..16) are the reserved field; flip them.
        bytes[12] = 0xDE;
        bytes[13] = 0xAD;
        bytes[14] = 0xBE;
        bytes[15] = 0xEF;
        std::fs::write(&bad, &bytes).unwrap();
        let err = GbdtModel::load(&bad).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("non-zero reserved"),
            "expected 'non-zero reserved' diagnostic, got: {msg}"
        );
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
