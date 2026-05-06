//! GBDT inference wrapper around the `gbdt` crate (#205).
//!
//! Trained models are wrapped in a small versioned envelope so a
//! feature-schema bump does not silently load with the wrong column
//! semantics:
//!
//! ```text
//! [u32 LE: magic "BFGB"]
//! [u32 LE: schema_version]
//! [u32 LE: gbdt_payload_offset (always 16 in this version)]
//! [u32 LE: reserved, must be 0]
//! ... gbdt-crate native serialization at gbdt_payload_offset ...
//! ```
//!
//! Schema version is the rerank-features version
//! ([`crate::geocoder::rerank::RerankFeatures::SCHEMA_VERSION`]).
//! Bumping it requires retraining.

use std::io::{Read, Write};
use std::path::Path;

use anyhow::{Context, Result, anyhow};
use gbdt::decision_tree::{Data, DataVec};
use gbdt::gradient_boost::GBDT;

use crate::geocoder::rerank::RerankFeatures;

/// Magic bytes identifying a butterfly-geocode GBDT envelope (`BFGB`).
const ENVELOPE_MAGIC: [u8; 4] = *b"BFGB";

/// Total length of the on-disk envelope header.
const ENVELOPE_HEADER_BYTES: usize = 16;

/// Owned trained GBDT.
pub struct GbdtModel {
    inner: GBDT,
}

impl std::fmt::Debug for GbdtModel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GbdtModel").finish_non_exhaustive()
    }
}

impl GbdtModel {
    #[must_use]
    pub fn from_inner(inner: GBDT) -> Self {
        Self { inner }
    }

    /// Load a trained model from disk. Recognises both the modern
    /// envelope format and the legacy bare-payload format. A mismatched
    /// `schema_version` is rejected.
    pub fn load(path: &Path) -> Result<Self> {
        let mut f = std::fs::File::open(path)
            .with_context(|| format!("opening GBDT model {}", path.display()))?;
        let mut head = [0u8; ENVELOPE_HEADER_BYTES];
        let nread = read_up_to(&mut f, &mut head)?;
        let has_envelope = nread == ENVELOPE_HEADER_BYTES && head[0..4] == ENVELOPE_MAGIC;

        if has_envelope {
            let schema_version = u32::from_le_bytes(head[4..8].try_into().unwrap());
            let payload_offset = u32::from_le_bytes(head[8..12].try_into().unwrap());
            let reserved = u32::from_le_bytes(head[12..16].try_into().unwrap());
            if reserved != 0 {
                return Err(anyhow!(
                    "GBDT model {} has non-zero reserved header field ({reserved:#010x}); \
                     header bytes [12..16) must be zero — file is corrupt or from a future format",
                    path.display()
                ));
            }
            if schema_version != RerankFeatures::SCHEMA_VERSION {
                return Err(anyhow!(
                    "GBDT model {} declares feature schema_version {} but this build expects {} — retrain the model",
                    path.display(),
                    schema_version,
                    RerankFeatures::SCHEMA_VERSION
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

        let parent = path.parent().unwrap_or_else(|| Path::new("."));
        let file_name = path.file_name().and_then(|s| s.to_str()).unwrap_or("model");
        let tmp_name = format!("{file_name}.tmp.{}", std::process::id());
        let tmp_path = parent.join(tmp_name);

        let mut out = std::fs::File::create(&tmp_path)
            .with_context(|| format!("creating temp file {}", tmp_path.display()))?;
        let mut header = [0u8; ENVELOPE_HEADER_BYTES];
        header[0..4].copy_from_slice(&ENVELOPE_MAGIC);
        header[4..8].copy_from_slice(&RerankFeatures::SCHEMA_VERSION.to_le_bytes());
        header[8..12].copy_from_slice(&(ENVELOPE_HEADER_BYTES as u32).to_le_bytes());
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

    /// Score a single raw feature row.
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

    /// Score a batch of raw feature rows.
    #[must_use]
    pub fn predict_batch_raw(&self, rows: &[Vec<f32>]) -> Vec<f32> {
        let data: DataVec = rows
            .iter()
            .map(|r| Data {
                feature: r.clone(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use gbdt::config::Config;

    fn lcg(state: &mut u64) -> f32 {
        *state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        ((*state >> 33) as f32) / ((1u32 << 31) as f32)
    }

    fn synthetic_model() -> GbdtModel {
        let mut s: u64 = 0xBEE;
        let n_feats = RerankFeatures::N;
        let mut data: DataVec = (0..200)
            .map(|_| {
                let feats: Vec<f32> = (0..n_feats).map(|_| lcg(&mut s)).collect();
                let label = if feats[0] + feats[5] > 1.0 { 1.0 } else { 0.0 };
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
        cfg.set_feature_size(n_feats);
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
    fn predict_one_raw_finite() {
        let model = synthetic_model();
        let row = vec![0.0_f32; RerankFeatures::N];
        let s = model.predict_one_raw(&row);
        assert!(s.is_finite(), "got {s}");
    }

    #[test]
    fn save_load_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("m.gbdt");
        let model = synthetic_model();
        model.save(&path).unwrap();
        let loaded = GbdtModel::load(&path).unwrap();
        let row = vec![0.5_f32; RerankFeatures::N];
        let a = model.predict_one_raw(&row);
        let b = loaded.predict_one_raw(&row);
        assert!((a - b).abs() < 1e-5, "{a} vs {b}");
    }

    #[test]
    fn load_rejects_nonzero_reserved_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let good = dir.path().join("good.gbdt");
        let bad = dir.path().join("bad.gbdt");
        let model = synthetic_model();
        model.save(&good).unwrap();
        let mut bytes = std::fs::read(&good).unwrap();
        bytes[12] = 0xDE;
        bytes[13] = 0xAD;
        bytes[14] = 0xBE;
        bytes[15] = 0xEF;
        std::fs::write(&bad, &bytes).unwrap();
        let err = GbdtModel::load(&bad).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("non-zero reserved"));
    }
}
