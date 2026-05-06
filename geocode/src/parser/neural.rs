//! Neural tagger-signal emitter (#205).
//!
//! Wraps [`crate::tagger`] inference output as a
//! [`crate::geocoder::recall::TaggerSignals`] for the recall + rerank
//! pipeline. No field extraction, no `ParsedQuery` — that was deleted
//! in #205.
//!
//! ## Lifecycle
//!
//! 1. [`NeuralParser::load`] — reads the safetensors file and the
//!    sidecar `<path>.config.json` (architecture + country vocab).
//! 2. [`NeuralParser::signals`] — forward pass + posterior merge.

use std::path::{Path, PathBuf};

use anyhow::Result;
use candle_core::Device;

use crate::geocoder::recall::{N_BIO_LABELS, TaggerSignals};
use crate::routing::{CountryId, classify_country};
use crate::tagger::training::CountryVocab;
use crate::tagger::transformer::{ModelConfig, NUM_BIO_LABELS, TaggerModel};

/// Loaded neural tagger. Single-process, single-thread inference;
/// the server's request concurrency keeps cores busy.
pub struct NeuralParser {
    pub model_path: PathBuf,
    pub config: ModelConfig,
    pub country_vocab: CountryVocab,
    model: TaggerModel,
    device: Device,
}

impl std::fmt::Debug for NeuralParser {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NeuralParser")
            .field("model_path", &self.model_path)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl NeuralParser {
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::load_on(path, crate::tagger::training::DevicePref::Auto)
    }

    pub fn load_on<P: AsRef<Path>>(
        path: P,
        device_pref: crate::tagger::training::DevicePref,
    ) -> Result<Self> {
        let (model, cfg, country_vocab, device) =
            crate::tagger::training::load_model_on(path.as_ref(), device_pref)?;
        Ok(Self {
            model_path: path.as_ref().to_path_buf(),
            config: cfg,
            country_vocab,
            model,
            device,
        })
    }

    /// Run the model and return a [`TaggerSignals`] with merged
    /// per-byte BIO logits and per-country posterior.
    pub fn signals(&self, text: &str) -> Result<TaggerSignals> {
        let inference = crate::tagger::inference::infer(&self.model, text, &self.device)?;

        // Promote the [f32; NUM_BIO_LABELS] (= 9) logits into the
        // public-API shape [f32; N_BIO_LABELS] (= 9 — currently
        // identical; the constants are duplicated to decouple the
        // public surface from the tagger crate's internals).
        const _: [(); N_BIO_LABELS] = [(); NUM_BIO_LABELS];
        let bio_logits: Vec<[f32; N_BIO_LABELS]> = inference
            .bio_logprobs
            .into_iter()
            .map(|row| {
                let mut out = [0.0_f32; N_BIO_LABELS];
                let n = row.len().min(N_BIO_LABELS);
                out[..n].copy_from_slice(&row[..n]);
                out
            })
            .collect();

        let country_posterior =
            merge_country_candidates(text, &inference.country_posterior, &self.country_vocab);

        // Global confidence: mean per-byte top-1 BIO probability,
        // collapsed to `[0, 1]`.
        let global_confidence = if bio_logits.is_empty() {
            0.0
        } else {
            let mut acc = 0.0_f32;
            for row in &bio_logits {
                let mut best = f32::NEG_INFINITY;
                for &lp in row {
                    if lp > best {
                        best = lp;
                    }
                }
                acc += best.exp().clamp(0.0, 1.0);
            }
            acc / bio_logits.len() as f32
        };

        Ok(TaggerSignals {
            bio_logits,
            country_posterior,
            global_confidence,
        })
    }
}

fn merge_country_candidates(
    text: &str,
    model_posterior: &[f32],
    vocab: &CountryVocab,
) -> Vec<(CountryId, f32)> {
    let cheap = classify_country(text);
    if cheap.is_empty() {
        return cheap;
    }
    let mut out: Vec<(CountryId, f32)> = Vec::with_capacity(cheap.len());
    for (cid, cheap_p) in &cheap {
        let model_p = vocab
            .id_of(cid.as_str())
            .and_then(|id| model_posterior.get(id as usize).copied())
            .unwrap_or(1.0);
        out.push((*cid, cheap_p * model_p));
    }
    let total: f32 = out.iter().map(|(_, p)| *p).sum();
    if total > 0.0 {
        for (_, p) in out.iter_mut() {
            *p /= total;
        }
    } else {
        return cheap;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tagger::training::{
        CountryVocab, LrSchedule, TrainConfig, generate_belgium_synthetic, train_and_save,
    };
    use tempfile::tempdir;

    #[test]
    fn neural_parser_emits_signals() {
        let corpus = generate_belgium_synthetic(64, 0xCAFE);
        let tcfg = TrainConfig {
            epochs: 1,
            batch_size: 8,
            warmup_steps: 0,
            lr_schedule: LrSchedule::Constant,
            gradient_clip: None,
            ..Default::default()
        };
        let dir = tempdir().unwrap();
        let out = dir.path().join("tiny.safetensors");
        let cfg = ModelConfig::tiny();
        let vocab = CountryVocab::new(&["BE"]).unwrap();
        let _ = train_and_save(cfg, tcfg, &vocab, &corpus, &out).unwrap();
        let parser = NeuralParser::load(&out).unwrap();
        let signals = parser.signals("Rue Wayez 122 1070 Anderlecht").unwrap();
        assert!(!signals.bio_logits.is_empty(), "expected per-byte logits");
        assert!(!signals.country_posterior.is_empty());
        // BE should rank highly for a Belgian query.
        assert_eq!(signals.country_posterior[0].0, CountryId::BE);
    }
}
