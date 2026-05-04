//! Neural parser backend (#96 §Tagger + #98 Phase 1).
//!
//! Wraps [`crate::tagger`] inference output through the retrieval-aware
//! decoder ([`super::decoding`]) to produce a [`ParsedQuery`].
//!
//! ## Lifecycle
//!
//! 1. [`NeuralParser::load`] — reads the safetensors file at the given
//!    path along with its sidecar `.config.json`. The sidecar carries
//!    the [`crate::tagger::transformer::ModelConfig`] so the architecture
//!    is rebuildable from disk.
//! 2. [`NeuralParser::parse`] — runs forward pass + decoding for one
//!    text. Pure function over `&self` and `&Shard`. Thread-safe.
//!
//! ## Country-prior merge
//!
//! The cheap classifier ([`crate::routing::classifier`]) emits a
//! posterior. The model's country head emits another. We **multiply**
//! them and renormalize, capping the result. Both are passed through
//! to the executor as `country_candidates`.

use std::path::{Path, PathBuf};

use anyhow::Result;
use candle_core::Device;

use crate::routing::{CountryId, classify_country};
use crate::shard::reader::Shard;
use crate::tagger::transformer::{ModelConfig, TaggerModel};
use crate::types::ParsedQuery;

use super::beam::BeamConfig;
use super::decoding::{DecodedQuery, UtilityConfig, decode, to_parsed_query};

/// Loaded neural parser. Single-process, single-thread inference (no
/// internal parallelism) — the server's request concurrency is what
/// keeps cores busy.
#[derive(Debug)]
pub struct NeuralParser {
    pub model_path: PathBuf,
    pub config: ModelConfig,
    model: TaggerModel,
    device: Device,
    pub beam_cfg: BeamConfig,
    pub util_cfg: UtilityConfig,
}

impl NeuralParser {
    /// Load from a safetensors path. Requires sidecar
    /// `<path>.config.json` next to the weights — written by
    /// [`crate::tagger::training::train_and_save`].
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
        let (model, cfg, device) = crate::tagger::training::load_model(path.as_ref())?;
        Ok(Self {
            model_path: path.as_ref().to_path_buf(),
            config: cfg,
            model,
            device,
            beam_cfg: BeamConfig::default(),
            util_cfg: UtilityConfig::default(),
        })
    }

    /// Parse a query string into a [`ParsedQuery`] using the model
    /// + #98 Phase 1 decoding.
    pub fn parse(&self, text: &str, shard: &Shard) -> Result<ParsedQuery> {
        let inference = crate::tagger::inference::infer(&self.model, text, &self.device)?;
        let decoded = decode(text, &inference, shard, &self.beam_cfg, &self.util_cfg);
        let country_candidates = merge_country_candidates(text, &inference.country_posterior);
        Ok(to_parsed_query(text, &decoded, country_candidates, 5))
    }

    /// Return the underlying [`DecodedQuery`] without converting it
    /// to a [`ParsedQuery`]. Useful for tests + observability.
    pub fn decode(&self, text: &str, shard: &Shard) -> Result<DecodedQuery> {
        let inference = crate::tagger::inference::infer(&self.model, text, &self.device)?;
        Ok(decode(
            text,
            &inference,
            shard,
            &self.beam_cfg,
            &self.util_cfg,
        ))
    }
}

/// Multiply the cheap classifier posterior by the model's country head
/// posterior, then renormalize. MVP: single country (BE) → both
/// collapse to `(BE, 1.0)`.
fn merge_country_candidates(text: &str, model_posterior: &[f32]) -> Vec<(CountryId, f32)> {
    let cheap = classify_country(text);
    if cheap.is_empty() {
        return cheap;
    }
    // Model is BE-only (n_countries==1) on the shipped tiny model.
    // For multi-country: multiply pairwise then normalize.
    let mut out: Vec<(CountryId, f32)> = Vec::with_capacity(cheap.len());
    for (idx, (cid, cheap_p)) in cheap.iter().enumerate() {
        let model_p = model_posterior.get(idx).copied().unwrap_or(1.0);
        out.push((*cid, cheap_p * model_p));
    }
    let total: f32 = out.iter().map(|(_, p)| *p).sum();
    if total > 0.0 {
        for (_, p) in out.iter_mut() {
            *p /= total;
        }
    } else {
        // Fall back to cheap classifier alone.
        return cheap;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shard::AddressRecord;
    use crate::shard::builder::build_shard;
    use crate::tagger::training::{TrainConfig, generate_belgium_synthetic, train_and_save};
    use tempfile::tempdir;

    fn small_shard() -> (tempfile::TempDir, Shard) {
        let dir = tempdir().unwrap();
        let path = dir.path().join("shard.bfgs");
        let addrs = vec![
            AddressRecord {
                street: "Rue Wayez".into(),
                housenumber: "122".into(),
                postcode: "1070".into(),
                locality: "Anderlecht".into(),
                lat: 50.834,
                lon: 4.314,
                ..Default::default()
            },
            AddressRecord {
                street: "Grote Markt".into(),
                housenumber: "1".into(),
                postcode: "2000".into(),
                locality: "Antwerpen".into(),
                lat: 51.221,
                lon: 4.401,
                ..Default::default()
            },
        ];
        build_shard(&path, crate::routing::CountryId::BE, addrs).unwrap();
        (dir, Shard::open(&path).unwrap())
    }

    #[test]
    fn neural_parser_round_trips_through_save_load() {
        let corpus = generate_belgium_synthetic(64, 0xCAFE);
        let tcfg = TrainConfig {
            epochs: 2,
            batch_size: 8,
            ..Default::default()
        };
        let dir = tempdir().unwrap();
        let out = dir.path().join("tiny.safetensors");
        let cfg = ModelConfig::tiny();
        let _ = train_and_save(cfg, tcfg, &corpus, &out).unwrap();
        let parser = NeuralParser::load(&out).unwrap();
        let (_sd, shard) = small_shard();
        let parsed = parser
            .parse("Rue Wayez 122 1070 Anderlecht", &shard)
            .unwrap();
        assert!(!parsed.hypotheses.is_empty());
        // Multi-country classifier returns one entry per supported
        // country. The merged neural posterior may stay flat for the
        // shipped tiny model (single-country output head) — the
        // top-1 country must still be Belgium given the input text.
        assert!(!parsed.country_candidates.is_empty());
        assert_eq!(parsed.country_candidates[0].0, CountryId::BE);
    }
}
