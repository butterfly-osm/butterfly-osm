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
use std::sync::Arc;

use anyhow::Result;
use candle_core::Device;

use crate::routing::{CountryId, classify_country};
use crate::shard::reader::Shard;
use crate::tagger::training::CountryVocab;
use crate::tagger::transformer::{ModelConfig, TaggerModel};
use crate::types::ParsedQuery;

use super::beam::BeamConfig;
use super::decoding::{DecodedQuery, UtilityConfig, decode, decode_with_scorer, to_parsed_query};
use super::retrieval_utility::RetrievalUtilityScorer;

/// Loaded neural parser. Single-process, single-thread inference (no
/// internal parallelism) — the server's request concurrency is what
/// keeps cores busy.
///
/// The optional `retrieval_scorer` is the #98 Phase 2 trait-backed
/// scorer. When `Some`, the decoder uses [`decode_with_scorer`] (the
/// learned scorer takes precedence over the legacy [`UtilityConfig`]).
/// When `None`, the decoder falls back to the Phase 1 heuristic via
/// [`decode`] — this is the default `--retrieval-utility heuristic`
/// path.
pub struct NeuralParser {
    pub model_path: PathBuf,
    pub config: ModelConfig,
    /// Country vocabulary used at training time. Index `i` of the
    /// model's country head corresponds to `country_vocab[i]`.
    pub country_vocab: CountryVocab,
    model: TaggerModel,
    device: Device,
    pub beam_cfg: BeamConfig,
    pub util_cfg: UtilityConfig,
    /// Phase 2 scorer. When set, the decoder consults this trait
    /// object instead of the heuristic-derived [`UtilityConfig`].
    pub retrieval_scorer: Option<Arc<dyn RetrievalUtilityScorer>>,
}

impl std::fmt::Debug for NeuralParser {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NeuralParser")
            .field("model_path", &self.model_path)
            .field("config", &self.config)
            .field("beam_cfg", &self.beam_cfg)
            .field("util_cfg", &self.util_cfg)
            .field(
                "retrieval_scorer",
                &self
                    .retrieval_scorer
                    .as_ref()
                    .map(|s| s.name())
                    .unwrap_or("none"),
            )
            .finish_non_exhaustive()
    }
}

impl NeuralParser {
    /// Load from a safetensors path. Requires sidecar
    /// `<path>.config.json` next to the weights — written by
    /// [`crate::tagger::training::train_and_save`].
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
        let (model, cfg, country_vocab, device) =
            crate::tagger::training::load_model(path.as_ref())?;
        Ok(Self {
            model_path: path.as_ref().to_path_buf(),
            config: cfg,
            country_vocab,
            model,
            device,
            beam_cfg: BeamConfig::default(),
            util_cfg: UtilityConfig::default(),
            retrieval_scorer: None,
        })
    }

    /// Builder-style: install a Phase 2 retrieval-utility scorer.
    /// `None` reverts to the Phase 1 heuristic path.
    #[must_use]
    pub fn with_retrieval_scorer(
        mut self,
        scorer: Option<Arc<dyn RetrievalUtilityScorer>>,
    ) -> Self {
        self.retrieval_scorer = scorer;
        self
    }

    /// Parse a query string into a [`ParsedQuery`] using the model
    /// + #98 Phase 1 (or Phase 2 if a scorer is installed) decoding.
    pub fn parse(&self, text: &str, shard: &Shard) -> Result<ParsedQuery> {
        let inference = crate::tagger::inference::infer(&self.model, text, &self.device)?;
        let decoded = match &self.retrieval_scorer {
            Some(scorer) => {
                decode_with_scorer(text, &inference, shard, &self.beam_cfg, scorer.as_ref())
            }
            None => decode(text, &inference, shard, &self.beam_cfg, &self.util_cfg),
        };
        let country_candidates =
            merge_country_candidates(text, &inference.country_posterior, &self.country_vocab);
        Ok(to_parsed_query(text, &decoded, country_candidates, 5))
    }

    /// Return the underlying [`DecodedQuery`] without converting it
    /// to a [`ParsedQuery`]. Useful for tests + observability.
    pub fn decode(&self, text: &str, shard: &Shard) -> Result<DecodedQuery> {
        let inference = crate::tagger::inference::infer(&self.model, text, &self.device)?;
        Ok(match &self.retrieval_scorer {
            Some(scorer) => {
                decode_with_scorer(text, &inference, shard, &self.beam_cfg, scorer.as_ref())
            }
            None => decode(text, &inference, shard, &self.beam_cfg, &self.util_cfg),
        })
    }
}

/// Merge the cheap-classifier country posterior with the model's
/// country-head posterior.
///
/// The cheap classifier returns `Vec<(CountryId, weight)>` over ALL
/// shipped country packs. The model head returns a posterior indexed
/// by the trained [`CountryVocab`] — which may be a subset of the
/// shipped packs (e.g. trained on `[BE]` but the classifier knows
/// about FR/NL/DE/...).
///
/// For each `(CountryId, cheap_p)` from the classifier, we look up
/// the country in the trained vocab. If present, multiply its
/// `cheap_p` by `model_posterior[id]`. If absent (the model never
/// saw this country during training, so its head can't speak to
/// it), the cheap classifier's value is used unchanged. Then
/// renormalize.
///
/// The single-country (`BE`) case collapses to the cheap classifier's
/// own posterior — the model is just a sanity check for the country
/// it does know.
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
    use crate::tagger::training::{
        CountryVocab, LrSchedule, TrainConfig, generate_belgium_synthetic, train_and_save,
    };
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
