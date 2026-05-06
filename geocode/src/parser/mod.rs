//! Parser surface — tagger signal emission only.
//!
//! Per #205 the parse-then-geocode shape is gone. The "parser"
//! modules that survive emit [`crate::geocoder::recall::TaggerSignals`]
//! for the recall + rerank pipeline; they no longer extract fields
//! into a [`ParsedQuery`] (which itself was deleted in #205).
//!
//! - [`heuristic`] — deterministic regex-driven baseline. Always
//!   available, no model file required. Currently emits a neutral
//!   `TaggerSignals` (empty BIO logits, classifier-only country
//!   posterior, fixed `global_confidence`); cheap deterministic
//!   priors that recall depends on (postcode regex, country
//!   classifier) live in `geocoder::rerank` and `routing::Classifier`
//!   respectively.
//! - [`neural`] — byte-level transformer ([`crate::tagger`]) that
//!   produces real BIO logits and a country posterior.
//! - [`normalize`] — string normalization shared by the recall
//!   index builder and every other text-comparing call site.

pub mod heuristic;
pub mod neural;
pub mod normalize;

use crate::geocoder::recall::TaggerSignals;
use crate::shard::reader::Shard;

/// Polymorphic tagger-signal source. Wraps either the heuristic
/// fallback ([`HeuristicBackend`]) or the neural [`neural::NeuralParser`]
/// behind a single trait object so the server can swap backends at
/// runtime.
pub trait ParserBackend: Send + Sync + std::fmt::Debug {
    /// Emit tagger signals for `text`. The shard handle is threaded
    /// through for parity with the legacy interface; current backends
    /// ignore it but a future tagger-aware preconditioner may consult
    /// shard stats.
    fn signals(&self, text: &str, shard: &Shard) -> anyhow::Result<TaggerSignals>;
    fn name(&self) -> &'static str;
}

/// Heuristic fallback backend — emits neutral `TaggerSignals`. Used
/// when no neural model is loaded; the recall + rerank pipeline still
/// runs, just without per-byte BIO weighting.
#[derive(Debug, Default)]
pub struct HeuristicBackend;

impl ParserBackend for HeuristicBackend {
    fn signals(&self, text: &str, _shard: &Shard) -> anyhow::Result<TaggerSignals> {
        Ok(heuristic::neutral_signals(text))
    }
    fn name(&self) -> &'static str {
        "heuristic"
    }
}

/// Neural backend — wraps [`neural::NeuralParser`] under [`ParserBackend`].
#[derive(Debug)]
pub struct NeuralBackend {
    pub parser: neural::NeuralParser,
}

impl NeuralBackend {
    #[must_use]
    pub fn new(parser: neural::NeuralParser) -> Self {
        Self { parser }
    }
}

impl ParserBackend for NeuralBackend {
    fn signals(&self, text: &str, _shard: &Shard) -> anyhow::Result<TaggerSignals> {
        self.parser.signals(text)
    }
    fn name(&self) -> &'static str {
        "neural"
    }
}
