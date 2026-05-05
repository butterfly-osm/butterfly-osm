//! Query parser.
//!
//! ## Backends
//!
//! - [`heuristic`] — deterministic, regex-driven baseline (Phase 0
//!   from PR #162). Single hypothesis, single country. Always
//!   available, no model file required.
//! - [`neural`] — byte-level transformer + retrieval-aware decoding
//!   (#96 §Tagger + #98 Phase 1). Loaded from a safetensors file at
//!   server startup; falls back to the heuristic parser if the file
//!   is missing.
//!
//! Both backends emit the same [`crate::types::ParsedQuery`] shape.
//! The executor consumes either without knowing which produced it.
//!
//! ## Backend trait
//!
//! [`ParserBackend`] is the dynamic-dispatch interface used by the
//! HTTP handlers. The neural backend is wrapped in the variant that
//! takes a `&Shard` because the decoder needs shard statistics for
//! anchor detection and retrieval-utility scoring; the heuristic
//! backend doesn't, so the trait passes `&Shard` to both for symmetry
//! (it's free for the heuristic implementation).

pub mod anchor;
pub mod beam;
pub mod decoding;
pub mod heuristic;
pub mod neural;
pub mod normalize;
pub mod phase2_features;
pub mod phase2_training;
pub mod retrieval_utility;

use crate::routing::CountryId;
use crate::shard::reader::Shard;
use crate::types::ParsedQuery;

pub use heuristic::parse_heuristic;
pub use neural::NeuralParser;

/// Polymorphic parser interface.
///
/// `parse` is fallible because the neural backend can fail at runtime
/// (forward pass error, shape mismatch on a malformed model file) —
/// the heuristic backend always returns `Ok(...)`.
pub trait ParserBackend: Send + Sync + std::fmt::Debug {
    fn parse(&self, text: &str, country: CountryId, shard: &Shard) -> anyhow::Result<ParsedQuery>;
    fn name(&self) -> &'static str;
}

#[derive(Debug, Default)]
pub struct HeuristicBackend;

impl ParserBackend for HeuristicBackend {
    fn parse(&self, text: &str, country: CountryId, _shard: &Shard) -> anyhow::Result<ParsedQuery> {
        Ok(parse_heuristic(text, country))
    }
    fn name(&self) -> &'static str {
        "heuristic"
    }
}

/// Wrapper for [`NeuralParser`] under the [`ParserBackend`] trait.
#[derive(Debug)]
pub struct NeuralBackend {
    pub parser: NeuralParser,
}

impl NeuralBackend {
    #[must_use]
    pub fn new(parser: NeuralParser) -> Self {
        Self { parser }
    }
}

impl ParserBackend for NeuralBackend {
    fn parse(&self, text: &str, _country: CountryId, shard: &Shard) -> anyhow::Result<ParsedQuery> {
        self.parser.parse(text, shard)
    }
    fn name(&self) -> &'static str {
        "neural"
    }
}
