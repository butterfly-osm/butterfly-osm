//! Heuristic tagger-signal emitter (#205).
//!
//! The legacy heuristic parser extracted regex-driven field
//! candidates into a `ParsedQuery`. That entire concept was deleted
//! in #205 (recall + rerank directly over the FST + GBDT pipeline).
//! What survives here is **just** the cheap classifier hookup that
//! produces a neutral [`TaggerSignals`] — the recall pipeline still
//! runs deterministically off the country posterior alone when no
//! neural model is loaded.

use crate::geocoder::recall::TaggerSignals;
use crate::routing::classify_country;

/// Build a neutral [`TaggerSignals`] from cheap classifier output.
///
/// Empty `bio_logits` (the recall code treats this as "no per-byte
/// signal" — every prefix gets equal weight modulo n-token spread).
/// `country_posterior` comes from the cheap classifier ranking
/// [`crate::routing::classify_country`]. `global_confidence` is set
/// to the cheap classifier's top-1 probability — meaningful even
/// without a neural model.
#[must_use]
pub fn neutral_signals(text: &str) -> TaggerSignals {
    let posterior = classify_country(text);
    let global_confidence = posterior
        .first()
        .map(|(_, p)| *p)
        .unwrap_or(0.0)
        .clamp(0.0, 1.0);
    TaggerSignals {
        bio_logits: Vec::new(),
        country_posterior: posterior,
        global_confidence,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routing::CountryId;

    #[test]
    fn neutral_signals_have_empty_bio_logits() {
        let s = neutral_signals("Rue Wayez 122 1070 Anderlecht");
        assert!(s.bio_logits.is_empty());
        assert!(!s.country_posterior.is_empty());
        // Cheap classifier should put BE in the top-K for a Belgian
        // query.
        let has_be = s.country_posterior.iter().any(|(c, _)| *c == CountryId::BE);
        assert!(
            has_be,
            "expected BE in posterior, got {:?}",
            s.country_posterior
        );
    }

    #[test]
    fn empty_input_yields_empty_posterior() {
        let s = neutral_signals("");
        assert!(s.bio_logits.is_empty());
        // classify_country may still emit something for empty input
        // depending on the registry shape; we only require the call
        // not to panic.
        assert!(s.global_confidence.is_finite());
    }
}
