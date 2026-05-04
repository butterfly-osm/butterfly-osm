//! Deterministic heuristic parser (Phase 0 baseline).
//!
//! See module docs on [`super`] for the relationship to #98.
//!
//! ## Algorithm
//!
//! 1. Run the cheap country classifier (#96 §Country Routing).
//! 2. Extract a Belgian postcode (`[1-9][0-9]{3}` matched as a
//!    standalone token).
//! 3. Extract a house number — leading or trailing numeric/alphanumeric
//!    token (e.g. `122`, `122a`, `122-126`).
//! 4. The remaining tokens are the street + locality.

use std::sync::OnceLock;

use regex::Regex;

use crate::geocoder::channels::Channel;
use crate::routing::{CountryId, classify_country};
use crate::types::{
    ExecutionBudget, FieldMask, ParseHypothesis, ParsedQuery, RecoveryFlags, RetrievalPolicy,
    Strictness,
};

#[must_use]
pub fn parse_heuristic(text: &str, country: CountryId) -> ParsedQuery {
    debug_assert_eq!(country, CountryId::BE, "MVP only ships BE");

    let original = text.to_string();
    let mut hypothesis = ParseHypothesis::default();
    let mut flags = RecoveryFlags::default();

    let postcode = extract_postcode(text);
    if let Some(ref pc) = postcode {
        hypothesis.postcode_candidates.push((pc.clone(), 1.0));
        flags.had_postcode = true;
    }

    let without_postcode = postcode
        .as_ref()
        .map(|pc| strip_token(text, pc))
        .unwrap_or_else(|| text.to_string());

    let house = extract_house_number(&without_postcode);
    if let Some(ref h) = house {
        hypothesis.house_candidates.push((h.clone(), 1.0));
        flags.had_house_number = true;
    }

    let without_house = house
        .as_ref()
        .map(|h| strip_token(&without_postcode, h))
        .unwrap_or(without_postcode);

    let remainder = clean_separators(&without_house);
    if !remainder.is_empty() {
        let words: Vec<&str> = remainder.split_whitespace().collect();
        if flags.had_postcode && words.len() >= 3 {
            // With a postcode anchor, the LAST word is most likely the
            // locality (Belgian convention). Split it off the street
            // candidate so the executor's exact street index hits.
            let last = words.last().copied().unwrap_or("");
            let street_part = words[..words.len() - 1].join(" ");
            if !street_part.is_empty() {
                hypothesis.street_candidates.push((street_part, 1.0));
            }
            if !last.is_empty() {
                hypothesis.locality_candidates.push((last.to_string(), 0.5));
                flags.had_locality = true;
            }
            // Also keep the full remainder as a low-weight street
            // candidate — covers cases where the locality is multi-word.
            hypothesis.street_candidates.push((remainder, 0.3));
        } else {
            // No postcode → keep both interpretations. The executor's
            // locality scorer will prefer the right one.
            hypothesis.street_candidates.push((remainder.clone(), 1.0));
            if words.len() >= 2 {
                let last = words.last().copied().unwrap_or("");
                if !last.is_empty() {
                    hypothesis.locality_candidates.push((last.to_string(), 0.5));
                    flags.had_locality = true;
                }
            }
        }
    }

    hypothesis.field_reliability = build_field_mask(&flags);
    hypothesis.retrieval_policy = RetrievalPolicy::belgium_default();
    hypothesis.strictness = Strictness::Exact;

    let confidence = score_confidence(&flags);

    ParsedQuery {
        original_text: original,
        country_candidates: classify_country(text),
        hypotheses: vec![hypothesis],
        global_confidence: confidence,
        recovery_flags: flags,
        execution_budget: ExecutionBudget::default(),
    }
}

fn build_field_mask(flags: &RecoveryFlags) -> FieldMask {
    let mut m = FieldMask::NONE;
    if flags.had_postcode {
        m = m.with(Channel::Postcode);
    }
    if flags.had_house_number {
        m = m.with(Channel::HouseNumber);
    }
    m
}

fn score_confidence(flags: &RecoveryFlags) -> f32 {
    let mut s = 0.5_f32;
    if flags.had_postcode {
        s += 0.3;
    }
    if flags.had_house_number {
        s += 0.15;
    }
    if flags.had_locality {
        s += 0.05;
    }
    s.min(1.0)
}

fn postcode_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\b[1-9][0-9]{3}\b").expect("valid regex"))
}

fn extract_postcode(text: &str) -> Option<String> {
    postcode_re().find(text).map(|m| m.as_str().to_string())
}

fn house_number_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\b\d+[A-Za-z]?(?:[-/]\d+[A-Za-z]?)?\b").expect("valid regex"))
}

fn extract_house_number(text: &str) -> Option<String> {
    let matches: Vec<_> = house_number_re().find_iter(text).collect();
    if matches.is_empty() {
        return None;
    }
    for m in matches.iter().rev() {
        let s = m.as_str();
        if !is_postcode_shape(s) {
            return Some(s.to_string());
        }
    }
    None
}

fn is_postcode_shape(s: &str) -> bool {
    s.len() == 4
        && s.chars().all(|c| c.is_ascii_digit())
        && s.chars().next().map(|c| c != '0').unwrap_or(false)
}

fn strip_token(text: &str, token: &str) -> String {
    if let Some(idx) = text.find(token) {
        let before = &text[..idx];
        let after = &text[idx + token.len()..];
        let mut out = String::with_capacity(text.len());
        out.push_str(before.trim_end());
        if !out.is_empty() {
            out.push(' ');
        }
        out.push_str(after.trim_start());
        out.trim().to_string()
    } else {
        text.to_string()
    }
}

fn clean_separators(s: &str) -> String {
    s.trim()
        .trim_matches(|c: char| matches!(c, ',' | ';' | '-' | '/' | '|'))
        .trim()
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_brussels_with_postcode_and_number() {
        let q = parse_heuristic("Rue Wayez 122 1070 Anderlecht", CountryId::BE);
        assert!(q.is_clean());
        let h = &q.hypotheses[0];
        assert_eq!(h.postcode_candidates[0].0, "1070");
        assert_eq!(h.house_candidates[0].0, "122");
        let s = h.street_candidates[0].0.to_lowercase();
        assert!(s.contains("rue wayez") || s.contains("wayez"), "got: {s}");
    }

    #[test]
    fn parses_postcode_only_query() {
        let q = parse_heuristic("1000 Bruxelles", CountryId::BE);
        let h = &q.hypotheses[0];
        assert_eq!(h.postcode_candidates[0].0, "1000");
        assert!(h.house_candidates.is_empty());
    }

    #[test]
    fn parses_no_postcode_and_no_number() {
        let q = parse_heuristic("Grote Markt Antwerpen", CountryId::BE);
        let h = &q.hypotheses[0];
        assert!(h.postcode_candidates.is_empty());
        assert!(!h.locality_candidates.is_empty());
    }

    #[test]
    fn empty_query_returns_clean_empty_hypothesis() {
        let q = parse_heuristic("", CountryId::BE);
        assert!(q.is_clean());
        let h = &q.hypotheses[0];
        assert!(h.street_candidates.is_empty());
        assert!(h.postcode_candidates.is_empty());
    }
}
