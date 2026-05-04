//! Deterministic heuristic parser (Phase 0 baseline).
//!
//! See module docs on [`super`] for the relationship to #98.
//!
//! ## Algorithm
//!
//! 1. Run the cheap country classifier (#96 §Country Routing).
//! 2. Extract a postcode using the per-country regex
//!    ([`extract_postcode_for`]). 4-digit ([1-9]\\d{3}) for BE, LU,
//!    AT, CH; 5-digit (\\d{5}) for FR and DE; 4-digit + alpha
//!    suffix for NL (`1011 AB`).
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

/// Parse a free-text address against a single, caller-asserted
/// country. The returned [`ParsedQuery`] has exactly one country
/// candidate (the one passed in, weight 1.0) and one hypothesis —
/// this is the **clean-query** input shape per #96
/// §Zero-Cost-on-Clean-Queries, and the executor's fast path will
/// fire on it.
///
/// For multi-country routing where the country is unknown, use
/// [`parse_with_classifier`].
#[must_use]
pub fn parse_heuristic(text: &str, country: CountryId) -> ParsedQuery {
    let mut q = parse_for_country(text, country);
    q.country_candidates = vec![(country, 1.0)];
    q
}

/// Parse against the cheap classifier's country posterior. Returns a
/// query with `country_candidates` populated from
/// [`classify_country`] (sorted descending by weight). The parser's
/// per-country regex is run for the **top** country; the executor's
/// multi-shard walk handles the remaining countries by re-using the
/// same hypothesis (postcodes that don't match a shard's record set
/// produce zero hits there).
#[must_use]
pub fn parse_with_classifier(text: &str) -> ParsedQuery {
    let posterior = classify_country(text);
    let primary = posterior.first().map(|(c, _)| *c).unwrap_or(CountryId::BE);
    let mut q = parse_for_country(text, primary);
    q.country_candidates = posterior;
    q
}

fn parse_for_country(text: &str, country: CountryId) -> ParsedQuery {
    let original = text.to_string();
    let mut hypothesis = ParseHypothesis::default();
    let mut flags = RecoveryFlags::default();

    let postcode = extract_postcode_for(text, country);
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
        if words.len() >= 3 {
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
    hypothesis.retrieval_policy = retrieval_policy_for(country);
    hypothesis.strictness = Strictness::Exact;

    let confidence = score_confidence(&flags);

    ParsedQuery {
        original_text: original,
        // `parse_heuristic` overrides this with `[(country, 1.0)]`
        // for the clean-query path; `parse_with_classifier` keeps
        // the full posterior. We populate a placeholder here.
        country_candidates: vec![(country, 1.0)],
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

/// 4-digit postcode (BE, LU, AT, CH).
fn pc_4digit_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\b[1-9][0-9]{3}\b").expect("valid regex"))
}

/// 5-digit postcode (FR, DE). FR: 01xxx-95xxx (Corsica is 200xx).
/// DE: 01xxx-99xxx. We accept any 5 digits at word boundaries.
fn pc_5digit_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\b\d{5}\b").expect("valid regex"))
}

/// NL postcode: 4-digit + 2-letter (e.g. `1011 AB`).
fn pc_nl_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\b\d{4}\s?[A-Za-z]{2}\b").expect("valid regex"))
}

/// L-prefixed Luxembourg postcode (`L-2453`). When this matches, drop
/// the prefix so the shard lookup uses the bare 4-digit form.
fn pc_lu_prefixed_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\bL-(\d{4})\b").expect("valid regex"))
}

fn extract_postcode_for(text: &str, country: CountryId) -> Option<String> {
    match country {
        // 5-digit countries.
        CountryId::FR | CountryId::DE => pc_5digit_re().find(text).map(|m| m.as_str().to_string()),
        // NL = 4-digit + 2-letter.
        CountryId::NL => pc_nl_re().find(text).map(|m| {
            // Canonicalize: collapse internal whitespace so `1011 AB`
            // and `1011AB` produce the same key.
            m.as_str().split_whitespace().collect::<Vec<_>>().join("")
        }),
        // Luxembourg accepts either bare 4-digit or L-prefixed.
        CountryId::LU => {
            if let Some(c) = pc_lu_prefixed_re().captures(text) {
                return c.get(1).map(|m| m.as_str().to_string());
            }
            pc_4digit_re().find(text).map(|m| m.as_str().to_string())
        }
        // 4-digit countries.
        CountryId::BE | CountryId::AT | CountryId::CH => {
            pc_4digit_re().find(text).map(|m| m.as_str().to_string())
        }
    }
}

/// Country-specific [`RetrievalPolicy`]. Per #96 §Channel Roles
/// each country pack chooses its strongest evidence anchor; for the
/// cluster #1 + #2 set (BE / FR / NL / LU / DE / AT / CH) the
/// shape is uniform: postcode as Blocker, street as Reducer,
/// house-number + locality as Scorers.
fn retrieval_policy_for(country: CountryId) -> RetrievalPolicy {
    match country {
        CountryId::BE
        | CountryId::FR
        | CountryId::NL
        | CountryId::LU
        | CountryId::DE
        | CountryId::AT
        | CountryId::CH => RetrievalPolicy::european_postcode_anchor(),
    }
}

fn house_number_re() -> &'static Regex {
    // Belgian house-number forms:
    //   "12"            digits
    //   "12A"           digit + letter
    //   "12-14"         hyphenated range
    //   "12/3"          slash unit
    //   "12 bis"        space + bis/ter/quater (Belgium standard)
    //   "12 ter"        space + ter
    //   "12 bis A"      space + bis + space + letter (rare, supported)
    //
    // The pattern matches a leading digit core (optionally followed by
    // a single letter), then optionally one of:
    //   - hyphen/slash + digits (range or unit)
    //   - space + bis/ter/quater/A-Z suffix
    //
    // [`extract_house_number`] runs a second pass on `bis`/`ter`/etc.
    // that aren't on the same span as the digits to ensure they're
    // captured as part of the number.
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r"(?ix)
            \b
            \d+ [A-Za-z]?
            (?:
                [-/] \d+ [A-Za-z]?
              | \s+ (?:bis|ter|quater)
            )?
            \b",
        )
        .expect("valid regex")
    })
}

fn extract_house_number(text: &str) -> Option<String> {
    let matches: Vec<_> = house_number_re().find_iter(text).collect();
    if matches.is_empty() {
        return None;
    }
    for m in matches.iter().rev() {
        let s = m.as_str();
        if !is_postcode_shape(s) {
            // Defensive: collapse whitespace inside the captured span
            // so "12  bis" → "12 bis". Most regex hits are already
            // well-formed; this is a no-op for them.
            let collapsed = s.split_whitespace().collect::<Vec<_>>().join(" ");
            return Some(collapsed);
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

    #[test]
    fn parses_french_postcode() {
        let q = parse_heuristic("10 rue de la Paix 75001 Paris", CountryId::FR);
        let h = &q.hypotheses[0];
        assert_eq!(h.postcode_candidates[0].0, "75001");
        assert_eq!(h.house_candidates[0].0, "10");
    }

    #[test]
    fn parses_dutch_postcode() {
        let q = parse_heuristic("Damrak 1 1012 LP Amsterdam", CountryId::NL);
        let h = &q.hypotheses[0];
        // Whitespace is collapsed canonical: "1012LP".
        assert_eq!(h.postcode_candidates[0].0, "1012LP");
        assert_eq!(h.house_candidates[0].0, "1");
    }

    #[test]
    fn parses_german_postcode() {
        let q = parse_heuristic("Friedrichstraße 100 10117 Berlin", CountryId::DE);
        let h = &q.hypotheses[0];
        assert_eq!(h.postcode_candidates[0].0, "10117");
        assert_eq!(h.house_candidates[0].0, "100");
    }

    #[test]
    fn parses_austrian_postcode() {
        let q = parse_heuristic("Stephansplatz 1 1010 Wien", CountryId::AT);
        let h = &q.hypotheses[0];
        assert_eq!(h.postcode_candidates[0].0, "1010");
    }

    #[test]
    fn parses_swiss_postcode() {
        let q = parse_heuristic("Bahnhofstrasse 1 8001 Zürich", CountryId::CH);
        let h = &q.hypotheses[0];
        assert_eq!(h.postcode_candidates[0].0, "8001");
    }

    #[test]
    fn parses_luxembourg_lprefixed_postcode() {
        let q = parse_heuristic("12 rue de la Gare L-2453 Luxembourg", CountryId::LU);
        let h = &q.hypotheses[0];
        // L-2453 is canonicalized to bare 2453 for shard lookup.
        assert_eq!(h.postcode_candidates[0].0, "2453");
    }

    #[test]
    fn parses_house_number_variants() {
        // Per C5: cover Belgian house-number forms.
        for (input, expected) in [
            ("Rue Wayez 12 1070", "12"),
            ("Rue Wayez 12A 1070", "12A"),
            ("Rue Wayez 12-14 1070", "12-14"),
            ("Rue Wayez 12/3 1070", "12/3"),
            ("Rue Wayez 12 bis 1070", "12 bis"),
            ("Rue Wayez 12 ter 1070", "12 ter"),
            ("Rue Wayez 12 quater 1070", "12 quater"),
        ] {
            let q = parse_heuristic(input, CountryId::BE);
            let h = &q.hypotheses[0];
            assert!(
                !h.house_candidates.is_empty(),
                "no house candidate for {input:?}"
            );
            assert_eq!(
                h.house_candidates[0].0, expected,
                "input {input:?} produced {:?}, expected {expected:?}",
                h.house_candidates[0].0
            );
        }
    }
}
