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
    let mut flags = RecoveryFlags::default();

    let postcode = extract_postcode_for(text, country);
    if postcode.is_some() {
        flags.had_postcode = true;
    }

    let without_postcode = postcode
        .as_ref()
        .map(|pc| strip_token(text, pc))
        .unwrap_or_else(|| text.to_string());

    // Detect whether the postcode appeared *before* or *after* the
    // bulk of the address. Reordered queries put the postcode first
    // (`9770 Kruisem René D'Huyvetterstraat 5c`); the standard form
    // puts it after the street (`René D'Huyvetterstraat 5c, 9770
    // Kruisem`). The heuristic used to assume the standard form
    // exclusively — recall@1 on reordered queries cratered to ~2%
    // because the locality was always taken from the *last* word.
    let postcode_at_start = postcode
        .as_ref()
        .map(|pc| {
            let trimmed = text.trim_start();
            trimmed.starts_with(pc.as_str())
        })
        .unwrap_or(false);

    let house = extract_house_number(&without_postcode);
    if house.is_some() {
        flags.had_house_number = true;
    }

    let without_house = house
        .as_ref()
        .map(|h| strip_token(&without_postcode, h))
        .unwrap_or(without_postcode);

    let remainder = clean_separators(&without_house);
    let remainder_words: Vec<String> = remainder.split_whitespace().map(str::to_string).collect();

    // Emit one hypothesis per plausible (street, locality) split.
    // Each variant becomes its own ParseHypothesis; the executor
    // canonicalizes + dedups the resulting programs (one per unique
    // canonical form), so identical splits collapse cleanly.
    //
    // Variants (in priority order — the first surviving one becomes
    // the representative for any deduped equivalence class):
    //
    //   1. **last-word-locality** (standard postcode-suffix layout):
    //       words[..-1] = street, words[-1] = locality.
    //   2. **first-word-locality** (reordered / postcode-prefix
    //       layout): words[0] = locality, words[1..] = street.
    //   3. **two-word-locality** when the remainder has ≥ 4 words:
    //       words[..-2] = street, words[-2..] = locality (covers
    //       multi-word names like "La Louvière", "Sint Niklaas").
    //   4. **whole-remainder-as-street** (low weight): handles
    //       multi-word streets where the locality is implicit in
    //       the postcode anchor.
    //
    // All variants share the same postcode + housenumber. The
    // executor's recombination invariant collapses the canonical
    // forms — we don't worry about over-emission here.
    let mut hypotheses: Vec<ParseHypothesis> = Vec::new();
    // Build a fresh hypothesis pre-seeded with the shared
    // postcode/housenumber. We pass `flags` by reference rather than
    // capturing — emitting locality variants below mutates `flags`,
    // and a closure capturing `&flags` would block those edits.
    fn base_hypothesis(
        country: CountryId,
        flags: &RecoveryFlags,
        postcode: Option<&String>,
        house: Option<&String>,
    ) -> ParseHypothesis {
        let mut h = ParseHypothesis {
            field_reliability: build_field_mask(flags),
            retrieval_policy: retrieval_policy_for(country),
            strictness: Strictness::Exact,
            ..ParseHypothesis::default()
        };
        if let Some(pc) = postcode {
            h.postcode_candidates.push((pc.clone(), 1.0));
        }
        if let Some(hn) = house {
            h.house_candidates.push((hn.clone(), 1.0));
        }
        h
    }
    let pc_ref = postcode.as_ref();
    let hn_ref = house.as_ref();

    if remainder_words.is_empty() {
        // No remainder — postcode-only / postcode+housenumber-only
        // query. One hypothesis covers it; the clean fast path
        // applies.
        hypotheses.push(base_hypothesis(country, &flags, pc_ref, hn_ref));
    } else {
        let n = remainder_words.len();

        // **Primary hypothesis**: matches the original parser's
        // behaviour exactly so the unambiguous case stays on the
        // clean fast path AND the executor sees the same scoring
        // shape it always did. Diversity variants below add
        // additional hypotheses ONLY when there's positive signal
        // for ambiguity.
        //
        // Splitting policy:
        //   - n ≥ 3: words[..-1] = street, words[-1] = locality
        //     (standard layout); add the full remainder as a
        //     low-weight street candidate to cover multi-word
        //     streets where the locality is implicit in the
        //     postcode anchor.
        //   - n == 2: keep both words as the street; the
        //     postcode anchor handles disambiguation. The pre-#181
        //     parser used this same shape and the
        //     `extract_clean_query_features` regression proved any
        //     other split produces a low fuzzy match at scoring
        //     time.
        //   - n == 1: take the single token as the locality
        //     candidate.
        {
            let mut h = base_hypothesis(country, &flags, pc_ref, hn_ref);
            if n >= 3 {
                let last = remainder_words[n - 1].clone();
                let street_part = remainder_words[..n - 1].join(" ");
                if !street_part.is_empty() {
                    h.street_candidates.push((street_part, 1.0));
                }
                h.locality_candidates.push((last, 0.5));
                flags.had_locality = true;
                // Whole-remainder fallback (low weight) for
                // multi-word streets.
                h.street_candidates.push((remainder.clone(), 0.3));
            } else if n == 2 {
                // Two tokens: keep them together as the street.
                // Splitting into street+locality on n==2 produced
                // low fuzzy match scores in `extract_features`.
                h.street_candidates.push((remainder.clone(), 1.0));
            } else {
                // n == 1: single token after stripping
                // postcode+house. Treat it as the locality.
                h.locality_candidates
                    .push((remainder_words[0].clone(), 0.5));
                flags.had_locality = true;
            }
            hypotheses.push(h);
        }

        // **Ambiguity variant — postcode at start (reordered)**.
        // Emit ONLY when we have explicit signal that the layout is
        // reordered (`9770 Kruisem René D'Huyvetterstraat 5c`); the
        // recall@1 on these queries was 2% before this variant
        // existed because the locality was always taken from the
        // last word (D'Huyvetterstraat), missing every Kruisem
        // record. Skipping the variant when the signal is absent
        // keeps the clean fast path warm for the typical layout.
        if postcode_at_start && n >= 2 {
            let first = remainder_words[0].clone();
            let street_part = remainder_words[1..].join(" ");
            let mut h = base_hypothesis(country, &flags, pc_ref, hn_ref);
            if !street_part.is_empty() {
                h.street_candidates.push((street_part, 1.0));
            }
            h.locality_candidates.push((first, 0.8));
            hypotheses.push(h);
            flags.had_locality = true;
        }

        // **Ambiguity variant — two-word locality**. Belgian /
        // French / Spanish localities frequently span two tokens
        // ("La Louvière", "Saint Niklaas", "Las Rozas"). Without
        // this hypothesis the street picks up the second locality
        // word and the street index misses. Emit only when:
        //   - the remainder has ≥ 4 words so the street fragment
        //     carries real signal, AND
        //   - we have a postcode anchor — without it, multiple
        //     hypotheses widen the executor's budget tier without
        //     adding signal (the postcode is the strongest blocker
        //     and the two-word-locality variant only helps when
        //     paired with it).
        if n >= 4 && postcode.is_some() {
            let last_two = remainder_words[n - 2..].join(" ");
            let street_part = remainder_words[..n - 2].join(" ");
            let mut h = base_hypothesis(country, &flags, pc_ref, hn_ref);
            if !street_part.is_empty() {
                h.street_candidates.push((street_part, 1.0));
            }
            h.locality_candidates.push((last_two, 0.4));
            hypotheses.push(h);
        }
    }

    // Defensive: if we somehow emitted nothing (empty input), keep
    // the ParsedQuery::is_clean() invariant alive with one empty
    // hypothesis. The executor's empty-program check returns no
    // results anyway.
    if hypotheses.is_empty() {
        hypotheses.push(base_hypothesis(country, &flags, pc_ref, hn_ref));
    }

    let confidence = score_confidence(&flags);

    ParsedQuery {
        original_text: original,
        // `parse_heuristic` overrides this with `[(country, 1.0)]`
        // for the clean-query path; `parse_with_classifier` keeps
        // the full posterior. We populate a placeholder here.
        country_candidates: vec![(country, 1.0)],
        hypotheses,
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

/// Per #96 "serve the world": postcode regex + canonicalization come
/// from the country pack (data), not a hardcoded match (code). The
/// pack-driven path means adding a country is a TOML drop, not a Rust
/// edit.
fn extract_postcode_for(text: &str, country: CountryId) -> Option<String> {
    let reg = crate::routing::Classifier::shipped().registry();
    let pack = reg.get(country)?;
    let re = pack.postcode_regex.as_ref()?;
    let m = re.find(text)?;
    Some(pack.canonicalize_postcode(m.as_str()))
}

/// Per-country [`RetrievalPolicy`].
///
/// MVP: every country uses the European postcode-anchor shape
/// (postcode = Blocker, street = Reducer, house-number / locality =
/// Scorers). For US-style (street = Blocker) and Japanese-style
/// (admin hierarchy = Blocker) we'd diverge here once those packs
/// declare a `[retrieval_policy]` section. The shape is uniform for
/// the postcode-anchored countries (BE/FR/NL/LU/DE/AT/CH/GB/ES/IT/AU)
/// in the shipped set; US/JP/BR/IN inherit the same shape pending the
/// per-pack policy override.
fn retrieval_policy_for(_country: CountryId) -> RetrievalPolicy {
    RetrievalPolicy::european_postcode_anchor()
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
    fn reordered_query_emits_first_word_locality_hypothesis() {
        // Bench shape: `9770 Kruisem René D'Huyvetterstraat 5c`.
        // The standard (last-word-locality) hypothesis would label
        // `D'Huyvetterstraat` as the locality and `Kruisem René` as
        // the street, missing every Kruisem record. The
        // postcode-at-start ambiguity variant emits a hypothesis
        // with `Kruisem` as the locality.
        let q = parse_heuristic("9770 Kruisem René D'Huyvetterstraat 5c", CountryId::BE);
        assert!(
            q.hypotheses.len() >= 2,
            "reordered query must emit ≥ 2 hypotheses (got {})",
            q.hypotheses.len()
        );
        let has_kruisem = q.hypotheses.iter().any(|h| {
            h.locality_candidates
                .iter()
                .any(|(loc, _)| loc.eq_ignore_ascii_case("Kruisem"))
        });
        assert!(
            has_kruisem,
            "expected at least one hypothesis with locality=Kruisem; got: {:?}",
            q.hypotheses
                .iter()
                .map(|h| h.locality_candidates.clone())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn standard_query_keeps_clean_fast_path() {
        // The Zero-Cost-on-Clean-Queries NFR depends on
        // `is_clean()` being true for the typical postcode-suffix
        // form. The hypothesis-diversity work must not regress this
        // for the common shape — we only emit ≥ 2 hypotheses when we
        // have positive signal that the layout is ambiguous.
        let q = parse_heuristic("Rue Wayez 122 1070 Anderlecht", CountryId::BE);
        assert!(
            q.is_clean(),
            "standard postcode-suffix layout must keep is_clean()==true; got {} hypotheses",
            q.hypotheses.len()
        );
    }

    #[test]
    fn multi_word_locality_query_emits_two_word_locality_variant() {
        // `Rue Hamoir 21, 7100 La Louvière` — the standard split
        // names `Louvière` as the locality and `Rue Hamoir 21 La` as
        // the street, missing the actual `La Louvière` locality.
        // The two-word-locality variant labels `La Louvière` as
        // locality.
        let q = parse_heuristic("Rue Hamoir 21 7100 La Louvière", CountryId::BE);
        let has_la_louviere = q.hypotheses.iter().any(|h| {
            h.locality_candidates
                .iter()
                .any(|(loc, _)| loc.eq_ignore_ascii_case("La Louvière"))
        });
        assert!(
            has_la_louviere,
            "expected at least one hypothesis with locality='La Louvière'; got: {:?}",
            q.hypotheses
                .iter()
                .map(|h| h.locality_candidates.clone())
                .collect::<Vec<_>>()
        );
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
