//! Cheap deterministic country classifier (#96 §Country Routing).
//!
//! The classifier is a fast, lexical pre-stage that runs BEFORE the
//! parser and BEFORE retrieval. It does not need to be right; it
//! needs to be **calibrated** — it returns a posterior `(country,
//! confidence)` distribution so the executor can budget its fanout.
//!
//! ## Signals
//!
//! Per #96 cluster #1 + cluster #2 (BE / FR / NL / LU / DE / AT / CH)
//! the classifier looks at three classes of signal, additively
//! contributing log-evidence per country, then softmaxed:
//!
//! 1. **Postcode shape.** Each country has a regex; matches contribute
//!    strong evidence for that country's shape and any peer country
//!    sharing the shape (e.g. 4-digit overlaps BE / LU / AT / CH).
//! 2. **Lexical markers.** Street-type keywords ("rue", "straat",
//!    "strasse", "platz") are the most reliable signal. Locality
//!    aliases (Brussels / Bruxelles / Brussel) follow.
//! 3. **Diacritics + script.** ß → DE/AT (not CH which uses ss).
//!    Accented Latin → not DE/NL primarily.
//!
//! When multiple countries are plausible, the classifier returns ALL
//! of them with normalized weights summing to 1.0. This is what the
//! executor consumes — see `executor::execute`.
//!
//! ## Shape, not full coverage
//!
//! This is the *cheap* classifier. If it is uncertain (top mass < 0.7)
//! the architecture defers to the byte-level transformer (#96 §Country
//! Routing — "If uncertain → let the transformer's country head run
//! fully"). When the transformer ships (in `parser::neural`), it
//! refines this posterior; until then, the classifier is the only
//! signal and the executor falls back on the budget's `max_countries`
//! cap to bound fanout.

use std::sync::OnceLock;

use regex::Regex;

use super::CountryId;

/// Classify the country distribution implied by an input string.
///
/// Returns a sorted (descending by weight) list of `(country, weight)`
/// pairs whose weights sum to 1.0. The list always contains every
/// country that matched at least one signal; if no signal fires, it
/// returns a uniform prior over all supported countries (so the
/// executor can degrade gracefully to "search every shard").
#[must_use]
pub fn classify_country(text: &str) -> Vec<(CountryId, f32)> {
    let lower = text.to_lowercase();
    let mut log_evidence: [f32; CountryId::ALL.len()] = [0.0; CountryId::ALL.len()];

    apply_postcode_signals(&lower, text, &mut log_evidence);
    apply_lexical_signals(&lower, &mut log_evidence);
    apply_script_signals(text, &mut log_evidence);

    if log_evidence.iter().all(|&v| v == 0.0) {
        // No signal — uniform prior. Executor will use
        // `max_countries` to cap the fanout.
        let n = CountryId::ALL.len() as f32;
        let mut out: Vec<(CountryId, f32)> = CountryId::ALL
            .iter()
            .copied()
            .map(|c| (c, 1.0 / n))
            .collect();
        // Stable sort by ISO so output is deterministic when uniform.
        out.sort_by_key(|(c, _)| c.iso2());
        return out;
    }

    // Softmax over the evidence vector. Subtract max for stability.
    let max = log_evidence
        .iter()
        .copied()
        .fold(f32::NEG_INFINITY, f32::max);
    let exps: Vec<f32> = log_evidence.iter().map(|&v| (v - max).exp()).collect();
    let sum: f32 = exps.iter().sum();

    let mut out: Vec<(CountryId, f32)> = CountryId::ALL
        .iter()
        .copied()
        .zip(exps.iter())
        .map(|(c, &e)| (c, e / sum))
        .collect();

    out.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.0.iso2().cmp(b.0.iso2()))
    });
    out
}

fn idx(c: CountryId) -> usize {
    match c {
        CountryId::BE => 0,
        CountryId::FR => 1,
        CountryId::NL => 2,
        CountryId::LU => 3,
        CountryId::DE => 4,
        CountryId::AT => 5,
        CountryId::CH => 6,
    }
}

fn add(evidence: &mut [f32; 7], c: CountryId, w: f32) {
    evidence[idx(c)] += w;
}

/// Per-country postcode regex. Run on the raw input (case is
/// irrelevant because the patterns are digit-anchored).
fn pc_be_lu_at_ch_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\b[1-9]\d{3}\b").expect("4-digit pc regex"))
}
fn pc_fr_de_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\b[0-9]\d{4}\b").expect("5-digit pc regex"))
}
fn pc_nl_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\b\d{4}\s?[A-Za-z]{2}\b").expect("nl pc regex"))
}
fn pc_lu_prefixed_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\bL-\d{4}\b").expect("lu pc regex"))
}

fn apply_postcode_signals(_lower: &str, raw: &str, evidence: &mut [f32; 7]) {
    // Strongest postcode signal first: NL (alpha suffix). If this
    // fires we're very confident NL.
    if pc_nl_re().is_match(raw) {
        add(evidence, CountryId::NL, 3.0);
    }
    // L-prefixed Luxembourg postcode (e.g. "L-2453") — distinctive.
    if pc_lu_prefixed_re().is_match(raw) {
        add(evidence, CountryId::LU, 3.0);
    }

    // 5-digit shape: FR / DE share it. Split between them for now;
    // lexical signals or a second pass narrows it.
    if pc_fr_de_re().is_match(raw) {
        add(evidence, CountryId::FR, 1.0);
        add(evidence, CountryId::DE, 1.0);
    }

    // 4-digit shape: BE / LU / AT / CH share it. Slightly biased
    // toward BE because BE has the densest queryable address corpus
    // in this group; lexical disambiguation kicks in if other signals
    // fire.
    if pc_be_lu_at_ch_re().is_match(raw) {
        add(evidence, CountryId::BE, 1.0);
        add(evidence, CountryId::LU, 0.8);
        add(evidence, CountryId::AT, 0.8);
        add(evidence, CountryId::CH, 0.8);
    }
}

fn contains_word(text: &str, word: &str) -> bool {
    let bytes = text.as_bytes();
    let wbytes = word.as_bytes();
    if wbytes.is_empty() || bytes.len() < wbytes.len() {
        return false;
    }
    let mut i = 0;
    while i + wbytes.len() <= bytes.len() {
        if &bytes[i..i + wbytes.len()] == wbytes {
            let before_ok = i == 0 || !bytes[i - 1].is_ascii_alphabetic();
            let after_ok =
                i + wbytes.len() == bytes.len() || !bytes[i + wbytes.len()].is_ascii_alphabetic();
            if before_ok && after_ok {
                return true;
            }
        }
        i += 1;
    }
    false
}

/// Returns `true` if any of `needles` occurs as a whole-token in
/// `text` (lowercase).
fn any_word(text: &str, needles: &[&str]) -> bool {
    needles.iter().any(|w| contains_word(text, w))
}

/// Returns `true` if any of `needles` occurs as a *substring* in
/// `text` (lowercase). Use for suffixes ("straat", "strasse") that
/// are not stand-alone words but appear glued to street stems.
fn any_substr(text: &str, needles: &[&str]) -> bool {
    needles.iter().any(|s| text.contains(s))
}

fn apply_lexical_signals(lower: &str, evidence: &mut [f32; 7]) {
    // ── French markers ──
    // "rue", "avenue", "boulevard", "place", "chaussée".
    // Belgian French is the same as Metropolitan French at the
    // street-type level. We boost FR slightly more than BE because
    // when these appear without other Belgian markers the prior is
    // FR.
    if any_word(
        lower,
        &["rue", "avenue", "boulevard", "chaussee", "chaussée"],
    ) {
        add(evidence, CountryId::FR, 1.5);
        add(evidence, CountryId::BE, 1.0);
        add(evidence, CountryId::LU, 0.7);
        add(evidence, CountryId::CH, 0.5);
    }
    if any_word(lower, &["allee", "allée", "impasse", "quai"]) {
        add(evidence, CountryId::FR, 1.0);
        add(evidence, CountryId::BE, 0.6);
        add(evidence, CountryId::LU, 0.5);
        add(evidence, CountryId::CH, 0.3);
    }

    // ── Dutch markers ──
    // "straat" (street suffix), "laan" (avenue suffix), "plein"
    // (square), "weg" (way), "gracht" (canal — strongly NL/BE-Flemish).
    if any_substr(lower, &["straat", "kerkstraat", "dorpsstraat"]) {
        add(evidence, CountryId::NL, 1.6);
        add(evidence, CountryId::BE, 1.1);
    }
    if any_substr(lower, &["laan", "plein", "gracht"]) {
        add(evidence, CountryId::NL, 1.3);
        add(evidence, CountryId::BE, 0.9);
    }
    if any_word(lower, &["markt", "grote markt"]) {
        add(evidence, CountryId::BE, 0.8);
        add(evidence, CountryId::NL, 0.7);
    }

    // ── German markers ──
    // "strasse", "straße", "platz", "weg", "gasse" (AT-leaning).
    if any_substr(lower, &["strasse", "straße", "str."]) {
        add(evidence, CountryId::DE, 1.5);
        add(evidence, CountryId::AT, 1.2);
        add(evidence, CountryId::CH, 1.1);
    }
    if any_substr(lower, &["platz"]) {
        add(evidence, CountryId::DE, 1.0);
        add(evidence, CountryId::AT, 0.9);
        add(evidence, CountryId::CH, 0.7);
    }
    // Austrian-leaning "gasse" (small street) — common in Vienna,
    // less so elsewhere.
    if any_substr(lower, &["gasse"]) {
        add(evidence, CountryId::AT, 1.2);
        add(evidence, CountryId::DE, 0.6);
        add(evidence, CountryId::CH, 0.3);
    }

    // ── Locality aliases (cluster #1 + #2 cross-border) ──
    // Strongest country-specific lexical evidence — locality names
    // that exist only in one country.
    for (term, c, w) in &[
        ("amsterdam", CountryId::NL, 2.0),
        ("rotterdam", CountryId::NL, 2.0),
        ("utrecht", CountryId::NL, 2.0),
        ("eindhoven", CountryId::NL, 2.0),
        ("paris", CountryId::FR, 2.0),
        ("marseille", CountryId::FR, 2.0),
        ("lyon", CountryId::FR, 2.0),
        ("toulouse", CountryId::FR, 2.0),
        ("strasbourg", CountryId::FR, 2.0),
        ("luxembourg", CountryId::LU, 1.5),
        ("lëtzebuerg", CountryId::LU, 2.5),
        ("esch-sur-alzette", CountryId::LU, 2.5),
        ("differdange", CountryId::LU, 2.0),
        ("berlin", CountryId::DE, 2.0),
        ("hamburg", CountryId::DE, 2.0),
        ("münchen", CountryId::DE, 2.0),
        ("munich", CountryId::DE, 1.5),
        ("frankfurt", CountryId::DE, 2.0),
        ("köln", CountryId::DE, 2.0),
        ("cologne", CountryId::DE, 1.5),
        ("wien", CountryId::AT, 2.0),
        ("vienna", CountryId::AT, 1.5),
        ("salzburg", CountryId::AT, 2.0),
        ("graz", CountryId::AT, 2.0),
        ("innsbruck", CountryId::AT, 2.0),
        ("zürich", CountryId::CH, 2.0),
        ("zurich", CountryId::CH, 1.5),
        ("genève", CountryId::CH, 2.0),
        ("geneva", CountryId::CH, 1.5),
        ("bern", CountryId::CH, 1.5),
        ("basel", CountryId::CH, 2.0),
        ("lausanne", CountryId::CH, 2.0),
        ("brussels", CountryId::BE, 2.0),
        ("bruxelles", CountryId::BE, 2.0),
        ("brussel", CountryId::BE, 2.0),
        ("anderlecht", CountryId::BE, 2.0),
        ("anvers", CountryId::BE, 2.0),
        ("antwerpen", CountryId::BE, 2.0),
        ("antwerp", CountryId::BE, 2.0),
        ("liege", CountryId::BE, 1.5),
        ("liège", CountryId::BE, 2.0),
        ("gent", CountryId::BE, 2.0),
        ("ghent", CountryId::BE, 1.5),
    ] {
        if contains_word(lower, term) {
            add(evidence, *c, *w);
        }
    }
}

fn apply_script_signals(raw: &str, evidence: &mut [f32; 7]) {
    // ß is German/Austrian — Swiss German uses 'ss'.
    if raw.contains('ß') {
        add(evidence, CountryId::DE, 0.8);
        add(evidence, CountryId::AT, 0.8);
    }
    // ë / ï in lowercase French/Dutch contexts is mildly anti-DE
    // (DE doesn't use diaereses outside loanwords).
    if raw.contains('ë') || raw.contains('ï') {
        add(evidence, CountryId::FR, 0.3);
        add(evidence, CountryId::NL, 0.3);
        add(evidence, CountryId::BE, 0.3);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn top(v: &[(CountryId, f32)]) -> CountryId {
        v[0].0
    }
    fn weight(v: &[(CountryId, f32)], c: CountryId) -> f32 {
        v.iter()
            .find(|(x, _)| *x == c)
            .map(|(_, w)| *w)
            .unwrap_or(0.0)
    }

    #[test]
    fn weights_sum_to_one() {
        for q in [
            "Rue Wayez 122 1070 Anderlecht",
            "Damrak 1 1012 LP Amsterdam",
            "Friedrichstraße 100 10117 Berlin",
            "Stephansplatz 1 1010 Wien",
            "Bahnhofstrasse 1 8001 Zürich",
            "L-2453 Luxembourg",
            "10 rue de la Paix 75001 Paris",
            "",
            "no markers here just gibberish",
        ] {
            let r = classify_country(q);
            let s: f32 = r.iter().map(|(_, w)| w).sum();
            assert!((s - 1.0).abs() < 1e-3, "weights for {q:?} sum to {s}");
        }
    }

    #[test]
    fn top_country_be_for_brussels_query() {
        let r = classify_country("Rue Wayez 122 1070 Anderlecht");
        assert_eq!(top(&r), CountryId::BE);
    }

    #[test]
    fn top_country_fr_for_paris_query() {
        let r = classify_country("10 rue de la Paix 75001 Paris");
        assert_eq!(top(&r), CountryId::FR);
    }

    #[test]
    fn top_country_nl_for_amsterdam_query() {
        let r = classify_country("Damrak 1 1012 LP Amsterdam");
        assert_eq!(top(&r), CountryId::NL);
    }

    #[test]
    fn top_country_de_for_berlin_query() {
        let r = classify_country("Friedrichstraße 100 10117 Berlin");
        assert_eq!(top(&r), CountryId::DE);
    }

    #[test]
    fn top_country_at_for_vienna_query() {
        let r = classify_country("Stephansplatz 1 1010 Wien");
        assert_eq!(top(&r), CountryId::AT);
    }

    #[test]
    fn top_country_ch_for_zurich_query() {
        let r = classify_country("Bahnhofstrasse 1 8001 Zürich");
        assert_eq!(top(&r), CountryId::CH);
    }

    #[test]
    fn top_country_lu_for_lprefixed() {
        let r = classify_country("L-2453 Luxembourg");
        assert_eq!(top(&r), CountryId::LU);
    }

    #[test]
    fn ambiguous_4digit_postcode_ranks_be_lu_at() {
        let r = classify_country("1070");
        assert!(weight(&r, CountryId::BE) > 0.0);
        assert!(weight(&r, CountryId::LU) > 0.0);
        assert!(weight(&r, CountryId::AT) > 0.0);
        assert!(weight(&r, CountryId::CH) > 0.0);
    }

    #[test]
    fn ambiguous_5digit_postcode_ranks_fr_de() {
        let r = classify_country("75001");
        assert!(weight(&r, CountryId::FR) > 0.05);
        assert!(weight(&r, CountryId::DE) > 0.05);
    }

    #[test]
    fn empty_falls_back_to_uniform() {
        let r = classify_country("");
        let n = CountryId::ALL.len();
        for (_, w) in &r {
            assert!((*w - 1.0 / n as f32).abs() < 1e-3);
        }
    }
}
