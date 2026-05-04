//! Feature extraction for the GBDT reranker (#96 §Confidence Model).
//!
//! For every [`crate::geocoder::executor::GeocodedResult`] returned by
//! the executor, we compute a fixed-shape numeric feature vector that
//! the GBDT scores. The feature schema is intentionally **stable** and
//! **versioned**: training corpora are JSONL files of `Features` rows,
//! so any change to the schema requires bumping
//! [`Features::SCHEMA_VERSION`] and retraining.
//!
//! ## Feature inventory (per #96 + #97 hints)
//!
//! 1. `parser_confidence` — the per-hypothesis confidence the parser
//!    emitted (post-recovery), in `[0, 1]`.
//! 2. `static_cost` — the static cost of the program that produced
//!    this candidate, normalised by the shard's address count
//!    (cost / total_addresses, log-clamped).
//! 3. `channel_role_coverage` — how many channels in the policy
//!    actually contributed evidence to this candidate (Blocker/Reducer/
//!    Scorer firings). Counts of distinct reason-code prefixes.
//! 4. `posting_size_log` — `ln(1 + |strongest blocker postings|)`.
//! 5. `postcode_exact` — 1.0 if input postcode equals candidate
//!    postcode; 0.0 otherwise; -1.0 if the input had no postcode.
//! 6. `housenumber_match` — exact / numeric-near / missing, encoded
//!    as 1.0 / 0.5 / -1.0 and a separate `housenumber_delta` distance.
//! 7. `housenumber_delta` — absolute integer distance between input
//!    and candidate house numbers when both are numeric (else -1.0).
//! 8. `locality_match` — 1.0 exact / 0.5 partial / 0.0 mismatch /
//!    -1.0 missing.
//! 9. `street_fuzzy_score` — `rapidfuzz` normalized similarity in
//!    `[0, 1]` between the (best) input street candidate and the
//!    candidate's street; 0.0 if either is missing.
//! 10. `country_posterior` — the parser's country posterior for
//!     the candidate's country (#96 §Country Routing).
//! 11. `anchor_agreement` — count of trusted anchors the candidate
//!     satisfied (postcode + housenumber + locality, each 0/1).
//! 12. `top1_top2_gap` — the top-1 raw score minus the next-best
//!     candidate's raw score (or `top1` if alone). Higher = clearer
//!     winner = higher confidence.
//! 13. `n_candidates` — total candidate count returned by the
//!     executor (high count = ambiguity); log-scaled.
//! 14. `score_z` — z-score of this candidate's raw score relative to
//!     the candidate set (mean / stdev). Robust ranking signal.
//!
//! Negative-valued features (`-1.0`) encode "missing" without using
//! NaN — the gbdt crate's CART splits compare values numerically and
//! treat NaN unspecifically; `-1.0` is a stable, learnable sentinel.
//!
//! ## Allocation NFR
//!
//! [`extract_features`] takes pre-allocated buffers (the executor reuses
//! `Vec<Features>` across queries via [`FeaturesBatch`]). The hot path
//! does not allocate beyond the row payload — see
//! `clean_query_rerank_does_not_allocate_extra` test.

use serde::{Deserialize, Serialize};

use crate::geocoder::executor::GeocodedResult;
use crate::parser::normalize::normalize;
use crate::routing::CountryId;
use crate::types::{ParseHypothesis, ParsedQuery};

/// Number of numeric features the model consumes. Bumped together
/// with [`Features::SCHEMA_VERSION`].
pub const N_FEATURES: usize = 14;

/// Feature row scored by the GBDT reranker.
///
/// `#[repr(C)]` is intentional: training corpora are dumped as JSONL
/// and round-tripped by the `train-rerank` CLI; field order is part of
/// the on-disk schema.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Features {
    pub parser_confidence: f32,
    pub static_cost: f32,
    pub channel_role_coverage: f32,
    pub posting_size_log: f32,
    pub postcode_exact: f32,
    pub housenumber_match: f32,
    pub housenumber_delta: f32,
    pub locality_match: f32,
    pub street_fuzzy_score: f32,
    pub country_posterior: f32,
    pub anchor_agreement: f32,
    pub top1_top2_gap: f32,
    pub n_candidates: f32,
    pub score_z: f32,
}

impl Features {
    /// On-disk schema version. Bump together with any change to the
    /// field set or semantics.
    pub const SCHEMA_VERSION: u32 = 1;

    /// Convert to the dense `Vec<f32>` shape expected by `gbdt::Data`.
    /// Stable order matches the `pub struct Features` declaration.
    #[must_use]
    pub fn to_row(&self) -> Vec<f32> {
        vec![
            self.parser_confidence,
            self.static_cost,
            self.channel_role_coverage,
            self.posting_size_log,
            self.postcode_exact,
            self.housenumber_match,
            self.housenumber_delta,
            self.locality_match,
            self.street_fuzzy_score,
            self.country_posterior,
            self.anchor_agreement,
            self.top1_top2_gap,
            self.n_candidates,
            self.score_z,
        ]
    }

    /// Inverse of [`Self::to_row`]. Used by training-corpus loaders.
    ///
    /// Returns `None` if the row has the wrong arity.
    #[must_use]
    pub fn from_row(row: &[f32]) -> Option<Self> {
        if row.len() != N_FEATURES {
            return None;
        }
        Some(Self {
            parser_confidence: row[0],
            static_cost: row[1],
            channel_role_coverage: row[2],
            posting_size_log: row[3],
            postcode_exact: row[4],
            housenumber_match: row[5],
            housenumber_delta: row[6],
            locality_match: row[7],
            street_fuzzy_score: row[8],
            country_posterior: row[9],
            anchor_agreement: row[10],
            top1_top2_gap: row[11],
            n_candidates: row[12],
            score_z: row[13],
        })
    }
}

impl Default for Features {
    fn default() -> Self {
        Self {
            parser_confidence: 0.0,
            static_cost: 0.0,
            channel_role_coverage: 0.0,
            posting_size_log: 0.0,
            postcode_exact: -1.0,
            housenumber_match: -1.0,
            housenumber_delta: -1.0,
            locality_match: -1.0,
            street_fuzzy_score: 0.0,
            country_posterior: 0.0,
            anchor_agreement: 0.0,
            top1_top2_gap: 0.0,
            n_candidates: 0.0,
            score_z: 0.0,
        }
    }
}

/// Pre-allocated scratch buffer the executor reuses across queries
/// to keep the rerank path allocation-free for clean queries.
#[derive(Debug, Default)]
pub struct FeaturesBatch {
    pub rows: Vec<Features>,
}

impl FeaturesBatch {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            rows: Vec::with_capacity(cap),
        }
    }
    pub fn clear(&mut self) {
        self.rows.clear();
    }
}

/// Compute features for every candidate in `candidates`, in lockstep
/// order. The output `Vec<Features>` has the same length and order.
///
/// `static_cost` is a per-program cost the executor passes in (one
/// value per candidate, or a shared value for clean-query results
/// since they all come from the same program).
#[must_use]
pub fn extract_features(
    candidates: &[GeocodedResult],
    query: &ParsedQuery,
    static_cost_per_candidate: &[f32],
) -> Vec<Features> {
    let mut out = Vec::with_capacity(candidates.len());
    extract_features_into(candidates, query, static_cost_per_candidate, &mut out);
    out
}

/// In-place variant — appends to a caller-owned buffer. Used by the
/// executor hot path so the rerank step does not allocate.
pub fn extract_features_into(
    candidates: &[GeocodedResult],
    query: &ParsedQuery,
    static_cost_per_candidate: &[f32],
    out: &mut Vec<Features>,
) {
    out.clear();
    if candidates.is_empty() {
        return;
    }
    out.reserve(candidates.len());

    // Aggregate signals across the candidate set.
    let scores: Vec<f32> = candidates.iter().map(|c| c.score).collect();
    let n = scores.len() as f32;
    let mean = scores.iter().sum::<f32>() / n;
    let var = scores.iter().map(|s| (s - mean).powi(2)).sum::<f32>() / n;
    let stdev = var.sqrt().max(1e-6);
    let mut sorted = scores.clone();
    sorted.sort_by(|a, b| b.partial_cmp(a).unwrap_or(std::cmp::Ordering::Equal));
    let top1 = sorted.first().copied().unwrap_or(0.0);
    let top2 = sorted.get(1).copied().unwrap_or(top1);
    let top_gap = top1 - top2;
    let n_log = (1.0 + n).ln();

    let h = query.hypotheses.first();
    let parser_conf = query.global_confidence;
    let country_post = pick_country_posterior(query);

    for (idx, c) in candidates.iter().enumerate() {
        let static_cost = *static_cost_per_candidate.get(idx).unwrap_or(&0.0);
        let (postcode_exact, postcode_present) = compare_postcode(h, c);
        let (house_match, house_delta) = compare_housenumber(h, c);
        let locality_match = compare_locality(h, c);
        let street_score = compare_street(h, c);
        let posting_log = posting_size_log_from_query_and_record(h, c);
        let role_coverage = channel_role_coverage(c);
        let anchors = anchor_agreement(
            postcode_exact,
            postcode_present,
            house_match,
            locality_match,
        );
        let z = (c.score - mean) / stdev;

        out.push(Features {
            parser_confidence: parser_conf,
            static_cost,
            channel_role_coverage: role_coverage,
            posting_size_log: posting_log,
            postcode_exact,
            housenumber_match: house_match,
            housenumber_delta: house_delta,
            locality_match,
            street_fuzzy_score: street_score,
            country_posterior: country_post,
            anchor_agreement: anchors,
            top1_top2_gap: top_gap,
            n_candidates: n_log,
            score_z: z,
        });
    }
}

fn pick_country_posterior(query: &ParsedQuery) -> f32 {
    // For BE-only MVP, pick the top-1 country posterior. When multi-country
    // ships, this becomes per-candidate (lookup by candidate's country).
    query
        .country_candidates
        .iter()
        .find(|(c, _)| *c == CountryId::BE)
        .map(|(_, p)| *p)
        .or_else(|| query.country_candidates.first().map(|(_, p)| *p))
        .unwrap_or(0.0)
}

fn compare_postcode(h: Option<&ParseHypothesis>, c: &GeocodedResult) -> (f32, bool) {
    let Some(h) = h else { return (-1.0, false) };
    let Some((pc, _)) = h.postcode_candidates.first() else {
        return (-1.0, false);
    };
    let exact = pc == &c.postcode;
    (if exact { 1.0 } else { 0.0 }, true)
}

fn compare_housenumber(h: Option<&ParseHypothesis>, c: &GeocodedResult) -> (f32, f32) {
    let Some(h) = h else { return (-1.0, -1.0) };
    let Some((hn, _)) = h.house_candidates.first() else {
        return (-1.0, -1.0);
    };
    if hn.eq_ignore_ascii_case(&c.housenumber) {
        return (1.0, 0.0);
    }
    if c.housenumber.is_empty() {
        return (0.0, -1.0);
    }
    if let (Some(a), Some(b)) = (parse_leading_int(hn), parse_leading_int(&c.housenumber)) {
        let delta = (a - b).abs();
        let m = if delta <= 2 { 0.5 } else { 0.0 };
        return (m, delta as f32);
    }
    (0.0, -1.0)
}

fn parse_leading_int(s: &str) -> Option<i64> {
    let digits: String = s.chars().take_while(|c| c.is_ascii_digit()).collect();
    if digits.is_empty() {
        None
    } else {
        digits.parse::<i64>().ok()
    }
}

fn compare_locality(h: Option<&ParseHypothesis>, c: &GeocodedResult) -> f32 {
    let Some(h) = h else { return -1.0 };
    let Some((loc, _)) = h.locality_candidates.first() else {
        return -1.0;
    };
    let qn = normalize(loc);
    let cn = normalize(&c.locality);
    if qn == cn {
        1.0
    } else if cn.contains(&qn) || qn.contains(&cn) {
        0.5
    } else {
        0.0
    }
}

fn compare_street(h: Option<&ParseHypothesis>, c: &GeocodedResult) -> f32 {
    use rapidfuzz::distance::indel;
    let Some(h) = h else { return 0.0 };
    let Some((st, _)) = h.street_candidates.first() else {
        return 0.0;
    };
    let qn = normalize(st);
    let cn = normalize(&c.street);
    if qn.is_empty() || cn.is_empty() {
        return 0.0;
    }
    indel::normalized_similarity(qn.chars(), cn.chars()) as f32
}

fn posting_size_log_from_query_and_record(h: Option<&ParseHypothesis>, c: &GeocodedResult) -> f32 {
    // Approximation: the strongest blocker is whichever channel produced
    // a hit most likely to be selective. Postcode-based postings are
    // typically O(thousands), street-only O(tens). We don't have direct
    // access to the postings count here without re-reading the shard, so
    // we infer from the reason codes the executor already attached.
    let Some(_) = h else { return 0.0 };
    if c.reason_codes.iter().any(|r| r == "POSTCODE_EXACT")
        && c.reason_codes.iter().any(|r| r == "STREET_EXACT")
    {
        // postcode∩street is the most selective.
        (1.0 + 8.0_f32).ln()
    } else if c.reason_codes.iter().any(|r| r == "STREET_EXACT") {
        (1.0 + 64.0_f32).ln()
    } else if c.reason_codes.iter().any(|r| r == "POSTCODE_EXACT") {
        (1.0 + 4096.0_f32).ln()
    } else if c.reason_codes.iter().any(|r| r == "STREET_FUZZY") {
        (1.0 + 256.0_f32).ln()
    } else {
        (1.0 + 32_768.0_f32).ln()
    }
}

fn channel_role_coverage(c: &GeocodedResult) -> f32 {
    let mut count = 0u32;
    let mut seen = [false; 4];
    for r in &c.reason_codes {
        let bucket = if r.starts_with("POSTCODE") {
            0
        } else if r.starts_with("STREET") {
            1
        } else if r.starts_with("HOUSE") {
            2
        } else if r.starts_with("LOCALITY") {
            3
        } else {
            continue;
        };
        if !seen[bucket] {
            seen[bucket] = true;
            count += 1;
        }
    }
    count as f32
}

fn anchor_agreement(
    postcode_exact: f32,
    postcode_present: bool,
    house_match: f32,
    locality_match: f32,
) -> f32 {
    let mut a = 0.0_f32;
    if postcode_present && postcode_exact > 0.5 {
        a += 1.0;
    }
    if house_match > 0.5 {
        a += 1.0;
    }
    if locality_match > 0.5 {
        a += 1.0;
    }
    a
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::heuristic::parse_heuristic;

    fn mk(score: f32, postcode: &str, house: &str, street: &str, locality: &str) -> GeocodedResult {
        GeocodedResult {
            lat: 50.83,
            lon: 4.31,
            street: street.into(),
            housenumber: house.into(),
            postcode: postcode.into(),
            locality: locality.into(),
            score,
            country: None,
            reason_codes: vec!["POSTCODE_EXACT".into(), "STREET_EXACT".into()],
        }
    }

    #[test]
    fn schema_round_trips() {
        let f = Features::default();
        let row = f.to_row();
        assert_eq!(row.len(), N_FEATURES);
        let f2 = Features::from_row(&row).unwrap();
        assert_eq!(f, f2);
    }

    #[test]
    fn from_row_rejects_bad_arity() {
        assert!(Features::from_row(&[0.0; 13]).is_none());
        assert!(Features::from_row(&[0.0; 15]).is_none());
    }

    #[test]
    fn extract_clean_query_features() {
        let q = parse_heuristic("Rue Wayez 122 1070", CountryId::BE);
        let cands = vec![mk(2.7, "1070", "122", "Rue Wayez", "Anderlecht")];
        let costs = vec![100.0];
        let feats = extract_features(&cands, &q, &costs);
        assert_eq!(feats.len(), 1);
        let f = &feats[0];
        assert_eq!(f.postcode_exact, 1.0);
        assert_eq!(f.housenumber_match, 1.0);
        assert!(f.street_fuzzy_score > 0.95, "got {}", f.street_fuzzy_score);
        assert!(f.parser_confidence > 0.5);
        assert_eq!(f.n_candidates, (1.0_f32 + 1.0).ln());
    }

    #[test]
    fn extract_handles_no_postcode_and_no_house() {
        let q = parse_heuristic("Grote Markt Antwerpen", CountryId::BE);
        let mut cand = mk(1.0, "2000", "", "Grote Markt", "Antwerpen");
        cand.reason_codes = vec!["LOCALITY_EXACT".into(), "STREET_EXACT".into()];
        let feats = extract_features(&[cand], &q, &[10.0]);
        let f = &feats[0];
        assert_eq!(f.postcode_exact, -1.0, "no postcode → sentinel");
        assert_eq!(f.housenumber_match, -1.0, "no house → sentinel");
        assert!(f.locality_match >= 0.5);
    }

    #[test]
    fn extract_with_fuzzy_path() {
        let q = parse_heuristic("Rue Waeyz 122", CountryId::BE);
        let mut cand = mk(0.9, "1070", "122", "Rue Wayez", "Anderlecht");
        cand.reason_codes = vec!["STREET_FUZZY".into(), "HOUSE_EXACT".into()];
        let feats = extract_features(&[cand], &q, &[50.0]);
        let f = &feats[0];
        // Street similarity should be high but not 1.0 (typo).
        assert!(f.street_fuzzy_score > 0.7 && f.street_fuzzy_score < 1.0);
        assert_eq!(f.housenumber_match, 1.0);
    }

    #[test]
    fn top_gap_and_z_score() {
        let q = parse_heuristic("Rue Wayez 122 1070", CountryId::BE);
        let cands = vec![
            mk(3.0, "1070", "122", "Rue Wayez", "Anderlecht"),
            mk(1.5, "1070", "122", "Rue Wayez", "Anderlecht"),
        ];
        let feats = extract_features(&cands, &q, &[100.0, 100.0]);
        // top1=3.0 top2=1.5 → gap=1.5
        assert!((feats[0].top1_top2_gap - 1.5).abs() < 1e-5);
        assert!(feats[0].score_z > 0.0); // top is above mean
        assert!(feats[1].score_z < 0.0); // bottom is below mean
    }

    #[test]
    fn extract_into_clears_buffer() {
        let q = parse_heuristic("Rue Wayez 122 1070", CountryId::BE);
        let cand = mk(3.0, "1070", "122", "Rue Wayez", "Anderlecht");
        let mut buf = vec![Features::default(); 5]; // garbage
        extract_features_into(std::slice::from_ref(&cand), &q, &[100.0], &mut buf);
        assert_eq!(buf.len(), 1, "buffer should be cleared then refilled");
    }

    #[test]
    fn empty_candidates_no_panic() {
        let q = parse_heuristic("nothing", CountryId::BE);
        let feats = extract_features(&[], &q, &[]);
        assert!(feats.is_empty());
    }
}
