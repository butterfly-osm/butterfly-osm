//! Recall — step 1 of the post-libpostal pipeline (#205).
//!
//! Two phases of retrieval, no parse intermediate. Recall produces
//! candidate `address_id`s; [`super::rerank`] scores them.
//!
//! ## Algorithm
//!
//! 1. Normalize the input string via [`crate::parser::normalize`].
//! 2. Run cheap deterministic priors (postcode regex extraction,
//!    country posterior) to gate which country FSTs to descend into.
//! 3. For each selected country FST:
//!    - Try **exact** match on the full normalized input → if it
//!      hits, the recall is O(1) and we stop early ("Zero-Cost-on-
//!      Clean-Queries").
//!    - Otherwise, generate candidate **prefixes** weighted by
//!      [`TaggerSignals::bio_logits`] (when present) — substrings
//!      where the tagger considers the bytes informative get higher
//!      weight. Each prefix scan is bounded by [`RecallBudget::max_fanout`].
//!    - Optionally fall back to a tokenwise scan: split on whitespace,
//!      treat each token as a prefix candidate.
//! 4. Aggregate postings; deduplicate by `(country, record_id)`; cap
//!    at [`RecallBudget::top_k`].
//!
//! ## Tagger signals as soft priors
//!
//! - `bio_logits[i]` is a per-input-byte log-prob over BIO labels.
//!   Bytes whose argmax label is non-`O` (street/house/postcode/
//!   locality) are weighted higher when generating prefixes.
//! - `country_posterior` is multiplied with the cheap classifier's
//!   per-country score; the merged top-K drives FST selection.
//! - `global_confidence` modulates how aggressively we expand
//!   prefixes — high confidence → fewer, longer prefixes; low
//!   confidence → more, shorter prefixes.
//!
//! Without tagger signals the recall still runs deterministically off
//! the cheap priors alone — the model is purely additive.

use std::collections::HashMap;

use crate::index::read::{Posting, RecallIndex};
use crate::parser::normalize::normalize;
use crate::routing::CountryId;
use crate::shard::SourceTag;

/// Number of BIO labels emitted by the tagger. Mirrors
/// [`crate::tagger::transformer::NUM_BIO_LABELS`] — duplicated here
/// only because [`TaggerSignals`] is the public boundary of the
/// recall API and we don't want a hard dep from `geocoder` to
/// `tagger` types in the trait surface.
pub const N_BIO_LABELS: usize = 9;

/// Soft priors emitted by the tagger that recall + rerank consume.
///
/// Construction: in production the tagger fills these from
/// [`crate::tagger::inference::InferenceOutput`]. In tests, deterministic
/// fixtures, or the no-model fallback path the values can be set to
/// neutral (`bio_logits = empty`, `country_posterior = empty`,
/// `global_confidence = 1.0`) — every consumer treats those as "no
/// signal" without crashing.
#[derive(Debug, Clone, Default)]
pub struct TaggerSignals {
    /// Per-input-byte log-prob distribution over BIO labels. Outer
    /// length matches `input.as_bytes().len()` modulo BOS/EOS strip.
    /// Empty when no tagger ran.
    pub bio_logits: Vec<[f32; N_BIO_LABELS]>,
    /// Per-country posterior `(country, p)` pairs. Sums to ~1.0 when
    /// non-empty; empty when the tagger did not produce a country
    /// signal (e.g. heuristic-only path).
    pub country_posterior: Vec<(CountryId, f32)>,
    /// Mean per-byte BIO confidence collapsed to a scalar in `[0, 1]`.
    /// Recall uses this to decide expansion aggressiveness; rerank
    /// uses it as a feature.
    pub global_confidence: f32,
}

/// One candidate emitted by [`Recaller::query`].
///
/// `recall_score` is a normalized lexical-alignment score in `[0, 1]`
/// — higher = better lexical match between the input and the
/// canonical key the candidate was retrieved under. Rerank consumes
/// it as a feature.
#[derive(Debug, Clone)]
pub struct Candidate {
    pub country: CountryId,
    pub address_id: u64,
    pub source_tag: SourceTag,
    pub recall_score: f32,
    /// Canonical key under which this candidate matched. Used by
    /// rerank to compute lexical-alignment features without
    /// re-deriving the key from the shard record.
    pub matched_key: String,
}

/// Budget knobs (`#97 invariant — recast`) controlling how aggressive
/// recall is. Constructed once per request from server defaults +
/// per-shard stats.
#[derive(Debug, Clone)]
pub struct RecallBudget {
    /// Maximum candidates returned across all countries.
    pub top_k: usize,
    /// Cap on postings examined per FST prefix scan. Adapted to the
    /// shard's p95 posting-list size by [`RecallBudget::adapt_to_stats`].
    pub max_fanout: usize,
    /// Cap on FST prefix scans per country. Each scan is bounded
    /// by `max_fanout`.
    pub max_prefix_scans: usize,
    /// Minimum prefix byte length. Below this we don't expand
    /// anywhere — guards against single-character spam queries.
    pub min_prefix_bytes: usize,
}

impl Default for RecallBudget {
    fn default() -> Self {
        Self {
            top_k: 50,
            max_fanout: 256,
            max_prefix_scans: 4,
            min_prefix_bytes: 3,
        }
    }
}

impl RecallBudget {
    /// Adapt `max_fanout` to a shard's recall stats. We size to
    /// roughly p95 to absorb common typo/abbreviation expansions
    /// without over-spending on an outlier key with an unusually
    /// long posting list.
    #[must_use]
    pub fn adapt_to_stats(mut self, p95_postings: u32) -> Self {
        let target = (p95_postings as usize).clamp(64, 4096);
        self.max_fanout = target;
        self
    }
}

/// Per-country recall handle. Holds the FST + postings for that
/// country.
#[derive(Debug)]
pub struct CountryRecall {
    pub country: CountryId,
    pub index: RecallIndex,
}

/// Top-level recall service. Holds one [`CountryRecall`] per loaded
/// shard.
#[derive(Debug, Default)]
pub struct Recaller {
    by_country: HashMap<CountryId, CountryRecall>,
}

impl Recaller {
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert a per-country recall handle.
    pub fn insert(&mut self, country: CountryId, index: RecallIndex) {
        self.by_country
            .insert(country, CountryRecall { country, index });
    }

    #[must_use]
    pub fn loaded_countries(&self) -> Vec<CountryId> {
        let mut v: Vec<CountryId> = self.by_country.keys().copied().collect();
        v.sort_by_key(|c| c.iso2());
        v
    }

    #[must_use]
    pub fn has(&self, country: CountryId) -> bool {
        self.by_country.contains_key(&country)
    }

    #[must_use]
    pub fn stats_for(&self, country: CountryId) -> Option<&crate::index::stats::ShardRecallStats> {
        self.by_country.get(&country).map(|c| c.index.stats())
    }

    /// Run recall against a list of countries (in priority order).
    ///
    /// Cheap deterministic priors (postcode regex, country posterior)
    /// are computed by the caller and threaded in via `signals` and
    /// `countries`. The recaller treats those as opaque ordering and
    /// gating signals — it does not re-derive them.
    pub fn query(
        &self,
        input: &str,
        signals: &TaggerSignals,
        countries: &[CountryId],
        budget: &RecallBudget,
    ) -> Vec<Candidate> {
        let normalized = normalize(input);
        if normalized.is_empty() {
            return Vec::new();
        }

        // Dedup by (country, record_id). Keep the highest score.
        let mut best: HashMap<(CountryId, u32), Candidate> = HashMap::new();

        for &c in countries {
            let Some(handle) = self.by_country.get(&c) else {
                continue;
            };
            self.query_one_country(handle, &normalized, signals, budget, &mut best);
            if best.len() >= budget.top_k {
                break;
            }
        }

        let mut out: Vec<Candidate> = best.into_values().collect();
        out.sort_by(|a, b| {
            b.recall_score
                .partial_cmp(&a.recall_score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.address_id.cmp(&b.address_id))
        });
        out.truncate(budget.top_k);
        out
    }

    fn query_one_country(
        &self,
        handle: &CountryRecall,
        normalized: &str,
        signals: &TaggerSignals,
        budget: &RecallBudget,
        out: &mut HashMap<(CountryId, u32), Candidate>,
    ) {
        let country = handle.country;

        // Phase 1: exact match on the full input. Strong-prior path —
        // O(1) FST descent, no allocation, no tail scans.
        let exact = handle.index.get(normalized);
        for p in &exact {
            insert_candidate(out, country, normalized, *p, 1.0);
        }
        if !exact.is_empty() {
            // Strong-prior queries early-exit. Honors the recast
            // "Zero-Cost-on-Clean-Queries" invariant.
            return;
        }

        // Phase 2: prefix scans on candidate substrings.
        let prefixes = build_prefix_candidates(normalized, signals, budget);
        let mut scans_left = budget.max_prefix_scans;
        for (prefix, weight) in prefixes {
            if scans_left == 0 {
                break;
            }
            scans_left -= 1;
            let hits = handle.index.prefix(&prefix, budget.max_fanout);
            for (key, p) in hits {
                let score = lexical_alignment_score(normalized, &key) * weight;
                insert_candidate(out, country, &key, p, score);
            }
            if out.len() >= budget.top_k {
                return;
            }
        }
    }
}

fn insert_candidate(
    out: &mut HashMap<(CountryId, u32), Candidate>,
    country: CountryId,
    matched_key: &str,
    p: Posting,
    score: f32,
) {
    let entry = out
        .entry((country, p.record_id))
        .or_insert_with(|| Candidate {
            country,
            address_id: ((country.iso2().as_bytes()[0] as u64) << 56)
                | ((country.iso2().as_bytes()[1] as u64) << 48)
                | u64::from(p.record_id),
            source_tag: p.source,
            recall_score: score,
            matched_key: matched_key.to_string(),
        });
    if score > entry.recall_score {
        entry.recall_score = score;
        entry.matched_key = matched_key.to_string();
    }
}

/// Build prefix candidates from the normalized input. Tagger BIO
/// logits (when present) bias which tokens we expand — bytes with
/// non-`O` argmax label (street/house/postcode/locality) are
/// considered informative.
fn build_prefix_candidates(
    normalized: &str,
    signals: &TaggerSignals,
    budget: &RecallBudget,
) -> Vec<(String, f32)> {
    let mut out: Vec<(String, f32)> = Vec::new();

    // First candidate: the whole normalized input as a prefix. This
    // covers the case where the input is itself a prefix of a longer
    // canonical key (e.g. user typed "rue wayez" and we want all
    // "rue wayez ..." matches).
    if normalized.len() >= budget.min_prefix_bytes {
        out.push((normalized.to_string(), 1.0));
    }

    // Per-token prefixes — split on whitespace, weighted by BIO
    // tagger if available. Without BIO signal, every token is treated
    // as equal weight 1.0 / N.
    let tokens: Vec<&str> = normalized
        .split_whitespace()
        .filter(|t| t.len() >= budget.min_prefix_bytes)
        .collect();
    if tokens.is_empty() {
        return out;
    }
    let n_tokens = tokens.len() as f32;

    // Map each token back to an approximate byte range in the
    // normalized string so we can sample BIO logits at that range.
    // Normalized != raw, so this is only an approximation; rerank
    // computes the exact agreement.
    let mut byte_cursor = 0usize;
    for tok in &tokens {
        let pos = normalized[byte_cursor..]
            .find(tok)
            .map(|p| byte_cursor + p)
            .unwrap_or(byte_cursor);
        byte_cursor = pos + tok.len();
        let bio_weight = bio_weight_for_range(signals, pos, pos + tok.len());
        let weight = (bio_weight + 1.0 / n_tokens).min(1.0);
        out.push(((*tok).to_string(), weight));
    }

    // Sort by weight descending, then by length descending (longer
    // tokens first when weights tie — they're more selective).
    out.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(b.0.len().cmp(&a.0.len()))
    });
    out.truncate(budget.max_prefix_scans);
    out
}

/// Mean per-byte probability of being in any non-`O` BIO label across
/// the requested byte range. Returns 0.0 when no signal is available.
///
/// `bio_logits` are log-probs; we exponentiate the sum of non-`O`
/// labels per byte and average. The `O` label is index 0 by the
/// tagger contract — see [`crate::tagger::transformer::BIO_O`].
fn bio_weight_for_range(signals: &TaggerSignals, start: usize, end: usize) -> f32 {
    if signals.bio_logits.is_empty() {
        return 0.0;
    }
    let lo = start.min(signals.bio_logits.len());
    let hi = end.min(signals.bio_logits.len());
    if lo >= hi {
        return 0.0;
    }
    let mut acc = 0.0_f32;
    let mut n = 0u32;
    for row in &signals.bio_logits[lo..hi] {
        // P(non-O) = 1 - exp(logp[O]); guard against numerical noise.
        let p_o = row[0].exp().clamp(0.0, 1.0);
        acc += 1.0 - p_o;
        n += 1;
    }
    if n == 0 { 0.0 } else { acc / n as f32 }
}

/// Normalized lexical alignment score in `[0, 1]` between an input
/// query and a key the FST returned.
///
/// We delegate to `rapidfuzz`'s normalized indel similarity — the same
/// metric the legacy executor used for fuzzy street matching. Stable,
/// fast, well-understood.
pub fn lexical_alignment_score(query: &str, key: &str) -> f32 {
    use rapidfuzz::distance::indel;
    if query.is_empty() || key.is_empty() {
        return 0.0;
    }
    indel::normalized_similarity(query.chars(), key.chars()) as f32
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::build::{BuildOptions, build_recall_index};
    use crate::routing::CountryId;
    use crate::shard::AddressRecord;
    use crate::shard::builder::build_shard;
    use crate::shard::reader::Shard;
    use tempfile::tempdir;

    fn fixture() -> (tempfile::TempDir, RecallIndex) {
        let dir = tempdir().unwrap();
        let p = dir.path().join("be.bfgs");
        let addrs = vec![
            AddressRecord {
                street: "Rue Wayez".into(),
                housenumber: "122".into(),
                postcode: "1070".into(),
                locality: "Anderlecht".into(),
                lat: 50.834,
                lon: 4.314,
                source: SourceTag::OpenAddresses,
                ..Default::default()
            },
            AddressRecord {
                street: "Grote Markt".into(),
                housenumber: "1".into(),
                postcode: "2000".into(),
                locality: "Antwerpen".into(),
                lat: 51.221,
                lon: 4.401,
                source: SourceTag::Osm,
                ..Default::default()
            },
        ];
        build_shard(&p, CountryId::BE, addrs).unwrap();
        let shard = Shard::open(&p).unwrap();
        build_recall_index(&p, &shard, &BuildOptions::default()).unwrap();
        let idx = RecallIndex::open(&p).unwrap();
        (dir, idx)
    }

    #[test]
    fn exact_match_o1_path() {
        let (_d, idx) = fixture();
        let mut recaller = Recaller::new();
        recaller.insert(CountryId::BE, idx);
        let signals = TaggerSignals::default();
        let cands = recaller.query(
            "Rue Wayez 122 1070 Anderlecht",
            &signals,
            &[CountryId::BE],
            &RecallBudget::default(),
        );
        assert_eq!(cands.len(), 1);
        assert_eq!(cands[0].source_tag, SourceTag::OpenAddresses);
        assert!((cands[0].recall_score - 1.0).abs() < 1e-6);
    }

    #[test]
    fn prefix_match_when_partial() {
        let (_d, idx) = fixture();
        let mut recaller = Recaller::new();
        recaller.insert(CountryId::BE, idx);
        let signals = TaggerSignals::default();
        let cands = recaller.query(
            "rue wayez",
            &signals,
            &[CountryId::BE],
            &RecallBudget::default(),
        );
        assert!(!cands.is_empty(), "expected at least one prefix hit");
        assert_eq!(cands[0].country, CountryId::BE);
    }

    #[test]
    fn empty_input_returns_empty() {
        let (_d, idx) = fixture();
        let mut recaller = Recaller::new();
        recaller.insert(CountryId::BE, idx);
        let signals = TaggerSignals::default();
        let cands = recaller.query("", &signals, &[CountryId::BE], &RecallBudget::default());
        assert!(cands.is_empty());
    }

    #[test]
    fn unknown_country_skipped() {
        let (_d, idx) = fixture();
        let mut recaller = Recaller::new();
        recaller.insert(CountryId::BE, idx);
        let signals = TaggerSignals::default();
        // BE not in the country list at all — the FR query yields nothing.
        let cands = recaller.query(
            "rue wayez",
            &signals,
            &[CountryId::FR],
            &RecallBudget::default(),
        );
        assert!(cands.is_empty());
    }

    #[test]
    fn lexical_alignment_score_bounds() {
        assert!((lexical_alignment_score("rue wayez", "rue wayez") - 1.0).abs() < 1e-6);
        assert_eq!(lexical_alignment_score("", "anything"), 0.0);
        assert_eq!(lexical_alignment_score("anything", ""), 0.0);
        let s = lexical_alignment_score("rue wayze", "rue wayez");
        assert!(s > 0.7 && s < 1.0);
    }
}
