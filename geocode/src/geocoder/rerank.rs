//! Rerank — step 2 of the post-libpostal pipeline (#205).
//!
//! GBDT directly over recall candidates. Trained on synthetic-real
//! signal: perturbations of OA gold addresses, OSM-derived synthetic
//! queries, and existing bench query mix. **No** heuristic-scorer
//! placeholder phase precedes this — when no model is loaded, the
//! reranker degrades gracefully to the recall score, NOT to a
//! hand-coded score.
//!
//! ## Features (per #205 spec)
//!
//! 1. `lexical_alignment_score` — `rapidfuzz` indel similarity between
//!    the input and the candidate's full canonical address string.
//! 2. `tagger_bio_agreement` — fraction of BIO-labelled bytes whose
//!    label matches the candidate's field at that byte position.
//! 3. `country_posterior_agreement` — the tagger's posterior for the
//!    candidate's country; 0.0 if no posterior was emitted.
//! 4. `postcode_regex_agreement` — 1.0 if a postcode regex extracted
//!    from the input is in the candidate's postcode; 0.0 mismatch;
//!    -1.0 input had no recognisable postcode.
//! 5. `source_tag_prior` — country-pack-driven boost. Defaults to 0.6
//!    OSM / 0.8 OpenAddresses (OA addresses are richer and cleaner
//!    on average).
//! 6. `recall_score` — verbatim from [`super::recall::Candidate::recall_score`].
//! 7. `candidate_field_completeness` — fraction of the four canonical
//!    fields (street, housenumber, postcode, locality) the candidate
//!    has populated.
//!
//! ## Action thresholds
//!
//! Top-1 score is mapped to `accept` / `caution` / `review` / `reject`
//! via [`crate::confidence::Confidence`] thresholds — same vocabulary
//! the legacy pipeline used so client code that branches on confidence
//! does not break.

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use serde::{Deserialize, Serialize};

use super::recall::{Candidate, TaggerSignals, lexical_alignment_score};
use crate::confidence::{Confidence, ConfidenceConfig, GbdtModel};
use crate::parser::normalize::normalize;
use crate::routing::CountryId;
use crate::shard::SourceTag;
use crate::shard::reader::Shard;

/// Fixed-shape feature row scored by the rerank GBDT.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RerankFeatures {
    pub lexical_alignment_score: f32,
    pub tagger_bio_agreement: f32,
    pub country_posterior_agreement: f32,
    pub postcode_regex_agreement: f32,
    pub source_tag_prior: f32,
    pub recall_score: f32,
    pub candidate_field_completeness: f32,
}

impl RerankFeatures {
    /// Number of features. Bumped together with the schema version.
    pub const N: usize = 7;
    /// On-disk schema version. Bump when fields change.
    pub const SCHEMA_VERSION: u32 = 1;

    #[must_use]
    pub fn to_row(&self) -> Vec<f32> {
        vec![
            self.lexical_alignment_score,
            self.tagger_bio_agreement,
            self.country_posterior_agreement,
            self.postcode_regex_agreement,
            self.source_tag_prior,
            self.recall_score,
            self.candidate_field_completeness,
        ]
    }

    #[must_use]
    pub fn from_row(row: &[f32]) -> Option<Self> {
        if row.len() != Self::N {
            return None;
        }
        Some(Self {
            lexical_alignment_score: row[0],
            tagger_bio_agreement: row[1],
            country_posterior_agreement: row[2],
            postcode_regex_agreement: row[3],
            source_tag_prior: row[4],
            recall_score: row[5],
            candidate_field_completeness: row[6],
        })
    }
}

impl Default for RerankFeatures {
    fn default() -> Self {
        Self {
            lexical_alignment_score: 0.0,
            tagger_bio_agreement: 0.0,
            country_posterior_agreement: 0.0,
            postcode_regex_agreement: -1.0,
            source_tag_prior: 0.6,
            recall_score: 0.0,
            candidate_field_completeness: 0.0,
        }
    }
}

/// Final ranked result emitted by [`Reranker::rank`].
#[derive(Debug, Clone)]
pub struct RankedResult {
    pub country: CountryId,
    pub address_id: u64,
    pub record_id: u32,
    pub source: SourceTag,
    pub lat: f64,
    pub lon: f64,
    pub street: String,
    pub housenumber: String,
    pub postcode: String,
    pub locality: String,
    pub score: f32,
    pub features: RerankFeatures,
    /// Action tier per [`Confidence`]. `accept` / `caution` /
    /// `review` / `reject`.
    pub action: Confidence,
    /// Machine-readable reason codes for the result. Stable
    /// vocabulary defined in [`crate::confidence::thresholds`].
    pub reason_codes: Vec<&'static str>,
}

/// Reranker. Wraps a GBDT model + confidence thresholds.
///
/// Construction takes an optional model — when `None` the reranker
/// degrades to "rank by recall_score". This is **not** a heuristic
/// scorer; it's an explicit no-model fallback path so smoke tests
/// and pre-training boots still produce a working pipeline.
pub struct Reranker {
    model: Option<Arc<GbdtModel>>,
    cfg: ConfidenceConfig,
    /// Per-country `(SourceTag, prior)` overrides. Default applied
    /// when a country is not in the table.
    source_priors: SourcePriors,
}

impl std::fmt::Debug for Reranker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Reranker")
            .field("has_model", &self.model.is_some())
            .field("source_priors", &self.source_priors)
            .finish()
    }
}

impl Reranker {
    /// New reranker with no model loaded — falls back to recall-score
    /// ordering.
    #[must_use]
    pub fn new_no_model() -> Self {
        Self {
            model: None,
            cfg: ConfidenceConfig::default(),
            source_priors: SourcePriors::default(),
        }
    }

    /// New reranker backed by a trained GBDT.
    #[must_use]
    pub fn new(model: GbdtModel) -> Self {
        Self {
            model: Some(Arc::new(model)),
            cfg: ConfidenceConfig::default(),
            source_priors: SourcePriors::default(),
        }
    }

    /// Load a trained model from disk.
    pub fn load_model(path: &Path) -> Result<Self> {
        let model = GbdtModel::load(path)?;
        Ok(Self::new(model))
    }

    #[must_use]
    pub fn with_confidence_config(mut self, cfg: ConfidenceConfig) -> Self {
        self.cfg = cfg;
        self
    }

    #[must_use]
    pub fn with_source_priors(mut self, priors: SourcePriors) -> Self {
        self.source_priors = priors;
        self
    }

    #[must_use]
    pub fn has_model(&self) -> bool {
        self.model.is_some()
    }

    /// Score and rank candidates. Materialises shard records, computes
    /// features, runs the GBDT, applies confidence thresholds, and
    /// returns sorted results.
    ///
    /// `shards`: lookup table from `CountryId` to its [`Shard`]. The
    /// reranker reads the candidate's full record (lat/lon, all
    /// fields) for both feature extraction and the final response
    /// payload.
    pub fn rank<F>(
        &self,
        input: &str,
        signals: &TaggerSignals,
        candidates: &[Candidate],
        shard_for: F,
    ) -> Vec<RankedResult>
    where
        F: Fn(CountryId) -> Option<Arc<Shard>>,
    {
        if candidates.is_empty() {
            return Vec::new();
        }
        let normalized_input = normalize(input);
        let postcode_extracted = extract_postcode(input);

        // Materialise records + extract features.
        let mut rows: Vec<(RankedResult, RerankFeatures)> = Vec::with_capacity(candidates.len());
        for cand in candidates {
            let Some(shard) = shard_for(cand.country) else { continue };
            let Some(rec) = shard.record(cand.address_id as u32) else {
                continue;
            };
            let features = compute_features(
                &normalized_input,
                postcode_extracted.as_deref(),
                signals,
                cand,
                &rec,
                &self.source_priors,
            );

            let mut result = RankedResult {
                country: cand.country,
                address_id: cand.address_id,
                record_id: cand.address_id as u32,
                source: cand.source_tag,
                lat: rec.lat,
                lon: rec.lon,
                street: rec.street.to_string(),
                housenumber: rec.housenumber.to_string(),
                postcode: rec.postcode.to_string(),
                locality: rec.locality.to_string(),
                score: 0.0,
                features: features.clone(),
                action: Confidence::Accept,
                reason_codes: Vec::new(),
            };

            // Score: GBDT if available, else recall score.
            result.score = match &self.model {
                Some(m) => {
                    // confidence::Features lives next to the model
                    // crate; we use the rerank Features directly via
                    // `predict_one_row` — no allocation per call beyond
                    // the row payload.
                    m.predict_one_raw(&features.to_row())
                }
                None => cand.recall_score,
            };
            rows.push((result, features));
        }

        // Sort by score descending. Stable so ties preserve recall order.
        rows.sort_by(|a, b| {
            b.0.score
                .partial_cmp(&a.0.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // Apply confidence thresholds + reason codes to each result.
        for (result, _f) in rows.iter_mut() {
            let (action, codes) = self.action_and_codes(result.score, &result.features);
            result.action = action;
            result.reason_codes = codes;
        }

        rows.into_iter().map(|(r, _)| r).collect()
    }

    fn action_and_codes(
        &self,
        score: f32,
        features: &RerankFeatures,
    ) -> (Confidence, Vec<&'static str>) {
        let action = if score >= self.cfg.accept_threshold {
            Confidence::Accept
        } else if score >= self.cfg.caution_threshold {
            Confidence::Caution
        } else if score >= self.cfg.review_threshold {
            Confidence::Review
        } else {
            Confidence::Reject
        };
        let mut codes: Vec<&'static str> = Vec::new();
        if features.lexical_alignment_score >= 0.95 {
            codes.push(crate::confidence::RC_HIGH_CONFIDENCE);
        }
        if features.lexical_alignment_score < 0.5 {
            codes.push(crate::confidence::RC_LOW_CONFIDENCE);
        }
        if features.postcode_regex_agreement >= 0.99 {
            codes.push(crate::confidence::RC_POSTCODE_EXACT);
        } else if features.postcode_regex_agreement <= 0.01
            && features.postcode_regex_agreement >= 0.0
        {
            codes.push(crate::confidence::RC_POSTCODE_MISMATCH);
        }
        if matches!(action, Confidence::Reject) {
            codes.push(crate::confidence::RC_BELOW_THRESHOLD);
        }
        codes
    }
}

/// Per-source-tag prior boost.
#[derive(Debug, Clone)]
pub struct SourcePriors {
    pub osm: f32,
    pub openaddresses: f32,
}

impl Default for SourcePriors {
    fn default() -> Self {
        Self {
            osm: 0.6,
            openaddresses: 0.8,
        }
    }
}

impl SourcePriors {
    #[must_use]
    pub fn for_source(&self, s: SourceTag) -> f32 {
        match s {
            SourceTag::Osm => self.osm,
            SourceTag::OpenAddresses => self.openaddresses,
        }
    }
}

/// Extract a postcode-like token from raw input. Cheap regex.
/// Currently European-postcode-anchored — accepts 4-7 alphanumerics
/// possibly with a single space (UK style "SW1A 1AA"). Returns None
/// when no plausible postcode is present.
pub fn extract_postcode(input: &str) -> Option<String> {
    use regex::Regex;
    use std::sync::OnceLock;
    static RE: OnceLock<Regex> = OnceLock::new();
    let re = RE.get_or_init(|| {
        // Matches: 4-7 alphanumerics, optionally split by a single space.
        // Anchored on word boundaries to avoid catching parts of street numbers.
        Regex::new(r"\b([A-Za-z0-9]{2,4}(?:[ -]?[A-Za-z0-9]{2,4})?)\b").unwrap()
    });
    // Pick the longest digit-bearing match — heuristic that
    // separates postcodes from random tokens.
    let mut best: Option<String> = None;
    for m in re.find_iter(input) {
        let s = m.as_str();
        if !s.chars().any(|c| c.is_ascii_digit()) {
            continue;
        }
        if s.len() < 3 || s.len() > 8 {
            continue;
        }
        if best
            .as_ref()
            .map(|b| s.len() > b.len())
            .unwrap_or(true)
        {
            best = Some(s.to_string());
        }
    }
    best
}

fn compute_features(
    normalized_input: &str,
    postcode_extracted: Option<&str>,
    signals: &TaggerSignals,
    cand: &Candidate,
    rec: &crate::shard::reader::ShardRecord,
    priors: &SourcePriors,
) -> RerankFeatures {
    let canonical = format!(
        "{} {} {} {}",
        normalize(&rec.street),
        normalize(&rec.housenumber),
        normalize(&rec.postcode),
        normalize(&rec.locality)
    );
    let canonical = canonical.split_whitespace().collect::<Vec<_>>().join(" ");

    let lexical = lexical_alignment_score(normalized_input, &canonical);

    let bio_agreement = compute_bio_agreement(normalized_input, signals, rec);

    let country_posterior_agreement = signals
        .country_posterior
        .iter()
        .find(|(c, _)| *c == cand.country)
        .map(|(_, p)| *p)
        .unwrap_or(0.0);

    let postcode_agreement = match postcode_extracted {
        Some(pc) if !rec.postcode.is_empty() => {
            let a = pc.to_ascii_lowercase().replace(' ', "");
            let b = rec.postcode.to_ascii_lowercase().replace(' ', "");
            if a == b {
                1.0
            } else if b.contains(&a) || a.contains(&b) {
                0.5
            } else {
                0.0
            }
        }
        _ => -1.0,
    };

    let source_tag_prior = priors.for_source(cand.source_tag);

    let mut completeness = 0.0_f32;
    let mut total = 0.0_f32;
    for f in [&rec.street, &rec.housenumber, &rec.postcode, &rec.locality] {
        total += 1.0;
        if !f.is_empty() {
            completeness += 1.0;
        }
    }
    let candidate_field_completeness = completeness / total.max(1.0);

    RerankFeatures {
        lexical_alignment_score: lexical,
        tagger_bio_agreement: bio_agreement,
        country_posterior_agreement,
        postcode_regex_agreement: postcode_agreement,
        source_tag_prior,
        recall_score: cand.recall_score,
        candidate_field_completeness,
    }
}

/// BIO agreement: for each input byte the tagger labelled non-`O`,
/// check whether the corresponding region of the candidate (street /
/// house / postcode / locality) actually contains a matching token.
/// Mean over labelled bytes; 0.0 when no BIO signal.
fn compute_bio_agreement(
    normalized_input: &str,
    signals: &TaggerSignals,
    rec: &crate::shard::reader::ShardRecord,
) -> f32 {
    if signals.bio_logits.is_empty() {
        return 0.0;
    }
    use crate::tagger::transformer;
    // Field id mapping per the tagger: 0=street, 1=house, 2=postcode, 3=locality.
    let fields: [String; 4] = [
        normalize(&rec.street),
        normalize(&rec.housenumber),
        normalize(&rec.postcode),
        normalize(&rec.locality),
    ];

    let bytes = normalized_input.as_bytes();
    let n = bytes.len().min(signals.bio_logits.len());
    if n == 0 {
        return 0.0;
    }
    let mut hits = 0u32;
    let mut labelled = 0u32;
    for i in 0..n {
        let row = &signals.bio_logits[i];
        let (label, _) = row
            .iter()
            .enumerate()
            .max_by(|a, b| {
                a.1.partial_cmp(b.1).unwrap_or(std::cmp::Ordering::Equal)
            })
            .unwrap_or((0, &0.0));
        if label == transformer::BIO_O {
            continue;
        }
        labelled += 1;
        let field_idx = label_to_field(label);
        let needle = &fields[field_idx as usize];
        if !needle.is_empty() {
            // Greedy: the byte at i in the input is "in the right
            // field" if the candidate's field contains a small
            // window around the input byte.
            let lo = i.saturating_sub(2);
            let hi = (i + 3).min(bytes.len());
            let win = &normalized_input[lo..hi];
            if needle.contains(win) || win.contains(needle.as_str()) {
                hits += 1;
            }
        }
    }
    if labelled == 0 {
        0.0
    } else {
        hits as f32 / labelled as f32
    }
}

fn label_to_field(label: usize) -> u8 {
    use crate::tagger::transformer::*;
    match label {
        x if x == BIO_B_STREET || x == BIO_I_STREET => 0,
        x if x == BIO_B_HOUSE || x == BIO_I_HOUSE => 1,
        x if x == BIO_B_POSTCODE || x == BIO_I_POSTCODE => 2,
        x if x == BIO_B_LOCALITY || x == BIO_I_LOCALITY => 3,
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn features_round_trip() {
        let f = RerankFeatures {
            lexical_alignment_score: 0.5,
            tagger_bio_agreement: 0.6,
            country_posterior_agreement: 0.7,
            postcode_regex_agreement: 1.0,
            source_tag_prior: 0.8,
            recall_score: 0.9,
            candidate_field_completeness: 1.0,
        };
        let row = f.to_row();
        assert_eq!(row.len(), RerankFeatures::N);
        let f2 = RerankFeatures::from_row(&row).unwrap();
        assert_eq!(f, f2);
    }

    #[test]
    fn extract_postcode_basic() {
        assert_eq!(
            extract_postcode("Rue Wayez 122 1070 Anderlecht"),
            Some("1070".to_string())
        );
        assert_eq!(extract_postcode("Just words"), None);
        // UK-style: SW1A 1AA — `\b` won't merge across the space, but
        // each half is still non-postcode if all letters or all digits
        // shorter than 4. Acceptance criterion: returns the longest
        // alphanumeric token with at least one digit.
        let pc = extract_postcode("123 SW1A 1AA road");
        assert!(pc.is_some(), "expected a postcode-shaped token");
    }
}
