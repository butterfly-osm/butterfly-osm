//! Action thresholds + reason-code vocabulary (#96 §Confidence Model).
//!
//! Per #96 the reranker must surface **action-level** decisions, not raw
//! scores: `accept` / `caution` / `review` / `reject`. The thresholds
//! are tunable; the defaults here are the BE Phase-0 starting point and
//! will be retuned once production telemetry exists.
//!
//! Reason codes are `&'static str` to avoid per-query allocation. The
//! executor pushes them as `String` into `GeocodedResult::reason_codes`
//! to stay compatible with the existing JSON shape, but the ones the
//! reranker emits are interned constants — `String::from(STATIC)` is a
//! single bump-allocator alloc per emission, not a format-time alloc.

use super::features::Features;
use crate::geocoder::executor::GeocodedResult;

/// Country-posterior threshold below which a result is flagged
/// `COUNTRY_UNCERTAIN`.
pub const COUNTRY_UNCERTAIN_BELOW: f32 = 0.7;

/// Street fuzzy-score threshold below which a result is flagged
/// `STREET_WEAK`.
pub const STREET_WEAK_BELOW: f32 = 0.6;

// Reason-code constants. Use these from the reranker; the existing
// scorer paths still emit their own codes verbatim.
pub const RC_RERANK_GBDT: &str = "RERANK_GBDT";
pub const RC_HIGH_CONFIDENCE: &str = "HIGH_CONFIDENCE";
pub const RC_LOW_CONFIDENCE: &str = "LOW_CONFIDENCE";
pub const RC_BELOW_THRESHOLD: &str = "BELOW_THRESHOLD";
pub const RC_COUNTRY_UNCERTAIN: &str = "COUNTRY_UNCERTAIN";
pub const RC_STREET_WEAK: &str = "STREET_WEAK";
pub const RC_POSTCODE_EXACT: &str = "POSTCODE_EXACT";
pub const RC_POSTCODE_MISMATCH: &str = "POSTCODE_MISMATCH";

/// Action tier returned by [`apply_thresholds`] for the top result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Confidence {
    /// `score >= accept` — caller can act on the result without
    /// further review.
    Accept,
    /// `caution <= score < accept` — return as-is but mark.
    Caution,
    /// `review <= score < caution` — return but flag in API response.
    Review,
    /// `score < review` — suppress; emit empty result with
    /// `BELOW_THRESHOLD` code.
    Reject,
}

impl Confidence {
    pub const fn as_str(self) -> &'static str {
        match self {
            Confidence::Accept => "accept",
            Confidence::Caution => "caution",
            Confidence::Review => "review",
            Confidence::Reject => "reject",
        }
    }
}

/// Tunable knobs for the action thresholds.
///
/// Defaults are the Belgium Phase-0 starting point (#96):
/// `accept >= 0.85, caution >= 0.5, review >= 0.2`.
#[derive(Debug, Clone, Copy)]
pub struct ConfidenceConfig {
    pub accept_at: f32,
    pub caution_at: f32,
    pub review_at: f32,
}

impl Default for ConfidenceConfig {
    fn default() -> Self {
        Self {
            accept_at: 0.85,
            caution_at: 0.5,
            review_at: 0.2,
        }
    }
}

impl ConfidenceConfig {
    /// Classify a single GBDT score into an action tier.
    #[must_use]
    pub fn classify(&self, score: f32) -> Confidence {
        if score >= self.accept_at {
            Confidence::Accept
        } else if score >= self.caution_at {
            Confidence::Caution
        } else if score >= self.review_at {
            Confidence::Review
        } else {
            Confidence::Reject
        }
    }
}

/// Apply thresholds + secondary reason codes to a candidate list (which
/// the reranker has already sorted by score descending).
///
/// - `Accept`: top-1 keeps its codes; `HIGH_CONFIDENCE` is appended.
/// - `Caution`: `LOW_CONFIDENCE` is appended.
/// - `Review`: `LOW_CONFIDENCE` is appended; the API layer is expected
///   to surface a `confidence: "review"` field (handled in the handler).
/// - `Reject`: ALL candidates are dropped. The handler returns an
///   empty result list with a synthetic `BELOW_THRESHOLD` marker on
///   the response (handler-level — this function returns empty here).
///
/// Secondary codes (`STREET_WEAK`, `COUNTRY_UNCERTAIN`,
/// `POSTCODE_MISMATCH`) are emitted on each kept candidate based on its
/// per-row [`Features`].
///
/// Returns the action tier of the top-1 candidate (or `Reject` if the
/// list is empty after filtering).
pub fn apply_thresholds(
    candidates: &mut Vec<GeocodedResult>,
    features: &[Features],
    cfg: &ConfidenceConfig,
) -> Confidence {
    if candidates.is_empty() {
        return Confidence::Reject;
    }
    debug_assert_eq!(candidates.len(), features.len());
    let top_score = candidates[0].score;
    let tier = cfg.classify(top_score);

    // Annotate per-candidate reason codes (skip if reject — we drop the list).
    if tier != Confidence::Reject {
        for (cand, feat) in candidates.iter_mut().zip(features.iter()) {
            annotate_secondary_reasons(cand, feat);
        }
        // Top-level action code on the top result only.
        match tier {
            Confidence::Accept => push_unique(&mut candidates[0].reason_codes, RC_HIGH_CONFIDENCE),
            Confidence::Caution | Confidence::Review => {
                push_unique(&mut candidates[0].reason_codes, RC_LOW_CONFIDENCE);
            }
            Confidence::Reject => {} // unreachable here, but kept for exhaustive match
        }
    } else {
        // Reject: clear the list. Caller decides whether to attach a
        // `BELOW_THRESHOLD` marker on the response envelope. We keep
        // the call site simple by leaving it empty.
        candidates.clear();
    }

    tier
}

fn annotate_secondary_reasons(c: &mut GeocodedResult, f: &Features) {
    if f.country_posterior < COUNTRY_UNCERTAIN_BELOW {
        push_unique(&mut c.reason_codes, RC_COUNTRY_UNCERTAIN);
    }
    if f.street_fuzzy_score < STREET_WEAK_BELOW && f.street_fuzzy_score > 0.0 {
        push_unique(&mut c.reason_codes, RC_STREET_WEAK);
    }
    // Postcode: 1.0 = exact, 0.0 = mismatch (input had a postcode but
    // candidate's postcode differs), -1.0 = input had no postcode.
    if f.postcode_exact == 0.0 {
        push_unique(&mut c.reason_codes, RC_POSTCODE_MISMATCH);
    } else if (f.postcode_exact - 1.0).abs() < 1e-3 {
        push_unique(&mut c.reason_codes, RC_POSTCODE_EXACT);
    }
}

fn push_unique(codes: &mut Vec<std::borrow::Cow<'static, str>>, code: &'static str) {
    if !codes.iter().any(|c| c.as_ref() == code) {
        codes.push(std::borrow::Cow::Borrowed(code));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cand(score: f32) -> GeocodedResult {
        GeocodedResult {
            lat: 50.0,
            lon: 4.0,
            street: "s".into(),
            housenumber: "1".into(),
            postcode: "1000".into(),
            locality: "loc".into(),
            score,
            country: None,
            reason_codes: vec![],
        }
    }

    #[test]
    fn classify_default_thresholds() {
        let cfg = ConfidenceConfig::default();
        assert_eq!(cfg.classify(0.95), Confidence::Accept);
        assert_eq!(cfg.classify(0.85), Confidence::Accept);
        assert_eq!(cfg.classify(0.7), Confidence::Caution);
        assert_eq!(cfg.classify(0.5), Confidence::Caution);
        assert_eq!(cfg.classify(0.3), Confidence::Review);
        assert_eq!(cfg.classify(0.2), Confidence::Review);
        assert_eq!(cfg.classify(0.1), Confidence::Reject);
        assert_eq!(cfg.classify(0.0), Confidence::Reject);
    }

    #[test]
    fn apply_accept_path() {
        let mut cands = vec![cand(0.9), cand(0.7)];
        let mut feats = vec![Features::default(); 2];
        feats[0].country_posterior = 1.0;
        feats[0].street_fuzzy_score = 1.0;
        feats[0].postcode_exact = 1.0;
        feats[1].country_posterior = 1.0;
        feats[1].street_fuzzy_score = 1.0;
        feats[1].postcode_exact = 1.0;
        let cfg = ConfidenceConfig::default();
        let t = apply_thresholds(&mut cands, &feats, &cfg);
        assert_eq!(t, Confidence::Accept);
        assert_eq!(cands.len(), 2);
        assert!(
            cands[0]
                .reason_codes
                .iter()
                .any(|r| r == RC_HIGH_CONFIDENCE)
        );
        assert!(
            !cands[1]
                .reason_codes
                .iter()
                .any(|r| r == RC_HIGH_CONFIDENCE)
        );
        // Both candidates carry POSTCODE_EXACT (from features).
        assert!(cands[0].reason_codes.iter().any(|r| r == RC_POSTCODE_EXACT));
    }

    #[test]
    fn apply_caution_emits_low_confidence_on_top() {
        let mut cands = vec![cand(0.7)];
        let feats = vec![Features::default()];
        let cfg = ConfidenceConfig::default();
        let t = apply_thresholds(&mut cands, &feats, &cfg);
        assert_eq!(t, Confidence::Caution);
        assert!(cands[0].reason_codes.iter().any(|r| r == RC_LOW_CONFIDENCE));
    }

    #[test]
    fn apply_reject_clears_candidates() {
        let mut cands = vec![cand(0.1)];
        let feats = vec![Features::default()];
        let cfg = ConfidenceConfig::default();
        let t = apply_thresholds(&mut cands, &feats, &cfg);
        assert_eq!(t, Confidence::Reject);
        assert!(cands.is_empty());
    }

    #[test]
    fn weak_street_emits_secondary_code() {
        let mut cands = vec![cand(0.9)];
        let mut feats = vec![Features::default(); 1];
        feats[0].country_posterior = 1.0;
        feats[0].street_fuzzy_score = 0.4; // weak
        feats[0].postcode_exact = 1.0;
        let cfg = ConfidenceConfig::default();
        let _ = apply_thresholds(&mut cands, &feats, &cfg);
        assert!(cands[0].reason_codes.iter().any(|r| r == RC_STREET_WEAK));
    }

    #[test]
    fn country_uncertain_secondary_code() {
        let mut cands = vec![cand(0.9)];
        let mut feats = vec![Features::default(); 1];
        feats[0].country_posterior = 0.5; // below 0.7
        feats[0].street_fuzzy_score = 1.0;
        feats[0].postcode_exact = 1.0;
        let cfg = ConfidenceConfig::default();
        let _ = apply_thresholds(&mut cands, &feats, &cfg);
        assert!(
            cands[0]
                .reason_codes
                .iter()
                .any(|r| r == RC_COUNTRY_UNCERTAIN)
        );
    }

    #[test]
    fn postcode_mismatch_secondary_code() {
        let mut cands = vec![cand(0.9)];
        let mut feats = vec![Features::default(); 1];
        feats[0].country_posterior = 1.0;
        feats[0].street_fuzzy_score = 1.0;
        feats[0].postcode_exact = 0.0; // mismatch
        let cfg = ConfidenceConfig::default();
        let _ = apply_thresholds(&mut cands, &feats, &cfg);
        assert!(
            cands[0]
                .reason_codes
                .iter()
                .any(|r| r == RC_POSTCODE_MISMATCH)
        );
    }
}
