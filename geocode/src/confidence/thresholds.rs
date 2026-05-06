//! Action thresholds + reason-code vocabulary (#205, simplified).
//!
//! Per the redesign, the thresholds + reason codes survive but the
//! per-result mutation helper (`apply_thresholds`) was inlined into
//! [`crate::geocoder::rerank::Reranker::action_and_codes`]. This
//! module is now just stable constants + the [`Confidence`] enum.

/// Country-posterior threshold below which a result is flagged
/// `COUNTRY_UNCERTAIN`.
pub const COUNTRY_UNCERTAIN_BELOW: f32 = 0.7;

/// Street fuzzy-score threshold below which a result is flagged
/// `STREET_WEAK`.
pub const STREET_WEAK_BELOW: f32 = 0.6;

// Reason-code constants. Stable vocabulary across the redesign so
// existing client code that branches on these does not break.
pub const RC_RERANK_GBDT: &str = "RERANK_GBDT";
pub const RC_HIGH_CONFIDENCE: &str = "HIGH_CONFIDENCE";
pub const RC_LOW_CONFIDENCE: &str = "LOW_CONFIDENCE";
pub const RC_BELOW_THRESHOLD: &str = "BELOW_THRESHOLD";
pub const RC_COUNTRY_UNCERTAIN: &str = "COUNTRY_UNCERTAIN";
pub const RC_STREET_WEAK: &str = "STREET_WEAK";
pub const RC_POSTCODE_EXACT: &str = "POSTCODE_EXACT";
pub const RC_POSTCODE_MISMATCH: &str = "POSTCODE_MISMATCH";

/// Action tier of a ranked result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Confidence {
    Accept,
    Caution,
    Review,
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
/// Defaults are the Belgium Phase-0 starting point:
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_tiers() {
        let cfg = ConfidenceConfig::default();
        assert_eq!(cfg.classify(0.95), Confidence::Accept);
        assert_eq!(cfg.classify(0.7), Confidence::Caution);
        assert_eq!(cfg.classify(0.3), Confidence::Review);
        assert_eq!(cfg.classify(0.05), Confidence::Reject);
    }
}
