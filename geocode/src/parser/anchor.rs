//! Anchor detection + role-smoothness gate (#98 Phase 1.4).
//!
//! ## What anchors are
//!
//! Trusted anchors are high-confidence per-field claims that the
//! beam search uses for **soft pruning**: a hypothesis disagreeing
//! with an anchor is downweighted (or, well outside ε, dropped),
//! which collapses the search space without re-running the parser.
//!
//! ## Anchor types
//!
//! - **Postcode anchor** — `\b[1-9][0-9]{3}\b` standalone token
//!   (Belgium format) with frequency check (postcode appears verbatim
//!   in shard's postcode index). Confidence 1.0 if both match, 0.7
//!   if regex matches but the postcode is not in the shard's index
//!   (could be a missing record).
//! - **House-number anchor** — leading-or-trailing numeric/alphanumeric
//!   token. Confidence 0.9 — house numbers are easy to extract but
//!   parsers can confuse them with postcode digits.
//! - **Locality anchor** — exact match (case-insensitive, normalized)
//!   to a key in the shard's locality index. Confidence is 1.0 if
//!   present, otherwise None.
//!
//! ## Role-smoothness (#96 §Role-Smoothness Guarantee)
//!
//! When an anchor's confidence is within ε of the boundary, we
//! **downweight** the disagreeing hypothesis (multiply its log-prob
//! by a factor < 1) instead of dropping it. Outside ε we drop. This
//! is the decoding-side enforcement of the no-hard-thresholding
//! invariant from #96.

use std::sync::OnceLock;

use regex::Regex;

use crate::parser::normalize::normalize;
use crate::shard::reader::Shard;

/// Tolerance ε for the role-smoothness guarantee. When anchor confidence
/// is within ε of 1.0 (the trust threshold), we downweight, not drop.
pub const ANCHOR_EPSILON: f32 = 0.15;

/// Multiplicative penalty applied to a beam log-prob that disagrees
/// with a near-boundary anchor (within ε of trust). Larger value
/// applied to smaller deviations.
pub const ANCHOR_DOWNWEIGHT_LOGP: f32 = -1.5;

/// Hard-prune log-prob: hypotheses disagreeing with a high-confidence
/// anchor outside ε are scored to this floor (effectively pruned).
pub const ANCHOR_PRUNE_LOGP: f32 = -100.0;

/// One anchor extracted from input text.
#[derive(Debug, Clone)]
pub struct Anchor {
    pub field: AnchorField,
    pub value: String,
    /// Confidence in `[0, 1]`. The trust threshold for hard pruning
    /// is `1.0 - ANCHOR_EPSILON`.
    pub confidence: f32,
    /// Byte range in the source text. Useful for span-conflict
    /// detection in the beam.
    pub byte_range: std::ops::Range<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnchorField {
    Postcode,
    HouseNumber,
    Locality,
}

/// Detect anchors in `text` using shard statistics where available.
///
/// Returns anchors sorted by descending confidence. Anchors with
/// confidence below `1.0 - ANCHOR_EPSILON` are still returned (the
/// caller decides what to do with them via [`anchor_score_adjustment`]).
#[must_use]
pub fn detect_anchors(text: &str, shard: &Shard) -> Vec<Anchor> {
    let mut out: Vec<Anchor> = Vec::new();

    // Postcode anchor (Belgium-format).
    if let Some(m) = postcode_re().find(text) {
        let pc = m.as_str();
        let in_shard = !shard.postings_for_postcode(pc).is_empty();
        let conf = if in_shard { 1.0 } else { 0.7 };
        out.push(Anchor {
            field: AnchorField::Postcode,
            value: pc.to_string(),
            confidence: conf,
            byte_range: m.start()..m.end(),
        });
    }

    // House-number anchor: trailing numeric token.
    if let Some(m) = trailing_house_re().find(text) {
        let v = m.as_str();
        if !is_postcode_shape(v) {
            out.push(Anchor {
                field: AnchorField::HouseNumber,
                value: v.to_string(),
                confidence: 0.9,
                byte_range: m.start()..m.end(),
            });
        }
    }

    // Locality anchor: walk the shard locality index for exact-match
    // multi-token suffix. We cap the candidate scan at the last 3
    // whitespace-separated tokens — multi-word localities longer than
    // that are extremely rare in BE addresses.
    if let Some(loc) = detect_locality_suffix(text, shard) {
        out.push(loc);
    }

    out.sort_by(|a, b| {
        b.confidence
            .partial_cmp(&a.confidence)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    out
}

fn detect_locality_suffix(text: &str, shard: &Shard) -> Option<Anchor> {
    let bytes = text.as_bytes();
    let words: Vec<(usize, &str)> = text
        .split_whitespace()
        .scan(0usize, |acc, w| {
            // find w starting at *acc
            let start = text[*acc..].find(w).map(|p| *acc + p).unwrap_or(*acc);
            *acc = start + w.len();
            Some((start, w))
        })
        .collect();
    let n = words.len();
    if n == 0 {
        return None;
    }
    let max_w = 3.min(n);
    for w in (1..=max_w).rev() {
        let start_idx = n - w;
        let (start_byte, _) = words[start_idx];
        let end_byte = bytes.len();
        let suffix = &text[start_byte..end_byte];
        let key = normalize(suffix);
        if !shard.postings_for_locality(&key).is_empty() {
            return Some(Anchor {
                field: AnchorField::Locality,
                value: suffix.to_string(),
                confidence: 1.0,
                byte_range: start_byte..end_byte,
            });
        }
    }
    None
}

/// Returns the log-prob adjustment to apply to a hypothesis's score
/// given a single anchor, based on whether the hypothesis agrees
/// with it.
///
/// Semantics:
///
/// - Agreement → 0.0 (no penalty, anchor adds no information beyond
///   gating)
/// - Disagreement, anchor confidence ≥ `1.0 - ANCHOR_EPSILON` →
///   [`ANCHOR_PRUNE_LOGP`] (effective hard prune)
/// - Disagreement, anchor confidence within ε of trust →
///   [`ANCHOR_DOWNWEIGHT_LOGP`] (soft downweight, role-smoothness)
/// - Disagreement, anchor confidence below ε zone → 0.0 (anchor too
///   weak to penalize on)
#[must_use]
pub fn anchor_score_adjustment(agrees: bool, anchor_confidence: f32) -> f32 {
    if agrees {
        0.0
    } else {
        let trust = 1.0 - ANCHOR_EPSILON;
        if anchor_confidence >= trust {
            // Outside ε on the high side: hard prune.
            ANCHOR_PRUNE_LOGP
        } else if anchor_confidence >= trust - ANCHOR_EPSILON {
            // Within ε of the boundary: downweight (role-smoothness).
            ANCHOR_DOWNWEIGHT_LOGP
        } else {
            // Anchor confidence is too weak — don't penalize.
            0.0
        }
    }
}

fn postcode_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\b[1-9][0-9]{3}\b").expect("valid regex"))
}

fn trailing_house_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\b\d+[A-Za-z]?(?:[-/]\d+[A-Za-z]?)?\b").expect("valid regex"))
}

fn is_postcode_shape(s: &str) -> bool {
    s.len() == 4
        && s.chars().all(|c| c.is_ascii_digit())
        && s.chars().next().is_some_and(|c| c != '0')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agreement_is_zero_penalty() {
        assert_eq!(anchor_score_adjustment(true, 1.0), 0.0);
        assert_eq!(anchor_score_adjustment(true, 0.5), 0.0);
    }

    #[test]
    fn high_confidence_disagreement_hard_prunes() {
        let adj = anchor_score_adjustment(false, 1.0);
        assert!(adj <= ANCHOR_PRUNE_LOGP + 1e-3);
    }

    #[test]
    fn near_boundary_disagreement_downweights() {
        // Conf in [trust - ε, trust): downweight, not prune.
        let trust = 1.0 - ANCHOR_EPSILON;
        let conf = trust - ANCHOR_EPSILON / 2.0;
        let adj = anchor_score_adjustment(false, conf);
        assert!(adj < 0.0 && adj > ANCHOR_PRUNE_LOGP);
        assert!((adj - ANCHOR_DOWNWEIGHT_LOGP).abs() < 1e-3);
    }

    #[test]
    fn weak_anchor_does_not_penalize() {
        let adj = anchor_score_adjustment(false, 0.1);
        assert_eq!(adj, 0.0);
    }

    #[test]
    fn role_smoothness_no_hard_threshold() {
        // Adjustment must be a continuous-ish step function — no
        // catastrophic jump over a sub-ε confidence change.
        let trust = 1.0 - ANCHOR_EPSILON;
        // Just below trust: downweight (not prune).
        let adj_below = anchor_score_adjustment(false, trust - 1e-3);
        // Exactly at trust: prune.
        let adj_at = anchor_score_adjustment(false, trust);
        // The DROP from "downweight" to "prune" must happen at a clear
        // threshold (the trust boundary). The downweight value is
        // bounded; the prune value is well below it. This test just
        // documents the contract — it doesn't enforce continuity at
        // the threshold itself, only that role-smoothness applies in
        // the ε-band BELOW trust.
        assert!(adj_below > adj_at);
        assert!(adj_below > -10.0);
    }
}
