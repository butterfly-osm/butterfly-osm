//! Beam search core (#98 Phase 1).
//!
//! ## Search state
//!
//! A beam hypothesis is a per-byte BIO label assignment plus the
//! accumulated log-prob. Hypotheses are extended one byte at a time;
//! at each step the beam keeps the top-K by log-prob.
//!
//! ## Why this is not Viterbi
//!
//! Viterbi gives the single most likely label sequence under a
//! markov assumption. The transformer here is non-markov (it gives
//! per-position posteriors that depend on the whole input), so
//! Viterbi's marginalization isn't quite the right shape — and we
//! don't actually want the most likely sequence, we want the K
//! most likely **structured parses**, which are then handed to the
//! retrieval-aware decoder ([`super::decoding`]) for canonicalization +
//! anchor + utility scoring.
//!
//! Beam search is the simplest algorithm that produces a top-K with
//! the per-position posterior shape we have.

use super::anchor::{ANCHOR_PRUNE_LOGP, Anchor, AnchorField};
use crate::tagger::transformer::{BIO_O, NUM_BIO_LABELS, bio_to_field, is_b};

/// One beam hypothesis: a label sequence + cumulative log-prob.
#[derive(Debug, Clone)]
pub struct BeamHypothesis {
    /// Per-byte BIO label.
    pub labels: Vec<usize>,
    /// Cumulative log-prob so far.
    pub log_prob: f32,
    /// Cumulative entropy so far (sum of per-position entropies).
    /// Used by adaptive beam to detect locally uncertain regions.
    pub cum_entropy: f32,
}

/// Configuration for beam search.
#[derive(Debug, Clone, Copy)]
pub struct BeamConfig {
    /// Minimum beam width (used for confident regions). Default 2.
    pub min_width: usize,
    /// Maximum beam width (used for uncertain regions). Default 8.
    pub max_width: usize,
    /// Per-position entropy threshold above which the beam widens.
    /// Default 0.6 nats — picked empirically (entropy of a 9-class
    /// uniform distribution is ~2.2 nats; 0.6 corresponds to one
    /// dominant class with ~0.85 probability).
    pub entropy_widen_threshold: f32,
    /// Static-cost-fraction-of-ceiling above which expansion is
    /// suppressed. 0.7 per #98 spec.
    pub static_cost_suppress_fraction: f32,
}

impl Default for BeamConfig {
    fn default() -> Self {
        Self {
            min_width: 2,
            max_width: 8,
            entropy_widen_threshold: 0.6,
            static_cost_suppress_fraction: 0.7,
        }
    }
}

/// Compute the beam width for the next expansion step given local
/// signals.
///
/// `local_entropy` is the entropy of the BIO posterior at the current
/// position. `accumulated_static_cost_fraction` is `static_cost / ceiling`
/// for the program implied by the current beam frontier.
#[must_use]
pub fn adaptive_beam_width(
    cfg: &BeamConfig,
    local_entropy: f32,
    accumulated_static_cost_fraction: f32,
) -> usize {
    if accumulated_static_cost_fraction >= cfg.static_cost_suppress_fraction {
        // Suppress: minimum beam.
        return cfg.min_width.max(1);
    }
    if local_entropy >= cfg.entropy_widen_threshold {
        cfg.max_width
    } else {
        // Linear interpolation between min and max.
        let t = (local_entropy / cfg.entropy_widen_threshold).clamp(0.0, 1.0);
        let span = (cfg.max_width as f32) - (cfg.min_width as f32);
        let w = (cfg.min_width as f32) + t * span;
        w.round() as usize
    }
}

/// Run beam search over per-byte BIO logprobs.
///
/// `logprobs[t]` is a `[f32; NUM_BIO_LABELS]` row of log-probs at byte t.
/// `entropies[t]` is the per-byte entropy in nats.
/// `cfg` controls width adaptation.
/// `static_cost_fn` is called with a partial label sequence and returns
///   a static-cost fraction in `[0, 1]` (cost / ceiling). Used by adaptive
///   beam suppression.
pub fn beam_search(
    logprobs: &[[f32; NUM_BIO_LABELS]],
    entropies: &[f32],
    cfg: &BeamConfig,
    mut static_cost_fn: impl FnMut(&[usize]) -> f32,
) -> Vec<BeamHypothesis> {
    let n = logprobs.len();
    if n == 0 {
        return vec![BeamHypothesis {
            labels: Vec::new(),
            log_prob: 0.0,
            cum_entropy: 0.0,
        }];
    }

    let mut beam: Vec<BeamHypothesis> = vec![BeamHypothesis {
        labels: Vec::with_capacity(n),
        log_prob: 0.0,
        cum_entropy: 0.0,
    }];

    for (t, row) in logprobs.iter().enumerate().take(n) {
        let h = entropies.get(t).copied().unwrap_or(0.0);
        // Compute current beam's max static cost fraction (single most-cost
        // hypothesis dominates the budget — the beam tracks the worst case).
        let cost_frac = beam
            .iter()
            .map(|hyp| static_cost_fn(&hyp.labels))
            .fold(0.0_f32, f32::max);
        let width = adaptive_beam_width(cfg, h, cost_frac);

        // Generate all extensions (beam_size × NUM_BIO_LABELS).
        let mut extensions: Vec<BeamHypothesis> = Vec::with_capacity(beam.len() * NUM_BIO_LABELS);
        for hyp in &beam {
            // Constraint: I-X must follow either B-X or another I-X. Pre-filter
            // pure I-X transitions on top of O / different field — they're
            // demoted to B-X (per the inference span extractor's relaxed BIO
            // decode). Use the model's posterior assignment though; the
            // beam doesn't override the model.
            for (k, &lp) in row.iter().enumerate() {
                if !lp.is_finite() {
                    continue;
                }
                let mut next_labels = Vec::with_capacity(hyp.labels.len() + 1);
                next_labels.extend_from_slice(&hyp.labels);
                next_labels.push(k);
                let mut new_lp = hyp.log_prob + lp;
                // Tiny structural correction: penalize rare invalid BIO
                // transitions so the beam prefers structurally clean parses.
                if let Some(&prev) = hyp.labels.last()
                    && invalid_transition(prev, k)
                {
                    new_lp -= 0.5;
                }
                extensions.push(BeamHypothesis {
                    labels: next_labels,
                    log_prob: new_lp,
                    cum_entropy: hyp.cum_entropy + h,
                });
            }
        }

        // Keep top-`width` by log_prob.
        extensions.sort_by(|a, b| {
            b.log_prob
                .partial_cmp(&a.log_prob)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        extensions.truncate(width.max(1));
        beam = extensions;
    }

    beam
}

/// I-X following either O or I-Y (where Y != X) is structurally
/// invalid in strict BIO. We don't drop it (the model's posterior
/// might genuinely prefer it on noisy boundaries) but we apply a
/// small penalty so the beam prefers structurally clean parses
/// when both are roughly equiprobable.
fn invalid_transition(prev: usize, cur: usize) -> bool {
    if !is_i(cur) {
        return false;
    }
    let cur_field = bio_to_field(cur);
    if prev == BIO_O {
        return true;
    }
    if is_b(prev) || is_i(prev) {
        return bio_to_field(prev) != cur_field;
    }
    false
}

fn is_i(label: usize) -> bool {
    matches!(
        label,
        crate::tagger::transformer::BIO_I_STREET
            | crate::tagger::transformer::BIO_I_HOUSE
            | crate::tagger::transformer::BIO_I_POSTCODE
            | crate::tagger::transformer::BIO_I_LOCALITY
    )
}

/// Apply anchor pruning to a finished beam.
///
/// For each anchor, walk every hypothesis and check whether the
/// hypothesis's span at that anchor's byte range agrees with the
/// anchor's value (= the anchor's bytes are covered by the same
/// field's span). Adjust log-prob via [`super::anchor::anchor_score_adjustment`].
///
/// The full text is passed in so we can extract the bytes the
/// hypothesis labels as belonging to a given field.
pub fn apply_anchor_pruning(text: &str, beam: &mut [BeamHypothesis], anchors: &[Anchor]) {
    let bytes = text.as_bytes();
    for anchor in anchors {
        for hyp in beam.iter_mut() {
            let agrees = hypothesis_agrees_with_anchor(bytes, &hyp.labels, anchor);
            let adj = super::anchor::anchor_score_adjustment(agrees, anchor.confidence);
            hyp.log_prob += adj;
        }
    }
    // Hard-pruned hypotheses (logprob ≤ ANCHOR_PRUNE_LOGP * something)
    // can be filtered by the caller via [`drop_pruned`].
}

/// Drop hypotheses whose log-prob fell below the prune floor.
/// Done as a separate step so callers can inspect the full beam
/// for debugging before pruning.
pub fn drop_pruned(beam: Vec<BeamHypothesis>) -> Vec<BeamHypothesis> {
    let prune_floor = ANCHOR_PRUNE_LOGP / 2.0; // headroom
    beam.into_iter()
        .filter(|h| h.log_prob > prune_floor)
        .collect()
}

fn hypothesis_agrees_with_anchor(bytes: &[u8], labels: &[usize], anchor: &Anchor) -> bool {
    if anchor.byte_range.end > bytes.len() || anchor.byte_range.end > labels.len() {
        return false;
    }
    let target_field = match anchor.field {
        AnchorField::Postcode => 2u8,
        AnchorField::HouseNumber => 1u8,
        AnchorField::Locality => 3u8,
    };
    // Every byte inside anchor.byte_range must be labeled with the
    // target field. (Field id mapping: 0=street, 1=house, 2=postcode,
    // 3=locality — see [`crate::tagger::transformer::bio_to_field`].)
    for i in anchor.byte_range.clone() {
        let label = labels[i];
        match bio_to_field(label) {
            Some(f) if f == target_field => {}
            _ => return false,
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::super::anchor::Anchor;
    use super::*;

    fn flat_logprobs(label: usize, n: usize) -> Vec<[f32; NUM_BIO_LABELS]> {
        let mut row = [-3.0_f32; NUM_BIO_LABELS];
        row[label] = -0.1;
        vec![row; n]
    }

    #[test]
    fn adaptive_beam_widens_on_high_entropy() {
        let cfg = BeamConfig::default();
        let narrow = adaptive_beam_width(&cfg, 0.0, 0.0);
        let wide = adaptive_beam_width(&cfg, 1.5, 0.0);
        assert!(wide > narrow);
        assert_eq!(wide, cfg.max_width);
    }

    #[test]
    fn adaptive_beam_suppresses_on_high_static_cost() {
        let cfg = BeamConfig::default();
        let suppressed = adaptive_beam_width(&cfg, 1.5, 0.9);
        assert_eq!(suppressed, cfg.min_width);
    }

    #[test]
    fn beam_returns_top_k_assignments() {
        let n = 4;
        let logprobs = flat_logprobs(BIO_O, n);
        let entropies = vec![0.0_f32; n];
        let cfg = BeamConfig::default();
        let out = beam_search(&logprobs, &entropies, &cfg, |_| 0.0);
        assert!(!out.is_empty());
        // Best should be all-O.
        let best = &out[0];
        assert_eq!(best.labels, vec![BIO_O; n]);
    }

    #[test]
    fn anchor_pruning_downweights_disagreeing_hypotheses() {
        let text = "Rue 1000";
        let n = text.len();
        // Hypothesis A: labels '1000' as postcode (field=2).
        let mut a_labels = vec![BIO_O; n];
        a_labels[4] = crate::tagger::transformer::BIO_B_POSTCODE;
        for slot in a_labels.iter_mut().take(8).skip(5) {
            *slot = crate::tagger::transformer::BIO_I_POSTCODE;
        }
        // Hypothesis B: labels '1000' as house (field=1).
        let mut b_labels = vec![BIO_O; n];
        b_labels[4] = crate::tagger::transformer::BIO_B_HOUSE;
        for slot in b_labels.iter_mut().take(8).skip(5) {
            *slot = crate::tagger::transformer::BIO_I_HOUSE;
        }

        let mut beam = vec![
            BeamHypothesis {
                labels: a_labels,
                log_prob: -1.0,
                cum_entropy: 0.0,
            },
            BeamHypothesis {
                labels: b_labels,
                log_prob: -1.0,
                cum_entropy: 0.0,
            },
        ];
        let anchors = vec![Anchor {
            field: super::super::anchor::AnchorField::Postcode,
            value: "1000".to_string(),
            confidence: 1.0,
            byte_range: 4..8,
        }];
        apply_anchor_pruning(text, &mut beam, &anchors);
        // A agrees → 0 penalty. B disagrees → strong prune.
        assert!((beam[0].log_prob - (-1.0)).abs() < 1e-3);
        assert!(beam[1].log_prob < -50.0);
    }
}
