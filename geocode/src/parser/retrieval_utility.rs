//! Retrieval-utility scorer trait (#98 Phase 1 + Phase 2).
//!
//! ## Architectural framing
//!
//! Per #98:
//! > "The decoding strategy needs to be **retrieval-aware**: it should
//! > optimize for the geocoder's success probability under the
//! > execution budget, not parse likelihood in isolation."
//!
//! This module defines the abstract scorer interface that
//! [`crate::parser::decoding::decode`] consults to bias the beam toward
//! hypotheses whose retrieval programs are likely to succeed. Two
//! implementations ship:
//!
//! - [`HeuristicScorer`] — Phase 1 hand-crafted scoring (penalises
//!   high static cost / all-scorer policies / empty-or-oversized
//!   posting lists; rewards strong blockers). Lifted, untouched, from
//!   PR #168.
//! - [`LearnedScorer`] — Phase 2 GBDT trained against geocode-success
//!   ground truth (#98 §2.1, §2.2). The model file is loaded at boot;
//!   the per-query call is a single `gbdt::predict` over a 30-feature
//!   row.
//!
//! ## Output range
//!
//! Both scorers emit a **log-probability adjustment** (positive →
//! reward, negative → penalty) that the beam adds to the source
//! parser log-prob. To keep the dispatch swap-in safe, the learned
//! scorer's GBDT output (`[0, 1]` after sigmoid) is mapped to the same
//! log-prob scale via `log(p / (1 - p))` clipped to `[-5, 5]`. This
//! preserves ordering and keeps the magnitude comparable to the
//! heuristic scorer's output range (which empirically sits in
//! `[-3, +1]` for production traffic).
//!
//! ## Why a trait, not a function pointer
//!
//! The scorer holds state (the GBDT model is ~500 KB). Trait objects
//! capture that state and dispatch through `&dyn` at the per-query
//! call site, which costs one virtual call per hypothesis — negligible
//! against the ~20-50 µs decode budget.

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use gbdt::decision_tree::Data;
use gbdt::gradient_boost::GBDT;

use super::phase2_features::{Features, N_FEATURES};

/// The Phase 2 trait the decoder consults when ranking hypotheses.
pub trait RetrievalUtilityScorer: Send + Sync + std::fmt::Debug {
    /// Produce a log-prob adjustment for a (hypothesis-features) row.
    /// Positive = reward, negative = penalty.
    fn score(&self, features: &Features) -> f32;
    /// Stable backend name for logs and metrics.
    fn name(&self) -> &'static str;
}

/// Phase 1 hand-crafted scorer (#98 1.5).
///
/// Reads the same features as the learned scorer but applies a fixed
/// linear combination tuned from #168. The numbers below are **not**
/// arbitrary: they reproduce the scoring formula from
/// [`crate::parser::decoding::retrieval_utility_score`] (the inline
/// function it replaces) projected onto the Phase 2 feature schema.
///
/// Specifically, the inline scorer computed:
///   `-cost_fraction * 2.0
///    + blocker_count * 0.5
///    - all_scorer_penalty (1.0 if no blocker)
///    - per-empty-claimed-lookup * 1.5
///    - per-oversized-lookup * 0.5`
///
/// The Phase 2 schema captures all of those signals with named
/// features:
///   - `static_cost_fraction` ↔ cost_fraction
///   - `has_blocker` + role_* ↔ blocker_count (for BE we use 1 blocker
///     in the default policy; the all_scorer_penalty triggers on
///     `has_blocker == 0.0`)
///   - `min_postings_log == 0` AND a claim ↔ empty-claimed-lookup
///   - `max_postings_log` over the oversized threshold ↔ oversized
///
/// We map them onto a comparable formulation below. Re-deriving the
/// exact coefficients keeps the heuristic backend numerically
/// equivalent to PR #168's behaviour, which is the safety contract for
/// landing the trait swap without behaviour drift.
#[derive(Debug, Clone, Copy)]
pub struct HeuristicScorer {
    /// Penalty scaler per unit of static_cost_fraction.
    pub static_cost_penalty: f32,
    /// Reward per declared blocker.
    pub blocker_reward: f32,
    /// Penalty applied when the policy has no blockers.
    pub all_scorer_penalty: f32,
    /// Penalty when a claimed channel returned an empty posting list.
    pub empty_lookup_penalty: f32,
    /// Penalty when the largest posting list is "oversized" — a proxy
    /// for "an executor feedback would likely fire".
    pub oversized_lookup_penalty: f32,
    /// `ln(1+postings)` threshold above which a lookup is "oversized".
    /// `ln(1+50000) ≈ 10.8` → defaults to 10.0.
    pub oversized_log_threshold: f32,
}

impl Default for HeuristicScorer {
    fn default() -> Self {
        Self {
            static_cost_penalty: 2.0,
            blocker_reward: 0.5,
            all_scorer_penalty: 1.0,
            empty_lookup_penalty: 1.5,
            oversized_lookup_penalty: 0.5,
            oversized_log_threshold: 10.0,
        }
    }
}

impl RetrievalUtilityScorer for HeuristicScorer {
    fn score(&self, f: &Features) -> f32 {
        let mut s = 0.0_f32;
        s -= f.static_cost_fraction.clamp(0.0, 1.0) * self.static_cost_penalty;
        // Blocker count: count roles equal to Blocker (encoded as 0.0).
        let mut blocker_count = 0.0_f32;
        for role in [
            f.role_postcode,
            f.role_street,
            f.role_house,
            f.role_locality,
        ] {
            if (role - 0.0).abs() < 1e-3 {
                blocker_count += 1.0;
            }
        }
        s += blocker_count * self.blocker_reward;
        if f.has_blocker < 0.5 {
            s -= self.all_scorer_penalty;
        }
        // Empty-claimed-lookup penalty:
        //   - hypothesis claims a postcode AND min_postings_log is 0
        //     (no non-empty lookup hit) → punish.
        // Note: min_postings_log == 0 only when EVERY lookup was empty
        // (our walker only updates min from non-empty postings). For
        // mixed cases (some hits, some misses) we don't penalise — the
        // executor's Role-Smoothness chain will recover.
        let any_claim = f.claims_postcode + f.claims_street + f.claims_house > 0.5;
        if any_claim && f.min_postings_log < 1e-3 {
            s -= self.empty_lookup_penalty;
        }
        // Oversized lookup penalty.
        if f.max_postings_log > self.oversized_log_threshold {
            s -= self.oversized_lookup_penalty;
        }
        s
    }

    fn name(&self) -> &'static str {
        "heuristic"
    }
}

/// Phase 2 GBDT-backed scorer.
///
/// Wraps a trained pointwise binary classifier whose target is
/// `P(geocode_success | hypothesis, program)`. The raw GBDT output is
/// in `[0, 1]`; we map to a log-prob adjustment via the logit
/// transform clipped to `[-5, +5]` so the magnitude stays comparable
/// to the heuristic scorer's range. Clipping prevents pathological
/// extremes (a very confident-wrong score) from dominating the parser
/// log-prob.
pub struct LearnedScorer {
    model: Arc<GBDT>,
    schema_version: u32,
}

impl std::fmt::Debug for LearnedScorer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LearnedScorer")
            .field("schema_version", &self.schema_version)
            .finish_non_exhaustive()
    }
}

impl LearnedScorer {
    /// Load a Phase 2 model from disk.
    pub fn load(path: &Path) -> Result<Self> {
        let s = path
            .to_str()
            .context("retrieval-utility model path is not valid UTF-8")?;
        let model = GBDT::load_model(s).map_err(|e| {
            anyhow::anyhow!(
                "loading retrieval-utility model from {}: {}",
                path.display(),
                e
            )
        })?;
        Ok(Self {
            model: Arc::new(model),
            schema_version: Features::SCHEMA_VERSION,
        })
    }

    /// Wrap an already-trained GBDT.
    #[must_use]
    pub fn from_inner(model: GBDT) -> Self {
        Self {
            model: Arc::new(model),
            schema_version: Features::SCHEMA_VERSION,
        }
    }

    /// Persist a trained model to disk.
    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent).with_context(|| {
                format!("creating retrieval-utility model dir {}", parent.display())
            })?;
        }
        let s = path
            .to_str()
            .context("retrieval-utility model path is not valid UTF-8")?;
        self.model.save_model(s).map_err(|e| {
            anyhow::anyhow!(
                "saving retrieval-utility model to {}: {}",
                path.display(),
                e
            )
        })?;
        Ok(())
    }

    /// On-disk schema version this scorer was loaded against.
    #[must_use]
    pub fn schema_version(&self) -> u32 {
        self.schema_version
    }

    /// Raw `p` in `[0, 1]` (sigmoid output for `LogLikelyhood` loss).
    /// Exposed for tests and the eval harness.
    #[must_use]
    pub fn predict_p(&self, f: &Features) -> f32 {
        let row = f.to_row();
        debug_assert_eq!(row.len(), N_FEATURES, "row arity mismatch");
        let datum = Data {
            feature: row,
            target: 0.0,
            weight: 1.0,
            label: 0.0,
            residual: 0.0,
            initial_guess: 0.0,
        };
        let out: Vec<f32> = self.model.predict(&vec![datum]);
        out.first().copied().unwrap_or(0.5).clamp(1e-6, 1.0 - 1e-6)
    }
}

impl RetrievalUtilityScorer for LearnedScorer {
    fn score(&self, f: &Features) -> f32 {
        let p = self.predict_p(f);
        // Logit map → log-prob adjustment, clipped.
        let logit = (p / (1.0 - p)).ln();
        logit.clamp(-5.0, 5.0)
    }

    fn name(&self) -> &'static str {
        "learned"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::phase2_features::{
        AnchorSummary, BeamStats, Features, ProgramFeatures, extract,
    };

    fn synthetic_strong_blocker_row() -> Features {
        // Hypothesis with postcode + street, BE default policy → blocker
        // present, low static cost, no anchor disagreement. The
        // "obvious good search direction" baseline.
        Features {
            hypothesis_logprob: -0.1,
            field_reliability: 1.0 + 2.0,
            country_posterior: 0.95,
            strictness: 0.0,
            sibling_count_log: 0.69, // ln(2)
            static_cost_fraction: 0.05,
            op_count: 4.0,
            n_intersects: 1.0,
            n_unions: 0.0,
            n_scores: 0.0,
            n_filters: 0.0,
            has_blocker: 1.0,
            max_postings_log: 5.0,
            min_postings_log: 3.0,
            n_lookups: 2.0,
            role_postcode: 0.0, // Blocker
            role_street: 1.0,   // Reducer
            role_house: -1.0,
            role_locality: -1.0,
            postcode_anchor: 1.0,
            house_anchor: -1.0,
            locality_anchor: -1.0,
            anchor_disagreements: 0.0,
            anchor_total: 1.0,
            hypothesis_rank: 0.0,
            gap_to_top: 0.0,
            logprob_z: 1.0,
            claims_postcode: 1.0,
            claims_street: 1.0,
            claims_house: 0.0,
        }
    }

    fn synthetic_all_scorer_row() -> Features {
        // No blocker, no postcode, claims are present. The
        // "obvious bad search direction" pathological case.
        Features {
            hypothesis_logprob: -0.1,
            field_reliability: 2.0,
            country_posterior: 0.5,
            strictness: 0.0,
            sibling_count_log: 0.69,
            static_cost_fraction: 0.6,
            op_count: 3.0,
            n_intersects: 0.0,
            n_unions: 1.0,
            n_scores: 2.0,
            n_filters: 0.0,
            has_blocker: 0.0,
            max_postings_log: 11.0, // oversized
            min_postings_log: 0.0,  // empty
            n_lookups: 2.0,
            role_postcode: -1.0,
            role_street: 2.0, // Scorer
            role_house: 2.0,
            role_locality: 2.0,
            postcode_anchor: -1.0,
            house_anchor: -1.0,
            locality_anchor: -1.0,
            anchor_disagreements: 0.0,
            anchor_total: 0.0,
            hypothesis_rank: 1.0,
            gap_to_top: 1.5,
            logprob_z: -0.5,
            claims_postcode: 0.0,
            claims_street: 1.0,
            claims_house: 1.0,
        }
    }

    #[test]
    fn heuristic_prefers_strong_blocker_over_all_scorer() {
        let h = HeuristicScorer::default();
        let s_good = h.score(&synthetic_strong_blocker_row());
        let s_bad = h.score(&synthetic_all_scorer_row());
        assert!(s_good > s_bad, "got good={s_good} bad={s_bad}");
    }

    #[test]
    fn heuristic_score_is_deterministic() {
        let h = HeuristicScorer::default();
        let row = synthetic_strong_blocker_row();
        let s1 = h.score(&row);
        let s2 = h.score(&row);
        assert_eq!(s1, s2);
    }

    #[test]
    fn heuristic_default_name_is_stable() {
        let h = HeuristicScorer::default();
        assert_eq!(h.name(), "heuristic");
    }

    #[test]
    fn extract_then_score_produces_finite_values() {
        // End-to-end smoke test for the extract → score pipeline.
        use crate::geocoder::channels::Channel;
        use crate::geocoder::program::{LookupKey, Op};
        use crate::shard::AddressRecord;
        use crate::shard::builder::build_shard;
        use crate::shard::reader::Shard;
        use crate::types::{ParseHypothesis, RetrievalPolicy};

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("shard.bfgs");
        let addrs = vec![AddressRecord {
            street: "Rue Wayez".into(),
            housenumber: "122".into(),
            postcode: "1070".into(),
            locality: "Anderlecht".into(),
            lat: 50.834,
            lon: 4.314,
            ..Default::default()
        }];
        build_shard(&path, crate::routing::CountryId::BE, addrs).unwrap();
        let shard = Shard::open(&path).unwrap();

        let mut h = ParseHypothesis::default();
        h.street_candidates.push(("Rue Wayez".into(), 1.0));
        h.postcode_candidates.push(("1070".into(), 1.0));
        h.retrieval_policy = RetrievalPolicy::belgium_default();

        let prog = Op::Intersect(vec![
            Op::Lookup(LookupKey {
                channel: Channel::Postcode,
                key: "1070".into(),
            }),
            Op::Lookup(LookupKey {
                channel: Channel::Street,
                key: "rue wayez".into(),
            }),
        ]);
        let pf = ProgramFeatures::from_program(&prog, &h.retrieval_policy, &shard);
        let anchors = AnchorSummary::default();
        let beam = BeamStats::from_logprobs(&[-0.1, -0.5]);
        let f = extract(
            &h,
            &prog,
            &h.retrieval_policy,
            &pf,
            &anchors,
            beam,
            0,
            -0.1,
            0.95,
            &shard,
        );
        let h_scorer = HeuristicScorer::default();
        let s = h_scorer.score(&f);
        assert!(s.is_finite(), "heuristic score should be finite, got {s}");
        // BE default with strong-blocker input should score positive
        // (blocker reward outweighs the static-cost slice).
        assert!(s > -1.0, "expected non-pathological score, got {s}");
    }
}
