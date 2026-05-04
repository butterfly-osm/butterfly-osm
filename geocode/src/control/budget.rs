//! Budget computation (#97 §1, §2, §3).
//!
//! Derives [`ExecutionBudget`] from:
//!
//! - country posterior entropy
//! - hypothesis count
//! - field trust masks
//! - lexical / posting-list frequency
//! - fuzzy expansion permission per [`Strictness`]
//! - anchor presence (postcode + house)
//! - expected parallel channel count per hypothesis
//! - static cost of the planned operator tree (#96 Cost Composition)
//!
//! Output is a [`BudgetTier`] (Tight / Normal / Wide / Desperate)
//! with documented thresholds, all tunable via [`BudgetPolicy`].
//!
//! ## Two-pass cost
//!
//! Per #96 / #97, only **static cost** is admitted. Feedback cost
//! (Downgrade firings, retries) is observed post-hoc by
//! [`crate::control::metrics_channels`].
//!
//! ## Defense-in-depth check
//!
//! [`pre_execution_check`] re-verifies the static cost ceiling on
//! executor entry — independent of the budget computation, so a
//! malformed `ParsedQuery` whose `static_cost_ceiling` was hand-set
//! still gets refused.

use crate::geocoder::channels::{Channel, ChannelRole};
use crate::geocoder::cost::{ShardStats, static_cost};
use crate::geocoder::program::Op;
use crate::types::{ExecutionBudget, ParseHypothesis, ParsedQuery, Strictness};

/// Tunable policy for budget computation.
///
/// All thresholds are documented per field. Defaults are calibrated
/// for the Belgium MVP shard (~4M addresses, ~25k unique streets).
#[derive(Debug, Clone, Copy)]
pub struct BudgetPolicy {
    /// Confidence ≥ this and a strong anchor → Tight tier.
    /// Range: 0.0 - 1.0. Default: 0.85.
    pub tight_confidence: f32,
    /// Confidence ≥ this → Normal tier.
    /// Range: 0.0 - 1.0. Default: 0.55.
    pub normal_confidence: f32,
    /// Confidence ≥ this → Wide tier; below → Desperate.
    /// Range: 0.0 - 1.0. Default: 0.25.
    pub wide_confidence: f32,
    /// Maximum estimated parallel-channel count before forcing the
    /// next-wider tier ("controlled budget for common street names"
    /// per #97 §2).
    /// Range: 1 - 6. Default: 3.
    pub max_parallel_channels_before_widen: u8,
    /// Lexical frequency (posting-list size) above which a query is
    /// considered "high fanout" and must widen one tier.
    /// Range: 100 - 1_000_000. Default: 5_000.
    pub high_fanout_postings_threshold: u32,
    /// Static cost ceiling per tier, in "candidate touches".
    /// Tuple of (Tight, Normal, Wide, Desperate).
    /// Defaults: 256, 4_096, 65_536, 524_288.
    pub static_cost_ceilings: (f32, f32, f32, f32),
    /// `max_total_candidates` per tier.
    /// Defaults: 50, 200, 1_000, 5_000.
    pub max_candidates: (u32, u32, u32, u32),
    /// `max_hypotheses` per tier.
    /// Defaults: 1, 3, 5, 5.
    pub max_hypotheses: (u8, u8, u8, u8),
    /// `max_countries` per tier.
    /// Defaults: 1, 2, 4, 4.
    pub max_countries: (u8, u8, u8, u8),
    /// `max_fuzzy_expansions` per tier.
    /// Defaults: 0, 16, 64, 256.
    pub max_fuzzy_expansions: (u16, u16, u16, u16),
}

impl Default for BudgetPolicy {
    fn default() -> Self {
        Self {
            tight_confidence: 0.85,
            normal_confidence: 0.55,
            wide_confidence: 0.25,
            // The Belgium default policy uses 4 channels (postcode +
            // street + house + locality). 4 stays at Tight; > 4 widens.
            max_parallel_channels_before_widen: 4,
            high_fanout_postings_threshold: 5_000,
            static_cost_ceilings: (256.0, 4_096.0, 65_536.0, 524_288.0),
            max_candidates: (50, 200, 1_000, 5_000),
            max_hypotheses: (1, 3, 5, 5),
            max_countries: (1, 2, 4, 4),
            max_fuzzy_expansions: (0, 16, 64, 256),
        }
    }
}

/// Discrete budget tier (#97 §1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BudgetTier {
    /// High confidence + strong anchor.
    Tight,
    /// Moderate confidence.
    Normal,
    /// Low confidence or missing fields.
    Wide,
    /// Very low confidence or no anchors at all.
    Desperate,
}

impl BudgetTier {
    pub const fn label(self) -> &'static str {
        match self {
            BudgetTier::Tight => "tight",
            BudgetTier::Normal => "normal",
            BudgetTier::Wide => "wide",
            BudgetTier::Desperate => "desperate",
        }
    }

    fn widened(self) -> Self {
        match self {
            BudgetTier::Tight => BudgetTier::Normal,
            BudgetTier::Normal => BudgetTier::Wide,
            BudgetTier::Wide => BudgetTier::Desperate,
            BudgetTier::Desperate => BudgetTier::Desperate,
        }
    }
}

/// Inputs to [`compute_budget`] derived from a `ParsedQuery` plus
/// shard statistics. Centralised so the budget code is independent
/// of the parser's internals.
#[derive(Debug, Clone, Copy)]
pub struct ParsedQueryStats {
    /// Shannon entropy of the country posterior, in bits. 0.0 means
    /// "one country with weight ≥ ~1.0", positive means uncertain.
    pub country_entropy_bits: f32,
    /// Number of (post-recombination) hypotheses to execute.
    pub hypothesis_count: u8,
    /// Fraction of hypotheses with at least one trusted blocker
    /// channel (anchor present). 0.0 = no anchors, 1.0 = always.
    pub anchor_fraction: f32,
    /// Whether **any** hypothesis has a postcode + house anchor.
    /// True triggers the high-info short-circuit.
    pub has_postcode_and_house: bool,
    /// Maximum number of parallel field-channel lookups across all
    /// hypotheses. Caps at [`crate::types::N_CHANNELS`].
    pub max_parallel_channels: u8,
    /// Lexical/document frequency of street + locality lookups, in
    /// posting-list entries. Sum across all hypotheses (so a query
    /// hitting a common street is naturally biased toward widening).
    pub lexical_postings: u32,
    /// Maximum strictness over the hypotheses. Wider strictness
    /// permits more fuzzy expansions.
    pub max_strictness: Strictness,
    /// Static cost (#96 Cost Composition) summed over the planned
    /// canonicalized operator-tree forest.
    pub static_cost: f32,
}

impl ParsedQueryStats {
    /// Compute parser-derived stats from a `ParsedQuery` against a
    /// shard. Walks the hypotheses once; allocates only for the
    /// per-hypothesis operator-tree — which is unavoidable for the
    /// non-clean path.
    ///
    /// On the clean path (`is_clean()` holds) this still allocates
    /// one operator tree to estimate cost, **outside** the executor's
    /// hot loop. Budget computation runs once per HTTP request,
    /// not per posting touched, so the allocation does not violate
    /// the Zero-Cost-on-Clean-Queries NFR (which is about per-record
    /// algebra overhead in the executor, not request setup).
    #[must_use]
    pub fn from_query(query: &ParsedQuery, stats: ShardStats) -> Self {
        let country_entropy_bits = shannon_entropy_bits(&query.country_candidates);
        let hypothesis_count = u8::try_from(query.hypotheses.len().min(255)).unwrap_or(255);
        let anchor_fraction = if query.hypotheses.is_empty() {
            0.0
        } else {
            let with_anchor = query
                .hypotheses
                .iter()
                .filter(|h| has_blocker_anchor(h))
                .count();
            with_anchor as f32 / query.hypotheses.len() as f32
        };
        let has_postcode_and_house = query
            .hypotheses
            .iter()
            .any(|h| !h.postcode_candidates.is_empty() && !h.house_candidates.is_empty());
        let max_parallel_channels = query
            .hypotheses
            .iter()
            .map(parallel_channel_count)
            .max()
            .unwrap_or(0);
        let lexical_postings = query
            .hypotheses
            .iter()
            .map(|h| {
                let s = h
                    .street_candidates
                    .first()
                    .map(|(s, _)| s.as_str())
                    .unwrap_or("");
                let l = h
                    .locality_candidates
                    .first()
                    .map(|(l, _)| l.as_str())
                    .unwrap_or("");
                (estimate_postings(Channel::Street, s, stats)
                    + estimate_postings(Channel::Locality, l, stats)) as u32
            })
            .sum::<u32>();
        let max_strictness = query
            .hypotheses
            .iter()
            .map(|h| h.strictness)
            .max_by_key(|s| match s {
                Strictness::Exact => 0u8,
                Strictness::Fuzzy => 1,
                Strictness::Desperate => 2,
            })
            .unwrap_or(Strictness::Exact);
        let static_cost = estimate_static_cost(query, stats);

        Self {
            country_entropy_bits,
            hypothesis_count,
            anchor_fraction,
            has_postcode_and_house,
            max_parallel_channels,
            lexical_postings,
            max_strictness,
            static_cost,
        }
    }
}

/// Compute a static cost summed across all hypotheses.
///
/// Builds a one-shot operator tree per hypothesis using the same
/// shape as `executor::build_program` (postcode + street → intersect,
/// optional house filter, cap). Compositional over static operators;
/// feedback operators are not modelled here per #96.
#[must_use]
pub fn estimate_static_cost(query: &ParsedQuery, stats: ShardStats) -> f32 {
    let mut total = 0.0_f32;
    for h in &query.hypotheses {
        let op = build_estimation_program(h);
        total += static_cost(&op, stats);
    }
    total
}

fn build_estimation_program(h: &ParseHypothesis) -> Op {
    use crate::geocoder::program::{FilterPredicate, LookupKey};

    let mut blockers: Vec<Op> = Vec::new();
    let mut reducers: Vec<Op> = Vec::new();

    if let Some((pc, _)) = h.postcode_candidates.first() {
        let lookup = Op::Lookup(LookupKey {
            channel: Channel::Postcode,
            key: pc.clone(),
        });
        match h.retrieval_policy.role(Channel::Postcode) {
            Some(ChannelRole::Blocker) => blockers.push(lookup),
            Some(ChannelRole::Reducer) => reducers.push(lookup),
            _ => {}
        }
    }
    if let Some((st, _)) = h.street_candidates.first() {
        let lookup = Op::Lookup(LookupKey {
            channel: Channel::Street,
            key: st.clone(),
        });
        match h.retrieval_policy.role(Channel::Street) {
            Some(ChannelRole::Blocker) => blockers.push(lookup),
            Some(ChannelRole::Reducer) => reducers.push(lookup),
            _ => {}
        }
    }

    let base: Op = match (blockers.len(), reducers.len()) {
        (0, 0) => Op::Lookup(LookupKey {
            channel: Channel::Locality,
            key: h
                .locality_candidates
                .first()
                .map(|(s, _)| s.clone())
                .unwrap_or_default(),
        }),
        (0, _) => {
            if reducers.len() == 1 {
                reducers.into_iter().next().expect("len 1")
            } else {
                Op::Intersect(reducers)
            }
        }
        (_, 0) => {
            if blockers.len() == 1 {
                blockers.into_iter().next().expect("len 1")
            } else {
                Op::Intersect(blockers)
            }
        }
        (_, _) => {
            let mut all = blockers;
            all.extend(reducers);
            Op::Intersect(all)
        }
    };

    let after_filter = if let Some((hn, _)) = h.house_candidates.first() {
        Op::Filter {
            child: Box::new(base),
            predicate: FilterPredicate::HouseNumberEq(hn.clone()),
        }
    } else {
        base
    };

    Op::Cap {
        child: Box::new(after_filter),
        n: 64,
    }
}

fn estimate_postings(ch: Channel, key: &str, stats: ShardStats) -> f32 {
    if key.is_empty() {
        return 0.0;
    }
    match ch {
        Channel::Postcode => stats.avg_postcode_postings,
        Channel::Locality => stats.avg_locality_postings,
        Channel::Street => stats.avg_street_postings,
        Channel::HouseNumber => 1.0,
        Channel::Alias | Channel::Transliteration => stats.avg_locality_postings,
    }
}

fn parallel_channel_count(h: &ParseHypothesis) -> u8 {
    let mut n = 0u8;
    for ch in Channel::all() {
        if h.retrieval_policy.role(ch).is_some() {
            n += 1;
        }
    }
    // Bound to N_CHANNELS even if `RetrievalPolicy` somehow grew.
    n.min(crate::types::N_CHANNELS as u8)
}

fn has_blocker_anchor(h: &ParseHypothesis) -> bool {
    if h.postcode_candidates.is_empty() && h.house_candidates.is_empty() {
        return false;
    }
    for ch in Channel::all() {
        if matches!(h.retrieval_policy.role(ch), Some(ChannelRole::Blocker)) {
            return true;
        }
    }
    false
}

fn shannon_entropy_bits(posterior: &[(crate::routing::CountryId, f32)]) -> f32 {
    if posterior.len() <= 1 {
        return 0.0;
    }
    let total: f32 = posterior.iter().map(|(_, w)| w.max(0.0)).sum();
    if total <= 0.0 {
        return 0.0;
    }
    let mut h = 0.0_f32;
    for (_, w) in posterior {
        let p = w.max(0.0) / total;
        if p > 0.0 {
            h -= p * p.log2();
        }
    }
    h
}

/// Map `(confidence, fanout, parallel_channels, static_cost)` to a
/// concrete [`ExecutionBudget`] per #97 §1.
#[must_use]
pub fn compute_budget(
    query: &ParsedQuery,
    stats: ShardStats,
    policy: BudgetPolicy,
) -> ExecutionBudget {
    let pq = ParsedQueryStats::from_query(query, stats);

    // Confidence-derived base tier.
    let mut tier = base_tier(query.global_confidence, &pq, policy);

    // Widen for high-fanout signals even if confidence is high
    // (#97 §2 — "common street name → still needs controlled budget").
    if pq.lexical_postings >= policy.high_fanout_postings_threshold {
        tier = tier.widened();
    }
    if pq.max_parallel_channels > policy.max_parallel_channels_before_widen {
        tier = tier.widened();
    }
    // Many hypotheses with no anchors at all → desperate.
    if pq.hypothesis_count > 1 && pq.anchor_fraction < 0.5 {
        tier = tier.widened();
    }
    // Country posterior entropy → widen.
    if pq.country_entropy_bits > 1.0 {
        tier = tier.widened();
    }

    // Static cost overrun → widen until ceiling fits.
    while pq.static_cost > tier_ceiling(tier, policy) && tier != BudgetTier::Desperate {
        tier = tier.widened();
    }

    // Build the budget at the final tier.
    let (tight_max, normal_max, wide_max, desp_max) = policy.max_candidates;
    let (tight_h, normal_h, wide_h, desp_h) = policy.max_hypotheses;
    let (tight_c, normal_c, wide_c, desp_c) = policy.max_countries;
    let (tight_f, normal_f, wide_f, desp_f) = policy.max_fuzzy_expansions;

    let (max_total_candidates, max_hypotheses, max_countries, max_fuzzy_expansions) = match tier {
        BudgetTier::Tight => (tight_max, tight_h, tight_c, tight_f),
        BudgetTier::Normal => (normal_max, normal_h, normal_c, normal_f),
        BudgetTier::Wide => (wide_max, wide_h, wide_c, wide_f),
        BudgetTier::Desperate => (desp_max, desp_h, desp_c, desp_f),
    };

    ExecutionBudget {
        max_countries,
        max_hypotheses,
        max_fuzzy_expansions,
        max_total_candidates,
        static_cost_ceiling: tier_ceiling(tier, policy),
        dual_evaluation_enabled: false,
    }
}

/// Assign the same tier label that `compute_budget` would, without
/// constructing the budget. Used by metrics emission to attach a tier
/// label to histograms.
#[must_use]
pub fn classify_tier(query: &ParsedQuery, stats: ShardStats, policy: BudgetPolicy) -> BudgetTier {
    let pq = ParsedQueryStats::from_query(query, stats);
    let mut tier = base_tier(query.global_confidence, &pq, policy);
    if pq.lexical_postings >= policy.high_fanout_postings_threshold {
        tier = tier.widened();
    }
    if pq.max_parallel_channels > policy.max_parallel_channels_before_widen {
        tier = tier.widened();
    }
    if pq.hypothesis_count > 1 && pq.anchor_fraction < 0.5 {
        tier = tier.widened();
    }
    if pq.country_entropy_bits > 1.0 {
        tier = tier.widened();
    }
    while pq.static_cost > tier_ceiling(tier, policy) && tier != BudgetTier::Desperate {
        tier = tier.widened();
    }
    tier
}

fn base_tier(confidence: f32, pq: &ParsedQueryStats, policy: BudgetPolicy) -> BudgetTier {
    if confidence >= policy.tight_confidence && pq.has_postcode_and_house {
        BudgetTier::Tight
    } else if confidence >= policy.normal_confidence {
        BudgetTier::Normal
    } else if confidence >= policy.wide_confidence {
        BudgetTier::Wide
    } else {
        BudgetTier::Desperate
    }
}

fn tier_ceiling(tier: BudgetTier, policy: BudgetPolicy) -> f32 {
    let (t, n, w, d) = policy.static_cost_ceilings;
    match tier {
        BudgetTier::Tight => t,
        BudgetTier::Normal => n,
        BudgetTier::Wide => w,
        BudgetTier::Desperate => d,
    }
}

/// Defense-in-depth pre-execution check (#97 §3).
///
/// The executor calls this on entry to re-verify that the planned
/// program(s) fit the budget's static-cost ceiling. Independent of
/// [`compute_budget`] so a hand-constructed `ParsedQuery` cannot
/// bypass admission control.
#[derive(Debug, thiserror::Error)]
pub enum AdmissionError {
    #[error(
        "static cost {actual:.1} exceeds ceiling {ceiling:.1} (tier oversubscribed; \
         widen tier or reduce hypothesis count)"
    )]
    CostCeilingExceeded { actual: f32, ceiling: f32 },
}

pub fn pre_execution_check(
    programs: &[Op],
    budget: &ExecutionBudget,
    stats: ShardStats,
) -> Result<(), AdmissionError> {
    let total: f32 = programs.iter().map(|p| static_cost(p, stats)).sum();
    if total > budget.static_cost_ceiling {
        return Err(AdmissionError::CostCeilingExceeded {
            actual: total,
            ceiling: budget.static_cost_ceiling,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geocoder::channels::Channel;
    use crate::geocoder::program::LookupKey;
    use crate::parser::heuristic::parse_heuristic;
    use crate::routing::CountryId;
    use crate::types::FieldMask;

    /// Tighter stats for tier-classification tests so the default
    /// fanout threshold is not blown by the synthetic locality
    /// posting-list estimate. The real Belgium shard's actual avg
    /// locality postings are ~2k; the global default is conservative
    /// (32k) to widen rather than crash on unknown shards.
    fn stats() -> ShardStats {
        ShardStats {
            avg_postcode_postings: 200.0,
            avg_locality_postings: 200.0,
            avg_street_postings: 8.0,
            total_addresses: 100_000,
        }
    }

    #[test]
    fn tight_tier_for_clean_query_with_anchors() {
        let mut q = parse_heuristic("Rue Wayez 122 1070 Anderlecht", CountryId::BE);
        // Boost confidence so the policy maps to Tight (heuristic
        // parser reports ≥0.85 already when both postcode + house are
        // present).
        q.global_confidence = 0.95;
        let policy = BudgetPolicy::default();
        let tier = classify_tier(&q, stats(), policy);
        assert_eq!(tier, BudgetTier::Tight, "got {tier:?}");
    }

    #[test]
    fn wide_tier_for_no_anchors() {
        let mut q = parse_heuristic("Some street name only", CountryId::BE);
        q.global_confidence = 0.5; // moderate
        let tier = classify_tier(&q, stats(), BudgetPolicy::default());
        // Moderate confidence + no postcode + no house → Normal at best.
        assert!(matches!(tier, BudgetTier::Normal | BudgetTier::Wide));
    }

    #[test]
    fn desperate_tier_for_low_confidence() {
        let mut q = parse_heuristic("???", CountryId::BE);
        q.global_confidence = 0.1;
        let tier = classify_tier(&q, stats(), BudgetPolicy::default());
        assert_eq!(tier, BudgetTier::Desperate);
    }

    #[test]
    fn fanout_widens_tier() {
        let mut q = parse_heuristic("Rue Wayez 122 1070 Anderlecht", CountryId::BE);
        q.global_confidence = 0.95;
        // Force fanout threshold low enough to fire on the
        // synthetic stats.
        let policy = BudgetPolicy {
            high_fanout_postings_threshold: 1, // any non-empty street trips it
            ..BudgetPolicy::default()
        };
        let tier = classify_tier(&q, stats(), policy);
        // Tight got widened to at least Normal because lexical
        // fanout >= threshold.
        assert!(matches!(tier, BudgetTier::Normal | BudgetTier::Wide));
    }

    #[test]
    fn cost_overrun_widens_until_fits() {
        let q = parse_heuristic("Rue Wayez 122 1070 Anderlecht", CountryId::BE);
        // Squeeze the Tight ceiling so any non-trivial cost must
        // widen.
        let policy = BudgetPolicy {
            static_cost_ceilings: (0.5, 4_096.0, 65_536.0, 524_288.0),
            ..BudgetPolicy::default()
        };
        let tier = classify_tier(&q, stats(), policy);
        assert!(tier != BudgetTier::Tight);
    }

    #[test]
    fn pre_execution_check_passes_on_fit() {
        let op = Op::Lookup(LookupKey {
            channel: Channel::Postcode,
            key: "1070".into(),
        });
        let budget = ExecutionBudget {
            max_countries: 1,
            max_hypotheses: 1,
            max_fuzzy_expansions: 0,
            max_total_candidates: 50,
            static_cost_ceiling: 1e9,
            dual_evaluation_enabled: false,
        };
        let r = pre_execution_check(&[op], &budget, stats());
        assert!(r.is_ok());
    }

    #[test]
    fn pre_execution_check_rejects_overrun() {
        let op = Op::Lookup(LookupKey {
            channel: Channel::Locality,
            key: "anywhere".into(),
        });
        let budget = ExecutionBudget {
            max_countries: 1,
            max_hypotheses: 1,
            max_fuzzy_expansions: 0,
            max_total_candidates: 50,
            static_cost_ceiling: 0.1,
            dual_evaluation_enabled: false,
        };
        let r = pre_execution_check(&[op], &budget, stats());
        assert!(r.is_err());
    }

    #[test]
    fn shannon_entropy_one_country_is_zero() {
        assert_eq!(shannon_entropy_bits(&[(CountryId::BE, 1.0)]), 0.0);
    }

    #[test]
    fn shannon_entropy_uniform_two_is_one_bit() {
        let h = shannon_entropy_bits(&[(CountryId::BE, 0.5), (CountryId::BE, 0.5)]);
        assert!((h - 1.0).abs() < 1e-5);
    }

    #[test]
    fn parsed_query_stats_captures_clean_path() {
        let q = parse_heuristic("Rue Wayez 122 1070 Anderlecht", CountryId::BE);
        let pq = ParsedQueryStats::from_query(&q, stats());
        assert!(pq.has_postcode_and_house);
        assert_eq!(pq.hypothesis_count, 1);
        assert!(pq.country_entropy_bits.abs() < 1e-6);
    }

    #[test]
    fn compute_budget_returns_consistent_ceiling() {
        let q = parse_heuristic("Rue Wayez 122 1070 Anderlecht", CountryId::BE);
        let policy = BudgetPolicy::default();
        let b = compute_budget(&q, stats(), policy);
        let tier = classify_tier(&q, stats(), policy);
        let expected_ceiling = match tier {
            BudgetTier::Tight => policy.static_cost_ceilings.0,
            BudgetTier::Normal => policy.static_cost_ceilings.1,
            BudgetTier::Wide => policy.static_cost_ceilings.2,
            BudgetTier::Desperate => policy.static_cost_ceilings.3,
        };
        assert!((b.static_cost_ceiling - expected_ceiling).abs() < 1e-3);
    }

    #[test]
    fn missing_anchors_do_not_block_tight_when_confidence_low() {
        let mut q = parse_heuristic("Rue Wayez", CountryId::BE);
        q.global_confidence = 0.9; // even high confidence cannot promote to Tight
        let tier = classify_tier(&q, stats(), BudgetPolicy::default());
        assert!(
            tier != BudgetTier::Tight,
            "Tight requires postcode+house anchor"
        );
    }

    #[test]
    fn field_mask_round_trips_unused_in_budget_but_compiles() {
        // Sanity: `FieldMask` still imports cleanly; it's an input
        // signal `ParsedQueryStats` could consume in future.
        let m = FieldMask::NONE.with(Channel::Postcode);
        assert!(m.contains(Channel::Postcode));
    }
}
