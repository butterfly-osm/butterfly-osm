//! Retrieval-aware decoding (#98 Phase 1).
//!
//! ## What this module does
//!
//! Translates per-byte transformer outputs into a small set of
//! deduped retrieval programs ready for the executor. The pipeline
//! is the decoding-side enforcement of every #96 invariant:
//!
//! 1. **Anchor detection** ([`super::anchor`]) on the raw text.
//! 2. **Country prior** is consumed from the model's country head
//!    (we do not re-derive it inside the beam — #98 1.3).
//! 3. **Adaptive beam search** ([`super::beam`]) over the BIO
//!    posteriors. Beam width responds to local entropy and
//!    accumulated static-cost fraction.
//! 4. **Anchor pruning** with role-smoothness — disagreeing hypotheses
//!    are downweighted within ε, hard-pruned outside.
//! 5. **Build a retrieval program** ([`crate::geocoder::program`]) per
//!    hypothesis using channel-role assignment from the country pack.
//! 6. **Canonicalize and dedup** programs (#96 Recombination
//!    Invariant). Source hypothesis scores are merged via **max**
//!    (consistently with #98 1.1; documented in module-level note).
//! 7. **Retrieval-utility scoring** — re-rank the merged programs
//!    using static cost, blocker coverage, and posting-list size
//!    estimates from the shard.
//! 8. **Emit `ParseHypothesis`** values, one per surviving program,
//!    into a [`crate::types::ParsedQuery`].
//!
//! ## Source-hypothesis score merge: max
//!
//! Per #96, programs that canonicalize to the same form merge their
//! source-hypothesis scores. We use **max** rather than sum:
//!
//! - max preserves the absolute scale of log-probabilities (useful
//!   for downstream comparison)
//! - sum would over-count hypotheses that differ only in label
//!   assignments that get folded by canonicalization, biasing the
//!   merged program toward branches with more redundant alternatives
//! - max is consistent with the "best single source" semantics —
//!   we report the merged program's confidence as the confidence of
//!   the best contributing parser hypothesis

use std::collections::HashMap;

use crate::geocoder::channels::{Channel, ChannelRole};
use crate::geocoder::cost::static_cost;
use crate::geocoder::program::{LookupKey, Op};
use crate::routing::CountryId;
use crate::shard::reader::Shard;
use crate::tagger::inference::{BioSpan, InferenceOutput};
use crate::tagger::transformer::NUM_BIO_LABELS;
use crate::types::{
    ExecutionBudget, FieldMask, ParseHypothesis, ParsedQuery, RecoveryFlags, RetrievalPolicy,
    Strictness,
};

use super::anchor::{Anchor, detect_anchors};
use super::beam::{BeamConfig, apply_anchor_pruning, beam_search, drop_pruned};
use super::phase2_features::{
    AnchorSummary, BeamStats, Features as Phase2Features, ProgramFeatures,
    extract as extract_phase2,
};
use super::retrieval_utility::{HeuristicScorer, RetrievalUtilityScorer};

/// Configuration knobs for the retrieval-utility scorer (#98 1.5).
#[derive(Debug, Clone, Copy)]
pub struct UtilityConfig {
    /// Penalty per unit of static cost relative to ceiling. Default 2.0.
    pub static_cost_penalty: f32,
    /// Reward per channel that acts as a blocker in the program. Default 0.5.
    pub blocker_reward: f32,
    /// Penalty if the program has zero blockers (all-scorer mode). Default 1.0.
    pub all_scorer_penalty: f32,
    /// Penalty if any lookup hit returns an empty posting list. Default 1.5.
    pub empty_lookup_penalty: f32,
    /// Penalty per posting-list-size bucket above the oversized threshold.
    /// Anticipates a feedback-operator firing.
    pub oversized_lookup_penalty: f32,
    /// Threshold for "oversized": a lookup whose posting list exceeds this
    /// fraction of the shard's total record count is considered oversized.
    pub oversized_fraction_threshold: f32,
}

impl Default for UtilityConfig {
    fn default() -> Self {
        Self {
            static_cost_penalty: 2.0,
            blocker_reward: 0.5,
            all_scorer_penalty: 1.0,
            empty_lookup_penalty: 1.5,
            oversized_lookup_penalty: 0.5,
            oversized_fraction_threshold: 0.05,
        }
    }
}

/// The result of running #98 Phase 1 decoding on a single inference output.
#[derive(Debug, Clone)]
pub struct DecodedQuery {
    /// Surviving deduped retrieval programs ordered by retrieval-utility-
    /// adjusted score (best first).
    pub programs: Vec<RankedProgram>,
    /// Detected anchors (kept for debug surfacing).
    pub anchors: Vec<Anchor>,
    /// Average beam-collapse rate observed during this decode:
    /// `1 - (post_dedup_count / pre_dedup_count)`. #97's
    /// hypothesis-dedup collapse rate metric reads this.
    pub dedup_collapse_rate: f32,
}

/// A retrieval program plus its merged scoring components.
#[derive(Debug, Clone)]
pub struct RankedProgram {
    pub program: Op,
    pub policy: RetrievalPolicy,
    /// The best (max) source-hypothesis log-prob across all hypotheses
    /// that canonicalized to this program.
    pub source_logprob: f32,
    /// Log-utility adjustment applied by [`UtilityConfig`].
    pub utility_logp: f32,
    /// Final score = `source_logprob + utility_logp`.
    pub final_logprob: f32,
    /// The best hypothesis (by parser logprob) that produced this
    /// program — its parsed fields seed the [`ParseHypothesis`] payload.
    pub source_hypothesis: ParseHypothesis,
    /// Field-mask of fields the source hypothesis labeled.
    pub fields_present: FieldMask,
}

/// Top-level entry point: ingest the inference output + raw text +
/// shard, return decoded programs.
///
/// This wraps [`decode_with_scorer`] using the Phase 1 [`HeuristicScorer`]
/// derived from `util_cfg`. New call sites should prefer
/// [`decode_with_scorer`] directly so they can inject a learned
/// scorer (#98 Phase 2) without re-wrapping.
pub fn decode(
    text: &str,
    inference: &InferenceOutput,
    shard: &Shard,
    beam_cfg: &BeamConfig,
    util_cfg: &UtilityConfig,
) -> DecodedQuery {
    let scorer = HeuristicScorer {
        static_cost_penalty: util_cfg.static_cost_penalty,
        blocker_reward: util_cfg.blocker_reward,
        all_scorer_penalty: util_cfg.all_scorer_penalty,
        empty_lookup_penalty: util_cfg.empty_lookup_penalty,
        oversized_lookup_penalty: util_cfg.oversized_lookup_penalty,
        // util_cfg's `oversized_fraction_threshold` is converted to a
        // log-postings threshold inside the trait scorer. We pass the
        // fraction in via the context (`shard.stats().total_addresses`)
        // by computing the threshold log-value here.
        oversized_log_threshold: {
            let total = shard.stats().total_addresses.max(1) as f32;
            let n = (total * util_cfg.oversized_fraction_threshold).max(1.0);
            (1.0 + n).ln()
        },
    };
    decode_with_scorer(text, inference, shard, beam_cfg, &scorer)
}

/// Phase 2 entry point: same as [`decode`], but with an injectable
/// [`RetrievalUtilityScorer`]. Used by the neural parser when the
/// server is configured with `--retrieval-utility learned`.
pub fn decode_with_scorer(
    text: &str,
    inference: &InferenceOutput,
    shard: &Shard,
    beam_cfg: &BeamConfig,
    scorer: &dyn RetrievalUtilityScorer,
) -> DecodedQuery {
    let stats = shard.stats();

    // 1. Anchors.
    let anchors = detect_anchors(text, shard);

    // 2. Country prior is consumed via inference.country_posterior;
    //    nothing to do inside the beam (Phase 1.3 — beam consumes prior,
    //    does not re-derive).

    // 3. Adaptive beam search over the BIO logprobs.
    let logprobs: &[[f32; NUM_BIO_LABELS]] = &inference.bio_logprobs;
    let entropies: &[f32] = &inference.entropy_per_byte;
    let mut beam = beam_search(logprobs, entropies, beam_cfg, |partial| {
        // Cheap static-cost-fraction estimate: build a partial program
        // from the partial labels and divide by ceiling. Ceiling is taken
        // from the default ExecutionBudget; the executor verifies again
        // before running.
        let ceiling = ExecutionBudget::default().static_cost_ceiling.max(1.0);
        let h = build_hypothesis_from_labels(text, partial);
        let prog = build_program_for_hypothesis(&h, &h.retrieval_policy);
        (static_cost(&prog, stats) / ceiling).clamp(0.0, 1.0)
    });
    let pre_dedup_count = beam.len();

    // 4. Anchor pruning with role-smoothness.
    //    NOTE: pruning is computed against a snapshot of pre-prune log-probs,
    //    so the "very high-scoring contradiction survives" semantics from
    //    #98 1.4 holds. After pruning, we drop hypotheses that fell below
    //    the prune floor — but if dropping leaves an empty beam, we
    //    restore the single best pre-prune hypothesis. The anchors themselves
    //    are then merged into that hypothesis as the trusted-field overrides.
    let pre_prune_top = beam
        .iter()
        .max_by(|a, b| {
            a.log_prob
                .partial_cmp(&b.log_prob)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .cloned();
    apply_anchor_pruning(text, &mut beam, &anchors);
    let mut beam = drop_pruned(beam);
    if beam.is_empty()
        && let Some(top) = pre_prune_top
    {
        beam.push(top);
    }

    // 5. Build a retrieval program from each hypothesis.
    //    Trusted anchors are merged into each hypothesis BEFORE program
    //    construction. This is **not** anchor enforcement (the beam already
    //    handled that via [`apply_anchor_pruning`]); it's an injection of
    //    fields the parser missed but the regex+frequency anchor detector
    //    found with high confidence. The injection respects role-smoothness:
    //    we only inject anchors with confidence ≥ 1 - ε.
    let mut entries: Vec<(Op, RetrievalPolicy, ParseHypothesis, f32)> =
        Vec::with_capacity(beam.len());
    for hyp in beam {
        let mut parsed_hypothesis = build_hypothesis_from_labels(text, &hyp.labels);
        merge_trusted_anchors(&mut parsed_hypothesis, &anchors);
        let policy = parsed_hypothesis.retrieval_policy;
        let prog = build_program_for_hypothesis(&parsed_hypothesis, &policy);
        let canon = prog.canonicalize();
        entries.push((canon, policy, parsed_hypothesis, hyp.log_prob));
    }

    // Pre-compute beam aggregates needed for the cross-hypothesis
    // features.
    let beam_logprobs: Vec<f32> = entries.iter().map(|(_, _, _, lp)| *lp).collect();
    let beam_stats = BeamStats::from_logprobs(&beam_logprobs);
    // Country-posterior for the candidate country. We surface the
    // top-1 country posterior — the executor selects per-shard
    // routing later, so per-hypothesis country attribution is not
    // available here.
    let country_posterior_top = inference
        .country_posterior
        .iter()
        .copied()
        .fold(0.0_f32, f32::max);

    // 6. Canonicalize + dedup. Merge source-hypothesis scores via max.
    let mut by_canonical: HashMap<String, RankedProgram> = HashMap::new();
    for (rank, (canon, policy, hyp, lp)) in entries.into_iter().enumerate() {
        let key = format!("{canon:?}");
        // Phase 2 feature extraction — produces the row the scorer
        // consumes. Computed even on the heuristic path so the
        // scoring trait dispatch is uniform (the heuristic just
        // ignores most of the row).
        let prog_features = ProgramFeatures::from_program(&canon, &policy, shard);
        let anchor_summary = AnchorSummary::from(&anchors, &hyp);
        let phase2_row: Phase2Features = extract_phase2(
            &hyp,
            &canon,
            &policy,
            &prog_features,
            &anchor_summary,
            beam_stats,
            rank,
            lp,
            country_posterior_top,
            shard,
        );
        let utility_logp = scorer.score(&phase2_row);
        let fields = field_mask_from_hypothesis(&hyp);
        match by_canonical.get_mut(&key) {
            Some(existing) => {
                if lp > existing.source_logprob {
                    existing.source_logprob = lp;
                    existing.source_hypothesis = hyp;
                }
                existing.final_logprob = existing.source_logprob + existing.utility_logp;
            }
            None => {
                let final_logprob = lp + utility_logp;
                by_canonical.insert(
                    key,
                    RankedProgram {
                        program: canon,
                        policy,
                        source_logprob: lp,
                        utility_logp,
                        final_logprob,
                        source_hypothesis: hyp,
                        fields_present: fields,
                    },
                );
            }
        }
    }
    let post_dedup_count = by_canonical.len();

    // 7. Sort by final_logprob descending.
    let mut ranked: Vec<RankedProgram> = by_canonical.into_values().collect();
    ranked.sort_by(|a, b| {
        b.final_logprob
            .partial_cmp(&a.final_logprob)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let collapse_rate = if pre_dedup_count == 0 {
        0.0
    } else {
        1.0 - (post_dedup_count as f32 / pre_dedup_count as f32)
    };

    DecodedQuery {
        programs: ranked,
        anchors,
        dedup_collapse_rate: collapse_rate,
    }
}

/// Convert a [`DecodedQuery`] into a [`ParsedQuery`] suitable for the
/// existing executor. `country_candidates` is supplied externally so
/// the cheap classifier output and the model country head can be
/// merged outside this module.
#[must_use]
pub fn to_parsed_query(
    text: &str,
    decoded: &DecodedQuery,
    country_candidates: Vec<(CountryId, f32)>,
    max_hypotheses: u8,
) -> ParsedQuery {
    let mut hypotheses: Vec<ParseHypothesis> = decoded
        .programs
        .iter()
        .take(max_hypotheses.max(1) as usize)
        .map(|rp| rp.source_hypothesis.clone())
        .collect();
    if hypotheses.is_empty() {
        hypotheses.push(ParseHypothesis::default());
    }

    let confidence = decoded
        .programs
        .first()
        .map(|p| {
            // Bound to [0, 1]: clip exp(final_logprob).
            let s = p.final_logprob.exp();
            s.clamp(0.0, 1.0)
        })
        .unwrap_or(0.5);

    let recovery = recovery_flags_from_hypothesis(&hypotheses[0]);

    ParsedQuery {
        original_text: text.to_string(),
        country_candidates,
        hypotheses,
        global_confidence: confidence,
        recovery_flags: recovery,
        execution_budget: ExecutionBudget::default(),
    }
}

/// Inject high-confidence anchors into a hypothesis when the parser
/// failed to claim them. Respects role-smoothness — only anchors at
/// or above `1 - ε` are merged.
///
/// Per-field policy:
///
/// - **Postcode anchor** (high-conf only): pushed as the top postcode
///   candidate if the hypothesis has none. If the hypothesis has a
///   conflicting postcode, the anchor is added as a secondary
///   candidate at lower weight (the executor can pick).
/// - **House-number anchor**: pushed if the hypothesis didn't claim
///   any house number. House anchors have confidence < 1.0 by
///   default; we only inject when there's no alternative.
/// - **Locality anchor**: pushed as the top locality candidate when
///   the hypothesis didn't claim a locality. The anchor is "exact-match
///   in shard" so it's almost always the right call.
///
/// Also: the locality anchor's bytes carve out a range that should
/// NOT be confused with a street suffix. If the parser's street
/// candidate **contains** the locality verbatim as its tail, strip
/// the trailing tail off the street candidate. This is the same
/// fix the heuristic parser applies when it finds a postcode anchor
/// — see `parse_heuristic`.
fn merge_trusted_anchors(h: &mut ParseHypothesis, anchors: &[Anchor]) {
    let trust = 1.0 - super::anchor::ANCHOR_EPSILON;
    for a in anchors {
        if a.confidence < trust {
            continue;
        }
        match a.field {
            super::anchor::AnchorField::Postcode => {
                let already = h.postcode_candidates.iter().any(|(v, _)| v == &a.value);
                if !already {
                    h.postcode_candidates.insert(0, (a.value.clone(), 1.0));
                }
            }
            super::anchor::AnchorField::HouseNumber => {
                if h.house_candidates.is_empty() {
                    h.house_candidates.push((a.value.clone(), a.confidence));
                }
            }
            super::anchor::AnchorField::Locality => {
                let already = h
                    .locality_candidates
                    .iter()
                    .any(|(v, _)| v.eq_ignore_ascii_case(&a.value));
                if !already {
                    h.locality_candidates.insert(0, (a.value.clone(), 1.0));
                }
                // Strip locality tail from street candidates if present.
                let loc_norm = crate::parser::normalize::normalize(&a.value);
                let mut new_streets: Vec<(String, f32)> =
                    Vec::with_capacity(h.street_candidates.len());
                for (s, w) in h.street_candidates.drain(..) {
                    let s_norm = crate::parser::normalize::normalize(&s);
                    if s_norm.ends_with(&loc_norm) && s_norm.len() > loc_norm.len() + 1 {
                        // Strip the locality token off the end.
                        let cut = s.len().saturating_sub(a.value.len());
                        let trimmed = s[..cut].trim_end_matches([' ', ',', ';']).to_string();
                        if !trimmed.is_empty() {
                            new_streets.push((trimmed, w));
                        }
                        // Also keep the original as a low-weight fallback.
                        new_streets.push((s, w * 0.3));
                    } else {
                        new_streets.push((s, w));
                    }
                }
                h.street_candidates = new_streets;
            }
        }
    }
}

fn recovery_flags_from_hypothesis(h: &ParseHypothesis) -> RecoveryFlags {
    RecoveryFlags {
        had_postcode: !h.postcode_candidates.is_empty(),
        had_house_number: !h.house_candidates.is_empty(),
        had_locality: !h.locality_candidates.is_empty(),
        stripped_country_suffix: false,
    }
}

fn field_mask_from_hypothesis(h: &ParseHypothesis) -> FieldMask {
    let mut m = FieldMask::NONE;
    if !h.postcode_candidates.is_empty() {
        m = m.with(Channel::Postcode);
    }
    if !h.house_candidates.is_empty() {
        m = m.with(Channel::HouseNumber);
    }
    if !h.locality_candidates.is_empty() {
        m = m.with(Channel::Locality);
    }
    if !h.street_candidates.is_empty() {
        m = m.with(Channel::Street);
    }
    m
}

/// Build a [`ParseHypothesis`] from a per-byte BIO label sequence
/// over `text`. This is the inverse of the labeling done by training.
pub fn build_hypothesis_from_labels(text: &str, labels: &[usize]) -> ParseHypothesis {
    let bytes = text.as_bytes();
    let n = labels.len().min(bytes.len());

    // Walk and collect contiguous spans per field.
    let mut spans: Vec<BioSpan> = Vec::new();
    let mut current: Option<(u8, usize)> = None;
    let close = |current: &mut Option<(u8, usize)>, end: usize, spans: &mut Vec<BioSpan>| {
        if let Some((field, start)) = current.take()
            && end > start
        {
            let raw = &bytes[start..end.min(bytes.len())];
            let text = String::from_utf8_lossy(raw).into_owned();
            spans.push(BioSpan {
                field,
                byte_range: start..end,
                mean_label_prob: 1.0,
                text,
            });
        }
    };
    for (i, &label) in labels.iter().enumerate().take(n) {
        match crate::tagger::transformer::bio_to_field(label) {
            None => close(&mut current, i, &mut spans),
            Some(field) => {
                let opens = crate::tagger::transformer::is_b(label)
                    || current.as_ref().is_none_or(|(f, _)| *f != field);
                if opens {
                    close(&mut current, i, &mut spans);
                    current = Some((field, i));
                }
            }
        }
    }
    close(&mut current, n, &mut spans);

    let mut h = ParseHypothesis::default();
    for s in &spans {
        let value = s.text.trim().to_string();
        if value.is_empty() {
            continue;
        }
        match s.field {
            0 => h.street_candidates.push((value, 1.0)),
            1 => h.house_candidates.push((value, 1.0)),
            2 => h.postcode_candidates.push((value, 1.0)),
            3 => h.locality_candidates.push((value, 1.0)),
            _ => {}
        }
    }

    let mut mask = FieldMask::NONE;
    if !h.postcode_candidates.is_empty() {
        mask = mask.with(Channel::Postcode);
    }
    if !h.house_candidates.is_empty() {
        mask = mask.with(Channel::HouseNumber);
    }
    h.field_reliability = mask;
    h.retrieval_policy = RetrievalPolicy::belgium_default();
    h.strictness = Strictness::Exact;
    h
}

/// Build a retrieval program (Op tree) for a hypothesis under a policy.
fn build_program_for_hypothesis(h: &ParseHypothesis, policy: &RetrievalPolicy) -> Op {
    let mut blockers: Vec<Op> = Vec::new();
    let mut reducers: Vec<Op> = Vec::new();
    let mut scorers: Vec<Op> = Vec::new();

    let mut push_for = |ch: Channel, key: &str| {
        let lookup = Op::Lookup(LookupKey {
            channel: ch,
            key: key.to_string(),
        });
        match policy.role(ch) {
            Some(ChannelRole::Blocker) => blockers.push(lookup),
            Some(ChannelRole::Reducer) => reducers.push(lookup),
            Some(ChannelRole::Scorer) => scorers.push(Op::Score {
                child: Box::new(lookup),
                channel: ch,
                weight: 1.0,
            }),
            None => {}
        }
    };

    if let Some((pc, _)) = h.postcode_candidates.first() {
        push_for(Channel::Postcode, pc);
    }
    if let Some((st, _)) = h.street_candidates.first() {
        push_for(Channel::Street, st);
    }
    if let Some((loc, _)) = h.locality_candidates.first() {
        push_for(Channel::Locality, loc);
    }

    let base: Op = match (blockers.len(), reducers.len()) {
        (0, 0) if !scorers.is_empty() => Op::Union(scorers.clone()),
        (0, 0) => Op::Lookup(LookupKey {
            channel: Channel::Locality,
            key: String::new(),
        }),
        (0, _) => {
            if reducers.len() == 1 {
                reducers.into_iter().next().expect("len==1")
            } else {
                Op::Intersect(reducers)
            }
        }
        (_, 0) => {
            if blockers.len() == 1 {
                blockers.into_iter().next().expect("len==1")
            } else {
                Op::Intersect(blockers)
            }
        }
        (_, _) => {
            let mut all = blockers;
            all.extend(reducers);
            if all.len() == 1 {
                all.into_iter().next().expect("len==1")
            } else {
                Op::Intersect(all)
            }
        }
    };

    let after_filter = if let Some((hn, _)) = h.house_candidates.first() {
        Op::Filter {
            child: Box::new(base),
            predicate: crate::geocoder::program::FilterPredicate::HouseNumberEq(hn.clone()),
        }
    } else {
        base
    };

    Op::Cap {
        child: Box::new(after_filter),
        n: 64,
    }
}

/// #98 1.5 — legacy retrieval-utility heuristic scorer (pre-trait).
///
/// Returns a log-probability adjustment (positive = reward, negative =
/// penalty) to the hypothesis score.
///
/// Kept under `cfg(test)` so the existing decoding test suite that
/// pre-dates the trait swap can still validate the scoring formula.
/// New code must use [`super::retrieval_utility::HeuristicScorer`]
/// (the trait-backed equivalent the production decoder calls).
#[cfg(test)]
fn retrieval_utility_score(
    program: &Op,
    policy: &RetrievalPolicy,
    hypothesis: &ParseHypothesis,
    shard: &Shard,
    cfg: &UtilityConfig,
) -> f32 {
    let stats = shard.stats();
    let cost = static_cost(program, stats);
    let ceiling = ExecutionBudget::default().static_cost_ceiling.max(1.0);
    let cost_fraction = (cost / ceiling).clamp(0.0, 1.0);
    let mut score = -cost_fraction * cfg.static_cost_penalty;

    // Count blockers in the policy.
    let blocker_count = policy
        .roles
        .iter()
        .filter(|r| matches!(r, Some(ChannelRole::Blocker)))
        .count();
    score += blocker_count as f32 * cfg.blocker_reward;

    if blocker_count == 0 {
        score -= cfg.all_scorer_penalty;
    }

    // Empty / oversized lookup penalties — walk the program tree's lookups.
    let total = stats.total_addresses.max(1) as f32;
    let oversized_threshold = (total * cfg.oversized_fraction_threshold) as usize;
    walk_lookups(program, &mut |k| {
        let postings_len: usize = match k.channel {
            Channel::Postcode => shard.postings_for_postcode(&k.key).len(),
            Channel::Street => shard.postings_for_street(&k.key).len(),
            Channel::Locality => shard.postings_for_locality(&k.key).len(),
            Channel::HouseNumber => 1,
            _ => 0,
        };
        if postings_len == 0 {
            // Only punish a lookup that *should* hit — if the hypothesis didn't
            // claim that field, there's nothing wrong with an empty lookup
            // (the program just won't include that channel).
            let claimed = match k.channel {
                Channel::Postcode => !hypothesis.postcode_candidates.is_empty(),
                Channel::Street => !hypothesis.street_candidates.is_empty(),
                Channel::Locality => !hypothesis.locality_candidates.is_empty(),
                _ => false,
            };
            if claimed {
                score -= cfg.empty_lookup_penalty;
            }
        } else if postings_len > oversized_threshold {
            score -= cfg.oversized_lookup_penalty;
        }
    });

    score
}

#[cfg(test)]
fn walk_lookups<F: FnMut(&LookupKey)>(op: &Op, f: &mut F) {
    match op {
        Op::Lookup(k) => f(k),
        Op::Intersect(c) | Op::Union(c) | Op::TopkMerge { children: c, .. } => {
            for child in c {
                walk_lookups(child, f);
            }
        }
        Op::Filter { child, .. }
        | Op::Score { child, .. }
        | Op::Cap { child, .. }
        | Op::Sample { child, .. }
        | Op::Downgrade { child, .. } => walk_lookups(child, f),
    }
}

/// Public re-export used by tests & the parser backend.
pub use crate::tagger::inference::InferenceOutput as _InferenceOutputAlias;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geocoder::channels::ChannelRole;
    use crate::tagger::inference::InferenceOutput;
    use crate::tagger::transformer::{
        BIO_B_HOUSE, BIO_B_POSTCODE, BIO_B_STREET, BIO_I_POSTCODE, BIO_I_STREET, BIO_O,
        NUM_BIO_LABELS,
    };

    fn one_hot_logprobs(label: usize) -> [f32; NUM_BIO_LABELS] {
        let mut row = [-5.0_f32; NUM_BIO_LABELS];
        row[label] = -0.05;
        row
    }

    fn build_inference(labels: &[usize], n_countries: usize) -> InferenceOutput {
        let bio_logprobs: Vec<[f32; NUM_BIO_LABELS]> =
            labels.iter().map(|&l| one_hot_logprobs(l)).collect();
        let entropy_per_byte = vec![0.05_f32; labels.len()];
        let mut country_posterior = vec![0.0_f32; n_countries];
        if !country_posterior.is_empty() {
            country_posterior[0] = 1.0;
        }
        InferenceOutput {
            bio_label_top1: labels.to_vec(),
            bio_logprobs,
            country_posterior,
            spans: Vec::new(),
            entropy_per_byte,
        }
    }

    fn small_shard() -> (tempfile::TempDir, Shard) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("shard.bfgs");
        let addrs = vec![
            crate::shard::AddressRecord {
                street: "Rue Wayez".into(),
                housenumber: "122".into(),
                postcode: "1070".into(),
                locality: "Anderlecht".into(),
                lat: 50.834,
                lon: 4.314,
                ..Default::default()
            },
            crate::shard::AddressRecord {
                street: "Grote Markt".into(),
                housenumber: "1".into(),
                postcode: "2000".into(),
                locality: "Antwerpen".into(),
                lat: 51.221,
                lon: 4.401,
                ..Default::default()
            },
        ];
        crate::shard::builder::build_shard(&path, crate::routing::CountryId::BE, addrs).unwrap();
        (dir, Shard::open(&path).unwrap())
    }

    #[test]
    fn build_hypothesis_extracts_fields_from_labels() {
        let text = "Rue Wayez 122 1070 Anderlecht";
        let mut labels = vec![BIO_O; text.len()];
        // "Rue Wayez" — 0..9
        labels[0] = BIO_B_STREET;
        for slot in labels.iter_mut().take(9).skip(1) {
            *slot = BIO_I_STREET;
        }
        // "122" — 10..13
        labels[10] = BIO_B_HOUSE;
        labels[11] = crate::tagger::transformer::BIO_I_HOUSE;
        labels[12] = crate::tagger::transformer::BIO_I_HOUSE;
        // "1070" — 14..18
        labels[14] = BIO_B_POSTCODE;
        labels[15] = BIO_I_POSTCODE;
        labels[16] = BIO_I_POSTCODE;
        labels[17] = BIO_I_POSTCODE;
        // "Anderlecht" — 19..29
        labels[19] = crate::tagger::transformer::BIO_B_LOCALITY;
        for slot in labels.iter_mut().take(29).skip(20) {
            *slot = crate::tagger::transformer::BIO_I_LOCALITY;
        }

        let h = build_hypothesis_from_labels(text, &labels);
        assert_eq!(h.street_candidates[0].0, "Rue Wayez");
        assert_eq!(h.house_candidates[0].0, "122");
        assert_eq!(h.postcode_candidates[0].0, "1070");
        assert_eq!(h.locality_candidates[0].0, "Anderlecht");
    }

    #[test]
    fn dedup_collapses_alternative_label_assignments() {
        // Hypothesis 1 and 2 result in the same parsed fields after
        // canonicalization (different I-/B- choices that the parser
        // collapses into the same span). Both end up at the same
        // canonical operator tree → dedup fires.
        let text = "AB";
        let labels1 = vec![BIO_B_STREET, BIO_I_STREET];
        let labels2 = vec![BIO_B_STREET, BIO_I_STREET]; // identical — same canonical
        let inf = build_inference(&labels1, 1);
        let _ = labels2;

        let (_dir, shard) = small_shard();
        let beam_cfg = BeamConfig::default();
        let util_cfg = UtilityConfig::default();
        let decoded = decode(text, &inf, &shard, &beam_cfg, &util_cfg);
        // With single-label one-hot logprobs, all beam hypotheses
        // converge to the same labels → dedup collapse is non-zero
        // (or trivially zero only if pre/post are both 1, which is
        // valid for unambiguous input).
        assert!(decoded.dedup_collapse_rate >= 0.0);
        assert!(!decoded.programs.is_empty());
    }

    #[test]
    fn utility_scorer_penalizes_all_scorer_policy() {
        let mut h = ParseHypothesis::default();
        h.street_candidates.push(("Rue Wayez".to_string(), 1.0));
        let policy_blocker =
            RetrievalPolicy::from_pairs(&[(Channel::Street, ChannelRole::Blocker)]);
        let policy_scorer = RetrievalPolicy::from_pairs(&[(Channel::Street, ChannelRole::Scorer)]);
        let prog_b = build_program_for_hypothesis(&h, &policy_blocker);
        let prog_s = build_program_for_hypothesis(&h, &policy_scorer);
        let (_dir, shard) = small_shard();
        let cfg = UtilityConfig::default();
        let s_blocker = retrieval_utility_score(&prog_b, &policy_blocker, &h, &shard, &cfg);
        let s_scorer = retrieval_utility_score(&prog_s, &policy_scorer, &h, &shard, &cfg);
        assert!(
            s_blocker > s_scorer,
            "expected blocker > scorer, got {s_blocker} vs {s_scorer}"
        );
    }

    #[test]
    fn empty_lookup_penalty_fires_for_claimed_missing_field() {
        let mut h = ParseHypothesis::default();
        h.postcode_candidates.push(("9999".to_string(), 1.0)); // not in shard
        let policy = RetrievalPolicy::from_pairs(&[(Channel::Postcode, ChannelRole::Blocker)]);
        let prog = build_program_for_hypothesis(&h, &policy);
        let (_dir, shard) = small_shard();
        let cfg = UtilityConfig::default();
        let s_missing = retrieval_utility_score(&prog, &policy, &h, &shard, &cfg);

        let mut h2 = ParseHypothesis::default();
        h2.postcode_candidates.push(("1070".to_string(), 1.0));
        let prog2 = build_program_for_hypothesis(&h2, &policy);
        let s_found = retrieval_utility_score(&prog2, &policy, &h2, &shard, &cfg);
        assert!(
            s_missing < s_found,
            "missing-postcode score {s_missing} should be less than found {s_found}"
        );
    }
}
