//! Phase 2 feature extractor (#98 Phase 2).
//!
//! ## Architectural framing
//!
//! These features describe a **(parser hypothesis, retrieval program)
//! pair**. They are NOT features of a candidate — that's the job of
//! `confidence::features::Features`. The Phase 2 GBDT scores
//! P(geocode-success | hypothesis, program) so the beam can prefer
//! hypotheses whose canonicalized retrieval programs land on the gold
//! record.
//!
//! Per #98 §2.2:
//! > Features: parse likelihood, channel role assignments, static cost
//! > (per #96), country posterior, anchor consistency, blocker coverage
//!
//! What this module emphatically does **NOT** do:
//! - Score parse quality. The hypothesis with the cleanest BIO labels
//!   is irrelevant if its retrieval program misses the gold.
//! - Re-derive country routing. The country posterior is consumed as a
//!   prior (#98 1.3).
//! - Encode shard-record candidate features. That's `confidence::Features`,
//!   a different layer with a different objective.
//!
//! ## Schema versioning
//!
//! On-disk schema version in [`Features::SCHEMA_VERSION`]. Bumping the
//! version invalidates every committed model file. Both the trainer
//! and the runtime check it on load.

use serde::{Deserialize, Serialize};

use crate::geocoder::channels::{Channel, ChannelRole};
use crate::geocoder::cost::{ShardStats, static_cost};
use crate::geocoder::program::{LookupKey, Op};
use crate::parser::anchor::{Anchor, AnchorField};
use crate::shard::reader::Shard;
use crate::types::{ExecutionBudget, ParseHypothesis, RetrievalPolicy, Strictness};

/// Total feature count. Must equal the number of fields actually
/// emitted by [`Features::to_row`]. Bumped together with
/// [`Features::SCHEMA_VERSION`].
pub const N_FEATURES: usize = 30;

/// Channel-role encoding: Blocker = 0.0, Reducer = 1.0, Scorer = 2.0,
/// None = -1.0. Bumped together with [`Features::SCHEMA_VERSION`].
fn encode_role(r: Option<ChannelRole>) -> f32 {
    match r {
        Some(ChannelRole::Blocker) => 0.0,
        Some(ChannelRole::Reducer) => 1.0,
        Some(ChannelRole::Scorer) => 2.0,
        None => -1.0,
    }
}

fn encode_strictness(s: Strictness) -> f32 {
    match s {
        Strictness::Exact => 0.0,
        Strictness::Fuzzy => 1.0,
        Strictness::Desperate => 2.0,
    }
}

/// Feature row scored by the Phase 2 GBDT.
///
/// Field order is part of the on-disk schema. Adding a field requires
/// bumping [`Features::SCHEMA_VERSION`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Features {
    // ── Parser-side (5)
    /// Hypothesis source log-prob (parser confidence). Range typically
    /// `(-inf, 0]` for a trained model; clipped at -50 for stability.
    pub hypothesis_logprob: f32,
    /// Field reliability mask, encoded as 4 bits packed into one f32:
    /// `1.0 * postcode + 2.0 * street + 4.0 * house + 8.0 * locality`.
    pub field_reliability: f32,
    /// Country posterior probability for the candidate country (#98 1.3).
    pub country_posterior: f32,
    /// Strictness level (Exact=0, Fuzzy=1, Desperate=2).
    pub strictness: f32,
    /// Sibling count (number of hypotheses in the surviving beam).
    /// `ln(1+n)` so the magnitude stays small.
    pub sibling_count_log: f32,

    // ── Program-side (10)
    /// Static cost of the canonicalized program (per #96 Cost
    /// Composition). Normalized as `cost / static_cost_ceiling`.
    pub static_cost_fraction: f32,
    /// Number of operators in the program tree.
    pub op_count: f32,
    /// Number of `Intersect` nodes (blocker count proxy).
    pub n_intersects: f32,
    /// Number of `Union` nodes (alias merge count).
    pub n_unions: f32,
    /// Number of `Score` nodes.
    pub n_scores: f32,
    /// Number of `Filter` nodes (typically house-number predicates).
    pub n_filters: f32,
    /// `1.0` if the program contains at least one Blocker channel,
    /// `0.0` otherwise.
    pub has_blocker: f32,
    /// `ln(1 + max(posting_list_size))` over all `Lookup` operators.
    pub max_postings_log: f32,
    /// `ln(1 + min(posting_list_size))` over all `Lookup` operators with
    /// non-empty postings (selectivity proxy). `0.0` when no lookup hits.
    pub min_postings_log: f32,
    /// Total `Lookup` count in the program.
    pub n_lookups: f32,

    // ── Channel-role assignments (4)
    /// Postcode role (Blocker=0, Reducer=1, Scorer=2, None=-1).
    pub role_postcode: f32,
    /// Street role.
    pub role_street: f32,
    /// House-number role.
    pub role_house: f32,
    /// Locality role.
    pub role_locality: f32,

    // ── Anchors (5)
    /// Postcode-anchor strength: confidence in `[0, 1]`, or -1 if no
    /// anchor was detected.
    pub postcode_anchor: f32,
    /// House-anchor strength.
    pub house_anchor: f32,
    /// Locality-anchor strength.
    pub locality_anchor: f32,
    /// Number of anchors that DISAGREE with the hypothesis's claimed
    /// fields. Higher = more contradiction risk.
    pub anchor_disagreements: f32,
    /// Number of anchors total (postcode/house/locality).
    pub anchor_total: f32,

    // ── Cross-hypothesis context (3)
    /// Rank of this hypothesis in the parser's output, 0-indexed.
    /// `0.0` = best hypothesis.
    pub hypothesis_rank: f32,
    /// Score gap to the top hypothesis (top - this; positive when this
    /// is below the best). `0.0` when this IS the top.
    pub gap_to_top: f32,
    /// Z-score of this hypothesis's logprob relative to the beam.
    /// Robust ordinal signal.
    pub logprob_z: f32,

    // ── Coverage / claimed-field signals (3)
    /// `1.0` if the hypothesis claims a postcode field, else `0.0`.
    pub claims_postcode: f32,
    /// `1.0` if the hypothesis claims a street field.
    pub claims_street: f32,
    /// `1.0` if the hypothesis claims a house number field.
    pub claims_house: f32,
}

impl Features {
    /// On-disk schema version. Bumped together with the feature set.
    /// SCHEMA_VERSION = 1: 30 features (parser/program/role/anchor/context/coverage).
    pub const SCHEMA_VERSION: u32 = 1;

    /// Convert to the dense `Vec<f32>` shape expected by `gbdt::Data`.
    /// Field order matches the struct declaration.
    #[must_use]
    pub fn to_row(&self) -> Vec<f32> {
        vec![
            // parser-side
            self.hypothesis_logprob,
            self.field_reliability,
            self.country_posterior,
            self.strictness,
            self.sibling_count_log,
            // program-side
            self.static_cost_fraction,
            self.op_count,
            self.n_intersects,
            self.n_unions,
            self.n_scores,
            self.n_filters,
            self.has_blocker,
            self.max_postings_log,
            self.min_postings_log,
            self.n_lookups,
            // role assignments
            self.role_postcode,
            self.role_street,
            self.role_house,
            self.role_locality,
            // anchors
            self.postcode_anchor,
            self.house_anchor,
            self.locality_anchor,
            self.anchor_disagreements,
            self.anchor_total,
            // cross-hypothesis context
            self.hypothesis_rank,
            self.gap_to_top,
            self.logprob_z,
            // claimed-field signals
            self.claims_postcode,
            self.claims_street,
            self.claims_house,
        ]
    }

    /// Inverse of [`Self::to_row`]. Returns `None` if the row arity is
    /// wrong (used by the trainer to validate corpus alignment).
    #[must_use]
    pub fn from_row(row: &[f32]) -> Option<Self> {
        if row.len() != N_FEATURES {
            return None;
        }
        Some(Self {
            hypothesis_logprob: row[0],
            field_reliability: row[1],
            country_posterior: row[2],
            strictness: row[3],
            sibling_count_log: row[4],
            static_cost_fraction: row[5],
            op_count: row[6],
            n_intersects: row[7],
            n_unions: row[8],
            n_scores: row[9],
            n_filters: row[10],
            has_blocker: row[11],
            max_postings_log: row[12],
            min_postings_log: row[13],
            n_lookups: row[14],
            role_postcode: row[15],
            role_street: row[16],
            role_house: row[17],
            role_locality: row[18],
            postcode_anchor: row[19],
            house_anchor: row[20],
            locality_anchor: row[21],
            anchor_disagreements: row[22],
            anchor_total: row[23],
            hypothesis_rank: row[24],
            gap_to_top: row[25],
            logprob_z: row[26],
            claims_postcode: row[27],
            claims_street: row[28],
            claims_house: row[29],
        })
    }
}

impl Default for Features {
    fn default() -> Self {
        Self {
            hypothesis_logprob: 0.0,
            field_reliability: 0.0,
            country_posterior: 0.0,
            strictness: 0.0,
            sibling_count_log: 0.0,
            static_cost_fraction: 0.0,
            op_count: 0.0,
            n_intersects: 0.0,
            n_unions: 0.0,
            n_scores: 0.0,
            n_filters: 0.0,
            has_blocker: 0.0,
            max_postings_log: 0.0,
            min_postings_log: 0.0,
            n_lookups: 0.0,
            role_postcode: -1.0,
            role_street: -1.0,
            role_house: -1.0,
            role_locality: -1.0,
            postcode_anchor: -1.0,
            house_anchor: -1.0,
            locality_anchor: -1.0,
            anchor_disagreements: 0.0,
            anchor_total: 0.0,
            hypothesis_rank: 0.0,
            gap_to_top: 0.0,
            logprob_z: 0.0,
            claims_postcode: 0.0,
            claims_street: 0.0,
            claims_house: 0.0,
        }
    }
}

/// Aggregate program-tree statistics. Pulled out as a separate struct
/// so the trait scorer can reuse the walk without re-traversing.
#[derive(Debug, Clone)]
pub struct ProgramFeatures {
    pub op_count: u32,
    pub n_intersects: u32,
    pub n_unions: u32,
    pub n_scores: u32,
    pub n_filters: u32,
    pub n_lookups: u32,
    pub has_blocker: bool,
    pub max_postings: usize,
    pub min_postings: usize,
    pub static_cost: f32,
}

impl ProgramFeatures {
    /// Walk the canonical program tree, accumulating tree-level stats
    /// and per-`Lookup` posting-list sizes from the shard.
    #[must_use]
    pub fn from_program(program: &Op, policy: &RetrievalPolicy, shard: &Shard) -> Self {
        let mut s = ProgramFeatures {
            op_count: 0,
            n_intersects: 0,
            n_unions: 0,
            n_scores: 0,
            n_filters: 0,
            n_lookups: 0,
            // Walked from the canonicalized program tree (set in
            // `walk` below). Reading from `policy.roles` instead would
            // give credit for blocker channels the parser DROPPED
            // during program construction (Copilot review on #178/#168).
            has_blocker: false,
            max_postings: 0,
            min_postings: usize::MAX,
            static_cost: static_cost(program, shard.stats()),
        };
        walk(program, shard, policy, &mut s);
        if s.min_postings == usize::MAX {
            s.min_postings = 0;
        }
        s
    }
}

fn walk(op: &Op, shard: &Shard, policy: &RetrievalPolicy, s: &mut ProgramFeatures) {
    s.op_count += 1;
    match op {
        Op::Lookup(k) => {
            s.n_lookups += 1;
            // `has_blocker` is true iff the program contains at least
            // one Lookup whose channel is policy-Blocker. Walking the
            // tree (not the policy) is what the rest of the row
            // describes, and is what the GBDT learns.
            if matches!(policy.role(k.channel), Some(ChannelRole::Blocker)) {
                s.has_blocker = true;
            }
            let n = posting_list_size(shard, k);
            if n > s.max_postings {
                s.max_postings = n;
            }
            if n > 0 && n < s.min_postings {
                s.min_postings = n;
            }
        }
        Op::Intersect(c) => {
            s.n_intersects += 1;
            for child in c {
                walk(child, shard, policy, s);
            }
        }
        Op::Union(c) => {
            s.n_unions += 1;
            for child in c {
                walk(child, shard, policy, s);
            }
        }
        Op::TopkMerge { children, .. } => {
            for child in children {
                walk(child, shard, policy, s);
            }
        }
        Op::Filter { child, .. } => {
            s.n_filters += 1;
            walk(child, shard, policy, s);
        }
        Op::Score { child, .. } => {
            s.n_scores += 1;
            walk(child, shard, policy, s);
        }
        Op::Cap { child, .. } | Op::Sample { child, .. } | Op::Downgrade { child, .. } => {
            walk(child, shard, policy, s);
        }
    }
}

fn posting_list_size(shard: &Shard, k: &LookupKey) -> usize {
    match k.channel {
        Channel::Postcode => shard.postings_for_postcode(&k.key).len(),
        Channel::Street => shard.postings_for_street(&k.key).len(),
        Channel::Locality => shard.postings_for_locality(&k.key).len(),
        Channel::HouseNumber => 1,
        // Alias / Transliteration channels are not exercised by the
        // MVP executor; treat as unknown.
        _ => 0,
    }
}

/// Anchor-strength summary for one hypothesis.
#[derive(Debug, Clone, Default)]
pub struct AnchorSummary {
    pub postcode: f32,
    pub house: f32,
    pub locality: f32,
    pub disagreements: u32,
    pub total: u32,
}

impl AnchorSummary {
    /// Build from detected anchors + the hypothesis claimed fields.
    /// Disagreement counter increments when the hypothesis claims a
    /// field that contradicts a high-confidence anchor.
    #[must_use]
    pub fn from(anchors: &[Anchor], h: &ParseHypothesis) -> Self {
        let mut s = AnchorSummary {
            postcode: -1.0,
            house: -1.0,
            locality: -1.0,
            disagreements: 0,
            total: 0,
        };
        for a in anchors {
            s.total += 1;
            match a.field {
                AnchorField::Postcode => {
                    s.postcode = a.confidence;
                    if let Some((pc, _)) = h.postcode_candidates.first()
                        && pc != &a.value
                        && a.confidence >= 0.85
                    {
                        s.disagreements += 1;
                    }
                }
                AnchorField::HouseNumber => {
                    s.house = a.confidence;
                    if let Some((hn, _)) = h.house_candidates.first()
                        && hn != &a.value
                        && a.confidence >= 0.85
                    {
                        s.disagreements += 1;
                    }
                }
                AnchorField::Locality => {
                    s.locality = a.confidence;
                    if let Some((loc, _)) = h.locality_candidates.first()
                        && !loc.eq_ignore_ascii_case(&a.value)
                        && a.confidence >= 0.85
                    {
                        s.disagreements += 1;
                    }
                }
            }
        }
        s
    }
}

/// Per-beam aggregates needed for cross-hypothesis features. Computed
/// once per beam, shared across feature extractions for that beam.
#[derive(Debug, Clone, Copy)]
pub struct BeamStats {
    pub size: usize,
    pub top_logprob: f32,
    pub mean_logprob: f32,
    pub stdev_logprob: f32,
}

impl BeamStats {
    #[must_use]
    pub fn from_logprobs(logprobs: &[f32]) -> Self {
        let n = logprobs.len();
        if n == 0 {
            return Self {
                size: 0,
                top_logprob: 0.0,
                mean_logprob: 0.0,
                stdev_logprob: 1.0,
            };
        }
        let top = logprobs.iter().copied().fold(f32::NEG_INFINITY, f32::max);
        let mean = logprobs.iter().sum::<f32>() / n as f32;
        let var = logprobs
            .iter()
            .map(|x| {
                let d = x - mean;
                d * d
            })
            .sum::<f32>()
            / n as f32;
        Self {
            size: n,
            top_logprob: top,
            mean_logprob: mean,
            // Floor stdev so divisions stay finite for degenerate beams.
            stdev_logprob: var.sqrt().max(1e-3),
        }
    }
}

/// Build a [`Features`] row for one (hypothesis, program) pair.
///
/// The caller supplies the canonical program, policy, source-hypothesis
/// log-prob, anchors, and per-beam stats. The function returns a
/// fully-populated row; it does NOT score the hypothesis (that's the
/// scorer's job).
#[allow(clippy::too_many_arguments)]
#[must_use]
pub fn extract(
    h: &ParseHypothesis,
    program: &Op,
    policy: &RetrievalPolicy,
    program_features: &ProgramFeatures,
    anchors: &AnchorSummary,
    beam_stats: BeamStats,
    hypothesis_rank: usize,
    hypothesis_logprob: f32,
    country_posterior: f32,
    shard: &Shard,
) -> Features {
    let _ = (program, shard); // walked already; signature keeps the dependency explicit
    let stats: ShardStats = shard.stats();
    let ceiling = ExecutionBudget::default().static_cost_ceiling.max(1.0);
    let cost_fraction = (program_features.static_cost / ceiling).clamp(0.0, 100.0);
    let _ = stats; // ShardStats is consumed by the program-features walk

    let claims_pc = !h.postcode_candidates.is_empty();
    let claims_st = !h.street_candidates.is_empty();
    let claims_hn = !h.house_candidates.is_empty();
    let claims_lc = !h.locality_candidates.is_empty();
    let field_mask = (claims_pc as u8) as f32
        + 2.0 * (claims_st as u8) as f32
        + 4.0 * (claims_hn as u8) as f32
        + 8.0 * (claims_lc as u8) as f32;

    let logp_clipped = hypothesis_logprob.max(-50.0);
    let gap_to_top = (beam_stats.top_logprob - logp_clipped).max(0.0);
    let logprob_z = (logp_clipped - beam_stats.mean_logprob) / beam_stats.stdev_logprob;
    let sibling_count_log = (1.0 + beam_stats.size as f32).ln();

    Features {
        hypothesis_logprob: logp_clipped,
        field_reliability: field_mask,
        country_posterior,
        strictness: encode_strictness(h.strictness),
        sibling_count_log,

        static_cost_fraction: cost_fraction,
        op_count: program_features.op_count as f32,
        n_intersects: program_features.n_intersects as f32,
        n_unions: program_features.n_unions as f32,
        n_scores: program_features.n_scores as f32,
        n_filters: program_features.n_filters as f32,
        has_blocker: if program_features.has_blocker {
            1.0
        } else {
            0.0
        },
        max_postings_log: (1.0 + program_features.max_postings as f32).ln(),
        min_postings_log: (1.0 + program_features.min_postings as f32).ln(),
        n_lookups: program_features.n_lookups as f32,

        role_postcode: encode_role(policy.role(Channel::Postcode)),
        role_street: encode_role(policy.role(Channel::Street)),
        role_house: encode_role(policy.role(Channel::HouseNumber)),
        role_locality: encode_role(policy.role(Channel::Locality)),

        postcode_anchor: anchors.postcode,
        house_anchor: anchors.house,
        locality_anchor: anchors.locality,
        anchor_disagreements: anchors.disagreements as f32,
        anchor_total: anchors.total as f32,

        hypothesis_rank: hypothesis_rank as f32,
        gap_to_top,
        logprob_z,

        claims_postcode: if claims_pc { 1.0 } else { 0.0 },
        claims_street: if claims_st { 1.0 } else { 0.0 },
        claims_house: if claims_hn { 1.0 } else { 0.0 },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geocoder::program::LookupKey;
    use crate::shard::AddressRecord;
    use crate::shard::builder::build_shard;
    use crate::types::RetrievalPolicy;
    use tempfile::TempDir;

    fn small_shard() -> (TempDir, Shard) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("shard.bfgs");
        let addrs = vec![
            AddressRecord {
                street: "Rue Wayez".into(),
                housenumber: "122".into(),
                postcode: "1070".into(),
                locality: "Anderlecht".into(),
                lat: 50.834,
                lon: 4.314,
                ..Default::default()
            },
            AddressRecord {
                street: "Grote Markt".into(),
                housenumber: "1".into(),
                postcode: "2000".into(),
                locality: "Antwerpen".into(),
                lat: 51.221,
                lon: 4.401,
                ..Default::default()
            },
        ];
        build_shard(&path, crate::routing::CountryId::BE, addrs).unwrap();
        (dir, Shard::open(&path).unwrap())
    }

    fn lookup(ch: Channel, key: &str) -> Op {
        Op::Lookup(LookupKey {
            channel: ch,
            key: key.into(),
        })
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
        assert!(Features::from_row(&[0.0; N_FEATURES - 1]).is_none());
        assert!(Features::from_row(&[0.0; N_FEATURES + 1]).is_none());
    }

    #[test]
    fn program_features_walk_intersect_tree() {
        let (_d, shard) = small_shard();
        let prog = Op::Intersect(vec![
            lookup(Channel::Postcode, "1070"),
            lookup(Channel::Street, "rue wayez"),
        ]);
        let policy = RetrievalPolicy::belgium_default();
        let pf = ProgramFeatures::from_program(&prog, &policy, &shard);
        assert!(pf.has_blocker, "BE default policy has postcode=Blocker");
        assert_eq!(pf.n_intersects, 1);
        assert_eq!(pf.n_lookups, 2);
        assert!(pf.max_postings > 0);
        // Both postcode + street should be present in this shard.
        assert!(pf.min_postings > 0);
    }

    #[test]
    fn program_features_handles_empty_lookup() {
        let (_d, shard) = small_shard();
        let prog = lookup(Channel::Street, "nonexistent street");
        let policy = RetrievalPolicy::belgium_default();
        let pf = ProgramFeatures::from_program(&prog, &policy, &shard);
        assert_eq!(pf.n_lookups, 1);
        // Empty postings → max=0, min=0 (no non-empty lookup).
        assert_eq!(pf.max_postings, 0);
        assert_eq!(pf.min_postings, 0);
    }

    #[test]
    fn beam_stats_handle_single_element() {
        let bs = BeamStats::from_logprobs(&[-1.5]);
        assert_eq!(bs.size, 1);
        assert!((bs.top_logprob + 1.5).abs() < 1e-5);
        assert!((bs.mean_logprob + 1.5).abs() < 1e-5);
    }

    #[test]
    fn beam_stats_empty_safe() {
        let bs = BeamStats::from_logprobs(&[]);
        assert_eq!(bs.size, 0);
        // Doesn't panic; gives non-zero stdev floor.
        assert!(bs.stdev_logprob > 0.0);
    }

    #[test]
    fn extract_clean_query_features() {
        let (_d, shard) = small_shard();
        let mut h = ParseHypothesis::default();
        h.street_candidates.push(("Rue Wayez".into(), 1.0));
        h.postcode_candidates.push(("1070".into(), 1.0));
        h.retrieval_policy = RetrievalPolicy::belgium_default();
        h.strictness = Strictness::Exact;
        let policy = h.retrieval_policy;
        let prog = Op::Intersect(vec![
            lookup(Channel::Postcode, "1070"),
            lookup(Channel::Street, "rue wayez"),
        ]);
        let pf = ProgramFeatures::from_program(&prog, &policy, &shard);
        let anchors = AnchorSummary::default();
        let beam = BeamStats::from_logprobs(&[-0.1]);
        let f = extract(
            &h, &prog, &policy, &pf, &anchors, beam, 0, -0.1, 0.95, &shard,
        );
        assert!(f.has_blocker > 0.5, "BE default has postcode blocker");
        assert!(f.claims_postcode > 0.5);
        assert!(f.claims_street > 0.5);
        assert!(f.claims_house < 0.5, "no house claimed");
        assert!((f.role_postcode - 0.0).abs() < 1e-5, "Blocker = 0.0");
        assert!((f.role_street - 1.0).abs() < 1e-5, "Reducer = 1.0");
        assert!(f.country_posterior > 0.9);
        assert_eq!(f.hypothesis_rank, 0.0);
        assert_eq!(f.gap_to_top, 0.0);
    }

    #[test]
    fn extract_handles_no_postcode_hypothesis() {
        let (_d, shard) = small_shard();
        let mut h = ParseHypothesis::default();
        h.street_candidates.push(("Grote Markt".into(), 1.0));
        h.locality_candidates.push(("Antwerpen".into(), 1.0));
        h.retrieval_policy = RetrievalPolicy::belgium_default();
        let policy = h.retrieval_policy;
        let prog = lookup(Channel::Street, "grote markt");
        let pf = ProgramFeatures::from_program(&prog, &policy, &shard);
        let anchors = AnchorSummary::default();
        let beam = BeamStats::from_logprobs(&[-0.5]);
        let f = extract(
            &h, &prog, &policy, &pf, &anchors, beam, 0, -0.5, 0.9, &shard,
        );
        assert!(f.claims_street > 0.5);
        assert!(f.claims_postcode < 0.5);
        // Without postcode in hypothesis, field_reliability should
        // exclude the postcode bit (1.0) but include street (2.0) +
        // locality (8.0).
        assert!((f.field_reliability - 10.0).abs() < 1e-5);
    }

    #[test]
    fn extract_anchor_disagreement_counted() {
        let (_d, shard) = small_shard();
        let mut h = ParseHypothesis::default();
        // Hypothesis claims 9999, anchor says 1070 with confidence 1.0.
        h.postcode_candidates.push(("9999".into(), 1.0));
        h.retrieval_policy = RetrievalPolicy::belgium_default();
        let policy = h.retrieval_policy;
        let prog = lookup(Channel::Postcode, "9999");
        let pf = ProgramFeatures::from_program(&prog, &policy, &shard);
        let pseudo_anchor = Anchor {
            field: AnchorField::Postcode,
            value: "1070".into(),
            confidence: 1.0,
            byte_range: 0..4,
        };
        let summary = AnchorSummary::from(&[pseudo_anchor], &h);
        assert_eq!(summary.disagreements, 1);
        assert_eq!(summary.total, 1);
        let beam = BeamStats::from_logprobs(&[-0.1]);
        let f = extract(
            &h, &prog, &policy, &pf, &summary, beam, 0, -0.1, 0.95, &shard,
        );
        assert!((f.anchor_disagreements - 1.0).abs() < 1e-5);
        assert!((f.postcode_anchor - 1.0).abs() < 1e-5);
    }

    #[test]
    fn role_encoding_is_stable() {
        assert_eq!(encode_role(Some(ChannelRole::Blocker)), 0.0);
        assert_eq!(encode_role(Some(ChannelRole::Reducer)), 1.0);
        assert_eq!(encode_role(Some(ChannelRole::Scorer)), 2.0);
        assert_eq!(encode_role(None), -1.0);
    }
}
