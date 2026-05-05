//! Retrieval-program operators (#96 §Retrieval Operators).
//!
//! ## Canonicalization (#96 Recombination Invariant)
//!
//! [`Op::canonicalize`] applies the full Recombination Invariant from
//! #96:
//!
//! - **Stable ordering** of commutative operator operands (`Intersect`
//!   and `Union` sort their children by canonical form)
//! - **Identity folding**:
//!     - `intersect(A, universe) → A` (universe = empty `Intersect`)
//!     - `union(A, ∅) → A` (∅ = empty `Union`)
//! - **Redundancy collapse**:
//!     - `cap(cap(A, n), m) → cap(A, min(n, m))`
//!     - `intersect(A, A) → A`
//!     - `union(A, A) → A`
//!     - `score(A, ∅) → A` (zero-weight score)
//! - **Source-hypothesis score merge** when [`dedup_canonical`]
//!   collapses equivalent programs. Per #96 the implementation choice
//!   (max vs sum) is documented per release: **we use `max`**. Sum
//!   over [0, 1] confidences is unbounded above and confounds
//!   calibration with retrieval frequency; max preserves the strongest
//!   hypothesis-source signal without distortion.
//!
//! ## Zero-Cost-on-Clean-Queries (#96 NFR)
//!
//! The executor's single-hypothesis fast path *skips* canonicalize
//! entirely; this method is here for the multi-hypothesis path that
//! #98 will exercise. On a single-element list, [`dedup_canonical`]
//! returns immediately.

use std::cmp::Ordering;

use super::channels::{Channel, ChannelRole};
use crate::types::ParseHypothesis;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LookupKey {
    pub channel: Channel,
    pub key: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum FilterPredicate {
    HouseNumberEq(String),
}

/// Retrieval-program operator (#96 operator table).
///
/// Static operators (compositional cost): `Lookup`, `Intersect`,
/// `Union`, `Filter`, `Score`, `Cap`.
///
/// Static-but-budget operators: `TopkMerge`, `Sample`.
///
/// Feedback operator (staged cost, non-local): `Downgrade`.
///
/// MVP executor only fires `Lookup`, `Intersect`, `Filter`, `Score`,
/// `Cap`. The remaining operators are defined and canonicalize
/// correctly so #98 and #97 do not need to extend the algebra later.
#[derive(Debug, Clone, PartialEq)]
pub enum Op {
    Lookup(LookupKey),
    Intersect(Vec<Op>),
    Union(Vec<Op>),
    TopkMerge {
        children: Vec<Op>,
        k: u32,
    },
    Filter {
        child: Box<Op>,
        predicate: FilterPredicate,
    },
    Score {
        child: Box<Op>,
        channel: Channel,
        weight: f32,
    },
    Cap {
        child: Box<Op>,
        n: u32,
    },
    Sample {
        child: Box<Op>,
        n: u32,
    },
    Downgrade {
        channel: Channel,
        from_role: ChannelRole,
        to_role: ChannelRole,
        child: Box<Op>,
    },
}

impl Op {
    /// Canonicalize the operator tree per #96 Recombination Invariant.
    pub fn canonicalize(self) -> Op {
        match self {
            Op::Lookup(_) => self,
            Op::Intersect(children) => canonicalize_intersect(children),
            Op::Union(children) => canonicalize_union(children),
            Op::TopkMerge { children, k } => {
                let mut children: Vec<Op> = children.into_iter().map(Op::canonicalize).collect();
                children.sort_by(cmp_op);
                Op::TopkMerge { children, k }
            }
            Op::Filter { child, predicate } => Op::Filter {
                child: Box::new(child.canonicalize()),
                predicate,
            },
            Op::Score {
                child,
                channel,
                weight,
            } => {
                // score(A, ∅) → A: a Score with weight 0 contributes
                // no rank evidence. Fold it away.
                let inner = child.canonicalize();
                if weight == 0.0 {
                    inner
                } else {
                    Op::Score {
                        child: Box::new(inner),
                        channel,
                        weight,
                    }
                }
            }
            Op::Cap { child, n } => {
                let inner = child.canonicalize();
                if let Op::Cap {
                    child: inner_child,
                    n: inner_n,
                } = inner
                {
                    Op::Cap {
                        child: inner_child,
                        n: n.min(inner_n),
                    }
                } else {
                    Op::Cap {
                        child: Box::new(inner),
                        n,
                    }
                }
            }
            Op::Sample { child, n } => Op::Sample {
                child: Box::new(child.canonicalize()),
                n,
            },
            Op::Downgrade {
                channel,
                from_role,
                to_role,
                child,
            } => Op::Downgrade {
                channel,
                from_role,
                to_role,
                child: Box::new(child.canonicalize()),
            },
        }
    }
}

/// Canonicalize `Intersect`:
///   - Recurse into children.
///   - **Identity fold**: `intersect(A, universe) → A`. We treat an
///     `Intersect` with zero children as the universe operand.
///   - Sort children for stable canonical form.
///   - **Redundancy collapse**: `intersect(A, A) → A` (dedup adjacent
///     equal children after sort).
///   - Single-child fold: `Intersect([A]) → A`.
fn canonicalize_intersect(children: Vec<Op>) -> Op {
    let mut children: Vec<Op> = children
        .into_iter()
        .map(Op::canonicalize)
        // Identity fold: drop empty Intersect (universe) operands.
        .filter(|c| !matches!(c, Op::Intersect(inner) if inner.is_empty()))
        .collect();

    children.sort_by(cmp_op);
    children.dedup();

    if children.len() == 1 {
        return children.pop().expect("len == 1");
    }
    Op::Intersect(children)
}

/// Canonicalize `Union`:
///   - Recurse into children.
///   - **Identity fold**: `union(A, ∅) → A`. We treat a `Union` with
///     zero children as the empty set.
///   - Sort children for stable canonical form.
///   - **Redundancy collapse**: `union(A, A) → A`.
///   - Single-child fold: `Union([A]) → A`.
fn canonicalize_union(children: Vec<Op>) -> Op {
    let mut children: Vec<Op> = children
        .into_iter()
        .map(Op::canonicalize)
        // Identity fold: drop empty Union (∅) operands.
        .filter(|c| !matches!(c, Op::Union(inner) if inner.is_empty()))
        .collect();

    children.sort_by(cmp_op);
    children.dedup();

    if children.len() == 1 {
        return children.pop().expect("len == 1");
    }
    Op::Union(children)
}

fn cmp_op(a: &Op, b: &Op) -> Ordering {
    use Op::*;
    let tag = |op: &Op| -> u8 {
        match op {
            Lookup(_) => 0,
            Intersect(_) => 1,
            Union(_) => 2,
            TopkMerge { .. } => 3,
            Filter { .. } => 4,
            Score { .. } => 5,
            Cap { .. } => 6,
            Sample { .. } => 7,
            Downgrade { .. } => 8,
        }
    };
    let t = tag(a).cmp(&tag(b));
    if t != Ordering::Equal {
        return t;
    }
    match (a, b) {
        (Lookup(la), Lookup(lb)) => la
            .channel
            .index()
            .cmp(&lb.channel.index())
            .then_with(|| la.key.cmp(&lb.key)),
        (Intersect(ca), Intersect(cb)) | (Union(ca), Union(cb)) => cmp_op_vec(ca, cb),
        (
            TopkMerge {
                children: ca,
                k: ka,
            },
            TopkMerge {
                children: cb,
                k: kb,
            },
        ) => cmp_op_vec(ca, cb).then_with(|| ka.cmp(kb)),
        (
            Filter {
                child: ca,
                predicate: pa,
            },
            Filter {
                child: cb,
                predicate: pb,
            },
        ) => cmp_op(ca, cb).then_with(|| format!("{pa:?}").cmp(&format!("{pb:?}"))),
        (
            Score {
                child: ca,
                channel: cha,
                weight: wa,
            },
            Score {
                child: cb,
                channel: chb,
                weight: wb,
            },
        ) => cmp_op(ca, cb)
            .then_with(|| cha.index().cmp(&chb.index()))
            .then_with(|| wa.partial_cmp(wb).unwrap_or(Ordering::Equal)),
        (Cap { child: ca, n: na }, Cap { child: cb, n: nb }) => {
            cmp_op(ca, cb).then_with(|| na.cmp(nb))
        }
        (Sample { child: ca, n: na }, Sample { child: cb, n: nb }) => {
            cmp_op(ca, cb).then_with(|| na.cmp(nb))
        }
        (
            Downgrade {
                channel: cha,
                from_role: fa,
                to_role: ta,
                child: chca,
            },
            Downgrade {
                channel: chb,
                from_role: fb,
                to_role: tb,
                child: chcb,
            },
        ) => cha
            .index()
            .cmp(&chb.index())
            .then_with(|| fa.cmp(fb))
            .then_with(|| ta.cmp(tb))
            .then_with(|| cmp_op(chca, chcb)),
        _ => Ordering::Equal,
    }
}

fn cmp_op_vec(a: &[Op], b: &[Op]) -> Ordering {
    let n = a.len().min(b.len());
    for i in 0..n {
        let c = cmp_op(&a[i], &b[i]);
        if c != Ordering::Equal {
            return c;
        }
    }
    a.len().cmp(&b.len())
}

/// Dedup a list of `(program, source_score)` pairs by canonical form.
///
/// Per the **Recombination Invariant** (#96):
///   "When equivalent programs are merged, their source-hypothesis
///    scores are combined (max or sum — implementation detail,
///    documented per release)."
///
/// **This release uses `max`.** Sum over [0, 1] confidences is
/// unbounded above and confounds calibration with retrieval frequency;
/// max preserves the strongest hypothesis-source signal without
/// distortion.
///
/// Fast path: a single-element list is canonicalized once and returned
/// without entering the comparison loop. That keeps the multi-hypothesis
/// path cheap when the parser happens to emit one hypothesis (the MVP
/// heuristic parser's only mode) and is part of the
/// Zero-Cost-on-Clean-Queries NFR.
///
/// Equality is structural via [`PartialEq`]. The O(N²) sweep is fine
/// because hypothesis counts are tiny (≤ 5 per #97 budget tier).
pub fn dedup_canonical(programs: Vec<(Op, f32)>) -> Vec<(Op, f32)> {
    // Single-element fast path: skip the comparison loop entirely.
    // The Vec is moved in and returned with one canonicalize call.
    if programs.len() <= 1 {
        return programs
            .into_iter()
            .map(|(p, s)| (p.canonicalize(), s))
            .collect();
    }
    let mut out: Vec<(Op, f32)> = Vec::with_capacity(programs.len());
    for (p, score) in programs {
        let canon = p.canonicalize();
        if let Some(existing) = out.iter_mut().find(|(o, _)| o == &canon) {
            existing.1 = existing.1.max(score);
        } else {
            out.push((canon, score));
        }
    }
    out
}

/// Dedup a list of `(program, hypothesis, final_logprob)` triples by
/// canonical form, paired with their source [`ParseHypothesis`].
///
/// This is the primary multi-hypothesis dedup entry point. Per #96 the
/// executor must run each canonical program ONCE against the hypothesis
/// that produced it — NOT against the cross-product of programs ×
/// hypotheses. When multiple hypotheses canonicalize to the same
/// program, we merge them by:
///
/// - **`final_logprob`**: max across the group (consistent with the
///   `max` source-score policy in [`dedup_canonical`]).
/// - **Representative hypothesis**: the one with the highest input
///   `final_logprob` — its parsed fields seed downstream scoring so the
///   strongest signal wins.
///
/// The fast path (≤ 1 input) avoids the comparison loop; this matters
/// because the heuristic parser emits a single hypothesis and that
/// path must stay allocation-light per the Zero-Cost-on-Clean-Queries
/// NFR.
#[must_use]
pub fn dedup_canonical_with_hyp(
    programs: Vec<(Op, ParseHypothesis, f32)>,
) -> Vec<(Op, ParseHypothesis, f32)> {
    if programs.len() <= 1 {
        return programs
            .into_iter()
            .map(|(p, h, lp)| (p.canonicalize(), h, lp))
            .collect();
    }
    let mut out: Vec<(Op, ParseHypothesis, f32)> = Vec::with_capacity(programs.len());
    for (p, h, lp) in programs {
        let canon = p.canonicalize();
        if let Some(idx) = out.iter().position(|(o, _, _)| o == &canon) {
            // Merge into existing entry: keep the representative
            // hypothesis with the higher source logprob, take max
            // final_logprob.
            let existing = &mut out[idx];
            if lp > existing.2 {
                existing.1 = h;
                existing.2 = lp;
            }
        } else {
            out.push((canon, h, lp));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lookup(ch: Channel, key: &str) -> Op {
        Op::Lookup(LookupKey {
            channel: ch,
            key: key.to_string(),
        })
    }

    #[test]
    fn canonicalize_intersect_is_commutative() {
        let a = lookup(Channel::Postcode, "1000");
        let b = lookup(Channel::Street, "rue wayez");
        let p1 = Op::Intersect(vec![a.clone(), b.clone()]).canonicalize();
        let p2 = Op::Intersect(vec![b, a]).canonicalize();
        assert_eq!(p1, p2);
    }

    #[test]
    fn canonicalize_collapses_nested_caps() {
        let inner = Op::Cap {
            child: Box::new(lookup(Channel::Street, "x")),
            n: 50,
        };
        let outer = Op::Cap {
            child: Box::new(inner),
            n: 20,
        };
        match outer.canonicalize() {
            Op::Cap { n, .. } => assert_eq!(n, 20),
            other => panic!("expected Cap, got {other:?}"),
        }
    }

    #[test]
    fn canonicalize_is_idempotent() {
        let p = Op::Intersect(vec![
            lookup(Channel::Postcode, "1000"),
            lookup(Channel::Street, "rue de la loi"),
        ]);
        let c1 = p.canonicalize();
        let c2 = c1.clone().canonicalize();
        assert_eq!(c1, c2);
    }

    #[test]
    fn dedup_collapses_equivalent_programs() {
        let p1 = Op::Intersect(vec![
            lookup(Channel::Postcode, "1000"),
            lookup(Channel::Street, "x"),
        ]);
        let p2 = Op::Intersect(vec![
            lookup(Channel::Street, "x"),
            lookup(Channel::Postcode, "1000"),
        ]);
        let out = dedup_canonical(vec![(p1, 0.7), (p2, 0.9)]);
        assert_eq!(out.len(), 1);
        // Source-hypothesis score merge via max (documented choice).
        assert!((out[0].1 - 0.9).abs() < 1e-6);
    }

    #[test]
    fn dedup_three_equivalent_collapses_to_one() {
        // Per the Recombination Invariant test in B1: build N
        // hypotheses that produce equivalent programs in different
        // commutative orderings; dedup must yield exactly 1 program.
        let h1 = Op::Intersect(vec![
            lookup(Channel::Postcode, "1070"),
            lookup(Channel::Street, "rue wayez"),
            lookup(Channel::HouseNumber, "122"),
        ]);
        let h2 = Op::Intersect(vec![
            lookup(Channel::Street, "rue wayez"),
            lookup(Channel::HouseNumber, "122"),
            lookup(Channel::Postcode, "1070"),
        ]);
        let h3 = Op::Intersect(vec![
            lookup(Channel::HouseNumber, "122"),
            lookup(Channel::Postcode, "1070"),
            lookup(Channel::Street, "rue wayez"),
        ]);
        let out = dedup_canonical(vec![(h1, 0.5), (h2, 0.8), (h3, 0.3)]);
        assert_eq!(out.len(), 1, "3 commutatively equivalent → 1 program");
        assert!((out[0].1 - 0.8).abs() < 1e-6);
    }

    #[test]
    fn single_child_intersect_folds_to_child() {
        let a = lookup(Channel::Postcode, "1000");
        let p = Op::Intersect(vec![a.clone()]).canonicalize();
        assert_eq!(p, a);
    }

    #[test]
    fn intersect_with_universe_folds_to_other() {
        // intersect(A, universe) → A. Universe is the empty Intersect.
        let a = lookup(Channel::Street, "x");
        let universe = Op::Intersect(vec![]);
        let p = Op::Intersect(vec![a.clone(), universe]).canonicalize();
        assert_eq!(p, a);
    }

    #[test]
    fn union_with_empty_set_folds_to_other() {
        // union(A, ∅) → A. ∅ is the empty Union.
        let a = lookup(Channel::Street, "x");
        let empty = Op::Union(vec![]);
        let p = Op::Union(vec![a.clone(), empty]).canonicalize();
        assert_eq!(p, a);
    }

    #[test]
    fn intersect_dedup_collapses_equal_children() {
        // intersect(A, A) → A (via sort + dedup).
        let a = lookup(Channel::Street, "x");
        let p = Op::Intersect(vec![a.clone(), a.clone()]).canonicalize();
        assert_eq!(p, a);
    }

    #[test]
    fn union_dedup_collapses_equal_children() {
        // union(A, A) → A.
        let a = lookup(Channel::Street, "x");
        let p = Op::Union(vec![a.clone(), a.clone()]).canonicalize();
        assert_eq!(p, a);
    }

    #[test]
    fn dedup_with_hyp_collapses_overlapping_fields() {
        // Three hypotheses with different parsed-field overlap that
        // canonicalize to the same Op tree → exactly 1 entry after
        // dedup. The representative hypothesis must be the one with
        // the highest final_logprob, and the merged final_logprob is
        // the max of the three inputs.
        let mk_hyp = |conf: f32| -> ParseHypothesis {
            let mut h = ParseHypothesis::default();
            h.postcode_candidates.push(("1070".to_string(), conf));
            h.street_candidates.push(("rue wayez".to_string(), conf));
            h
        };
        let p1 = Op::Intersect(vec![
            lookup(Channel::Postcode, "1070"),
            lookup(Channel::Street, "rue wayez"),
        ]);
        let p2 = Op::Intersect(vec![
            lookup(Channel::Street, "rue wayez"),
            lookup(Channel::Postcode, "1070"),
        ]);
        let p3 = p1.clone();
        let triples = vec![
            (p1, mk_hyp(0.7), -2.5_f32),
            (p2, mk_hyp(0.95), -0.3_f32),
            (p3, mk_hyp(0.5), -3.0_f32),
        ];
        let out = dedup_canonical_with_hyp(triples);
        assert_eq!(out.len(), 1, "3 commutatively equivalent → 1 program");
        // final_logprob is the max of the three (-0.3).
        assert!((out[0].2 - (-0.3)).abs() < 1e-6);
        // Representative hypothesis is the one with logprob -0.3
        // (corresponding to confidence 0.95).
        assert!((out[0].1.postcode_candidates[0].1 - 0.95).abs() < 1e-6);
    }

    #[test]
    fn dedup_with_hyp_preserves_distinct_programs() {
        // Two hypotheses that produce different canonical programs
        // must NOT collapse — both survive.
        let h1 = ParseHypothesis {
            postcode_candidates: vec![("1070".to_string(), 1.0)],
            ..Default::default()
        };
        let h2 = ParseHypothesis {
            postcode_candidates: vec![("2000".to_string(), 1.0)],
            ..Default::default()
        };
        let p1 = Op::Lookup(LookupKey {
            channel: Channel::Postcode,
            key: "1070".to_string(),
        });
        let p2 = Op::Lookup(LookupKey {
            channel: Channel::Postcode,
            key: "2000".to_string(),
        });
        let out = dedup_canonical_with_hyp(vec![(p1, h1, -0.1), (p2, h2, -0.2)]);
        assert_eq!(out.len(), 2);
    }

    #[test]
    fn dedup_with_hyp_single_element_fast_path() {
        // Single-element input must take the fast path: canonicalize
        // and return without entering the comparison loop.
        let h = ParseHypothesis::default();
        let p = lookup(Channel::Postcode, "1070");
        let out = dedup_canonical_with_hyp(vec![(p.clone(), h, -0.5)]);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].0, p);
    }

    #[test]
    fn score_with_zero_weight_folds_to_child() {
        // score(A, ∅) → A, where ∅ is encoded as weight=0.
        let a = lookup(Channel::Street, "x");
        let p = Op::Score {
            child: Box::new(a.clone()),
            channel: Channel::HouseNumber,
            weight: 0.0,
        }
        .canonicalize();
        assert_eq!(p, a);
    }
}
