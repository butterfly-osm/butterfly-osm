//! Retrieval-program operators (#96 §Retrieval Operators).
//!
//! ## Canonicalization (#96 Recombination Invariant)
//!
//! [`Op::canonicalize`] applies:
//!
//! - Stable ordering of commutative operator operands (`Intersect`
//!   and `Union` sort their children by canonical form)
//! - Identity folding (`intersect(A, universe) → A`, `union(A, ∅) →
//!   A`)
//! - Redundancy collapse (`cap(cap(A, n), m) → cap(A, min(n, m))`)
//!
//! ## Zero-Cost-on-Clean-Queries (#96 NFR)
//!
//! The executor's single-hypothesis fast path *skips* canonicalize
//! entirely; this method is here for the multi-hypothesis path that
//! #98 will exercise. On a single-element list, [`dedup_canonical`]
//! returns immediately.

use std::cmp::Ordering;

use super::channels::{Channel, ChannelRole};

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
            Op::Intersect(children) => canonicalize_commutative(children, Op::Intersect),
            Op::Union(children) => canonicalize_commutative(children, Op::Union),
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
            } => Op::Score {
                child: Box::new(child.canonicalize()),
                channel,
                weight,
            },
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

fn canonicalize_commutative(children: Vec<Op>, ctor: fn(Vec<Op>) -> Op) -> Op {
    let mut children: Vec<Op> = children.into_iter().map(Op::canonicalize).collect();
    if children.len() == 1 {
        return children.pop().expect("len == 1");
    }
    children.sort_by(cmp_op);
    ctor(children)
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

/// Dedup a list of programs by their canonical form.
///
/// On a single-element list this is a single canonicalize-and-return
/// — no allocation, no comparison loop. That keeps the multi-hypothesis
/// path cheap when the parser happens to emit one hypothesis (which
/// is the MVP heuristic parser's only mode).
///
/// Equality is structural via [`PartialEq`]. The O(N²) sweep is fine
/// because hypothesis counts are tiny (≤ 5 per #97 budget tier).
pub fn dedup_canonical(programs: Vec<Op>) -> Vec<Op> {
    let mut out: Vec<Op> = Vec::with_capacity(programs.len());
    for p in programs {
        let canon = p.canonicalize();
        if !out.iter().any(|existing| existing == &canon) {
            out.push(canon);
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
        let out = dedup_canonical(vec![p1, p2]);
        assert_eq!(out.len(), 1);
    }

    #[test]
    fn single_child_intersect_folds_to_child() {
        let a = lookup(Channel::Postcode, "1000");
        let p = Op::Intersect(vec![a.clone()]).canonicalize();
        assert_eq!(p, a);
    }
}
