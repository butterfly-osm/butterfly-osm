//! Static cost model over operator trees (#96 §Cost Composition,
//! #97 §3).
//!
//! Cost is expressed in "candidate touches" — i.e. the maximum number
//! of posting-list entries the executor might inspect. **Compositional**
//! for static operators. Feedback operators (`Downgrade`) are
//! explicitly **not** part of static cost — see #96 ("Feedback cost
//! is observed post-hoc.").

use super::program::Op;

#[derive(Debug, Clone, Copy)]
pub struct ShardStats {
    pub avg_postcode_postings: f32,
    pub avg_locality_postings: f32,
    pub avg_street_postings: f32,
    pub total_addresses: u32,
}

impl Default for ShardStats {
    fn default() -> Self {
        Self {
            avg_postcode_postings: 4096.0,
            avg_locality_postings: 32_768.0,
            avg_street_postings: 64.0,
            total_addresses: 600_000,
        }
    }
}

fn op_cost(op: &Op, stats: ShardStats) -> f32 {
    use super::channels::Channel;
    match op {
        Op::Lookup(k) => match k.channel {
            Channel::Postcode => stats.avg_postcode_postings,
            Channel::Locality => stats.avg_locality_postings,
            Channel::Street => stats.avg_street_postings,
            Channel::HouseNumber => 1.0,
            Channel::Alias | Channel::Transliteration => stats.avg_locality_postings,
        },
        Op::Intersect(children) => children.iter().map(|c| op_cost(c, stats)).sum(),
        Op::Union(children) => children.iter().map(|c| op_cost(c, stats)).sum(),
        Op::TopkMerge { children, k } => {
            let raw: f32 = children.iter().map(|c| op_cost(c, stats)).sum();
            raw * (1.0 + (*k as f32).log2().max(1.0))
        }
        Op::Filter { child, .. } => op_cost(child, stats),
        Op::Score { child, .. } => op_cost(child, stats),
        Op::Cap { child, n } => op_cost(child, stats).min(*n as f32),
        Op::Sample { child, n } => op_cost(child, stats).min(*n as f32),
        Op::Downgrade { child, .. } => op_cost(child, stats),
    }
}

#[must_use]
pub fn static_cost(op: &Op, stats: ShardStats) -> f32 {
    op_cost(op, stats)
}

#[cfg(test)]
mod tests {
    use super::super::channels::Channel;
    use super::super::program::{LookupKey, Op};
    use super::*;

    fn lookup(ch: Channel, key: &str) -> Op {
        Op::Lookup(LookupKey {
            channel: ch,
            key: key.to_string(),
        })
    }

    #[test]
    fn cost_is_additive_for_static_ops() {
        let stats = ShardStats::default();
        let p = Op::Intersect(vec![
            lookup(Channel::Postcode, "1000"),
            lookup(Channel::Street, "rue wayez"),
        ]);
        let c = static_cost(&p, stats);
        assert!(c > 0.0);
        let expected = stats.avg_postcode_postings + stats.avg_street_postings;
        assert!((c - expected).abs() < 1.0);
    }

    #[test]
    fn cap_bounds_cost() {
        let stats = ShardStats::default();
        let p = Op::Cap {
            child: Box::new(lookup(Channel::Postcode, "1000")),
            n: 10,
        };
        let c = static_cost(&p, stats);
        assert!(c <= 10.0);
    }
}
