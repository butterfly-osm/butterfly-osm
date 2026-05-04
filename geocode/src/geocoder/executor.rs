//! Multi-channel executor (#96 §Geocoder).
//!
//! ## Pipeline
//!
//! 1. **Clean-query fast path** — when [`crate::types::ParsedQuery::is_clean`]
//!    holds, the executor takes a hand-rolled path that performs no
//!    canonicalization, no dedup, no dynamic dispatch. This is the
//!    contract behind the Zero-Cost-on-Clean-Queries NFR.
//! 2. **Multi-hypothesis path** — for every hypothesis, build an
//!    operator tree, canonicalize, dedup, then walk it via
//!    `lookup → intersect → cap → score`. The MVP only ever produces
//!    one hypothesis per query, but the code path exists for #98.

use serde::{Deserialize, Serialize};

use super::channels::{Channel, ChannelRole};
use super::cost::static_cost;
use super::program::{FilterPredicate, LookupKey, Op};
use crate::shard::reader::Shard;
use crate::types::{ParseHypothesis, ParsedQuery, RetrievalPolicy, Strictness};

/// Final geocoding result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeocodedResult {
    pub lat: f64,
    pub lon: f64,
    pub street: String,
    pub housenumber: String,
    pub postcode: String,
    pub locality: String,
    pub score: f32,
    /// Why this candidate scored as it did. Machine-readable.
    /// Vocabulary: `POSTCODE_EXACT`, `STREET_EXACT`, `STREET_PARTIAL`,
    /// `STREET_FUZZY`, `HOUSE_EXACT`, `HOUSE_NEAR`, `LOCALITY_EXACT`,
    /// `NEAREST`, `NEAREST_OUT_OF_RADIUS`, `EXEC`.
    pub reason_codes: Vec<String>,
}

/// Execute a parsed query against a shard.
pub fn execute(query: &ParsedQuery, shard: &Shard, limit: usize) -> Vec<GeocodedResult> {
    if query.hypotheses.is_empty() {
        return Vec::new();
    }

    if query.is_clean() {
        return execute_clean(&query.hypotheses[0], shard, limit);
    }

    // Multi-hypothesis path. Build, canonicalize, dedup, execute.
    let mut programs: Vec<Op> = query
        .hypotheses
        .iter()
        .map(|h| build_program(h).canonicalize())
        .collect();
    programs = super::program::dedup_canonical(programs);

    let stats = shard.stats();
    let total_static: f32 = programs.iter().map(|p| static_cost(p, stats)).sum();
    if total_static > query.execution_budget.static_cost_ceiling {
        programs.sort_by(|a, b| {
            static_cost(a, stats)
                .partial_cmp(&static_cost(b, stats))
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        let mut acc = 0.0_f32;
        let mut keep = 0usize;
        for p in &programs {
            acc += static_cost(p, stats);
            if acc > query.execution_budget.static_cost_ceiling {
                break;
            }
            keep += 1;
        }
        programs.truncate(keep);
    }

    let mut results: Vec<GeocodedResult> = Vec::new();
    let mut total_used = 0u32;
    for prog in &programs {
        if total_used >= query.execution_budget.max_total_candidates {
            break;
        }
        for h in &query.hypotheses {
            let r = execute_program(prog, h, shard);
            total_used = total_used.saturating_add(r.len() as u32);
            results.extend(r);
            if total_used >= query.execution_budget.max_total_candidates {
                break;
            }
        }
    }

    rerank_and_truncate(&mut results, limit);
    results
}

fn execute_clean(h: &ParseHypothesis, shard: &Shard, limit: usize) -> Vec<GeocodedResult> {
    let mut out: Vec<GeocodedResult> = Vec::new();

    let postcode = h.postcode_candidates.first().map(|c| c.0.as_str());
    let street = h.street_candidates.first().map(|c| c.0.as_str());
    let house = h.house_candidates.first().map(|c| c.0.as_str());

    let postings: &[u32] = match (postcode, street) {
        (Some(pc), Some(st)) => {
            let p = shard.postings_for_postcode_and_street(pc, st);
            if !p.is_empty() {
                p
            } else {
                shard.postings_for_postcode(pc)
            }
        }
        (Some(pc), None) => shard.postings_for_postcode(pc),
        (None, Some(st)) => shard.postings_for_street(st),
        (None, None) => h
            .locality_candidates
            .first()
            .map_or(&[][..], |(loc, _)| shard.postings_for_locality(loc)),
    };

    let intersect_with_street = postcode.is_some()
        && street.is_some()
        && shard
            .postings_for_postcode_and_street(
                postcode.expect("Some"),
                street.expect("Some"),
            )
            .is_empty();

    let street_norm = street.map(crate::parser::normalize::normalize);
    let house_norm = house;

    for &id in postings {
        let Some(rec) = shard.record(id) else {
            continue;
        };

        if intersect_with_street
            && let Some(ref s) = street_norm
            && &crate::parser::normalize::normalize(&rec.street) != s
        {
            continue;
        }

        let mut score = 0.0_f32;
        let mut reasons: Vec<String> = Vec::new();

        if let Some(pc) = postcode
            && pc == rec.postcode.as_ref()
        {
            score += 1.0;
            reasons.push("POSTCODE_EXACT".to_string());
        }
        if let Some(ref s_norm) = street_norm {
            let rec_norm = crate::parser::normalize::normalize(&rec.street);
            if &rec_norm == s_norm {
                score += 1.0;
                reasons.push("STREET_EXACT".to_string());
            } else if rec_norm.contains(s_norm) || s_norm.contains(&rec_norm) {
                score += 0.5;
                reasons.push("STREET_PARTIAL".to_string());
            }
        }
        if let Some(hn) = house_norm {
            if hn.eq_ignore_ascii_case(rec.housenumber.as_ref()) {
                score += 0.7;
                reasons.push("HOUSE_EXACT".to_string());
            } else if !rec.housenumber.is_empty()
                && let (Ok(a), Ok(b)) = (
                    parse_leading_int(hn),
                    parse_leading_int(rec.housenumber.as_ref()),
                )
            {
                let delta = (a - b).abs();
                if delta <= 2 {
                    score += 0.3 / (1.0 + delta as f32);
                    reasons.push("HOUSE_NEAR".to_string());
                }
            }
        }
        if let Some((loc, w)) = h.locality_candidates.first() {
            let l_norm = crate::parser::normalize::normalize(loc);
            let rec_loc = crate::parser::normalize::normalize(&rec.locality);
            if rec_loc == l_norm {
                score += 0.2 * w;
                reasons.push("LOCALITY_EXACT".to_string());
            }
        }

        if score <= 0.0 {
            continue;
        }

        out.push(GeocodedResult {
            lat: rec.lat,
            lon: rec.lon,
            street: rec.street.to_string(),
            housenumber: rec.housenumber.to_string(),
            postcode: rec.postcode.to_string(),
            locality: rec.locality.to_string(),
            score,
            reason_codes: reasons,
        });
    }

    rerank_and_truncate(&mut out, limit);

    if out.is_empty()
        && h.strictness == Strictness::Exact
        && let Some(s) = street
    {
        let mut fuzzy_h = h.clone();
        fuzzy_h.strictness = Strictness::Fuzzy;
        out = execute_fuzzy_street(&fuzzy_h, shard, s, limit);
    }

    out
}

fn execute_fuzzy_street(
    h: &ParseHypothesis,
    shard: &Shard,
    street_query: &str,
    limit: usize,
) -> Vec<GeocodedResult> {
    use rapidfuzz::distance::indel;

    let q_norm = crate::parser::normalize::normalize(street_query);
    let postcode = h.postcode_candidates.first().map(|c| c.0.as_str());
    let house = h.house_candidates.first().map(|c| c.0.as_str());

    let mut best_streets: Vec<(String, f64)> = Vec::new();
    for key in shard.all_street_keys() {
        let sim = indel::normalized_similarity(q_norm.chars(), key.chars());
        if sim >= 0.85 {
            best_streets.push((key.to_string(), sim));
        }
    }
    best_streets.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    best_streets.truncate(8);

    let mut out: Vec<GeocodedResult> = Vec::new();
    for (sk, sim) in &best_streets {
        let postings: &[u32] = if let Some(pc) = postcode {
            shard.postings_for_postcode_and_street(pc, sk)
        } else {
            shard.postings_for_street(sk)
        };
        for &id in postings {
            let Some(rec) = shard.record(id) else {
                continue;
            };
            let mut score = *sim as f32;
            let mut reasons = vec!["STREET_FUZZY".to_string()];
            if let Some(pc) = postcode
                && pc == rec.postcode.as_ref()
            {
                score += 0.5;
                reasons.push("POSTCODE_EXACT".to_string());
            }
            if let Some(hn) = house
                && hn.eq_ignore_ascii_case(rec.housenumber.as_ref())
            {
                score += 0.5;
                reasons.push("HOUSE_EXACT".to_string());
            }
            out.push(GeocodedResult {
                lat: rec.lat,
                lon: rec.lon,
                street: rec.street.to_string(),
                housenumber: rec.housenumber.to_string(),
                postcode: rec.postcode.to_string(),
                locality: rec.locality.to_string(),
                score,
                reason_codes: reasons,
            });
        }
    }

    rerank_and_truncate(&mut out, limit);
    out
}

fn parse_leading_int(s: &str) -> Result<i64, std::num::ParseIntError> {
    let digits: String = s.chars().take_while(|c| c.is_ascii_digit()).collect();
    if digits.is_empty() {
        "".parse::<i64>()
    } else {
        digits.parse::<i64>()
    }
}

fn rerank_and_truncate(results: &mut Vec<GeocodedResult>, limit: usize) {
    results.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    results.dedup_by(|a, b| {
        let dlat = (a.lat - b.lat).abs() < 1e-7;
        let dlon = (a.lon - b.lon).abs() < 1e-7;
        dlat && dlon && a.housenumber == b.housenumber
    });
    if results.len() > limit {
        results.truncate(limit);
    }
}

fn build_program(h: &ParseHypothesis) -> Op {
    let policy: RetrievalPolicy = h.retrieval_policy;

    let mut blockers: Vec<Op> = Vec::new();
    let mut reducers: Vec<Op> = Vec::new();

    if let Some((pc, _)) = h.postcode_candidates.first() {
        let lookup = Op::Lookup(LookupKey {
            channel: Channel::Postcode,
            key: pc.clone(),
        });
        match policy.role(Channel::Postcode) {
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
        match policy.role(Channel::Street) {
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
        (0, _) => Op::Intersect(reducers),
        (_, 0) => Op::Intersect(blockers),
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

fn execute_program(op: &Op, h: &ParseHypothesis, shard: &Shard) -> Vec<GeocodedResult> {
    let postings = walk_postings(op, shard);
    let mut out: Vec<GeocodedResult> = Vec::new();
    for id in postings {
        let Some(rec) = shard.record(id) else {
            continue;
        };
        let mut score = 1.0_f32;
        let mut reasons: Vec<String> = vec!["EXEC".to_string()];
        if let Some((pc, _)) = h.postcode_candidates.first()
            && pc == rec.postcode.as_ref()
        {
            score += 1.0;
            reasons.push("POSTCODE_EXACT".to_string());
        }
        if let Some((st, _)) = h.street_candidates.first() {
            let s_norm = crate::parser::normalize::normalize(st);
            let r_norm = crate::parser::normalize::normalize(&rec.street);
            if s_norm == r_norm {
                score += 1.0;
                reasons.push("STREET_EXACT".to_string());
            }
        }
        if let Some((hn, _)) = h.house_candidates.first()
            && hn.eq_ignore_ascii_case(rec.housenumber.as_ref())
        {
            score += 0.7;
            reasons.push("HOUSE_EXACT".to_string());
        }
        out.push(GeocodedResult {
            lat: rec.lat,
            lon: rec.lon,
            street: rec.street.to_string(),
            housenumber: rec.housenumber.to_string(),
            postcode: rec.postcode.to_string(),
            locality: rec.locality.to_string(),
            score,
            reason_codes: reasons,
        });
    }
    out
}

fn walk_postings(op: &Op, shard: &Shard) -> Vec<u32> {
    match op {
        Op::Lookup(k) => {
            let v = match k.channel {
                Channel::Postcode => shard.postings_for_postcode(&k.key),
                Channel::Locality => shard.postings_for_locality(&k.key),
                Channel::Street => shard.postings_for_street(&k.key),
                _ => &[][..],
            };
            v.to_vec()
        }
        Op::Intersect(children) => {
            if children.is_empty() {
                return Vec::new();
            }
            let mut iter = children.iter();
            let mut acc = walk_postings(iter.next().expect("non-empty"), shard);
            acc.sort_unstable();
            acc.dedup();
            for c in iter {
                let mut next = walk_postings(c, shard);
                next.sort_unstable();
                next.dedup();
                let mut out = Vec::with_capacity(acc.len().min(next.len()));
                let (mut i, mut j) = (0, 0);
                while i < acc.len() && j < next.len() {
                    match acc[i].cmp(&next[j]) {
                        std::cmp::Ordering::Less => i += 1,
                        std::cmp::Ordering::Greater => j += 1,
                        std::cmp::Ordering::Equal => {
                            out.push(acc[i]);
                            i += 1;
                            j += 1;
                        }
                    }
                }
                acc = out;
            }
            acc
        }
        Op::Union(children) => {
            let mut acc: Vec<u32> = Vec::new();
            for c in children {
                acc.extend(walk_postings(c, shard));
            }
            acc.sort_unstable();
            acc.dedup();
            acc
        }
        Op::TopkMerge { children, k } => {
            let mut acc: Vec<u32> = Vec::new();
            for c in children {
                acc.extend(walk_postings(c, shard));
            }
            acc.sort_unstable();
            acc.dedup();
            acc.truncate(*k as usize);
            acc
        }
        Op::Filter { child, predicate } => {
            let parent = walk_postings(child, shard);
            match predicate {
                FilterPredicate::HouseNumberEq(hn) => parent
                    .into_iter()
                    .filter(|id| {
                        shard
                            .record(*id)
                            .map(|r| r.housenumber.eq_ignore_ascii_case(hn))
                            .unwrap_or(false)
                    })
                    .collect(),
            }
        }
        Op::Score { child, .. } => walk_postings(child, shard),
        Op::Cap { child, n } => {
            let mut v = walk_postings(child, shard);
            v.truncate(*n as usize);
            v
        }
        Op::Sample { child, n } => {
            let v = walk_postings(child, shard);
            let start = v.len().saturating_sub(*n as usize);
            v[start..].to_vec()
        }
        Op::Downgrade { child, .. } => walk_postings(child, shard),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::heuristic::parse_heuristic;
    use crate::routing::CountryId;
    use crate::shard::AddressRecord;
    use crate::shard::builder::build_shard;

    fn small_shard() -> (tempfile::TempDir, Shard) {
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
            },
            AddressRecord {
                street: "Rue Wayez".into(),
                housenumber: "124".into(),
                postcode: "1070".into(),
                locality: "Anderlecht".into(),
                lat: 50.834,
                lon: 4.315,
            },
            AddressRecord {
                street: "Grote Markt".into(),
                housenumber: "1".into(),
                postcode: "2000".into(),
                locality: "Antwerpen".into(),
                lat: 51.221,
                lon: 4.401,
            },
        ];
        build_shard(&path, addrs).unwrap();
        let s = Shard::open(&path).unwrap();
        (dir, s)
    }

    #[test]
    fn forward_clean_query_hits() {
        let (_dir, shard) = small_shard();
        let q = parse_heuristic("Rue Wayez 122 1070 Anderlecht", CountryId::BE);
        let results = execute(&q, &shard, 5);
        assert!(!results.is_empty(), "expected at least one hit");
        let top = &results[0];
        assert_eq!(top.postcode, "1070");
        assert_eq!(top.housenumber, "122");
    }

    #[test]
    fn forward_postcode_only_returns_locality() {
        let (_dir, shard) = small_shard();
        let q = parse_heuristic("1070", CountryId::BE);
        let results = execute(&q, &shard, 5);
        assert!(!results.is_empty());
        for r in &results {
            assert_eq!(r.postcode, "1070");
        }
    }

    #[test]
    fn forward_fuzzy_street_falls_back() {
        let (_dir, shard) = small_shard();
        let q = parse_heuristic("Rue Waeyz 122 1070", CountryId::BE);
        let results = execute(&q, &shard, 5);
        assert!(!results.is_empty(), "fuzzy fallback should match Rue Wayez");
        assert!(
            results
                .iter()
                .any(|r| r.reason_codes.iter().any(|c| c == "STREET_FUZZY"))
        );
    }

    #[test]
    fn empty_query_returns_empty() {
        let (_dir, shard) = small_shard();
        let q = parse_heuristic("", CountryId::BE);
        let results = execute(&q, &shard, 5);
        assert!(results.is_empty());
    }

    #[test]
    fn clean_query_path_is_taken() {
        let (_dir, shard) = small_shard();
        let q = parse_heuristic("Rue Wayez 122 1070", CountryId::BE);
        assert!(q.is_clean());
        let results = execute(&q, &shard, 3);
        assert!(!results.is_empty());
        assert!(results.len() <= 3);
    }
}
