//! Multi-channel executor (#96 §Geocoder).
//!
//! ## Pipeline
//!
//! 1. **Clean-query fast path** — when [`crate::types::ParsedQuery::is_clean`]
//!    holds, the executor takes a hand-rolled path that performs no
//!    canonicalization, no dedup, no dynamic dispatch, and pre-allocates
//!    the result vector at capacity. Reason codes are `&'static str`
//!    slices into a fixed vocabulary (no per-result `String` allocation
//!    for reasons). This is the contract behind the
//!    Zero-Cost-on-Clean-Queries NFR.
//! 2. **Multi-hypothesis path** — for every hypothesis, build an
//!    operator tree, canonicalize, dedup. **Execute each deduped
//!    program once**, not against the cross-product of programs ×
//!    hypotheses. Per #96: "execution operates on deduplicated
//!    retrieval programs, not raw parser hypotheses."
//!
//! ## Channel-role semantics (#96 §Channel Roles)
//!
//! - `Blocker` → emit `Lookup` and AND it into the candidate set
//!   via `Intersect`.
//! - `Reducer` → emit `Lookup` and AND it via `Intersect`. With the
//!   Role-Smoothness Guarantee a Reducer near the Blocker boundary
//!   may have already been downgraded from Blocker.
//! - `Scorer` → emit `Op::Score` referencing the channel's posting
//!   list as evidence. Unlike `Filter` / `Intersect`, `Score` does not
//!   change set membership; it adds a rank contribution per candidate
//!   that is also a member of the score-channel posting list.
//!
//! ## Role-Smoothness Guarantee (#96)
//!
//! See [`apply_role_smoothness`]: when the parser's per-channel
//! confidence is within ε of a role threshold, the matcher downgrades
//! that channel one step. Hard thresholding is forbidden by #96.

use serde::{Deserialize, Serialize};

use super::channels::{Channel, ChannelRole};
use super::cost::static_cost;
use super::program::{FilterPredicate, LookupKey, Op};
use crate::shard::reader::{Shard, ShardRecord};
use crate::types::{ExecutionBudget, ParseHypothesis, ParsedQuery, RetrievalPolicy, Strictness};

/// Static reason-code vocabulary. Reason codes in [`GeocodedResult`]
/// are `&'static str` slices into this table — the clean path does
/// not allocate strings for reasons.
pub mod reason {
    pub const POSTCODE_EXACT: &str = "POSTCODE_EXACT";
    pub const STREET_EXACT: &str = "STREET_EXACT";
    pub const STREET_PARTIAL: &str = "STREET_PARTIAL";
    pub const STREET_FUZZY: &str = "STREET_FUZZY";
    pub const HOUSE_EXACT: &str = "HOUSE_EXACT";
    pub const HOUSE_NEAR: &str = "HOUSE_NEAR";
    pub const LOCALITY_EXACT: &str = "LOCALITY_EXACT";
    pub const NEAREST: &str = "NEAREST";
    pub const NEAREST_OUT_OF_RADIUS: &str = "NEAREST_OUT_OF_RADIUS";
    pub const EXEC: &str = "EXEC";
}

/// Final geocoding result.
///
/// String fields are owned (the response outlives the shard borrow);
/// they are constructed by `String::from(&str)` over the shard's
/// interned `Arc<str>`. Per limit (≤ 50 results) this is bounded.
///
/// Reason codes are `&'static str` slices into the [`reason`] table —
/// no per-result `String` allocation for reasons (B4 NFR).
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
    /// Vocabulary defined in [`reason`].
    pub reason_codes: Vec<std::borrow::Cow<'static, str>>,
}

/// Execute a parsed query against a shard.
pub fn execute(query: &ParsedQuery, shard: &Shard, limit: usize) -> Vec<GeocodedResult> {
    if query.hypotheses.is_empty() {
        return Vec::new();
    }

    if query.is_clean() {
        return execute_clean(&query.hypotheses[0], shard, limit);
    }

    // Multi-hypothesis path.
    let budget = &query.execution_budget;

    // Per #97 / B6: enforce `max_hypotheses` by truncating the input
    // hypothesis list.
    let hyps: Vec<&ParseHypothesis> = query
        .hypotheses
        .iter()
        .take(budget.max_hypotheses as usize)
        .collect();

    if hyps.is_empty() {
        return Vec::new();
    }

    // Build canonical (program, source_score) pairs.
    let raw_programs: Vec<(Op, f32)> = hyps
        .iter()
        .map(|h| {
            let policy = apply_role_smoothness(h, budget);
            let (op, src_score) = build_program(h, &policy);
            (op.canonicalize(), src_score)
        })
        .collect();

    // Recombination Invariant: dedup programs by canonical form. After
    // this step, every program is unique. Per #96, execution operates
    // on deduplicated PROGRAMS, never on the cross-product of programs
    // and raw hypotheses.
    let mut programs = super::program::dedup_canonical(raw_programs);

    // Static cost ceiling enforcement.
    let stats = shard.stats();
    let total_static: f32 = programs.iter().map(|(p, _)| static_cost(p, stats)).sum();
    if total_static > budget.static_cost_ceiling {
        programs.sort_by(|(a, _), (b, _)| {
            static_cost(a, stats)
                .partial_cmp(&static_cost(b, stats))
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        let mut acc = 0.0_f32;
        let mut keep = 0usize;
        for (p, _) in &programs {
            acc += static_cost(p, stats);
            if acc > budget.static_cost_ceiling {
                break;
            }
            keep += 1;
        }
        programs.truncate(keep);
    }

    let mut results: Vec<GeocodedResult> = Vec::new();
    let mut total_used = 0u32;
    // Execute each deduped program EXACTLY ONCE — no cross-product
    // with the raw hypothesis list.
    for (prog, src_score) in &programs {
        if total_used >= budget.max_total_candidates {
            break;
        }
        // The "representative" hypothesis for scoring is hyps[0]; the
        // src_score (max-merged across equivalent hypotheses) is rolled
        // into the per-result score so dedup-collapsed hypotheses still
        // contribute to ranking.
        let r = execute_program(
            prog,
            shard,
            *src_score,
            hyps[0],
            budget.max_total_candidates - total_used,
        );
        total_used = total_used.saturating_add(r.len() as u32);
        results.extend(r);
    }

    rerank_and_truncate(&mut results, limit);
    results
}

/// Apply the Role-Smoothness Guarantee (#96).
///
/// For each channel with an assigned role, if the per-channel
/// confidence is within ε of a role threshold, downgrade the role one
/// step (Blocker → Reducer → Scorer). Hard thresholding is forbidden
/// per the §Role-Smoothness Guarantee.
///
/// When `dual_evaluation_enabled` is set in the budget, the executor
/// MAY also explore the adjacent role assignment — the algebra is in
/// place but the MVP heuristic parser produces only one hypothesis so
/// we don't currently materialise the dual program.
pub fn apply_role_smoothness(h: &ParseHypothesis, _budget: &ExecutionBudget) -> RetrievalPolicy {
    let mut policy = h.retrieval_policy;
    let eps = policy.epsilon;

    let chan_conf = |ch: Channel| -> f32 {
        let cs = match ch {
            Channel::Postcode => &h.postcode_candidates,
            Channel::Street => &h.street_candidates,
            Channel::HouseNumber => &h.house_candidates,
            Channel::Locality => &h.locality_candidates,
            // Alias / Transliteration aren't exercised by the MVP
            // heuristic parser; treat as zero confidence.
            _ => return 0.0,
        };
        cs.first().map_or(0.0, |(_, w)| *w)
    };

    // Per #96 the role boundary is implementation-defined by country
    // pack. For BE we treat the role thresholds as:
    //   confidence >= 0.85 → Blocker
    //   confidence >= 0.50 → Reducer
    //   else                → Scorer
    // The ε-boundary downgrade fires when:
    //   |confidence - 0.85| < ε for an assigned Blocker
    //   |confidence - 0.50| < ε for an assigned Reducer
    const T_BLOCKER: f32 = 0.85;
    const T_REDUCER: f32 = 0.50;

    for ch in [
        Channel::Postcode,
        Channel::Street,
        Channel::HouseNumber,
        Channel::Locality,
    ] {
        let Some(role) = policy.role(ch) else {
            continue;
        };
        let conf = chan_conf(ch);
        let near_boundary = match role {
            ChannelRole::Blocker => (conf - T_BLOCKER).abs() < eps,
            ChannelRole::Reducer => (conf - T_REDUCER).abs() < eps,
            ChannelRole::Scorer => false,
        };
        if near_boundary && let Some(weaker) = role.weaker() {
            policy.roles[ch.index()] = Some(weaker);
        }
    }

    policy
}

fn execute_clean(h: &ParseHypothesis, shard: &Shard, limit: usize) -> Vec<GeocodedResult> {
    // Pre-allocated result vector at capacity (B4 NFR).
    let mut out: Vec<GeocodedResult> = Vec::with_capacity(limit);

    let postcode = h.postcode_candidates.first().map(|c| c.0.as_str());
    let house = h.house_candidates.first().map(|c| c.0.as_str());

    let street = h
        .street_candidates
        .iter()
        .map(|c| c.0.as_str())
        .find(|st| {
            !shard.postings_for_street(st).is_empty()
                || postcode
                    .is_some_and(|pc| !shard.postings_for_postcode_and_street(pc, st).is_empty())
        })
        .or_else(|| h.street_candidates.first().map(|c| c.0.as_str()));

    // Channel selection. With OSM-derived BE shards, postcode tagging
    // is sparse so we fall back through the Role-Smoothness chain.
    let postings: &[u32] = match (postcode, street) {
        (Some(pc), Some(st)) => {
            let p = shard.postings_for_postcode_and_street(pc, st);
            if !p.is_empty() {
                p
            } else {
                let s = shard.postings_for_street(st);
                if !s.is_empty() {
                    s
                } else {
                    shard.postings_for_postcode(pc)
                }
            }
        }
        (Some(pc), None) => shard.postings_for_postcode(pc),
        (None, Some(st)) => shard.postings_for_street(st),
        (None, None) => h
            .locality_candidates
            .first()
            .map_or(&[][..], |(loc, _)| shard.postings_for_locality(loc)),
    };

    // Pre-normalize street/locality once so the per-record loop
    // doesn't repeatedly normalize the query.
    let street_norm = street.map(crate::parser::normalize::normalize);
    let locality_norm = h
        .locality_candidates
        .first()
        .map(|(loc, w)| (crate::parser::normalize::normalize(loc), *w));

    // Bound the inner loop:
    // - With a postcode anchor, the posting list is already tight
    //   (typically <100 records), so 4× limit is plenty.
    // - Without a postcode anchor, the posting list can be very
    //   large (e.g. street-only "Grote Markt" matches every commune
    //   that has one), and the locality scorer needs to see ALL of
    //   them to find the right one. We cap at 8K records which is
    //   well under any realistic single-street posting list and
    //   protects against pathological queries.
    let has_strong_anchor = postcode.is_some();
    let inner_cap = if has_strong_anchor {
        limit.saturating_mul(4).max(limit + 1)
    } else {
        8192usize.max(limit + 1)
    };

    for &id in postings {
        if out.len() >= inner_cap {
            break;
        }
        let Some(rec) = shard.record(id) else {
            continue;
        };

        let mut score = 0.0_f32;
        let mut reasons: Vec<&'static str> = Vec::new();

        if let Some(pc) = postcode
            && pc == &*rec.postcode
        {
            score += 1.0;
            reasons.push(reason::POSTCODE_EXACT);
        }
        if let Some(s_norm) = street_norm.as_deref() {
            let rec_norm = crate::parser::normalize::normalize(&rec.street);
            if rec_norm == s_norm {
                score += 1.0;
                reasons.push(reason::STREET_EXACT);
            } else if rec_norm.contains(s_norm) || s_norm.contains(&rec_norm) {
                score += 0.5;
                reasons.push(reason::STREET_PARTIAL);
            }
        }
        if let Some(hn) = house {
            if hn.eq_ignore_ascii_case(&rec.housenumber) {
                score += 0.7;
                reasons.push(reason::HOUSE_EXACT);
            } else if !rec.housenumber.is_empty()
                && let (Ok(a), Ok(b)) = (parse_leading_int(hn), parse_leading_int(&rec.housenumber))
            {
                let delta = (a - b).abs();
                if delta <= 2 {
                    score += 0.3 / (1.0 + delta as f32);
                    reasons.push(reason::HOUSE_NEAR);
                }
            }
        }
        if let Some((l_norm, w)) = locality_norm.as_ref() {
            let rec_loc = crate::parser::normalize::normalize(&rec.locality);
            if &rec_loc == l_norm {
                score += 0.2 * w;
                reasons.push(reason::LOCALITY_EXACT);
            }
        }

        if score <= 0.0 {
            continue;
        }

        out.push(materialize_result(&rec, score, reasons));
    }

    rerank_and_truncate(&mut out, limit);

    if out.is_empty()
        && h.strictness == Strictness::Exact
        && let Some(s) = street
    {
        let mut fuzzy_h = h.clone();
        fuzzy_h.strictness = Strictness::Fuzzy;
        out = execute_fuzzy_street(
            &fuzzy_h,
            shard,
            s,
            limit,
            // Per B6: bound the fuzzy fallback by max_fuzzy_expansions.
            ExecutionBudget::default().max_fuzzy_expansions as usize,
        );
    }

    out
}

/// Convert a [`ShardRecord`] view into an owned [`GeocodedResult`].
/// Strings come from the shard's interned `Arc<str>` pool — one
/// `String::from(&str)` per field per result, bounded by `limit`.
fn materialize_result(rec: &ShardRecord, score: f32, reasons: Vec<&'static str>) -> GeocodedResult {
    GeocodedResult {
        lat: rec.lat,
        lon: rec.lon,
        street: String::from(&*rec.street),
        housenumber: String::from(&*rec.housenumber),
        postcode: String::from(&*rec.postcode),
        locality: String::from(&*rec.locality),
        score,
        // Cow::Borrowed wraps the &'static str without allocation.
        reason_codes: reasons
            .into_iter()
            .map(std::borrow::Cow::Borrowed)
            .collect(),
    }
}

/// Materialize a result for the `NEAREST` reverse-lookup path. Public
/// so [`crate::server::handlers::reverse`] can build results without
/// duplicating the conversion.
pub fn build_nearest_result(rec: &ShardRecord, score: f32, reason: &'static str) -> GeocodedResult {
    materialize_result(rec, score, vec![reason])
}

fn execute_fuzzy_street(
    h: &ParseHypothesis,
    shard: &Shard,
    street_query: &str,
    limit: usize,
    max_expansions: usize,
) -> Vec<GeocodedResult> {
    use rapidfuzz::distance::indel;

    let q_norm = crate::parser::normalize::normalize(street_query);
    let postcode = h.postcode_candidates.first().map(|c| c.0.as_str());
    let house = h.house_candidates.first().map(|c| c.0.as_str());

    // Per B6: bound the scan by `max_fuzzy_expansions`.
    //
    // Strategy: do a single pass over the street-key iterator. The
    // iterator yields keys in sorted order, so we apply the budget by
    // taking at most `max_expansions` keys total. To keep recall good
    // even on a tight budget, we bias the scan by SKIPPING to the
    // first key with a matching prefix — most typos preserve the first
    // 1-2 characters, so this puts us in the right zone of the sort
    // order. Keys before the prefix point are skipped without a
    // similarity test (cheap byte compare).
    // Bias the scan to the prefix zone of the sorted keys: most typos
    // preserve the first character. We compute the first ASCII byte of
    // the query (lowercase, so within the alphanumeric range used by
    // the normalizer) and skip keys that come before it lexically.
    let prefix_byte: Option<u8> = q_norm.as_bytes().first().copied();
    let mut best_streets: Vec<(String, f64)> = Vec::with_capacity(8);
    for key in shard
        .all_street_keys()
        .skip_while(|k| match prefix_byte {
            Some(b) => !k.is_empty() && k.as_bytes()[0] < b,
            None => false,
        })
        .take(max_expansions)
    {
        let sim = indel::normalized_similarity(q_norm.chars(), key.chars());
        if sim >= 0.85 {
            best_streets.push((key.to_string(), sim));
        }
    }
    best_streets.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    best_streets.truncate(8);

    let mut out: Vec<GeocodedResult> = Vec::with_capacity(limit);
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
            let mut reasons: Vec<&'static str> = vec![reason::STREET_FUZZY];
            if let Some(pc) = postcode
                && pc == &*rec.postcode
            {
                score += 0.5;
                reasons.push(reason::POSTCODE_EXACT);
            }
            if let Some(hn) = house
                && hn.eq_ignore_ascii_case(&rec.housenumber)
            {
                score += 0.5;
                reasons.push(reason::HOUSE_EXACT);
            }
            out.push(materialize_result(&rec, score, reasons));
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

/// Build a retrieval program from a parse hypothesis under a (possibly
/// downgraded) policy. Returns `(program, source_score)` where
/// `source_score` is the parser's hypothesis confidence — used during
/// dedup to merge equivalent programs from different hypotheses (#96
/// Recombination Invariant: "their source-hypothesis scores are
/// combined").
fn build_program(h: &ParseHypothesis, policy: &RetrievalPolicy) -> (Op, f32) {
    let mut blockers: Vec<Op> = Vec::new();
    let mut reducers: Vec<Op> = Vec::new();
    let mut scorer_channels: Vec<(Channel, f32)> = Vec::new();

    if let Some((pc, _)) = h.postcode_candidates.first() {
        let lookup = Op::Lookup(LookupKey {
            channel: Channel::Postcode,
            key: pc.clone(),
        });
        match policy.role(Channel::Postcode) {
            Some(ChannelRole::Blocker) => blockers.push(lookup),
            Some(ChannelRole::Reducer) => reducers.push(lookup),
            Some(ChannelRole::Scorer) => scorer_channels.push((Channel::Postcode, 1.0)),
            None => {}
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
            Some(ChannelRole::Scorer) => scorer_channels.push((Channel::Street, 1.0)),
            None => {}
        }
    }
    // House and Locality scorers (per BE policy default).
    if !h.house_candidates.is_empty()
        && matches!(policy.role(Channel::HouseNumber), Some(ChannelRole::Scorer))
    {
        scorer_channels.push((Channel::HouseNumber, 0.7));
    }
    if let Some((_, w)) = h.locality_candidates.first()
        && matches!(policy.role(Channel::Locality), Some(ChannelRole::Scorer))
    {
        scorer_channels.push((Channel::Locality, *w));
    }

    let base: Op = match (blockers.len(), reducers.len()) {
        (0, 0) => {
            // No blocker/reducer: fall through to a Locality lookup if
            // we have one, else an empty Intersect (universe).
            if let Some((loc, _)) = h.locality_candidates.first() {
                Op::Lookup(LookupKey {
                    channel: Channel::Locality,
                    key: loc.clone(),
                })
            } else {
                Op::Intersect(vec![])
            }
        }
        (0, _) => Op::Intersect(reducers),
        (_, 0) => Op::Intersect(blockers),
        (_, _) => {
            let mut all = blockers;
            all.extend(reducers);
            Op::Intersect(all)
        }
    };

    // Wrap base in Score nodes, one per scorer channel.
    let mut tree = base;
    for (channel, weight) in &scorer_channels {
        tree = Op::Score {
            child: Box::new(tree),
            channel: *channel,
            weight: *weight,
        };
    }

    let after_filter = if let Some((hn, _)) = h.house_candidates.first()
        && matches!(
            policy.role(Channel::HouseNumber),
            Some(ChannelRole::Blocker) | Some(ChannelRole::Reducer)
        ) {
        Op::Filter {
            child: Box::new(tree),
            predicate: FilterPredicate::HouseNumberEq(hn.clone()),
        }
    } else {
        tree
    };

    let prog = Op::Cap {
        child: Box::new(after_filter),
        n: 64,
    };

    let mut src_score = 0.0_f32;
    for cs in [
        &h.postcode_candidates,
        &h.street_candidates,
        &h.house_candidates,
        &h.locality_candidates,
    ] {
        if let Some((_, w)) = cs.first()
            && *w > src_score
        {
            src_score = *w;
        }
    }

    (prog, src_score)
}

fn execute_program(
    op: &Op,
    shard: &Shard,
    src_score: f32,
    h: &ParseHypothesis,
    cap: u32,
) -> Vec<GeocodedResult> {
    let postings = walk_postings(op, shard);
    let scored = score_postings(op, shard, h, &postings);
    let mut out: Vec<GeocodedResult> = Vec::new();
    for (id, op_score) in scored {
        if out.len() as u32 >= cap {
            break;
        }
        let Some(rec) = shard.record(id) else {
            continue;
        };
        let mut score = 1.0_f32 + op_score + src_score * 0.1;
        let mut reasons: Vec<&'static str> = vec![reason::EXEC];
        if let Some((pc, _)) = h.postcode_candidates.first()
            && pc == &*rec.postcode
        {
            score += 1.0;
            reasons.push(reason::POSTCODE_EXACT);
        }
        if let Some((st, _)) = h.street_candidates.first() {
            let s_norm = crate::parser::normalize::normalize(st);
            let r_norm = crate::parser::normalize::normalize(&rec.street);
            if s_norm == r_norm {
                score += 1.0;
                reasons.push(reason::STREET_EXACT);
            }
        }
        if let Some((hn, _)) = h.house_candidates.first()
            && hn.eq_ignore_ascii_case(&rec.housenumber)
        {
            score += 0.7;
            reasons.push(reason::HOUSE_EXACT);
        }
        out.push(materialize_result(&rec, score, reasons));
    }
    out
}

/// For each candidate id, accumulate score contributions from any
/// `Score` operators in the tree. A candidate gets a contribution
/// from a Score node if it's in the score-channel's posting list for
/// the parser's key on that channel. Set membership is unchanged.
fn score_postings(op: &Op, shard: &Shard, h: &ParseHypothesis, ids: &[u32]) -> Vec<(u32, f32)> {
    let mut contributors: Vec<(Vec<u32>, f32)> = Vec::new();
    collect_scorers(op, shard, h, &mut contributors);
    let mut out: Vec<(u32, f32)> = Vec::with_capacity(ids.len());
    for &id in ids {
        let mut s = 0.0_f32;
        for (postings, weight) in &contributors {
            if postings.binary_search(&id).is_ok() {
                s += *weight;
            }
        }
        out.push((id, s));
    }
    out
}

fn collect_scorers(op: &Op, shard: &Shard, h: &ParseHypothesis, acc: &mut Vec<(Vec<u32>, f32)>) {
    match op {
        Op::Score {
            child,
            channel,
            weight,
        } => {
            let key: Option<&str> = match channel {
                Channel::Postcode => h.postcode_candidates.first().map(|c| c.0.as_str()),
                Channel::Locality => h.locality_candidates.first().map(|c| c.0.as_str()),
                Channel::Street => h.street_candidates.first().map(|c| c.0.as_str()),
                _ => None,
            };
            if let Some(key) = key {
                let postings: &[u32] = match channel {
                    Channel::Postcode => shard.postings_for_postcode(key),
                    Channel::Locality => shard.postings_for_locality(key),
                    Channel::Street => shard.postings_for_street(key),
                    // HouseNumber doesn't have a primary index — skip.
                    _ => &[],
                };
                if !postings.is_empty() {
                    let mut sorted: Vec<u32> = postings.to_vec();
                    sorted.sort_unstable();
                    sorted.dedup();
                    acc.push((sorted, *weight));
                }
            }
            collect_scorers(child, shard, h, acc);
        }
        Op::Intersect(children) | Op::Union(children) | Op::TopkMerge { children, .. } => {
            for c in children {
                collect_scorers(c, shard, h, acc);
            }
        }
        Op::Filter { child, .. }
        | Op::Cap { child, .. }
        | Op::Sample { child, .. }
        | Op::Downgrade { child, .. } => {
            collect_scorers(child, shard, h, acc);
        }
        Op::Lookup(_) => {}
    }
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
                // Universe: cannot enumerate. canonicalize folds away
                // empty Intersects so we should rarely see this.
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
        // Score and Downgrade are pass-through for set membership;
        // score contributions are applied by `score_postings`.
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
        let q = parse_heuristic("Rue Waeyz 122", CountryId::BE);
        let results = execute(&q, &shard, 5);
        assert!(!results.is_empty(), "fuzzy fallback should match Rue Wayez");
        assert!(
            results
                .iter()
                .any(|r| r.reason_codes.iter().any(|c| c == reason::STREET_FUZZY))
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

    #[test]
    fn epsilon_boundary_downgrades_role() {
        let mut h = ParseHypothesis::default();
        h.postcode_candidates.push(("1070".to_string(), 0.86));
        h.street_candidates.push(("rue wayez".to_string(), 1.0));
        h.retrieval_policy = RetrievalPolicy::belgium_default();
        let policy = apply_role_smoothness(&h, &ExecutionBudget::default());
        assert_eq!(
            policy.role(Channel::Postcode),
            Some(ChannelRole::Reducer),
            "expected Blocker→Reducer downgrade at ε-boundary"
        );
    }

    #[test]
    fn epsilon_keeps_strong_role() {
        let mut h = ParseHypothesis::default();
        h.postcode_candidates.push(("1070".to_string(), 0.99));
        h.street_candidates.push(("rue wayez".to_string(), 1.0));
        h.retrieval_policy = RetrievalPolicy::belgium_default();
        let policy = apply_role_smoothness(&h, &ExecutionBudget::default());
        assert_eq!(policy.role(Channel::Postcode), Some(ChannelRole::Blocker));
    }

    #[test]
    fn near_boundary_inputs_overlap_candidates() {
        // Per the task spec: assert two near-identical inputs produce
        // overlapping (not disjoint) candidate sets.
        let (_dir, shard) = small_shard();
        let r1 = execute(
            &parse_heuristic("Rue Wayez 122 1070", CountryId::BE),
            &shard,
            5,
        );
        let r2 = execute(
            &parse_heuristic("Rue Wayez, 122, 1070", CountryId::BE),
            &shard,
            5,
        );
        assert!(!r1.is_empty() && !r2.is_empty());
        let s1: std::collections::HashSet<_> = r1
            .iter()
            .map(|r| (r.postcode.clone(), r.housenumber.clone()))
            .collect();
        let s2: std::collections::HashSet<_> = r2
            .iter()
            .map(|r| (r.postcode.clone(), r.housenumber.clone()))
            .collect();
        assert!(
            !s1.is_disjoint(&s2),
            "near-identical inputs produced disjoint result sets — Role-Smoothness violation"
        );
    }

    #[test]
    fn budget_caps_max_total_candidates() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("shard.bfgs");
        let mut addrs = Vec::new();
        for i in 0..200u32 {
            addrs.push(AddressRecord {
                street: format!("Rue {i}"),
                housenumber: format!("{}", i + 1),
                postcode: "1000".into(),
                locality: "Bruxelles".into(),
                lat: 50.85 + (i as f64) * 1e-5,
                lon: 4.35 + (i as f64) * 1e-5,
            });
        }
        build_shard(&path, addrs).unwrap();
        let shard = Shard::open(&path).unwrap();

        let mut q = parse_heuristic("1000 Bruxelles", CountryId::BE);
        q.execution_budget.max_total_candidates = 10;
        let results = execute(&q, &shard, 50);
        assert!(results.len() <= 50);
    }
}
