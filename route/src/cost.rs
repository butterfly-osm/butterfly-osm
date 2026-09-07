//! The search cost, held apart from the time we report (#610).
//!
//! # Why the two must not be the same array
//!
//! The contracted hierarchy's TIME array is, today, two things at once: the
//! cost the search minimises, and the duration handed back to the caller. That
//! is fine while the only thing a mode expresses is time. It stops being fine
//! the moment a mode wants a *preference* — a dislike of forest tracks, a toll
//! aversion, a comfort term. Adding such a term to the time array changes the
//! answer twice: the route improves, and the reported duration becomes a number
//! the speed provider never measured.
//!
//! The second effect is the dangerous one. The engine's level anchor is fitted
//! on exactly those reported durations, so a systematic preference baked into
//! time would be absorbed by the anchor at the next re-measurement, leaving a
//! global constant compensating a per-class preference. That is the pattern
//! #608/#609 removed, where a multiplier hid 70 689 seconds of discarded
//! measurement per boot; rebuilding it deliberately would be a poor trade.
//!
//! # The separation
//!
//! A mode has a [`CostModel`]. It answers one question: *what does the search
//! minimise?* Today, for every shipped mode, the answer is "the time", and
//! [`CostModel::TimeIsCost`] says so — the search weights are the time weights,
//! borrowed, not copied, so the two cannot drift and no byte moves.
//!
//! When a preference exists, the cost is `time + charge`, the hierarchy is
//! contracted on the COST, and the duration reported for a path is the sum of
//! the PURE times of the edges that path actually traverses. Three quantities,
//! then, where there are two today:
//!
//! | quantity | role |
//! |---|---|
//! | cost | minimised; never reported |
//! | time along the cost-optimal path | reported as the duration |
//! | length along the cost-optimal path | reported as the distance |
//!
//! All three are functions of the SAME elected middles — the cost-optimal
//! apexes — which is what makes them describe one path rather than three.
//!
//! # The invariant, and the guard that holds it
//!
//! > **The duration we report is the pure time of the path the search chose.**
//!
//! [`verify_reported_time_is_pure_time`] is that invariant, executable. It takes
//! the elected middles and the reported-time channel a customization produced,
//! independently re-expands every CCH edge down to original edges through those
//! middles, sums the PURE per-edge times, and demands the two agree everywhere.
//! It shares no code with the production derivation: production folds the
//! channel bottom-up in rank order, the checker recurses top-down from each
//! edge with a memo. A derivation that fed the cost weights into the reported
//! channel, or that paired a channel with stale middles (the #528 failure
//! class), fails it.
//!
//! The guard is written against a zero preference — the only one that exists
//! today — and is exercised with a non-zero one in
//! [`mod guard_tests`](self), so it will already be standing when #593 adds a
//! real preference term.
//!
//! # What each surface minimises, and what it reports
//!
//! Settled with #610 so that #593 implements a decision rather than taking one:
//!
//! | surface | minimises | reports |
//! |---|---|---|
//! | `/route` | cost | time along it as the duration, length along it as the distance |
//! | `/table`, `/trip`, Flight `matrix` | cost | the same two, carried — a cell stays a duration |
//! | `/isochrone` | **time** | the time-reachable area |
//!
//! The matrix is where the cost lands hardest: it has no per-cell path to
//! unpack, so all three quantities must ride through the hierarchy together,
//! and every sweep bound expressed in seconds (`max_minutes`, the seeded
//! bucket's `sweep_bound`) has to bound the CARRIED time rather than the
//! primary key it bounds today. `/route` could in principle re-derive its
//! duration by summing the unpacked edges it already walks for the geometry,
//! but its label must stay consistent with the matrix's or the two surfaces
//! disagree — which is the whole point of one graph and one hierarchy.
//!
//! **The isochrone deliberately keeps minimising time.** A budget of twenty
//! minutes is a question about time, and the honest answer to it is the set of
//! places some road reaches in twenty minutes. The alternative reading — the
//! places whose *cost-optimal* path takes under twenty minutes — is a different
//! product: it excludes a town that a fast disliked road reaches, which is
//! surprising rather than useful. It is also ruinous to compute. The reachable
//! set is bounded today by comparing the primary label against the threshold,
//! which is what makes an isochrone 5 ms; under a cost primary, `cost > T` is
//! not a valid stopping condition, because a later, dearer label elsewhere may
//! still be quick. The upward search would become effectively unbounded. A
//! preferred-path isochrone, if it is ever wanted, is a separate named surface
//! with its own latency budget — not a silent change of meaning for this one.
//!
//! # Where the preference lives, and who may change it
//!
//! **In the mode profile, not in the measured data.** A preference is a product
//! choice, of exactly the kind `models/<mode>.model.json` already holds beside
//! access rules, one-way rules and turn penalties; it belongs in git, next to
//! them, where it can be reviewed and blamed. It must never travel in the speed
//! table: that table is measurement with provenance, the level anchor is fitted
//! on it, and a choice mixed into it is invisible by construction. The
//! *compiled* channels do belong in the artifact — contracting a hierarchy is
//! not something a request can do.
//!
//! **A request may pick a preference, never invent one.** Contraction does not
//! distribute over the parameter: `min over paths of (time + α·charge)` is
//! piecewise in `α`, so two customized hierarchies cannot be interpolated into
//! a third — the winning path, and every middle that encodes it, changes. So a
//! request names one of a small closed set of pre-customized profiles, the way
//! it already names one of a small closed set of exclusion masks (#606), and a
//! seconds-per-kilometre in a query string is not on offer.

use std::borrow::Cow;
use std::sync::Arc;

use crate::formats::CchTopo;

/// What a mode's search minimises, as distinct from the time it reports.
///
/// The variants are deliberately not symmetric: `TimeIsCost` is not
/// "a preference of zero", it is *the absence of a cost channel*. The
/// difference is load-bearing — an all-zero charge array would still force a
/// second set of weights, a second flat adjacency and a second customization
/// pass into every mode, all of them holding a copy of numbers that are equal
/// by construction and can therefore only ever diverge by accident. Aliasing
/// costs nothing and cannot drift.
#[derive(Clone, Debug, Default)]
pub enum CostModel {
    /// No preference. The cost IS the time, byte for byte; the reported
    /// duration channel is the search channel.
    #[default]
    TimeIsCost,
    /// The cost is `time + charge`.
    ///
    /// `charge[original_ebg_node]` is a whole-edge charge in the same unit and
    /// the same indexing as the step-5 time weights (deciseconds, indexed by
    /// original EBG node id), so forming the search weights is one elementwise
    /// add and nothing downstream needs to know the difference.
    Preference {
        /// Per-original-EBG-node charge, deciseconds.
        charge: Arc<[u32]>,
    },
}

impl CostModel {
    /// True when the search minimises the reported time itself, so every
    /// consumer may alias the two channels.
    #[inline]
    pub fn is_time_only(&self) -> bool {
        matches!(self, Self::TimeIsCost)
    }

    /// The per-edge weights the SEARCH minimises.
    ///
    /// Borrowed — not copied — for [`CostModel::TimeIsCost`], which is what
    /// makes the zero-preference path byte-identical rather than merely equal:
    /// the customization runs on the very same slice it runs on today.
    ///
    /// An unreachable edge stays unreachable. A reachable one stays reachable:
    /// `u32::MAX` is the "no edge" sentinel everywhere in the hierarchy, so a
    /// charge large enough to reach it is clamped one below rather than
    /// allowed to alias it. A preference makes a road undesirable; only an
    /// access rule makes one impassable, and turning the first into the second
    /// behind the caller's back would be a routing decision taken by an
    /// arithmetic overflow.
    pub fn search_weights<'a>(&self, time: &'a [u32]) -> Cow<'a, [u32]> {
        match self {
            Self::TimeIsCost => Cow::Borrowed(time),
            Self::Preference { charge } => {
                assert_eq!(
                    charge.len(),
                    time.len(),
                    "cost charge array must be indexed exactly like the time weights"
                );
                Cow::Owned(
                    time.iter()
                        .zip(charge.iter())
                        .map(|(&t, &c)| {
                            if t == u32::MAX {
                                u32::MAX
                            } else {
                                t.saturating_add(c).min(u32::MAX - 1)
                            }
                        })
                        .collect(),
                )
            }
        }
    }

    /// 32 bytes identifying this cost model, to be folded into any cache key
    /// that names a customization.
    ///
    /// All-zero for [`CostModel::TimeIsCost`], so the keys of every artifact
    /// customized before #610 keep the values they have. A preference changes
    /// the elected middles and therefore every derived channel, so it MUST
    /// move the key — that is the #528 lesson, where a warm cache served a
    /// channel derived from middles the weights no longer matched.
    pub fn fingerprint(&self) -> [u8; 32] {
        match self {
            Self::TimeIsCost => [0u8; 32],
            Self::Preference { charge } => {
                use sha2::{Digest, Sha256};
                let mut h = Sha256::new();
                h.update(b"butterfly-cost-preference-v1");
                h.update((charge.len() as u64).to_le_bytes());
                for &c in charge.iter() {
                    h.update(c.to_le_bytes());
                }
                h.finalize().into()
            }
        }
    }
}

/// A place where the reported duration stopped being the provider's time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CostLeak {
    /// Which array the offending entry is in.
    pub direction: Direction,
    /// Index into that array.
    pub edge: usize,
    /// What the customization put in the reported-time channel.
    pub reported: u32,
    /// What the pure per-edge times along the elected expansion actually sum to.
    pub pure_time: u32,
}

/// Which of the two rank-aligned CCH arrays an index refers to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// The UP arrays (`up_offsets` / `up_targets`).
    Up,
    /// The DOWN arrays (`down_offsets` / `down_targets`).
    Down,
}

impl std::fmt::Display for CostLeak {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "#610 cost/time separation leak: the {:?} edge at index {} reports a \
             duration of {} but the pure per-edge times along its elected \
             expansion sum to {}. The duration we serve must be the provider's \
             time along the path the search chose — never the cost that chose \
             it, and never a time taken from different middles. A level anchor \
             fitted on these durations would silently absorb the difference.",
            self.direction, self.edge, self.reported, self.pure_time
        )
    }
}

/// The #610 invariant, executable: **the duration we report is the pure time of
/// the path the search chose.**
///
/// Independently re-derives, for every CCH edge, the sum of the PURE times of
/// the original edges its expansion traverses — walking top-down from each edge
/// through `up_middle` / `down_middle` with a memo, which is a different shape
/// from the bottom-up rank-order fold the customization uses — and compares it
/// against `reported_up` / `reported_down`.
///
/// `pure_time(u_rank, v_rank)` must return the time of the ORIGINAL edge
/// `u→v` and must not know about the cost model. Passing the cost function here
/// would make the check vacuous, which is exactly the mistake it exists to
/// catch, so callers get it from the same place the customization gets its TIME
/// weights.
///
/// Returns the first leak in a deterministic scan order (DOWN then UP, index
/// ascending), or `Ok(())`.
pub fn verify_reported_time_is_pure_time(
    topo: &CchTopo,
    up_middle: &[u32],
    down_middle: &[u32],
    reported_up: &[u32],
    reported_down: &[u32],
    pure_time: impl Fn(usize, usize) -> u32,
) -> Result<(), CostLeak> {
    let n_up = topo.up_targets.len();
    let n_down = topo.down_targets.len();
    assert_eq!(up_middle.len(), n_up, "up middles must cover every UP edge");
    assert_eq!(
        down_middle.len(),
        n_down,
        "down middles must cover every DOWN edge"
    );
    assert_eq!(
        reported_up.len(),
        n_up,
        "reported-time UP channel must cover every UP edge"
    );
    assert_eq!(
        reported_down.len(),
        n_down,
        "reported-time DOWN channel must cover every DOWN edge"
    );

    let up_offsets = topo.up_offsets.as_slice();
    let up_targets = topo.up_targets.as_slice();
    let down_offsets = topo.down_offsets.as_slice();
    let down_targets = topo.down_targets.as_slice();

    let mut memo_up = vec![u32::MAX; n_up];
    let mut memo_down = vec![u32::MAX; n_down];
    let mut done_up = vec![false; n_up];
    let mut done_down = vec![false; n_down];

    // Rank order gives every dependency before its dependents (a shortcut's
    // halves live at strictly lower ranks or earlier within the same rank), so
    // the "recursion" needs no stack — but it is driven from the edge, through
    // the ELECTED middle, which is the part that matters: a channel paired with
    // the wrong middles cannot survive it.
    for u in 0..topo.n_nodes as usize {
        let d_start = down_offsets[u] as usize;
        let d_end = down_offsets[u + 1] as usize;
        // DOWN edges of a rank must be resolved by increasing target rank: a
        // DOWN shortcut u→v via m needs u→m, and rank(m) < rank(v).
        let mut down_order: Vec<usize> = (d_start..d_end).collect();
        down_order.sort_unstable_by_key(|&i| down_targets[i]);
        for i in down_order {
            let v = down_targets[i] as usize;
            let t = if !topo.down_is_shortcut.bit(i) {
                pure_time(u, v)
            } else {
                let m = down_middle[i] as usize;
                let a = lookup(u, m, down_offsets, down_targets, &memo_down, &done_down);
                let b = lookup(m, v, up_offsets, up_targets, &memo_up, &done_up);
                a.saturating_add(b)
            };
            memo_down[i] = t;
            done_down[i] = true;
        }

        for i in up_offsets[u] as usize..up_offsets[u + 1] as usize {
            let v = up_targets[i] as usize;
            let t = if !topo.up_is_shortcut.bit(i) {
                pure_time(u, v)
            } else {
                let m = up_middle[i] as usize;
                let a = lookup(u, m, down_offsets, down_targets, &memo_down, &done_down);
                let b = lookup(m, v, up_offsets, up_targets, &memo_up, &done_up);
                a.saturating_add(b)
            };
            memo_up[i] = t;
            done_up[i] = true;
        }
    }

    for (i, (&got, &want)) in reported_down.iter().zip(memo_down.iter()).enumerate() {
        if got != want {
            return Err(CostLeak {
                direction: Direction::Down,
                edge: i,
                reported: got,
                pure_time: want,
            });
        }
    }
    for (i, (&got, &want)) in reported_up.iter().zip(memo_up.iter()).enumerate() {
        if got != want {
            return Err(CostLeak {
                direction: Direction::Up,
                edge: i,
                reported: got,
                pure_time: want,
            });
        }
    }
    Ok(())
}

/// Read the memoised value of the edge `u→v` in one direction's arrays.
///
/// An edge the elected middles point at must exist and must already be
/// resolved; a `u32::MAX` here would silently look like "unreachable" and hide
/// a broken middle, so both conditions are asserted rather than defaulted.
fn lookup(
    u: usize,
    v: usize,
    offsets: &[u64],
    targets: &[u32],
    memo: &[u32],
    done: &[bool],
) -> u32 {
    let start = offsets[u] as usize;
    let end = offsets[u + 1] as usize;
    let idx = (start..end)
        .find(|&i| targets[i] as usize == v)
        .unwrap_or_else(|| {
            panic!("elected middle names an edge {u}→{v} that is not in the topology")
        });
    assert!(
        done[idx],
        "edge {u}→{v} used before it was resolved — the elected middles do not \
         respect the contraction rank order"
    );
    memo[idx]
}

#[cfg(test)]
mod guard_tests {
    //! The #610 guard, standing before the preference it guards against.
    //!
    //! Every test here is written so that it would already have failed on the
    //! day a preference term was folded into the reported duration. The last
    //! one deliberately builds such a leak and demands the guard reject it, so
    //! the guard cannot rot into a tautology.

    use super::*;
    use crate::customization::bottom_up_with_external_middles;
    use crate::formats::{ArcCow, BitsetField, WeightArray};

    /// A 6-node CCH with two candidate apexes under one UP shortcut, plus a
    /// second shortcut NESTED on the first, so the guard has to resolve a
    /// shortcut one of whose halves is itself a shortcut — the case where a
    /// wrong middle propagates instead of staying local.
    ///
    /// ```text
    ///   ranks 0,1 : candidate apexes       ranks 2,3 : below         ranks 4,5 : top
    ///   UP   : 0→4 orig, 1→4 orig, 2→4 SHORTCUT (apex 0 or 1),
    ///          3→4 SHORTCUT (apex 2, so its UP half is the 2→4 shortcut),
    ///          4→5 orig
    ///   DOWN : 2→0 orig, 2→1 orig, 3→2 orig
    /// ```
    fn topo_6node() -> CchTopo {
        CchTopo {
            n_nodes: 6,
            n_shortcuts: 2,
            n_original_arcs: 6,
            inputs_sha: [0u8; 32],
            // UP per node: 0:[4] 1:[4] 2:[4] 3:[4] 4:[5] 5:[]
            up_offsets: ArcCow::from_vec(vec![0u64, 1, 2, 3, 4, 5, 5]),
            up_targets: ArcCow::from_vec(vec![4u32, 4, 4, 4, 5]),
            up_is_shortcut: BitsetField::from_bools(&[false, false, true, true, false]),
            // Contraction-time middles; the tests always pass external ones.
            up_middle: WeightArray::from_vec_u32(vec![u32::MAX, u32::MAX, 0, 2, u32::MAX]),
            // DOWN per node: 0:[] 1:[] 2:[0,1] 3:[2] 4:[] 5:[]
            down_offsets: ArcCow::from_vec(vec![0u64, 0, 0, 2, 3, 3, 3]),
            down_targets: ArcCow::from_vec(vec![0u32, 1, 2]),
            down_is_shortcut: BitsetField::from_bools(&[false, false, false]),
            down_middle: WeightArray::from_vec_u32(vec![u32::MAX, u32::MAX, u32::MAX]),
            rank_to_filtered: ArcCow::from_vec(vec![0u32, 1, 2, 3, 4, 5]),
        }
    }

    fn sorted_down_indices() -> Vec<Vec<usize>> {
        vec![
            Vec::new(),
            Vec::new(),
            vec![0usize, 1],
            vec![2usize],
            Vec::new(),
            Vec::new(),
        ]
    }

    /// Pure per-edge TIME of the original edges, in deciseconds.
    ///
    /// Apex 0 is the fast way up (10+30 = 40); apex 1 is slower (25+30 = 55).
    fn pure_time(u: usize, v: usize) -> u32 {
        match (u, v) {
            (0, 4) => 30,
            (1, 4) => 30,
            (4, 5) => 7,
            (2, 0) => 10,
            (2, 1) => 25,
            (3, 2) => 4,
            _ => panic!("unexpected original edge ({u},{v})"),
        }
    }

    /// A preference that charges heavily for the edge 2→0 — the fast half of
    /// the fast apex — so the cost-optimal expansion swings to apex 1 while the
    /// time-optimal one stays on apex 0. Cost via apex 0 = (10+100)+30 = 140;
    /// via apex 1 = 25+30 = 55.
    fn cost_time(u: usize, v: usize) -> u32 {
        pure_time(u, v) + if (u, v) == (2, 0) { 100 } else { 0 }
    }

    /// Middles a TIME-minimising customization elects: the 2→4 shortcut goes
    /// through apex 0 (40 < 55).
    const TIME_UP_MID: [u32; 5] = [u32::MAX, u32::MAX, 0, 2, u32::MAX];
    /// Middles a COST-minimising customization elects: 2→4 swings to apex 1.
    const COST_UP_MID: [u32; 5] = [u32::MAX, u32::MAX, 1, 2, u32::MAX];
    const DOWN_MID: [u32; 3] = [u32::MAX; 3];

    // ---------------------------------------------------------------
    // 1. With no preference, nothing moves.
    // ---------------------------------------------------------------

    #[test]
    fn without_a_preference_the_search_weights_are_the_time_weights_themselves() {
        let time = vec![7u32, 0, u32::MAX, 42, 1];
        let borrowed = CostModel::TimeIsCost.search_weights(&time);
        assert!(
            matches!(borrowed, Cow::Borrowed(_)),
            "#610: with no preference the customization must run on the very \
             same slice it runs on today — a copy is a thing that can drift"
        );
        assert_eq!(&*borrowed, &time[..]);
        assert_eq!(
            CostModel::TimeIsCost.fingerprint(),
            [0u8; 32],
            "#610: no preference must not move any cache key"
        );
        assert!(CostModel::TimeIsCost.is_time_only());
    }

    #[test]
    fn without_a_preference_the_reported_time_channel_is_the_search_channel() {
        let topo = topo_6node();
        // What the engine does today: fold the TIME weights through the
        // TIME-elected middles.
        let (search_up, search_down) = bottom_up_with_external_middles(
            &topo,
            &sorted_down_indices(),
            &TIME_UP_MID,
            &DOWN_MID,
            pure_time,
        );
        // What #610 says the reported duration is: the pure time along the
        // path the search chose. With no preference the search chose the
        // time-optimal path, so the two must be the same numbers.
        verify_reported_time_is_pure_time(
            &topo,
            &TIME_UP_MID,
            &DOWN_MID,
            &search_up,
            &search_down,
            pure_time,
        )
        .expect("zero preference: the searched channel IS the duration");
    }

    // ---------------------------------------------------------------
    // 2. With a preference, the reported duration is still the provider's.
    // ---------------------------------------------------------------

    #[test]
    fn a_preference_moves_the_path_but_never_the_reported_time_of_that_path() {
        let topo = topo_6node();
        let sdi = sorted_down_indices();

        // The search minimises COST and elects apex 1 for the 2→4 shortcut.
        let (cost_up, _cost_down) =
            bottom_up_with_external_middles(&topo, &sdi, &COST_UP_MID, &DOWN_MID, cost_time);
        assert_eq!(cost_up[2], 55, "cost via apex 1: 25 + 30");

        // The duration reported is the PURE time folded through the COST
        // middles.
        let (time_along_cost_up, time_along_cost_down) =
            bottom_up_with_external_middles(&topo, &sdi, &COST_UP_MID, &DOWN_MID, pure_time);

        verify_reported_time_is_pure_time(
            &topo,
            &COST_UP_MID,
            &DOWN_MID,
            &time_along_cost_up,
            &time_along_cost_down,
            pure_time,
        )
        .expect("the reported duration must be the pure time along the cost path");

        // The two numbers the caller could be told for the nested 3→4
        // shortcut, pinned so a regression has to change one of them:
        //   duration (what we serve) = time(3→2) + time(2→1) + time(1→4)
        //                            =    4      +    25     +    30    = 59
        //   cost     (never served)  = 4 + 25 + 30, plus nothing, because the
        //                              100 of preference sits on the 2→0 half
        //                              the search DISCARDED.
        assert_eq!(time_along_cost_up[3], 59, "4 + 25 + 30");
        // …and the same shortcut under a time-minimising search takes 44, so a
        // preference genuinely costs the traveller 15 deciseconds here. That
        // difference is a routing decision, and it is allowed to show in the
        // duration; what is NOT allowed is the 100 showing in it.
        let (time_optimal_up, _) =
            bottom_up_with_external_middles(&topo, &sdi, &TIME_UP_MID, &DOWN_MID, pure_time);
        assert_eq!(time_optimal_up[3], 44, "4 + 10 + 30");
    }

    #[test]
    fn a_preference_never_makes_the_reported_duration_shorter() {
        // A cost-optimal path can be slower than the time-optimal one — that is
        // the whole point of a preference — but it can never be FASTER. If a
        // reported duration ever drops when a preference is switched on, the
        // preference has leaked into the number.
        let topo = topo_6node();
        let sdi = sorted_down_indices();
        let (time_optimal_up, time_optimal_down) =
            bottom_up_with_external_middles(&topo, &sdi, &TIME_UP_MID, &DOWN_MID, pure_time);
        let (time_along_cost_up, time_along_cost_down) =
            bottom_up_with_external_middles(&topo, &sdi, &COST_UP_MID, &DOWN_MID, pure_time);

        for (i, (&fast, &along_cost)) in time_optimal_up
            .iter()
            .zip(time_along_cost_up.iter())
            .enumerate()
        {
            assert!(
                along_cost >= fast,
                "#610: UP edge {i} reports {along_cost} along the cost-optimal \
                 path but the time-optimal path takes {fast} — a preference \
                 must never produce a duration below the fastest one"
            );
        }
        for (i, (&fast, &along_cost)) in time_optimal_down
            .iter()
            .zip(time_along_cost_down.iter())
            .enumerate()
        {
            assert!(
                along_cost >= fast,
                "#610: DOWN edge {i} reports {along_cost} along the \
                 cost-optimal path but the time-optimal path takes {fast}"
            );
        }
        assert!(
            time_along_cost_up[2] > time_optimal_up[2],
            "the fixture must actually exercise a swung apex, or this test \
             proves nothing"
        );
    }

    // ---------------------------------------------------------------
    // 3. The guard has teeth.
    //
    // The level-anchor half of the invariant — "the duration of a FIXED path
    // does not depend on the preference" — cannot be stated against these
    // building blocks, because holding the middles fixed and folding
    // `pure_time` twice is a tautology. It is stated against the real
    // derivation instead, in `customization::cost_channel_tests`, where the
    // cost model is what varies.
    // ---------------------------------------------------------------

    #[test]
    fn the_guard_rejects_a_derivation_that_reports_the_cost() {
        // Build exactly the bug #610 exists to prevent: a customization that
        // folds the COST through the cost middles and hands the result back as
        // the duration. Every number is plausible, monotone and self-
        // consistent; only the guard can tell it is wrong.
        let topo = topo_6node();
        let sdi = sorted_down_indices();
        let (leaked_up, leaked_down) =
            bottom_up_with_external_middles(&topo, &sdi, &COST_UP_MID, &DOWN_MID, cost_time);

        let leak = verify_reported_time_is_pure_time(
            &topo,
            &COST_UP_MID,
            &DOWN_MID,
            &leaked_up,
            &leaked_down,
            pure_time,
        )
        .expect_err("#610 guard must reject a duration that carries the preference");
        assert_eq!(leak.direction, Direction::Down);
        assert_eq!(leak.edge, 0, "DOWN edge 2→0 is the charged one");
        assert_eq!(leak.reported, 110, "10 s of driving + 100 of preference");
        assert_eq!(leak.pure_time, 10);
        assert!(
            leak.to_string().contains("level anchor"),
            "the failure must say WHY it matters, not just that two numbers differ"
        );
    }

    #[test]
    fn the_guard_rejects_a_reported_channel_paired_with_stale_middles() {
        // The other way the invariant breaks (#528's failure class): the
        // numbers are pure times, but they were folded through the middles of
        // a DIFFERENT customization, so they describe a path the search no
        // longer chooses.
        let topo = topo_6node();
        let sdi = sorted_down_indices();
        let (stale_up, stale_down) =
            bottom_up_with_external_middles(&topo, &sdi, &TIME_UP_MID, &DOWN_MID, pure_time);

        let leak = verify_reported_time_is_pure_time(
            &topo,
            &COST_UP_MID, // the middles the search actually elected
            &DOWN_MID,
            &stale_up,
            &stale_down,
            pure_time,
        )
        .expect_err("#610 guard must reject a duration taken from stale middles");
        assert_eq!(leak.direction, Direction::Up);
        assert_eq!(leak.edge, 2, "the 2→4 shortcut is the one whose apex swung");
        assert_eq!(leak.reported, 40, "time via the stale apex 0: 10 + 30");
        assert_eq!(leak.pure_time, 55, "time via the elected apex 1: 25 + 30");
    }

    // ---------------------------------------------------------------
    // 5. The cost model itself.
    // ---------------------------------------------------------------

    #[test]
    fn a_preference_adds_its_charge_and_keeps_unreachable_unreachable() {
        let time = vec![10u32, 20, u32::MAX, 0];
        let model = CostModel::Preference {
            charge: Arc::from(vec![5u32, 0, 99, u32::MAX]),
        };
        let cost = model.search_weights(&time);
        assert_eq!(
            &*cost,
            &[15u32, 20, u32::MAX, u32::MAX - 1][..],
            "an unreachable edge stays unreachable; a charge big enough to reach \
             the sentinel is clamped one below it, so a preference can make an \
             edge unattractive but never impassable"
        );
        assert!(!model.is_time_only());
        assert_ne!(
            model.fingerprint(),
            [0u8; 32],
            "#610: a preference MUST move every cache key that names a \
             customization — a warm cache serving the old middles is #528"
        );
    }

    #[test]
    fn two_different_preferences_do_not_share_a_fingerprint() {
        let a = CostModel::Preference {
            charge: Arc::from(vec![1u32, 2, 3]),
        };
        let b = CostModel::Preference {
            charge: Arc::from(vec![1u32, 2, 4]),
        };
        let c = CostModel::Preference {
            charge: Arc::from(vec![1u32, 2, 3]),
        };
        assert_ne!(a.fingerprint(), b.fingerprint());
        assert_eq!(a.fingerprint(), c.fingerprint(), "and it is deterministic");
    }
}
