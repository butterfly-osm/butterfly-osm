//! RAPTOR: round-based earliest-arrival public-transport routing.
//!
//! Reference: Delling, Pajor, Werneck — "Round-Based Public Transit
//! Routing" (2012). We implement the classic, non-ULTRA variant (transfers
//! are pre-computed stop-to-stop walks; see [`super::transfers`]).
//!
//! ## Semantics
//!
//! Given:
//!   * a [`Timetable`],
//!   * a [`TransferGraph`],
//!   * a set of *sources*, each `(stop, earliest_dep_at_stop)` — multiple
//!     access stops (coming out of the origin foot-walk) can be injected at
//!     round 0 to model `multi-source` RAPTOR; and
//!   * a set of *targets*, each `(stop, target_weight_seconds)` — the
//!     foot-walk time from stop to final destination, added as an offset
//!     when comparing candidate journeys.
//!
//! Returns the best journey (minimum absolute arrival time at the
//! destination — i.e. `label[dest_stop] + target_weight`) across all
//! sources, and a reconstructable path.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, HashMap};

use super::timetable::{RouteIdx, StopIdx, Timetable};
use super::transfers::TransferGraph;

/// Sentinel for "not yet reached". We use `u32::MAX` as infinity seconds.
const INF: u32 = u32::MAX;

/// Upper bound on rounds. RAPTOR rounds = max number of transit legs (+ 1
/// for the foot prefix). Belgian networks rarely need more than 4–5.
const MAX_ROUNDS: usize = 8;

/// A single leg of a RAPTOR journey.
#[derive(Debug, Clone)]
pub enum RaptorLeg {
    /// Walk from `from_stop` to `to_stop` (stop-to-stop walking transfer).
    Walk {
        from_stop: StopIdx,
        to_stop: StopIdx,
        duration_s: u32,
    },
    /// Ride a specific trip between two stops on its route.
    Ride {
        route: RouteIdx,
        trip_in_route: u32,
        from_stop: StopIdx,
        to_stop: StopIdx,
        board_time: u32,
        alight_time: u32,
    },
}

/// A complete RAPTOR journey (on-network portion only).
///
/// Origin-side and destination-side foot walks are added by the caller.
#[derive(Debug, Clone)]
pub struct RaptorJourney {
    /// Absolute arrival time at the destination stop (seconds since midnight).
    pub arrival_time: u32,
    /// Stop at which the journey ends (one of the access-target stops).
    pub final_stop: StopIdx,
    /// Stop at which the journey began (one of the access-source stops).
    pub origin_stop: StopIdx,
    /// Ordered legs.
    pub legs: Vec<RaptorLeg>,
}

/// How a stop was reached in a particular round. We carry enough data to
/// reconstruct the journey. This is kept small: one `StopLabel` per
/// `(stop, round)` slot.
#[derive(Debug, Clone, Copy)]
struct StopLabel {
    time: u32,
    via: Via,
}

#[derive(Debug, Clone, Copy)]
enum Via {
    /// Origin injection at round 0.
    Origin,
    /// Boarded `route` at `trip_in_route`, at `from_stop` (origin of this
    /// trip segment). `board_time` is the **trip's scheduled departure at
    /// `from_stop`** — not the rider's arrival at that stop. This matters
    /// whenever the rider waits on the platform for the trip to depart:
    /// the leg's displayed board time must be the train's departure, not
    /// the rider's (earlier) arrival. See issue #107.
    Trip {
        route: RouteIdx,
        trip_in_route: u32,
        from_stop: StopIdx,
        from_round: u8,
        board_time: u32,
    },
    /// Walked from `from_stop`. Walks don't consume a RAPTOR round so no
    /// `from_round` is stored — path reconstruction looks up the `from_stop`
    /// label at the same round as the current stop.
    Walk { from_stop: StopIdx },
}

impl Default for StopLabel {
    fn default() -> Self {
        Self {
            time: INF,
            via: Via::Origin,
        }
    }
}

/// RAPTOR query parameters.
#[derive(Debug, Clone)]
pub struct RaptorQuery<'a> {
    pub sources: &'a [(StopIdx, u32)],
    pub target_weights: &'a HashMap<StopIdx, u32>,
}

/// Run a RAPTOR query and return the best journey, if any.
pub fn run_raptor(
    timetable: &Timetable,
    transfers: &TransferGraph,
    query: &RaptorQuery<'_>,
) -> Option<RaptorJourney> {
    let n_stops = timetable.n_stops();
    if n_stops == 0 || query.sources.is_empty() || query.target_weights.is_empty() {
        return None;
    }

    // labels[k][stop] = earliest arrival at `stop` in ≤ k rounds.
    let mut labels: Vec<Vec<StopLabel>> = (0..MAX_ROUNDS + 1)
        .map(|_| vec![StopLabel::default(); n_stops])
        .collect();

    // best_arrival_at_stop[stop] = earliest across all rounds (for pruning).
    let mut best_at_stop: Vec<u32> = vec![INF; n_stops];

    // best_absolute_arrival = best (label[stop] + target_weight[stop]) so far.
    let mut best_absolute = INF;
    let mut best_final_stop: Option<StopIdx> = None;
    let mut best_round: usize = 0;

    // Inject sources at round 0.
    for &(stop, dep) in query.sources {
        let idx = stop as usize;
        if idx >= n_stops {
            continue;
        }
        if dep < labels[0][idx].time {
            labels[0][idx].time = dep;
            labels[0][idx].via = Via::Origin;
            best_at_stop[idx] = best_at_stop[idx].min(dep);
        }
        // Immediately seed the absolute best with origin stops that are also
        // target stops (0-transit journey).
        if let Some(&tw) = query.target_weights.get(&stop) {
            let total = dep.saturating_add(tw);
            if total < best_absolute {
                best_absolute = total;
                best_final_stop = Some(stop);
                best_round = 0;
            }
        }
    }

    // marked[stop] = was `stop` improved in the last round?
    let mut marked: Vec<bool> = vec![false; n_stops];
    for &(stop, _dep) in query.sources {
        if (stop as usize) < n_stops {
            marked[stop as usize] = true;
        }
    }

    // Pre-round-1: walking transfers from the origin-injected stops.
    // These are "free" walks that happen before boarding any trip; they
    // stay in round 0 because they don't consume a transfer.
    apply_transfers(&mut labels[0], transfers, &mut marked, &mut best_at_stop);
    // Re-check absolute best after initial walks.
    for (stop_idx, label) in labels[0].iter().enumerate() {
        if label.time == INF {
            continue;
        }
        if let Some(&tw) = query.target_weights.get(&(stop_idx as u32)) {
            let total = label.time.saturating_add(tw);
            if total < best_absolute {
                best_absolute = total;
                best_final_stop = Some(stop_idx as u32);
                best_round = 0;
            }
        }
    }

    for k in 1..=MAX_ROUNDS {
        // Initialise round k with round k-1 labels (monotone).
        let (prev_slice, curr_slice) = labels.split_at_mut(k);
        let prev = &prev_slice[k - 1];
        let curr: &mut Vec<StopLabel> = &mut curr_slice[0];
        for (i, label) in prev.iter().enumerate() {
            if label.time < curr[i].time {
                curr[i] = *label;
            }
        }

        // Collect queue: one (route, earliest stop index) entry per
        // route touching a marked stop. If a route is touched by several
        // marked stops, keep the earliest stop along the route.
        let mut queue: BTreeMap<RouteIdx, u32> = BTreeMap::new();
        for (s_idx, m) in marked.iter().enumerate() {
            if !*m {
                continue;
            }
            for &(route, pos_in_route) in timetable.routes_for_stop(s_idx as StopIdx) {
                queue
                    .entry(route)
                    .and_modify(|p| {
                        if pos_in_route < *p {
                            *p = pos_in_route;
                        }
                    })
                    .or_insert(pos_in_route);
            }
        }

        // Reset marks — they'll be repopulated for round k by trip/transfer scans.
        for m in marked.iter_mut() {
            *m = false;
        }

        let mut any_improvement = false;

        for (route, start_pos) in queue {
            let route_stops = timetable.route_stops_slice(route);

            // Walk the route from start_pos to the end, maintaining a
            // "current trip": once we board, subsequent stops use the same
            // trip — unless an earlier arrival lets us take an even earlier
            // trip when we reach a new "hop-on" stop later.
            let mut current_trip: Option<u32> = None;
            let mut board_stop: StopIdx = 0;
            // Scheduled departure of `current_trip` at `board_stop`.
            // Captured when we (re-)board so reconstruction doesn't have to
            // guess from stale labels (issue #107).
            let mut board_dep: u32 = 0;

            for (pos, &stop) in route_stops.iter().enumerate().skip(start_pos as usize) {
                // If we have a running trip, try to alight at `stop`.
                if let Some(trip) = current_trip {
                    let st = timetable.stop_time(route, trip, pos as u32);
                    let arr = st.arrival;
                    // Target pruning: an arrival that already exceeds the best
                    // absolute known journey can't improve anything.
                    let min_tw = query.target_weights.get(&stop).copied().unwrap_or(0);
                    if arr.saturating_add(min_tw) < best_absolute
                        && arr < curr[stop as usize].time
                        && arr < best_at_stop[stop as usize]
                    {
                        curr[stop as usize] = StopLabel {
                            time: arr,
                            via: Via::Trip {
                                route,
                                trip_in_route: trip,
                                from_stop: board_stop,
                                from_round: (k - 1) as u8,
                                board_time: board_dep,
                            },
                        };
                        best_at_stop[stop as usize] = arr;
                        marked[stop as usize] = true;
                        any_improvement = true;

                        if let Some(&tw) = query.target_weights.get(&stop) {
                            let total = arr.saturating_add(tw);
                            if total < best_absolute {
                                best_absolute = total;
                                best_final_stop = Some(stop);
                                best_round = k;
                            }
                        }
                    }
                }

                // Can we (re-)board an earlier trip at this stop? Use the
                // label at `stop` from the *previous* round (`prev`), which
                // represents "already reachable by k-1 rounds and then
                // boarding a new trip".
                let prev_arr = prev[stop as usize].time;
                if prev_arr == INF {
                    continue;
                }
                // Board stop must have a departure; check if a trip
                // departs at or after prev_arr.
                if let Some(candidate_trip) = timetable.earliest_trip(route, pos as u32, prev_arr) {
                    let should_switch = match current_trip {
                        None => true,
                        Some(t) => candidate_trip < t,
                    };
                    if should_switch {
                        current_trip = Some(candidate_trip);
                        board_stop = stop;
                        // Capture the trip's scheduled departure at this
                        // boarding stop — the actual board_time. See #107.
                        let st = timetable.stop_time(route, candidate_trip, pos as u32);
                        board_dep = st.departure;
                    }
                }
            }
        }

        if !any_improvement {
            break;
        }

        // Apply foot transfers from newly-marked stops.
        apply_transfers(curr, transfers, &mut marked, &mut best_at_stop);

        // Re-check absolute best using transfer-relaxed labels.
        for (stop_idx, label) in curr.iter().enumerate() {
            if label.time == INF {
                continue;
            }
            if let Some(&tw) = query.target_weights.get(&(stop_idx as u32)) {
                let total = label.time.saturating_add(tw);
                if total < best_absolute {
                    best_absolute = total;
                    best_final_stop = Some(stop_idx as u32);
                    best_round = k;
                }
            }
        }
    }

    let final_stop = best_final_stop?;
    if best_absolute == INF {
        return None;
    }
    // Reconstruct the path using the labels from `best_round`.
    let legs = reconstruct(&labels, best_round, final_stop);
    let origin_stop = legs
        .iter()
        .map(|l| match l {
            RaptorLeg::Walk { from_stop, .. } => *from_stop,
            RaptorLeg::Ride { from_stop, .. } => *from_stop,
        })
        .next()
        .unwrap_or(final_stop);

    Some(RaptorJourney {
        arrival_time: labels[best_round][final_stop as usize].time,
        final_stop,
        origin_stop,
        legs,
    })
}

/// Relax walking transfers to closure, starting from every stop currently
/// marked as "just improved" in this round.
///
/// This is a **bounded multi-source Dijkstra** over the precomputed walking
/// transfer graph. On entry, `marked[s] = true` iff stop `s` was just
/// improved by this round's route-scan phase (the "frontier"). On exit,
/// `marked[s] = true` for every stop whose label was improved by any chain
/// of walking transfers starting from the frontier, and `labels[s]` holds
/// the new arrival time reached via that chain.
///
/// ## Why closure is mandatory (issue #106)
///
/// The ULTRA triangle dominance restriction in `transfers::ultra_restrict_transfers`
/// is only sound when RAPTOR is willing to traverse **chained walking
/// transfers within a single round**. The previous single-hop relaxation
/// failed the safety argument: ULTRA could drop a direct edge `u -> v`
/// because `u -> w + w -> v <= u -> v`, and then RAPTOR would fail to
/// reach `v` within the round because it only performed the first hop.
/// Closure restores the invariant: if `u -> w -> v` is geometrically
/// feasible, RAPTOR will walk it, so ULTRA's triangle rule is now safe to
/// prune `u -> v`.
///
/// ## Cost
///
/// Standard Dijkstra complexity over a sparse graph: `O((n_frontier + E) log n)`
/// where `E` is the number of transfer edges traversed. On Belgium (64k
/// stops, ~480k ULTRA-restricted transfer edges), this is microseconds —
/// the transfer graph is sparse enough that the closure cost is dominated
/// by the per-round route-scan in practice.
///
/// ## Reconstruction note
///
/// Each relaxation records `Via::Walk { from_stop: u }`, i.e. the immediate
/// predecessor. A 3-hop closure chain `u -> v -> w` reconstructs as three
/// `Walk` legs when path extraction walks back through `Via::Walk`
/// pointers. Collapsing consecutive walk legs into one is a response-layer
/// cosmetic, not a correctness concern.
fn apply_transfers(
    labels: &mut [StopLabel],
    transfers: &TransferGraph,
    marked: &mut [bool],
    best_at_stop: &mut [u32],
) {
    let mut heap: BinaryHeap<Reverse<(u32, StopIdx)>> = BinaryHeap::new();
    for (s_idx, m) in marked.iter().enumerate() {
        if *m && labels[s_idx].time != INF {
            heap.push(Reverse((labels[s_idx].time, s_idx as StopIdx)));
        }
    }

    while let Some(Reverse((d, u))) = heap.pop() {
        // Stale PQ entry: a later relaxation improved this stop, skip.
        if d > labels[u as usize].time {
            continue;
        }
        for (v, walk_s) in transfers.neighbours(u) {
            if v == u {
                continue;
            }
            let new_time = d.saturating_add(walk_s);
            if new_time < labels[v as usize].time && new_time < best_at_stop[v as usize] {
                labels[v as usize] = StopLabel {
                    time: new_time,
                    via: Via::Walk { from_stop: u },
                };
                best_at_stop[v as usize] = new_time;
                marked[v as usize] = true;
                heap.push(Reverse((new_time, v)));
            }
        }
    }
}

fn reconstruct(
    labels: &[Vec<StopLabel>],
    final_round: usize,
    final_stop: StopIdx,
) -> Vec<RaptorLeg> {
    let mut legs: Vec<RaptorLeg> = Vec::new();
    let mut cur_stop = final_stop;
    let mut cur_round = final_round;

    loop {
        let label = labels[cur_round][cur_stop as usize];
        match label.via {
            Via::Origin => break,
            Via::Trip {
                route,
                trip_in_route,
                from_stop,
                from_round,
                board_time,
            } => {
                let alight_time = label.time;
                legs.push(RaptorLeg::Ride {
                    route,
                    trip_in_route,
                    from_stop,
                    to_stop: cur_stop,
                    board_time,
                    alight_time,
                });
                cur_stop = from_stop;
                cur_round = from_round as usize;
            }
            Via::Walk { from_stop } => {
                let dur = label
                    .time
                    .saturating_sub(labels[cur_round][from_stop as usize].time);
                legs.push(RaptorLeg::Walk {
                    from_stop,
                    to_stop: cur_stop,
                    duration_s: dur,
                });
                cur_stop = from_stop;
                // Walks are within the same round — cur_round unchanged.
            }
        }
        if cur_round == 0 && matches!(labels[cur_round][cur_stop as usize].via, Via::Origin) {
            break;
        }
    }
    legs.reverse();
    legs
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transit::timetable::{StopTime, TimetableBuilder};
    use crate::transit::transfers::TransferGraph;

    fn stime(arr: u32, dep: u32) -> StopTime {
        StopTime {
            arrival: arr,
            departure: dep,
        }
    }

    #[test]
    fn direct_single_route() {
        // Graph:
        //   A --[Route R, trip T, dep 600, arr 1200]--> B
        //   No transfers needed.
        //
        // Rider arrives at A at time 500 and waits 100 seconds for the
        // train, which departs at 600. The reconstructed leg's board_time
        // must be the train's departure (600), NOT the rider's arrival
        // (500). This is the #107 fix: before it landed, the reconstruct
        // code dug board_time out of the old label at `from_stop` and
        // returned the rider's arrival, hiding the platform wait.
        let mut b = TimetableBuilder::new();
        let a = b.add_stop("A", "A", 0.0, 0.0, None);
        let bb = b.add_stop("B", "B", 0.1, 0.0, None);
        b.add_trip(
            "T1",
            "R",
            "Route R",
            "To B",
            vec![a, bb],
            vec![stime(600, 600), stime(1200, 1200)],
        );
        let tt = b.build().unwrap();
        let transfers = TransferGraph::empty(tt.n_stops());

        let mut targets = HashMap::new();
        targets.insert(bb, 0u32);
        let sources = vec![(a, 500u32)];
        let q = RaptorQuery {
            sources: &sources,
            target_weights: &targets,
        };
        let journey = run_raptor(&tt, &transfers, &q).expect("journey should exist");
        assert_eq!(journey.arrival_time, 1200);
        assert_eq!(journey.legs.len(), 1);
        if let RaptorLeg::Ride {
            from_stop,
            to_stop,
            board_time,
            alight_time,
            ..
        } = &journey.legs[0]
        {
            assert_eq!(*from_stop, a);
            assert_eq!(*to_stop, bb);
            assert_eq!(
                *board_time, 600,
                "board_time must be the trip's scheduled departure, not the rider's arrival at the boarding stop (issue #107)"
            );
            assert_eq!(*alight_time, 1200);
        } else {
            panic!("expected Ride leg");
        }
    }

    #[test]
    fn one_transfer_journey() {
        // A→B on route R1 (dep 600, arr 700), transfer walking 60s
        // from B to C, C→D on R2 (dep 800, arr 900).
        let mut b = TimetableBuilder::new();
        let a = b.add_stop("A", "A", 0.0, 0.0, None);
        let bb = b.add_stop("B", "B", 0.1, 0.0, None);
        let c = b.add_stop("C", "C", 0.1001, 0.0, None);
        let d = b.add_stop("D", "D", 0.2, 0.0, None);

        b.add_trip(
            "T1",
            "R1",
            "R1",
            "To B",
            vec![a, bb],
            vec![stime(600, 600), stime(700, 700)],
        );
        b.add_trip(
            "T2",
            "R2",
            "R2",
            "To D",
            vec![c, d],
            vec![stime(800, 800), stime(900, 900)],
        );

        let tt = b.build().unwrap();
        // Walking graph: B <-> C at 60s.
        let mut g = TransferGraph::empty(tt.n_stops());
        g.add_edge(bb, c, 60);
        g.add_edge(c, bb, 60);
        g.finalise();

        let mut targets = HashMap::new();
        targets.insert(d, 0u32);
        let sources = vec![(a, 500u32)];
        let q = RaptorQuery {
            sources: &sources,
            target_weights: &targets,
        };
        let journey = run_raptor(&tt, &g, &q).expect("journey should exist");
        assert_eq!(journey.arrival_time, 900);
        // Must have at least one Walk leg between B and C.
        let has_walk = journey
            .legs
            .iter()
            .any(|l| matches!(l, RaptorLeg::Walk { from_stop, to_stop, .. } if *from_stop == bb && *to_stop == c));
        assert!(has_walk, "expected walking transfer");
    }

    #[test]
    fn chained_walking_transfers_within_a_round() {
        // Regression test for issue #106 (ULTRA transfer restriction
        // safety). Topology:
        //
        //   Route R1: A --> B         (dep 600, arr 700)
        //   Transfer graph:   B --> U (60 s),  U --> V (60 s)
        //                     (No direct B --> V edge.)
        //   Route R2: V --> D         (dep 900, arr 1000)
        //
        // The only way to board R2 is to walk B -> U -> V. The direct
        // B->V edge is absent (as if ULTRA pruned it). With the old
        // single-hop `apply_transfers`, V is never reached within round 1
        // and the journey is lost. With the new bounded-Dijkstra closure
        // in `apply_transfers`, V is reached via the U hop and RAPTOR
        // finds the journey.
        let mut b = TimetableBuilder::new();
        let a = b.add_stop("A", "A", 0.0, 0.0, None);
        let bb = b.add_stop("B", "B", 0.1, 0.0, None);
        let u = b.add_stop("U", "U", 0.11, 0.0, None);
        let v = b.add_stop("V", "V", 0.12, 0.0, None);
        let d = b.add_stop("D", "D", 0.2, 0.0, None);

        b.add_trip(
            "T1",
            "R1",
            "R1",
            "To B",
            vec![a, bb],
            vec![stime(600, 600), stime(700, 700)],
        );
        b.add_trip(
            "T2",
            "R2",
            "R2",
            "To D",
            vec![v, d],
            vec![stime(900, 900), stime(1000, 1000)],
        );
        let tt = b.build().unwrap();

        // Transfer graph: B -> U (60s), U -> V (60s). Deliberately NO
        // direct B -> V edge — this is the ULTRA-restricted state where
        // the single-hop relaxation would fail.
        let mut g = TransferGraph::empty(tt.n_stops());
        g.add_edge(bb, u, 60);
        g.add_edge(u, bb, 60);
        g.add_edge(u, v, 60);
        g.add_edge(v, u, 60);
        g.finalise();

        let mut targets = HashMap::new();
        targets.insert(d, 0u32);
        let sources = vec![(a, 0u32)];
        let q = RaptorQuery {
            sources: &sources,
            target_weights: &targets,
        };
        let journey = run_raptor(&tt, &g, &q)
            .expect("closure must find the B->U->V chained walk; single-hop relaxation would fail");
        assert_eq!(journey.arrival_time, 1000);
        // The journey must contain at least one walking transfer leg.
        assert!(
            journey
                .legs
                .iter()
                .any(|l| matches!(l, RaptorLeg::Walk { .. })),
            "expected at least one walking transfer leg"
        );
        // The journey must contain both R1 and R2 rides.
        let ride_routes: Vec<RouteIdx> = journey
            .legs
            .iter()
            .filter_map(|l| match l {
                RaptorLeg::Ride { route, .. } => Some(*route),
                _ => None,
            })
            .collect();
        assert_eq!(
            ride_routes.len(),
            2,
            "expected exactly two ride legs (R1 and R2)"
        );
    }

    #[test]
    fn no_journey_when_target_unreachable() {
        let mut b = TimetableBuilder::new();
        let a = b.add_stop("A", "A", 0.0, 0.0, None);
        let bb = b.add_stop("B", "B", 1.0, 0.0, None);
        // No trips and no transfers.
        b.add_trip("T", "R", "R", "h", vec![a], vec![stime(0, 0)]);
        let tt = b.build().unwrap();
        let transfers = TransferGraph::empty(tt.n_stops());
        let mut targets = HashMap::new();
        targets.insert(bb, 0u32);
        let sources = vec![(a, 0u32)];
        let q = RaptorQuery {
            sources: &sources,
            target_weights: &targets,
        };
        assert!(run_raptor(&tt, &transfers, &q).is_none());
    }
}
