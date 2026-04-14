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

use std::cell::RefCell;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};

use super::timetable::{RouteIdx, StopIdx, Timetable};
use super::transfers::TransferGraph;

/// Sentinel for "not yet reached". We use `u32::MAX` as infinity seconds.
const INF: u32 = u32::MAX;

/// Upper bound on rounds. RAPTOR rounds = max number of transit legs (+ 1
/// for the foot prefix). Belgian networks rarely need more than 4–5.
const MAX_ROUNDS: usize = 8;

/// Sentinel used in the flat route queue (`queue_start_pos[r]`) to mean
/// "route r is not in the queue this round". We pick a value that can
/// never appear as a real route position.
const QUEUE_EMPTY: u32 = u32::MAX;

/// Thread-local scratch state reused across RAPTOR queries.
///
/// Issue #104: the previous implementation allocated 9 full label
/// arrays, one `best_at_stop` array, one `marked` bitmap, and a fresh
/// `BTreeMap` queue on every `/transit` request. For Belgium's 64k
/// stops that was ~6 MB of zeroed allocation per query plus a fresh
/// `BTreeMap` with log-cost inserts per round.
///
/// `RaptorState` holds **one** instance of each buffer, sized to the
/// current timetable, and resets them in `start(...)` at the top of
/// every query. The `labels` grid is a flat `Vec<StopLabel>` of
/// `(MAX_ROUNDS + 1) * n_stops` cells, accessed row-major by round.
/// The route queue is an indexed `Vec<u32>` of length `n_routes`, with
/// a separate `queue_touched: Vec<RouteIdx>` tracking which entries
/// are live so the per-round reset is O(touched) instead of O(n_routes).
/// The marked frontier is a `Vec<bool>` plus a `Vec<StopIdx>` list of
/// the actual marked stops, so transfer relaxation iterates only the
/// frontier — not every stop in the timetable.
struct RaptorState {
    n_stops: usize,
    n_routes: usize,
    /// Row-major `[round][stop]` grid, flattened. Index:
    /// `round * n_stops + stop`.
    labels: Vec<StopLabel>,
    /// `best_at_stop[s]` = earliest arrival across all rounds so far.
    best_at_stop: Vec<u32>,
    /// `marked[s]` = true iff `s` was improved in the current round's
    /// route-scan or transfer-closure phase.
    marked: Vec<bool>,
    /// Live list of stops currently marked — the frontier. Walking
    /// this is O(frontier size), not O(n_stops).
    frontier: Vec<StopIdx>,
    /// Indexed route queue: `queue_start_pos[route]` = earliest stop
    /// position at which the route was touched this round, or
    /// `QUEUE_EMPTY` if the route is not in the queue.
    queue_start_pos: Vec<u32>,
    /// List of routes currently in the queue — drives O(touched)
    /// iteration and per-round reset.
    queue_touched: Vec<RouteIdx>,
    /// Scratch priority queue for transfer-closure Dijkstra. Cleared
    /// at the top of every `apply_transfers` call.
    xfer_heap: BinaryHeap<Reverse<(u32, StopIdx)>>,
}

impl RaptorState {
    fn new() -> Self {
        Self {
            n_stops: 0,
            n_routes: 0,
            labels: Vec::new(),
            best_at_stop: Vec::new(),
            marked: Vec::new(),
            frontier: Vec::new(),
            queue_start_pos: Vec::new(),
            queue_touched: Vec::new(),
            xfer_heap: BinaryHeap::new(),
        }
    }

    /// Reset the state to accept a new query against a timetable of
    /// `n_stops` and `n_routes`. Resizes the scratch buffers if the
    /// timetable shape changed; otherwise fills in-place via `fill`.
    fn start(&mut self, n_stops: usize, n_routes: usize) {
        let rounds = MAX_ROUNDS + 1;
        let label_cells = rounds * n_stops;
        if self.n_stops != n_stops || self.labels.len() != label_cells {
            self.labels = vec![StopLabel::default(); label_cells];
            self.best_at_stop = vec![INF; n_stops];
            self.marked = vec![false; n_stops];
            self.frontier = Vec::with_capacity(n_stops.min(1024));
        } else {
            // Fast reset: memset the labels (LLVM vectorises this).
            self.labels.fill(StopLabel::default());
            self.best_at_stop.fill(INF);
            // `marked` is cleared via the frontier below.
        }
        if self.n_routes != n_routes {
            self.queue_start_pos = vec![QUEUE_EMPTY; n_routes];
            self.queue_touched = Vec::with_capacity(n_routes.min(1024));
        } else {
            // Walk the previous frontier to clear only live entries.
            for &s in &self.frontier {
                self.marked[s as usize] = false;
            }
            for &r in &self.queue_touched {
                self.queue_start_pos[r as usize] = QUEUE_EMPTY;
            }
        }
        self.frontier.clear();
        self.queue_touched.clear();
        self.xfer_heap.clear();
        self.n_stops = n_stops;
        self.n_routes = n_routes;
    }

    /// Index into the flat labels grid.
    #[inline]
    fn label_idx(&self, round: usize, stop: StopIdx) -> usize {
        round * self.n_stops + stop as usize
    }

    #[inline]
    fn label(&self, round: usize, stop: StopIdx) -> StopLabel {
        self.labels[self.label_idx(round, stop)]
    }

    #[inline]
    fn set_label(&mut self, round: usize, stop: StopIdx, label: StopLabel) {
        let idx = self.label_idx(round, stop);
        self.labels[idx] = label;
    }

    /// Copy round `k-1` labels into round `k` at the start of the
    /// round (monotone invariant). Cheap: `copy_within`.
    fn carry_forward(&mut self, k: usize) {
        let n = self.n_stops;
        let src = (k - 1) * n;
        let dst = k * n;
        self.labels.copy_within(src..src + n, dst);
    }

    /// Mark a stop as improved this round. Idempotent; only adds the
    /// stop to the frontier list the first time.
    #[inline]
    fn mark(&mut self, stop: StopIdx) {
        let s = stop as usize;
        if !self.marked[s] {
            self.marked[s] = true;
            self.frontier.push(stop);
        }
    }

    /// Reset the marked bitmap and frontier at the top of a round.
    /// Walks only the frontier, not the whole bitmap.
    fn clear_frontier(&mut self) {
        for &s in &self.frontier {
            self.marked[s as usize] = false;
        }
        self.frontier.clear();
    }

    /// Register a route as touched at `start_pos` (taking the min if
    /// the route was already in the queue this round).
    #[inline]
    fn enqueue_route(&mut self, route: RouteIdx, start_pos: u32) {
        let slot = &mut self.queue_start_pos[route as usize];
        if *slot == QUEUE_EMPTY {
            *slot = start_pos;
            self.queue_touched.push(route);
        } else if start_pos < *slot {
            *slot = start_pos;
        }
    }

    /// Reset the queue at the end of a round.
    fn clear_queue(&mut self) {
        for &r in &self.queue_touched {
            self.queue_start_pos[r as usize] = QUEUE_EMPTY;
        }
        self.queue_touched.clear();
    }
}

thread_local! {
    /// One RAPTOR state per thread, reused across queries. See #104.
    static RAPTOR_STATE: RefCell<RaptorState> = RefCell::new(RaptorState::new());
}

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
///
/// Uses a thread-local [`RaptorState`] reused across queries. The
/// per-query reset is O(touched) in the live frontier + queue, plus a
/// vectorised label-grid memset. See issue #104.
pub fn run_raptor(
    timetable: &Timetable,
    transfers: &TransferGraph,
    query: &RaptorQuery<'_>,
) -> Option<RaptorJourney> {
    let n_stops = timetable.n_stops();
    let n_routes = timetable.n_routes();
    if n_stops == 0 || query.sources.is_empty() || query.target_weights.is_empty() {
        return None;
    }

    RAPTOR_STATE.with(|cell| {
        let mut st = cell.borrow_mut();
        st.start(n_stops, n_routes);

        let mut best_absolute = INF;
        let mut best_final_stop: Option<StopIdx> = None;
        let mut best_round: usize = 0;

        // ---- Round 0 seeding ----------------------------------------
        for &(stop, dep) in query.sources {
            let idx = stop as usize;
            if idx >= n_stops {
                continue;
            }
            let current = st.label(0, stop).time;
            if dep < current {
                st.set_label(
                    0,
                    stop,
                    StopLabel {
                        time: dep,
                        via: Via::Origin,
                    },
                );
                let bas = &mut st.best_at_stop[idx];
                if dep < *bas {
                    *bas = dep;
                }
                st.mark(stop);
            }
            if let Some(&tw) = query.target_weights.get(&stop) {
                let total = dep.saturating_add(tw);
                if total < best_absolute {
                    best_absolute = total;
                    best_final_stop = Some(stop);
                    best_round = 0;
                }
            }
        }

        // Pre-round-1 transfer closure from origin seeds.
        apply_transfers_state(&mut st, 0, transfers);
        // Re-check absolute best using transfer-relaxed labels.
        // Walk only the live frontier — not the whole label grid.
        for &stop in &st.frontier.clone() {
            let label_time = st.label(0, stop).time;
            if label_time == INF {
                continue;
            }
            if let Some(&tw) = query.target_weights.get(&stop) {
                let total = label_time.saturating_add(tw);
                if total < best_absolute {
                    best_absolute = total;
                    best_final_stop = Some(stop);
                    best_round = 0;
                }
            }
        }

        // ---- Rounds 1..=MAX_ROUNDS ----------------------------------
        for k in 1..=MAX_ROUNDS {
            // Monotone carry: copy round k-1 into round k.
            st.carry_forward(k);

            // Build the route queue from the current frontier. We
            // consume `st.frontier` into `queue_touched` and reset it.
            // The frontier collected during round k-1's transfer pass
            // is what we iterate now.
            let round_frontier: Vec<StopIdx> = st.frontier.clone();
            st.clear_frontier();

            for s in round_frontier {
                for &(route, pos_in_route) in timetable.routes_for_stop(s) {
                    st.enqueue_route(route, pos_in_route);
                }
            }

            let mut any_improvement = false;

            // Iterate queued routes. Drain-style: take a snapshot so we
            // can borrow `st` mutably inside the loop without a
            // self-borrow conflict.
            let queued: Vec<(RouteIdx, u32)> = st
                .queue_touched
                .iter()
                .map(|&r| (r, st.queue_start_pos[r as usize]))
                .collect();
            st.clear_queue();

            for (route, start_pos) in queued {
                let route_stops = timetable.route_stops_slice(route);

                let mut current_trip: Option<u32> = None;
                let mut board_stop: StopIdx = 0;
                let mut board_dep: u32 = 0;

                for (pos, &stop) in route_stops.iter().enumerate().skip(start_pos as usize) {
                    // Try to alight.
                    if let Some(trip) = current_trip {
                        let st_time = timetable.stop_time(route, trip, pos as u32);
                        let arr = st_time.arrival;
                        let min_tw = query.target_weights.get(&stop).copied().unwrap_or(0);
                        let curr_time = st.label(k, stop).time;
                        let best_time = st.best_at_stop[stop as usize];
                        if arr.saturating_add(min_tw) < best_absolute
                            && arr < curr_time
                            && arr < best_time
                        {
                            st.set_label(
                                k,
                                stop,
                                StopLabel {
                                    time: arr,
                                    via: Via::Trip {
                                        route,
                                        trip_in_route: trip,
                                        from_stop: board_stop,
                                        from_round: (k - 1) as u8,
                                        board_time: board_dep,
                                    },
                                },
                            );
                            st.best_at_stop[stop as usize] = arr;
                            st.mark(stop);
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

                    // Can we (re-)board using the k-1 label at this stop?
                    let prev_arr = st.label(k - 1, stop).time;
                    if prev_arr == INF {
                        continue;
                    }
                    if let Some(candidate_trip) =
                        timetable.earliest_trip(route, pos as u32, prev_arr)
                    {
                        let should_switch = match current_trip {
                            None => true,
                            Some(t) => candidate_trip < t,
                        };
                        if should_switch {
                            current_trip = Some(candidate_trip);
                            board_stop = stop;
                            let st_time = timetable.stop_time(route, candidate_trip, pos as u32);
                            board_dep = st_time.departure;
                        }
                    }
                }
            }

            if !any_improvement {
                break;
            }

            // Transfer closure from newly-marked stops.
            apply_transfers_state(&mut st, k, transfers);

            // Re-check absolute best using the frontier (not the
            // whole stop universe).
            for &stop in &st.frontier.clone() {
                let label_time = st.label(k, stop).time;
                if label_time == INF {
                    continue;
                }
                if let Some(&tw) = query.target_weights.get(&stop) {
                    let total = label_time.saturating_add(tw);
                    if total < best_absolute {
                        best_absolute = total;
                        best_final_stop = Some(stop);
                        best_round = k;
                    }
                }
            }
        }

        let final_stop = best_final_stop?;
        if best_absolute == INF {
            return None;
        }

        // Reconstruct the journey from the flat label grid held by `st`.
        let legs = reconstruct_from_state(&st, best_round, final_stop);
        let origin_stop = legs
            .iter()
            .map(|l| match l {
                RaptorLeg::Walk { from_stop, .. } => *from_stop,
                RaptorLeg::Ride { from_stop, .. } => *from_stop,
            })
            .next()
            .unwrap_or(final_stop);

        Some(RaptorJourney {
            arrival_time: st.label(best_round, final_stop).time,
            final_stop,
            origin_stop,
            legs,
        })
    })
}

/// Relax walking transfers to closure, starting from every stop
/// currently marked as "just improved" in this round. Operates on the
/// shared [`RaptorState`] so no heap reallocation happens per call.
///
/// On entry, `state.frontier` contains the stops that were just improved
/// by this round's route-scan phase. On exit, every stop whose label
/// was improved by any chain of walking transfers from the frontier is
/// now marked and on the frontier list.
///
/// See issue #106 for the correctness argument (closure is what makes
/// the ULTRA triangle dominance restriction safe) and issue #104 for
/// the allocation-free rewrite (the heap lives on the state).
fn apply_transfers_state(state: &mut RaptorState, round: usize, transfers: &TransferGraph) {
    state.xfer_heap.clear();
    for &s in &state.frontier {
        let t = state.label(round, s).time;
        if t != INF {
            state.xfer_heap.push(Reverse((t, s)));
        }
    }

    while let Some(Reverse((d, u))) = state.xfer_heap.pop() {
        if d > state.label(round, u).time {
            continue; // stale entry
        }
        for (v, walk_s) in transfers.neighbours(u) {
            if v == u {
                continue;
            }
            let new_time = d.saturating_add(walk_s);
            if new_time < state.label(round, v).time && new_time < state.best_at_stop[v as usize] {
                state.set_label(
                    round,
                    v,
                    StopLabel {
                        time: new_time,
                        via: Via::Walk { from_stop: u },
                    },
                );
                state.best_at_stop[v as usize] = new_time;
                state.mark(v);
                state.xfer_heap.push(Reverse((new_time, v)));
            }
        }
    }
}

fn reconstruct_from_state(
    state: &RaptorState,
    final_round: usize,
    final_stop: StopIdx,
) -> Vec<RaptorLeg> {
    let mut legs: Vec<RaptorLeg> = Vec::new();
    let mut cur_stop = final_stop;
    let mut cur_round = final_round;

    loop {
        let label = state.label(cur_round, cur_stop);
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
                    .saturating_sub(state.label(cur_round, from_stop).time);
                legs.push(RaptorLeg::Walk {
                    from_stop,
                    to_stop: cur_stop,
                    duration_s: dur,
                });
                cur_stop = from_stop;
                // Walks are within the same round — cur_round unchanged.
            }
        }
        if cur_round == 0 && matches!(state.label(cur_round, cur_stop).via, Via::Origin) {
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
