//! GTFS-RT trip update ingestion.
//!
//! We consume a `FeedMessage` protobuf, locate matching trips in the
//! current [`Timetable`] by `trip_id`, and apply the delay/time overrides
//! to the relevant `StopTime` cells. The result is a new timetable
//! snapshot (the original is left intact so hot-swap is atomic).
//!
//! Malformed or unknown updates are logged and skipped — we never panic.

use anyhow::{Context, Result};
use gtfs_rt::FeedMessage;
// gtfs-rt 0.5 is built against prost 0.11; we pull in a renamed
// `prost_011` alias so `FeedMessage::decode` / `encode` resolves against
// the matching trait version.
use prost_011::Message;

use super::timetable::{StopTime, Timetable};

/// Statistics returned after applying a batch of trip updates.
#[derive(Debug, Default, Clone)]
pub struct RtApplyStats {
    pub entities_seen: usize,
    pub trips_matched: usize,
    pub trips_unknown: usize,
    pub stop_times_patched: usize,
    pub invalid_entities: usize,
}

/// Decode a protobuf blob into a [`FeedMessage`].
pub fn decode(bytes: &[u8]) -> Result<FeedMessage> {
    FeedMessage::decode(bytes).context("decoding GTFS-RT FeedMessage")
}

/// Return a *new* [`Timetable`] with GTFS-RT trip updates applied.
///
/// This clones the input timetable (cheap: `Arc`s aren't cloned here, but
/// `Vec`s are — `Timetable` doesn't use interior mutability). The clone
/// is acceptable because realtime updates are applied at most every
/// 60 seconds, and Belgian timetables are ~200 MB in memory.
pub fn apply_trip_updates(base: &Timetable, feed: &FeedMessage) -> (Timetable, RtApplyStats) {
    let mut out = base.clone();
    let mut stats = RtApplyStats::default();

    for entity in &feed.entity {
        stats.entities_seen += 1;
        let Some(trip_update) = &entity.trip_update else {
            continue;
        };
        let trip_id = trip_update.trip.trip_id.as_deref();
        let Some(trip_id) = trip_id else {
            stats.invalid_entities += 1;
            continue;
        };

        let Some(&global_trip_idx) = out.trip_id_to_idx.get(trip_id) else {
            stats.trips_unknown += 1;
            continue;
        };
        stats.trips_matched += 1;
        let (route_idx, trip_in_route) = out.trip_to_route[global_trip_idx as usize];
        let n_stops = out.n_stops[route_idx as usize];
        let base_offset = out.stop_times_offset[route_idx as usize];
        let trip_base = base_offset + trip_in_route as u64 * n_stops as u64;

        // Build a working copy of this trip's stop_times so we can apply
        // updates in-place, then write it back. Reads from the two
        // SoA arrays (#126) and reconstructs transient `StopTime`
        // structs; the write-back loop below splits them again.
        let mut times: Vec<StopTime> = (0..n_stops)
            .map(|i| {
                let idx = (trip_base + i as u64) as usize;
                StopTime {
                    arrival: out.arrivals[idx],
                    departure: out.departures[idx],
                }
            })
            .collect();

        // Build a per-stop (arr_delay, dep_delay) delta table seeded
        // with running deltas propagated from earlier updates. This
        // matches the GTFS-RT rule: a StopTimeUpdate at stop i applies
        // to stop i and to every downstream stop that does not have
        // its own update.
        let mut arr_deltas: Vec<i32> = vec![0; n_stops as usize];
        let mut dep_deltas: Vec<i32> = vec![0; n_stops as usize];
        let mut running_arr: i32 = 0;
        let mut running_dep: i32 = 0;
        let mut first_update_pos: Option<usize> = None;

        // Map stop-sequence → (arr_event, dep_event) for quick lookup.
        let mut events_by_pos: std::collections::BTreeMap<
            usize,
            &gtfs_rt::trip_update::StopTimeUpdate,
        > = std::collections::BTreeMap::new();
        for stu in &trip_update.stop_time_update {
            let Some(pos) = resolve_stop_position(&out, route_idx, stu) else {
                stats.invalid_entities += 1;
                continue;
            };
            if pos >= n_stops as usize {
                stats.invalid_entities += 1;
                continue;
            }
            events_by_pos.insert(pos, stu);
            stats.stop_times_patched += 1;
            first_update_pos = Some(first_update_pos.map(|p| p.min(pos)).unwrap_or(pos));
        }

        // Walk every stop in-order, updating running deltas from any
        // encountered update and storing the current delta at each stop.
        for pos in 0..n_stops as usize {
            if let Some(stu) = events_by_pos.get(&pos) {
                if let Some(ev) = &stu.arrival {
                    if let Some(delay) = ev.delay {
                        running_arr = delay;
                    } else if let Some(time) = ev.time {
                        let target = time.rem_euclid(86_400);
                        running_arr = (target - times[pos].arrival as i64) as i32;
                    }
                }
                if let Some(ev) = &stu.departure {
                    if let Some(delay) = ev.delay {
                        running_dep = delay;
                    } else if let Some(time) = ev.time {
                        let target = time.rem_euclid(86_400);
                        running_dep = (target - times[pos].departure as i64) as i32;
                    }
                }
            }
            // Apply running delays only to stops at or after the first
            // update position — earlier stops are unaffected.
            if first_update_pos.map(|fp| pos >= fp).unwrap_or(false) {
                arr_deltas[pos] = running_arr;
                dep_deltas[pos] = running_dep;
            }
        }

        for (pos, st) in times.iter_mut().enumerate() {
            st.arrival = offset_time(st.arrival, arr_deltas[pos]);
            st.departure = offset_time(st.departure, dep_deltas[pos]);
        }

        // Write back to both SoA arrays (#126) AND to the
        // column-major mirror (#127). The mirror must stay in
        // lockstep with the row-major `departures` array because
        // `earliest_trip` reads from it directly. For a write at
        // `(r, trip, pos)`, the mirror index is
        // `col_base + pos * n_trips[r] + trip`.
        let n_trips = out.n_trips[route_idx as usize] as u64;
        let col_base = out.col_departures_route_offset[route_idx as usize];
        for (i, st) in times.into_iter().enumerate() {
            let idx = (trip_base + i as u64) as usize;
            out.arrivals[idx] = st.arrival;
            out.departures[idx] = st.departure;
            let col_idx = (col_base + (i as u64) * n_trips + trip_in_route as u64) as usize;
            out.col_departures[col_idx] = st.departure;
        }
    }

    (out, stats)
}

fn offset_time(base: u32, delta_secs: i32) -> u32 {
    if delta_secs >= 0 {
        base.saturating_add(delta_secs as u32)
    } else {
        base.saturating_sub((-delta_secs) as u32)
    }
}

/// Resolve a `StopTimeUpdate`'s position along the route's stop pattern.
///
/// Preference order:
///   1. `stop_sequence` (treated as 0-based offset into the pattern);
///   2. `stop_id` lookup against the pattern's `StopIdx` list.
fn resolve_stop_position(
    tt: &Timetable,
    route_idx: u32,
    stu: &gtfs_rt::trip_update::StopTimeUpdate,
) -> Option<usize> {
    if let Some(seq) = stu.stop_sequence {
        // GTFS sequences are typically 1-based in source data. Accept
        // both: if `seq` exceeds n_stops, try (seq - 1).
        let n = tt.n_stops_on_route(route_idx);
        if (seq as usize) < n {
            return Some(seq as usize);
        }
        if seq > 0 && (seq as usize - 1) < n {
            return Some(seq as usize - 1);
        }
    }
    if let Some(stop_id) = &stu.stop_id {
        let &s = tt.stop_id_to_idx.get(stop_id)?;
        let pattern = tt.route_stops_slice(route_idx);
        return pattern.iter().position(|&x| x == s);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transit::timetable::{StopTime, TimetableBuilder};
    use gtfs_rt::trip_update::{StopTimeEvent, StopTimeUpdate};
    use gtfs_rt::{FeedEntity, TripDescriptor, TripUpdate};

    fn st(a: u32, d: u32) -> StopTime {
        StopTime {
            arrival: a,
            departure: d,
        }
    }

    #[test]
    fn decodes_empty_feed() {
        let feed = FeedMessage {
            header: gtfs_rt::FeedHeader {
                gtfs_realtime_version: "2.0".to_string(),
                ..Default::default()
            },
            entity: vec![],
        };
        let mut buf = Vec::new();
        feed.encode(&mut buf).unwrap();
        let decoded = decode(&buf).unwrap();
        assert_eq!(decoded.entity.len(), 0);
    }

    #[test]
    fn applies_delay_to_matching_trip() {
        let mut b = TimetableBuilder::new();
        let a = b.add_stop("A", "A", 0.0, 0.0, None);
        let bb = b.add_stop("B", "B", 1.0, 0.0, None);
        let c = b.add_stop("C", "C", 2.0, 0.0, None);
        b.add_trip(
            "trip_X",
            "R",
            "R",
            "h",
            vec![a, bb, c],
            vec![st(100, 100), st(200, 210), st(300, 310)],
        );
        let tt = b.build().unwrap();

        // Build a FeedMessage that delays trip_X by 120s at stop 1 (B).
        let feed = FeedMessage {
            header: gtfs_rt::FeedHeader {
                gtfs_realtime_version: "2.0".to_string(),
                ..Default::default()
            },
            entity: vec![FeedEntity {
                id: "e1".to_string(),
                trip_update: Some(TripUpdate {
                    trip: TripDescriptor {
                        trip_id: Some("trip_X".to_string()),
                        ..Default::default()
                    },
                    stop_time_update: vec![StopTimeUpdate {
                        stop_sequence: Some(1),
                        arrival: Some(StopTimeEvent {
                            delay: Some(120),
                            ..Default::default()
                        }),
                        departure: Some(StopTimeEvent {
                            delay: Some(120),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                ..Default::default()
            }],
        };

        let (patched, stats) = apply_trip_updates(&tt, &feed);
        assert_eq!(stats.trips_matched, 1);
        assert_eq!(stats.stop_times_patched, 1);
        // B (index 1) is now +120s on arrival and departure,
        // and the running delay propagates to C (index 2).
        let route_b = patched.routes_for_stop(bb)[0].0;
        let b_pos = patched
            .route_stops_slice(route_b)
            .iter()
            .position(|&x| x == bb)
            .unwrap() as u32;
        let c_pos = patched
            .route_stops_slice(route_b)
            .iter()
            .position(|&x| x == c)
            .unwrap() as u32;
        assert_eq!(patched.stop_time(route_b, 0, b_pos).arrival, 320);
        assert_eq!(patched.stop_time(route_b, 0, c_pos).arrival, 420);
    }

    /// Regression for #111: GTFS-RT delay flips the relative order of
    /// two trips on a route. The in-place patch does not re-sort the
    /// underlying `stop_times` grid, so `earliest_trip` must be robust
    /// to non-monotone trip order (subsumed by the #108 fix). Without
    /// order-independent lookup, a scan of "first trip with dep ≥ t"
    /// would pick the delayed trip instead of the actually-earliest one.
    #[test]
    fn gtfs_rt_delay_flips_trip_order_still_picks_earliest() {
        let mut b = TimetableBuilder::new();
        let a = b.add_stop("A", "A", 0.0, 0.0, None);
        let bb = b.add_stop("B", "B", 1.0, 0.0, None);
        // Two trips on the same A→B pattern. trip_early departs A at 500,
        // trip_late departs A at 600. In the builder's natural order,
        // trip_early is stored first.
        b.add_trip(
            "trip_early",
            "R",
            "R",
            "h",
            vec![a, bb],
            vec![st(500, 500), st(560, 560)],
        );
        b.add_trip(
            "trip_late",
            "R",
            "R",
            "h",
            vec![a, bb],
            vec![st(600, 600), st(660, 660)],
        );
        let tt = b.build().unwrap();
        let route_idx = tt.routes_for_stop(a)[0].0;
        // Sanity: before the patch, earliest_trip(dep ≥ 0) at stop A is
        // trip_early (position 0), and at dep ≥ 550 it's trip_late.
        assert_eq!(tt.earliest_trip(route_idx, 0, 0), Some(0));
        assert_eq!(tt.earliest_trip(route_idx, 0, 550), Some(1));

        // GTFS-RT: delay trip_early by +300s at every stop. A is now
        // 500 + 300 = 800, which is AFTER trip_late's 600. The storage
        // order (trip_early first) is unchanged, but temporally
        // trip_late is now earlier.
        let feed = FeedMessage {
            header: gtfs_rt::FeedHeader {
                gtfs_realtime_version: "2.0".to_string(),
                ..Default::default()
            },
            entity: vec![FeedEntity {
                id: "e1".to_string(),
                trip_update: Some(TripUpdate {
                    trip: TripDescriptor {
                        trip_id: Some("trip_early".to_string()),
                        ..Default::default()
                    },
                    stop_time_update: vec![StopTimeUpdate {
                        stop_sequence: Some(0),
                        arrival: Some(StopTimeEvent {
                            delay: Some(300),
                            ..Default::default()
                        }),
                        departure: Some(StopTimeEvent {
                            delay: Some(300),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                ..Default::default()
            }],
        };
        let (patched, stats) = apply_trip_updates(&tt, &feed);
        assert_eq!(stats.trips_matched, 1);

        // Verify the patch: trip_early now departs A at 800.
        assert_eq!(patched.stop_time(route_idx, 0, 0).departure, 800);
        assert_eq!(patched.stop_time(route_idx, 1, 0).departure, 600);

        // The critical assertion. `earliest_trip` at A with dep ≥ 0 must
        // return trip_late (storage index 1), because temporally it is
        // now the earliest. A naive "first trip with dep ≥ t in storage
        // order" implementation would return trip_early (storage index 0)
        // which departs at 800 — strictly later than trip_late's 600.
        assert_eq!(patched.earliest_trip(route_idx, 0, 0), Some(1));

        // Similarly, asking for dep ≥ 700 must return trip_early (800),
        // because trip_late (600) is already gone.
        assert_eq!(patched.earliest_trip(route_idx, 0, 700), Some(0));
    }
}
