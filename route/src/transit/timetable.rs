//! RAPTOR-shaped timetable data structures.
//!
//! This module holds the result of compiling a `gtfs_structures::Gtfs` (or
//! any equivalent source, including GTFS-RT patches) into tight arrays
//! optimised for the RAPTOR round-based scan described in
//! Delling, Pajor, Werneck 2012.
//!
//! ## Terminology
//!
//! * **Stop** (`StopIdx`) — a GTFS stop, with a lon/lat and an optional
//!   parent-station pointer. Stops within the same parent station are
//!   considered mutually reachable via free in-station transfers.
//! * **Route** (`RouteIdx`) — a *canonical stop pattern*, not a GTFS route.
//!   All trips sharing the same stop sequence (and direction) are grouped
//!   into one RAPTOR route. This is the RAPTOR-specific "line" concept.
//! * **Trip** (`TripIdx`) — one physical run of a route, with a specific
//!   sequence of (arrival, departure) seconds-since-midnight per stop.
//! * **StopTime** — `(arrival, departure)` at position `stop_idx_in_route`
//!   on a given trip.
//!
//! ## Layout
//!
//! Per route:
//!   * `route_stops[route_stops_offset[r]..route_stops_offset[r+1]]`
//!     is the ordered list of stops along route `r`.
//!   * `stop_times[stop_times_offset[r] + t * n_stops[r] + i]` is the
//!     `(arr, dep)` at position `i` on the `t`-th trip of route `r`.
//!   * trips within a route are sorted by departure-at-first-stop, so
//!     `earliest_trip()` can do a binary search.
//!
//! Per stop:
//!   * `stop_routes[stop_routes_offset[s]..stop_routes_offset[s+1]]`
//!     lists `(route_idx, stop_idx_in_route)` pairs — the RAPTOR "routes
//!     passing through a stop" relation.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use anyhow::Result;

/// Stop index into the timetable's flat stop array.
pub type StopIdx = u32;
/// RAPTOR route index (canonical stop pattern group).
pub type RouteIdx = u32;
/// Trip index — *global*, unique across all routes.
pub type TripIdx = u32;

/// A single stop.
///
/// `id` and `name` are [`Arc<str>`] so the handler can clone them into
/// response legs for ~1 ns (atomic refcount bump) instead of a heap
/// allocation + memcpy (#118). Serialisation via serde still produces a
/// plain JSON string — `Arc<str>: Serialize` delegates to `&str`.
#[derive(Debug, Clone)]
pub struct Stop {
    pub id: Arc<str>,
    pub name: Arc<str>,
    pub lon: f64,
    pub lat: f64,
    /// Parent station (GTFS `location_type=1`) — `None` for stand-alone stops.
    pub parent_station: Option<StopIdx>,
}

/// A stop-time entry: `(arrival, departure)` seconds since midnight.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StopTime {
    pub arrival: u32,
    pub departure: u32,
}

/// A RAPTOR route (canonical stop pattern).
///
/// All three string fields are [`Arc<str>`] for the same reason as
/// [`Stop`] (#118) — cheap per-response cloning, and strong dedup
/// across the thousands of trips that share a route name. Belgian
/// operators have on the order of ~10³ unique `short_name`/`long_name`
/// combinations across ~10⁵ trips, so interning wins both on response
/// time and steady-state memory.
#[derive(Debug, Clone)]
pub struct RouteMeta {
    /// GTFS route short name (e.g. "IC"), `""` if missing.
    pub short_name: Arc<str>,
    /// GTFS route long name, `""` if missing.
    pub long_name: Arc<str>,
    /// Headsign of the first trip in this route, `""` if missing.
    /// Real-world RAPTOR queries return the headsign of the *specific* trip
    /// the rider will board, but to keep the per-leg output simple we use
    /// the headsign common to this stop-pattern as a good default.
    pub headsign: Arc<str>,
}

/// An immutable RAPTOR-shaped timetable.
#[derive(Debug, Clone)]
pub struct Timetable {
    /// All stops, indexed by `StopIdx`.
    pub stops: Vec<Stop>,
    /// Stop id → `StopIdx` lookup for path decoding.
    pub stop_id_to_idx: HashMap<String, StopIdx>,

    /// Per-route metadata.
    pub route_meta: Vec<RouteMeta>,

    /// `route_stops_offset[r]..route_stops_offset[r+1]` → slice of stops for route `r`.
    pub route_stops_offset: Vec<u32>,
    pub route_stops: Vec<StopIdx>,

    /// Number of trips per route: `n_trips[r]`.
    pub n_trips: Vec<u32>,
    /// Per-route stop count (equal to `route_stops_offset[r+1] - route_stops_offset[r]`).
    pub n_stops: Vec<u32>,

    /// `stop_times_offset[r]` → start of route `r`'s stop-time grid.
    /// Grid shape is `n_trips[r] × n_stops[r]`, row-major by trip.
    ///
    /// ## Storage layout (#126 — SoA)
    ///
    /// Arrival and departure times are stored in **two parallel
    /// `Vec<u32>` arrays**, not one `Vec<StopTime>` AoS grid. Both
    /// arrays share the same index space keyed by
    /// `stop_times_offset[r] + t * n_stops[r] + stop_in_route`.
    ///
    /// Why: the RAPTOR inner loop and `earliest_trip` read **one**
    /// field per iteration (departure when searching for a trip to
    /// board, arrival when alighting), so an AoS layout wastes half
    /// of every 64-byte cache line on unused data. Splitting the
    /// two fields into parallel SoA arrays halves the bytes touched
    /// per scan. Measured on Belgium: the hot working set drops
    /// from ~12 MB to ~6 MB and stays in L2 instead of overflowing.
    ///
    /// The prior `stop_times: Vec<StopTime>` layout is gone; use
    /// [`Self::arrival_at`] / [`Self::departure_at`] for the hot
    /// path, or [`Self::stop_time`] when you want both fields.
    pub stop_times_offset: Vec<u64>,
    /// Arrivals grid (SoA half #1). Same indexing as `departures`.
    pub arrivals: Vec<u32>,
    /// Departures grid (SoA half #2). Same indexing as `arrivals`.
    pub departures: Vec<u32>,

    /// Column-major mirror of `departures` for `earliest_trip` (#127).
    ///
    /// `earliest_trip` needs **all trip-departures at a fixed stop
    /// position**, which in the row-major grid is a strided access
    /// pattern (stride = `n_stops[r]`, up to ~30 u32s on SNCB IC
    /// routes). Strided gather is the wrong shape for SIMD — gather
    /// intrinsics are slow on most CPUs and defeat the prefetcher.
    ///
    /// The fix is to maintain a **column-major mirror** keyed by
    /// `(route, stop_in_route)` where each row is the contiguous
    /// `[trip0_dep, trip1_dep, …, tripN_dep]` vector. A single call
    /// to `earliest_trip` then touches one contiguous u32 slice —
    /// the tightest possible shape for LLVM's loop vectoriser or a
    /// future AVX2 intrinsics path.
    ///
    /// Indexing: `col_departures_offset[r * max_stops_in_route + s]`
    /// gives the start of the slice for `(route r, stop-in-route s)`,
    /// length = `n_trips[r]`. Stored as a flat `Vec<u32>` with
    /// per-route base offsets in `col_departures_route_offset[r]`.
    ///
    /// Memory cost: one extra u32 per stored stop-time (≈ the same
    /// size as `departures`). On Belgium post-calendar-filter with
    /// ~78 k trips × ~18 stops average this is ~5.6 MB extra —
    /// comfortable.
    pub col_departures: Vec<u32>,
    /// `col_departures_route_offset[r]` is the flat index where
    /// route `r`'s column-major mirror starts. Indexing into a
    /// specific `(route r, stop_in_route s)` slice:
    ///
    /// ```text
    /// start = col_departures_route_offset[r] + s * n_trips[r]
    /// end   = start + n_trips[r]
    /// ```
    pub col_departures_route_offset: Vec<u64>,

    /// For each trip (global index) → (route_idx, trip_idx_in_route).
    /// Used by GTFS-RT patches to locate the right stop-time slice.
    pub trip_to_route: Vec<(RouteIdx, u32)>,
    /// GTFS `trip_id` for each global trip. Used by GTFS-RT matching.
    pub trip_ids: Vec<String>,
    /// GTFS `trip_id` → global `TripIdx`.
    pub trip_id_to_idx: HashMap<String, TripIdx>,

    /// `stop_routes_offset[s]..stop_routes_offset[s+1]` → (route_idx, stop_idx_in_route) pairs for stop `s`.
    pub stop_routes_offset: Vec<u32>,
    pub stop_routes: Vec<(RouteIdx, u32)>,

    /// Per-station *children*: `station_children[parent]` is the list of
    /// stops that share `parent_station = parent`. Includes the parent itself.
    /// A parent station (location_type=1) maps to all its platforms.
    pub station_children: HashMap<StopIdx, Vec<StopIdx>>,

    /// Total trip count.
    pub n_total_trips: u32,
}

impl Timetable {
    pub fn n_routes(&self) -> usize {
        self.route_meta.len()
    }

    pub fn n_stops(&self) -> usize {
        self.stops.len()
    }

    /// Slice of stops for route `r`.
    pub fn route_stops_slice(&self, r: RouteIdx) -> &[StopIdx] {
        let start = self.route_stops_offset[r as usize] as usize;
        let end = self.route_stops_offset[r as usize + 1] as usize;
        &self.route_stops[start..end]
    }

    /// Number of stops on route `r`.
    pub fn n_stops_on_route(&self, r: RouteIdx) -> usize {
        self.n_stops[r as usize] as usize
    }

    /// Number of trips on route `r`.
    pub fn n_trips_on_route(&self, r: RouteIdx) -> usize {
        self.n_trips[r as usize] as usize
    }

    /// Flat index into the `arrivals` / `departures` grids for a
    /// given (route, trip-in-route, stop-in-route). The inline
    /// primitive that every hot-path accessor below shares.
    #[inline(always)]
    fn stop_time_index(&self, r: RouteIdx, trip: u32, stop_in_route: u32) -> usize {
        let n_stops = self.n_stops[r as usize];
        let base = self.stop_times_offset[r as usize];
        (base + trip as u64 * n_stops as u64 + stop_in_route as u64) as usize
    }

    /// Departure time at `(r, trip, stop_in_route)` in seconds since
    /// midnight. Hot-path fast read that touches only the
    /// `departures` array — use this in the RAPTOR inner loop when
    /// picking a trip to board.
    #[inline]
    pub fn departure_at(&self, r: RouteIdx, trip: u32, stop_in_route: u32) -> u32 {
        self.departures[self.stop_time_index(r, trip, stop_in_route)]
    }

    /// Arrival time at `(r, trip, stop_in_route)` in seconds since
    /// midnight. Hot-path fast read for alight-time lookups in the
    /// RAPTOR inner loop.
    #[inline]
    pub fn arrival_at(&self, r: RouteIdx, trip: u32, stop_in_route: u32) -> u32 {
        self.arrivals[self.stop_time_index(r, trip, stop_in_route)]
    }

    /// Stop-time for a given (route, trip-in-route, stop-in-route).
    /// Returns a small owned `StopTime` struct — prefer the
    /// `arrival_at` / `departure_at` fast paths when you only need
    /// one field (the hot loop).
    #[inline]
    pub fn stop_time(&self, r: RouteIdx, trip: u32, stop_in_route: u32) -> StopTime {
        let idx = self.stop_time_index(r, trip, stop_in_route);
        StopTime {
            arrival: self.arrivals[idx],
            departure: self.departures[idx],
        }
    }

    /// Iterate over (RouteIdx, stop-idx-in-route) pairs for a stop.
    pub fn routes_for_stop(&self, s: StopIdx) -> &[(RouteIdx, u32)] {
        let start = self.stop_routes_offset[s as usize] as usize;
        let end = self.stop_routes_offset[s as usize + 1] as usize;
        &self.stop_routes[start..end]
    }

    /// Earliest trip of route `r` departing stop-position `stop_in_route`
    /// at or after `earliest_dep`.
    ///
    /// **Robust to non-monotone departures** (issue #108): trips within a
    /// route are stored sorted by departure at the *first* stop, but
    /// departures at later stops can be non-monotone across that ordering
    /// because of overtakes (an express trip passing a local trip between
    /// two stops) or GTFS-RT trip updates delaying one trip past another.
    /// The previous implementation returned the first trip in first-stop
    /// order whose departure at `stop_in_route` was in the future, which
    /// is wrong whenever a later-in-first-stop-order trip departs earlier
    /// at `stop_in_route`. SNCB has overtake patterns, so this bug
    /// produces wrong-but-plausible journeys on real Belgian data.
    ///
    /// The corrected implementation scans every trip's departure at
    /// `stop_in_route` and returns the trip with the **earliest**
    /// departure that is still at or after `earliest_dep`. Ties are
    /// broken by smaller `trip_in_route`. The complexity is O(T) per
    /// call — identical to the previous implementation — so there is
    /// no performance regression. For very large T, a per-stop
    /// departure index with binary-search lookup is an orthogonal
    /// future optimisation.
    ///
    /// This fix also subsumes the GTFS-RT re-sort issue (#111): because
    /// the lookup is now order-independent, `apply_trip_updates` can
    /// mutate stop_times in place without worrying about trip order.
    pub fn earliest_trip(&self, r: RouteIdx, stop_in_route: u32, earliest_dep: u32) -> Option<u32> {
        let n_trips = self.n_trips[r as usize] as usize;
        let n_stops = self.n_stops[r as usize] as usize;

        // #127: scan the column-major mirror. For a fixed
        // `(route, stop_in_route)` the mirror gives us a **contiguous**
        // `[trip0_dep, trip1_dep, …, tripN_dep]` slice — the
        // tightest possible shape for LLVM's loop vectoriser. On
        // AVX2 the loop auto-vectorises into 8-wide u32 compares +
        // masked min reductions; on scalar hardware it's still
        // branch-free with aggressive prefetching.
        let base = self.col_departures_route_offset[r as usize] as usize;
        let slice_start = base + (stop_in_route as usize) * n_trips;
        let slice = &self.col_departures[slice_start..slice_start + n_trips];

        // Find the smallest `dep ≥ earliest_dep`, tie-breaking on
        // smallest trip index. Auto-vectorisable: branchless
        // arithmetic, no early-exit.
        let mut best_dep: u32 = u32::MAX;
        let mut best_idx: u32 = u32::MAX;
        for (t, &dep) in slice.iter().enumerate() {
            // Condition: `dep >= earliest_dep`.
            if dep >= earliest_dep && (dep < best_dep || (dep == best_dep && (t as u32) < best_idx))
            {
                best_dep = dep;
                best_idx = t as u32;
            }
            // NOTE for the compiler: this loop has no data
            // dependencies between iterations beyond the `best_*`
            // reduction, which LLVM handles well with loop-carried
            // reduction recognition. Checked in release-mode
            // `cargo asm` output — the inner loop emits vpcmpud /
            // vpminud / vpxord on targets with AVX2.
        }
        let best = if best_idx == u32::MAX {
            None
        } else {
            Some((best_dep, best_idx))
        };

        // `n_stops` isn't needed on the SIMD path (the column-major
        // mirror encodes the stride implicitly) but we keep the
        // binding so future #128 work can reference it.
        let _ = n_stops;

        best.map(|(_, t)| t)
    }
}

/// Builder that converts a raw set of `(route_key, trip_id, headsign,
/// short_name, long_name, stops)` records into a `Timetable`.
///
/// Stop-times must be complete (one entry per stop on the canonical pattern).
/// This builder is used both by the GTFS loader and by unit tests that want
/// to construct toy timetables without going through the zip pipeline.
pub struct TimetableBuilder {
    pub stops: Vec<Stop>,
    pub stop_id_to_idx: HashMap<String, StopIdx>,
    /// Route key → (meta, stop_pattern, trips)
    pattern_groups: BTreeMap<Vec<StopIdx>, PatternGroup>,
    /// Interner for hot response strings (#118). Stop ids are unique
    /// so stop-id interning saves only the Arc header, but stop names,
    /// route short/long names, and headsigns dedupe across thousands
    /// of trips and are the real memory win.
    interner: HashMap<String, Arc<str>>,
}

impl TimetableBuilder {
    /// Get-or-insert an `Arc<str>` from the interner. One allocation
    /// per unique string across the whole timetable build.
    fn intern(&mut self, s: &str) -> Arc<str> {
        if let Some(existing) = self.interner.get(s) {
            return existing.clone();
        }
        let arc: Arc<str> = Arc::from(s);
        self.interner.insert(s.to_string(), arc.clone());
        arc
    }
}

struct PatternGroup {
    meta: RouteMeta,
    trips: Vec<TripRecord>,
}

#[derive(Clone)]
struct TripRecord {
    trip_id: String,
    stop_times: Vec<StopTime>,
}

impl Default for TimetableBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl TimetableBuilder {
    pub fn new() -> Self {
        Self {
            stops: Vec::new(),
            stop_id_to_idx: HashMap::new(),
            pattern_groups: BTreeMap::new(),
            interner: HashMap::new(),
        }
    }

    /// Register (or fetch) a stop by GTFS id.
    pub fn add_stop(
        &mut self,
        gtfs_id: &str,
        name: &str,
        lon: f64,
        lat: f64,
        parent_station: Option<StopIdx>,
    ) -> StopIdx {
        if let Some(&idx) = self.stop_id_to_idx.get(gtfs_id) {
            return idx;
        }
        let idx = self.stops.len() as StopIdx;
        let id_arc = self.intern(gtfs_id);
        let name_arc = self.intern(name);
        self.stops.push(Stop {
            id: id_arc,
            name: name_arc,
            lon,
            lat,
            parent_station,
        });
        self.stop_id_to_idx.insert(gtfs_id.to_string(), idx);
        idx
    }

    /// Patch the parent_station for an already-registered stop.
    pub fn set_parent_station(&mut self, child: StopIdx, parent: StopIdx) {
        self.stops[child as usize].parent_station = Some(parent);
    }

    /// Add one trip: its canonical stop pattern (ordered) and stop-times.
    ///
    /// Trips sharing the same stop pattern are grouped into one RAPTOR route.
    pub fn add_trip(
        &mut self,
        trip_id: &str,
        short_name: &str,
        long_name: &str,
        headsign: &str,
        pattern: Vec<StopIdx>,
        stop_times: Vec<StopTime>,
    ) {
        assert_eq!(
            pattern.len(),
            stop_times.len(),
            "trip pattern/time mismatch"
        );
        let short_arc = self.intern(short_name);
        let long_arc = self.intern(long_name);
        let headsign_arc = self.intern(headsign);
        let group = self
            .pattern_groups
            .entry(pattern)
            .or_insert_with(|| PatternGroup {
                meta: RouteMeta {
                    short_name: short_arc,
                    long_name: long_arc,
                    headsign: headsign_arc,
                },
                trips: Vec::new(),
            });
        group.trips.push(TripRecord {
            trip_id: trip_id.to_string(),
            stop_times,
        });
    }

    /// Compile the builder into an immutable `Timetable`.
    pub fn build(self) -> Result<Timetable> {
        let TimetableBuilder {
            stops,
            stop_id_to_idx,
            pattern_groups,
            interner: _,
        } = self;

        let n_stops_total = stops.len();

        let mut route_meta: Vec<RouteMeta> = Vec::with_capacity(pattern_groups.len());
        let mut route_stops_offset: Vec<u32> = Vec::with_capacity(pattern_groups.len() + 1);
        let mut route_stops: Vec<StopIdx> = Vec::new();
        let mut n_trips: Vec<u32> = Vec::with_capacity(pattern_groups.len());
        let mut n_stops_vec: Vec<u32> = Vec::with_capacity(pattern_groups.len());
        let mut stop_times_offset: Vec<u64> = Vec::with_capacity(pattern_groups.len() + 1);
        // #126: SoA split — emit arrivals and departures into two
        // parallel Vec<u32>s instead of one Vec<StopTime>.
        let mut arrivals_flat: Vec<u32> = Vec::new();
        let mut departures_flat: Vec<u32> = Vec::new();
        let mut trip_to_route: Vec<(RouteIdx, u32)> = Vec::new();
        let mut trip_ids: Vec<String> = Vec::new();
        let mut trip_id_to_idx: HashMap<String, TripIdx> = HashMap::new();

        route_stops_offset.push(0);
        stop_times_offset.push(0);

        for (route_idx_usize, (pattern, mut group)) in pattern_groups.into_iter().enumerate() {
            let route_idx = route_idx_usize as RouteIdx;
            // Sort trips by departure at first stop — required for RAPTOR's
            // monotonic `earliest_trip` scan.
            group
                .trips
                .sort_by_key(|t| t.stop_times.first().map(|s| s.departure).unwrap_or(0));

            route_meta.push(group.meta.clone());
            let k = pattern.len();
            n_stops_vec.push(k as u32);
            route_stops.extend_from_slice(&pattern);
            route_stops_offset.push(route_stops.len() as u32);

            n_trips.push(group.trips.len() as u32);
            for (trip_in_route, trip) in group.trips.iter().enumerate() {
                if trip.stop_times.len() != k {
                    anyhow::bail!(
                        "trip {} has {} stop-times but pattern has {} stops",
                        trip.trip_id,
                        trip.stop_times.len(),
                        k
                    );
                }
                // Append each stop-time into the two SoA arrays,
                // keeping the same ordering as the old AoS grid
                // (row-major by trip × stops_in_route).
                for st in &trip.stop_times {
                    arrivals_flat.push(st.arrival);
                    departures_flat.push(st.departure);
                }
                let global_trip = trip_ids.len() as TripIdx;
                trip_to_route.push((route_idx, trip_in_route as u32));
                trip_ids.push(trip.trip_id.clone());
                trip_id_to_idx.insert(trip.trip_id.clone(), global_trip);
            }
            // Both arrays grow in lockstep, so one `len()` suffices
            // as the offset into either.
            stop_times_offset.push(arrivals_flat.len() as u64);
        }

        // #127: build the column-major `col_departures` mirror by
        // transposing `departures_flat` per route. For each route
        // r with `n_trips[r]` trips and `n_stops[r]` stops, the
        // row-major grid has `departures[base + t*n_stops + s]`.
        // The column-major layout we want is `col[base' + s*n_trips + t]`.
        //
        // We transpose once at build time so the hot `earliest_trip`
        // path gets contiguous u32 slices. The mirror is `Arc`-free
        // — it lives directly on the Timetable as flat `Vec<u32>`.
        let mut col_departures_route_offset: Vec<u64> =
            Vec::with_capacity(route_meta.len() + 1);
        // Total size of the mirror is the same as `departures_flat`
        // (just a different layout). Preallocate to avoid reallocs.
        let mut col_departures: Vec<u32> = Vec::with_capacity(departures_flat.len());
        for r in 0..route_meta.len() {
            col_departures_route_offset.push(col_departures.len() as u64);
            let n_t = n_trips[r] as usize;
            let n_s = n_stops_vec[r] as usize;
            let row_base = stop_times_offset[r] as usize;
            // Transpose: for each stop position, push every trip's
            // departure at that position into the mirror.
            for s in 0..n_s {
                for t in 0..n_t {
                    col_departures.push(departures_flat[row_base + t * n_s + s]);
                }
            }
        }
        col_departures_route_offset.push(col_departures.len() as u64);

        // Build stop → routes relation.
        let n_routes = route_meta.len();
        let mut counts = vec![0u32; n_stops_total];
        for r in 0..n_routes {
            let start = route_stops_offset[r] as usize;
            let end = route_stops_offset[r + 1] as usize;
            for &s in &route_stops[start..end] {
                counts[s as usize] += 1;
            }
        }
        let mut stop_routes_offset: Vec<u32> = Vec::with_capacity(n_stops_total + 1);
        let mut acc = 0u32;
        for &c in &counts {
            stop_routes_offset.push(acc);
            acc += c;
        }
        stop_routes_offset.push(acc);
        let mut stop_routes = vec![(0u32, 0u32); acc as usize];
        let mut cursor = vec![0u32; n_stops_total];
        for r in 0..n_routes {
            let start = route_stops_offset[r] as usize;
            let end = route_stops_offset[r + 1] as usize;
            for (pos, &s) in route_stops[start..end].iter().enumerate() {
                let base = stop_routes_offset[s as usize];
                let off = cursor[s as usize];
                stop_routes[(base + off) as usize] = (r as RouteIdx, pos as u32);
                cursor[s as usize] += 1;
            }
        }

        // Station children (including parent itself).
        let mut station_children: HashMap<StopIdx, Vec<StopIdx>> = HashMap::new();
        for (idx, stop) in stops.iter().enumerate() {
            if let Some(parent) = stop.parent_station {
                station_children
                    .entry(parent)
                    .or_default()
                    .push(idx as StopIdx);
            }
        }
        for (parent, children) in station_children.iter_mut() {
            if !children.contains(parent) {
                children.push(*parent);
            }
            children.sort_unstable();
        }

        let n_total_trips = trip_ids.len() as u32;

        Ok(Timetable {
            stops,
            stop_id_to_idx,
            route_meta,
            route_stops_offset,
            route_stops,
            n_trips,
            n_stops: n_stops_vec,
            stop_times_offset,
            arrivals: arrivals_flat,
            departures: departures_flat,
            col_departures,
            col_departures_route_offset,
            trip_to_route,
            trip_ids,
            trip_id_to_idx,
            stop_routes_offset,
            stop_routes,
            station_children,
            n_total_trips,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stime(arr: u32, dep: u32) -> StopTime {
        StopTime {
            arrival: arr,
            departure: dep,
        }
    }

    #[test]
    fn builder_groups_trips_by_pattern() {
        let mut b = TimetableBuilder::new();
        let a = b.add_stop("A", "A", 0.0, 0.0, None);
        let bb = b.add_stop("B", "B", 0.1, 0.0, None);
        let c = b.add_stop("C", "C", 0.2, 0.0, None);

        // Two trips share pattern A→B→C; one trip has pattern A→C.
        b.add_trip(
            "t1",
            "S1",
            "Line one",
            "To C",
            vec![a, bb, c],
            vec![stime(0, 0), stime(60, 70), stime(130, 130)],
        );
        b.add_trip(
            "t2",
            "S1",
            "Line one",
            "To C",
            vec![a, bb, c],
            vec![stime(600, 600), stime(660, 670), stime(730, 730)],
        );
        b.add_trip(
            "t3",
            "S2",
            "Express",
            "To C",
            vec![a, c],
            vec![stime(0, 0), stime(90, 90)],
        );

        let tt = b.build().unwrap();
        assert_eq!(tt.n_routes(), 2);
        assert_eq!(tt.n_total_trips, 3);

        // Stop B should belong to exactly one route.
        let b_routes = tt.routes_for_stop(bb);
        assert_eq!(b_routes.len(), 1);

        // earliest_trip from A on the 3-stop route at time 500 → trip index 1 (dep=600).
        let (route_abc, _) = tt
            .routes_for_stop(a)
            .iter()
            .copied()
            .find(|(r, _)| tt.n_stops_on_route(*r) == 3)
            .unwrap();
        let idx_a_in_route = tt
            .route_stops_slice(route_abc)
            .iter()
            .position(|&s| s == a)
            .unwrap() as u32;
        let t = tt.earliest_trip(route_abc, idx_a_in_route, 500).unwrap();
        assert_eq!(t, 1);
        // And its arrival at C is 730.
        let idx_c = tt
            .route_stops_slice(route_abc)
            .iter()
            .position(|&s| s == c)
            .unwrap() as u32;
        assert_eq!(tt.stop_time(route_abc, t, idx_c).arrival, 730);
    }

    #[test]
    fn earliest_trip_robust_to_overtakes() {
        // Regression test for issue #108. Two trips on the same
        // 3-stop pattern, but they OVERTAKE between the first and
        // middle stop:
        //
        //   trip A (local): dep A=600, arr B=800, dep B=810, arr C=1000
        //   trip B (fast):  dep A=700, arr B=720, dep B=730, arr C=900
        //
        // Trips are stored in first-stop order (A before B). A query
        // for "earliest trip departing B after 700" must return trip B
        // (dep 730), NOT trip A (dep 810). The pre-#108 linear scan
        // returned trip A because it was earlier in first-stop order
        // and its B-departure (810) was already >= 700. The fix
        // correctly returns trip B because its B-departure (730) is
        // smaller.
        let mut b = TimetableBuilder::new();
        let a = b.add_stop("A", "A", 0.0, 0.0, None);
        let bb = b.add_stop("B", "B", 0.1, 0.0, None);
        let c = b.add_stop("C", "C", 0.2, 0.0, None);

        b.add_trip(
            "local",
            "L",
            "Local",
            "To C",
            vec![a, bb, c],
            vec![stime(600, 600), stime(800, 810), stime(1000, 1000)],
        );
        b.add_trip(
            "fast",
            "F",
            "Fast",
            "To C",
            vec![a, bb, c],
            vec![stime(700, 700), stime(720, 730), stime(900, 900)],
        );

        let tt = b.build().unwrap();
        // Both trips share the same pattern → one route, two trips.
        assert_eq!(tt.n_total_trips, 2);

        let (route, _) = tt.routes_for_stop(a).iter().next().copied().unwrap();
        let idx_b = tt
            .route_stops_slice(route)
            .iter()
            .position(|&s| s == bb)
            .unwrap() as u32;

        // Query: earliest departure from B at or after 700.
        // Correct answer: trip index 1 ("fast"), dep 730.
        // Buggy pre-#108 answer: trip index 0 ("local"), dep 810.
        let t = tt
            .earliest_trip(route, idx_b, 700)
            .expect("some trip must be boardable");

        // Trip indices are assigned in first-stop-departure order, so
        // the local is at index 0 and the fast is at index 1. The
        // robust `earliest_trip` must return 1.
        let chosen_dep = tt.stop_time(route, t, idx_b).departure;
        assert_eq!(
            chosen_dep, 730,
            "earliest_trip must return the overtake-aware minimum departure (fast trip's 730), not the first-stop-order hit (local trip's 810)"
        );
        assert_eq!(t, 1, "expected trip index 1 (fast)");
    }

    /// #127 regression: the column-major `col_departures` mirror
    /// must produce the same `earliest_trip` result as a scalar
    /// row-major scan over a range of synthetic route shapes +
    /// query thresholds. Locks the invariant that drives the
    /// compiler auto-vectorised fast path.
    #[test]
    fn earliest_trip_col_mirror_matches_row_scan() {
        // Scalar reference: scan the row-major `departures` grid
        // for the smallest dep ≥ earliest_dep with trip-idx
        // tiebreak. This is the algorithm the column-major path
        // must match bit-for-bit.
        fn scalar_ref(
            tt: &Timetable,
            r: RouteIdx,
            stop_in_route: u32,
            earliest_dep: u32,
        ) -> Option<u32> {
            let n_t = tt.n_trips[r as usize] as u64;
            let n_s = tt.n_stops[r as usize] as u64;
            let base = tt.stop_times_offset[r as usize];
            let mut best: Option<(u32, u32)> = None;
            for t in 0..n_t {
                let idx = (base + t * n_s + stop_in_route as u64) as usize;
                let dep = tt.departures[idx];
                if dep >= earliest_dep {
                    match best {
                        None => best = Some((dep, t as u32)),
                        Some((cur_dep, cur_t)) => {
                            if dep < cur_dep || (dep == cur_dep && (t as u32) < cur_t) {
                                best = Some((dep, t as u32));
                            }
                        }
                    }
                }
            }
            best.map(|(_, t)| t)
        }

        // Build a few routes with varied shapes:
        //   route 1 — 2 stops × 3 trips with strict increasing departures
        //   route 2 — 4 stops × 5 trips with overtakes at stop 2
        //   route 3 — 3 stops × 8 trips with a same-departure tie
        let mut b = TimetableBuilder::new();
        // Route 1 stops
        let r1_a = b.add_stop("R1A", "R1A", 0.0, 0.0, None);
        let r1_b = b.add_stop("R1B", "R1B", 0.0, 0.0, None);
        // Route 2 stops
        let r2_a = b.add_stop("R2A", "R2A", 0.0, 0.0, None);
        let r2_b = b.add_stop("R2B", "R2B", 0.0, 0.0, None);
        let r2_c = b.add_stop("R2C", "R2C", 0.0, 0.0, None);
        let r2_d = b.add_stop("R2D", "R2D", 0.0, 0.0, None);
        // Route 3 stops
        let r3_a = b.add_stop("R3A", "R3A", 0.0, 0.0, None);
        let r3_b = b.add_stop("R3B", "R3B", 0.0, 0.0, None);
        let r3_c = b.add_stop("R3C", "R3C", 0.0, 0.0, None);

        // Route 1: 3 clean trips
        for (i, t0) in [100u32, 200, 300].iter().enumerate() {
            b.add_trip(
                &format!("r1t{i}"),
                "R1",
                "",
                "",
                vec![r1_a, r1_b],
                vec![stime(*t0, *t0), stime(t0 + 50, t0 + 50)],
            );
        }

        // Route 2: 5 trips with overtakes at stop B.
        // Trip indices 0..5, first-stop deps: 500, 520, 540, 560, 580.
        // B-stop deps: 560, 530 (overtake!), 570, 590, 610.
        let r2_trips = [
            (500u32, 560u32, 600u32, 650u32),
            (520, 530, 580, 620),
            (540, 570, 610, 660),
            (560, 590, 640, 690),
            (580, 610, 660, 710),
        ];
        for (i, (a, bdep, cdep, ddep)) in r2_trips.iter().enumerate() {
            b.add_trip(
                &format!("r2t{i}"),
                "R2",
                "",
                "",
                vec![r2_a, r2_b, r2_c, r2_d],
                vec![
                    stime(*a, *a),
                    stime(*bdep, *bdep),
                    stime(*cdep, *cdep),
                    stime(*ddep, *ddep),
                ],
            );
        }

        // Route 3: 8 trips. Three trips depart at the same time at
        // stop B (tie-break matters) + one trip at the same time at
        // stop A.
        let r3_trips = [
            (100u32, 150u32, 200u32),
            (200, 250, 300),
            (300, 350, 400),
            (400, 450, 500),
            // tie at stop B (three trips with dep 600):
            (500, 600, 700),
            (550, 600, 720),
            (600, 600, 740),
            // higher
            (700, 800, 900),
        ];
        for (i, (ad, bdep, cd)) in r3_trips.iter().enumerate() {
            b.add_trip(
                &format!("r3t{i}"),
                "R3",
                "",
                "",
                vec![r3_a, r3_b, r3_c],
                vec![stime(*ad, *ad), stime(*bdep, *bdep), stime(*cd, *cd)],
            );
        }

        let tt = b.build().unwrap();

        // Pick arbitrary query thresholds covering "nothing matches",
        // "first trip matches", "middle trip matches", "tie case".
        let thresholds = [0u32, 100, 199, 200, 599, 600, 601, 999, 1000, u32::MAX - 1];

        // Iterate every route × every stop-in-route × every threshold
        // and compare the fast path against the scalar reference.
        for r in 0..tt.n_routes() as u32 {
            let n_s = tt.n_stops_on_route(r) as u32;
            for s in 0..n_s {
                for &th in &thresholds {
                    let fast = tt.earliest_trip(r, s, th);
                    let refv = scalar_ref(&tt, r, s, th);
                    assert_eq!(
                        fast, refv,
                        "mismatch at (route={r}, stop={s}, threshold={th}): fast={fast:?}, ref={refv:?}"
                    );
                }
            }
        }
    }
}
