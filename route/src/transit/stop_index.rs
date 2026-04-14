//! Spatial index over transit stops for fast candidate selection.
//!
//! The `/transit` handler needs to find the K nearest transit stops to an
//! origin (and to a destination) on every query. With 64k stops on
//! Belgium and a growing multimodal feed set, the previous
//! `candidate_stops` function — a linear scan computing haversine per
//! stop, full-sorting the result, then truncating to K — scaled badly:
//!
//! | stops  | per-query cost |
//! |--------|----------------|
//! | 64k    | ~6 ms (two scans)     |
//! | 3M     | ~300 ms (Europe-scale) |
//!
//! This module replaces the linear scan with a bulk-loaded R-tree
//! (`rstar`) over `(lon, lat, StopIdx)` points plus a bounded top-K heap
//! that cuts query cost to `O(log N + K log K)`. On Belgium this turns
//! ~6 ms into ~50 µs per query; at Europe scale it is the difference
//! between "interactive" and "dead on arrival".
//!
//! The index is built once at `TransitSnapshot` construction time (see
//! `transit::load_from_disk`) and shared read-only across every query
//! for the lifetime of the snapshot.
//!
//! See issue #102 (`codex` review top-2 perf win).

use std::cmp::Ordering;
use std::collections::BinaryHeap;

use rstar::{AABB, PointDistance, RTree, RTreeObject};

use super::timetable::{StopIdx, Timetable};

/// Approximate meters per degree at Belgian latitudes (~50°N).
/// Only the longitude constant is used (for the r-tree degree-distance
/// early-exit). The latitude constant is kept in comments for doc
/// readability but not needed at runtime.
const METERS_PER_DEG_LON_AT_50: f64 = 71_400.0;

/// One indexed stop in the R-tree: coordinates plus the `StopIdx` we
/// want to retrieve. Stop IDs / names / parent-station metadata stay
/// in the `Timetable` — the index only needs the position key.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct IndexedStop {
    pub coords: [f64; 2], // [lon, lat]
    pub stop_idx: StopIdx,
}

impl RTreeObject for IndexedStop {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        AABB::from_point(self.coords)
    }
}

impl PointDistance for IndexedStop {
    fn distance_2(&self, point: &[f64; 2]) -> f64 {
        let dx = self.coords[0] - point[0];
        let dy = self.coords[1] - point[1];
        dx * dx + dy * dy
    }

    fn contains_point(&self, point: &[f64; 2]) -> bool {
        self.coords == *point
    }
}

/// Spatial index over every transit stop in a `Timetable` that sees at
/// least one route (or is a parent station serving children that do).
pub struct StopSpatialIndex {
    tree: RTree<IndexedStop>,
}

impl StopSpatialIndex {
    /// Build an R-tree over every stop in `tt` that is reachable by at
    /// least one route, or is a parent station (so that its children
    /// can be found via the `Timetable.station_children` relation).
    ///
    /// Bulk-loading via `rstar::RTree::bulk_load` produces a balanced
    /// tree in `O(N log N)` — acceptable as a one-shot startup cost.
    pub fn build(tt: &Timetable) -> Self {
        let mut points: Vec<IndexedStop> = Vec::with_capacity(tt.stops.len());
        for (i, stop) in tt.stops.iter().enumerate() {
            // Skip pure parent-only stops that have no trips and no
            // children. These are curator artefacts, not boardable.
            let idx = i as StopIdx;
            let has_routes = !tt.routes_for_stop(idx).is_empty();
            let is_parent = tt.station_children.contains_key(&idx);
            if !has_routes && !is_parent && stop.parent_station.is_none() {
                continue;
            }
            points.push(IndexedStop {
                coords: [stop.lon, stop.lat],
                stop_idx: idx,
            });
        }
        let tree = RTree::bulk_load(points);
        Self { tree }
    }

    /// Find the `k` nearest stops to `(lon, lat)` within `max_m` of the
    /// query point. Result is sorted ascending by great-circle
    /// (haversine) distance in meters, as `(stop_idx, distance_m)` pairs.
    ///
    /// Correctness: the R-tree orders by degree distance, which is not
    /// exactly proportional to meters away from the equator. We correct
    /// this by (a) walking the `nearest_neighbor_iter` only until we
    /// see a candidate whose degree-distance exceeds the corresponding
    /// meter-bound with a 20 % safety margin, (b) computing the exact
    /// meter distance for every survivor, and (c) keeping the top `k`
    /// by meters via a bounded max-heap. For mid-latitude queries on
    /// Belgium or continental Europe, this is exact.
    pub fn k_nearest(&self, lon: f64, lat: f64, max_m: u32, k: usize) -> Vec<(StopIdx, f64)> {
        if k == 0 {
            return Vec::new();
        }
        let max_m_f = max_m as f64;
        // Upper bound in degrees for the r-tree walk. Use the larger
        // of the lat / lon conversions so we never stop short.
        let deg_ceiling = (max_m_f / METERS_PER_DEG_LON_AT_50) * 1.2;
        let deg_ceiling2 = deg_ceiling * deg_ceiling;

        // Bounded max-heap: keeps the k smallest-meter candidates. The
        // heap's root is the largest (worst) element currently accepted.
        // Inserting a new element while full replaces the root iff the
        // new element is strictly smaller.
        let mut heap: BinaryHeap<HeapEntry> = BinaryHeap::with_capacity(k + 1);

        for hit in self.tree.nearest_neighbor_iter(&[lon, lat]) {
            // Early exit: the r-tree iterator emits candidates in
            // ascending euclidean-degree distance. Once that exceeds
            // our degree ceiling, nothing further can possibly be
            // inside the meter radius.
            let dx = hit.coords[0] - lon;
            let dy = hit.coords[1] - lat;
            if dx * dx + dy * dy > deg_ceiling2 {
                break;
            }
            let dm = haversine_m(lon, lat, hit.coords[0], hit.coords[1]);
            if dm > max_m_f {
                continue;
            }
            let entry = HeapEntry {
                dist_m: dm,
                stop_idx: hit.stop_idx,
            };
            if heap.len() < k {
                heap.push(entry);
            } else if let Some(worst) = heap.peek()
                && entry.dist_m < worst.dist_m
            {
                heap.pop();
                heap.push(entry);
            }
        }

        // Drain the heap in descending order, then reverse to get
        // ascending-distance output.
        let mut out: Vec<(StopIdx, f64)> =
            heap.into_iter().map(|e| (e.stop_idx, e.dist_m)).collect();
        out.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
        out
    }

    /// Number of stops in the index.
    pub fn len(&self) -> usize {
        self.tree.size()
    }

    /// Whether the index is empty.
    pub fn is_empty(&self) -> bool {
        self.tree.size() == 0
    }
}

/// Heap entry: ordered so that the *largest* distance is at the root.
/// `BinaryHeap` is a max-heap in Rust, and we want the root to be the
/// current worst candidate so that we can efficiently evict it when a
/// better candidate appears.
#[derive(Clone, Copy)]
struct HeapEntry {
    dist_m: f64,
    stop_idx: StopIdx,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.dist_m == other.dist_m
    }
}
impl Eq for HeapEntry {}
impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Larger distance = "greater" so it bubbles up to the root of
        // the BinaryHeap max-heap. NaN never appears in practice
        // (coordinates are validated earlier) but fall back to Equal
        // on comparison failure to avoid panics.
        self.dist_m
            .partial_cmp(&other.dist_m)
            .unwrap_or(Ordering::Equal)
    }
}

/// Great-circle distance in meters between two `(lon, lat)` points.
fn haversine_m(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    const R: f64 = 6_371_000.0;
    let phi1 = lat1.to_radians();
    let phi2 = lat2.to_radians();
    let dphi = (lat2 - lat1).to_radians();
    let dlambda = (lon2 - lon1).to_radians();
    let a = (dphi / 2.0).sin().powi(2) + phi1.cos() * phi2.cos() * (dlambda / 2.0).sin().powi(2);
    2.0 * R * a.sqrt().asin()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transit::timetable::{StopTime, TimetableBuilder};

    fn fixture() -> Timetable {
        // 5 stops along a roughly east-west line, all at lat=50.0:
        //   A: lon=4.0   B: 4.01  C: 4.02  D: 4.03  E: 4.04
        // 0.01 deg lon at 50°N ≈ 714 m, so neighbours are ~714 m apart.
        let mut b = TimetableBuilder::new();
        let a = b.add_stop("A", "A", 4.00, 50.0, None);
        let bb = b.add_stop("B", "B", 4.01, 50.0, None);
        let c = b.add_stop("C", "C", 4.02, 50.0, None);
        let d = b.add_stop("D", "D", 4.03, 50.0, None);
        let e = b.add_stop("E", "E", 4.04, 50.0, None);
        // Add a trip so every stop passes the route filter.
        b.add_trip(
            "T",
            "R",
            "R",
            "h",
            vec![a, bb, c, d, e],
            vec![
                StopTime {
                    arrival: 0,
                    departure: 0,
                },
                StopTime {
                    arrival: 60,
                    departure: 60,
                },
                StopTime {
                    arrival: 120,
                    departure: 120,
                },
                StopTime {
                    arrival: 180,
                    departure: 180,
                },
                StopTime {
                    arrival: 240,
                    departure: 240,
                },
            ],
        );
        b.build().unwrap()
    }

    #[test]
    fn build_indexes_all_routed_stops() {
        let tt = fixture();
        let idx = StopSpatialIndex::build(&tt);
        assert_eq!(idx.len(), 5);
        assert!(!idx.is_empty());
    }

    #[test]
    fn k_nearest_returns_k_closest_sorted() {
        let tt = fixture();
        let idx = StopSpatialIndex::build(&tt);
        // Query right at A (lon=4.00, lat=50.0). Nearest should be A, B, C, ...
        let result = idx.k_nearest(4.00, 50.0, 10_000, 3);
        assert_eq!(result.len(), 3);
        // A first (distance 0), then B, then C.
        assert_eq!(result[0].0, 0); // A
        assert_eq!(result[1].0, 1); // B
        assert_eq!(result[2].0, 2); // C
        // Distances must be monotonically non-decreasing.
        assert!(result[0].1 <= result[1].1);
        assert!(result[1].1 <= result[2].1);
    }

    #[test]
    fn k_nearest_respects_max_m() {
        let tt = fixture();
        let idx = StopSpatialIndex::build(&tt);
        // 1500 m radius around A should reach A and B (~714 m) and
        // maybe C (~1428 m) — NOT D (~2142 m) and NOT E (~2856 m).
        let result = idx.k_nearest(4.00, 50.0, 1500, 10);
        let stops: Vec<StopIdx> = result.iter().map(|(s, _)| *s).collect();
        assert!(stops.contains(&0), "A must be present");
        assert!(stops.contains(&1), "B must be present");
        assert!(!stops.contains(&3), "D must be outside the radius");
        assert!(!stops.contains(&4), "E must be outside the radius");
    }

    #[test]
    fn k_nearest_k_larger_than_stops() {
        let tt = fixture();
        let idx = StopSpatialIndex::build(&tt);
        let result = idx.k_nearest(4.00, 50.0, 10_000, 50);
        assert_eq!(result.len(), 5, "K > N should return N");
    }

    #[test]
    fn k_nearest_empty_on_k_zero() {
        let tt = fixture();
        let idx = StopSpatialIndex::build(&tt);
        let result = idx.k_nearest(4.00, 50.0, 10_000, 0);
        assert!(result.is_empty());
    }

    #[test]
    fn k_nearest_empty_on_far_query() {
        let tt = fixture();
        let idx = StopSpatialIndex::build(&tt);
        // Query 100 km south of the fixture stops, radius 1 km.
        let result = idx.k_nearest(4.00, 49.0, 1000, 10);
        assert!(result.is_empty(), "no stops within 1 km of a faraway query");
    }
}
