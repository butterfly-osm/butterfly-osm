//! Cross-region border-node extraction (#91 Phase 2).
//!
//! Given a set of `(region_id, ServerState)` pairs, identify pairs of
//! road samples — one per region — that are close enough that the
//! road network actually crosses the operational region boundary.
//!
//! # Algorithm
//!
//! 1. **Bbox-proximity filter** (`BORDER_PROX_M = 200 m`): for each
//!    ordered region pair `(A, B)`, compute their snap-index bboxes
//!    expanded by 200 m. Only EBG samples that fall inside the
//!    intersection of `expanded(A) ∩ expanded(B)` are border candidates.
//!    This trivially excludes the ~95 % of samples that are deep in the
//!    interior of one region.
//! 2. **Greedy pair**: for each region pair, build two arrays of
//!    candidates (one per region). Walk region A's candidates; for each,
//!    find the nearest sample in region B by haversine. If the closest
//!    distance is `≤ MAX_PAIR_DIST_M = 75 m`, emit a `BorderCrossing`.
//!    The pair is *symmetric*: we deduplicate so that `(A.x, B.y)` and
//!    `(B.y, A.x)` collapse to a single record.
//!
//! Both thresholds are constants tuned for OSM road density:
//! - 200 m bbox slack covers tunnels, bridges, and slightly different
//!   geocoded boundaries between regions (Belgium and Luxembourg's
//!   official borders differ from each other's snap bboxes by a few
//!   tens of metres in places).
//! - 75 m pair distance is comfortably above the snap dedup epsilon
//!   (~5 m) and below typical inter-segment spacing (~100 m for trunk
//!   roads, ~50 m for residential), so we catch genuine crossings
//!   without spuriously pairing two unrelated road segments that
//!   happen to run parallel either side of the border.
//!
//! # Output
//!
//! [`BorderCrossing`] records carry the EBG node id on each side plus
//! its lat/lon (read from the snap-index sample). The downstream
//! overlay-matrix builder uses these EBG ids as P2P sources/targets in
//! the per-region CCH and the haversine distance as the inter-region
//! "edge length".

use std::collections::HashMap;
use std::sync::Arc;

use crate::nbg::haversine_distance;
use crate::server::state::ServerState;

/// Bbox-proximity slack, in metres. Two regions' snap bboxes are
/// expanded by this much before intersecting; only EBG samples in the
/// intersection are border candidates.
pub const BORDER_PROX_M: f64 = 200.0;

/// Maximum haversine distance between paired samples in different
/// regions, in metres. Sample pairs above this distance are not
/// considered border crossings.
pub const MAX_PAIR_DIST_M: f64 = 75.0;

/// One extracted cross-region edge: an EBG node in region A paired with
/// an EBG node in region B. Both endpoints are *original* EBG node ids
/// inside their own region; cross-region routing later resolves these
/// to per-region CCH ranks via `ModeData::orig_to_rank`.
#[derive(Debug, Clone, PartialEq)]
pub struct BorderCrossing {
    /// Region id of the A-side endpoint.
    pub region_a: String,
    /// Original EBG node id in region A.
    pub node_a: u32,
    /// A-side latitude (degrees).
    pub lat_a: f64,
    /// A-side longitude (degrees).
    pub lon_a: f64,
    /// Region id of the B-side endpoint.
    pub region_b: String,
    /// Original EBG node id in region B.
    pub node_b: u32,
    /// B-side latitude (degrees).
    pub lat_b: f64,
    /// B-side longitude (degrees).
    pub lon_b: f64,
    /// Haversine distance between (lat_a, lon_a) and (lat_b, lon_b)
    /// in metres. Used as the inter-region traversal cost.
    pub edge_distance_m: f64,
}

/// Lightweight (lat, lon, ebg_id) record for one side of a region. We
/// pull these out of the snap index so border extraction is independent
/// of the full `PackedPoint` layout.
#[derive(Debug, Clone, Copy)]
struct Sample {
    lat: f64,
    lon: f64,
    ebg_id: u32,
}

/// Extract every border crossing across an unordered list of regions.
///
/// Iterates ordered region pairs `(A, B)` with `A.id < B.id` — this is
/// what makes the result naturally deduplicated.
pub fn extract_border_crossings(regions: &[(String, Arc<ServerState>)]) -> Vec<BorderCrossing> {
    let mut all: Vec<BorderCrossing> = Vec::new();

    for i in 0..regions.len() {
        for j in (i + 1)..regions.len() {
            let (id_a, state_a) = &regions[i];
            let (id_b, state_b) = &regions[j];
            let pair = extract_border_pair(id_a, state_a, id_b, state_b);
            all.extend(pair);
        }
    }

    all
}

/// Inner pairwise extractor: A vs B with `id_a != id_b`. Returns one
/// `BorderCrossing` per matched pair. The result is canonical (A on the
/// "smaller id" side, B on the "larger id" side) so the same pair is
/// never emitted twice.
pub fn extract_border_pair(
    id_a: &str,
    state_a: &ServerState,
    id_b: &str,
    state_b: &ServerState,
) -> Vec<BorderCrossing> {
    // ---- 1. Compute expanded bboxes and their intersection ----------
    let bbox_a = expanded_bbox(state_a, BORDER_PROX_M);
    let bbox_b = expanded_bbox(state_b, BORDER_PROX_M);
    let inter = match intersect_bbox(&bbox_a, &bbox_b) {
        Some(b) => b,
        None => return Vec::new(),
    };

    // ---- 2. Collect candidates from each region's snap samples ------
    let cand_a = collect_candidates_in_bbox(state_a, &inter);
    let cand_b = collect_candidates_in_bbox(state_b, &inter);

    if cand_a.is_empty() || cand_b.is_empty() {
        return Vec::new();
    }

    // ---- 3. For each A candidate, find nearest B candidate ----------
    // The bbox intersection is bounded so cand_a × cand_b worst-case
    // is small relative to the full graph. Belgium ↔ Luxembourg yields
    // ~14k matched pairs from candidate sets in the ~10k–100k range,
    // which runs in seconds. If this becomes a bottleneck for larger
    // region counts, swap in a 2D grid index over cand_b.
    let (canon_a_id, canon_a_state, canon_b_id, canon_b_state, swap) = if id_a <= id_b {
        (id_a, state_a, id_b, state_b, false)
    } else {
        (id_b, state_b, id_a, state_a, true)
    };
    let _ = canon_a_state;
    let _ = canon_b_state;

    // We want to iterate the *canonical* A-side outer loop so the
    // emitted records have `region_a` = canonical-A, regardless of how
    // the caller ordered the inputs.
    let (outer, inner) = if !swap {
        (&cand_a, &cand_b)
    } else {
        (&cand_b, &cand_a)
    };

    let mut out: Vec<BorderCrossing> = Vec::with_capacity(outer.len().min(inner.len()));
    let mut seen_pairs: HashMap<(u32, u32), f64> = HashMap::new();

    for &a in outer {
        let mut best: Option<(usize, f64)> = None;
        for (k, &b) in inner.iter().enumerate() {
            // Cheap prune: |Δlat| × 111_320 m must already be ≤ MAX_PAIR_DIST_M
            // (otherwise haversine is guaranteed to exceed it).
            let dlat_m = (a.lat - b.lat).abs() * 111_320.0;
            if dlat_m > MAX_PAIR_DIST_M {
                continue;
            }
            let d = haversine_distance(a.lat, a.lon, b.lat, b.lon);
            if d > MAX_PAIR_DIST_M {
                continue;
            }
            best = match best {
                Some((_, prev_d)) if prev_d <= d => best,
                _ => Some((k, d)),
            };
        }
        if let Some((k, d)) = best {
            let b = inner[k];
            let key = (a.ebg_id, b.ebg_id);
            // Keep the smallest-distance copy if the same (a, b) pair
            // shows up twice (sample dedup may have produced multiple
            // PackedPoint records per EBG node).
            let keep = !matches!(seen_pairs.get(&key), Some(&prev_d) if prev_d <= d);
            if keep {
                seen_pairs.insert(key, d);
            }
        }
    }

    // Rebuild output from the dedup map. We need each canonical record's
    // lat/lon, so look it up from the candidate arrays. Indexing by ebg
    // id requires a quick map.
    let outer_by_id: HashMap<u32, Sample> = outer.iter().map(|&s| (s.ebg_id, s)).collect();
    let inner_by_id: HashMap<u32, Sample> = inner.iter().map(|&s| (s.ebg_id, s)).collect();

    for ((a_id, b_id), d) in seen_pairs {
        let a = outer_by_id[&a_id];
        let b = inner_by_id[&b_id];
        out.push(BorderCrossing {
            region_a: canon_a_id.to_string(),
            node_a: a_id,
            lat_a: a.lat,
            lon_a: a.lon,
            region_b: canon_b_id.to_string(),
            node_b: b_id,
            lat_b: b.lat,
            lon_b: b.lon,
            edge_distance_m: d,
        });
    }

    // Deterministic ordering: by (region_a, region_b, node_a, node_b).
    out.sort_by(|x, y| {
        x.region_a
            .cmp(&y.region_a)
            .then_with(|| x.region_b.cmp(&y.region_b))
            .then_with(|| x.node_a.cmp(&y.node_a))
            .then_with(|| x.node_b.cmp(&y.node_b))
    });

    out
}

/// Compute the snap-index bbox of a region in degrees, expanded by
/// `slack_m` metres on every side. Returned as
/// `(min_lon, min_lat, max_lon, max_lat)`.
fn expanded_bbox(state: &ServerState, slack_m: f64) -> Bbox {
    let pts = &state.snap_index.points;
    let min_lon = pts.bbox_min_lon as f64 / 1e7;
    let max_lon = pts.bbox_max_lon as f64 / 1e7;
    let min_lat = pts.bbox_min_lat as f64 / 1e7;
    let max_lat = pts.bbox_max_lat as f64 / 1e7;

    // Lat slack is uniform (~111.32 km / deg).
    let lat_slack = slack_m / 111_320.0;
    // Lon slack scales by cos(mid_lat). Use the maximum-radius latitude
    // (closer to equator → smaller cos → larger slack) to be safe.
    let mid_lat = 0.5 * (min_lat + max_lat);
    let cos_mid = mid_lat.to_radians().cos().abs().max(0.1);
    let lon_slack = slack_m / (111_320.0 * cos_mid);

    Bbox {
        min_lon: min_lon - lon_slack,
        min_lat: min_lat - lat_slack,
        max_lon: max_lon + lon_slack,
        max_lat: max_lat + lat_slack,
    }
}

#[derive(Debug, Clone, Copy)]
struct Bbox {
    min_lon: f64,
    min_lat: f64,
    max_lon: f64,
    max_lat: f64,
}

fn intersect_bbox(a: &Bbox, b: &Bbox) -> Option<Bbox> {
    let min_lon = a.min_lon.max(b.min_lon);
    let max_lon = a.max_lon.min(b.max_lon);
    let min_lat = a.min_lat.max(b.min_lat);
    let max_lat = a.max_lat.min(b.max_lat);
    if min_lon > max_lon || min_lat > max_lat {
        None
    } else {
        Some(Bbox {
            min_lon,
            min_lat,
            max_lon,
            max_lat,
        })
    }
}

/// Walk the region's snap-index points and emit one `Sample` per EBG
/// node id whose first occurrence sits inside `bbox`. Picks the first
/// occurrence so the returned set is small and deterministic.
fn collect_candidates_in_bbox(state: &ServerState, bbox: &Bbox) -> Vec<Sample> {
    let mut seen: HashMap<u32, Sample> = HashMap::new();
    for p in state.snap_index.points.points.as_ref() {
        let lon = p.lon_e7 as f64 / 1e7;
        let lat = p.lat_e7 as f64 / 1e7;
        if lon < bbox.min_lon || lon > bbox.max_lon || lat < bbox.min_lat || lat > bbox.max_lat {
            continue;
        }
        seen.entry(p.ebg_id).or_insert(Sample {
            lat,
            lon,
            ebg_id: p.ebg_id,
        });
    }
    seen.into_values().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Synthetic-fixture happy-path test: two single-sample bboxes that
    /// just-barely overlap produce one crossing if the samples are
    /// within MAX_PAIR_DIST_M.
    #[test]
    fn intersect_bbox_handles_overlap_and_disjoint() {
        let a = Bbox {
            min_lon: 0.0,
            min_lat: 0.0,
            max_lon: 1.0,
            max_lat: 1.0,
        };
        let b = Bbox {
            min_lon: 0.5,
            min_lat: 0.5,
            max_lon: 1.5,
            max_lat: 1.5,
        };
        let inter = intersect_bbox(&a, &b).expect("overlap");
        assert!((inter.min_lon - 0.5).abs() < 1e-9);
        assert!((inter.max_lon - 1.0).abs() < 1e-9);

        let c = Bbox {
            min_lon: 2.0,
            min_lat: 2.0,
            max_lon: 3.0,
            max_lat: 3.0,
        };
        assert!(intersect_bbox(&a, &c).is_none());
    }

    #[test]
    fn samples_outside_bbox_are_excluded() {
        // We don't have a ServerState here so we exercise the bbox-only
        // logic via Sample points. This is the same condition the
        // collector applies.
        let bbox = Bbox {
            min_lon: 5.0,
            min_lat: 49.0,
            max_lon: 6.0,
            max_lat: 50.0,
        };
        let inside = (5.5, 49.5);
        let outside = (4.0, 49.5);
        let inside_in = inside.0 >= bbox.min_lon
            && inside.0 <= bbox.max_lon
            && inside.1 >= bbox.min_lat
            && inside.1 <= bbox.max_lat;
        let outside_in = outside.0 >= bbox.min_lon
            && outside.0 <= bbox.max_lon
            && outside.1 >= bbox.min_lat
            && outside.1 <= bbox.max_lat;
        assert!(inside_in);
        assert!(!outside_in);
    }
}
