//! Spatial index for snapping coordinates to EBG nodes

use rstar::{AABB, PointDistance, RTree, RTreeObject};

use crate::formats::{EbgNodes, NbgGeo};

/// Maximum snap distance in meters (5km)
/// Points further than this from any road will fail to snap
const MAX_SNAP_DISTANCE_M: f64 = 5000.0;

/// Approximate meters per degree at Belgian latitudes (~50°N)
const METERS_PER_DEG_LAT: f64 = 111_000.0;
const METERS_PER_DEG_LON_AT_50: f64 = 71_400.0; // 111km * cos(50°)

/// Point with EBG node ID and bearing for R-tree
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct IndexedPoint {
    pub coords: [f64; 2], // [lon, lat]
    pub ebg_id: u32,
    pub bearing: u16, // edge bearing in degrees (0=North, clockwise)
}

impl RTreeObject for IndexedPoint {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        AABB::from_point(self.coords)
    }
}

impl PointDistance for IndexedPoint {
    fn distance_2(&self, point: &[f64; 2]) -> f64 {
        let dx = self.coords[0] - point[0];
        let dy = self.coords[1] - point[1];
        dx * dx + dy * dy
    }

    fn contains_point(&self, point: &[f64; 2]) -> bool {
        self.coords == *point
    }
}

/// Spatial index over EBG nodes
pub struct SpatialIndex {
    tree: RTree<IndexedPoint>,
}

impl SpatialIndex {
    /// Build spatial index from EBG nodes (global — includes every node,
    /// used by legacy callers that snap with a mode mask rejection loop).
    pub fn build(ebg_nodes: &EbgNodes, nbg_geo: &NbgGeo) -> Self {
        Self::build_inner(ebg_nodes, nbg_geo, None)
    }

    /// Build a spatial index that contains **only nodes passing the
    /// given mode mask**. This lets callers avoid the pathological
    /// rejection-loop behavior of `snap()` in dense pedestrianised
    /// areas where the global R-tree is dominated by foot-only nodes
    /// and the car-accessible nearest is buried many candidates deep.
    ///
    /// See issue #116.
    pub fn build_filtered(ebg_nodes: &EbgNodes, nbg_geo: &NbgGeo, mask: &[u64]) -> Self {
        Self::build_inner(ebg_nodes, nbg_geo, Some(mask))
    }

    fn build_inner(ebg_nodes: &EbgNodes, nbg_geo: &NbgGeo, mask: Option<&[u64]>) -> Self {
        // Index polyline vertices on a coarse grid (~50 m) instead of
        // just one midpoint per edge.
        //
        // Why: consolidated NBG edges can span hundreds of metres between
        // decision nodes (e.g. Chemin de Bomal in Jodoigne is a single
        // 644 m unclassified edge). Indexing only the midpoint left a snap
        // gap of >200 m along the rest of the edge, and a query near the
        // endpoint snapped to a *different* edge entirely — producing 5 km
        // routes for 2 km trips (#88).
        //
        // The earlier fix used a 5 m dedup epsilon which kept practically
        // every polyline vertex; on Belgium that ballooned the per-mode
        // R-tree to ~20 M points × 4 modes ≈ 30 GB RAM and turned a
        // 50 s server start into 16+ minutes. The 50 m epsilon is the
        // sweet spot: short edges (the ~30 m Belgian average) keep their
        // single midpoint with no regression, while long edges get one
        // indexed point every ~50 m so the worst-case snap gap stays
        // bounded at ~25 m on either side. Memory growth is ~1.3x.
        let mut points = Vec::with_capacity(ebg_nodes.n_nodes as usize);

        // Squared distance threshold below which two consecutive vertices
        // are considered the same indexed point (~50 m at Belgian
        // latitudes). Uses the same lat/lon meter approximation as
        // `distance_meters`.
        let dedup_eps_m: f64 = 50.0;
        let dedup_eps2 = dedup_eps_m * dedup_eps_m;

        for (ebg_id, node) in ebg_nodes.nodes.iter().enumerate() {
            // Per-mode filter: skip nodes whose mask bit is clear.
            if let Some(mask_slice) = mask {
                let word = ebg_id / 64;
                let bit = ebg_id % 64;
                if word >= mask_slice.len() || (mask_slice[word] & (1u64 << bit)) == 0 {
                    continue;
                }
            }

            // Get geometry from NBG
            let geom_idx = node.geom_idx as usize;
            if geom_idx >= nbg_geo.polylines.len() {
                continue;
            }

            let polyline = &nbg_geo.polylines[geom_idx];
            if polyline.lat_fxp.is_empty() {
                continue;
            }

            let n_pts = polyline.lat_fxp.len();

            // Compute edge bearing from first to last point (0=North, clockwise).
            // Same bearing applies to every indexed point along the edge in
            // this direction — the bearing filter is a per-edge property.
            let bearing = if n_pts >= 2 {
                let lat1 = polyline.lat_fxp[0] as f64 / 1e7;
                let lon1 = polyline.lon_fxp[0] as f64 / 1e7;
                let lat2 = polyline.lat_fxp[n_pts - 1] as f64 / 1e7;
                let lon2 = polyline.lon_fxp[n_pts - 1] as f64 / 1e7;
                Self::compute_bearing(lat1, lon1, lat2, lon2)
            } else {
                0
            };

            // Insert every polyline vertex, deduplicating co-located ones.
            let mut last_kept_lon = f64::INFINITY;
            let mut last_kept_lat = f64::INFINITY;
            for i in 0..n_pts {
                let lon = polyline.lon_fxp[i] as f64 / 1e7;
                let lat = polyline.lat_fxp[i] as f64 / 1e7;

                // Always keep the first vertex; skip subsequent ones that are
                // near the previous kept vertex.
                if last_kept_lon.is_finite() {
                    let dlat = (lat - last_kept_lat) * METERS_PER_DEG_LAT;
                    let dlon = (lon - last_kept_lon) * METERS_PER_DEG_LON_AT_50;
                    if dlat * dlat + dlon * dlon < dedup_eps2 {
                        // Force-keep the last vertex even if close, so the
                        // edge is represented at both ends.
                        if i + 1 < n_pts {
                            continue;
                        }
                    }
                }

                points.push(IndexedPoint {
                    coords: [lon, lat],
                    ebg_id: ebg_id as u32,
                    bearing,
                });
                last_kept_lon = lon;
                last_kept_lat = lat;
            }
        }

        Self {
            tree: RTree::bulk_load(points),
        }
    }

    /// Number of points in the index (for diagnostics and tests).
    pub fn n_indexed(&self) -> usize {
        self.tree.size()
    }

    /// Snap without mode-mask rejection. Safe to call on a
    /// `build_filtered` index because every node in the index is
    /// already mode-accessible by construction — the first
    /// nearest-neighbour hit within `MAX_SNAP_DISTANCE_M` is the
    /// answer, no rejection loop needed.
    pub fn snap_unfiltered(&self, lon: f64, lat: f64) -> Option<u32> {
        let point = self.tree.nearest_neighbor(&[lon, lat])?;
        let dist_m = Self::distance_meters(lon, lat, point.coords[0], point.coords[1]);
        if dist_m > MAX_SNAP_DISTANCE_M {
            return None;
        }
        Some(point.ebg_id)
    }

    /// Compute bearing in degrees (0=North, clockwise) from point 1 to point 2
    fn compute_bearing(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> u16 {
        let dlat_m = (lat2 - lat1) * METERS_PER_DEG_LAT;
        let dlon_m = (lon2 - lon1) * METERS_PER_DEG_LON_AT_50;
        let angle_rad = dlon_m.atan2(dlat_m); // North=0, East=π/2
        let deg = angle_rad.to_degrees();
        ((deg + 360.0) % 360.0) as u16
    }

    /// Check if a candidate bearing is within the specified range of the requested bearing
    fn bearing_matches(candidate: u16, requested: u16, range: u16) -> bool {
        let diff = (candidate as i32 - requested as i32).unsigned_abs() as u16;
        let diff = diff.min(360 - diff); // shortest arc
        diff <= range
    }

    /// Find nearest accessible EBG node for given mode
    /// Returns None if no accessible road within MAX_SNAP_DISTANCE_M
    pub fn snap(&self, lon: f64, lat: f64, mask: &[u64], _k: usize) -> Option<u32> {
        // Iterate through all candidates by distance until we exceed max distance
        // Note: we don't limit by count because pedestrianized areas may have
        // thousands of non-car edges before the nearest car-accessible one
        for point in self.tree.nearest_neighbor_iter(&[lon, lat]) {
            // Check distance in meters
            let dist_m = Self::distance_meters(lon, lat, point.coords[0], point.coords[1]);
            if dist_m > MAX_SNAP_DISTANCE_M {
                // All subsequent candidates will be even further
                return None;
            }

            let word = point.ebg_id as usize / 64;
            let bit = point.ebg_id as usize % 64;
            if word < mask.len() && (mask[word] & (1u64 << bit)) != 0 {
                return Some(point.ebg_id);
            }
        }

        None
    }

    /// Calculate approximate distance in meters between two points
    fn distance_meters(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
        let dlat = (lat2 - lat1) * METERS_PER_DEG_LAT;
        let dlon = (lon2 - lon1) * METERS_PER_DEG_LON_AT_50;
        (dlat * dlat + dlon * dlon).sqrt()
    }

    /// Find K nearest accessible EBG nodes within max snap distance.
    /// Dedupes by ebg_id since the dense vertex index can return the same
    /// edge multiple times (once per polyline vertex).
    pub fn snap_k(&self, lon: f64, lat: f64, mask: &[u64], k: usize) -> Vec<u32> {
        let mut result = Vec::with_capacity(k);
        let mut seen: std::collections::HashSet<u32> = std::collections::HashSet::with_capacity(k);

        for point in self.tree.nearest_neighbor_iter(&[lon, lat]) {
            // Check distance in meters
            let dist_m = Self::distance_meters(lon, lat, point.coords[0], point.coords[1]);
            if dist_m > MAX_SNAP_DISTANCE_M {
                break; // All subsequent candidates will be even further
            }

            let word = point.ebg_id as usize / 64;
            let bit = point.ebg_id as usize % 64;
            if word < mask.len() && (mask[word] & (1u64 << bit)) != 0 && seen.insert(point.ebg_id) {
                result.push(point.ebg_id);
                if result.len() >= k {
                    break;
                }
            }
        }

        result
    }

    /// Snap with distance info for debugging
    /// Returns (ebg_id, snapped_lon, snapped_lat, distance_m)
    pub fn snap_with_info(
        &self,
        lon: f64,
        lat: f64,
        mask: &[u64],
        _k: usize,
    ) -> Option<(u32, f64, f64, f64)> {
        for point in self.tree.nearest_neighbor_iter(&[lon, lat]) {
            let dist_m = Self::distance_meters(lon, lat, point.coords[0], point.coords[1]);
            if dist_m > MAX_SNAP_DISTANCE_M {
                return None;
            }

            let word = point.ebg_id as usize / 64;
            let bit = point.ebg_id as usize % 64;
            if word < mask.len() && (mask[word] & (1u64 << bit)) != 0 {
                return Some((point.ebg_id, point.coords[0], point.coords[1], dist_m));
            }
        }

        None
    }

    /// Find K nearest accessible EBG nodes with full info.
    /// Returns Vec<(ebg_id, snapped_lon, snapped_lat, distance_m)> sorted by
    /// meter distance. Dedupes by ebg_id since the dense vertex index can
    /// return the same edge multiple times (once per polyline vertex);
    /// for each edge only the *closest* indexed vertex is kept.
    pub fn snap_k_with_info(
        &self,
        lon: f64,
        lat: f64,
        mask: &[u64],
        k: usize,
    ) -> Vec<(u32, f64, f64, f64)> {
        let mut result = Vec::with_capacity(k);
        let mut seen: std::collections::HashSet<u32> = std::collections::HashSet::with_capacity(k);

        for point in self.tree.nearest_neighbor_iter(&[lon, lat]) {
            let dist_m = Self::distance_meters(lon, lat, point.coords[0], point.coords[1]);
            if dist_m > MAX_SNAP_DISTANCE_M {
                break;
            }

            let word = point.ebg_id as usize / 64;
            let bit = point.ebg_id as usize % 64;
            if word < mask.len() && (mask[word] & (1u64 << bit)) != 0 && seen.insert(point.ebg_id) {
                result.push((point.ebg_id, point.coords[0], point.coords[1], dist_m));
                if result.len() >= k {
                    break;
                }
            }
        }

        // Sort by meter distance (R-tree orders by degree distance which differs from meters)
        result.sort_by(|a, b| a.3.partial_cmp(&b.3).unwrap_or(std::cmp::Ordering::Equal));
        result
    }

    /// Snap with bearing filter — returns (ebg_id, snapped_lon, snapped_lat, distance_m)
    /// Only returns candidates whose edge bearing is within `range` degrees of `bearing`
    pub fn snap_with_bearing(
        &self,
        lon: f64,
        lat: f64,
        mask: &[u64],
        bearing: u16,
        range: u16,
    ) -> Option<(u32, f64, f64, f64)> {
        for point in self.tree.nearest_neighbor_iter(&[lon, lat]) {
            let dist_m = Self::distance_meters(lon, lat, point.coords[0], point.coords[1]);
            if dist_m > MAX_SNAP_DISTANCE_M {
                return None;
            }

            let word = point.ebg_id as usize / 64;
            let bit = point.ebg_id as usize % 64;
            if word < mask.len()
                && (mask[word] & (1u64 << bit)) != 0
                && Self::bearing_matches(point.bearing, bearing, range)
            {
                return Some((point.ebg_id, point.coords[0], point.coords[1], dist_m));
            }
        }

        None
    }

    /// Public bearing match for testing
    #[cfg(test)]
    pub fn bearing_matches_pub(candidate: u16, requested: u16, range: u16) -> bool {
        Self::bearing_matches(candidate, requested, range)
    }

    /// Get coordinates for an EBG node
    pub fn get_coords(&self, ebg_id: u32, ebg_nodes: &EbgNodes, nbg_geo: &NbgGeo) -> (f64, f64) {
        let node = &ebg_nodes.nodes[ebg_id as usize];
        let geom_idx = node.geom_idx as usize;

        if geom_idx < nbg_geo.polylines.len() {
            let polyline = &nbg_geo.polylines[geom_idx];
            if !polyline.lat_fxp.is_empty() {
                let mid_idx = polyline.lat_fxp.len() / 2;
                return (
                    polyline.lon_fxp[mid_idx] as f64 / 1e7,
                    polyline.lat_fxp[mid_idx] as f64 / 1e7,
                );
            }
        }

        (0.0, 0.0)
    }

    /// Find all indexed polyline vertices within a bounding box.
    /// Returns an iterator of `&IndexedPoint` for vertices that fall within
    /// `[min_lon, min_lat] .. [max_lon, max_lat]`. Note: the index is dense
    /// (one entry per polyline vertex), so the same edge may appear multiple
    /// times in the iterator. Callers must dedupe by `ebg_id` if they care
    /// about distinct edges.
    pub fn edges_in_envelope(
        &self,
        min_lon: f64,
        min_lat: f64,
        max_lon: f64,
        max_lat: f64,
    ) -> impl Iterator<Item = &IndexedPoint> {
        let envelope = AABB::from_corners([min_lon, min_lat], [max_lon, max_lat]);
        self.tree.locate_in_envelope(&envelope)
    }
}

#[cfg(test)]
mod tests {
    //! Pure-math + synthetic-index unit tests. `compute_bearing`,
    //! `bearing_matches` and `distance_meters` are exact functions of their
    //! inputs; the snap tests build a tiny R-tree over hand-placed polyline
    //! vertices (no Belgium data) and assert the meter cutoff, mask rejection
    //! and per-edge dedup.
    use super::*;
    use crate::formats::ArcCow;
    use crate::formats::ebg_nodes::{EbgNode, EbgNodes};
    use crate::formats::nbg_geo::{NbgGeo, PolyLine};

    // --- pure trig / distance ----------------------------------------------

    #[test]
    fn compute_bearing_is_zero_north_and_clockwise_through_the_cardinals() {
        // Bearing convention: 0 = North (increasing lat), clockwise. The four
        // cardinal directions are exact regardless of the lat/lon metre
        // scaling because each has one zero component.
        let n = SpatialIndex::compute_bearing(50.0, 4.0, 51.0, 4.0); // +lat
        let e = SpatialIndex::compute_bearing(50.0, 4.0, 50.0, 5.0); // +lon
        let s = SpatialIndex::compute_bearing(50.0, 4.0, 49.0, 4.0); // -lat
        let w = SpatialIndex::compute_bearing(50.0, 4.0, 50.0, 3.0); // -lon
        assert_eq!(n, 0, "north");
        assert_eq!(e, 90, "east");
        assert_eq!(s, 180, "south");
        assert_eq!(w, 270, "west");
    }

    #[test]
    fn distance_meters_matches_the_per_degree_scaling_and_is_zero_at_a_point() {
        // One degree of latitude = METERS_PER_DEG_LAT; one degree of longitude
        // (at the ~50N constant) = METERS_PER_DEG_LON_AT_50; a point to itself
        // is 0.
        let dlat = SpatialIndex::distance_meters(4.0, 50.0, 4.0, 51.0);
        assert!(
            (dlat - METERS_PER_DEG_LAT).abs() < 1e-6,
            "1 deg lat, got {dlat}"
        );
        let dlon = SpatialIndex::distance_meters(4.0, 50.0, 5.0, 50.0);
        assert!(
            (dlon - METERS_PER_DEG_LON_AT_50).abs() < 1e-6,
            "1 deg lon, got {dlon}"
        );
        assert_eq!(SpatialIndex::distance_meters(4.0, 50.0, 4.0, 50.0), 0.0);
    }

    // --- synthetic index / snap --------------------------------------------

    /// Build an `(EbgNodes, NbgGeo)` from a list of edges, each a list of
    /// (lat_deg, lon_deg) polyline vertices. `geom_idx == edge index`.
    fn synth(edges: &[&[(f64, f64)]]) -> (EbgNodes, NbgGeo) {
        let mut nodes = Vec::new();
        let mut polylines = Vec::new();
        for (i, verts) in edges.iter().enumerate() {
            nodes.push(EbgNode {
                tail_nbg: 0,
                head_nbg: 0,
                geom_idx: i as u32,
                length_m: 100,
                class_bits: 0,
                primary_way: 0,
            });
            polylines.push(PolyLine {
                lat_fxp: verts.iter().map(|v| (v.0 * 1e7).round() as i32).collect(),
                lon_fxp: verts.iter().map(|v| (v.1 * 1e7).round() as i32).collect(),
            });
        }
        let ebg = EbgNodes {
            n_nodes: edges.len() as u32,
            created_unix: 0,
            inputs_sha: [0u8; 32],
            nodes: ArcCow::from_vec(nodes),
        };
        let nbg = NbgGeo {
            n_edges_und: edges.len() as u64,
            edges: Vec::new(), // build_inner only reads polylines
            polylines,
        };
        (ebg, nbg)
    }

    #[test]
    fn snap_returns_the_nearest_edge_and_snap_k_dedups_and_sorts_by_metres() {
        // edge0 ~14 m from the query, edge1 ~700 m. Both < 5 km cutoff.
        let (ebg, nbg) = synth(&[
            &[(50.0, 4.0), (50.0, 4.0005)],  // near
            &[(50.0, 4.01), (50.0, 4.0105)], // ~700 m east
        ]);
        let idx = SpatialIndex::build(&ebg, &nbg);
        let mask = vec![0b11u64]; // both accessible

        assert_eq!(
            idx.snap_unfiltered(4.0002, 50.0),
            Some(0),
            "nearest is edge0"
        );

        let k = idx.snap_k_with_info(4.0002, 50.0, &mask, 8);
        assert_eq!(
            k.len(),
            2,
            "each distinct edge appears exactly once (deduped)"
        );
        assert_eq!(k[0].0, 0, "sorted by metre distance: edge0 first");
        assert_eq!(k[1].0, 1, "edge1 second");
        assert!(k[0].3 < k[1].3, "distances strictly increasing");
        assert!(k[0].3 < 50.0, "edge0 is within ~14 m");
    }

    #[test]
    fn snap_skips_masked_out_edges_via_the_rejection_loop() {
        // Same geometry, but the mask clears edge0 (bit 0). The nearest
        // ACCESSIBLE edge is edge1, so snap must walk past edge0's closer
        // vertices — the #116 rejection loop.
        let (ebg, nbg) = synth(&[
            &[(50.0, 4.0), (50.0, 4.0005)],
            &[(50.0, 4.01), (50.0, 4.0105)],
        ]);
        let idx = SpatialIndex::build(&ebg, &nbg);
        let mask_no_edge0 = vec![0b10u64];
        assert_eq!(idx.snap(4.0002, 50.0, &mask_no_edge0, 8), Some(1));
    }

    #[test]
    fn snap_rejects_points_beyond_the_max_snap_distance() {
        // The only edge sits ~71 km east — well past MAX_SNAP_DISTANCE_M (5 km)
        // — so a query near lon 4.0 must fail to snap.
        let (ebg, nbg) = synth(&[&[(50.0, 5.0), (50.0, 5.0005)]]);
        let idx = SpatialIndex::build(&ebg, &nbg);
        assert_eq!(idx.snap_unfiltered(4.0, 50.0), None);
        assert!(idx.snap_k_with_info(4.0, 50.0, &[0b1u64], 8).is_empty());
    }
}
