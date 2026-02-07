//! Spatial index for snapping coordinates to EBG nodes

use rstar::{PointDistance, RTree, RTreeObject, AABB};

use crate::formats::{EbgNodes, NbgGeo};

/// Maximum snap distance in meters (5km)
/// Points further than this from any road will fail to snap
const MAX_SNAP_DISTANCE_M: f64 = 5000.0;

/// Approximate meters per degree at Belgian latitudes (~50°N)
const METERS_PER_DEG_LAT: f64 = 111_000.0;
const METERS_PER_DEG_LON_AT_50: f64 = 71_400.0; // 111km * cos(50°)

/// Point with EBG node ID for R-tree
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct IndexedPoint {
    pub coords: [f64; 2], // [lon, lat]
    pub ebg_id: u32,
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
    /// Build spatial index from EBG nodes
    pub fn build(ebg_nodes: &EbgNodes, nbg_geo: &NbgGeo) -> Self {
        let mut points = Vec::with_capacity(ebg_nodes.n_nodes as usize);

        for (ebg_id, node) in ebg_nodes.nodes.iter().enumerate() {
            // Get geometry from NBG
            let geom_idx = node.geom_idx as usize;
            if geom_idx >= nbg_geo.polylines.len() {
                continue;
            }

            let polyline = &nbg_geo.polylines[geom_idx];
            if polyline.lat_fxp.is_empty() {
                continue;
            }

            // Use midpoint of edge as representative point
            let mid_idx = polyline.lat_fxp.len() / 2;
            let lon = polyline.lon_fxp[mid_idx] as f64 / 1e7;
            let lat = polyline.lat_fxp[mid_idx] as f64 / 1e7;

            points.push(IndexedPoint {
                coords: [lon, lat],
                ebg_id: ebg_id as u32,
            });
        }

        Self {
            tree: RTree::bulk_load(points),
        }
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

    /// Find K nearest accessible EBG nodes within max snap distance
    pub fn snap_k(&self, lon: f64, lat: f64, mask: &[u64], k: usize) -> Vec<u32> {
        let mut result = Vec::with_capacity(k);

        for point in self.tree.nearest_neighbor_iter(&[lon, lat]) {
            // Check distance in meters
            let dist_m = Self::distance_meters(lon, lat, point.coords[0], point.coords[1]);
            if dist_m > MAX_SNAP_DISTANCE_M {
                break; // All subsequent candidates will be even further
            }

            let word = point.ebg_id as usize / 64;
            let bit = point.ebg_id as usize % 64;
            if word < mask.len() && (mask[word] & (1u64 << bit)) != 0 {
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

    /// Find K nearest accessible EBG nodes with full info
    /// Returns Vec<(ebg_id, snapped_lon, snapped_lat, distance_m)> sorted by meter distance
    pub fn snap_k_with_info(
        &self,
        lon: f64,
        lat: f64,
        mask: &[u64],
        k: usize,
    ) -> Vec<(u32, f64, f64, f64)> {
        let mut result = Vec::with_capacity(k);

        for point in self.tree.nearest_neighbor_iter(&[lon, lat]) {
            let dist_m = Self::distance_meters(lon, lat, point.coords[0], point.coords[1]);
            if dist_m > MAX_SNAP_DISTANCE_M {
                break;
            }

            let word = point.ebg_id as usize / 64;
            let bit = point.ebg_id as usize % 64;
            if word < mask.len() && (mask[word] & (1u64 << bit)) != 0 {
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
}
