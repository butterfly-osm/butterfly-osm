//! Spatial index for snapping coordinates to EBG nodes

use rstar::{PointDistance, RTree, RTreeObject, AABB};

use crate::formats::{EbgNodes, NbgGeo};

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
    pub fn snap(&self, lon: f64, lat: f64, mask: &[u64], k: usize) -> Option<u32> {
        // Find K nearest candidates
        let candidates = self.tree.nearest_neighbor_iter(&[lon, lat]).take(k * 10);

        // Return first accessible one
        for point in candidates {
            let word = point.ebg_id as usize / 64;
            let bit = point.ebg_id as usize % 64;
            if word < mask.len() && (mask[word] & (1u64 << bit)) != 0 {
                return Some(point.ebg_id);
            }
        }

        None
    }

    /// Find K nearest accessible EBG nodes
    pub fn snap_k(&self, lon: f64, lat: f64, mask: &[u64], k: usize) -> Vec<u32> {
        let mut result = Vec::with_capacity(k);
        let candidates = self.tree.nearest_neighbor_iter(&[lon, lat]).take(k * 10);

        for point in candidates {
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
