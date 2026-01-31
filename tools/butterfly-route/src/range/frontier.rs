//! Base Graph Frontier Extraction
//!
//! Extracts frontier on original EBG edges (real road segments), not CCH shortcuts.
//! This is what we need for meaningful isochrone polygons.

use anyhow::Result;
use std::path::Path;

use crate::formats::{
    EbgNodesFile, FilteredEbgFile, NbgGeoFile,
    EbgNodes, FilteredEbg, NbgGeo,
};
use crate::profile_abi::Mode;

/// A frontier cut point on the base graph
#[derive(Debug, Clone)]
pub struct FrontierCutPoint {
    /// Original EBG node ID (the directed edge that's cut)
    pub ebg_node_id: u32,
    /// Distance from origin to the start of this edge
    pub dist_start: u32,
    /// Traversal cost of this edge
    pub edge_weight: u32,
    /// Fraction along the edge where threshold is crossed (0.0 to 1.0)
    pub cut_fraction: f32,
    /// Interpolated latitude (fixed point, 1e-7 degrees)
    pub lat_fxp: i32,
    /// Interpolated longitude (fixed point, 1e-7 degrees)
    pub lon_fxp: i32,
}

/// A reachable point (interior of isochrone)
#[derive(Debug, Clone)]
pub struct ReachablePoint {
    /// Latitude (fixed point, 1e-7 degrees)
    pub lat_fxp: i32,
    /// Longitude (fixed point, 1e-7 degrees)
    pub lon_fxp: i32,
}

/// A reachable road segment for grid stamping
#[derive(Debug, Clone)]
pub struct ReachableSegment {
    /// Polyline points (lat, lon in fixed point 1e-7 degrees)
    pub points: Vec<(i32, i32)>,
}

/// Base graph frontier extractor
pub struct FrontierExtractor {
    /// Filtered EBG (for ID mapping)
    filtered_ebg: FilteredEbg,
    /// Original EBG nodes (for geometry index)
    ebg_nodes: EbgNodes,
    /// NBG geometry (for polylines)
    nbg_geo: NbgGeo,
    /// Edge weights (original EBG node ID → deciseconds)
    weights: Vec<u32>,
}

impl FrontierExtractor {
    /// Load all required data
    pub fn load(
        filtered_ebg_path: &Path,
        ebg_nodes_path: &Path,
        nbg_geo_path: &Path,
        weights_path: &Path,
    ) -> Result<Self> {
        println!("Loading frontier extractor data...");

        let filtered_ebg = FilteredEbgFile::read(filtered_ebg_path)?;
        println!("  ✓ Filtered EBG: {} nodes", filtered_ebg.n_filtered_nodes);

        let ebg_nodes = EbgNodesFile::read(ebg_nodes_path)?;
        println!("  ✓ EBG nodes: {} nodes", ebg_nodes.n_nodes);

        let nbg_geo = NbgGeoFile::read(nbg_geo_path)?;
        println!("  ✓ NBG geo: {} edges", nbg_geo.n_edges_und);

        let weights = load_weights(weights_path)?;
        println!("  ✓ Weights: {} entries", weights.len());

        Ok(Self {
            filtered_ebg,
            ebg_nodes,
            nbg_geo,
            weights,
        })
    }

    /// Extract frontier cut points from PHAST distances
    ///
    /// For each base edge where dist[edge_start] ≤ T < dist[edge_start] + weight,
    /// compute the cut point.
    ///
    /// Note: phast_dist values are in deciseconds (CCH weight units).
    /// threshold_ms is in milliseconds.
    pub fn extract(&self, phast_dist: &[u32], threshold_ms: u32) -> Vec<FrontierCutPoint> {
        let mut cut_points = Vec::new();

        // Iterate over all filtered nodes (which are base EBG edges)
        for filtered_id in 0..self.filtered_ebg.n_filtered_nodes {
            let dist_ds = phast_dist[filtered_id as usize];

            // Skip unreachable edges
            if dist_ds == u32::MAX {
                continue;
            }

            // Convert PHAST distance from deciseconds to milliseconds
            let dist_ms = (dist_ds as u64 * 100).min(u32::MAX as u64) as u32;

            // Skip edges that start beyond the threshold
            if dist_ms > threshold_ms {
                continue;
            }

            // Get original EBG node ID
            let ebg_node_id = self.filtered_ebg.filtered_to_original[filtered_id as usize];

            // Get edge weight (in deciseconds, convert to ms)
            let weight_ds = self.weights[ebg_node_id as usize];
            if weight_ds == 0 {
                continue; // Inaccessible
            }
            let weight_ms = weight_ds * 100; // deciseconds → milliseconds

            // Check if this edge crosses the threshold
            let dist_end_ms = dist_ms.saturating_add(weight_ms);
            if dist_end_ms <= threshold_ms {
                continue; // Edge fully inside
            }

            // This edge crosses the frontier!
            // Compute cut fraction
            let cut_fraction = if weight_ms > 0 {
                (threshold_ms - dist_ms) as f32 / weight_ms as f32
            } else {
                0.0
            };

            // Get geometry
            let ebg_node = &self.ebg_nodes.nodes[ebg_node_id as usize];
            let geom_idx = ebg_node.geom_idx as usize;

            // Interpolate position along polyline
            let (lat_fxp, lon_fxp) = self.interpolate_position(geom_idx, cut_fraction);

            cut_points.push(FrontierCutPoint {
                ebg_node_id,
                dist_start: dist_ms,
                edge_weight: weight_ms,
                cut_fraction,
                lat_fxp,
                lon_fxp,
            });
        }

        cut_points
    }

    /// Extract all reachable points (interior of isochrone)
    ///
    /// Returns the midpoint of each reachable edge for raster filling.
    ///
    /// Note: phast_dist values are in deciseconds (CCH weight units).
    /// threshold_ms is in milliseconds.
    pub fn extract_reachable(&self, phast_dist: &[u32], threshold_ms: u32) -> Vec<ReachablePoint> {
        let mut points = Vec::new();

        for filtered_id in 0..self.filtered_ebg.n_filtered_nodes {
            let dist_ds = phast_dist[filtered_id as usize];

            // Skip unreachable edges
            if dist_ds == u32::MAX {
                continue;
            }

            // Convert PHAST distance from deciseconds to milliseconds
            let dist_ms = (dist_ds as u64 * 100).min(u32::MAX as u64) as u32;

            // Only include edges fully inside the reachable region
            if dist_ms > threshold_ms {
                continue;
            }

            // Get original EBG node ID
            let ebg_node_id = self.filtered_ebg.filtered_to_original[filtered_id as usize];

            // Get edge weight (in deciseconds, convert to ms)
            let weight_ds = self.weights[ebg_node_id as usize];
            if weight_ds == 0 {
                continue;
            }
            let weight_ms = weight_ds * 100;

            // Only include edges fully inside (not crossing the threshold)
            if dist_ms.saturating_add(weight_ms) > threshold_ms {
                continue;
            }

            // Get geometry - use midpoint of the edge
            let ebg_node = &self.ebg_nodes.nodes[ebg_node_id as usize];
            let geom_idx = ebg_node.geom_idx as usize;
            let (lat_fxp, lon_fxp) = self.interpolate_position(geom_idx, 0.5);

            points.push(ReachablePoint { lat_fxp, lon_fxp });
        }

        points
    }

    /// Extract ONLY frontier segments (edges that cross the threshold boundary)
    ///
    /// This is much smaller than all reachable segments and defines the actual
    /// isochrone boundary. Use this for concave hull polygon generation.
    ///
    /// Returns polylines from start to cut point for each frontier edge.
    pub fn extract_frontier_segments(&self, phast_dist: &[u32], threshold_ms: u32) -> Vec<ReachableSegment> {
        let mut segments = Vec::new();

        for filtered_id in 0..self.filtered_ebg.n_filtered_nodes {
            let dist_ds = phast_dist[filtered_id as usize];

            // Skip unreachable edges
            if dist_ds == u32::MAX {
                continue;
            }

            // Convert PHAST distance from deciseconds to milliseconds
            let dist_ms = (dist_ds as u64 * 100).min(u32::MAX as u64) as u32;

            // Skip edges that start beyond the threshold
            if dist_ms > threshold_ms {
                continue;
            }

            // Get original EBG node ID
            let ebg_node_id = self.filtered_ebg.filtered_to_original[filtered_id as usize];

            // Get edge weight (in deciseconds, convert to ms)
            let weight_ds = self.weights[ebg_node_id as usize];
            if weight_ds == 0 {
                continue;
            }
            let weight_ms = weight_ds * 100;

            // Check if this edge crosses the threshold (frontier edge)
            let dist_end_ms = dist_ms.saturating_add(weight_ms);
            if dist_end_ms <= threshold_ms {
                // Fully reachable - NOT a frontier edge, skip
                continue;
            }

            // This IS a frontier edge - extract from start to cut point
            let geom_idx = self.ebg_nodes.nodes[ebg_node_id as usize].geom_idx as usize;
            let cut_fraction = (threshold_ms - dist_ms) as f32 / weight_ms as f32;
            let points = self.extract_partial_polyline(geom_idx, cut_fraction);
            if !points.is_empty() {
                segments.push(ReachableSegment { points });
            }
        }

        segments
    }

    /// Extract all reachable road segments with their geometry for grid stamping
    ///
    /// Returns polylines for:
    /// - Fully reachable edges: entire polyline
    /// - Frontier edges: from start to cut point
    pub fn extract_reachable_segments(&self, phast_dist: &[u32], threshold_ms: u32) -> Vec<ReachableSegment> {
        let mut segments = Vec::new();

        for filtered_id in 0..self.filtered_ebg.n_filtered_nodes {
            let dist_ds = phast_dist[filtered_id as usize];

            // Skip unreachable edges
            if dist_ds == u32::MAX {
                continue;
            }

            // Convert PHAST distance from deciseconds to milliseconds
            let dist_ms = (dist_ds as u64 * 100).min(u32::MAX as u64) as u32;

            // Skip edges that start beyond the threshold
            if dist_ms > threshold_ms {
                continue;
            }

            // Get original EBG node ID
            let ebg_node_id = self.filtered_ebg.filtered_to_original[filtered_id as usize];

            // Get edge weight (in deciseconds, convert to ms)
            let weight_ds = self.weights[ebg_node_id as usize];
            if weight_ds == 0 {
                continue;
            }
            let weight_ms = weight_ds * 100;

            // Get geometry
            let ebg_node = &self.ebg_nodes.nodes[ebg_node_id as usize];
            let geom_idx = ebg_node.geom_idx as usize;
            let polyline = &self.nbg_geo.polylines[geom_idx];

            if polyline.lat_fxp.is_empty() {
                continue;
            }

            // Determine how much of the edge is reachable
            let dist_end_ms = dist_ms.saturating_add(weight_ms);

            if dist_end_ms <= threshold_ms {
                // Fully reachable: include entire polyline
                let points: Vec<(i32, i32)> = polyline.lat_fxp
                    .iter()
                    .zip(polyline.lon_fxp.iter())
                    .map(|(&lat, &lon)| (lat, lon))
                    .collect();
                segments.push(ReachableSegment { points });
            } else {
                // Frontier edge: include from start to cut point
                let cut_fraction = (threshold_ms - dist_ms) as f32 / weight_ms as f32;
                let points = self.extract_partial_polyline(geom_idx, cut_fraction);
                if !points.is_empty() {
                    segments.push(ReachableSegment { points });
                }
            }
        }

        segments
    }

    /// Extract partial polyline from start to given fraction
    fn extract_partial_polyline(&self, geom_idx: usize, fraction: f32) -> Vec<(i32, i32)> {
        let polyline = &self.nbg_geo.polylines[geom_idx];
        let n_pts = polyline.lat_fxp.len();

        if n_pts == 0 || fraction <= 0.0 {
            return vec![];
        }

        if n_pts == 1 {
            return vec![(polyline.lat_fxp[0], polyline.lon_fxp[0])];
        }

        if fraction >= 1.0 {
            return polyline.lat_fxp
                .iter()
                .zip(polyline.lon_fxp.iter())
                .map(|(&lat, &lon)| (lat, lon))
                .collect();
        }

        // Find the segment where the cut occurs
        let n_segments = n_pts - 1;
        let segment_frac = fraction * n_segments as f32;
        let segment_idx = (segment_frac.floor() as usize).min(n_segments - 1);
        let local_frac = segment_frac - segment_idx as f32;

        // Include all points up to and including the start of the cut segment
        let mut points: Vec<(i32, i32)> = polyline.lat_fxp[..=segment_idx]
            .iter()
            .zip(polyline.lon_fxp[..=segment_idx].iter())
            .map(|(&lat, &lon)| (lat, lon))
            .collect();

        // Add the interpolated cut point
        if local_frac > 0.0 && segment_idx + 1 < n_pts {
            let lat1 = polyline.lat_fxp[segment_idx];
            let lon1 = polyline.lon_fxp[segment_idx];
            let lat2 = polyline.lat_fxp[segment_idx + 1];
            let lon2 = polyline.lon_fxp[segment_idx + 1];

            let lat = lat1 + ((lat2 - lat1) as f32 * local_frac) as i32;
            let lon = lon1 + ((lon2 - lon1) as f32 * local_frac) as i32;
            points.push((lat, lon));
        }

        points
    }

    /// Interpolate position along a polyline
    fn interpolate_position(&self, geom_idx: usize, fraction: f32) -> (i32, i32) {
        let polyline = &self.nbg_geo.polylines[geom_idx];
        let n_pts = polyline.lat_fxp.len();

        if n_pts == 0 {
            return (0, 0);
        }

        if n_pts == 1 || fraction <= 0.0 {
            return (polyline.lat_fxp[0], polyline.lon_fxp[0]);
        }

        if fraction >= 1.0 {
            return (polyline.lat_fxp[n_pts - 1], polyline.lon_fxp[n_pts - 1]);
        }

        // For multi-point polylines, we need to find which segment the fraction falls on
        // Simple approach: assume uniform distribution along segments
        // (Better: use actual segment lengths, but this is a reasonable approximation)

        let n_segments = n_pts - 1;
        let segment_frac = fraction * n_segments as f32;
        let segment_idx = (segment_frac.floor() as usize).min(n_segments - 1);
        let local_frac = segment_frac - segment_idx as f32;

        let lat1 = polyline.lat_fxp[segment_idx];
        let lon1 = polyline.lon_fxp[segment_idx];
        let lat2 = polyline.lat_fxp[segment_idx + 1];
        let lon2 = polyline.lon_fxp[segment_idx + 1];

        let lat = lat1 + ((lat2 - lat1) as f32 * local_frac) as i32;
        let lon = lon1 + ((lon2 - lon1) as f32 * local_frac) as i32;

        (lat, lon)
    }

    /// Get number of filtered nodes
    pub fn n_filtered_nodes(&self) -> u32 {
        self.filtered_ebg.n_filtered_nodes
    }
}

/// Load weights from w.*.u32 file
fn load_weights(path: &Path) -> Result<Vec<u32>> {
    use std::fs::File;
    use std::io::{BufReader, Read};

    let mut reader = BufReader::new(File::open(path)?);

    // Skip header (32 bytes)
    let mut header = [0u8; 32];
    reader.read_exact(&mut header)?;

    // Read count from header
    let count = u32::from_le_bytes([header[8], header[9], header[10], header[11]]) as usize;

    // Read weights
    let mut weights = Vec::with_capacity(count);
    for _ in 0..count {
        let mut buf = [0u8; 4];
        reader.read_exact(&mut buf)?;
        weights.push(u32::from_le_bytes(buf));
    }

    Ok(weights)
}

/// Run frontier extraction from command line
pub fn run_frontier_extraction(
    filtered_ebg_path: &Path,
    ebg_nodes_path: &Path,
    nbg_geo_path: &Path,
    weights_path: &Path,
    phast_dist: &[u32],
    threshold: u32,
    mode: Mode,
) -> Result<Vec<FrontierCutPoint>> {
    let mode_name = match mode {
        Mode::Car => "car",
        Mode::Bike => "bike",
        Mode::Foot => "foot",
    };

    println!("\n🔍 Base Graph Frontier Extraction ({} mode)", mode_name);
    println!("  Threshold: {} ms ({:.1} min)", threshold, threshold as f64 / 60_000.0);

    let extractor = FrontierExtractor::load(
        filtered_ebg_path,
        ebg_nodes_path,
        nbg_geo_path,
        weights_path,
    )?;

    println!("\nExtracting frontier...");
    let cut_points = extractor.extract(phast_dist, threshold);

    println!("\n=== FRONTIER RESULTS ===");
    println!("  Cut points: {}", cut_points.len());

    if !cut_points.is_empty() {
        // Show some statistics
        let avg_fraction: f32 = cut_points.iter().map(|p| p.cut_fraction).sum::<f32>()
            / cut_points.len() as f32;
        println!("  Avg cut fraction: {:.3}", avg_fraction);

        // Geographic bounds
        let min_lat = cut_points.iter().map(|p| p.lat_fxp).min().unwrap();
        let max_lat = cut_points.iter().map(|p| p.lat_fxp).max().unwrap();
        let min_lon = cut_points.iter().map(|p| p.lon_fxp).min().unwrap();
        let max_lon = cut_points.iter().map(|p| p.lon_fxp).max().unwrap();

        println!("  Lat range: {:.6} to {:.6}",
                 min_lat as f64 / 1e7, max_lat as f64 / 1e7);
        println!("  Lon range: {:.6} to {:.6}",
                 min_lon as f64 / 1e7, max_lon as f64 / 1e7);
    }

    Ok(cut_points)
}

/// Export frontier cut points to GeoJSON for visualization
pub fn export_geojson(cut_points: &[FrontierCutPoint], output_path: &Path) -> Result<()> {
    use std::fs::File;
    use std::io::Write;

    let mut file = File::create(output_path)?;

    writeln!(file, r#"{{"type": "FeatureCollection", "features": ["#)?;

    for (i, pt) in cut_points.iter().enumerate() {
        let lat = pt.lat_fxp as f64 / 1e7;
        let lon = pt.lon_fxp as f64 / 1e7;

        if i > 0 {
            write!(file, ",")?;
        }

        writeln!(file, r#"{{"type": "Feature", "geometry": {{"type": "Point", "coordinates": [{:.7}, {:.7}]}}, "properties": {{"ebg_id": {}, "dist_ms": {}, "weight_ms": {}, "cut_frac": {:.3}}}}}"#,
                 lon, lat, pt.ebg_node_id, pt.dist_start, pt.edge_weight, pt.cut_fraction)?;
    }

    writeln!(file, "]}}")?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cut_fraction() {
        // dist=50, weight=100, threshold=75 → fraction = (75-50)/100 = 0.25
        let dist = 50u32;
        let weight = 100u32;
        let threshold = 75u32;
        let fraction = (threshold - dist) as f32 / weight as f32;
        assert!((fraction - 0.25).abs() < 0.001);
    }
}
