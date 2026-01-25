//! K-Lane Batched Isochrone Generation
//!
//! Processes K isochrone queries efficiently by:
//! 1. Running K-lane batched PHAST (single downward scan for K origins)
//! 2. Extracting reachable segments for each origin (parallel)
//! 3. Generating contour polygons for each origin (parallel)
//!
//! This amortizes the expensive downward scan across K queries.

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use rayon::prelude::*;

use crate::matrix::batched_phast::{BatchedPhastEngine, BatchedPhastResult, K_LANES};
use crate::profile_abi::Mode;
use super::frontier::{FrontierExtractor, ReachableSegment};
use super::contour::{ContourResult, export_contour_geojson};
use super::sparse_contour::{SparseContourConfig, generate_sparse_contour};

/// Result of a batched isochrone query
#[derive(Debug)]
pub struct BatchedIsochroneResult {
    /// Number of origins processed
    pub n_origins: usize,
    /// Contour results for each origin
    pub contours: Vec<ContourResult>,
    /// Statistics
    pub stats: BatchedIsochroneStats,
}

/// Statistics for batched isochrone generation
#[derive(Debug, Default, Clone)]
pub struct BatchedIsochroneStats {
    /// Number of origins processed
    pub n_origins: usize,
    /// PHAST computation time (shared across K)
    pub phast_time_ms: u64,
    /// Segment extraction time (sum across K)
    pub segment_time_ms: u64,
    /// Contour generation time (sum across K)
    pub contour_time_ms: u64,
    /// Total time
    pub total_time_ms: u64,
    /// Total reachable segments across all origins
    pub total_segments: usize,
    /// Average vertices per contour
    pub avg_vertices: usize,
}

/// Batched isochrone engine
pub struct BatchedIsochroneEngine {
    /// Batched PHAST engine for distance computation
    phast: BatchedPhastEngine,
    /// Frontier extractor (shared across queries)
    extractor: Arc<FrontierExtractor>,
    /// Sparse contour config per mode
    config: SparseContourConfig,
    /// Mode
    mode: Mode,
}

impl BatchedIsochroneEngine {
    /// Create batched isochrone engine from file paths
    pub fn load(
        cch_topo_path: &Path,
        cch_weights_path: &Path,
        order_path: &Path,
        filtered_ebg_path: &Path,
        ebg_nodes_path: &Path,
        nbg_geo_path: &Path,
        base_weights_path: &Path,
        mode: Mode,
    ) -> Result<Self> {
        // Load PHAST engine
        let phast = BatchedPhastEngine::load(cch_topo_path, cch_weights_path, order_path)?;

        // Load frontier extractor
        let extractor = FrontierExtractor::load(
            filtered_ebg_path,
            ebg_nodes_path,
            nbg_geo_path,
            base_weights_path,
        )?;

        // Get sparse contour config for mode
        let config = match mode {
            Mode::Car => SparseContourConfig::for_car(),
            Mode::Bike => SparseContourConfig::for_bike(),
            Mode::Foot => SparseContourConfig::for_foot(),
        };

        Ok(Self {
            phast,
            extractor: Arc::new(extractor),
            config,
            mode,
        })
    }

    /// Get lane width (K)
    pub fn lane_width(&self) -> usize {
        K_LANES
    }

    /// Get number of nodes
    pub fn n_nodes(&self) -> usize {
        self.phast.n_nodes()
    }

    /// Generate isochrones for up to K origins in a single batch
    ///
    /// # Arguments
    /// * `origins` - Up to K origin node IDs (len must be <= K_LANES)
    /// * `threshold_ms` - Time threshold in milliseconds
    ///
    /// # Returns
    /// BatchedIsochroneResult with K contour polygons
    pub fn query_batch(
        &self,
        origins: &[u32],
        threshold_ms: u32,
    ) -> Result<BatchedIsochroneResult> {
        let start = std::time::Instant::now();
        let k = origins.len();

        assert!(k <= K_LANES, "Too many origins for batch (max {})", K_LANES);

        let mut stats = BatchedIsochroneStats {
            n_origins: k,
            ..Default::default()
        };

        // ============================================================
        // Phase 1: K-lane batched PHAST (single downward scan for K)
        // ============================================================
        let phast_start = std::time::Instant::now();
        let phast_result = self.phast.query_batch(origins);
        stats.phast_time_ms = phast_start.elapsed().as_millis() as u64;

        // ============================================================
        // Phase 2: Extract reachable segments for each origin (parallel)
        // ============================================================
        let segment_start = std::time::Instant::now();

        let extractor = Arc::clone(&self.extractor);
        let segments: Vec<Vec<ReachableSegment>> = (0..k)
            .into_par_iter()
            .map(|lane| {
                extractor.extract_reachable_segments(&phast_result.dist[lane], threshold_ms)
            })
            .collect();

        stats.segment_time_ms = segment_start.elapsed().as_millis() as u64;
        stats.total_segments = segments.iter().map(|s| s.len()).sum();

        // ============================================================
        // Phase 3: Generate contours for each origin (parallel)
        // ============================================================
        let contour_start = std::time::Instant::now();

        let config = self.config.clone();
        let contours: Vec<ContourResult> = segments
            .par_iter()
            .filter_map(|segs| {
                generate_sparse_contour(segs, &config).ok().map(|sparse| {
                    // Convert SparseContourResult to ContourResult
                    let elapsed_us = sparse.stats.stamp_time_us
                        + sparse.stats.morphology_time_us
                        + sparse.stats.contour_time_us
                        + sparse.stats.simplify_time_us;
                    ContourResult {
                        outer_ring: sparse.outer_ring,
                        holes: sparse.holes,
                        stats: super::contour::ContourStats {
                            input_segments: sparse.stats.input_segments,
                            grid_cols: 0,
                            grid_rows: 0,
                            filled_cells: sparse.stats.total_cells_set,
                            contour_vertices_before_simplify: sparse.stats.contour_vertices_before_simplify,
                            contour_vertices_after_simplify: sparse.stats.contour_vertices_after_simplify,
                            elapsed_ms: elapsed_us / 1000,
                        },
                    }
                })
            })
            .collect();

        stats.contour_time_ms = contour_start.elapsed().as_millis() as u64;

        // Compute average vertices
        if !contours.is_empty() {
            let total_verts: usize = contours.iter()
                .map(|c| c.outer_ring.len())
                .sum();
            stats.avg_vertices = total_verts / contours.len();
        }

        stats.total_time_ms = start.elapsed().as_millis() as u64;

        Ok(BatchedIsochroneResult {
            n_origins: k,
            contours,
            stats,
        })
    }

    /// Generate multiple isochrones with automatic batching
    ///
    /// Processes all origins in batches of K, maximizing efficiency.
    ///
    /// # Arguments
    /// * `origins` - Any number of origin node IDs
    /// * `threshold_ms` - Time threshold in milliseconds
    ///
    /// # Returns
    /// Vector of ContourResult, one per origin
    pub fn query_many(
        &self,
        origins: &[u32],
        threshold_ms: u32,
    ) -> Result<Vec<ContourResult>> {
        let mut all_contours = Vec::with_capacity(origins.len());

        // Process in batches of K
        for chunk in origins.chunks(K_LANES) {
            let result = self.query_batch(chunk, threshold_ms)?;
            all_contours.extend(result.contours);
        }

        Ok(all_contours)
    }
}

/// Export multiple contours to separate GeoJSON files
pub fn export_batch_geojson(
    contours: &[ContourResult],
    output_dir: &Path,
    prefix: &str,
) -> Result<Vec<std::path::PathBuf>> {
    std::fs::create_dir_all(output_dir)?;

    let paths: Vec<std::path::PathBuf> = contours
        .iter()
        .enumerate()
        .map(|(i, contour)| {
            let path = output_dir.join(format!("{}_{:04}.geojson", prefix, i));
            let _ = export_contour_geojson(contour, &path);
            path
        })
        .collect();

    Ok(paths)
}

/// Export multiple contours to a single GeoJSON FeatureCollection
pub fn export_batch_geojson_collection(
    contours: &[ContourResult],
    origins: &[u32],
    output_path: &Path,
) -> Result<()> {
    use std::io::Write;

    let mut features = Vec::new();

    for (i, contour) in contours.iter().enumerate() {
        if contour.outer_ring.is_empty() {
            continue;
        }

        // Build coordinates array
        let coords: Vec<String> = contour.outer_ring
            .iter()
            .map(|(lon, lat)| format!("[{:.6}, {:.6}]", lon, lat))
            .collect();

        let origin_id = origins.get(i).copied().unwrap_or(i as u32);

        features.push(format!(
            r#"{{
      "type": "Feature",
      "properties": {{
        "origin": {},
        "vertices": {}
      }},
      "geometry": {{
        "type": "Polygon",
        "coordinates": [[{}]]
      }}
    }}"#,
            origin_id,
            contour.outer_ring.len(),
            coords.join(", ")
        ));
    }

    let geojson = format!(
        r#"{{
  "type": "FeatureCollection",
  "features": [
    {}
  ]
}}"#,
        features.join(",\n    ")
    );

    let mut file = std::fs::File::create(output_path)?;
    file.write_all(geojson.as_bytes())?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batch_size() {
        assert_eq!(K_LANES, 8);
    }
}
