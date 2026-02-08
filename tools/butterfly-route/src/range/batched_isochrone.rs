//! K-Lane Batched Isochrone Generation
//!
//! Processes K isochrone queries efficiently by:
//! 1. Running K-lane batched PHAST (single downward scan for K origins)
//! 2. Extracting reachable segments for each origin (parallel)
//! 3. Generating contour polygons for each origin (parallel)
//!
//! This amortizes the expensive downward scan across K queries.
//!
//! ## Adaptive Mode Selection
//!
//! For bounded queries, the optimal algorithm depends on threshold:
//! - Small thresholds (< 15 min): Single-source + early-stop wins
//! - Large thresholds (> 20 min): K-lane batching + lane masking wins
//!
//! Use `AdaptiveIsochroneEngine` to automatically select the best mode.

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use rayon::prelude::*;

use super::contour::{export_contour_geojson, ContourResult};
use super::frontier::{FrontierExtractor, ReachableSegment};
use super::phast::PhastEngine;
use super::sparse_contour::{generate_sparse_contour, SparseContourConfig};
use crate::matrix::batched_phast::{BatchedPhastEngine, K_LANES};
use crate::profile_abi::Mode;

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
    #[allow(dead_code)]
    mode: Mode,
}

impl BatchedIsochroneEngine {
    /// Create batched isochrone engine from file paths
    #[allow(clippy::too_many_arguments)]
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
    /// * `threshold_ds` - Time threshold in deciseconds (CCH weight units)
    ///
    /// # Returns
    /// BatchedIsochroneResult with K contour polygons
    pub fn query_batch(
        &self,
        origins: &[u32],
        threshold_ds: u32,
    ) -> Result<BatchedIsochroneResult> {
        // Convert to milliseconds for frontier extraction
        let threshold_ms = threshold_ds * 100;
        let start = std::time::Instant::now();
        let k = origins.len();

        assert!(k <= K_LANES, "Too many origins for batch (max {})", K_LANES);

        let mut stats = BatchedIsochroneStats {
            n_origins: k,
            ..Default::default()
        };

        // ============================================================
        // Phase 1: K-lane batched PHAST with early-stop and lane masking
        // ============================================================
        let phast_start = std::time::Instant::now();
        let phast_result = self.phast.query_batch_bounded(origins, threshold_ds);
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
                            contour_vertices_before_simplify: sparse
                                .stats
                                .contour_vertices_before_simplify,
                            contour_vertices_after_simplify: sparse
                                .stats
                                .contour_vertices_after_simplify,
                            elapsed_ms: elapsed_us / 1000,
                        },
                    }
                })
            })
            .collect();

        stats.contour_time_ms = contour_start.elapsed().as_millis() as u64;

        // Compute average vertices
        if !contours.is_empty() {
            let total_verts: usize = contours.iter().map(|c| c.outer_ring.len()).sum();
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
    /// * `threshold_ds` - Time threshold in deciseconds
    ///
    /// # Returns
    /// Vector of ContourResult, one per origin
    pub fn query_many(&self, origins: &[u32], threshold_ds: u32) -> Result<Vec<ContourResult>> {
        let mut all_contours = Vec::with_capacity(origins.len());

        // Process in batches of K
        for chunk in origins.chunks(K_LANES) {
            let result = self.query_batch(chunk, threshold_ds)?;
            all_contours.extend(result.contours);
        }

        Ok(all_contours)
    }
}

/// Threshold in deciseconds below which single-source beats K-lane batched.
/// Empirically determined on Belgium dataset:
/// - Below 15 min: single-source + early-stop is faster
/// - Above 18 min: K-lane batching + lane masking is faster
pub const ADAPTIVE_THRESHOLD_DS: u32 = 10000; // ~17 min crossover

/// Adaptive isochrone engine that automatically selects the best algorithm.
///
/// For small thresholds: uses single-source PHAST with early-stop
/// For large thresholds: uses K-lane batched PHAST with lane masking
pub struct AdaptiveIsochroneEngine {
    /// Single-source PHAST engine (for small thresholds)
    single_phast: PhastEngine,
    /// K-lane batched PHAST engine (for large thresholds)
    batched_phast: BatchedPhastEngine,
    /// Frontier extractor (shared)
    extractor: Arc<FrontierExtractor>,
    /// Sparse contour config
    config: SparseContourConfig,
    /// Mode
    mode: Mode,
}

impl AdaptiveIsochroneEngine {
    /// Load adaptive isochrone engine from file paths
    #[allow(clippy::too_many_arguments)]
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
        // Load both PHAST engines (they share the same underlying data)
        let single_phast = PhastEngine::load(cch_topo_path, cch_weights_path, order_path)?;
        let batched_phast = BatchedPhastEngine::load(cch_topo_path, cch_weights_path, order_path)?;

        // Load frontier extractor
        let extractor = FrontierExtractor::load(
            filtered_ebg_path,
            ebg_nodes_path,
            nbg_geo_path,
            base_weights_path,
        )?;

        let config = match mode {
            Mode::Car => SparseContourConfig::for_car(),
            Mode::Bike => SparseContourConfig::for_bike(),
            Mode::Foot => SparseContourConfig::for_foot(),
        };

        Ok(Self {
            single_phast,
            batched_phast,
            extractor: Arc::new(extractor),
            config,
            mode,
        })
    }

    /// Get number of nodes
    pub fn n_nodes(&self) -> usize {
        self.single_phast.n_nodes()
    }

    /// Generate a single isochrone using the optimal algorithm for the threshold
    pub fn query_single(&self, origin: u32, threshold_ds: u32) -> Result<ContourResult> {
        let threshold_ms = threshold_ds * 100;

        // Always use single-source for single queries (no batching benefit)
        let phast_result = self.single_phast.query_bounded(origin, threshold_ds);
        let segments = self
            .extractor
            .extract_reachable_segments(&phast_result.dist, threshold_ms);

        if segments.is_empty() {
            return Ok(ContourResult {
                outer_ring: vec![],
                holes: vec![],
                stats: super::contour::ContourStats::default(),
            });
        }

        let sparse_result = generate_sparse_contour(&segments, &self.config)?;

        // Convert to ContourResult
        let elapsed_us = sparse_result.stats.stamp_time_us
            + sparse_result.stats.morphology_time_us
            + sparse_result.stats.contour_time_us
            + sparse_result.stats.simplify_time_us;

        Ok(ContourResult {
            outer_ring: sparse_result.outer_ring,
            holes: sparse_result.holes,
            stats: super::contour::ContourStats {
                input_segments: sparse_result.stats.input_segments,
                grid_cols: 0,
                grid_rows: 0,
                filled_cells: sparse_result.stats.total_cells_set,
                contour_vertices_before_simplify: sparse_result
                    .stats
                    .contour_vertices_before_simplify,
                contour_vertices_after_simplify: sparse_result
                    .stats
                    .contour_vertices_after_simplify,
                elapsed_ms: elapsed_us / 1000,
            },
        })
    }

    /// Generate multiple isochrones, automatically choosing the best algorithm
    ///
    /// - For threshold < ADAPTIVE_THRESHOLD_DS: runs single-source queries
    /// - For threshold >= ADAPTIVE_THRESHOLD_DS: uses K-lane batching
    pub fn query_many(&self, origins: &[u32], threshold_ds: u32) -> Result<Vec<ContourResult>> {
        let threshold_ms = threshold_ds * 100;

        if threshold_ds < ADAPTIVE_THRESHOLD_DS {
            // Small threshold: single-source is faster
            // Process sequentially (could parallelize with rayon if needed)
            let mut results = Vec::with_capacity(origins.len());
            for &origin in origins {
                results.push(self.query_single(origin, threshold_ds)?);
            }
            Ok(results)
        } else {
            // Large threshold: K-lane batching is faster
            let mut all_contours = Vec::with_capacity(origins.len());

            for chunk in origins.chunks(K_LANES) {
                let phast_result = self.batched_phast.query_batch_bounded(chunk, threshold_ds);

                // Extract segments and generate contours for each lane
                let extractor = Arc::clone(&self.extractor);
                let config = self.config.clone();

                let contours: Vec<ContourResult> = (0..chunk.len())
                    .into_par_iter()
                    .filter_map(|lane| {
                        let segments = extractor
                            .extract_reachable_segments(&phast_result.dist[lane], threshold_ms);
                        if segments.is_empty() {
                            return Some(ContourResult {
                                outer_ring: vec![],
                                holes: vec![],
                                stats: super::contour::ContourStats::default(),
                            });
                        }
                        generate_sparse_contour(&segments, &config)
                            .ok()
                            .map(|sparse| {
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
                                        contour_vertices_before_simplify: sparse
                                            .stats
                                            .contour_vertices_before_simplify,
                                        contour_vertices_after_simplify: sparse
                                            .stats
                                            .contour_vertices_after_simplify,
                                        elapsed_ms: elapsed_us / 1000,
                                    },
                                }
                            })
                    })
                    .collect();

                all_contours.extend(contours);
            }

            Ok(all_contours)
        }
    }

    /// Get the mode being used
    pub fn mode(&self) -> Mode {
        self.mode
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
        let coords: Vec<String> = contour
            .outer_ring
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
