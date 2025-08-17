//! Memory-efficient streaming 3-pass geometry pipeline (A→B→C)

use crate::delta::{DeltaEncoder, FullFidelityGeometry};
use crate::resample::{ArcLengthResampler, Point2D, SnapSkeleton};
use crate::simplify::{NavigationGeometry, NavigationSimplifier};
use serde::{Deserialize, Serialize};

/// Configuration for the 3-pass geometry pipeline
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineConfig {
    // Pass A configuration
    pub urban_spacing: f64,
    pub rural_spacing: f64,
    pub angle_threshold: f64,
    pub heading_sample_interval: f64,

    // Pass B configuration
    pub rdp_epsilon: f64,
    pub curvature_threshold: f64,
    pub max_chunk_size: f64,
    pub hausdorff_median_target: f64,
    pub hausdorff_p95_target: f64,

    // Pass C configuration
    pub noise_threshold: f64,
    pub max_delta_meters: f64,

    // Pipeline configuration
    pub enable_pass_c: bool,    // Optional pass for meeting time SLAs
    pub memory_limit_mb: usize, // Per-worker memory limit
    pub batch_size: usize,      // Points per batch for streaming
}

impl PipelineConfig {
    pub fn default() -> Self {
        Self {
            // Pass A
            urban_spacing: 5.0,
            rural_spacing: 25.0,
            angle_threshold: 12.0,
            heading_sample_interval: 40.0,

            // Pass B
            rdp_epsilon: 2.0,
            curvature_threshold: 15.0,
            max_chunk_size: 512.0,
            hausdorff_median_target: 2.0,
            hausdorff_p95_target: 5.0,

            // Pass C
            noise_threshold: 0.25,
            max_delta_meters: 32.0,

            // Pipeline
            enable_pass_c: true,
            memory_limit_mb: 64, // Conservative per-worker limit
            batch_size: 1000,    // Points per streaming batch
        }
    }

    /// Create configuration optimized for 8-10h planet SLA (skip Pass C)
    pub fn planet_fast() -> Self {
        let mut config = Self::default();
        config.enable_pass_c = false; // Skip Pass C for time SLA
        config.batch_size = 2000; // Larger batches for efficiency
        config.memory_limit_mb = 128; // Larger memory budget
        config
    }
}

/// Result of the 3-pass geometry pipeline
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeometryPipelineResult {
    pub pass_a_result: SnapSkeleton,
    pub pass_b_result: NavigationGeometry,
    pub pass_c_result: Option<FullFidelityGeometry>,
    pub processing_stats: ProcessingStats,
}

impl GeometryPipelineResult {
    pub fn new(
        pass_a_result: SnapSkeleton,
        pass_b_result: NavigationGeometry,
        pass_c_result: Option<FullFidelityGeometry>,
        processing_stats: ProcessingStats,
    ) -> Self {
        Self {
            pass_a_result,
            pass_b_result,
            pass_c_result,
            processing_stats,
        }
    }

    /// Get memory usage across all passes
    pub fn total_memory_usage(&self) -> usize {
        let pass_a_size = std::mem::size_of_val(&self.pass_a_result);
        let pass_b_size = std::mem::size_of_val(&self.pass_b_result);
        let pass_c_size = self
            .pass_c_result
            .as_ref()
            .map(|c| c.memory_usage())
            .unwrap_or(0);

        pass_a_size + pass_b_size + pass_c_size
    }
}

/// Statistics about pipeline processing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessingStats {
    pub original_points: usize,
    pub pass_a_points: usize,
    pub pass_b_points: usize,
    pub pass_c_points: usize,
    pub processing_time_ms: u64,
    pub memory_peak_mb: f64,
    pub batches_processed: usize,
    pub fallback_triggered: bool,
}

impl ProcessingStats {
    pub fn new() -> Self {
        Self {
            original_points: 0,
            pass_a_points: 0,
            pass_b_points: 0,
            pass_c_points: 0,
            processing_time_ms: 0,
            memory_peak_mb: 0.0,
            batches_processed: 0,
            fallback_triggered: false,
        }
    }

    /// Calculate overall compression ratio
    pub fn compression_ratio(&self) -> f64 {
        if self.original_points == 0 {
            return 1.0;
        }

        let final_points = if self.pass_c_points > 0 {
            self.pass_c_points
        } else {
            self.pass_b_points
        };

        final_points as f64 / self.original_points as f64
    }
}

/// Memory-efficient streaming geometry pipeline
pub struct GeometryPipeline {
    config: PipelineConfig,
    pass_a_resampler: ArcLengthResampler,
    pass_b_simplifier: NavigationSimplifier,
    pass_c_encoder: DeltaEncoder,
}

impl GeometryPipeline {
    pub fn new(config: PipelineConfig) -> Self {
        let pass_a_resampler = ArcLengthResampler::new(
            config.urban_spacing,
            config.rural_spacing,
            config.angle_threshold,
            config.heading_sample_interval,
        );

        let pass_b_simplifier = NavigationSimplifier::new(
            config.rdp_epsilon,
            config.curvature_threshold,
            config.max_chunk_size,
            config.hausdorff_median_target,
            config.hausdorff_p95_target,
        );

        let pass_c_encoder = DeltaEncoder::new(config.noise_threshold, config.max_delta_meters);

        Self {
            config,
            pass_a_resampler,
            pass_b_simplifier,
            pass_c_encoder,
        }
    }

    pub fn default() -> Self {
        Self::new(PipelineConfig::default())
    }

    /// Process geometry through all enabled passes
    pub fn process_geometry(&self, geometry: &[Point2D]) -> Result<GeometryPipelineResult, String> {
        let start_time = std::time::Instant::now();
        let mut stats = ProcessingStats::new();
        stats.original_points = geometry.len();

        // Estimate memory usage
        let estimated_memory_mb = self.estimate_memory_usage(geometry.len());
        if estimated_memory_mb > self.config.memory_limit_mb as f64 {
            stats.fallback_triggered = true;
            return self.process_with_fallback(geometry, stats);
        }

        // Pass A: Snap skeleton with arc-length resampling
        let pass_a_result = self
            .pass_a_resampler
            .create_snap_skeleton(geometry)
            .map_err(|e| format!("Pass A failed: {}", e))?;
        stats.pass_a_points = pass_a_result.points.len();

        // Pass B: Navigation simplification
        let pass_b_result = self
            .pass_b_simplifier
            .simplify_for_navigation(&pass_a_result.points)
            .map_err(|e| format!("Pass B failed: {}", e))?;
        stats.pass_b_points = pass_b_result.simplified_points.len();

        // Check if B→A fallback was triggered
        if pass_b_result.hausdorff_median > self.config.hausdorff_median_target * 1.5
            || pass_b_result.hausdorff_p95 > self.config.hausdorff_p95_target * 1.5
        {
            stats.fallback_triggered = true;
        }

        // Pass C: Delta encoding (optional)
        let pass_c_result = if self.config.enable_pass_c {
            let result = self
                .pass_c_encoder
                .create_full_fidelity(&pass_b_result.simplified_points)
                .map_err(|e| format!("Pass C failed: {}", e))?;
            stats.pass_c_points = result.delta_points.len() + 1; // +1 for reference point
            Some(result)
        } else {
            None
        };

        stats.processing_time_ms = start_time.elapsed().as_millis() as u64;
        stats.memory_peak_mb = estimated_memory_mb;
        stats.batches_processed = 1; // Single batch for non-streaming mode

        Ok(GeometryPipelineResult::new(
            pass_a_result,
            pass_b_result,
            pass_c_result,
            stats,
        ))
    }

    /// Process large geometries with streaming and bounded memory
    pub fn process_streaming(
        &self,
        geometry_stream: &[&[Point2D]],
    ) -> Result<Vec<GeometryPipelineResult>, String> {
        let mut results = Vec::new();

        for (batch_idx, batch) in geometry_stream.iter().enumerate() {
            // Check memory limit per batch
            let estimated_memory_mb = self.estimate_memory_usage(batch.len());
            if estimated_memory_mb > self.config.memory_limit_mb as f64 {
                return Err(format!(
                    "Batch {} exceeds memory limit: {:.1}MB > {}MB",
                    batch_idx, estimated_memory_mb, self.config.memory_limit_mb
                ));
            }

            let result = self.process_geometry(batch)?;
            results.push(result);
        }

        Ok(results)
    }

    /// Fallback processing for memory-constrained scenarios
    fn process_with_fallback(
        &self,
        geometry: &[Point2D],
        mut stats: ProcessingStats,
    ) -> Result<GeometryPipelineResult, String> {
        // Split into smaller batches
        let batch_size = self.config.batch_size.min(geometry.len() / 2).max(100);
        let mut all_a_points = Vec::new();

        for chunk in geometry.chunks(batch_size) {
            stats.batches_processed += 1;

            // Process Pass A on chunk
            let chunk_a = self.pass_a_resampler.create_snap_skeleton(chunk)?;
            all_a_points.extend(chunk_a.points);
        }

        stats.pass_a_points = all_a_points.len();

        // Process Pass B on accumulated A results
        let pass_b_result = self
            .pass_b_simplifier
            .simplify_for_navigation(&all_a_points)?;
        stats.pass_b_points = pass_b_result.simplified_points.len();

        // Skip Pass C in fallback mode to save memory
        let pass_c_result = None;

        // Create synthetic Pass A result for compatibility
        let total_length = geometry.windows(2).map(|w| w[0].distance_to(&w[1])).sum();
        let pass_a_result = SnapSkeleton::new(
            all_a_points,
            Vec::new(), // No heading samples in fallback
            total_length,
            self.config.urban_spacing,
        );

        Ok(GeometryPipelineResult::new(
            pass_a_result,
            pass_b_result,
            pass_c_result,
            stats,
        ))
    }

    /// Estimate memory usage for a given number of points
    fn estimate_memory_usage(&self, point_count: usize) -> f64 {
        let point_size = std::mem::size_of::<Point2D>();

        // Estimate working memory for all passes
        let pass_a_memory = point_count * point_size * 2; // Original + resampled
        let pass_b_memory = point_count * point_size; // Simplified
        let pass_c_memory = if self.config.enable_pass_c {
            point_count * 8 // Delta encoding typically smaller
        } else {
            0
        };

        let working_memory = point_size * 1024; // Working buffers

        (pass_a_memory + pass_b_memory + pass_c_memory + working_memory) as f64 / (1024.0 * 1024.0)
    }

    /// Create batches from large geometry for streaming processing
    pub fn create_batches<'a>(&self, geometry: &'a [Point2D]) -> Vec<&'a [Point2D]> {
        geometry.chunks(self.config.batch_size).collect()
    }

    /// Get pipeline configuration
    pub fn config(&self) -> &PipelineConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pipeline_configuration() {
        let default_config = PipelineConfig::default();
        assert!(default_config.enable_pass_c);
        assert_eq!(default_config.urban_spacing, 5.0);

        let fast_config = PipelineConfig::planet_fast();
        assert!(!fast_config.enable_pass_c);
        assert_eq!(fast_config.batch_size, 2000);
    }

    #[test]
    fn test_basic_pipeline_processing() {
        let geometry = vec![
            Point2D::new(0.0, 0.0),
            Point2D::new(10.0, 0.0),
            Point2D::new(20.0, 5.0),
            Point2D::new(30.0, 5.0),
            Point2D::new(40.0, 0.0),
        ];

        let pipeline = GeometryPipeline::default();
        let result = pipeline.process_geometry(&geometry).unwrap();

        assert_eq!(result.processing_stats.original_points, 5);
        assert!(result.processing_stats.pass_a_points > 0);
        assert!(result.processing_stats.pass_b_points > 0);
        assert!(result.pass_c_result.is_some());
        assert!(result.processing_stats.compression_ratio() <= 1.0);
    }

    #[test]
    fn test_memory_estimation() {
        let pipeline = GeometryPipeline::default();
        let memory_1k = pipeline.estimate_memory_usage(1000);
        let memory_10k = pipeline.estimate_memory_usage(10000);

        assert!(memory_10k > memory_1k);
        assert!(memory_1k > 0.0);
    }

    #[test]
    fn test_streaming_processing() {
        let batch1 = vec![
            Point2D::new(0.0, 0.0),
            Point2D::new(10.0, 0.0),
            Point2D::new(20.0, 0.0),
        ];

        let batch2 = vec![
            Point2D::new(30.0, 0.0),
            Point2D::new(40.0, 5.0),
            Point2D::new(50.0, 5.0),
        ];

        let pipeline = GeometryPipeline::default();
        let batches = vec![batch1.as_slice(), batch2.as_slice()];
        let results = pipeline.process_streaming(&batches).unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].processing_stats.original_points, 3);
        assert_eq!(results[1].processing_stats.original_points, 3);
    }

    #[test]
    fn test_fallback_processing() {
        // Create a config with very low memory limit to trigger fallback
        let mut config = PipelineConfig::default();
        config.memory_limit_mb = 0; // Zero limit forces fallback
        config.batch_size = 3;

        let large_geometry: Vec<Point2D> = (0..10)
            .map(|i| Point2D::new(i as f64, (i % 3) as f64))
            .collect();

        let pipeline = GeometryPipeline::new(config);
        let result = pipeline.process_geometry(&large_geometry).unwrap();

        assert!(result.processing_stats.fallback_triggered);
        // The key is that fallback was triggered and Pass C was skipped
        assert!(result.processing_stats.batches_processed >= 1);
        assert!(result.pass_c_result.is_none()); // Pass C skipped in fallback
    }

    #[test]
    fn test_pipeline_result_memory_usage() {
        let geometry = vec![
            Point2D::new(0.0, 0.0),
            Point2D::new(10.0, 0.0),
            Point2D::new(20.0, 0.0),
        ];

        let pipeline = GeometryPipeline::default();
        let result = pipeline.process_geometry(&geometry).unwrap();

        let memory_usage = result.total_memory_usage();
        assert!(memory_usage > 0);
    }

    #[test]
    fn test_batch_creation() {
        let geometry: Vec<Point2D> = (0..100).map(|i| Point2D::new(i as f64, 0.0)).collect();

        let mut config = PipelineConfig::default();
        config.batch_size = 25;

        let pipeline = GeometryPipeline::new(config);
        let batches = pipeline.create_batches(&geometry);

        assert_eq!(batches.len(), 4); // 100 points / 25 per batch
        assert_eq!(batches[0].len(), 25);
        assert_eq!(batches[3].len(), 25);
    }

    #[test]
    fn test_planet_fast_configuration() {
        let config = PipelineConfig::planet_fast();
        let pipeline = GeometryPipeline::new(config);

        let geometry = vec![
            Point2D::new(0.0, 0.0),
            Point2D::new(10.0, 0.0),
            Point2D::new(20.0, 0.0),
        ];

        let result = pipeline.process_geometry(&geometry).unwrap();

        // Pass C should be disabled for planet fast mode
        assert!(result.pass_c_result.is_none());
        assert_eq!(result.processing_stats.pass_c_points, 0);
    }
}
