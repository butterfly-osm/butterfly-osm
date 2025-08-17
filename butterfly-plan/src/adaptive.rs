//! Adaptive planning using telemetry data
//!
//! Implements M1.4 - telemetry-driven parameter derivation for BuildPlan

use crate::{MemoryBudget, PlanConfig};
use butterfly_extract::{DensityClass, GlobalPercentiles, TelemetryOutput};

/// Adaptive planner that uses telemetry data to optimize parameters
pub struct AdaptivePlanner {
    /// Base configuration
    base_config: PlanConfig,
    /// Telemetry data for analysis
    telemetry: Option<TelemetryOutput>,
}

impl AdaptivePlanner {
    /// Create new adaptive planner
    pub fn new(base_config: PlanConfig) -> Self {
        Self {
            base_config,
            telemetry: None,
        }
    }

    /// Load telemetry data for adaptive planning
    pub fn load_telemetry(&mut self, telemetry: TelemetryOutput) {
        self.telemetry = Some(telemetry);
    }

    /// Create adaptive build plan based on telemetry
    pub fn create_adaptive_plan(&self) -> AdaptiveBuildPlan {
        let Some(ref telemetry) = self.telemetry else {
            // Fallback to static plan if no telemetry available
            return AdaptiveBuildPlan::static_plan(&self.base_config);
        };

        // Analyze density distribution
        let density_stats = self.analyze_density_distribution(telemetry);

        // Derive adaptive parameters
        let worker_scaling =
            self.calculate_worker_scaling(&density_stats, &telemetry.global_percentiles);
        let memory_factors =
            self.calculate_memory_factors(&density_stats, &telemetry.global_percentiles);
        let chunk_sizes = self.calculate_adaptive_chunk_sizes(&density_stats);

        AdaptiveBuildPlan {
            base_config: self.base_config.clone(),
            density_stats,
            worker_scaling,
            memory_factors,
            chunk_sizes,
            telemetry_summary: TelemetrySummary::from_telemetry(telemetry),
        }
    }

    /// Analyze density distribution across tiles
    fn analyze_density_distribution(&self, telemetry: &TelemetryOutput) -> DensityDistribution {
        let mut urban_count = 0;
        let mut suburban_count = 0;
        let mut rural_count = 0;

        let mut total_urban_density = 0.0;
        let mut total_suburban_density = 0.0;
        let mut total_rural_density = 0.0;

        for tile in &telemetry.tiles {
            let road_density = tile.metrics.total_length_m / (125.0 * 125.0); // per m²

            match tile.density_class {
                DensityClass::Urban => {
                    urban_count += 1;
                    total_urban_density += road_density;
                }
                DensityClass::Suburban => {
                    suburban_count += 1;
                    total_suburban_density += road_density;
                }
                DensityClass::Rural => {
                    rural_count += 1;
                    total_rural_density += road_density;
                }
            }
        }

        let total_tiles = telemetry.total_tiles;

        DensityDistribution {
            urban_ratio: urban_count as f64 / total_tiles as f64,
            suburban_ratio: suburban_count as f64 / total_tiles as f64,
            rural_ratio: rural_count as f64 / total_tiles as f64,
            avg_urban_density: if urban_count > 0 {
                total_urban_density / urban_count as f64
            } else {
                0.0
            },
            avg_suburban_density: if suburban_count > 0 {
                total_suburban_density / suburban_count as f64
            } else {
                0.0
            },
            avg_rural_density: if rural_count > 0 {
                total_rural_density / rural_count as f64
            } else {
                0.0
            },
            complexity_score: self.calculate_complexity_score(telemetry),
        }
    }

    /// Calculate overall complexity score for the dataset
    fn calculate_complexity_score(&self, telemetry: &TelemetryOutput) -> f64 {
        let global = &telemetry.global_percentiles;

        // Normalize metrics to 0-1 scale for complexity calculation
        let junction_complexity = (global.junction_count_p85 / 20.0).min(1.0); // Max ~20 junctions per tile
        let density_complexity = (global.density_p85 / 0.05).min(1.0); // Max 0.05 road density
        let length_complexity = (global.total_length_p85 / 10000.0).min(1.0); // Max 10km per tile

        // Weighted average with emphasis on junctions and density
        junction_complexity * 0.4 + density_complexity * 0.4 + length_complexity * 0.2
    }

    /// Calculate worker scaling factors based on density distribution
    fn calculate_worker_scaling(
        &self,
        density: &DensityDistribution,
        _global: &GlobalPercentiles,
    ) -> WorkerScaling {
        // Scale workers based on complexity and urban ratio
        let base_workers = self.base_config.workers as usize;

        // Urban areas need more parallelism due to complexity
        let urban_factor = 1.0 + (density.urban_ratio * 0.5); // Up to 50% more workers

        // High complexity also increases worker needs
        let complexity_factor = 1.0 + (density.complexity_score * 0.3); // Up to 30% more

        // But don't over-scale for rural-heavy datasets
        let rural_penalty = 1.0 - (density.rural_ratio * 0.2); // Up to 20% fewer workers

        let total_factor = (urban_factor * complexity_factor * rural_penalty).clamp(0.5, 2.0);
        let recommended_workers = (base_workers as f64 * total_factor).round() as usize;

        WorkerScaling {
            base_workers,
            recommended_workers: recommended_workers.max(1),
            scaling_factor: total_factor,
            urban_factor,
            complexity_factor,
            rural_penalty,
        }
    }

    /// Calculate memory scaling factors
    fn calculate_memory_factors(
        &self,
        density: &DensityDistribution,
        _global: &GlobalPercentiles,
    ) -> MemoryFactors {
        // Memory needs scale with density and complexity
        let io_buffer_factor = 1.0 + (density.complexity_score * 0.4); // Up to 40% more I/O buffers
        let merge_heap_factor = 1.0 + (density.urban_ratio * 0.3); // Up to 30% more merge heaps
        let per_worker_factor =
            1.0 + ((density.avg_urban_density + density.avg_suburban_density) * 0.1);

        MemoryFactors {
            io_buffer_factor,
            merge_heap_factor,
            per_worker_factor,
            overhead_factor: 1.0, // Keep overhead constant
        }
    }

    /// Calculate adaptive chunk sizes for different processing phases
    fn calculate_adaptive_chunk_sizes(&self, density: &DensityDistribution) -> ChunkSizes {
        // Smaller chunks for dense urban areas, larger for rural
        let density_mix =
            density.urban_ratio * 2.0 + density.suburban_ratio * 1.0 + density.rural_ratio * 0.5;

        // Base chunk sizes (in number of elements)
        let base_read_chunk = 10000;
        let base_process_chunk = 5000;
        let base_write_chunk = 8000;

        // Scale based on density - denser areas need smaller chunks for better parallelism
        let chunk_factor = (2.0 - density_mix).clamp(0.5, 1.5);

        ChunkSizes {
            read_chunk_size: (base_read_chunk as f64 * chunk_factor) as usize,
            process_chunk_size: (base_process_chunk as f64 * chunk_factor) as usize,
            write_chunk_size: (base_write_chunk as f64 * chunk_factor) as usize,
            sort_buffer_size: (50_000_f64 * chunk_factor) as usize,
        }
    }
}

/// Density distribution analysis
#[derive(Debug, Clone)]
pub struct DensityDistribution {
    pub urban_ratio: f64,
    pub suburban_ratio: f64,
    pub rural_ratio: f64,
    pub avg_urban_density: f64,
    pub avg_suburban_density: f64,
    pub avg_rural_density: f64,
    pub complexity_score: f64,
}

/// Worker scaling parameters
#[derive(Debug, Clone)]
pub struct WorkerScaling {
    pub base_workers: usize,
    pub recommended_workers: usize,
    pub scaling_factor: f64,
    pub urban_factor: f64,
    pub complexity_factor: f64,
    pub rural_penalty: f64,
}

/// Memory scaling factors
#[derive(Debug, Clone)]
pub struct MemoryFactors {
    pub io_buffer_factor: f64,
    pub merge_heap_factor: f64,
    pub per_worker_factor: f64,
    pub overhead_factor: f64,
}

/// Adaptive chunk sizes for processing
#[derive(Debug, Clone)]
pub struct ChunkSizes {
    pub read_chunk_size: usize,
    pub process_chunk_size: usize,
    pub write_chunk_size: usize,
    pub sort_buffer_size: usize,
}

/// Telemetry summary for planning
#[derive(Debug, Clone)]
pub struct TelemetrySummary {
    pub total_tiles: usize,
    pub avg_road_density: f64,
    pub avg_junctions_per_tile: f64,
    pub complexity_rating: ComplexityRating,
}

impl TelemetrySummary {
    fn from_telemetry(telemetry: &TelemetryOutput) -> Self {
        let avg_density = telemetry.global_percentiles.density_p50;
        let avg_junctions = telemetry.global_percentiles.junction_count_p50;

        let complexity_rating = if avg_density > 0.03 && avg_junctions > 10.0 {
            ComplexityRating::High
        } else if avg_density > 0.015 || avg_junctions > 5.0 {
            ComplexityRating::Medium
        } else {
            ComplexityRating::Low
        };

        Self {
            total_tiles: telemetry.total_tiles,
            avg_road_density: avg_density,
            avg_junctions_per_tile: avg_junctions,
            complexity_rating,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ComplexityRating {
    Low,
    Medium,
    High,
}

/// Complete adaptive build plan
#[derive(Debug, Clone)]
pub struct AdaptiveBuildPlan {
    pub base_config: PlanConfig,
    pub density_stats: DensityDistribution,
    pub worker_scaling: WorkerScaling,
    pub memory_factors: MemoryFactors,
    pub chunk_sizes: ChunkSizes,
    pub telemetry_summary: TelemetrySummary,
}

impl AdaptiveBuildPlan {
    /// Create static fallback plan
    fn static_plan(config: &PlanConfig) -> Self {
        Self {
            base_config: config.clone(),
            density_stats: DensityDistribution {
                urban_ratio: 0.33,
                suburban_ratio: 0.33,
                rural_ratio: 0.34,
                avg_urban_density: 0.02,
                avg_suburban_density: 0.01,
                avg_rural_density: 0.005,
                complexity_score: 0.5,
            },
            worker_scaling: WorkerScaling {
                base_workers: config.workers as usize,
                recommended_workers: config.workers as usize,
                scaling_factor: 1.0,
                urban_factor: 1.0,
                complexity_factor: 1.0,
                rural_penalty: 1.0,
            },
            memory_factors: MemoryFactors {
                io_buffer_factor: 1.0,
                merge_heap_factor: 1.0,
                per_worker_factor: 1.0,
                overhead_factor: 1.0,
            },
            chunk_sizes: ChunkSizes {
                read_chunk_size: 10000,
                process_chunk_size: 5000,
                write_chunk_size: 8000,
                sort_buffer_size: 50000,
            },
            telemetry_summary: TelemetrySummary {
                total_tiles: 0,
                avg_road_density: 0.01,
                avg_junctions_per_tile: 3.0,
                complexity_rating: ComplexityRating::Medium,
            },
        }
    }

    /// Create optimized configuration from adaptive plan
    pub fn to_optimized_config(&self) -> PlanConfig {
        let mut config = self.base_config.clone();
        config.workers = self.worker_scaling.recommended_workers as u32;
        config
    }

    /// Create memory budget with adaptive factors
    pub fn create_adaptive_budget(&self) -> MemoryBudget {
        let config = self.to_optimized_config();
        let mut budget = MemoryBudget::new(config.max_ram_mb, config.workers);

        // Apply memory scaling factors
        budget.io_buffers_mb =
            (budget.io_buffers_mb as f64 * self.memory_factors.io_buffer_factor) as u32;
        budget.merge_heaps_mb =
            (budget.merge_heaps_mb as f64 * self.memory_factors.merge_heap_factor) as u32;
        budget.per_worker_mb =
            (budget.per_worker_mb as f64 * self.memory_factors.per_worker_factor) as u32;

        budget
    }

    /// Generate planning recommendations
    pub fn recommendations(&self) -> Vec<String> {
        let mut recs = Vec::new();

        // Worker recommendations
        if self.worker_scaling.recommended_workers != self.worker_scaling.base_workers {
            recs.push(format!(
                "Adjust workers: {} → {} ({}% change)",
                self.worker_scaling.base_workers,
                self.worker_scaling.recommended_workers,
                ((self.worker_scaling.scaling_factor - 1.0) * 100.0) as i32
            ));
        }

        // Complexity recommendations
        match self.telemetry_summary.complexity_rating {
            ComplexityRating::High => {
                recs.push("High complexity detected: Consider using smaller chunks and more aggressive parallel processing".to_string());
            }
            ComplexityRating::Low => {
                recs.push("Low complexity detected: Can use larger chunks and fewer workers for efficiency".to_string());
            }
            _ => {}
        }

        // Density-specific recommendations
        if self.density_stats.urban_ratio > 0.7 {
            recs.push("Urban-heavy dataset: Prioritize memory for merge operations and smaller chunk sizes".to_string());
        } else if self.density_stats.rural_ratio > 0.7 {
            recs.push(
                "Rural-heavy dataset: Can optimize for larger chunks and sequential processing"
                    .to_string(),
            );
        }

        recs
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use butterfly_extract::GlobalPercentiles;

    #[test]
    fn test_static_adaptive_plan() {
        let config = PlanConfig::default();
        let planner = AdaptivePlanner::new(config.clone());
        let plan = planner.create_adaptive_plan();

        // Should create static plan when no telemetry
        assert_eq!(
            plan.worker_scaling.recommended_workers,
            config.workers as usize
        );
        assert_eq!(
            plan.telemetry_summary.complexity_rating,
            ComplexityRating::Medium
        );
    }

    #[test]
    fn test_density_distribution_analysis() {
        let config = PlanConfig::default();
        let mut planner = AdaptivePlanner::new(config);

        // Create mock telemetry with urban-heavy distribution
        let global_percentiles = GlobalPercentiles {
            junction_count_p15: 1.0,
            junction_count_p50: 5.0,
            junction_count_p85: 15.0,
            junction_count_p99: 25.0,
            total_length_p15: 100.0,
            total_length_p50: 500.0,
            total_length_p85: 2000.0,
            total_length_p99: 5000.0,
            density_p15: 0.005,
            density_p50: 0.02,
            density_p85: 0.04,
            density_p99: 0.08,
        };

        let telemetry = TelemetryOutput {
            tile_size_meters: 125.0,
            total_tiles: 1000,
            global_percentiles,
            tiles: vec![], // Empty for this test
        };

        planner.load_telemetry(telemetry);
        let plan = planner.create_adaptive_plan();

        // Should have calculated density distribution
        assert!(plan.density_stats.complexity_score > 0.0);
        assert!(plan.worker_scaling.scaling_factor >= 0.5);
        assert!(plan.memory_factors.io_buffer_factor >= 1.0);
    }

    #[test]
    fn test_worker_scaling() {
        let config = PlanConfig {
            workers: 4,
            ..Default::default()
        };
        let planner = AdaptivePlanner::new(config);

        // High urban ratio should increase workers
        let density = DensityDistribution {
            urban_ratio: 0.8,
            suburban_ratio: 0.15,
            rural_ratio: 0.05,
            avg_urban_density: 0.03,
            avg_suburban_density: 0.015,
            avg_rural_density: 0.005,
            complexity_score: 0.7,
        };

        let global = GlobalPercentiles {
            junction_count_p15: 5.0,
            junction_count_p50: 15.0,
            junction_count_p85: 25.0,
            junction_count_p99: 40.0,
            total_length_p15: 500.0,
            total_length_p50: 2000.0,
            total_length_p85: 5000.0,
            total_length_p99: 8000.0,
            density_p15: 0.01,
            density_p50: 0.03,
            density_p85: 0.05,
            density_p99: 0.08,
        };

        let scaling = planner.calculate_worker_scaling(&density, &global);

        assert!(scaling.recommended_workers >= 4); // Should recommend at least base workers
        assert!(scaling.urban_factor > 1.0); // Urban areas increase workers
        assert!(scaling.complexity_factor > 1.0); // High complexity increases workers
    }

    #[test]
    fn test_complexity_score_calculation() {
        let config = PlanConfig::default();
        let planner = AdaptivePlanner::new(config);

        // High complexity telemetry
        let high_complexity = TelemetryOutput {
            tile_size_meters: 125.0,
            total_tiles: 100,
            global_percentiles: GlobalPercentiles {
                junction_count_p15: 10.0,
                junction_count_p50: 20.0,
                junction_count_p85: 30.0,
                junction_count_p99: 50.0,
                total_length_p15: 2000.0,
                total_length_p50: 5000.0,
                total_length_p85: 8000.0,
                total_length_p99: 12000.0,
                density_p15: 0.02,
                density_p50: 0.04,
                density_p85: 0.06,
                density_p99: 0.1,
            },
            tiles: vec![],
        };

        let score = planner.calculate_complexity_score(&high_complexity);
        assert!(score > 0.7); // Should be high complexity

        // Low complexity telemetry
        let low_complexity = TelemetryOutput {
            tile_size_meters: 125.0,
            total_tiles: 100,
            global_percentiles: GlobalPercentiles {
                junction_count_p15: 0.0,
                junction_count_p50: 1.0,
                junction_count_p85: 3.0,
                junction_count_p99: 5.0,
                total_length_p15: 50.0,
                total_length_p50: 200.0,
                total_length_p85: 500.0,
                total_length_p99: 1000.0,
                density_p15: 0.001,
                density_p50: 0.005,
                density_p85: 0.01,
                density_p99: 0.02,
            },
            tiles: vec![],
        };

        let score = planner.calculate_complexity_score(&low_complexity);
        assert!(score < 0.3); // Should be low complexity
    }
}
