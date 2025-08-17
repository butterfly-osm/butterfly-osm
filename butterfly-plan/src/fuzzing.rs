//! Plan fuzzing and validation for M1.5
//!
//! Implements synthetic histogram fuzzing with planet-like distributions
//! and safety invariant checking to ensure plans never exceed compile-time cap

use crate::{AdaptivePlanner, MemoryBudget, PlanConfig, BFLY_MAX_RAM_MB};
use butterfly_extract::{
    DensityClass, GlobalPercentiles, TelemetryOutput, TileId, TileMetrics, TileTelemetry,
};

/// Fuzzing configuration for synthetic data generation
#[derive(Debug, Clone)]
pub struct FuzzingConfig {
    /// Number of synthetic tiles to generate
    pub tile_count: usize,
    /// Distribution type for testing
    pub distribution_type: DistributionType,
    /// Maximum complexity multiplier for stress testing
    pub max_complexity_multiplier: f64,
    /// Enable planet-scale simulation (millions of tiles)
    pub planet_scale: bool,
    /// Random seed for reproducible fuzzing
    pub seed: u64,
}

impl Default for FuzzingConfig {
    fn default() -> Self {
        Self {
            tile_count: 10000,
            distribution_type: DistributionType::Realistic,
            max_complexity_multiplier: 10.0,
            planet_scale: false,
            seed: 42,
        }
    }
}

/// Distribution types for synthetic data generation
#[derive(Debug, Clone, PartialEq)]
pub enum DistributionType {
    /// Realistic distribution based on real-world patterns
    Realistic,
    /// Urban-heavy distribution (stress test for memory)
    UrbanHeavy,
    /// Rural-only distribution (minimal complexity)
    RuralOnly,
    /// Extreme complexity (maximum junctions and density)
    ExtremeComplexity,
    /// Planet-like distribution with long tails and hot spots
    PlanetLike,
    /// Random distribution for edge case testing
    Random,
}

/// Synthetic telemetry generator for fuzzing
pub struct TelemetryFuzzer {
    config: FuzzingConfig,
    rng_state: u64, // Simple LCG for deterministic random numbers
}

impl TelemetryFuzzer {
    /// Create new telemetry fuzzer
    pub fn new(config: FuzzingConfig) -> Self {
        Self {
            rng_state: config.seed,
            config,
        }
    }

    /// Generate synthetic telemetry data for fuzzing
    pub fn generate_synthetic_telemetry(&mut self) -> TelemetryOutput {
        let tiles = match self.config.distribution_type {
            DistributionType::Realistic => self.generate_realistic_distribution(),
            DistributionType::UrbanHeavy => self.generate_urban_heavy_distribution(),
            DistributionType::RuralOnly => self.generate_rural_only_distribution(),
            DistributionType::ExtremeComplexity => self.generate_extreme_complexity_distribution(),
            DistributionType::PlanetLike => self.generate_planet_like_distribution(),
            DistributionType::Random => self.generate_random_distribution(),
        };

        let global_percentiles = self.calculate_global_percentiles(&tiles);

        TelemetryOutput {
            tile_size_meters: 125.0,
            total_tiles: tiles.len(),
            global_percentiles,
            tiles,
        }
    }

    /// Generate realistic world-like distribution
    fn generate_realistic_distribution(&mut self) -> Vec<TileTelemetry> {
        let mut tiles = Vec::new();
        let total_tiles = self.config.tile_count;

        // Realistic proportions: ~10% urban, ~30% suburban, ~60% rural
        let urban_count = (total_tiles as f64 * 0.1) as usize;
        let suburban_count = (total_tiles as f64 * 0.3) as usize;
        let rural_count = total_tiles - urban_count - suburban_count;

        // Generate urban tiles (high density, many junctions)
        for i in 0..urban_count {
            let tile_id = TileId { x: i as i32, y: 0 };
            let metrics = self.generate_urban_metrics();
            tiles.push(self.create_tile_telemetry(tile_id, metrics, DensityClass::Urban));
        }

        // Generate suburban tiles (medium density)
        for i in 0..suburban_count {
            let tile_id = TileId {
                x: (urban_count + i) as i32,
                y: 0,
            };
            let metrics = self.generate_suburban_metrics();
            tiles.push(self.create_tile_telemetry(tile_id, metrics, DensityClass::Suburban));
        }

        // Generate rural tiles (low density)
        for i in 0..rural_count {
            let tile_id = TileId {
                x: (urban_count + suburban_count + i) as i32,
                y: 0,
            };
            let metrics = self.generate_rural_metrics();
            tiles.push(self.create_tile_telemetry(tile_id, metrics, DensityClass::Rural));
        }

        tiles
    }

    /// Generate urban-heavy distribution for stress testing
    fn generate_urban_heavy_distribution(&mut self) -> Vec<TileTelemetry> {
        let mut tiles = Vec::new();

        // 80% urban, 15% suburban, 5% rural - stress test memory usage
        let urban_count = (self.config.tile_count as f64 * 0.8) as usize;
        let suburban_count = (self.config.tile_count as f64 * 0.15) as usize;
        let rural_count = self.config.tile_count - urban_count - suburban_count;

        for i in 0..urban_count {
            let tile_id = TileId { x: i as i32, y: 1 };
            let mut metrics = self.generate_urban_metrics();
            // Amplify complexity for stress testing
            metrics.total_length_m *= self.config.max_complexity_multiplier;
            metrics.junction_count =
                (metrics.junction_count as f64 * self.config.max_complexity_multiplier) as u32;
            tiles.push(self.create_tile_telemetry(tile_id, metrics, DensityClass::Urban));
        }

        for i in 0..suburban_count {
            let tile_id = TileId {
                x: (urban_count + i) as i32,
                y: 1,
            };
            let metrics = self.generate_suburban_metrics();
            tiles.push(self.create_tile_telemetry(tile_id, metrics, DensityClass::Suburban));
        }

        for i in 0..rural_count {
            let tile_id = TileId {
                x: (urban_count + suburban_count + i) as i32,
                y: 1,
            };
            let metrics = self.generate_rural_metrics();
            tiles.push(self.create_tile_telemetry(tile_id, metrics, DensityClass::Rural));
        }

        tiles
    }

    /// Generate rural-only distribution (minimal complexity)
    fn generate_rural_only_distribution(&mut self) -> Vec<TileTelemetry> {
        (0..self.config.tile_count)
            .map(|i| {
                let tile_id = TileId { x: i as i32, y: 2 };
                let metrics = self.generate_rural_metrics();
                self.create_tile_telemetry(tile_id, metrics, DensityClass::Rural)
            })
            .collect()
    }

    /// Generate extreme complexity distribution for edge case testing
    fn generate_extreme_complexity_distribution(&mut self) -> Vec<TileTelemetry> {
        (0..self.config.tile_count)
            .map(|i| {
                let tile_id = TileId { x: i as i32, y: 3 };
                let metrics = TileMetrics {
                    junction_count: (50.0 * self.config.max_complexity_multiplier) as u32, // Extreme junction count
                    total_length_m: 15000.0 * self.config.max_complexity_multiplier, // Max road density
                    way_lengths: vec![100.0; 150], // Many short segments
                    way_curvatures: vec![0.5; 150], // High curvature
                    junction_complexities: vec![7.0; 50], // Complex junctions
                };
                self.create_tile_telemetry(tile_id, metrics, DensityClass::Urban)
            })
            .collect()
    }

    /// Generate planet-like distribution with long tails and hot urban spots
    fn generate_planet_like_distribution(&mut self) -> Vec<TileTelemetry> {
        let mut tiles = Vec::new();
        let tile_count = if self.config.planet_scale {
            1_000_000
        } else {
            self.config.tile_count
        };

        // Planet-like distribution:
        // - 0.1% mega-urban hot spots
        // - 2% urban areas
        // - 15% suburban areas
        // - 82.9% rural/empty areas

        let mega_urban_count = (tile_count as f64 * 0.001) as usize;
        let urban_count = (tile_count as f64 * 0.02) as usize;
        let suburban_count = (tile_count as f64 * 0.15) as usize;
        let rural_count = tile_count - mega_urban_count - urban_count - suburban_count;

        // Mega-urban hot spots (extreme density)
        for i in 0..mega_urban_count {
            let tile_id = TileId {
                x: i as i32,
                y: 100,
            };
            let metrics = TileMetrics {
                junction_count: (100.0 * self.config.max_complexity_multiplier) as u32,
                total_length_m: 25000.0 * self.config.max_complexity_multiplier,
                way_lengths: vec![50.0; 500],
                way_curvatures: vec![0.8; 500],
                junction_complexities: vec![10.0; 100],
            };
            tiles.push(self.create_tile_telemetry(tile_id, metrics, DensityClass::Urban));
        }

        // Standard urban areas
        for i in 0..urban_count {
            let tile_id = TileId {
                x: (mega_urban_count + i) as i32,
                y: 100,
            };
            let metrics = self.generate_urban_metrics();
            tiles.push(self.create_tile_telemetry(tile_id, metrics, DensityClass::Urban));
        }

        // Suburban areas
        for i in 0..suburban_count {
            let tile_id = TileId {
                x: (mega_urban_count + urban_count + i) as i32,
                y: 100,
            };
            let metrics = self.generate_suburban_metrics();
            tiles.push(self.create_tile_telemetry(tile_id, metrics, DensityClass::Suburban));
        }

        // Rural areas (long tail)
        for i in 0..rural_count {
            let tile_id = TileId {
                x: (mega_urban_count + urban_count + suburban_count + i) as i32,
                y: 100,
            };
            let metrics = self.generate_rural_metrics();
            tiles.push(self.create_tile_telemetry(tile_id, metrics, DensityClass::Rural));
        }

        tiles
    }

    /// Generate random distribution for edge case testing
    fn generate_random_distribution(&mut self) -> Vec<TileTelemetry> {
        (0..self.config.tile_count)
            .map(|i| {
                let tile_id = TileId { x: i as i32, y: 4 };

                // Random metrics within reasonable bounds
                let junction_count =
                    (self.random_f64() * 50.0 * self.config.max_complexity_multiplier) as u32;
                let total_length_m =
                    self.random_f64() * 20000.0 * self.config.max_complexity_multiplier;

                let density_class = match self.random_f64() {
                    x if x < 0.1 => DensityClass::Urban,
                    x if x < 0.4 => DensityClass::Suburban,
                    _ => DensityClass::Rural,
                };

                let way_count = (self.random_f64() * 200.0) as usize;
                let metrics = TileMetrics {
                    junction_count,
                    total_length_m,
                    way_lengths: (0..way_count).map(|_| self.random_f64() * 500.0).collect(),
                    way_curvatures: (0..way_count).map(|_| self.random_f64()).collect(),
                    junction_complexities: (0..junction_count as usize)
                        .map(|_| self.random_f64() * 10.0)
                        .collect(),
                };

                self.create_tile_telemetry(tile_id, metrics, density_class)
            })
            .collect()
    }

    /// Generate typical urban tile metrics
    fn generate_urban_metrics(&mut self) -> TileMetrics {
        let junction_count = 5 + (self.random_f64() * 15.0) as u32; // 5-20 junctions
        let total_length_m = 2000.0 + self.random_f64() * 6000.0; // 2-8km of roads
        let way_count = 30 + (self.random_f64() * 50.0) as usize;

        TileMetrics {
            junction_count,
            total_length_m,
            way_lengths: (0..way_count)
                .map(|_| 50.0 + self.random_f64() * 200.0)
                .collect(),
            way_curvatures: (0..way_count).map(|_| self.random_f64() * 0.6).collect(),
            junction_complexities: (0..junction_count as usize)
                .map(|_| 2.0 + self.random_f64() * 5.0)
                .collect(),
        }
    }

    /// Generate typical suburban tile metrics
    fn generate_suburban_metrics(&mut self) -> TileMetrics {
        let junction_count = 1 + (self.random_f64() * 8.0) as u32; // 1-9 junctions
        let total_length_m = 500.0 + self.random_f64() * 2000.0; // 0.5-2.5km of roads
        let way_count = 10 + (self.random_f64() * 20.0) as usize;

        TileMetrics {
            junction_count,
            total_length_m,
            way_lengths: (0..way_count)
                .map(|_| 100.0 + self.random_f64() * 300.0)
                .collect(),
            way_curvatures: (0..way_count).map(|_| self.random_f64() * 0.3).collect(),
            junction_complexities: (0..junction_count as usize)
                .map(|_| 1.0 + self.random_f64() * 3.0)
                .collect(),
        }
    }

    /// Generate typical rural tile metrics
    fn generate_rural_metrics(&mut self) -> TileMetrics {
        let junction_count = (self.random_f64() * 3.0) as u32; // 0-3 junctions
        let total_length_m = self.random_f64() * 800.0; // 0-800m of roads
        let way_count = (self.random_f64() * 10.0) as usize;

        TileMetrics {
            junction_count,
            total_length_m,
            way_lengths: (0..way_count)
                .map(|_| 200.0 + self.random_f64() * 500.0)
                .collect(),
            way_curvatures: (0..way_count).map(|_| self.random_f64() * 0.2).collect(),
            junction_complexities: (0..junction_count as usize)
                .map(|_| self.random_f64() * 2.0)
                .collect(),
        }
    }

    /// Create tile telemetry from metrics
    fn create_tile_telemetry(
        &self,
        tile_id: TileId,
        metrics: TileMetrics,
        density_class: DensityClass,
    ) -> TileTelemetry {
        let percentiles = metrics.calculate_percentiles();

        TileTelemetry {
            tile_id,
            bbox: tile_id.bbox(),
            metrics,
            percentiles,
            density_class,
        }
    }

    /// Calculate global percentiles from all tiles
    fn calculate_global_percentiles(&self, tiles: &[TileTelemetry]) -> GlobalPercentiles {
        let junction_counts: Vec<f64> = tiles
            .iter()
            .map(|t| t.metrics.junction_count as f64)
            .collect();
        let total_lengths: Vec<f64> = tiles.iter().map(|t| t.metrics.total_length_m).collect();
        let densities: Vec<f64> = tiles
            .iter()
            .map(|t| t.metrics.total_length_m / (125.0 * 125.0))
            .collect();

        GlobalPercentiles {
            junction_count_p15: percentile(&junction_counts, 0.15),
            junction_count_p50: percentile(&junction_counts, 0.50),
            junction_count_p85: percentile(&junction_counts, 0.85),
            junction_count_p99: percentile(&junction_counts, 0.99),

            total_length_p15: percentile(&total_lengths, 0.15),
            total_length_p50: percentile(&total_lengths, 0.50),
            total_length_p85: percentile(&total_lengths, 0.85),
            total_length_p99: percentile(&total_lengths, 0.99),

            density_p15: percentile(&densities, 0.15),
            density_p50: percentile(&densities, 0.50),
            density_p85: percentile(&densities, 0.85),
            density_p99: percentile(&densities, 0.99),
        }
    }

    /// Simple deterministic random number generator (LCG)
    fn random_f64(&mut self) -> f64 {
        // Linear congruential generator
        self.rng_state = self.rng_state.wrapping_mul(1103515245).wrapping_add(12345);
        (self.rng_state & 0x7FFFFFFF) as f64 / 0x7FFFFFFF as f64
    }
}

/// Plan fuzzer for invariant checking
pub struct PlanFuzzer {
    telemetry_fuzzer: TelemetryFuzzer,
}

impl PlanFuzzer {
    /// Create new plan fuzzer
    pub fn new(config: FuzzingConfig) -> Self {
        Self {
            telemetry_fuzzer: TelemetryFuzzer::new(config),
        }
    }

    /// Run comprehensive fuzzing tests
    pub fn run_fuzzing_tests(&mut self) -> FuzzingResults {
        let mut results = FuzzingResults::new();

        // Test all distribution types
        let distributions = vec![
            DistributionType::Realistic,
            DistributionType::UrbanHeavy,
            DistributionType::RuralOnly,
            DistributionType::ExtremeComplexity,
            DistributionType::PlanetLike,
            DistributionType::Random,
        ];

        for distribution in distributions {
            let test_result = self.fuzz_distribution(distribution);
            results.add_test_result(test_result);
        }

        results
    }

    /// Fuzz test a specific distribution type
    fn fuzz_distribution(&mut self, distribution: DistributionType) -> DistributionTestResult {
        // Update fuzzer config for this distribution
        self.telemetry_fuzzer.config.distribution_type = distribution.clone();

        // Generate synthetic telemetry
        let telemetry = self.telemetry_fuzzer.generate_synthetic_telemetry();

        // Test different base configurations
        let configs = self.generate_test_configurations();
        let mut config_results = Vec::new();

        for config in configs {
            let config_result = self.test_config_with_telemetry(&config, &telemetry);
            config_results.push(config_result);
        }

        DistributionTestResult {
            distribution_type: distribution,
            total_tiles: telemetry.total_tiles,
            config_results,
        }
    }

    /// Generate various configurations for testing
    fn generate_test_configurations(&self) -> Vec<PlanConfig> {
        vec![
            // Minimal configuration
            PlanConfig {
                max_ram_mb: 1024, // 1GB
                workers: 1,
                ..Default::default()
            },
            // Typical configuration
            PlanConfig {
                max_ram_mb: 4096, // 4GB
                workers: 4,
                ..Default::default()
            },
            // High-memory configuration
            PlanConfig {
                max_ram_mb: 8192, // 8GB
                workers: 8,
                ..Default::default()
            },
            // Maximum configuration (at compile-time cap)
            PlanConfig {
                max_ram_mb: BFLY_MAX_RAM_MB, // 16GB
                workers: 16,
                ..Default::default()
            },
        ]
    }

    /// Test a specific configuration with telemetry data
    fn test_config_with_telemetry(
        &self,
        config: &PlanConfig,
        telemetry: &TelemetryOutput,
    ) -> ConfigTestResult {
        let mut result = ConfigTestResult {
            base_config: config.clone(),
            invariant_violations: Vec::new(),
            memory_usage: None,
            adaptive_recommendations: Vec::new(),
            test_passed: true,
        };

        // Test 1: Basic memory budget validation
        let budget = MemoryBudget::new(config.max_ram_mb, config.workers);
        if !budget.validate(config.workers) {
            result.invariant_violations.push(InvariantViolation {
                violation_type: ViolationType::MemoryBudgetExceeded,
                description: "Base memory budget validation failed".to_string(),
                severity: Severity::Critical,
            });
            result.test_passed = false;
        }

        // Test 2: Compile-time cap invariant
        if config.max_ram_mb > BFLY_MAX_RAM_MB {
            result.invariant_violations.push(InvariantViolation {
                violation_type: ViolationType::CompileTimeCapExceeded,
                description: format!(
                    "Configuration exceeds compile-time cap: {} MB > {} MB",
                    config.max_ram_mb, BFLY_MAX_RAM_MB
                ),
                severity: Severity::Critical,
            });
            result.test_passed = false;
        }

        // Test 3: Adaptive planning with telemetry
        let mut adaptive_planner = AdaptivePlanner::new(config.clone());
        adaptive_planner.load_telemetry(telemetry.clone());
        let adaptive_plan = adaptive_planner.create_adaptive_plan();

        // Test adaptive budget
        let adaptive_budget = adaptive_plan.create_adaptive_budget();
        if !adaptive_budget.validate(adaptive_plan.worker_scaling.recommended_workers as u32) {
            result.invariant_violations.push(InvariantViolation {
                violation_type: ViolationType::AdaptiveBudgetExceeded,
                description: "Adaptive memory budget validation failed".to_string(),
                severity: Severity::High,
            });
            result.test_passed = false;
        }

        // Test 4: Adaptive cap invariant
        if adaptive_budget.cap_mb > BFLY_MAX_RAM_MB {
            result.invariant_violations.push(InvariantViolation {
                violation_type: ViolationType::AdaptiveCapExceeded,
                description: format!(
                    "Adaptive plan exceeds compile-time cap: {} MB > {} MB",
                    adaptive_budget.cap_mb, BFLY_MAX_RAM_MB
                ),
                severity: Severity::Critical,
            });
            result.test_passed = false;
        }

        // Test 5: Worker scaling sanity
        if adaptive_plan.worker_scaling.recommended_workers == 0 {
            result.invariant_violations.push(InvariantViolation {
                violation_type: ViolationType::InvalidWorkerCount,
                description: "Adaptive plan recommended 0 workers".to_string(),
                severity: Severity::High,
            });
            result.test_passed = false;
        }

        // Store results
        result.memory_usage = Some(MemoryUsageStats {
            base_budget_mb: budget.cap_mb,
            adaptive_budget_mb: adaptive_budget.cap_mb,
            scaling_factor: adaptive_plan.worker_scaling.scaling_factor,
            memory_efficiency: adaptive_budget.usable_mb as f64 / adaptive_budget.cap_mb as f64,
        });

        result.adaptive_recommendations = adaptive_plan.recommendations();

        result
    }
}

/// Results from fuzzing tests
#[derive(Debug)]
pub struct FuzzingResults {
    pub distribution_results: Vec<DistributionTestResult>,
    pub total_tests: usize,
    pub passed_tests: usize,
    pub failed_tests: usize,
    pub critical_violations: usize,
}

impl FuzzingResults {
    fn new() -> Self {
        Self {
            distribution_results: Vec::new(),
            total_tests: 0,
            passed_tests: 0,
            failed_tests: 0,
            critical_violations: 0,
        }
    }

    fn add_test_result(&mut self, result: DistributionTestResult) {
        for config_result in &result.config_results {
            self.total_tests += 1;
            if config_result.test_passed {
                self.passed_tests += 1;
            } else {
                self.failed_tests += 1;
            }

            self.critical_violations += config_result
                .invariant_violations
                .iter()
                .filter(|v| v.severity == Severity::Critical)
                .count();
        }

        self.distribution_results.push(result);
    }

    /// Check if all tests passed
    pub fn all_tests_passed(&self) -> bool {
        self.failed_tests == 0 && self.critical_violations == 0
    }

    /// Generate summary report
    pub fn summary(&self) -> String {
        format!(
            "Fuzzing Results Summary:\n\
             Total Tests: {}\n\
             Passed: {}\n\
             Failed: {}\n\
             Critical Violations: {}\n\
             Success Rate: {:.1}%",
            self.total_tests,
            self.passed_tests,
            self.failed_tests,
            self.critical_violations,
            if self.total_tests > 0 {
                (self.passed_tests as f64 / self.total_tests as f64) * 100.0
            } else {
                0.0
            }
        )
    }
}

/// Results for a specific distribution type
#[derive(Debug)]
pub struct DistributionTestResult {
    pub distribution_type: DistributionType,
    pub total_tiles: usize,
    pub config_results: Vec<ConfigTestResult>,
}

/// Results for a specific configuration test
#[derive(Debug)]
pub struct ConfigTestResult {
    pub base_config: PlanConfig,
    pub invariant_violations: Vec<InvariantViolation>,
    pub memory_usage: Option<MemoryUsageStats>,
    pub adaptive_recommendations: Vec<String>,
    pub test_passed: bool,
}

/// Invariant violation details
#[derive(Debug)]
pub struct InvariantViolation {
    pub violation_type: ViolationType,
    pub description: String,
    pub severity: Severity,
}

#[derive(Debug, PartialEq)]
pub enum ViolationType {
    MemoryBudgetExceeded,
    CompileTimeCapExceeded,
    AdaptiveBudgetExceeded,
    AdaptiveCapExceeded,
    InvalidWorkerCount,
}

#[derive(Debug, PartialEq)]
pub enum Severity {
    Low,
    Medium,
    High,
    Critical,
}

/// Memory usage statistics
#[derive(Debug)]
pub struct MemoryUsageStats {
    pub base_budget_mb: u32,
    pub adaptive_budget_mb: u32,
    pub scaling_factor: f64,
    pub memory_efficiency: f64,
}

/// Calculate percentile of a dataset
fn percentile(data: &[f64], p: f64) -> f64 {
    if data.is_empty() {
        return 0.0;
    }

    let mut sorted_data = data.to_vec();
    sorted_data.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    let index = (p * (sorted_data.len() - 1) as f64) as usize;
    sorted_data[index.min(sorted_data.len() - 1)]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_synthetic_telemetry_generation() {
        let config = FuzzingConfig::default();
        let mut fuzzer = TelemetryFuzzer::new(config);

        let telemetry = fuzzer.generate_synthetic_telemetry();

        assert_eq!(telemetry.total_tiles, 10000);
        assert!(!telemetry.tiles.is_empty());
        assert!(telemetry.global_percentiles.density_p50 > 0.0);
    }

    #[test]
    fn test_urban_heavy_distribution() {
        let config = FuzzingConfig {
            tile_count: 1000,
            distribution_type: DistributionType::UrbanHeavy,
            ..Default::default()
        };
        let mut fuzzer = TelemetryFuzzer::new(config);

        let telemetry = fuzzer.generate_synthetic_telemetry();

        // Should have mostly urban tiles
        let urban_count = telemetry
            .tiles
            .iter()
            .filter(|t| t.density_class == DensityClass::Urban)
            .count();
        assert!(urban_count as f64 / telemetry.total_tiles as f64 > 0.7); // >70% urban
    }

    #[test]
    fn test_rural_only_distribution() {
        let config = FuzzingConfig {
            tile_count: 500,
            distribution_type: DistributionType::RuralOnly,
            ..Default::default()
        };
        let mut fuzzer = TelemetryFuzzer::new(config);

        let telemetry = fuzzer.generate_synthetic_telemetry();

        // Should have only rural tiles
        assert!(telemetry
            .tiles
            .iter()
            .all(|t| t.density_class == DensityClass::Rural));
    }

    #[test]
    fn test_planet_like_distribution() {
        let config = FuzzingConfig {
            tile_count: 10000,
            distribution_type: DistributionType::PlanetLike,
            planet_scale: false,
            ..Default::default()
        };
        let mut fuzzer = TelemetryFuzzer::new(config);

        let telemetry = fuzzer.generate_synthetic_telemetry();

        // Should have long tail distribution
        let rural_count = telemetry
            .tiles
            .iter()
            .filter(|t| t.density_class == DensityClass::Rural)
            .count();
        assert!(rural_count as f64 / telemetry.total_tiles as f64 > 0.8); // >80% rural
    }

    #[test]
    fn test_plan_fuzzing_invariants() {
        let config = FuzzingConfig {
            tile_count: 100,
            distribution_type: DistributionType::Realistic,
            max_complexity_multiplier: 2.0,
            ..Default::default()
        };
        let mut fuzzer = PlanFuzzer::new(config);

        let results = fuzzer.run_fuzzing_tests();

        // All tests should pass (no critical violations)
        assert_eq!(
            results.critical_violations, 0,
            "Critical invariant violations detected"
        );
        assert!(results.total_tests > 0, "No tests were run");
    }

    #[test]
    fn test_compile_time_cap_invariant() {
        let config = FuzzingConfig {
            tile_count: 10,
            distribution_type: DistributionType::ExtremeComplexity,
            max_complexity_multiplier: 1.0, // Don't amplify for this test
            ..Default::default()
        };
        let mut plan_fuzzer = PlanFuzzer::new(config);

        // Test with configuration at compile-time cap
        let telemetry = plan_fuzzer.telemetry_fuzzer.generate_synthetic_telemetry();
        let max_config = PlanConfig {
            max_ram_mb: BFLY_MAX_RAM_MB,
            workers: 8,
            ..Default::default()
        };

        let result = plan_fuzzer.test_config_with_telemetry(&max_config, &telemetry);

        // Should not exceed compile-time cap
        assert!(result
            .invariant_violations
            .iter()
            .all(|v| v.violation_type != ViolationType::CompileTimeCapExceeded));
        assert!(result
            .invariant_violations
            .iter()
            .all(|v| v.violation_type != ViolationType::AdaptiveCapExceeded));
    }

    #[test]
    fn test_deterministic_fuzzing() {
        let config1 = FuzzingConfig {
            seed: 12345,
            tile_count: 100,
            ..Default::default()
        };
        let config2 = FuzzingConfig {
            seed: 12345,
            tile_count: 100,
            ..Default::default()
        };

        let mut fuzzer1 = TelemetryFuzzer::new(config1);
        let mut fuzzer2 = TelemetryFuzzer::new(config2);

        let telemetry1 = fuzzer1.generate_synthetic_telemetry();
        let telemetry2 = fuzzer2.generate_synthetic_telemetry();

        // Should generate identical results with same seed
        assert_eq!(telemetry1.total_tiles, telemetry2.total_tiles);
        assert_eq!(telemetry1.tiles.len(), telemetry2.tiles.len());

        // First tile should be identical
        if !telemetry1.tiles.is_empty() && !telemetry2.tiles.is_empty() {
            assert_eq!(
                telemetry1.tiles[0].metrics.junction_count,
                telemetry2.tiles[0].metrics.junction_count
            );
        }
    }
}
