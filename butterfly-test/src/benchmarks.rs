//! Comprehensive benchmarking infrastructure and micro-benchmark suite

use crate::generators::{KnownProblematicRegions, ProblematicRegionGenerator};
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Benchmark results and statistics
#[derive(Debug, Clone)]
pub struct BenchmarkResults {
    pub name: String,
    pub iterations: usize,
    pub total_duration: Duration,
    pub mean_duration: Duration,
    pub min_duration: Duration,
    pub max_duration: Duration,
    pub std_deviation: Duration,
}

impl BenchmarkResults {
    pub fn throughput_per_second(&self) -> f64 {
        self.iterations as f64 / self.total_duration.as_secs_f64()
    }

    pub fn format_summary(&self) -> String {
        format!(
            "{}: {} iterations, mean: {:?}, min: {:?}, max: {:?}, throughput: {:.2}/sec",
            self.name,
            self.iterations,
            self.mean_duration,
            self.min_duration,
            self.max_duration,
            self.throughput_per_second()
        )
    }
}

/// Advanced benchmark runner with statistics
pub struct BenchmarkRunner {
    results: HashMap<String, BenchmarkResults>,
    warmup_iterations: usize,
    measurement_iterations: usize,
}

impl BenchmarkRunner {
    pub fn new() -> Self {
        Self {
            results: HashMap::new(),
            warmup_iterations: 3,
            measurement_iterations: 10,
        }
    }

    pub fn with_iterations(warmup: usize, measurement: usize) -> Self {
        Self {
            results: HashMap::new(),
            warmup_iterations: warmup,
            measurement_iterations: measurement,
        }
    }

    /// Run a benchmark with multiple iterations and statistical analysis
    pub fn benchmark<F, R>(&mut self, name: &str, mut f: F) -> BenchmarkResults
    where
        F: FnMut() -> R,
    {
        // Warmup runs
        for _ in 0..self.warmup_iterations {
            let _ = f();
        }

        // Measurement runs
        let mut durations = Vec::new();
        let overall_start = Instant::now();

        for _ in 0..self.measurement_iterations {
            let start = Instant::now();
            let _ = f();
            durations.push(start.elapsed());
        }

        let total_duration = overall_start.elapsed();
        let mean_duration = Duration::from_nanos(
            (durations.iter().map(|d| d.as_nanos()).sum::<u128>() / durations.len() as u128) as u64,
        );
        let min_duration = *durations.iter().min().unwrap();
        let max_duration = *durations.iter().max().unwrap();

        // Calculate standard deviation
        let variance = durations
            .iter()
            .map(|d| {
                let diff = d.as_nanos() as f64 - mean_duration.as_nanos() as f64;
                diff * diff
            })
            .sum::<f64>()
            / durations.len() as f64;
        let std_deviation = Duration::from_nanos(variance.sqrt() as u64);

        let results = BenchmarkResults {
            name: name.to_string(),
            iterations: self.measurement_iterations,
            total_duration,
            mean_duration,
            min_duration,
            max_duration,
            std_deviation,
        };

        println!("{}", results.format_summary());
        self.results.insert(name.to_string(), results.clone());
        results
    }

    /// Simple single-run benchmark for backwards compatibility
    pub fn time_function<F, R>(name: &str, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        let start = Instant::now();
        let result = f();
        let elapsed = start.elapsed();
        println!("Benchmark {}: {:?}", name, elapsed);
        result
    }

    /// Get all benchmark results
    pub fn results(&self) -> &HashMap<String, BenchmarkResults> {
        &self.results
    }

    /// Print a summary of all benchmarks
    pub fn print_summary(&self) {
        println!("\n=== Benchmark Summary ===");
        let mut sorted_results: Vec<_> = self.results.values().collect();
        sorted_results.sort_by(|a, b| a.mean_duration.cmp(&b.mean_duration));

        for result in sorted_results {
            println!("{}", result.format_summary());
        }
    }
}

impl Default for BenchmarkRunner {
    fn default() -> Self {
        Self::new()
    }
}

/// Geometry operation micro-benchmarks
pub struct GeometryBenchmarks;

impl GeometryBenchmarks {
    /// Benchmark distance calculations on problematic data
    pub fn benchmark_distance_calculations(runner: &mut BenchmarkRunner) {
        let mut gen = ProblematicRegionGenerator::new();

        // Benchmark normal coordinates
        let normal_points = (0..1000)
            .map(|_| {
                (
                    rand::random::<f64>() * 360.0 - 180.0,
                    rand::random::<f64>() * 180.0 - 90.0,
                )
            })
            .collect::<Vec<_>>();

        runner.benchmark("distance_normal_coords", || {
            for i in 0..(normal_points.len() - 1) {
                let _ = haversine_distance(normal_points[i], normal_points[i + 1]);
            }
        });

        // Benchmark dateline crossing
        let dateline_points = gen.generate_dateline_crossing(1000);
        runner.benchmark("distance_dateline_crossing", || {
            for i in 0..(dateline_points.len() - 1) {
                let _ = haversine_distance(dateline_points[i], dateline_points[i + 1]);
            }
        });

        // Benchmark polar regions
        let polar_points = gen.generate_polar_regions(1000);
        runner.benchmark("distance_polar_regions", || {
            for i in 0..(polar_points.len() - 1) {
                let _ = haversine_distance(polar_points[i], polar_points[i + 1]);
            }
        });

        // Benchmark precision stress
        let precision_points = gen.generate_precision_stress(1000);
        runner.benchmark("distance_precision_stress", || {
            for i in 0..(precision_points.len() - 1) {
                let _ = haversine_distance(precision_points[i], precision_points[i + 1]);
            }
        });
    }

    /// Benchmark simplification algorithms on various data types
    pub fn benchmark_simplification(runner: &mut BenchmarkRunner) {
        let mut gen = ProblematicRegionGenerator::new();

        // Test on normal data
        let normal_linestring = (0..10000)
            .map(|i| (i as f64 * 0.01, (i as f64 * 0.01).sin()))
            .collect::<Vec<_>>();

        runner.benchmark("simplify_normal_linestring", || {
            let _ = douglas_peucker_simplify(&normal_linestring, 0.001);
        });

        // Test on degenerate shapes
        let degenerate_shapes = gen.generate_degenerate_shapes(10000);
        runner.benchmark("simplify_degenerate_shapes", || {
            let _ = douglas_peucker_simplify(&degenerate_shapes, 0.001);
        });

        // Test on dense clusters
        let dense_cluster = gen.generate_dense_cluster(10000);
        runner.benchmark("simplify_dense_cluster", || {
            let _ = douglas_peucker_simplify(&dense_cluster, 0.001);
        });
    }

    /// Benchmark coordinate transformations
    pub fn benchmark_coordinate_transforms(runner: &mut BenchmarkRunner) {
        let test_points = (0..10000)
            .map(|_| {
                (
                    rand::random::<f64>() * 360.0 - 180.0,
                    rand::random::<f64>() * 180.0 - 90.0,
                )
            })
            .collect::<Vec<_>>();

        runner.benchmark("wgs84_to_mercator", || {
            for point in &test_points {
                let _ = wgs84_to_web_mercator(*point);
            }
        });

        runner.benchmark("mercator_to_wgs84", || {
            for point in &test_points {
                let mercator = wgs84_to_web_mercator(*point);
                let _ = web_mercator_to_wgs84(mercator);
            }
        });

        // Test on problematic regions
        let polar_points = KnownProblematicRegions::arctic_ocean_region(1000);
        runner.benchmark("transform_polar_regions", || {
            for point in &polar_points {
                let _ = wgs84_to_web_mercator(*point);
            }
        });
    }

    /// Run all geometry benchmarks
    pub fn run_all_benchmarks() -> HashMap<String, BenchmarkResults> {
        let mut runner = BenchmarkRunner::with_iterations(5, 20);

        println!("Running geometry micro-benchmarks...");

        Self::benchmark_distance_calculations(&mut runner);
        Self::benchmark_simplification(&mut runner);
        Self::benchmark_coordinate_transforms(&mut runner);

        runner.print_summary();
        runner.results().clone()
    }
}

// Helper functions for benchmarks

/// Haversine distance calculation
fn haversine_distance(p1: (f64, f64), p2: (f64, f64)) -> f64 {
    let r = 6371000.0; // Earth radius in meters
    let lat1_rad = p1.1.to_radians();
    let lat2_rad = p2.1.to_radians();
    let delta_lat = (p2.1 - p1.1).to_radians();
    let delta_lon = (p2.0 - p1.0).to_radians();

    let a = (delta_lat / 2.0).sin().powi(2)
        + lat1_rad.cos() * lat2_rad.cos() * (delta_lon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());

    r * c
}

/// Simplified Douglas-Peucker algorithm for testing
fn douglas_peucker_simplify(points: &[(f64, f64)], tolerance: f64) -> Vec<(f64, f64)> {
    if points.len() <= 2 {
        return points.to_vec();
    }

    let mut max_distance = 0.0;
    let mut max_index = 0;

    for i in 1..(points.len() - 1) {
        let distance = point_line_distance(points[i], points[0], points[points.len() - 1]);
        if distance > max_distance {
            max_distance = distance;
            max_index = i;
        }
    }

    if max_distance > tolerance {
        let left = douglas_peucker_simplify(&points[0..=max_index], tolerance);
        let right = douglas_peucker_simplify(&points[max_index..], tolerance);

        let mut result = left;
        result.extend_from_slice(&right[1..]);
        result
    } else {
        vec![points[0], points[points.len() - 1]]
    }
}

/// Point to line distance
fn point_line_distance(point: (f64, f64), line_start: (f64, f64), line_end: (f64, f64)) -> f64 {
    let a = line_end.1 - line_start.1;
    let b = line_start.0 - line_end.0;
    let c = line_end.0 * line_start.1 - line_start.0 * line_end.1;

    (a * point.0 + b * point.1 + c).abs() / (a * a + b * b).sqrt()
}

/// WGS84 to Web Mercator transformation
fn wgs84_to_web_mercator(point: (f64, f64)) -> (f64, f64) {
    let x = point.0 * 20037508.34 / 180.0;
    let y = ((90.0 + point.1) * std::f64::consts::PI / 360.0).tan().ln()
        / (std::f64::consts::PI / 180.0);
    let y = y * 20037508.34 / 180.0;
    (x, y)
}

/// Web Mercator to WGS84 transformation  
fn web_mercator_to_wgs84(point: (f64, f64)) -> (f64, f64) {
    let lon = point.0 * 180.0 / 20037508.34;
    let lat = (2.0
        * ((point.1 * 180.0 / 20037508.34 * std::f64::consts::PI / 180.0)
            .exp()
            .atan())
        - std::f64::consts::PI / 2.0)
        * 180.0
        / std::f64::consts::PI;
    (lon, lat)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_benchmark_runner() {
        let mut runner = BenchmarkRunner::with_iterations(1, 3);

        let result = runner.benchmark("test_sleep", || {
            std::thread::sleep(Duration::from_millis(1));
        });

        assert_eq!(result.iterations, 3);
        assert!(result.mean_duration >= Duration::from_millis(1));
        assert!(result.throughput_per_second() > 0.0);
    }

    #[test]
    fn test_haversine_distance() {
        // Distance from NYC to London (approximately 5585 km)
        let nyc = (-74.006, 40.7128);
        let london = (-0.1278, 51.5074);
        let distance = haversine_distance(nyc, london);

        // Should be approximately 5.585 million meters
        assert!((distance - 5_585_000.0).abs() < 100_000.0);
    }

    #[test]
    fn test_coordinate_transforms() {
        let wgs84_point = (0.0, 0.0); // Null Island
        let mercator_point = wgs84_to_web_mercator(wgs84_point);
        let back_to_wgs84 = web_mercator_to_wgs84(mercator_point);

        assert!((back_to_wgs84.0 - wgs84_point.0).abs() < 0.0001);
        assert!((back_to_wgs84.1 - wgs84_point.1).abs() < 0.0001);
    }

    #[test]
    fn test_douglas_peucker() {
        let points = vec![(0.0, 0.0), (1.0, 0.1), (2.0, 0.0)];
        let simplified = douglas_peucker_simplify(&points, 0.2);

        // Should simplify to just start and end points
        assert_eq!(simplified.len(), 2);
        assert_eq!(simplified[0], (0.0, 0.0));
        assert_eq!(simplified[1], (2.0, 0.0));
    }
}
