//! Profile Regression Suite v2 - Enhanced testing for M5 dual core system

use crate::dijkstra::DistanceRouter;
use crate::dual_core::{DualCoreGraph, GeometryPass, NodeId};
#[cfg(test)]
use crate::profiles::EdgeId;
use crate::profiles::{TestStatus, TransportProfile};
use crate::spatial;
use crate::spatial::SnapEngine;
use butterfly_geometry::GeometryPipeline;
use butterfly_geometry::Point2D;
use serde::{Deserialize, Serialize};
use std::time::Instant;

/// Enhanced PRS v2 test result with geometry and performance metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PRSv2TestResult {
    pub test_type: PRSv2TestType,
    pub profile: TransportProfile,
    pub status: TestStatus,
    pub message: String,
    pub metrics: PRSv2Metrics,
    pub timestamp: u64,
}

/// PRS v2 specific test types (extends PRS v1)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PRSv2TestType {
    // Legacy PRS v1 tests
    AccessLegality,
    RoutingSmoke,

    // New PRS v2 tests
    SnapRecall,
    SnapAccuracy,
    GeometryQuality,
    RoutingConsistency,
    PerformanceRegression,
    ColdIOPerformance,
    DualCoreConsistency,
}

/// Metrics specific to PRS v2 testing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PRSv2Metrics {
    pub execution_time_ms: u64,
    pub memory_usage_mb: f64,
    pub snap_recall_rate: Option<f64>,
    pub snap_accuracy_meters: Option<f64>,
    pub hausdorff_median: Option<f64>,
    pub hausdorff_p95: Option<f64>,
    pub routing_time_ms: Option<u64>,
    pub io_latency_p95_ms: Option<u64>,
    pub consistency_hash_matches: Option<bool>,
}

impl PRSv2Metrics {
    pub fn new() -> Self {
        Self {
            execution_time_ms: 0,
            memory_usage_mb: 0.0,
            snap_recall_rate: None,
            snap_accuracy_meters: None,
            hausdorff_median: None,
            hausdorff_p95: None,
            routing_time_ms: None,
            io_latency_p95_ms: None,
            consistency_hash_matches: None,
        }
    }
}

/// PRS v2 test configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PRSv2Config {
    pub snap_recall_target: f64,        // ≥98% within 5m
    pub snap_accuracy_target: f64,      // ≤2m median accuracy
    pub hausdorff_median_target: f64,   // ≤2m median Hausdorff
    pub hausdorff_p95_target: f64,      // ≤5m p95 Hausdorff
    pub routing_time_budget_ms: u64,    // Performance budget per route
    pub cold_io_p95_target_ms: u64,     // <20ms p95 on chunk miss
    pub test_points_per_profile: usize, // Number of test points per profile
    pub test_routes_per_profile: usize, // Number of test routes per profile
}

impl Default for PRSv2Config {
    fn default() -> Self {
        Self {
            snap_recall_target: 0.98,
            snap_accuracy_target: 2.0,
            hausdorff_median_target: 2.0,
            hausdorff_p95_target: 5.0,
            routing_time_budget_ms: 100,
            cold_io_p95_target_ms: 20,
            test_points_per_profile: 1000,
            test_routes_per_profile: 50,
        }
    }
}

/// Enhanced Profile Regression Suite v2
pub struct ProfileRegressionSuiteV2 {
    config: PRSv2Config,
    dual_core: DualCoreGraph,
    snap_engine: Option<SnapEngine>,
    geometry_pipeline: GeometryPipeline,
    distance_router: DistanceRouter,
}

impl ProfileRegressionSuiteV2 {
    pub fn new(
        config: PRSv2Config,
        dual_core: DualCoreGraph,
        snap_engine: Option<SnapEngine>,
    ) -> Result<Self, String> {
        let geometry_pipeline = GeometryPipeline::default();
        let distance_router = DistanceRouter::new(dual_core.clone())?;

        Ok(Self {
            config,
            dual_core,
            snap_engine,
            geometry_pipeline,
            distance_router,
        })
    }

    /// Run complete PRS v2 test suite
    pub fn run_complete_suite(&self, profiles: &[TransportProfile]) -> PRSv2Report {
        let start_time = Instant::now();
        let mut results = Vec::new();

        for profile in profiles {
            // Run all test types for this profile
            results.extend(self.run_profile_tests(profile));
        }

        let total_time = start_time.elapsed().as_millis() as u64;
        let passed = results
            .iter()
            .filter(|r| r.status == TestStatus::Pass)
            .count();
        let failed = results
            .iter()
            .filter(|r| r.status == TestStatus::Fail)
            .count();

        PRSv2Report {
            version: "2.0".to_string(),
            profiles: profiles.to_vec(),
            results,
            summary: PRSv2Summary {
                total_tests: passed + failed,
                passed,
                failed,
                execution_time_ms: total_time,
                overall_status: if failed == 0 {
                    TestStatus::Pass
                } else {
                    TestStatus::Fail
                },
            },
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }

    /// Run all tests for a specific profile
    fn run_profile_tests(&self, profile: &TransportProfile) -> Vec<PRSv2TestResult> {
        let mut results = Vec::new();

        // Test 1: Snap recall (≥98% within 5m)
        results.push(self.test_snap_recall(profile));

        // Test 2: Snap accuracy (median ≤2m)
        results.push(self.test_snap_accuracy(profile));

        // Test 3: Geometry quality (Hausdorff distances)
        results.push(self.test_geometry_quality(profile));

        // Test 4: Routing consistency (Time vs Nav graph)
        results.push(self.test_routing_consistency(profile));

        // Test 5: Performance regression
        results.push(self.test_performance_regression(profile));

        // Test 6: Cold I/O performance (0.1% requests p95 <20ms)
        results.push(self.test_cold_io_performance(profile));

        // Test 7: Dual core consistency
        results.push(self.test_dual_core_consistency(profile));

        results
    }

    /// Test snap recall rate (≥98% within 5m on random points)
    fn test_snap_recall(&self, profile: &TransportProfile) -> PRSv2TestResult {
        let start_time = Instant::now();
        let mut metrics = PRSv2Metrics::new();

        if let Some(snap_engine) = &self.snap_engine {
            let test_points = self.generate_test_points(self.config.test_points_per_profile);
            let mut successful_snaps = 0;

            for point in &test_points {
                let spatial_point = spatial::Point2D::new(point.x, point.y);
                if let Some(snap_result) = snap_engine.snap_point(&spatial_point, None) {
                    if snap_result.distance <= 5.0 {
                        successful_snaps += 1;
                    }
                }
            }

            let recall_rate = successful_snaps as f64 / test_points.len() as f64;
            metrics.snap_recall_rate = Some(recall_rate);
            metrics.execution_time_ms = start_time.elapsed().as_millis() as u64;

            let status = if recall_rate >= self.config.snap_recall_target {
                TestStatus::Pass
            } else {
                TestStatus::Fail
            };

            PRSv2TestResult {
                test_type: PRSv2TestType::SnapRecall,
                profile: *profile,
                status,
                message: format!(
                    "Snap recall: {:.2}% (target: {:.2}%)",
                    recall_rate * 100.0,
                    self.config.snap_recall_target * 100.0
                ),
                metrics,
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            }
        } else {
            PRSv2TestResult {
                test_type: PRSv2TestType::SnapRecall,
                profile: *profile,
                status: TestStatus::Skip,
                message: "No snap engine available".to_string(),
                metrics,
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            }
        }
    }

    /// Test snap accuracy (median distance ≤2m)
    fn test_snap_accuracy(&self, profile: &TransportProfile) -> PRSv2TestResult {
        let start_time = Instant::now();
        let mut metrics = PRSv2Metrics::new();

        if let Some(snap_engine) = &self.snap_engine {
            let test_points = self.generate_test_points(self.config.test_points_per_profile);
            let mut distances = Vec::new();

            for point in &test_points {
                let spatial_point = spatial::Point2D::new(point.x, point.y);
                if let Some(snap_result) = snap_engine.snap_point(&spatial_point, None) {
                    distances.push(snap_result.distance);
                }
            }

            if !distances.is_empty() {
                distances.sort_by(|a, b| a.partial_cmp(b).unwrap());
                let median = distances[distances.len() / 2];

                metrics.snap_accuracy_meters = Some(median);
                metrics.execution_time_ms = start_time.elapsed().as_millis() as u64;

                let status = if median <= self.config.snap_accuracy_target {
                    TestStatus::Pass
                } else {
                    TestStatus::Fail
                };

                PRSv2TestResult {
                    test_type: PRSv2TestType::SnapAccuracy,
                    profile: *profile,
                    status,
                    message: format!(
                        "Snap accuracy median: {:.2}m (target: ≤{:.2}m)",
                        median, self.config.snap_accuracy_target
                    ),
                    metrics,
                    timestamp: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                }
            } else {
                PRSv2TestResult {
                    test_type: PRSv2TestType::SnapAccuracy,
                    profile: *profile,
                    status: TestStatus::Fail,
                    message: "No successful snaps for accuracy measurement".to_string(),
                    metrics,
                    timestamp: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                }
            }
        } else {
            PRSv2TestResult {
                test_type: PRSv2TestType::SnapAccuracy,
                profile: *profile,
                status: TestStatus::Skip,
                message: "No snap engine available".to_string(),
                metrics,
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            }
        }
    }

    /// Test geometry quality (Hausdorff distance targets)
    fn test_geometry_quality(&self, profile: &TransportProfile) -> PRSv2TestResult {
        let start_time = Instant::now();
        let mut metrics = PRSv2Metrics::new();

        // Generate test geometry
        let test_geometry = self.generate_test_geometry();

        // Process through geometry pipeline
        match self.geometry_pipeline.process_geometry(&test_geometry) {
            Ok(result) => {
                let hausdorff_median = result.pass_b_result.hausdorff_median;
                let hausdorff_p95 = result.pass_b_result.hausdorff_p95;

                metrics.hausdorff_median = Some(hausdorff_median);
                metrics.hausdorff_p95 = Some(hausdorff_p95);
                metrics.execution_time_ms = start_time.elapsed().as_millis() as u64;

                let status = if hausdorff_median <= self.config.hausdorff_median_target
                    && hausdorff_p95 <= self.config.hausdorff_p95_target
                {
                    TestStatus::Pass
                } else {
                    TestStatus::Fail
                };

                PRSv2TestResult {
                    test_type: PRSv2TestType::GeometryQuality,
                    profile: *profile,
                    status,
                    message: format!(
                        "Hausdorff median: {:.2}m, p95: {:.2}m (targets: ≤{:.2}m, ≤{:.2}m)",
                        hausdorff_median,
                        hausdorff_p95,
                        self.config.hausdorff_median_target,
                        self.config.hausdorff_p95_target
                    ),
                    metrics,
                    timestamp: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                }
            }
            Err(e) => PRSv2TestResult {
                test_type: PRSv2TestType::GeometryQuality,
                profile: *profile,
                status: TestStatus::Fail,
                message: format!("Geometry processing failed: {}", e),
                metrics,
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            },
        }
    }

    /// Test routing consistency between time and nav graphs
    fn test_routing_consistency(&self, profile: &TransportProfile) -> PRSv2TestResult {
        let start_time = Instant::now();
        let mut metrics = PRSv2Metrics::new();

        // Generate test routes
        let test_routes = self.generate_test_routes(self.config.test_routes_per_profile);
        let mut consistent_routes = 0;

        for (start_node, end_node) in test_routes {
            // Route in time graph
            let time_result = self
                .distance_router
                .route_time_graph(start_node, end_node, *profile);

            // Route in nav graph
            let nav_result = self.distance_router.route_nav_graph(
                start_node,
                end_node,
                *profile,
                GeometryPass::Navigation,
            );

            match (time_result, nav_result) {
                (Ok(time_route), Ok(nav_route)) => {
                    // Check that time and distances are consistent (within 0.5s tolerance)
                    let time_diff = (time_route.total_time - nav_route.total_time).abs();
                    let distance_diff =
                        (time_route.total_distance - nav_route.total_distance).abs();

                    if time_diff <= 0.5 && distance_diff <= 1.0 {
                        consistent_routes += 1;
                    }
                }
                _ => {
                    // Routes should succeed or fail consistently
                }
            }
        }

        let consistency_rate =
            consistent_routes as f64 / self.config.test_routes_per_profile as f64;
        metrics.execution_time_ms = start_time.elapsed().as_millis() as u64;

        let status = if consistency_rate >= 0.95 {
            // 95% consistency target
            TestStatus::Pass
        } else {
            TestStatus::Fail
        };

        PRSv2TestResult {
            test_type: PRSv2TestType::RoutingConsistency,
            profile: *profile,
            status,
            message: format!(
                "Routing consistency: {:.1}% (target: ≥95%)",
                consistency_rate * 100.0
            ),
            metrics,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }

    /// Test performance regression (routing time budget)
    fn test_performance_regression(&self, profile: &TransportProfile) -> PRSv2TestResult {
        let start_time = Instant::now();
        let mut metrics = PRSv2Metrics::new();

        // Test single route performance
        let test_routes = self.generate_test_routes(10); // Small sample for performance
        let mut route_times = Vec::new();

        for (start_node, end_node) in test_routes {
            let route_start = Instant::now();
            let _result = self
                .distance_router
                .route_time_graph(start_node, end_node, *profile);
            let route_time = route_start.elapsed().as_millis() as u64;
            route_times.push(route_time);
        }

        if !route_times.is_empty() {
            route_times.sort();
            let median_time = route_times[route_times.len() / 2];
            let p95_time = route_times[(route_times.len() as f64 * 0.95) as usize];

            metrics.routing_time_ms = Some(median_time);
            metrics.execution_time_ms = start_time.elapsed().as_millis() as u64;

            let status = if p95_time <= self.config.routing_time_budget_ms {
                TestStatus::Pass
            } else {
                TestStatus::Fail
            };

            PRSv2TestResult {
                test_type: PRSv2TestType::PerformanceRegression,
                profile: *profile,
                status,
                message: format!(
                    "Routing p95: {}ms (budget: ≤{}ms)",
                    p95_time, self.config.routing_time_budget_ms
                ),
                metrics,
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            }
        } else {
            PRSv2TestResult {
                test_type: PRSv2TestType::PerformanceRegression,
                profile: *profile,
                status: TestStatus::Fail,
                message: "No route timing data available".to_string(),
                metrics,
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            }
        }
    }

    /// Test cold I/O performance (simulated chunk miss scenario)
    fn test_cold_io_performance(&self, profile: &TransportProfile) -> PRSv2TestResult {
        let start_time = Instant::now();
        let mut metrics = PRSv2Metrics::new();

        // Simulate cold I/O by timing geometry extraction (represents chunk loading)
        let test_routes = self.generate_test_routes(10);
        let mut io_times = Vec::new();

        for (start_node, end_node) in test_routes {
            let io_start = Instant::now();

            // Simulate cold I/O by routing with geometry (represents loading nav chunks)
            let _result = self.distance_router.route_nav_graph(
                start_node,
                end_node,
                *profile,
                GeometryPass::Navigation,
            );

            let io_time = io_start.elapsed().as_millis() as u64;
            io_times.push(io_time);
        }

        if !io_times.is_empty() {
            io_times.sort();
            let p95_io_time = io_times[(io_times.len() as f64 * 0.95) as usize];

            metrics.io_latency_p95_ms = Some(p95_io_time);
            metrics.execution_time_ms = start_time.elapsed().as_millis() as u64;

            let status = if p95_io_time <= self.config.cold_io_p95_target_ms {
                TestStatus::Pass
            } else {
                TestStatus::Fail
            };

            PRSv2TestResult {
                test_type: PRSv2TestType::ColdIOPerformance,
                profile: *profile,
                status,
                message: format!(
                    "Cold I/O p95: {}ms (target: ≤{}ms)",
                    p95_io_time, self.config.cold_io_p95_target_ms
                ),
                metrics,
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            }
        } else {
            PRSv2TestResult {
                test_type: PRSv2TestType::ColdIOPerformance,
                profile: *profile,
                status: TestStatus::Fail,
                message: "No I/O timing data available".to_string(),
                metrics,
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            }
        }
    }

    /// Test dual core consistency (XXH3 hash verification)
    fn test_dual_core_consistency(&self, profile: &TransportProfile) -> PRSv2TestResult {
        let start_time = Instant::now();
        let mut metrics = PRSv2Metrics::new();

        // Check if dual core consistency is verified
        let consistent = self.dual_core.consistency_verified;
        let (time_hash, nav_hash) = {
            let mut dual_core = self.dual_core.clone();
            dual_core.get_consistency_digests()
        };

        metrics.consistency_hash_matches = Some(time_hash == nav_hash);
        metrics.execution_time_ms = start_time.elapsed().as_millis() as u64;

        let status = if consistent && time_hash == nav_hash {
            TestStatus::Pass
        } else {
            TestStatus::Fail
        };

        PRSv2TestResult {
            test_type: PRSv2TestType::DualCoreConsistency,
            profile: *profile,
            status,
            message: format!(
                "Dual core consistency: {} (hashes: time={}, nav={})",
                if consistent { "verified" } else { "failed" },
                time_hash,
                nav_hash
            ),
            metrics,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }

    /// Generate realistic test points for snapping tests
    fn generate_test_points(&self, count: usize) -> Vec<Point2D> {
        // Realistic test data corpus with urban, suburban, and rural patterns
        let mut points = Vec::new();
        let patterns = [
            // Urban grid pattern (Berlin-like)
            (52.520008, 13.404954, 0.0005), // High density
            // Suburban pattern (outskirts)
            (52.4755, 13.3679, 0.002), // Medium density
            // Rural pattern (countryside)
            (52.3906, 13.0645, 0.01), // Low density
        ];

        for i in 0..count {
            let pattern_idx = i % patterns.len();
            let (base_lat, base_lon, spread) = patterns[pattern_idx];

            // Add realistic variation based on road network patterns
            let offset_lat = ((i as f64 * 0.123456) % 1.0 - 0.5) * spread;
            let offset_lon = ((i as f64 * 0.789012) % 1.0 - 0.5) * spread;

            points.push(Point2D::new(base_lat + offset_lat, base_lon + offset_lon));
        }

        points
    }

    /// Generate realistic test geometry for geometry quality tests
    fn generate_test_geometry(&self) -> Vec<Point2D> {
        // Realistic geometry patterns based on real OSM data characteristics
        vec![
            // Highway on-ramp curve (realistic curvature)
            Point2D::new(52.520008, 13.404954), // Start
            Point2D::new(52.520158, 13.405104), // Slight curve
            Point2D::new(52.520308, 13.405354), // More curve
            Point2D::new(52.520408, 13.405654), // Peak curve
            Point2D::new(52.520458, 13.406004), // Straightening
            Point2D::new(52.520508, 13.406404), // Straight section
            // Urban street with slight variations
            Point2D::new(52.520558, 13.406754), // Building deflection
            Point2D::new(52.520608, 13.407104), // Slight adjustment
            Point2D::new(52.520658, 13.407454), // End point
        ]
    }

    /// Generate realistic test route pairs for routing tests
    fn generate_test_routes(&self, count: usize) -> Vec<(NodeId, NodeId)> {
        // Realistic route patterns based on typical routing scenarios
        let mut routes = Vec::new();

        // Common routing patterns
        let route_patterns = [
            // Short urban routes (1-5km)
            (1, 2),
            (2, 3),
            (3, 4),
            (4, 5),
            // Medium suburban routes (5-15km)
            (1, 5),
            (2, 6),
            (3, 7),
            (4, 8),
            // Longer inter-city routes (15km+)
            (1, 8),
            (2, 9),
            (3, 10),
            (4, 11),
            // Return trips
            (5, 1),
            (6, 2),
            (7, 3),
            (8, 4),
        ];

        for i in 0..count {
            let pattern_idx = i % route_patterns.len();
            let (start_base, end_base) = route_patterns[pattern_idx];

            // Add variation to node IDs to create realistic distribution
            let start_offset = (i / route_patterns.len()) as u64;
            let end_offset = ((i + 7) / route_patterns.len()) as u64; // Different offset for variety

            routes.push((
                NodeId::new(start_base + start_offset),
                NodeId::new(end_base + end_offset),
            ));
        }

        routes
    }

    /// Get PRS v2 configuration
    pub fn config(&self) -> &PRSv2Config {
        &self.config
    }
}

/// PRS v2 test report
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PRSv2Report {
    pub version: String,
    pub profiles: Vec<TransportProfile>,
    pub results: Vec<PRSv2TestResult>,
    pub summary: PRSv2Summary,
    pub timestamp: u64,
}

/// PRS v2 summary statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PRSv2Summary {
    pub total_tests: usize,
    pub passed: usize,
    pub failed: usize,
    pub execution_time_ms: u64,
    pub overall_status: TestStatus,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dual_core::{GraphNode, NavEdge, TimeEdge, TimeWeight};
    use butterfly_geometry::{NavigationGeometry, SnapSkeleton};

    fn create_test_setup() -> (DualCoreGraph, PRSv2Config) {
        let profiles = vec![TransportProfile::Car];
        let mut dual_core = DualCoreGraph::new(profiles);

        // Add test nodes and edges
        let node1 = GraphNode::new(NodeId::new(1), Point2D::new(0.0, 0.0));
        let node2 = GraphNode::new(NodeId::new(2), Point2D::new(1.0, 0.0));
        let node3 = GraphNode::new(NodeId::new(3), Point2D::new(2.0, 0.0));

        dual_core.time_graph.add_node(node1.clone());
        dual_core.time_graph.add_node(node2.clone());
        dual_core.time_graph.add_node(node3.clone());
        dual_core.nav_graph.add_node(node1);
        dual_core.nav_graph.add_node(node2);
        dual_core.nav_graph.add_node(node3);

        // Add matching edges
        let mut time_edge = TimeEdge::new(EdgeId(1), NodeId::new(1), NodeId::new(2));
        time_edge.add_weight(TransportProfile::Car, TimeWeight::new(60.0, 1000.0));
        dual_core.time_graph.add_edge(time_edge);

        let snap_skeleton = SnapSkeleton::new(
            vec![Point2D::new(0.0, 0.0), Point2D::new(1.0, 0.0)],
            vec![],
            1000.0,
            5.0,
        );
        let nav_geometry = NavigationGeometry::new(
            vec![Point2D::new(0.0, 0.0), Point2D::new(1.0, 0.0)],
            vec![],
            500.0,
            0.5,
            1.0,
            0.8,
        );
        let mut nav_edge = NavEdge::new(
            EdgeId(1),
            NodeId::new(1),
            NodeId::new(2),
            snap_skeleton,
            nav_geometry,
            None,
        );
        nav_edge.add_weight(TransportProfile::Car, TimeWeight::new(60.0, 1000.0));
        dual_core.nav_graph.add_edge(nav_edge);

        let config = PRSv2Config::default();
        (dual_core, config)
    }

    #[test]
    fn test_prs_v2_creation() {
        let (dual_core, config) = create_test_setup();
        let prs = ProfileRegressionSuiteV2::new(config, dual_core, None);
        assert!(prs.is_ok());
    }

    #[test]
    fn test_geometry_quality_test() {
        let (dual_core, config) = create_test_setup();
        let prs = ProfileRegressionSuiteV2::new(config, dual_core, None).unwrap();

        let result = prs.test_geometry_quality(&TransportProfile::Car);

        assert_eq!(result.test_type, PRSv2TestType::GeometryQuality);
        assert_eq!(result.profile, TransportProfile::Car);
        assert!(result.metrics.hausdorff_median.is_some());
        assert!(result.metrics.hausdorff_p95.is_some());
    }

    #[test]
    fn test_dual_core_consistency_test() {
        let (dual_core, config) = create_test_setup();
        let prs = ProfileRegressionSuiteV2::new(config, dual_core, None).unwrap();

        let result = prs.test_dual_core_consistency(&TransportProfile::Car);

        assert_eq!(result.test_type, PRSv2TestType::DualCoreConsistency);
        assert_eq!(result.profile, TransportProfile::Car);
        assert!(result.metrics.consistency_hash_matches.is_some());
    }

    #[test]
    fn test_performance_regression_test() {
        let (dual_core, config) = create_test_setup();
        let prs = ProfileRegressionSuiteV2::new(config, dual_core, None).unwrap();

        let result = prs.test_performance_regression(&TransportProfile::Car);

        assert_eq!(result.test_type, PRSv2TestType::PerformanceRegression);
        assert_eq!(result.profile, TransportProfile::Car);
        // Note: May fail due to missing routes in minimal test setup
    }

    #[test]
    fn test_complete_suite_execution() {
        let (dual_core, config) = create_test_setup();
        let prs = ProfileRegressionSuiteV2::new(config, dual_core, None).unwrap();

        let profiles = vec![TransportProfile::Car];
        let report = prs.run_complete_suite(&profiles);

        assert_eq!(report.version, "2.0");
        assert_eq!(report.profiles, profiles);
        assert!(report.results.len() > 0);
        assert!(report.summary.total_tests > 0);
    }

    #[test]
    fn test_test_point_generation() {
        let (dual_core, config) = create_test_setup();
        let prs = ProfileRegressionSuiteV2::new(config, dual_core, None).unwrap();

        let points = prs.generate_test_points(100);
        assert_eq!(points.len(), 100);

        // Points should be deterministic
        let points2 = prs.generate_test_points(100);
        assert_eq!(points, points2);
    }

    #[test]
    fn test_test_route_generation() {
        let (dual_core, config) = create_test_setup();
        let prs = ProfileRegressionSuiteV2::new(config, dual_core, None).unwrap();

        let routes = prs.generate_test_routes(10);
        assert_eq!(routes.len(), 10);

        // Routes should be valid node pairs
        for (start, end) in routes {
            assert_ne!(start, end);
        }
    }

    #[test]
    fn test_realistic_test_data_corpus() {
        let (dual_core, config) = create_test_setup();
        let prs = ProfileRegressionSuiteV2::new(config, dual_core, None).unwrap();

        // Test realistic point generation
        let points = prs.generate_test_points(100);
        assert_eq!(points.len(), 100);

        // Points should be deterministic
        let points2 = prs.generate_test_points(100);
        assert_eq!(points, points2);

        // Should have urban, suburban, and rural patterns
        // Check that we have different coordinate ranges (urban should be tighter)
        let urban_points: Vec<_> = points.iter().step_by(3).collect(); // Every 3rd point
        let suburban_points: Vec<_> = points.iter().skip(1).step_by(3).collect();
        let rural_points: Vec<_> = points.iter().skip(2).step_by(3).collect();

        assert!(!urban_points.is_empty());
        assert!(!suburban_points.is_empty());
        assert!(!rural_points.is_empty());

        // Test realistic geometry
        let geometry = prs.generate_test_geometry();
        assert_eq!(geometry.len(), 9); // Highway on-ramp + urban street

        // Verify realistic coordinate progression (should be Berlin area)
        assert!(geometry[0].x > 52.0 && geometry[0].x < 53.0); // Latitude
        assert!(geometry[0].y > 13.0 && geometry[0].y < 14.0); // Longitude

        // Test realistic routes
        let routes = prs.generate_test_routes(50);
        assert_eq!(routes.len(), 50);

        // Should have variety in route patterns
        let unique_starts: std::collections::HashSet<_> = routes.iter().map(|(s, _)| s).collect();
        let unique_ends: std::collections::HashSet<_> = routes.iter().map(|(_, e)| e).collect();

        assert!(unique_starts.len() > 5); // Should have variety
        assert!(unique_ends.len() > 5); // Should have variety
    }

    #[test]
    fn test_prs_v2_with_realistic_corpus() {
        let (dual_core, mut config) = create_test_setup();

        // Use larger corpus for realistic testing
        config.test_points_per_profile = 50; // Smaller for test performance
        config.test_routes_per_profile = 10;

        let prs = ProfileRegressionSuiteV2::new(config, dual_core, None).unwrap();

        let profiles = vec![TransportProfile::Car];
        let report = prs.run_complete_suite(&profiles);

        assert_eq!(report.version, "2.0");
        assert_eq!(report.profiles, profiles);
        assert!(report.results.len() > 0);

        // With realistic corpus, we should get meaningful test coverage
        assert!(report.summary.total_tests > 0); // Should have some tests

        // Check that realistic data produced valid results
        for result in &report.results {
            assert!(result.timestamp > 0);
            // Basic validation that the test ran
            // execution_time_ms is u64, so it's always >= 0
        }
    }
}
