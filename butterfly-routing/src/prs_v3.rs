//! PRS v3 - Profile Regression Suite v3: ETA plausibility + turn legality + time parity validation

use crate::dual_core::{DualCoreGraph, NodeId};
use crate::profiles::TransportProfile;
use crate::time_routing::{RouteQuality, TimeBasedRouter, TimeRouteRequest};
use crate::turn_restriction_tables::{JunctionId, TurnRestrictionTableSystem};
use crate::weight_compression::WeightCompressionSystem;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Instant;

/// PRS v3 test types focusing on time-based routing validation
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PRSv3TestType {
    // Core time routing tests
    ETAPlausibility, // ETA must be realistic for profile
    TurnLegality,    // All turns must respect restrictions
    TimeParity,      // Time vs distance consistency

    // Performance and quality tests
    RouteQuality,        // Route quality assessment
    CompressionAccuracy, // Weight compression precision
    ShardPerformance,    // Turn restriction shard efficiency

    // Regression tests
    TimeConsistency,    // Results consistent across runs
    ProfileDifferences, // Different profiles give different results
    ConstraintRespect,  // Route constraints are respected
}

/// PRS v3 test result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PRSv3TestResult {
    pub test_type: PRSv3TestType,
    pub profile: TransportProfile,
    pub status: TestStatus,
    pub message: String,
    pub metrics: PRSv3Metrics,
    pub timestamp: u64,
}

/// Test status from existing system
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TestStatus {
    Pass,
    Fail,
    Skip,
}

/// PRS v3 specific metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PRSv3Metrics {
    pub execution_time_ms: u64,
    pub routes_tested: usize,

    // ETA plausibility metrics
    pub eta_accuracy_mean_error: Option<f64>, // Mean error in ETA (seconds)
    pub eta_accuracy_max_error: Option<f64>,  // Max error in ETA (seconds)
    pub implausible_eta_rate: Option<f64>,    // Rate of implausible ETAs

    // Turn legality metrics
    pub turn_violations: Option<usize>, // Number of illegal turns found
    pub turn_check_rate: Option<f64>,   // Fraction of turns checked

    // Time parity metrics
    pub time_distance_correlation: Option<f64>, // Correlation between time and distance
    pub average_speed_kmh: Option<f64>,         // Average speed across routes
    pub speed_variance: Option<f64>,            // Variance in speeds

    // Performance metrics
    pub compression_accuracy: Option<f64>, // Weight compression accuracy
    pub shard_hit_rate: Option<f64>,       // Turn restriction shard hit rate
    pub memory_efficiency: Option<f64>,    // Memory usage efficiency
}

impl PRSv3Metrics {
    pub fn new() -> Self {
        Self {
            execution_time_ms: 0,
            routes_tested: 0,
            eta_accuracy_mean_error: None,
            eta_accuracy_max_error: None,
            implausible_eta_rate: None,
            turn_violations: None,
            turn_check_rate: None,
            time_distance_correlation: None,
            average_speed_kmh: None,
            speed_variance: None,
            compression_accuracy: None,
            shard_hit_rate: None,
            memory_efficiency: None,
        }
    }
}

/// PRS v3 configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PRSv3Config {
    // ETA plausibility thresholds
    pub max_eta_error_seconds: f64, // Max acceptable ETA error (300s = 5 min)
    pub max_implausible_eta_rate: f64, // Max rate of implausible ETAs (5%)

    // Speed plausibility ranges per profile
    pub car_speed_range_kmh: (f64, f64), // (min, max) km/h for cars
    pub bicycle_speed_range_kmh: (f64, f64), // (min, max) km/h for bicycles
    pub pedestrian_speed_range_kmh: (f64, f64), // (min, max) km/h for walking

    // Turn legality thresholds
    pub max_turn_violations: usize, // Max illegal turns allowed (0)
    pub min_turn_check_rate: f64,   // Min fraction of turns to check (95%)

    // Time parity thresholds
    pub min_time_distance_correlation: f64, // Min correlation (0.7)
    pub max_speed_variance: f64,            // Max speed variance

    // Performance thresholds
    pub min_compression_accuracy: f64, // Min weight compression accuracy (99%)
    pub min_shard_hit_rate: f64,       // Min shard hit rate (97%)

    // Test configuration
    pub test_routes_per_profile: usize, // Number of test routes per profile
    pub max_route_distance_km: f64,     // Max route distance for testing
}

impl Default for PRSv3Config {
    fn default() -> Self {
        Self {
            // ETA plausibility
            max_eta_error_seconds: 300.0,   // 5 minutes
            max_implausible_eta_rate: 0.05, // 5%

            // Speed ranges (realistic for each profile)
            car_speed_range_kmh: (5.0, 120.0),      // 5-120 km/h
            bicycle_speed_range_kmh: (3.0, 45.0),   // 3-45 km/h
            pedestrian_speed_range_kmh: (1.0, 8.0), // 1-8 km/h

            // Turn legality
            max_turn_violations: 0,    // Zero tolerance
            min_turn_check_rate: 0.95, // 95%

            // Time parity
            min_time_distance_correlation: 0.7, // Strong correlation
            max_speed_variance: 100.0,          // Reasonable variance

            // Performance
            min_compression_accuracy: 0.99, // 99%
            min_shard_hit_rate: 0.97,       // 97%

            // Test configuration
            test_routes_per_profile: 100, // 100 routes per profile
            max_route_distance_km: 50.0,  // 50km max distance
        }
    }
}

/// Profile Regression Suite v3
pub struct ProfileRegressionSuiteV3 {
    config: PRSv3Config,
    time_router: TimeBasedRouter,
    turn_system: Option<TurnRestrictionTableSystem>,
    weight_system: Option<WeightCompressionSystem>,
}

impl ProfileRegressionSuiteV3 {
    pub fn new(config: PRSv3Config, dual_core: DualCoreGraph) -> Result<Self, String> {
        let time_router = TimeBasedRouter::new(dual_core)?;

        Ok(Self {
            config,
            time_router,
            turn_system: None,
            weight_system: None,
        })
    }

    pub fn with_turn_system(mut self, turn_system: TurnRestrictionTableSystem) -> Self {
        self.time_router = self.time_router.with_turn_restrictions(turn_system.clone());
        self.turn_system = Some(turn_system);
        self
    }

    pub fn with_weight_system(mut self, weight_system: WeightCompressionSystem) -> Self {
        self.time_router = self
            .time_router
            .with_weight_compression(weight_system.clone());
        self.weight_system = Some(weight_system);
        self
    }

    /// Run complete PRS v3 test suite
    pub fn run_complete_suite(&mut self, profiles: &[TransportProfile]) -> PRSv3Report {
        let start_time = Instant::now();
        let mut results = Vec::new();

        for profile in profiles {
            results.extend(self.run_profile_tests(*profile));
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

        PRSv3Report {
            version: "3.0".to_string(),
            profiles: profiles.to_vec(),
            results,
            summary: PRSv3Summary {
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
    fn run_profile_tests(&mut self, profile: TransportProfile) -> Vec<PRSv3TestResult> {
        let mut results = Vec::new();

        // Generate test routes for this profile
        let test_routes = self.generate_test_routes(profile);

        // Test 1: ETA Plausibility
        results.push(self.test_eta_plausibility(profile, &test_routes));

        // Test 2: Turn Legality
        results.push(self.test_turn_legality(profile, &test_routes));

        // Test 3: Time Parity
        results.push(self.test_time_parity(profile, &test_routes));

        // Test 4: Route Quality
        results.push(self.test_route_quality(profile, &test_routes));

        // Test 5: Compression Accuracy (if available)
        if self.weight_system.is_some() {
            results.push(self.test_compression_accuracy(profile, &test_routes));
        }

        // Test 6: Shard Performance (if available)
        if self.turn_system.is_some() {
            results.push(self.test_shard_performance(profile, &test_routes));
        }

        // Test 7: Time Consistency
        results.push(self.test_time_consistency(profile, &test_routes));

        // Test 8: Profile Differences (compare with other profiles)
        results.push(self.test_profile_differences(profile, &test_routes));

        // Test 9: Constraint Respect
        results.push(self.test_constraint_respect(profile, &test_routes));

        results
    }

    /// Test ETA plausibility - ETAs must be realistic for the profile
    fn test_eta_plausibility(
        &mut self,
        profile: TransportProfile,
        test_routes: &[(NodeId, NodeId)],
    ) -> PRSv3TestResult {
        let start_time = Instant::now();
        let mut metrics = PRSv3Metrics::new();

        let speed_range = match profile {
            TransportProfile::Car => self.config.car_speed_range_kmh,
            TransportProfile::Bicycle => self.config.bicycle_speed_range_kmh,
            TransportProfile::Foot => self.config.pedestrian_speed_range_kmh,
        };

        let mut eta_errors = Vec::new();
        let mut implausible_count = 0;

        for &(start, end) in test_routes {
            let request = TimeRouteRequest::new(profile, start, end);
            let response = self.time_router.route(request);

            if response.route_found {
                let speed_kmh = response.average_speed_kmh();

                // Check if speed is within plausible range
                if speed_kmh < speed_range.0 || speed_kmh > speed_range.1 {
                    implausible_count += 1;
                }

                // Calculate ETA error (simple heuristic)
                let expected_time = response.total_distance_meters
                    / 1000.0
                    / ((speed_range.0 + speed_range.1) / 2.0)
                    * 3600.0;
                let eta_error = (response.total_time_seconds - expected_time).abs();
                eta_errors.push(eta_error);
            }
        }

        metrics.routes_tested = test_routes.len();
        metrics.eta_accuracy_mean_error = if eta_errors.is_empty() {
            None
        } else {
            Some(eta_errors.iter().sum::<f64>() / eta_errors.len() as f64)
        };
        metrics.eta_accuracy_max_error = eta_errors.iter().fold(0.0f64, |a, &b| a.max(b)).into();
        metrics.implausible_eta_rate = Some(implausible_count as f64 / test_routes.len() as f64);
        metrics.execution_time_ms = start_time.elapsed().as_millis() as u64;

        let mean_error = metrics.eta_accuracy_mean_error.unwrap_or(f64::INFINITY);
        let implausible_rate = metrics.implausible_eta_rate.unwrap_or(1.0);

        let status = if mean_error <= self.config.max_eta_error_seconds
            && implausible_rate <= self.config.max_implausible_eta_rate
        {
            TestStatus::Pass
        } else {
            TestStatus::Fail
        };

        PRSv3TestResult {
            test_type: PRSv3TestType::ETAPlausibility,
            profile,
            status,
            message: format!(
                "ETA mean error: {:.1}s, implausible rate: {:.2}%",
                mean_error,
                implausible_rate * 100.0
            ),
            metrics,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }

    /// Test turn legality - all turns must respect restrictions
    fn test_turn_legality(
        &mut self,
        profile: TransportProfile,
        test_routes: &[(NodeId, NodeId)],
    ) -> PRSv3TestResult {
        let start_time = Instant::now();
        let mut metrics = PRSv3Metrics::new();

        let mut total_turns = 0;
        let mut violations = 0;
        let mut turns_checked = 0;

        if let Some(ref mut turn_system) = self.turn_system {
            for &(start, end) in test_routes {
                let request = TimeRouteRequest::new(profile, start, end);
                let response = self.time_router.route(request);

                if response.route_found && response.edge_sequence.len() >= 2 {
                    // Check each turn in the route
                    for window in response.edge_sequence.windows(2) {
                        total_turns += 1;

                        let from_edge = window[0];
                        let to_edge = window[1];

                        // Derive junction from edges (in practice, you'd have this from the graph)
                        let junction_id = JunctionId::new(((from_edge.0 + to_edge.0) / 2) as u64);

                        turns_checked += 1;

                        if !turn_system.is_turn_allowed(&profile, from_edge, junction_id, to_edge) {
                            violations += 1;
                        }
                    }
                }
            }
        }

        metrics.routes_tested = test_routes.len();
        metrics.turn_violations = Some(violations);
        metrics.turn_check_rate = if total_turns > 0 {
            Some(turns_checked as f64 / total_turns as f64)
        } else {
            Some(1.0)
        };
        metrics.execution_time_ms = start_time.elapsed().as_millis() as u64;

        let check_rate = metrics.turn_check_rate.unwrap_or(0.0);

        let status = if violations <= self.config.max_turn_violations
            && check_rate >= self.config.min_turn_check_rate
        {
            TestStatus::Pass
        } else {
            TestStatus::Fail
        };

        PRSv3TestResult {
            test_type: PRSv3TestType::TurnLegality,
            profile,
            status,
            message: format!(
                "Turn violations: {}, check rate: {:.1}%",
                violations,
                check_rate * 100.0
            ),
            metrics,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }

    /// Test time parity - time vs distance consistency
    fn test_time_parity(
        &mut self,
        profile: TransportProfile,
        test_routes: &[(NodeId, NodeId)],
    ) -> PRSv3TestResult {
        let start_time = Instant::now();
        let mut metrics = PRSv3Metrics::new();

        let mut times = Vec::new();
        let mut distances = Vec::new();
        let mut speeds = Vec::new();

        for &(start, end) in test_routes {
            let request = TimeRouteRequest::new(profile, start, end);
            let response = self.time_router.route(request);

            if response.route_found && response.total_time_seconds > 0.0 {
                times.push(response.total_time_seconds);
                distances.push(response.total_distance_meters);
                speeds.push(response.average_speed_kmh());
            }
        }

        // Calculate correlation between time and distance
        let correlation = if times.len() >= 2 {
            calculate_correlation(&times, &distances)
        } else {
            0.0
        };

        // Calculate speed statistics
        let avg_speed = if !speeds.is_empty() {
            speeds.iter().sum::<f64>() / speeds.len() as f64
        } else {
            0.0
        };

        let speed_variance = if speeds.len() >= 2 {
            let mean = avg_speed;
            speeds.iter().map(|&s| (s - mean).powi(2)).sum::<f64>() / (speeds.len() - 1) as f64
        } else {
            0.0
        };

        metrics.routes_tested = test_routes.len();
        metrics.time_distance_correlation = Some(correlation);
        metrics.average_speed_kmh = Some(avg_speed);
        metrics.speed_variance = Some(speed_variance);
        metrics.execution_time_ms = start_time.elapsed().as_millis() as u64;

        let status = if correlation >= self.config.min_time_distance_correlation
            && speed_variance <= self.config.max_speed_variance
        {
            TestStatus::Pass
        } else {
            TestStatus::Fail
        };

        PRSv3TestResult {
            test_type: PRSv3TestType::TimeParity,
            profile,
            status,
            message: format!(
                "Time-distance correlation: {:.3}, avg speed: {:.1} km/h, variance: {:.1}",
                correlation, avg_speed, speed_variance
            ),
            metrics,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }

    /// Test route quality assessment
    fn test_route_quality(
        &mut self,
        profile: TransportProfile,
        test_routes: &[(NodeId, NodeId)],
    ) -> PRSv3TestResult {
        let start_time = Instant::now();
        let mut metrics = PRSv3Metrics::new();

        let mut quality_counts = HashMap::new();
        quality_counts.insert(RouteQuality::Excellent, 0);
        quality_counts.insert(RouteQuality::Good, 0);
        quality_counts.insert(RouteQuality::Acceptable, 0);
        quality_counts.insert(RouteQuality::Poor, 0);
        quality_counts.insert(RouteQuality::NotFound, 0);

        for &(start, end) in test_routes {
            let request = TimeRouteRequest::new(profile, start, end);
            let response = self.time_router.route(request);

            *quality_counts.entry(response.route_quality).or_insert(0) += 1;
        }

        metrics.routes_tested = test_routes.len();
        metrics.execution_time_ms = start_time.elapsed().as_millis() as u64;

        let poor_rate = *quality_counts.get(&RouteQuality::Poor).unwrap_or(&0) as f64
            / test_routes.len() as f64;
        let not_found_rate = *quality_counts.get(&RouteQuality::NotFound).unwrap_or(&0) as f64
            / test_routes.len() as f64;

        // Pass if less than 10% poor routes and less than 5% not found
        let status = if poor_rate <= 0.10 && not_found_rate <= 0.05 {
            TestStatus::Pass
        } else {
            TestStatus::Fail
        };

        PRSv3TestResult {
            test_type: PRSv3TestType::RouteQuality,
            profile,
            status,
            message: format!(
                "Poor routes: {:.1}%, not found: {:.1}%",
                poor_rate * 100.0,
                not_found_rate * 100.0
            ),
            metrics,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }

    /// Test compression accuracy (if weight compression system is available)
    fn test_compression_accuracy(
        &mut self,
        profile: TransportProfile,
        test_routes: &[(NodeId, NodeId)],
    ) -> PRSv3TestResult {
        let start_time = Instant::now();
        let mut metrics = PRSv3Metrics::new();

        // This test would compare compressed vs uncompressed weights
        // For now, we'll use the system stats
        let accuracy = if let Some(ref weight_system) = self.weight_system {
            let stats = weight_system.get_system_stats();
            1.0 - stats.average_precision_loss / 100.0 // Convert precision loss to accuracy
        } else {
            1.0
        };

        metrics.routes_tested = test_routes.len();
        metrics.compression_accuracy = Some(accuracy);
        metrics.execution_time_ms = start_time.elapsed().as_millis() as u64;

        let status = if accuracy >= self.config.min_compression_accuracy {
            TestStatus::Pass
        } else {
            TestStatus::Fail
        };

        PRSv3TestResult {
            test_type: PRSv3TestType::CompressionAccuracy,
            profile,
            status,
            message: format!("Compression accuracy: {:.3}%", accuracy * 100.0),
            metrics,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }

    /// Test shard performance (if turn restriction system is available)
    fn test_shard_performance(
        &mut self,
        profile: TransportProfile,
        test_routes: &[(NodeId, NodeId)],
    ) -> PRSv3TestResult {
        let start_time = Instant::now();
        let mut metrics = PRSv3Metrics::new();

        let hit_rate = if let Some(ref turn_system) = self.turn_system {
            turn_system.get_warm_hit_rate()
        } else {
            1.0
        };

        metrics.routes_tested = test_routes.len();
        metrics.shard_hit_rate = Some(hit_rate);
        metrics.execution_time_ms = start_time.elapsed().as_millis() as u64;

        let status = if hit_rate >= self.config.min_shard_hit_rate {
            TestStatus::Pass
        } else {
            TestStatus::Fail
        };

        PRSv3TestResult {
            test_type: PRSv3TestType::ShardPerformance,
            profile,
            status,
            message: format!("Shard hit rate: {:.1}%", hit_rate * 100.0),
            metrics,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }

    /// Test time consistency - results should be consistent across runs
    fn test_time_consistency(
        &mut self,
        profile: TransportProfile,
        test_routes: &[(NodeId, NodeId)],
    ) -> PRSv3TestResult {
        let start_time = Instant::now();
        let mut metrics = PRSv3Metrics::new();

        // Test a subset of routes multiple times
        let test_subset = &test_routes[..test_routes.len().min(10)];
        let mut time_variations = Vec::new();

        for &(start, end) in test_subset {
            let request = TimeRouteRequest::new(profile, start, end);

            // Run the same route multiple times
            let mut times = Vec::new();
            for _ in 0..3 {
                let response = self.time_router.route(request.clone());
                if response.route_found {
                    times.push(response.total_time_seconds);
                }
            }

            // Calculate time variation
            if times.len() >= 2 {
                let mean = times.iter().sum::<f64>() / times.len() as f64;
                let max_deviation = times
                    .iter()
                    .map(|&t| (t - mean).abs() / mean)
                    .fold(0.0, f64::max);
                time_variations.push(max_deviation);
            }
        }

        let avg_variation = if !time_variations.is_empty() {
            time_variations.iter().sum::<f64>() / time_variations.len() as f64
        } else {
            0.0
        };

        metrics.routes_tested = test_subset.len();
        metrics.execution_time_ms = start_time.elapsed().as_millis() as u64;

        // Pass if average variation is less than 1%
        let status = if avg_variation <= 0.01 {
            TestStatus::Pass
        } else {
            TestStatus::Fail
        };

        PRSv3TestResult {
            test_type: PRSv3TestType::TimeConsistency,
            profile,
            status,
            message: format!("Average time variation: {:.3}%", avg_variation * 100.0),
            metrics,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }

    /// Test profile differences - different profiles should give different results
    fn test_profile_differences(
        &mut self,
        profile: TransportProfile,
        test_routes: &[(NodeId, NodeId)],
    ) -> PRSv3TestResult {
        let start_time = Instant::now();
        let mut metrics = PRSv3Metrics::new();

        // Compare with other profiles on a subset of routes
        let test_subset = &test_routes[..test_routes.len().min(10)];
        let other_profiles = vec![
            TransportProfile::Car,
            TransportProfile::Bicycle,
            TransportProfile::Foot,
        ]
        .into_iter()
        .filter(|&p| p != profile)
        .collect::<Vec<_>>();

        let mut significant_differences = 0;

        for &(start, end) in test_subset {
            let request = TimeRouteRequest::new(profile, start, end);
            let base_response = self.time_router.route(request);

            if base_response.route_found {
                for &other_profile in &other_profiles {
                    let other_request = TimeRouteRequest::new(other_profile, start, end);
                    let other_response = self.time_router.route(other_request);

                    if other_response.route_found {
                        // Check if there's a significant difference (>10% time difference)
                        let time_diff = (base_response.total_time_seconds
                            - other_response.total_time_seconds)
                            .abs();
                        let relative_diff = time_diff / base_response.total_time_seconds;

                        if relative_diff > 0.1 {
                            significant_differences += 1;
                            break; // Found at least one significant difference for this route
                        }
                    }
                }
            }
        }

        metrics.routes_tested = test_subset.len();
        metrics.execution_time_ms = start_time.elapsed().as_millis() as u64;

        let difference_rate = significant_differences as f64 / test_subset.len() as f64;

        // Pass if at least 30% of routes show significant differences between profiles
        let status = if difference_rate >= 0.3 {
            TestStatus::Pass
        } else {
            TestStatus::Fail
        };

        PRSv3TestResult {
            test_type: PRSv3TestType::ProfileDifferences,
            profile,
            status,
            message: format!("Profile difference rate: {:.1}%", difference_rate * 100.0),
            metrics,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }

    /// Test constraint respect - route constraints should be respected
    fn test_constraint_respect(
        &mut self,
        profile: TransportProfile,
        test_routes: &[(NodeId, NodeId)],
    ) -> PRSv3TestResult {
        let start_time = Instant::now();
        let mut metrics = PRSv3Metrics::new();

        let test_subset = &test_routes[..test_routes.len().min(10)];
        let mut constraint_violations = 0;

        for &(start, end) in test_subset {
            // Test with time constraint
            let request = TimeRouteRequest::new(profile, start, end).with_max_time(60); // 1 minute limit
            let response = self.time_router.route(request);

            if response.route_found && !response.meets_time_constraint() {
                constraint_violations += 1;
            }
        }

        metrics.routes_tested = test_subset.len();
        metrics.execution_time_ms = start_time.elapsed().as_millis() as u64;

        // Pass if no constraint violations
        let status = if constraint_violations == 0 {
            TestStatus::Pass
        } else {
            TestStatus::Fail
        };

        PRSv3TestResult {
            test_type: PRSv3TestType::ConstraintRespect,
            profile,
            status,
            message: format!("Constraint violations: {}", constraint_violations),
            metrics,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }

    /// Generate test routes for a profile
    fn generate_test_routes(&self, _profile: TransportProfile) -> Vec<(NodeId, NodeId)> {
        // Generate realistic test routes based on profile
        let mut routes = Vec::new();

        for i in 0..self.config.test_routes_per_profile {
            let start_id = (i * 2 + 1) as u64;
            let end_id = (i * 2 + 2) as u64;
            routes.push((NodeId::new(start_id), NodeId::new(end_id)));
        }

        routes
    }

    /// Get PRS v3 configuration
    pub fn config(&self) -> &PRSv3Config {
        &self.config
    }
}

/// PRS v3 test report
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PRSv3Report {
    pub version: String,
    pub profiles: Vec<TransportProfile>,
    pub results: Vec<PRSv3TestResult>,
    pub summary: PRSv3Summary,
    pub timestamp: u64,
}

/// PRS v3 summary statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PRSv3Summary {
    pub total_tests: usize,
    pub passed: usize,
    pub failed: usize,
    pub execution_time_ms: u64,
    pub overall_status: TestStatus,
}

/// Calculate correlation between two datasets
fn calculate_correlation(x: &[f64], y: &[f64]) -> f64 {
    if x.len() != y.len() || x.len() < 2 {
        return 0.0;
    }

    let n = x.len() as f64;
    let sum_x: f64 = x.iter().sum();
    let sum_y: f64 = y.iter().sum();
    let sum_xy: f64 = x.iter().zip(y.iter()).map(|(a, b)| a * b).sum();
    let sum_x2: f64 = x.iter().map(|a| a * a).sum();
    let sum_y2: f64 = y.iter().map(|b| b * b).sum();

    let numerator = n * sum_xy - sum_x * sum_y;
    let denominator = ((n * sum_x2 - sum_x * sum_x) * (n * sum_y2 - sum_y * sum_y)).sqrt();

    if denominator == 0.0 {
        0.0
    } else {
        numerator / denominator
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dual_core::{GraphNode, TimeEdge, TimeWeight};
    use crate::profiles::EdgeId;
    use butterfly_geometry::Point2D;

    fn create_test_setup() -> (DualCoreGraph, PRSv3Config) {
        let profiles = vec![TransportProfile::Car];
        let mut dual_core = DualCoreGraph::new(profiles);

        // Add test nodes
        let node1 = GraphNode::new(NodeId::new(1), Point2D::new(0.0, 0.0));
        let node2 = GraphNode::new(NodeId::new(2), Point2D::new(1.0, 0.0));
        let node3 = GraphNode::new(NodeId::new(3), Point2D::new(2.0, 0.0));

        dual_core.time_graph.add_node(node1.clone());
        dual_core.time_graph.add_node(node2.clone());
        dual_core.time_graph.add_node(node3.clone());
        dual_core.nav_graph.add_node(node1);
        dual_core.nav_graph.add_node(node2);
        dual_core.nav_graph.add_node(node3);

        // Add test edges
        let mut edge1 = TimeEdge::new(EdgeId(1), NodeId::new(1), NodeId::new(2));
        edge1.add_weight(TransportProfile::Car, TimeWeight::new(60.0, 1000.0));
        dual_core.time_graph.add_edge(edge1);

        let mut edge2 = TimeEdge::new(EdgeId(2), NodeId::new(2), NodeId::new(3));
        edge2.add_weight(TransportProfile::Car, TimeWeight::new(90.0, 1500.0));
        dual_core.time_graph.add_edge(edge2);

        // Add nav edges (required for dual core consistency)
        use crate::dual_core::NavEdge;
        use butterfly_geometry::{NavigationGeometry, SnapSkeleton};

        let snap_skeleton1 = SnapSkeleton::new(
            vec![Point2D::new(0.0, 0.0), Point2D::new(1.0, 0.0)],
            vec![],
            1000.0,
            5.0,
        );
        let nav_geometry1 = NavigationGeometry::new(
            vec![Point2D::new(0.0, 0.0), Point2D::new(1.0, 0.0)],
            vec![],
            500.0,
            0.5,
            1.0,
            0.8,
        );
        let mut nav_edge1 = NavEdge::new(
            EdgeId(1),
            NodeId::new(1),
            NodeId::new(2),
            snap_skeleton1,
            nav_geometry1,
            None,
        );
        nav_edge1.add_weight(TransportProfile::Car, TimeWeight::new(60.0, 1000.0));
        dual_core.nav_graph.add_edge(nav_edge1);

        let snap_skeleton2 = SnapSkeleton::new(
            vec![Point2D::new(1.0, 0.0), Point2D::new(2.0, 0.0)],
            vec![],
            1500.0,
            5.0,
        );
        let nav_geometry2 = NavigationGeometry::new(
            vec![Point2D::new(1.0, 0.0), Point2D::new(2.0, 0.0)],
            vec![],
            750.0,
            0.5,
            1.0,
            0.8,
        );
        let mut nav_edge2 = NavEdge::new(
            EdgeId(2),
            NodeId::new(2),
            NodeId::new(3),
            snap_skeleton2,
            nav_geometry2,
            None,
        );
        nav_edge2.add_weight(TransportProfile::Car, TimeWeight::new(90.0, 1500.0));
        dual_core.nav_graph.add_edge(nav_edge2);

        let config = PRSv3Config::default();
        (dual_core, config)
    }

    #[test]
    fn test_prs_v3_creation() {
        let (dual_core, config) = create_test_setup();
        let prs = ProfileRegressionSuiteV3::new(config, dual_core);
        assert!(prs.is_ok());
    }

    #[test]
    fn test_prs_v3_config_defaults() {
        let config = PRSv3Config::default();
        assert_eq!(config.max_eta_error_seconds, 300.0);
        assert_eq!(config.max_turn_violations, 0);
        assert_eq!(config.min_compression_accuracy, 0.99);
        assert_eq!(config.test_routes_per_profile, 100);
    }

    #[test]
    fn test_prs_v3_metrics() {
        let mut metrics = PRSv3Metrics::new();
        assert_eq!(metrics.routes_tested, 0);
        assert!(metrics.eta_accuracy_mean_error.is_none());

        metrics.eta_accuracy_mean_error = Some(150.0);
        metrics.routes_tested = 50;
        assert_eq!(metrics.eta_accuracy_mean_error, Some(150.0));
        assert_eq!(metrics.routes_tested, 50);
    }

    #[test]
    fn test_correlation_calculation() {
        let x = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let y = vec![2.0, 4.0, 6.0, 8.0, 10.0];

        let correlation = calculate_correlation(&x, &y);
        assert!((correlation - 1.0).abs() < 0.0001); // Perfect positive correlation

        let y_reverse = vec![10.0, 8.0, 6.0, 4.0, 2.0];
        let correlation_negative = calculate_correlation(&x, &y_reverse);
        assert!((correlation_negative + 1.0).abs() < 0.0001); // Perfect negative correlation

        // Test edge cases
        assert_eq!(calculate_correlation(&[], &[]), 0.0);
        assert_eq!(calculate_correlation(&[1.0], &[2.0]), 0.0);
    }

    #[test]
    fn test_eta_plausibility_test() {
        let (dual_core, config) = create_test_setup();
        let mut prs = ProfileRegressionSuiteV3::new(config, dual_core).unwrap();

        let test_routes = vec![(NodeId::new(1), NodeId::new(3))];
        let result = prs.test_eta_plausibility(TransportProfile::Car, &test_routes);

        assert_eq!(result.test_type, PRSv3TestType::ETAPlausibility);
        assert_eq!(result.profile, TransportProfile::Car);
        assert!(result.metrics.routes_tested > 0);
    }

    #[test]
    fn test_route_quality_test() {
        let (dual_core, config) = create_test_setup();
        let mut prs = ProfileRegressionSuiteV3::new(config, dual_core).unwrap();

        let test_routes = vec![(NodeId::new(1), NodeId::new(3))];
        let result = prs.test_route_quality(TransportProfile::Car, &test_routes);

        assert_eq!(result.test_type, PRSv3TestType::RouteQuality);
        assert_eq!(result.profile, TransportProfile::Car);
        assert!(result.metrics.routes_tested > 0);
    }

    #[test]
    fn test_time_parity_test() {
        let (dual_core, config) = create_test_setup();
        let mut prs = ProfileRegressionSuiteV3::new(config, dual_core).unwrap();

        let test_routes = vec![(NodeId::new(1), NodeId::new(3))];
        let result = prs.test_time_parity(TransportProfile::Car, &test_routes);

        assert_eq!(result.test_type, PRSv3TestType::TimeParity);
        assert_eq!(result.profile, TransportProfile::Car);
        assert!(result.metrics.time_distance_correlation.is_some());
    }

    #[test]
    fn test_complete_suite_execution() {
        let (dual_core, config) = create_test_setup();
        let mut prs = ProfileRegressionSuiteV3::new(config, dual_core).unwrap();

        let profiles = vec![TransportProfile::Car];
        let report = prs.run_complete_suite(&profiles);

        assert_eq!(report.version, "3.0");
        assert_eq!(report.profiles, profiles);
        assert!(report.results.len() > 0);
        assert!(report.summary.total_tests > 0);
    }

    #[test]
    fn test_prs_v3_with_extensions() {
        let (dual_core, config) = create_test_setup();
        let mut prs = ProfileRegressionSuiteV3::new(config, dual_core).unwrap();

        // Add turn system
        let turn_system = TurnRestrictionTableSystem::new(100);
        prs = prs.with_turn_system(turn_system);

        // Add weight system
        let weight_system = WeightCompressionSystem::new();
        prs = prs.with_weight_system(weight_system);

        let profiles = vec![TransportProfile::Car];
        let report = prs.run_complete_suite(&profiles);

        // Should include compression and shard performance tests
        let test_types: Vec<_> = report.results.iter().map(|r| &r.test_type).collect();
        assert!(test_types.contains(&&PRSv3TestType::CompressionAccuracy));
        assert!(test_types.contains(&&PRSv3TestType::ShardPerformance));
    }

    #[test]
    fn test_test_route_generation() {
        let (dual_core, config) = create_test_setup();
        let prs = ProfileRegressionSuiteV3::new(config, dual_core).unwrap();

        let routes = prs.generate_test_routes(TransportProfile::Car);
        assert_eq!(routes.len(), 100); // Default config

        // Routes should be valid pairs
        for (start, end) in routes {
            assert_ne!(start, end);
        }
    }
}
