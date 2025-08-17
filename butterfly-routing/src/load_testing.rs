//! M7.3 - Load Testing: Concurrent multi-profile serving validation

use crate::profiles::TransportProfile;
use crate::thread_architecture::{ThreadArchitectureSystem, ThreadPoolConfig};
use crate::sharded_caching::AutoRebalancingCacheManager;
use crate::time_routing::{TimeBasedRouter, TimeRouteRequest, TimeRouteResponse};
use crate::dual_core::{DualCoreGraph, NodeId};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::time::sleep;

/// Load test configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadTestConfig {
    pub duration_seconds: u64,
    pub concurrent_clients: usize,
    pub requests_per_second: f64,
    pub profile_distribution: HashMap<TransportProfile, f64>, // Profile weight distribution
    pub route_complexity_mix: RouteComplexityMix,
    pub enable_streaming: bool,
    pub target_latency_p95_ms: u64,
    pub target_throughput_rps: f64,
}

impl Default for LoadTestConfig {
    fn default() -> Self {
        let mut profile_distribution = HashMap::new();
        profile_distribution.insert(TransportProfile::Car, 0.6);      // 60% car routes
        profile_distribution.insert(TransportProfile::Bicycle, 0.25); // 25% bike routes
        profile_distribution.insert(TransportProfile::Foot, 0.15);    // 15% foot routes

        Self {
            duration_seconds: 60,
            concurrent_clients: 50,
            requests_per_second: 100.0,
            profile_distribution,
            route_complexity_mix: RouteComplexityMix::default(),
            enable_streaming: true,
            target_latency_p95_ms: 100,
            target_throughput_rps: 95.0, // Allow 5% tolerance
        }
    }
}

/// Mix of route complexities for realistic load testing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouteComplexityMix {
    pub short_routes_percent: f64,   // <5km routes
    pub medium_routes_percent: f64,  // 5-20km routes  
    pub long_routes_percent: f64,    // >20km routes
    pub complex_routes_percent: f64, // Routes with turn restrictions
}

impl Default for RouteComplexityMix {
    fn default() -> Self {
        Self {
            short_routes_percent: 0.5,   // 50% short routes
            medium_routes_percent: 0.35, // 35% medium routes
            long_routes_percent: 0.1,    // 10% long routes
            complex_routes_percent: 0.05, // 5% complex routes
        }
    }
}

/// Individual routing request for load testing
#[derive(Debug, Clone)]
pub struct LoadTestRequest {
    pub id: u64,
    pub profile: TransportProfile,
    pub start_node: NodeId,
    pub end_node: NodeId,
    pub complexity: RouteComplexity,
    pub timestamp: Instant,
}

/// Route complexity classification
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RouteComplexity {
    Short,   // <5km
    Medium,  // 5-20km
    Long,    // >20km
    Complex, // With turn restrictions
}

/// Response from a load test request
#[derive(Debug, Clone)]
pub struct LoadTestResponse {
    pub request_id: u64,
    pub response: Result<TimeRouteResponse, String>,
    pub latency: Duration,
    pub timestamp: Instant,
    pub profile: TransportProfile,
    pub complexity: RouteComplexity,
}

/// Streaming chunk for Axum response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutingStreamChunk {
    pub chunk_id: usize,
    pub total_chunks: usize,
    pub data: Vec<u8>,
    pub is_final: bool,
}

/// Load test client that generates requests
pub struct LoadTestClient {
    #[allow(dead_code)]
    client_id: usize,
    config: LoadTestConfig,
    router: Arc<TimeBasedRouter>,
    request_counter: AtomicU64,
    stats: Mutex<LoadTestClientStats>,
}

impl LoadTestClient {
    pub fn new(
        client_id: usize,
        config: LoadTestConfig,
        router: Arc<TimeBasedRouter>,
    ) -> Self {
        Self {
            client_id,
            config,
            router,
            request_counter: AtomicU64::new(0),
            stats: Mutex::new(LoadTestClientStats::new(client_id)),
        }
    }

    /// Run load test client
    pub async fn run(
        &self,
        response_tx: mpsc::UnboundedSender<LoadTestResponse>,
    ) -> Result<LoadTestClientStats, String> {
        let start_time = Instant::now();
        let duration = Duration::from_secs(self.config.duration_seconds);
        let request_interval = Duration::from_secs_f64(1.0 / self.config.requests_per_second);

        while start_time.elapsed() < duration {
            let request = self.generate_request();
            let response = self.execute_request(request).await;
            
            if response_tx.send(response).is_err() {
                break; // Receiver dropped
            }

            sleep(request_interval).await;
        }

        Ok(self.stats.lock().unwrap().clone())
    }

    /// Generate a random routing request
    fn generate_request(&self) -> LoadTestRequest {
        let request_id = self.request_counter.fetch_add(1, Ordering::Relaxed);
        let profile = self.select_random_profile();
        let complexity = self.select_random_complexity();
        let (start_node, end_node) = self.generate_route_nodes(complexity);

        LoadTestRequest {
            id: request_id,
            profile,
            start_node,
            end_node,
            complexity,
            timestamp: Instant::now(),
        }
    }

    /// Select a random transport profile based on distribution
    fn select_random_profile(&self) -> TransportProfile {
        let rand_val: f64 = rand::random();
        let mut cumulative = 0.0;

        for (profile, weight) in &self.config.profile_distribution {
            cumulative += weight;
            if rand_val <= cumulative {
                return *profile;
            }
        }

        TransportProfile::Car // Fallback
    }

    /// Select random route complexity
    fn select_random_complexity(&self) -> RouteComplexity {
        let rand_val: f64 = rand::random();
        let mix = &self.config.route_complexity_mix;

        if rand_val < mix.short_routes_percent {
            RouteComplexity::Short
        } else if rand_val < mix.short_routes_percent + mix.medium_routes_percent {
            RouteComplexity::Medium
        } else if rand_val < mix.short_routes_percent + mix.medium_routes_percent + mix.long_routes_percent {
            RouteComplexity::Long
        } else {
            RouteComplexity::Complex
        }
    }

    /// Generate start and end nodes based on complexity
    fn generate_route_nodes(&self, complexity: RouteComplexity) -> (NodeId, NodeId) {
        match complexity {
            RouteComplexity::Short => {
                // Short routes: nodes close together
                let base = (rand::random::<u64>() % 1000) + 1;
                (NodeId::new(base), NodeId::new(base + 1 + (rand::random::<u64>() % 10)))
            }
            RouteComplexity::Medium => {
                // Medium routes: nodes moderately far apart
                let base = (rand::random::<u64>() % 1000) + 1;
                (NodeId::new(base), NodeId::new(base + 10 + (rand::random::<u64>() % 50)))
            }
            RouteComplexity::Long => {
                // Long routes: nodes far apart
                let base = (rand::random::<u64>() % 1000) + 1;
                (NodeId::new(base), NodeId::new(base + 100 + (rand::random::<u64>() % 500)))
            }
            RouteComplexity::Complex => {
                // Complex routes: specific nodes known to have turn restrictions
                let complex_nodes = [123, 456, 789, 1011, 1213]; // Predefined complex nodes
                let start_idx = rand::random::<usize>() % complex_nodes.len();
                let end_idx = (start_idx + 1 + rand::random::<usize>()) % complex_nodes.len();
                (NodeId::new(complex_nodes[start_idx]), NodeId::new(complex_nodes[end_idx]))
            }
        }
    }

    /// Execute a routing request
    async fn execute_request(&self, request: LoadTestRequest) -> LoadTestResponse {
        let start_time = Instant::now();
        
        let route_request = TimeRouteRequest {
            profile: request.profile,
            start_node: request.start_node,
            end_node: request.end_node,
            departure_time: None,
            avoid_toll_roads: false,
            avoid_highways: false,
            avoid_ferries: false,
            max_route_time_seconds: Some(3600),
        };

        // Create a mutable clone of the router for this request
        // Note: In production, this would use a pool of routers or make the router thread-safe
        let response = {
            // Create a new router instance for this request to avoid contention
            match TimeBasedRouter::new(self.router.dual_core().clone()) {
                Ok(mut router) => {
                    // Copy configuration from the shared router
                    if let Some(weight_compression) = self.router.weight_compression() {
                        router = router.with_weight_compression(weight_compression.clone());
                    }
                    if let Some(turn_restrictions) = self.router.turn_restrictions() {
                        router = router.with_turn_restrictions(turn_restrictions.clone());
                    }
                    for (profile, config) in self.router.profile_configs() {
                        router = router.with_profile_config(*profile, config.clone());
                    }
                    Ok(router.route(route_request))
                }
                Err(e) => Err(e)
            }
        };

        let latency = start_time.elapsed();

        // Update client stats
        if let Ok(mut stats) = self.stats.lock() {
            stats.record_request(request.profile, latency, response.is_ok());
        }

        LoadTestResponse {
            request_id: request.id,
            response,
            latency,
            timestamp: Instant::now(),
            profile: request.profile,
            complexity: request.complexity,
        }
    }
}

/// Statistics for a load test client
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadTestClientStats {
    pub client_id: usize,
    pub total_requests: u64,
    pub successful_requests: u64,
    pub failed_requests: u64,
    pub total_latency_ms: u64,
    pub profile_stats: HashMap<TransportProfile, ProfileClientStats>,
}

impl LoadTestClientStats {
    pub fn new(client_id: usize) -> Self {
        let mut profile_stats = HashMap::new();
        for profile in [TransportProfile::Car, TransportProfile::Bicycle, TransportProfile::Foot] {
            profile_stats.insert(profile, ProfileClientStats::new());
        }

        Self {
            client_id,
            total_requests: 0,
            successful_requests: 0,
            failed_requests: 0,
            total_latency_ms: 0,
            profile_stats,
        }
    }

    pub fn record_request(&mut self, _profile: TransportProfile, latency: Duration, success: bool) {
        self.total_requests += 1;
        self.total_latency_ms += latency.as_millis() as u64;

        if success {
            self.successful_requests += 1;
        } else {
            self.failed_requests += 1;
        }
    }

    pub fn success_rate(&self) -> f64 {
        if self.total_requests == 0 {
            return 0.0;
        }
        self.successful_requests as f64 / self.total_requests as f64
    }

    pub fn average_latency_ms(&self) -> f64 {
        if self.total_requests == 0 {
            return 0.0;
        }
        self.total_latency_ms as f64 / self.total_requests as f64
    }
}

/// Per-profile statistics for a client
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfileClientStats {
    pub requests: u64,
    pub successes: u64,
    pub total_latency_ms: u64,
}

impl ProfileClientStats {
    pub fn new() -> Self {
        Self {
            requests: 0,
            successes: 0,
            total_latency_ms: 0,
        }
    }
}

/// Main load testing orchestrator
pub struct LoadTestOrchestrator {
    config: LoadTestConfig,
    thread_architecture: ThreadArchitectureSystem,
    cache_manager: AutoRebalancingCacheManager,
    router: Arc<TimeBasedRouter>,
}

impl LoadTestOrchestrator {
    pub fn new(
        config: LoadTestConfig,
        dual_core: DualCoreGraph,
    ) -> Result<Self, String> {
        let thread_config = ThreadPoolConfig::default();
        let mut thread_architecture = ThreadArchitectureSystem::new(thread_config);
        thread_architecture.start()?;

        let cache_manager = AutoRebalancingCacheManager::new(10000, 10000, true);
        let router = Arc::new(TimeBasedRouter::new(dual_core)?);

        Ok(Self {
            config,
            thread_architecture,
            cache_manager,
            router,
        })
    }

    /// Run comprehensive load test
    pub async fn run_load_test(&mut self) -> Result<LoadTestReport, String> {
        let start_time = Instant::now();
        
        // Set up response collection
        let (response_tx, mut response_rx) = mpsc::unbounded_channel();
        
        // Spawn clients
        let mut client_handles = Vec::new();
        for client_id in 0..self.config.concurrent_clients {
            let client = LoadTestClient::new(
                client_id,
                self.config.clone(),
                Arc::clone(&self.router),
            );
            
            let tx = response_tx.clone();
            let handle = tokio::spawn(async move {
                client.run(tx).await
            });
            
            client_handles.push(handle);
        }

        // Drop the original sender so the receiver can terminate
        drop(response_tx);

        // Collect responses while test runs
        let response_collection_handle = tokio::spawn(async move {
            let mut collected_responses = Vec::new();
            while let Some(response) = response_rx.recv().await {
                collected_responses.push(response);
            }
            collected_responses
        });

        // Wait for all clients to complete
        let mut client_stats = Vec::new();
        for handle in client_handles {
            match handle.await {
                Ok(Ok(stats)) => client_stats.push(stats),
                Ok(Err(e)) => return Err(format!("Client error: {}", e)),
                Err(e) => return Err(format!("Client join error: {}", e)),
            }
        }

        // Collect all responses
        let responses = response_collection_handle.await
            .map_err(|e| format!("Response collection error: {}", e))?;

        let total_duration = start_time.elapsed();

        // Generate comprehensive report
        let report = self.generate_report(client_stats, responses, total_duration).await?;
        
        Ok(report)
    }

    /// Generate comprehensive load test report
    async fn generate_report(
        &mut self,
        client_stats: Vec<LoadTestClientStats>,
        responses: Vec<LoadTestResponse>,
        total_duration: Duration,
    ) -> Result<LoadTestReport, String> {
        
        // Calculate latency percentiles
        let mut latencies: Vec<u64> = responses.iter()
            .map(|r| r.latency.as_millis() as u64)
            .collect();
        latencies.sort();

        let latency_p50 = percentile(&latencies, 0.5);
        let latency_p95 = percentile(&latencies, 0.95);
        let latency_p99 = percentile(&latencies, 0.99);

        // Calculate throughput
        let total_requests = responses.len();
        let throughput_rps = total_requests as f64 / total_duration.as_secs_f64();

        // Calculate success rate
        let successful_requests = responses.iter().filter(|r| r.response.is_ok()).count();
        let success_rate = successful_requests as f64 / total_requests as f64;

        // Profile breakdown
        let mut profile_breakdown = HashMap::new();
        for profile in [TransportProfile::Car, TransportProfile::Bicycle, TransportProfile::Foot] {
            let profile_responses: Vec<_> = responses.iter()
                .filter(|r| r.profile == profile)
                .collect();
            
            let profile_latencies: Vec<u64> = profile_responses.iter()
                .map(|r| r.latency.as_millis() as u64)
                .collect();
            
            let profile_successes = profile_responses.iter()
                .filter(|r| r.response.is_ok())
                .count();

            profile_breakdown.insert(profile, ProfileLoadTestStats {
                total_requests: profile_responses.len(),
                successful_requests: profile_successes,
                success_rate: if profile_responses.len() > 0 {
                    profile_successes as f64 / profile_responses.len() as f64
                } else { 0.0 },
                average_latency_ms: if !profile_latencies.is_empty() {
                    profile_latencies.iter().sum::<u64>() as f64 / profile_latencies.len() as f64
                } else { 0.0 },
                latency_p95_ms: percentile(&profile_latencies, 0.95),
            });
        }

        // Complexity breakdown
        let mut complexity_breakdown = HashMap::new();
        for complexity in [RouteComplexity::Short, RouteComplexity::Medium, RouteComplexity::Long, RouteComplexity::Complex] {
            let complexity_responses: Vec<_> = responses.iter()
                .filter(|r| r.complexity == complexity)
                .collect();
            
            let complexity_latencies: Vec<u64> = complexity_responses.iter()
                .map(|r| r.latency.as_millis() as u64)
                .collect();

            complexity_breakdown.insert(complexity, ComplexityLoadTestStats {
                total_requests: complexity_responses.len(),
                average_latency_ms: if !complexity_latencies.is_empty() {
                    complexity_latencies.iter().sum::<u64>() as f64 / complexity_latencies.len() as f64
                } else { 0.0 },
                latency_p95_ms: percentile(&complexity_latencies, 0.95),
            });
        }

        // Check if targets were met
        let meets_latency_target = latency_p95 <= self.config.target_latency_p95_ms;
        let meets_throughput_target = throughput_rps >= self.config.target_throughput_rps;

        // Get system stats
        let thread_stats = self.thread_architecture.stats();
        let cache_stats = self.cache_manager.stats();

        Ok(LoadTestReport {
            config: self.config.clone(),
            total_duration_ms: total_duration.as_millis() as u64,
            total_requests,
            successful_requests,
            success_rate,
            throughput_rps,
            latency_p50_ms: latency_p50,
            latency_p95_ms: latency_p95,
            latency_p99_ms: latency_p99,
            meets_latency_target,
            meets_throughput_target,
            profile_breakdown,
            complexity_breakdown,
            client_stats,
            thread_architecture_stats: thread_stats,
            cache_manager_stats: cache_stats,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        })
    }
}

/// Load test report
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadTestReport {
    pub config: LoadTestConfig,
    pub total_duration_ms: u64,
    pub total_requests: usize,
    pub successful_requests: usize,
    pub success_rate: f64,
    pub throughput_rps: f64,
    pub latency_p50_ms: u64,
    pub latency_p95_ms: u64,
    pub latency_p99_ms: u64,
    pub meets_latency_target: bool,
    pub meets_throughput_target: bool,
    pub profile_breakdown: HashMap<TransportProfile, ProfileLoadTestStats>,
    pub complexity_breakdown: HashMap<RouteComplexity, ComplexityLoadTestStats>,
    pub client_stats: Vec<LoadTestClientStats>,
    pub thread_architecture_stats: crate::thread_architecture::ThreadArchitectureStats,
    pub cache_manager_stats: crate::sharded_caching::CacheManagerStats,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfileLoadTestStats {
    pub total_requests: usize,
    pub successful_requests: usize,
    pub success_rate: f64,
    pub average_latency_ms: f64,
    pub latency_p95_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComplexityLoadTestStats {
    pub total_requests: usize,
    pub average_latency_ms: f64,
    pub latency_p95_ms: u64,
}

/// Calculate percentile from sorted array
fn percentile(sorted_values: &[u64], p: f64) -> u64 {
    if sorted_values.is_empty() {
        return 0;
    }
    
    let index = (p * (sorted_values.len() - 1) as f64) as usize;
    sorted_values[index.min(sorted_values.len() - 1)]
}

/// Axum streaming implementation for routing responses
pub struct AxumStreamingHandler {
    chunk_size: usize,
}

impl AxumStreamingHandler {
    pub fn new(chunk_size: usize) -> Self {
        Self { chunk_size }
    }

    /// Convert routing response to streaming chunks
    pub fn create_streaming_response(&self, response: TimeRouteResponse) -> Vec<RoutingStreamChunk> {
        // Serialize the response
        let serialized = serde_json::to_vec(&response).unwrap_or_default();
        let total_size = serialized.len();
        let total_chunks = (total_size + self.chunk_size - 1) / self.chunk_size;

        let mut chunks = Vec::new();
        for (i, chunk_data) in serialized.chunks(self.chunk_size).enumerate() {
            chunks.push(RoutingStreamChunk {
                chunk_id: i,
                total_chunks,
                data: chunk_data.to_vec(),
                is_final: i == total_chunks - 1,
            });
        }

        chunks
    }

    /// Stream chunks over async channel
    pub async fn stream_response(
        &self,
        response: TimeRouteResponse,
        chunk_tx: mpsc::UnboundedSender<RoutingStreamChunk>,
    ) -> Result<(), String> {
        let chunks = self.create_streaming_response(response);
        
        for chunk in chunks {
            if chunk_tx.send(chunk).is_err() {
                return Err("Receiver dropped".to_string());
            }
            
            // Small delay between chunks to simulate real streaming
            sleep(Duration::from_millis(1)).await;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dual_core::{GraphNode, TimeEdge, NavEdge, TimeWeight};
    use crate::profiles::EdgeId;
    use butterfly_geometry::{Point2D, SnapSkeleton, NavigationGeometry};

    fn create_test_dual_core() -> DualCoreGraph {
        let profiles = vec![TransportProfile::Car, TransportProfile::Bicycle, TransportProfile::Foot];
        let mut dual_core = DualCoreGraph::new(profiles);

        // Add test nodes
        for i in 1..=10 {
            let node = GraphNode::new(NodeId::new(i), Point2D::new(i as f64, i as f64));
            dual_core.time_graph.add_node(node.clone());
            dual_core.nav_graph.add_node(node);
        }

        // Add test edges
        for i in 1..=5 {
            let mut time_edge = TimeEdge::new(EdgeId(i), NodeId::new(i as u64), NodeId::new((i + 1) as u64));
            time_edge.add_weight(TransportProfile::Car, TimeWeight::new(60.0, 1000.0));
            time_edge.add_weight(TransportProfile::Bicycle, TimeWeight::new(120.0, 1000.0));
            time_edge.add_weight(TransportProfile::Foot, TimeWeight::new(600.0, 1000.0));
            dual_core.time_graph.add_edge(time_edge);

            let snap_skeleton = SnapSkeleton::new(
                vec![Point2D::new(i as f64, i as f64), Point2D::new((i + 1) as f64, (i + 1) as f64)],
                vec![],
                1000.0,
                5.0,
            );
            let nav_geometry = NavigationGeometry::new(
                vec![Point2D::new(i as f64, i as f64), Point2D::new((i + 1) as f64, (i + 1) as f64)],
                vec![],
                500.0,
                0.5,
                1.0,
                0.8,
            );
            let mut nav_edge = NavEdge::new(
                EdgeId(i),
                NodeId::new(i as u64),
                NodeId::new((i + 1) as u64),
                snap_skeleton,
                nav_geometry,
                None,
            );
            nav_edge.add_weight(TransportProfile::Car, TimeWeight::new(60.0, 1000.0));
            nav_edge.add_weight(TransportProfile::Bicycle, TimeWeight::new(120.0, 1000.0));
            nav_edge.add_weight(TransportProfile::Foot, TimeWeight::new(600.0, 1000.0));
            dual_core.nav_graph.add_edge(nav_edge);
        }

        dual_core
    }

    #[test]
    fn test_load_test_config_default() {
        let config = LoadTestConfig::default();
        assert_eq!(config.duration_seconds, 60);
        assert_eq!(config.concurrent_clients, 50);
        assert_eq!(config.requests_per_second, 100.0);
        assert!(config.enable_streaming);
        
        // Check profile distribution sums to 1.0
        let total_weight: f64 = config.profile_distribution.values().sum();
        assert!((total_weight - 1.0).abs() < 0.001);
    }

    #[test]
    fn test_route_complexity_mix() {
        let mix = RouteComplexityMix::default();
        let total = mix.short_routes_percent + mix.medium_routes_percent + 
                   mix.long_routes_percent + mix.complex_routes_percent;
        assert!((total - 1.0).abs() < 0.001);
    }

    #[test]
    fn test_load_test_client_creation() {
        let dual_core = create_test_dual_core();
        let router = Arc::new(TimeBasedRouter::new(dual_core).unwrap());
        let config = LoadTestConfig::default();
        
        let client = LoadTestClient::new(0, config, router);
        assert_eq!(client.client_id, 0);
        assert_eq!(client.stats.lock().unwrap().client_id, 0);
    }

    #[test]
    fn test_request_generation() {
        let dual_core = create_test_dual_core();
        let router = Arc::new(TimeBasedRouter::new(dual_core).unwrap());
        let config = LoadTestConfig::default();
        let client = LoadTestClient::new(0, config, router);
        
        let request = client.generate_request();
        assert_eq!(request.id, 0);
        assert!(matches!(request.profile, TransportProfile::Car | TransportProfile::Bicycle | TransportProfile::Foot));
        assert!(matches!(request.complexity, RouteComplexity::Short | RouteComplexity::Medium | RouteComplexity::Long | RouteComplexity::Complex));
    }

    #[test]
    fn test_profile_selection() {
        let dual_core = create_test_dual_core();
        let router = Arc::new(TimeBasedRouter::new(dual_core).unwrap());
        let config = LoadTestConfig::default();
        let client = LoadTestClient::new(0, config, router);
        
        // Generate many profiles and check distribution
        let mut profile_counts = HashMap::new();
        for _ in 0..1000 {
            let profile = client.select_random_profile();
            *profile_counts.entry(profile).or_insert(0) += 1;
        }
        
        // Should have all three profiles
        assert!(profile_counts.contains_key(&TransportProfile::Car));
        assert!(profile_counts.contains_key(&TransportProfile::Bicycle));
        assert!(profile_counts.contains_key(&TransportProfile::Foot));
        
        // Car should be most common (60% target)
        let car_count = profile_counts.get(&TransportProfile::Car).unwrap_or(&0);
        assert!(*car_count > 500); // Should be roughly 600, allow some variance
    }

    #[test]
    fn test_complexity_selection() {
        let dual_core = create_test_dual_core();
        let router = Arc::new(TimeBasedRouter::new(dual_core).unwrap());
        let config = LoadTestConfig::default();
        let client = LoadTestClient::new(0, config, router);
        
        // Generate many complexities and check distribution
        let mut complexity_counts = HashMap::new();
        for _ in 0..1000 {
            let complexity = client.select_random_complexity();
            *complexity_counts.entry(complexity).or_insert(0) += 1;
        }
        
        // Short routes should be most common (50% target)
        let short_count = complexity_counts.get(&RouteComplexity::Short).unwrap_or(&0);
        assert!(*short_count > 400); // Should be roughly 500, allow some variance
    }

    #[test]
    fn test_node_generation_by_complexity() {
        let dual_core = create_test_dual_core();
        let router = Arc::new(TimeBasedRouter::new(dual_core).unwrap());
        let config = LoadTestConfig::default();
        let client = LoadTestClient::new(0, config, router);
        
        // Test short route generation
        let (start, end) = client.generate_route_nodes(RouteComplexity::Short);
        let distance = if end.0 > start.0 { end.0 - start.0 } else { start.0 - end.0 };
        assert!(distance <= 11); // Should be close together
        
        // Test long route generation
        let (start, end) = client.generate_route_nodes(RouteComplexity::Long);
        let distance = if end.0 > start.0 { end.0 - start.0 } else { start.0 - end.0 };
        assert!(distance >= 100); // Should be far apart
    }

    #[test]
    fn test_client_stats() {
        let mut stats = LoadTestClientStats::new(5);
        assert_eq!(stats.client_id, 5);
        assert_eq!(stats.total_requests, 0);
        assert_eq!(stats.success_rate(), 0.0);
        assert_eq!(stats.average_latency_ms(), 0.0);
        
        // Record some requests
        stats.record_request(TransportProfile::Car, Duration::from_millis(100), true);
        stats.record_request(TransportProfile::Car, Duration::from_millis(200), false);
        
        assert_eq!(stats.total_requests, 2);
        assert_eq!(stats.successful_requests, 1);
        assert_eq!(stats.success_rate(), 0.5);
        assert_eq!(stats.average_latency_ms(), 150.0);
    }

    #[test]
    fn test_percentile_calculation() {
        let values = vec![10, 20, 30, 40, 50, 60, 70, 80, 90, 100];
        
        assert_eq!(percentile(&values, 0.0), 10);
        assert_eq!(percentile(&values, 0.5), 50);
        assert_eq!(percentile(&values, 0.9), 90);
        assert_eq!(percentile(&values, 1.0), 100);
        
        // Empty array
        assert_eq!(percentile(&[], 0.5), 0);
    }

    #[test]
    fn test_axum_streaming_handler() {
        let handler = AxumStreamingHandler::new(100);
        
        // Create a test response
        let request = TimeRouteRequest {
            profile: TransportProfile::Car,
            start_node: NodeId::new(1),
            end_node: NodeId::new(3),
            departure_time: None,
            avoid_toll_roads: false,
            avoid_highways: false,
            avoid_ferries: false,
            max_route_time_seconds: None,
        };
        let response = TimeRouteResponse {
            request,
            route_found: true,
            total_time_seconds: 3600.0,
            total_distance_meters: 50000.0,
            edge_sequence: vec![EdgeId(1), EdgeId(2)],
            node_sequence: vec![NodeId::new(1), NodeId::new(2), NodeId::new(3)],
            turn_penalty_seconds: 0.0,
            estimated_arrival_time: None,
            computation_stats: crate::time_routing::TimeComputationStats {
                computation_time_ms: 45,
                nodes_visited: 100,
                edges_relaxed: 250,
                turn_checks: 0,
                weight_decompressions: 0,
                shard_hits: 0,
                shard_misses: 0,
                compression_system_used: false,
                turn_system_used: false,
            },
            route_quality: crate::time_routing::RouteQuality::Excellent,
        };
        
        let chunks = handler.create_streaming_response(response);
        assert!(!chunks.is_empty());
        
        // Check chunk properties
        for (i, chunk) in chunks.iter().enumerate() {
            assert_eq!(chunk.chunk_id, i);
            assert_eq!(chunk.total_chunks, chunks.len());
            assert_eq!(chunk.is_final, i == chunks.len() - 1);
            assert!(!chunk.data.is_empty());
        }
    }

    #[tokio::test]
    async fn test_load_test_orchestrator_creation() {
        let dual_core = create_test_dual_core();
        let config = LoadTestConfig {
            duration_seconds: 1,
            concurrent_clients: 2,
            requests_per_second: 5.0,
            ..LoadTestConfig::default()
        };
        
        let orchestrator = LoadTestOrchestrator::new(config, dual_core);
        assert!(orchestrator.is_ok());
    }

    #[tokio::test]
    async fn test_streaming_response() {
        let handler = AxumStreamingHandler::new(50);
        let (tx, mut rx) = mpsc::unbounded_channel();
        
        let request = TimeRouteRequest {
            profile: TransportProfile::Bicycle,
            start_node: NodeId::new(1),
            end_node: NodeId::new(2),
            departure_time: None,
            avoid_toll_roads: false,
            avoid_highways: false,
            avoid_ferries: false,
            max_route_time_seconds: None,
        };
        let response = TimeRouteResponse {
            request,
            route_found: true,
            total_time_seconds: 1800.0,
            total_distance_meters: 25000.0,
            edge_sequence: vec![EdgeId(1)],
            node_sequence: vec![NodeId::new(1), NodeId::new(2)],
            turn_penalty_seconds: 0.0,
            estimated_arrival_time: None,
            computation_stats: crate::time_routing::TimeComputationStats {
                computation_time_ms: 25,
                nodes_visited: 50,
                edges_relaxed: 125,
                turn_checks: 0,
                weight_decompressions: 0,
                shard_hits: 0,
                shard_misses: 0,
                compression_system_used: false,
                turn_system_used: false,
            },
            route_quality: crate::time_routing::RouteQuality::Good,
        };
        
        let stream_handle = tokio::spawn(async move {
            handler.stream_response(response, tx).await
        });
        
        let mut chunks_received = 0;
        while let Some(chunk) = rx.recv().await {
            chunks_received += 1;
            assert!(!chunk.data.is_empty());
            if chunk.is_final {
                break;
            }
        }
        
        assert!(chunks_received > 0);
        assert!(stream_handle.await.unwrap().is_ok());
    }
}