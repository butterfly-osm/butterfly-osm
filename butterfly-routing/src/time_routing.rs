//! M6.3 - Time-Cost Routing: /route time-based Dijkstra per profile

use crate::profiles::{EdgeId, TransportProfile};
use crate::dual_core::{NodeId, DualCoreGraph};
use crate::dijkstra::{DistanceRouter, RouteResult};
use crate::weight_compression::WeightCompressionSystem;
use crate::turn_restriction_tables::{TurnRestrictionTableSystem, JunctionId, TurnMovement};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Instant;

/// Time-based route request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeRouteRequest {
    pub profile: TransportProfile,
    pub start_node: NodeId,
    pub end_node: NodeId,
    pub departure_time: Option<u64>, // Unix timestamp (optional for traffic-aware routing)
    pub avoid_toll_roads: bool,
    pub avoid_highways: bool,
    pub avoid_ferries: bool,
    pub max_route_time_seconds: Option<u64>,
}

impl TimeRouteRequest {
    pub fn new(
        profile: TransportProfile,
        start_node: NodeId,
        end_node: NodeId,
    ) -> Self {
        Self {
            profile,
            start_node,
            end_node,
            departure_time: None,
            avoid_toll_roads: false,
            avoid_highways: false,
            avoid_ferries: false,
            max_route_time_seconds: None,
        }
    }

    pub fn with_departure_time(mut self, timestamp: u64) -> Self {
        self.departure_time = Some(timestamp);
        self
    }

    pub fn with_constraints(
        mut self,
        avoid_toll: bool,
        avoid_highways: bool,
        avoid_ferries: bool,
    ) -> Self {
        self.avoid_toll_roads = avoid_toll;
        self.avoid_highways = avoid_highways;
        self.avoid_ferries = avoid_ferries;
        self
    }

    pub fn with_max_time(mut self, max_seconds: u64) -> Self {
        self.max_route_time_seconds = Some(max_seconds);
        self
    }
}

/// Time-based route response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeRouteResponse {
    pub request: TimeRouteRequest,
    pub route_found: bool,
    pub total_time_seconds: f64,
    pub total_distance_meters: f64,
    pub edge_sequence: Vec<EdgeId>,
    pub node_sequence: Vec<NodeId>,
    pub turn_penalty_seconds: f64,
    pub estimated_arrival_time: Option<u64>,
    pub computation_stats: TimeComputationStats,
    pub route_quality: RouteQuality,
}

impl TimeRouteResponse {
    pub fn not_found(request: TimeRouteRequest, stats: TimeComputationStats) -> Self {
        Self {
            request,
            route_found: false,
            total_time_seconds: f64::INFINITY,
            total_distance_meters: f64::INFINITY,
            edge_sequence: Vec::new(),
            node_sequence: Vec::new(),
            turn_penalty_seconds: 0.0,
            estimated_arrival_time: None,
            computation_stats: stats,
            route_quality: RouteQuality::NotFound,
        }
    }

    pub fn found(
        request: TimeRouteRequest,
        route_result: RouteResult,
        turn_penalty: f64,
        stats: TimeComputationStats,
    ) -> Self {
        let estimated_arrival = request.departure_time
            .map(|dep| dep + route_result.total_time as u64);

        let quality = RouteQuality::evaluate(
            route_result.total_time,
            route_result.total_distance,
            turn_penalty,
            route_result.edge_path.len(),
        );

        Self {
            request,
            route_found: true,
            total_time_seconds: route_result.total_time,
            total_distance_meters: route_result.total_distance,
            edge_sequence: route_result.edge_path,
            node_sequence: route_result.node_path,
            turn_penalty_seconds: turn_penalty,
            estimated_arrival_time: estimated_arrival,
            computation_stats: stats,
            route_quality: quality,
        }
    }

    /// Get average speed in km/h
    pub fn average_speed_kmh(&self) -> f64 {
        if self.total_time_seconds <= 0.0 {
            0.0
        } else {
            (self.total_distance_meters / 1000.0) / (self.total_time_seconds / 3600.0)
        }
    }

    /// Check if route meets time constraint
    pub fn meets_time_constraint(&self) -> bool {
        if let Some(max_time) = self.request.max_route_time_seconds {
            self.total_time_seconds <= max_time as f64
        } else {
            true
        }
    }
}

/// Route quality assessment
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RouteQuality {
    Excellent,   // Fast, direct route
    Good,        // Reasonable route
    Acceptable,  // Longer but valid route
    Poor,        // Very long or indirect route
    NotFound,    // No route found
}

impl RouteQuality {
    fn evaluate(
        time_seconds: f64,
        distance_meters: f64,
        turn_penalty: f64,
        edge_count: usize,
    ) -> Self {
        // Simple heuristic for route quality
        let speed_kmh = if time_seconds > 0.0 {
            (distance_meters / 1000.0) / (time_seconds / 3600.0)
        } else {
            0.0
        };

        let turn_penalty_ratio = turn_penalty / time_seconds.max(1.0);
        let complexity_factor = edge_count as f64 / distance_meters.max(1000.0) * 1000.0;

        // Quality thresholds (these could be profile-specific)
        if speed_kmh >= 40.0 && turn_penalty_ratio <= 0.05 && complexity_factor <= 2.0 {
            RouteQuality::Excellent
        } else if speed_kmh >= 25.0 && turn_penalty_ratio <= 0.10 && complexity_factor <= 4.0 {
            RouteQuality::Good
        } else if speed_kmh >= 15.0 && turn_penalty_ratio <= 0.20 && complexity_factor <= 8.0 {
            RouteQuality::Acceptable
        } else {
            RouteQuality::Poor
        }
    }
}

/// Time-specific computation statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeComputationStats {
    pub computation_time_ms: u64,
    pub nodes_visited: usize,
    pub edges_relaxed: usize,
    pub turn_checks: usize,
    pub weight_decompressions: usize,
    pub shard_hits: usize,
    pub shard_misses: usize,
    pub compression_system_used: bool,
    pub turn_system_used: bool,
}

impl TimeComputationStats {
    pub fn new() -> Self {
        Self {
            computation_time_ms: 0,
            nodes_visited: 0,
            edges_relaxed: 0,
            turn_checks: 0,
            weight_decompressions: 0,
            shard_hits: 0,
            shard_misses: 0,
            compression_system_used: false,
            turn_system_used: false,
        }
    }

    pub fn shard_hit_rate(&self) -> f64 {
        let total = self.shard_hits + self.shard_misses;
        if total == 0 {
            1.0
        } else {
            self.shard_hits as f64 / total as f64
        }
    }
}

/// Time-based router using compressed weights and turn restrictions
pub struct TimeBasedRouter {
    dual_core: DualCoreGraph,
    distance_router: DistanceRouter,
    weight_compression: Option<WeightCompressionSystem>,
    turn_restrictions: Option<TurnRestrictionTableSystem>,
    profile_configs: HashMap<TransportProfile, ProfileRouteConfig>,
}

impl TimeBasedRouter {
    pub fn new(dual_core: DualCoreGraph) -> Result<Self, String> {
        let distance_router = DistanceRouter::new(dual_core.clone())?;

        Ok(Self {
            dual_core,
            distance_router,
            weight_compression: None,
            turn_restrictions: None,
            profile_configs: HashMap::new(),
        })
    }

    pub fn with_weight_compression(mut self, compression_system: WeightCompressionSystem) -> Self {
        self.weight_compression = Some(compression_system);
        self
    }

    pub fn with_turn_restrictions(mut self, turn_system: TurnRestrictionTableSystem) -> Self {
        self.turn_restrictions = Some(turn_system);
        self
    }

    pub fn with_profile_config(mut self, profile: TransportProfile, config: ProfileRouteConfig) -> Self {
        self.profile_configs.insert(profile, config);
        self
    }

    /// Get a reference to the dual core graph
    pub fn dual_core(&self) -> &DualCoreGraph {
        &self.dual_core
    }

    /// Get a reference to the weight compression system
    pub fn weight_compression(&self) -> Option<&WeightCompressionSystem> {
        self.weight_compression.as_ref()
    }

    /// Get a reference to the turn restrictions system
    pub fn turn_restrictions(&self) -> Option<&TurnRestrictionTableSystem> {
        self.turn_restrictions.as_ref()
    }

    /// Get a reference to the profile configurations
    pub fn profile_configs(&self) -> &HashMap<TransportProfile, ProfileRouteConfig> {
        &self.profile_configs
    }

    /// Route using time-based costs
    pub fn route(&mut self, request: TimeRouteRequest) -> TimeRouteResponse {
        let start_time = Instant::now();
        let mut stats = TimeComputationStats::new();

        stats.compression_system_used = self.weight_compression.is_some();
        stats.turn_system_used = self.turn_restrictions.is_some();

        // Apply route constraints based on profile and request
        let route_result = self.route_with_time_costs(&request, &mut stats);

        stats.computation_time_ms = start_time.elapsed().as_millis() as u64;

        match route_result {
            Ok((route, turn_penalty)) => {
                TimeRouteResponse::found(request, route, turn_penalty, stats)
            }
            Err(_) => {
                TimeRouteResponse::not_found(request, stats)
            }
        }
    }

    /// Internal routing with time cost optimization
    fn route_with_time_costs(
        &mut self,
        request: &TimeRouteRequest,
        stats: &mut TimeComputationStats,
    ) -> Result<(RouteResult, f64), String> {
        // Use distance router but with time-based costs
        let base_result = self.distance_router.route_time_graph(
            request.start_node,
            request.end_node,
            request.profile,
        )?;

        stats.nodes_visited = base_result.computation_stats.nodes_explored;
        stats.edges_relaxed = base_result.computation_stats.edges_relaxed;

        // Calculate turn penalties if turn restriction system is available
        let turn_penalty = if let Some(ref mut turn_system) = self.turn_restrictions {
            Self::calculate_turn_penalties_static(&base_result.edge_path, turn_system, request.profile, stats)
        } else {
            0.0
        };

        // Apply time optimizations if compression system is available
        let optimized_result = if let Some(ref weight_system) = self.weight_compression {
            self.apply_compressed_weights(&base_result, weight_system, request.profile, stats)?
        } else {
            base_result
        };

        Ok((optimized_result, turn_penalty))
    }

    /// Calculate turn penalties for a route
    fn calculate_turn_penalties_static(
        edge_sequence: &[EdgeId],
        turn_system: &mut TurnRestrictionTableSystem,
        profile: TransportProfile,
        stats: &mut TimeComputationStats,
    ) -> f64 {
        let mut total_penalty = 0.0;

        for window in edge_sequence.windows(2) {
            let from_edge = window[0];
            let to_edge = window[1];

            // Find the via junction (in practice, you'd get this from the graph)
            // For now, use a heuristic based on edge IDs
            let via_junction = JunctionId::new(((from_edge.0 + to_edge.0) / 2) as u64);

            let movement = TurnMovement::new(from_edge, via_junction, to_edge);
            let penalty = turn_system.get_turn_penalty_seconds(&profile, &movement);

            stats.turn_checks += 1;
            if penalty.is_finite() {
                total_penalty += penalty;
                stats.shard_hits += 1;
            } else {
                stats.shard_misses += 1;
            }
        }

        total_penalty
    }

    /// Apply compressed weights for more accurate time calculation
    fn apply_compressed_weights(
        &self,
        base_result: &RouteResult,
        weight_system: &WeightCompressionSystem,
        profile: TransportProfile,
        stats: &mut TimeComputationStats,
    ) -> Result<RouteResult, String> {
        let mut total_time = 0.0;
        let mut total_distance = 0.0;

        for &edge_id in &base_result.edge_path {
            // Calculate block ID from edge ID (in practice, you'd have a mapping)
            let block_id = (edge_id.0 / 131072) as u32; // EDGE_BLOCK_SIZE

            if let Some((time, distance)) = weight_system.get_weight(block_id, &edge_id, &profile) {
                total_time += time;
                total_distance += distance;
                stats.weight_decompressions += 1;
            } else {
                // Fallback to original weight if not in compression system
                total_time += base_result.total_time / base_result.edge_path.len() as f64;
                total_distance += base_result.total_distance / base_result.edge_path.len() as f64;
            }
        }

        Ok(RouteResult {
            profile: base_result.profile,
            start_node: base_result.start_node,
            end_node: base_result.end_node,
            total_time,
            total_distance,
            node_path: base_result.node_path.clone(),
            edge_path: base_result.edge_path.clone(),
            geometry: base_result.geometry.clone(),
            computation_stats: base_result.computation_stats.clone(),
        })
    }

    /// Get router statistics
    pub fn get_stats(&self) -> TimeRouterStats {
        let dual_core_stats = self.dual_core.get_stats();
        
        let compression_stats = self.weight_compression.as_ref()
            .map(|ws| ws.get_system_stats());

        let turn_stats = self.turn_restrictions.as_ref()
            .map(|ts| ts.get_system_stats());

        TimeRouterStats {
            dual_core_stats,
            compression_stats,
            turn_stats,
            profiles_configured: self.profile_configs.len(),
        }
    }
}

/// Profile-specific routing configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfileRouteConfig {
    pub profile: TransportProfile,
    pub max_detour_factor: f64,      // Maximum detour allowed (1.5 = 50% longer)
    pub turn_penalty_weight: f64,    // How much to weight turn penalties
    pub avoid_unpaved: bool,
    pub avoid_steps: bool,
    pub max_grade_percent: Option<f64>,
}

impl ProfileRouteConfig {
    pub fn for_car() -> Self {
        Self {
            profile: TransportProfile::Car,
            max_detour_factor: 1.3,
            turn_penalty_weight: 1.0,
            avoid_unpaved: true,
            avoid_steps: true,
            max_grade_percent: Some(15.0),
        }
    }

    pub fn for_bicycle() -> Self {
        Self {
            profile: TransportProfile::Bicycle,
            max_detour_factor: 1.5,
            turn_penalty_weight: 0.5,
            avoid_unpaved: false,
            avoid_steps: true,
            max_grade_percent: Some(8.0),
        }
    }

    pub fn for_pedestrian() -> Self {
        Self {
            profile: TransportProfile::Foot,
            max_detour_factor: 2.0,
            turn_penalty_weight: 0.2,
            avoid_unpaved: false,
            avoid_steps: false,
            max_grade_percent: None,
        }
    }
}

/// Router statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeRouterStats {
    pub dual_core_stats: crate::dual_core::DualCoreStats,
    pub compression_stats: Option<crate::weight_compression::SystemCompressionStats>,
    pub turn_stats: Option<crate::turn_restriction_tables::TurnRestrictionSystemStats>,
    pub profiles_configured: usize,
}

/// HTTP API endpoint handler for /route
pub fn route_endpoint(request: TimeRouteRequest, router: &mut TimeBasedRouter) -> Result<TimeRouteResponse, String> {
    // Validate request
    if request.start_node == request.end_node {
        return Err("Start and end nodes cannot be the same".to_string());
    }

    // Route
    let response = router.route(request);

    // Validate response quality
    if response.route_found && response.route_quality == RouteQuality::Poor {
        // Could implement fallback strategies here
        return Ok(response);
    }

    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dual_core::{TimeEdge, TimeWeight, GraphNode};
    use butterfly_geometry::Point2D;

    fn create_test_router() -> TimeBasedRouter {
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
        use butterfly_geometry::{SnapSkeleton, NavigationGeometry};

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

        TimeBasedRouter::new(dual_core).unwrap()
    }

    #[test]
    fn test_time_route_request_creation() {
        let request = TimeRouteRequest::new(
            TransportProfile::Car,
            NodeId::new(1),
            NodeId::new(2),
        );

        assert_eq!(request.profile, TransportProfile::Car);
        assert_eq!(request.start_node, NodeId::new(1));
        assert_eq!(request.end_node, NodeId::new(2));
        assert!(request.departure_time.is_none());
        assert!(!request.avoid_toll_roads);
    }

    #[test]
    fn test_time_route_request_with_constraints() {
        let request = TimeRouteRequest::new(
            TransportProfile::Car,
            NodeId::new(1),
            NodeId::new(2),
        )
        .with_departure_time(1234567890)
        .with_constraints(true, false, true)
        .with_max_time(3600);

        assert_eq!(request.departure_time, Some(1234567890));
        assert!(request.avoid_toll_roads);
        assert!(!request.avoid_highways);
        assert!(request.avoid_ferries);
        assert_eq!(request.max_route_time_seconds, Some(3600));
    }

    #[test]
    fn test_route_quality_evaluation() {
        assert_eq!(
            RouteQuality::evaluate(60.0, 2000.0, 5.0, 10),
            RouteQuality::Acceptable
        );

        assert_eq!(
            RouteQuality::evaluate(36.0, 2000.0, 1.0, 5),
            RouteQuality::Good
        );

        assert_eq!(
            RouteQuality::evaluate(300.0, 2000.0, 60.0, 50),
            RouteQuality::Poor
        );
    }

    #[test]
    fn test_time_computation_stats() {
        let mut stats = TimeComputationStats::new();
        stats.shard_hits = 90;
        stats.shard_misses = 10;

        assert_eq!(stats.shard_hit_rate(), 0.9);

        let empty_stats = TimeComputationStats::new();
        assert_eq!(empty_stats.shard_hit_rate(), 1.0);
    }

    #[test]
    fn test_time_based_router_creation() {
        let router = create_test_router();
        
        let stats = router.get_stats();
        assert_eq!(stats.profiles_configured, 0); // No profiles configured yet
        assert!(stats.compression_stats.is_none());
        assert!(stats.turn_stats.is_none());
    }

    #[test]
    fn test_basic_time_routing() {
        let mut router = create_test_router();
        
        let request = TimeRouteRequest::new(
            TransportProfile::Car,
            NodeId::new(1),
            NodeId::new(3),
        );

        let response = router.route(request);
        
        // Should find a route (1 -> 2 -> 3)
        assert!(response.route_found);
        assert!(response.total_time_seconds > 0.0);
        assert!(response.total_distance_meters > 0.0);
        assert_eq!(response.edge_sequence.len(), 2);
        assert_eq!(response.node_sequence.len(), 3);
    }

    #[test]
    fn test_time_route_response_methods() {
        let request = TimeRouteRequest::new(
            TransportProfile::Car,
            NodeId::new(1),
            NodeId::new(2),
        ).with_departure_time(1000).with_max_time(120);

        let mut stats = TimeComputationStats::new();
        stats.computation_time_ms = 50;

        let route_result = RouteResult {
            profile: TransportProfile::Car,
            start_node: NodeId::new(1),
            end_node: NodeId::new(2),
            total_time: 100.0,
            total_distance: 2000.0,
            node_path: vec![NodeId::new(1), NodeId::new(2)],
            edge_path: vec![EdgeId(1)],
            geometry: None,
            computation_stats: crate::dijkstra::ComputationStats {
                nodes_explored: 10,
                edges_relaxed: 5,
                computation_time_ms: 50,
                graph_type: crate::dijkstra::GraphType::TimeOnly,
            },
        };

        let response = TimeRouteResponse::found(request, route_result, 10.0, stats);

        assert_eq!(response.average_speed_kmh(), 72.0); // 2km in 100s = 72 km/h
        assert!(response.meets_time_constraint()); // 100s < 120s limit
        assert_eq!(response.estimated_arrival_time, Some(1100)); // 1000 + 100
    }

    #[test]
    fn test_profile_route_configs() {
        let car_config = ProfileRouteConfig::for_car();
        assert_eq!(car_config.profile, TransportProfile::Car);
        assert_eq!(car_config.max_detour_factor, 1.3);
        assert!(car_config.avoid_unpaved);

        let bike_config = ProfileRouteConfig::for_bicycle();
        assert_eq!(bike_config.profile, TransportProfile::Bicycle);
        assert_eq!(bike_config.max_detour_factor, 1.5);
        assert!(!bike_config.avoid_unpaved);

        let pedestrian_config = ProfileRouteConfig::for_pedestrian();
        assert_eq!(pedestrian_config.profile, TransportProfile::Foot);
        assert_eq!(pedestrian_config.max_detour_factor, 2.0);
        assert!(!pedestrian_config.avoid_steps);
    }

    #[test]
    fn test_route_endpoint() {
        let mut router = create_test_router();
        
        let request = TimeRouteRequest::new(
            TransportProfile::Car,
            NodeId::new(1),
            NodeId::new(3),
        );

        let result = route_endpoint(request, &mut router);
        assert!(result.is_ok());

        let response = result.unwrap();
        assert!(response.route_found);

        // Test invalid request (same start/end)
        let invalid_request = TimeRouteRequest::new(
            TransportProfile::Car,
            NodeId::new(1),
            NodeId::new(1),
        );

        let invalid_result = route_endpoint(invalid_request, &mut router);
        assert!(invalid_result.is_err());
    }

    #[test]
    fn test_router_with_extensions() {
        let router = create_test_router();
        
        // Test with weight compression
        let weight_system = crate::weight_compression::WeightCompressionSystem::new();
        let router_with_compression = router.with_weight_compression(weight_system);
        
        let stats = router_with_compression.get_stats();
        assert!(stats.compression_stats.is_some());

        // Test with turn restrictions
        let turn_system = crate::turn_restriction_tables::TurnRestrictionTableSystem::new(100);
        let router_with_turns = router_with_compression.with_turn_restrictions(turn_system);
        
        let stats = router_with_turns.get_stats();
        assert!(stats.turn_stats.is_some());

        // Test with profile config
        let router_with_config = router_with_turns.with_profile_config(
            TransportProfile::Car,
            ProfileRouteConfig::for_car(),
        );
        
        let stats = router_with_config.get_stats();
        assert_eq!(stats.profiles_configured, 1);
    }
}