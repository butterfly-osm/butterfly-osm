//! Adaptive coarsening for topology preservation and efficiency

use std::collections::{HashMap, HashSet};
use serde::{Deserialize, Serialize};
use crate::pbf::OsmPrimitive;

/// Semantic breakpoint detection for routing-critical topology changes
#[derive(Debug, Clone, Default)]
pub struct SemanticBreakpoints {
    /// Cache of way tags that indicate semantic importance
    way_cache: HashMap<i64, SemanticImportance>,
    /// Turn restriction relation anchors
    turn_restriction_nodes: HashSet<i64>,
}

/// Semantic importance flags for routing preservation
#[derive(Debug, Clone, PartialEq)]
pub struct SemanticImportance {
    /// Has name tag (street names are routing-critical)
    pub has_name: bool,
    /// Has ref tag (route references are critical)
    pub has_ref: bool,
    /// Access restrictions present
    pub has_access_restriction: bool,
    /// Speed limit changes
    pub has_speed_limit: bool,
    /// Layer changes (bridges/tunnels)
    pub has_layer_change: bool,
    /// Bridge structure
    pub is_bridge: bool,
    /// Tunnel structure
    pub is_tunnel: bool,
    /// Overall importance score
    pub importance_score: u8,
}

impl Default for SemanticImportance {
    fn default() -> Self {
        Self {
            has_name: false,
            has_ref: false,
            has_access_restriction: false,
            has_speed_limit: false,
            has_layer_change: false,
            is_bridge: false,
            is_tunnel: false,
            importance_score: 0,
        }
    }
}

impl SemanticBreakpoints {
    pub fn new() -> Self {
        Self {
            way_cache: HashMap::new(),
            turn_restriction_nodes: HashSet::new(),
        }
    }

    /// Process OSM primitive to detect semantic breakpoints
    pub fn process_primitive(&mut self, primitive: &OsmPrimitive) {
        match primitive {
            OsmPrimitive::Node { .. } => {
                // Nodes are processed in relation context
            }
            OsmPrimitive::Way { id, tags, .. } => {
                let importance = self.analyze_way_semantics(tags);
                self.way_cache.insert(*id, importance);
            }
            OsmPrimitive::Relation { tags, members, .. } => {
                self.process_relation_semantics(tags, members);
            }
        }
    }

    /// Analyze way tags for semantic importance
    fn analyze_way_semantics(&self, tags: &HashMap<String, String>) -> SemanticImportance {
        let mut importance = SemanticImportance::default();
        let mut score = 0u8;

        // Name detection - street names are routing-critical
        if tags.contains_key("name") || tags.contains_key("name:en") {
            importance.has_name = true;
            score += 3; // High importance for named ways
        }

        // Ref detection - route references (A1, M25, etc.)
        if tags.contains_key("ref") {
            importance.has_ref = true;
            score += 4; // Very high importance for referenced routes
        }

        // Access restrictions
        if self.has_access_restrictions(tags) {
            importance.has_access_restriction = true;
            score += 2;
        }

        // Speed limit detection
        if tags.contains_key("maxspeed") {
            importance.has_speed_limit = true;
            score += 1;
        }

        // Layer changes (bridges/tunnels/underpasses)
        if let Some(layer_str) = tags.get("layer") {
            if layer_str.parse::<i32>().unwrap_or(0) != 0 {
                importance.has_layer_change = true;
                score += 2;
            }
        }

        // Bridge detection
        if tags.get("bridge").map_or(false, |v| v == "yes" || v == "true") {
            importance.is_bridge = true;
            score += 3; // Bridges are routing-critical
        }

        // Tunnel detection
        if tags.get("tunnel").map_or(false, |v| v == "yes" || v == "true") {
            importance.is_tunnel = true;
            score += 3; // Tunnels are routing-critical
        }

        importance.importance_score = score;
        importance
    }

    /// Check for access restrictions that affect routing
    fn has_access_restrictions(&self, tags: &HashMap<String, String>) -> bool {
        let access_tags = [
            "access", "vehicle", "motor_vehicle", "motorcar",
            "foot", "bicycle", "horse", "barrier"
        ];

        for tag in &access_tags {
            if let Some(value) = tags.get(*tag) {
                match value.as_str() {
                    "no" | "private" | "destination" | "permit" | "customers" => return true,
                    _ => {}
                }
            }
        }

        false
    }

    /// Process relations for turn restrictions and other semantic anchors
    fn process_relation_semantics(&mut self, tags: &HashMap<String, String>, members: &[crate::pbf::RelationMember]) {
        // Turn restriction detection
        if let Some(restriction_type) = tags.get("type") {
            if restriction_type == "restriction" {
                self.process_turn_restriction(tags, members);
            }
        }

        // Route relations (bus routes, cycling routes, etc.)
        if let Some(route_type) = tags.get("route") {
            self.process_route_relation(route_type, members);
        }
    }

    /// Process turn restriction relations to mark anchor nodes
    fn process_turn_restriction(&mut self, _tags: &HashMap<String, String>, members: &[crate::pbf::RelationMember]) {
        for member in members {
            if member.role == "via" && member.member_type == crate::pbf::MemberType::Node {
                // Via nodes in turn restrictions are critical routing anchors
                self.turn_restriction_nodes.insert(member.id);
            }
        }
    }

    /// Process route relations for additional semantic importance
    fn process_route_relation(&mut self, _route_type: &str, _members: &[crate::pbf::RelationMember]) {
        // Route relations affect importance of member ways
        // This could be extended to mark route-specific nodes as important
    }

    /// Check if a way has semantic importance
    pub fn is_semantically_important(&self, way_id: i64) -> bool {
        self.way_cache.get(&way_id)
            .map_or(false, |imp| imp.importance_score > 2)
    }

    /// Check if a node is a turn restriction anchor
    pub fn is_turn_restriction_anchor(&self, node_id: i64) -> bool {
        self.turn_restriction_nodes.contains(&node_id)
    }

    /// Get semantic importance for a way
    pub fn get_way_importance(&self, way_id: i64) -> Option<&SemanticImportance> {
        self.way_cache.get(&way_id)
    }

    /// Get all turn restriction anchor nodes
    pub fn get_turn_restriction_anchors(&self) -> &HashSet<i64> {
        &self.turn_restriction_nodes
    }

    /// Clear caches for memory management
    pub fn clear_caches(&mut self) {
        self.way_cache.clear();
        self.turn_restriction_nodes.clear();
    }
}

/// Curvature analysis for geometry-aware vertex retention
#[derive(Debug, Clone)]
pub struct CurvatureAnalyzer {
    /// Threshold for considering angles as straight (degrees)
    straight_angle_threshold: f64,
    /// Minimum arc length for fast-path optimization (meters)
    min_arc_length: f64,
}

impl Default for CurvatureAnalyzer {
    fn default() -> Self {
        Self {
            straight_angle_threshold: 3.0, // <3° is considered straight
            min_arc_length: 50.0, // 50m minimum for fast-path
        }
    }
}

impl CurvatureAnalyzer {
    pub fn new() -> Self {
        Self::default()
    }

    /// Analyze local angles for a way geometry
    pub fn analyze_local_angles(&self, coordinates: &[(f64, f64)]) -> Vec<LocalAngle> {
        let mut angles = Vec::new();

        if coordinates.len() < 3 {
            return angles;
        }

        for i in 1..coordinates.len() - 1 {
            let prev = coordinates[i - 1];
            let curr = coordinates[i];
            let next = coordinates[i + 1];

            let angle = self.calculate_angle(prev, curr, next);
            let importance = self.calculate_angle_importance(angle, curr, prev, next);

            // Check if angle is straight (close to 0 or 180 degrees)
            let is_straight = angle.abs() < self.straight_angle_threshold || 
                             (angle.abs() - 180.0).abs() < self.straight_angle_threshold;
            
            angles.push(LocalAngle {
                position: i,
                coordinates: curr,
                angle_degrees: angle,
                importance_score: importance,
                is_straight,
            });
        }

        angles
    }

    /// Calculate angle at a vertex (in degrees)
    fn calculate_angle(&self, prev: (f64, f64), curr: (f64, f64), next: (f64, f64)) -> f64 {
        let vec1 = (prev.0 - curr.0, prev.1 - curr.1);
        let vec2 = (next.0 - curr.0, next.1 - curr.1);

        let dot_product = vec1.0 * vec2.0 + vec1.1 * vec2.1;
        let cross_product = vec1.0 * vec2.1 - vec1.1 * vec2.0;

        let angle_rad = cross_product.atan2(dot_product);
        angle_rad.to_degrees()
    }

    /// Calculate importance score for an angle
    fn calculate_angle_importance(&self, angle: f64, curr: (f64, f64), prev: (f64, f64), next: (f64, f64)) -> u8 {
        let abs_angle = angle.abs();
        
        // Arc length calculation (approximate)
        let dist1 = self.haversine_distance(prev, curr);
        let dist2 = self.haversine_distance(curr, next);
        let arc_length = dist1 + dist2;

        let mut score = 0u8;

        // Angle-based scoring
        if abs_angle > 90.0 {
            score += 5; // Sharp turns are very important
        } else if abs_angle > 30.0 {
            score += 3; // Moderate turns are important
        } else if abs_angle > 10.0 {
            score += 1; // Slight turns have some importance
        }

        // Arc length consideration
        if arc_length > self.min_arc_length * 2.0 {
            score += 1; // Longer segments get slight boost
        }

        score
    }

    /// Haversine distance calculation (approximate for small distances)
    fn haversine_distance(&self, p1: (f64, f64), p2: (f64, f64)) -> f64 {
        let dlat = (p2.0 - p1.0).to_radians();
        let dlon = (p2.1 - p1.1).to_radians();
        
        let a = (dlat / 2.0).sin().powi(2) + 
                p1.0.to_radians().cos() * p2.0.to_radians().cos() * 
                (dlon / 2.0).sin().powi(2);
        
        let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());
        6371000.0 * c // Earth radius in meters
    }

    /// Check if a segment qualifies for fast-path optimization
    pub fn is_fast_path_eligible(&self, angles: &[LocalAngle], coordinates: &[(f64, f64)]) -> bool {
        // Calculate actual arc length from geometry first
        let mut total_arc_length = 0.0;
        for i in 1..coordinates.len() {
            total_arc_length += self.haversine_distance(coordinates[i-1], coordinates[i]);
        }

        // Arc-length guard: segment must be long enough for fast-path optimization
        if total_arc_length < self.min_arc_length {
            return false;
        }

        // If no angles to analyze, it's a straight line and eligible
        if angles.is_empty() {
            return true;
        }

        // All angles must be straight
        angles.iter().all(|a| a.is_straight)
    }
}

/// Local angle information for curvature analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalAngle {
    /// Position index in way geometry
    pub position: usize,
    /// Geographic coordinates
    pub coordinates: (f64, f64),
    /// Angle in degrees (positive = left turn, negative = right turn)
    pub angle_degrees: f64,
    /// Importance score for retention
    pub importance_score: u8,
    /// Whether this is considered a straight segment
    pub is_straight: bool,
}

/// Cumulative bend analysis for importance scoring
#[derive(Debug, Clone)]
pub struct BendWindow {
    /// Start position in way
    pub start_position: usize,
    /// End position in way
    pub end_position: usize,
    /// Total curvature in this window
    pub total_curvature: f64,
    /// Number of significant bends
    pub bend_count: usize,
    /// Importance score for this window
    pub importance_score: u8,
}

impl CurvatureAnalyzer {
    /// Analyze cumulative bends in sliding windows
    pub fn analyze_bend_windows(&self, angles: &[LocalAngle], window_size: usize) -> Vec<BendWindow> {
        let mut windows = Vec::new();

        if angles.len() < window_size {
            return windows;
        }

        for start in 0..=angles.len() - window_size {
            let end = start + window_size;
            let window_angles = &angles[start..end];

            let total_curvature: f64 = window_angles.iter()
                .map(|a| a.angle_degrees.abs())
                .sum();

            let bend_count = window_angles.iter()
                .filter(|a| !a.is_straight)
                .count();

            let importance_score = self.calculate_window_importance(total_curvature, bend_count);

            windows.push(BendWindow {
                start_position: start,
                end_position: end,
                total_curvature,
                bend_count,
                importance_score,
            });
        }

        windows
    }

    /// Calculate importance score for a bend window
    fn calculate_window_importance(&self, total_curvature: f64, bend_count: usize) -> u8 {
        let mut score = 0u8;

        // Curvature-based scoring
        if total_curvature > 180.0 {
            score += 5; // Very curvy sections
        } else if total_curvature > 90.0 {
            score += 3; // Moderately curvy
        } else if total_curvature > 30.0 {
            score += 1; // Slightly curvy
        }

        // Bend count scoring
        if bend_count > 3 {
            score += 2; // Many bends in window
        } else if bend_count > 1 {
            score += 1; // Some bends
        }

        score
    }
}

/// Node canonicalization with collision-safe coordinate merging
#[derive(Debug, Clone)]
pub struct NodeCanonicalizer {
    /// Grid hash for spatial binning
    grid_hash: HashMap<GridCell, Vec<CanonicalNode>>,
    /// Union-find structure for node merging
    union_find: UnionFind,
    /// Grid resolution in meters
    grid_resolution: f64,
    /// Maximum merge distance in meters
    max_merge_distance: f64,
    /// Canonical ID mapping
    canonical_mapping: HashMap<i64, i64>,
}

/// Grid cell for spatial hashing
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct GridCell {
    x: i64,
    y: i64,
}

/// Canonical node with merged coordinates
#[derive(Debug, Clone)]
struct CanonicalNode {
    /// Original OSM node ID
    original_id: i64,
    /// Canonical coordinates (average of merged nodes)
    canonical_coords: (f64, f64),
    /// All nodes merged into this canonical node
    merged_nodes: Vec<i64>,
    /// Semantic importance flags
    is_semantically_important: bool,
}

/// Union-Find data structure for efficient merging
#[derive(Debug, Clone)]
struct UnionFind {
    parent: HashMap<i64, i64>,
    rank: HashMap<i64, usize>,
}

impl UnionFind {
    fn new() -> Self {
        Self {
            parent: HashMap::new(),
            rank: HashMap::new(),
        }
    }

    fn make_set(&mut self, x: i64) {
        if !self.parent.contains_key(&x) {
            self.parent.insert(x, x);
            self.rank.insert(x, 0);
        }
    }

    fn find(&mut self, x: i64) -> i64 {
        if self.parent[&x] != x {
            let root = self.find(self.parent[&x]);
            self.parent.insert(x, root); // Path compression
            root
        } else {
            x
        }
    }

    fn union(&mut self, x: i64, y: i64) -> bool {
        let root_x = self.find(x);
        let root_y = self.find(y);

        if root_x == root_y {
            return false; // Already in same set
        }

        // Union by rank
        let rank_x = self.rank[&root_x];
        let rank_y = self.rank[&root_y];

        if rank_x < rank_y {
            self.parent.insert(root_x, root_y);
        } else if rank_x > rank_y {
            self.parent.insert(root_y, root_x);
        } else {
            self.parent.insert(root_y, root_x);
            self.rank.insert(root_x, rank_x + 1);
        }

        true
    }
}

impl Default for NodeCanonicalizer {
    fn default() -> Self {
        Self {
            grid_hash: HashMap::new(),
            union_find: UnionFind::new(),
            grid_resolution: 1.0, // 1 meter grid cells
            max_merge_distance: 5.0, // 5 meter merge threshold
            canonical_mapping: HashMap::new(),
        }
    }
}

impl NodeCanonicalizer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_resolution(grid_resolution: f64, max_merge_distance: f64) -> Self {
        Self {
            grid_hash: HashMap::new(),
            union_find: UnionFind::new(),
            grid_resolution,
            max_merge_distance,
            canonical_mapping: HashMap::new(),
        }
    }

    /// Add a node for canonicalization
    pub fn add_node(&mut self, node_id: i64, coords: (f64, f64), is_semantically_important: bool) {
        self.union_find.make_set(node_id);
        
        let grid_cell = self.coords_to_grid(coords);
        
        // Find merge candidate without borrowing
        let mut merge_target: Option<i64> = None;
        
        // Check current and adjacent cells for merge candidates
        for dx in -1..=1 {
            for dy in -1..=1 {
                let check_cell = GridCell {
                    x: grid_cell.x + dx,
                    y: grid_cell.y + dy,
                };

                if let Some(nodes) = self.grid_hash.get(&check_cell) {
                    for node in nodes {
                        let distance = self.haversine_distance(coords, node.canonical_coords);
                        if distance <= self.max_merge_distance 
                            && self.can_merge_nodes(coords, node.canonical_coords, is_semantically_important, node.is_semantically_important) {
                            merge_target = Some(node.original_id);
                            break;
                        }
                    }
                    if merge_target.is_some() {
                        break;
                    }
                }
            }
            if merge_target.is_some() {
                break;
            }
        }

        if let Some(target_id) = merge_target {
            // Merge with existing canonical node
            self.union_find.union(node_id, target_id);
            self.update_canonical_node(&grid_cell, target_id, node_id, coords);
        } else {
            // Create new canonical node
            let canonical_node = CanonicalNode {
                original_id: node_id,
                canonical_coords: coords,
                merged_nodes: vec![node_id],
                is_semantically_important,
            };

            self.grid_hash.entry(grid_cell)
                .or_insert_with(Vec::new)
                .push(canonical_node);
        }
    }

    /// Convert coordinates to grid cell
    fn coords_to_grid(&self, coords: (f64, f64)) -> GridCell {
        GridCell {
            x: (coords.0 / self.grid_resolution).floor() as i64,
            y: (coords.1 / self.grid_resolution).floor() as i64,
        }
    }


    /// Check if two nodes can be safely merged
    fn can_merge_nodes(&self, coords1: (f64, f64), coords2: (f64, f64), important1: bool, important2: bool) -> bool {
        let distance = self.haversine_distance(coords1, coords2);
        
        // Distance check
        if distance > self.max_merge_distance {
            return false;
        }

        // Illegal merge guards - never merge semantically important nodes with non-important ones
        if important1 != important2 {
            return false;
        }

        // Additional safety checks could go here (e.g., topology preservation)
        true
    }

    /// Update canonical node with merged coordinates
    fn update_canonical_node(&mut self, grid_cell: &GridCell, canonical_id: i64, new_node_id: i64, new_coords: (f64, f64)) {
        if let Some(nodes) = self.grid_hash.get_mut(grid_cell) {
            for node in nodes.iter_mut() {
                if node.original_id == canonical_id {
                    // Recalculate canonical coordinates as average
                    let total_nodes = node.merged_nodes.len() as f64;
                    let new_x = (node.canonical_coords.0 * total_nodes + new_coords.0) / (total_nodes + 1.0);
                    let new_y = (node.canonical_coords.1 * total_nodes + new_coords.1) / (total_nodes + 1.0);
                    
                    node.canonical_coords = (new_x, new_y);
                    node.merged_nodes.push(new_node_id);
                    break;
                }
            }
        }
    }

    /// Haversine distance calculation
    fn haversine_distance(&self, p1: (f64, f64), p2: (f64, f64)) -> f64 {
        let dlat = (p2.0 - p1.0).to_radians();
        let dlon = (p2.1 - p1.1).to_radians();
        
        let a = (dlat / 2.0).sin().powi(2) + 
                p1.0.to_radians().cos() * p2.0.to_radians().cos() * 
                (dlon / 2.0).sin().powi(2);
        
        let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());
        6371000.0 * c // Earth radius in meters
    }

    /// Finalize canonicalization and build mapping
    pub fn finalize(&mut self) {
        self.canonical_mapping.clear();

        for nodes in self.grid_hash.values() {
            for canonical_node in nodes {
                let canonical_id = self.union_find.find(canonical_node.original_id);
                for &merged_id in &canonical_node.merged_nodes {
                    self.canonical_mapping.insert(merged_id, canonical_id);
                }
            }
        }
    }

    /// Get canonical ID for a node
    pub fn get_canonical_id(&mut self, node_id: i64) -> Option<i64> {
        if let Some(&canonical_id) = self.canonical_mapping.get(&node_id) {
            Some(canonical_id)
        } else {
            // Fallback to union-find
            if self.union_find.parent.contains_key(&node_id) {
                Some(self.union_find.find(node_id))
            } else {
                None
            }
        }
    }

    /// Get canonical coordinates for a node
    pub fn get_canonical_coords(&self, node_id: i64) -> Option<(f64, f64)> {
        // Find canonical node in grid hash
        for nodes in self.grid_hash.values() {
            for canonical_node in nodes {
                if canonical_node.merged_nodes.contains(&node_id) {
                    return Some(canonical_node.canonical_coords);
                }
            }
        }
        None
    }

    /// Get statistics about canonicalization
    pub fn get_stats(&self) -> CanonicalStats {
        let total_nodes: usize = self.grid_hash.values()
            .flat_map(|nodes| nodes.iter())
            .map(|node| node.merged_nodes.len())
            .sum();

        let canonical_nodes: usize = self.grid_hash.values()
            .map(|nodes| nodes.len())
            .sum();

        let merged_count = total_nodes - canonical_nodes;

        CanonicalStats {
            total_original_nodes: total_nodes,
            canonical_nodes,
            merged_nodes: merged_count,
            compression_ratio: if total_nodes > 0 { 
                canonical_nodes as f64 / total_nodes as f64 
            } else { 
                1.0 
            },
        }
    }

    /// Clear all data for memory management
    pub fn clear(&mut self) {
        self.grid_hash.clear();
        self.union_find = UnionFind::new();
        self.canonical_mapping.clear();
    }
}

/// Statistics about node canonicalization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CanonicalStats {
    pub total_original_nodes: usize,
    pub canonical_nodes: usize,
    pub merged_nodes: usize,
    pub compression_ratio: f64,
}

/// Policy smoothing for tile boundary consistency
#[derive(Debug, Clone)]
pub struct PolicySmoother {
    /// Tile grid for coarsening policies
    tile_grid: HashMap<TileCoord, CoarseningPolicy>,
    /// Tile size in meters (aligned with telemetry)
    tile_size: f64,
    /// Smoothing window size (3x3 = 1)
    window_radius: usize,
}

/// Tile coordinate for policy grid
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TileCoord {
    x: i32,
    y: i32,
}

/// Coarsening policy for a tile
#[derive(Debug, Clone)]
pub struct CoarseningPolicy {
    /// Semantic importance threshold
    pub semantic_threshold: f64,
    /// Curvature importance threshold
    pub curvature_threshold: f64,
    /// Maximum merge distance for canonicalization
    pub merge_distance: f64,
    /// Importance score for this tile
    pub tile_importance: f64,
    /// Number of nodes in this tile
    pub node_count: usize,
}

impl Default for CoarseningPolicy {
    fn default() -> Self {
        Self {
            semantic_threshold: 2.0,
            curvature_threshold: 10.0, // degrees
            merge_distance: 5.0, // meters
            tile_importance: 1.0,
            node_count: 0,
        }
    }
}

impl Default for PolicySmoother {
    fn default() -> Self {
        Self::new(125.0) // Default to telemetry tile size
    }
}

impl PolicySmoother {
    pub fn new(tile_size: f64) -> Self {
        Self {
            tile_grid: HashMap::new(),
            tile_size,
            window_radius: 1, // 3x3 window
        }
    }

    /// Add coarsening data for a tile
    pub fn add_tile_data(&mut self, lat: f64, lon: f64, semantic_score: f64, curvature_score: f64, node_count: usize) {
        let tile_coord = self.coords_to_tile(lat, lon);
        
        let policy = CoarseningPolicy {
            semantic_threshold: semantic_score,
            curvature_threshold: curvature_score,
            merge_distance: 5.0,
            tile_importance: (semantic_score + curvature_score) / 2.0,
            node_count,
        };

        self.tile_grid.insert(tile_coord, policy);
    }

    /// Convert lat/lon to tile coordinates
    fn coords_to_tile(&self, lat: f64, lon: f64) -> TileCoord {
        // Convert to meters using Web Mercator approximation
        let x_meters = lon * 111_320.0; // Approximate meters per degree longitude
        let y_meters = lat * 110_540.0; // Approximate meters per degree latitude
        
        TileCoord {
            x: (x_meters / self.tile_size).floor() as i32,
            y: (y_meters / self.tile_size).floor() as i32,
        }
    }

    /// Perform 3x3 median smoothing on coarsening policies
    pub fn smooth_policies(&mut self) {
        let mut smoothed_grid = HashMap::new();

        for (tile_coord, _policy) in &self.tile_grid {
            let smoothed_policy = self.calculate_smoothed_policy(tile_coord);
            smoothed_grid.insert(tile_coord.clone(), smoothed_policy);
        }

        self.tile_grid = smoothed_grid;
    }

    /// Calculate smoothed policy using 3x3 median filter
    fn calculate_smoothed_policy(&self, center: &TileCoord) -> CoarseningPolicy {
        let mut semantic_values = Vec::new();
        let mut curvature_values = Vec::new();
        let mut importance_values = Vec::new();
        let mut node_counts = Vec::new();

        // Collect values from 3x3 neighborhood
        for dx in -(self.window_radius as i32)..=(self.window_radius as i32) {
            for dy in -(self.window_radius as i32)..=(self.window_radius as i32) {
                let neighbor = TileCoord {
                    x: center.x + dx,
                    y: center.y + dy,
                };

                if let Some(policy) = self.tile_grid.get(&neighbor) {
                    semantic_values.push(policy.semantic_threshold);
                    curvature_values.push(policy.curvature_threshold);
                    importance_values.push(policy.tile_importance);
                    node_counts.push(policy.node_count);
                }
            }
        }

        // Calculate medians
        let semantic_median = Self::calculate_median(&mut semantic_values);
        let curvature_median = Self::calculate_median(&mut curvature_values);
        let importance_median = Self::calculate_median(&mut importance_values);
        let node_count_median = Self::calculate_median_usize(&mut node_counts);

        CoarseningPolicy {
            semantic_threshold: semantic_median,
            curvature_threshold: curvature_median,
            merge_distance: 5.0, // Keep constant
            tile_importance: importance_median,
            node_count: node_count_median,
        }
    }

    /// Calculate median of f64 values
    fn calculate_median(values: &mut [f64]) -> f64 {
        if values.is_empty() {
            return 0.0;
        }

        values.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let mid = values.len() / 2;

        if values.len() % 2 == 0 {
            (values[mid - 1] + values[mid]) / 2.0
        } else {
            values[mid]
        }
    }

    /// Calculate median of usize values
    fn calculate_median_usize(values: &mut [usize]) -> usize {
        if values.is_empty() {
            return 0;
        }

        values.sort();
        let mid = values.len() / 2;

        if values.len() % 2 == 0 {
            (values[mid - 1] + values[mid]) / 2
        } else {
            values[mid]
        }
    }

    /// Get policy for a tile
    pub fn get_policy(&self, lat: f64, lon: f64) -> Option<&CoarseningPolicy> {
        let tile_coord = self.coords_to_tile(lat, lon);
        self.tile_grid.get(&tile_coord)
    }

    /// Generate coarsen.map artifact
    pub fn generate_coarsen_map(&self) -> Result<Vec<u8>, std::io::Error> {
        use std::io::{Cursor, Write};

        let mut buffer = Cursor::new(Vec::new());

        // Write header
        buffer.write_all(b"COARSEN_MAP_V1\n")?;
        buffer.write_all(&(self.tile_grid.len() as u32).to_le_bytes())?;
        buffer.write_all(&(self.tile_size as f32).to_le_bytes())?;

        // Write tile policies
        for (coord, policy) in &self.tile_grid {
            buffer.write_all(&coord.x.to_le_bytes())?;
            buffer.write_all(&coord.y.to_le_bytes())?;
            buffer.write_all(&(policy.semantic_threshold as f32).to_le_bytes())?;
            buffer.write_all(&(policy.curvature_threshold as f32).to_le_bytes())?;
            buffer.write_all(&(policy.merge_distance as f32).to_le_bytes())?;
            buffer.write_all(&(policy.tile_importance as f32).to_le_bytes())?;
            buffer.write_all(&(policy.node_count as u32).to_le_bytes())?;
        }

        Ok(buffer.into_inner())
    }

    /// Generate debug heatmap as JSON
    pub fn generate_debug_heatmap(&self) -> serde_json::Value {
        let mut heatmap_data = Vec::new();

        for (coord, policy) in &self.tile_grid {
            let tile_data = serde_json::json!({
                "x": coord.x,
                "y": coord.y,
                "semantic_threshold": policy.semantic_threshold,
                "curvature_threshold": policy.curvature_threshold,
                "tile_importance": policy.tile_importance,
                "node_count": policy.node_count
            });
            heatmap_data.push(tile_data);
        }

        serde_json::json!({
            "type": "coarsening_heatmap",
            "tile_size_meters": self.tile_size,
            "total_tiles": self.tile_grid.len(),
            "tiles": heatmap_data
        })
    }

    /// Clear policy grid
    pub fn clear(&mut self) {
        self.tile_grid.clear();
    }
}

/// Node mapping for canonical ID tracking
#[derive(Debug, Clone, Default)]
pub struct NodeMapper {
    /// Original ID to canonical ID mapping
    mapping: HashMap<i64, i64>,
    /// Reverse mapping for lookups
    reverse_mapping: HashMap<i64, Vec<i64>>,
}

impl NodeMapper {
    pub fn new() -> Self {
        Self {
            mapping: HashMap::new(),
            reverse_mapping: HashMap::new(),
        }
    }

    /// Add node mapping
    pub fn add_mapping(&mut self, original_id: i64, canonical_id: i64) {
        self.mapping.insert(original_id, canonical_id);
        self.reverse_mapping.entry(canonical_id)
            .or_insert_with(Vec::new)
            .push(original_id);
    }

    /// Get canonical ID for original node
    pub fn get_canonical(&self, original_id: i64) -> Option<i64> {
        self.mapping.get(&original_id).copied()
    }

    /// Get all original IDs for canonical node
    pub fn get_originals(&self, canonical_id: i64) -> Vec<i64> {
        self.reverse_mapping.get(&canonical_id)
            .cloned()
            .unwrap_or_default()
    }

    /// Generate node_map.bin artifact
    pub fn generate_node_map_bin(&self) -> Result<Vec<u8>, std::io::Error> {
        use std::io::{Cursor, Write};

        let mut buffer = Cursor::new(Vec::new());

        // Write header
        buffer.write_all(b"NODE_MAP_V1\n")?;
        buffer.write_all(&(self.mapping.len() as u32).to_le_bytes())?;

        // Write mappings
        for (&original_id, &canonical_id) in &self.mapping {
            buffer.write_all(&original_id.to_le_bytes())?;
            buffer.write_all(&canonical_id.to_le_bytes())?;
        }

        Ok(buffer.into_inner())
    }

    /// Get mapping statistics
    pub fn get_stats(&self) -> NodeMappingStats {
        let total_original = self.mapping.len();
        let unique_canonical = self.reverse_mapping.len();
        let merged_count = total_original - unique_canonical;

        NodeMappingStats {
            total_original_nodes: total_original,
            unique_canonical_nodes: unique_canonical,
            merged_nodes: merged_count,
            compression_ratio: if total_original > 0 {
                unique_canonical as f64 / total_original as f64
            } else {
                1.0
            },
        }
    }

    /// Clear all mappings
    pub fn clear(&mut self) {
        self.mapping.clear();
        self.reverse_mapping.clear();
    }
}

/// Statistics about node mapping
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeMappingStats {
    pub total_original_nodes: usize,
    pub unique_canonical_nodes: usize,
    pub merged_nodes: usize,
    pub compression_ratio: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_semantic_importance_detection() {
        let breakpoints = SemanticBreakpoints::new();
        
        // Test way with name and ref
        let mut tags = HashMap::new();
        tags.insert("name".to_string(), "Main Street".to_string());
        tags.insert("ref".to_string(), "A1".to_string());
        tags.insert("bridge".to_string(), "yes".to_string());
        
        let importance = breakpoints.analyze_way_semantics(&tags);
        
        assert!(importance.has_name);
        assert!(importance.has_ref);
        assert!(importance.is_bridge);
        assert!(importance.importance_score > 5);
    }

    #[test]
    fn test_access_restrictions() {
        let breakpoints = SemanticBreakpoints::new();
        
        let mut tags = HashMap::new();
        tags.insert("access".to_string(), "private".to_string());
        
        assert!(breakpoints.has_access_restrictions(&tags));
        
        tags.clear();
        tags.insert("motor_vehicle".to_string(), "no".to_string());
        
        assert!(breakpoints.has_access_restrictions(&tags));
    }

    #[test]
    fn test_angle_calculation() {
        let analyzer = CurvatureAnalyzer::new();
        
        // Straight line - should have 180 degree angle (straight through)
        let coords = vec![(0.0, 0.0), (0.0, 1.0), (0.0, 2.0)];
        let angles = analyzer.analyze_local_angles(&coords);
        
        assert_eq!(angles.len(), 1);
        // The angle calculation returns the turn angle. For a straight line,
        // it should be close to 180 degrees (straight through) or close to 0
        let angle_abs = angles[0].angle_degrees.abs();
        assert!(angle_abs < 5.0 || (angle_abs - 180.0).abs() < 5.0);
        assert!(angles[0].is_straight);
        
        // 90-degree turn
        let coords = vec![(0.0, 0.0), (0.0, 1.0), (1.0, 1.0)];
        let angles = analyzer.analyze_local_angles(&coords);
        
        assert_eq!(angles.len(), 1);
        assert!((angles[0].angle_degrees.abs() - 90.0).abs() < 15.0);
        assert!(!angles[0].is_straight);
    }

    #[test]
    fn test_fast_path_eligibility() {
        let analyzer = CurvatureAnalyzer::new();
        
        // Long straight segment should be fast-path eligible
        let coords = vec![(0.0, 0.0), (0.0, 1.0), (0.0, 2.0)]; // ~222km total
        let straight_angles = vec![
            LocalAngle {
                position: 1,
                coordinates: (0.0, 1.0),
                angle_degrees: 180.0, // Straight line
                importance_score: 0,
                is_straight: true,
            }
        ];
        
        assert!(analyzer.is_fast_path_eligible(&straight_angles, &coords));
        
        // Sharp angles should not be fast-path eligible
        let sharp_coords = vec![(0.0, 0.0), (0.0, 1.0), (1.0, 1.0)];
        let sharp_angles = vec![
            LocalAngle {
                position: 1,
                coordinates: (0.0, 1.0),
                angle_degrees: 90.0,
                importance_score: 3,
                is_straight: false,
            }
        ];
        
        assert!(!analyzer.is_fast_path_eligible(&sharp_angles, &sharp_coords));
        
        // Short straight segment should not be fast-path eligible  
        let short_coords = vec![(0.0, 0.0), (0.0, 0.0001)]; // ~11m
        let short_straight_angles = vec![];
        
        assert!(!analyzer.is_fast_path_eligible(&short_straight_angles, &short_coords));
    }

    #[test]
    fn test_node_canonicalization() {
        let mut canonicalizer = NodeCanonicalizer::new();
        
        // Add two nodes close together
        canonicalizer.add_node(1, (52.5200, 13.4050), false);
        canonicalizer.add_node(2, (52.5201, 13.4051), false); // ~15m apart
        
        canonicalizer.finalize();
        
        // Should not merge (distance > 5m threshold)
        let canonical1 = canonicalizer.get_canonical_id(1).unwrap();
        let canonical2 = canonicalizer.get_canonical_id(2).unwrap();
        assert_ne!(canonical1, canonical2);
        
        // Add two nodes very close together
        canonicalizer.add_node(3, (52.5200, 13.4050), false);
        canonicalizer.add_node(4, (52.5200, 13.4050), false); // Same coordinates
        
        canonicalizer.finalize();
        
        // Should merge
        let canonical3 = canonicalizer.get_canonical_id(3).unwrap();
        let canonical4 = canonicalizer.get_canonical_id(4).unwrap();
        assert_eq!(canonical3, canonical4);
    }

    #[test]
    fn test_union_find() {
        let mut uf = UnionFind::new();
        
        uf.make_set(1);
        uf.make_set(2);
        uf.make_set(3);
        
        assert_eq!(uf.find(1), 1);
        assert_eq!(uf.find(2), 2);
        
        uf.union(1, 2);
        assert_eq!(uf.find(1), uf.find(2));
        assert_ne!(uf.find(1), uf.find(3));
        
        uf.union(2, 3);
        assert_eq!(uf.find(1), uf.find(3));
    }
}