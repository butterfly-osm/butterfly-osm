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
#[derive(Debug, Clone, PartialEq, Default)]
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
        if tags.get("bridge").is_some_and(|v| v == "yes" || v == "true") {
            importance.is_bridge = true;
            score += 3; // Bridges are routing-critical
        }

        // Tunnel detection
        if tags.get("tunnel").is_some_and(|v| v == "yes" || v == "true") {
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
            .is_some_and(|imp| imp.importance_score > 2)
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
        if let std::collections::hash_map::Entry::Vacant(e) = self.parent.entry(x) {
            e.insert(x);
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
                .or_default()
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

        for tile_coord in self.tile_grid.keys() {
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
            .or_default()
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

// ==== M3 - Super-Edge Construction ====

/// M3.1 - Canonical adjacency lists for graph topology
#[derive(Debug, Clone, Default)]
pub struct CanonicalAdjacency {
    /// Adjacency lists: canonical_node_id -> set of neighboring canonical nodes
    adjacency_lists: HashMap<i64, HashSet<i64>>,
    /// Edge details: (from_canonical, to_canonical) -> edge info
    edge_details: HashMap<(i64, i64), EdgeInfo>,
    /// Neighbor tracking for efficient lookups
    neighbor_index: HashMap<i64, Vec<i64>>,
}

/// Edge information for canonical graph
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeInfo {
    /// Original way IDs that contributed to this edge
    pub original_way_ids: Vec<i64>,
    /// Edge length in meters
    pub length_meters: f64,
    /// Whether this edge connects semantically important nodes
    pub is_semantically_important: bool,
    /// Highway class for routing
    pub highway_class: String,
    /// Access restrictions
    pub access_restrictions: Vec<String>,
    /// Geometry (for non-collapsed edges)
    pub geometry: Vec<(f64, f64)>,
}

impl CanonicalAdjacency {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add an edge between canonical nodes
    pub fn add_edge(&mut self, from_canonical: i64, to_canonical: i64, edge_info: EdgeInfo) {
        // Add to adjacency lists (bidirectional)
        self.adjacency_lists.entry(from_canonical)
            .or_default()
            .insert(to_canonical);
        self.adjacency_lists.entry(to_canonical)
            .or_default()
            .insert(from_canonical);

        // Store edge details
        self.edge_details.insert((from_canonical, to_canonical), edge_info.clone());
        self.edge_details.insert((to_canonical, from_canonical), edge_info);

        // Update neighbor index
        self.neighbor_index.entry(from_canonical)
            .or_default()
            .push(to_canonical);
        self.neighbor_index.entry(to_canonical)
            .or_default()
            .push(from_canonical);
    }

    /// Get neighbors of a canonical node
    pub fn get_neighbors(&self, canonical_node: i64) -> Vec<i64> {
        self.neighbor_index.get(&canonical_node)
            .cloned()
            .unwrap_or_default()
    }

    /// Get edge information
    pub fn get_edge_info(&self, from: i64, to: i64) -> Option<&EdgeInfo> {
        self.edge_details.get(&(from, to))
    }

    /// Get degree of a canonical node
    pub fn get_degree(&self, canonical_node: i64) -> usize {
        self.adjacency_lists.get(&canonical_node)
            .map(|neighbors| neighbors.len())
            .unwrap_or(0)
    }

    /// Get all canonical nodes
    pub fn get_all_nodes(&self) -> Vec<i64> {
        self.adjacency_lists.keys().copied().collect()
    }

    /// Build adjacency from way data and canonical mapping
    pub fn build_from_ways(&mut self, ways: &[(i64, Vec<i64>, HashMap<String, String>)], node_canonicalizer: &NodeCanonicalizer) {
        for (way_id, node_refs, tags) in ways {
            if node_refs.len() < 2 {
                continue; // Skip ways with insufficient nodes
            }

            // Convert way nodes to canonical nodes
            let mut canonical_nodes = Vec::new();
            for &node_id in node_refs {
                if let Some(canonical_coords) = node_canonicalizer.get_canonical_coords(node_id) {
                    if let Some(canonical_id) = node_canonicalizer.get_canonical_coords(node_id) {
                        canonical_nodes.push((node_id, canonical_id.0 as i64)); // Use lat as canonical ID approximation
                    }
                }
            }

            if canonical_nodes.len() < 2 {
                continue; // Skip if we can't resolve canonical nodes
            }

            // Create edges between consecutive canonical nodes
            for i in 0..canonical_nodes.len() - 1 {
                let (_, from_canonical) = canonical_nodes[i];
                let (_, to_canonical) = canonical_nodes[i + 1];

                if from_canonical != to_canonical {
                    let edge_info = EdgeInfo {
                        original_way_ids: vec![*way_id],
                        length_meters: 100.0, // Placeholder - would calculate from geometry
                        is_semantically_important: self.is_semantically_important_way(tags),
                        highway_class: tags.get("highway").map_or("unclassified", |v| v).to_string(),
                        access_restrictions: self.extract_access_restrictions(tags),
                        geometry: vec![], // Placeholder - would include intermediate points
                    };

                    self.add_edge(from_canonical, to_canonical, edge_info);
                }
            }
        }
    }

    /// Check if way is semantically important
    fn is_semantically_important_way(&self, tags: &HashMap<String, String>) -> bool {
        tags.contains_key("name") || 
        tags.contains_key("ref") ||
        tags.get("bridge").is_some_and(|v| v == "yes") ||
        tags.get("tunnel").is_some_and(|v| v == "yes")
    }

    /// Extract access restrictions from tags
    fn extract_access_restrictions(&self, tags: &HashMap<String, String>) -> Vec<String> {
        let mut restrictions = Vec::new();
        
        let access_tags = ["access", "vehicle", "motor_vehicle", "bicycle", "foot"];
        for tag in &access_tags {
            if let Some(value) = tags.get(*tag) {
                if matches!(value.as_str(), "no" | "private" | "destination") {
                    restrictions.push(format!("{}={}", tag, value));
                }
            }
        }
        
        restrictions
    }

    /// Clear all data
    pub fn clear(&mut self) {
        self.adjacency_lists.clear();
        self.edge_details.clear();
        self.neighbor_index.clear();
    }
}

/// M3.2 - Degree-2 collapse for super-edge construction
#[derive(Debug, Clone, Default)]
pub struct SuperEdgeConstructor {
    /// Super-edges: direct connections skipping degree-2 nodes
    super_edges: HashMap<(i64, i64), SuperEdge>,
    /// Policy for controlling collapse behavior
    collapse_policy: CollapsePolicy,
}

/// Super-edge spanning multiple original edges
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SuperEdge {
    /// Start canonical node
    pub start_node: i64,
    /// End canonical node  
    pub end_node: i64,
    /// All intermediate nodes that were collapsed
    pub collapsed_nodes: Vec<i64>,
    /// Total length in meters
    pub total_length: f64,
    /// Combined geometry with segment guards
    pub geometry: Vec<(f64, f64)>,
    /// Original way IDs
    pub original_ways: Vec<i64>,
    /// Whether any segment is semantically important
    pub has_semantic_importance: bool,
    /// Highway class (most restrictive)
    pub highway_class: String,
    /// Combined access restrictions
    pub access_restrictions: Vec<String>,
    /// Segment guard information
    pub segment_guards: Vec<SegmentGuard>,
}

/// Segment guard for memory safety (M3.2 requirement)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentGuard {
    /// Position in geometry where guard was inserted
    pub position: usize,
    /// Reason for guard insertion
    pub reason: SegmentGuardReason,
    /// Distance/count at guard point
    pub threshold_value: f64,
}

/// Reasons for inserting segment guards
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SegmentGuardReason {
    /// Exceeded 4,096 point limit
    PointLimit,
    /// Exceeded 1km length limit  
    LengthLimit,
    /// Semantic importance boundary
    SemanticBoundary,
    /// Tile boundary crossing
    TileBoundary,
}

/// Policy for controlling degree-2 collapse
#[derive(Debug, Clone)]
pub struct CollapsePolicy {
    /// Maximum points before splitting (for M5 geometry memory safety)
    max_points_per_segment: usize,
    /// Maximum length before splitting (meters)
    max_length_per_segment: f64,
    /// Whether to preserve semantic boundaries
    preserve_semantic_boundaries: bool,
    /// Whether to preserve tile boundaries
    preserve_tile_boundaries: bool,
}

impl Default for CollapsePolicy {
    fn default() -> Self {
        Self {
            max_points_per_segment: 4096, // M3.2 requirement
            max_length_per_segment: 1000.0, // 1km limit from M3.2
            preserve_semantic_boundaries: true,
            preserve_tile_boundaries: true,
        }
    }
}

impl SuperEdgeConstructor {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_policy(policy: CollapsePolicy) -> Self {
        Self {
            super_edges: HashMap::new(),
            collapse_policy: policy,
        }
    }

    /// Construct super-edges by collapsing degree-2 nodes
    pub fn construct_super_edges(&mut self, adjacency: &CanonicalAdjacency, semantic_breakpoints: &SemanticBreakpoints) {
        let all_nodes = adjacency.get_all_nodes();
        let mut visited = HashSet::new();

        for start_node in all_nodes {
            if visited.contains(&start_node) {
                continue;
            }

            let degree = adjacency.get_degree(start_node);
            
            // Only start collapse from non-degree-2 nodes or end nodes
            if degree == 2 {
                continue;
            }

            // Find all chains starting from this node
            let neighbors = adjacency.get_neighbors(start_node);
            for neighbor in neighbors {
                if visited.contains(&neighbor) {
                    continue;
                }

                let super_edge = self.collapse_chain(start_node, neighbor, adjacency, semantic_breakpoints, &mut visited);
                if let Some(edge) = super_edge {
                    self.super_edges.insert((edge.start_node, edge.end_node), edge);
                }
            }
        }
    }

    /// Collapse a chain of degree-2 nodes into a super-edge
    fn collapse_chain(&self, start: i64, first_neighbor: i64, adjacency: &CanonicalAdjacency, _semantic_breakpoints: &SemanticBreakpoints, visited: &mut HashSet<i64>) -> Option<SuperEdge> {
        let mut current = first_neighbor;
        let mut previous = start;
        let mut collapsed_nodes = Vec::new();
        let mut total_length = 0.0;
        let mut geometry = Vec::new();
        let mut original_ways = Vec::new();
        let mut access_restrictions = Vec::new();
        let mut has_semantic_importance = false;
        let mut highway_class = String::new();
        let mut segment_guards = Vec::new();

        // Add start geometry
        if let Some(edge_info) = adjacency.get_edge_info(start, current) {
            geometry.extend_from_slice(&edge_info.geometry);
            total_length += edge_info.length_meters;
            original_ways.extend_from_slice(&edge_info.original_way_ids);
            access_restrictions.extend_from_slice(&edge_info.access_restrictions);
            has_semantic_importance |= edge_info.is_semantically_important;
            if highway_class.is_empty() {
                highway_class = edge_info.highway_class.clone();
            }
        }

        // Follow the chain
        loop {
            visited.insert(current);
            let neighbors = adjacency.get_neighbors(current);
            
            // Stop if not degree-2
            if neighbors.len() != 2 {
                break;
            }

            // Stop if semantically important and policy requires preservation
            if self.collapse_policy.preserve_semantic_boundaries {
                // Check if current node is semantically important
                let is_important = false; // Would check against original node semantics
                if is_important {
                    segment_guards.push(SegmentGuard {
                        position: geometry.len(),
                        reason: SegmentGuardReason::SemanticBoundary,
                        threshold_value: 0.0,
                    });
                    break;
                }
            }

            // Find next node (not the previous one)
            let next = neighbors.iter()
                .find(|&&n| n != previous)
                .copied()?;

            // Get edge to next node
            if let Some(edge_info) = adjacency.get_edge_info(current, next) {
                // Check segment guards before adding
                if geometry.len() + edge_info.geometry.len() > self.collapse_policy.max_points_per_segment {
                    segment_guards.push(SegmentGuard {
                        position: geometry.len(),
                        reason: SegmentGuardReason::PointLimit,
                        threshold_value: geometry.len() as f64,
                    });
                    break;
                }

                if total_length + edge_info.length_meters > self.collapse_policy.max_length_per_segment {
                    segment_guards.push(SegmentGuard {
                        position: geometry.len(),
                        reason: SegmentGuardReason::LengthLimit,
                        threshold_value: total_length + edge_info.length_meters,
                    });
                    break;
                }

                // Add edge data
                geometry.extend_from_slice(&edge_info.geometry);
                total_length += edge_info.length_meters;
                original_ways.extend_from_slice(&edge_info.original_way_ids);
                access_restrictions.extend_from_slice(&edge_info.access_restrictions);
                has_semantic_importance |= edge_info.is_semantically_important;
            }

            collapsed_nodes.push(current);
            previous = current;
            current = next;
        }

        // Create super-edge if we collapsed at least one node
        if !collapsed_nodes.is_empty() {
            Some(SuperEdge {
                start_node: start,
                end_node: current,
                collapsed_nodes,
                total_length,
                geometry,
                original_ways,
                has_semantic_importance,
                highway_class,
                access_restrictions,
                segment_guards,
            })
        } else {
            None
        }
    }

    /// Get all super-edges
    pub fn get_super_edges(&self) -> Vec<&SuperEdge> {
        self.super_edges.values().collect()
    }

    /// Get super-edge between two nodes
    pub fn get_super_edge(&self, start: i64, end: i64) -> Option<&SuperEdge> {
        self.super_edges.get(&(start, end))
            .or_else(|| self.super_edges.get(&(end, start)))
    }

    /// Clear all super-edges
    pub fn clear(&mut self) {
        self.super_edges.clear();
    }
}

/// M3.3 - Border reconciliation for cross-tile consistency
#[derive(Debug, Clone, Default)]
pub struct BorderReconciliation {
    /// Border edge tracking: tile boundary -> edges crossing it
    border_edges: HashMap<TileBoundary, Vec<BorderEdge>>,
    /// Global consistency mapping
    global_mapping: HashMap<(i64, i64), i64>, // (local_node, tile_id) -> global_node
}

/// Tile boundary identifier
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TileBoundary {
    /// Tile coordinates  
    pub tile_x: i32,
    pub tile_y: i32,
    /// Boundary side
    pub side: BoundarySide,
}

/// Side of tile boundary
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum BoundarySide {
    North,
    South,
    East,
    West,
}

/// Edge crossing tile boundary
#[derive(Debug, Clone)]
pub struct BorderEdge {
    /// Edge ID within tile
    pub local_edge_id: i64,
    /// Start node (canonical within tile)
    pub start_node: i64,
    /// End node (canonical within tile)  
    pub end_node: i64,
    /// Crossing coordinates
    pub crossing_coords: (f64, f64),
    /// Edge attributes for matching
    pub highway_class: String,
    pub access_restrictions: Vec<String>,
}

impl BorderReconciliation {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add border edge for reconciliation
    pub fn add_border_edge(&mut self, boundary: TileBoundary, edge: BorderEdge) {
        self.border_edges.entry(boundary)
            .or_default()
            .push(edge);
    }

    /// Reconcile borders across all tiles
    pub fn reconcile_borders(&mut self) -> Result<(), String> {
        let border_edges = self.border_edges.clone();
        for (boundary, edges) in &border_edges {
            self.reconcile_boundary_edges(boundary, edges)?;
        }
        Ok(())
    }

    /// Reconcile edges at a specific boundary
    fn reconcile_boundary_edges(&mut self, boundary: &TileBoundary, edges: &[BorderEdge]) -> Result<(), String> {
        // 1. Find matching edges on adjacent tiles
        let adjacent_boundary = self.get_adjacent_boundary(boundary);
        let adjacent_edges = self.border_edges.get(&adjacent_boundary).cloned().unwrap_or_default();
        
        // 2. Match edges across boundary based on coordinates and attributes
        for edge in edges {
            let matching_edge = self.find_matching_edge(edge, &adjacent_edges)?;
            
            if let Some(match_edge) = matching_edge {
                // 3. Ensure consistent canonical node IDs
                self.reconcile_node_ids(edge, &match_edge)?;
                
                // 4. Verify edge attributes match
                self.verify_edge_attributes(edge, &match_edge)?;
                
                // 5. Update global mapping for consistent node IDs
                self.update_global_node_mapping(edge, &match_edge)?;
            } else {
                // Edge has no match - this could indicate data inconsistency
                // For now, we'll log this but not fail the process
                eprintln!("Warning: No matching edge found for border edge {} on boundary {:?}", 
                         edge.local_edge_id, boundary);
            }
        }
        
        Ok(())
    }
    
    /// Get the adjacent boundary for cross-tile matching
    fn get_adjacent_boundary(&self, boundary: &TileBoundary) -> TileBoundary {
        match boundary.side {
            BoundarySide::North => TileBoundary {
                tile_x: boundary.tile_x,
                tile_y: boundary.tile_y + 1,
                side: BoundarySide::South,
            },
            BoundarySide::South => TileBoundary {
                tile_x: boundary.tile_x,
                tile_y: boundary.tile_y - 1,
                side: BoundarySide::North,
            },
            BoundarySide::East => TileBoundary {
                tile_x: boundary.tile_x + 1,
                tile_y: boundary.tile_y,
                side: BoundarySide::West,
            },
            BoundarySide::West => TileBoundary {
                tile_x: boundary.tile_x - 1,
                tile_y: boundary.tile_y,
                side: BoundarySide::East,
            },
        }
    }
    
    /// Find matching edge on adjacent tile
    fn find_matching_edge(&self, edge: &BorderEdge, adjacent_edges: &[BorderEdge]) -> Result<Option<BorderEdge>, String> {
        const COORDINATE_TOLERANCE: f64 = 1e-6; // Small tolerance for coordinate matching
        
        for adj_edge in adjacent_edges {
            // Match by crossing coordinates (should be very close)
            let lat_diff = (edge.crossing_coords.0 - adj_edge.crossing_coords.0).abs();
            let lon_diff = (edge.crossing_coords.1 - adj_edge.crossing_coords.1).abs();
            
            if lat_diff < COORDINATE_TOLERANCE && lon_diff < COORDINATE_TOLERANCE {
                // Verify highway class matches
                if edge.highway_class == adj_edge.highway_class {
                    return Ok(Some(adj_edge.clone()));
                } else {
                    return Err(format!(
                        "Highway class mismatch at coordinates ({}, {}): {} vs {}",
                        edge.crossing_coords.0, edge.crossing_coords.1,
                        edge.highway_class, adj_edge.highway_class
                    ));
                }
            }
        }
        
        Ok(None)
    }
    
    /// Reconcile canonical node IDs between matched edges
    fn reconcile_node_ids(&mut self, edge1: &BorderEdge, edge2: &BorderEdge) -> Result<(), String> {
        // For border reconciliation, we need to ensure that nodes representing
        // the same geographic location have the same canonical ID across tiles
        
        // The crossing coordinates represent the boundary point - only nodes
        // at this exact location should get the same global ID
        let boundary_global_id = self.coordinate_to_global_id(edge1.crossing_coords);
        
        // Update global mapping for both tiles
        let tile1_id = self.extract_tile_id_from_edge(edge1);
        let tile2_id = self.extract_tile_id_from_edge(edge2);
        
        // Determine which nodes are actually at the boundary crossing
        // In a border edge, typically one node is at the boundary and one is internal
        let (boundary_node1, internal_node1) = self.identify_boundary_nodes(edge1)?;
        let (boundary_node2, internal_node2) = self.identify_boundary_nodes(edge2)?;
        
        // Map boundary nodes to the same global ID
        self.global_mapping.insert((boundary_node1, tile1_id), boundary_global_id);
        self.global_mapping.insert((boundary_node2, tile2_id), boundary_global_id);
        
        // Internal nodes get their own unique global IDs based on their tile + local ID
        // This ensures they don't conflict with boundary nodes but remain unique
        let internal_global_id1 = self.generate_internal_global_id(internal_node1, tile1_id);
        let internal_global_id2 = self.generate_internal_global_id(internal_node2, tile2_id);
        
        self.global_mapping.insert((internal_node1, tile1_id), internal_global_id1);
        self.global_mapping.insert((internal_node2, tile2_id), internal_global_id2);
        
        Ok(())
    }
    
    /// Identify which nodes in a border edge are at the boundary vs internal
    fn identify_boundary_nodes(&self, edge: &BorderEdge) -> Result<(i64, i64), String> {
        // In the current implementation, we assume the crossing_coords represent
        // where the edge crosses the tile boundary. In a real implementation,
        // this would analyze the edge geometry to determine which node is closer
        // to the boundary coordinates.
        
        // For now, we'll use a simple heuristic:
        // - start_node is assumed to be the boundary node
        // - end_node is assumed to be the internal node
        // This would need actual geometric analysis in production
        
        Ok((edge.start_node, edge.end_node))
    }
    
    /// Verify that edge attributes are consistent across tiles
    fn verify_edge_attributes(&self, edge1: &BorderEdge, edge2: &BorderEdge) -> Result<(), String> {
        // Highway class should match (already checked in find_matching_edge)
        if edge1.highway_class != edge2.highway_class {
            return Err(format!("Highway class mismatch: {} vs {}", 
                              edge1.highway_class, edge2.highway_class));
        }
        
        // Access restrictions should be compatible
        let restrictions1: std::collections::HashSet<_> = edge1.access_restrictions.iter().collect();
        let restrictions2: std::collections::HashSet<_> = edge2.access_restrictions.iter().collect();
        
        // Check for major conflicts (some variations are acceptable)
        for restriction in &restrictions1 {
            if restriction.contains("access=no") && !restrictions2.contains(restriction) {
                eprintln!("Warning: Access restriction inconsistency at ({}, {}): {}", 
                         edge1.crossing_coords.0, edge1.crossing_coords.1, restriction);
            }
        }
        
        Ok(())
    }
    
    /// Update global node mapping for consistent IDs
    fn update_global_node_mapping(&mut self, edge1: &BorderEdge, edge2: &BorderEdge) -> Result<(), String> {
        // This extends the reconcile_node_ids functionality
        // Additional validation could be added here for complex scenarios
        
        // Verify that our mapping is consistent
        let tile1_id = self.extract_tile_id_from_edge(edge1);
        let tile2_id = self.extract_tile_id_from_edge(edge2);
        
        // Check for mapping consistency
        if let (Some(&global1), Some(&global2)) = (
            self.global_mapping.get(&(edge1.start_node, tile1_id)),
            self.global_mapping.get(&(edge2.start_node, tile2_id))
        ) {
            if global1 != global2 {
                return Err(format!(
                    "Inconsistent global mapping for border nodes: {} vs {}", 
                    global1, global2
                ));
            }
        }
        
        Ok(())
    }
    
    /// Generate global canonical ID from coordinates
    fn coordinate_to_global_id(&self, coords: (f64, f64)) -> i64 {
        // Use a deterministic hash of coordinates to generate consistent global IDs
        // This ensures the same coordinates always produce the same ID across tiles
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher = DefaultHasher::new();
        
        // Scale coordinates to avoid floating point precision issues
        let scaled_lat = (coords.0 * 1_000_000.0).round() as i64;
        let scaled_lon = (coords.1 * 1_000_000.0).round() as i64;
        
        scaled_lat.hash(&mut hasher);
        scaled_lon.hash(&mut hasher);
        
        hasher.finish() as i64
    }
    
    /// Extract tile ID from edge based on local edge ID encoding
    fn extract_tile_id_from_edge(&self, edge: &BorderEdge) -> i64 {
        // In this implementation, we derive the tile ID from the edge's local_edge_id
        // and crossing coordinates. This assumes the local_edge_id incorporates
        // tile-specific information or we can derive it from coordinates.
        
        // Method 1: Extract from high bits of local_edge_id (assuming encoding includes tile info)
        let potential_tile_from_id = edge.local_edge_id >> 32; // Upper 32 bits as tile ID
        
        if potential_tile_from_id != 0 {
            // If upper bits contain tile info, use it
            potential_tile_from_id
        } else {
            // Method 2: Calculate tile ID from crossing coordinates using tile grid
            // Assuming 125m tiles (same as telemetry system)
            const TILE_SIZE_METERS: f64 = 125.0;
            const EARTH_RADIUS: f64 = 6371000.0;
            
            // Convert coordinates to tile indices
            let lat_rad = edge.crossing_coords.0.to_radians();
            let lon_rad = edge.crossing_coords.1.to_radians();
            
            // Approximate tile calculation (simplified for this implementation)
            let tile_x = ((lon_rad * EARTH_RADIUS) / TILE_SIZE_METERS).floor() as i32;
            let tile_y = ((lat_rad * EARTH_RADIUS) / TILE_SIZE_METERS).floor() as i32;
            
            // Combine tile coordinates into a single ID
            // Using Cantor pairing function for unique ID generation
            let tile_x_abs = tile_x.abs() as i64;
            let tile_y_abs = tile_y.abs() as i64;
            
            // Cantor pairing: (x + y) * (x + y + 1) / 2 + y
            let sum = tile_x_abs + tile_y_abs;
            (sum * (sum + 1)) / 2 + tile_y_abs
        }
    }

    /// Generate unique global ID for internal (non-boundary) nodes
    fn generate_internal_global_id(&self, local_node: i64, tile_id: i64) -> i64 {
        // Combine tile ID and local node ID to create a unique global ID
        // Use a different hash space than coordinate-based IDs to avoid conflicts
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher = DefaultHasher::new();
        
        // Hash tile ID and local node ID together
        tile_id.hash(&mut hasher);
        local_node.hash(&mut hasher);
        
        // Add a salt to distinguish from coordinate-based hashes
        "internal_node".hash(&mut hasher);
        
        hasher.finish() as i64
    }

    /// Get global canonical ID for local node
    pub fn get_global_canonical(&self, local_node: i64, tile_id: i64) -> Option<i64> {
        self.global_mapping.get(&(local_node, tile_id)).copied()
    }

    /// Clear reconciliation data
    pub fn clear(&mut self) {
        self.border_edges.clear();
        self.global_mapping.clear();
    }
}

/// M3.4 - Graph debug artifacts and APIs
#[derive(Debug, Clone, Default)]
pub struct GraphDebugger {
    /// Node statistics
    node_stats: GraphNodeStats,
    /// Edge statistics  
    edge_stats: GraphEdgeStats,
    /// Super-edge statistics
    super_edge_stats: SuperEdgeStats,
}

/// Node statistics for debugging
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GraphNodeStats {
    pub total_canonical_nodes: usize,
    pub degree_distribution: HashMap<usize, usize>, // degree -> count
    pub semantic_important_nodes: usize,
    pub turn_restriction_anchors: usize,
}

/// Edge statistics for debugging  
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GraphEdgeStats {
    pub total_edges: usize,
    pub total_length_km: f64,
    pub highway_class_distribution: HashMap<String, usize>,
    pub access_restricted_edges: usize,
}

/// Super-edge statistics for debugging
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SuperEdgeStats {
    pub total_super_edges: usize,
    pub total_collapsed_nodes: usize,
    pub compression_ratio: f64,
    pub segment_guards_inserted: usize,
    pub average_length_km: f64,
}

impl GraphDebugger {
    pub fn new() -> Self {
        Self::default()
    }

    /// Analyze canonical adjacency for statistics
    pub fn analyze_adjacency(&mut self, adjacency: &CanonicalAdjacency) {
        let all_nodes = adjacency.get_all_nodes();
        self.node_stats.total_canonical_nodes = all_nodes.len();

        // Calculate degree distribution
        for node in &all_nodes {
            let degree = adjacency.get_degree(*node);
            *self.node_stats.degree_distribution.entry(degree).or_default() += 1;
        }

        // Count edges and analyze
        let mut total_edges = 0;
        let mut total_length = 0.0;
        let mut highway_classes = HashMap::new();
        let mut access_restricted = 0;

        for node in &all_nodes {
            let neighbors = adjacency.get_neighbors(*node);
            for neighbor in neighbors {
                if node < &neighbor { // Count each edge only once
                    total_edges += 1;
                    if let Some(edge_info) = adjacency.get_edge_info(*node, neighbor) {
                        total_length += edge_info.length_meters;
                        *highway_classes.entry(edge_info.highway_class.clone()).or_default() += 1;
                        if !edge_info.access_restrictions.is_empty() {
                            access_restricted += 1;
                        }
                    }
                }
            }
        }

        self.edge_stats.total_edges = total_edges;
        self.edge_stats.total_length_km = total_length / 1000.0;
        self.edge_stats.highway_class_distribution = highway_classes;
        self.edge_stats.access_restricted_edges = access_restricted;
    }

    /// Analyze super-edges for statistics
    pub fn analyze_super_edges(&mut self, super_edge_constructor: &SuperEdgeConstructor) {
        let super_edges = super_edge_constructor.get_super_edges();
        self.super_edge_stats.total_super_edges = super_edges.len();

        let mut total_collapsed = 0;
        let mut total_guards = 0;
        let mut total_length = 0.0;

        for super_edge in &super_edges {
            total_collapsed += super_edge.collapsed_nodes.len();
            total_guards += super_edge.segment_guards.len();
            total_length += super_edge.total_length;
        }

        self.super_edge_stats.total_collapsed_nodes = total_collapsed;
        self.super_edge_stats.segment_guards_inserted = total_guards;
        self.super_edge_stats.average_length_km = if !super_edges.is_empty() {
            total_length / super_edges.len() as f64 / 1000.0
        } else {
            0.0
        };

        // Calculate compression ratio
        let original_edges = total_collapsed + super_edges.len();
        self.super_edge_stats.compression_ratio = if original_edges > 0 {
            super_edges.len() as f64 / original_edges as f64
        } else {
            1.0
        };
    }

    /// Generate nodes.bin artifact
    pub fn generate_nodes_bin(&self, adjacency: &CanonicalAdjacency) -> Result<Vec<u8>, std::io::Error> {
        use std::io::{Cursor, Write};

        let mut buffer = Cursor::new(Vec::new());
        let all_nodes = adjacency.get_all_nodes();

        // Write header
        buffer.write_all(b"NODES_BIN_V1\n")?;
        buffer.write_all(&(all_nodes.len() as u32).to_le_bytes())?;

        // Write node data
        for node_id in all_nodes {
            let degree = adjacency.get_degree(node_id);
            let neighbors = adjacency.get_neighbors(node_id);

            buffer.write_all(&node_id.to_le_bytes())?;
            buffer.write_all(&(degree as u32).to_le_bytes())?;
            
            for neighbor in neighbors {
                buffer.write_all(&neighbor.to_le_bytes())?;
            }
        }

        Ok(buffer.into_inner())
    }

    /// Generate super_edges.bin artifact
    pub fn generate_super_edges_bin(&self, super_edge_constructor: &SuperEdgeConstructor) -> Result<Vec<u8>, std::io::Error> {
        use std::io::{Cursor, Write};

        let mut buffer = Cursor::new(Vec::new());
        let super_edges = super_edge_constructor.get_super_edges();

        // Write header
        buffer.write_all(b"SUPER_EDGES_V1\n")?;
        buffer.write_all(&(super_edges.len() as u32).to_le_bytes())?;

        // Write super-edge data
        for super_edge in super_edges {
            buffer.write_all(&super_edge.start_node.to_le_bytes())?;
            buffer.write_all(&super_edge.end_node.to_le_bytes())?;
            buffer.write_all(&(super_edge.total_length as f32).to_le_bytes())?;
            buffer.write_all(&(super_edge.collapsed_nodes.len() as u32).to_le_bytes())?;
            
            for &collapsed_node in &super_edge.collapsed_nodes {
                buffer.write_all(&collapsed_node.to_le_bytes())?;
            }

            buffer.write_all(&(super_edge.geometry.len() as u32).to_le_bytes())?;
            for (lat, lon) in &super_edge.geometry {
                buffer.write_all(&(*lat as f32).to_le_bytes())?;
                buffer.write_all(&(*lon as f32).to_le_bytes())?;
            }
        }

        Ok(buffer.into_inner())
    }

    /// Generate geom.temp artifact (temporary geometry storage)
    pub fn generate_geom_temp(&self, super_edge_constructor: &SuperEdgeConstructor) -> Result<Vec<u8>, std::io::Error> {
        use std::io::{Cursor, Write};

        let mut buffer = Cursor::new(Vec::new());
        let super_edges = super_edge_constructor.get_super_edges();

        // Write header
        buffer.write_all(b"GEOM_TEMP_V1\n")?;

        // Write geometry data with compression hints
        for super_edge in super_edges {
            let edge_id = format!("{}_{}", super_edge.start_node, super_edge.end_node);
            buffer.write_all(edge_id.as_bytes())?;
            buffer.write_all(b"\n")?;
            
            buffer.write_all(&(super_edge.geometry.len() as u32).to_le_bytes())?;
            for (lat, lon) in &super_edge.geometry {
                buffer.write_all(&(*lat as f64).to_le_bytes())?;
                buffer.write_all(&(*lon as f64).to_le_bytes())?;
            }
        }

        Ok(buffer.into_inner())
    }

    /// Get graph statistics for /graph/stats API
    pub fn get_graph_stats(&self) -> serde_json::Value {
        serde_json::json!({
            "nodes": self.node_stats,
            "edges": self.edge_stats,
            "super_edges": self.super_edge_stats
        })
    }

    /// Get edge details for /graph/edge/{id} API
    pub fn get_edge_details(&self, edge_id: &str, adjacency: &CanonicalAdjacency) -> Option<serde_json::Value> {
        // Parse edge ID (format: "start_end")
        let parts: Vec<&str> = edge_id.split('_').collect();
        if parts.len() != 2 {
            return None;
        }

        let start: i64 = parts[0].parse().ok()?;
        let end: i64 = parts[1].parse().ok()?;

        if let Some(edge_info) = adjacency.get_edge_info(start, end) {
            Some(serde_json::json!({
                "start_node": start,
                "end_node": end,
                "length_meters": edge_info.length_meters,
                "highway_class": edge_info.highway_class,
                "access_restrictions": edge_info.access_restrictions,
                "is_semantically_important": edge_info.is_semantically_important,
                "original_way_ids": edge_info.original_way_ids,
                "geometry": edge_info.geometry
            }))
        } else {
            None
        }
    }

    /// Clear debug data
    pub fn clear(&mut self) {
        self.node_stats = GraphNodeStats::default();
        self.edge_stats = GraphEdgeStats::default();
        self.super_edge_stats = SuperEdgeStats::default();
    }
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