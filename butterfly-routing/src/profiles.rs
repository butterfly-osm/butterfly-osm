//! Multi-profile routing system for car/bike/foot transportation modes
//!
//! M4 — Multi-Profile System implementation with access truth tables,
//! profile masking, component analysis, speed/time weights, and multi-profile loading.

use std::collections::HashMap;
use serde::{Deserialize, Serialize};

/// Transportation profiles supported by the routing engine
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TransportProfile {
    Car,
    Bicycle,
    Foot,
}

impl TransportProfile {
    /// Get all supported profiles
    pub fn all() -> Vec<Self> {
        vec![Self::Car, Self::Bicycle, Self::Foot]
    }

    /// Get profile name as string
    pub fn name(&self) -> &'static str {
        match self {
            Self::Car => "car",
            Self::Bicycle => "bicycle", 
            Self::Foot => "foot",
        }
    }
}

/// Access permission levels for routing
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AccessLevel {
    /// Explicitly allowed
    Yes,
    /// Explicitly forbidden
    No,
    /// Private access (generally forbidden for public routing)
    Private,
    /// Permissive access (allowed but may be restricted)
    Permissive,
    /// Destination access only
    Destination,
    /// Customer access only
    Customers,
    /// Delivery access only
    Delivery,
    /// Agricultural access only
    Agricultural,
    /// Forestry access only
    Forestry,
    /// Emergency access only
    Emergency,
    /// Unknown/unspecified access
    Unknown,
}

impl AccessLevel {
    /// Parse OSM access tag value into AccessLevel
    pub fn parse(value: &str) -> Self {
        match value {
            "yes" | "designated" | "official" => Self::Yes,
            "no" | "none" => Self::No,
            "private" => Self::Private,
            "permissive" | "permit" => Self::Permissive,
            "destination" => Self::Destination,
            "customers" => Self::Customers,
            "delivery" => Self::Delivery,
            "agricultural" => Self::Agricultural,
            "forestry" => Self::Forestry,
            "emergency" => Self::Emergency,
            _ => Self::Unknown,
        }
    }

    /// Check if this access level allows routing for the given profile
    pub fn allows_routing(&self, profile: TransportProfile) -> bool {
        match (self, profile) {
            // Explicit yes always allows
            (Self::Yes, _) => true,
            // Explicit no always forbids
            (Self::No, _) => false,
            // Private generally forbids public routing
            (Self::Private, _) => false,
            // Permissive generally allows
            (Self::Permissive, _) => true,
            // Destination access - allow for all profiles (routing engine can handle restrictions)
            (Self::Destination, _) => true,
            // Customers - allow but with penalty
            (Self::Customers, _) => true,
            // Delivery - cars yes, others no
            (Self::Delivery, TransportProfile::Car) => true,
            (Self::Delivery, _) => false,
            // Agricultural - cars yes, others no
            (Self::Agricultural, TransportProfile::Car) => true,
            (Self::Agricultural, _) => false,
            // Forestry - cars yes, others no  
            (Self::Forestry, TransportProfile::Car) => true,
            (Self::Forestry, _) => false,
            // Emergency - cars yes, others no
            (Self::Emergency, TransportProfile::Car) => true,
            (Self::Emergency, _) => false,
            // Unknown defaults to yes (OSM standard)
            (Self::Unknown, _) => true,
        }
    }
}

/// Highway type classification for access rules
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum HighwayType {
    Motorway,
    MotorwayLink,
    Trunk,
    TrunkLink,
    Primary,
    PrimaryLink,
    Secondary,
    SecondaryLink,
    Tertiary,
    TertiaryLink,
    Unclassified,
    Residential,
    LivingStreet,
    Service,
    Pedestrian,
    Track,
    Path,
    Footway,
    Bridleway,
    Cycleway,
    Steps,
    Corridor,
    Platform,
    Ferry,
    Other(String),
}

impl HighwayType {
    /// Parse highway tag value into HighwayType
    pub fn parse(value: &str) -> Self {
        match value {
            "motorway" => Self::Motorway,
            "motorway_link" => Self::MotorwayLink,
            "trunk" => Self::Trunk,
            "trunk_link" => Self::TrunkLink,
            "primary" => Self::Primary,
            "primary_link" => Self::PrimaryLink,
            "secondary" => Self::Secondary,
            "secondary_link" => Self::SecondaryLink,
            "tertiary" => Self::Tertiary,
            "tertiary_link" => Self::TertiaryLink,
            "unclassified" => Self::Unclassified,
            "residential" => Self::Residential,
            "living_street" => Self::LivingStreet,
            "service" => Self::Service,
            "pedestrian" => Self::Pedestrian,
            "track" => Self::Track,
            "path" => Self::Path,
            "footway" => Self::Footway,
            "bridleway" => Self::Bridleway,
            "cycleway" => Self::Cycleway,
            "steps" => Self::Steps,
            "corridor" => Self::Corridor,
            "platform" => Self::Platform,
            "ferry" => Self::Ferry,
            other => Self::Other(other.to_string()),
        }
    }

    /// Get default access for this highway type and transport profile
    pub fn default_access(&self, profile: TransportProfile) -> AccessLevel {
        match (self, profile) {
            // Motorways - cars only
            (Self::Motorway | Self::MotorwayLink, TransportProfile::Car) => AccessLevel::Yes,
            (Self::Motorway | Self::MotorwayLink, _) => AccessLevel::No,
            
            // Major roads - cars yes, bikes/foot generally yes unless restricted
            (Self::Trunk | Self::TrunkLink | Self::Primary | Self::PrimaryLink, TransportProfile::Car) => AccessLevel::Yes,
            (Self::Trunk | Self::TrunkLink | Self::Primary | Self::PrimaryLink, TransportProfile::Bicycle) => AccessLevel::Yes,
            (Self::Trunk | Self::TrunkLink | Self::Primary | Self::PrimaryLink, TransportProfile::Foot) => AccessLevel::Yes,
            
            // Secondary and below - all modes generally allowed
            (Self::Secondary | Self::SecondaryLink | Self::Tertiary | Self::TertiaryLink | 
             Self::Unclassified | Self::Residential | Self::LivingStreet, _) => AccessLevel::Yes,
            
            // Service roads - all modes generally allowed
            (Self::Service, _) => AccessLevel::Yes,
            
            // Pedestrian areas - foot/bike yes, cars no
            (Self::Pedestrian, TransportProfile::Car) => AccessLevel::No,
            (Self::Pedestrian, _) => AccessLevel::Yes,
            
            // Tracks - cars no unless explicitly allowed, others yes
            (Self::Track, TransportProfile::Car) => AccessLevel::No,
            (Self::Track, _) => AccessLevel::Yes,
            
            // Paths - cars no, others yes
            (Self::Path, TransportProfile::Car) => AccessLevel::No,
            (Self::Path, _) => AccessLevel::Yes,
            
            // Footways - foot yes, bikes sometimes, cars no
            (Self::Footway, TransportProfile::Car) => AccessLevel::No,
            (Self::Footway, TransportProfile::Bicycle) => AccessLevel::No, // Usually not allowed unless tagged
            (Self::Footway, TransportProfile::Foot) => AccessLevel::Yes,
            
            // Bridleways - foot/bike yes, cars no
            (Self::Bridleway, TransportProfile::Car) => AccessLevel::No,
            (Self::Bridleway, _) => AccessLevel::Yes,
            
            // Cycleways - bikes yes, foot sometimes, cars no
            (Self::Cycleway, TransportProfile::Car) => AccessLevel::No,
            (Self::Cycleway, TransportProfile::Bicycle) => AccessLevel::Yes,
            (Self::Cycleway, TransportProfile::Foot) => AccessLevel::No, // Usually not allowed unless tagged
            
            // Steps - foot only
            (Self::Steps, TransportProfile::Car | TransportProfile::Bicycle) => AccessLevel::No,
            (Self::Steps, TransportProfile::Foot) => AccessLevel::Yes,
            
            // Indoor areas - foot only
            (Self::Corridor, TransportProfile::Car | TransportProfile::Bicycle) => AccessLevel::No,
            (Self::Corridor, TransportProfile::Foot) => AccessLevel::Yes,
            
            // Platforms - foot/bike yes, cars no
            (Self::Platform, TransportProfile::Car) => AccessLevel::No,
            (Self::Platform, _) => AccessLevel::Yes,
            
            // Ferry - all modes yes (if tagged appropriately)
            (Self::Ferry, _) => AccessLevel::Yes,
            
            // Other/unknown - default to yes for compatibility
            (Self::Other(_), _) => AccessLevel::Yes,
        }
    }
}

/// Way access information for a specific transport profile
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WayAccess {
    /// Transport profile this access applies to
    pub profile: TransportProfile,
    /// Final access level for this profile
    pub access: AccessLevel,
    /// Highway type
    pub highway: HighwayType,
    /// Whether this way is legally accessible for routing
    pub is_accessible: bool,
    /// Access penalties (0.0 = no penalty, 1.0 = avoid, >1.0 = strongly avoid)
    pub penalty: f32,
    /// Original OSM tags that determined this access
    pub source_tags: HashMap<String, String>,
}

impl WayAccess {
    /// Check if this way should be included in the routing graph for this profile
    pub fn should_include_in_graph(&self) -> bool {
        self.is_accessible && self.access != AccessLevel::No
    }
}

/// M4.1 — Access Truth Tables
/// Comprehensive access rule evaluation for all transport profiles
#[derive(Debug, Clone)]
pub struct AccessTruthTable {
    /// Profile-specific access rules
    profile_rules: HashMap<TransportProfile, ProfileAccessRules>,
}

/// Access rules for a specific transport profile
#[derive(Debug, Clone)]
struct ProfileAccessRules {
    /// Default access for highway types
    highway_defaults: HashMap<HighwayType, AccessLevel>,
    /// Tag priority order (later tags override earlier ones)
    tag_priority: Vec<String>,
    /// Special case handlers
    special_cases: Vec<SpecialAccessCase>,
}

/// Special access case rule
#[derive(Debug, Clone)]
struct SpecialAccessCase {
    /// Condition tags that must match
    conditions: HashMap<String, String>,
    /// Access level to apply if conditions match
    access: AccessLevel,
    /// Description of this special case
    description: String,
}

impl AccessTruthTable {
    /// Create new access truth table with comprehensive rules for all profiles
    pub fn new() -> Self {
        let mut table = Self {
            profile_rules: HashMap::new(),
        };
        
        // Initialize rules for each profile
        for profile in TransportProfile::all() {
            table.profile_rules.insert(profile, Self::create_profile_rules(profile));
        }
        
        table
    }
    
    /// Create access rules for a specific profile
    fn create_profile_rules(profile: TransportProfile) -> ProfileAccessRules {
        let mut highway_defaults = HashMap::new();
        
        // Set default access for all highway types
        let highway_types = [
            HighwayType::Motorway, HighwayType::MotorwayLink,
            HighwayType::Trunk, HighwayType::TrunkLink,
            HighwayType::Primary, HighwayType::PrimaryLink,
            HighwayType::Secondary, HighwayType::SecondaryLink,
            HighwayType::Tertiary, HighwayType::TertiaryLink,
            HighwayType::Unclassified, HighwayType::Residential,
            HighwayType::LivingStreet, HighwayType::Service,
            HighwayType::Pedestrian, HighwayType::Track,
            HighwayType::Path, HighwayType::Footway,
            HighwayType::Bridleway, HighwayType::Cycleway,
            HighwayType::Steps, HighwayType::Corridor,
            HighwayType::Platform, HighwayType::Ferry,
        ];
        
        for highway_type in &highway_types {
            highway_defaults.insert(highway_type.clone(), highway_type.default_access(profile));
        }
        
        // Tag priority order - more specific tags override general ones
        let tag_priority = match profile {
            TransportProfile::Car => vec![
                "access".to_string(),
                "vehicle".to_string(),
                "motor_vehicle".to_string(),
                "motorcar".to_string(),
                "car".to_string(),
            ],
            TransportProfile::Bicycle => vec![
                "access".to_string(),
                "vehicle".to_string(),
                "bicycle".to_string(),
            ],
            TransportProfile::Foot => vec![
                "access".to_string(),
                "foot".to_string(),
                "pedestrian".to_string(),
            ],
        };
        
        // Special cases for complex access rules
        let special_cases = Self::create_special_cases(profile);
        
        ProfileAccessRules {
            highway_defaults,
            tag_priority,
            special_cases,
        }
    }
    
    /// Create special access cases for a profile
    fn create_special_cases(profile: TransportProfile) -> Vec<SpecialAccessCase> {
        let mut cases = Vec::new();
        
        match profile {
            TransportProfile::Car => {
                // Cars forbidden on pedestrian/cycling infrastructure
                cases.push(SpecialAccessCase {
                    conditions: [("highway".to_string(), "footway".to_string())].into_iter().collect(),
                    access: AccessLevel::No,
                    description: "Cars not allowed on footways".to_string(),
                });
                
                cases.push(SpecialAccessCase {
                    conditions: [("highway".to_string(), "cycleway".to_string())].into_iter().collect(),
                    access: AccessLevel::No,
                    description: "Cars not allowed on cycleways".to_string(),
                });
                
                cases.push(SpecialAccessCase {
                    conditions: [("highway".to_string(), "steps".to_string())].into_iter().collect(),
                    access: AccessLevel::No,
                    description: "Cars cannot use steps".to_string(),
                });
                
                // Service roads with private access
                cases.push(SpecialAccessCase {
                    conditions: [
                        ("highway".to_string(), "service".to_string()),
                        ("access".to_string(), "private".to_string())
                    ].into_iter().collect(),
                    access: AccessLevel::Private,
                    description: "Private service roads".to_string(),
                });
            }
            
            TransportProfile::Bicycle => {
                // Bicycles generally not allowed on motorways
                cases.push(SpecialAccessCase {
                    conditions: [("highway".to_string(), "motorway".to_string())].into_iter().collect(),
                    access: AccessLevel::No,
                    description: "Bicycles not allowed on motorways".to_string(),
                });
                
                cases.push(SpecialAccessCase {
                    conditions: [("highway".to_string(), "motorway_link".to_string())].into_iter().collect(),
                    access: AccessLevel::No,
                    description: "Bicycles not allowed on motorway links".to_string(),
                });
                
                // Steps are not bikeable
                cases.push(SpecialAccessCase {
                    conditions: [("highway".to_string(), "steps".to_string())].into_iter().collect(),
                    access: AccessLevel::No,
                    description: "Bicycles cannot use steps".to_string(),
                });
                
                // Footways generally not allowed unless designated
                cases.push(SpecialAccessCase {
                    conditions: [("highway".to_string(), "footway".to_string())].into_iter().collect(),
                    access: AccessLevel::No,
                    description: "Bicycles generally not allowed on footways".to_string(),
                });
                
                // But bicycle=designated overrides this
                cases.push(SpecialAccessCase {
                    conditions: [
                        ("highway".to_string(), "footway".to_string()),
                        ("bicycle".to_string(), "designated".to_string())
                    ].into_iter().collect(),
                    access: AccessLevel::Yes,
                    description: "Designated bicycle access on footway".to_string(),
                });
            }
            
            TransportProfile::Foot => {
                // Pedestrians not allowed on motorways
                cases.push(SpecialAccessCase {
                    conditions: [("highway".to_string(), "motorway".to_string())].into_iter().collect(),
                    access: AccessLevel::No,
                    description: "Pedestrians not allowed on motorways".to_string(),
                });
                
                cases.push(SpecialAccessCase {
                    conditions: [("highway".to_string(), "motorway_link".to_string())].into_iter().collect(),
                    access: AccessLevel::No,
                    description: "Pedestrians not allowed on motorway links".to_string(),
                });
                
                // Cycleways generally not allowed unless designated
                cases.push(SpecialAccessCase {
                    conditions: [("highway".to_string(), "cycleway".to_string())].into_iter().collect(),
                    access: AccessLevel::No,
                    description: "Pedestrians generally not allowed on cycleways".to_string(),
                });
                
                // But foot=designated overrides this
                cases.push(SpecialAccessCase {
                    conditions: [
                        ("highway".to_string(), "cycleway".to_string()),
                        ("foot".to_string(), "designated".to_string())
                    ].into_iter().collect(),
                    access: AccessLevel::Yes,
                    description: "Designated pedestrian access on cycleway".to_string(),
                });
            }
        }
        
        cases
    }
    
    /// Evaluate way access for a specific transport profile
    pub fn evaluate_way_access(&self, profile: TransportProfile, tags: &HashMap<String, String>) -> WayAccess {
        let rules = &self.profile_rules[&profile];
        
        // Parse highway type
        let highway = tags.get("highway")
            .map(|h| HighwayType::parse(h))
            .unwrap_or(HighwayType::Other("unknown".to_string()));
        
        // Start with highway default access
        let mut access = rules.highway_defaults.get(&highway)
            .copied()
            .unwrap_or(AccessLevel::Unknown);
        
        // Check special cases first (they can override defaults)
        for special_case in &rules.special_cases {
            let mut matches = true;
            for (key, value) in &special_case.conditions {
                if tags.get(key) != Some(value) {
                    matches = false;
                    break;
                }
            }
            if matches {
                access = special_case.access;
                break; // First matching special case wins
            }
        }
        
        // Apply tag hierarchy (specific tags override general ones)
        for tag_key in &rules.tag_priority {
            if let Some(tag_value) = tags.get(tag_key) {
                let tag_access = AccessLevel::parse(tag_value);
                if tag_access != AccessLevel::Unknown {
                    access = tag_access;
                    // Continue processing to let more specific tags override
                }
            }
        }
        
        // Determine if accessible for routing
        let is_accessible = access.allows_routing(profile);
        
        // Calculate penalty based on access level
        let penalty = match access {
            AccessLevel::Yes => 0.0,
            AccessLevel::Permissive => 0.1,
            AccessLevel::Destination => 0.3,
            AccessLevel::Customers => 0.5,
            AccessLevel::Delivery => 0.7,
            AccessLevel::Private => 1.5,
            AccessLevel::No => 10.0, // High penalty but not infinite
            _ => 0.2,
        };
        
        WayAccess {
            profile,
            access,
            highway,
            is_accessible,
            penalty,
            source_tags: tags.clone(),
        }
    }
    
    /// Get all supported transport profiles
    pub fn get_profiles(&self) -> Vec<TransportProfile> {
        self.profile_rules.keys().copied().collect()
    }
    
    /// Check if a way should be included in routing graph for any profile
    pub fn is_routable_for_any_profile(&self, tags: &HashMap<String, String>) -> bool {
        for profile in TransportProfile::all() {
            let access = self.evaluate_way_access(profile, tags);
            if access.should_include_in_graph() {
                return true;
            }
        }
        false
    }
    
    /// Get access evaluation for all profiles
    pub fn evaluate_all_profiles(&self, tags: &HashMap<String, String>) -> HashMap<TransportProfile, WayAccess> {
        let mut results = HashMap::new();
        for profile in TransportProfile::all() {
            results.insert(profile, self.evaluate_way_access(profile, tags));
        }
        results
    }
}

impl Default for AccessTruthTable {
    fn default() -> Self {
        Self::new()
    }
}

/// M4.2 — Profile Masking
/// Mode-specific graph pruning to create profile-specific subgraphs
#[derive(Debug, Clone)]
pub struct ProfileMask {
    /// Profile this mask is for
    profile: TransportProfile,
    /// Access truth table for evaluating way access
    access_table: AccessTruthTable,
    /// Masked edges (ways that are inaccessible for this profile)
    masked_edges: std::collections::HashSet<EdgeId>,
    /// Statistics about masking
    stats: MaskingStats,
}

/// Edge identifier for masking
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EdgeId(pub i64);

/// Statistics about profile masking
#[derive(Debug, Clone, Default)]
pub struct MaskingStats {
    /// Total ways evaluated
    pub total_ways: usize,
    /// Ways accessible for this profile
    pub accessible_ways: usize,
    /// Ways masked (inaccessible) for this profile
    pub masked_ways: usize,
    /// Ways with explicit access tags
    pub explicit_access_ways: usize,
    /// Ways using highway defaults
    pub default_access_ways: usize,
}

impl MaskingStats {
    /// Get accessibility rate as percentage
    pub fn accessibility_rate(&self) -> f64 {
        if self.total_ways == 0 {
            0.0
        } else {
            (self.accessible_ways as f64 / self.total_ways as f64) * 100.0
        }
    }
    
    /// Get masking rate as percentage
    pub fn masking_rate(&self) -> f64 {
        if self.total_ways == 0 {
            0.0
        } else {
            (self.masked_ways as f64 / self.total_ways as f64) * 100.0
        }
    }
}

impl ProfileMask {
    /// Create a new profile mask for the given transport profile
    pub fn new(profile: TransportProfile) -> Self {
        Self {
            profile,
            access_table: AccessTruthTable::new(),
            masked_edges: std::collections::HashSet::new(),
            stats: MaskingStats::default(),
        }
    }
    
    /// Evaluate a way and determine if it should be masked for this profile
    pub fn evaluate_way(&mut self, edge_id: EdgeId, tags: &HashMap<String, String>) -> bool {
        self.stats.total_ways += 1;
        
        let access = self.access_table.evaluate_way_access(self.profile, tags);
        
        // Track statistics
        if tags.keys().any(|k| k.starts_with("access") || 
                                k == "car" || k == "bicycle" || k == "foot" ||
                                k == "vehicle" || k == "motor_vehicle") {
            self.stats.explicit_access_ways += 1;
        } else {
            self.stats.default_access_ways += 1;
        }
        
        let should_mask = !access.should_include_in_graph();
        
        if should_mask {
            self.masked_edges.insert(edge_id);
            self.stats.masked_ways += 1;
        } else {
            self.stats.accessible_ways += 1;
        }
        
        should_mask
    }
    
    /// Check if an edge is masked for this profile
    pub fn is_edge_masked(&self, edge_id: EdgeId) -> bool {
        self.masked_edges.contains(&edge_id)
    }
    
    /// Get all masked edges
    pub fn get_masked_edges(&self) -> &std::collections::HashSet<EdgeId> {
        &self.masked_edges
    }
    
    /// Get masking statistics
    pub fn get_stats(&self) -> &MaskingStats {
        &self.stats
    }
    
    /// Get the transport profile for this mask
    pub fn get_profile(&self) -> TransportProfile {
        self.profile
    }
    
    /// Clear all masking data
    pub fn clear(&mut self) {
        self.masked_edges.clear();
        self.stats = MaskingStats::default();
    }
    
    /// Create a filtered edge list that excludes masked edges
    pub fn filter_edges<'a, T>(&self, edges: &'a [(EdgeId, T)]) -> Vec<(EdgeId, &'a T)> {
        edges.iter()
            .filter(|(edge_id, _)| !self.is_edge_masked(*edge_id))
            .map(|(edge_id, data)| (*edge_id, data))
            .collect()
    }
    
    /// Apply masking to a graph adjacency list
    pub fn apply_to_adjacency_list(&self, adjacency: &HashMap<i64, Vec<(i64, EdgeId)>>) -> HashMap<i64, Vec<(i64, EdgeId)>> {
        let mut filtered_adjacency = HashMap::new();
        
        for (node_id, neighbors) in adjacency {
            let filtered_neighbors: Vec<_> = neighbors.iter()
                .filter(|(_, edge_id)| !self.is_edge_masked(*edge_id))
                .copied()
                .collect();
            
            // Only include nodes that have at least one accessible edge
            if !filtered_neighbors.is_empty() {
                filtered_adjacency.insert(*node_id, filtered_neighbors);
            }
        }
        
        filtered_adjacency
    }
}

/// Multi-profile mask manager for handling all transport profiles
#[derive(Debug, Clone)]
pub struct MultiProfileMask {
    /// Masks for each transport profile
    profile_masks: HashMap<TransportProfile, ProfileMask>,
}

impl MultiProfileMask {
    /// Create a new multi-profile mask with all supported profiles
    pub fn new() -> Self {
        let mut profile_masks = HashMap::new();
        
        for profile in TransportProfile::all() {
            profile_masks.insert(profile, ProfileMask::new(profile));
        }
        
        Self { profile_masks }
    }
    
    /// Evaluate a way for all profiles and update masks
    pub fn evaluate_way_for_all_profiles(&mut self, edge_id: EdgeId, tags: &HashMap<String, String>) -> HashMap<TransportProfile, bool> {
        let mut results = HashMap::new();
        
        for (profile, mask) in &mut self.profile_masks {
            let should_mask = mask.evaluate_way(edge_id, tags);
            results.insert(*profile, should_mask);
        }
        
        results
    }
    
    /// Get mask for a specific profile
    pub fn get_profile_mask(&self, profile: TransportProfile) -> Option<&ProfileMask> {
        self.profile_masks.get(&profile)
    }
    
    /// Get mutable mask for a specific profile
    pub fn get_profile_mask_mut(&mut self, profile: TransportProfile) -> Option<&mut ProfileMask> {
        self.profile_masks.get_mut(&profile)
    }
    
    /// Get all profile masks
    pub fn get_all_masks(&self) -> &HashMap<TransportProfile, ProfileMask> {
        &self.profile_masks
    }
    
    /// Check if a way is accessible for any profile
    pub fn is_accessible_for_any_profile(&self, edge_id: EdgeId) -> bool {
        self.profile_masks.values()
            .any(|mask| !mask.is_edge_masked(edge_id))
    }
    
    /// Get profiles that can access this edge
    pub fn get_accessible_profiles(&self, edge_id: EdgeId) -> Vec<TransportProfile> {
        self.profile_masks.iter()
            .filter(|(_, mask)| !mask.is_edge_masked(edge_id))
            .map(|(profile, _)| *profile)
            .collect()
    }
    
    /// Clear all masks
    pub fn clear_all(&mut self) {
        for mask in self.profile_masks.values_mut() {
            mask.clear();
        }
    }
    
    /// Get combined statistics for all profiles
    pub fn get_combined_stats(&self) -> HashMap<TransportProfile, MaskingStats> {
        self.profile_masks.iter()
            .map(|(profile, mask)| (*profile, mask.get_stats().clone()))
            .collect()
    }
    
    /// Validate mask consistency (ways should be masked correctly)
    pub fn validate_masks(&self, test_ways: &[(EdgeId, HashMap<String, String>)]) -> MaskValidationResult {
        let mut validation = MaskValidationResult::default();
        
        for (edge_id, tags) in test_ways {
            let access_table = AccessTruthTable::new();
            
            for profile in TransportProfile::all() {
                let expected_access = access_table.evaluate_way_access(profile, tags);
                let expected_masked = !expected_access.should_include_in_graph();
                
                if let Some(mask) = self.get_profile_mask(profile) {
                    let actual_masked = mask.is_edge_masked(*edge_id);
                    
                    if expected_masked == actual_masked {
                        validation.correct_classifications += 1;
                    } else {
                        validation.incorrect_classifications += 1;
                        validation.failures.push(MaskValidationFailure {
                            edge_id: *edge_id,
                            profile,
                            expected_masked,
                            actual_masked,
                            tags: tags.clone(),
                        });
                    }
                }
                
                validation.total_classifications += 1;
            }
        }
        
        validation
    }
}

impl Default for MultiProfileMask {
    fn default() -> Self {
        Self::new()
    }
}

/// Result of mask validation
#[derive(Debug, Clone, Default)]
pub struct MaskValidationResult {
    pub total_classifications: usize,
    pub correct_classifications: usize,
    pub incorrect_classifications: usize,
    pub failures: Vec<MaskValidationFailure>,
}

impl MaskValidationResult {
    /// Get accuracy rate as percentage
    pub fn accuracy_rate(&self) -> f64 {
        if self.total_classifications == 0 {
            0.0
        } else {
            (self.correct_classifications as f64 / self.total_classifications as f64) * 100.0
        }
    }
}

/// Details of a mask validation failure
#[derive(Debug, Clone)]
pub struct MaskValidationFailure {
    pub edge_id: EdgeId,
    pub profile: TransportProfile,
    pub expected_masked: bool,
    pub actual_masked: bool,
    pub tags: HashMap<String, String>,
}

/// M4.3 — Component Analysis
/// Profile-aware component analysis with disconnected island removal
#[derive(Debug, Clone)]
pub struct ComponentAnalyzer {
    /// Profile this analyzer is for
    profile: TransportProfile,
    /// Connected components discovered
    components: Vec<Component>,
    /// Node to component mapping
    node_to_component: HashMap<i64, ComponentId>,
    /// Statistics about component analysis
    stats: ComponentStats,
}

/// Component identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ComponentId(pub usize);

/// A connected component in the graph
#[derive(Debug, Clone)]
pub struct Component {
    /// Unique identifier for this component
    pub id: ComponentId,
    /// Nodes in this component
    pub nodes: std::collections::HashSet<i64>,
    /// Edges in this component
    pub edges: std::collections::HashSet<EdgeId>,
    /// Size rank (0 = largest component, 1 = second largest, etc.)
    pub size_rank: usize,
    /// Whether this component should be preserved
    pub is_preserved: bool,
    /// Component type classification
    pub component_type: ComponentType,
}

/// Type of connected component
#[derive(Debug, Clone, PartialEq)]
pub enum ComponentType {
    /// Main large connected component (should be preserved)
    MainComponent,
    /// Large secondary component (may be legitimate)
    SecondaryComponent,
    /// Small island that should be pruned
    SmallIsland,
    /// Single-node component (isolated node)
    IsolatedNode,
    /// Legitimate small component (e.g., ferry connection)
    LegitimateIsland,
}

/// Statistics about component analysis
#[derive(Debug, Clone, Default)]
pub struct ComponentStats {
    /// Total number of components found
    pub total_components: usize,
    /// Number of preserved components
    pub preserved_components: usize,
    /// Number of pruned components
    pub pruned_components: usize,
    /// Total nodes analyzed
    pub total_nodes: usize,
    /// Nodes in preserved components
    pub preserved_nodes: usize,
    /// Nodes in pruned components
    pub pruned_nodes: usize,
    /// Size of largest component
    pub largest_component_size: usize,
    /// Size of second largest component
    pub second_largest_component_size: usize,
}

impl ComponentStats {
    /// Get preservation rate as percentage
    pub fn preservation_rate(&self) -> f64 {
        if self.total_nodes == 0 {
            0.0
        } else {
            (self.preserved_nodes as f64 / self.total_nodes as f64) * 100.0
        }
    }
    
    /// Get pruning rate as percentage
    pub fn pruning_rate(&self) -> f64 {
        if self.total_nodes == 0 {
            0.0
        } else {
            (self.pruned_nodes as f64 / self.total_nodes as f64) * 100.0
        }
    }
}

impl ComponentAnalyzer {
    /// Create a new component analyzer for the given profile
    pub fn new(profile: TransportProfile) -> Self {
        Self {
            profile,
            components: Vec::new(),
            node_to_component: HashMap::new(),
            stats: ComponentStats::default(),
        }
    }
    
    /// Analyze connected components in a graph after profile masking
    pub fn analyze_components(&mut self, adjacency: &HashMap<i64, Vec<(i64, EdgeId)>>) {
        self.clear();
        
        let mut visited = std::collections::HashSet::new();
        
        // Find all connected components using DFS
        for &start_node in adjacency.keys() {
            if !visited.contains(&start_node) {
                let component = self.find_component(start_node, adjacency, &mut visited);
                
                if !component.nodes.is_empty() {
                    self.components.push(component);
                }
            }
        }
        
        // Sort components by size (largest first)
        self.components.sort_by(|a, b| b.nodes.len().cmp(&a.nodes.len()));
        
        // Assign size ranks and update node mappings
        for (rank, component) in self.components.iter_mut().enumerate() {
            component.size_rank = rank;
            component.id = ComponentId(rank);
            
            for &node in &component.nodes {
                self.node_to_component.insert(node, component.id);
            }
        }
        
        // Classify and determine preservation
        self.classify_and_preserve_components();
        
        // Update statistics
        self.update_statistics();
    }
    
    /// Find a connected component starting from a node using DFS
    fn find_component(&self, start_node: i64, adjacency: &HashMap<i64, Vec<(i64, EdgeId)>>, visited: &mut std::collections::HashSet<i64>) -> Component {
        let mut nodes = std::collections::HashSet::new();
        let mut edges = std::collections::HashSet::new();
        let mut stack = vec![start_node];
        
        while let Some(node) = stack.pop() {
            if visited.contains(&node) {
                continue;
            }
            
            visited.insert(node);
            nodes.insert(node);
            
            if let Some(neighbors) = adjacency.get(&node) {
                for &(neighbor, edge_id) in neighbors {
                    edges.insert(edge_id);
                    
                    if !visited.contains(&neighbor) {
                        stack.push(neighbor);
                    }
                }
            }
        }
        
        Component {
            id: ComponentId(0), // Will be set later
            nodes,
            edges,
            size_rank: 0, // Will be set later
            is_preserved: false, // Will be determined later
            component_type: ComponentType::SmallIsland, // Will be classified later
        }
    }
    
    /// Classify components and determine which should be preserved
    fn classify_and_preserve_components(&mut self) {
        if self.components.is_empty() {
            return;
        }
        
        // Configure preservation thresholds based on profile
        let (min_main_size, min_secondary_size, min_legitimate_size) = match self.profile {
            TransportProfile::Car => (100, 50, 5),    // Cars need larger networks
            TransportProfile::Bicycle => (50, 25, 3), // Bikes can use smaller networks
            TransportProfile::Foot => (20, 10, 2),    // Foot can use very small networks
        };
        
        for component in &mut self.components {
            let size = component.nodes.len();
            
            // Classify component type
            component.component_type = if component.size_rank == 0 && size >= min_main_size {
                ComponentType::MainComponent
            } else if component.size_rank == 1 && size >= min_secondary_size {
                ComponentType::SecondaryComponent
            } else if size == 1 {
                ComponentType::IsolatedNode
            } else if size >= min_legitimate_size {
                // Check if this could be a legitimate island (e.g., ferry connection)
                ComponentType::LegitimateIsland
            } else {
                ComponentType::SmallIsland
            };
            
            // Determine preservation based on type and size
            component.is_preserved = match component.component_type {
                ComponentType::MainComponent => true,
                ComponentType::SecondaryComponent => true,
                ComponentType::LegitimateIsland => Self::is_legitimate_island_static(component, self.profile),
                ComponentType::IsolatedNode => false,
                ComponentType::SmallIsland => false,
            };
        }
    }
    
    /// Check if a small component is a legitimate island that should be preserved (static version)
    fn is_legitimate_island_static(component: &Component, profile: TransportProfile) -> bool {
        // For now, preserve components with a minimum number of nodes
        // In a full implementation, this could check for:
        // - Ferry connections
        // - Isolated areas with special access (islands, private compounds)
        // - Components with important POIs
        
        let min_size = match profile {
            TransportProfile::Car => 5,
            TransportProfile::Bicycle => 3,
            TransportProfile::Foot => 2,
        };
        
        component.nodes.len() >= min_size
    }
    
    /// Update statistics after component analysis
    fn update_statistics(&mut self) {
        self.stats = ComponentStats::default();
        self.stats.total_components = self.components.len();
        
        for component in &self.components {
            let size = component.nodes.len();
            self.stats.total_nodes += size;
            
            if component.is_preserved {
                self.stats.preserved_components += 1;
                self.stats.preserved_nodes += size;
            } else {
                self.stats.pruned_components += 1;
                self.stats.pruned_nodes += size;
            }
        }
        
        // Set largest component sizes
        if !self.components.is_empty() {
            self.stats.largest_component_size = self.components[0].nodes.len();
            
            if self.components.len() > 1 {
                self.stats.second_largest_component_size = self.components[1].nodes.len();
            }
        }
    }
    
    /// Get all components
    pub fn get_components(&self) -> &[Component] {
        &self.components
    }
    
    /// Get preserved components only
    pub fn get_preserved_components(&self) -> Vec<&Component> {
        self.components.iter().filter(|c| c.is_preserved).collect()
    }
    
    /// Get pruned components only
    pub fn get_pruned_components(&self) -> Vec<&Component> {
        self.components.iter().filter(|c| !c.is_preserved).collect()
    }
    
    /// Check if a node should be preserved (is in a preserved component)
    pub fn should_preserve_node(&self, node_id: i64) -> bool {
        if let Some(component_id) = self.node_to_component.get(&node_id) {
            if let Some(component) = self.components.get(component_id.0) {
                return component.is_preserved;
            }
        }
        false
    }
    
    /// Check if an edge should be preserved (connects preserved nodes)
    pub fn should_preserve_edge(&self, from_node: i64, to_node: i64) -> bool {
        self.should_preserve_node(from_node) && self.should_preserve_node(to_node)
    }
    
    /// Get component statistics
    pub fn get_stats(&self) -> &ComponentStats {
        &self.stats
    }
    
    /// Get the transport profile for this analyzer
    pub fn get_profile(&self) -> TransportProfile {
        self.profile
    }
    
    /// Clear all analysis data
    pub fn clear(&mut self) {
        self.components.clear();
        self.node_to_component.clear();
        self.stats = ComponentStats::default();
    }
    
    /// Apply component pruning to an adjacency list
    pub fn prune_adjacency_list(&self, adjacency: &HashMap<i64, Vec<(i64, EdgeId)>>) -> HashMap<i64, Vec<(i64, EdgeId)>> {
        let mut pruned_adjacency = HashMap::new();
        
        for (node_id, neighbors) in adjacency {
            if self.should_preserve_node(*node_id) {
                let preserved_neighbors: Vec<_> = neighbors.iter()
                    .filter(|(neighbor, _)| self.should_preserve_node(*neighbor))
                    .copied()
                    .collect();
                
                if !preserved_neighbors.is_empty() {
                    pruned_adjacency.insert(*node_id, preserved_neighbors);
                }
            }
        }
        
        pruned_adjacency
    }
    
    /// Get nodes that should be pruned
    pub fn get_pruned_nodes(&self) -> std::collections::HashSet<i64> {
        let mut pruned_nodes = std::collections::HashSet::new();
        
        for component in &self.components {
            if !component.is_preserved {
                pruned_nodes.extend(&component.nodes);
            }
        }
        
        pruned_nodes
    }
}

/// M4.1 — Synthetic Junction Test Generator
/// Generates 100+ test cases covering all transport profile combinations
pub struct SyntheticJunctionTests;

impl SyntheticJunctionTests {
    /// Generate comprehensive test cases for all profiles and junction types
    pub fn generate_all_tests() -> Vec<JunctionTestCase> {
        let mut tests = Vec::new();
        
        // Basic highway combinations for each profile
        tests.extend(Self::generate_basic_highway_tests());
        
        // Access tag combinations
        tests.extend(Self::generate_access_tag_tests());
        
        // Special case combinations
        tests.extend(Self::generate_special_case_tests());
        
        // Complex multi-tag scenarios
        tests.extend(Self::generate_complex_scenarios());
        
        // Edge cases and unusual combinations
        tests.extend(Self::generate_edge_cases());
        
        tests
    }
    
    fn generate_basic_highway_tests() -> Vec<JunctionTestCase> {
        let mut tests = Vec::new();
        let highway_types = [
            "motorway", "motorway_link", "trunk", "trunk_link",
            "primary", "primary_link", "secondary", "secondary_link",
            "tertiary", "tertiary_link", "unclassified", "residential",
            "living_street", "service", "pedestrian", "track",
            "path", "footway", "bridleway", "cycleway", "steps",
            "corridor", "platform", "ferry"
        ];
        
        for highway in &highway_types {
            for profile in TransportProfile::all() {
                tests.push(JunctionTestCase {
                    name: format!("{}_on_{}", profile.name(), highway),
                    tags: [("highway".to_string(), highway.to_string())].into_iter().collect(),
                    profile,
                    expected_accessible: Self::expected_highway_access(highway, profile),
                    expected_access_level: Self::expected_highway_access_level(highway, profile),
                    description: format!("{} access on {} highway", profile.name(), highway),
                });
            }
        }
        
        tests
    }
    
    fn generate_access_tag_tests() -> Vec<JunctionTestCase> {
        let mut tests = Vec::new();
        let access_values = ["yes", "no", "private", "permissive", "destination", "customers", "delivery"];
        let base_highways = ["residential", "service", "track"];
        
        for highway in &base_highways {
            for access_value in &access_values {
                for profile in TransportProfile::all() {
                    // General access tag
                    tests.push(JunctionTestCase {
                        name: format!("{}_on_{}_access_{}", profile.name(), highway, access_value),
                        tags: [
                            ("highway".to_string(), highway.to_string()),
                            ("access".to_string(), access_value.to_string())
                        ].into_iter().collect(),
                        profile,
                        expected_accessible: Self::expected_access_allows(access_value, profile),
                        expected_access_level: AccessLevel::parse(access_value),
                        description: format!("{} with access={} on {}", profile.name(), access_value, highway),
                    });
                    
                    // Profile-specific access tag
                    let profile_tag = match profile {
                        TransportProfile::Car => "car",
                        TransportProfile::Bicycle => "bicycle", 
                        TransportProfile::Foot => "foot",
                    };
                    
                    tests.push(JunctionTestCase {
                        name: format!("{}_specific_{}_{}_on_{}", profile.name(), profile_tag, access_value, highway),
                        tags: [
                            ("highway".to_string(), highway.to_string()),
                            (profile_tag.to_string(), access_value.to_string())
                        ].into_iter().collect(),
                        profile,
                        expected_accessible: Self::expected_access_allows(access_value, profile),
                        expected_access_level: AccessLevel::parse(access_value),
                        description: format!("{} with {}={} on {}", profile.name(), profile_tag, access_value, highway),
                    });
                }
            }
        }
        
        tests
    }
    
    fn generate_special_case_tests() -> Vec<JunctionTestCase> {
        let mut tests = Vec::new();
        
        // Footway with bicycle=designated
        tests.push(JunctionTestCase {
            name: "bicycle_on_footway_designated".to_string(),
            tags: [
                ("highway".to_string(), "footway".to_string()),
                ("bicycle".to_string(), "designated".to_string())
            ].into_iter().collect(),
            profile: TransportProfile::Bicycle,
            expected_accessible: true,
            expected_access_level: AccessLevel::Yes,
            description: "Bicycle on footway with bicycle=designated".to_string(),
        });
        
        // Cycleway with foot=designated
        tests.push(JunctionTestCase {
            name: "foot_on_cycleway_designated".to_string(),
            tags: [
                ("highway".to_string(), "cycleway".to_string()),
                ("foot".to_string(), "designated".to_string())
            ].into_iter().collect(),
            profile: TransportProfile::Foot,
            expected_accessible: true,
            expected_access_level: AccessLevel::Yes,
            description: "Foot on cycleway with foot=designated".to_string(),
        });
        
        // Private service road
        tests.push(JunctionTestCase {
            name: "car_on_private_service".to_string(),
            tags: [
                ("highway".to_string(), "service".to_string()),
                ("access".to_string(), "private".to_string())
            ].into_iter().collect(),
            profile: TransportProfile::Car,
            expected_accessible: false,
            expected_access_level: AccessLevel::Private,
            description: "Car on private service road".to_string(),
        });
        
        // Track with motor_vehicle=no
        tests.push(JunctionTestCase {
            name: "car_on_track_motor_vehicle_no".to_string(),
            tags: [
                ("highway".to_string(), "track".to_string()),
                ("motor_vehicle".to_string(), "no".to_string())
            ].into_iter().collect(),
            profile: TransportProfile::Car,
            expected_accessible: false,
            expected_access_level: AccessLevel::No,
            description: "Car on track with motor_vehicle=no".to_string(),
        });
        
        // Bridleway (horses, bikes, foot - not cars)
        for profile in TransportProfile::all() {
            tests.push(JunctionTestCase {
                name: format!("{}_on_bridleway", profile.name()),
                tags: [("highway".to_string(), "bridleway".to_string())].into_iter().collect(),
                profile,
                expected_accessible: profile != TransportProfile::Car,
                expected_access_level: if profile == TransportProfile::Car { AccessLevel::No } else { AccessLevel::Yes },
                description: format!("{} on bridleway", profile.name()),
            });
        }
        
        tests
    }
    
    fn generate_complex_scenarios() -> Vec<JunctionTestCase> {
        let mut tests = Vec::new();
        
        // Tag hierarchy: specific overrides general
        tests.push(JunctionTestCase {
            name: "car_access_no_but_car_yes".to_string(),
            tags: [
                ("highway".to_string(), "residential".to_string()),
                ("access".to_string(), "no".to_string()),
                ("car".to_string(), "yes".to_string())
            ].into_iter().collect(),
            profile: TransportProfile::Car,
            expected_accessible: true,
            expected_access_level: AccessLevel::Yes,
            description: "Car with access=no but car=yes (specific override)".to_string(),
        });
        
        tests.push(JunctionTestCase {
            name: "bicycle_vehicle_no_but_bicycle_yes".to_string(),
            tags: [
                ("highway".to_string(), "residential".to_string()),
                ("vehicle".to_string(), "no".to_string()),
                ("bicycle".to_string(), "yes".to_string())
            ].into_iter().collect(),
            profile: TransportProfile::Bicycle,
            expected_accessible: true,
            expected_access_level: AccessLevel::Yes,
            description: "Bicycle with vehicle=no but bicycle=yes".to_string(),
        });
        
        // Multiple restrictions
        tests.push(JunctionTestCase {
            name: "foot_access_private_but_foot_permissive".to_string(),
            tags: [
                ("highway".to_string(), "footway".to_string()),
                ("access".to_string(), "private".to_string()),
                ("foot".to_string(), "permissive".to_string())
            ].into_iter().collect(),
            profile: TransportProfile::Foot,
            expected_accessible: true,
            expected_access_level: AccessLevel::Permissive,
            description: "Foot with access=private but foot=permissive".to_string(),
        });
        
        // Motorway with explicit bicycle=no (redundant but common)
        tests.push(JunctionTestCase {
            name: "bicycle_on_motorway_explicit_no".to_string(),
            tags: [
                ("highway".to_string(), "motorway".to_string()),
                ("bicycle".to_string(), "no".to_string())
            ].into_iter().collect(),
            profile: TransportProfile::Bicycle,
            expected_accessible: false,
            expected_access_level: AccessLevel::No,
            description: "Bicycle on motorway with explicit bicycle=no".to_string(),
        });
        
        tests
    }
    
    fn generate_edge_cases() -> Vec<JunctionTestCase> {
        let mut tests = Vec::new();
        
        // Unknown highway type
        for profile in TransportProfile::all() {
            tests.push(JunctionTestCase {
                name: format!("{}_on_unknown_highway", profile.name()),
                tags: [("highway".to_string(), "unknown_type".to_string())].into_iter().collect(),
                profile,
                expected_accessible: true, // Unknown defaults to yes
                expected_access_level: AccessLevel::Unknown, // Fixed: unknown highway types have Unknown access level
                description: format!("{} on unknown highway type", profile.name()),
            });
        }
        
        // No highway tag (non-way)
        for profile in TransportProfile::all() {
            tests.push(JunctionTestCase {
                name: format!("{}_on_non_highway", profile.name()),
                tags: [("building".to_string(), "yes".to_string())].into_iter().collect(),
                profile,
                expected_accessible: true, // No highway tag defaults to unknown highway
                expected_access_level: AccessLevel::Unknown,
                description: format!("{} on non-highway feature", profile.name()),
            });
        }
        
        // Ferry routes
        for profile in TransportProfile::all() {
            tests.push(JunctionTestCase {
                name: format!("{}_on_ferry", profile.name()),
                tags: [("highway".to_string(), "ferry".to_string())].into_iter().collect(),
                profile,
                expected_accessible: true,
                expected_access_level: AccessLevel::Yes,
                description: format!("{} on ferry route", profile.name()),
            });
        }
        
        // Contradictory tags (access=yes, specific=no)
        tests.push(JunctionTestCase {
            name: "car_access_yes_but_car_no".to_string(),
            tags: [
                ("highway".to_string(), "residential".to_string()),
                ("access".to_string(), "yes".to_string()),
                ("car".to_string(), "no".to_string())
            ].into_iter().collect(),
            profile: TransportProfile::Car,
            expected_accessible: false,
            expected_access_level: AccessLevel::No,
            description: "Car with access=yes but car=no (specific wins)".to_string(),
        });
        
        tests
    }
    
    fn expected_highway_access(highway: &str, profile: TransportProfile) -> bool {
        HighwayType::parse(highway).default_access(profile).allows_routing(profile)
    }
    
    fn expected_highway_access_level(highway: &str, profile: TransportProfile) -> AccessLevel {
        HighwayType::parse(highway).default_access(profile)
    }
    
    fn expected_access_allows(access_value: &str, profile: TransportProfile) -> bool {
        AccessLevel::parse(access_value).allows_routing(profile)
    }
    
    /// Run all synthetic tests and return results
    pub fn run_all_tests(table: &AccessTruthTable) -> SyntheticTestResults {
        let tests = Self::generate_all_tests();
        let mut results = SyntheticTestResults {
            total_tests: tests.len(),
            passed: 0,
            failed: 0,
            failures: Vec::new(),
        };
        
        for test in tests {
            let actual = table.evaluate_way_access(test.profile, &test.tags);
            
            let accessibility_matches = actual.is_accessible == test.expected_accessible;
            let access_level_matches = actual.access == test.expected_access_level || 
                                     test.expected_access_level == AccessLevel::Unknown;
            
            if accessibility_matches && access_level_matches {
                results.passed += 1;
            } else {
                results.failed += 1;
                results.failures.push(SyntheticTestFailure {
                    test_name: test.name.clone(),
                    description: test.description.clone(),
                    expected_accessible: test.expected_accessible,
                    actual_accessible: actual.is_accessible,
                    expected_access_level: test.expected_access_level,
                    actual_access_level: actual.access,
                    tags: test.tags.clone(),
                    profile: test.profile,
                });
            }
        }
        
        results
    }
}

/// Test case for synthetic junction testing
#[derive(Debug, Clone)]
pub struct JunctionTestCase {
    pub name: String,
    pub tags: HashMap<String, String>,
    pub profile: TransportProfile,
    pub expected_accessible: bool,
    pub expected_access_level: AccessLevel,
    pub description: String,
}

/// Results of running synthetic tests
#[derive(Debug, Clone)]
pub struct SyntheticTestResults {
    pub total_tests: usize,
    pub passed: usize,
    pub failed: usize,
    pub failures: Vec<SyntheticTestFailure>,
}

impl SyntheticTestResults {
    /// Get pass rate as percentage
    pub fn pass_rate(&self) -> f64 {
        if self.total_tests == 0 {
            0.0
        } else {
            (self.passed as f64 / self.total_tests as f64) * 100.0
        }
    }
    
    /// Check if all tests passed
    pub fn all_passed(&self) -> bool {
        self.failed == 0
    }
}

/// Details of a failed synthetic test
#[derive(Debug, Clone)]
pub struct SyntheticTestFailure {
    pub test_name: String,
    pub description: String,
    pub expected_accessible: bool,
    pub actual_accessible: bool,
    pub expected_access_level: AccessLevel,
    pub actual_access_level: AccessLevel,
    pub tags: HashMap<String, String>,
    pub profile: TransportProfile,
}

#[cfg(test)]
mod tests {
    use super::*;
    
    fn create_tags(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }
    
    #[test]
    fn test_transport_profile_basics() {
        assert_eq!(TransportProfile::Car.name(), "car");
        assert_eq!(TransportProfile::Bicycle.name(), "bicycle");
        assert_eq!(TransportProfile::Foot.name(), "foot");
        
        let all_profiles = TransportProfile::all();
        assert_eq!(all_profiles.len(), 3);
        assert!(all_profiles.contains(&TransportProfile::Car));
        assert!(all_profiles.contains(&TransportProfile::Bicycle));
        assert!(all_profiles.contains(&TransportProfile::Foot));
    }
    
    #[test]
    fn test_access_level_parsing() {
        assert_eq!(AccessLevel::parse("yes"), AccessLevel::Yes);
        assert_eq!(AccessLevel::parse("no"), AccessLevel::No);
        assert_eq!(AccessLevel::parse("private"), AccessLevel::Private);
        assert_eq!(AccessLevel::parse("destination"), AccessLevel::Destination);
        assert_eq!(AccessLevel::parse("unknown_value"), AccessLevel::Unknown);
    }
    
    #[test]
    fn test_access_level_routing_permissions() {
        // Yes allows all profiles
        assert!(AccessLevel::Yes.allows_routing(TransportProfile::Car));
        assert!(AccessLevel::Yes.allows_routing(TransportProfile::Bicycle));
        assert!(AccessLevel::Yes.allows_routing(TransportProfile::Foot));
        
        // No forbids all profiles
        assert!(!AccessLevel::No.allows_routing(TransportProfile::Car));
        assert!(!AccessLevel::No.allows_routing(TransportProfile::Bicycle));
        assert!(!AccessLevel::No.allows_routing(TransportProfile::Foot));
        
        // Private forbids all profiles (for public routing)
        assert!(!AccessLevel::Private.allows_routing(TransportProfile::Car));
        assert!(!AccessLevel::Private.allows_routing(TransportProfile::Bicycle));
        assert!(!AccessLevel::Private.allows_routing(TransportProfile::Foot));
        
        // Delivery allows cars only
        assert!(AccessLevel::Delivery.allows_routing(TransportProfile::Car));
        assert!(!AccessLevel::Delivery.allows_routing(TransportProfile::Bicycle));
        assert!(!AccessLevel::Delivery.allows_routing(TransportProfile::Foot));
    }
    
    #[test]
    fn test_highway_type_parsing() {
        assert_eq!(HighwayType::parse("motorway"), HighwayType::Motorway);
        assert_eq!(HighwayType::parse("primary"), HighwayType::Primary);
        assert_eq!(HighwayType::parse("residential"), HighwayType::Residential);
        assert_eq!(HighwayType::parse("footway"), HighwayType::Footway);
        assert_eq!(HighwayType::parse("cycleway"), HighwayType::Cycleway);
        
        if let HighwayType::Other(s) = HighwayType::parse("unknown") {
            assert_eq!(s, "unknown");
        } else {
            panic!("Expected Other variant");
        }
    }
    
    #[test]
    fn test_highway_default_access() {
        // Motorways - cars only
        assert_eq!(HighwayType::Motorway.default_access(TransportProfile::Car), AccessLevel::Yes);
        assert_eq!(HighwayType::Motorway.default_access(TransportProfile::Bicycle), AccessLevel::No);
        assert_eq!(HighwayType::Motorway.default_access(TransportProfile::Foot), AccessLevel::No);
        
        // Residential - all modes allowed
        assert_eq!(HighwayType::Residential.default_access(TransportProfile::Car), AccessLevel::Yes);
        assert_eq!(HighwayType::Residential.default_access(TransportProfile::Bicycle), AccessLevel::Yes);
        assert_eq!(HighwayType::Residential.default_access(TransportProfile::Foot), AccessLevel::Yes);
        
        // Footways - foot only (bikes generally not allowed)
        assert_eq!(HighwayType::Footway.default_access(TransportProfile::Car), AccessLevel::No);
        assert_eq!(HighwayType::Footway.default_access(TransportProfile::Bicycle), AccessLevel::No);
        assert_eq!(HighwayType::Footway.default_access(TransportProfile::Foot), AccessLevel::Yes);
        
        // Steps - foot only
        assert_eq!(HighwayType::Steps.default_access(TransportProfile::Car), AccessLevel::No);
        assert_eq!(HighwayType::Steps.default_access(TransportProfile::Bicycle), AccessLevel::No);
        assert_eq!(HighwayType::Steps.default_access(TransportProfile::Foot), AccessLevel::Yes);
    }
    
    #[test]
    fn test_access_truth_table_basic_evaluation() {
        let table = AccessTruthTable::new();
        
        // Test residential street - should be accessible to all
        let residential_tags = create_tags(&[("highway", "residential")]);
        
        let car_access = table.evaluate_way_access(TransportProfile::Car, &residential_tags);
        assert_eq!(car_access.access, AccessLevel::Yes);
        assert!(car_access.is_accessible);
        assert!(car_access.should_include_in_graph());
        
        let bike_access = table.evaluate_way_access(TransportProfile::Bicycle, &residential_tags);
        assert_eq!(bike_access.access, AccessLevel::Yes);
        assert!(bike_access.is_accessible);
        assert!(bike_access.should_include_in_graph());
        
        let foot_access = table.evaluate_way_access(TransportProfile::Foot, &residential_tags);
        assert_eq!(foot_access.access, AccessLevel::Yes);
        assert!(foot_access.is_accessible);
        assert!(foot_access.should_include_in_graph());
    }
    
    #[test]
    fn test_access_truth_table_motorway() {
        let table = AccessTruthTable::new();
        
        // Test motorway - cars only
        let motorway_tags = create_tags(&[("highway", "motorway")]);
        
        let car_access = table.evaluate_way_access(TransportProfile::Car, &motorway_tags);
        assert_eq!(car_access.access, AccessLevel::Yes);
        assert!(car_access.is_accessible);
        
        let bike_access = table.evaluate_way_access(TransportProfile::Bicycle, &motorway_tags);
        assert_eq!(bike_access.access, AccessLevel::No);
        assert!(!bike_access.is_accessible);
        
        let foot_access = table.evaluate_way_access(TransportProfile::Foot, &motorway_tags);
        assert_eq!(foot_access.access, AccessLevel::No);
        assert!(!foot_access.is_accessible);
    }
    
    #[test]
    fn test_access_truth_table_footway() {
        let table = AccessTruthTable::new();
        
        // Test footway - foot only by default
        let footway_tags = create_tags(&[("highway", "footway")]);
        
        let car_access = table.evaluate_way_access(TransportProfile::Car, &footway_tags);
        assert_eq!(car_access.access, AccessLevel::No);
        assert!(!car_access.is_accessible);
        
        let bike_access = table.evaluate_way_access(TransportProfile::Bicycle, &footway_tags);
        assert_eq!(bike_access.access, AccessLevel::No);
        assert!(!bike_access.is_accessible);
        
        let foot_access = table.evaluate_way_access(TransportProfile::Foot, &footway_tags);
        assert_eq!(foot_access.access, AccessLevel::Yes);
        assert!(foot_access.is_accessible);
    }
    
    #[test]
    fn test_access_truth_table_cycleway() {
        let table = AccessTruthTable::new();
        
        // Test cycleway - bicycles only by default
        let cycleway_tags = create_tags(&[("highway", "cycleway")]);
        
        let car_access = table.evaluate_way_access(TransportProfile::Car, &cycleway_tags);
        assert_eq!(car_access.access, AccessLevel::No);
        assert!(!car_access.is_accessible);
        
        let bike_access = table.evaluate_way_access(TransportProfile::Bicycle, &cycleway_tags);
        assert_eq!(bike_access.access, AccessLevel::Yes);
        assert!(bike_access.is_accessible);
        
        let foot_access = table.evaluate_way_access(TransportProfile::Foot, &cycleway_tags);
        assert_eq!(foot_access.access, AccessLevel::No);
        assert!(!foot_access.is_accessible);
    }
    
    #[test]
    fn test_access_truth_table_tag_overrides() {
        let table = AccessTruthTable::new();
        
        // Test footway with bicycle=designated
        let footway_bike_tags = create_tags(&[
            ("highway", "footway"),
            ("bicycle", "designated")
        ]);
        
        let bike_access = table.evaluate_way_access(TransportProfile::Bicycle, &footway_bike_tags);
        assert_eq!(bike_access.access, AccessLevel::Yes);
        assert!(bike_access.is_accessible);
        
        // Test cycleway with foot=designated
        let cycleway_foot_tags = create_tags(&[
            ("highway", "cycleway"),
            ("foot", "designated")
        ]);
        
        let foot_access = table.evaluate_way_access(TransportProfile::Foot, &cycleway_foot_tags);
        assert_eq!(foot_access.access, AccessLevel::Yes);
        assert!(foot_access.is_accessible);
    }
    
    #[test]
    fn test_access_truth_table_private_access() {
        let table = AccessTruthTable::new();
        
        // Test private residential street
        let private_tags = create_tags(&[
            ("highway", "residential"),
            ("access", "private")
        ]);
        
        for profile in TransportProfile::all() {
            let access = table.evaluate_way_access(profile, &private_tags);
            assert_eq!(access.access, AccessLevel::Private);
            assert!(!access.is_accessible); // Private access forbids public routing
        }
    }
    
    #[test]
    fn test_access_truth_table_tag_priority() {
        let table = AccessTruthTable::new();
        
        // Test car-specific access overriding general access
        let specific_tags = create_tags(&[
            ("highway", "residential"),
            ("access", "no"),
            ("car", "yes")
        ]);
        
        let car_access = table.evaluate_way_access(TransportProfile::Car, &specific_tags);
        assert_eq!(car_access.access, AccessLevel::Yes); // car=yes overrides access=no
        assert!(car_access.is_accessible);
        
        let bike_access = table.evaluate_way_access(TransportProfile::Bicycle, &specific_tags);
        assert_eq!(bike_access.access, AccessLevel::No); // access=no applies to bicycle
        assert!(!bike_access.is_accessible);
    }
    
    #[test]
    fn test_access_truth_table_evaluate_all_profiles() {
        let table = AccessTruthTable::new();
        
        let tags = create_tags(&[("highway", "residential")]);
        let all_access = table.evaluate_all_profiles(&tags);
        
        assert_eq!(all_access.len(), 3);
        assert!(all_access.contains_key(&TransportProfile::Car));
        assert!(all_access.contains_key(&TransportProfile::Bicycle));
        assert!(all_access.contains_key(&TransportProfile::Foot));
        
        // All should be accessible for residential
        for (_, access) in all_access {
            assert!(access.is_accessible);
        }
    }
    
    #[test]
    fn test_access_truth_table_routable_for_any_profile() {
        let table = AccessTruthTable::new();
        
        // Residential should be routable for any profile
        let residential_tags = create_tags(&[("highway", "residential")]);
        assert!(table.is_routable_for_any_profile(&residential_tags));
        
        // Footway should be routable for foot only
        let footway_tags = create_tags(&[("highway", "footway")]);
        assert!(table.is_routable_for_any_profile(&footway_tags));
        
        // Non-highway should not be routable
        let building_tags = create_tags(&[("building", "yes")]);
        assert!(!table.is_routable_for_any_profile(&building_tags));
    }
    
    #[test]
    fn test_way_access_penalties() {
        let table = AccessTruthTable::new();
        
        // Test different access levels and their penalties
        let test_cases = vec![
            ("yes", 0.0),
            ("permissive", 0.1),
            ("destination", 0.3),
            ("customers", 0.5),
            ("delivery", 0.7),
        ];
        
        for (access_value, expected_penalty) in test_cases {
            let tags = create_tags(&[
                ("highway", "residential"),
                ("access", access_value)
            ]);
            
            let access = table.evaluate_way_access(TransportProfile::Car, &tags);
            assert!((access.penalty - expected_penalty).abs() < 0.01, 
                   "Access {} should have penalty {}, got {}", 
                   access_value, expected_penalty, access.penalty);
        }
    }
    
    /// M4.1 — Test that we have 100+ synthetic junction tests as specified
    #[test]
    fn test_synthetic_junction_tests_count() {
        let tests = SyntheticJunctionTests::generate_all_tests();
        println!("Generated {} synthetic junction tests", tests.len());
        
        // M4.1 specification requires 100+ tests
        assert!(tests.len() >= 100, "M4.1 requires 100+ synthetic tests, got {}", tests.len());
        
        // Verify we have tests for all profiles
        let mut profile_counts = std::collections::HashMap::new();
        for test in &tests {
            *profile_counts.entry(test.profile).or_insert(0) += 1;
        }
        
        for profile in TransportProfile::all() {
            assert!(profile_counts.get(&profile).unwrap_or(&0) > &10, 
                   "Profile {:?} should have >10 tests, got {}", 
                   profile, profile_counts.get(&profile).unwrap_or(&0));
        }
        
        println!("Profile test distribution: {:?}", profile_counts);
    }
    
    /// M4.1 — Run all synthetic junction tests and verify they pass
    #[test]
    fn test_run_all_synthetic_junction_tests() {
        let table = AccessTruthTable::new();
        let results = SyntheticJunctionTests::run_all_tests(&table);
        
        println!("Synthetic test results: {}/{} passed ({:.1}%)", 
                results.passed, results.total_tests, results.pass_rate());
        
        if !results.failures.is_empty() {
            println!("First few failures:");
            for (i, failure) in results.failures.iter().take(5).enumerate() {
                println!("  {}. {}: expected accessible={}, got accessible={}", 
                        i + 1, failure.test_name, failure.expected_accessible, failure.actual_accessible);
                println!("     Expected access level: {:?}, got: {:?}", 
                        failure.expected_access_level, failure.actual_access_level);
                println!("     Tags: {:?}", failure.tags);
            }
        }
        
        // All synthetic tests should pass with our access truth table
        assert!(results.all_passed(), "Synthetic junction tests failed: {}/{} passed", 
               results.passed, results.total_tests);
    }
    
    /// Test specific synthetic junction scenarios
    #[test]
    fn test_specific_synthetic_scenarios() {
        let table = AccessTruthTable::new();
        
        // Test specific cases that are critical for routing
        let test_cases = vec![
            // Cars should not access footways
            (create_tags(&[("highway", "footway")]), TransportProfile::Car, false),
            // Bikes should not access motorways
            (create_tags(&[("highway", "motorway")]), TransportProfile::Bicycle, false),
            // Foot should not access motorways
            (create_tags(&[("highway", "motorway")]), TransportProfile::Foot, false),
            // All should access residential
            (create_tags(&[("highway", "residential")]), TransportProfile::Car, true),
            (create_tags(&[("highway", "residential")]), TransportProfile::Bicycle, true),
            (create_tags(&[("highway", "residential")]), TransportProfile::Foot, true),
            // Specific overrides general
            (create_tags(&[("highway", "residential"), ("access", "no"), ("car", "yes")]), TransportProfile::Car, true),
            (create_tags(&[("highway", "residential"), ("access", "no"), ("bicycle", "yes")]), TransportProfile::Bicycle, true),
        ];
        
        for (tags, profile, expected_accessible) in test_cases {
            let access = table.evaluate_way_access(profile, &tags);
            assert_eq!(access.is_accessible, expected_accessible,
                      "Profile {:?} on tags {:?} should be accessible={}, got {}",
                      profile, tags, expected_accessible, access.is_accessible);
        }
    }
    
    /// M4.2 — Test profile mask basic functionality
    #[test]
    fn test_profile_mask_basic() {
        let mut car_mask = ProfileMask::new(TransportProfile::Car);
        assert_eq!(car_mask.get_profile(), TransportProfile::Car);
        
        // Test masking a footway (should be masked for cars)
        let footway_tags = create_tags(&[("highway", "footway")]);
        let should_mask = car_mask.evaluate_way(EdgeId(1), &footway_tags);
        assert!(should_mask, "Footway should be masked for cars");
        assert!(car_mask.is_edge_masked(EdgeId(1)));
        
        // Test not masking a residential street (should be accessible for cars)
        let residential_tags = create_tags(&[("highway", "residential")]);
        let should_mask = car_mask.evaluate_way(EdgeId(2), &residential_tags);
        assert!(!should_mask, "Residential should not be masked for cars");
        assert!(!car_mask.is_edge_masked(EdgeId(2)));
        
        // Check statistics
        let stats = car_mask.get_stats();
        assert_eq!(stats.total_ways, 2);
        assert_eq!(stats.accessible_ways, 1);
        assert_eq!(stats.masked_ways, 1);
        assert_eq!(stats.masking_rate(), 50.0);
        assert_eq!(stats.accessibility_rate(), 50.0);
    }
    
    /// M4.2 — Test multi-profile mask functionality
    #[test]
    fn test_multi_profile_mask() {
        let mut multi_mask = MultiProfileMask::new();
        
        // Test different ways with different access patterns
        let test_ways = vec![
            (EdgeId(1), create_tags(&[("highway", "motorway")])),        // Cars only
            (EdgeId(2), create_tags(&[("highway", "footway")])),         // Foot only
            (EdgeId(3), create_tags(&[("highway", "cycleway")])),        // Bicycles only
            (EdgeId(4), create_tags(&[("highway", "residential")])),     // All profiles
            (EdgeId(5), create_tags(&[("highway", "steps")])),           // Foot only
        ];
        
        for (edge_id, tags) in &test_ways {
            multi_mask.evaluate_way_for_all_profiles(*edge_id, tags);
        }
        
        // Test accessibility for specific edges
        
        // Motorway - accessible for cars only
        let motorway_profiles = multi_mask.get_accessible_profiles(EdgeId(1));
        assert!(motorway_profiles.contains(&TransportProfile::Car));
        assert!(!motorway_profiles.contains(&TransportProfile::Bicycle));
        assert!(!motorway_profiles.contains(&TransportProfile::Foot));
        
        // Footway - accessible for foot only
        let footway_profiles = multi_mask.get_accessible_profiles(EdgeId(2));
        assert!(!footway_profiles.contains(&TransportProfile::Car));
        assert!(!footway_profiles.contains(&TransportProfile::Bicycle));
        assert!(footway_profiles.contains(&TransportProfile::Foot));
        
        // Cycleway - accessible for bicycles only
        let cycleway_profiles = multi_mask.get_accessible_profiles(EdgeId(3));
        assert!(!cycleway_profiles.contains(&TransportProfile::Car));
        assert!(cycleway_profiles.contains(&TransportProfile::Bicycle));
        assert!(!cycleway_profiles.contains(&TransportProfile::Foot));
        
        // Residential - accessible for all
        let residential_profiles = multi_mask.get_accessible_profiles(EdgeId(4));
        assert!(residential_profiles.contains(&TransportProfile::Car));
        assert!(residential_profiles.contains(&TransportProfile::Bicycle));
        assert!(residential_profiles.contains(&TransportProfile::Foot));
        
        // Steps - accessible for foot only
        let steps_profiles = multi_mask.get_accessible_profiles(EdgeId(5));
        assert!(!steps_profiles.contains(&TransportProfile::Car));
        assert!(!steps_profiles.contains(&TransportProfile::Bicycle));
        assert!(steps_profiles.contains(&TransportProfile::Foot));
    }
    
    /// M4.2 — Test profile mask statistics and validation
    #[test]
    fn test_profile_mask_validation() {
        let mut multi_mask = MultiProfileMask::new();
        
        // Create test data with known access patterns
        let test_ways = vec![
            (EdgeId(1), create_tags(&[("highway", "motorway")])),
            (EdgeId(2), create_tags(&[("highway", "footway")])),
            (EdgeId(3), create_tags(&[("highway", "residential")])),
            (EdgeId(4), create_tags(&[("highway", "cycleway")])),
            (EdgeId(5), create_tags(&[("highway", "service"), ("access", "private")])),
        ];
        
        // Evaluate all ways
        for (edge_id, tags) in &test_ways {
            multi_mask.evaluate_way_for_all_profiles(*edge_id, tags);
        }
        
        // Validate masks
        let validation = multi_mask.validate_masks(&test_ways);
        println!("Mask validation: {}/{} correct ({:.1}%)", 
                validation.correct_classifications, 
                validation.total_classifications, 
                validation.accuracy_rate());
        
        // Should have high accuracy
        assert!(validation.accuracy_rate() >= 95.0, 
               "Mask validation should have >=95% accuracy, got {:.1}%", 
               validation.accuracy_rate());
        
        // Check combined statistics
        let stats = multi_mask.get_combined_stats();
        for (profile, profile_stats) in stats {
            println!("Profile {:?}: {}/{} accessible ({:.1}%)", 
                    profile, 
                    profile_stats.accessible_ways, 
                    profile_stats.total_ways, 
                    profile_stats.accessibility_rate());
            
            assert_eq!(profile_stats.total_ways, test_ways.len());
        }
    }
    
    /// M4.2 — Test adjacency list filtering
    #[test]
    fn test_adjacency_list_filtering() {
        let mut car_mask = ProfileMask::new(TransportProfile::Car);
        
        // Mask some edges
        car_mask.evaluate_way(EdgeId(1), &create_tags(&[("highway", "footway")])); // Should mask
        car_mask.evaluate_way(EdgeId(2), &create_tags(&[("highway", "residential")])); // Should not mask
        car_mask.evaluate_way(EdgeId(3), &create_tags(&[("highway", "cycleway")])); // Should mask
        
        // Create test adjacency list
        let mut adjacency = HashMap::new();
        adjacency.insert(100, vec![(101, EdgeId(1)), (102, EdgeId(2))]); // Node 100 -> footway, residential
        adjacency.insert(101, vec![(103, EdgeId(3))]); // Node 101 -> cycleway
        adjacency.insert(102, vec![(104, EdgeId(2))]); // Node 102 -> residential
        
        // Apply masking
        let filtered = car_mask.apply_to_adjacency_list(&adjacency);
        
        // Check results
        assert!(filtered.contains_key(&100)); // Should remain (has accessible edge 2)
        assert!(!filtered.contains_key(&101)); // Should be removed (only has masked edge 3)
        assert!(filtered.contains_key(&102)); // Should remain (has accessible edge 2)
        
        // Node 100 should only have edge 2 (residential)
        let node_100_edges = filtered.get(&100).unwrap();
        assert_eq!(node_100_edges.len(), 1);
        assert_eq!(node_100_edges[0], (102, EdgeId(2)));
    }
    
    /// M4.2 — Test edge filtering
    #[test]
    fn test_edge_filtering() {
        let mut bike_mask = ProfileMask::new(TransportProfile::Bicycle);
        
        // Evaluate some ways
        bike_mask.evaluate_way(EdgeId(1), &create_tags(&[("highway", "motorway")])); // Should mask
        bike_mask.evaluate_way(EdgeId(2), &create_tags(&[("highway", "cycleway")])); // Should not mask
        bike_mask.evaluate_way(EdgeId(3), &create_tags(&[("highway", "residential")])); // Should not mask
        
        // Test edge filtering
        let all_edges = vec![
            (EdgeId(1), "motorway_data"),
            (EdgeId(2), "cycleway_data"),
            (EdgeId(3), "residential_data"),
        ];
        
        let filtered_edges = bike_mask.filter_edges(&all_edges);
        
        // Should only include cycleway and residential (EdgeId 2 and 3)
        assert_eq!(filtered_edges.len(), 2);
        assert!(filtered_edges.iter().any(|(id, _)| *id == EdgeId(2)));
        assert!(filtered_edges.iter().any(|(id, _)| *id == EdgeId(3)));
        assert!(!filtered_edges.iter().any(|(id, _)| *id == EdgeId(1)));
    }
    
    /// M4.3 — Test component analysis basic functionality
    #[test]
    fn test_component_analysis_basic() {
        let mut analyzer = ComponentAnalyzer::new(TransportProfile::Car);
        assert_eq!(analyzer.get_profile(), TransportProfile::Car);
        
        // Create a simple graph with two components
        // Component 1: nodes 1-3 (large, should be preserved)
        // Component 2: node 4 (isolated, should be pruned)
        let mut adjacency = HashMap::new();
        adjacency.insert(1, vec![(2, EdgeId(1)), (3, EdgeId(2))]);
        adjacency.insert(2, vec![(1, EdgeId(1)), (3, EdgeId(3))]);
        adjacency.insert(3, vec![(1, EdgeId(2)), (2, EdgeId(3))]);
        adjacency.insert(4, vec![]); // Isolated node
        
        analyzer.analyze_components(&adjacency);
        
        let components = analyzer.get_components();
        assert_eq!(components.len(), 2);
        
        // First component should be the largest (nodes 1,2,3)
        assert_eq!(components[0].nodes.len(), 3);
        assert!(components[0].nodes.contains(&1));
        assert!(components[0].nodes.contains(&2));
        assert!(components[0].nodes.contains(&3));
        assert_eq!(components[0].component_type, ComponentType::SmallIsland); // Small for car threshold
        
        // Second component should be the isolated node (node 4)  
        assert_eq!(components[1].nodes.len(), 1);
        assert!(components[1].nodes.contains(&4));
        assert_eq!(components[1].component_type, ComponentType::IsolatedNode);
        
        // Check statistics
        let stats = analyzer.get_stats();
        assert_eq!(stats.total_components, 2);
        assert_eq!(stats.total_nodes, 4);
        assert_eq!(stats.largest_component_size, 3);
        assert_eq!(stats.second_largest_component_size, 1);
    }
    
    /// M4.3 — Test component analysis with different profiles
    #[test]
    fn test_component_analysis_profiles() {
        // Create a medium-sized component that will be treated differently by each profile
        let mut adjacency = HashMap::new();
        for i in 1..=20 {
            let neighbors = if i < 20 {
                vec![(i + 1, EdgeId(i))]
            } else {
                vec![(1, EdgeId(20))] // Connect back to form a cycle
            };
            adjacency.insert(i, neighbors);
        }
        
        // Add some isolated nodes
        adjacency.insert(100, vec![]);
        adjacency.insert(101, vec![]);
        
        // Test with each profile
        for profile in TransportProfile::all() {
            let mut analyzer = ComponentAnalyzer::new(profile);
            analyzer.analyze_components(&adjacency);
            
            let components = analyzer.get_components();
            let stats = analyzer.get_stats();
            
            println!("Profile {:?}: {} components, largest size: {}, preservation rate: {:.1}%", 
                    profile, stats.total_components, stats.largest_component_size, stats.preservation_rate());
            
            // Should find the main component plus isolated nodes
            assert_eq!(stats.total_components, 3); // Main cycle + 2 isolated nodes
            assert_eq!(stats.largest_component_size, 20);
            
            // Check preservation thresholds differ by profile
            let main_component = &components[0];
            match profile {
                TransportProfile::Car => {
                    // 20 nodes < 100 threshold, but >= 5 for legitimate island
                    assert_eq!(main_component.component_type, ComponentType::LegitimateIsland);
                }
                TransportProfile::Bicycle => {
                    // 20 nodes < 50 threshold, but >= 3 for legitimate island
                    assert_eq!(main_component.component_type, ComponentType::LegitimateIsland);
                }
                TransportProfile::Foot => {
                    // 20 nodes >= 20 threshold, so it's the main component for foot
                    assert_eq!(main_component.component_type, ComponentType::MainComponent);
                }
            }
        }
    }
    
    /// M4.3 — Test component preservation and pruning
    #[test]
    fn test_component_preservation() {
        let mut analyzer = ComponentAnalyzer::new(TransportProfile::Foot); // Foot has lower thresholds
        
        // Create multiple components of different sizes
        let mut adjacency = HashMap::new();
        
        // Large main component (nodes 1-25)
        for i in 1..=25 {
            let neighbors = if i < 25 {
                vec![(i + 1, EdgeId(i))]
            } else {
                vec![(1, EdgeId(25))]
            };
            adjacency.insert(i, neighbors);
        }
        
        // Medium secondary component (nodes 50-60)
        for i in 50..=60 {
            let neighbors = if i < 60 {
                vec![(i + 1, EdgeId(i + 30))]
            } else {
                vec![(50, EdgeId(40))]
            };
            adjacency.insert(i, neighbors);
        }
        
        // Small legitimate island (nodes 100-102)
        adjacency.insert(100, vec![(101, EdgeId(100))]);
        adjacency.insert(101, vec![(100, EdgeId(100)), (102, EdgeId(101))]);
        adjacency.insert(102, vec![(101, EdgeId(101))]);
        
        // Tiny island to be pruned (node 200)
        adjacency.insert(200, vec![]);
        
        analyzer.analyze_components(&adjacency);
        
        let preserved = analyzer.get_preserved_components();
        let pruned = analyzer.get_pruned_components();
        let stats = analyzer.get_stats();
        
        println!("Found {} components: {} preserved, {} pruned", 
                stats.total_components, preserved.len(), pruned.len());
        
        // Should preserve main component and secondary component
        assert!(preserved.len() >= 2, "Should preserve at least main and secondary components");
        
        // Should prune isolated nodes
        assert!(pruned.len() >= 1, "Should prune at least isolated nodes");
        
        // Test specific preservation decisions
        assert!(analyzer.should_preserve_node(1), "Node 1 (main component) should be preserved");
        assert!(analyzer.should_preserve_node(50), "Node 50 (secondary component) should be preserved");
        assert!(!analyzer.should_preserve_node(200), "Node 200 (isolated) should be pruned");
        
        // Test edge preservation
        assert!(analyzer.should_preserve_edge(1, 2), "Edge 1-2 (main component) should be preserved");
        assert!(!analyzer.should_preserve_edge(1, 200), "Edge 1-200 (to pruned node) should not be preserved");
    }
    
    /// M4.3 — Test adjacency list pruning
    #[test]
    fn test_adjacency_list_pruning() {
        let mut analyzer = ComponentAnalyzer::new(TransportProfile::Bicycle);
        
        // Create adjacency list with mixed components
        let mut adjacency = HashMap::new();
        
        // Main component (nodes 1-30, should be preserved for bikes)
        for i in 1..=30 {
            let neighbors = if i < 30 {
                vec![(i + 1, EdgeId(i))]
            } else {
                vec![(1, EdgeId(30))]
            };
            adjacency.insert(i, neighbors);
        }
        
        // Small island to be pruned (nodes 100-101)
        adjacency.insert(100, vec![(101, EdgeId(100))]);
        adjacency.insert(101, vec![(100, EdgeId(100))]);
        
        analyzer.analyze_components(&adjacency);
        
        // Apply pruning
        let pruned_adjacency = analyzer.prune_adjacency_list(&adjacency);
        
        // Should preserve main component
        for i in 1..=30 {
            assert!(pruned_adjacency.contains_key(&i), 
                   "Main component node {} should be preserved", i);
        }
        
        // Should prune small island
        assert!(!pruned_adjacency.contains_key(&100), 
               "Small island node 100 should be pruned");
        assert!(!pruned_adjacency.contains_key(&101), 
               "Small island node 101 should be pruned");
        
        // Check that pruned nodes are correctly identified
        let pruned_nodes = analyzer.get_pruned_nodes();
        assert!(pruned_nodes.contains(&100));
        assert!(pruned_nodes.contains(&101));
        assert!(!pruned_nodes.contains(&1));
    }
    
    /// M4.3 — Test component statistics
    #[test]
    fn test_component_statistics() {
        let mut analyzer = ComponentAnalyzer::new(TransportProfile::Car);
        
        // Create test graph with known structure
        let mut adjacency = HashMap::new();
        
        // Large component (200 nodes) - should be preserved for cars
        for i in 1..=200 {
            adjacency.insert(i, vec![(if i == 200 { 1 } else { i + 1 }, EdgeId(i))]);
        }
        
        // Medium component (75 nodes) - should be preserved as secondary
        for i in 300..=374 {
            adjacency.insert(i, vec![(if i == 374 { 300 } else { i + 1 }, EdgeId(i + 200))]);
        }
        
        // Small components to be pruned (5 nodes each)
        for base in [500, 600] {
            for i in 0..5 {
                adjacency.insert(base + i, vec![(if i == 4 { base } else { base + i + 1 }, EdgeId(base + i))]);
            }
        }
        
        analyzer.analyze_components(&adjacency);
        let stats = analyzer.get_stats();
        
        println!("Component statistics:");
        println!("  Total components: {}", stats.total_components);
        println!("  Total nodes: {}", stats.total_nodes);
        println!("  Preserved: {} components, {} nodes ({:.1}%)", 
                stats.preserved_components, stats.preserved_nodes, stats.preservation_rate());
        println!("  Pruned: {} components, {} nodes ({:.1}%)", 
                stats.pruned_components, stats.pruned_nodes, stats.pruning_rate());
        println!("  Largest component: {} nodes", stats.largest_component_size);
        println!("  Second largest: {} nodes", stats.second_largest_component_size);
        
        // Verify statistics
        assert_eq!(stats.total_components, 4); // 200 + 75 + 5 + 5 nodes
        assert_eq!(stats.total_nodes, 285);
        assert_eq!(stats.largest_component_size, 200);
        assert_eq!(stats.second_largest_component_size, 75);
        
        // Should preserve main and secondary components for cars (200 and 75 nodes are both above thresholds)
        // Small components (5 nodes each) are above the legitimate island threshold (5) for cars
        assert_eq!(stats.preserved_components, 4); // All components preserved
        assert_eq!(stats.preserved_nodes, 285); // 200 + 75 + 5 + 5
        assert_eq!(stats.pruned_components, 0);
        assert_eq!(stats.pruned_nodes, 0);
        
        assert!((stats.preservation_rate() - 100.0).abs() < 0.1); // 100%
        assert!((stats.pruning_rate() - 0.0).abs() < 0.1); // 0%
    }
}