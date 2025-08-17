//! Tag filtering and sieving for routing-relevant OSM data
//!
//! Provides comprehensive truth tables and filtering logic for 
//! routing-specific OSM tags based on established routing engines.

use std::collections::{HashMap, HashSet};
use serde::{Deserialize, Serialize};

/// Highway classification for routing priority
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum HighwayClass {
    Motorway,
    Trunk,
    Primary,
    Secondary,
    Tertiary,
    Unclassified,
    Residential,
    Service,
    Track,
    Path,
    Footway,
    Cycleway,
    Steps,
    Ferry,
    Other(String),
}

/// Access permission levels
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AccessLevel {
    Yes,
    No,
    Private,
    Permissive,
    Destination,
    Customers,
    Delivery,
    Agricultural,
    Forestry,
    Emergency,
}

/// Vehicle profiles for routing
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum VehicleProfile {
    Car,
    Bicycle,
    Foot,
    Motorcycle,
    Bus,
    Hgv,
    Emergency,
    Delivery,
    Agricultural,
    Forestry,
}

/// Tag truth table for routing relevance determination
#[derive(Debug, Clone)]
pub struct TagTruthTable {
    /// Highway tags that make ways routable
    highway_whitelist: HashSet<String>,
    /// Access-related tags that affect routing
    access_tags: HashSet<String>,
    /// Restriction tags that modify routing behavior
    restriction_tags: HashSet<String>,
    /// Physical constraint tags (weight, height, etc.)
    constraint_tags: HashSet<String>,
    /// Tags that affect routing cost/speed
    speed_tags: HashSet<String>,
    /// Surface/quality tags affecting traversability
    surface_tags: HashSet<String>,
}

impl Default for TagTruthTable {
    fn default() -> Self {
        Self::new()
    }
}

impl TagTruthTable {
    /// Create new truth table with comprehensive routing tag sets
    pub fn new() -> Self {
        let highway_whitelist = [
            // Major roads
            "motorway", "motorway_link", "trunk", "trunk_link",
            "primary", "primary_link", "secondary", "secondary_link",
            "tertiary", "tertiary_link", "unclassified", "residential",
            // Service roads
            "service", "living_street", "pedestrian",
            // Tracks and paths
            "track", "path", "footway", "bridleway", "cycleway",
            "steps", "corridor", "platform",
            // Specialized
            "ferry", "aerialway", "busway", "bus_guideway",
            "escape", "raceway", "road", "via_ferrata"
        ].iter().map(|s| s.to_string()).collect();

        let access_tags = [
            "access", "vehicle", "motor_vehicle", "motorcar", "car",
            "bicycle", "foot", "pedestrian", "motorcycle", "moped",
            "mofa", "hgv", "goods", "bus", "taxi", "psv", "emergency",
            "delivery", "agricultural", "forestry", "destination",
            "residents", "customers", "permit", "disabled"
        ].iter().map(|s| s.to_string()).collect();

        let restriction_tags = [
            "oneway", "oneway:bicycle", "oneway:psv", "junction",
            "barrier", "bollard", "gate", "lift_gate", "cycle_barrier",
            "toll", "ferry", "bridge", "tunnel", "layer", "level",
            "restriction", "except", "conditional"
        ].iter().map(|s| s.to_string()).collect();

        let constraint_tags = [
            "maxspeed", "maxweight", "maxaxleload", "maxheight",
            "maxwidth", "maxlength", "min_height", "min_width",
            "weight", "axleload", "height", "width", "length",
            "lanes", "turn:lanes", "change:lanes"
        ].iter().map(|s| s.to_string()).collect();

        let speed_tags = [
            "maxspeed", "maxspeed:forward", "maxspeed:backward",
            "maxspeed:advisory", "zone:maxspeed", "source:maxspeed",
            "smoothness", "surface", "tracktype", "mtb:scale",
            "sac_scale", "trail_visibility", "incline", "ford"
        ].iter().map(|s| s.to_string()).collect();

        let surface_tags = [
            "surface", "tracktype", "smoothness", "paved",
            "unpaved", "compacted", "gravel", "asphalt", "concrete",
            "paving_stones", "sett", "cobblestone", "metal", "wood",
            "grass", "gravel", "sand", "mud", "snow", "ice"
        ].iter().map(|s| s.to_string()).collect();

        Self {
            highway_whitelist,
            access_tags,
            restriction_tags,
            constraint_tags,
            speed_tags,
            surface_tags,
        }
    }

    /// Check if a way is potentially routable based on its tags
    pub fn is_routable_way(&self, tags: &HashMap<String, String>) -> bool {
        // Must have highway tag
        if let Some(highway) = tags.get("highway") {
            if self.highway_whitelist.contains(highway) {
                return true;
            }
        }
        
        // Check for ferry routes
        if tags.get("route") == Some(&"ferry".to_string()) {
            return true;
        }
        
        false
    }

    /// Check if tags are routing-relevant for any primitive type
    pub fn is_routing_relevant(&self, tags: &HashMap<String, String>) -> bool {
        // Check highway tags
        if tags.keys().any(|k| k == "highway") {
            return true;
        }
        
        // Check access tags
        if tags.keys().any(|k| self.access_tags.contains(k)) {
            return true;
        }
        
        // Check restriction tags
        if tags.keys().any(|k| self.restriction_tags.contains(k)) {
            return true;
        }
        
        // Check constraint tags
        if tags.keys().any(|k| self.constraint_tags.contains(k)) {
            return true;
        }
        
        // Check speed/surface tags
        if tags.keys().any(|k| self.speed_tags.contains(k) || self.surface_tags.contains(k)) {
            return true;
        }
        
        // Check special relation types
        if let Some(rel_type) = tags.get("type") {
            if matches!(rel_type.as_str(), "route" | "restriction" | "multipolygon") {
                return true;
            }
        }
        
        false
    }

    /// Classify highway type for routing priority
    pub fn classify_highway(&self, highway: &str) -> HighwayClass {
        match highway {
            "motorway" | "motorway_link" => HighwayClass::Motorway,
            "trunk" | "trunk_link" => HighwayClass::Trunk,
            "primary" | "primary_link" => HighwayClass::Primary,
            "secondary" | "secondary_link" => HighwayClass::Secondary,
            "tertiary" | "tertiary_link" => HighwayClass::Tertiary,
            "unclassified" => HighwayClass::Unclassified,
            "residential" | "living_street" => HighwayClass::Residential,
            "service" => HighwayClass::Service,
            "track" => HighwayClass::Track,
            "path" | "bridleway" => HighwayClass::Path,
            "footway" | "pedestrian" => HighwayClass::Footway,
            "cycleway" => HighwayClass::Cycleway,
            "steps" => HighwayClass::Steps,
            "ferry" => HighwayClass::Ferry,
            other => HighwayClass::Other(other.to_string()),
        }
    }

    /// Parse access value into structured enum
    pub fn parse_access(&self, access_value: &str) -> AccessLevel {
        match access_value {
            "yes" | "designated" | "official" => AccessLevel::Yes,
            "no" | "none" => AccessLevel::No,
            "private" => AccessLevel::Private,
            "permissive" | "permit" => AccessLevel::Permissive,
            "destination" => AccessLevel::Destination,
            "customers" => AccessLevel::Customers,
            "delivery" => AccessLevel::Delivery,
            "agricultural" => AccessLevel::Agricultural,
            "forestry" => AccessLevel::Forestry,
            "emergency" => AccessLevel::Emergency,
            _ => AccessLevel::No,
        }
    }

    /// Extract all routing-relevant tags from a tag set
    pub fn extract_routing_tags(&self, tags: &HashMap<String, String>) -> HashMap<String, String> {
        let mut routing_tags = HashMap::new();
        
        for (key, value) in tags {
            if self.is_routing_tag(key) {
                routing_tags.insert(key.clone(), value.clone());
            }
        }
        
        routing_tags
    }

    /// Check if a specific tag key is routing-relevant
    fn is_routing_tag(&self, key: &str) -> bool {
        self.access_tags.contains(key) ||
        self.restriction_tags.contains(key) ||
        self.constraint_tags.contains(key) ||
        self.speed_tags.contains(key) ||
        self.surface_tags.contains(key) ||
        key == "highway" ||
        key == "route" ||
        key == "type"
    }
}

/// Tag sieve for filtering OSM data to routing-relevant elements
pub struct TagSieve {
    truth_table: TagTruthTable,
    /// Vehicle profiles to consider
    _profiles: Vec<VehicleProfile>,
}

impl Default for TagSieve {
    fn default() -> Self {
        Self::new()
    }
}

impl TagSieve {
    /// Create a new tag sieve with default routing profiles
    pub fn new() -> Self {
        Self {
            truth_table: TagTruthTable::new(),
            _profiles: vec![
                VehicleProfile::Car,
                VehicleProfile::Bicycle,
                VehicleProfile::Foot,
            ],
        }
    }
    
    /// Create sieve for specific vehicle profiles
    pub fn for_profiles(profiles: Vec<VehicleProfile>) -> Self {
        Self {
            truth_table: TagTruthTable::new(),
            _profiles: profiles,
        }
    }
    
    /// Filter way based on routing relevance
    pub fn filter_way(&self, id: i64, nodes: &[i64], tags: &HashMap<String, String>) -> Option<FilteredWay> {
        if self.truth_table.is_routable_way(tags) {
            let routing_tags = self.truth_table.extract_routing_tags(tags);
            let highway_class = tags.get("highway")
                .map(|h| self.truth_table.classify_highway(h));
            
            Some(FilteredWay {
                id,
                nodes: nodes.to_vec(),
                highway_class,
                routing_tags,
            })
        } else {
            None
        }
    }
    
    /// Filter node based on routing relevance
    pub fn filter_node(&self, id: i64, lat: f64, lon: f64, tags: &HashMap<String, String>) -> Option<FilteredNode> {
        if self.truth_table.is_routing_relevant(tags) {
            let routing_tags = self.truth_table.extract_routing_tags(tags);
            
            Some(FilteredNode {
                id,
                lat,
                lon,
                routing_tags,
            })
        } else {
            None
        }
    }
    
    /// Filter relation based on routing relevance
    pub fn filter_relation(&self, id: i64, members: &[crate::pbf::RelationMember], tags: &HashMap<String, String>) -> Option<FilteredRelation> {
        if self.truth_table.is_routing_relevant(tags) {
            let routing_tags = self.truth_table.extract_routing_tags(tags);
            
            Some(FilteredRelation {
                id,
                members: members.to_vec(),
                routing_tags,
            })
        } else {
            None
        }
    }
    
    /// Get statistics about tag filtering
    pub fn get_truth_table(&self) -> &TagTruthTable {
        &self.truth_table
    }
}

/// Filtered way with routing-relevant information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilteredWay {
    pub id: i64,
    pub nodes: Vec<i64>,
    pub highway_class: Option<HighwayClass>,
    pub routing_tags: HashMap<String, String>,
}

/// Filtered node with routing-relevant information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilteredNode {
    pub id: i64,
    pub lat: f64,
    pub lon: f64,
    pub routing_tags: HashMap<String, String>,
}

/// Filtered relation with routing-relevant information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilteredRelation {
    pub id: i64,
    pub members: Vec<crate::pbf::RelationMember>,
    pub routing_tags: HashMap<String, String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    
    fn create_test_tags(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }
    
    #[test]
    fn test_highway_classification() {
        let table = TagTruthTable::new();
        
        assert_eq!(table.classify_highway("motorway"), HighwayClass::Motorway);
        assert_eq!(table.classify_highway("primary"), HighwayClass::Primary);
        assert_eq!(table.classify_highway("residential"), HighwayClass::Residential);
        assert_eq!(table.classify_highway("footway"), HighwayClass::Footway);
        assert_eq!(table.classify_highway("cycleway"), HighwayClass::Cycleway);
        assert_eq!(table.classify_highway("unknown"), HighwayClass::Other("unknown".to_string()));
    }
    
    #[test]
    fn test_access_parsing() {
        let table = TagTruthTable::new();
        
        assert_eq!(table.parse_access("yes"), AccessLevel::Yes);
        assert_eq!(table.parse_access("no"), AccessLevel::No);
        assert_eq!(table.parse_access("private"), AccessLevel::Private);
        assert_eq!(table.parse_access("destination"), AccessLevel::Destination);
        assert_eq!(table.parse_access("unknown"), AccessLevel::No);
    }
    
    #[test]
    fn test_routable_way_detection() {
        let table = TagTruthTable::new();
        
        // Highway ways should be routable
        let highway_tags = create_test_tags(&[("highway", "primary")]);
        assert!(table.is_routable_way(&highway_tags));
        
        let residential_tags = create_test_tags(&[("highway", "residential")]);
        assert!(table.is_routable_way(&residential_tags));
        
        let footway_tags = create_test_tags(&[("highway", "footway")]);
        assert!(table.is_routable_way(&footway_tags));
        
        // Ferry routes should be routable
        let ferry_tags = create_test_tags(&[("route", "ferry")]);
        assert!(table.is_routable_way(&ferry_tags));
        
        // Non-highway ways should not be routable
        let building_tags = create_test_tags(&[("building", "yes")]);
        assert!(!table.is_routable_way(&building_tags));
        
        let landuse_tags = create_test_tags(&[("landuse", "forest")]);
        assert!(!table.is_routable_way(&landuse_tags));
    }
    
    #[test]
    fn test_routing_relevance_detection() {
        let table = TagTruthTable::new();
        
        // Highway tags should be routing-relevant
        let highway_tags = create_test_tags(&[("highway", "secondary")]);
        assert!(table.is_routing_relevant(&highway_tags));
        
        // Access tags should be routing-relevant
        let access_tags = create_test_tags(&[("access", "private")]);
        assert!(table.is_routing_relevant(&access_tags));
        
        let bicycle_tags = create_test_tags(&[("bicycle", "no")]);
        assert!(table.is_routing_relevant(&bicycle_tags));
        
        // Restriction tags should be routing-relevant
        let oneway_tags = create_test_tags(&[("oneway", "yes")]);
        assert!(table.is_routing_relevant(&oneway_tags));
        
        let barrier_tags = create_test_tags(&[("barrier", "gate")]);
        assert!(table.is_routing_relevant(&barrier_tags));
        
        // Constraint tags should be routing-relevant
        let maxspeed_tags = create_test_tags(&[("maxspeed", "50")]);
        assert!(table.is_routing_relevant(&maxspeed_tags));
        
        let maxweight_tags = create_test_tags(&[("maxweight", "7.5")]);
        assert!(table.is_routing_relevant(&maxweight_tags));
        
        // Surface tags should be routing-relevant
        let surface_tags = create_test_tags(&[("surface", "gravel")]);
        assert!(table.is_routing_relevant(&surface_tags));
        
        // Routing relations should be relevant
        let route_relation = create_test_tags(&[("type", "route")]);
        assert!(table.is_routing_relevant(&route_relation));
        
        let restriction_relation = create_test_tags(&[("type", "restriction")]);
        assert!(table.is_routing_relevant(&restriction_relation));
        
        // Non-routing tags should not be relevant
        let building_tags = create_test_tags(&[("building", "residential")]);
        assert!(!table.is_routing_relevant(&building_tags));
        
        let amenity_tags = create_test_tags(&[("amenity", "restaurant")]);
        assert!(!table.is_routing_relevant(&amenity_tags));
    }
    
    #[test]
    fn test_extract_routing_tags() {
        let table = TagTruthTable::new();
        
        let mixed_tags = create_test_tags(&[
            ("highway", "primary"),
            ("name", "Main Street"),
            ("maxspeed", "50"),
            ("building", "yes"),
            ("oneway", "yes"),
            ("amenity", "parking"),
        ]);
        
        let routing_tags = table.extract_routing_tags(&mixed_tags);
        
        // Should include routing-relevant tags
        assert!(routing_tags.contains_key("highway"));
        assert!(routing_tags.contains_key("maxspeed"));
        assert!(routing_tags.contains_key("oneway"));
        
        // Should exclude non-routing tags
        assert!(!routing_tags.contains_key("name"));
        assert!(!routing_tags.contains_key("building"));
        assert!(!routing_tags.contains_key("amenity"));
        
        assert_eq!(routing_tags.len(), 3);
    }
    
    #[test]
    fn test_tag_sieve_way_filtering() {
        let sieve = TagSieve::new();
        
        // Routable way should be filtered
        let highway_tags = create_test_tags(&[
            ("highway", "residential"),
            ("name", "Oak Street"),
            ("maxspeed", "30"),
        ]);
        
        let nodes = vec![1, 2, 3, 4];
        let filtered = sieve.filter_way(123, &nodes, &highway_tags);
        assert!(filtered.is_some());
        
        let way = filtered.unwrap();
        assert_eq!(way.id, 123);
        assert_eq!(way.nodes, nodes);
        assert_eq!(way.highway_class, Some(HighwayClass::Residential));
        assert!(way.routing_tags.contains_key("highway"));
        assert!(way.routing_tags.contains_key("maxspeed"));
        assert!(!way.routing_tags.contains_key("name")); // Non-routing tag filtered out
        
        // Non-routable way should not be filtered
        let building_tags = create_test_tags(&[("building", "residential")]);
        let filtered = sieve.filter_way(456, &nodes, &building_tags);
        assert!(filtered.is_none());
    }
    
    #[test]
    fn test_tag_sieve_node_filtering() {
        let sieve = TagSieve::new();
        
        // Node with routing tags should be filtered
        let barrier_tags = create_test_tags(&[
            ("barrier", "gate"),
            ("access", "private"),
            ("name", "Farm Gate"),
        ]);
        
        let filtered = sieve.filter_node(789, 52.5, 13.4, &barrier_tags);
        assert!(filtered.is_some());
        
        let node = filtered.unwrap();
        assert_eq!(node.id, 789);
        assert_eq!(node.lat, 52.5);
        assert_eq!(node.lon, 13.4);
        assert!(node.routing_tags.contains_key("barrier"));
        assert!(node.routing_tags.contains_key("access"));
        assert!(!node.routing_tags.contains_key("name")); // Non-routing tag filtered out
        
        // Node without routing tags should not be filtered
        let poi_tags = create_test_tags(&[("amenity", "restaurant")]);
        let filtered = sieve.filter_node(999, 52.5, 13.4, &poi_tags);
        assert!(filtered.is_none());
    }
    
    #[test]
    fn test_tag_sieve_relation_filtering() {
        let sieve = TagSieve::new();
        
        // Routing relation should be filtered
        let route_tags = create_test_tags(&[
            ("type", "route"),
            ("route", "bus"),
            ("name", "Bus Line 42"),
        ]);
        
        let members = vec![
            crate::pbf::RelationMember {
                id: 1,
                role: "".to_string(),
                member_type: crate::pbf::MemberType::Way,
            },
            crate::pbf::RelationMember {
                id: 2,
                role: "stop".to_string(),
                member_type: crate::pbf::MemberType::Node,
            },
        ];
        
        let filtered = sieve.filter_relation(555, &members, &route_tags);
        assert!(filtered.is_some());
        
        let relation = filtered.unwrap();
        assert_eq!(relation.id, 555);
        assert_eq!(relation.members.len(), 2);
        assert!(relation.routing_tags.contains_key("type"));
        assert!(!relation.routing_tags.contains_key("name")); // Non-routing tag filtered out
        
        // Non-routing relation should not be filtered
        let admin_tags = create_test_tags(&[("type", "boundary"), ("admin_level", "8")]);
        let filtered = sieve.filter_relation(777, &members, &admin_tags);
        assert!(filtered.is_none());
    }
    
    #[test]
    fn test_vehicle_profile_specific_sieve() {
        let bicycle_sieve = TagSieve::for_profiles(vec![VehicleProfile::Bicycle]);
        let car_sieve = TagSieve::for_profiles(vec![VehicleProfile::Car]);
        
        // Both should filter highway ways
        let highway_tags = create_test_tags(&[("highway", "cycleway")]);
        let nodes = vec![1, 2, 3];
        
        assert!(bicycle_sieve.filter_way(1, &nodes, &highway_tags).is_some());
        assert!(car_sieve.filter_way(1, &nodes, &highway_tags).is_some());
        
        // Truth table should be the same regardless of profiles
        let bicycle_table = bicycle_sieve.get_truth_table();
        let car_table = car_sieve.get_truth_table();
        
        let test_tags = create_test_tags(&[("highway", "primary")]);
        assert_eq!(
            bicycle_table.is_routable_way(&test_tags),
            car_table.is_routable_way(&test_tags)
        );
    }
    
    #[test]
    fn test_complex_tag_combinations() {
        let table = TagTruthTable::new();
        
        // Way with multiple routing constraints
        let complex_tags = create_test_tags(&[
            ("highway", "residential"),
            ("oneway", "yes"),
            ("maxspeed", "30"),
            ("surface", "asphalt"),
            ("access", "private"),
            ("bicycle", "designated"),
            ("name", "Private Road"),
            ("building", "garage"), // Non-routing tag
        ]);
        
        assert!(table.is_routable_way(&complex_tags));
        assert!(table.is_routing_relevant(&complex_tags));
        
        let routing_tags = table.extract_routing_tags(&complex_tags);
        assert!(routing_tags.contains_key("highway"));
        assert!(routing_tags.contains_key("oneway"));
        assert!(routing_tags.contains_key("maxspeed"));
        assert!(routing_tags.contains_key("surface"));
        assert!(routing_tags.contains_key("access"));
        assert!(routing_tags.contains_key("bicycle"));
        assert!(!routing_tags.contains_key("name"));
        assert!(!routing_tags.contains_key("building"));
    }
}
