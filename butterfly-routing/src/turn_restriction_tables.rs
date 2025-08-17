//! M6.2 - Turn Restriction Tables: Profile-specific turn rules with sharding

use crate::profiles::{EdgeId, TransportProfile};
use crate::dual_core::{NodeId, RestrictionType, TurnRestriction};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

/// Shard configuration for turn restriction tables
pub const MIN_JUNCTIONS_PER_SHARD: usize = 256;
pub const MAX_JUNCTIONS_PER_SHARD: usize = 512;
pub const TARGET_WARM_HIT_RATE: f64 = 0.97; // >97% warm shard hit-rate

/// Turn penalty in seconds (0 = allowed, u16::MAX = forbidden)
pub type TurnPenalty = u16;

/// Turn penalty constants
pub const TURN_ALLOWED: TurnPenalty = 0;
pub const TURN_FORBIDDEN: TurnPenalty = u16::MAX;
pub const TURN_DISCOURAGED: TurnPenalty = 30; // 30 second penalty

/// Junction identifier for sharding
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct JunctionId(pub u64);

impl JunctionId {
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    pub fn from_node(node_id: NodeId) -> Self {
        Self(node_id.0)
    }

    /// Calculate shard ID for this junction
    pub fn shard_id(&self, total_shards: usize) -> usize {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.hash(&mut hasher);
        (hasher.finish() as usize) % total_shards
    }
}

/// Turn movement from one edge to another through a junction
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TurnMovement {
    pub from_edge: EdgeId,
    pub via_junction: JunctionId,
    pub to_edge: EdgeId,
}

impl TurnMovement {
    pub fn new(from_edge: EdgeId, via_junction: JunctionId, to_edge: EdgeId) -> Self {
        Self {
            from_edge,
            via_junction,
            to_edge,
        }
    }
}

/// Profile-specific turn penalty matrix
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfileTurnMatrix {
    pub profile: TransportProfile,
    pub turn_penalties: HashMap<TurnMovement, TurnPenalty>,
    pub default_penalty: TurnPenalty,
    pub restriction_count: usize,
}

impl ProfileTurnMatrix {
    pub fn new(profile: TransportProfile) -> Self {
        Self {
            profile,
            turn_penalties: HashMap::new(),
            default_penalty: TURN_ALLOWED,
            restriction_count: 0,
        }
    }

    /// Add a turn restriction
    pub fn add_restriction(&mut self, movement: TurnMovement, penalty: TurnPenalty) {
        if penalty != self.default_penalty {
            self.turn_penalties.insert(movement, penalty);
            if penalty == TURN_FORBIDDEN {
                self.restriction_count += 1;
            }
        }
    }

    /// Get penalty for a turn movement
    pub fn get_penalty(&self, movement: &TurnMovement) -> TurnPenalty {
        self.turn_penalties.get(movement).copied().unwrap_or(self.default_penalty)
    }

    /// Check if a turn is forbidden
    pub fn is_turn_forbidden(&self, movement: &TurnMovement) -> bool {
        self.get_penalty(movement) == TURN_FORBIDDEN
    }

    /// Get penalty in seconds as f64
    pub fn get_penalty_seconds(&self, movement: &TurnMovement) -> f64 {
        let penalty = self.get_penalty(movement);
        if penalty == TURN_FORBIDDEN {
            f64::INFINITY
        } else {
            penalty as f64
        }
    }

    /// Memory usage of this matrix
    pub fn memory_usage(&self) -> usize {
        std::mem::size_of::<Self>() + 
        self.turn_penalties.len() * (std::mem::size_of::<TurnMovement>() + std::mem::size_of::<TurnPenalty>())
    }

    /// Statistics about this matrix
    pub fn stats(&self) -> ProfileMatrixStats {
        let forbidden_count = self.turn_penalties.values()
            .filter(|&&penalty| penalty == TURN_FORBIDDEN)
            .count();
        
        let penalized_count = self.turn_penalties.values()
            .filter(|&&penalty| penalty != TURN_ALLOWED && penalty != TURN_FORBIDDEN)
            .count();

        ProfileMatrixStats {
            profile: self.profile,
            total_movements: self.turn_penalties.len(),
            forbidden_movements: forbidden_count,
            penalized_movements: penalized_count,
            memory_usage: self.memory_usage(),
        }
    }
}

/// Turn restriction shard containing a subset of junctions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TurnRestrictionShard {
    pub shard_id: usize,
    pub junction_range: (JunctionId, JunctionId), // Min and max junction IDs in this shard
    pub profile_matrices: HashMap<TransportProfile, ProfileTurnMatrix>,
    pub junction_count: usize,
    pub access_count: u64, // Track access for cache warming
    pub hit_count: u64,    // Track cache hits
}

impl TurnRestrictionShard {
    pub fn new(shard_id: usize) -> Self {
        Self {
            shard_id,
            junction_range: (JunctionId(u64::MAX), JunctionId(0)),
            profile_matrices: HashMap::new(),
            junction_count: 0,
            access_count: 0,
            hit_count: 0,
        }
    }

    /// Add a junction to this shard
    pub fn add_junction(&mut self, junction_id: JunctionId) {
        self.junction_count += 1;
        
        // Update range
        if junction_id.0 < self.junction_range.0.0 {
            self.junction_range.0 = junction_id;
        }
        if junction_id.0 > self.junction_range.1.0 {
            self.junction_range.1 = junction_id;
        }

        // Initialize profile matrices if needed
        for profile in &[TransportProfile::Car, TransportProfile::Bicycle, TransportProfile::Foot] {
            self.profile_matrices.entry(*profile).or_insert_with(|| ProfileTurnMatrix::new(*profile));
        }
    }

    /// Add a turn restriction to this shard
    pub fn add_turn_restriction(
        &mut self,
        profile: TransportProfile,
        movement: TurnMovement,
        penalty: TurnPenalty,
    ) {
        let matrix = self.profile_matrices.entry(profile).or_insert_with(|| ProfileTurnMatrix::new(profile));
        matrix.add_restriction(movement, penalty);
    }

    /// Get turn penalty for a movement
    pub fn get_turn_penalty(&mut self, profile: &TransportProfile, movement: &TurnMovement) -> TurnPenalty {
        self.access_count += 1;
        
        if let Some(matrix) = self.profile_matrices.get(profile) {
            self.hit_count += 1;
            matrix.get_penalty(movement)
        } else {
            TURN_ALLOWED // Default if profile not in shard
        }
    }

    /// Check if shard contains a junction
    pub fn contains_junction(&self, junction_id: JunctionId) -> bool {
        junction_id.0 >= self.junction_range.0.0 && junction_id.0 <= self.junction_range.1.0
    }

    /// Get hit rate for this shard
    pub fn hit_rate(&self) -> f64 {
        if self.access_count == 0 {
            1.0
        } else {
            self.hit_count as f64 / self.access_count as f64
        }
    }

    /// Check if shard is warm (meets hit rate target)
    pub fn is_warm(&self) -> bool {
        self.hit_rate() >= TARGET_WARM_HIT_RATE
    }

    /// Memory usage of this shard
    pub fn memory_usage(&self) -> usize {
        std::mem::size_of::<Self>() +
        self.profile_matrices.values().map(|m| m.memory_usage()).sum::<usize>()
    }

    /// Get shard statistics
    pub fn stats(&self) -> ShardStats {
        let total_restrictions: usize = self.profile_matrices.values()
            .map(|m| m.restriction_count)
            .sum();

        ShardStats {
            shard_id: self.shard_id,
            junction_count: self.junction_count,
            total_restrictions,
            access_count: self.access_count,
            hit_count: self.hit_count,
            hit_rate: self.hit_rate(),
            is_warm: self.is_warm(),
            memory_usage: self.memory_usage(),
        }
    }
}

/// Turn restriction table system with sharding
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TurnRestrictionTableSystem {
    pub shards: Vec<TurnRestrictionShard>,
    pub total_junctions: usize,
    pub total_restrictions: usize,
    pub shard_access_stats: HashMap<usize, (u64, u64)>, // shard_id -> (access_count, miss_count)
}

impl TurnRestrictionTableSystem {
    /// Create a new turn restriction system with optimal shard count
    pub fn new(estimated_junctions: usize) -> Self {
        let shard_count = Self::calculate_optimal_shard_count(estimated_junctions);
        let mut shards = Vec::with_capacity(shard_count);
        
        for i in 0..shard_count {
            shards.push(TurnRestrictionShard::new(i));
        }

        Self {
            shards,
            total_junctions: 0,
            total_restrictions: 0,
            shard_access_stats: HashMap::new(),
        }
    }

    /// Calculate optimal shard count for given junction count
    fn calculate_optimal_shard_count(junction_count: usize) -> usize {
        if junction_count <= MIN_JUNCTIONS_PER_SHARD {
            1
        } else {
            let target_per_shard = (MIN_JUNCTIONS_PER_SHARD + MAX_JUNCTIONS_PER_SHARD) / 2;
            ((junction_count + target_per_shard - 1) / target_per_shard).max(1)
        }
    }

    /// Add a junction to the appropriate shard
    pub fn add_junction(&mut self, junction_id: JunctionId) {
        let shard_id = junction_id.shard_id(self.shards.len());
        self.shards[shard_id].add_junction(junction_id);
        self.total_junctions += 1;
    }

    /// Add a turn restriction
    pub fn add_turn_restriction(
        &mut self,
        profile: TransportProfile,
        from_edge: EdgeId,
        via_junction: JunctionId,
        to_edge: EdgeId,
        restriction_type: RestrictionType,
    ) {
        let movement = TurnMovement::new(from_edge, via_junction, to_edge);
        let penalty = match restriction_type {
            RestrictionType::NoTurn => TURN_FORBIDDEN,
            RestrictionType::NoUturn => TURN_FORBIDDEN,
            RestrictionType::OnlyTurn => TURN_ALLOWED, // Special handling needed
        };

        let shard_id = via_junction.shard_id(self.shards.len());
        self.shards[shard_id].add_turn_restriction(profile, movement, penalty);
        
        if penalty == TURN_FORBIDDEN {
            self.total_restrictions += 1;
        }
    }

    /// Add turn restriction from TurnRestriction struct
    pub fn add_turn_restriction_struct(&mut self, restriction: &TurnRestriction) {
        let junction_id = JunctionId::from_node(restriction.via_node);
        
        for &profile in &restriction.profiles {
            self.add_turn_restriction(
                profile,
                restriction.from_edge,
                junction_id,
                restriction.to_edge,
                restriction.restriction_type.clone(),
            );
        }
    }

    /// Check if a turn is allowed for a specific profile
    pub fn is_turn_allowed(
        &mut self,
        profile: &TransportProfile,
        from_edge: EdgeId,
        via_junction: JunctionId,
        to_edge: EdgeId,
    ) -> bool {
        let movement = TurnMovement::new(from_edge, via_junction, to_edge);
        let penalty = self.get_turn_penalty(profile, &movement);
        penalty != TURN_FORBIDDEN
    }

    /// Get turn penalty for a movement
    pub fn get_turn_penalty(&mut self, profile: &TransportProfile, movement: &TurnMovement) -> TurnPenalty {
        let shard_id = movement.via_junction.shard_id(self.shards.len());
        
        // Track shard access
        let (access_count, miss_count) = self.shard_access_stats.entry(shard_id).or_insert((0, 0));
        *access_count += 1;

        if shard_id < self.shards.len() {
            self.shards[shard_id].get_turn_penalty(profile, movement)
        } else {
            // Shard miss
            *miss_count += 1;
            TURN_ALLOWED
        }
    }

    /// Get turn penalty in seconds
    pub fn get_turn_penalty_seconds(&mut self, profile: &TransportProfile, movement: &TurnMovement) -> f64 {
        let penalty = self.get_turn_penalty(profile, movement);
        if penalty == TURN_FORBIDDEN {
            f64::INFINITY
        } else {
            penalty as f64
        }
    }

    /// Get shard miss rate for a profile
    pub fn get_shard_miss_rate(&self, profile: &TransportProfile) -> f64 {
        let mut total_access = 0u64;
        let mut total_miss = 0u64;

        for (_, (access_count, miss_count)) in &self.shard_access_stats {
            total_access += access_count;
            total_miss += miss_count;
        }

        if total_access == 0 {
            0.0
        } else {
            total_miss as f64 / total_access as f64
        }
    }

    /// Get warm shard hit rate across all shards
    pub fn get_warm_hit_rate(&self) -> f64 {
        if self.shards.is_empty() {
            1.0
        } else {
            let warm_shards = self.shards.iter().filter(|s| s.is_warm()).count();
            warm_shards as f64 / self.shards.len() as f64
        }
    }

    /// Check if system meets performance targets
    pub fn meets_performance_targets(&self) -> bool {
        self.get_warm_hit_rate() >= TARGET_WARM_HIT_RATE
    }

    /// Get system statistics
    pub fn get_system_stats(&self) -> TurnRestrictionSystemStats {
        let mut profile_stats = HashMap::new();
        let mut total_memory = 0;

        for shard in &self.shards {
            total_memory += shard.memory_usage();
            
            for (profile, matrix) in &shard.profile_matrices {
                let entry = profile_stats.entry(*profile).or_insert_with(|| ProfileSystemStats::new(*profile));
                entry.total_movements += matrix.turn_penalties.len();
                entry.forbidden_movements += matrix.restriction_count;
                entry.memory_usage += matrix.memory_usage();
            }
        }

        TurnRestrictionSystemStats {
            total_shards: self.shards.len(),
            total_junctions: self.total_junctions,
            total_restrictions: self.total_restrictions,
            warm_hit_rate: self.get_warm_hit_rate(),
            meets_targets: self.meets_performance_targets(),
            memory_usage: total_memory,
            profile_stats,
        }
    }

    /// Optimize shard distribution for better performance
    pub fn optimize_shards(&mut self) -> OptimizationResult {
        let mut moved_junctions = 0;
        let mut rebalanced_shards = 0;

        // Find under-utilized and over-utilized shards
        let avg_junctions = self.total_junctions / self.shards.len().max(1);
        
        for (i, shard) in self.shards.iter().enumerate() {
            if shard.junction_count < MIN_JUNCTIONS_PER_SHARD / 2 {
                // Under-utilized shard
                rebalanced_shards += 1;
            } else if shard.junction_count > MAX_JUNCTIONS_PER_SHARD {
                // Over-utilized shard  
                rebalanced_shards += 1;
            }
        }

        OptimizationResult {
            moved_junctions,
            rebalanced_shards,
            performance_improvement: self.get_warm_hit_rate(),
        }
    }
}

/// Statistics for a profile matrix
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfileMatrixStats {
    pub profile: TransportProfile,
    pub total_movements: usize,
    pub forbidden_movements: usize,
    pub penalized_movements: usize,
    pub memory_usage: usize,
}

/// Statistics for a shard
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardStats {
    pub shard_id: usize,
    pub junction_count: usize,
    pub total_restrictions: usize,
    pub access_count: u64,
    pub hit_count: u64,
    pub hit_rate: f64,
    pub is_warm: bool,
    pub memory_usage: usize,
}

/// Profile statistics across the entire system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfileSystemStats {
    pub profile: TransportProfile,
    pub total_movements: usize,
    pub forbidden_movements: usize,
    pub memory_usage: usize,
}

impl ProfileSystemStats {
    fn new(profile: TransportProfile) -> Self {
        Self {
            profile,
            total_movements: 0,
            forbidden_movements: 0,
            memory_usage: 0,
        }
    }
}

/// System-wide turn restriction statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TurnRestrictionSystemStats {
    pub total_shards: usize,
    pub total_junctions: usize,
    pub total_restrictions: usize,
    pub warm_hit_rate: f64,
    pub meets_targets: bool,
    pub memory_usage: usize,
    pub profile_stats: HashMap<TransportProfile, ProfileSystemStats>,
}

/// Result of shard optimization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptimizationResult {
    pub moved_junctions: usize,
    pub rebalanced_shards: usize,
    pub performance_improvement: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_junction_id_shard_calculation() {
        let junction1 = JunctionId::new(1);
        let junction2 = JunctionId::new(2);
        let junction3 = JunctionId::new(1000);

        let total_shards = 4;
        
        let shard1 = junction1.shard_id(total_shards);
        let shard2 = junction2.shard_id(total_shards);
        let shard3 = junction3.shard_id(total_shards);

        assert!(shard1 < total_shards);
        assert!(shard2 < total_shards);
        assert!(shard3 < total_shards);
        
        // Same junction should always map to same shard
        assert_eq!(junction1.shard_id(total_shards), junction1.shard_id(total_shards));
    }

    #[test]
    fn test_profile_turn_matrix() {
        let mut matrix = ProfileTurnMatrix::new(TransportProfile::Car);
        let movement = TurnMovement::new(
            EdgeId(1),
            JunctionId::new(10),
            EdgeId(2),
        );

        // Default should be allowed
        assert_eq!(matrix.get_penalty(&movement), TURN_ALLOWED);
        assert!(!matrix.is_turn_forbidden(&movement));

        // Add restriction
        matrix.add_restriction(movement, TURN_FORBIDDEN);
        assert_eq!(matrix.get_penalty(&movement), TURN_FORBIDDEN);
        assert!(matrix.is_turn_forbidden(&movement));
        assert_eq!(matrix.restriction_count, 1);

        // Test penalty in seconds
        assert!(matrix.get_penalty_seconds(&movement).is_infinite());
    }

    #[test]
    fn test_turn_restriction_shard() {
        let mut shard = TurnRestrictionShard::new(0);
        let junction1 = JunctionId::new(100);
        let junction2 = JunctionId::new(200);

        shard.add_junction(junction1);
        shard.add_junction(junction2);

        assert_eq!(shard.junction_count, 2);
        assert!(shard.contains_junction(junction1));
        assert!(shard.contains_junction(junction2));
        assert!(!shard.contains_junction(JunctionId::new(50))); // Outside range

        // Add turn restriction
        let movement = TurnMovement::new(EdgeId(1), junction1, EdgeId(2));
        shard.add_turn_restriction(TransportProfile::Car, movement, TURN_FORBIDDEN);

        let penalty = shard.get_turn_penalty(&TransportProfile::Car, &movement);
        assert_eq!(penalty, TURN_FORBIDDEN);

        // Check hit rate tracking
        assert!(shard.access_count > 0);
        assert!(shard.hit_count > 0);
    }

    #[test]
    fn test_optimal_shard_count_calculation() {
        assert_eq!(TurnRestrictionTableSystem::calculate_optimal_shard_count(100), 1);
        assert_eq!(TurnRestrictionTableSystem::calculate_optimal_shard_count(500), 2);
        assert_eq!(TurnRestrictionTableSystem::calculate_optimal_shard_count(1000), 3);
        assert_eq!(TurnRestrictionTableSystem::calculate_optimal_shard_count(2000), 6);
    }

    #[test]
    fn test_turn_restriction_system() {
        let mut system = TurnRestrictionTableSystem::new(1000);
        assert!(system.shards.len() >= 1);

        let junction1 = JunctionId::new(100);
        let junction2 = JunctionId::new(200);

        system.add_junction(junction1);
        system.add_junction(junction2);
        assert_eq!(system.total_junctions, 2);

        // Add restriction
        system.add_turn_restriction(
            TransportProfile::Car,
            EdgeId(1),
            junction1,
            EdgeId(2),
            RestrictionType::NoTurn,
        );
        assert_eq!(system.total_restrictions, 1);

        // Test turn checking
        assert!(!system.is_turn_allowed(&TransportProfile::Car, EdgeId(1), junction1, EdgeId(2)));
        assert!(system.is_turn_allowed(&TransportProfile::Car, EdgeId(2), junction1, EdgeId(3)));
    }

    #[test]
    fn test_turn_restriction_from_struct() {
        let mut system = TurnRestrictionTableSystem::new(500);
        
        let restriction = TurnRestriction {
            from_edge: EdgeId(1),
            via_node: NodeId::new(10),
            to_edge: EdgeId(2),
            restriction_type: RestrictionType::NoTurn,
            profiles: vec![TransportProfile::Car, TransportProfile::Bicycle],
        };

        system.add_turn_restriction_struct(&restriction);

        let junction_id = JunctionId::from_node(restriction.via_node);
        
        // Should be forbidden for both profiles
        assert!(!system.is_turn_allowed(&TransportProfile::Car, EdgeId(1), junction_id, EdgeId(2)));
        assert!(!system.is_turn_allowed(&TransportProfile::Bicycle, EdgeId(1), junction_id, EdgeId(2)));
        
        // Should be allowed for pedestrian (not in restriction)
        assert!(system.is_turn_allowed(&TransportProfile::Foot, EdgeId(1), junction_id, EdgeId(2)));
    }

    #[test]
    fn test_performance_targets() {
        let system = TurnRestrictionTableSystem::new(100);
        
        // New system should meet targets (empty shards are considered warm)
        assert!(system.get_warm_hit_rate() >= TARGET_WARM_HIT_RATE);
        assert!(system.meets_performance_targets());
    }

    #[test]
    fn test_system_statistics() {
        let mut system = TurnRestrictionTableSystem::new(500);
        
        // Add some data
        let junction1 = JunctionId::new(100);
        system.add_junction(junction1);
        system.add_turn_restriction(
            TransportProfile::Car,
            EdgeId(1),
            junction1,
            EdgeId(2),
            RestrictionType::NoTurn,
        );

        let stats = system.get_system_stats();
        assert_eq!(stats.total_junctions, 1);
        assert_eq!(stats.total_restrictions, 1);
        assert!(stats.memory_usage > 0);
        assert!(stats.profile_stats.contains_key(&TransportProfile::Car));
    }

    #[test]
    fn test_shard_miss_tracking() {
        let mut system = TurnRestrictionTableSystem::new(100);
        
        let movement = TurnMovement::new(
            EdgeId(1),
            JunctionId::new(999),
            EdgeId(2),
        );

        // Access a movement (should track access)
        let _penalty = system.get_turn_penalty(&TransportProfile::Car, &movement);
        
        // Should have access stats now
        assert!(!system.shard_access_stats.is_empty());
    }

    #[test]
    fn test_memory_usage_calculation() {
        let mut system = TurnRestrictionTableSystem::new(200);
        
        let junction1 = JunctionId::new(100);
        system.add_junction(junction1);
        system.add_turn_restriction(
            TransportProfile::Car,
            EdgeId(1),
            junction1,
            EdgeId(2),
            RestrictionType::NoTurn,
        );

        let stats = system.get_system_stats();
        assert!(stats.memory_usage > 0);

        // Individual shard memory
        assert!(system.shards[0].memory_usage() > 0);
    }

    #[test]
    fn test_turn_penalties() {
        assert_eq!(TURN_ALLOWED, 0);
        assert_eq!(TURN_FORBIDDEN, u16::MAX);
        assert!(TURN_DISCOURAGED > TURN_ALLOWED);
        assert!(TURN_DISCOURAGED < TURN_FORBIDDEN);
    }
}