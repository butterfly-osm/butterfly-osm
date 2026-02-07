//! Turn Restriction Index for NBG CH Junction Expansion
//!
//! This module provides data structures and utilities for handling turn restrictions
//! during NBG CH queries. The key insight is that only 0.3% of junctions have explicit
//! turn restrictions (based on Belgium analysis), so the overhead is minimal.
//!
//! ## Data Flow
//!
//! 1. Load turn rules from `turn_rules.<mode>.bin`
//! 2. Convert OSM node IDs to compact NBG node IDs using `nbg.node_map`
//! 3. Build restriction index keyed by compact NBG node ID
//! 4. At query time, check restrictions at turn-relevant junctions
//!
//! ## Turn Rule Types
//!
//! - **Ban**: Cannot turn from `from_way` to `to_way` at `via_node`
//! - **Only**: Can ONLY turn from `from_way` to `to_way` at `via_node`

use std::collections::{HashMap, HashSet};
use std::path::Path;

use anyhow::{Context, Result};

use crate::formats::{
    turn_rules,
    NbgNodeMapFile, EbgNodesFile, EbgNodes,
};
use crate::profile_abi::TurnRuleKind;

/// Index for fast turn restriction lookups during NBG CH queries
pub struct TurnRestrictionIndex {
    /// Compact NBG node IDs that have turn restrictions
    pub restricted_nodes: HashSet<u32>,

    /// For each restricted node: (from_way, to_way) -> rule
    /// Uses lower 32 bits of way_id for memory efficiency
    restrictions: HashMap<u32, Vec<NodeRestriction>>,

    /// For "only" restrictions: (via_node, from_way) -> allowed_to_ways
    only_allowed: HashMap<(u32, u32), HashSet<u32>>,
}

/// A turn restriction at a specific node
#[derive(Debug, Clone)]
struct NodeRestriction {
    from_way: u32,  // lower 32 bits of OSM way ID
    to_way: u32,    // lower 32 bits of OSM way ID
    kind: TurnRuleKind,
}

impl TurnRestrictionIndex {
    /// Build turn restriction index from files
    pub fn load(
        turn_rules_path: &Path,
        node_map_path: &Path,
    ) -> Result<Self> {
        // Load OSM -> compact node ID mapping
        let osm_to_compact = NbgNodeMapFile::read(node_map_path)
            .context("Failed to load node map")?;

        // Load turn rules
        let rules = turn_rules::read_all(turn_rules_path)
            .context("Failed to load turn rules")?;

        // Build restriction index
        let mut restricted_nodes = HashSet::new();
        let mut restrictions: HashMap<u32, Vec<NodeRestriction>> = HashMap::new();
        let mut only_allowed: HashMap<(u32, u32), HashSet<u32>> = HashMap::new();

        for rule in &rules {
            // Convert OSM node ID to compact ID
            let compact_id = match osm_to_compact.get(&rule.via_node_id) {
                Some(&id) => id,
                None => {
                    // Node not in our graph (might be outside Belgium or on excluded roads)
                    continue;
                }
            };

            restricted_nodes.insert(compact_id);

            // Store restriction (using lower 32 bits of way IDs)
            let from_way = rule.from_way_id as u32;
            let to_way = rule.to_way_id as u32;

            let node_restriction = NodeRestriction {
                from_way,
                to_way,
                kind: rule.kind,
            };

            restrictions
                .entry(compact_id)
                .or_default()
                .push(node_restriction);

            // For "only" restrictions, track allowed turns
            if rule.kind == TurnRuleKind::Only {
                only_allowed
                    .entry((compact_id, from_way))
                    .or_default()
                    .insert(to_way);
            }
        }

        Ok(Self {
            restricted_nodes,
            restrictions,
            only_allowed,
        })
    }

    /// Check if a node has turn restrictions
    #[inline]
    pub fn is_restricted(&self, node: u32) -> bool {
        self.restricted_nodes.contains(&node)
    }

    /// Check if a turn is allowed at a junction
    ///
    /// Returns true if the turn from `from_way` to `to_way` at `via_node` is allowed.
    ///
    /// # Arguments
    /// - `via_node`: Compact NBG node ID of the junction
    /// - `from_way`: Lower 32 bits of the incoming way's OSM ID
    /// - `to_way`: Lower 32 bits of the outgoing way's OSM ID
    pub fn is_turn_allowed(&self, via_node: u32, from_way: u32, to_way: u32) -> bool {
        // Fast path: not a restricted node
        if !self.restricted_nodes.contains(&via_node) {
            return true;
        }

        // Check restrictions at this node
        if let Some(node_rules) = self.restrictions.get(&via_node) {
            for rule in node_rules {
                if rule.from_way == from_way {
                    match rule.kind {
                        TurnRuleKind::Ban => {
                            if rule.to_way == to_way {
                                return false; // This specific turn is banned
                            }
                        }
                        TurnRuleKind::Only => {
                            // With "only" restriction, check if to_way is in allowed set
                            if let Some(allowed) = self.only_allowed.get(&(via_node, from_way)) {
                                return allowed.contains(&to_way);
                            }
                            // No "only" entry found, shouldn't happen but allow it
                        }
                        TurnRuleKind::Penalty | TurnRuleKind::None => {
                            // Penalties don't affect allowed/banned status
                        }
                    }
                }
            }
        }

        // No restriction matched, turn is allowed
        true
    }

    /// Get number of restricted nodes
    pub fn n_restricted_nodes(&self) -> usize {
        self.restricted_nodes.len()
    }

    /// Get number of total restrictions
    pub fn n_restrictions(&self) -> usize {
        self.restrictions.values().map(|v| v.len()).sum()
    }
}

/// Maps NBG edges to their way IDs for turn restriction checking
pub struct NbgEdgeWayMap {
    /// (tail_nbg, head_nbg) -> way_id (lower 32 bits)
    edge_to_way: HashMap<(u32, u32), u32>,
}

impl NbgEdgeWayMap {
    /// Build edge-to-way mapping from EBG nodes
    pub fn from_ebg_nodes(ebg_nodes: &EbgNodes) -> Self {
        let mut edge_to_way = HashMap::with_capacity(ebg_nodes.n_nodes as usize);

        for node in &ebg_nodes.nodes {
            // Each EBG node represents a directed NBG edge
            edge_to_way.insert(
                (node.tail_nbg, node.head_nbg),
                node.primary_way,
            );
        }

        Self { edge_to_way }
    }

    /// Load from EBG nodes file
    pub fn load(ebg_nodes_path: &Path) -> Result<Self> {
        let ebg_nodes = EbgNodesFile::read(ebg_nodes_path)
            .context("Failed to load EBG nodes")?;
        Ok(Self::from_ebg_nodes(&ebg_nodes))
    }

    /// Get way ID for an NBG edge
    #[inline]
    pub fn get_way(&self, tail: u32, head: u32) -> Option<u32> {
        self.edge_to_way.get(&(tail, head)).copied()
    }

    /// Get number of edges in map
    pub fn n_edges(&self) -> usize {
        self.edge_to_way.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_restriction_index_basic() {
        // Test basic restriction checking logic
        let mut restricted_nodes = HashSet::new();
        restricted_nodes.insert(100);

        let mut restrictions = HashMap::new();
        restrictions.insert(100, vec![
            NodeRestriction {
                from_way: 1,
                to_way: 2,
                kind: TurnRuleKind::Ban,
            }
        ]);

        let index = TurnRestrictionIndex {
            restricted_nodes,
            restrictions,
            only_allowed: HashMap::new(),
        };

        // Node 100: banned turn from way 1 to way 2
        assert!(!index.is_turn_allowed(100, 1, 2));
        // But way 1 to way 3 is allowed
        assert!(index.is_turn_allowed(100, 1, 3));
        // And other nodes are unrestricted
        assert!(index.is_turn_allowed(200, 1, 2));
    }

    #[test]
    fn test_only_restriction() {
        let mut restricted_nodes = HashSet::new();
        restricted_nodes.insert(100);

        let mut restrictions = HashMap::new();
        restrictions.insert(100, vec![
            NodeRestriction {
                from_way: 1,
                to_way: 2,
                kind: TurnRuleKind::Only,
            }
        ]);

        let mut only_allowed = HashMap::new();
        only_allowed.insert((100, 1), HashSet::from([2]));

        let index = TurnRestrictionIndex {
            restricted_nodes,
            restrictions,
            only_allowed,
        };

        // Node 100: can ONLY turn from way 1 to way 2
        assert!(index.is_turn_allowed(100, 1, 2));
        assert!(!index.is_turn_allowed(100, 1, 3));
        // But from way 5, no restriction
        assert!(index.is_turn_allowed(100, 5, 3));
    }
}
