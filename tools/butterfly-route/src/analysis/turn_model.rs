//! Turn Model Analysis
//!
//! Analyzes when turns matter and identifies turn-relevant junctions
//! for node-based CH + junction expansion strategy.
//!
//! Key questions answered:
//! 1. How many explicit turn restrictions exist?
//! 2. How many junctions have non-trivial turn behavior?
//! 3. What fraction of searches would need junction expansion?

use std::collections::HashSet;
use std::path::Path;
use anyhow::Result;

use crate::formats::{
    EbgNodesFile, EbgCsrFile, TurnTableFile, turn_rules, TurnKind,
    NbgCsrFile,
};

/// Result of turn model analysis
#[derive(Debug, Clone)]
pub struct TurnModelAnalysis {
    // Turn restriction counts
    pub n_turn_rules_car: usize,
    pub n_turn_rules_bike: usize,
    pub n_turn_rules_foot: usize,

    // Turn table entries by kind
    pub n_turn_entries: usize,
    pub n_ban_entries: usize,
    pub n_only_entries: usize,
    pub n_penalty_entries: usize,
    pub n_none_entries: usize,

    // Junction analysis (NBG level)
    pub n_nbg_nodes: usize,
    pub n_junctions_with_restriction: usize,
    pub n_junctions_multi_way: usize,  // degree > 2
    pub n_junctions_degree_3: usize,
    pub n_junctions_degree_4_plus: usize,

    // EBG arc analysis
    pub n_ebg_arcs: usize,
    pub n_arcs_with_ban: usize,
    pub n_arcs_with_penalty: usize,
    pub n_arcs_allow_all_modes: usize,

    // U-turn analysis
    pub n_potential_uturns: usize,
    pub n_uturns_allowed_car: usize,
    pub n_uturns_at_deadends: usize,

    // Turn-relevant junction breakdown
    pub junctions_by_reason: JunctionBreakdown,

    // Percentiles for junction degree distribution
    pub degree_p50: usize,
    pub degree_p90: usize,
    pub degree_p95: usize,
    pub degree_p99: usize,
    pub degree_max: usize,
}

#[derive(Debug, Clone, Default)]
pub struct JunctionBreakdown {
    pub has_explicit_restriction: usize,  // OSM turn:restriction
    pub has_uturn_ban: usize,             // Implicit U-turn prohibition
    pub has_oneway_constraint: usize,     // Oneway roads
    pub has_mode_restriction: usize,      // Different modes allowed on different exits
    pub total_turn_relevant: usize,       // Union of above
    pub turn_free: usize,                 // Junctions with no turn constraints
}

impl TurnModelAnalysis {
    pub fn print(&self) {
        println!("\n=== TURN MODEL ANALYSIS ===\n");

        println!("Turn Restriction Rules (from OSM relations):");
        println!("  Car:  {} rules", self.n_turn_rules_car);
        println!("  Bike: {} rules", self.n_turn_rules_bike);
        println!("  Foot: {} rules", self.n_turn_rules_foot);

        println!("\nTurn Table Entries (deduplicated):");
        println!("  Total:   {} entries", self.n_turn_entries);
        println!("  Ban:     {} ({:.1}%)", self.n_ban_entries,
                 self.n_ban_entries as f64 * 100.0 / self.n_turn_entries.max(1) as f64);
        println!("  Only:    {} ({:.1}%)", self.n_only_entries,
                 self.n_only_entries as f64 * 100.0 / self.n_turn_entries.max(1) as f64);
        println!("  Penalty: {} ({:.1}%)", self.n_penalty_entries,
                 self.n_penalty_entries as f64 * 100.0 / self.n_turn_entries.max(1) as f64);
        println!("  None:    {} ({:.1}%)", self.n_none_entries,
                 self.n_none_entries as f64 * 100.0 / self.n_turn_entries.max(1) as f64);

        println!("\nNBG Junction Analysis:");
        println!("  Total NBG nodes:        {}", self.n_nbg_nodes);
        println!("  Multi-way (degree > 2): {} ({:.2}%)", self.n_junctions_multi_way,
                 self.n_junctions_multi_way as f64 * 100.0 / self.n_nbg_nodes.max(1) as f64);
        println!("  Degree = 3:             {} ({:.2}%)", self.n_junctions_degree_3,
                 self.n_junctions_degree_3 as f64 * 100.0 / self.n_nbg_nodes.max(1) as f64);
        println!("  Degree >= 4:            {} ({:.2}%)", self.n_junctions_degree_4_plus,
                 self.n_junctions_degree_4_plus as f64 * 100.0 / self.n_nbg_nodes.max(1) as f64);
        println!("  With explicit restrict: {} ({:.2}%)", self.n_junctions_with_restriction,
                 self.n_junctions_with_restriction as f64 * 100.0 / self.n_nbg_nodes.max(1) as f64);

        println!("\nJunction Degree Distribution:");
        println!("  p50: {}", self.degree_p50);
        println!("  p90: {}", self.degree_p90);
        println!("  p95: {}", self.degree_p95);
        println!("  p99: {}", self.degree_p99);
        println!("  max: {}", self.degree_max);

        println!("\nEBG Arc Analysis:");
        println!("  Total arcs:        {}", self.n_ebg_arcs);
        println!("  With ban:          {} ({:.2}%)", self.n_arcs_with_ban,
                 self.n_arcs_with_ban as f64 * 100.0 / self.n_ebg_arcs.max(1) as f64);
        println!("  With penalty:      {} ({:.2}%)", self.n_arcs_with_penalty,
                 self.n_arcs_with_penalty as f64 * 100.0 / self.n_ebg_arcs.max(1) as f64);
        println!("  Allow all modes:   {} ({:.2}%)", self.n_arcs_allow_all_modes,
                 self.n_arcs_allow_all_modes as f64 * 100.0 / self.n_ebg_arcs.max(1) as f64);

        println!("\nU-Turn Analysis:");
        println!("  Potential U-turns:     {}", self.n_potential_uturns);
        println!("  Allowed for car:       {} ({:.2}%)", self.n_uturns_allowed_car,
                 self.n_uturns_allowed_car as f64 * 100.0 / self.n_potential_uturns.max(1) as f64);
        println!("  At dead-ends only:     {}", self.n_uturns_at_deadends);

        println!("\nTurn-Relevant Junction Breakdown:");
        let b = &self.junctions_by_reason;
        println!("  Has explicit OSM restriction: {} ({:.2}%)", b.has_explicit_restriction,
                 b.has_explicit_restriction as f64 * 100.0 / self.n_nbg_nodes.max(1) as f64);
        println!("\n  (Informational - NOT counted as turn-relevant:)");
        println!("  Multi-way junctions (U-turn policy): {} ({:.2}%)", b.has_uturn_ban,
                 b.has_uturn_ban as f64 * 100.0 / self.n_nbg_nodes.max(1) as f64);
        println!("  Note: U-turn bans are handled by search policy, not junction expansion");

        println!("\n  === SUMMARY ===");
        println!("  TRUE turn-relevant junctions: {} ({:.2}%)", b.total_turn_relevant,
                 b.total_turn_relevant as f64 * 100.0 / self.n_nbg_nodes.max(1) as f64);
        println!("  Turn-free junctions:          {} ({:.2}%)", b.turn_free,
                 b.turn_free as f64 * 100.0 / self.n_nbg_nodes.max(1) as f64);

        // Verdict
        println!("\n=== VERDICT FOR NODE-BASED CH + JUNCTION EXPANSION ===\n");

        let expansion_ratio = b.total_turn_relevant as f64 / self.n_nbg_nodes.max(1) as f64;
        if expansion_ratio < 0.05 {
            println!("  EXCELLENT: Only {:.2}% of junctions need expansion", expansion_ratio * 100.0);
            println!("  → Node-based CH + junction expansion is HIGHLY RECOMMENDED");
            println!("  → Expected overhead: minimal (most searches never hit turn-relevant junctions)");
        } else if expansion_ratio < 0.15 {
            println!("  GOOD: {:.2}% of junctions need expansion", expansion_ratio * 100.0);
            println!("  → Node-based CH + junction expansion is VIABLE");
            println!("  → Expected overhead: moderate (some searches will need expansion)");
        } else {
            println!("  CAUTION: {:.2}% of junctions need expansion", expansion_ratio * 100.0);
            println!("  → Node-based CH may lose most benefit");
            println!("  → Consider: edge-based CH might be necessary");
        }

        println!();
    }
}

/// Analyze turn model from raw files
pub fn analyze_turn_model(
    ebg_nodes_path: &Path,
    ebg_csr_path: &Path,
    turn_table_path: &Path,
    nbg_csr_path: &Path,
    turn_rules_car_path: &Path,
    turn_rules_bike_path: &Path,
    turn_rules_foot_path: &Path,
) -> Result<TurnModelAnalysis> {
    println!("[1/7] Loading EBG nodes...");
    let ebg_nodes = EbgNodesFile::read(ebg_nodes_path)?;
    println!("  {} EBG nodes", ebg_nodes.n_nodes);

    println!("[2/7] Loading EBG CSR...");
    let ebg_csr = EbgCsrFile::read(ebg_csr_path)?;
    println!("  {} arcs", ebg_csr.n_arcs);

    println!("[3/7] Loading turn table...");
    let turn_table = TurnTableFile::read(turn_table_path)?;
    println!("  {} entries", turn_table.n_entries);

    println!("[4/7] Loading NBG CSR...");
    let nbg_csr = NbgCsrFile::read(nbg_csr_path)?;
    println!("  {} nodes", nbg_csr.n_nodes);

    println!("[5/7] Loading turn rules (car)...");
    let car_rules = turn_rules::read_all(turn_rules_car_path)?;
    println!("  {} rules", car_rules.len());

    println!("[6/7] Loading turn rules (bike)...");
    let bike_rules = turn_rules::read_all(turn_rules_bike_path)?;
    println!("  {} rules", bike_rules.len());

    println!("[7/7] Loading turn rules (foot)...");
    let foot_rules = turn_rules::read_all(turn_rules_foot_path)?;
    println!("  {} rules", foot_rules.len());

    println!("\nAnalyzing...");

    // Count turn table entries by kind
    let mut n_ban = 0;
    let mut n_only = 0;
    let mut n_penalty = 0;
    let mut n_none = 0;

    for entry in &turn_table.entries {
        match entry.kind {
            TurnKind::Ban => n_ban += 1,
            TurnKind::Only => n_only += 1,
            TurnKind::Penalty => n_penalty += 1,
            TurnKind::None => n_none += 1,
        }
    }

    // Analyze EBG arcs
    let mut n_arcs_with_ban = 0;
    let mut n_arcs_with_penalty = 0;
    let mut n_arcs_allow_all = 0;

    for i in 0..ebg_csr.n_arcs as usize {
        let turn_idx = ebg_csr.turn_idx[i] as usize;
        let entry = &turn_table.entries[turn_idx];

        match entry.kind {
            TurnKind::Ban => n_arcs_with_ban += 1,
            TurnKind::Penalty => n_arcs_with_penalty += 1,
            _ => {}
        }

        if entry.mode_mask == 0b111 {  // All modes allowed
            n_arcs_allow_all += 1;
        }
    }

    // Analyze NBG junction degrees
    let mut degrees: Vec<usize> = Vec::with_capacity(nbg_csr.n_nodes as usize);
    for i in 0..nbg_csr.n_nodes as usize {
        let start = nbg_csr.offsets[i] as usize;
        let end = nbg_csr.offsets[i + 1] as usize;
        degrees.push(end - start);
    }

    degrees.sort_unstable();

    let n_nbg = nbg_csr.n_nodes as usize;
    let degree_p50 = degrees[n_nbg / 2];
    let degree_p90 = degrees[n_nbg * 90 / 100];
    let degree_p95 = degrees[n_nbg * 95 / 100];
    let degree_p99 = degrees[n_nbg * 99 / 100];
    let degree_max = *degrees.last().unwrap_or(&0);

    let n_multi_way = degrees.iter().filter(|&&d| d > 2).count();
    let n_degree_3 = degrees.iter().filter(|&&d| d == 3).count();
    let n_degree_4_plus = degrees.iter().filter(|&&d| d >= 4).count();

    // Identify junctions with explicit restrictions
    let mut restricted_nodes: HashSet<i64> = HashSet::new();
    for rule in &car_rules {
        restricted_nodes.insert(rule.via_node_id);
    }
    let n_with_restriction = restricted_nodes.len();

    // U-turn analysis: count EBG arcs that are potential U-turns
    // U-turn: edge A→B followed by B→A (same NBG edge, opposite direction)
    let mut n_potential_uturns = 0;
    let mut n_uturns_allowed_car = 0;

    // Build NBG edge index: for each EBG node, track which NBG nodes it connects
    // EBG node = directed NBG edge: (tail_nbg, head_nbg)
    let ebg_nodes_vec = &ebg_nodes.nodes;

    // For each EBG node, check if any of its outgoing arcs lead back to the same
    // NBG edge in reverse direction
    for (ebg_id, ebg_node) in ebg_nodes_vec.iter().enumerate() {
        let start = ebg_csr.offsets[ebg_id] as usize;
        let end = ebg_csr.offsets[ebg_id + 1] as usize;

        for i in start..end {
            let target_ebg = ebg_csr.heads[i] as usize;
            let target_node = &ebg_nodes_vec[target_ebg];

            // U-turn if we go back to where we came from
            if ebg_node.tail_nbg == target_node.head_nbg && ebg_node.head_nbg == target_node.tail_nbg {
                n_potential_uturns += 1;

                // Check if car is allowed
                let turn_idx = ebg_csr.turn_idx[i] as usize;
                let entry = &turn_table.entries[turn_idx];
                if (entry.mode_mask & 0b001) != 0 && entry.kind != TurnKind::Ban {
                    n_uturns_allowed_car += 1;
                }
            }
        }
    }

    // Count dead-end U-turns (degree 1 nodes where U-turn is the only option)
    let n_deadends = degrees.iter().filter(|&&d| d == 1).count();

    // Junction breakdown by reason
    // Key insight: U-turn bans are NOT junction expansion problems!
    // - U-turns can be handled by simple search policy (don't reverse)
    // - Only EXPLICIT restrictions require actual junction expansion
    //
    // The real "turn-relevant" junctions are those with:
    // 1. Explicit no_left_turn, no_right_turn, only_straight_on, etc.
    // 2. Turn penalties (different cost per turn direction)
    //
    // NOT counted as turn-relevant:
    // - U-turn bans (handled by search policy, not junction expansion)
    // - Oneway constraints (handled by edge directionality)
    // - Mode restrictions (handled by mode filtering in edge weights)

    let breakdown = JunctionBreakdown {
        has_explicit_restriction: n_with_restriction,
        has_uturn_ban: n_multi_way,  // Informational only - NOT counted as turn-relevant
        has_oneway_constraint: 0,    // Would need way_attrs to compute
        has_mode_restriction: 0,     // Would need full arc analysis
        // Only explicit restrictions truly need junction expansion!
        total_turn_relevant: n_with_restriction,
        turn_free: n_nbg.saturating_sub(n_with_restriction),
    };

    Ok(TurnModelAnalysis {
        n_turn_rules_car: car_rules.len(),
        n_turn_rules_bike: bike_rules.len(),
        n_turn_rules_foot: foot_rules.len(),

        n_turn_entries: turn_table.entries.len(),
        n_ban_entries: n_ban,
        n_only_entries: n_only,
        n_penalty_entries: n_penalty,
        n_none_entries: n_none,

        n_nbg_nodes: n_nbg,
        n_junctions_with_restriction: n_with_restriction,
        n_junctions_multi_way: n_multi_way,
        n_junctions_degree_3: n_degree_3,
        n_junctions_degree_4_plus: n_degree_4_plus,

        n_ebg_arcs: ebg_csr.n_arcs as usize,
        n_arcs_with_ban,
        n_arcs_with_penalty,
        n_arcs_allow_all_modes: n_arcs_allow_all,

        n_potential_uturns,
        n_uturns_allowed_car,
        n_uturns_at_deadends: n_deadends,

        junctions_by_reason: breakdown,

        degree_p50,
        degree_p90,
        degree_p95,
        degree_p99,
        degree_max,
    })
}
