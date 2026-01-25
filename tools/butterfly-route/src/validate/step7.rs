///! Step 7 validation - CCH topology lock conditions (per-mode on filtered EBG)

use anyhow::Result;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::{BinaryHeap, HashSet};
use std::path::Path;

use crate::formats::{CchTopoFile, FilteredEbg, FilteredEbgFile, OrderEbgFile};
use crate::step7::Step7Result;

#[derive(Debug, Serialize, Deserialize)]
pub struct Step7LockFile {
    pub mode: String,
    pub inputs_sha256: String,
    pub topo_sha256: String,
    pub n_nodes: u32,
    pub n_original_arcs: u64,
    pub n_shortcuts: u64,
    pub n_up_edges: u64,
    pub n_down_edges: u64,
    pub shortcut_ratio: f64,
    pub build_time_ms: u64,
    pub created_at_utc: String,
}

/// Validate Step 7 outputs and generate lock file
pub fn validate_step7(
    result: &Step7Result,
    filtered_ebg_path: &Path,
    order_path: &Path,
) -> Result<Step7LockFile> {
    let mode_name = match result.mode {
        crate::profile_abi::Mode::Car => "car",
        crate::profile_abi::Mode::Bike => "bike",
        crate::profile_abi::Mode::Foot => "foot",
    };
    println!("\n🔐 Running Step 7 validation for {} mode...\n", mode_name);

    // Load data
    let topo = CchTopoFile::read(&result.topo_path)?;
    let filtered_ebg = FilteredEbgFile::read(filtered_ebg_path)?;
    let order = OrderEbgFile::read(order_path)?;

    // Lock Condition A: Structural integrity
    println!("A. Structural integrity checks...");
    verify_node_counts(&topo, &filtered_ebg, &order)?;
    println!("  ✓ Node counts match");
    verify_csr_structure(&topo)?;
    println!("  ✓ CSR structure valid");

    // Lock Condition B: Hierarchy property
    println!("\nB. Hierarchy property checks...");
    verify_upward_property(&topo, &order)?;
    println!("  ✓ Upward edges go to higher ranks");
    verify_downward_property(&topo, &order)?;
    println!("  ✓ Downward edges go to lower ranks");

    // Lock Condition C: Original edges preserved
    println!("\nC. Edge preservation checks...");
    verify_original_edges(&topo, &filtered_ebg)?;
    println!("  ✓ Original edges preserved");

    // Lock Condition D: Reachability correctness (random samples)
    println!("\nD. Reachability correctness checks (random samples)...");
    verify_query_correctness(&topo, &filtered_ebg, &order, 100)?;
    println!("  ✓ CCH reachability matches BFS on {} samples", 100);

    // Compute SHA-256
    let inputs_sha256 = hex::encode(&topo.inputs_sha);
    let topo_sha256 = compute_file_sha256(&result.topo_path)?;

    let shortcut_ratio = result.n_shortcuts as f64 / result.n_original_arcs as f64;

    println!("\n✅ Step 7 validation passed for {} mode!", mode_name);
    println!("  Shortcut ratio: {:.2}x original arcs", shortcut_ratio);

    Ok(Step7LockFile {
        mode: mode_name.to_string(),
        inputs_sha256,
        topo_sha256,
        n_nodes: result.n_nodes,
        n_original_arcs: result.n_original_arcs,
        n_shortcuts: result.n_shortcuts,
        n_up_edges: result.n_up_edges,
        n_down_edges: result.n_down_edges,
        shortcut_ratio,
        build_time_ms: result.build_time_ms,
        created_at_utc: chrono::Utc::now().to_rfc3339(),
    })
}

fn verify_node_counts(
    topo: &crate::formats::CchTopo,
    filtered_ebg: &FilteredEbg,
    order: &crate::formats::OrderEbg,
) -> Result<()> {
    anyhow::ensure!(
        topo.n_nodes == filtered_ebg.n_filtered_nodes,
        "topo.n_nodes ({}) != ebg.n_nodes ({})",
        topo.n_nodes,
        filtered_ebg.n_filtered_nodes
    );
    anyhow::ensure!(
        topo.n_nodes == order.n_nodes,
        "topo.n_nodes ({}) != order.n_nodes ({})",
        topo.n_nodes,
        order.n_nodes
    );
    Ok(())
}

fn verify_csr_structure(topo: &crate::formats::CchTopo) -> Result<()> {
    let n = topo.n_nodes as usize;

    // Up graph
    anyhow::ensure!(
        topo.up_offsets.len() == n + 1,
        "up_offsets length {} != n_nodes + 1 ({})",
        topo.up_offsets.len(),
        n + 1
    );
    let n_up = *topo.up_offsets.last().unwrap() as usize;
    anyhow::ensure!(
        topo.up_targets.len() == n_up,
        "up_targets length {} != {}",
        topo.up_targets.len(),
        n_up
    );
    anyhow::ensure!(
        topo.up_is_shortcut.len() == n_up,
        "up_is_shortcut length {} != {}",
        topo.up_is_shortcut.len(),
        n_up
    );
    anyhow::ensure!(
        topo.up_middle.len() == n_up,
        "up_middle length {} != {}",
        topo.up_middle.len(),
        n_up
    );

    // Down graph
    anyhow::ensure!(
        topo.down_offsets.len() == n + 1,
        "down_offsets length {} != n_nodes + 1 ({})",
        topo.down_offsets.len(),
        n + 1
    );
    let n_down = *topo.down_offsets.last().unwrap() as usize;
    anyhow::ensure!(
        topo.down_targets.len() == n_down,
        "down_targets length {} != {}",
        topo.down_targets.len(),
        n_down
    );
    anyhow::ensure!(
        topo.down_is_shortcut.len() == n_down,
        "down_is_shortcut length {} != {}",
        topo.down_is_shortcut.len(),
        n_down
    );
    anyhow::ensure!(
        topo.down_middle.len() == n_down,
        "down_middle length {} != {}",
        topo.down_middle.len(),
        n_down
    );

    // Offsets are monotonic
    for i in 1..=n {
        anyhow::ensure!(
            topo.up_offsets[i] >= topo.up_offsets[i - 1],
            "up_offsets not monotonic at {}",
            i
        );
        anyhow::ensure!(
            topo.down_offsets[i] >= topo.down_offsets[i - 1],
            "down_offsets not monotonic at {}",
            i
        );
    }

    Ok(())
}

fn verify_upward_property(topo: &crate::formats::CchTopo, _order: &crate::formats::OrderEbg) -> Result<()> {
    let n = topo.n_nodes as usize;

    // Sample check: verify first 10000 nodes
    let check_limit = n.min(10000);

    // RANK-ALIGNED CCH: node_id IS the rank, no perm lookup needed!
    // In rank-aligned CCH, offsets[rank] gives edges for node at that rank
    // targets[i] is the target's rank
    for rank_u in 0..check_limit {
        let start = topo.up_offsets[rank_u] as usize;
        let end = topo.up_offsets[rank_u + 1] as usize;

        for i in start..end {
            // In rank-aligned CCH, up_targets[i] IS the target's rank
            let rank_v = topo.up_targets[i] as usize;
            anyhow::ensure!(
                rank_v > rank_u,
                "Upward edge {} -> {} violates hierarchy: rank {} -> {}",
                rank_u,
                rank_v,
                rank_u,
                rank_v
            );
        }
    }

    Ok(())
}

fn verify_downward_property(topo: &crate::formats::CchTopo, _order: &crate::formats::OrderEbg) -> Result<()> {
    let n = topo.n_nodes as usize;

    // Sample check: verify first 10000 nodes
    let check_limit = n.min(10000);

    // RANK-ALIGNED CCH: node_id IS the rank, no perm lookup needed!
    for rank_u in 0..check_limit {
        let start = topo.down_offsets[rank_u] as usize;
        let end = topo.down_offsets[rank_u + 1] as usize;

        for i in start..end {
            // In rank-aligned CCH, down_targets[i] IS the target's rank
            let rank_v = topo.down_targets[i] as usize;
            anyhow::ensure!(
                rank_v < rank_u,
                "Downward edge {} -> {} violates hierarchy: rank {} -> {}",
                rank_u,
                rank_v,
                rank_u,
                rank_v
            );
        }
    }

    Ok(())
}

fn verify_original_edges(topo: &crate::formats::CchTopo, filtered_ebg: &FilteredEbg) -> Result<()> {
    // Count non-shortcut edges
    let n_up_original = topo.up_is_shortcut.iter().filter(|&&x| !x).count();
    let n_down_original = topo.down_is_shortcut.iter().filter(|&&x| !x).count();
    let total_original = n_up_original + n_down_original;

    // Count self-loops in EBG (which we exclude from CCH)
    let mut n_self_loops = 0usize;
    for u in 0..filtered_ebg.n_filtered_nodes as usize {
        let start = filtered_ebg.offsets[u] as usize;
        let end = filtered_ebg.offsets[u + 1] as usize;
        for i in start..end {
            if filtered_ebg.heads[i] == u as u32 {
                n_self_loops += 1;
            }
        }
    }

    let expected = filtered_ebg.n_filtered_arcs as usize - n_self_loops;

    anyhow::ensure!(
        total_original == expected,
        "Original edge count mismatch: {} in CCH, {} in EBG (excluding {} self-loops)",
        total_original,
        expected,
        n_self_loops
    );

    if n_self_loops > 0 {
        println!("  (note: {} self-loops excluded from CCH)", n_self_loops);
    }

    Ok(())
}

fn compute_file_sha256(path: &Path) -> Result<String> {
    use sha2::{Digest, Sha256};
    let data = std::fs::read(path)?;
    let hash = Sha256::digest(&data);
    Ok(hex::encode(hash))
}

/// Verify CCH query correctness by comparing against BFS on original graph
fn verify_query_correctness(
    topo: &crate::formats::CchTopo,
    filtered_ebg: &FilteredEbg,
    order: &crate::formats::OrderEbg,
    n_samples: usize,
) -> Result<()> {
    use std::hash::{Hash, Hasher};

    let n_nodes = filtered_ebg.n_filtered_nodes as usize;

    // RANK-ALIGNED CCH: Need to convert between filtered_id and rank
    // perm[filtered_id] = rank (for query input)
    // rank_to_filtered[rank] = filtered_id (from topo, for mapping results)
    let perm = &order.perm;

    // Precompute reverse DOWN adjacency (for backward search)
    // Note: In rank-aligned CCH, indices are ranks
    println!("    Building reverse DOWN graph...");
    let mut down_rev: Vec<Vec<u32>> = vec![Vec::new(); n_nodes];
    for rank_u in 0..n_nodes {
        let start = topo.down_offsets[rank_u] as usize;
        let end = topo.down_offsets[rank_u + 1] as usize;
        for i in start..end {
            let rank_v = topo.down_targets[i] as usize;
            down_rev[rank_v].push(rank_u as u32);
        }
    }
    println!("    ✓ Built reverse DOWN graph");

    // Generate deterministic "random" pairs using a simple hash
    // These are filtered_id pairs (for BFS)
    let pairs: Vec<(usize, usize)> = (0..n_samples)
        .map(|i| {
            // Simple deterministic pseudo-random
            let mut h = std::collections::hash_map::DefaultHasher::new();
            i.hash(&mut h);
            let hash = h.finish();
            let src = (hash % n_nodes as u64) as usize;
            let dst = ((hash >> 32) % n_nodes as u64) as usize;
            (src, dst)
        })
        .collect();

    // Run queries in parallel
    let down_rev_ref = &down_rev;
    let results: Vec<Result<(), String>> = pairs
        .par_iter()
        .map(|&(src_filtered, dst_filtered)| {
            // Skip self-queries
            if src_filtered == dst_filtered {
                return Ok(());
            }

            // BFS on original graph (unweighted shortest path, uses filtered IDs)
            let original_dist = bfs_distance(filtered_ebg, src_filtered, dst_filtered);

            // Convert filtered_id to rank for CCH query
            let src_rank = perm[src_filtered] as usize;
            let dst_rank = perm[dst_filtered] as usize;

            // CCH query (up-then-down with unweighted edges, uses rank positions)
            let cch_dist = cch_query_distance_with_rev(topo, down_rev_ref, src_rank, dst_rank);

            // For unweighted CCH, we only check REACHABILITY, not distance
            // CCH shortcuts compress multiple hops into one, so hop counts differ
            match (original_dist.is_some(), cch_dist.is_some()) {
                (true, true) => Ok(()), // Both reachable - OK
                (false, false) => Ok(()), // Both unreachable - OK
                (true, false) => {
                    Err(format!(
                        "CCH reports unreachable but BFS found path {}->{} (filtered) / {}->{} (rank), dist={}",
                        src_filtered, dst_filtered, src_rank, dst_rank, original_dist.unwrap()
                    ))
                }
                (false, true) => {
                    // CCH found a path but BFS didn't - this shouldn't happen
                    // unless there's a bug in BFS or CCH has spurious paths
                    Err(format!(
                        "CCH found path {}->{} (filtered) / {}->{} (rank) but BFS reports unreachable",
                        src_filtered, dst_filtered, src_rank, dst_rank
                    ))
                }
            }
        })
        .collect();

    // Check for any failures
    let failures: Vec<_> = results.iter().filter_map(|r| r.as_ref().err()).collect();
    if !failures.is_empty() {
        let sample_failures: Vec<_> = failures.iter().take(5).collect();
        anyhow::bail!(
            "Query correctness check failed ({} failures):\n  {}",
            failures.len(),
            sample_failures.iter().map(|s| s.as_str()).collect::<Vec<_>>().join("\n  ")
        );
    }

    Ok(())
}

/// BFS shortest path distance on original EBG (unweighted)
fn bfs_distance(filtered_ebg: &FilteredEbg, src: usize, dst: usize) -> Option<u32> {
    use std::collections::VecDeque;

    if src == dst {
        return Some(0);
    }

    let n = filtered_ebg.n_filtered_nodes as usize;
    let mut dist = vec![u32::MAX; n];
    let mut queue = VecDeque::new();

    dist[src] = 0;
    queue.push_back(src);

    while let Some(u) = queue.pop_front() {
        if u == dst {
            return Some(dist[dst]);
        }

        let start = filtered_ebg.offsets[u] as usize;
        let end = filtered_ebg.offsets[u + 1] as usize;

        for i in start..end {
            let v = filtered_ebg.heads[i] as usize;
            if dist[v] == u32::MAX {
                dist[v] = dist[u] + 1;
                queue.push_back(v);
            }
        }
    }

    if dist[dst] == u32::MAX { None } else { Some(dist[dst]) }
}

/// CCH query: forward UP search from src, backward search using reversed DOWN edges from dst
/// The path goes: src --(up)--> meeting --(down)--> dst
fn cch_query_distance_with_rev(
    topo: &crate::formats::CchTopo,
    down_rev: &[Vec<u32>],
    src: usize,
    dst: usize,
) -> Option<u32> {
    use std::collections::VecDeque;

    if src == dst {
        return Some(0);
    }

    let n = topo.n_nodes as usize;

    // Forward search from src using UP edges (BFS for unweighted)
    let mut dist_fwd = vec![u32::MAX; n];
    let mut queue_fwd = VecDeque::new();
    dist_fwd[src] = 0;
    queue_fwd.push_back(src);

    while let Some(u) = queue_fwd.pop_front() {
        let d = dist_fwd[u];
        // Explore upward edges
        let start = topo.up_offsets[u] as usize;
        let end = topo.up_offsets[u + 1] as usize;
        for i in start..end {
            let v = topo.up_targets[i] as usize;
            if dist_fwd[v] == u32::MAX {
                dist_fwd[v] = d + 1;
                queue_fwd.push_back(v);
            }
        }
    }

    // Backward search from dst using reversed DOWN edges
    // down_rev[v] contains all u where there's a DOWN edge u->v
    // This lets us find nodes that can reach dst via DOWN edges
    let mut dist_bwd = vec![u32::MAX; n];
    let mut queue_bwd = VecDeque::new();
    dist_bwd[dst] = 0;
    queue_bwd.push_back(dst);

    while let Some(v) = queue_bwd.pop_front() {
        let d = dist_bwd[v];
        // Follow reversed DOWN edges: nodes that have DOWN edges TO v
        for &u in &down_rev[v] {
            let u_idx = u as usize;
            if dist_bwd[u_idx] == u32::MAX {
                dist_bwd[u_idx] = d + 1;
                queue_bwd.push_back(u_idx);
            }
        }
    }

    // Find best meeting point
    let mut best = u32::MAX;
    for u in 0..n {
        if dist_fwd[u] != u32::MAX && dist_bwd[u] != u32::MAX {
            let total = dist_fwd[u].saturating_add(dist_bwd[u]);
            if total < best {
                best = total;
            }
        }
    }

    if best == u32::MAX { None } else { Some(best) }
}
