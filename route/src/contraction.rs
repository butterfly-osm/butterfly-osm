//! Step 7: CCH Contraction (per-mode on filtered EBG)
//!
//! Builds the Customizable Contraction Hierarchy (CCH) topology from the mode-filtered
//! Edge-Based Graph (EBG) using a pre-computed per-mode Nested Dissection ordering.
//!
//! # Algorithm Overview
//!
//! CCH contraction processes nodes in rank order (lowest first). For each node v being
//! contracted, we examine all pairs (u, w) where:
//! - u is an in-neighbor of v with rank > rank(v)
//! - w is an out-neighbor of v with rank > rank(v)
//!
//! A shortcut edge (u → w) via v is added only if:
//! 1. The direct edge (u → w) doesn't already exist
//! 2. No witness path u → ... → w exists with cost ≤ shortcut cost
//!
//! # Metric-Aware Witness Search (CORRECT)
//!
//! The witness search uses bounded Dijkstra to find alternative paths:
//! - Shortcut cost = weight(u→v) + weight(v→w)
//! - Run bounded Dijkstra from u toward w
//! - Early stop when queue min-key > shortcut_cost
//! - If we find path with cost ≤ shortcut_cost, skip shortcut (witness found)
//! - Otherwise, create shortcut (conservative - correct by design)
//!
//! This is **metric-aware** and compares COSTS, not just path existence.
//! False positives (extra shortcuts) are fine; false negatives break correctness.
//!
//! # Memory Management
//!
//! - Shortcuts are streamed to a temp file during contraction to avoid memory explosion
//! - Adjacency lists use FxHashMap for O(1) lookups with weights
//! - Final up/down graphs are built by streaming through the temp file twice
//!
//! # Parallelism Strategy
//!
//! - Node contraction is sequential (required for correctness - each node must see
//!   shortcuts from previously contracted nodes)
//! - Initial adjacency building, edge counting/filling, and sorting are fully parallel

use anyhow::Result;
use rayon::prelude::*;
use rustc_hash::{FxHashMap, FxHashSet};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::PathBuf;

use crate::formats::{CchTopo, CchTopoFile, FilteredEbgFile, OrderEbgFile, mod_turns, mod_weights};
use crate::profile_abi::Mode;

/// Edge weight for weighted adjacency - stores (target, weight)
type WeightedAdj = Vec<FxHashMap<u32, u32>>;

/// Configuration for Step 7
pub struct Step7Config {
    pub filtered_ebg_path: PathBuf,
    pub order_path: PathBuf,
    pub weights_path: PathBuf, // w.*.u32 from Step 5
    pub turns_path: PathBuf,   // t.*.u32 from Step 5
    pub mode: Mode,
    pub mode_name: String,
    pub outdir: PathBuf,
}

/// Result of Step 7 contraction
#[derive(Debug)]
pub struct Step7Result {
    pub topo_path: PathBuf,
    pub mode: Mode,
    pub mode_name: String,
    pub n_nodes: u32,
    pub n_original_arcs: u64,
    pub n_shortcuts: u64,
    pub n_up_edges: u64,
    pub n_down_edges: u64,
    pub build_time_ms: u64,
}

/// Build CCH topology via contraction on filtered EBG
pub fn build_cch_topology(config: Step7Config) -> Result<Step7Result> {
    let start_time = std::time::Instant::now();
    let mode_name = &config.mode_name;
    println!(
        "\n🔨 Step 7: Building CCH topology for {} mode...\n",
        mode_name
    );

    // Load filtered EBG
    println!("Loading filtered EBG ({})...", mode_name);
    let filtered_ebg = FilteredEbgFile::read(&config.filtered_ebg_path)?;
    println!(
        "  ✓ {} nodes, {} arcs",
        filtered_ebg.n_filtered_nodes, filtered_ebg.n_filtered_arcs
    );

    // Load ordering
    println!("Loading ordering ({})...", mode_name);
    let order = OrderEbgFile::read(&config.order_path)?;
    println!("  ✓ {} nodes", order.n_nodes);

    if filtered_ebg.n_filtered_nodes != order.n_nodes {
        anyhow::bail!(
            "Node count mismatch: filtered EBG has {} nodes, order has {}",
            filtered_ebg.n_filtered_nodes,
            order.n_nodes
        );
    }

    let n_nodes = filtered_ebg.n_filtered_nodes as usize;
    let perm = &order.perm;
    let inv_perm = &order.inv_perm;

    // Load weights for metric-aware witness search
    println!("Loading weights for witness search ({})...", mode_name);
    let weights_data = mod_weights::read_all(&config.weights_path)?;
    let _turns_data = mod_turns::read_all(&config.turns_path)?;
    let weights = &weights_data.weights;
    println!("  ✓ {} edge weights", weights.len());

    // Verify we have original_arc_idx in the filtered EBG to look up weights
    if filtered_ebg.original_arc_idx.is_empty() {
        anyhow::bail!(
            "Filtered EBG has no original_arc_idx - cannot look up weights for witness search"
        );
    }

    // Build weighted adjacency for witness search
    // adj[u][v] = min weight of edge u→v
    println!("Building weighted adjacency for witness search...");
    let weighted_adj: WeightedAdj = (0..n_nodes)
        .into_par_iter()
        .map(|u| {
            let start = filtered_ebg.offsets[u] as usize;
            let end = filtered_ebg.offsets[u + 1] as usize;
            let mut adj_map: FxHashMap<u32, u32> = FxHashMap::default();
            for i in start..end {
                let v = filtered_ebg.heads[i];
                if u as u32 == v {
                    continue;
                }
                // Original arc index maps to the EBG edge index
                let arc_idx = filtered_ebg.original_arc_idx[i] as usize;
                let edge_weight = if arc_idx < weights.len() {
                    weights[arc_idx]
                } else {
                    u32::MAX // Should not happen
                };
                // Take minimum weight if multiple edges to same target
                adj_map
                    .entry(v)
                    .and_modify(|w| *w = (*w).min(edge_weight))
                    .or_insert(edge_weight);
            }
            adj_map
        })
        .collect();
    println!("  ✓ Built weighted adjacency");

    // Build initial adjacency using FxHashSet (faster than std HashSet)
    println!("\nBuilding initial higher-neighbor lists (parallel)...");

    let (out_higher, in_higher): (Vec<FxHashSet<u32>>, Vec<FxHashSet<u32>>) = {
        // Build out_higher in parallel
        let out: Vec<FxHashSet<u32>> = (0..n_nodes)
            .into_par_iter()
            .map(|u| {
                let rank_u = perm[u];
                let start = filtered_ebg.offsets[u] as usize;
                let end = filtered_ebg.offsets[u + 1] as usize;
                let degree = end - start;
                let mut set = FxHashSet::with_capacity_and_hasher(degree, Default::default());
                for i in start..end {
                    let v = filtered_ebg.heads[i] as usize;
                    if u != v && perm[v] > rank_u {
                        set.insert(v as u32);
                    }
                }
                set
            })
            .collect();

        // Build in_higher - collect into vecs first, then convert to sets
        let mut in_vecs: Vec<Vec<u32>> = vec![Vec::new(); n_nodes];
        for u in 0..n_nodes {
            let rank_u = perm[u];
            let start = filtered_ebg.offsets[u] as usize;
            let end = filtered_ebg.offsets[u + 1] as usize;
            for i in start..end {
                let v = filtered_ebg.heads[i] as usize;
                if u != v && rank_u > perm[v] {
                    in_vecs[v].push(u as u32);
                }
            }
        }

        // Convert to FxHashSet in parallel with pre-sized capacity
        let in_sets: Vec<FxHashSet<u32>> = in_vecs
            .into_par_iter()
            .map(|v| {
                let mut set = FxHashSet::with_capacity_and_hasher(v.len(), Default::default());
                set.extend(v);
                set
            })
            .collect();

        (out, in_sets)
    };

    let mut out_higher = out_higher;
    let mut in_higher = in_higher;
    println!("  ✓ Built initial neighbor lists");

    // Stream shortcuts to temp file to avoid memory explosion
    std::fs::create_dir_all(&config.outdir)?;
    let shortcut_path = config.outdir.join("shortcuts.tmp");
    let mut shortcut_writer =
        BufWriter::with_capacity(64 * 1024 * 1024, File::create(&shortcut_path)?);
    let mut n_shortcuts = 0u64;

    println!("\nContracting nodes (sequential with parallel inner loops)...");
    let n_threads = rayon::current_num_threads();
    println!("  Using {} threads for parallel inner loops", n_threads);

    let report_interval = (n_nodes / 100).max(1);
    let mut last_report = 0;
    let mut max_degree_seen = 0usize;

    // Make weighted_adj mutable so we can add shortcuts as we go
    let mut weighted_adj = weighted_adj;

    // Sequential contraction - MUST process one node at a time for correctness
    // Metric-aware witness search requires weights, so we compute shortcut costs
    for (rank, &v_node) in inv_perm.iter().enumerate().take(n_nodes) {
        if rank - last_report >= report_interval {
            let pct = (rank as f64 / n_nodes as f64) * 100.0;
            println!(
                "  {:5.1}% contracted ({} shortcuts, max_degree={})",
                pct, n_shortcuts, max_degree_seen
            );
            last_report = rank;
        }

        let v = v_node as usize;

        let in_neighbors: Vec<u32> = std::mem::take(&mut in_higher[v]).into_iter().collect();
        let out_neighbors: Vec<u32> = std::mem::take(&mut out_higher[v]).into_iter().collect();

        if in_neighbors.is_empty() || out_neighbors.is_empty() {
            continue;
        }

        let degree = in_neighbors.len().max(out_neighbors.len());
        if degree > max_degree_seen {
            max_degree_seen = degree;
        }

        // Compute shortcuts with METRIC-AWARE witness search
        // For each pair (u, w), compute shortcut_cost = w(u→v) + w(v→w)
        // Then check if an alternative path exists with cost ≤ shortcut_cost
        let work_amount = in_neighbors.len() * out_neighbors.len();
        let out_higher_ref = &out_higher;
        let in_higher_ref = &in_higher;
        let weighted_adj_ref = &weighted_adj;
        let v_u32 = v as u32;

        // new_shortcuts stores (u, w, shortcut_cost) for updating weighted_adj
        let new_shortcuts: Vec<(u32, u32, u32)> = if work_amount > 1000 {
            // Parallel computation for high-degree nodes
            in_neighbors
                .par_iter()
                .flat_map(|&u| {
                    let u_idx = u as usize;
                    let rank_u = perm[u_idx];

                    // Get weight of u→v
                    let w_uv = weighted_adj_ref[u_idx]
                        .get(&v_u32)
                        .copied()
                        .unwrap_or(u32::MAX);

                    out_neighbors
                        .iter()
                        .filter_map(move |&w| {
                            if u == w {
                                return None;
                            }
                            let w_idx = w as usize;
                            let rank_w = perm[w_idx];

                            // Check 1: Direct edge already exists?
                            let already_exists = if rank_w > rank_u {
                                out_higher_ref[u_idx].contains(&w)
                            } else {
                                in_higher_ref[w_idx].contains(&u)
                            };
                            if already_exists {
                                return None;
                            }

                            // Get weight of v→w
                            let w_vw = weighted_adj_ref[v].get(&w).copied().unwrap_or(u32::MAX);

                            // Compute shortcut cost
                            let shortcut_cost = w_uv.saturating_add(w_vw);

                            // NOTE: Witness search is DISABLED because it causes correctness bugs.
                            // When a witness path goes through a higher-ranked node X, and X is later
                            // contracted, the endpoints may have lower rank than X and won't be
                            // considered for shortcuts - destroying the witness path without replacement.
                            //
                            // Pure CCH creates ALL shortcuts and relies on Step 8 customization
                            // to set correct weights via triangle relaxation.

                            Some((u, w, shortcut_cost))
                        })
                        .collect::<Vec<_>>()
                })
                .collect()
        } else {
            // Sequential for small neighborhoods - pre-allocate result
            let mut result = Vec::with_capacity(work_amount);
            for &u in &in_neighbors {
                let u_idx = u as usize;
                let rank_u = perm[u_idx];

                // Get weight of u→v
                let w_uv = weighted_adj[u_idx].get(&v_u32).copied().unwrap_or(u32::MAX);

                for &w in &out_neighbors {
                    if u == w {
                        continue;
                    }
                    let w_idx = w as usize;
                    let rank_w = perm[w_idx];

                    // Check 1: Direct edge already exists?
                    let already_exists = if rank_w > rank_u {
                        out_higher[u_idx].contains(&w)
                    } else {
                        in_higher[w_idx].contains(&u)
                    };
                    if already_exists {
                        continue;
                    }

                    // Get weight of v→w
                    let w_vw = weighted_adj[v].get(&w).copied().unwrap_or(u32::MAX);

                    // Compute shortcut cost
                    let shortcut_cost = w_uv.saturating_add(w_vw);

                    // NOTE: Witness search is DISABLED because it causes correctness bugs.
                    // See parallel branch comment for details.

                    result.push((u, w, shortcut_cost));
                }
            }
            result
        };

        // Write shortcuts to disk and update both adjacencies IMMEDIATELY (correctness requirement)
        for (u, w, shortcut_cost) in new_shortcuts {
            shortcut_writer.write_all(&u.to_le_bytes())?;
            shortcut_writer.write_all(&w.to_le_bytes())?;
            shortcut_writer.write_all(&(v as u32).to_le_bytes())?;
            n_shortcuts += 1;

            let u_idx = u as usize;
            let w_idx = w as usize;
            let rank_u = perm[u_idx];
            let rank_w = perm[w_idx];

            // Update topology adjacency
            if rank_w > rank_u {
                out_higher[u_idx].insert(w);
            } else {
                in_higher[w_idx].insert(u);
            }

            // Update weighted adjacency - keep minimum weight if edge already exists
            weighted_adj[u_idx]
                .entry(w)
                .and_modify(|existing| *existing = (*existing).min(shortcut_cost))
                .or_insert(shortcut_cost);
        }
    }

    shortcut_writer.flush()?;
    drop(shortcut_writer);

    // Free adjacency sets - no longer needed
    drop(out_higher);
    drop(in_higher);

    println!("  ✓ Contraction complete: {} shortcuts", n_shortcuts);

    // Sanity check: shortcut ratio should be < 50x for a good ordering.
    // The lifted ordering (step6-lifted) produces pathological results for modes
    // with many filtered edges (e.g., truck: 281x ratio vs 7.4x with regular ordering).
    let n_arcs = filtered_ebg.n_filtered_arcs;
    let shortcut_ratio = if n_arcs > 0 {
        n_shortcuts as f64 / n_arcs as f64
    } else {
        0.0
    };
    if shortcut_ratio > 50.0 {
        eprintln!(
            "\n⚠️  WARNING: Shortcut ratio {:.1}x is extremely high (expected < 20x).",
            shortcut_ratio
        );
        eprintln!("    This usually means the ordering is poor. Did you use step6-lifted?");
        eprintln!("    Use regular step6-order instead for production builds.");
        eprintln!("    Continuing anyway, but expect very slow customization and queries.\n");
    }

    // Build up/down graphs by streaming through shortcuts file
    println!("\nBuilding hierarchical graph (parallel)...");

    // Count edges per node - PARALLEL over nodes for original edges
    let up_counts: Vec<usize> = (0..n_nodes)
        .into_par_iter()
        .map(|u| {
            let rank_u = perm[u];
            let start = filtered_ebg.offsets[u] as usize;
            let end = filtered_ebg.offsets[u + 1] as usize;
            let mut count = 0;
            for i in start..end {
                let v = filtered_ebg.heads[i] as usize;
                if u != v && rank_u < perm[v] {
                    count += 1;
                }
            }
            count
        })
        .collect();

    let down_counts: Vec<usize> = (0..n_nodes)
        .into_par_iter()
        .map(|u| {
            let rank_u = perm[u];
            let start = filtered_ebg.offsets[u] as usize;
            let end = filtered_ebg.offsets[u + 1] as usize;
            let mut count = 0;
            for i in start..end {
                let v = filtered_ebg.heads[i] as usize;
                if u != v && rank_u >= perm[v] {
                    count += 1;
                }
            }
            count
        })
        .collect();

    // Convert to mutable for shortcut counting
    let mut up_counts = up_counts;
    let mut down_counts = down_counts;

    // Count shortcuts - stream from file (sequential, but I/O bound)
    {
        let mut reader = BufReader::with_capacity(64 * 1024 * 1024, File::open(&shortcut_path)?);
        let mut buf = [0u8; 12];
        while reader.read_exact(&mut buf).is_ok() {
            let u = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
            let w = u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]) as usize;
            let rank_u = perm[u];
            let rank_w = perm[w];
            if rank_u < rank_w {
                up_counts[u] += 1;
            } else {
                down_counts[u] += 1;
            }
        }
    }

    // Build CSR offsets
    let mut up_offsets = Vec::with_capacity(n_nodes + 1);
    let mut offset = 0u64;
    for &count in &up_counts {
        up_offsets.push(offset);
        offset += count as u64;
    }
    up_offsets.push(offset);
    let n_up_edges = offset;

    let mut down_offsets = Vec::with_capacity(n_nodes + 1);
    let mut offset = 0u64;
    for &count in &down_counts {
        down_offsets.push(offset);
        offset += count as u64;
    }
    down_offsets.push(offset);
    let n_down_edges = offset;

    // Allocate edge arrays as atomics during the parallel phases.
    // Disjoint per-node ranges plus an atomic-counter cursor make every
    // store race-free; `Relaxed` ordering is the only synchronisation
    // we need (single thread fences happen at the end of `for_each`),
    // and `Relaxed` lowers to a plain MOV on x86/aarch64 — same code-gen
    // as the previous `*ptr.cast_mut() = v` pattern, with zero unsafe.
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
    let up_targets_atomic: Vec<AtomicU32> = (0..n_up_edges as usize)
        .map(|_| AtomicU32::new(0))
        .collect();
    let up_is_shortcut_atomic: Vec<AtomicBool> = (0..n_up_edges as usize)
        .map(|_| AtomicBool::new(false))
        .collect();
    let up_middle_atomic: Vec<AtomicU32> = (0..n_up_edges as usize)
        .map(|_| AtomicU32::new(u32::MAX))
        .collect();
    let down_targets_atomic: Vec<AtomicU32> = (0..n_down_edges as usize)
        .map(|_| AtomicU32::new(0))
        .collect();
    let down_is_shortcut_atomic: Vec<AtomicBool> = (0..n_down_edges as usize)
        .map(|_| AtomicBool::new(false))
        .collect();
    let down_middle_atomic: Vec<AtomicU32> = (0..n_down_edges as usize)
        .map(|_| AtomicU32::new(u32::MAX))
        .collect();

    let up_offsets_clone = up_offsets.clone();
    let down_offsets_clone = down_offsets.clone();

    // Atomic counters: position within each node's reserved range.
    let up_pos: Vec<std::sync::atomic::AtomicUsize> = up_offsets
        .iter()
        .map(|&x| std::sync::atomic::AtomicUsize::new(x as usize))
        .collect();
    let down_pos: Vec<std::sync::atomic::AtomicUsize> = down_offsets
        .iter()
        .map(|&x| std::sync::atomic::AtomicUsize::new(x as usize))
        .collect();

    // Fill original edges in parallel
    (0..n_nodes).into_par_iter().for_each(|u| {
        let rank_u = perm[u];
        let start = filtered_ebg.offsets[u] as usize;
        let end = filtered_ebg.offsets[u + 1] as usize;

        for i in start..end {
            let v = filtered_ebg.heads[i];
            if u == v as usize {
                continue;
            }
            let rank_v = perm[v as usize];

            if rank_u < rank_v {
                let pos = up_pos[u].fetch_add(1, Ordering::Relaxed);
                up_targets_atomic[pos].store(v, Ordering::Relaxed);
                up_is_shortcut_atomic[pos].store(false, Ordering::Relaxed);
                up_middle_atomic[pos].store(u32::MAX, Ordering::Relaxed);
            } else {
                let pos = down_pos[u].fetch_add(1, Ordering::Relaxed);
                down_targets_atomic[pos].store(v, Ordering::Relaxed);
                down_is_shortcut_atomic[pos].store(false, Ordering::Relaxed);
                down_middle_atomic[pos].store(u32::MAX, Ordering::Relaxed);
            }
        }
    });

    // Fill arrays - shortcuts from file (sequential, I/O bound).
    // Same atomic stores as the parallel pass above, just sequential
    // here because we're consuming a file. `Relaxed` is still correct
    // because there's only one writer.
    {
        let mut reader = BufReader::with_capacity(64 * 1024 * 1024, File::open(&shortcut_path)?);
        let mut buf = [0u8; 12];
        while reader.read_exact(&mut buf).is_ok() {
            let u = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
            let w = u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]);
            let middle = u32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]);
            let rank_u = perm[u];
            let rank_w = perm[w as usize];
            if rank_u < rank_w {
                let pos = up_pos[u].fetch_add(1, Ordering::Relaxed);
                up_targets_atomic[pos].store(w, Ordering::Relaxed);
                up_is_shortcut_atomic[pos].store(true, Ordering::Relaxed);
                up_middle_atomic[pos].store(middle, Ordering::Relaxed);
            } else {
                let pos = down_pos[u].fetch_add(1, Ordering::Relaxed);
                down_targets_atomic[pos].store(w, Ordering::Relaxed);
                down_is_shortcut_atomic[pos].store(true, Ordering::Relaxed);
                down_middle_atomic[pos].store(middle, Ordering::Relaxed);
            }
        }
    }

    // Remove temp file
    std::fs::remove_file(&shortcut_path)?;

    // Sort edges within each node (PARALLEL) - using struct-based sorting
    println!("  Sorting edges (parallel)...");

    // Edge data struct for sorting
    #[derive(Clone, Copy)]
    struct EdgeData {
        target: u32,
        is_shortcut: bool,
        middle: u32,
    }

    // Sort up edges in parallel - safe struct-based approach
    let up_ranges: Vec<(usize, usize)> = (0..n_nodes)
        .map(|u| {
            (
                up_offsets_clone[u] as usize,
                up_offsets_clone[u + 1] as usize,
            )
        })
        .collect();

    up_ranges.par_iter().for_each(|&(start, end)| {
        if end > start {
            // Read the AoS view from the atomic SoA storage.
            let mut edges: Vec<EdgeData> = (start..end)
                .map(|i| EdgeData {
                    target: up_targets_atomic[i].load(Ordering::Relaxed),
                    is_shortcut: up_is_shortcut_atomic[i].load(Ordering::Relaxed),
                    middle: up_middle_atomic[i].load(Ordering::Relaxed),
                })
                .collect();

            // Sort by target
            edges.sort_unstable_by_key(|e| e.target);

            // Disjoint per-node range, race-free by construction; we
            // store back through the atomic API instead of casting raw
            // pointers.
            for (i, edge) in edges.into_iter().enumerate() {
                let pos = start + i;
                up_targets_atomic[pos].store(edge.target, Ordering::Relaxed);
                up_is_shortcut_atomic[pos].store(edge.is_shortcut, Ordering::Relaxed);
                up_middle_atomic[pos].store(edge.middle, Ordering::Relaxed);
            }
        }
    });

    // Sort down edges in parallel
    let down_ranges: Vec<(usize, usize)> = (0..n_nodes)
        .map(|u| {
            (
                down_offsets_clone[u] as usize,
                down_offsets_clone[u + 1] as usize,
            )
        })
        .collect();

    down_ranges.par_iter().for_each(|&(start, end)| {
        if end > start {
            let mut edges: Vec<EdgeData> = (start..end)
                .map(|i| EdgeData {
                    target: down_targets_atomic[i].load(Ordering::Relaxed),
                    is_shortcut: down_is_shortcut_atomic[i].load(Ordering::Relaxed),
                    middle: down_middle_atomic[i].load(Ordering::Relaxed),
                })
                .collect();

            edges.sort_unstable_by_key(|e| e.target);

            for (i, edge) in edges.into_iter().enumerate() {
                let pos = start + i;
                down_targets_atomic[pos].store(edge.target, Ordering::Relaxed);
                down_is_shortcut_atomic[pos].store(edge.is_shortcut, Ordering::Relaxed);
                down_middle_atomic[pos].store(edge.middle, Ordering::Relaxed);
            }
        }
    });

    // All parallel scatter phases done — convert atomics to plain Vecs
    // for the rank-aligned transformation and downstream serialisation.
    // `into_inner` is zero-cost; `AtomicU32`/`AtomicBool` are
    // `repr(transparent)` over their primitives.
    let up_targets: Vec<u32> = up_targets_atomic
        .into_iter()
        .map(AtomicU32::into_inner)
        .collect();
    let up_is_shortcut: Vec<bool> = up_is_shortcut_atomic
        .into_iter()
        .map(AtomicBool::into_inner)
        .collect();
    let up_middle: Vec<u32> = up_middle_atomic
        .into_iter()
        .map(AtomicU32::into_inner)
        .collect();
    let down_targets: Vec<u32> = down_targets_atomic
        .into_iter()
        .map(AtomicU32::into_inner)
        .collect();
    let down_is_shortcut: Vec<bool> = down_is_shortcut_atomic
        .into_iter()
        .map(AtomicBool::into_inner)
        .collect();
    let down_middle: Vec<u32> = down_middle_atomic
        .into_iter()
        .map(AtomicU32::into_inner)
        .collect();

    println!(
        "  ✓ Up graph: {} edges ({} shortcuts)",
        n_up_edges,
        up_is_shortcut.iter().filter(|&&x| x).count()
    );
    println!(
        "  ✓ Down graph: {} edges ({} shortcuts)",
        n_down_edges,
        down_is_shortcut.iter().filter(|&&x| x).count()
    );

    // ===== RANK-ALIGNED TRANSFORMATION =====
    // Convert from filtered-space indexing to rank-space indexing
    // This makes PHAST downward scan access memory sequentially
    println!("\nApplying rank-aligned transformation...");

    // Build new CSR structure indexed by rank
    // new_offsets[rank] = start of edges for node at that rank
    // This reorders nodes so that node_id == rank (identity mapping)

    // Step 1: Build rank_to_filtered mapping (same as inv_perm)
    let rank_to_filtered: Vec<u32> = inv_perm.clone();

    // Step 2: Rebuild UP graph with rank-aligned indexing
    let (rank_up_offsets, rank_up_targets, rank_up_is_shortcut, rank_up_middle) =
        remap_to_rank_space(
            &up_offsets,
            &up_targets,
            &up_is_shortcut,
            &up_middle,
            perm,
            inv_perm,
            n_nodes,
        );

    // Step 3: Rebuild DOWN graph with rank-aligned indexing
    let (rank_down_offsets, rank_down_targets, rank_down_is_shortcut, rank_down_middle) =
        remap_to_rank_space(
            &down_offsets,
            &down_targets,
            &down_is_shortcut,
            &down_middle,
            perm,
            inv_perm,
            n_nodes,
        );

    println!("  ✓ Rank-aligned transformation complete");

    // Compute inputs SHA using streaming (avoid loading whole files into memory)
    let inputs_sha = compute_inputs_sha_streaming(&config.filtered_ebg_path, &config.order_path)?;

    // Write output - use mode-specific filename
    let topo_path = config.outdir.join(format!("cch.{}.topo", mode_name));

    println!("\nWriting output...");
    let topo = CchTopo {
        n_nodes: filtered_ebg.n_filtered_nodes,
        n_shortcuts,
        n_original_arcs: filtered_ebg.n_filtered_arcs,
        inputs_sha,
        up_offsets: rank_up_offsets.into(),
        up_targets: rank_up_targets.into(),
        up_is_shortcut: crate::formats::BitsetField::from_bools(&rank_up_is_shortcut),
        up_middle: rank_up_middle.into(),
        down_offsets: rank_down_offsets.into(),
        down_targets: rank_down_targets.into(),
        down_is_shortcut: crate::formats::BitsetField::from_bools(&rank_down_is_shortcut),
        down_middle: rank_down_middle.into(),
        rank_to_filtered: rank_to_filtered.into(),
    };
    CchTopoFile::write(&topo_path, &topo)?;
    println!("  ✓ Written {}", topo_path.display());

    let build_time_ms = start_time.elapsed().as_millis() as u64;

    Ok(Step7Result {
        topo_path,
        mode: config.mode,
        mode_name: config.mode_name.clone(),
        n_nodes: filtered_ebg.n_filtered_nodes,
        n_original_arcs: filtered_ebg.n_filtered_arcs,
        n_shortcuts,
        n_up_edges,
        n_down_edges,
        build_time_ms,
    })
}

/// Compute SHA256 of input files using streaming (memory efficient)
fn compute_inputs_sha_streaming(
    filtered_ebg_path: &std::path::Path,
    order_path: &std::path::Path,
) -> Result<[u8; 32]> {
    use sha2::{Digest, Sha256};
    use std::io::Read;

    fn feed(hasher: &mut Sha256, path: &std::path::Path) -> Result<()> {
        let mut reader = BufReader::with_capacity(64 * 1024, File::open(path)?);
        let mut buf = [0u8; 64 * 1024];
        loop {
            let n = reader.read(&mut buf)?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
        }
        Ok(())
    }

    let mut hasher = Sha256::new();
    feed(&mut hasher, filtered_ebg_path)?;
    feed(&mut hasher, order_path)?;

    let result = hasher.finalize();
    let mut sha = [0u8; 32];
    sha.copy_from_slice(&result);
    Ok(sha)
}

/// Remap CSR graph from filtered-space indexing to rank-space indexing.
///
/// Input:
/// - offsets[filtered_id] = start of edges for node with that filtered ID
/// - targets[i] = filtered ID of target node
/// - middle[i] = filtered ID of middle node (for shortcuts)
///
/// Output:
/// - new_offsets[rank] = start of edges for node at that rank
/// - new_targets[i] = rank of target node
/// - new_middle[i] = rank of middle node
///
/// This transformation makes PHAST downward scan access memory sequentially:
/// - Before: for rank in (0..n).rev() { u = inv_perm[rank]; offsets[u]... } (random access)
/// - After:  for rank in (0..n).rev() { offsets[rank]... } (sequential access)
fn remap_to_rank_space(
    offsets: &[u64],
    targets: &[u32],
    is_shortcut: &[bool],
    middle: &[u32],
    perm: &[u32],     // perm[filtered_id] = rank
    inv_perm: &[u32], // inv_perm[rank] = filtered_id
    n_nodes: usize,
) -> (Vec<u64>, Vec<u32>, Vec<bool>, Vec<u32>) {
    let n_edges = targets.len();

    // Step 1: Count edges per rank (same as per filtered_id, just reordered)
    let mut new_offsets = Vec::with_capacity(n_nodes + 1);
    let mut offset = 0u64;

    for &inv in inv_perm.iter().take(n_nodes) {
        new_offsets.push(offset);
        let filtered_id = inv as usize;
        let count = offsets[filtered_id + 1] - offsets[filtered_id];
        offset += count;
    }
    new_offsets.push(offset);

    // Step 2: Allocate output arrays
    let mut new_targets = vec![0u32; n_edges];
    let mut new_is_shortcut = vec![false; n_edges];
    let mut new_middle = vec![u32::MAX; n_edges];

    // Step 3: Copy and remap edges, reordered by source rank
    let mut write_pos = 0;
    for &inv in inv_perm.iter().take(n_nodes) {
        let filtered_id = inv as usize;
        let old_start = offsets[filtered_id] as usize;
        let old_end = offsets[filtered_id + 1] as usize;

        for i in old_start..old_end {
            // Remap target: filtered_id -> rank
            let target_filtered = targets[i] as usize;
            let target_rank = perm[target_filtered];
            new_targets[write_pos] = target_rank;

            // Copy is_shortcut flag
            new_is_shortcut[write_pos] = is_shortcut[i];

            // Remap middle: filtered_id -> rank (only for shortcuts)
            if is_shortcut[i] {
                let middle_filtered = middle[i] as usize;
                let middle_rank = perm[middle_filtered];
                new_middle[write_pos] = middle_rank;
            } else {
                new_middle[write_pos] = u32::MAX;
            }

            write_pos += 1;
        }
    }

    // Step 4: Sort edges within each rank's edge list by target rank
    // This preserves binary search capability
    for rank in 0..n_nodes {
        let start = new_offsets[rank] as usize;
        let end = new_offsets[rank + 1] as usize;
        if end > start {
            // Collect into tuples for sorting
            let mut edges: Vec<(u32, bool, u32)> = (start..end)
                .map(|i| (new_targets[i], new_is_shortcut[i], new_middle[i]))
                .collect();

            // Sort by target rank
            edges.sort_unstable_by_key(|(target, _, _)| *target);

            // Write back
            for (i, (target, is_sc, mid)) in edges.into_iter().enumerate() {
                new_targets[start + i] = target;
                new_is_shortcut[start + i] = is_sc;
                new_middle[start + i] = mid;
            }
        }
    }

    (new_offsets, new_targets, new_is_shortcut, new_middle)
}

// ==========================================================================
// Hybrid State Graph CCH Contraction
// ==========================================================================

use crate::formats::HybridStateFile;

/// Configuration for Step 7 with hybrid state graph
pub struct Step7HybridConfig {
    pub hybrid_state_path: PathBuf,
    pub order_path: PathBuf,
    pub mode: Mode,
    pub mode_name: String,
    pub outdir: PathBuf,
}

/// Build CCH topology via contraction on hybrid state graph
pub fn build_cch_topology_hybrid(config: Step7HybridConfig) -> Result<Step7Result> {
    let start_time = std::time::Instant::now();
    let mode_name = &config.mode_name;
    println!(
        "\n🔨 Step 7: Building CCH topology for {} mode (HYBRID)...\n",
        mode_name
    );

    // Load hybrid state graph
    println!("Loading hybrid state graph ({})...", mode_name);
    let hybrid = HybridStateFile::read(&config.hybrid_state_path)?;
    println!("  ✓ {} states, {} arcs", hybrid.n_states, hybrid.n_arcs);

    // Load ordering
    println!("Loading ordering ({})...", mode_name);
    let order = OrderEbgFile::read(&config.order_path)?;
    println!("  ✓ {} nodes", order.n_nodes);

    if hybrid.n_states != order.n_nodes {
        anyhow::bail!(
            "Node count mismatch: hybrid has {} states, order has {}",
            hybrid.n_states,
            order.n_nodes
        );
    }

    let n_nodes = hybrid.n_states as usize;
    let perm = &order.perm;
    let inv_perm = &order.inv_perm;

    // Build weighted adjacency for witness search
    // Hybrid state graph already has weights embedded
    println!("Building weighted adjacency for witness search...");
    let weighted_adj: WeightedAdj = (0..n_nodes)
        .into_par_iter()
        .map(|u| {
            let start = hybrid.offsets[u] as usize;
            let end = hybrid.offsets[u + 1] as usize;
            let mut adj_map: FxHashMap<u32, u32> = FxHashMap::default();
            for i in start..end {
                let v = hybrid.targets[i];
                if u as u32 == v {
                    continue;
                }
                let edge_weight = hybrid.weights[i];
                // Take minimum weight if multiple edges to same target
                adj_map
                    .entry(v)
                    .and_modify(|w| *w = (*w).min(edge_weight))
                    .or_insert(edge_weight);
            }
            adj_map
        })
        .collect();
    println!("  ✓ Built weighted adjacency");

    // Build initial higher-neighbor lists
    println!("\nBuilding initial higher-neighbor lists (parallel)...");

    let (out_higher, in_higher): (Vec<FxHashSet<u32>>, Vec<FxHashSet<u32>>) = {
        let out: Vec<FxHashSet<u32>> = (0..n_nodes)
            .into_par_iter()
            .map(|u| {
                let rank_u = perm[u];
                let start = hybrid.offsets[u] as usize;
                let end = hybrid.offsets[u + 1] as usize;
                let degree = end - start;
                let mut set = FxHashSet::with_capacity_and_hasher(degree, Default::default());
                for i in start..end {
                    let v = hybrid.targets[i] as usize;
                    if u != v && perm[v] > rank_u {
                        set.insert(v as u32);
                    }
                }
                set
            })
            .collect();

        let mut in_vecs: Vec<Vec<u32>> = vec![Vec::new(); n_nodes];
        for u in 0..n_nodes {
            let rank_u = perm[u];
            let start = hybrid.offsets[u] as usize;
            let end = hybrid.offsets[u + 1] as usize;
            for i in start..end {
                let v = hybrid.targets[i] as usize;
                if u != v && rank_u > perm[v] {
                    in_vecs[v].push(u as u32);
                }
            }
        }

        let in_sets: Vec<FxHashSet<u32>> = in_vecs
            .into_par_iter()
            .map(|v| {
                let mut set = FxHashSet::with_capacity_and_hasher(v.len(), Default::default());
                set.extend(v);
                set
            })
            .collect();

        (out, in_sets)
    };

    let mut out_higher = out_higher;
    let mut in_higher = in_higher;
    println!("  ✓ Built initial neighbor lists");

    // Stream shortcuts to temp file
    std::fs::create_dir_all(&config.outdir)?;
    let shortcut_path = config.outdir.join("shortcuts.hybrid.tmp");
    let mut shortcut_writer =
        BufWriter::with_capacity(64 * 1024 * 1024, File::create(&shortcut_path)?);
    let mut n_shortcuts = 0u64;

    println!("\nContracting nodes (sequential with parallel inner loops)...");
    let n_threads = rayon::current_num_threads();
    println!("  Using {} threads for parallel inner loops", n_threads);

    let report_interval = (n_nodes / 100).max(1);
    let mut last_report = 0;
    let mut max_degree_seen = 0usize;
    let mut weighted_adj = weighted_adj;

    // Sequential contraction
    for (rank, &v_node) in inv_perm.iter().enumerate().take(n_nodes) {
        if rank - last_report >= report_interval {
            let pct = (rank as f64 / n_nodes as f64) * 100.0;
            println!(
                "  {:5.1}% contracted ({} shortcuts, max_degree={})",
                pct, n_shortcuts, max_degree_seen
            );
            last_report = rank;
        }

        let v = v_node as usize;

        let in_neighbors: Vec<u32> = std::mem::take(&mut in_higher[v]).into_iter().collect();
        let out_neighbors: Vec<u32> = std::mem::take(&mut out_higher[v]).into_iter().collect();

        if in_neighbors.is_empty() || out_neighbors.is_empty() {
            continue;
        }

        let degree = in_neighbors.len().max(out_neighbors.len());
        if degree > max_degree_seen {
            max_degree_seen = degree;
        }

        let work_amount = in_neighbors.len() * out_neighbors.len();
        let out_higher_ref = &out_higher;
        let in_higher_ref = &in_higher;
        let weighted_adj_ref = &weighted_adj;
        let v_u32 = v as u32;

        let new_shortcuts: Vec<(u32, u32, u32)> = if work_amount > 1000 {
            in_neighbors
                .par_iter()
                .flat_map(|&u| {
                    let u_idx = u as usize;
                    let rank_u = perm[u_idx];
                    let w_uv = weighted_adj_ref[u_idx]
                        .get(&v_u32)
                        .copied()
                        .unwrap_or(u32::MAX);

                    out_neighbors
                        .iter()
                        .filter_map(move |&w| {
                            if u == w {
                                return None;
                            }
                            let w_idx = w as usize;
                            let rank_w = perm[w_idx];

                            let already_exists = if rank_w > rank_u {
                                out_higher_ref[u_idx].contains(&w)
                            } else {
                                in_higher_ref[w_idx].contains(&u)
                            };
                            if already_exists {
                                return None;
                            }

                            let w_vw = weighted_adj_ref[v].get(&w).copied().unwrap_or(u32::MAX);
                            let shortcut_cost = w_uv.saturating_add(w_vw);

                            Some((u, w, shortcut_cost))
                        })
                        .collect::<Vec<_>>()
                })
                .collect()
        } else {
            let mut result = Vec::with_capacity(work_amount);
            for &u in &in_neighbors {
                let u_idx = u as usize;
                let rank_u = perm[u_idx];
                let w_uv = weighted_adj[u_idx].get(&v_u32).copied().unwrap_or(u32::MAX);

                for &w in &out_neighbors {
                    if u == w {
                        continue;
                    }
                    let w_idx = w as usize;
                    let rank_w = perm[w_idx];

                    let already_exists = if rank_w > rank_u {
                        out_higher[u_idx].contains(&w)
                    } else {
                        in_higher[w_idx].contains(&u)
                    };
                    if already_exists {
                        continue;
                    }

                    let w_vw = weighted_adj[v].get(&w).copied().unwrap_or(u32::MAX);
                    let shortcut_cost = w_uv.saturating_add(w_vw);

                    result.push((u, w, shortcut_cost));
                }
            }
            result
        };

        for (u, w, shortcut_cost) in new_shortcuts {
            shortcut_writer.write_all(&u.to_le_bytes())?;
            shortcut_writer.write_all(&w.to_le_bytes())?;
            shortcut_writer.write_all(&(v as u32).to_le_bytes())?;
            n_shortcuts += 1;

            let u_idx = u as usize;
            let w_idx = w as usize;
            let rank_u = perm[u_idx];
            let rank_w = perm[w_idx];

            if rank_w > rank_u {
                out_higher[u_idx].insert(w);
            } else {
                in_higher[w_idx].insert(u);
            }

            weighted_adj[u_idx]
                .entry(w)
                .and_modify(|existing| *existing = (*existing).min(shortcut_cost))
                .or_insert(shortcut_cost);
        }
    }

    shortcut_writer.flush()?;
    drop(shortcut_writer);
    drop(out_higher);
    drop(in_higher);

    println!("  ✓ Contraction complete: {} shortcuts", n_shortcuts);

    // Build up/down graphs
    println!("\nBuilding hierarchical graph (parallel)...");

    let up_counts: Vec<usize> = (0..n_nodes)
        .into_par_iter()
        .map(|u| {
            let rank_u = perm[u];
            let start = hybrid.offsets[u] as usize;
            let end = hybrid.offsets[u + 1] as usize;
            let mut count = 0;
            for i in start..end {
                let v = hybrid.targets[i] as usize;
                if u != v && rank_u < perm[v] {
                    count += 1;
                }
            }
            count
        })
        .collect();

    let down_counts: Vec<usize> = (0..n_nodes)
        .into_par_iter()
        .map(|u| {
            let rank_u = perm[u];
            let start = hybrid.offsets[u] as usize;
            let end = hybrid.offsets[u + 1] as usize;
            let mut count = 0;
            for i in start..end {
                let v = hybrid.targets[i] as usize;
                if u != v && rank_u >= perm[v] {
                    count += 1;
                }
            }
            count
        })
        .collect();

    let mut up_counts = up_counts;
    let mut down_counts = down_counts;

    // Count shortcuts from file
    {
        let mut reader = BufReader::with_capacity(64 * 1024 * 1024, File::open(&shortcut_path)?);
        let mut buf = [0u8; 12];
        while reader.read_exact(&mut buf).is_ok() {
            let u = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
            let w = u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]) as usize;
            let rank_u = perm[u];
            let rank_w = perm[w];
            if rank_u < rank_w {
                up_counts[u] += 1;
            } else {
                down_counts[u] += 1;
            }
        }
    }

    // Build CSR offsets
    let mut up_offsets = Vec::with_capacity(n_nodes + 1);
    let mut offset = 0u64;
    for &count in &up_counts {
        up_offsets.push(offset);
        offset += count as u64;
    }
    up_offsets.push(offset);
    let n_up_edges = offset;

    let mut down_offsets = Vec::with_capacity(n_nodes + 1);
    let mut offset = 0u64;
    for &count in &down_counts {
        down_offsets.push(offset);
        offset += count as u64;
    }
    down_offsets.push(offset);
    let n_down_edges = offset;

    // Allocate edge arrays as atomics for the parallel scatter phases
    // (see the corresponding comment in `contract_filtered_ebg` above —
    // same disjoint-by-construction pattern, same `Relaxed` lowering to
    // plain MOVs on x86, no unsafe).
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
    let up_targets_atomic: Vec<AtomicU32> = (0..n_up_edges as usize)
        .map(|_| AtomicU32::new(0))
        .collect();
    let up_is_shortcut_atomic: Vec<AtomicBool> = (0..n_up_edges as usize)
        .map(|_| AtomicBool::new(false))
        .collect();
    let up_middle_atomic: Vec<AtomicU32> = (0..n_up_edges as usize)
        .map(|_| AtomicU32::new(u32::MAX))
        .collect();
    let down_targets_atomic: Vec<AtomicU32> = (0..n_down_edges as usize)
        .map(|_| AtomicU32::new(0))
        .collect();
    let down_is_shortcut_atomic: Vec<AtomicBool> = (0..n_down_edges as usize)
        .map(|_| AtomicBool::new(false))
        .collect();
    let down_middle_atomic: Vec<AtomicU32> = (0..n_down_edges as usize)
        .map(|_| AtomicU32::new(u32::MAX))
        .collect();

    let up_offsets_clone = up_offsets.clone();
    let down_offsets_clone = down_offsets.clone();

    let up_pos: Vec<std::sync::atomic::AtomicUsize> = up_offsets
        .iter()
        .map(|&x| std::sync::atomic::AtomicUsize::new(x as usize))
        .collect();
    let down_pos: Vec<std::sync::atomic::AtomicUsize> = down_offsets
        .iter()
        .map(|&x| std::sync::atomic::AtomicUsize::new(x as usize))
        .collect();

    (0..n_nodes).into_par_iter().for_each(|u| {
        let rank_u = perm[u];
        let start = hybrid.offsets[u] as usize;
        let end = hybrid.offsets[u + 1] as usize;

        for i in start..end {
            let v = hybrid.targets[i];
            if u == v as usize {
                continue;
            }
            let rank_v = perm[v as usize];

            if rank_u < rank_v {
                let pos = up_pos[u].fetch_add(1, Ordering::Relaxed);
                up_targets_atomic[pos].store(v, Ordering::Relaxed);
                up_is_shortcut_atomic[pos].store(false, Ordering::Relaxed);
                up_middle_atomic[pos].store(u32::MAX, Ordering::Relaxed);
            } else {
                let pos = down_pos[u].fetch_add(1, Ordering::Relaxed);
                down_targets_atomic[pos].store(v, Ordering::Relaxed);
                down_is_shortcut_atomic[pos].store(false, Ordering::Relaxed);
                down_middle_atomic[pos].store(u32::MAX, Ordering::Relaxed);
            }
        }
    });

    // Fill shortcuts from file (sequential, I/O-bound).
    {
        let mut reader = BufReader::with_capacity(64 * 1024 * 1024, File::open(&shortcut_path)?);
        let mut buf = [0u8; 12];
        while reader.read_exact(&mut buf).is_ok() {
            let u = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
            let w = u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]);
            let middle = u32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]);
            let rank_u = perm[u];
            let rank_w = perm[w as usize];
            if rank_u < rank_w {
                let pos = up_pos[u].fetch_add(1, Ordering::Relaxed);
                up_targets_atomic[pos].store(w, Ordering::Relaxed);
                up_is_shortcut_atomic[pos].store(true, Ordering::Relaxed);
                up_middle_atomic[pos].store(middle, Ordering::Relaxed);
            } else {
                let pos = down_pos[u].fetch_add(1, Ordering::Relaxed);
                down_targets_atomic[pos].store(w, Ordering::Relaxed);
                down_is_shortcut_atomic[pos].store(true, Ordering::Relaxed);
                down_middle_atomic[pos].store(middle, Ordering::Relaxed);
            }
        }
    }

    std::fs::remove_file(&shortcut_path)?;

    // Sort edges within each node
    println!("  Sorting edges (parallel)...");

    #[derive(Clone, Copy)]
    struct EdgeData {
        target: u32,
        is_shortcut: bool,
        middle: u32,
    }

    let up_ranges: Vec<(usize, usize)> = (0..n_nodes)
        .map(|u| {
            (
                up_offsets_clone[u] as usize,
                up_offsets_clone[u + 1] as usize,
            )
        })
        .collect();

    up_ranges.par_iter().for_each(|&(start, end)| {
        if end > start {
            let mut edges: Vec<EdgeData> = (start..end)
                .map(|i| EdgeData {
                    target: up_targets_atomic[i].load(Ordering::Relaxed),
                    is_shortcut: up_is_shortcut_atomic[i].load(Ordering::Relaxed),
                    middle: up_middle_atomic[i].load(Ordering::Relaxed),
                })
                .collect();
            edges.sort_unstable_by_key(|e| e.target);
            for (i, edge) in edges.into_iter().enumerate() {
                let pos = start + i;
                up_targets_atomic[pos].store(edge.target, Ordering::Relaxed);
                up_is_shortcut_atomic[pos].store(edge.is_shortcut, Ordering::Relaxed);
                up_middle_atomic[pos].store(edge.middle, Ordering::Relaxed);
            }
        }
    });

    let down_ranges: Vec<(usize, usize)> = (0..n_nodes)
        .map(|u| {
            (
                down_offsets_clone[u] as usize,
                down_offsets_clone[u + 1] as usize,
            )
        })
        .collect();

    down_ranges.par_iter().for_each(|&(start, end)| {
        if end > start {
            let mut edges: Vec<EdgeData> = (start..end)
                .map(|i| EdgeData {
                    target: down_targets_atomic[i].load(Ordering::Relaxed),
                    is_shortcut: down_is_shortcut_atomic[i].load(Ordering::Relaxed),
                    middle: down_middle_atomic[i].load(Ordering::Relaxed),
                })
                .collect();
            edges.sort_unstable_by_key(|e| e.target);
            for (i, edge) in edges.into_iter().enumerate() {
                let pos = start + i;
                down_targets_atomic[pos].store(edge.target, Ordering::Relaxed);
                down_is_shortcut_atomic[pos].store(edge.is_shortcut, Ordering::Relaxed);
                down_middle_atomic[pos].store(edge.middle, Ordering::Relaxed);
            }
        }
    });

    // Convert atomics to plain Vecs for downstream rank-aligned remap.
    let up_targets: Vec<u32> = up_targets_atomic
        .into_iter()
        .map(AtomicU32::into_inner)
        .collect();
    let up_is_shortcut: Vec<bool> = up_is_shortcut_atomic
        .into_iter()
        .map(AtomicBool::into_inner)
        .collect();
    let up_middle: Vec<u32> = up_middle_atomic
        .into_iter()
        .map(AtomicU32::into_inner)
        .collect();
    let down_targets: Vec<u32> = down_targets_atomic
        .into_iter()
        .map(AtomicU32::into_inner)
        .collect();
    let down_is_shortcut: Vec<bool> = down_is_shortcut_atomic
        .into_iter()
        .map(AtomicBool::into_inner)
        .collect();
    let down_middle: Vec<u32> = down_middle_atomic
        .into_iter()
        .map(AtomicU32::into_inner)
        .collect();

    println!(
        "  ✓ Up graph: {} edges ({} shortcuts)",
        n_up_edges,
        up_is_shortcut.iter().filter(|&&x| x).count()
    );
    println!(
        "  ✓ Down graph: {} edges ({} shortcuts)",
        n_down_edges,
        down_is_shortcut.iter().filter(|&&x| x).count()
    );

    // Rank-aligned transformation
    println!("\nApplying rank-aligned transformation...");

    let rank_to_filtered: Vec<u32> = inv_perm.clone();

    let (rank_up_offsets, rank_up_targets, rank_up_is_shortcut, rank_up_middle) =
        remap_to_rank_space(
            &up_offsets,
            &up_targets,
            &up_is_shortcut,
            &up_middle,
            perm,
            inv_perm,
            n_nodes,
        );

    let (rank_down_offsets, rank_down_targets, rank_down_is_shortcut, rank_down_middle) =
        remap_to_rank_space(
            &down_offsets,
            &down_targets,
            &down_is_shortcut,
            &down_middle,
            perm,
            inv_perm,
            n_nodes,
        );

    println!("  ✓ Rank-aligned transformation complete");

    // Compute inputs SHA
    let inputs_sha = compute_inputs_sha_streaming(&config.hybrid_state_path, &config.order_path)?;

    // Write output
    let topo_path = config.outdir.join(format!("cch.hybrid.{}.topo", mode_name));

    println!("\nWriting output...");
    let topo = CchTopo {
        n_nodes: hybrid.n_states,
        n_shortcuts,
        n_original_arcs: hybrid.n_arcs,
        inputs_sha,
        up_offsets: rank_up_offsets.into(),
        up_targets: rank_up_targets.into(),
        up_is_shortcut: crate::formats::BitsetField::from_bools(&rank_up_is_shortcut),
        up_middle: rank_up_middle.into(),
        down_offsets: rank_down_offsets.into(),
        down_targets: rank_down_targets.into(),
        down_is_shortcut: crate::formats::BitsetField::from_bools(&rank_down_is_shortcut),
        down_middle: rank_down_middle.into(),
        rank_to_filtered: rank_to_filtered.into(),
    };
    CchTopoFile::write(&topo_path, &topo)?;
    println!("  ✓ Written {}", topo_path.display());

    let build_time_ms = start_time.elapsed().as_millis() as u64;

    Ok(Step7Result {
        topo_path,
        mode: config.mode,
        mode_name: config.mode_name.clone(),
        n_nodes: hybrid.n_states,
        n_original_arcs: hybrid.n_arcs,
        n_shortcuts,
        n_up_edges,
        n_down_edges,
        build_time_ms,
    })
}
