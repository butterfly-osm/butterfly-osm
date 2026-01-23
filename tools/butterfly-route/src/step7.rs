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
//! 2. No witness path u → ... → w exists through other higher-ranked nodes
//!
//! # Per-Mode Filtered CCH
//!
//! The CCH is now built on the filtered EBG (mode-accessible subgraph only). This ensures
//! that all nodes in the CCH are actually reachable in that mode, eliminating orphaned
//! nodes that caused routing failures in the previous design.
//!
//! # Key Optimization: Witness Search
//!
//! The witness search is critical for reducing shortcut count. Without it, Belgium
//! generates 2.45B shortcuts (167x ratio). With depth-3 witness search, this drops
//! to 45.7M shortcuts (3.12x ratio) - a 54x reduction.
//!
//! The witness search checks:
//! - Depth-2 forward: u → x → w (where x ≠ v)
//! - Depth-2 backward: u → x → w via in-neighbors of w
//! - Depth-3 forward: u → x → y → w
//! - Depth-3 backward: u → x → y → w via in-neighbors
//!
//! # Memory Management
//!
//! - Shortcuts are streamed to a temp file during contraction to avoid memory explosion
//! - Adjacency lists use FxHashSet (fast hash) for O(1) lookups during witness search
//! - Final up/down graphs are built by streaming through the temp file twice
//!
//! # Parallelism Strategy
//!
//! - Node contraction is sequential (required for correctness - each node must see
//!   shortcuts from previously contracted nodes)
//! - Inner shortcut computation is parallel for high-degree nodes (work > 1000 pairs)
//! - Initial adjacency building, edge counting/filling, and sorting are fully parallel

use anyhow::Result;
use rayon::prelude::*;
use rustc_hash::FxHashSet;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::PathBuf;

use crate::formats::{CchTopo, CchTopoFile, FilteredEbg, FilteredEbgFile, OrderEbgFile};
use crate::profile_abi::Mode;

/// Check if there's a witness path u → ... → w (not through v).
///
/// DISABLED: Witness search was causing missing shortcuts needed for CCH bidirectional
/// queries to find optimal paths. For correctness, we now create ALL shortcuts.
/// This increases shortcut count but ensures the CCH property holds.
///
/// The triangle relaxation in Step 8 will assign appropriate weights to shortcuts,
/// including u32::MAX for redundant ones.
///
/// # Returns
///
/// Always `false` (no witness, always add shortcut) - witness search disabled.
#[inline]
fn has_witness(
    _u: usize,
    _w: u32,
    _v: u32,
    _out_higher: &[FxHashSet<u32>],
    _in_higher: &[FxHashSet<u32>],
) -> bool {
    // Witness search disabled for correctness
    false
}

/// Configuration for Step 7
pub struct Step7Config {
    pub filtered_ebg_path: PathBuf,
    pub order_path: PathBuf,
    pub mode: Mode,
    pub outdir: PathBuf,
}

/// Result of Step 7 contraction
#[derive(Debug)]
pub struct Step7Result {
    pub topo_path: PathBuf,
    pub mode: Mode,
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
    let mode_name = match config.mode {
        Mode::Car => "car",
        Mode::Bike => "bike",
        Mode::Foot => "foot",
    };
    println!("\n🔨 Step 7: Building CCH topology for {} mode...\n", mode_name);

    // Load filtered EBG
    println!("Loading filtered EBG ({})...", mode_name);
    let filtered_ebg = FilteredEbgFile::read(&config.filtered_ebg_path)?;
    println!("  ✓ {} nodes, {} arcs", filtered_ebg.n_filtered_nodes, filtered_ebg.n_filtered_arcs);

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

    // Sequential contraction - MUST process one node at a time for correctness
    // But parallelize the inner shortcut computation for high-degree nodes
    for rank in 0..n_nodes {
        if rank - last_report >= report_interval {
            let pct = (rank as f64 / n_nodes as f64) * 100.0;
            println!(
                "  {:5.1}% contracted ({} shortcuts, max_degree={})",
                pct, n_shortcuts, max_degree_seen
            );
            last_report = rank;
        }

        let v = inv_perm[rank] as usize;

        let in_neighbors: Vec<u32> = std::mem::take(&mut in_higher[v]).into_iter().collect();
        let out_neighbors: Vec<u32> = std::mem::take(&mut out_higher[v]).into_iter().collect();

        if in_neighbors.is_empty() || out_neighbors.is_empty() {
            continue;
        }

        let degree = in_neighbors.len().max(out_neighbors.len());
        if degree > max_degree_seen {
            max_degree_seen = degree;
        }

        // Compute shortcuts with WITNESS SEARCH - parallel for large neighborhoods
        let work_amount = in_neighbors.len() * out_neighbors.len();
        let out_higher_ref = &out_higher;
        let in_higher_ref = &in_higher;
        let v_u32 = v as u32;

        let new_shortcuts: Vec<(u32, u32)> = if work_amount > 1000 {
            // Parallel computation for high-degree nodes
            in_neighbors
                .par_iter()
                .flat_map(|&u| {
                    let u_idx = u as usize;
                    let rank_u = perm[u_idx];
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

                            // Check 2: Witness path exists?
                            if has_witness(u_idx, w, v_u32, out_higher_ref, in_higher_ref) {
                                return None;
                            }

                            Some((u, w))
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

                    // Check 2: Witness path exists?
                    if has_witness(u_idx, w, v_u32, &out_higher, &in_higher) {
                        continue;
                    }

                    result.push((u, w));
                }
            }
            result
        };

        // Write shortcuts to disk and update adjacency IMMEDIATELY (correctness requirement)
        for (u, w) in new_shortcuts {
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
        }
    }

    shortcut_writer.flush()?;
    drop(shortcut_writer);

    // Free adjacency sets - no longer needed
    drop(out_higher);
    drop(in_higher);

    println!("  ✓ Contraction complete: {} shortcuts", n_shortcuts);

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
        let mut reader =
            BufReader::with_capacity(64 * 1024 * 1024, File::open(&shortcut_path)?);
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

    // Allocate edge arrays
    let mut up_targets = vec![0u32; n_up_edges as usize];
    let mut up_is_shortcut = vec![false; n_up_edges as usize];
    let mut up_middle = vec![u32::MAX; n_up_edges as usize];

    let mut down_targets = vec![0u32; n_down_edges as usize];
    let mut down_is_shortcut = vec![false; n_down_edges as usize];
    let mut down_middle = vec![u32::MAX; n_down_edges as usize];

    // Fill arrays - original edges (PARALLEL)
    // Each node writes to its own disjoint range, so this is safe
    let up_offsets_clone = up_offsets.clone();
    let down_offsets_clone = down_offsets.clone();

    // Use atomic counters for positions within each node's range
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
                let pos = up_pos[u].fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                // Safe: each thread writes to disjoint ranges
                unsafe {
                    *up_targets.as_ptr().add(pos).cast_mut() = v;
                    *up_is_shortcut.as_ptr().add(pos).cast_mut() = false;
                    *up_middle.as_ptr().add(pos).cast_mut() = u32::MAX;
                }
            } else {
                let pos = down_pos[u].fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                unsafe {
                    *down_targets.as_ptr().add(pos).cast_mut() = v;
                    *down_is_shortcut.as_ptr().add(pos).cast_mut() = false;
                    *down_middle.as_ptr().add(pos).cast_mut() = u32::MAX;
                }
            }
        }
    });

    // Fill arrays - shortcuts from file (sequential, I/O bound)
    {
        let mut reader =
            BufReader::with_capacity(64 * 1024 * 1024, File::open(&shortcut_path)?);
        let mut buf = [0u8; 12];
        while reader.read_exact(&mut buf).is_ok() {
            let u = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
            let w = u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]);
            let middle = u32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]);
            let rank_u = perm[u];
            let rank_w = perm[w as usize];
            if rank_u < rank_w {
                let pos = up_pos[u].fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                up_targets[pos] = w;
                up_is_shortcut[pos] = true;
                up_middle[pos] = middle;
            } else {
                let pos = down_pos[u].fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                down_targets[pos] = w;
                down_is_shortcut[pos] = true;
                down_middle[pos] = middle;
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
        .map(|u| (up_offsets_clone[u] as usize, up_offsets_clone[u + 1] as usize))
        .collect();

    up_ranges.par_iter().for_each(|&(start, end)| {
        if end > start {
            // Collect into struct vec
            let mut edges: Vec<EdgeData> = (start..end)
                .map(|i| EdgeData {
                    target: up_targets[i],
                    is_shortcut: up_is_shortcut[i],
                    middle: up_middle[i],
                })
                .collect();

            // Sort by target
            edges.sort_unstable_by_key(|e| e.target);

            // Write back - safe because ranges are disjoint
            for (i, edge) in edges.into_iter().enumerate() {
                unsafe {
                    *up_targets.as_ptr().add(start + i).cast_mut() = edge.target;
                    *up_is_shortcut.as_ptr().add(start + i).cast_mut() = edge.is_shortcut;
                    *up_middle.as_ptr().add(start + i).cast_mut() = edge.middle;
                }
            }
        }
    });

    // Sort down edges in parallel
    let down_ranges: Vec<(usize, usize)> = (0..n_nodes)
        .map(|u| (down_offsets_clone[u] as usize, down_offsets_clone[u + 1] as usize))
        .collect();

    down_ranges.par_iter().for_each(|&(start, end)| {
        if end > start {
            let mut edges: Vec<EdgeData> = (start..end)
                .map(|i| EdgeData {
                    target: down_targets[i],
                    is_shortcut: down_is_shortcut[i],
                    middle: down_middle[i],
                })
                .collect();

            edges.sort_unstable_by_key(|e| e.target);

            for (i, edge) in edges.into_iter().enumerate() {
                unsafe {
                    *down_targets.as_ptr().add(start + i).cast_mut() = edge.target;
                    *down_is_shortcut.as_ptr().add(start + i).cast_mut() = edge.is_shortcut;
                    *down_middle.as_ptr().add(start + i).cast_mut() = edge.middle;
                }
            }
        }
    });

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
        up_offsets,
        up_targets,
        up_is_shortcut,
        up_middle,
        down_offsets,
        down_targets,
        down_is_shortcut,
        down_middle,
    };
    CchTopoFile::write(&topo_path, &topo)?;
    println!("  ✓ Written {}", topo_path.display());

    let build_time_ms = start_time.elapsed().as_millis() as u64;

    Ok(Step7Result {
        topo_path,
        mode: config.mode,
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

    let mut hasher = Sha256::new();

    // Stream first file
    let mut file1 = BufReader::with_capacity(64 * 1024, File::open(filtered_ebg_path)?);
    std::io::copy(&mut file1, &mut hasher)?;

    // Stream second file
    let mut file2 = BufReader::with_capacity(64 * 1024, File::open(order_path)?);
    std::io::copy(&mut file2, &mut hasher)?;

    let result = hasher.finalize();
    let mut sha = [0u8; 32];
    sha.copy_from_slice(&result);
    Ok(sha)
}
