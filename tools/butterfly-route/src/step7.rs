///! Step 7: CCH Contraction
///!
///! Builds the CCH topology (shortcuts) using the EBG ordering.
///! Uses streaming to disk to avoid memory explosion.
///!
///! Key optimization: **Witness Search**
///! Before adding a shortcut (u, w) when contracting v, we check if there's
///! already a path u → x → w through some other higher-ranked node x.
///! If such a witness path exists, the shortcut is redundant and skipped.

use anyhow::Result;
use rayon::prelude::*;
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::PathBuf;

use crate::formats::{CchTopo, CchTopoFile, EbgCsrFile, OrderEbgFile};

/// Check if there's a witness path u → ... → w (not through v) using depth-3 search.
/// Returns true if a witness exists (meaning we DON'T need the shortcut).
#[inline]
fn has_witness(
    u: usize,
    w: u32,
    v: u32,
    out_higher: &[HashSet<u32>],
    in_higher: &[HashSet<u32>],
) -> bool {
    let w_idx = w as usize;

    // Depth-2 forward check: u → x → w
    for &x in &out_higher[u] {
        if x == v {
            continue;
        }
        if out_higher[x as usize].contains(&w) {
            return true;
        }
    }

    // Depth-2 backward check: u → x → w via in_higher of w
    for &x in &in_higher[w_idx] {
        if x == v {
            continue;
        }
        if out_higher[u].contains(&x) {
            return true;
        }
    }

    // Depth-3 check: u → x → y → w
    // Forward from u: check if any neighbor of u can reach w in 2 hops
    for &x in &out_higher[u] {
        if x == v {
            continue;
        }
        let x_idx = x as usize;
        for &y in &out_higher[x_idx] {
            if y == v {
                continue;
            }
            if out_higher[y as usize].contains(&w) {
                return true;
            }
        }
    }

    // Depth-3 backward check: u → x → y → w via in_higher
    for &y in &in_higher[w_idx] {
        if y == v {
            continue;
        }
        let y_idx = y as usize;
        for &x in &in_higher[y_idx] {
            if x == v {
                continue;
            }
            if out_higher[u].contains(&x) {
                return true;
            }
        }
    }

    false
}

/// Configuration for Step 7
pub struct Step7Config {
    pub ebg_csr_path: PathBuf,
    pub order_path: PathBuf,
    pub outdir: PathBuf,
}

/// Result of Step 7 contraction
#[derive(Debug)]
pub struct Step7Result {
    pub topo_path: PathBuf,
    pub n_nodes: u32,
    pub n_original_arcs: u64,
    pub n_shortcuts: u64,
    pub n_up_edges: u64,
    pub n_down_edges: u64,
    pub build_time_ms: u64,
}

/// Build CCH topology via contraction
pub fn build_cch_topology(config: Step7Config) -> Result<Step7Result> {
    let start_time = std::time::Instant::now();
    println!("\n🔨 Step 7: Building CCH topology...\n");

    // Load EBG CSR
    println!("Loading EBG CSR...");
    let ebg_csr = EbgCsrFile::read(&config.ebg_csr_path)?;
    println!("  ✓ {} nodes, {} arcs", ebg_csr.n_nodes, ebg_csr.n_arcs);

    // Load ordering
    println!("Loading ordering...");
    let order = OrderEbgFile::read(&config.order_path)?;
    println!("  ✓ {} nodes", order.n_nodes);

    if ebg_csr.n_nodes != order.n_nodes {
        anyhow::bail!(
            "Node count mismatch: EBG has {} nodes, order has {}",
            ebg_csr.n_nodes,
            order.n_nodes
        );
    }

    let n_nodes = ebg_csr.n_nodes as usize;
    let perm = &order.perm;
    let inv_perm = &order.inv_perm;

    // Build initial adjacency using HashSet
    println!("\nBuilding initial higher-neighbor lists (parallel)...");

    let (out_higher, in_higher): (Vec<HashSet<u32>>, Vec<HashSet<u32>>) = {
        let out: Vec<HashSet<u32>> = (0..n_nodes)
            .into_par_iter()
            .map(|u| {
                let rank_u = perm[u];
                let start = ebg_csr.offsets[u] as usize;
                let end = ebg_csr.offsets[u + 1] as usize;
                let mut set = HashSet::new();
                for i in start..end {
                    let v = ebg_csr.heads[i] as usize;
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
            let start = ebg_csr.offsets[u] as usize;
            let end = ebg_csr.offsets[u + 1] as usize;
            for i in start..end {
                let v = ebg_csr.heads[i] as usize;
                if u != v && rank_u > perm[v] {
                    in_vecs[v].push(u as u32);
                }
            }
        }

        let in_sets: Vec<HashSet<u32>> = in_vecs
            .into_par_iter()
            .map(|v| v.into_iter().collect())
            .collect();

        (out, in_sets)
    };

    let mut out_higher = out_higher;
    let mut in_higher = in_higher;
    println!("  ✓ Built initial neighbor lists");

    // Stream shortcuts to temp file to avoid memory explosion
    std::fs::create_dir_all(&config.outdir)?;
    let shortcut_path = config.outdir.join("shortcuts.tmp");
    let mut shortcut_writer = BufWriter::with_capacity(64 * 1024 * 1024, File::create(&shortcut_path)?);
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
            println!("  {:5.1}% contracted ({} shortcuts, max_degree={})", pct, n_shortcuts, max_degree_seen);
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
        // A shortcut (u, w) is only needed if there's no witness path u → x → w
        // through some other higher-ranked node x (not v)
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

                            // Check 2: Witness path exists? (depth-2 search)
                            if has_witness(u_idx, w, v_u32, out_higher_ref, in_higher_ref) {
                                return None;
                            }

                            Some((u, w))
                        })
                        .collect::<Vec<_>>()
                })
                .collect()
        } else {
            // Sequential for small neighborhoods
            let mut result = Vec::new();
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

                    // Check 2: Witness path exists? (depth-2 search)
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

    // Count edges per node (parallel)
    let mut up_counts: Vec<usize> = vec![0; n_nodes];
    let mut down_counts: Vec<usize> = vec![0; n_nodes];

    // Original edges
    for u in 0..n_nodes {
        let rank_u = perm[u];
        let start = ebg_csr.offsets[u] as usize;
        let end = ebg_csr.offsets[u + 1] as usize;
        for i in start..end {
            let v = ebg_csr.heads[i] as usize;
            if u == v { continue; }
            let rank_v = perm[v];
            if rank_u < rank_v {
                up_counts[u] += 1;
            } else {
                down_counts[u] += 1;
            }
        }
    }

    // Shortcuts - stream from file
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

    // Allocate edge arrays
    let mut up_targets = vec![0u32; n_up_edges as usize];
    let mut up_is_shortcut = vec![false; n_up_edges as usize];
    let mut up_middle = vec![u32::MAX; n_up_edges as usize];

    let mut down_targets = vec![0u32; n_down_edges as usize];
    let mut down_is_shortcut = vec![false; n_down_edges as usize];
    let mut down_middle = vec![u32::MAX; n_down_edges as usize];

    // Fill arrays - original edges
    let mut up_pos: Vec<usize> = up_offsets.iter().map(|&x| x as usize).collect();
    let mut down_pos: Vec<usize> = down_offsets.iter().map(|&x| x as usize).collect();

    for u in 0..n_nodes {
        let rank_u = perm[u];
        let start = ebg_csr.offsets[u] as usize;
        let end = ebg_csr.offsets[u + 1] as usize;
        for i in start..end {
            let v = ebg_csr.heads[i];
            if u == v as usize { continue; }
            let rank_v = perm[v as usize];
            if rank_u < rank_v {
                let pos = up_pos[u];
                up_targets[pos] = v;
                up_is_shortcut[pos] = false;
                up_middle[pos] = u32::MAX;
                up_pos[u] += 1;
            } else {
                let pos = down_pos[u];
                down_targets[pos] = v;
                down_is_shortcut[pos] = false;
                down_middle[pos] = u32::MAX;
                down_pos[u] += 1;
            }
        }
    }

    // Fill arrays - shortcuts from file
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
                let pos = up_pos[u];
                up_targets[pos] = w;
                up_is_shortcut[pos] = true;
                up_middle[pos] = middle;
                up_pos[u] += 1;
            } else {
                let pos = down_pos[u];
                down_targets[pos] = w;
                down_is_shortcut[pos] = true;
                down_middle[pos] = middle;
                down_pos[u] += 1;
            }
        }
    }

    // Remove temp file
    std::fs::remove_file(&shortcut_path)?;

    // Sort edges within each node (parallel)
    println!("  Sorting edges (parallel)...");

    // Collect ranges for parallel processing
    let up_ranges: Vec<(usize, usize)> = (0..n_nodes)
        .map(|u| (up_offsets[u] as usize, up_offsets[u + 1] as usize))
        .collect();
    let down_ranges: Vec<(usize, usize)> = (0..n_nodes)
        .map(|u| (down_offsets[u] as usize, down_offsets[u + 1] as usize))
        .collect();

    // Sort up edges in parallel using unsafe for disjoint slices
    up_ranges.par_iter().for_each(|&(start, end)| {
        if end > start {
            let len = end - start;
            let mut indices: Vec<usize> = (0..len).collect();

            // Safe: each thread works on disjoint ranges
            let targets_ptr = up_targets.as_ptr();
            let is_sc_ptr = up_is_shortcut.as_ptr();
            let mid_ptr = up_middle.as_ptr();

            indices.sort_by_key(|&i| unsafe { *targets_ptr.add(start + i) });

            let sorted_targets: Vec<u32> = indices.iter().map(|&i| unsafe { *targets_ptr.add(start + i) }).collect();
            let sorted_is_sc: Vec<bool> = indices.iter().map(|&i| unsafe { *is_sc_ptr.add(start + i) }).collect();
            let sorted_mid: Vec<u32> = indices.iter().map(|&i| unsafe { *mid_ptr.add(start + i) }).collect();

            // Write back - safe because ranges are disjoint
            unsafe {
                let targets_mut = up_targets.as_ptr() as *mut u32;
                let is_sc_mut = up_is_shortcut.as_ptr() as *mut bool;
                let mid_mut = up_middle.as_ptr() as *mut u32;

                for (i, &t) in sorted_targets.iter().enumerate() {
                    *targets_mut.add(start + i) = t;
                }
                for (i, &s) in sorted_is_sc.iter().enumerate() {
                    *is_sc_mut.add(start + i) = s;
                }
                for (i, &m) in sorted_mid.iter().enumerate() {
                    *mid_mut.add(start + i) = m;
                }
            }
        }
    });

    // Sort down edges in parallel
    down_ranges.par_iter().for_each(|&(start, end)| {
        if end > start {
            let len = end - start;
            let mut indices: Vec<usize> = (0..len).collect();

            let targets_ptr = down_targets.as_ptr();
            let is_sc_ptr = down_is_shortcut.as_ptr();
            let mid_ptr = down_middle.as_ptr();

            indices.sort_by_key(|&i| unsafe { *targets_ptr.add(start + i) });

            let sorted_targets: Vec<u32> = indices.iter().map(|&i| unsafe { *targets_ptr.add(start + i) }).collect();
            let sorted_is_sc: Vec<bool> = indices.iter().map(|&i| unsafe { *is_sc_ptr.add(start + i) }).collect();
            let sorted_mid: Vec<u32> = indices.iter().map(|&i| unsafe { *mid_ptr.add(start + i) }).collect();

            unsafe {
                let targets_mut = down_targets.as_ptr() as *mut u32;
                let is_sc_mut = down_is_shortcut.as_ptr() as *mut bool;
                let mid_mut = down_middle.as_ptr() as *mut u32;

                for (i, &t) in sorted_targets.iter().enumerate() {
                    *targets_mut.add(start + i) = t;
                }
                for (i, &s) in sorted_is_sc.iter().enumerate() {
                    *is_sc_mut.add(start + i) = s;
                }
                for (i, &m) in sorted_mid.iter().enumerate() {
                    *mid_mut.add(start + i) = m;
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

    // Compute inputs SHA
    let inputs_sha = compute_inputs_sha(&config.ebg_csr_path, &config.order_path)?;

    // Write output
    let topo_path = config.outdir.join("cch.topo");

    println!("\nWriting output...");
    let topo = CchTopo {
        n_nodes: ebg_csr.n_nodes,
        n_shortcuts,
        n_original_arcs: ebg_csr.n_arcs,
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
        n_nodes: ebg_csr.n_nodes,
        n_original_arcs: ebg_csr.n_arcs,
        n_shortcuts,
        n_up_edges,
        n_down_edges,
        build_time_ms,
    })
}

fn compute_inputs_sha(
    ebg_csr_path: &std::path::Path,
    order_path: &std::path::Path,
) -> Result<[u8; 32]> {
    use sha2::{Digest, Sha256};

    let mut hasher = Sha256::new();
    hasher.update(&std::fs::read(ebg_csr_path)?);
    hasher.update(&std::fs::read(order_path)?);

    let result = hasher.finalize();
    let mut sha = [0u8; 32];
    sha.copy_from_slice(&result);
    Ok(sha)
}
