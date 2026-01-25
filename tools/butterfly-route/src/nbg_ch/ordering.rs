//! Parallel Nested Dissection ordering for NBG
//!
//! Computes a high-quality elimination order using inertial partitioning
//! with coordinate-based bisection. Uses rayon for parallel recursion.

use anyhow::Result;
use rayon::prelude::*;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering as AtomicOrdering};

use crate::formats::{NbgCsr, NbgGeo};

/// Nested dissection ordering result
#[derive(Debug, Clone)]
pub struct NbgNdOrdering {
    /// Node ID → rank (0..n_nodes-1)
    pub perm: Vec<u32>,
    /// Rank → node ID
    pub inv_perm: Vec<u32>,
    pub n_nodes: u32,
    pub n_components: usize,
    pub max_depth: usize,
}

/// Compute nested dissection ordering on NBG (parallelized)
pub fn compute_nbg_ordering(
    nbg_csr: &NbgCsr,
    nbg_geo: &NbgGeo,
    leaf_threshold: usize,
    _balance_eps: f32,
) -> Result<NbgNdOrdering> {
    let n_nodes = nbg_csr.n_nodes as usize;

    println!("Computing NBG ordering ({} nodes, parallel)...", n_nodes);

    // Extract node coordinates from NBG geo
    println!("  Extracting node coordinates...");
    let coords = extract_nbg_coords(nbg_csr, nbg_geo)?;
    println!("    {} coordinates extracted", coords.len());

    // Find connected components
    println!("  Finding connected components...");
    let components = find_components(nbg_csr)?;
    let n_components = components.len();
    println!("    {} components", n_components);
    for (i, comp) in components.iter().take(5).enumerate() {
        println!("      Component {}: {} nodes", i, comp.len());
    }
    if components.len() > 5 {
        println!("      ... and {} more components", components.len() - 5);
    }

    // Build adjacency list for faster neighbor lookup
    println!("  Building adjacency index...");
    let adj = build_adjacency(nbg_csr);

    // Process components in parallel and collect orderings
    println!("  Running parallel nested dissection...");
    let progress = AtomicUsize::new(0);
    let total_nodes = n_nodes;

    let component_orderings: Vec<(Vec<u32>, usize)> = components
        .par_iter()
        .map(|component| {
            let result = parallel_nd(&adj, &coords, component, leaf_threshold, 0);
            let done = progress.fetch_add(component.len(), AtomicOrdering::Relaxed);
            if (done + component.len()) * 100 / total_nodes > done * 100 / total_nodes {
                let pct = (done + component.len()) * 100 / total_nodes;
                if pct % 10 == 0 {
                    eprintln!("    {}% complete", pct);
                }
            }
            result
        })
        .collect();

    // Combine orderings from all components
    println!("  Combining orderings...");
    let mut perm = vec![u32::MAX; n_nodes];
    let mut inv_perm = Vec::with_capacity(n_nodes);
    let mut max_depth = 0;

    for (ordering, depth) in &component_orderings {
        max_depth = max_depth.max(*depth);
        for &node in ordering {
            let rank = inv_perm.len() as u32;
            perm[node as usize] = rank;
            inv_perm.push(node);
        }
    }

    println!("    Generated ordering (max depth: {})", max_depth);

    Ok(NbgNdOrdering {
        perm,
        inv_perm,
        n_nodes: n_nodes as u32,
        n_components,
        max_depth,
    })
}

/// Build adjacency list for fast neighbor lookup
fn build_adjacency(nbg_csr: &NbgCsr) -> Vec<Vec<u32>> {
    let n_nodes = nbg_csr.n_nodes as usize;
    let mut adj = vec![Vec::new(); n_nodes];

    for u in 0..n_nodes {
        let start = nbg_csr.offsets[u] as usize;
        let end = nbg_csr.offsets[u + 1] as usize;
        adj[u] = nbg_csr.heads[start..end].to_vec();
    }

    adj
}

/// Parallel nested dissection on a component
fn parallel_nd(
    adj: &[Vec<u32>],
    coords: &[(f64, f64)],
    nodes: &[u32],
    leaf_threshold: usize,
    depth: usize,
) -> (Vec<u32>, usize) {
    // Base case: small component - use simple ordering
    if nodes.len() <= leaf_threshold {
        return (nodes.to_vec(), depth);
    }

    // Inertial bisection
    let (left, right, separator) = inertial_bisect_fast(adj, coords, nodes);

    // If bisection failed (all in one side), just return the nodes
    if left.is_empty() || right.is_empty() {
        return (nodes.to_vec(), depth);
    }

    // Parallel recursion on left and right
    let ((left_order, left_depth), (right_order, right_depth)) = rayon::join(
        || parallel_nd(adj, coords, &left, leaf_threshold, depth + 1),
        || parallel_nd(adj, coords, &right, leaf_threshold, depth + 1),
    );

    // Combine: left, right, then separator (separator gets highest ranks)
    let mut ordering = Vec::with_capacity(nodes.len());
    ordering.extend(left_order);
    ordering.extend(right_order);
    ordering.extend(separator);

    let max_depth = left_depth.max(right_depth);
    (ordering, max_depth)
}

/// Fast inertial bisection using principal axis
fn inertial_bisect_fast(
    adj: &[Vec<u32>],
    coords: &[(f64, f64)],
    nodes: &[u32],
) -> (Vec<u32>, Vec<u32>, Vec<u32>) {
    if nodes.len() <= 1 {
        return (vec![], vec![], nodes.to_vec());
    }

    // Compute centroid
    let mut sum_x = 0.0f64;
    let mut sum_y = 0.0f64;
    for &node in nodes {
        let (x, y) = coords[node as usize];
        sum_x += x;
        sum_y += y;
    }
    let n = nodes.len() as f64;
    let cx = sum_x / n;
    let cy = sum_y / n;

    // Compute principal axis via covariance
    let mut cxx = 0.0f64;
    let mut cyy = 0.0f64;
    let mut cxy = 0.0f64;
    for &node in nodes {
        let (x, y) = coords[node as usize];
        let dx = x - cx;
        let dy = y - cy;
        cxx += dx * dx;
        cyy += dy * dy;
        cxy += dx * dy;
    }

    // Principal axis direction
    let (ax, ay) = if cxy.abs() > 1e-10 {
        let trace = cxx + cyy;
        let det = cxx * cyy - cxy * cxy;
        let discrim = ((trace * trace / 4.0) - det).max(0.0).sqrt();
        let ratio = (cxx - (trace / 2.0 - discrim)) / cxy;
        let len = (1.0 + ratio * ratio).sqrt();
        (1.0 / len, ratio / len)
    } else if cxx >= cyy {
        (1.0, 0.0)
    } else {
        (0.0, 1.0)
    };

    // Project nodes and sort by projection
    let mut projections: Vec<(u32, f64)> = nodes
        .iter()
        .map(|&node| {
            let (x, y) = coords[node as usize];
            let proj = (x - cx) * ax + (y - cy) * ay;
            (node, proj)
        })
        .collect();

    projections.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

    // Find median cut point
    let cut_idx = nodes.len() / 2;
    let cut_value = projections[cut_idx].1;

    // Build node set for fast lookup
    let node_set: std::collections::HashSet<u32> = nodes.iter().copied().collect();

    // Partition into left/right/separator
    // A node is in separator if it has neighbors on both sides
    let mut left = Vec::new();
    let mut right = Vec::new();
    let mut separator = Vec::new();

    for &(node, proj) in &projections {
        let mut has_left = false;
        let mut has_right = false;

        for &neighbor in &adj[node as usize] {
            if !node_set.contains(&neighbor) {
                continue;
            }
            // Find neighbor's projection (binary search since sorted)
            if let Ok(idx) = projections.binary_search_by(|(n, _)| n.cmp(&neighbor)) {
                let n_proj = projections[idx].1;
                if n_proj < cut_value {
                    has_left = true;
                } else {
                    has_right = true;
                }
            }
        }

        if has_left && has_right {
            separator.push(node);
        } else if proj < cut_value {
            left.push(node);
        } else {
            right.push(node);
        }
    }

    // If separator is too large (>20% of nodes), just use positional split
    if separator.len() > nodes.len() / 5 {
        left.clear();
        right.clear();
        separator.clear();

        for (i, &(node, _)) in projections.iter().enumerate() {
            if i < cut_idx {
                left.push(node);
            } else {
                right.push(node);
            }
        }
    }

    (left, right, separator)
}

/// Extract coordinates for all NBG nodes
fn extract_nbg_coords(nbg_csr: &NbgCsr, nbg_geo: &NbgGeo) -> Result<Vec<(f64, f64)>> {
    let n_nodes = nbg_csr.n_nodes as usize;
    let mut coords = vec![(0.0, 0.0); n_nodes];
    let mut seen = vec![false; n_nodes];

    // Each edge in nbg_geo connects u_node to v_node
    // Coordinates are stored in the polyline (first/last points)
    for (edge_idx, edge) in nbg_geo.edges.iter().enumerate() {
        let u = edge.u_node as usize;
        let v = edge.v_node as usize;

        // Get coordinates from polyline
        let poly = &nbg_geo.polylines[edge_idx];
        if poly.lat_fxp.is_empty() {
            continue;
        }

        // First point is u_node
        if !seen[u] {
            let u_lat = poly.lat_fxp[0] as f64 * 1e-7;
            let u_lon = poly.lon_fxp[0] as f64 * 1e-7;
            coords[u] = (u_lon, u_lat);
            seen[u] = true;
        }

        // Last point is v_node
        let last_idx = poly.lat_fxp.len() - 1;
        if !seen[v] {
            let v_lat = poly.lat_fxp[last_idx] as f64 * 1e-7;
            let v_lon = poly.lon_fxp[last_idx] as f64 * 1e-7;
            coords[v] = (v_lon, v_lat);
            seen[v] = true;
        }
    }

    // Verify all nodes have coordinates
    let n_missing = seen.iter().filter(|&&s| !s).count();
    if n_missing > 0 {
        println!("    WARNING: {} nodes without coordinates", n_missing);
    }

    Ok(coords)
}

/// Find connected components in NBG
fn find_components(nbg_csr: &NbgCsr) -> Result<Vec<Vec<u32>>> {
    let n_nodes = nbg_csr.n_nodes as usize;
    let mut visited = vec![false; n_nodes];
    let mut components = Vec::new();

    for start in 0..n_nodes {
        if visited[start] {
            continue;
        }

        let mut component = Vec::new();
        let mut queue = VecDeque::new();
        queue.push_back(start);
        visited[start] = true;

        while let Some(node) = queue.pop_front() {
            component.push(node as u32);

            let start_edge = nbg_csr.offsets[node] as usize;
            let end_edge = nbg_csr.offsets[node + 1] as usize;

            for i in start_edge..end_edge {
                let neighbor = nbg_csr.heads[i] as usize;
                if !visited[neighbor] {
                    visited[neighbor] = true;
                    queue.push_back(neighbor);
                }
            }
        }

        components.push(component);
    }

    // Sort by size descending
    components.sort_by(|a, b| b.len().cmp(&a.len()));

    Ok(components)
}
