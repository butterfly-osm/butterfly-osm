//! Nested Dissection ordering for NBG
//!
//! Computes a high-quality elimination order using inertial partitioning
//! with coordinate-based bisection.

use anyhow::Result;
use std::collections::{HashSet, VecDeque};

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

/// Compute nested dissection ordering on NBG
pub fn compute_nbg_ordering(
    nbg_csr: &NbgCsr,
    nbg_geo: &NbgGeo,
    leaf_threshold: usize,
    balance_eps: f32,
) -> Result<NbgNdOrdering> {
    let n_nodes = nbg_csr.n_nodes as usize;

    println!("Computing NBG ordering ({} nodes)...", n_nodes);

    // Extract node coordinates from NBG geo
    // Each NBG node can be found as the u_node or v_node of edges
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

    // Build ordering via nested dissection
    println!("  Building nested dissection ordering...");
    let mut builder = NdBuilder::new(n_nodes, leaf_threshold, balance_eps);

    let mut max_depth = 0;
    for (comp_idx, component) in components.iter().enumerate() {
        if comp_idx % 100 == 0 && comp_idx > 0 {
            println!("    Processing component {} / {}...", comp_idx, components.len());
        }
        let depth = builder.order_component(nbg_csr, &coords, component)?;
        max_depth = max_depth.max(depth);
    }

    let (perm, inv_perm) = builder.finish();
    println!("    Generated ordering (max depth: {})", max_depth);

    Ok(NbgNdOrdering {
        perm,
        inv_perm,
        n_nodes: nbg_csr.n_nodes,
        n_components,
        max_depth,
    })
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

/// Nested dissection builder
struct NdBuilder {
    n_nodes: usize,
    perm: Vec<u32>,        // node → rank
    inv_perm: Vec<u32>,    // rank → node
    next_rank: u32,
    leaf_threshold: usize,
    balance_eps: f32,
}

impl NdBuilder {
    fn new(n_nodes: usize, leaf_threshold: usize, balance_eps: f32) -> Self {
        Self {
            n_nodes,
            perm: vec![u32::MAX; n_nodes],
            inv_perm: Vec::with_capacity(n_nodes),
            next_rank: 0,
            leaf_threshold,
            balance_eps,
        }
    }

    fn assign_rank(&mut self, node: u32) {
        if self.perm[node as usize] == u32::MAX {
            self.perm[node as usize] = self.next_rank;
            self.inv_perm.push(node);
            self.next_rank += 1;
        }
    }

    fn order_component(
        &mut self,
        nbg_csr: &NbgCsr,
        coords: &[(f64, f64)],
        component: &[u32],
    ) -> Result<usize> {
        if component.is_empty() {
            return Ok(0);
        }

        let result = self.recursive_nd(nbg_csr, coords, component, 0)?;

        // Assign ranks in order
        for &node in &result.ordering {
            self.assign_rank(node);
        }

        Ok(result.depth)
    }

    fn recursive_nd(
        &self,
        nbg_csr: &NbgCsr,
        coords: &[(f64, f64)],
        nodes: &[u32],
        depth: usize,
    ) -> Result<NdResult> {
        // Base case: small component
        if nodes.len() <= self.leaf_threshold {
            return Ok(NdResult {
                ordering: nodes.to_vec(),
                depth,
            });
        }

        // Inertial bisection
        let (left, right, separator) = self.inertial_bisect(nbg_csr, coords, nodes)?;

        // Recursively order left and right
        let left_result = if !left.is_empty() {
            self.recursive_nd(nbg_csr, coords, &left, depth + 1)?
        } else {
            NdResult { ordering: vec![], depth }
        };

        let right_result = if !right.is_empty() {
            self.recursive_nd(nbg_csr, coords, &right, depth + 1)?
        } else {
            NdResult { ordering: vec![], depth }
        };

        // Combine: left first, then right, then separator (highest rank)
        let mut ordering = Vec::with_capacity(nodes.len());
        ordering.extend(left_result.ordering);
        ordering.extend(right_result.ordering);
        ordering.extend(separator);

        let max_depth = left_result.depth.max(right_result.depth);

        Ok(NdResult {
            ordering,
            depth: max_depth,
        })
    }

    fn inertial_bisect(
        &self,
        nbg_csr: &NbgCsr,
        coords: &[(f64, f64)],
        nodes: &[u32],
    ) -> Result<(Vec<u32>, Vec<u32>, Vec<u32>)> {
        if nodes.len() <= 1 {
            return Ok((vec![], vec![], nodes.to_vec()));
        }

        // Find principal axis via coordinate centroid and variance
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

        // Compute covariance matrix
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

        // Principal axis direction (eigenvector of larger eigenvalue)
        let trace = cxx + cyy;
        let det = cxx * cyy - cxy * cxy;
        let discrim = ((trace * trace / 4.0) - det).max(0.0).sqrt();
        let _lambda1 = trace / 2.0 + discrim;

        // Direction of principal axis
        let (ax, ay) = if cxy.abs() > 1e-10 {
            let ratio = (cxx - (trace / 2.0 - discrim)) / cxy;
            let len = (1.0 + ratio * ratio).sqrt();
            (1.0 / len, ratio / len)
        } else if cxx >= cyy {
            (1.0, 0.0)
        } else {
            (0.0, 1.0)
        };

        // Project nodes onto principal axis
        let mut projections: Vec<(u32, f64)> = nodes.iter()
            .map(|&node| {
                let (x, y) = coords[node as usize];
                let proj = (x - cx) * ax + (y - cy) * ay;
                (node, proj)
            })
            .collect();

        projections.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

        // Find balanced cut point
        let target = (nodes.len() as f32 * (0.5 - self.balance_eps)) as usize;
        let cut_idx = target.max(1).min(nodes.len() - 1);

        let cut_value = projections[cut_idx].1;

        // Build left/right/separator sets
        let node_set: HashSet<u32> = nodes.iter().copied().collect();

        let mut left = Vec::new();
        let mut right = Vec::new();
        let mut separator = Vec::new();

        for &(node, proj) in &projections {
            // Check if this node has neighbors on both sides (separator candidate)
            let start = nbg_csr.offsets[node as usize] as usize;
            let end = nbg_csr.offsets[node as usize + 1] as usize;

            let mut has_left_neighbor = false;
            let mut has_right_neighbor = false;

            for i in start..end {
                let neighbor = nbg_csr.heads[i];
                if !node_set.contains(&neighbor) {
                    continue;
                }
                // Find neighbor's projection
                if let Some(&(_, n_proj)) = projections.iter().find(|(n, _)| *n == neighbor) {
                    if n_proj < cut_value {
                        has_left_neighbor = true;
                    } else {
                        has_right_neighbor = true;
                    }
                }
            }

            if has_left_neighbor && has_right_neighbor {
                separator.push(node);
            } else if proj < cut_value {
                left.push(node);
            } else {
                right.push(node);
            }
        }

        // If separator is too large, just use positional split
        if separator.len() > nodes.len() / 3 {
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

        Ok((left, right, separator))
    }

    fn finish(self) -> (Vec<u32>, Vec<u32>) {
        (self.perm, self.inv_perm)
    }
}

struct NdResult {
    ordering: Vec<u32>,
    depth: usize,
}
