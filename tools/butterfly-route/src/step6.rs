///! Step 6: Nested Dissection ordering on EBG
///!
///! Computes a high-quality elimination order on the Edge-Based Graph for CCH.
///! This is the correct approach: routing state = directed edge ID.

use anyhow::Result;
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};

use crate::formats::{EbgCsrFile, EbgNodesFile, NbgGeoFile, OrderEbg, OrderEbgFile};

/// Configuration for Step 6
pub struct Step6Config {
    pub ebg_csr_path: PathBuf,
    pub ebg_nodes_path: PathBuf,
    pub nbg_geo_path: PathBuf,
    pub outdir: PathBuf,
    pub leaf_threshold: usize,
    pub balance_eps: f32,
}

/// Result of Step 6 ordering
#[derive(Debug)]
pub struct Step6Result {
    pub order_path: PathBuf,
    pub n_nodes: u32,
    pub n_components: usize,
    pub tree_depth: usize,
    pub build_time_ms: u64,
}

/// Generate nested dissection ordering on EBG
pub fn generate_ordering(config: Step6Config) -> Result<Step6Result> {
    let start_time = std::time::Instant::now();
    println!("\n📐 Step 6: Generating CCH ordering on EBG...\n");

    // Load EBG CSR
    println!("Loading EBG CSR...");
    let ebg_csr = EbgCsrFile::read(&config.ebg_csr_path)?;
    println!("  ✓ {} nodes, {} arcs", ebg_csr.n_nodes, ebg_csr.n_arcs);

    // Load EBG nodes (for geometry linkage)
    println!("Loading EBG nodes...");
    let ebg_nodes = EbgNodesFile::read(&config.ebg_nodes_path)?;
    println!("  ✓ {} nodes", ebg_nodes.n_nodes);

    // Load NBG geo (for coordinates)
    println!("Loading NBG geo...");
    let nbg_geo = NbgGeoFile::read(&config.nbg_geo_path)?;
    println!("  ✓ {} edges", nbg_geo.n_edges_und);

    // Extract coordinates for each EBG node
    // EBG node i corresponds to directed edge (tail_nbg -> head_nbg)
    // Use the midpoint of the NBG edge as the coordinate
    println!("\nExtracting EBG node coordinates...");
    let coords = extract_ebg_coordinates(&ebg_nodes, &nbg_geo)?;
    println!("  ✓ {} coordinates", coords.len());

    // Find connected components
    println!("\nFinding connected components...");
    let components = find_components(&ebg_csr)?;
    let n_components = components.len();
    println!("  ✓ {} components", n_components);
    for (i, comp) in components.iter().take(5).enumerate() {
        println!("    Component {}: {} nodes", i, comp.len());
    }
    if components.len() > 5 {
        println!("    ... and {} more small components", components.len() - 5);
    }

    // Build ordering via nested dissection
    println!("\nBuilding nested dissection ordering...");
    let mut builder = NdBuilder::new(
        ebg_csr.n_nodes as usize,
        config.leaf_threshold,
        config.balance_eps,
    );

    let mut max_depth = 0;
    for (comp_idx, component) in components.iter().enumerate() {
        if comp_idx % 100 == 0 && comp_idx > 0 {
            println!("  Processing component {} / {}...", comp_idx, components.len());
        }
        let depth = builder.order_component(&ebg_csr, &coords, component)?;
        max_depth = max_depth.max(depth);
    }

    let (perm, inv_perm) = builder.finish();
    println!("  ✓ Generated ordering (max depth: {})", max_depth);

    // Compute inputs SHA
    let inputs_sha = compute_inputs_sha(
        &config.ebg_csr_path,
        &config.ebg_nodes_path,
        &config.nbg_geo_path,
    )?;

    // Write output
    std::fs::create_dir_all(&config.outdir)?;
    let order_path = config.outdir.join("order.ebg");

    println!("\nWriting output...");
    let order = OrderEbg {
        n_nodes: ebg_csr.n_nodes,
        inputs_sha,
        perm,
        inv_perm,
    };
    OrderEbgFile::write(&order_path, &order)?;
    println!("  ✓ Written {}", order_path.display());

    let build_time_ms = start_time.elapsed().as_millis() as u64;

    Ok(Step6Result {
        order_path,
        n_nodes: ebg_csr.n_nodes,
        n_components,
        tree_depth: max_depth,
        build_time_ms,
    })
}

/// Extract (lon, lat) coordinates for each EBG node from NBG geometry
///
/// EBG node i represents a directed edge. We use the midpoint of the
/// corresponding NBG edge's geometry as the coordinate for partitioning.
fn extract_ebg_coordinates(
    ebg_nodes: &crate::formats::EbgNodes,
    nbg_geo: &crate::formats::NbgGeo,
) -> Result<Vec<(f64, f64)>> {
    let mut coords = Vec::with_capacity(ebg_nodes.n_nodes as usize);

    // If polylines are available, build a coordinate cache from them
    // Otherwise fall back to edge endpoint interpolation
    let has_polylines = !nbg_geo.polylines.is_empty();

    for node in &ebg_nodes.nodes {
        let geom_idx = node.geom_idx as usize;

        if geom_idx < nbg_geo.edges.len() {
            let edge = &nbg_geo.edges[geom_idx];

            // Try to get coordinates from polyline
            if has_polylines && geom_idx < nbg_geo.polylines.len() {
                let poly = &nbg_geo.polylines[geom_idx];
                if !poly.lat_fxp.is_empty() && !poly.lon_fxp.is_empty() {
                    // Use midpoint of polyline
                    let mid = poly.lat_fxp.len() / 2;
                    let lat = poly.lat_fxp[mid] as f64 * 1e-7;
                    let lon = poly.lon_fxp[mid] as f64 * 1e-7;
                    coords.push((lon, lat));
                    continue;
                }
            }

            // Fallback: use bearing and length to estimate position
            // This creates a pseudo-coordinate that preserves spatial locality
            let bearing_rad = (edge.bearing_deci_deg as f64 / 10.0).to_radians();
            let length_km = edge.length_mm as f64 / 1_000_000.0;

            // Use edge index as base position, offset by bearing/length
            let base_x = (geom_idx as f64) * 0.001;
            let base_y = (geom_idx as f64) * 0.001;
            let lon = base_x + bearing_rad.cos() * length_km * 0.01;
            let lat = base_y + bearing_rad.sin() * length_km * 0.01;
            coords.push((lon, lat));
        } else {
            // Last resort: use EBG node index as pseudo-coordinate
            let idx = coords.len() as f64;
            coords.push((idx * 0.0001, idx * 0.0001));
        }
    }

    Ok(coords)
}

/// Find connected components using BFS on the EBG
fn find_components(csr: &crate::formats::EbgCsr) -> Result<Vec<Vec<u32>>> {
    let n = csr.n_nodes as usize;
    let mut visited = vec![false; n];
    let mut components = Vec::new();

    for start in 0..n {
        if visited[start] {
            continue;
        }

        let mut component = Vec::new();
        let mut queue = VecDeque::new();
        queue.push_back(start);
        visited[start] = true;

        while let Some(u) = queue.pop_front() {
            component.push(u as u32);

            let start_idx = csr.offsets[u] as usize;
            let end_idx = csr.offsets[u + 1] as usize;
            for i in start_idx..end_idx {
                let v = csr.heads[i] as usize;
                if !visited[v] {
                    visited[v] = true;
                    queue.push_back(v);
                }
            }
        }

        components.push(component);
    }

    // Sort by size descending, then by min node ID for determinism
    components.sort_by(|a, b| {
        b.len()
            .cmp(&a.len())
            .then_with(|| a.iter().min().cmp(&b.iter().min()))
    });

    Ok(components)
}

/// Nested dissection builder
struct NdBuilder {
    perm: Vec<u32>,
    inv_perm: Vec<u32>,
    next_rank: u32,
    leaf_threshold: usize,
    balance_eps: f32,
}

impl NdBuilder {
    fn new(n_nodes: usize, leaf_threshold: usize, balance_eps: f32) -> Self {
        Self {
            perm: vec![u32::MAX; n_nodes],
            inv_perm: vec![u32::MAX; n_nodes],
            next_rank: 0,
            leaf_threshold,
            balance_eps,
        }
    }

    fn order_component(
        &mut self,
        csr: &crate::formats::EbgCsr,
        coords: &[(f64, f64)],
        component: &[u32],
    ) -> Result<usize> {
        if component.is_empty() {
            return Ok(0);
        }

        let result = self.recursive_nd(csr, coords, component, 0)?;

        // Assign ranks
        for &node in &result.ordering {
            self.assign_rank(node);
        }

        Ok(result.depth)
    }

    fn recursive_nd(
        &self,
        csr: &crate::formats::EbgCsr,
        coords: &[(f64, f64)],
        nodes: &[u32],
        depth: usize,
    ) -> Result<NdResult> {
        let n_sub = nodes.len();

        // Base case: small subgraph
        if n_sub <= self.leaf_threshold {
            let ordering = self.minimum_degree_order(csr, nodes);
            return Ok(NdResult {
                ordering,
                depth,
            });
        }

        // Inertial partitioning
        let (part_a, part_b, separator) = self.inertial_partition(csr, coords, nodes)?;

        let balance = part_a.len() as f32 / (part_a.len() + part_b.len()).max(1) as f32;

        // Quality check: if balance is very bad, fall back to leaf ordering
        // Be more permissive - only give up on very extreme imbalance
        if balance < 0.2 || balance > 0.8 {
            let ordering = self.minimum_degree_order(csr, nodes);
            return Ok(NdResult {
                ordering,
                depth,
            });
        }

        // Parallel recursion for large subgraphs
        const PARALLEL_THRESHOLD: usize = 50_000;

        let (result_a, result_b) = if part_a.len() >= PARALLEL_THRESHOLD
            && part_b.len() >= PARALLEL_THRESHOLD
        {
            rayon::join(
                || self.recursive_nd(csr, coords, &part_a, depth + 1),
                || self.recursive_nd(csr, coords, &part_b, depth + 1),
            )
        } else {
            let a = self.recursive_nd(csr, coords, &part_a, depth + 1)?;
            let b = self.recursive_nd(csr, coords, &part_b, depth + 1)?;
            (Ok(a), Ok(b))
        };

        let result_a = result_a?;
        let result_b = result_b?;

        // Combine: [A, B, S] - separator eliminated last
        let mut ordering = result_a.ordering;
        ordering.extend(result_b.ordering);
        ordering.extend(separator);

        Ok(NdResult {
            ordering,
            depth: result_a.depth.max(result_b.depth),
        })
    }

    fn inertial_partition(
        &self,
        csr: &crate::formats::EbgCsr,
        coords: &[(f64, f64)],
        nodes: &[u32],
    ) -> Result<(Vec<u32>, Vec<u32>, Vec<u32>)> {
        if nodes.len() <= 2 {
            return Ok((vec![], vec![], nodes.to_vec()));
        }

        // Compute mean
        let mut mean_x = 0.0;
        let mut mean_y = 0.0;
        for &node in nodes {
            let (x, y) = coords[node as usize];
            mean_x += x;
            mean_y += y;
        }
        mean_x /= nodes.len() as f64;
        mean_y /= nodes.len() as f64;

        // Compute covariance matrix for PCA
        let mut cov_xx = 0.0;
        let mut cov_xy = 0.0;
        let mut cov_yy = 0.0;
        for &node in nodes {
            let (x, y) = coords[node as usize];
            let dx = x - mean_x;
            let dy = y - mean_y;
            cov_xx += dx * dx;
            cov_xy += dx * dy;
            cov_yy += dy * dy;
        }

        // Principal direction via eigenvalue decomposition of 2x2 matrix
        let (dir_x, dir_y) = compute_principal_direction(cov_xx, cov_xy, cov_yy);

        // Project nodes and use histogram-based median
        let projections: Vec<(f64, u32)> = nodes
            .iter()
            .map(|&node| {
                let (x, y) = coords[node as usize];
                let proj = (x - mean_x) * dir_x + (y - mean_y) * dir_y;
                (proj, node)
            })
            .collect();

        // Histogram-based median (O(n) instead of O(n log n))
        let (part_a, part_b) = histogram_partition(&projections);

        // Extract separator via greedy vertex cover of cross-edges
        let separator = self.extract_separator(csr, &part_a, &part_b);

        // Remove separator from partitions
        let sep_set: HashSet<u32> = separator.iter().copied().collect();
        let part_a: Vec<u32> = part_a.into_iter().filter(|n| !sep_set.contains(n)).collect();
        let part_b: Vec<u32> = part_b.into_iter().filter(|n| !sep_set.contains(n)).collect();

        Ok((part_a, part_b, separator))
    }

    fn extract_separator(
        &self,
        csr: &crate::formats::EbgCsr,
        part_a: &[u32],
        part_b: &[u32],
    ) -> Vec<u32> {
        let set_b: HashSet<u32> = part_b.iter().copied().collect();

        // Find cross-edges and their endpoints
        let mut cross_edges: Vec<(u32, u32)> = Vec::new();
        let mut ring: HashSet<u32> = HashSet::new();

        for &node in part_a {
            let start = csr.offsets[node as usize] as usize;
            let end = csr.offsets[node as usize + 1] as usize;
            for i in start..end {
                let neighbor = csr.heads[i];
                if set_b.contains(&neighbor) {
                    ring.insert(node);
                    ring.insert(neighbor);
                    cross_edges.push((node, neighbor));
                }
            }
        }

        if cross_edges.is_empty() {
            return vec![];
        }

        // Greedy minimum vertex cover
        let mut node_edges: HashMap<u32, Vec<usize>> = HashMap::new();
        for (idx, &(u, v)) in cross_edges.iter().enumerate() {
            node_edges.entry(u).or_default().push(idx);
            node_edges.entry(v).or_default().push(idx);
        }

        let mut ring_sorted: Vec<(u32, usize)> = node_edges
            .iter()
            .map(|(&node, edges)| (node, edges.len()))
            .collect();
        ring_sorted.sort_by_key(|(node, deg)| (std::cmp::Reverse(*deg), *node));

        let mut separator = Vec::new();
        let mut covered = vec![false; cross_edges.len()];
        let mut num_covered = 0;

        for (node, _) in ring_sorted {
            if num_covered == cross_edges.len() {
                break;
            }

            if let Some(edges) = node_edges.get(&node) {
                let mut covers_new = false;
                for &edge_idx in edges {
                    if !covered[edge_idx] {
                        covers_new = true;
                        break;
                    }
                }

                if covers_new {
                    separator.push(node);
                    for &edge_idx in edges {
                        if !covered[edge_idx] {
                            covered[edge_idx] = true;
                            num_covered += 1;
                        }
                    }
                }
            }
        }

        separator.sort_unstable();
        separator
    }

    fn minimum_degree_order(&self, csr: &crate::formats::EbgCsr, nodes: &[u32]) -> Vec<u32> {
        if nodes.is_empty() {
            return vec![];
        }

        // Build local adjacency with node-local IDs
        let n = nodes.len();
        let mut local_id: HashMap<u32, usize> = HashMap::with_capacity(n);
        let mut global_id: Vec<u32> = Vec::with_capacity(n);

        for (i, &node) in nodes.iter().enumerate() {
            local_id.insert(node, i);
            global_id.push(node);
        }

        // Build adjacency lists (only edges within the subgraph)
        let mut adj: Vec<HashSet<usize>> = vec![HashSet::new(); n];

        for &node in nodes {
            let u = local_id[&node];
            let start = csr.offsets[node as usize] as usize;
            let end = csr.offsets[node as usize + 1] as usize;

            for i in start..end {
                let neighbor = csr.heads[i];
                if let Some(&v) = local_id.get(&neighbor) {
                    if u != v {
                        adj[u].insert(v);
                        adj[v].insert(u); // Undirected for elimination
                    }
                }
            }
        }

        // Track degrees and eliminated status
        let mut degrees: Vec<usize> = adj.iter().map(|s| s.len()).collect();
        let mut eliminated = vec![false; n];
        let mut ordered = Vec::with_capacity(n);

        for _ in 0..n {
            // Find minimum degree node among remaining
            let mut min_deg = usize::MAX;
            let mut min_node = 0;

            for u in 0..n {
                if !eliminated[u] && (degrees[u] < min_deg || (degrees[u] == min_deg && global_id[u] < global_id[min_node])) {
                    min_deg = degrees[u];
                    min_node = u;
                }
            }

            // Eliminate this node
            eliminated[min_node] = true;
            ordered.push(global_id[min_node]);

            // Get neighbors to form clique
            let neighbors: Vec<usize> = adj[min_node]
                .iter()
                .filter(|&&v| !eliminated[v])
                .copied()
                .collect();

            // Add fill-in edges (form clique among remaining neighbors)
            for i in 0..neighbors.len() {
                for j in (i + 1)..neighbors.len() {
                    let u = neighbors[i];
                    let v = neighbors[j];

                    if !adj[u].contains(&v) {
                        adj[u].insert(v);
                        adj[v].insert(u);
                        degrees[u] += 1;
                        degrees[v] += 1;
                    }
                }
            }

            // Remove eliminated node from neighbors' adjacency
            for &v in &neighbors {
                adj[v].remove(&min_node);
                degrees[v] = degrees[v].saturating_sub(1);
            }
        }

        ordered
    }

    fn assign_rank(&mut self, node: u32) {
        self.perm[node as usize] = self.next_rank;
        self.inv_perm[self.next_rank as usize] = node;
        self.next_rank += 1;
    }

    fn finish(self) -> (Vec<u32>, Vec<u32>) {
        (self.perm, self.inv_perm)
    }
}

struct NdResult {
    ordering: Vec<u32>,
    depth: usize,
}

/// Compute principal direction from 2x2 covariance matrix
fn compute_principal_direction(cov_xx: f64, cov_xy: f64, cov_yy: f64) -> (f64, f64) {
    // Eigenvalue decomposition of [[cov_xx, cov_xy], [cov_xy, cov_yy]]
    let trace = cov_xx + cov_yy;
    let det = cov_xx * cov_yy - cov_xy * cov_xy;
    let discriminant = (trace * trace / 4.0 - det).max(0.0);
    let lambda1 = trace / 2.0 + discriminant.sqrt();

    // Eigenvector for largest eigenvalue
    if cov_xy.abs() > 1e-10 {
        let vx = lambda1 - cov_yy;
        let vy = cov_xy;
        let norm = (vx * vx + vy * vy).sqrt();
        if norm > 1e-10 {
            return (vx / norm, vy / norm);
        }
    }

    // Fallback to x-axis
    (1.0, 0.0)
}

/// Histogram-based partition (O(n) median finding)
fn histogram_partition(projections: &[(f64, u32)]) -> (Vec<u32>, Vec<u32>) {
    const NUM_BINS: usize = 4096;

    if projections.is_empty() {
        return (vec![], vec![]);
    }

    // Find range
    let (min_proj, max_proj) = projections.iter().fold(
        (f64::INFINITY, f64::NEG_INFINITY),
        |(min, max), &(proj, _)| (min.min(proj), max.max(proj)),
    );

    let range = (max_proj - min_proj).max(1e-9);

    // Build histogram
    let mut histogram: Vec<Vec<u32>> = vec![Vec::new(); NUM_BINS];
    for &(proj, node) in projections {
        let normalized = ((proj - min_proj) / range).clamp(0.0, 0.9999);
        let bin_idx = (normalized * NUM_BINS as f64) as usize;
        histogram[bin_idx].push(node);
    }

    // Sort within bins for determinism
    for bin in &mut histogram {
        bin.sort_unstable();
    }

    // Find median bin
    let target = projections.len() / 2;
    let mut cumulative = 0;
    let mut median_bin = 0;

    for (i, bin) in histogram.iter().enumerate() {
        cumulative += bin.len();
        if cumulative >= target {
            median_bin = i;
            break;
        }
    }

    // Collect partitions
    let mut part_a = Vec::new();
    let mut part_b = Vec::new();

    for (i, bin) in histogram.into_iter().enumerate() {
        if i < median_bin {
            part_a.extend(bin);
        } else if i > median_bin {
            part_b.extend(bin);
        } else {
            // Split median bin
            let needed_for_a = target.saturating_sub(part_a.len());
            part_a.extend(bin.iter().take(needed_for_a).copied());
            part_b.extend(bin.iter().skip(needed_for_a).copied());
        }
    }

    (part_a, part_b)
}

fn compute_inputs_sha(ebg_csr_path: &Path, ebg_nodes_path: &Path, nbg_geo_path: &Path) -> Result<[u8; 32]> {
    use sha2::{Digest, Sha256};

    let mut hasher = Sha256::new();
    hasher.update(&std::fs::read(ebg_csr_path)?);
    hasher.update(&std::fs::read(ebg_nodes_path)?);
    hasher.update(&std::fs::read(nbg_geo_path)?);

    let result = hasher.finalize();
    let mut sha = [0u8; 32];
    sha.copy_from_slice(&result);
    Ok(sha)
}
