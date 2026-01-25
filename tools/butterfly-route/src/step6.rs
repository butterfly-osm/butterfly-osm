///! Step 6: Nested Dissection ordering on per-mode filtered EBG
///!
///! Computes a high-quality elimination order on the mode-filtered Edge-Based Graph.
///! Each mode gets its own ordering computed on only the mode-accessible nodes.

use anyhow::Result;
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};

use crate::formats::{EbgCsrFile, EbgNodesFile, FilteredEbg, FilteredEbgFile, NbgGeoFile, OrderEbg, OrderEbgFile};
use crate::profile_abi::Mode;

/// Configuration for Step 6
pub struct Step6Config {
    pub filtered_ebg_path: PathBuf,
    pub ebg_nodes_path: PathBuf,
    pub nbg_geo_path: PathBuf,
    pub mode: Mode,
    pub outdir: PathBuf,
    pub leaf_threshold: usize,
    pub balance_eps: f32,
}

/// Result of Step 6 ordering
#[derive(Debug)]
pub struct Step6Result {
    pub order_path: PathBuf,
    pub mode: Mode,
    pub n_nodes: u32,
    pub n_components: usize,
    pub tree_depth: usize,
    pub build_time_ms: u64,
}

/// Generate nested dissection ordering on per-mode filtered EBG
pub fn generate_ordering(config: Step6Config) -> Result<Step6Result> {
    let start_time = std::time::Instant::now();
    let mode_name = match config.mode {
        Mode::Car => "car",
        Mode::Bike => "bike",
        Mode::Foot => "foot",
    };
    println!("\n📐 Step 6: Generating CCH ordering for {} mode...\n", mode_name);

    // Load filtered EBG
    println!("Loading filtered EBG ({})...", mode_name);
    let filtered_ebg = FilteredEbgFile::read(&config.filtered_ebg_path)?;
    println!("  ✓ {} filtered nodes (of {} original), {} arcs",
        filtered_ebg.n_filtered_nodes, filtered_ebg.n_original_nodes, filtered_ebg.n_filtered_arcs);

    // Load EBG nodes (for geometry linkage)
    println!("Loading EBG nodes...");
    let ebg_nodes = EbgNodesFile::read(&config.ebg_nodes_path)?;
    println!("  ✓ {} nodes", ebg_nodes.n_nodes);

    // Load NBG geo (for coordinates)
    println!("Loading NBG geo...");
    let nbg_geo = NbgGeoFile::read(&config.nbg_geo_path)?;
    println!("  ✓ {} edges", nbg_geo.n_edges_und);

    // Extract coordinates for filtered EBG nodes
    // Each filtered node maps back to an original EBG node
    println!("\nExtracting filtered EBG node coordinates...");
    let coords = extract_filtered_ebg_coordinates(&filtered_ebg, &ebg_nodes, &nbg_geo)?;
    println!("  ✓ {} coordinates", coords.len());

    // Find connected components on filtered graph
    println!("\nFinding connected components...");
    let components = find_filtered_components(&filtered_ebg)?;
    let n_components = components.len();
    println!("  ✓ {} components", n_components);
    for (i, comp) in components.iter().take(5).enumerate() {
        println!("    Component {}: {} nodes", i, comp.len());
    }
    if components.len() > 5 {
        println!("    ... and {} more small components", components.len() - 5);
    }

    // Build ordering via nested dissection on filtered space
    println!("\nBuilding nested dissection ordering...");
    let mut builder = NdBuilder::new(
        filtered_ebg.n_filtered_nodes as usize,
        config.leaf_threshold,
        config.balance_eps,
    );

    let mut max_depth = 0;
    for (comp_idx, component) in components.iter().enumerate() {
        if comp_idx % 100 == 0 && comp_idx > 0 {
            println!("  Processing component {} / {}...", comp_idx, components.len());
        }
        let depth = builder.order_component_filtered(&filtered_ebg, &coords, component)?;
        max_depth = max_depth.max(depth);
    }

    let (perm, inv_perm) = builder.finish();
    println!("  ✓ Generated ordering (max depth: {})", max_depth);

    // Compute inputs SHA
    let inputs_sha = compute_inputs_sha(
        &config.filtered_ebg_path,
        &config.ebg_nodes_path,
        &config.nbg_geo_path,
    )?;

    // Write output - use mode-specific filename
    std::fs::create_dir_all(&config.outdir)?;
    let order_path = config.outdir.join(format!("order.{}.ebg", mode_name));

    println!("\nWriting output...");
    let order = OrderEbg {
        n_nodes: filtered_ebg.n_filtered_nodes,
        inputs_sha,
        perm,
        inv_perm,
    };
    OrderEbgFile::write(&order_path, &order)?;
    println!("  ✓ Written {}", order_path.display());

    let build_time_ms = start_time.elapsed().as_millis() as u64;

    Ok(Step6Result {
        order_path,
        mode: config.mode,
        n_nodes: filtered_ebg.n_filtered_nodes,
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

/// Extract coordinates for filtered EBG nodes
///
/// Maps filtered node IDs back to original EBG nodes to get geometry.
fn extract_filtered_ebg_coordinates(
    filtered_ebg: &FilteredEbg,
    ebg_nodes: &crate::formats::EbgNodes,
    nbg_geo: &crate::formats::NbgGeo,
) -> Result<Vec<(f64, f64)>> {
    let mut coords = Vec::with_capacity(filtered_ebg.n_filtered_nodes as usize);
    let has_polylines = !nbg_geo.polylines.is_empty();

    for filtered_id in 0..filtered_ebg.n_filtered_nodes {
        // Map filtered node to original EBG node
        let original_id = filtered_ebg.filtered_to_original[filtered_id as usize] as usize;
        let node = &ebg_nodes.nodes[original_id];
        let geom_idx = node.geom_idx as usize;

        if geom_idx < nbg_geo.edges.len() {
            let edge = &nbg_geo.edges[geom_idx];

            // Try to get coordinates from polyline
            if has_polylines && geom_idx < nbg_geo.polylines.len() {
                let poly = &nbg_geo.polylines[geom_idx];
                if !poly.lat_fxp.is_empty() && !poly.lon_fxp.is_empty() {
                    let mid = poly.lat_fxp.len() / 2;
                    let lat = poly.lat_fxp[mid] as f64 * 1e-7;
                    let lon = poly.lon_fxp[mid] as f64 * 1e-7;
                    coords.push((lon, lat));
                    continue;
                }
            }

            // Fallback: use bearing and length
            let bearing_rad = (edge.bearing_deci_deg as f64 / 10.0).to_radians();
            let length_km = edge.length_mm as f64 / 1_000_000.0;
            let base_x = (geom_idx as f64) * 0.001;
            let base_y = (geom_idx as f64) * 0.001;
            let lon = base_x + bearing_rad.cos() * length_km * 0.01;
            let lat = base_y + bearing_rad.sin() * length_km * 0.01;
            coords.push((lon, lat));
        } else {
            let idx = coords.len() as f64;
            coords.push((idx * 0.0001, idx * 0.0001));
        }
    }

    Ok(coords)
}

/// Find connected components using BFS on SYMMETRIZED filtered EBG
///
/// For CCH ordering to work correctly on directed graphs, we need to find
/// weakly connected components (treating edges as undirected). This ensures
/// that nodes reachable in either direction are in the same component.
fn find_filtered_components(filtered_ebg: &FilteredEbg) -> Result<Vec<Vec<u32>>> {
    let n = filtered_ebg.n_filtered_nodes as usize;

    // Build reverse adjacency for symmetric traversal
    let mut reverse_adj: Vec<Vec<u32>> = vec![Vec::new(); n];
    for u in 0..n {
        let start_idx = filtered_ebg.offsets[u] as usize;
        let end_idx = filtered_ebg.offsets[u + 1] as usize;
        for i in start_idx..end_idx {
            let v = filtered_ebg.heads[i] as usize;
            if v < n {
                reverse_adj[v].push(u as u32);
            }
        }
    }

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

            // Follow forward edges
            let start_idx = filtered_ebg.offsets[u] as usize;
            let end_idx = filtered_ebg.offsets[u + 1] as usize;
            for i in start_idx..end_idx {
                let v = filtered_ebg.heads[i] as usize;
                if v < n && !visited[v] {
                    visited[v] = true;
                    queue.push_back(v);
                }
            }

            // Follow reverse edges (symmetric)
            for &v in &reverse_adj[u] {
                let v = v as usize;
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

    // ==========================================================================
    // Filtered EBG versions of the ND methods
    // ==========================================================================

    fn order_component_filtered(
        &mut self,
        filtered_ebg: &FilteredEbg,
        coords: &[(f64, f64)],
        component: &[u32],
    ) -> Result<usize> {
        if component.is_empty() {
            return Ok(0);
        }

        let result = self.recursive_nd_filtered(filtered_ebg, coords, component, 0)?;

        for &node in &result.ordering {
            self.assign_rank(node);
        }

        Ok(result.depth)
    }

    fn recursive_nd_filtered(
        &self,
        filtered_ebg: &FilteredEbg,
        coords: &[(f64, f64)],
        nodes: &[u32],
        depth: usize,
    ) -> Result<NdResult> {
        let n_sub = nodes.len();

        if n_sub <= self.leaf_threshold {
            let ordering = self.minimum_degree_order_filtered(filtered_ebg, nodes);
            return Ok(NdResult { ordering, depth });
        }

        let (part_a, part_b, separator) = self.inertial_partition_filtered(filtered_ebg, coords, nodes)?;

        let balance = part_a.len() as f32 / (part_a.len() + part_b.len()).max(1) as f32;

        if balance < 0.2 || balance > 0.8 {
            let ordering = self.minimum_degree_order_filtered(filtered_ebg, nodes);
            return Ok(NdResult { ordering, depth });
        }

        const PARALLEL_THRESHOLD: usize = 50_000;

        let (result_a, result_b) = if part_a.len() >= PARALLEL_THRESHOLD
            && part_b.len() >= PARALLEL_THRESHOLD
        {
            rayon::join(
                || self.recursive_nd_filtered(filtered_ebg, coords, &part_a, depth + 1),
                || self.recursive_nd_filtered(filtered_ebg, coords, &part_b, depth + 1),
            )
        } else {
            let a = self.recursive_nd_filtered(filtered_ebg, coords, &part_a, depth + 1)?;
            let b = self.recursive_nd_filtered(filtered_ebg, coords, &part_b, depth + 1)?;
            (Ok(a), Ok(b))
        };

        let result_a = result_a?;
        let result_b = result_b?;

        let mut ordering = result_a.ordering;
        ordering.extend(result_b.ordering);
        ordering.extend(separator);

        Ok(NdResult {
            ordering,
            depth: result_a.depth.max(result_b.depth),
        })
    }

    fn inertial_partition_filtered(
        &self,
        filtered_ebg: &FilteredEbg,
        coords: &[(f64, f64)],
        nodes: &[u32],
    ) -> Result<(Vec<u32>, Vec<u32>, Vec<u32>)> {
        if nodes.len() <= 2 {
            return Ok((vec![], vec![], nodes.to_vec()));
        }

        let mut mean_x = 0.0;
        let mut mean_y = 0.0;
        for &node in nodes {
            let (x, y) = coords[node as usize];
            mean_x += x;
            mean_y += y;
        }
        mean_x /= nodes.len() as f64;
        mean_y /= nodes.len() as f64;

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

        let (dir_x, dir_y) = compute_principal_direction(cov_xx, cov_xy, cov_yy);

        let projections: Vec<(f64, u32)> = nodes
            .iter()
            .map(|&node| {
                let (x, y) = coords[node as usize];
                let proj = (x - mean_x) * dir_x + (y - mean_y) * dir_y;
                (proj, node)
            })
            .collect();

        let (part_a, part_b) = histogram_partition(&projections);

        let separator = self.extract_separator_filtered(filtered_ebg, &part_a, &part_b);

        let sep_set: HashSet<u32> = separator.iter().copied().collect();
        let part_a: Vec<u32> = part_a.into_iter().filter(|n| !sep_set.contains(n)).collect();
        let part_b: Vec<u32> = part_b.into_iter().filter(|n| !sep_set.contains(n)).collect();

        Ok((part_a, part_b, separator))
    }

    fn extract_separator_filtered(
        &self,
        filtered_ebg: &FilteredEbg,
        part_a: &[u32],
        part_b: &[u32],
    ) -> Vec<u32> {
        let set_a: HashSet<u32> = part_a.iter().copied().collect();
        let set_b: HashSet<u32> = part_b.iter().copied().collect();

        let mut cross_edges: Vec<(u32, u32)> = Vec::new();
        let mut ring: HashSet<u32> = HashSet::new();

        // Check edges from part_a to part_b
        for &node in part_a {
            let start = filtered_ebg.offsets[node as usize] as usize;
            let end = filtered_ebg.offsets[node as usize + 1] as usize;
            for i in start..end {
                let neighbor = filtered_ebg.heads[i];
                if set_b.contains(&neighbor) {
                    ring.insert(node);
                    ring.insert(neighbor);
                    cross_edges.push((node, neighbor));
                }
            }
        }

        // Also check edges from part_b to part_a (symmetric for directed graphs)
        // This ensures the separator properly disconnects both directions
        for &node in part_b {
            let start = filtered_ebg.offsets[node as usize] as usize;
            let end = filtered_ebg.offsets[node as usize + 1] as usize;
            for i in start..end {
                let neighbor = filtered_ebg.heads[i];
                if set_a.contains(&neighbor) {
                    ring.insert(node);
                    ring.insert(neighbor);
                    cross_edges.push((node, neighbor));
                }
            }
        }

        if cross_edges.is_empty() {
            return vec![];
        }

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

    fn minimum_degree_order_filtered(&self, filtered_ebg: &FilteredEbg, nodes: &[u32]) -> Vec<u32> {
        if nodes.is_empty() {
            return vec![];
        }

        let n = nodes.len();
        let mut local_id: HashMap<u32, usize> = HashMap::with_capacity(n);
        let mut global_id: Vec<u32> = Vec::with_capacity(n);

        for (i, &node) in nodes.iter().enumerate() {
            local_id.insert(node, i);
            global_id.push(node);
        }

        let mut adj: Vec<HashSet<usize>> = vec![HashSet::new(); n];

        for &node in nodes {
            let u = local_id[&node];
            let start = filtered_ebg.offsets[node as usize] as usize;
            let end = filtered_ebg.offsets[node as usize + 1] as usize;

            for i in start..end {
                let neighbor = filtered_ebg.heads[i];
                if let Some(&v) = local_id.get(&neighbor) {
                    if u != v {
                        adj[u].insert(v);
                        adj[v].insert(u);
                    }
                }
            }
        }

        let mut degrees: Vec<usize> = adj.iter().map(|s| s.len()).collect();
        let mut eliminated = vec![false; n];
        let mut ordered = Vec::with_capacity(n);

        for _ in 0..n {
            let mut min_deg = usize::MAX;
            let mut min_node = 0;

            for u in 0..n {
                if !eliminated[u] && (degrees[u] < min_deg || (degrees[u] == min_deg && global_id[u] < global_id[min_node])) {
                    min_deg = degrees[u];
                    min_node = u;
                }
            }

            eliminated[min_node] = true;
            ordered.push(global_id[min_node]);

            let neighbors: Vec<usize> = adj[min_node]
                .iter()
                .filter(|&&v| !eliminated[v])
                .copied()
                .collect();

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

            for &v in &neighbors {
                adj[v].remove(&min_node);
                degrees[v] = degrees[v].saturating_sub(1);
            }
        }

        ordered
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

// ==========================================================================
// Hybrid State Graph Ordering
// ==========================================================================

use crate::formats::hybrid_state::HybridState;

/// Configuration for Step 6 with hybrid state graph
pub struct Step6HybridConfig {
    pub hybrid_state_path: PathBuf,
    pub nbg_geo_path: PathBuf,
    pub mode: Mode,
    pub outdir: PathBuf,
    pub leaf_threshold: usize,
    pub balance_eps: f32,
    /// Use graph-based partitioning instead of geometry-based
    /// Set to true when coordinate-based ND fails (e.g., equivalence-class hybrid)
    pub use_graph_partition: bool,
}

/// Generate nested dissection ordering on hybrid state graph
pub fn generate_ordering_hybrid(config: Step6HybridConfig) -> Result<Step6Result> {
    use crate::formats::HybridStateFile;

    let start_time = std::time::Instant::now();
    let mode_name = match config.mode {
        Mode::Car => "car",
        Mode::Bike => "bike",
        Mode::Foot => "foot",
    };
    println!("\n📐 Step 6: Generating CCH ordering for {} mode (HYBRID)...\n", mode_name);

    // Load hybrid state graph
    println!("Loading hybrid state graph ({})...", mode_name);
    let hybrid = HybridStateFile::read(&config.hybrid_state_path)?;
    println!("  ✓ {} hybrid states ({} node-states, {} edge-states), {} arcs",
        hybrid.n_states, hybrid.n_node_states, hybrid.n_edge_states, hybrid.n_arcs);

    // Load NBG geo (for coordinates)
    println!("Loading NBG geo...");
    let nbg_geo = NbgGeoFile::read(&config.nbg_geo_path)?;
    println!("  ✓ {} edges", nbg_geo.n_edges_und);

    // Extract coordinates for hybrid states
    println!("\nExtracting hybrid state coordinates...");
    let coords = extract_hybrid_coordinates(&hybrid, &nbg_geo)?;
    println!("  ✓ {} coordinates", coords.len());

    // Find connected components on hybrid graph
    println!("\nFinding connected components...");
    let components = find_hybrid_components(&hybrid)?;
    let n_components = components.len();
    println!("  ✓ {} components", n_components);
    for (i, comp) in components.iter().take(5).enumerate() {
        println!("    Component {}: {} states", i, comp.len());
    }
    if components.len() > 5 {
        println!("    ... and {} more small components", components.len() - 5);
    }

    // Build ordering via nested dissection
    let ordering_method = if config.use_graph_partition {
        "GRAPH-BASED (BFS bisection)"
    } else {
        "GEOMETRY-BASED (inertial partitioning)"
    };
    println!("\nBuilding nested dissection ordering ({})...", ordering_method);
    let mut builder = NdBuilder::new(
        hybrid.n_states as usize,
        config.leaf_threshold,
        config.balance_eps,
    );

    let mut max_depth = 0;
    for (comp_idx, component) in components.iter().enumerate() {
        if comp_idx % 100 == 0 && comp_idx > 0 {
            println!("  Processing component {} / {}...", comp_idx, components.len());
        }
        let depth = if config.use_graph_partition {
            builder.order_component_hybrid_graph(&hybrid, component)?
        } else {
            builder.order_component_hybrid(&hybrid, &coords, component)?
        };
        max_depth = max_depth.max(depth);
    }

    let (perm, inv_perm) = builder.finish();
    println!("  ✓ Generated ordering (max depth: {})", max_depth);

    // Compute inputs SHA
    let inputs_sha = compute_inputs_sha_hybrid(&config.hybrid_state_path, &config.nbg_geo_path)?;

    // Write output
    std::fs::create_dir_all(&config.outdir)?;
    let order_path = config.outdir.join(format!("order.hybrid.{}.ebg", mode_name));

    println!("\nWriting output...");
    let order = OrderEbg {
        n_nodes: hybrid.n_states,
        inputs_sha,
        perm,
        inv_perm,
    };
    OrderEbgFile::write(&order_path, &order)?;
    println!("  ✓ Written {}", order_path.display());

    let build_time_ms = start_time.elapsed().as_millis() as u64;

    Ok(Step6Result {
        order_path,
        mode: config.mode,
        n_nodes: hybrid.n_states,
        n_components,
        tree_depth: max_depth,
        build_time_ms,
    })
}

/// Extract coordinates for hybrid states
///
/// - Node-states: Use NBG node coordinates from edge geometry
/// - Edge-states: Use head NBG node coordinates (where edge arrives)
fn extract_hybrid_coordinates(hybrid: &HybridState, nbg_geo: &crate::formats::NbgGeo) -> Result<Vec<(f64, f64)>> {
    use std::collections::HashMap;

    // Build map from NBG node ID to coordinate
    // We use the first point of each edge's polyline for u_node,
    // and the last point for v_node
    let mut nbg_node_coords: HashMap<u32, (f64, f64)> = HashMap::new();

    for (edge_idx, edge) in nbg_geo.edges.iter().enumerate() {
        if edge_idx < nbg_geo.polylines.len() {
            let poly = &nbg_geo.polylines[edge_idx];
            if !poly.lat_fxp.is_empty() && !poly.lon_fxp.is_empty() {
                // Use first point for u_node (source)
                if !nbg_node_coords.contains_key(&edge.u_node) {
                    let lat = poly.lat_fxp[0] as f64 * 1e-7;
                    let lon = poly.lon_fxp[0] as f64 * 1e-7;
                    nbg_node_coords.insert(edge.u_node, (lon, lat));
                }
                // Use last point for v_node (target)
                if !nbg_node_coords.contains_key(&edge.v_node) {
                    let last = poly.lat_fxp.len() - 1;
                    let lat = poly.lat_fxp[last] as f64 * 1e-7;
                    let lon = poly.lon_fxp[last] as f64 * 1e-7;
                    nbg_node_coords.insert(edge.v_node, (lon, lat));
                }
            }
        }
    }

    println!("  Built coordinate map for {} NBG nodes", nbg_node_coords.len());

    // Now extract coordinates for each hybrid state
    let mut coords = Vec::with_capacity(hybrid.n_states as usize);
    let mut found_count = 0usize;
    let mut fallback_count = 0usize;

    for state in 0..hybrid.n_states {
        // Get NBG node for this state
        let nbg_node = hybrid.state_to_nbg(state);

        if let Some(&(lon, lat)) = nbg_node_coords.get(&nbg_node) {
            coords.push((lon, lat));
            found_count += 1;
        } else {
            // Fallback: use state index as pseudo-coordinate
            // This preserves some locality since states are numbered sequentially
            let idx = state as f64;
            coords.push((idx * 0.0001, idx * 0.0001));
            fallback_count += 1;
        }
    }

    if fallback_count > 0 {
        println!("  WARNING: {} states used fallback coordinates ({:.2}%)",
            fallback_count, 100.0 * fallback_count as f64 / hybrid.n_states as f64);
    }
    println!("  Found coordinates for {} states ({:.2}%)",
        found_count, 100.0 * found_count as f64 / hybrid.n_states as f64);

    Ok(coords)
}

/// Find connected components in hybrid graph using BFS on symmetrized graph
fn find_hybrid_components(hybrid: &HybridState) -> Result<Vec<Vec<u32>>> {
    let n = hybrid.n_states as usize;

    // Build reverse adjacency for symmetric traversal
    let mut reverse_adj: Vec<Vec<u32>> = vec![Vec::new(); n];
    for u in 0..n {
        let start_idx = hybrid.offsets[u] as usize;
        let end_idx = hybrid.offsets[u + 1] as usize;
        for i in start_idx..end_idx {
            let v = hybrid.targets[i] as usize;
            if v < n {
                reverse_adj[v].push(u as u32);
            }
        }
    }

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

            // Follow forward edges
            let start_idx = hybrid.offsets[u] as usize;
            let end_idx = hybrid.offsets[u + 1] as usize;
            for i in start_idx..end_idx {
                let v = hybrid.targets[i] as usize;
                if v < n && !visited[v] {
                    visited[v] = true;
                    queue.push_back(v);
                }
            }

            // Follow reverse edges (symmetric)
            for &v in &reverse_adj[u] {
                let v = v as usize;
                if !visited[v] {
                    visited[v] = true;
                    queue.push_back(v);
                }
            }
        }

        components.push(component);
    }

    // Sort by size descending
    components.sort_by(|a, b| {
        b.len()
            .cmp(&a.len())
            .then_with(|| a.iter().min().cmp(&b.iter().min()))
    });

    Ok(components)
}

fn compute_inputs_sha_hybrid(hybrid_path: &Path, nbg_geo_path: &Path) -> Result<[u8; 32]> {
    use sha2::{Digest, Sha256};

    let mut hasher = Sha256::new();
    hasher.update(&std::fs::read(hybrid_path)?);
    hasher.update(&std::fs::read(nbg_geo_path)?);

    let result = hasher.finalize();
    let mut sha = [0u8; 32];
    sha.copy_from_slice(&result);
    Ok(sha)
}

impl NdBuilder {
    // ==========================================================================
    // Hybrid State Graph versions of the ND methods
    // ==========================================================================

    fn order_component_hybrid(
        &mut self,
        hybrid: &HybridState,
        coords: &[(f64, f64)],
        component: &[u32],
    ) -> Result<usize> {
        if component.is_empty() {
            return Ok(0);
        }

        let result = self.recursive_nd_hybrid(hybrid, coords, component, 0)?;

        for &node in &result.ordering {
            self.assign_rank(node);
        }

        Ok(result.depth)
    }

    fn recursive_nd_hybrid(
        &self,
        hybrid: &HybridState,
        coords: &[(f64, f64)],
        nodes: &[u32],
        depth: usize,
    ) -> Result<NdResult> {
        let n_sub = nodes.len();

        if n_sub <= self.leaf_threshold {
            let ordering = self.minimum_degree_order_hybrid(hybrid, nodes);
            return Ok(NdResult { ordering, depth });
        }

        let (part_a, part_b, separator) = self.inertial_partition_hybrid(hybrid, coords, nodes)?;

        let balance = part_a.len() as f32 / (part_a.len() + part_b.len()).max(1) as f32;

        if balance < 0.2 || balance > 0.8 {
            let ordering = self.minimum_degree_order_hybrid(hybrid, nodes);
            return Ok(NdResult { ordering, depth });
        }

        const PARALLEL_THRESHOLD: usize = 50_000;

        let (result_a, result_b) = if part_a.len() >= PARALLEL_THRESHOLD
            && part_b.len() >= PARALLEL_THRESHOLD
        {
            rayon::join(
                || self.recursive_nd_hybrid(hybrid, coords, &part_a, depth + 1),
                || self.recursive_nd_hybrid(hybrid, coords, &part_b, depth + 1),
            )
        } else {
            let a = self.recursive_nd_hybrid(hybrid, coords, &part_a, depth + 1)?;
            let b = self.recursive_nd_hybrid(hybrid, coords, &part_b, depth + 1)?;
            (Ok(a), Ok(b))
        };

        let result_a = result_a?;
        let result_b = result_b?;

        let mut ordering = result_a.ordering;
        ordering.extend(result_b.ordering);
        ordering.extend(separator);

        Ok(NdResult {
            ordering,
            depth: result_a.depth.max(result_b.depth),
        })
    }

    fn inertial_partition_hybrid(
        &self,
        hybrid: &HybridState,
        coords: &[(f64, f64)],
        nodes: &[u32],
    ) -> Result<(Vec<u32>, Vec<u32>, Vec<u32>)> {
        if nodes.len() <= 2 {
            return Ok((vec![], vec![], nodes.to_vec()));
        }

        let mut mean_x = 0.0;
        let mut mean_y = 0.0;
        for &node in nodes {
            let (x, y) = coords[node as usize];
            mean_x += x;
            mean_y += y;
        }
        mean_x /= nodes.len() as f64;
        mean_y /= nodes.len() as f64;

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

        let (dir_x, dir_y) = compute_principal_direction(cov_xx, cov_xy, cov_yy);

        let projections: Vec<(f64, u32)> = nodes
            .iter()
            .map(|&node| {
                let (x, y) = coords[node as usize];
                let proj = (x - mean_x) * dir_x + (y - mean_y) * dir_y;
                (proj, node)
            })
            .collect();

        let (part_a, part_b) = histogram_partition(&projections);

        let separator = self.extract_separator_hybrid(hybrid, &part_a, &part_b);

        let sep_set: HashSet<u32> = separator.iter().copied().collect();
        let part_a: Vec<u32> = part_a.into_iter().filter(|n| !sep_set.contains(n)).collect();
        let part_b: Vec<u32> = part_b.into_iter().filter(|n| !sep_set.contains(n)).collect();

        Ok((part_a, part_b, separator))
    }

    fn extract_separator_hybrid(
        &self,
        hybrid: &HybridState,
        part_a: &[u32],
        part_b: &[u32],
    ) -> Vec<u32> {
        let set_b: HashSet<u32> = part_b.iter().copied().collect();

        let mut cross_edges: Vec<(u32, u32)> = Vec::new();

        for &node in part_a {
            let start = hybrid.offsets[node as usize] as usize;
            let end = hybrid.offsets[node as usize + 1] as usize;
            for i in start..end {
                let neighbor = hybrid.targets[i];
                if set_b.contains(&neighbor) {
                    cross_edges.push((node, neighbor));
                }
            }
        }

        if cross_edges.is_empty() {
            return vec![];
        }

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

    /// Graph-based partition using BFS bisection (no coordinates needed)
    /// This is for graphs where coordinate-based ND fails (e.g., equivalence-class hybrid)
    fn graph_partition_hybrid(
        &self,
        hybrid: &HybridState,
        nodes: &[u32],
    ) -> Result<(Vec<u32>, Vec<u32>, Vec<u32>)> {
        if nodes.len() <= 2 {
            return Ok((vec![], vec![], nodes.to_vec()));
        }

        let n = nodes.len();

        // Build local adjacency (undirected)
        let mut local_id: HashMap<u32, usize> = HashMap::with_capacity(n);
        for (i, &node) in nodes.iter().enumerate() {
            local_id.insert(node, i);
        }

        let mut adj: Vec<Vec<usize>> = vec![Vec::new(); n];
        for &node in nodes {
            let u = local_id[&node];
            let start = hybrid.offsets[node as usize] as usize;
            let end = hybrid.offsets[node as usize + 1] as usize;

            for i in start..end {
                let neighbor = hybrid.targets[i];
                if let Some(&v) = local_id.get(&neighbor) {
                    if u != v && !adj[u].contains(&v) {
                        adj[u].push(v);
                        adj[v].push(u);
                    }
                }
            }
        }

        // Step 1: Find peripheral node using BFS (pseudo-diameter heuristic)
        let seed1 = self.find_peripheral_node(&adj, 0);
        let seed2 = self.find_peripheral_node(&adj, seed1);

        // Step 2: Bidirectional BFS from two seeds to partition nodes
        let mut dist1 = vec![u32::MAX; n];
        let mut dist2 = vec![u32::MAX; n];

        // BFS from seed1
        let mut queue: VecDeque<usize> = VecDeque::new();
        queue.push_back(seed1);
        dist1[seed1] = 0;
        while let Some(u) = queue.pop_front() {
            for &v in &adj[u] {
                if dist1[v] == u32::MAX {
                    dist1[v] = dist1[u] + 1;
                    queue.push_back(v);
                }
            }
        }

        // BFS from seed2
        queue.push_back(seed2);
        dist2[seed2] = 0;
        while let Some(u) = queue.pop_front() {
            for &v in &adj[u] {
                if dist2[v] == u32::MAX {
                    dist2[v] = dist2[u] + 1;
                    queue.push_back(v);
                }
            }
        }

        // Step 3: Assign nodes to partitions based on which seed is closer
        let mut part_a_local: Vec<usize> = Vec::with_capacity(n / 2);
        let mut part_b_local: Vec<usize> = Vec::with_capacity(n / 2);

        for u in 0..n {
            // Handle disconnected nodes
            if dist1[u] == u32::MAX && dist2[u] == u32::MAX {
                // Assign to smaller partition for balance
                if part_a_local.len() <= part_b_local.len() {
                    part_a_local.push(u);
                } else {
                    part_b_local.push(u);
                }
            } else if dist1[u] == u32::MAX {
                part_b_local.push(u);
            } else if dist2[u] == u32::MAX {
                part_a_local.push(u);
            } else if dist1[u] < dist2[u] {
                part_a_local.push(u);
            } else if dist2[u] < dist1[u] {
                part_b_local.push(u);
            } else {
                // Tie: assign to smaller partition for balance
                if part_a_local.len() <= part_b_local.len() {
                    part_a_local.push(u);
                } else {
                    part_b_local.push(u);
                }
            }
        }

        // Step 4: Extract separator (boundary nodes with edges to both partitions)
        let mut in_a = vec![false; n];
        let mut in_b = vec![false; n];
        for &u in &part_a_local {
            in_a[u] = true;
        }
        for &u in &part_b_local {
            in_b[u] = true;
        }

        let mut separator_local: Vec<usize> = Vec::new();
        let mut final_a: Vec<usize> = Vec::new();
        let mut final_b: Vec<usize> = Vec::new();

        for &u in &part_a_local {
            let has_neighbor_in_b = adj[u].iter().any(|&v| in_b[v]);
            if has_neighbor_in_b {
                separator_local.push(u);
            } else {
                final_a.push(u);
            }
        }

        for &u in &part_b_local {
            let has_neighbor_in_a = adj[u].iter().any(|&v| in_a[v]);
            if has_neighbor_in_a {
                separator_local.push(u);
            } else {
                final_b.push(u);
            }
        }

        // Convert back to global IDs
        let part_a: Vec<u32> = final_a.iter().map(|&u| nodes[u]).collect();
        let part_b: Vec<u32> = final_b.iter().map(|&u| nodes[u]).collect();
        let separator: Vec<u32> = separator_local.iter().map(|&u| nodes[u]).collect();

        Ok((part_a, part_b, separator))
    }

    /// Find a peripheral node using BFS (farthest from start)
    fn find_peripheral_node(&self, adj: &[Vec<usize>], start: usize) -> usize {
        let n = adj.len();
        let mut dist = vec![u32::MAX; n];
        let mut queue: VecDeque<usize> = VecDeque::new();

        queue.push_back(start);
        dist[start] = 0;
        let mut farthest = start;
        let mut max_dist = 0;

        while let Some(u) = queue.pop_front() {
            if dist[u] > max_dist {
                max_dist = dist[u];
                farthest = u;
            }
            for &v in &adj[u] {
                if dist[v] == u32::MAX {
                    dist[v] = dist[u] + 1;
                    queue.push_back(v);
                }
            }
        }

        farthest
    }

    /// Recursive ND using graph-based partitioning (no coordinates)
    fn recursive_nd_hybrid_graph(
        &self,
        hybrid: &HybridState,
        nodes: &[u32],
        depth: usize,
    ) -> Result<NdResult> {
        let n_sub = nodes.len();

        if n_sub <= self.leaf_threshold {
            let ordering = self.minimum_degree_order_hybrid(hybrid, nodes);
            return Ok(NdResult { ordering, depth });
        }

        let (part_a, part_b, separator) = self.graph_partition_hybrid(hybrid, nodes)?;

        let balance = part_a.len() as f32 / (part_a.len() + part_b.len()).max(1) as f32;

        // If partition is too unbalanced, fall back to minimum degree
        if balance < 0.1 || balance > 0.9 || part_a.is_empty() || part_b.is_empty() {
            let ordering = self.minimum_degree_order_hybrid(hybrid, nodes);
            return Ok(NdResult { ordering, depth });
        }

        const PARALLEL_THRESHOLD: usize = 50_000;

        let (result_a, result_b) = if part_a.len() >= PARALLEL_THRESHOLD
            && part_b.len() >= PARALLEL_THRESHOLD
        {
            rayon::join(
                || self.recursive_nd_hybrid_graph(hybrid, &part_a, depth + 1),
                || self.recursive_nd_hybrid_graph(hybrid, &part_b, depth + 1),
            )
        } else {
            let a = self.recursive_nd_hybrid_graph(hybrid, &part_a, depth + 1)?;
            let b = self.recursive_nd_hybrid_graph(hybrid, &part_b, depth + 1)?;
            (Ok(a), Ok(b))
        };

        let result_a = result_a?;
        let result_b = result_b?;

        let mut ordering = result_a.ordering;
        ordering.extend(result_b.ordering);
        ordering.extend(separator);

        Ok(NdResult {
            ordering,
            depth: result_a.depth.max(result_b.depth),
        })
    }

    /// Order component using graph-based ND (no coordinates)
    fn order_component_hybrid_graph(
        &mut self,
        hybrid: &HybridState,
        component: &[u32],
    ) -> Result<usize> {
        if component.is_empty() {
            return Ok(0);
        }

        let result = self.recursive_nd_hybrid_graph(hybrid, component, 0)?;

        for &node in &result.ordering {
            self.assign_rank(node);
        }

        Ok(result.depth)
    }

    fn minimum_degree_order_hybrid(&self, hybrid: &HybridState, nodes: &[u32]) -> Vec<u32> {
        if nodes.is_empty() {
            return vec![];
        }

        let n = nodes.len();
        let mut local_id: HashMap<u32, usize> = HashMap::with_capacity(n);
        let mut global_id: Vec<u32> = Vec::with_capacity(n);

        for (i, &node) in nodes.iter().enumerate() {
            local_id.insert(node, i);
            global_id.push(node);
        }

        let mut adj: Vec<HashSet<usize>> = vec![HashSet::new(); n];

        for &node in nodes {
            let u = local_id[&node];
            let start = hybrid.offsets[node as usize] as usize;
            let end = hybrid.offsets[node as usize + 1] as usize;

            for i in start..end {
                let neighbor = hybrid.targets[i];
                if let Some(&v) = local_id.get(&neighbor) {
                    if u != v {
                        adj[u].insert(v);
                        adj[v].insert(u);
                    }
                }
            }
        }

        let mut degrees: Vec<usize> = adj.iter().map(|s| s.len()).collect();
        let mut eliminated = vec![false; n];
        let mut ordered = Vec::with_capacity(n);

        for _ in 0..n {
            let mut min_deg = usize::MAX;
            let mut min_node = 0;

            for u in 0..n {
                if !eliminated[u] && (degrees[u] < min_deg || (degrees[u] == min_deg && global_id[u] < global_id[min_node])) {
                    min_deg = degrees[u];
                    min_node = u;
                }
            }

            eliminated[min_node] = true;
            ordered.push(global_id[min_node]);

            let neighbors: Vec<usize> = adj[min_node]
                .iter()
                .filter(|&&v| !eliminated[v])
                .copied()
                .collect();

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

            for &v in &neighbors {
                adj[v].remove(&min_node);
                degrees[v] = degrees[v].saturating_sub(1);
            }
        }

        ordered
    }
}
