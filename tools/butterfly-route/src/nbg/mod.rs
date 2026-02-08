//! Step 3: Node-based graph (NBG) construction

use anyhow::Result;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use crate::formats::{
    NbgCsr, NbgCsrFile, NbgEdge, NbgGeo, NbgGeoFile, NbgNodeMap, NbgNodeMapFile, NodeMapping,
    PolyLine, WaysFile,
};

pub struct NbgConfig {
    pub nodes_sa_path: PathBuf,
    pub ways_path: PathBuf,
    pub way_attrs_car_path: PathBuf,
    pub way_attrs_bike_path: PathBuf,
    pub way_attrs_foot_path: PathBuf,
    pub outdir: PathBuf,
}

pub struct NbgResult {
    pub csr_path: PathBuf,
    pub geo_path: PathBuf,
    pub node_map_path: PathBuf,
    pub n_nodes: u32,
    pub n_edges_und: u64,
}

const EARTH_RADIUS_M: f64 = 6_371_008.8;

/// Compute haversine distance between two points in meters
pub fn haversine_distance(lat1_deg: f64, lon1_deg: f64, lat2_deg: f64, lon2_deg: f64) -> f64 {
    let lat1 = lat1_deg.to_radians();
    let lat2 = lat2_deg.to_radians();
    let delta_lat = (lat2_deg - lat1_deg).to_radians();
    let delta_lon = (lon2_deg - lon1_deg).to_radians();

    let a =
        (delta_lat / 2.0).sin().powi(2) + lat1.cos() * lat2.cos() * (delta_lon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());

    EARTH_RADIUS_M * c
}

/// Compute bearing from point 1 to point 2 in deci-degrees (0-3599)
pub fn compute_bearing(lat1_deg: f64, lon1_deg: f64, lat2_deg: f64, lon2_deg: f64) -> u16 {
    let lat1 = lat1_deg.to_radians();
    let lat2 = lat2_deg.to_radians();
    let delta_lon = (lon2_deg - lon1_deg).to_radians();

    let y = delta_lon.sin() * lat2.cos();
    let x = lat1.cos() * lat2.sin() - lat1.sin() * lat2.cos() * delta_lon.cos();
    let bearing_rad = y.atan2(x);
    let bearing_deg = bearing_rad.to_degrees();
    let normalized = (bearing_deg + 360.0) % 360.0;
    let deci_deg = (normalized * 10.0).round() as u16;
    deci_deg.min(3599)
}

/// Check if a way has access in any mode
fn has_any_access(way_attrs_car: &[u8], way_attrs_bike: &[u8], way_attrs_foot: &[u8]) -> bool {
    // way_attrs record format: way_id(8) + access_fwd(1) + access_rev(1) + ...
    // Check if any mode has access_fwd or access_rev
    let car_fwd = way_attrs_car.get(8).copied().unwrap_or(0) != 0;
    let car_rev = way_attrs_car.get(9).copied().unwrap_or(0) != 0;
    let bike_fwd = way_attrs_bike.get(8).copied().unwrap_or(0) != 0;
    let bike_rev = way_attrs_bike.get(9).copied().unwrap_or(0) != 0;
    let foot_fwd = way_attrs_foot.get(8).copied().unwrap_or(0) != 0;
    let foot_rev = way_attrs_foot.get(9).copied().unwrap_or(0) != 0;

    car_fwd || car_rev || bike_fwd || bike_rev || foot_fwd || foot_rev
}

/// Build NBG from Step 1 and Step 2 outputs
pub fn build_nbg(config: NbgConfig) -> Result<NbgResult> {
    use std::time::Instant;

    let start_time = Instant::now();

    println!("🦋 Starting Step 3: Node-Based Graph Construction");
    println!("📂 nodes.sa: {}", config.nodes_sa_path.display());
    println!("📂 ways.raw: {}", config.ways_path.display());
    println!("📂 Output: {}", config.outdir.display());
    println!();

    std::fs::create_dir_all(&config.outdir)?;

    // Step 1: Load way_attrs to determine included ways
    println!("Loading way_attrs to determine included ways...");
    let way_attrs_car = load_way_attrs_index(&config.way_attrs_car_path)?;
    let way_attrs_bike = load_way_attrs_index(&config.way_attrs_bike_path)?;
    let way_attrs_foot = load_way_attrs_index(&config.way_attrs_foot_path)?;
    println!("  ✓ Loaded way_attrs indices");

    // Step 2: Load nodes.sa for coordinate lookup
    println!("Loading nodes.sa...");
    let node_coords = load_node_coordinates(&config.nodes_sa_path)?;
    println!("  ✓ Loaded {} node coordinates", node_coords.len());

    // Step 3: Stream ways and collect decision nodes
    println!("Streaming ways to collect decision nodes...");
    let (decision_nodes, included_ways) = collect_decision_nodes(
        &config.ways_path,
        &way_attrs_car,
        &way_attrs_bike,
        &way_attrs_foot,
    )?;
    println!("  ✓ Found {} decision nodes", decision_nodes.len());
    println!("  ✓ Found {} included ways", included_ways.len());

    // Step 4: Build node map (OSM ID -> compact ID)
    println!("Building node map...");
    let node_map = build_node_map(&decision_nodes)?;
    println!("  ✓ Assigned {} compact node IDs", node_map.mappings.len());

    // Create lookup for compact IDs
    let osm_to_compact: HashMap<i64, u32> = node_map
        .mappings
        .iter()
        .map(|m| (m.osm_node_id, m.compact_id))
        .collect();

    // Step 5: Emit edges
    println!("Emitting edges...");
    let (edges, adjacency) = emit_edges(
        &config.ways_path,
        &included_ways,
        &osm_to_compact,
        &node_coords,
    )?;
    println!("  ✓ Emitted {} undirected edges", edges.len());

    // Step 6: Assemble CSR
    println!("Assembling CSR...");
    let csr = assemble_csr(
        &adjacency,
        node_map.mappings.len() as u32,
        edges.len() as u64,
    )?;
    println!("  ✓ CSR assembled");

    // Step 7: Write outputs
    println!();
    println!("Writing output files...");

    let node_map_path = config.outdir.join("nbg.node_map");
    NbgNodeMapFile::write(&node_map_path, &node_map)?;
    println!("  ✓ Wrote {}", node_map_path.display());

    let geo = build_geo_structure(edges)?;
    let geo_path = config.outdir.join("nbg.geo");
    NbgGeoFile::write(&geo_path, &geo)?;
    println!("  ✓ Wrote {}", geo_path.display());

    let csr_path = config.outdir.join("nbg.csr");
    NbgCsrFile::write(&csr_path, &csr)?;
    println!("  ✓ Wrote {}", csr_path.display());

    let elapsed = start_time.elapsed();

    println!();
    println!("✅ NBG construction complete!");
    println!("  Nodes: {}", csr.n_nodes);
    println!("  Edges: {}", csr.n_edges_und);
    println!("  Time: {:.2}s", elapsed.as_secs_f64());

    Ok(NbgResult {
        csr_path,
        geo_path,
        node_map_path,
        n_nodes: csr.n_nodes,
        n_edges_und: csr.n_edges_und,
    })
}

fn load_way_attrs_index(path: &PathBuf) -> Result<HashMap<i64, Vec<u8>>> {
    use std::fs::File;
    use std::io::Read;

    let mut file = File::open(path)?;
    let mut header = vec![0u8; 80];
    file.read_exact(&mut header)?;

    let count = u64::from_le_bytes(header[8..16].try_into()?);
    let mut index = HashMap::new();

    for _ in 0..count {
        let mut record = vec![0u8; 32];
        file.read_exact(&mut record)?;

        let way_id = i64::from_le_bytes(record[0..8].try_into()?);
        index.insert(way_id, record);
    }

    Ok(index)
}

fn load_node_coordinates(path: &PathBuf) -> Result<HashMap<i64, (f64, f64)>> {
    use std::fs::File;
    use std::io::Read;

    let mut file = File::open(path)?;
    let mut header = vec![0u8; 128];
    file.read_exact(&mut header)?;

    let count = u64::from_le_bytes(header[8..16].try_into()?);
    let mut coords = HashMap::new();

    for _ in 0..count {
        let mut record = [0u8; 16];
        file.read_exact(&mut record)?;

        let node_id = i64::from_le_bytes(record[0..8].try_into()?);
        let lat_lon = u64::from_le_bytes(record[8..16].try_into()?);

        // Decode lat/lon from packed format (1e-7 degrees)
        // In little-endian: lower 32 bits are lat_fxp (bytes 8-11), upper 32 bits are lon_fxp (bytes 12-15)
        let lat_fxp = (lat_lon & 0xFFFFFFFF) as i32;
        let lon_fxp = (lat_lon >> 32) as i32;
        let lat = lat_fxp as f64 * 1e-7;
        let lon = lon_fxp as f64 * 1e-7;

        coords.insert(node_id, (lat, lon));
    }

    Ok(coords)
}

fn collect_decision_nodes(
    ways_path: &PathBuf,
    way_attrs_car: &HashMap<i64, Vec<u8>>,
    way_attrs_bike: &HashMap<i64, Vec<u8>>,
    way_attrs_foot: &HashMap<i64, Vec<u8>>,
) -> Result<(HashSet<i64>, HashSet<i64>)> {
    let mut node_usage: HashMap<i64, usize> = HashMap::new();
    let mut decision_nodes = HashSet::new();
    let mut included_ways = HashSet::new();

    // Stream ways and count node usage
    let way_stream = WaysFile::stream_ways(ways_path)?;

    for result in way_stream {
        let (way_id, _keys, _vals, nodes) = result?;

        // Check if way is included (has access in any mode)
        let car_rec = way_attrs_car.get(&way_id);
        let bike_rec = way_attrs_bike.get(&way_id);
        let foot_rec = way_attrs_foot.get(&way_id);

        if let (Some(car), Some(bike), Some(foot)) = (car_rec, bike_rec, foot_rec) {
            if has_any_access(car, bike, foot) {
                included_ways.insert(way_id);

                // Mark endpoints as decision nodes
                if let Some(&first) = nodes.first() {
                    decision_nodes.insert(first);
                }
                if let Some(&last) = nodes.last() {
                    decision_nodes.insert(last);
                }

                // Count node usage for intersection detection
                for &node_id in &nodes {
                    *node_usage.entry(node_id).or_insert(0) += 1;
                }
            }
        }
    }

    // Add intersections (nodes used by >= 2 ways) as decision nodes
    for (node_id, count) in node_usage {
        if count >= 2 {
            decision_nodes.insert(node_id);
        }
    }

    Ok((decision_nodes, included_ways))
}

fn build_node_map(decision_nodes: &HashSet<i64>) -> Result<NbgNodeMap> {
    let mut nodes: Vec<i64> = decision_nodes.iter().copied().collect();
    nodes.sort_unstable();

    let mappings: Vec<NodeMapping> = nodes
        .into_iter()
        .enumerate()
        .map(|(idx, osm_id)| NodeMapping {
            osm_node_id: osm_id,
            compact_id: idx as u32,
        })
        .collect();

    Ok(NbgNodeMap { mappings })
}

#[derive(Debug, Clone)]
struct EdgeInfo {
    u_node: u32,
    v_node: u32,
    length_mm: u32,
    bearing_deci_deg: u16,
    polyline: PolyLine,
    first_osm_way_id: i64,
    flags: u32,
}

#[allow(clippy::type_complexity)]
fn emit_edges(
    ways_path: &PathBuf,
    included_ways: &HashSet<i64>,
    osm_to_compact: &HashMap<i64, u32>,
    node_coords: &HashMap<i64, (f64, f64)>,
) -> Result<(Vec<EdgeInfo>, HashMap<u32, Vec<(u32, u64)>>)> {
    let mut edges = Vec::new();
    let mut adjacency: HashMap<u32, Vec<(u32, u64)>> = HashMap::new();

    let way_stream = WaysFile::stream_ways(ways_path)?;

    for result in way_stream {
        let (way_id, _keys, _vals, nodes) = result?;

        if !included_ways.contains(&way_id) {
            continue;
        }

        // Walk the way and emit edges between decision nodes
        let mut seg_start_idx = 0;

        for i in 1..nodes.len() {
            let node_id = nodes[i];

            // Check if this is a decision node
            if osm_to_compact.contains_key(&node_id) {
                // Emit edge from seg_start_idx to i
                let start_osm = nodes[seg_start_idx];
                let end_osm = node_id;

                if let (Some(&u_compact), Some(&v_compact)) =
                    (osm_to_compact.get(&start_osm), osm_to_compact.get(&end_osm))
                {
                    // Collect polyline
                    let mut lat_fxp = Vec::new();
                    let mut lon_fxp = Vec::new();
                    let mut length_m = 0.0;

                    for j in seg_start_idx..=i {
                        let osm_id = nodes[j];
                        if let Some(&(lat, lon)) = node_coords.get(&osm_id) {
                            lat_fxp.push((lat * 1e7).round() as i32);
                            lon_fxp.push((lon * 1e7).round() as i32);

                            if j > seg_start_idx {
                                let prev_osm = nodes[j - 1];
                                if let Some(&(prev_lat, prev_lon)) = node_coords.get(&prev_osm) {
                                    length_m += haversine_distance(prev_lat, prev_lon, lat, lon);
                                }
                            }
                        }
                    }

                    if lat_fxp.len() >= 2 && length_m > 0.0 {
                        let length_mm = (length_m * 1000.0).round() as u32;
                        // Saturate to minimum 1m as per spec (1m ≤ length_mm ≤ 500km)
                        let length_mm = length_mm.max(1000);

                        // Compute bearing
                        let (start_lat, start_lon) =
                            node_coords.get(&start_osm).copied().unwrap_or((0.0, 0.0));
                        let (end_lat, end_lon) =
                            node_coords.get(&end_osm).copied().unwrap_or((0.0, 0.0));
                        let bearing = compute_bearing(start_lat, start_lon, end_lat, end_lon);

                        let edge_idx = edges.len() as u64;
                        let edge = EdgeInfo {
                            u_node: u_compact,
                            v_node: v_compact,
                            length_mm,
                            bearing_deci_deg: bearing,
                            polyline: PolyLine { lat_fxp, lon_fxp },
                            first_osm_way_id: way_id,
                            flags: 0, // Reserved for future use (roundabout, ferry, tunnel, bridge); see NbgEdge definition in formats/nbg_geo.rs
                        };

                        edges.push(edge);

                        // Add both directions to adjacency
                        adjacency
                            .entry(u_compact)
                            .or_default()
                            .push((v_compact, edge_idx));
                        adjacency
                            .entry(v_compact)
                            .or_default()
                            .push((u_compact, edge_idx));
                    }
                }

                seg_start_idx = i;
            }
        }
    }

    Ok((edges, adjacency))
}

fn assemble_csr(
    adjacency: &HashMap<u32, Vec<(u32, u64)>>,
    n_nodes: u32,
    n_edges_und: u64,
) -> Result<NbgCsr> {
    let mut offsets = vec![0u64; (n_nodes + 1) as usize];
    let mut heads = Vec::new();
    let mut edge_idx = Vec::new();

    // Build CSR
    for node_id in 0..n_nodes {
        offsets[node_id as usize] = heads.len() as u64;

        if let Some(neighbors) = adjacency.get(&node_id) {
            for &(neighbor, edge_id) in neighbors {
                heads.push(neighbor);
                edge_idx.push(edge_id);
            }
        }
    }
    offsets[n_nodes as usize] = heads.len() as u64;

    let created_unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();

    Ok(NbgCsr {
        n_nodes,
        n_edges_und,
        created_unix,
        inputs_sha: [0u8; 32], // TODO: compute from inputs
        offsets,
        heads,
        edge_idx,
    })
}

fn build_geo_structure(edges: Vec<EdgeInfo>) -> Result<NbgGeo> {
    let n_edges_und = edges.len() as u64;
    let mut nbg_edges = Vec::new();
    let mut polylines = Vec::new();
    let mut poly_off = 0u64;

    for edge in edges {
        let n_poly_pts = edge.polyline.lat_fxp.len() as u16;
        let poly_bytes = (n_poly_pts as u64) * 4 * 2; // lat + lon, 4 bytes each

        nbg_edges.push(NbgEdge {
            u_node: edge.u_node,
            v_node: edge.v_node,
            length_mm: edge.length_mm,
            bearing_deci_deg: edge.bearing_deci_deg,
            n_poly_pts,
            poly_off,
            first_osm_way_id: edge.first_osm_way_id,
            flags: edge.flags,
        });

        polylines.push(edge.polyline);
        poly_off += poly_bytes;
    }

    Ok(NbgGeo {
        n_edges_und,
        edges: nbg_edges,
        polylines,
    })
}
