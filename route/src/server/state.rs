//! Server state - loaded data for query processing
//!
//! Per-mode CCH architecture: each mode has its own filtered CCH topology and ordering.
//! The spatial index operates in original EBG space, then maps to filtered space for query.

use anyhow::{Context, Result};
use std::collections::HashMap;
use std::path::Path;

use crate::formats::{
    CchTopo, CchTopoFile, CchWeightsFile, EbgCsr, EbgCsrFile, EbgNodes, EbgNodesFile, FilteredEbg,
    FilteredEbgFile, NbgGeo, NbgGeoFile, NbgNodeMapFile, OrderEbg, OrderEbgFile, WaysFile,
    mod_weights,
};
// Re-export CchWeights for use by api.rs
pub use crate::formats::CchWeights;
use crate::matrix::bucket_ch::{DownReverseAdjFlat, UpAdjFlat};
use crate::profile_abi::Mode;

use super::exclude::{self, ExcludeWeights};

use super::elevation::ElevationData;
use super::spatial::SpatialIndex;

/// Per-mode data including CCH topology (since each mode has its own filtered CCH)
pub struct ModeData {
    pub mode: Mode,
    // CCH hierarchy for this mode
    pub cch_topo: CchTopo,
    pub order: OrderEbg,
    pub down_rev: DownReverseAdj,
    pub cch_weights: CchWeights,
    pub cch_weights_dist: CchWeights,
    // Filtered EBG for node ID mapping
    pub filtered_ebg: FilteredEbg,
    // Original node weights and mask (indexed by original EBG node ID)
    pub node_weights: Vec<u32>,
    pub mask: Vec<u64>,
    // Flat adjacencies for bucket M2M - TIME metric (pre-built for performance)
    pub up_adj_flat: UpAdjFlat,
    pub down_rev_flat: DownReverseAdjFlat,
    // Flat adjacencies for bucket M2M - DISTANCE metric (shortest-distance, independent of time)
    pub up_adj_flat_dist: UpAdjFlat,
    pub down_rev_flat_dist: DownReverseAdjFlat,
    // Cached exclude weight sets (keyed by exclude bitmask)
    pub exclude_cache: parking_lot::RwLock<HashMap<u8, std::sync::Arc<ExcludeWeights>>>,
}

// CchWeights is imported from crate::formats

/// Reverse adjacency for DOWN edges (used in backward search)
/// For each node y, stores all nodes x that have DOWN edges x→y
/// along with the original edge index (to look up weights)
pub struct DownReverseAdj {
    pub offsets: Vec<u64>,  // n_nodes + 1
    pub sources: Vec<u32>,  // source node x for reverse edge
    pub edge_idx: Vec<u32>, // index into down_targets/down_weights for the original x→y edge
}

/// Server state containing all loaded data
pub struct ServerState {
    // Graph structure (original EBG, used for spatial index and geometry)
    pub ebg_nodes: EbgNodes,
    pub ebg_csr: EbgCsr,
    pub nbg_geo: NbgGeo,
    /// NBG compact node id → OSM node id. Indexed by `compact_id`,
    /// loaded once at startup from `step3*/nbg.node_map`. Used by the
    /// Flight `edges_batch` action (#125) to expose per-edge OSM node
    /// references in the unnested output schema. Memory cost on
    /// Belgium: ~11 MB (≈1.4M nodes × 8 bytes).
    pub nbg_node_to_osm: Vec<i64>,

    // Per-mode data (dynamically discovered, indexed by mode_index)
    pub modes: Vec<ModeData>,
    /// Mode names indexed by mode_index (alphabetically sorted)
    pub mode_names: Vec<String>,
    /// Mode name → mode index lookup
    pub mode_lookup: HashMap<String, u8>,

    // Spatial index for snapping (operates in original EBG space).
    // Global index: includes every EBG node regardless of mode, used
    // by legacy callers (nearest handler, table, etc.) that pass a
    // mode mask for filtering at query time.
    pub spatial_index: SpatialIndex,

    // Per-mode spatial indexes built at startup for every loaded mode.
    // Each index contains ONLY nodes passing that mode's mask, so
    // `snap_unfiltered` returns the correct mode-specific nearest in a
    // single R-tree walk with no rejection loop. Indexed by mode index
    // (same space as `modes` / `mode_lookup`).
    //
    // See issue #116. Used by `/transit` on the hot path; other
    // endpoints continue to use the global index until migrated.
    pub mode_spatial_indexes: HashMap<u8, SpatialIndex>,

    // Elevation data (optional, loaded from SRTM .hgt files)
    pub elevation: Option<ElevationData>,

    // Road names: OSM way_id → name string (for turn-by-turn instructions)
    pub way_names: HashMap<i64, String>,

    // Distance weights indexed by original EBG node ID (length_mm per edge)
    // Used for isodistance isochrones — same role as ModeData.node_weights but in millimeters
    pub node_weights_dist: Vec<u32>,

    // Per-EBG-edge exclude flags (toll/ferry/motorway), indexed by original EBG edge ID
    pub edge_exclude_flags: Vec<u8>,

    // Optional transit (public transport) state
    pub transit: Option<crate::transit::TransitState>,

    // Server metadata
    pub started_at: std::time::Instant,
    pub data_dir: String,
}

impl ServerState {
    /// Load all data from directory. If `mode_filter` is Some, only load those modes.
    pub fn load(data_dir: &Path, mode_filter: Option<&[String]>) -> Result<Self> {
        // Determine subdirectories
        let step1_dir = find_step_dir(data_dir, "step1")?;
        let step2_dir = find_step_dir(data_dir, "step2")?;
        let step3_dir = find_step_dir(data_dir, "step3")?;
        let step4_dir = find_step_dir(data_dir, "step4")?;
        let step5_dir = find_step_dir(data_dir, "step5")?;
        let step6_dir = find_step_dir(data_dir, "step6")?;
        let step7_dir = find_step_dir(data_dir, "step7")?;
        let step8_dir = find_step_dir(data_dir, "step8")?;

        tracing::info!("Loading EBG nodes...");
        let ebg_nodes = EbgNodesFile::read(step4_dir.join("ebg.nodes"))?;
        tracing::info!(nodes = ebg_nodes.n_nodes, "loaded EBG nodes");

        tracing::info!("Loading EBG CSR...");
        let ebg_csr = EbgCsrFile::read(step4_dir.join("ebg.csr"))?;
        tracing::info!(arcs = ebg_csr.n_arcs, "loaded EBG CSR");

        tracing::info!("Loading NBG geo...");
        let nbg_geo = NbgGeoFile::read(step3_dir.join("nbg.geo"))?;
        tracing::info!(edges = nbg_geo.edges.len(), "loaded NBG geo");

        tracing::info!("Loading NBG node-id map (osm → compact)...");
        let nbg_node_map = NbgNodeMapFile::read_map(step3_dir.join("nbg.node_map"))?;
        // Invert into a Vec indexed by NBG compact_id so the Flight
        // edges_batch action (#125) can do `osm_node_ids[u_node]` in
        // O(1). Compact ids are dense and contiguous from 0.
        let max_compact = nbg_node_map
            .mappings
            .iter()
            .map(|m| m.compact_id)
            .max()
            .unwrap_or(0);
        let mut nbg_node_to_osm: Vec<i64> = vec![0; (max_compact as usize) + 1];
        for m in &nbg_node_map.mappings {
            nbg_node_to_osm[m.compact_id as usize] = m.osm_node_id;
        }
        tracing::info!(
            n_nbg_nodes = nbg_node_to_osm.len(),
            "loaded NBG node id map"
        );

        // Discover ALL available modes (for global index assignment), then filter
        let all_modes = discover_modes(&step5_dir)?;
        // Global index: position in alphabetically sorted all_modes list
        let global_index: HashMap<String, u8> = all_modes
            .iter()
            .enumerate()
            .map(|(i, name)| (name.clone(), i as u8))
            .collect();

        let discovered_modes: Vec<String> = if let Some(filter) = mode_filter {
            all_modes
                .into_iter()
                .filter(|m| filter.iter().any(|f| f == m))
                .collect()
        } else {
            all_modes
        };
        tracing::info!(modes = ?discovered_modes, "discovered transport modes");

        if discovered_modes.is_empty() {
            anyhow::bail!(
                "No transport modes found in {}. Expected w.*.u32 files.",
                step5_dir.display()
            );
        }

        // Load per-mode CCH data
        tracing::info!("Loading per-mode CCH data...");
        let mut modes_data = Vec::with_capacity(discovered_modes.len());
        let mut mode_names = Vec::with_capacity(discovered_modes.len());
        let mut mode_lookup = HashMap::with_capacity(discovered_modes.len());

        for (mode_index, mode_name) in discovered_modes.iter().enumerate() {
            // Use GLOBAL index (from full alphabetical discovery) — must match step 4/5 indexing
            let mode = Mode(global_index[mode_name]);
            let mode_data = load_mode_data(
                mode_name, mode, &step5_dir, &step6_dir, &step7_dir, &step8_dir,
            )?;
            tracing::info!(
                mode = mode_name.as_str(),
                index = mode_index,
                filtered_nodes = mode_data.filtered_ebg.n_filtered_nodes,
                up_edges = mode_data.cch_topo.up_targets.len(),
                "loaded mode data"
            );
            modes_data.push(mode_data);
            mode_lookup.insert(mode_name.clone(), mode_index as u8);
            mode_names.push(mode_name.clone());
        }

        tracing::info!("Building spatial index...");
        let spatial_index = SpatialIndex::build(&ebg_nodes, &nbg_geo);
        tracing::info!(nodes = ebg_nodes.n_nodes, "built spatial index");

        // Per-mode spatial indexes: one R-tree per loaded mode,
        // pre-filtered to that mode's accessible nodes. Built once at
        // startup; queried via `snap_unfiltered` which skips the
        // pathological rejection loop that the global index incurs.
        // See issue #116.
        tracing::info!("Building per-mode spatial indexes...");
        let mut mode_spatial_indexes: HashMap<u8, SpatialIndex> =
            HashMap::with_capacity(modes_data.len());
        for (mode_index, mode_data) in modes_data.iter().enumerate() {
            let idx = SpatialIndex::build_filtered(&ebg_nodes, &nbg_geo, &mode_data.mask);
            tracing::info!(
                mode = mode_names[mode_index].as_str(),
                index = mode_index,
                indexed_nodes = idx.n_indexed(),
                "built per-mode spatial index"
            );
            mode_spatial_indexes.insert(mode_index as u8, idx);
        }

        // Load road names from ways.raw for turn-by-turn instructions
        tracing::info!("Loading road names...");
        let way_names = load_way_names(&step1_dir)?;
        tracing::info!(named_roads = way_names.len(), "loaded road names");

        // Build per-edge exclude flags from way_attrs.car.bin
        // Try car first, then any available mode's way_attrs
        tracing::info!("Loading edge exclude flags...");
        let way_attrs_path = find_way_attrs_path(&step2_dir, &discovered_modes);
        let edge_exclude_flags = if let Some(attrs_path) = way_attrs_path {
            exclude::build_edge_exclude_flags(&ebg_nodes, &attrs_path)?
        } else {
            tracing::warn!("No way_attrs file found, exclude feature disabled");
            vec![0u8; ebg_nodes.n_nodes as usize]
        };

        // Build distance-based node weights from EBG edge lengths (mm)
        // Used for isodistance isochrones: same role as ModeData.node_weights but distance-based
        let node_weights_dist: Vec<u32> = ebg_nodes.nodes.iter().map(|n| n.length_mm).collect();
        tracing::info!(
            edges = node_weights_dist.len(),
            "built distance node weights"
        );

        // Try to load elevation data from srtm/ subdirectory
        let srtm_dir = data_dir.join("srtm");
        let elevation = if srtm_dir.is_dir() {
            match ElevationData::load_from_dir(&srtm_dir) {
                Ok(elev) => {
                    tracing::info!(tiles = elev.tile_count(), "loaded SRTM elevation tiles");
                    Some(elev)
                }
                Err(e) => {
                    tracing::warn!(error = %e, "could not load SRTM data");
                    None
                }
            }
        } else {
            tracing::info!("no srtm/ directory found, /height endpoint disabled");
            None
        };

        // Transit subsystem is loaded asynchronously by the outer
        // `serve()` function (after `ServerState::load` returns), because
        // downloading feeds and running reqwest requires an active Tokio
        // runtime. We start with `None` here; the caller installs the
        // transit state via `install_transit()` before accepting traffic.
        let transit = None;

        Ok(Self {
            ebg_nodes,
            ebg_csr,
            nbg_geo,
            nbg_node_to_osm,
            modes: modes_data,
            mode_names,
            mode_lookup,
            spatial_index,
            mode_spatial_indexes,
            elevation,
            way_names,
            node_weights_dist,
            edge_exclude_flags,
            transit,
            started_at: std::time::Instant::now(),
            data_dir: data_dir.to_string_lossy().to_string(),
        })
    }

    /// Get mode data by mode (index-based lookup)
    pub fn get_mode(&self, mode: Mode) -> &ModeData {
        &self.modes[mode.index()]
    }

    /// Install the transit subsystem after async bootstrap. Must be
    /// called exactly once, before the server starts accepting traffic.
    /// Returns an error if transit was already installed or if foot mode
    /// is not available.
    pub fn install_transit(&mut self, state: crate::transit::TransitState) {
        self.transit = Some(state);
    }

    /// Get or compute exclude weights for a mode and exclude mask.
    /// Returns Arc<ExcludeWeights> from cache, computing on first access.
    pub fn get_exclude_weights(
        &self,
        mode: Mode,
        exclude_mask: u8,
    ) -> std::sync::Arc<ExcludeWeights> {
        let mode_data = self.get_mode(mode);

        // Fast path: check cache with read lock
        {
            let cache = mode_data.exclude_cache.read();
            if let Some(weights) = cache.get(&exclude_mask) {
                return std::sync::Arc::clone(weights);
            }
        }

        // Slow path: compute and insert with write lock
        let mut cache = mode_data.exclude_cache.write();
        // Double-check after acquiring write lock
        if let Some(weights) = cache.get(&exclude_mask) {
            return std::sync::Arc::clone(weights);
        }

        let mode_name = &self.mode_names[mode.index()];
        tracing::info!(
            mode = mode_name.as_str(),
            exclude_mask,
            "computing exclude weights (first request)"
        );

        let weights = std::sync::Arc::new(exclude::compute_exclude_weights(
            &mode_data.cch_topo,
            &mode_data.cch_weights,
            &mode_data.cch_weights_dist,
            &self.edge_exclude_flags,
            exclude_mask,
            &mode_data.filtered_ebg.filtered_to_original,
        ));

        cache.insert(exclude_mask, std::sync::Arc::clone(&weights));
        weights
    }
}

/// Find step directory (handles both "step3" and "step3-belgium" naming)
fn find_step_dir(data_dir: &Path, step: &str) -> Result<std::path::PathBuf> {
    // Try exact match first
    let exact = data_dir.join(step);
    if exact.exists() {
        return Ok(exact);
    }

    // Try with suffix pattern -- collect all matches and sort for determinism
    let mut matches: Vec<std::path::PathBuf> = Vec::new();
    for entry in std::fs::read_dir(data_dir).context("Failed to read data directory")? {
        let entry = entry?;
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if name_str.starts_with(step) && entry.file_type()?.is_dir() {
            matches.push(entry.path());
        }
    }
    matches.sort();
    if let Some(first) = matches.into_iter().next() {
        return Ok(first);
    }

    anyhow::bail!(
        "Could not find {} directory in {}",
        step,
        data_dir.display()
    );
}

/// Discover available modes by scanning for `w.*.u32` files in the step5 directory.
/// Returns mode names sorted alphabetically for deterministic indexing.
fn discover_modes(step5_dir: &Path) -> Result<Vec<String>> {
    let mut mode_names: Vec<String> = Vec::new();

    for entry in std::fs::read_dir(step5_dir).context("Failed to read step5 directory")? {
        let entry = entry?;
        let name = entry.file_name();
        let name_str = name.to_string_lossy();

        // Pattern: w.{mode_name}.u32
        if let Some(rest) = name_str.strip_prefix("w.")
            && let Some(mode_name) = rest.strip_suffix(".u32")
                && !mode_name.is_empty() {
                    mode_names.push(mode_name.to_string());
                }
    }

    // Sort alphabetically for deterministic indexing
    mode_names.sort();
    mode_names.dedup();

    Ok(mode_names)
}

/// Find the best way_attrs file for exclude flags.
/// Prefers "car" if available, otherwise uses the first available mode.
fn find_way_attrs_path(step2_dir: &Path, modes: &[String]) -> Option<std::path::PathBuf> {
    // Prefer car mode for exclude flags (toll/ferry/motorway are car-centric)
    let car_path = step2_dir.join("way_attrs.car.bin");
    if car_path.exists() {
        return Some(car_path);
    }

    // Fall back to any available mode
    for mode_name in modes {
        let path = step2_dir.join(format!("way_attrs.{}.bin", mode_name));
        if path.exists() {
            return Some(path);
        }
    }

    None
}

/// Load per-mode data (CCH topo, ordering, weights, filtered EBG)
fn load_mode_data(
    mode_name: &str,
    mode: Mode,
    step5_dir: &Path,
    step6_dir: &Path,
    step7_dir: &Path,
    step8_dir: &Path,
) -> Result<ModeData> {
    // Load filtered EBG from step 5
    let filtered_ebg_path = step5_dir.join(format!("filtered.{}.ebg", mode_name));
    let filtered_ebg = FilteredEbgFile::read(&filtered_ebg_path)?;

    // Load per-mode ordering from step 6
    let order_path = step6_dir.join(format!("order.{}.ebg", mode_name));
    let order = OrderEbgFile::read(&order_path)?;

    // Load per-mode CCH topology from step 7
    let topo_path = step7_dir.join(format!("cch.{}.topo", mode_name));
    let cch_topo = CchTopoFile::read(&topo_path)?;

    // Build reverse DOWN adjacency for this mode's CCH
    let down_rev = build_down_reverse_adj(&cch_topo);

    // Load node weights from step 5 (indexed by original EBG node ID)
    let weights_path = step5_dir.join(format!("w.{}.u32", mode_name));
    let weights_data = mod_weights::read_all(&weights_path)?;

    // Build snap mask from the SCC-filtered EBG (only nodes in the largest
    // strongly connected component are snappable). This ensures queries never
    // snap to dead-end stubs or disconnected fragments.
    let n_original = filtered_ebg.n_original_nodes as usize;
    let mask = {
        let n_words = n_original.div_ceil(64);
        let mut m = vec![0u64; n_words];
        for &orig_id in &filtered_ebg.filtered_to_original {
            let word = orig_id as usize / 64;
            let bit = orig_id as usize % 64;
            m[word] |= 1u64 << bit;
        }
        m
    };

    // Load CCH weights from step 8
    let cch_weights_path = step8_dir.join(format!("cch.w.{}.u32", mode_name));
    let cch_weights = CchWeightsFile::read(&cch_weights_path)?;

    // Build flat adjacencies for bucket M2M - TIME metric (pre-filtered for INF, embedded weights)
    let up_adj_flat = UpAdjFlat::build(&cch_topo, &cch_weights);
    let down_rev_flat = DownReverseAdjFlat::build(&cch_topo, &cch_weights);

    // Load pre-computed distance weights from step 8 (cch.d.{mode}.u32)
    let cch_dist_weights_path = step8_dir.join(format!("cch.d.{}.u32", mode_name));
    tracing::info!(mode = mode_name, "loading distance weights");
    let cch_weights_dist = CchWeightsFile::read(&cch_dist_weights_path)?;
    let up_adj_flat_dist = UpAdjFlat::build(&cch_topo, &cch_weights_dist);
    let down_rev_flat_dist = DownReverseAdjFlat::build(&cch_topo, &cch_weights_dist);

    Ok(ModeData {
        mode,
        cch_topo,
        order,
        down_rev,
        cch_weights,
        cch_weights_dist,
        filtered_ebg,
        node_weights: weights_data.weights,
        mask,
        up_adj_flat,
        down_rev_flat,
        up_adj_flat_dist,
        down_rev_flat_dist,
        exclude_cache: parking_lot::RwLock::new(HashMap::new()),
    })
}

/// Load CCH weights from file
/// Build reverse adjacency for DOWN edges
/// For each node y, we want to find all edges x→y in the DOWN graph
/// This allows backward search to iterate over incoming edges efficiently
fn build_down_reverse_adj(topo: &CchTopo) -> DownReverseAdj {
    let n_nodes = topo.n_nodes as usize;
    let n_down = topo.down_targets.len();

    // First pass: count incoming edges per node
    let mut counts = vec![0usize; n_nodes];
    for &target in &topo.down_targets {
        counts[target as usize] += 1;
    }

    // Build offsets
    let mut offsets = Vec::with_capacity(n_nodes + 1);
    let mut offset = 0u64;
    for &count in &counts {
        offsets.push(offset);
        offset += count as u64;
    }
    offsets.push(offset);

    // Allocate arrays
    let mut sources = vec![0u32; n_down];
    let mut edge_idx = vec![0u32; n_down];

    // Second pass: fill in reverse edges
    // Reset counts to use as position trackers
    counts.fill(0);

    for source in 0..n_nodes {
        let start = topo.down_offsets[source] as usize;
        let end = topo.down_offsets[source + 1] as usize;

        for i in start..end {
            let target = topo.down_targets[i] as usize;
            let pos = offsets[target] as usize + counts[target];
            sources[pos] = source as u32;
            edge_idx[pos] = i as u32;
            counts[target] += 1;
        }
    }

    DownReverseAdj {
        offsets,
        sources,
        edge_idx,
    }
}

/// Load road names from ways.raw (step1 output).
/// Uses streaming to avoid loading all way data into memory at once.
/// Returns way_id → name mapping for all ways that have a "name" tag.
fn load_way_names(step1_dir: &Path) -> Result<HashMap<i64, String>> {
    let ways_path = step1_dir.join("ways.raw");
    if !ways_path.exists() {
        tracing::warn!("ways.raw not found, road names unavailable");
        return Ok(HashMap::new());
    }

    // Load dictionaries first
    let (key_dict, val_dict, _, _) = WaysFile::read_dictionaries(&ways_path)?;

    // Find key ID for "name"
    let name_key_id = key_dict
        .iter()
        .find(|(_, v)| v.as_str() == "name")
        .map(|(k, _)| *k);

    let name_key_id = match name_key_id {
        Some(id) => id,
        None => {
            tracing::warn!("no 'name' key in dictionary, road names unavailable");
            return Ok(HashMap::new());
        }
    };

    // Stream ways and extract names
    let mut way_names = HashMap::new();
    let way_stream = WaysFile::stream_ways(&ways_path)?;

    for result in way_stream {
        let (way_id, keys, vals, _nodes) = result?;

        // Find "name" tag value for this way
        for (i, &k) in keys.iter().enumerate() {
            if k == name_key_id {
                if let Some(name) = val_dict.get(&vals[i])
                    && !name.is_empty() {
                        way_names.insert(way_id, name.clone());
                    }
                break; // each way has at most one "name" tag
            }
        }
    }

    Ok(way_names)
}

// Distance weights are now pre-computed in step8 pipeline (cch.d.{mode}.u32)
// and loaded from file alongside time weights at startup.
