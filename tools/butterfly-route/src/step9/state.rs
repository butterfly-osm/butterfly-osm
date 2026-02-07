//! Server state - loaded data for query processing
//!
//! Per-mode CCH architecture: each mode has its own filtered CCH topology and ordering.
//! The spatial index operates in original EBG space, then maps to filtered space for query.

use anyhow::{Context, Result};
use std::path::Path;

use crate::formats::{
    mod_mask, mod_weights, CchTopo, CchTopoFile, CchWeightsFile, EbgCsr, EbgCsrFile, EbgNodes,
    EbgNodesFile, FilteredEbg, FilteredEbgFile, NbgGeo, NbgGeoFile, OrderEbg, OrderEbgFile,
};
// Re-export CchWeights for use by api.rs
pub use crate::formats::CchWeights;
use crate::matrix::bucket_ch::{DownReverseAdjFlat, UpAdjFlat};
use crate::profile_abi::Mode;

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
}

// CchWeights is imported from crate::formats

/// Reverse adjacency for DOWN edges (used in backward search)
/// For each node y, stores all nodes x that have DOWN edges x→y
/// along with the original edge index (to look up weights)
pub struct DownReverseAdj {
    pub offsets: Vec<u64>,   // n_nodes + 1
    pub sources: Vec<u32>,   // source node x for reverse edge
    pub edge_idx: Vec<u32>,  // index into down_targets/down_weights for the original x→y edge
}

/// Server state containing all loaded data
pub struct ServerState {
    // Graph structure (original EBG, used for spatial index and geometry)
    pub ebg_nodes: EbgNodes,
    pub ebg_csr: EbgCsr,
    pub nbg_geo: NbgGeo,

    // Per-mode data (each mode has its own CCH)
    pub car: ModeData,
    pub bike: ModeData,
    pub foot: ModeData,

    // Spatial index for snapping (operates in original EBG space)
    pub spatial_index: SpatialIndex,

    // Elevation data (optional, loaded from SRTM .hgt files)
    pub elevation: Option<ElevationData>,
}

impl ServerState {
    /// Load all data from directory
    pub fn load(data_dir: &Path) -> Result<Self> {
        // Determine subdirectories
        let step3_dir = find_step_dir(data_dir, "step3")?;
        let step4_dir = find_step_dir(data_dir, "step4")?;
        let step5_dir = find_step_dir(data_dir, "step5")?;
        let step6_dir = find_step_dir(data_dir, "step6")?;
        let step7_dir = find_step_dir(data_dir, "step7")?;
        let step8_dir = find_step_dir(data_dir, "step8")?;

        println!("Loading EBG nodes...");
        let ebg_nodes = EbgNodesFile::read(step4_dir.join("ebg.nodes"))?;
        println!("  ✓ {} nodes", ebg_nodes.n_nodes);

        println!("Loading EBG CSR...");
        let ebg_csr = EbgCsrFile::read(step4_dir.join("ebg.csr"))?;
        println!("  ✓ {} arcs", ebg_csr.n_arcs);

        println!("Loading NBG geo...");
        let nbg_geo = NbgGeoFile::read(step3_dir.join("nbg.geo"))?;
        println!("  ✓ {} edges", nbg_geo.edges.len());

        println!("Loading per-mode CCH data...");
        let car = load_mode_data(Mode::Car, &step5_dir, &step6_dir, &step7_dir, &step8_dir)?;
        println!("  ✓ car: {} filtered nodes, {} up edges", car.filtered_ebg.n_filtered_nodes, car.cch_topo.up_targets.len());
        let bike = load_mode_data(Mode::Bike, &step5_dir, &step6_dir, &step7_dir, &step8_dir)?;
        println!("  ✓ bike: {} filtered nodes, {} up edges", bike.filtered_ebg.n_filtered_nodes, bike.cch_topo.up_targets.len());
        let foot = load_mode_data(Mode::Foot, &step5_dir, &step6_dir, &step7_dir, &step8_dir)?;
        println!("  ✓ foot: {} filtered nodes, {} up edges", foot.filtered_ebg.n_filtered_nodes, foot.cch_topo.up_targets.len());

        println!("Building spatial index...");
        let spatial_index = SpatialIndex::build(&ebg_nodes, &nbg_geo);
        println!("  ✓ Indexed {} nodes", ebg_nodes.n_nodes);

        // Try to load elevation data from srtm/ subdirectory
        let srtm_dir = data_dir.join("srtm");
        let elevation = if srtm_dir.is_dir() {
            match ElevationData::load_from_dir(&srtm_dir) {
                Ok(elev) => {
                    println!("  ✓ Loaded {} SRTM tiles", elev.tile_count());
                    Some(elev)
                }
                Err(e) => {
                    println!("  ⚠ Could not load SRTM data: {}", e);
                    None
                }
            }
        } else {
            println!("  ℹ No srtm/ directory found, /height endpoint disabled");
            None
        };

        Ok(Self {
            ebg_nodes,
            ebg_csr,
            nbg_geo,
            car,
            bike,
            foot,
            spatial_index,
            elevation,
        })
    }

    /// Get mode data by mode
    pub fn get_mode(&self, mode: Mode) -> &ModeData {
        match mode {
            Mode::Car => &self.car,
            Mode::Bike => &self.bike,
            Mode::Foot => &self.foot,
        }
    }
}

/// Find step directory (handles both "step3" and "step3-belgium" naming)
fn find_step_dir(data_dir: &Path, step: &str) -> Result<std::path::PathBuf> {
    // Try exact match first
    let exact = data_dir.join(step);
    if exact.exists() {
        return Ok(exact);
    }

    // Try with suffix pattern
    for entry in std::fs::read_dir(data_dir).context("Failed to read data directory")? {
        let entry = entry?;
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if name_str.starts_with(step) && entry.file_type()?.is_dir() {
            return Ok(entry.path());
        }
    }

    anyhow::bail!("Could not find {} directory in {}", step, data_dir.display());
}

/// Load per-mode data (CCH topo, ordering, weights, filtered EBG)
fn load_mode_data(
    mode: Mode,
    step5_dir: &Path,
    step6_dir: &Path,
    step7_dir: &Path,
    step8_dir: &Path,
) -> Result<ModeData> {
    let mode_name = match mode {
        Mode::Car => "car",
        Mode::Bike => "bike",
        Mode::Foot => "foot",
    };

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

    // Load mask from step 5 and convert to u64 words
    let mask_path = step5_dir.join(format!("mask.{}.bitset", mode_name));
    let mask_data = mod_mask::read_all(&mask_path)?;
    let mask = bytes_to_u64_words(&mask_data.mask);

    // Load CCH weights from step 8
    let cch_weights_path = step8_dir.join(format!("cch.w.{}.u32", mode_name));
    let cch_weights = CchWeightsFile::read(&cch_weights_path)?;

    // Build flat adjacencies for bucket M2M - TIME metric (pre-filtered for INF, embedded weights)
    let up_adj_flat = UpAdjFlat::build(&cch_topo, &cch_weights);
    let down_rev_flat = DownReverseAdjFlat::build(&cch_topo, &cch_weights);

    // Load pre-computed distance weights from step 8 (cch.d.{mode}.u32)
    let cch_dist_weights_path = step8_dir.join(format!("cch.d.{}.u32", mode_name));
    println!("    Loading distance weights for {}...", mode_name);
    let cch_weights_dist = CchWeightsFile::read(&cch_dist_weights_path)?;
    let up_adj_flat_dist = UpAdjFlat::build(&cch_topo, &cch_weights_dist);
    let down_rev_flat_dist = DownReverseAdjFlat::build(&cch_topo, &cch_weights_dist);

    Ok(ModeData {
        mode,
        cch_topo,
        order,
        down_rev,
        cch_weights,
        filtered_ebg,
        node_weights: weights_data.weights,
        mask,
        up_adj_flat,
        down_rev_flat,
        up_adj_flat_dist,
        down_rev_flat_dist,
    })
}

/// Convert byte array to u64 word array for efficient bit testing
fn bytes_to_u64_words(bytes: &[u8]) -> Vec<u64> {
    let n_words = (bytes.len() + 7) / 8;
    let mut words = vec![0u64; n_words];

    for (i, &byte) in bytes.iter().enumerate() {
        let word_idx = i / 8;
        let byte_offset = (i % 8) * 8;
        words[word_idx] |= (byte as u64) << byte_offset;
    }

    words
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

// Distance weights are now pre-computed in step8 pipeline (cch.d.{mode}.u32)
// and loaded from file alongside time weights at startup.
