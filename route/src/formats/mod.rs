//! Binary file formats for Step 1, Step 2, and Step 3 output

// Step 1 formats
pub mod bitset;
pub mod butterfly_dat;
pub mod crc;
pub mod lazy_verify;
pub mod mmap;
pub mod node_signals;
pub mod nodes_sa;
pub mod nodes_si;
pub mod relations;
pub mod ways;

// Step 2 formats
pub mod turn_rules;
pub mod way_attrs;

// Step 3 formats
pub mod nbg_csr;
pub mod nbg_geo;
pub mod nbg_node_map;

// Step 4 formats
pub mod ebg_csr;
pub mod ebg_nodes;
pub mod ebg_turn_table;

// Step 5 formats
pub mod filtered_ebg;
pub mod hybrid_state;
pub mod mod_mask;
pub mod mod_turns;
pub mod mod_weights;

// Step 6 formats
pub mod order_ebg;

// Server-only per-mode mapping sections (#153)
pub mod mode_index;

// Server-only packed snap index sections (#154)
pub mod snap_index;

// Server-only flat edge geometry sections (#155)
pub mod edge_geom;

// Multi-region coarse tile coverage set (#142)
pub mod region_tiles;

// Compact OSM way-name lookup index (#282)
pub mod way_names_idx;

// Step 7 formats
pub mod cch_topo;

// Step 8 formats
pub mod cch_weights;

// Transparent zstd compression for cold container sections (#347)
pub mod zstd_compress;

pub use bitset::BitsetField;
pub use cch_topo::{CchTopo, CchTopoFile, Shortcut};
pub use cch_weights::{CchWeights, CchWeightsFile, U24_SENTINEL, WeightArray, WeightWidth};
pub use ebg_csr::{EbgCsr, EbgCsrFile};
pub use ebg_nodes::{EbgNode, EbgNodes, EbgNodesFile};
pub use ebg_turn_table::{TurnEntry, TurnKind, TurnTable, TurnTableFile};
pub use edge_geom::{EdgeGeomOffsets, EdgeGeomOffsetsFile, EdgeGeomPoints, EdgeGeomPointsFile};
pub use filtered_ebg::{FilteredEbg, FilteredEbgFile};
pub use hybrid_state::{HybridState, HybridStateFile};
pub use mmap::ArcCow;
pub use mod_mask::ModMask;
pub use mod_turns::ModTurns;
pub use mod_weights::ModWeights;
pub use mode_index::{ModeIndex, ModeIndexFile, ModeIndexKind};
pub use nbg_csr::{NbgCsr, NbgCsrFile};
pub use nbg_geo::{NbgEdge, NbgGeo, NbgGeoFile, PolyLine};
pub use nbg_node_map::{NbgNodeMap, NbgNodeMapFile, NodeMapping};
pub use node_signals::{NodeSignals, NodeSignalsFile};
pub use order_ebg::{OrderEbg, OrderEbgFile};
pub use region_tiles::{
    RegionTiles, RegionTilesFile, build_from_snap_points as build_region_tiles, tile_id_from_f64,
};
pub use relations::{Member, MemberKind, Relation, RelationsFile};
pub use snap_index::{
    PackedPoint, SnapBbox, SnapGrid, SnapGridFile, SnapMask, SnapMaskFile, SnapPoints,
    SnapPointsFile, peek_snap_points_bbox,
};
pub use turn_rules::TurnRule;
pub use way_attrs::WayAttr;
pub use ways::{Way, WaysFile};
