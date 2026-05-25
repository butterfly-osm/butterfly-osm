//! Server state - loaded data for query processing
//!
//! Per-mode CCH architecture: each mode has its own filtered CCH topology and ordering.
//! The spatial index operates in original EBG space, then maps to filtered space for query.

use anyhow::{Context, Result};
use std::borrow::Cow;
use std::collections::HashMap;
use std::path::Path;

use crate::formats::{
    CchTopo, CchTopoFile, CchWeightsFile, EbgCsr, EbgCsrFile, EbgNodes, EbgNodesFile,
    FilteredEbgFile, NbgGeo, NbgGeoFile, NbgNodeMapFile, OrderEbgFile, WaysFile, mod_weights,
    mode_index::{ModeIndexFile, ModeIndexKind},
};
// Re-export CchWeights for use by api.rs
pub use crate::formats::CchWeights;
use crate::matrix::bucket_ch::{
    DownAdjFlat, DownAdjFlatFile, DownReverseAdjFlat, DownReverseAdjFlatFile, UpAdjFlat,
    UpAdjFlatFile,
};
use crate::profile_abi::Mode;

use super::exclude::{self, ExcludeWeights};

use super::edge_geom::EdgeGeometry;
use super::elevation::ElevationData;
use super::snap_index::{DEFAULT_CELL_LOG2, PackedSnapIndex, SnapBuilderMode, build_snap_index};
use crate::formats::way_names_idx::WayNamesIdx;

/// Road-name lookup backend.
///
/// Two storage variants behind the same `get(way_id) -> Option<&str>`
/// API:
///
/// - [`WayNames::Idx`] — compact mmap-backed sorted-array + offsets
///   index loaded from a container's `shared/way_names_idx` section
///   (#282). On Belgium this holds ~5-10 KB heap with 754 K named ways
///   addressable; scales to ~3 GiB heap saved on planet-scale corpora.
/// - [`WayNames::Heap`] — legacy `HashMap<i64, String>` built from
///   `step1/ways.raw` at boot. Used by the data-dir path and as a
///   fallback when the container pre-dates #282.
pub enum WayNames {
    Idx(WayNamesIdx),
    Heap(HashMap<i64, String>),
}

impl WayNames {
    /// Look up a name by OSM way id. Returns the borrowed string when
    /// present; identical semantics for both backends.
    #[inline]
    pub fn get(&self, way_id: i64) -> Option<&str> {
        match self {
            Self::Idx(idx) => idx.get(way_id),
            Self::Heap(m) => m.get(&way_id).map(|s| s.as_str()),
        }
    }

    /// Number of named ways indexed.
    #[inline]
    pub fn len(&self) -> usize {
        match self {
            Self::Idx(idx) => idx.len(),
            Self::Heap(m) => m.len(),
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Construct from a legacy HashMap (data-dir path or container
    /// without `shared/way_names_idx`).
    #[inline]
    pub fn from_heap(m: HashMap<i64, String>) -> Self {
        Self::Heap(m)
    }
}

/// Per-mode data including CCH topology (since each mode has its own filtered CCH)
pub struct ModeData {
    pub mode: Mode,
    // CCH hierarchy for this mode
    pub cch_topo: CchTopo,
    pub cch_weights: CchWeights,
    pub cch_weights_dist: CchWeights,
    // ---- Server-only per-mode mapping sections (#153) -------------
    // These replace the build-time `OrderEbg` + `FilteredEbg` structs
    // on the serve path. They are loaded from the container's
    // `mode/<m>/orig_to_rank` and `mode/<m>/filtered_to_original`
    // sections (zero-copy when reading from a packed container) or
    // synthesised from the legacy structs as a back-compat fallback
    // for old containers / `--data-dir` boot.
    //
    // `orig_to_rank[orig_ebg_id]` → CCH rank for this mode, or
    // `u32::MAX` if the original node is not accessible in this mode.
    // Replaces the two-step `original_to_filtered → perm` chain at
    // every serve-path snap site.
    pub orig_to_rank: crate::formats::ArcCow<u32>,
    /// `filtered_to_original[filtered_id]` → original EBG node id.
    /// Used on the unpack/back-reference direction (route geometry,
    /// road-name lookup, exclude/avoid recustomization).
    pub filtered_to_original: crate::formats::ArcCow<u32>,
    /// Number of filtered (mode-accessible) EBG nodes. Equals
    /// `filtered_to_original.len()`. Kept as a u32 for the few
    /// metadata / log sites that read it directly.
    pub n_filtered_nodes: u32,
    /// Number of original EBG nodes. Equals `orig_to_rank.len()`.
    /// Reported in /health and a couple of spot diagnostics.
    pub n_original_nodes: u32,
    /// Per-edge weights (deciseconds) indexed by original EBG node id.
    /// `Cow::Borrowed` for mmap-backed container reads (#294); `Cow::Owned`
    /// for the legacy --data-dir / clone paths.
    pub node_weights: Cow<'static, [u32]>,
    pub mask: Vec<u64>,
    /// Per-mode source snap bitset (indexed by original EBG node ID).
    /// Built at boot from the filtered EBG. A set bit means the node
    /// has at least one mode-valid outbound arc and can reach the main
    /// routing core. Used by role-aware snap (#197) so source snaps do
    /// not land in isolated snap traps.
    pub has_outbound: Vec<u64>,
    /// Per-mode destination snap bitset (indexed by original EBG node
    /// ID). Built at boot from the filtered EBG. A set bit means the
    /// node has at least one mode-valid inbound arc and is reachable
    /// from the main routing core. Used by role-aware snap (#197) so
    /// destination snaps do not land in isolated snap traps.
    pub has_inbound: Vec<u64>,
    // Flat adjacencies for bucket M2M - TIME metric (pre-built for performance)
    //
    // After #152, the time flats also serve as the topology back-end for
    // the cold custom-weight `CchQuery` path (alternatives, exclude/avoid,
    // transit access/egress, map matching). They carry `topo_edge_idx`,
    // which custom callers use to index their per-call `CchWeights.up` /
    // `CchWeights.down` arrays. The legacy `DownReverseAdj` Vec-of-Vec
    // that previously lived here is gone (~320 MB heap reclaimed on
    // Belgium across 4 modes).
    pub up_adj_flat: UpAdjFlat,
    pub down_rev_flat: DownReverseAdjFlat,
    /// Forward DOWN flat (TIME metric). Used by the isochrone forward
    /// PHAST downward scan after #149 — replaces direct
    /// `cch_weights.down[i]` reads on the hot path.
    pub down_adj_flat: DownAdjFlat,
    // Flat adjacencies for bucket M2M - DISTANCE metric (shortest-distance, independent of time)
    pub up_adj_flat_dist: UpAdjFlat,
    pub down_rev_flat_dist: DownReverseAdjFlat,
    /// Forward DOWN flat (DISTANCE metric). Used by the isodistance
    /// forward PHAST downward scan.
    pub down_adj_flat_dist: DownAdjFlat,
    // Cached exclude weight sets (keyed by exclude bitmask)
    pub exclude_cache: parking_lot::RwLock<HashMap<u8, std::sync::Arc<ExcludeWeights>>>,
}

impl ModeData {
    /// Borrow the `orig_to_rank` mapping as a flat slice. Equivalent
    /// to `&mode_data.orig_to_rank[..]`.
    #[inline]
    pub fn orig_to_rank(&self) -> &[u32] {
        &self.orig_to_rank
    }

    /// Borrow the `filtered_to_original` mapping as a flat slice.
    #[inline]
    pub fn filtered_to_original(&self) -> &[u32] {
        &self.filtered_to_original
    }

    /// Look up the CCH rank for an original EBG node id, or `None` if
    /// the node is not accessible in this mode. Replaces the
    /// `original_to_filtered → perm` chain at every snap site.
    #[inline]
    pub fn rank_for_original(&self, orig_id: u32) -> Option<u32> {
        let rank = *self.orig_to_rank.get(orig_id as usize)?;
        if rank == u32::MAX { None } else { Some(rank) }
    }
}

// CchWeights is imported from crate::formats

/// Server state containing all loaded data
pub struct ServerState {
    // Graph structure (original EBG, used for spatial index and geometry)
    pub ebg_nodes: EbgNodes,
    pub ebg_csr: EbgCsr,
    pub nbg_geo: NbgGeo,
    /// Flat mmap-friendly per-edge geometry (#155). Replaces the
    /// heap-resident `nbg_geo.polylines: Vec<PolyLine>` shape on the
    /// serve path. All polyline-reading hot consumers (route geometry,
    /// isochrone stamping, turn-by-turn locations / bearings, map
    /// matching, transit legs) consult this field instead of
    /// `nbg_geo.polylines`.
    ///
    /// On the container path with #155 sections present, this borrows
    /// directly from the mmap. On the directory-tree path or for old
    /// containers, this is built in-memory from `nbg_geo.polylines` via
    /// `EdgeGeometry::from_legacy_polylines` at boot. The accessors are
    /// identical either way.
    pub edge_geom: EdgeGeometry,
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

    /// Packed snap index (#154). One shared point array + uniform-grid
    /// CSR + per-mode bitmaps. Replaces the legacy heap-resident
    /// `SpatialIndex` (one global rstar + one per-mode rstar) which
    /// dominated boot-time anon RSS.
    ///
    /// Loaded zero-copy from the container's `shared/snap_points`,
    /// `shared/snap_grid`, and `mode/<m>/snap_mask` sections when
    /// they're present (every container packed since #154). Old
    /// containers that pre-date #154 fall back to building the same
    /// structure in heap memory at boot via [`build_snap_index`] — no
    /// caller-visible difference, only the storage backing.
    pub snap_index: PackedSnapIndex,

    // Elevation data (optional, loaded from SRTM .hgt files)
    pub elevation: Option<ElevationData>,

    // Road names: OSM way_id → name string (for turn-by-turn instructions).
    //
    // #282: when the container has `shared/way_names_idx`, this is a
    // compact mmap-backed sorted-array + offsets + UTF-8 blob view
    // (~5-10 KB heap on Belgium). Otherwise it's the legacy
    // `HashMap<i64, String>` built from `step1/ways.raw` (~30-50 MB
    // heap on Belgium). Both expose the same `get(way_id) -> Option<&str>` API.
    pub way_names: WayNames,

    // Distance weights indexed by original EBG node ID (length_mm per edge)
    // Used for isodistance isochrones — same role as ModeData.node_weights but in millimeters
    pub node_weights_dist: Vec<u32>,

    // Per-EBG-edge exclude flags (toll/ferry/motorway), indexed by original EBG edge ID
    pub edge_exclude_flags: Vec<u8>,

    // Bounded LRU cache for avoid_polygons-recustomized weights.
    // Keyed by (mode, polygon_hash, exclude_mask). Each entry is
    // ~100-200 MB on Belgium — capacity defaults to 8 (~1.6 GB cap),
    // overridable via the BUTTERFLY_AVOID_CACHE_CAP env var. Cache
    // hits drop avoid_polygons latency from ~30 s to ~5 ms. See
    // server/avoid.rs::AvoidWeightCache.
    pub avoid_cache: super::avoid::AvoidWeightCache,

    // Optional transit (public transport) state
    pub transit: Option<crate::transit::TransitState>,

    // Server metadata
    pub started_at: std::time::Instant,
    pub data_dir: String,

    /// Live mmap kept alive for the server's lifetime when the data
    /// source was a `.butterfly` container. Format readers in this
    /// crate produce owning `Vec`s, so this is currently *not* required
    /// for correctness — but holding the Arc keeps the OS file backing
    /// pinned for any future zero-copy reader and for demand-paged
    /// access patterns. `None` when loaded from a directory.
    pub _mmap_arc: Option<std::sync::Arc<memmap2::Mmap>>,

    /// Lazy-CRC handle (#160). Tracks per-section verification state and
    /// gates request-time access for sections that have not yet had
    /// their CRC walked. `None` when loaded from a directory tree (the
    /// directory loader has no manifest CRCs to defer).
    ///
    /// The handle is read by:
    /// - the `/health` handler, to report aggregate verification status,
    /// - the corrupt-section integration test, to gate access on
    ///   `Failed` and produce 503 responses,
    /// - the `--warmup-on-boot` background task, to drive verification
    ///   off the request path.
    pub lazy: Option<std::sync::Arc<crate::formats::lazy_verify::LazyContainer>>,
}

/// Options controlling how a container is loaded. Lifted into a struct
/// so we can extend without churning every call site.
#[derive(Debug, Clone, Default)]
pub struct LoadOptions {
    /// If true, every section CRC is walked at boot (legacy behaviour).
    /// If false (default after #160), CRC walks are deferred to first
    /// access via the [`crate::formats::lazy_verify::LazyContainer`]
    /// gate; an optional background warmup pass can be requested via
    /// `warmup_on_boot`.
    pub eager_verify: bool,

    /// If true, schedule a background thread after boot to walk every
    /// still-`Unverified` section's CRC in parallel. Matches pre-#160
    /// total-coverage at the cost of a transient per-section page fault
    /// burst, but does not block the listener.
    pub warmup_on_boot: bool,
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

        crate::server::rss::checkpoint("load.shared");

        // Track each base mode's index in `modes_data` so we can later
        // synthesize traffic variants from the same in-memory topology.
        let mut base_mode_idx: HashMap<String, usize> = HashMap::new();

        for (mode_index, mode_name) in discovered_modes.iter().enumerate() {
            // Use GLOBAL index (from full alphabetical discovery) — must match step 4/5 indexing
            let mode = Mode(global_index[mode_name]);
            let mode_data = load_mode_data(
                mode_name, mode, &step5_dir, &step6_dir, &step7_dir, &step8_dir,
            )?;
            tracing::info!(
                mode = mode_name.as_str(),
                index = mode_index,
                filtered_nodes = mode_data.n_filtered_nodes,
                up_edges = mode_data.cch_topo.up_targets.len(),
                "loaded mode data"
            );
            modes_data.push(mode_data);
            base_mode_idx.insert(mode_name.clone(), mode_index);
            mode_lookup.insert(mode_name.clone(), mode_index as u8);
            mode_names.push(mode_name.clone());
            crate::server::rss::checkpoint(&format!("load.mode.{}", mode_name));
        }

        // ---- Traffic variants (#84) ---------------------------------
        // Auto-discover `cch.w.<base>_<variant>.u32` weight files in step8
        // and register each as a synthetic mode `<base>_<variant>` that
        // shares topology with `<base>` but uses the variant weights.
        match discover_traffic_variants(&step8_dir, &discovered_modes) {
            Ok(variants) if !variants.is_empty() => {
                tracing::info!(n_variants = variants.len(), "registering traffic variants");
                for (base, variant) in &variants {
                    let synthetic = format!("{}_{}", base, variant);
                    if mode_lookup.contains_key(&synthetic) {
                        tracing::warn!(
                            mode = synthetic.as_str(),
                            "skipping traffic variant: a base mode with the same name already exists"
                        );
                        continue;
                    }
                    let base_idx = match base_mode_idx.get(base) {
                        Some(i) => *i,
                        None => {
                            tracing::warn!(
                                base = base.as_str(),
                                variant = variant.as_str(),
                                "skipping traffic variant: base mode not loaded"
                            );
                            continue;
                        }
                    };
                    let base_data = &modes_data[base_idx];
                    let variant_data =
                        load_traffic_variant_mode_data(base_data, variant, base, &step8_dir)?;
                    let new_index = modes_data.len();
                    tracing::info!(
                        base = base.as_str(),
                        variant = variant.as_str(),
                        synthetic = synthetic.as_str(),
                        index = new_index,
                        "registered traffic variant"
                    );
                    modes_data.push(variant_data);
                    mode_lookup.insert(synthetic.clone(), new_index as u8);
                    mode_names.push(synthetic.clone());
                    crate::server::rss::checkpoint(&format!("load.mode.{}", synthetic));
                }
            }
            Ok(_) => {
                tracing::info!("no traffic variants found in step8 directory");
            }
            Err(e) => {
                tracing::warn!(error = %e, "traffic variant discovery failed; ignoring");
            }
        }

        // ---- Packed snap index (#154) -------------------------------
        // Always build in memory for the directory path. The container
        // path can read prebuilt sections zero-copy.
        tracing::info!("Building packed snap index (in memory)...");
        let snap_index =
            build_packed_snap_index_inmem(&ebg_nodes, &nbg_geo, &modes_data, &mode_names);
        crate::server::rss::checkpoint("spatial.global");
        for name in &mode_names {
            crate::server::rss::checkpoint(&format!("spatial.mode.{}", name));
        }

        // Load road names from ways.raw for turn-by-turn instructions.
        // Data-dir path always uses the legacy HashMap; the container
        // path (`load_state_from_bundle`) can use the compact mmap
        // index when `shared/way_names_idx` is present (#282).
        tracing::info!("Loading road names...");
        let way_names = WayNames::from_heap(load_way_names(&step1_dir)?);
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

        // ---- Flat edge geometry (#155) ------------------------------
        // Directory-tree path always synthesises from the heap NbgGeo.
        // Containers packed with #155 will use the zero-copy path in
        // `load_from_container` instead.
        tracing::info!("Building flat edge geometry (in memory)...");
        let edge_geom = EdgeGeometry::from_legacy_polylines(&nbg_geo);
        tracing::info!(
            n_edges = edge_geom.n_edges(),
            n_points = edge_geom.n_points(),
            "built edge geometry"
        );
        crate::server::rss::checkpoint("load.edge_geom");

        Ok(Self {
            ebg_nodes,
            ebg_csr,
            nbg_geo,
            edge_geom,
            nbg_node_to_osm,
            modes: modes_data,
            mode_names,
            mode_lookup,
            snap_index,
            elevation,
            way_names,
            node_weights_dist,
            edge_exclude_flags,
            avoid_cache: super::avoid::AvoidWeightCache::default(),
            transit,
            started_at: std::time::Instant::now(),
            data_dir: data_dir.to_string_lossy().to_string(),
            _mmap_arc: None,
            lazy: None,
        })
    }

    /// Load all data from a `.butterfly` container produced by `pack`.
    /// The file is mmapped read-only; per-mode bundles + shared sections
    /// are parsed via the bytes APIs added in #90 phase 5b.
    ///
    /// Mirrors [`ServerState::load`] in every observable respect — the
    /// resulting state is functionally equivalent to loading the same
    /// data from a directory tree, the only difference is the input
    /// format.
    ///
    /// Defaults to **lazy** CRC verification (#160): per-section CRC
    /// walks are deferred to first access. To restore pre-#160 eager
    /// behaviour, use [`Self::load_from_container_with_options`] with
    /// `eager_verify=true`.
    pub fn load_from_container(
        container_path: &Path,
        mode_filter: Option<&[String]>,
    ) -> Result<Self> {
        Self::load_from_container_with_options(container_path, mode_filter, &LoadOptions::default())
    }

    /// Like [`Self::load_from_container`] but takes explicit
    /// [`LoadOptions`]. The lazy / eager / warmup-on-boot toggles are
    /// the entry point for #160's per-section verification policy.
    pub fn load_from_container_with_options(
        container_path: &Path,
        mode_filter: Option<&[String]>,
        opts: &LoadOptions,
    ) -> Result<Self> {
        use crate::formats::lazy_verify::LazyContainer;

        tracing::info!(
            container = %container_path.display(),
            eager_verify = opts.eager_verify,
            warmup_on_boot = opts.warmup_on_boot,
            "loading server state from butterfly.dat container"
        );

        // Open lazily by default; eager_verify forces a full CRC walk
        // up front (matches pre-#160 behaviour).
        //
        // #175: register_pending MUST run BEFORE any verification that
        // calls record_section_verified/_failed, otherwise PENDING goes
        // negative. We always open lazily first so every section is
        // registered as Unverified, register the pending count, then
        // optionally drive the eager full walk through the lazy gate.
        let lazy = LazyContainer::open_lazy(container_path)?;
        let lazy_arc = std::sync::Arc::new(lazy);
        // Register pending count for /metrics. Every section starts in
        // Unverified state (open_lazy never walks); the eager pass below
        // (if enabled) drives them through the verify state machine and
        // emits matching record_section_verified events.
        crate::server::metrics::register_pending(lazy_arc.n_sections());

        if opts.eager_verify {
            tracing::info!("eager CRC verification enabled (legacy boot path)");
            // Walk every section through `verify_now`, which transitions
            // each runtime through the lazy state machine and emits the
            // matching metric events. This keeps register_pending and
            // the recorded counters in sync.
            let names: Vec<String> = lazy_arc.iter_runtimes().map(|(n, _)| n.clone()).collect();
            for name in &names {
                lazy_arc.verify_now(name).with_context(|| {
                    format!(
                        "eager verification of section '{}' in {}",
                        name,
                        container_path.display()
                    )
                })?;
            }
        }

        let mmap = std::sync::Arc::clone(lazy_arc.mmap_arc());
        let container = lazy_arc.container().clone();

        // #296: Container bytes are accessed through the `Arc<Mmap>` held
        // by `lazy_arc`. Format readers now consume `(Arc<Mmap>, offset,
        // len)` triples via their `read_from_mmap_unverified` entry
        // points; each reader holds its own `Arc<Mmap>` clone for the
        // returned struct's lifetime. When `ServerState` drops, every
        // reader's `ArcCow` drops, the strong count hits zero, `Mmap`
        // drops, `munmap` fires, and the kernel reclaims the pages.
        //
        // Pre-#296 this scope leaked a clone of the Arc to obtain
        // `&'static [u8]` views, which permanently pinned the mapping
        // in RSS and defeated the eviction story added in #292. The
        // leak is gone.
        //
        // #160 + #161: per-section CRC is verified through the
        // [`LazyContainer`] gate held by `lazy_arc`. Calling
        // `verify_now` transitions the section through the lazy state
        // machine, drives the metrics counters, and returns once the
        // section is `Verified`. Format readers are then called via
        // their `_unverified` entry points so the section body is walked
        // exactly once on the container load path. For readers that
        // lack an `_unverified` variant the format CRC is still walked,
        // paging the body in twice for those sections; the readers we
        // did upgrade are the largest by far (CCH weights, EBG
        // nodes/CSR, snap index, edge geom, flats).
        //
        // Page-fault footprint after a `section_arc` call — i.e. AFTER
        // LazyContainer's CRC walk:
        //   - `EbgNodesFile`, `EbgCsrFile`, `SnapPointsFile`,
        //     `SnapGridFile`, `EdgeGeomOffsetsFile`,
        //     `EdgeGeomPointsFile`, `ModeIndexFile`, `CchTopoFile`,
        //     `CchWeightsFile`, `SnapMaskFile`, `FilteredEbgFile`,
        //     `UpAdjFlatFile`, `DownReverseAdjFlatFile`,
        //     `DownAdjFlatFile` — all of these read only the section
        //     header (~32-80 bytes) plus a handful of length fields and
        //     hand back `ArcCow::Mmap` views; body pages are paged in
        //     lazily when the slices are subsequently read by routing.
        //   - `NbgGeoFile::read_edges_only_from_bytes` does walk the
        //     full body to populate the edges Vec; an explicit
        //     `madvise(DONTNEED)` immediately after parsing returns
        //     those pages to the kernel.
        let lazy_for_bytes = std::sync::Arc::clone(&lazy_arc);
        let mmap_for_bytes = std::sync::Arc::clone(&mmap);

        // Returns `(Arc<Mmap>, byte_offset, byte_len)` for the
        // `read_from_mmap_unverified` path. Cloning the Arc is cheap
        // (atomic inc). Each format reader holds its own clone so the
        // mapping stays alive as long as any reader does — when
        // `ServerState` drops, every reader's `ArcCow` drops, refcount
        // hits 0, `munmap` fires.
        let section_arc = |name: &str| -> Result<(std::sync::Arc<memmap2::Mmap>, usize, usize)> {
            let entry = container
                .get(name)
                .ok_or_else(|| anyhow::anyhow!("missing required section '{}'", name))?;
            let off = entry.offset as usize;
            let len = entry.len as usize;
            // Use checked_add so a malformed container with
            // pathologically large offset+len cannot wrap usize and
            // bypass the bounds check.
            let _end = off.checked_add(len).ok_or_else(|| {
                anyhow::anyhow!(
                    "section '{}' offset+len overflows usize (off={}, len={})",
                    name,
                    off,
                    len
                )
            })?;
            anyhow::ensure!(
                off + len <= mmap_for_bytes.len(),
                "section '{}' bytes [{},{}) exceed mmap len {}",
                name,
                off,
                off + len,
                mmap_for_bytes.len()
            );
            // Drive lazy CRC verification through LazyContainer. The
            // first call to `verify_now` walks the section body once;
            // subsequent calls observe `Verified` and short-circuit.
            // This both updates `butterfly_route_sections_*` metrics
            // and lets format readers skip their own body CRC walk
            // via the `_unverified` entry points.
            lazy_for_bytes.verify_now(name)?;
            Ok((std::sync::Arc::clone(&mmap_for_bytes), off, len))
        };
        // Byte-slice accessors borrowed from the live `Arc<Mmap>`.
        // Lifetimes are tied to `mmap_for_bytes` (not `'static`), used
        // by `madvise(DONTNEED)` callers and the non-zero-copy readers
        // that still consume `&[u8]` directly (NbgGeoFile, WaysFile,
        // way_attrs, mod_weights).
        let section_bytes = |name: &str| -> Result<&[u8]> {
            let entry = container
                .get(name)
                .ok_or_else(|| anyhow::anyhow!("missing required section '{}'", name))?;
            let off = entry.offset as usize;
            let len = entry.len as usize;
            anyhow::ensure!(
                off + len <= mmap_for_bytes.len(),
                "section '{}' bytes [{},{}) exceed mmap len {}",
                name,
                off,
                off + len,
                mmap_for_bytes.len()
            );
            lazy_for_bytes.verify_now(name)?;
            Ok(&mmap_for_bytes[off..off + len])
        };
        let optional_section = |name: &str| -> Result<Option<&[u8]>> {
            match container.get(name) {
                Some(entry) => {
                    let off = entry.offset as usize;
                    let len = entry.len as usize;
                    let _end = off.checked_add(len).ok_or_else(|| {
                        anyhow::anyhow!(
                            "section '{}' offset+len overflows usize (off={}, len={})",
                            name,
                            off,
                            len
                        )
                    })?;
                    anyhow::ensure!(
                        off + len <= mmap_for_bytes.len(),
                        "section '{}' bytes [{},{}) exceed mmap len {}",
                        name,
                        off,
                        off + len,
                        mmap_for_bytes.len()
                    );
                    lazy_for_bytes.verify_now(name)?;
                    Ok(Some(&mmap_for_bytes[off..off + len]))
                }
                None => Ok(None),
            }
        };

        // ---- Shared graph tables ------------------------------------
        // #152: ebg.nodes / ebg.csr are now read zero-copy. The
        // numeric arrays (`nodes`, `offsets`, `heads`, `turn_idx`)
        // borrow straight from the mmap, so we save ~250 MB of heap
        // on Belgium that the legacy owning-Vec readers used to copy.
        crate::server::rss::checkpoint("load.container.opened");

        tracing::info!("Loading EBG nodes (zero-copy)...");
        // #161: LazyContainer already CRC-verified the section bytes;
        // skip the per-format CRC walk to avoid paging the body twice.
        let (m, off, len) = section_arc("shared/ebg.nodes")?;
        let ebg_nodes = EbgNodesFile::read_from_mmap_unverified(m, off, len)?;
        tracing::info!(nodes = ebg_nodes.n_nodes, "loaded EBG nodes");

        tracing::info!("Loading EBG CSR (zero-copy)...");
        let (m, off, len) = section_arc("shared/ebg.csr")?;
        let ebg_csr_bytes = &mmap_for_bytes[off..off + len];
        let ebg_csr = EbgCsrFile::read_from_mmap_unverified(m, off, len)?;
        tracing::info!(arcs = ebg_csr.n_arcs, "loaded EBG CSR");
        // #152: ebg.csr is build/validate-only at serve time. The only
        // field any handler reads is `n_arcs` (a u64 in the header used
        // by /health). The body arrays (offsets, heads, turn_idx) are
        // touched by validate/step4 + ordering/contraction, none of
        // which run on the serve path. Drop the file pages from RSS;
        // the borrowed ArcCow slices stay valid (the Arc<Mmap> is still
        // alive) and a rare cold reader pages them back at fault cost.
        if let Err(e) = crate::formats::mmap::madvise_dontneed(ebg_csr_bytes) {
            tracing::warn!(
                section = "shared/ebg.csr",
                error = %e,
                "madvise(DONTNEED) on ebg.csr failed; ignoring"
            );
        } else {
            tracing::info!(
                section = "shared/ebg.csr",
                bytes = ebg_csr_bytes.len(),
                "madvise(DONTNEED) on cold ebg.csr section"
            );
        }

        // ---- NBG geo ----
        // If the container carries the flat edge geometry sections (#155),
        // we read NBG geo edges-only and let the polyline body stay on
        // disk. The new sections back the serve-path geometry hot
        // consumers; nothing downstream reads `nbg_geo.polylines` once
        // EdgeGeometry is wired below.
        let nbg_geo_section = section_bytes("shared/nbg.geo")?;
        let has_flat_edge_geom = container.get("shared/edge_geom_offsets").is_some()
            && container.get("shared/edge_geom_points").is_some();
        let nbg_geo = if has_flat_edge_geom {
            tracing::info!("Loading NBG geo (edges-only — polylines via flat sections)...");
            NbgGeoFile::read_edges_only_from_bytes(nbg_geo_section)?
        } else {
            tracing::info!("Loading NBG geo (full polylines — no flat sections)...");
            NbgGeoFile::read_from_bytes(nbg_geo_section)?
        };
        tracing::info!(edges = nbg_geo.edges.len(), "loaded NBG geo");

        // When we read edges-only, the polyline body bytes have been
        // streamed through the CRC verifier but never copied onto the
        // heap. Hint the kernel to drop those pages from RSS — the bytes
        // are cold under steady-state operation (the flat sections carry
        // the serve-path representation), so freeing them yields the
        // bulk of #155's RSS win.
        if has_flat_edge_geom {
            if let Err(e) = crate::formats::mmap::madvise_dontneed(nbg_geo_section) {
                tracing::warn!(
                    section = "shared/nbg.geo",
                    error = %e,
                    "madvise(DONTNEED) on nbg.geo failed; ignoring"
                );
            } else {
                tracing::info!(
                    section = "shared/nbg.geo",
                    bytes = nbg_geo_section.len(),
                    "madvise(DONTNEED) on cold nbg.geo section (polylines live in flat sections)"
                );
            }
        }

        tracing::info!("Loading NBG node-id map...");
        let nbg_node_map =
            NbgNodeMapFile::read_map_from_bytes(section_bytes("shared/nbg.node_map")?)?;
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

        // ---- Mode discovery + filter --------------------------------
        let all_modes = container.list_modes();
        if all_modes.is_empty() {
            anyhow::bail!(
                "container {} has no `mode/<name>/...` bundles; cannot serve",
                container_path.display()
            );
        }
        let global_index: HashMap<String, u8> = all_modes
            .iter()
            .enumerate()
            .map(|(i, n)| (n.clone(), i as u8))
            .collect();
        let discovered_modes: Vec<String> = if let Some(filter) = mode_filter {
            all_modes
                .into_iter()
                .filter(|m| filter.iter().any(|f| f == m))
                .collect()
        } else {
            all_modes
        };
        if discovered_modes.is_empty() {
            anyhow::bail!("mode filter excluded every mode in the container");
        }
        tracing::info!(modes = ?discovered_modes, "discovered transport modes");

        crate::server::rss::checkpoint("load.shared");

        // ---- Per-mode bundle load -----------------------------------
        let mut modes_data = Vec::with_capacity(discovered_modes.len());
        let mut mode_names = Vec::with_capacity(discovered_modes.len());
        let mut mode_lookup = HashMap::with_capacity(discovered_modes.len());

        for (mode_index, mode_name) in discovered_modes.iter().enumerate() {
            let mode = Mode(global_index[mode_name]);
            let mode_data = load_mode_data_from_bundle(
                mode_name,
                mode,
                &container,
                &mmap_for_bytes,
                &lazy_arc,
            )?;
            tracing::info!(
                mode = mode_name.as_str(),
                index = mode_index,
                filtered_nodes = mode_data.n_filtered_nodes,
                up_edges = mode_data.cch_topo.up_targets.len(),
                "loaded mode bundle"
            );
            modes_data.push(mode_data);
            mode_lookup.insert(mode_name.clone(), mode_index as u8);
            mode_names.push(mode_name.clone());
            crate::server::rss::checkpoint(&format!("load.mode.{}", mode_name));
        }

        // ---- Packed snap index (#154) -------------------------------
        // Prefer mmap-backed sections from the container; fall back to
        // building the legacy rstar in heap memory when the container
        // pre-dates #154.
        let snap_index = match try_load_packed_snap_index(
            &container,
            &mmap_for_bytes,
            &mode_names,
            &lazy_arc,
        )? {
            Some(idx) => {
                tracing::info!(
                    n_points = idx.n_indexed(),
                    "loaded packed snap index zero-copy"
                );
                crate::server::rss::checkpoint("spatial.global");
                for name in &mode_names {
                    crate::server::rss::checkpoint(&format!("spatial.mode.{}", name));
                }
                idx
            }
            None => {
                tracing::warn!(
                    "packed snap index sections missing; building rstar at boot \
                         (this container pre-dates #154 — re-pack to drop ~1 GB anon)"
                );
                let idx =
                    build_packed_snap_index_inmem(&ebg_nodes, &nbg_geo, &modes_data, &mode_names);
                crate::server::rss::checkpoint("spatial.global");
                for name in &mode_names {
                    crate::server::rss::checkpoint(&format!("spatial.mode.{}", name));
                }
                idx
            }
        };

        // #149: Now that every mode's flat adjacencies are built, hint
        // the kernel that the cch_weights.{time,dist} byte ranges are
        // cold. The routing hot path (CchQuery, isochrone PHAST,
        // matrix bucket M2M) reads weights through the flats; the only
        // remaining `cch_weights.up`/`.down` readers are
        //   - the transit fingerprint hash (one-time, at startup)
        //   - the per-call exclude/avoid recustomizers (cold)
        //   - validators / bench harness (off the production path)
        // so dropping these pages from RSS is a pure win. The Cow
        // slices into them remain valid; subsequent rare reads page
        // them back in at standard fault cost.
        for mode_name in &discovered_modes {
            for leaf in ["weights.time", "weights.dist"] {
                let section = format!("mode/{}/{}", mode_name, leaf);
                if let Some(entry) = container.get(&section) {
                    let off = entry.offset as usize;
                    let len = entry.len as usize;
                    let range = &mmap_for_bytes[off..off + len];
                    match crate::formats::mmap::madvise_dontneed(range) {
                        Ok(()) => tracing::info!(
                            section = %section,
                            bytes = len,
                            "madvise(DONTNEED) on cold weight section"
                        ),
                        Err(e) => tracing::warn!(
                            section = %section,
                            error = %e,
                            "madvise(DONTNEED) failed, ignoring"
                        ),
                    }
                }
            }
        }

        // #279: evict TIME flats for non-default modes too.
        //
        // Each mode loads three time-metric flat sections
        // (up_adj_flat.time, down_reverse_adj_flat.time,
        // down_adj_flat.time). They are zero-copy Cow::Borrowed views
        // and the boot-time CRC walk forces them resident. For
        // workloads dominated by one mode (almost always car on
        // consumer routing, truck on delivery, etc.) the non-default
        // modes' flats sit resident for nothing.
        //
        // Pick the same "default" mode as the exclude-flag loader
        // (car if present, otherwise the first discovered mode). Evict
        // every other mode's time flats. madvise on a zero-copy view's
        // backing pages is safe — the view stays valid; kernel
        // demand-pages on first query of that mode.
        //
        // On Belgium with 4 modes (car/bike/foot/truck) this evicts
        // ~3 × 1.6 GiB ≈ 5 GiB of flats. First bike/foot/truck query
        // pays one cold page-in pass; the kernel keeps subsequent
        // queries hot via its page cache.
        let default_mode = if discovered_modes.iter().any(|m| m == "car") {
            "car"
        } else {
            discovered_modes[0].as_str()
        };
        for mode_name in &discovered_modes {
            if mode_name == default_mode {
                continue;
            }
            for leaf in [
                "up_adj_flat.time",
                "down_reverse_adj_flat.time",
                "down_adj_flat.time",
            ] {
                let section = format!("mode/{}/{}", mode_name, leaf);
                if let Some(entry) = container.get(&section) {
                    let off = entry.offset as usize;
                    let len = entry.len as usize;
                    // Use checked_add so corrupted container metadata
                    // can't overflow usize and silently bypass the
                    // bounds check.
                    let end = match off.checked_add(len) {
                        Some(e) => e,
                        None => {
                            tracing::warn!(
                                section = %section,
                                offset = off,
                                len = len,
                                "container section offset+len overflows usize; skipping madvise"
                            );
                            continue;
                        }
                    };
                    if end > mmap_for_bytes.len() {
                        tracing::warn!(
                            section = %section,
                            offset = off,
                            len = len,
                            mmap_len = mmap_for_bytes.len(),
                            "container section out-of-bounds vs mmap; skipping madvise"
                        );
                        continue;
                    }
                    let range = &mmap_for_bytes[off..end];
                    if let Err(e) = crate::formats::mmap::madvise_dontneed(range) {
                        tracing::warn!(
                            section = %section,
                            error = %e,
                            "madvise(DONTNEED) on non-default flat failed; ignoring"
                        );
                    } else {
                        tracing::info!(
                            section = %section,
                            bytes = len,
                            "madvise(DONTNEED) on non-default mode time flat (#279)"
                        );
                    }
                }
            }
        }

        // ---- Road names ---------------------------------------------
        // #282: prefer the compact mmap-backed `shared/way_names_idx`
        // section (~5-10 KB heap on Belgium, scales to ~3 GiB saved at
        // planet scale). Fall back to the legacy
        // `shared/step1.ways.raw` HashMap build (~30-50 MB heap on
        // Belgium) for containers that pre-date the index.
        tracing::info!("Loading road names from container...");
        // PR #324 review: do NOT call `lazy_for_bytes.verify_now` here.
        // A synchronous verify walks the full ~19 MiB way_names_idx
        // body, paging it in at boot, which defeats the
        // demand-paged-mmap goal of the lazy index. The lazy header
        // (magic / version / sizes) is still validated by
        // `read_from_mmap_unverified` below; the body CRC stays
        // deferred. Operators that want eager body CRC can opt in
        // via `--warmup-on-boot` or `--eager-verify`, which the
        // existing `LazyContainer::spawn_warmup` path already covers.
        let way_names = if let Some(entry) = container.get("shared/way_names_idx") {
            let off = entry.offset as usize;
            let len = entry.len as usize;
            let idx = crate::formats::way_names_idx::read_from_mmap_unverified(
                std::sync::Arc::clone(&mmap_for_bytes),
                off,
                len,
            )?;
            tracing::info!(
                source = "shared/way_names_idx",
                named_roads = idx.len(),
                "loaded road names (mmap-backed, body CRC deferred to warmup/eager flags)"
            );
            WayNames::Idx(idx)
        } else if let Some(ways_bytes) = optional_section("shared/step1.ways.raw")? {
            let names = load_way_names_from_bytes(ways_bytes)?;
            if let Err(e) = crate::formats::mmap::madvise_dontneed(ways_bytes) {
                tracing::warn!(
                    section = "shared/step1.ways.raw",
                    error = %e,
                    "madvise(DONTNEED) on ways.raw failed; ignoring"
                );
            } else {
                tracing::info!(
                    section = "shared/step1.ways.raw",
                    bytes = ways_bytes.len(),
                    "madvise(DONTNEED) on cold ways.raw section"
                );
            }
            tracing::info!(
                source = "shared/step1.ways.raw",
                named_roads = names.len(),
                "loaded road names (heap HashMap fallback)"
            );
            WayNames::Heap(names)
        } else {
            tracing::warn!("no way_names section in container, road names unavailable");
            WayNames::Heap(HashMap::new())
        };

        // ---- Edge exclude flags from one mode's way_attrs -----------
        // #275: way_attrs is read once at boot to build the per-edge
        // exclude flag table. The flags live in a heap Vec from that
        // point on; the mmap'd byte range (this mode's plus every other
        // mode's, all forced resident by the boot CRC walk) is cold for
        // the rest of the process lifetime. Drop those pages too.
        //
        // Prefer car if available, otherwise the alphabetically first mode.
        let attrs_mode = if discovered_modes.iter().any(|m| m == "car") {
            "car".to_string()
        } else {
            discovered_modes[0].clone()
        };
        let attrs_section = format!("mode/{}/way_attrs", attrs_mode);
        let edge_exclude_flags = if let Some(attr_bytes) = optional_section(&attrs_section)? {
            let attrs = crate::formats::way_attrs::read_all_from_bytes(attr_bytes)?;
            let flags = exclude::build_edge_exclude_flags_from_attrs(&ebg_nodes, &attrs)?;
            if let Err(e) = crate::formats::mmap::madvise_dontneed(attr_bytes) {
                tracing::warn!(
                    section = %attrs_section,
                    error = %e,
                    "madvise(DONTNEED) on way_attrs failed; ignoring"
                );
            } else {
                tracing::info!(
                    section = %attrs_section,
                    bytes = attr_bytes.len(),
                    "madvise(DONTNEED) on cold way_attrs section"
                );
            }
            flags
        } else {
            tracing::warn!(section = %attrs_section, "way_attrs absent, exclude feature disabled");
            vec![0u8; ebg_nodes.n_nodes as usize]
        };

        // Evict the other modes' way_attrs sections too — only one mode
        // supplies the exclude flags, the rest stay cold forever.
        //
        // Important: resolve byte ranges via `container.get(..)` +
        // `static_bytes[..]` directly. Do NOT route through
        // `optional_section(..)` because that calls
        // `lazy.verify_now(name)`, which would force a full CRC walk
        // (and page-in) of every other mode's way_attrs at boot —
        // defeating lazy verification. `madvise(MADV_DONTNEED)` is
        // safe on unverified bytes: it evicts resident pages (a no-op
        // on pages that were never faulted in).
        for other_mode in &discovered_modes {
            if other_mode == &attrs_mode {
                continue;
            }
            let other_section = format!("mode/{}/way_attrs", other_mode);
            let Some(entry) = container.get(&other_section) else {
                continue;
            };
            let off = entry.offset as usize;
            let len = entry.len as usize;
            let Some(end) = off.checked_add(len) else {
                tracing::warn!(
                    section = %other_section,
                    offset = off,
                    len = len,
                    "way_attrs section offset+len overflows usize; skipping evict"
                );
                continue;
            };
            if end > mmap_for_bytes.len() {
                tracing::warn!(
                    section = %other_section,
                    "way_attrs section bytes exceed mmap; skipping evict"
                );
                continue;
            }
            let other_bytes = &mmap_for_bytes[off..end];
            if let Err(e) = crate::formats::mmap::madvise_dontneed(other_bytes) {
                tracing::warn!(
                    section = %other_section,
                    error = %e,
                    "madvise(DONTNEED) on way_attrs failed; ignoring"
                );
            } else {
                tracing::info!(
                    section = %other_section,
                    bytes = other_bytes.len(),
                    "madvise(DONTNEED) on cold way_attrs section (no verify)"
                );
            }
        }

        let node_weights_dist: Vec<u32> = ebg_nodes.nodes.iter().map(|n| n.length_mm).collect();

        // ---- Optional SRTM (looked up next to the container file) --
        let srtm_dir = container_path
            .parent()
            .map(|p| p.join("srtm"))
            .unwrap_or_else(|| std::path::PathBuf::from("srtm"));
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
            None
        };

        // ---- Flat edge geometry (#155) ------------------------------
        // Prefer mmap-backed sections from the container; fall back to
        // building the flat layout from the heap NbgGeo polylines when
        // the container pre-dates #155.
        //
        // The dispatch matches the `has_flat_edge_geom` check used above
        // for the NBG geo edges-only loader: if the sections existed at
        // open time they're still there now, so the back-compat branch
        // is for old containers that loaded the full NbgGeo.
        let edge_geom = if has_flat_edge_geom {
            let eg = try_load_edge_geometry(&container, &mmap_for_bytes, &lazy_arc)?.ok_or_else(
                || {
                    anyhow::anyhow!(
                        "edge_geom sections vanished between open and load — container corrupt?"
                    )
                },
            )?;
            tracing::info!(
                n_edges = eg.n_edges(),
                n_points = eg.n_points(),
                "loaded flat edge geometry zero-copy"
            );
            eg
        } else {
            tracing::warn!(
                "flat edge geometry sections missing; building from heap polylines \
                 at boot (this container pre-dates #155 — re-pack to drop ~544 MB anon)"
            );
            EdgeGeometry::from_legacy_polylines(&nbg_geo)
        };
        crate::server::rss::checkpoint("load.edge_geom");

        // #160: optionally schedule a background warmup pass to walk
        // every still-`Unverified` section's CRC in parallel. This
        // matches pre-#160 total-coverage at the cost of a transient
        // page-fault burst, but does NOT block the listener.
        if opts.warmup_on_boot {
            tracing::info!("scheduling background CRC warmup pass for unverified sections");
            lazy_arc.spawn_warmup();
        }

        Ok(Self {
            ebg_nodes,
            ebg_csr,
            nbg_geo,
            edge_geom,
            nbg_node_to_osm,
            modes: modes_data,
            mode_names,
            mode_lookup,
            snap_index,
            elevation,
            way_names,
            node_weights_dist,
            edge_exclude_flags,
            avoid_cache: super::avoid::AvoidWeightCache::default(),
            transit: None,
            started_at: std::time::Instant::now(),
            data_dir: container_path.to_string_lossy().to_string(),
            _mmap_arc: Some(mmap),
            lazy: Some(lazy_arc),
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
            &mode_data.filtered_to_original,
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
            && !mode_name.is_empty()
        {
            mode_names.push(mode_name.to_string());
        }
    }

    // Sort alphabetically for deterministic indexing
    mode_names.sort();
    mode_names.dedup();

    Ok(mode_names)
}

/// Discover traffic variants by scanning step8 for `cch.w.<base>_<variant>.u32`
/// files where `<base>` matches a known base mode and a sibling
/// `cch.w.<base>_<variant>.traffic.json` exists for provenance.
///
/// Returns `(base_mode, variant)` pairs sorted by `(base, variant)`. Files
/// without the sibling provenance JSON are skipped with a warning.
pub(crate) fn discover_traffic_variants(
    step8_dir: &Path,
    base_modes: &[String],
) -> Result<Vec<(String, String)>> {
    let mut variants: Vec<(String, String)> = Vec::new();

    // Sort longest-first so a base mode like "car" matches before "ca" when
    // scanning a synthetic name "car_rush_hour".
    let mut bases_sorted: Vec<&str> = base_modes.iter().map(String::as_str).collect();
    bases_sorted.sort_by_key(|s| std::cmp::Reverse(s.len()));

    for entry in std::fs::read_dir(step8_dir)
        .with_context(|| format!("Failed to read {}", step8_dir.display()))?
    {
        let entry = entry?;
        let name = entry.file_name();
        let name_str = name.to_string_lossy();

        // Pattern: cch.w.{base}_{variant}.u32
        let stem = match name_str
            .strip_prefix("cch.w.")
            .and_then(|s| s.strip_suffix(".u32"))
        {
            Some(s) => s,
            None => continue,
        };

        // Try to split <base>_<variant> by trying every known base mode as a prefix.
        let mut matched: Option<(String, String)> = None;
        for base in &bases_sorted {
            if let Some(rest) = stem.strip_prefix(*base)
                && let Some(variant) = rest.strip_prefix('_')
                && !variant.is_empty()
            {
                matched = Some(((*base).to_string(), variant.to_string()));
                break;
            }
        }
        let (base, variant) = match matched {
            Some(t) => t,
            None => continue, // Plain `cch.w.<base>.u32` — handled by base modes.
        };

        // Provenance check: refuse to expose a variant without its sibling
        // .traffic.json so a stray weight file from a previous experiment
        // can't accidentally pollute the live mode set.
        let provenance = step8_dir.join(format!("cch.w.{}_{}.traffic.json", base, variant));
        if !provenance.exists() {
            tracing::warn!(
                base = base.as_str(),
                variant = variant.as_str(),
                provenance = %provenance.display(),
                "skipping traffic variant: missing sibling .traffic.json"
            );
            continue;
        }

        variants.push((base, variant));
    }

    variants.sort();
    variants.dedup();
    Ok(variants)
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

/// Build a synthetic `ModeData` for a traffic variant by reusing the base
/// mode's topology + distance structures and loading just the variant
/// `cch.w.<base>_<variant>.u32` file.
///
/// This is the runtime side of the step-8 traffic recustomization: weights
/// change but topology, distance, masks, accessibility, and adjacency
/// structure are all identical to the base mode.
fn load_traffic_variant_mode_data(
    base: &ModeData,
    variant_name: &str,
    base_mode_name: &str,
    step8_dir: &Path,
) -> Result<ModeData> {
    let weights_path = step8_dir.join(format!("cch.w.{}_{}.u32", base_mode_name, variant_name));
    let cch_weights = CchWeightsFile::read(&weights_path)
        .with_context(|| format!("loading traffic variant weights {}", weights_path.display()))?;

    // Rebuild the TIME flats against the new weights — they're the only
    // thing that depends on cch_weights. Distance flats and topology are
    // shared with the base by clone (Cow::clone is cheap for borrowed
    // sections; for owned Vecs it duplicates, see comment above).
    let up_adj_flat = UpAdjFlat::build_with(&base.cch_topo, &cch_weights, true);
    let down_rev_flat = DownReverseAdjFlat::build_with(&base.cch_topo, &cch_weights, true);
    let down_adj_flat = DownAdjFlat::build(&base.cch_topo, &cch_weights);

    Ok(ModeData {
        mode: base.mode,
        cch_topo: base.cch_topo.clone(),
        cch_weights,
        cch_weights_dist: base.cch_weights_dist.clone(),
        orig_to_rank: base.orig_to_rank.clone(),
        filtered_to_original: base.filtered_to_original.clone(),
        n_filtered_nodes: base.n_filtered_nodes,
        n_original_nodes: base.n_original_nodes,
        node_weights: base.node_weights.clone(),
        mask: base.mask.clone(),
        has_outbound: base.has_outbound.clone(),
        has_inbound: base.has_inbound.clone(),
        up_adj_flat,
        down_rev_flat,
        down_adj_flat,
        up_adj_flat_dist: base.up_adj_flat_dist.clone(),
        down_rev_flat_dist: base.down_rev_flat_dist.clone(),
        down_adj_flat_dist: base.down_adj_flat_dist.clone(),
        exclude_cache: parking_lot::RwLock::new(HashMap::new()),
    })
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

    // Load node weights from step 5 (indexed by original EBG node ID)
    let weights_path = step5_dir.join(format!("w.{}.u32", mode_name));
    let weights_data = mod_weights::read_all(&weights_path)?;

    // Build the base snap mask from the mode-filtered EBG. Directional
    // role masks below further restrict candidates to nodes connected
    // to the main routing core.
    let n_original = filtered_ebg.n_original_nodes as usize;
    let mask = {
        let n_words = n_original.div_ceil(64);
        let mut m = vec![0u64; n_words];
        for &orig_id in filtered_ebg.filtered_to_original.iter() {
            let word = orig_id as usize / 64;
            let bit = orig_id as usize % 64;
            m[word] |= 1u64 << bit;
        }
        m
    };

    // Load CCH weights from step 8
    let cch_weights_path = step8_dir.join(format!("cch.w.{}.u32", mode_name));
    let cch_weights = CchWeightsFile::read(&cch_weights_path)?;

    // Build flat adjacencies for bucket M2M - TIME metric (pre-filtered for INF, embedded weights).
    // TIME flats carry topo_edge_idx because CchQuery's parent pointers need it.
    let up_adj_flat = UpAdjFlat::build_with(&cch_topo, &cch_weights, true);
    let down_rev_flat = DownReverseAdjFlat::build_with(&cch_topo, &cch_weights, true);
    let down_adj_flat = DownAdjFlat::build(&cch_topo, &cch_weights);

    // Load pre-computed distance weights from step 8 (cch.d.{mode}.u32)
    let cch_dist_weights_path = step8_dir.join(format!("cch.d.{}.u32", mode_name));
    tracing::info!(mode = mode_name, "loading distance weights");
    let cch_weights_dist = CchWeightsFile::read(&cch_dist_weights_path)?;
    // DIST flats: only PHAST forward + isodistance use them — no topo back-ref needed.
    let up_adj_flat_dist = UpAdjFlat::build(&cch_topo, &cch_weights_dist);
    let down_rev_flat_dist = DownReverseAdjFlat::build(&cch_topo, &cch_weights_dist);
    let down_adj_flat_dist = DownAdjFlat::build(&cch_topo, &cch_weights_dist);

    // ---- Build server-only mappings (#153) ----------------------
    // The `--data-dir` path always synthesises these from the legacy
    // structs at boot. Container path prefers the dedicated sections;
    // see `load_mode_data_from_bundle`.
    let n_original_nodes = filtered_ebg.n_original_nodes;
    let n_filtered_nodes = filtered_ebg.n_filtered_nodes;
    let orig_to_rank = build_orig_to_rank(&filtered_ebg, &order);
    let filtered_to_original: Vec<u32> = filtered_ebg.filtered_to_original.to_vec();

    // Build role-aware snap bitsets (#197) from the same filtered EBG.
    // The filtered EBG already encodes both node-level mode access AND
    // per-arc turn-table mode masking. We also require connectivity to
    // the main routing core so primary snaps do not land in isolated
    // components that force per-cell matrix fallback.
    let (has_outbound, has_inbound) = build_role_masks(&filtered_ebg);

    Ok(ModeData {
        mode,
        cch_topo,
        cch_weights,
        cch_weights_dist,
        orig_to_rank: crate::formats::ArcCow::from_vec(orig_to_rank),
        filtered_to_original: crate::formats::ArcCow::from_vec(filtered_to_original),
        n_filtered_nodes,
        n_original_nodes,
        node_weights: weights_data.weights,
        mask,
        has_outbound,
        has_inbound,
        up_adj_flat,
        down_rev_flat,
        down_adj_flat,
        up_adj_flat_dist,
        down_rev_flat_dist,
        down_adj_flat_dist,
        exclude_cache: parking_lot::RwLock::new(HashMap::new()),
    })
}

/// Build per-mode source and destination snap bitsets indexed by
/// **original** EBG node id, from the mode's `FilteredEbg`.
///
/// The filtered EBG already encodes both node-level mode accessibility
/// and per-arc turn-table mode masking. A source candidate must also be
/// able to reach the largest SCC, and a destination candidate must be
/// reachable from that SCC. This preserves directed endpoint stubs
/// (sources can be outside the core if they can drive/walk into it;
/// destinations can be outside the core if the core can reach them)
/// while filtering isolated small SCCs that otherwise look valid under
/// a plain outbound/inbound test and poison matrix primary snaps.
///
/// Fixes #197: directional snap asymmetry. The legacy snap returned
/// the geometrically-closest mode-eligible EBG node without checking
/// whether that node could be a starting state (src role: needs
/// outbound) or a terminal state (dst role: needs inbound). On
/// directional roads (one-way exit ramps, motorway slip roads) the
/// closest sample to a point can lie on the "wrong-side" EBG node,
/// causing /route to 404 in one direction even though OSRM finds the
/// route. Bike/foot are effectively undirected so they were unaffected
/// in practice; car was 15.6 % broken on the Belgium correctness sweep.
fn build_role_masks(filtered_ebg: &crate::formats::FilteredEbg) -> (Vec<u64>, Vec<u64>) {
    let n_orig = filtered_ebg.n_original_nodes as usize;
    let n_words = n_orig.div_ceil(64);
    let f2o = filtered_ebg.filtered_to_original.as_ref();
    let offsets = filtered_ebg.offsets.as_ref();
    let heads = filtered_ebg.heads.as_ref();
    let n_filt = f2o.len();

    let mut has_outbound_f = vec![false; n_filt];
    let mut has_inbound_f = vec![false; n_filt];

    for filt_id in 0..n_filt {
        let start = offsets[filt_id] as usize;
        let end = offsets[filt_id + 1] as usize;
        if end > start {
            has_outbound_f[filt_id] = true;
        }
        for &head_filt in &heads[start..end] {
            let head = head_filt as usize;
            if head < n_filt {
                has_inbound_f[head] = true;
            }
        }
    }

    let reverse = build_reverse_csr(n_filt, offsets, heads);
    let core = largest_scc_mask(n_filt, offsets, heads, &reverse);
    let can_reach_core = flood_from_seeds(n_filt, &reverse.offsets, &reverse.heads, &core);
    let reachable_from_core = flood_from_seeds(n_filt, offsets, heads, &core);

    let mut has_outbound = vec![0u64; n_words];
    let mut has_inbound = vec![0u64; n_words];
    let mut core_nodes = 0usize;
    let mut src_nodes = 0usize;
    let mut dst_nodes = 0usize;

    for (filt_id, &orig_id) in f2o.iter().enumerate() {
        if core[filt_id] {
            core_nodes += 1;
        }
        let oi = orig_id as usize;
        if has_outbound_f[filt_id] && can_reach_core[filt_id] {
            has_outbound[oi / 64] |= 1u64 << (oi % 64);
            src_nodes += 1;
        }
        if has_inbound_f[filt_id] && reachable_from_core[filt_id] {
            has_inbound[oi / 64] |= 1u64 << (oi % 64);
            dst_nodes += 1;
        }
    }

    tracing::info!(
        filtered_nodes = n_filt,
        core_nodes,
        source_snap_nodes = src_nodes,
        destination_snap_nodes = dst_nodes,
        "built connectivity-aware role snap masks"
    );

    (has_outbound, has_inbound)
}

struct ReverseCsr {
    offsets: Vec<u64>,
    heads: Vec<u32>,
}

fn build_reverse_csr(n_nodes: usize, offsets: &[u64], heads: &[u32]) -> ReverseCsr {
    let mut counts = vec![0usize; n_nodes];
    for u in 0..n_nodes {
        let start = offsets[u] as usize;
        let end = offsets[u + 1] as usize;
        for &v in &heads[start..end] {
            let v = v as usize;
            if v < n_nodes {
                counts[v] += 1;
            }
        }
    }

    let mut rev_offsets = Vec::with_capacity(n_nodes + 1);
    let mut acc = 0u64;
    rev_offsets.push(acc);
    for &count in &counts {
        acc += count as u64;
        rev_offsets.push(acc);
    }

    let mut rev_heads = vec![0u32; acc as usize];
    counts.fill(0);
    for u in 0..n_nodes {
        let start = offsets[u] as usize;
        let end = offsets[u + 1] as usize;
        for &v in &heads[start..end] {
            let v = v as usize;
            if v >= n_nodes {
                continue;
            }
            let pos = rev_offsets[v] as usize + counts[v];
            rev_heads[pos] = u as u32;
            counts[v] += 1;
        }
    }

    ReverseCsr {
        offsets: rev_offsets,
        heads: rev_heads,
    }
}

fn largest_scc_mask(
    n_nodes: usize,
    offsets: &[u64],
    heads: &[u32],
    reverse: &ReverseCsr,
) -> Vec<bool> {
    if n_nodes == 0 {
        return Vec::new();
    }

    // Kosaraju, iterative to avoid a multi-million-node recursion stack.
    let mut seen = vec![false; n_nodes];
    let mut finish_order = Vec::with_capacity(n_nodes);
    let mut stack: Vec<(usize, usize)> = Vec::new(); // (node, next edge slot)

    for start in 0..n_nodes {
        if seen[start] {
            continue;
        }
        seen[start] = true;
        stack.push((start, offsets[start] as usize));

        while let Some((u, next)) = stack.last_mut() {
            let end = offsets[*u + 1] as usize;
            if *next < end {
                let v = heads[*next] as usize;
                *next += 1;
                if v < n_nodes && !seen[v] {
                    seen[v] = true;
                    stack.push((v, offsets[v] as usize));
                }
            } else {
                finish_order.push(*u as u32);
                stack.pop();
            }
        }
    }

    let mut assigned = vec![false; n_nodes];
    let mut best_component: Vec<u32> = Vec::new();
    let mut node_stack: Vec<u32> = Vec::new();

    for &start in finish_order.iter().rev() {
        let start_usize = start as usize;
        if assigned[start_usize] {
            continue;
        }

        let mut component: Vec<u32> = Vec::new();
        assigned[start_usize] = true;
        node_stack.push(start);

        while let Some(u) = node_stack.pop() {
            component.push(u);
            let u = u as usize;
            let start = reverse.offsets[u] as usize;
            let end = reverse.offsets[u + 1] as usize;
            for &v in &reverse.heads[start..end] {
                let v = v as usize;
                if !assigned[v] {
                    assigned[v] = true;
                    node_stack.push(v as u32);
                }
            }
        }

        if component.len() > best_component.len() {
            best_component = component;
        }
    }

    let mut mask = vec![false; n_nodes];
    for node in best_component {
        mask[node as usize] = true;
    }
    mask
}

fn flood_from_seeds(n_nodes: usize, offsets: &[u64], heads: &[u32], seeds: &[bool]) -> Vec<bool> {
    let mut seen = vec![false; n_nodes];
    let mut stack = Vec::new();
    for (node, &is_seed) in seeds.iter().enumerate() {
        if is_seed {
            seen[node] = true;
            stack.push(node as u32);
        }
    }

    while let Some(u) = stack.pop() {
        let u = u as usize;
        let start = offsets[u] as usize;
        let end = offsets[u + 1] as usize;
        for &v in &heads[start..end] {
            let v = v as usize;
            if v < n_nodes && !seen[v] {
                seen[v] = true;
                stack.push(v as u32);
            }
        }
    }

    seen
}

/// Build the composed `orig_to_rank` array from a legacy
/// `(FilteredEbg, OrderEbg)` pair. Used by:
///   - the `--data-dir` loader (always),
///   - the container loader when `mode/<m>/orig_to_rank` is absent
///     (back-compat for pre-#153 containers).
fn build_orig_to_rank(
    filtered_ebg: &crate::formats::FilteredEbg,
    order: &crate::formats::OrderEbg,
) -> Vec<u32> {
    let n_original = filtered_ebg.n_original_nodes as usize;
    let mut out = vec![u32::MAX; n_original];
    for (orig_id, &filt_id) in filtered_ebg.original_to_filtered.iter().enumerate() {
        if filt_id != u32::MAX {
            out[orig_id] = order.perm[filt_id as usize];
        }
    }
    out
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
                    && !name.is_empty()
                {
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

/// Load one flat section from a container with the #150 mmap path.
///
/// #161: per-section CRC verification is performed via the
/// [`crate::formats::lazy_verify::LazyContainer`] gate — `verify_now`
/// transitions the section through the lazy state machine and walks
/// the body once. The format reader is then called via the
/// `_unverified` entry point, so the per-format body CRC walk is
/// elided.
///
/// 1. Look up by name. If absent, fall back to building from
///    `(cch_topo, cch_weights)` so legacy containers keep working.
/// 2. Drive `lazy.verify_now(section_name)`, which walks the body once
///    and updates the lazy CRC metrics.
/// 3. Parse the bytes via the format reader's `_unverified` variant
///    (zero-copy view).
///
/// Note on madvise: a `madvise(DONTNEED)` is **not required for
/// correctness** after parsing — the format reader's `_unverified`
/// entry point only touches the header (~32–80 bytes) and returns
/// `Cow::Borrowed` slices over the body; `bytemuck::cast_slice` is a
/// pointer-only cast and does not page the body in. The body therefore
/// stays cold in the page cache once LazyContainer's CRC walk has
/// completed and any pages it pulled in are reclaimable by the kernel
/// under memory pressure.
///
/// **Callers that want to proactively drop CRC-warmed pages** (e.g.
/// the #277 distance-flat path) call `madvise_section_in_container`
/// after `load_flat_section` returns. This is an RSS optimisation, not
/// a correctness requirement: it pre-evicts the bytes the boot CRC
/// walk pulled resident, instead of waiting for memory pressure.
///
/// `parse` is a closure that turns `(Arc<Mmap>, byte_offset, byte_len)`
/// into the typed flat view via the `read_from_mmap_unverified` reader;
/// `build_owned` is the legacy heap-build fallback for containers that
/// pre-date the prebuilt flat sections.
fn load_flat_section<T, P, B>(
    container: &crate::formats::butterfly_dat::Container,
    mmap: &std::sync::Arc<memmap2::Mmap>,
    section_name: &str,
    lazy: &std::sync::Arc<crate::formats::lazy_verify::LazyContainer>,
    parse: P,
    build_owned: B,
) -> Result<T>
where
    P: FnOnce(std::sync::Arc<memmap2::Mmap>, usize, usize) -> Result<T>,
    B: FnOnce() -> T,
{
    let entry = match container.get(section_name) {
        Some(e) => e,
        None => {
            tracing::info!(section = %section_name, "flat section absent — building owned at boot");
            return Ok(build_owned());
        }
    };
    let off = entry.offset as usize;
    let len = entry.len as usize;
    let _end = off.checked_add(len).ok_or_else(|| {
        anyhow::anyhow!(
            "flat section '{}' offset+len overflows usize (off={}, len={})",
            section_name,
            off,
            len
        )
    })?;
    anyhow::ensure!(
        off + len <= mmap.len(),
        "flat section '{}' bytes [{},{}) exceed mmap len {}",
        section_name,
        off,
        off + len,
        mmap.len()
    );
    // #161: verify CRC via LazyContainer, then read with the unverified
    // format reader to avoid paging the body in twice.
    lazy.verify_now(section_name)?;
    let parsed = parse(std::sync::Arc::clone(mmap), off, len)?;
    Ok(parsed)
}

/// #277 madvise(DONTNEED) on a container section, addressed by name.
/// After Phase 6 un-leak, the mapping is owned by an `Arc<Mmap>` rather
/// than a leaked `'static [u8]` — so the bytes we hand to `madvise` are
/// borrowed from the live `Arc` and the slice lifetime stays tied to it.
///
/// Non-fatal optimisation: an out-of-bounds or overflowing range logs a
/// warning and skips the madvise.
fn madvise_section_in_container(
    container: &crate::formats::butterfly_dat::Container,
    mmap: &std::sync::Arc<memmap2::Mmap>,
    section_name: &str,
) {
    let entry = match container.get(section_name) {
        Some(e) => e,
        None => return,
    };
    let off = entry.offset as usize;
    let len = entry.len as usize;
    let end = match off.checked_add(len) {
        Some(e) => e,
        None => {
            tracing::warn!(
                section = %section_name,
                offset = off,
                len = len,
                "container section offset+len overflows usize; skipping madvise"
            );
            return;
        }
    };
    if end > mmap.len() {
        tracing::warn!(
            section = %section_name,
            offset = off,
            len = len,
            mmap_len = mmap.len(),
            "container section out-of-bounds vs mmap; skipping madvise"
        );
        return;
    }
    let bytes = &mmap[off..end];
    if let Err(e) = crate::formats::mmap::madvise_dontneed(bytes) {
        tracing::warn!(
            section = %section_name,
            error = %e,
            "madvise(DONTNEED) on distance section failed; ignoring"
        );
    } else {
        tracing::info!(
            section = %section_name,
            bytes = len,
            "madvise(DONTNEED) on warm-only distance section (#277)"
        );
    }
}

/// Same as `load_mode_data` but reads from a `.butterfly` container's
/// `mode/<mode>/...` bundle instead of from `step{N}/` directories.
///
/// #160: per-section CRC verification is gated by the
/// [`crate::formats::lazy_verify::LazyContainer`] held by the caller —
/// **not** here. This function only resolves byte ranges. Body pages
/// stay cold until routing traverses them (or the warmup pass /
/// `--eager-verify` walks them off the request path).
fn load_mode_data_from_bundle(
    mode_name: &str,
    mode: Mode,
    container: &crate::formats::butterfly_dat::Container,
    mmap: &std::sync::Arc<memmap2::Mmap>,
    lazy: &std::sync::Arc<crate::formats::lazy_verify::LazyContainer>,
) -> Result<ModeData> {
    // Required section → `(Arc<Mmap>, off, len)` for the
    // `read_from_mmap_unverified` path.
    let fetch_arc = |leaf: &str| -> Result<(std::sync::Arc<memmap2::Mmap>, usize, usize)> {
        let name = format!("mode/{}/{}", mode_name, leaf);
        let entry = container
            .get(&name)
            .ok_or_else(|| anyhow::anyhow!("missing mode bundle section '{}'", name))?;
        let off = entry.offset as usize;
        let len = entry.len as usize;
        let _end = off.checked_add(len).ok_or_else(|| {
            anyhow::anyhow!(
                "section '{}' offset+len overflows usize (off={}, len={})",
                name,
                off,
                len
            )
        })?;
        anyhow::ensure!(
            off + len <= mmap.len(),
            "section '{}' bytes [{},{}) exceed mmap len {}",
            name,
            off,
            off + len,
            mmap.len()
        );
        // #161: drive lazy CRC verification before handing out bytes.
        lazy.verify_now(&name)?;
        Ok((std::sync::Arc::clone(mmap), off, len))
    };
    // Required section → borrowed byte slice from the live mapping.
    // Used by readers that still consume `&[u8]` directly
    // (`mod_weights::read_all_from_bytes`).
    let fetch_bytes = |leaf: &str| -> Result<&[u8]> {
        let name = format!("mode/{}/{}", mode_name, leaf);
        let entry = container
            .get(&name)
            .ok_or_else(|| anyhow::anyhow!("missing mode bundle section '{}'", name))?;
        let off = entry.offset as usize;
        let len = entry.len as usize;
        anyhow::ensure!(
            off + len <= mmap.len(),
            "section '{}' bytes [{},{}) exceed mmap len {}",
            name,
            off,
            off + len,
            mmap.len()
        );
        lazy.verify_now(&name)?;
        Ok(&mmap[off..off + len])
    };

    // ---- Server-only mapping sections (#153) -------------------
    // Preferred path: load `mode/<m>/orig_to_rank` and
    // `mode/<m>/filtered_to_original` zero-copy from the container.
    // Saves the entire `FilteredEbg` cold prefix (~80 MB/mode on
    // Belgium) and the entire `OrderEbg` (~40 MB/mode) from RSS.
    //
    // Back-compat: if either section is absent, fall back to reading
    // `FilteredEbg` + `OrderEbg` and synthesising the arrays at boot.
    // The fallback path matches pre-#153 behaviour byte-for-byte.
    let try_optional_arc =
        |name: &str| -> Result<Option<(std::sync::Arc<memmap2::Mmap>, usize, usize)>> {
            let section_name = format!("mode/{}/{}", mode_name, name);
            match container.get(&section_name) {
                Some(entry) => {
                    let off = entry.offset as usize;
                    let len = entry.len as usize;
                    anyhow::ensure!(
                        off + len <= mmap.len(),
                        "section '{}' bytes [{},{}) exceed mmap len {}",
                        section_name,
                        off,
                        off + len,
                        mmap.len()
                    );
                    lazy.verify_now(&section_name)?;
                    Ok(Some((std::sync::Arc::clone(mmap), off, len)))
                }
                None => Ok(None),
            }
        };

    let o2r_section = try_optional_arc("orig_to_rank")?;
    let f2o_section = try_optional_arc("filtered_to_original")?;

    // #197: role-aware snap masks need the per-mode filtered EBG
    // adjacency. We fetch it transiently, build the bitsets, then
    // madvise the bytes back out (the serve hot path doesn't read
    // them). Required regardless of whether the preferred (#153)
    // mapping path is taken or the legacy fallback runs, so we hoist
    // the read up here.
    let filtered_ebg_section = try_optional_arc("filtered_ebg")?;

    let (
        orig_to_rank,
        filtered_to_original,
        n_filtered_nodes,
        n_original_nodes,
        has_outbound,
        has_inbound,
    ) = match (o2r_section, f2o_section) {
        (Some((o2r_mmap, o2r_off, o2r_len)), Some((f2o_mmap, f2o_off, f2o_len))) => {
            let o2r = ModeIndexFile::read_from_mmap_unverified(o2r_mmap, o2r_off, o2r_len)?;
            anyhow::ensure!(
                o2r.kind == ModeIndexKind::OrigToRank,
                "mode/{}/orig_to_rank has wrong kind discriminator: {:?}",
                mode_name,
                o2r.kind
            );
            let f2o = ModeIndexFile::read_from_mmap_unverified(f2o_mmap, f2o_off, f2o_len)?;
            anyhow::ensure!(
                f2o.kind == ModeIndexKind::FilteredToOriginal,
                "mode/{}/filtered_to_original has wrong kind discriminator: {:?}",
                mode_name,
                f2o.kind
            );

            let n_original_nodes = o2r.data.len() as u32;
            let n_filtered_nodes = f2o.data.len() as u32;
            tracing::info!(
                mode = mode_name,
                n_original_nodes,
                n_filtered_nodes,
                "loaded mapping sections (zero-copy)"
            );

            // #197: build the role-aware snap bitsets from the
            // filtered EBG section. The section is required because
            // the in-memory `orig_to_rank`/`filtered_to_original`
            // mappings discard arc-level connectivity info — they
            // only say which nodes are mode-accessible, not whether
            // each node has any mode-valid outbound/inbound arcs.
            let (has_out, has_in) = match filtered_ebg_section {
                Some((fe_mmap, fe_off, fe_len)) => {
                    let filtered_ebg = crate::formats::FilteredEbgFile::read_from_mmap_unverified(
                        fe_mmap, fe_off, fe_len,
                    )?;
                    build_role_masks(&filtered_ebg)
                }
                None => {
                    anyhow::bail!(
                        "mode/{}/filtered_ebg section missing — required for #197 role-aware snap masks. \
                             Re-pack the container with the current pack tool.",
                        mode_name
                    );
                }
            };

            // The legacy `mode/<m>/filtered_ebg` and
            // `mode/<m>/order` sections are still in the container
            // for back-compat (build/validation tools may read
            // them). The serve path no longer reads them after the
            // role-mask build above, so we still madvise(DONTNEED)
            // their bytes to keep them off RSS.
            for legacy in ["filtered_ebg", "order"] {
                let nm = format!("mode/{}/{}", mode_name, legacy);
                if let Some(entry) = container.get(&nm) {
                    let off = entry.offset as usize;
                    let len = entry.len as usize;
                    let range = &mmap[off..off + len];
                    match crate::formats::mmap::madvise_dontneed(range) {
                        Ok(()) => tracing::info!(
                            section = %nm,
                            bytes = len,
                            "madvise(DONTNEED) on legacy section (#153 dropped from serve path)"
                        ),
                        Err(e) => tracing::warn!(
                            section = %nm,
                            error = %e,
                            "madvise(DONTNEED) on legacy section failed, ignoring"
                        ),
                    }
                }
            }

            (
                o2r.data,
                f2o.data,
                n_filtered_nodes,
                n_original_nodes,
                has_out,
                has_in,
            )
        }
        _ => {
            // Back-compat fallback: read `FilteredEbg` + `OrderEbg`,
            // synthesise the arrays at boot, drop the legacy
            // structs. RSS cost: one heap copy of each array.
            tracing::warn!(
                mode = mode_name,
                "mode/{0}/orig_to_rank or mode/{0}/filtered_to_original missing; \
                     this build pre-dates #153, falling back to FilteredEbg/OrderEbg",
                mode_name
            );
            let (fe_mmap, fe_off, fe_len) = fetch_arc("filtered_ebg")?;
            let filtered_ebg = FilteredEbgFile::read_from_mmap_unverified(fe_mmap, fe_off, fe_len)?;
            let order_section = fetch_bytes("order")?;
            let order_data = OrderEbgFile::read_from_bytes(order_section)?;

            let n_original_nodes = filtered_ebg.n_original_nodes;
            let n_filtered_nodes = filtered_ebg.n_filtered_nodes;
            let orig_to_rank = build_orig_to_rank(&filtered_ebg, &order_data);
            let filtered_to_original: Vec<u32> = filtered_ebg.filtered_to_original.to_vec();

            // #197: build role-aware snap bitsets while the
            // filtered EBG is still in scope.
            let (has_out, has_in) = build_role_masks(&filtered_ebg);

            // Both legacy sections are now fully consumed onto the
            // heap (orig_to_rank from order, filtered_to_original
            // copied out). CRC verification paged them in; advise
            // the kernel it can drop them so we don't carry the
            // file_kb cost for the rest of the process lifetime.
            drop(order_data);
            drop(filtered_ebg);
            if let Err(e) = crate::formats::mmap::madvise_dontneed(order_section) {
                tracing::warn!(
                    mode = mode_name,
                    error = %e,
                    "madvise(DONTNEED) on order section failed; ignoring"
                );
            }
            // Madvise the filtered_ebg section bytes (we no longer have
            // a `cold_filtered` sub-slice; pass the whole section).
            let fe_range = &mmap[fe_off..fe_off + fe_len];
            if let Err(e) = crate::formats::mmap::madvise_dontneed(fe_range) {
                tracing::warn!(
                    mode = mode_name,
                    error = %e,
                    "madvise(DONTNEED) on filtered_ebg section failed; ignoring"
                );
            }
            (
                crate::formats::ArcCow::from_vec(orig_to_rank),
                crate::formats::ArcCow::from_vec(filtered_to_original),
                n_filtered_nodes,
                n_original_nodes,
                has_out,
                has_in,
            )
        }
    };
    let (topo_mmap, topo_off, topo_len) = fetch_arc("topo")?;
    // #151: cch.topo is now v4. Header is 80 bytes (u64-aligned) and
    // every variable-length u32 array is padded to a u64 boundary, so
    // the zero-copy reader works regardless of n_up_edges/n_down_edges
    // parity. Saves ≈ 3-5 GB of heap on Belgium vs the v3 owning
    // reader; the topo body now lives in mmap'd file pages and is
    // demand-paged like the flats. The offsets/targets/middles/bitset
    // slices are borrowed from the mmap via `ArcCow::from_mmap` (no
    // leak — the Arc<Mmap> strong-count is tied to the returned
    // struct's lifetime, #296).
    let cch_topo = CchTopoFile::read_from_mmap_unverified(topo_mmap, topo_off, topo_len)?;
    // After CRC verification we hint the kernel that the topo bytes can
    // be reclaimed. Hot routing pages page back in lazily; cold ones
    // (e.g. `up_middle` bytes for shortcuts that no query ever unpacks)
    // stay off RSS. Same mechanism the flats use.
    let topo_bytes_for_madvise = &mmap[topo_off..topo_off + topo_len];
    if let Err(e) = crate::formats::mmap::madvise_dontneed(topo_bytes_for_madvise) {
        tracing::warn!(
            section = "topo",
            error = %e,
            "madvise(DONTNEED) on cch.topo section failed; ignoring"
        );
    } else {
        tracing::info!(
            section = "topo",
            bytes = topo_len,
            "madvise(DONTNEED) on cch.topo section"
        );
    }

    let weights_data = mod_weights::read_all_from_bytes(fetch_bytes("node_weights.time")?)?;

    let n_original = n_original_nodes as usize;
    let mask = {
        let n_words = n_original.div_ceil(64);
        let mut m = vec![0u64; n_words];
        for &orig_id in filtered_to_original.iter() {
            let word = orig_id as usize / 64;
            let bit = orig_id as usize % 64;
            m[word] |= 1u64 << bit;
        }
        m
    };

    // #147: zero-copy CCH weights — `up`/`down` u32 slices come straight
    // from the mmap. Saves ~6 GB of heap (4 modes × 2 metrics × ~750MB).
    let (wt_mmap, wt_off, wt_len) = fetch_arc("weights.time")?;
    let cch_weights = CchWeightsFile::read_from_mmap_unverified(wt_mmap, wt_off, wt_len)?;

    // #150: prefer pre-built flat sections from the container so the
    // flats live in mmap'd file pages instead of process heap. Bounds
    // idle RSS to working set rather than dataset size. If a flat is
    // absent (e.g. a container packed before #150), fall back to
    // building at boot — same heap cost as the legacy --data-dir path,
    // but the server still serves correctly.
    //
    // CRC verification touches every page, so right after parsing we
    // hint the kernel that the section can be paged out. The hot pages
    // (the slice ranges actually traversed by routing) page back in
    // lazily on first access; the cold ones stay off RSS. This is the
    // mechanism that makes idle RSS scale with working set, not dataset
    // size.
    let up_adj_flat = load_flat_section(
        container,
        mmap,
        &format!("mode/{}/up_adj_flat.time", mode_name),
        lazy,
        |m, off, len| UpAdjFlatFile::read_from_mmap_unverified(m, off, len),
        || UpAdjFlat::build_with(&cch_topo, &cch_weights, true),
    )?;
    let down_rev_flat = load_flat_section(
        container,
        mmap,
        &format!("mode/{}/down_reverse_adj_flat.time", mode_name),
        lazy,
        |m, off, len| DownReverseAdjFlatFile::read_from_mmap_unverified(m, off, len),
        || DownReverseAdjFlat::build_with(&cch_topo, &cch_weights, true),
    )?;
    let down_adj_flat = load_flat_section(
        container,
        mmap,
        &format!("mode/{}/down_adj_flat.time", mode_name),
        lazy,
        |m, off, len| DownAdjFlatFile::read_from_mmap_unverified(m, off, len),
        || DownAdjFlat::build(&cch_topo, &cch_weights),
    )?;

    let (wd_mmap, wd_off, wd_len) = fetch_arc("weights.dist")?;
    let cch_weights_dist = CchWeightsFile::read_from_mmap_unverified(wd_mmap, wd_off, wd_len)?;
    let up_adj_flat_dist_section = format!("mode/{}/up_adj_flat.dist", mode_name);
    let up_adj_flat_dist = load_flat_section(
        container,
        mmap,
        &up_adj_flat_dist_section,
        lazy,
        |m, off, len| UpAdjFlatFile::read_from_mmap_unverified(m, off, len),
        || UpAdjFlat::build(&cch_topo, &cch_weights_dist),
    )?;
    madvise_section_in_container(
        container,
        mmap,
        &up_adj_flat_dist_section,
    );
    let down_rev_flat_dist_section = format!("mode/{}/down_reverse_adj_flat.dist", mode_name);
    let down_rev_flat_dist = load_flat_section(
        container,
        mmap,
        &down_rev_flat_dist_section,
        lazy,
        |m, off, len| DownReverseAdjFlatFile::read_from_mmap_unverified(m, off, len),
        || DownReverseAdjFlat::build(&cch_topo, &cch_weights_dist),
    )?;
    madvise_section_in_container(
        container,
        mmap,
        &down_rev_flat_dist_section,
    );
    let down_adj_flat_dist_section = format!("mode/{}/down_adj_flat.dist", mode_name);
    let down_adj_flat_dist = load_flat_section(
        container,
        mmap,
        &down_adj_flat_dist_section,
        lazy,
        |m, off, len| DownAdjFlatFile::read_from_mmap_unverified(m, off, len),
        || DownAdjFlat::build(&cch_topo, &cch_weights_dist),
    )?;
    madvise_section_in_container(
        container,
        mmap,
        &down_adj_flat_dist_section,
    );

    Ok(ModeData {
        mode,
        cch_topo,
        cch_weights,
        cch_weights_dist,
        orig_to_rank,
        filtered_to_original,
        n_filtered_nodes,
        n_original_nodes,
        node_weights: weights_data.weights,
        mask,
        has_outbound,
        has_inbound,
        up_adj_flat,
        down_rev_flat,
        down_adj_flat,
        up_adj_flat_dist,
        down_rev_flat_dist,
        down_adj_flat_dist,
        exclude_cache: parking_lot::RwLock::new(HashMap::new()),
    })
}

/// Same as `load_way_names` but reads from an in-memory ways.raw byte
/// slice (mmap-backed container section).
fn load_way_names_from_bytes(ways_bytes: &[u8]) -> Result<HashMap<i64, String>> {
    let (key_dict, val_dict, _, _) = WaysFile::read_dictionaries_from_bytes(ways_bytes)?;
    let name_key_id = key_dict
        .iter()
        .find(|(_, v)| v.as_str() == "name")
        .map(|(k, _)| *k);
    let name_key_id = match name_key_id {
        Some(id) => id,
        None => return Ok(HashMap::new()),
    };

    let mut way_names = HashMap::new();
    for result in WaysFile::stream_ways_from_bytes(ways_bytes)? {
        let (way_id, keys, vals, _nodes) = result?;
        for (i, &k) in keys.iter().enumerate() {
            if k == name_key_id {
                if let Some(name) = val_dict.get(&vals[i])
                    && !name.is_empty()
                {
                    way_names.insert(way_id, name.clone());
                }
                break;
            }
        }
    }
    Ok(way_names)
}

// ---------- Packed snap index helpers (#154) -------------------------------

/// Build a packed snap index in heap memory from the loaded EBG + NBG
/// + per-mode masks. Used by:
///   - the directory-tree loader (always),
///   - the container loader's back-compat path when the new sections
///     are absent.
///
/// The resulting masks are aligned to `mode_names`, i.e. local-mode
/// position in `modes_data`. On the container path with the prebuilt
/// sections, `mode_names` order matches the container's mode-section
/// emission order, which matches the global mode-byte alphabetical
/// order — see [`try_load_packed_snap_index`] for the constraint.
fn build_packed_snap_index_inmem(
    ebg_nodes: &crate::formats::EbgNodes,
    nbg_geo: &crate::formats::NbgGeo,
    modes_data: &[ModeData],
    mode_names: &[String],
) -> PackedSnapIndex {
    let builder_modes: Vec<SnapBuilderMode<'_>> = modes_data
        .iter()
        .map(|m| SnapBuilderMode {
            mode_byte: m.mode.0,
            mask: &m.mask,
            inputs_sha: [0u8; 16],
        })
        .collect();
    let built = build_snap_index(ebg_nodes, nbg_geo, &builder_modes, DEFAULT_CELL_LOG2);
    tracing::info!(
        n_points = built.points.points.len(),
        n_cells = built.grid.n_cells_x as usize * built.grid.n_cells_y as usize,
        n_modes = mode_names.len(),
        "snap index built in memory"
    );
    PackedSnapIndex {
        points: built.points,
        grid: built.grid,
        masks: built.masks,
    }
}

/// Try to load a packed snap index zero-copy from a container.
/// Returns `Ok(None)` if any of the required sections is missing —
/// caller falls back to the in-memory builder.
///
/// #160: per-section CRC verification is gated by the [`LazyContainer`]
/// in [`ServerState`], not here. We only resolve byte ranges; body
/// pages stay cold until snap-index queries traverse them (or warmup
/// walks them off the request path).
fn try_load_packed_snap_index(
    container: &crate::formats::butterfly_dat::Container,
    mmap: &std::sync::Arc<memmap2::Mmap>,
    mode_names: &[String],
    lazy: &std::sync::Arc<crate::formats::lazy_verify::LazyContainer>,
) -> Result<Option<PackedSnapIndex>> {
    use crate::formats::snap_index::{SnapGridFile, SnapMaskFile, SnapPointsFile};

    let pts_entry = match container.get("shared/snap_points") {
        Some(e) => e,
        None => return Ok(None),
    };
    let grid_entry = match container.get("shared/snap_grid") {
        Some(e) => e,
        None => return Ok(None),
    };

    let pts_off = pts_entry.offset as usize;
    let pts_len = pts_entry.len as usize;
    let grid_off = grid_entry.offset as usize;
    let grid_len = grid_entry.len as usize;
    anyhow::ensure!(
        pts_off + pts_len <= mmap.len(),
        "shared/snap_points section out of mmap bounds"
    );
    anyhow::ensure!(
        grid_off + grid_len <= mmap.len(),
        "shared/snap_grid section out of mmap bounds"
    );

    // #161: drive lazy CRC verification through LazyContainer; format
    // readers below skip their own body walk.
    lazy.verify_now("shared/snap_points")?;
    lazy.verify_now("shared/snap_grid")?;
    let points =
        SnapPointsFile::read_from_mmap_unverified(std::sync::Arc::clone(mmap), pts_off, pts_len)
            .with_context(|| "reading shared/snap_points zero-copy")?;
    let grid =
        SnapGridFile::read_from_mmap_unverified(std::sync::Arc::clone(mmap), grid_off, grid_len)
            .with_context(|| "reading shared/snap_grid zero-copy")?;

    // Per-mode masks: for every loaded mode_name, look up
    // `mode/<name>/snap_mask`. Caller may have filtered to a subset of
    // modes — if any one is missing, fall back to the legacy build
    // path (rather than partially-load the index).
    let mut masks = Vec::with_capacity(mode_names.len());
    for name in mode_names {
        let key = format!("mode/{}/snap_mask", name);
        let entry = match container.get(&key) {
            Some(e) => e,
            None => return Ok(None),
        };
        let mask_off = entry.offset as usize;
        let mask_len = entry.len as usize;
        anyhow::ensure!(
            mask_off + mask_len <= mmap.len(),
            "{} section out of mmap bounds",
            key
        );
        lazy.verify_now(&key)?;
        let mask = SnapMaskFile::read_from_mmap_unverified(
            std::sync::Arc::clone(mmap),
            mask_off,
            mask_len,
        )
        .with_context(|| format!("reading {} zero-copy", key))?;
        // Sanity: mask sample count must match the shared point array.
        anyhow::ensure!(
            mask.n_points == points.n_points,
            "{} n_points {} != snap_points n_points {}",
            key,
            mask.n_points,
            points.n_points
        );
        masks.push(mask);
    }
    Ok(Some(PackedSnapIndex {
        points,
        grid,
        masks,
    }))
}

/// Try to load the flat edge geometry sections (#155) zero-copy from a
/// container. Returns `Ok(None)` if either section is missing — caller
/// falls back to building from the heap polylines.
///
/// #160: per-section CRC verification is gated by [`LazyContainer`].
fn try_load_edge_geometry(
    container: &crate::formats::butterfly_dat::Container,
    mmap: &std::sync::Arc<memmap2::Mmap>,
    lazy: &std::sync::Arc<crate::formats::lazy_verify::LazyContainer>,
) -> Result<Option<EdgeGeometry>> {
    use crate::formats::edge_geom::{EdgeGeomOffsetsFile, EdgeGeomPointsFile};

    let off_entry = match container.get("shared/edge_geom_offsets") {
        Some(e) => e,
        None => return Ok(None),
    };
    let pts_entry = match container.get("shared/edge_geom_points") {
        Some(e) => e,
        None => return Ok(None),
    };
    // #161: drive lazy CRC verification through LazyContainer.
    lazy.verify_now("shared/edge_geom_offsets")?;
    lazy.verify_now("shared/edge_geom_points")?;

    let off_off = off_entry.offset as usize;
    let off_len = off_entry.len as usize;
    let pts_off = pts_entry.offset as usize;
    let pts_len = pts_entry.len as usize;
    anyhow::ensure!(
        off_off + off_len <= mmap.len(),
        "shared/edge_geom_offsets section out of mmap bounds"
    );
    anyhow::ensure!(
        pts_off + pts_len <= mmap.len(),
        "shared/edge_geom_points section out of mmap bounds"
    );

    let off = EdgeGeomOffsetsFile::read_from_mmap_unverified(
        std::sync::Arc::clone(mmap),
        off_off,
        off_len,
    )
    .with_context(|| "reading shared/edge_geom_offsets zero-copy")?;
    let pts = EdgeGeomPointsFile::read_from_mmap_unverified(
        std::sync::Arc::clone(mmap),
        pts_off,
        pts_len,
    )
    .with_context(|| "reading shared/edge_geom_points zero-copy")?;

    let eg =
        EdgeGeometry::from_sections(off, pts).with_context(|| "stitching edge_geom sections")?;
    Ok(Some(eg))
}

#[cfg(test)]
mod tests {
    use super::build_role_masks;
    use crate::formats::FilteredEbg;
    use crate::profile_abi::Mode;

    fn tiny_filtered_ebg(offsets: Vec<u64>, heads: Vec<u32>) -> FilteredEbg {
        let n = offsets.len() - 1;
        FilteredEbg {
            mode: Mode(1),
            n_filtered_nodes: n as u32,
            n_filtered_arcs: heads.len() as u64,
            n_original_nodes: n as u32,
            inputs_sha: [0; 32],
            offsets: crate::formats::ArcCow::from_vec(offsets),
            heads: crate::formats::ArcCow::from_vec(heads.clone()),
            original_arc_idx: crate::formats::ArcCow::from_vec((0..heads.len() as u32).collect()),
            filtered_to_original: crate::formats::ArcCow::from_vec((0..n as u32).collect()),
            original_to_filtered: crate::formats::ArcCow::from_vec((0..n as u32).collect()),
        }
    }

    fn bit(mask: &[u64], node: usize) -> bool {
        (mask[node / 64] & (1u64 << (node % 64))) != 0
    }

    #[test]
    fn role_masks_keep_core_reachable_stubs_and_drop_small_sccs() {
        // 0 -> 1, 1 <-> 2 <-> 6, 2 -> 3, plus isolated 4 <-> 5.
        // The largest SCC is {1,2,6}. Sources may include 0 because it
        // can reach the core; destinations may include 3 because the
        // core can reach it. The isolated SCC looks internally valid
        // but is not useful for Belgium-wide table/route snaps.
        let fe = tiny_filtered_ebg(vec![0, 1, 2, 5, 5, 6, 7, 8], vec![1, 2, 1, 6, 3, 5, 4, 1]);

        let (src, dst) = build_role_masks(&fe);

        assert!(bit(&src, 0));
        assert!(bit(&src, 1));
        assert!(bit(&src, 2));
        assert!(!bit(&src, 3));
        assert!(!bit(&src, 4));
        assert!(!bit(&src, 5));
        assert!(bit(&src, 6));

        assert!(!bit(&dst, 0));
        assert!(bit(&dst, 1));
        assert!(bit(&dst, 2));
        assert!(bit(&dst, 3));
        assert!(!bit(&dst, 4));
        assert!(!bit(&dst, 5));
        assert!(bit(&dst, 6));
    }
}
