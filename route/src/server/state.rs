//! Server state - loaded data for query processing
//!
//! Per-mode CCH architecture: each mode has its own filtered CCH topology and ordering.
//! The spatial index operates in original EBG space, then maps to filtered space for query.

use anyhow::{Context, Result};
use std::collections::HashMap;
use std::path::Path;

use crate::formats::{
    CchTopo, CchTopoFile, CchWeightsFile, EbgCsr, EbgCsrFile, EbgNodes, EbgNodesFile,
    FilteredEbgFile, NbgGeo, NbgGeoFile, NbgNodeMapFile, OrderEbgFile, WaysFile, mod_weights,
    mode_index::{ModeIndexFile, ModeIndexKind},
};
use std::borrow::Cow;
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
    pub orig_to_rank: Cow<'static, [u32]>,
    /// `filtered_to_original[filtered_id]` → original EBG node id.
    /// Used on the unpack/back-reference direction (route geometry,
    /// road-name lookup, exclude/avoid recustomization).
    pub filtered_to_original: Cow<'static, [u32]>,
    /// Number of filtered (mode-accessible) EBG nodes. Equals
    /// `filtered_to_original.len()`. Kept as a u32 for the few
    /// metadata / log sites that read it directly.
    pub n_filtered_nodes: u32,
    /// Number of original EBG nodes. Equals `orig_to_rank.len()`.
    /// Reported in /health and a couple of spot diagnostics.
    pub n_original_nodes: u32,
    // Original node weights and mask (indexed by original EBG node ID)
    pub node_weights: Vec<u32>,
    pub mask: Vec<u64>,
    /// Per-mode "has at least one outbound mode-valid arc" bitset
    /// (indexed by original EBG node ID). Built at boot from the
    /// filtered EBG. Used by role-aware snap (#197) so that snapping a
    /// SOURCE point only returns EBG nodes that can actually start a
    /// route in this mode (excludes one-way exit ramps where every
    /// outbound transition is banned in this mode).
    pub has_outbound: Vec<u64>,
    /// Per-mode "has at least one inbound mode-valid arc" bitset
    /// (indexed by original EBG node ID). Built at boot from the
    /// filtered EBG. Used by role-aware snap (#197) so that snapping a
    /// DESTINATION point only returns EBG nodes that can actually be
    /// reached in this mode (excludes nodes only reachable backward).
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
            mode_lookup.insert(mode_name.clone(), mode_index as u8);
            mode_names.push(mode_name.clone());
            crate::server::rss::checkpoint(&format!("load.mode.{}", mode_name));
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

        // #147: leak a clone of the Arc so derived `&'static [u8]` views
        // remain valid for process lifetime. Server lifetime IS process
        // lifetime today, so the leak is one Arc per loaded container —
        // negligible. Original `mmap` Arc still drops at end of scope;
        // the leaked Arc keeps the mapping alive forever.
        //
        // SAFETY: `Box::leak` is safe; it returns `&'static T` from
        // `Box<T>`. The `unsafe_code` carveout policy is unaffected —
        // the only `unsafe` site remains `formats/mmap.rs::map_readonly`.
        let leaked_arc: &'static std::sync::Arc<memmap2::Mmap> =
            Box::leak(Box::new(std::sync::Arc::clone(&mmap)));
        let static_mmap: &'static memmap2::Mmap = leaked_arc.as_ref();
        let static_bytes: &'static [u8] = &static_mmap[..];

        // Convenience accessor: section name → `'static` bytes from the
        // leaked mapping.
        //
        // #160 + #161: per-section CRC is verified through the
        // [`LazyContainer`] gate held by `lazy_arc` — calling
        // `verify_now` here transitions the section through the lazy
        // state machine, drives the metrics counters, and returns once
        // the section is `Verified`. Format readers below are then
        // called via their `_unverified` entry points so the section
        // body is walked exactly once on the container load path
        // (LazyContainer's CRC walk replaces the per-format walk).
        // For readers that lack an `_unverified` variant the format CRC
        // is still walked, paging the body in twice for those sections;
        // the readers we did upgrade are the largest by far (CCH
        // weights, EBG nodes/CSR, snap index, edge geom, flats).
        //
        // Page-fault footprint per section after `section_bytes`
        // returns — i.e. **after** LazyContainer's CRC walk:
        //   - `EbgNodesFile::read_from_bytes_zero_copy_unverified`,
        //     `EbgCsrFile::read_from_bytes_zero_copy_unverified`,
        //     `SnapPointsFile::*_unverified`,
        //     `SnapGridFile::*_unverified`,
        //     `EdgeGeomOffsetsFile::*_unverified`,
        //     `EdgeGeomPointsFile::*_unverified`,
        //     `ModeIndexFile::*_unverified`,
        //     `CchTopoFile::*_unverified` — these read only the section
        //     header (~32–80 bytes) plus a handful of length fields and
        //     return `Cow::Borrowed` slices into the mmap; body pages
        //     are paged in lazily when the slices are subsequently read
        //     by routing.
        //   - `CchWeightsFile::*_unverified`,
        //     `UpAdjFlatFile::read_from_bytes_unverified`,
        //     `DownReverseAdjFlatFile::read_from_bytes_unverified`,
        //     `DownAdjFlatFile::read_from_bytes_unverified`,
        //     `SnapMaskFile::*_unverified` — these likewise return
        //     `Cow::Borrowed` slices and only touch the header. The
        //     subsequent `bytemuck::cast_slice` is a pointer-only cast
        //     that does not touch body bytes.
        //   - `NbgGeoFile::read_edges_only_from_bytes` does walk the
        //     full body to populate the edges Vec; an explicit
        //     `madvise(DONTNEED)` immediately after parsing returns
        //     those pages to the kernel. Same for the legacy
        //     `FilteredEbgFile` / `OrderEbgFile` fallback path.
        let lazy_for_bytes = std::sync::Arc::clone(&lazy_arc);
        let section_bytes = |name: &str| -> Result<&'static [u8]> {
            let entry = container
                .get(name)
                .ok_or_else(|| anyhow::anyhow!("missing required section '{}'", name))?;
            let off = entry.offset as usize;
            let len = entry.len as usize;
            anyhow::ensure!(
                off + len <= static_bytes.len(),
                "section '{}' bytes [{},{}) exceed mmap len {}",
                name,
                off,
                off + len,
                static_bytes.len()
            );
            // Drive lazy CRC verification through LazyContainer. The
            // first call to `verify_now` walks the section body once;
            // subsequent calls observe `Verified` and short-circuit.
            // This both updates `butterfly_route_sections_*` metrics
            // and lets format readers skip their own body CRC walk
            // via the `_unverified` entry points.
            lazy_for_bytes.verify_now(name)?;
            Ok(&static_bytes[off..off + len])
        };
        let lazy_for_optional = std::sync::Arc::clone(&lazy_arc);
        let optional_section = |name: &str| -> Result<Option<&'static [u8]>> {
            match container.get(name) {
                Some(entry) => {
                    let off = entry.offset as usize;
                    let len = entry.len as usize;
                    anyhow::ensure!(
                        off + len <= static_bytes.len(),
                        "section '{}' bytes [{},{}) exceed mmap len {}",
                        name,
                        off,
                        off + len,
                        static_bytes.len()
                    );
                    lazy_for_optional.verify_now(name)?;
                    Ok(Some(&static_bytes[off..off + len]))
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
        let ebg_nodes =
            EbgNodesFile::read_from_bytes_zero_copy_unverified(section_bytes("shared/ebg.nodes")?)?;
        tracing::info!(nodes = ebg_nodes.n_nodes, "loaded EBG nodes");

        tracing::info!("Loading EBG CSR (zero-copy)...");
        let ebg_csr_section = section_bytes("shared/ebg.csr")?;
        let ebg_csr = EbgCsrFile::read_from_bytes_zero_copy_unverified(ebg_csr_section)?;
        tracing::info!(arcs = ebg_csr.n_arcs, "loaded EBG CSR");
        // #152: ebg.csr is build/validate-only at serve time. The only
        // field any handler reads is `n_arcs` (a u64 in the header used
        // by /health). The body arrays (offsets, heads, turn_idx) are
        // touched by validate/step4 + ordering/contraction, none of
        // which run on the serve path. Drop the file pages from RSS;
        // the borrowed Cow slices stay valid and a rare cold reader
        // pages them back at fault cost.
        if let Err(e) = crate::formats::mmap::madvise_dontneed(ebg_csr_section) {
            tracing::warn!(
                section = "shared/ebg.csr",
                error = %e,
                "madvise(DONTNEED) on ebg.csr failed; ignoring"
            );
        } else {
            tracing::info!(
                section = "shared/ebg.csr",
                bytes = ebg_csr_section.len(),
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
            let mode_data =
                load_mode_data_from_bundle(mode_name, mode, &container, static_mmap, &lazy_arc)?;
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
            static_mmap,
            static_bytes,
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
                    let range = &static_bytes[off..off + len];
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

        // ---- Road names from shared/step1.ways.raw ------------------
        tracing::info!("Loading road names from container...");
        let way_names = if let Some(ways_bytes) = optional_section("shared/step1.ways.raw")? {
            load_way_names_from_bytes(ways_bytes)?
        } else {
            tracing::warn!("ways.raw missing in container, road names unavailable");
            HashMap::new()
        };
        tracing::info!(named_roads = way_names.len(), "loaded road names");

        // ---- Edge exclude flags from one mode's way_attrs -----------
        // Prefer car if available, otherwise the alphabetically first mode.
        let attrs_mode = if discovered_modes.iter().any(|m| m == "car") {
            "car".to_string()
        } else {
            discovered_modes[0].clone()
        };
        let attrs_section = format!("mode/{}/way_attrs", attrs_mode);
        let edge_exclude_flags = if let Some(attr_bytes) = optional_section(&attrs_section)? {
            let attrs = crate::formats::way_attrs::read_all_from_bytes(attr_bytes)?;
            exclude::build_edge_exclude_flags_from_attrs(&ebg_nodes, &attrs)?
        } else {
            tracing::warn!(section = %attrs_section, "way_attrs absent, exclude feature disabled");
            vec![0u8; ebg_nodes.n_nodes as usize]
        };

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
            let eg = try_load_edge_geometry(&container, static_mmap, static_bytes, &lazy_arc)?
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "edge_geom sections vanished between open and load — container corrupt?"
                    )
                })?;
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
    // per-arc turn-table mode masking, so this is the exact ground
    // truth for "can this node start a route" / "can this node end a
    // route" in this mode.
    let (has_outbound, has_inbound) = build_role_masks(&filtered_ebg);

    Ok(ModeData {
        mode,
        cch_topo,
        cch_weights,
        cch_weights_dist,
        orig_to_rank: Cow::Owned(orig_to_rank),
        filtered_to_original: Cow::Owned(filtered_to_original),
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

/// Build per-mode `has_outbound` and `has_inbound` bitsets indexed by
/// **original** EBG node id, from the mode's `FilteredEbg`. The
/// filtered EBG already encodes both node-level mode accessibility and
/// per-arc turn-table mode masking, so a node's outbound (or inbound)
/// bit is set iff the node has at least one mode-valid arc out (or in).
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
    let mut has_outbound = vec![0u64; n_words];
    let mut has_inbound = vec![0u64; n_words];

    let f2o = filtered_ebg.filtered_to_original.as_ref();
    let offsets = filtered_ebg.offsets.as_ref();
    let heads = filtered_ebg.heads.as_ref();

    for (filt_id, &orig_id) in f2o.iter().enumerate() {
        let start = offsets[filt_id] as usize;
        let end = offsets[filt_id + 1] as usize;
        if end > start {
            let oi = orig_id as usize;
            has_outbound[oi / 64] |= 1u64 << (oi % 64);
        }
        for &head_filt in &heads[start..end] {
            let head_orig = f2o[head_filt as usize] as usize;
            has_inbound[head_orig / 64] |= 1u64 << (head_orig % 64);
        }
    }

    (has_outbound, has_inbound)
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
/// Note: a `madvise(DONTNEED)` after parsing is **not** required here.
/// The format reader's `_unverified` entry point only touches the
/// header (~32–80 bytes) and returns `Cow::Borrowed` slices over the
/// body; `bytemuck::cast_slice` is a pointer-only cast and does not
/// page the body in. The body therefore stays cold in the page cache
/// once LazyContainer's CRC walk has completed and any pages it pulled
/// in are reclaimable by the kernel under memory pressure.
///
/// `parse` is a closure that turns `&'static [u8]` into the typed flat
/// view; `build_owned` is the legacy heap-build fallback.
fn load_flat_section<T, P, B>(
    container: &crate::formats::butterfly_dat::Container,
    _static_mmap: &'static memmap2::Mmap,
    static_bytes: &'static [u8],
    section_name: &str,
    lazy: &std::sync::Arc<crate::formats::lazy_verify::LazyContainer>,
    parse: P,
    build_owned: B,
) -> Result<T>
where
    P: FnOnce(&'static [u8]) -> Result<T>,
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
    anyhow::ensure!(
        off + len <= static_bytes.len(),
        "flat section '{}' bytes [{},{}) exceed mmap len {}",
        section_name,
        off,
        off + len,
        static_bytes.len()
    );
    // #161: verify CRC via LazyContainer, then read with the unverified
    // format reader to avoid paging the body in twice.
    lazy.verify_now(section_name)?;
    let bytes: &'static [u8] = &static_bytes[off..off + len];
    let parsed = parse(bytes)?;
    Ok(parsed)
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
    static_mmap: &'static memmap2::Mmap,
    lazy: &std::sync::Arc<crate::formats::lazy_verify::LazyContainer>,
) -> Result<ModeData> {
    let static_bytes: &'static [u8] = &static_mmap[..];
    let fetch = |leaf: &str| -> Result<&'static [u8]> {
        let name = format!("mode/{}/{}", mode_name, leaf);
        let entry = container
            .get(&name)
            .ok_or_else(|| anyhow::anyhow!("missing mode bundle section '{}'", name))?;
        let off = entry.offset as usize;
        let len = entry.len as usize;
        anyhow::ensure!(
            off + len <= static_bytes.len(),
            "section '{}' bytes [{},{}) exceed mmap len {}",
            name,
            off,
            off + len,
            static_bytes.len()
        );
        // #161: drive lazy CRC verification before handing out bytes.
        lazy.verify_now(&name)?;
        Ok(&static_bytes[off..off + len])
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
    let try_optional = |name: &str| -> Result<Option<&'static [u8]>> {
        let section_name = format!("mode/{}/{}", mode_name, name);
        match container.get(&section_name) {
            Some(entry) => {
                let off = entry.offset as usize;
                let len = entry.len as usize;
                anyhow::ensure!(
                    off + len <= static_bytes.len(),
                    "section '{}' bytes [{},{}) exceed mmap len {}",
                    section_name,
                    off,
                    off + len,
                    static_bytes.len()
                );
                lazy.verify_now(&section_name)?;
                Ok(Some(&static_bytes[off..off + len]))
            }
            None => Ok(None),
        }
    };

    let o2r_section = try_optional("orig_to_rank")?;
    let f2o_section = try_optional("filtered_to_original")?;

    // #197: role-aware snap masks need the per-mode filtered EBG
    // adjacency. We fetch it transiently, build the bitsets, then
    // madvise the bytes back out (the serve hot path doesn't read
    // them). Required regardless of whether the preferred (#153)
    // mapping path is taken or the legacy fallback runs, so we hoist
    // the read up here.
    let filtered_ebg_section = try_optional("filtered_ebg")?;

    let (
        orig_to_rank,
        filtered_to_original,
        n_filtered_nodes,
        n_original_nodes,
        has_outbound,
        has_inbound,
    ) = match (o2r_section, f2o_section) {
            (Some(o2r_bytes), Some(f2o_bytes)) => {
                let o2r = ModeIndexFile::read_from_bytes_zero_copy_unverified(o2r_bytes)?;
                anyhow::ensure!(
                    o2r.kind == ModeIndexKind::OrigToRank,
                    "mode/{}/orig_to_rank has wrong kind discriminator: {:?}",
                    mode_name,
                    o2r.kind
                );
                let f2o = ModeIndexFile::read_from_bytes_zero_copy_unverified(f2o_bytes)?;
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
                    Some(bytes) => {
                        let (filtered_ebg, _cold) =
                            crate::formats::FilteredEbgFile::read_from_bytes_zero_copy_with_cold(
                                bytes,
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
                        let range = &static_bytes[off..off + len];
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
                let filtered_section = fetch("filtered_ebg")?;
                let (filtered_ebg, cold_filtered) =
                    FilteredEbgFile::read_from_bytes_zero_copy_with_cold(filtered_section)?;
                if let Err(e) = crate::formats::mmap::madvise_dontneed(cold_filtered) {
                    tracing::warn!(
                        mode = mode_name,
                        error = %e,
                        "madvise(DONTNEED) on filtered_ebg cold prefix failed; ignoring"
                    );
                }
                let order_section = fetch("order")?;
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
                if let Err(e) = crate::formats::mmap::madvise_dontneed(filtered_section) {
                    tracing::warn!(
                        mode = mode_name,
                        error = %e,
                        "madvise(DONTNEED) on filtered_ebg section failed; ignoring"
                    );
                }
                (
                    Cow::Owned(orig_to_rank),
                    Cow::Owned(filtered_to_original),
                    n_filtered_nodes,
                    n_original_nodes,
                    has_out,
                    has_in,
                )
            }
        };
    let topo_section_bytes: &'static [u8] = fetch("topo")?;
    // #151: cch.topo is now v4. Header is 80 bytes (u64-aligned) and
    // every variable-length u32 array is padded to a u64 boundary, so
    // the zero-copy reader works regardless of n_up_edges/n_down_edges
    // parity. Saves ≈ 3-5 GB of heap on Belgium vs the v3 owning
    // reader; the topo body now lives in mmap'd file pages and is
    // demand-paged like the flats. The offsets/targets/middles/bitset
    // slices are borrowed from the mmap with the same Box::leak'd
    // 'static lifetime trick as the flats.
    let cch_topo = CchTopoFile::read_from_bytes_zero_copy_unverified(topo_section_bytes)?;
    // After CRC verification we hint the kernel that the topo bytes can
    // be reclaimed. Hot routing pages page back in lazily; cold ones
    // (e.g. `up_middle` bytes for shortcuts that no query ever unpacks)
    // stay off RSS. Same mechanism the flats use.
    if let Err(e) = crate::formats::mmap::madvise_dontneed(topo_section_bytes) {
        tracing::warn!(
            section = "topo",
            error = %e,
            "madvise(DONTNEED) on cch.topo section failed; ignoring"
        );
    } else {
        tracing::info!(
            section = "topo",
            bytes = topo_section_bytes.len(),
            "madvise(DONTNEED) on cch.topo section"
        );
    }

    let weights_data = mod_weights::read_all_from_bytes(fetch("node_weights.time")?)?;

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
    let cch_weights = CchWeightsFile::read_from_bytes_zero_copy_unverified(fetch("weights.time")?)?;

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
        static_mmap,
        static_bytes,
        &format!("mode/{}/up_adj_flat.time", mode_name),
        lazy,
        |bytes| UpAdjFlatFile::read_from_bytes_unverified(bytes),
        || UpAdjFlat::build_with(&cch_topo, &cch_weights, true),
    )?;
    let down_rev_flat = load_flat_section(
        container,
        static_mmap,
        static_bytes,
        &format!("mode/{}/down_reverse_adj_flat.time", mode_name),
        lazy,
        |bytes| DownReverseAdjFlatFile::read_from_bytes_unverified(bytes),
        || DownReverseAdjFlat::build_with(&cch_topo, &cch_weights, true),
    )?;
    let down_adj_flat = load_flat_section(
        container,
        static_mmap,
        static_bytes,
        &format!("mode/{}/down_adj_flat.time", mode_name),
        lazy,
        |bytes| DownAdjFlatFile::read_from_bytes_unverified(bytes),
        || DownAdjFlat::build(&cch_topo, &cch_weights),
    )?;

    let cch_weights_dist =
        CchWeightsFile::read_from_bytes_zero_copy_unverified(fetch("weights.dist")?)?;
    let up_adj_flat_dist = load_flat_section(
        container,
        static_mmap,
        static_bytes,
        &format!("mode/{}/up_adj_flat.dist", mode_name),
        lazy,
        |bytes| UpAdjFlatFile::read_from_bytes_unverified(bytes),
        || UpAdjFlat::build(&cch_topo, &cch_weights_dist),
    )?;
    let down_rev_flat_dist = load_flat_section(
        container,
        static_mmap,
        static_bytes,
        &format!("mode/{}/down_reverse_adj_flat.dist", mode_name),
        lazy,
        |bytes| DownReverseAdjFlatFile::read_from_bytes_unverified(bytes),
        || DownReverseAdjFlat::build(&cch_topo, &cch_weights_dist),
    )?;
    let down_adj_flat_dist = load_flat_section(
        container,
        static_mmap,
        static_bytes,
        &format!("mode/{}/down_adj_flat.dist", mode_name),
        lazy,
        |bytes| DownAdjFlatFile::read_from_bytes_unverified(bytes),
        || DownAdjFlat::build(&cch_topo, &cch_weights_dist),
    )?;

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
    _static_mmap: &'static memmap2::Mmap,
    static_bytes: &'static [u8],
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

    let pts_bytes = &static_bytes
        [pts_entry.offset as usize..pts_entry.offset as usize + pts_entry.len as usize];
    let grid_bytes = &static_bytes
        [grid_entry.offset as usize..grid_entry.offset as usize + grid_entry.len as usize];

    // #161: drive lazy CRC verification through LazyContainer; format
    // readers below skip their own body walk.
    lazy.verify_now("shared/snap_points")?;
    lazy.verify_now("shared/snap_grid")?;
    let points = SnapPointsFile::read_from_bytes_zero_copy_unverified(pts_bytes)
        .with_context(|| "reading shared/snap_points zero-copy")?;
    let grid = SnapGridFile::read_from_bytes_zero_copy_unverified(grid_bytes)
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
        let mask_bytes =
            &static_bytes[entry.offset as usize..entry.offset as usize + entry.len as usize];
        lazy.verify_now(&key)?;
        let mask = SnapMaskFile::read_from_bytes_zero_copy_unverified(mask_bytes)
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
    _static_mmap: &'static memmap2::Mmap,
    static_bytes: &'static [u8],
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

    let off_bytes = &static_bytes
        [off_entry.offset as usize..off_entry.offset as usize + off_entry.len as usize];
    let pts_bytes = &static_bytes
        [pts_entry.offset as usize..pts_entry.offset as usize + pts_entry.len as usize];

    let off = EdgeGeomOffsetsFile::read_from_bytes_zero_copy_unverified(off_bytes)
        .with_context(|| "reading shared/edge_geom_offsets zero-copy")?;
    let pts = EdgeGeomPointsFile::read_from_bytes_zero_copy_unverified(pts_bytes)
        .with_context(|| "reading shared/edge_geom_points zero-copy")?;

    let eg =
        EdgeGeometry::from_sections(off, pts).with_context(|| "stitching edge_geom sections")?;
    Ok(Some(eg))
}
