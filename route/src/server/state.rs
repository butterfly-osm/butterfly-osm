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
    FilteredEbgFile, NbgGeo, NbgGeoFile, NbgNodeMapFile, OrderEbgFile, mod_weights,
    mode_index::{ModeIndexFile, ModeIndexKind},
};
// Re-export CchWeights for use by api.rs
pub use crate::formats::CchWeights;
use crate::matrix::bucket_ch::{
    DownAdjFlat, DownAdjFlatFile, DownReverseAdjFlat, DownReverseAdjFlatFile, UpAdjFlat,
    UpAdjFlatFile,
};
use crate::model::types::Mode;

use super::exclude::{self, ExcludeWeights};

mod recustomize;
pub use recustomize::{EdgeRecustomizePrep, EdgeTableColumn};

// #578: the boot loader, split along the phase banners it already had.
// `load` / `load_from_container_with_options` below are the
// orchestrators; each module owns one self-contained phase.
mod auxiliary;
mod modes;
mod shared;
mod snap;

use super::edge_geom::EdgeGeometry;
use super::elevation::ElevationData;
use super::snap_index::PackedSnapIndex;
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
    /// Length-along-time-shortest weights (#371/#372). `None` for
    /// containers built before PR #377. Once the 2-channel bucket-M2M
    /// lands, /table /trip Flight matrix consumers will REQUIRE this
    /// — the loader will then fail boot if it's missing rather than
    /// silently falling back to the broken `cch_weights_dist`.
    pub cch_weights_len_along_time: Option<CchWeights>,
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
    // #553: there is no `down_adj_flat_dist`. The forward-DOWN DISTANCE
    // flat was built/mmapped/cloned per mode and never read — isodistance
    // was removed in #371 (time-only metrics) and the surviving distance
    // consumers (2-channel bucket M2M, len-along-time) go through
    // `down_rev_flat_dist` / the len-along-time flats.
    /// Flat UP adjacency carrying the length-along-time weights
    /// (#371/#372). `None` for old containers or pre-PR #377 step8
    /// outputs. Same topology as `up_adj_flat` (time) — index `i`
    /// addresses the same CCH edge. Used by the 2-channel bucket-M2M
    /// alongside `up_adj_flat` to report distance along the
    /// time-shortest path.
    pub up_adj_flat_len_along_time: Option<UpAdjFlat>,
    /// Reverse DOWN flat carrying length-along-time weights. See
    /// `up_adj_flat_len_along_time`.
    pub down_rev_flat_len_along_time: Option<DownReverseAdjFlat>,
    /// #527: forward-DOWN len-along-time flat for the 2-channel lopsided
    /// PHAST matrix plan. Built lazily on first duration+distance lopsided
    /// query (modes that never serve one pay nothing) and cached per
    /// ModeData instance — a freshly recustomized car rebuilds its own, so
    /// weights never go stale. `None` inside means no len weights exist.
    pub down_adj_flat_len_along_time_lazy: std::sync::OnceLock<Option<DownAdjFlat>>,
    // Cached exclude weight sets (keyed by exclude bitmask)
    pub exclude_cache: super::exclude::ExcludeWeightCache,
}

impl ModeData {
    /// #527: forward-down len-along-time flat, built + cached on demand.
    /// Returns None when this mode carries no len-along-time weights.
    pub fn down_len_flat(&self) -> Option<&crate::matrix::bucket_ch::DownAdjFlat> {
        self.down_adj_flat_len_along_time_lazy
            .get_or_init(|| {
                self.cch_weights_len_along_time
                    .as_ref()
                    .map(|w| crate::matrix::bucket_ch::DownAdjFlat::build(&self.cch_topo, w))
            })
            .as_ref()
    }

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

/// #402 lazy-load + eviction slot for a single mode.
///
/// Holds a parking_lot RwLock around the optional `Arc<ModeData>` so:
/// - the fast path (mode resident, just clone the Arc) is wait-free
///   after the read-lock acquire,
/// - the lazy-reload path takes the write lock and re-checks (handles
///   single-flight: simultaneous queries for the same cold mode all
///   wait on the same write lock, only one load runs),
/// - the idle compactor can evict (drop the Arc inside the slot)
///   while in-flight queries continue to hold their own Arc clones —
///   the data only actually drops when the last clone is released.
pub struct ModeSlot {
    /// Mode name (`"car"`, `"bike"`, `"foot"`, ...) used by
    /// the lazy reloader to call back into the loader.
    pub mode_name: String,
    /// The actual mode data. `Some` when resident, `None` after the
    /// idle compactor evicted it (next `get_mode` lazy-reloads).
    pub state: parking_lot::RwLock<Option<std::sync::Arc<ModeData>>>,
    /// Monotonic millis-since-server-start of the most recent
    /// `get_mode(...)` that found this slot resident. Drives idle
    /// eviction.
    pub last_used_ms: std::sync::atomic::AtomicU64,
    /// #402: synthetic modes (`car_freeflow`, the #521 uncertainty
    /// bands) have no container bundle behind them — they are built by
    /// cloning a base mode's topology and swapping weights, which
    /// `load_mode_data_from_bundle` cannot redo. The compactor skips
    /// them. Base modes (the heavy ones — 1-4 GB each) are the
    /// eviction target.
    ///
    /// Atomic so the #433 serve-boot car recustomization can pin the
    /// slot AFTER it hot-swaps the calibrated car in: if it stayed
    /// evictable, the idle compactor would drop the recustomized Arc
    /// and the next query would lazy-reload the CLEAN base car from the
    /// container — a silent traffic regression.
    pub evictable: std::sync::atomic::AtomicBool,
}

impl ModeSlot {
    /// #578: `evictable` is passed by the construction site, which
    /// knows what it just built — a base mode reloadable from its
    /// container bundle (`true`), or a synthetic mode with no bundle
    /// behind it (`false`). It used to be inferred by scanning every
    /// mode name for an underscore whose prefix was also a loaded mode,
    /// which was O(n^2) over the mode list and silently pinned any
    /// future base mode named after a loaded one plus a suffix.
    pub fn new_loaded(mode_name: String, data: ModeData, evictable: bool) -> Self {
        Self {
            mode_name,
            state: parking_lot::RwLock::new(Some(std::sync::Arc::new(data))),
            last_used_ms: std::sync::atomic::AtomicU64::new(0),
            evictable: std::sync::atomic::AtomicBool::new(evictable),
        }
    }
}

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
    /// #460: per-NBG-edge OSM node id chains (same `geom_idx` space as
    /// `edge_geom`). Empty when the container pre-dates the sections —
    /// `edges_flow` then emits NBG-endpoint rows instead of
    /// per-OSM-segment rows.
    pub edge_osm: crate::server::edge_osm::EdgeOsmChains,
    /// NBG compact node id → OSM node id. Indexed by `compact_id`,
    /// loaded once at startup from `step3*/nbg.node_map`. Used by the
    /// Flight `edges_batch` action (#125) to expose per-edge OSM node
    /// references in the unnested output schema. Memory cost on
    /// Belgium: ~11 MB (≈1.4M nodes × 8 bytes).
    pub nbg_node_to_osm: Vec<i64>,

    // Per-mode data (dynamically discovered, indexed by mode_index).
    // #402: each mode lives behind a `ModeSlot` lock so the idle
    // compactor can evict cold modes and the next query lazy-reloads
    // them. Callers go through `get_mode(...)` which returns an
    // `Arc<ModeData>` — holding the Arc keeps the mode alive even if
    // the compactor races to evict.
    pub modes: Vec<ModeSlot>,
    // #521 uncertainty bands: hidden variant slots (NOT in mode_lookup, so
    // unreachable via ?mode=). pess = worst-speed weights (slower world),
    // opt = best-speed. None until register_car_bands_from_edge_speeds runs.
    pub band_worst_idx: Option<usize>,
    pub band_best_idx: Option<usize>,
    /// #450→(single-car): index of the resident clean legal-limit base
    /// (`car_freeflow`). Kept RESIDENT as the internal base the uncertainty
    /// bands recustomize from, but NOT inserted into `mode_lookup` — the ONLY
    /// public car profile is the survey-median `car` (#521). No `?mode=` and
    /// no BUTTERFLY_CAR_PROFILE override reaches it.
    pub car_freeflow_idx: Option<usize>,
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

    // Distance weights indexed by original EBG node ID (length_m per edge).
    // Used for isodistance isochrones — same role as ModeData.node_weights but in meters.
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
        let mut tables = modes::ModeTables::with_capacity(discovered_modes.len());

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
            // Base modes are evictable (#578); the directory-tree path
            // has no container to reload from, but eviction is not armed
            // there either — see the slot comment below.
            tables.push(mode_name.clone(), mode_data, true);
            crate::server::rss::checkpoint(&format!("load.mode.{}", mode_name));
        }

        // ---- Packed snap index (#154) -------------------------------
        // Always build in memory for the directory path. The container
        // path can read prebuilt sections zero-copy.
        tracing::info!("Building packed snap index (in memory)...");
        let snap_index = snap::build_packed_snap_index_inmem(&ebg_nodes, &nbg_geo, &tables);
        crate::server::rss::checkpoint("spatial.global");
        for name in &tables.names {
            crate::server::rss::checkpoint(&format!("spatial.mode.{}", name));
        }

        // Load road names from ways.raw for turn-by-turn instructions.
        // Data-dir path always uses the legacy HashMap; the container
        // path (`load_state_from_bundle`) can use the compact mmap
        // index when `shared/way_names_idx` is present (#282).
        tracing::info!("Loading road names...");
        let way_names = WayNames::from_heap(auxiliary::load_way_names(&step1_dir)?);
        tracing::info!(named_roads = way_names.len(), "loaded road names");

        // Build per-edge exclude flags from way_attrs.car.bin
        // Try car first, then any available mode's way_attrs
        tracing::info!("Loading edge exclude flags...");
        let way_attrs_path = auxiliary::find_way_attrs_path(&step2_dir, &discovered_modes);
        let edge_exclude_flags = if let Some(attrs_path) = way_attrs_path {
            exclude::build_edge_exclude_flags(&ebg_nodes, &attrs_path)?
        } else {
            tracing::warn!("No way_attrs file found, exclude feature disabled");
            vec![0u8; ebg_nodes.n_nodes as usize]
        };

        // Build distance-based node weights from EBG edge lengths (m).
        // Used for isodistance isochrones: same role as ModeData.node_weights but distance-based.
        let node_weights_dist: Vec<u32> = ebg_nodes.nodes.iter().map(|n| n.length_m).collect();
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

        // #402: wrap each ModeData in a lazy/evictable slot. The
        // directory-tree loader has no container backing, so the
        // lazy reload path will panic if a slot is ever evicted —
        // this path is only used by tests + the legacy --data-dir
        // flow where eviction shouldn't fire.
        let (modes_slots, mode_names, mode_lookup) = tables.finish();
        Ok(Self {
            ebg_nodes,
            ebg_csr,
            nbg_geo,
            edge_geom,
            // #460: chains only ship in containers; the dir-tree boot
            // path serves NBG-endpoint rows.
            edge_osm: crate::server::edge_osm::EdgeOsmChains::empty(),
            nbg_node_to_osm,
            modes: modes_slots,
            band_worst_idx: None,
            band_best_idx: None,
            car_freeflow_idx: None,
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
        // Page-fault footprint after a `Sections::arc` call — i.e. AFTER
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

        // The section accessors used by every phase below: they resolve
        // a name to a byte range, drive the lazy CRC gate, and hand the
        // format readers an `Arc<Mmap>` clone.
        let sec = shared::Sections::new(&container, &mmap, &lazy_arc);

        // ---- Shared graph tables ------------------------------------
        crate::server::rss::checkpoint("load.container.opened");
        let graph = shared::load_shared_tables(&sec)?;

        // ---- Mode discovery + filter --------------------------------
        let (discovered_modes, global_index) =
            modes::discover(&container, mode_filter, container_path)?;

        crate::server::rss::checkpoint("load.shared");

        // ---- Per-mode bundle load -----------------------------------
        let mut tables = modes::load_bundles(&sec, &discovered_modes, &global_index)?;

        // ---- Packed snap index (#154) -------------------------------
        let mut snap_index = snap::load_or_build(&sec, &graph, &tables)?;

        // ---- Cold per-mode sections ---------------------------------
        modes::evict_cold_mode_sections(&sec, &discovered_modes);

        // ---- Road names, exclude flags, elevation, geometry ----------
        let aux = auxiliary::load_auxiliary(&sec, &graph, &discovered_modes, container_path)?;

        // #160: optionally schedule a background warmup pass to walk
        // every still-`Unverified` section's CRC in parallel. This
        // matches pre-#160 total-coverage at the cost of a transient
        // page-fault burst, but does NOT block the listener.
        if opts.warmup_on_boot {
            tracing::info!("scheduling background CRC warmup pass for unverified sections");
            lazy_arc.spawn_warmup();
        }

        // ---- Hidden free-flow car base (#450) -----------------------
        let car_freeflow_idx = modes::register_car_freeflow(&mut tables, &mut snap_index.masks);

        let (modes_slots, mode_names, mode_lookup) = tables.finish();
        let shared::SharedTables {
            ebg_nodes,
            ebg_csr,
            nbg_geo,
            nbg_node_to_osm,
            has_flat_edge_geom: _,
        } = graph;
        Ok(Self {
            ebg_nodes,
            ebg_csr,
            nbg_geo,
            edge_geom: aux.edge_geom,
            edge_osm: aux.edge_osm,
            nbg_node_to_osm,
            modes: modes_slots,
            band_worst_idx: None,
            band_best_idx: None,
            car_freeflow_idx,
            mode_names,
            mode_lookup,
            snap_index,
            elevation: aux.elevation,
            way_names: aux.way_names,
            node_weights_dist: aux.node_weights_dist,
            edge_exclude_flags: aux.edge_exclude_flags,
            avoid_cache: super::avoid::AvoidWeightCache::default(),
            transit: None,
            started_at: std::time::Instant::now(),
            data_dir: container_path.to_string_lossy().to_string(),
            _mmap_arc: Some(mmap),
            lazy: Some(lazy_arc),
        })
    }

    /// Get mode data by mode (index-based lookup). #402: lazy-reloads
    /// if the slot was previously evicted by the idle compactor.
    /// Returns an `Arc<ModeData>` — holding the Arc keeps the mode
    /// alive for the duration of the caller's work even if the
    /// compactor races to evict in the background.
    pub fn get_mode(&self, mode: Mode) -> std::sync::Arc<ModeData> {
        let slot = &self.modes[mode.index()];
        // Fast path: resident slot.
        {
            let r = slot.state.read();
            if let Some(arc) = r.as_ref() {
                slot.last_used_ms.store(
                    self.started_at.elapsed().as_millis() as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
                return std::sync::Arc::clone(arc);
            }
        }
        // Slow path: evicted, lazy-reload under write lock with
        // single-flight semantics.
        let mut w = slot.state.write();
        if let Some(arc) = w.as_ref() {
            // Someone else loaded while we waited for the lock.
            slot.last_used_ms.store(
                self.started_at.elapsed().as_millis() as u64,
                std::sync::atomic::Ordering::Relaxed,
            );
            return std::sync::Arc::clone(arc);
        }
        let loaded = self
            .lazy_load_mode(&slot.mode_name, mode)
            .unwrap_or_else(|e| {
                panic!(
                    "#402 lazy reload of mode '{}' failed: {}",
                    slot.mode_name, e
                )
            });
        let arc = std::sync::Arc::new(loaded);
        *w = Some(std::sync::Arc::clone(&arc));
        slot.last_used_ms.store(
            self.started_at.elapsed().as_millis() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
        tracing::info!(
            mode = slot.mode_name.as_str(),
            "#402 lazy-reloaded mode after eviction"
        );
        arc
    }

    /// #402: re-run the container loader for a single mode. Used by
    /// `get_mode` on the slow path when the slot has been evicted.
    /// Requires that the container path was used to construct
    /// `ServerState` (i.e. `_mmap_arc` and `lazy` are populated).
    fn lazy_load_mode(&self, mode_name: &str, mode: Mode) -> Result<ModeData> {
        let mmap = self._mmap_arc.as_ref().ok_or_else(|| {
            anyhow::anyhow!("lazy_load_mode requires container-backed ServerState")
        })?;
        let lazy = self
            .lazy
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("lazy_load_mode requires LazyContainer"))?;
        let container = lazy.container();
        load_mode_data_from_bundle(mode_name, mode, container, mmap, lazy)
    }

    /// #402: evict a single mode slot if it has been idle for at least
    /// `threshold_ms`. Drops the inner `Arc<ModeData>`; any in-flight
    /// query holding its own Arc clone keeps the data alive until that
    /// query finishes. Returns `true` if the slot was evicted.
    pub fn try_evict_mode_if_idle(&self, mode_idx: usize, threshold_ms: u64) -> bool {
        let slot = &self.modes[mode_idx];
        if !slot.evictable.load(std::sync::atomic::Ordering::Relaxed) {
            // #402: a synthetic mode has no bundle to reload via
            // load_mode_data_from_bundle. Skip. (#433: the serve-boot
            // recustomized car is also pinned here.)
            return false;
        }
        let now = self.started_at.elapsed().as_millis() as u64;
        let last = slot.last_used_ms.load(std::sync::atomic::Ordering::Relaxed);
        // `last == 0` means "never touched by get_mode" — for those
        // we measure idleness from `now` directly, which makes never-
        // queried modes the most evictable. Boot grace is provided by
        // the compactor's poll interval (won't run until at least
        // ~threshold/4 after boot).
        let idle_ms = now.saturating_sub(last);
        if idle_ms < threshold_ms {
            return false;
        }
        let mut w = slot.state.write();
        // Re-check under the write lock to handle races with get_mode AND with
        // a concurrent pin. #433: a background recustomize can flip `evictable`
        // false→pinned after our pre-lock read above; without this re-check the
        // compactor could evict (and the next query lazy-reload the CLEAN base)
        // a slot that was just pinned. The synchronous boot path pins before the
        // compactor is even spawned, so this is defense-in-depth that also keeps
        // the background-swap variant correct.
        if !slot.evictable.load(std::sync::atomic::Ordering::Relaxed) {
            return false;
        }
        let last2 = slot.last_used_ms.load(std::sync::atomic::Ordering::Relaxed);
        if now.saturating_sub(last2) < threshold_ms {
            return false;
        }
        if w.take().is_some() {
            tracing::info!(
                mode = slot.mode_name.as_str(),
                idle_ms = now.saturating_sub(last),
                "#402 evicted idle mode"
            );
            true
        } else {
            false
        }
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

        // #407: bounded-LRU fast path.
        if let Some(weights) = mode_data.exclude_cache.get(exclude_mask) {
            return weights;
        }

        // Miss: compute, then insert (evicting the LRU entry if at cap).
        // Two simultaneous misses on the same mask may both compute and
        // the second insert wins — the same benign racy semantics
        // AvoidWeightCache documents; the only cost is a rare duplicate
        // recustomization, never a correctness issue.
        let mode_name = &self.mode_names[mode.index()];
        tracing::info!(
            mode = mode_name.as_str(),
            exclude_mask,
            "computing exclude weights (cache miss)"
        );

        let weights = std::sync::Arc::new(exclude::compute_exclude_weights(
            &mode_data.cch_topo,
            &mode_data.cch_weights,
            &mode_data.cch_weights_dist,
            &self.edge_exclude_flags,
            exclude_mask,
            &mode_data.filtered_to_original,
        ));

        mode_data
            .exclude_cache
            .insert(exclude_mask, std::sync::Arc::clone(&weights));
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

    // #371/#372: optional length-along-time weights (cch.lat.<mode>.u32).
    // Containers built before PR #377 don't have this file; we boot
    // without it and `/table` / `/trip` / Flight matrix endpoints
    // continue to use `cch_weights_dist` (the broken metric). Once
    // the 2-channel bucket-M2M lands (and Belgium is repacked), the
    // matrix endpoints switch to this for drivetime-consistent
    // distance reporting.
    let cch_lat_weights_path = step8_dir.join(format!("cch.lat.{}.u32", mode_name));
    let cch_weights_len_along_time = if cch_lat_weights_path.exists() {
        tracing::info!(mode = mode_name, "loading length-along-time weights");
        Some(CchWeightsFile::read(&cch_lat_weights_path)?)
    } else {
        tracing::info!(
            mode = mode_name,
            "no cch.lat.<mode>.u32 — old container, falling back to distance-shortest for /table /trip /Flight"
        );
        None
    };
    let (up_adj_flat_len_along_time, down_rev_flat_len_along_time) =
        build_len_along_time_flats(&cch_topo, &cch_weights_len_along_time);

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
        cch_weights_len_along_time,
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
        up_adj_flat_len_along_time,
        down_rev_flat_len_along_time,
        down_adj_flat_len_along_time_lazy: std::sync::OnceLock::new(),
        exclude_cache: super::exclude::ExcludeWeightCache::default(),
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
/// Shared #372 helper: build length-along-time flats from the optional
/// `cch_weights_len_along_time`. Both `--data-dir` and container-mode
/// loaders call this so the construction stays in lock-step. Returns
/// `(None, None)` when weights are absent (old containers / pre-#377
/// step8 outputs).
fn build_len_along_time_flats(
    cch_topo: &CchTopo,
    cch_weights_len_along_time: &Option<CchWeights>,
) -> (Option<UpAdjFlat>, Option<DownReverseAdjFlat>) {
    if let Some(ref w) = *cch_weights_len_along_time {
        (
            Some(UpAdjFlat::build(cch_topo, w)),
            Some(DownReverseAdjFlat::build(cch_topo, w)),
        )
    } else {
        (None, None)
    }
}

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

/// #345: read a split-format UpAdjFlat from a FlatTopo + FlatWeights
/// section pair. Returns None if either section is absent so the
/// caller can fall back to the legacy v4 path.
fn try_load_flat_split_up(
    container: &crate::formats::butterfly_dat::Container,
    mmap: &std::sync::Arc<memmap2::Mmap>,
    lazy: &std::sync::Arc<crate::formats::lazy_verify::LazyContainer>,
    mode_name: &str,
    metric: &str,
) -> Result<Option<crate::matrix::bucket_ch::UpAdjFlat>> {
    let topo_name = format!("mode/{}/up_adj.topo", mode_name);
    let weights_name = format!("mode/{}/up_adj.weights.{}", mode_name, metric);
    let (Some(topo_entry), Some(weights_entry)) =
        (container.get(&topo_name), container.get(&weights_name))
    else {
        return Ok(None);
    };
    lazy.verify_now(&topo_name)?;
    lazy.verify_now(&weights_name)?;
    let (offsets, targets, topo_edge_idx) = crate::matrix::bucket_ch::decode_flat_topo_from_mmap(
        std::sync::Arc::clone(mmap),
        topo_entry.offset as usize,
        topo_entry.len as usize,
    )?;
    let weights = crate::matrix::bucket_ch::decode_flat_weights_from_mmap(
        std::sync::Arc::clone(mmap),
        weights_entry.offset as usize,
        weights_entry.len as usize,
    )?;
    Ok(Some(crate::matrix::bucket_ch::UpAdjFlat {
        offsets,
        targets,
        weights,
        topo_edge_idx,
    }))
}

/// #345: read a split-format DownReverseAdjFlat from FlatTopo +
/// FlatWeights. Returns None if either section is absent.
fn try_load_flat_split_down_rev(
    container: &crate::formats::butterfly_dat::Container,
    mmap: &std::sync::Arc<memmap2::Mmap>,
    lazy: &std::sync::Arc<crate::formats::lazy_verify::LazyContainer>,
    mode_name: &str,
    metric: &str,
) -> Result<Option<crate::matrix::bucket_ch::DownReverseAdjFlat>> {
    let topo_name = format!("mode/{}/down_reverse_adj.topo", mode_name);
    let weights_name = format!("mode/{}/down_reverse_adj.weights.{}", mode_name, metric);
    let (Some(topo_entry), Some(weights_entry)) =
        (container.get(&topo_name), container.get(&weights_name))
    else {
        return Ok(None);
    };
    lazy.verify_now(&topo_name)?;
    lazy.verify_now(&weights_name)?;
    let (offsets, sources, topo_edge_idx) = crate::matrix::bucket_ch::decode_flat_topo_from_mmap(
        std::sync::Arc::clone(mmap),
        topo_entry.offset as usize,
        topo_entry.len as usize,
    )?;
    let weights = crate::matrix::bucket_ch::decode_flat_weights_from_mmap(
        std::sync::Arc::clone(mmap),
        weights_entry.offset as usize,
        weights_entry.len as usize,
    )?;
    Ok(Some(crate::matrix::bucket_ch::DownReverseAdjFlat {
        offsets,
        sources,
        weights,
        topo_edge_idx,
    }))
}

/// #345: read a split-format DownAdjFlat from FlatTopo +
/// FlatWeights. Returns None if either section is absent.
fn try_load_flat_split_down(
    container: &crate::formats::butterfly_dat::Container,
    mmap: &std::sync::Arc<memmap2::Mmap>,
    lazy: &std::sync::Arc<crate::formats::lazy_verify::LazyContainer>,
    mode_name: &str,
    metric: &str,
) -> Result<Option<crate::matrix::bucket_ch::DownAdjFlat>> {
    let topo_name = format!("mode/{}/down_adj.topo", mode_name);
    let weights_name = format!("mode/{}/down_adj.weights.{}", mode_name, metric);
    let (Some(topo_entry), Some(weights_entry)) =
        (container.get(&topo_name), container.get(&weights_name))
    else {
        return Ok(None);
    };
    lazy.verify_now(&topo_name)?;
    lazy.verify_now(&weights_name)?;
    let (offsets, targets, _topo_idx_ignored) =
        crate::matrix::bucket_ch::decode_flat_topo_from_mmap(
            std::sync::Arc::clone(mmap),
            topo_entry.offset as usize,
            topo_entry.len as usize,
        )?;
    let weights = crate::matrix::bucket_ch::decode_flat_weights_from_mmap(
        std::sync::Arc::clone(mmap),
        weights_entry.offset as usize,
        weights_entry.len as usize,
    )?;
    Ok(Some(crate::matrix::bucket_ch::DownAdjFlat {
        offsets,
        targets,
        weights,
    }))
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
    let mut cch_topo = CchTopoFile::read_from_mmap_unverified(topo_mmap, topo_off, topo_len)?;
    // #359: if the topo's middles are absent (split format), populate
    // them from the CchMiddles sibling section. This is the
    // matrix-RAM-isolation path — middles live in their own cold
    // section that matrix-only workloads can madvise(DONTNEED).
    if cch_topo.up_middle.is_empty() && cch_topo.down_middle.is_empty() {
        let middles_name = format!("mode/{}/middles", mode_name);
        if let Some(entry) = container.get(&middles_name) {
            let mid_off = entry.offset as usize;
            let mid_len = entry.len as usize;
            lazy.verify_now(&middles_name)?;
            let middles = crate::formats::cch_middles::decode_section_from_mmap(
                std::sync::Arc::clone(mmap),
                mid_off,
                mid_len,
            )?;
            cch_topo.up_middle = middles.up_middle;
            cch_topo.down_middle = middles.down_middle;
            // #359 Phase 4: madvise(DONTNEED) on the CchMiddles range.
            // CRC verification above paged the bytes in; matrix /
            // isochrone / bucket-M2M never touch middles, so we hint
            // the kernel to drop those pages. Route-unpack paths page
            // them back in on demand at standard fault cost. Estimated
            // ~300-420 MB RSS savings per Belgium mode under matrix
            // load (codex assessment on #352).
            let middles_bytes_for_madvise = &mmap[mid_off..mid_off + mid_len];
            if let Err(e) = crate::formats::mmap::madvise_dontneed(middles_bytes_for_madvise) {
                tracing::warn!(
                    section = %middles_name,
                    error = %e,
                    "madvise(DONTNEED) on cch.middles section failed; ignoring"
                );
            } else {
                tracing::info!(
                    section = %middles_name,
                    bytes = mid_len,
                    "loaded CchMiddles + madvise(DONTNEED) (#359 — cold section, route unpack pages back on demand)"
                );
            }
        }
    }
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
    // #345: prefer the split FlatTopo + FlatWeights sections; fall
    // back to the legacy v4 monolithic flat; fall back to building
    // from cch_topo + cch_weights on the heap if neither is present.
    let up_adj_flat =
        if let Some(f) = try_load_flat_split_up(container, mmap, lazy, mode_name, "time")? {
            f
        } else {
            load_flat_section(
                container,
                mmap,
                &format!("mode/{}/up_adj_flat.time", mode_name),
                lazy,
                |m, off, len| UpAdjFlatFile::read_from_mmap_unverified(m, off, len),
                || UpAdjFlat::build_with(&cch_topo, &cch_weights, true),
            )?
        };
    let down_rev_flat =
        if let Some(f) = try_load_flat_split_down_rev(container, mmap, lazy, mode_name, "time")? {
            f
        } else {
            load_flat_section(
                container,
                mmap,
                &format!("mode/{}/down_reverse_adj_flat.time", mode_name),
                lazy,
                |m, off, len| DownReverseAdjFlatFile::read_from_mmap_unverified(m, off, len),
                || DownReverseAdjFlat::build_with(&cch_topo, &cch_weights, true),
            )?
        };
    let down_adj_flat =
        if let Some(f) = try_load_flat_split_down(container, mmap, lazy, mode_name, "time")? {
            f
        } else {
            load_flat_section(
                container,
                mmap,
                &format!("mode/{}/down_adj_flat.time", mode_name),
                lazy,
                |m, off, len| DownAdjFlatFile::read_from_mmap_unverified(m, off, len),
                || DownAdjFlat::build(&cch_topo, &cch_weights),
            )?
        };

    let (wd_mmap, wd_off, wd_len) = fetch_arc("weights.dist")?;
    let cch_weights_dist = CchWeightsFile::read_from_mmap_unverified(wd_mmap, wd_off, wd_len)?;
    let up_adj_flat_dist_section = format!("mode/{}/up_adj_flat.dist", mode_name);
    let up_adj_flat_dist =
        if let Some(f) = try_load_flat_split_up(container, mmap, lazy, mode_name, "dist")? {
            f
        } else {
            load_flat_section(
                container,
                mmap,
                &up_adj_flat_dist_section,
                lazy,
                |m, off, len| UpAdjFlatFile::read_from_mmap_unverified(m, off, len),
                || UpAdjFlat::build(&cch_topo, &cch_weights_dist),
            )?
        };
    madvise_section_in_container(container, mmap, &up_adj_flat_dist_section);
    let down_rev_flat_dist_section = format!("mode/{}/down_reverse_adj_flat.dist", mode_name);
    let down_rev_flat_dist =
        if let Some(f) = try_load_flat_split_down_rev(container, mmap, lazy, mode_name, "dist")? {
            f
        } else {
            load_flat_section(
                container,
                mmap,
                &down_rev_flat_dist_section,
                lazy,
                |m, off, len| DownReverseAdjFlatFile::read_from_mmap_unverified(m, off, len),
                || DownReverseAdjFlat::build(&cch_topo, &cch_weights_dist),
            )?
        };
    madvise_section_in_container(container, mmap, &down_rev_flat_dist_section);

    // #371/#372: length-along-time weights section, when present.
    // Pre-PR #377 containers don't have this section; we boot with
    // None and matrix endpoints fall back to cch_weights_dist
    // (broken metric, see #371 / #372). Once everyone repacks, this
    // becomes a hard load like weights.dist above.
    let cch_weights_len_along_time = {
        let name = format!("mode/{}/weights.lat", mode_name);
        if let Some(entry) = container.get(&name) {
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
            Some(CchWeightsFile::read_from_mmap_unverified(
                mmap.clone(),
                off,
                len,
            )?)
        } else {
            None
        }
    };
    let (up_adj_flat_len_along_time, down_rev_flat_len_along_time) =
        build_len_along_time_flats(&cch_topo, &cch_weights_len_along_time);

    Ok(ModeData {
        mode,
        cch_topo,
        cch_weights,
        cch_weights_dist,
        cch_weights_len_along_time,
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
        up_adj_flat_len_along_time,
        down_rev_flat_len_along_time,
        down_adj_flat_len_along_time_lazy: std::sync::OnceLock::new(),
        exclude_cache: super::exclude::ExcludeWeightCache::default(),
    })
}

/// #450: field-clone a loaded ModeData. Cheap on the container path — every
/// heavy field is Arc/mmap-backed (ArcCow / WeightArray / flats borrowed from
/// the container mapping); only small Vecs copy. Used to register
/// `car_freeflow` as an alias of the pre-recustomization base car.
/// #528: rebuild the (Option) len-along-time weights + flats for a car-family
/// mode whose TIME weights were just recustomized. The base build-time lat
/// describes the CLEAN paths; the new time middles describe the recustomized
/// paths, so the length that belongs to the served duration is recomputed
/// from those middles (physical lengths are traffic-invariant). Returns
/// cloned-None when the mode carries no lat weights (old container).
fn refresh_len_along_time(
    base: &ModeData,
    ebg_nodes: &EbgNodes,
    new_time: &crate::formats::CchWeights,
    node_weights_time: &[u32],
) -> (
    Option<crate::formats::CchWeights>,
    Option<UpAdjFlat>,
    Option<DownReverseAdjFlat>,
) {
    if base.cch_weights_len_along_time.is_none() {
        return (None, None, None);
    }
    let lat = crate::customization::recompute_len_along_time_from_middles(
        &base.cch_topo,
        &base.filtered_to_original,
        ebg_nodes,
        node_weights_time,
        new_time.up_middle.as_ref(),
        new_time.down_middle.as_ref(),
    );
    let up = UpAdjFlat::build(&base.cch_topo, &lat);
    let down_rev = DownReverseAdjFlat::build(&base.cch_topo, &lat);
    (Some(lat), Some(up), Some(down_rev))
}

fn clone_mode_data(base: &ModeData) -> ModeData {
    ModeData {
        mode: base.mode,
        cch_topo: base.cch_topo.clone(),
        cch_weights: base.cch_weights.clone(),
        cch_weights_dist: base.cch_weights_dist.clone(),
        cch_weights_len_along_time: base.cch_weights_len_along_time.clone(),
        orig_to_rank: base.orig_to_rank.clone(),
        filtered_to_original: base.filtered_to_original.clone(),
        n_filtered_nodes: base.n_filtered_nodes,
        n_original_nodes: base.n_original_nodes,
        node_weights: base.node_weights.clone(),
        mask: base.mask.clone(),
        has_outbound: base.has_outbound.clone(),
        has_inbound: base.has_inbound.clone(),
        up_adj_flat: base.up_adj_flat.clone(),
        down_rev_flat: base.down_rev_flat.clone(),
        down_adj_flat: base.down_adj_flat.clone(),
        up_adj_flat_dist: base.up_adj_flat_dist.clone(),
        down_rev_flat_dist: base.down_rev_flat_dist.clone(),
        up_adj_flat_len_along_time: base.up_adj_flat_len_along_time.clone(),
        down_rev_flat_len_along_time: base.down_rev_flat_len_along_time.clone(),
        down_adj_flat_len_along_time_lazy: std::sync::OnceLock::new(),
        exclude_cache: super::exclude::ExcludeWeightCache::default(),
    }
}

#[cfg(test)]
mod tests {
    use super::build_role_masks;
    use crate::formats::FilteredEbg;
    use crate::model::types::Mode;

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
