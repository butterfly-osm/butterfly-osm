//! Multi-region container loading + same-region query dispatch (#91 Phase 1).
//!
//! [`RegionsState`] is the top-level server state that wraps one or more
//! [`ServerState`] instances — one per loaded region — together with a
//! lightweight dispatcher that picks the right region for a given query.
//!
//! # Discovery
//!
//! `serve --data-dir <dir>` discovers `*.butterfly` files in `<dir>`.
//! Each container is opened once, its `shared/manifest.json` is parsed
//! for the embedded `region_id`, and the per-region `ServerState` is
//! built from the container exactly the same way the single-region
//! `--data <file>` path builds it. Optional `--regions BE,LU` filters
//! the discovery to a subset.
//!
//! # Dispatch
//!
//! Each routing request snaps its source (and target, if any) to a
//! road sample. The snap is performed in *every* loaded region; the
//! region with the closest snap wins. If source and target snap into
//! different regions, the request returns HTTP 501 with a
//! `route spans regions X → Y; cross-region overlay not yet
//! implemented (#91 Phase 2)` payload — the cross-region overlay is
//! deferred to PR C.
//!
//! # Out of scope (PR C)
//!
//! - Cross-region overlay graph, border-node extraction, border-matrix
//!   precomputation. The 501 path here is the correctness invariant
//!   that prevents wrong answers in the meantime.
//! - Per-region transit. Transit is loaded against the *first*
//!   discovered region's foot CCH today (Belgium-shaped deployment);
//!   multi-region transit is out of scope.

use anyhow::{Context, Result};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

/// Monotonic millisecond offset from server boot. Used for LRU
/// timestamps in [`RegionEntry::last_used_ms`]. The exact epoch isn't
/// important — only the ordering between calls within one process —
/// so we anchor at the first call via a [`std::sync::OnceLock`].
fn boot_offset_ms() -> u64 {
    use std::sync::OnceLock;
    static EPOCH: OnceLock<std::time::Instant> = OnceLock::new();
    let epoch = *EPOCH.get_or_init(std::time::Instant::now);
    std::time::Instant::now().duration_since(epoch).as_millis() as u64
}

use super::state::ServerState;
use super::types::ErrorResponse;

/// One loaded region: container path, region id, and the per-region
/// `ServerState`. `verify_status` records whether the per-section CRC
/// walk completed cleanly during boot. (Boot today is eager-CRC; #160
/// introduced lazy CRC and may extend `VerifyStatus` to a per-section
/// shape.)
pub struct RegionEntry {
    pub id: String,
    pub container: PathBuf,
    /// #292 Phase 4: cached snap_points bbox, peeked from the container
    /// at registration time without loading the rest of the region.
    /// Drives the lazy `snap_winner` filter so a query that lies
    /// entirely outside a Pending region's coverage box never triggers
    /// its load. `None` for legacy containers whose snap_points header
    /// could not be peeked — those fall back to the unconditional snap
    /// (and trigger load) as before.
    pub bbox: Option<crate::formats::SnapBbox>,
    /// #292 Phase 4: sorted list of mode names available in the
    /// container, peeked from the section directory at registration.
    /// Lets `has_mode` / `available_modes` answer without forcing a
    /// Pending region to load just to enumerate its modes.
    pub mode_names: Vec<String>,
    /// #142: coarse 0.1° tile coverage set for the region's road
    /// network. Loaded at registration when `shared/region_tiles` is
    /// present in the container. Drives `snap_winner`'s tighter
    /// pre-filter after the bbox check — adjacent regions only
    /// false-positive on tiles that genuinely border, instead of any
    /// bbox overlap. `None` for legacy containers without the
    /// section; `snap_winner` falls back to bbox-only filtering.
    pub tiles: Option<crate::formats::RegionTiles>,
    /// #292 Phase 2: lazy-loadable region state. `Loaded(Arc<ServerState>)`
    /// is the after-load steady state; `Pending` is the lazy-boot
    /// default — entries are registered as Pending and the first call
    /// to [`Self::state()`] drives the per-container load. Operators
    /// can opt back into eager boot with `--eager-regions`.
    ///
    /// Private so the choice of representation can evolve (e.g. add an
    /// `Unloaded` variant for LRU eviction) without breaking callers.
    /// Use [`Self::state()`] to read.
    state_cell: parking_lot::RwLock<RegionState>,
    /// #292 Phase 6: monotonic timestamp (millis since server start)
    /// of the most recent [`Self::state()`] call that found the entry
    /// `Loaded` (fast path) or transitioned it to `Loaded` (slow path).
    /// Drives LRU eviction: when the background poller is over the
    /// RSS budget it evicts the region with the smallest `last_used`.
    /// `0` means "never accessed since registration".
    pub last_used_ms: std::sync::atomic::AtomicU64,
    /// Snapshot of the section verification state for this region —
    /// see [`VerifyStatus`]. Today this is always `Verified` once a
    /// region is added to [`RegionsState`] because the boot path bails
    /// on the first per-section CRC failure; the field is exposed so
    /// the `/regions` endpoint can report it explicitly and so future
    /// per-section variants don't break the JSON shape.
    pub verify_status: VerifyStatus,
    /// Pre-allocated per-region metric handles. One Counter +
    /// Histogram per (endpoint) entry from
    /// [`super::region_metrics::ENDPOINTS`], plus the size gauges.
    /// Hot path looks up the handle by endpoint key and increments /
    /// observes on it directly — saves the `region.to_string()` +
    /// `endpoint.to_string()` allocations the macro path imposed.
    pub metrics: super::region_metrics::RegionMetrics,
}

/// #292 Phase 2: per-region load state.
///
/// `Pending` is the lazy default: the container path is registered
/// without a corresponding ServerState, and the first call to
/// [`RegionEntry::state`] drives the load on the serving thread.
/// `Loaded(Arc<ServerState>)` is the after-load steady state — also
/// the variant used when an operator forces eager boot via
/// `--eager-regions` (every region is `Loaded` at boot then).
///
/// An `Unloaded` variant for LRU eviction will be added alongside the
/// memory-budget background task.
enum RegionState {
    /// Lazy registration — container path known, ServerState not yet
    /// constructed. This is the default state of every entry built by
    /// the multi-region `serve --data-dir` boot path.
    Pending,
    Loaded(Arc<ServerState>),
}

impl RegionEntry {
    /// Read the per-region `ServerState`, lazy-loading from the
    /// container on first access if the entry was registered as
    /// `Pending`. The default `serve --data-dir` boot path is
    /// **lazy** (every entry starts as `Pending`); operators that
    /// pass `--eager-regions` get the legacy stall-at-boot behaviour
    /// where every entry is constructed `Loaded` up front. Either
    /// way, once an entry is `Loaded` the read-lock fast path wins.
    ///
    /// Panics if the lazy load fails. This is intentional for now:
    /// a corrupt-container failure is a deployment-time error, not a
    /// per-request graceful-degradation case. The panic propagates
    /// out of the serving handler and Axum's `CatchPanicLayer` turns
    /// it into a 500. A future [`Self::try_state`] returning
    /// `Result<Arc<ServerState>>` would let handlers downgrade to a
    /// 503 with `Retry-After`; until that ships, treat
    /// `state()` as the panic-on-fail flavour.
    ///
    /// #292 Phase 6: every successful resolution (fast path or slow
    /// path) bumps `last_used_ms` so the LRU eviction poller can pick
    /// the genuinely-oldest region without false-positives on entries
    /// that were recently touched.
    #[inline]
    pub fn state(&self) -> Arc<ServerState> {
        // Fast path: read lock; if Loaded, clone the Arc and return.
        if let RegionState::Loaded(arc) = &*self.state_cell.read() {
            self.touch();
            return Arc::clone(arc);
        }
        // Slow path: take write lock, double-check, load + cache.
        let mut guard = self.state_cell.write();
        if let RegionState::Loaded(arc) = &*guard {
            self.touch();
            return Arc::clone(arc);
        }
        let load_start = std::time::Instant::now();
        let state = ServerState::load_from_container(&self.container, None).unwrap_or_else(|e| {
            panic!(
                "lazy region load failed for {}: {}",
                self.container.display(),
                e
            )
        });
        tracing::info!(
            region = %self.id,
            container = %self.container.display(),
            load_ms = load_start.elapsed().as_millis() as u64,
            nodes = state.ebg_nodes.n_nodes,
            edges = state.ebg_csr.n_arcs,
            "lazy-loaded region on demand"
        );
        let arc = Arc::new(state);
        *guard = RegionState::Loaded(Arc::clone(&arc));
        self.touch();
        arc
    }

    /// Bump `last_used_ms` to the current monotonic offset. Cheap atomic
    /// store (no fence — LRU comparison is racy by design, off-by-a-tick
    /// is fine).
    #[inline]
    fn touch(&self) {
        let now_ms = boot_offset_ms();
        self.last_used_ms
            .store(now_ms, std::sync::atomic::Ordering::Relaxed);
    }

    /// Snapshot of the last time this region was touched, in millis
    /// since server boot. `0` means "never accessed".
    #[inline]
    pub fn last_used(&self) -> u64 {
        self.last_used_ms.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// #292 Phase 6: evict the loaded ServerState back to `Pending`,
    /// dropping the strong `Arc` reference held by `state_cell`.
    /// In-flight clones survive until their requests finish; on the
    /// last drop the `ServerState` destructor runs and mmaps unmap.
    ///
    /// Returns `true` if a state was evicted, `false` if the entry was
    /// already `Pending`. The eviction takes the write lock — concurrent
    /// `state()` calls will briefly block, then either hit the new
    /// `Pending` and lazy-reload, or pass through if `Loaded`.
    pub fn try_evict(&self) -> bool {
        let mut guard = self.state_cell.write();
        match &*guard {
            RegionState::Pending => false,
            RegionState::Loaded(arc) => {
                let strong = Arc::strong_count(arc);
                tracing::info!(
                    region = %self.id,
                    in_flight = strong.saturating_sub(1),
                    "evicting region back to Pending"
                );
                *guard = RegionState::Pending;
                // Reset LRU stamp so it doesn't beat regions that are
                // genuinely older next round.
                self.last_used_ms
                    .store(0, std::sync::atomic::Ordering::Relaxed);
                true
            }
        }
    }

    /// `true` if this entry is currently `Loaded`. Cheap read-lock peek;
    /// used by the eviction poller to enumerate candidates without
    /// triggering a load.
    #[inline]
    pub fn is_loaded(&self) -> bool {
        matches!(&*self.state_cell.read(), RegionState::Loaded(_))
    }

    /// Non-loading peek: returns `Some(Arc<ServerState>)` if the entry
    /// is already `Loaded`, `None` if `Pending`. Used by code that
    /// reports on already-loaded regions (e.g. metrics, /regions
    /// endpoint) but doesn't want to trigger a full lazy load just to
    /// publish a stat.
    #[inline]
    pub fn state_loaded(&self) -> Option<Arc<ServerState>> {
        if let RegionState::Loaded(arc) = &*self.state_cell.read() {
            Some(Arc::clone(arc))
        } else {
            None
        }
    }

    /// Boot-only helper: invoke `f` with a `&mut ServerState`. If the
    /// entry is `Pending` (the lazy multi-region default), this
    /// transitions it to `Loaded` first — every caller of this helper
    /// runs at boot, before any handler clone leaks out, so triggering
    /// the load here is the right semantic. If the entry is `Loaded`
    /// but the inner `Arc` was already shared (refcount > 1) returns
    /// an error — the caller is then on the hook for explaining why
    /// state escaped before boot finished.
    pub fn with_loaded_state_mut<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&mut ServerState) -> R,
    {
        let mut guard = self.state_cell.write();
        // If still Pending, drive the per-container load synchronously.
        // This is called only from boot (transit attach), so triggering
        // a load here is intentional even on the otherwise-lazy path.
        if matches!(&*guard, RegionState::Pending) {
            let load_start = std::time::Instant::now();
            let state = ServerState::load_from_container(&self.container, None).map_err(|e| {
                anyhow::anyhow!(
                    "lazy region load failed for {}: {}",
                    self.container.display(),
                    e
                )
            })?;
            tracing::info!(
                region = %self.id,
                container = %self.container.display(),
                load_ms = load_start.elapsed().as_millis() as u64,
                nodes = state.ebg_nodes.n_nodes,
                edges = state.ebg_csr.n_arcs,
                "loaded region for boot-time mutation (transit attach)"
            );
            *guard = RegionState::Loaded(Arc::new(state));
            self.touch();
        }
        match &mut *guard {
            RegionState::Loaded(arc) => {
                let s = Arc::get_mut(arc).ok_or_else(|| {
                    anyhow::anyhow!("region state already shared (cannot get_mut)")
                })?;
                Ok(f(s))
            }
            RegionState::Pending => unreachable!("transitioned above"),
        }
    }
}

/// State of a region's CRC-verification at boot.
///
/// Today the boot path verifies every section eagerly (so any region
/// that makes it into [`RegionsState`] is `Verified`). When #160 lands,
/// `Pending` becomes possible for sections that have not yet been
/// touched on the serve path. The variant is part of the public API now
/// so adding `Pending` later does not break the `/regions` JSON shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerifyStatus {
    /// All sections verified at boot.
    Verified,
}

impl VerifyStatus {
    /// String label used in the JSON `/regions` response.
    pub fn label(self) -> &'static str {
        match self {
            VerifyStatus::Verified => "verified",
        }
    }
}

/// Top-level multi-region server state. Holds every loaded region in
/// `regions` plus an `id → index` lookup in `by_id` and an optional
/// cross-region overlay. Cloned `Arc` views of an inner
/// [`ServerState`] are returned by [`RegionsState::dispatch_p2p`] /
/// [`RegionsState::dispatch_single`] so request handlers can run their
/// query body unchanged.
pub struct RegionsState {
    /// All loaded regions, in deterministic order (sorted by region id).
    pub regions: Vec<RegionEntry>,
    /// Region id → index into `regions`. Used by `/regions` introspection
    /// and by the dispatcher's "I know the region already" fast path
    /// (today only used by tests, but a public attribute on
    /// [`RegionEntry::id`] keeps the future overlay path's "stuff this
    /// query in region X" call site obvious).
    pub by_id: HashMap<String, usize>,
    /// Cross-region overlay (#91 Phase 2). When `Some`, cross-region
    /// queries are routed through [`Self::dispatch_p2p_with_overlay`]
    /// instead of returning [`DispatchError::CrossRegion`]. When `None`
    /// (default), cross-region queries continue to return 501 via the
    /// existing [`Self::dispatch_p2p_id`] code path.
    pub overlay: Option<Arc<super::overlay::OverlayCluster>>,
    /// #292 Phase 3: server-level boot time. Used by /health to
    /// report uptime without forcing a lazy region load. (The
    /// per-`ServerState` `started_at` is per-region-load, not server
    /// start.)
    pub server_started_at: std::time::Instant,
}

impl RegionsState {
    /// Wrap a single already-loaded `ServerState` as a one-region
    /// [`RegionsState`]. Used by the legacy single-container
    /// `serve --data <file>` and `serve --data-dir <step-tree>` paths,
    /// so handlers that take an `Arc<RegionsState>` work uniformly.
    pub fn from_single(id: impl Into<String>, container: PathBuf, state: ServerState) -> Self {
        let id = id.into();
        let metrics = super::region_metrics::RegionMetrics::new(&id);
        // Best-effort bbox peek — Loaded entries don't strictly need it
        // (snap_winner runs on the in-memory index for Loaded), but
        // keeping it consistent across constructors keeps debug output
        // and /regions JSON uniform.
        // Peek bbox+modes for parity with the multi-region path. For
        // already-Loaded entries this is informational (snap_winner
        // bbox-checks Pending regions; has_mode still consults the
        // ServerState mode_lookup once the entry is loaded), but it
        // keeps the /regions JSON shape uniform regardless of how the
        // entry was constructed.
        let (peeked_bbox, peeked_modes, peeked_tiles) = peek_region_meta(&container)
            .map(|(_, b, m, t)| (b, m, t))
            .unwrap_or((None, Vec::new(), None));
        // Prefer the live ServerState's mode names when available — for
        // legacy step-tree containers the manifest may not list modes
        // but the live state knows them.
        let mode_names = if peeked_modes.is_empty() {
            state.mode_names.clone()
        } else {
            peeked_modes
        };
        let entry = RegionEntry {
            id: id.clone(),
            container,
            bbox: peeked_bbox,
            tiles: peeked_tiles,
            mode_names,
            state_cell: parking_lot::RwLock::new(RegionState::Loaded(Arc::new(state))),
            last_used_ms: AtomicU64::new(boot_offset_ms()),
            verify_status: VerifyStatus::Verified,
            metrics,
        };
        let mut by_id = HashMap::new();
        by_id.insert(id, 0);
        Self {
            regions: vec![entry],
            by_id,
            overlay: None,
            server_started_at: std::time::Instant::now(),
        }
    }

    /// Load multiple regions from explicit container paths. Used by the
    /// overlay test fixture and by `extract-borders` / `build-overlay`
    /// CLI subcommands. Each path is opened, its `shared/manifest.json`
    /// is read for the region id, and a per-region `ServerState` is
    /// loaded. Region ids must be unique. The resulting `RegionsState`
    /// has `overlay = None`; callers wire an overlay separately.
    pub fn load_from_paths(paths: &[PathBuf]) -> Result<Self> {
        anyhow::ensure!(
            !paths.is_empty(),
            "load_from_paths requires at least one container"
        );
        let mut entries: Vec<RegionEntry> = Vec::with_capacity(paths.len());
        let mut seen: HashMap<String, PathBuf> = HashMap::new();
        for path in paths {
            let (region_id, bbox, peeked_modes, peeked_tiles) = peek_region_meta(path)
                .with_context(|| format!("reading region id from {}", path.display()))?;
            if let Some(prev) = seen.get(&region_id) {
                anyhow::bail!(
                    "duplicate region id '{}' across containers: {} and {}",
                    region_id,
                    prev.display(),
                    path.display()
                );
            }
            seen.insert(region_id.clone(), path.clone());
            let state = ServerState::load_from_container(path, None).with_context(|| {
                format!("loading region '{}' from {}", region_id, path.display())
            })?;
            let metrics = super::region_metrics::RegionMetrics::new(&region_id);
            let mode_names = if peeked_modes.is_empty() {
                state.mode_names.clone()
            } else {
                peeked_modes
            };
            entries.push(RegionEntry {
                id: region_id,
                container: path.clone(),
                bbox,
                mode_names,
                tiles: peeked_tiles,
                state_cell: parking_lot::RwLock::new(RegionState::Loaded(Arc::new(state))),
                last_used_ms: AtomicU64::new(boot_offset_ms()),
                verify_status: VerifyStatus::Verified,
                metrics,
            });
        }
        entries.sort_by(|a, b| a.id.cmp(&b.id));
        let mut by_id = HashMap::new();
        for (i, e) in entries.iter().enumerate() {
            by_id.insert(e.id.clone(), i);
        }
        Ok(Self {
            regions: entries,
            by_id,
            overlay: None,
            server_started_at: std::time::Instant::now(),
        })
    }

    /// Discover and load every `*.butterfly` container in `dir`. If
    /// `region_filter` is `Some`, only regions whose id is in the list
    /// are loaded.
    ///
    /// At least one region must load; an empty directory or a filter
    /// that excludes every container is a hard error so an operator
    /// does not accidentally start a server with zero data.
    /// `lazy: true` (the **default behaviour** for `serve --data-dir`,
    /// `serve` itself drives this through CLI) registers regions as
    /// [`RegionState::Pending`] without constructing their
    /// `ServerState` at boot. First call to [`RegionEntry::state`] for
    /// each region pays the per-container load latency (~few seconds
    /// on Belgium-sized regions); subsequent calls are free.
    ///
    /// `lazy: false` is the legacy eager-boot path that constructs
    /// every `ServerState` up front. It's reachable via
    /// `--eager-regions` for operators that explicitly want
    /// stall-at-boot semantics. The convenience wrapper
    /// [`Self::load_from_dir`] keeps the lazy default so existing
    /// callers get the sane default for free.
    pub fn load_from_dir(
        dir: &Path,
        region_filter: Option<&[String]>,
        mode_filter: Option<&[String]>,
    ) -> Result<Self> {
        Self::load_from_dir_with_opts(dir, region_filter, mode_filter, true)
    }

    /// Like [`Self::load_from_dir`] with explicit `lazy` flag. When
    /// `lazy` is true, regions are registered as `Pending` and their
    /// `ServerState` is constructed on first query. The
    /// `mode_filter` is ignored on the lazy path (it'd need to be
    /// remembered per-region; revisit when the use case appears).
    pub fn load_from_dir_with_opts(
        dir: &Path,
        region_filter: Option<&[String]>,
        mode_filter: Option<&[String]>,
        lazy: bool,
    ) -> Result<Self> {
        anyhow::ensure!(
            dir.is_dir(),
            "expected --data-dir to be a directory containing *.butterfly files; got {}",
            dir.display()
        );

        let mut containers: Vec<PathBuf> = Vec::new();
        for entry in
            std::fs::read_dir(dir).with_context(|| format!("reading data dir {}", dir.display()))?
        {
            let entry = entry?;
            let path = entry.path();
            // `metadata()` follows symlinks, so a symlinked container
            // file is treated identically to a real file. Operators
            // routinely point a multi-region directory at containers
            // that live elsewhere on disk via symlink, and integration
            // tests stage containers the same way.
            let is_file = std::fs::metadata(&path)
                .map(|m| m.is_file())
                .unwrap_or(false);
            if is_file
                && path
                    .extension()
                    .and_then(|e| e.to_str())
                    .map(|s| s.eq_ignore_ascii_case("butterfly"))
                    .unwrap_or(false)
            {
                containers.push(path);
            }
        }
        anyhow::ensure!(
            !containers.is_empty(),
            "no *.butterfly containers found in {} — multi-region serve requires at least one container",
            dir.display()
        );
        // Deterministic load order so /regions output is stable.
        containers.sort();

        // Pre-pass: read each container's manifest to map container path
        // → region id, and peek the snap_points bbox + mode list so the
        // lazy path's snap_winner / has_mode can filter without
        // loading. The whole pre-pass is <1 ms per container (manifest
        // section read + a 40-byte snap_points header read).
        type RegionPreloadEntry = (
            String,
            Option<crate::formats::SnapBbox>,
            Vec<String>,
            Option<crate::formats::RegionTiles>,
            PathBuf,
        );
        let mut to_load: Vec<RegionPreloadEntry> = Vec::new();
        let mut skipped: Vec<String> = Vec::new();
        for path in &containers {
            let (region, bbox, modes, tiles) = peek_region_meta(path)
                .with_context(|| format!("reading region id from {}", path.display()))?;
            if let Some(filter) = region_filter
                && !filter.iter().any(|r| r.eq_ignore_ascii_case(&region))
            {
                skipped.push(format!("{} (region={})", path.display(), region));
                continue;
            }
            to_load.push((region, bbox, modes, tiles, path.clone()));
        }

        if !skipped.is_empty() {
            tracing::info!(
                count = skipped.len(),
                skipped = ?skipped,
                "regions filter skipped containers"
            );
        }

        // Reject duplicate region ids — operator error, fail loudly.
        let mut seen: HashMap<&str, &Path> = HashMap::new();
        for (id, _bbox, _modes, _tiles, path) in &to_load {
            if let Some(prev) = seen.insert(id.as_str(), path.as_path()) {
                anyhow::bail!(
                    "duplicate region id '{}' across containers: {} and {}",
                    id,
                    prev.display(),
                    path.display()
                );
            }
        }

        anyhow::ensure!(
            !to_load.is_empty(),
            "no containers in {} match --regions filter {:?}",
            dir.display(),
            region_filter
        );

        // Sort by region id so by-index iteration matches by-id sort.
        to_load.sort_by(|a, b| a.0.cmp(&b.0));

        let mut regions: Vec<RegionEntry> = Vec::with_capacity(to_load.len());
        let mut by_id: HashMap<String, usize> = HashMap::new();
        if lazy && mode_filter.is_some() {
            tracing::warn!(
                "lazy region load ignores --modes (the filter would need to be remembered per-region; either pass --eager-regions to honour the filter, or revisit if needed)"
            );
        }
        for (id, bbox, peeked_modes, peeked_tiles, path) in to_load {
            let idx = regions.len();
            by_id.insert(id.clone(), idx);
            let metrics = super::region_metrics::RegionMetrics::new(&id);

            if lazy {
                // Register-only: no ServerState construction at boot.
                // First state() call drives the load.
                tracing::info!(
                    region = %id,
                    container = %path.display(),
                    bbox = ?bbox.map(|b| b.to_f64()),
                    modes = ?peeked_modes,
                    n_tiles = peeked_tiles.as_ref().map(|t| t.len()).unwrap_or(0),
                    "registered region (lazy — load on first query)"
                );
                regions.push(RegionEntry {
                    id,
                    container: path,
                    bbox,
                    mode_names: peeked_modes,
                    tiles: peeked_tiles,
                    state_cell: parking_lot::RwLock::new(RegionState::Pending),
                    last_used_ms: AtomicU64::new(0),
                    verify_status: VerifyStatus::Verified,
                    metrics,
                });
                continue;
            }

            tracing::info!(region = %id, container = %path.display(), "loading region");
            let load_start = std::time::Instant::now();
            let state = ServerState::load_from_container(&path, mode_filter)
                .with_context(|| format!("loading region '{}' from {}", id, path.display()))?;
            let elapsed = load_start.elapsed();
            tracing::info!(
                region = %id,
                container = %path.display(),
                load_ms = elapsed.as_millis() as u64,
                nodes = state.ebg_nodes.n_nodes,
                edges = state.ebg_csr.n_arcs,
                modes = ?state.mode_names,
                "loaded region"
            );
            let mode_names = if peeked_modes.is_empty() {
                state.mode_names.clone()
            } else {
                peeked_modes
            };
            regions.push(RegionEntry {
                id,
                container: path,
                bbox,
                mode_names,
                tiles: peeked_tiles,
                state_cell: parking_lot::RwLock::new(RegionState::Loaded(Arc::new(state))),
                last_used_ms: AtomicU64::new(boot_offset_ms()),
                verify_status: VerifyStatus::Verified,
                metrics,
            });
        }

        Ok(Self {
            regions,
            by_id,
            overlay: None,
            server_started_at: std::time::Instant::now(),
        })
    }

    /// Number of loaded regions.
    pub fn len(&self) -> usize {
        self.regions.len()
    }

    /// `true` if no regions are loaded. Should never be the case after
    /// successful construction; here for completeness with `len()`.
    pub fn is_empty(&self) -> bool {
        self.regions.is_empty()
    }

    /// #402: iterate all registered regions for the idle compactor's
    /// per-mode eviction sweep. Returns `&RegionEntry` so the caller
    /// can call `state_loaded()` (non-loading peek) and then walk
    /// that region's mode slots.
    pub fn iter_regions(&self) -> impl Iterator<Item = &RegionEntry> {
        self.regions.iter()
    }

    /// Look up a region by id, case-insensitive on the user's input.
    /// Ids in storage are already normalised upper-case (see
    /// [`crate::pack::normalize_region_id`]), so we upper-case the
    /// caller's input before the `by_id` lookup.
    pub fn get(&self, id: &str) -> Option<&RegionEntry> {
        let normalized = id.trim().to_ascii_uppercase();
        self.by_id.get(&normalized).map(|&i| &self.regions[i])
    }

    /// `true` if at least one loaded region carries the given transport
    /// mode. Handlers call this before [`Self::dispatch_p2p_id`] /
    /// [`Self::dispatch_single_id`] / [`Self::dispatch_many`] to detect
    /// a typo'd mode early, otherwise the dispatcher returns
    /// `NoRegion` (because no region snaps the point on a mode that
    /// doesn't exist) which the operator reads as "out of coverage"
    /// rather than "wrong mode".
    pub fn has_mode(&self, mode_name: &str) -> bool {
        // #292 Phase 4: consult the registration-time cached mode_names
        // (peeked from the container's section directory) rather than
        // forcing a Pending region to load just to enumerate its modes.
        // The cache is authoritative for any container with the
        // `mode/<m>/...` schema; legacy containers fall back to the
        // ServerState mode_lookup via `state()` (which loads them
        // eagerly in the legacy path anyway).
        let lower = mode_name.to_lowercase();
        self.regions.iter().any(|r| {
            r.mode_names.iter().any(|m| m.eq_ignore_ascii_case(&lower))
                // Legacy fallback: only consult state() for entries
                // whose container didn't expose modes in its directory.
                // For lazy-boot containers this branch is unreachable
                // because peek_region_meta filled mode_names.
                || (r.mode_names.is_empty() && r.state().mode_lookup.contains_key(&lower))
        })
    }

    /// Sorted union of every mode name across loaded regions. Used by
    /// the "Invalid mode" error to tell the caller what they could have
    /// asked for.
    pub fn available_modes(&self) -> Vec<String> {
        let mut set: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        for r in &self.regions {
            // #292 Phase 4: use cached mode_names for Pending-safety.
            if !r.mode_names.is_empty() {
                for name in &r.mode_names {
                    set.insert(name.clone());
                }
            } else {
                // Legacy container with no per-mode section names —
                // need state() to enumerate. Boot path loaded it
                // eagerly, so no lazy regression here.
                for name in r.state().mode_lookup.keys() {
                    set.insert(name.clone());
                }
            }
        }
        set.into_iter().collect()
    }

    /// Return the first region's state. Used as a fallback by metadata
    /// endpoints (`/health`, `/metrics`) and by tests that don't care
    /// which region answers. Single-region deployments behave exactly
    /// like before this PR.
    ///
    /// Phase 1 migration: returns owned `Arc<ServerState>` (was
    /// `&Arc<ServerState>`). Callers that need a reference auto-deref
    /// via `&*primary()` or store the Arc locally.
    pub fn primary(&self) -> Arc<ServerState> {
        self.regions[0].state()
    }

    /// Snap a single coordinate to whichever region's road network
    /// produces the closest hit for the given mode. The mode index is
    /// per-region — every region must carry the named mode. If a region
    /// is missing the mode, we skip it (the dispatcher must succeed in
    /// at least one region or we return `None`).
    ///
    /// Returns `(region_idx, snap_distance_m)` for the winner, or
    /// `None` if no region snapped the point.
    pub fn snap_winner(&self, lon: f64, lat: f64, mode_name: &str) -> Option<(usize, f64)> {
        // #292 Phase 4: bbox margin in degrees. A degree of latitude is
        // ~111 km; ~0.01 ≈ 1.1 km, which comfortably covers the
        // server's default snap radius (typically a few hundred metres)
        // plus any near-border slack. A query farther than 1.1 km from
        // a region's bbox edge would not have snapped there anyway.
        const BBOX_MARGIN_DEG: f64 = 0.01;
        // #142: tile margin = 0 (exact tile membership). Initial
        // attempt at margin=1 (3x3 ring, ~21×33 km) was too
        // permissive: BE's border tiles satisfied the ring around a
        // pure-LU query coord, causing BE to load unnecessarily.
        // margin=0 means: only load this region if the query coord
        // lands in a tile that the region's road network actually
        // touches. The bbox margin (0.01° ≈ 1.1 km) handles the
        // very-near-border cases. Queries 1-5 km outside a region's
        // tile coverage but within snap radius (5 km) are a known
        // false-negative — rare, and snap_index falls back to
        // returning None which is the correct behaviour for that
        // case anyway.
        const TILE_MARGIN: i32 = 0;

        let mut best: Option<(usize, f64)> = None;
        for (idx, region) in self.regions.iter().enumerate() {
            // Bbox pre-filter (cheap, 4 cmps). Catches "completely
            // outside this region's bounding box".
            if let Some(bbox) = region.bbox
                && !bbox.contains_with_margin(lon, lat, BBOX_MARGIN_DEG)
            {
                continue;
            }
            // #142 tile pre-filter. Tighter than bbox: a query inside
            // BE's bbox but in LU territory (where BE bbox contains
            // LU but BE's road tiles don't cover that LU sub-area)
            // will skip BE even though bbox passes. This is the
            // difference between "load BE for every LU query because
            // LU sits inside BE's bbox" and "only load BE when the
            // query is in BE's road tile set".
            if let Some(rt) = &region.tiles
                && !rt.contains_with_margin(lon, lat, TILE_MARGIN)
            {
                continue;
            }
            let state = region.state();
            let mode_idx = match state.mode_lookup.get(mode_name) {
                Some(&m) => m,
                None => continue,
            };
            if let Some((_ebg_id, _slon, _slat, dist_m)) =
                state.snap_index.snap_with_info(lon, lat, mode_idx)
            {
                let candidate = (idx, dist_m);
                best = match best {
                    Some((_, prev_dist)) if prev_dist <= dist_m => best,
                    _ => Some(candidate),
                };
            }
        }
        best
    }

    /// Fast bbox-tier affinity check for `(lon, lat)` against a
    /// previously-picked region (#343). Used by bulk preflights where
    /// most queries land in the same region as `query[0]` and full
    /// `snap_winner` per point is wasted work.
    ///
    /// - [`RegionAffinity::In`] — bbox + tile both clear AND no other
    ///   region's bbox covers the point. Caller accepts without
    ///   running a full snap.
    /// - [`RegionAffinity::OutOfBbox`] — bbox or tile rejects the
    ///   point. The point cannot belong to this region; the caller
    ///   surfaces a cross-region 501.
    /// - [`RegionAffinity::Ambiguous`] — the bbox of at least one
    ///   other region also covers this point. The caller MUST fall
    ///   back to full `snap_winner` to disambiguate (covers the
    ///   BE/LU border-overlap case).
    pub fn confirm_in_region(&self, region_idx: usize, lon: f64, lat: f64) -> RegionAffinity {
        const BBOX_MARGIN_DEG: f64 = 0.01;
        const TILE_MARGIN: i32 = 0;
        let region = &self.regions[region_idx];
        // bbox check on the picked region
        if let Some(bbox) = region.bbox
            && !bbox.contains_with_margin(lon, lat, BBOX_MARGIN_DEG)
        {
            return RegionAffinity::OutOfBbox;
        }
        // #142 tile pre-filter (tighter than bbox)
        if let Some(rt) = &region.tiles
            && !rt.contains_with_margin(lon, lat, TILE_MARGIN)
        {
            return RegionAffinity::OutOfBbox;
        }
        // Ambiguity check: any OTHER region's bbox covers this point?
        // If yes, the point sits in the BE/LU-style border overlap
        // zone — the bbox alone cannot say which region owns it. Caller
        // must run a full snap to disambiguate.
        for (other_idx, other) in self.regions.iter().enumerate() {
            if other_idx == region_idx {
                continue;
            }
            if let Some(other_bbox) = other.bbox
                && other_bbox.contains_with_margin(lon, lat, BBOX_MARGIN_DEG)
            {
                return RegionAffinity::Ambiguous;
            }
        }
        RegionAffinity::In
    }

    /// Pick the region for a single-coordinate request (e.g. `/nearest`,
    /// `/isochrone`, `/height`). Returns the per-region `Arc<ServerState>`
    /// or a [`DispatchError::NoRegion`] payload (renders as **400**
    /// caller-side via [`DispatchError::into_response_parts`]).
    pub fn dispatch_single(
        &self,
        lon: f64,
        lat: f64,
        mode_name: &str,
    ) -> Result<Arc<ServerState>, DispatchError> {
        self.dispatch_single_id(lon, lat, mode_name).map(|(s, _)| s)
    }

    /// Same as [`Self::dispatch_single`] but also returns the winning
    /// region id (so the handler can label its per-region metric
    /// without a second lookup).
    pub fn dispatch_single_id(
        &self,
        lon: f64,
        lat: f64,
        mode_name: &str,
    ) -> Result<(Arc<ServerState>, String), DispatchError> {
        if !self.has_mode(mode_name) {
            return Err(DispatchError::InvalidMode {
                mode: mode_name.to_string(),
                available: self.available_modes(),
            });
        }
        match self.snap_winner(lon, lat, mode_name) {
            Some((idx, _dist)) => Ok((self.regions[idx].state(), self.regions[idx].id.clone())),
            None => Err(DispatchError::NoRegion {
                endpoint: Endpoint::Single,
                lon,
                lat,
                mode: mode_name.to_string(),
                tried: self.region_ids().into_iter().collect(),
            }),
        }
    }

    /// Pick the region for a two-coordinate request (e.g. `/route`,
    /// `/table` with one source + targets, `/match`). Both points must
    /// snap to the same region; otherwise return
    /// [`DispatchError::CrossRegion`] which the caller renders as 501.
    pub fn dispatch_p2p(
        &self,
        origin_lon: f64,
        origin_lat: f64,
        destination_lon: f64,
        destination_lat: f64,
        mode_name: &str,
    ) -> Result<Arc<ServerState>, DispatchError> {
        self.dispatch_p2p_id(
            origin_lon,
            origin_lat,
            destination_lon,
            destination_lat,
            mode_name,
        )
        .map(|(s, _)| s)
    }

    /// #343: same as [`Self::dispatch_p2p_id`] but also returns the
    /// winning region's index. Used by bulk preflights that follow up
    /// with [`Self::confirm_in_region`] on queries[1..] against the
    /// returned index.
    pub fn dispatch_p2p_with_idx(
        &self,
        origin_lon: f64,
        origin_lat: f64,
        destination_lon: f64,
        destination_lat: f64,
        mode_name: &str,
    ) -> Result<(Arc<ServerState>, String, usize), DispatchError> {
        if !self.has_mode(mode_name) {
            return Err(DispatchError::InvalidMode {
                mode: mode_name.to_string(),
                available: self.available_modes(),
            });
        }
        let src = self.snap_winner(origin_lon, origin_lat, mode_name);
        let dst = self.snap_winner(destination_lon, destination_lat, mode_name);
        match (src, dst) {
            (Some((s_idx, _)), Some((d_idx, _))) if s_idx == d_idx => Ok((
                self.regions[s_idx].state(),
                self.regions[s_idx].id.clone(),
                s_idx,
            )),
            (Some((s_idx, _)), Some((d_idx, _))) => {
                let src_region = self.regions[s_idx].id.clone();
                let dst_region = self.regions[d_idx].id.clone();
                super::region_metrics::record_cross_region_reject(&src_region, &dst_region);
                Err(DispatchError::CrossRegion {
                    src_region,
                    dst_region,
                })
            }
            (None, _) => Err(DispatchError::NoRegion {
                endpoint: Endpoint::Source,
                lon: origin_lon,
                lat: origin_lat,
                mode: mode_name.to_string(),
                tried: self.region_ids().into_iter().collect(),
            }),
            (_, None) => Err(DispatchError::NoRegion {
                endpoint: Endpoint::Destination,
                lon: destination_lon,
                lat: destination_lat,
                mode: mode_name.to_string(),
                tried: self.region_ids().into_iter().collect(),
            }),
        }
    }

    /// Same as [`Self::dispatch_p2p`] but also returns the winning
    /// region id. Increments the cross-region rejection counter on
    /// `Err(CrossRegion)` so operators can monitor 501 traffic
    /// without parsing log lines.
    pub fn dispatch_p2p_id(
        &self,
        origin_lon: f64,
        origin_lat: f64,
        destination_lon: f64,
        destination_lat: f64,
        mode_name: &str,
    ) -> Result<(Arc<ServerState>, String), DispatchError> {
        if !self.has_mode(mode_name) {
            return Err(DispatchError::InvalidMode {
                mode: mode_name.to_string(),
                available: self.available_modes(),
            });
        }
        let src = self.snap_winner(origin_lon, origin_lat, mode_name);
        let dst = self.snap_winner(destination_lon, destination_lat, mode_name);
        match (src, dst) {
            (Some((s_idx, _)), Some((d_idx, _))) if s_idx == d_idx => {
                Ok((self.regions[s_idx].state(), self.regions[s_idx].id.clone()))
            }
            (Some((s_idx, _)), Some((d_idx, _))) => {
                let src_region = self.regions[s_idx].id.clone();
                let dst_region = self.regions[d_idx].id.clone();
                super::region_metrics::record_cross_region_reject(&src_region, &dst_region);
                Err(DispatchError::CrossRegion {
                    src_region,
                    dst_region,
                })
            }
            (None, _) => Err(DispatchError::NoRegion {
                endpoint: Endpoint::Source,
                lon: origin_lon,
                lat: origin_lat,
                mode: mode_name.to_string(),
                tried: self.region_ids().into_iter().collect(),
            }),
            (_, None) => Err(DispatchError::NoRegion {
                endpoint: Endpoint::Destination,
                lon: destination_lon,
                lat: destination_lat,
                mode: mode_name.to_string(),
                tried: self.region_ids().into_iter().collect(),
            }),
        }
    }

    /// Pick the region for a many-coordinate request (e.g. `/match`
    /// trace, `/trip`, `/table` with multiple sources + multiple
    /// targets). All points must snap to the same region; otherwise
    /// 501. Returns the per-region state plus the winning region id.
    ///
    /// On `CrossRegion` rejection, the
    /// `butterfly_route_query_cross_region_total` counter is
    /// incremented exactly once via
    /// [`super::region_metrics::record_cross_region_reject`] —
    /// callers don't need to bump it separately.
    pub fn dispatch_many<I>(
        &self,
        coords: I,
        mode_name: &str,
    ) -> Result<(Arc<ServerState>, String), DispatchError>
    where
        I: IntoIterator<Item = (f64, f64)>,
    {
        if !self.has_mode(mode_name) {
            return Err(DispatchError::InvalidMode {
                mode: mode_name.to_string(),
                available: self.available_modes(),
            });
        }
        // Single-region fast path: skip the per-coord snap_winner sweep.
        // The matrix handler runs its own K-best snap downstream, which
        // is what actually validates whether each coord lies in a region.
        // A coord that doesn't snap there will get an INF cell, which is
        // the correct behaviour for a single-region container. Without
        // this short-circuit, dispatch_many ran 200+ serial single-best
        // snaps per /table call (~200 ms wall on Belgium at N=100) —
        // dominating the request latency.
        if self.regions.len() == 1 {
            let only = &self.regions[0];
            // We still need to consume the first coord to check the
            // iterator isn't empty (matches the original Empty error).
            let mut iter = coords.into_iter();
            iter.next().ok_or(DispatchError::Empty)?;
            return Ok((only.state(), only.id.clone()));
        }
        let mut iter = coords.into_iter();
        let first = iter.next().ok_or(DispatchError::Empty)?;
        let first_winner = self
            .snap_winner(first.0, first.1, mode_name)
            .ok_or_else(|| DispatchError::NoRegion {
                endpoint: Endpoint::ManyAt(0),
                lon: first.0,
                lat: first.1,
                mode: mode_name.to_string(),
                tried: self.region_ids().into_iter().collect(),
            })?;
        let s_idx = first_winner.0;
        for (i, (lon, lat)) in iter.enumerate() {
            // i counts from 0 over the *remaining* iterator, so the
            // index in the original sequence is i + 1.
            let next =
                self.snap_winner(lon, lat, mode_name)
                    .ok_or_else(|| DispatchError::NoRegion {
                        endpoint: Endpoint::ManyAt(i + 1),
                        lon,
                        lat,
                        mode: mode_name.to_string(),
                        tried: self.region_ids().into_iter().collect(),
                    })?;
            if next.0 != s_idx {
                let src_region = self.regions[s_idx].id.clone();
                let dst_region = self.regions[next.0].id.clone();
                super::region_metrics::record_cross_region_reject(&src_region, &dst_region);
                return Err(DispatchError::CrossRegion {
                    src_region,
                    dst_region,
                });
            }
        }
        Ok((self.regions[s_idx].state(), self.regions[s_idx].id.clone()))
    }

    /// Sorted list of all loaded region ids.
    pub fn region_ids(&self) -> Vec<String> {
        self.regions.iter().map(|r| r.id.clone()).collect()
    }

    /// #292 Phase 6: count of currently-loaded regions (excludes
    /// Pending entries). Used by the LRU eviction poller's
    /// "anything to do" check before it walks the regions array.
    pub fn loaded_count(&self) -> usize {
        self.regions.iter().filter(|r| r.is_loaded()).count()
    }

    /// #292 Phase 6: evict loaded regions in LRU order until either
    /// `target` returns `false` (we're back under budget) or every
    /// region except `keep_min` regions has been evicted.
    ///
    /// `target_satisfied` is a closure that returns `true` when the
    /// caller is satisfied (i.e. under budget). It's called once
    /// before any eviction (early exit if already under budget) and
    /// after each eviction. Wiring through a closure rather than a
    /// concrete RSS check keeps this module decoupled from the
    /// /proc/self/status reader living in [`super::rss`].
    ///
    /// `keep_min`: always leave at least this many regions loaded.
    /// Set to 0 to allow draining everything; a serve-loop poller
    /// will typically set it to 1 so the most-recently-used region
    /// stays warm.
    ///
    /// Returns the number of regions evicted in this call.
    pub fn evict_lru_until<F: FnMut() -> bool>(
        &self,
        mut target_satisfied: F,
        keep_min: usize,
    ) -> usize {
        if target_satisfied() {
            return 0;
        }
        // Collect (last_used, index) for currently Loaded entries;
        // sort ascending so the LRU entries come first.
        let mut candidates: Vec<(u64, usize)> = self
            .regions
            .iter()
            .enumerate()
            .filter_map(|(i, r)| {
                if r.is_loaded() {
                    Some((r.last_used(), i))
                } else {
                    None
                }
            })
            .collect();
        candidates.sort_by_key(|&(ts, _)| ts);

        let loaded_total = candidates.len();
        let evictable = loaded_total.saturating_sub(keep_min);
        if evictable == 0 {
            tracing::debug!(
                loaded = loaded_total,
                keep_min,
                "evict_lru_until: nothing evictable (keep_min reached)"
            );
            return 0;
        }

        let mut evicted = 0usize;
        for &(_ts, idx) in candidates.iter().take(evictable) {
            if self.regions[idx].try_evict() {
                evicted += 1;
            }
            if target_satisfied() {
                break;
            }
        }
        tracing::info!(
            evicted,
            loaded_before = loaded_total,
            loaded_after = self.loaded_count(),
            "evict_lru_until complete"
        );
        evicted
    }

    /// Cross-region-aware P2P dispatch (#91 Phase 2).
    ///
    /// Like [`Self::dispatch_p2p_id`] but, when an overlay is wired up
    /// and the source/target snap to *different* regions, returns a
    /// [`P2pPlan::CrossRegion`] handle instead of an error. The
    /// [`super::cross_region::solve_cross_region`] coordinator consumes
    /// this handle.
    ///
    /// If no overlay is wired, behaviour is identical to `dispatch_p2p_id`
    /// (cross-region → 501 via [`DispatchError::CrossRegion`]). This
    /// keeps existing handlers that haven't been migrated correct.
    pub fn dispatch_p2p_with_overlay(
        &self,
        origin_lon: f64,
        origin_lat: f64,
        destination_lon: f64,
        destination_lat: f64,
        mode_name: &str,
    ) -> Result<P2pPlan, DispatchError> {
        if !self.has_mode(mode_name) {
            return Err(DispatchError::InvalidMode {
                mode: mode_name.to_string(),
                available: self.available_modes(),
            });
        }
        let src = self.snap_winner(origin_lon, origin_lat, mode_name);
        let dst = self.snap_winner(destination_lon, destination_lat, mode_name);
        match (src, dst) {
            (Some((s_idx, _)), Some((d_idx, _))) if s_idx == d_idx => Ok(P2pPlan::SameRegion {
                state: self.regions[s_idx].state(),
                region: self.regions[s_idx].id.clone(),
            }),
            (Some((s_idx, _)), Some((d_idx, _))) => {
                let src_region = self.regions[s_idx].id.clone();
                let dst_region = self.regions[d_idx].id.clone();
                match &self.overlay {
                    Some(o) => Ok(P2pPlan::CrossRegion {
                        src_state: self.regions[s_idx].state(),
                        src_region,
                        dst_state: self.regions[d_idx].state(),
                        dst_region,
                        overlay: Arc::clone(o),
                    }),
                    None => {
                        super::region_metrics::record_cross_region_reject(&src_region, &dst_region);
                        Err(DispatchError::CrossRegion {
                            src_region,
                            dst_region,
                        })
                    }
                }
            }
            (None, _) => Err(DispatchError::NoRegion {
                endpoint: Endpoint::Source,
                lon: origin_lon,
                lat: origin_lat,
                mode: mode_name.to_string(),
                tried: self.region_ids().into_iter().collect(),
            }),
            (_, None) => Err(DispatchError::NoRegion {
                endpoint: Endpoint::Destination,
                lon: destination_lon,
                lat: destination_lat,
                mode: mode_name.to_string(),
                tried: self.region_ids().into_iter().collect(),
            }),
        }
    }
}

/// Outcome of [`RegionsState::dispatch_p2p_with_overlay`].
///
/// `SameRegion` matches the existing [`RegionsState::dispatch_p2p_id`]
/// behaviour: handlers run their existing intra-region path on `state`.
///
/// `CrossRegion` carries enough state for
/// [`super::cross_region::solve_cross_region`] to compute access leg in
/// `src_state`, look up the prebuilt overlay matrix, and run egress in
/// `dst_state`.
pub enum P2pPlan {
    SameRegion {
        state: Arc<ServerState>,
        region: String,
    },
    CrossRegion {
        src_state: Arc<ServerState>,
        src_region: String,
        dst_state: Arc<ServerState>,
        dst_region: String,
        overlay: Arc<super::overlay::OverlayCluster>,
    },
}

/// Result of [`RegionsState::confirm_in_region`] — the fast bbox-tier
/// pre-check that bulk preflights run instead of a full per-point snap
/// (#343).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegionAffinity {
    /// Bbox + tile both confirm this point belongs to the picked
    /// region, and no other region's bbox overlaps it. Fast-path
    /// accept — caller skips the full snap.
    In,
    /// Bbox or tile rejects the point. Definitely not in this region.
    /// Caller surfaces a cross-region 501.
    OutOfBbox,
    /// At least one other region's bbox also covers this point — the
    /// bbox alone cannot decide ownership (e.g. the BE/LU border
    /// strip). Caller MUST fall back to full `snap_winner` to
    /// disambiguate.
    Ambiguous,
}

/// Which side of a P2P request the failing coordinate is on. Carried
/// by [`DispatchError::NoRegion`] so the error message points at the
/// right input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Endpoint {
    /// Source / origin coordinate (e.g. `origin_lon`, `origin_lat`).
    Source,
    /// Destination / target coordinate (e.g. `destination_lon`, `destination_lat`).
    Destination,
    /// Single-coordinate request (e.g. `/nearest`, `/isochrone`,
    /// `/height`). The endpoint distinction does not apply.
    Single,
    /// One element of a many-coordinate request (`/match`, `/trip`,
    /// `/table`). Carries the 0-based index so the error points at
    /// the right input.
    ManyAt(usize),
}

impl Endpoint {
    pub fn label(&self) -> String {
        match self {
            Endpoint::Source => "source".to_string(),
            Endpoint::Destination => "destination".to_string(),
            Endpoint::Single => "point".to_string(),
            Endpoint::ManyAt(i) => format!("coordinate[{}]", i),
        }
    }
}

/// What can go wrong dispatching a request to a region.
#[derive(Debug, Clone)]
pub enum DispatchError {
    /// One of the input points did not snap into any loaded region's
    /// road network for the requested mode. Renders as 400 with a
    /// targeted error message; reuses the existing
    /// "No road found within snap distance" semantics.
    NoRegion {
        endpoint: Endpoint,
        lon: f64,
        lat: f64,
        mode: String,
        tried: Vec<String>,
    },
    /// Mode is not loaded in any region. Renders as 400 with the union
    /// of available modes so the caller can correct the typo without
    /// guessing which region they were aiming at.
    InvalidMode {
        mode: String,
        available: Vec<String>,
    },
    /// The points snapped into *different* regions — same-region
    /// dispatch can't service this. Renders as 501 with a clear
    /// "spans regions X → Y" error per the #91 spec.
    CrossRegion {
        src_region: String,
        dst_region: String,
    },
    /// `dispatch_many` was called with no coordinates. Caller bug.
    Empty,
}

impl DispatchError {
    /// Convert the dispatch error to a (status, JSON) pair the handler
    /// can return. Centralises the wording so every endpoint says the
    /// same thing on 501.
    pub fn into_response_parts(self) -> (axum::http::StatusCode, ErrorResponse) {
        use axum::http::StatusCode;
        match self {
            DispatchError::NoRegion {
                endpoint,
                lon,
                lat,
                mode,
                ..
            } => (
                StatusCode::BAD_REQUEST,
                ErrorResponse {
                    error: format!(
                        "No road found within snap distance for {} ({}, {}) mode={}",
                        endpoint.label(),
                        lon,
                        lat,
                        mode
                    ),
                },
            ),
            DispatchError::InvalidMode { mode, available } => (
                StatusCode::BAD_REQUEST,
                ErrorResponse {
                    error: format!(
                        "Invalid mode '{}'. Available across loaded regions: {}.",
                        mode,
                        available.join(", ")
                    ),
                },
            ),
            DispatchError::CrossRegion {
                src_region,
                dst_region,
            } => (
                StatusCode::NOT_IMPLEMENTED,
                ErrorResponse {
                    error: format!(
                        "route spans regions {} \u{2192} {}; cross-region overlay not yet implemented (#91 Phase 2)",
                        src_region, dst_region
                    ),
                },
            ),
            DispatchError::Empty => (
                StatusCode::BAD_REQUEST,
                ErrorResponse {
                    error: "no coordinates supplied to dispatcher".to_string(),
                },
            ),
        }
    }
}

/// Peek the region id, `shared/snap_points` bbox, and mode-name list
/// from a container without loading the body. Bbox is `None` for legacy
/// containers without the packed snap index; `mode_names` is empty for
/// legacy step-tree containers that pre-date the `mode/<m>/...` schema.
/// Used by the lazy-region boot path so `snap_winner` / `has_mode` /
/// `available_modes` can answer Pending-region questions without
/// triggering a full load.
/// `(region_id, optional snap bbox, mode names, optional region tiles)`
/// — the tuple shape [`peek_region_meta`] returns. Promoted to a type
/// alias so the signature stays readable and the clippy
/// `clippy::type_complexity` lint stops firing.
type RegionMetaPeek = (
    String,
    Option<crate::formats::SnapBbox>,
    Vec<String>,
    Option<crate::formats::RegionTiles>,
);

fn peek_region_meta(path: &Path) -> Result<RegionMetaPeek> {
    use crate::formats::butterfly_dat::Container;
    let container =
        Container::open(path).with_context(|| format!("opening container {}", path.display()))?;
    let region_id = container.read_region_id(path)?;
    let bbox = match container.get("shared/snap_points") {
        Some(entry) => match crate::formats::peek_snap_points_bbox(path, entry.offset) {
            Ok(b) => Some(b),
            Err(e) => {
                tracing::warn!(
                    container = %path.display(),
                    error = %e,
                    "could not peek snap_points bbox; lazy snap_winner falls back to full load",
                );
                None
            }
        },
        None => None,
    };
    let mut mode_names = container.list_modes();
    // #392: also surface traffic-variant synthetic modes
    // (`<base>_<variant>`) so the dispatcher's mode-validation accepts
    // `?mode=car_realistic` on both eagerly- and lazily-loaded regions.
    for (base, variant) in container.list_traffic_variants() {
        let synthetic = format!("{}_{}", base, variant);
        if !mode_names.contains(&synthetic) {
            mode_names.push(synthetic);
        }
    }
    mode_names.sort();
    // #142: peek region_tiles if present. Loaded into heap as an
    // owned Vec — the array is small (~5 KiB Belgium, ~10 MiB planet)
    // and we want it always-resident so snap_winner's tile check is
    // a pure binary search.
    let tiles = match container.get("shared/region_tiles") {
        Some(entry) => match container.read_section_verified(path, entry) {
            Ok(bytes) => match crate::formats::RegionTilesFile::read_from_bytes(&bytes) {
                Ok(rt) => Some(rt),
                Err(e) => {
                    tracing::warn!(
                        container = %path.display(),
                        error = %e,
                        "region_tiles section unreadable; falling back to bbox-only filter",
                    );
                    None
                }
            },
            Err(e) => {
                tracing::warn!(
                    container = %path.display(),
                    error = %e,
                    "region_tiles section CRC failed; falling back to bbox-only filter",
                );
                None
            }
        },
        None => None,
    };
    Ok((region_id, bbox, mode_names, tiles))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verify_status_label_is_stable() {
        // /regions JSON consumers depend on this exact string.
        assert_eq!(VerifyStatus::Verified.label(), "verified");
    }

    #[test]
    fn dispatch_error_cross_region_is_501_with_helpful_text() {
        let err = DispatchError::CrossRegion {
            src_region: "BE".into(),
            dst_region: "LU".into(),
        };
        let (code, body) = err.into_response_parts();
        assert_eq!(code, axum::http::StatusCode::NOT_IMPLEMENTED);
        assert!(body.error.contains("BE"), "{}", body.error);
        assert!(body.error.contains("LU"), "{}", body.error);
        assert!(
            body.error.contains("#91"),
            "expected error to reference issue #91, got: {}",
            body.error
        );
    }

    #[test]
    fn dispatch_error_no_region_is_400() {
        let err = DispatchError::NoRegion {
            endpoint: Endpoint::Single,
            lon: 0.0,
            lat: 0.0,
            mode: "car".into(),
            tried: vec!["BE".into()],
        };
        let (code, _) = err.into_response_parts();
        assert_eq!(code, axum::http::StatusCode::BAD_REQUEST);
    }

    #[test]
    fn dispatch_error_no_region_distinguishes_source_vs_destination() {
        let src_err = DispatchError::NoRegion {
            endpoint: Endpoint::Source,
            lon: 1.0,
            lat: 2.0,
            mode: "car".into(),
            tried: vec!["BE".into()],
        };
        let (_, body_src) = src_err.into_response_parts();
        assert!(body_src.error.contains("source"), "{}", body_src.error);

        let dst_err = DispatchError::NoRegion {
            endpoint: Endpoint::Destination,
            lon: 3.0,
            lat: 4.0,
            mode: "car".into(),
            tried: vec!["BE".into()],
        };
        let (_, body_dst) = dst_err.into_response_parts();
        assert!(body_dst.error.contains("destination"), "{}", body_dst.error);
    }

    #[test]
    fn dispatch_error_invalid_mode_is_400_and_lists_available() {
        let err = DispatchError::InvalidMode {
            mode: "ferry".into(),
            available: vec!["bike".into(), "car".into(), "foot".into()],
        };
        let (code, body) = err.into_response_parts();
        assert_eq!(code, axum::http::StatusCode::BAD_REQUEST);
        assert!(body.error.contains("Invalid mode"), "{}", body.error);
        assert!(body.error.contains("car"), "{}", body.error);
    }

    #[test]
    fn dispatch_error_no_region_carries_endpoint_label() {
        // Many-coordinate failure points at the index of the bad coord.
        let err = DispatchError::NoRegion {
            endpoint: Endpoint::ManyAt(7),
            lon: 0.0,
            lat: 0.0,
            mode: "car".into(),
            tried: vec!["BE".into()],
        };
        let (_, body) = err.into_response_parts();
        assert!(body.error.contains("coordinate[7]"), "{}", body.error);
    }
}
