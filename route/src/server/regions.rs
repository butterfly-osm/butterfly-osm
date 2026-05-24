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
    pub state: Arc<ServerState>,
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

impl RegionEntry {
    /// Forward-compatible accessor for the per-region `ServerState`.
    ///
    /// Today the boot path eagerly loads every region, so this is
    /// always a cheap `Arc::clone`.
    ///
    /// **#292 Phase 2** will introduce lazy region loading — at that
    /// point this method's body changes to drive an on-demand load
    /// if the region was previously evicted or never loaded, and the
    /// return type becomes `Result<Arc<ServerState>>`. Consumers that
    /// use `region.state()` today get a no-op alias; they will need
    /// a `?` after the call when #292 lands. Direct `&region.state`
    /// field access keeps working too (back-compat), but new code
    /// should prefer this method so the lazy migration is local.
    #[inline]
    pub fn state(&self) -> Arc<ServerState> {
        Arc::clone(&self.state)
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
}

impl RegionsState {
    /// Wrap a single already-loaded `ServerState` as a one-region
    /// [`RegionsState`]. Used by the legacy single-container
    /// `serve --data <file>` and `serve --data-dir <step-tree>` paths,
    /// so handlers that take an `Arc<RegionsState>` work uniformly.
    pub fn from_single(id: impl Into<String>, container: PathBuf, state: ServerState) -> Self {
        let id = id.into();
        let metrics = super::region_metrics::RegionMetrics::new(&id);
        let entry = RegionEntry {
            id: id.clone(),
            container,
            state: Arc::new(state),
            verify_status: VerifyStatus::Verified,
            metrics,
        };
        let mut by_id = HashMap::new();
        by_id.insert(id, 0);
        Self {
            regions: vec![entry],
            by_id,
            overlay: None,
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
            let region_id = peek_region_id(path)
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
            entries.push(RegionEntry {
                id: region_id,
                container: path.clone(),
                state: Arc::new(state),
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
        })
    }

    /// Discover and load every `*.butterfly` container in `dir`. If
    /// `region_filter` is `Some`, only regions whose id is in the list
    /// are loaded.
    ///
    /// At least one region must load; an empty directory or a filter
    /// that excludes every container is a hard error so an operator
    /// does not accidentally start a server with zero data.
    pub fn load_from_dir(
        dir: &Path,
        region_filter: Option<&[String]>,
        mode_filter: Option<&[String]>,
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
        // → region id. We do this first so that the region filter is
        // applied before the (much heavier) full state load.
        let mut to_load: Vec<(String, PathBuf)> = Vec::new();
        let mut skipped: Vec<String> = Vec::new();
        for path in &containers {
            let region = peek_region_id(path)
                .with_context(|| format!("reading region id from {}", path.display()))?;
            if let Some(filter) = region_filter
                && !filter.iter().any(|r| r.eq_ignore_ascii_case(&region))
            {
                skipped.push(format!("{} (region={})", path.display(), region));
                continue;
            }
            to_load.push((region, path.clone()));
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
        for (id, path) in &to_load {
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
        for (id, path) in to_load {
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
            let idx = regions.len();
            by_id.insert(id.clone(), idx);
            let metrics = super::region_metrics::RegionMetrics::new(&id);
            regions.push(RegionEntry {
                id,
                container: path,
                state: Arc::new(state),
                verify_status: VerifyStatus::Verified,
                metrics,
            });
        }

        Ok(Self {
            regions,
            by_id,
            overlay: None,
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
        let lower = mode_name.to_lowercase();
        self.regions
            .iter()
            .any(|r| r.state.mode_lookup.contains_key(&lower))
    }

    /// Sorted union of every mode name across loaded regions. Used by
    /// the "Invalid mode" error to tell the caller what they could have
    /// asked for.
    pub fn available_modes(&self) -> Vec<String> {
        let mut set: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        for r in &self.regions {
            for name in r.state.mode_lookup.keys() {
                set.insert(name.clone());
            }
        }
        set.into_iter().collect()
    }

    /// Borrow the first region's state. Used as a fallback by metadata
    /// endpoints (`/health`, `/metrics`) and by tests that don't care
    /// which region answers. Single-region deployments behave exactly
    /// like before this PR.
    pub fn primary(&self) -> &Arc<ServerState> {
        &self.regions[0].state
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
        let mut best: Option<(usize, f64)> = None;
        for (idx, region) in self.regions.iter().enumerate() {
            let mode_idx = match region.state.mode_lookup.get(mode_name) {
                Some(&m) => m,
                None => continue,
            };
            if let Some((_ebg_id, _slon, _slat, dist_m)) =
                region.state.snap_index.snap_with_info(lon, lat, mode_idx)
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
            Some((idx, _dist)) => Ok((
                Arc::clone(&self.regions[idx].state),
                self.regions[idx].id.clone(),
            )),
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
        src_lon: f64,
        src_lat: f64,
        dst_lon: f64,
        dst_lat: f64,
        mode_name: &str,
    ) -> Result<Arc<ServerState>, DispatchError> {
        self.dispatch_p2p_id(src_lon, src_lat, dst_lon, dst_lat, mode_name)
            .map(|(s, _)| s)
    }

    /// Same as [`Self::dispatch_p2p`] but also returns the winning
    /// region id. Increments the cross-region rejection counter on
    /// `Err(CrossRegion)` so operators can monitor 501 traffic
    /// without parsing log lines.
    pub fn dispatch_p2p_id(
        &self,
        src_lon: f64,
        src_lat: f64,
        dst_lon: f64,
        dst_lat: f64,
        mode_name: &str,
    ) -> Result<(Arc<ServerState>, String), DispatchError> {
        if !self.has_mode(mode_name) {
            return Err(DispatchError::InvalidMode {
                mode: mode_name.to_string(),
                available: self.available_modes(),
            });
        }
        let src = self.snap_winner(src_lon, src_lat, mode_name);
        let dst = self.snap_winner(dst_lon, dst_lat, mode_name);
        match (src, dst) {
            (Some((s_idx, _)), Some((d_idx, _))) if s_idx == d_idx => Ok((
                Arc::clone(&self.regions[s_idx].state),
                self.regions[s_idx].id.clone(),
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
                lon: src_lon,
                lat: src_lat,
                mode: mode_name.to_string(),
                tried: self.region_ids().into_iter().collect(),
            }),
            (_, None) => Err(DispatchError::NoRegion {
                endpoint: Endpoint::Destination,
                lon: dst_lon,
                lat: dst_lat,
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
            return Ok((Arc::clone(&only.state), only.id.clone()));
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
        Ok((
            Arc::clone(&self.regions[s_idx].state),
            self.regions[s_idx].id.clone(),
        ))
    }

    /// Sorted list of all loaded region ids.
    pub fn region_ids(&self) -> Vec<String> {
        self.regions.iter().map(|r| r.id.clone()).collect()
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
        src_lon: f64,
        src_lat: f64,
        dst_lon: f64,
        dst_lat: f64,
        mode_name: &str,
    ) -> Result<P2pPlan, DispatchError> {
        if !self.has_mode(mode_name) {
            return Err(DispatchError::InvalidMode {
                mode: mode_name.to_string(),
                available: self.available_modes(),
            });
        }
        let src = self.snap_winner(src_lon, src_lat, mode_name);
        let dst = self.snap_winner(dst_lon, dst_lat, mode_name);
        match (src, dst) {
            (Some((s_idx, _)), Some((d_idx, _))) if s_idx == d_idx => Ok(P2pPlan::SameRegion {
                state: Arc::clone(&self.regions[s_idx].state),
                region: self.regions[s_idx].id.clone(),
            }),
            (Some((s_idx, _)), Some((d_idx, _))) => {
                let src_region = self.regions[s_idx].id.clone();
                let dst_region = self.regions[d_idx].id.clone();
                match &self.overlay {
                    Some(o) => Ok(P2pPlan::CrossRegion {
                        src_state: Arc::clone(&self.regions[s_idx].state),
                        src_region,
                        dst_state: Arc::clone(&self.regions[d_idx].state),
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
                lon: src_lon,
                lat: src_lat,
                mode: mode_name.to_string(),
                tried: self.region_ids().into_iter().collect(),
            }),
            (_, None) => Err(DispatchError::NoRegion {
                endpoint: Endpoint::Destination,
                lon: dst_lon,
                lat: dst_lat,
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

/// Which side of a P2P request the failing coordinate is on. Carried
/// by [`DispatchError::NoRegion`] so the error message points at the
/// right input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Endpoint {
    /// Source / origin coordinate (e.g. `src_lon`, `src_lat`).
    Source,
    /// Destination / target coordinate (e.g. `dst_lon`, `dst_lat`).
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
    fn label(&self) -> String {
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

/// Read just the region id from a container without loading the rest.
/// Used by the discovery pre-pass so the region filter is applied
/// before the heavy state load.
fn peek_region_id(path: &Path) -> Result<String> {
    use crate::formats::butterfly_dat::Container;
    let container =
        Container::open(path).with_context(|| format!("opening container {}", path.display()))?;
    container.read_region_id(path)
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
