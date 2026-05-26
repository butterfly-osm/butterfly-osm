//! Step 9: Query Engine
//!
//! Dual-transport server: Axum REST/JSON + Arrow Flight gRPC, sharing `Arc<ServerState>`.
//!
//! # REST Endpoints (Axum, `--port`)
//!
//! - `GET /route` - Point-to-point routing with geometry, steps, alternatives
//! - `GET /nearest` - Snap to nearest road segments
//! - `POST /table` - Distance matrix (bucket M2M)
//! - `GET /isochrone` - Reachability polygon (GeoJSON/WKB)
//! - `POST /isochrone/bulk` - Parallel batch isochrones (WKB stream)
//! - `POST /trip` - TSP/trip optimization
//! - `POST /match` - GPS trace map matching (HMM + Viterbi)
//! - `GET /height` - Elevation lookup (SRTM DEM)
//! - `GET /health` - Health check with uptime and stats
//! - `GET /metrics` - Prometheus metrics
//! - `GET /swagger-ui/` - OpenAPI documentation
//!
//! # Arrow Flight gRPC Endpoints (`--grpc-port`)
//!
//! All via `DoGet` with ticket format `action:profile:params_json`:
//! - `matrix` - Distance/duration matrix (Bucket M2M or PHAST tiling)
//! - `route_batch` - Batch P2P routing with WKB geometry
//! - `isochrone` - Reachability polygons as WKB per interval
//!
//! # Architecture
//!
//! All queries use the same CCH hierarchy with edge-based state:
//! - Bidirectional Dijkstra on up/down graphs
//! - Shortcut unpacking for path reconstruction
//! - Geometry lookup via EBG -> NBG mapping

pub mod api;
pub mod avoid;
pub mod border;
pub mod catchment;
pub mod cross_region;
pub mod edge_geom;
pub mod elevation;
pub mod exclude;
pub mod overlay;
// tonic::Status is 176 bytes — the canonical gRPC error type.
// Every gRPC function returns Result<_, Status>; boxing adds indirection with no benefit.
#[allow(clippy::result_large_err)]
pub mod flight;
pub mod geometry;
pub mod health_handler;
pub mod height_handler;
pub mod isochrone_handler;
pub mod map_match;
pub mod matching;
pub mod metrics;
pub mod nearest;
pub mod query;
pub mod region_metrics;
pub mod regions;
pub mod regions_handler;
pub mod route;
pub mod rss;
pub mod snap_index;
pub mod snap_kbest;
pub mod spatial;
pub mod state;
pub mod table;
pub mod transit_handler;
pub mod trip;
pub mod types;
pub mod unpack;

#[cfg(test)]
mod api_tests;
#[cfg(test)]
mod consistency_test;
#[cfg(test)]
mod isochrone_test;

use anyhow::{Context, Result};
use std::net::TcpListener;
use std::path::Path;
use std::sync::Arc;

pub use state::ServerState;

/// Initialize structured logging with tracing.
///
/// - `log_format`: "text" for human-readable, "json" for structured JSON lines.
/// - Respects RUST_LOG env var for filtering (default: `info,tower_http=debug`).
pub fn init_tracing(log_format: &str) {
    use tracing_subscriber::{EnvFilter, fmt};

    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,tower_http=debug"));

    match log_format {
        "json" => {
            fmt()
                .json()
                .with_env_filter(filter)
                .with_target(true)
                .init();
        }
        _ => {
            fmt().with_env_filter(filter).with_target(false).init();
        }
    }
}

/// Find a free port starting from the given port
pub fn find_free_port(start: u16) -> Result<u16> {
    for port in start..65535 {
        if TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return Ok(port);
        }
    }
    anyhow::bail!("No free port found starting from {}", start);
}

/// Detect whether `dir` contains at least one `*.butterfly` container
/// file. Used to dispatch between the legacy step-tree loader and the
/// multi-region container loader. Errors propagate explicitly rather
/// than getting swallowed into a stale `false` — operators noticing a
/// permission issue at this site is more useful than silently falling
/// back to the legacy path.
/// #292 Phase 6: process-wide override for the RSS budget, set by the
/// CLI via `--rss-budget-gb`. The OnceLock keeps the value initialise-
/// once and lets the eviction poller read it without needing to grow
/// the `serve()` signature.
static RSS_BUDGET_OVERRIDE_GIB: std::sync::OnceLock<f64> = std::sync::OnceLock::new();

/// Set the process-wide RSS budget override (in GiB). Called once by
/// the CLI before `serve()` runs; later sets are ignored (OnceLock
/// semantics).
///
/// Rejects NaN, infinity, and non-positive values with a warning —
/// these would either propagate NaN through the `clamp()` in
/// [`rss_budget_bytes`] or saturate the `as u64` cast to wild
/// numbers. Invalid input falls through to the env-var / MemTotal
/// default path.
pub fn set_rss_budget_override(gib: f64) {
    if !gib.is_finite() || gib <= 0.0 {
        tracing::warn!(
            value = gib,
            "--rss-budget-gb must be finite and > 0; falling back to env / MemTotal default"
        );
        return;
    }
    let _ = RSS_BUDGET_OVERRIDE_GIB.set(gib);
}

/// #292 Phase 6: read the server's RSS budget in bytes.
///
/// Source order: process-wide override (`--rss-budget-gb` CLI flag),
/// then environment variable `BUTTERFLY_RSS_BUDGET_GB` if set and
/// parseable, then 80% of the system's MemTotal (read from
/// `/proc/meminfo` on Linux). The final number is clamped to at
/// least 1 GiB and at most 1 TiB to catch operator typos.
fn rss_budget_bytes() -> u64 {
    const MIN_GIB: f64 = 1.0;
    const MAX_GIB: f64 = 1024.0;
    let gib = if let Some(&v) = RSS_BUDGET_OVERRIDE_GIB.get() {
        v
    } else if let Ok(s) = std::env::var("BUTTERFLY_RSS_BUDGET_GB") {
        match s.parse::<f64>() {
            Ok(v) if v.is_finite() && v > 0.0 => v,
            _ => {
                tracing::warn!(value = %s, "BUTTERFLY_RSS_BUDGET_GB unparseable; using default");
                default_rss_budget_gib()
            }
        }
    } else {
        default_rss_budget_gib()
    };
    let clamped = gib.clamp(MIN_GIB, MAX_GIB);
    (clamped * (1u64 << 30) as f64) as u64
}

/// Default budget: 80% of `MemTotal` from `/proc/meminfo`. If the
/// file can't be read (non-Linux dev env), fall back to 8 GiB so
/// the eviction logic still has a reasonable threshold to enforce.
fn default_rss_budget_gib() -> f64 {
    const FALLBACK_GIB: f64 = 8.0;
    let path = "/proc/meminfo";
    let s = match std::fs::read_to_string(path) {
        Ok(s) => s,
        Err(_) => return FALLBACK_GIB,
    };
    for line in s.lines() {
        if let Some(rest) = line.strip_prefix("MemTotal:") {
            // e.g. "MemTotal:       65789012 kB"
            let kb: u64 = rest
                .split_whitespace()
                .next()
                .and_then(|n| n.parse().ok())
                .unwrap_or(0);
            if kb > 0 {
                let gib = (kb as f64) / (1024.0 * 1024.0);
                return gib * 0.80;
            }
        }
    }
    FALLBACK_GIB
}

/// Read this process's `VmRSS` in bytes from `/proc/self/status`.
/// Returns `None` if the file can't be read or the line can't be
/// parsed (the poller treats `None` as "skip this tick").
fn read_proc_vm_rss_bytes() -> Option<u64> {
    let path = "/proc/self/status";
    let s = std::fs::read_to_string(path).ok()?;
    for line in s.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let kb: u64 = rest
                .split_whitespace()
                .next()
                .and_then(|n| n.parse().ok())?;
            return Some(kb * 1024);
        }
    }
    None
}

fn directory_has_butterfly_container(dir: &Path) -> Result<bool> {
    let read_dir =
        std::fs::read_dir(dir).with_context(|| format!("reading data dir {}", dir.display()))?;
    for entry in read_dir {
        let entry =
            entry.with_context(|| format!("iterating directory entries in {}", dir.display()))?;
        let path = entry.path();
        let metadata =
            std::fs::metadata(&path).with_context(|| format!("stat {}", path.display()))?;
        if !metadata.is_file() {
            continue;
        }
        let is_butterfly = path
            .extension()
            .and_then(|e| e.to_str())
            .map(|s| s.eq_ignore_ascii_case("butterfly"))
            .unwrap_or(false);
        if is_butterfly {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Shutdown signal: waits for SIGINT (Ctrl-C) or SIGTERM.
async fn shutdown_signal() {
    use tokio::signal;

    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    tracing::info!("shutdown signal received, starting graceful shutdown");
}

/// Transport mode controlling which servers start
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transport {
    /// REST/JSON only (Axum HTTP)
    Rest,
    /// Arrow Flight gRPC only
    Grpc,
    /// Both REST and gRPC (default)
    Both,
}

impl Transport {
    pub fn parse(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "rest" => Ok(Transport::Rest),
            "grpc" => Ok(Transport::Grpc),
            "both" => Ok(Transport::Both),
            other => anyhow::bail!("Invalid transport '{}'. Use: rest, grpc, both", other),
        }
    }
}

/// Where the server's static data lives.
pub enum DataSource<'a> {
    /// Legacy directory layout with `step{N}/` subtrees, OR a
    /// multi-region directory of `*.butterfly` containers (#91 Phase 1).
    /// Detected at boot: if the directory contains at least one
    /// `*.butterfly` file we treat it as a multi-region container
    /// directory; otherwise we fall back to the legacy step-tree
    /// loader for backwards compatibility.
    Directory(&'a Path),
    /// Single `.butterfly` container produced by `pack`. Loaded via
    /// mmap; per-mode bundles + shared sections are read directly from
    /// the mapped slice. Wrapped as a one-region [`regions::RegionsState`]
    /// so the dispatch + per-region metric paths run uniformly for
    /// single-region deployments.
    Container(&'a Path),
}

/// Load all data and start the server(s)
#[allow(clippy::too_many_arguments)]
pub async fn serve(
    source: DataSource<'_>,
    port: Option<u16>,
    grpc_port: Option<u16>,
    transport: Transport,
    mode_filter: Option<&[String]>,
    region_filter: Option<&[String]>,
    load_options: &crate::server::state::LoadOptions,
    overlay_path: Option<&Path>,
    lazy_regions: bool,
) -> Result<()> {
    tracing::info!("Step 9: Starting query server...");

    // ---- Load every region as its own ServerState ------------------
    let (regions_state, data_dir_for_transit): (regions::RegionsState, std::path::PathBuf) =
        match source {
            DataSource::Directory(dir) => {
                let has_container = directory_has_butterfly_container(dir)?;
                if has_container {
                    tracing::info!(
                        dir = %dir.display(),
                        lazy = lazy_regions,
                        "multi-region container directory detected"
                    );
                    let regions_state = regions::RegionsState::load_from_dir_with_opts(
                        dir,
                        region_filter,
                        mode_filter,
                        lazy_regions,
                    )?;
                    (regions_state, dir.to_path_buf())
                } else {
                    tracing::info!(dir = %dir.display(), "legacy step-tree directory detected");
                    if region_filter.is_some() {
                        anyhow::bail!(
                            "--regions filter cannot be used with a legacy step-tree directory ({}); use a directory of *.butterfly containers instead",
                            dir.display()
                        );
                    }
                    let state = ServerState::load(dir, mode_filter)?;
                    let region_id = crate::pack::DEFAULT_REGION_ID.to_string();
                    let regions_state =
                        regions::RegionsState::from_single(region_id, dir.to_path_buf(), state);
                    (regions_state, dir.to_path_buf())
                }
            }
            DataSource::Container(file) => {
                if region_filter.is_some() {
                    anyhow::bail!(
                        "--regions filter cannot be used with --data (single container); use --data-dir for multi-region serve"
                    );
                }
                // load_options carries #160 lazy-CRC + warmup config.
                let state =
                    ServerState::load_from_container_with_options(file, mode_filter, load_options)?;
                let region_id = {
                    use crate::formats::butterfly_dat::Container;
                    let container = Container::open(file)
                        .with_context(|| format!("opening container {}", file.display()))?;
                    container.read_region_id(file).unwrap_or_else(|e| {
                        tracing::warn!(error = %e, "could not read region id; defaulting");
                        crate::pack::DEFAULT_REGION_ID.to_string()
                    })
                };
                let regions_state =
                    regions::RegionsState::from_single(region_id, file.to_path_buf(), state);
                let parent = file
                    .parent()
                    .unwrap_or_else(|| Path::new("."))
                    .to_path_buf();
                (regions_state, parent)
            }
        };

    // ---- Transit bootstrap (per-region, #334) ----------------------
    //
    // Each region can carry its own transit feeds. Discovery order, for
    // every loaded region:
    //   1. `<data_dir>/<region_id_lowercase>/transit/transit.toml`
    //      (multi-region convention — operator stages each region's
    //      feeds next to its container)
    //   2. `<data_dir>/transit/transit.toml`
    //      (legacy single-region convention — kept so an existing
    //      single-region deployment doesn't need reorganisation)
    //
    // For multi-region deployments without per-region transit dirs, only
    // the primary region picks up option 2; the others stay road-only.
    // Cross-region transit (origin in BE, destination in LU) is a future
    // follow-up — for now, each region's transit serves intra-region
    // origin/destination pairs.
    let mut regions_state = regions_state;
    let mut transit_loaded_count = 0usize;
    let n_regions = regions_state.regions.len();
    for idx in 0..n_regions {
        let region_id_lower = regions_state.regions[idx].id.to_lowercase();
        let per_region_dir = data_dir_for_transit.join(&region_id_lower);

        // Prefer per-region transit/. Fall back to the global
        // <data_dir>/transit/ only for the primary region so we don't
        // accidentally point every region at the same feeds.
        let cfg = match crate::transit::config::load(&per_region_dir)? {
            Some(cfg) => Some(cfg),
            None if idx == 0 => crate::transit::config::load(&data_dir_for_transit)?,
            None => None,
        };

        let Some(cfg) = cfg else {
            continue;
        };

        let region_id = regions_state.regions[idx].id.clone();
        regions_state.regions[idx].with_loaded_state_mut(|state_owned| {
            let foot_idx = match state_owned.mode_lookup.get("foot").copied() {
                Some(idx) => idx,
                None => {
                    tracing::warn!(
                        region = %region_id,
                        "transit configured but foot mode is not loaded; add 'foot' to --modes"
                    );
                    return;
                }
            };
            let foot = &state_owned.modes[foot_idx as usize];
            match crate::transit::load_from_disk(&cfg, foot, foot_idx, &state_owned.snap_index) {
                Ok(snapshot) => {
                    tracing::info!(
                        region = %region_id,
                        stops = snapshot.timetable.n_stops(),
                        routes = snapshot.timetable.n_routes(),
                        trips = snapshot.timetable.n_total_trips,
                        "transit snapshot loaded"
                    );
                    state_owned.install_transit(crate::transit::TransitState::new(cfg, snapshot));
                }
                Err(e) => {
                    tracing::warn!(
                        region = %region_id,
                        error = %e,
                        "no usable transit feeds on disk — run `butterfly-route transit-fetch` to populate. Continuing in road-only mode for this region."
                    );
                }
            }
        })?;

        if regions_state.regions[idx]
            .state_loaded()
            .is_some_and(|s| s.transit.is_some())
        {
            transit_loaded_count += 1;
        }
    }
    if transit_loaded_count == 0 {
        if n_regions > 1 {
            tracing::info!(
                "multi-region serve — no transit feeds discovered in any region (looked for `<data_dir>/<region>/transit/` + global fallback)"
            );
        } else {
            tracing::info!("no transit/ directory — running in road-only mode");
        }
    } else {
        tracing::info!(
            transit_loaded_count,
            n_regions,
            "transit subsystem loaded for {} of {} regions",
            transit_loaded_count,
            n_regions
        );
    }

    // ---- Per-region size metrics -----------------------------------
    // Skip Pending regions on the lazy boot path; their stats publish
    // after the first query loads the ServerState. state_loaded() is a
    // non-loading peek so this loop doesn't trigger N region loads.
    for r in &regions_state.regions {
        if let Some(s) = r.state_loaded() {
            crate::server::region_metrics::register_region_size(
                &r.id,
                s.ebg_nodes.n_nodes as u64,
                s.ebg_csr.n_arcs,
            );
        }
    }

    // ---- Cross-region overlay (#91 Phase 2) ------------------------
    if let Some(p) = overlay_path {
        tracing::info!(path = %p.display(), "loading cross-region overlay");
        let overlay = overlay::OverlayCluster::load(p)
            .with_context(|| format!("loading cross-region overlay from {}", p.display()))?;
        tracing::info!(
            n_regions = overlay.n_regions(),
            n_modes = overlay.modes.len(),
            "overlay loaded"
        );
        regions_state.overlay = Some(overlay);
    }

    let state = Arc::new(regions_state);

    // #152: emit the final RSS checkpoint after every initialization
    // step is done but before REST/gRPC listeners bind. Every
    // demand-paged section has been paged in once, every spatial
    // index is built, and every transit table is populated. This is
    // the steady-state baseline #153/#154/#155 will measure against,
    // captured prior to observable readiness on `/health`.
    crate::server::rss::checkpoint("boot.complete");

    // #292 Phase 6: spawn the LRU eviction poller. Reads VmRSS once
    // per `EVICT_POLL_SECS` and, if over budget, evicts the oldest
    // Loaded region(s) until back under budget or only `keep_min`
    // regions remain. Single-region deployments never evict because
    // the budget check is trivially satisfied (only one region —
    // nothing to compare against).
    {
        let state_for_evictor = Arc::clone(&state);
        let budget_bytes = rss_budget_bytes();
        tokio::spawn(async move {
            const EVICT_POLL_SECS: u64 = 30;
            const KEEP_MIN: usize = 1;
            tracing::info!(
                budget_gib = budget_bytes as f64 / (1u64 << 30) as f64,
                poll_secs = EVICT_POLL_SECS,
                "RSS-budget eviction poller started"
            );
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(EVICT_POLL_SECS)).await;
                let cur = match read_proc_vm_rss_bytes() {
                    Some(v) => v,
                    None => continue,
                };
                if cur <= budget_bytes {
                    continue;
                }
                tracing::info!(
                    rss_gib = cur as f64 / (1u64 << 30) as f64,
                    budget_gib = budget_bytes as f64 / (1u64 << 30) as f64,
                    "over RSS budget; evicting LRU regions"
                );
                let evicted = state_for_evictor.evict_lru_until(
                    || {
                        // Re-read RSS so each round sees fresh state.
                        read_proc_vm_rss_bytes()
                            .map(|r| r <= budget_bytes)
                            .unwrap_or(true)
                    },
                    KEEP_MIN,
                );
                if evicted > 0 {
                    tracing::info!(
                        evicted,
                        loaded_after = state_for_evictor.loaded_count(),
                        "LRU eviction pass complete"
                    );
                }
            }
        });
    }

    // Find free ports
    let http_port = match port {
        Some(p) => p,
        None => find_free_port(8080)?,
    };
    let grpc_port = grpc_port.unwrap_or(http_port + 1);

    match transport {
        Transport::Rest => {
            start_rest_server(state, http_port).await?;
        }
        Transport::Grpc => {
            start_grpc_server(state, grpc_port).await?;
        }
        Transport::Both => {
            let state_rest = Arc::clone(&state);
            let state_grpc = Arc::clone(&state);

            let rest_handle =
                tokio::spawn(async move { start_rest_server(state_rest, http_port).await });
            let grpc_handle =
                tokio::spawn(async move { start_grpc_server(state_grpc, grpc_port).await });

            // Wait for either to finish (typically via shutdown signal)
            tokio::select! {
                res = rest_handle => {
                    if let Err(e) = res {
                        tracing::error!(error = %e, "REST server task failed");
                    }
                }
                res = grpc_handle => {
                    if let Err(e) = res {
                        tracing::error!(error = %e, "gRPC server task failed");
                    }
                }
            }
        }
    }

    tracing::info!("server shut down gracefully");
    Ok(())
}

/// Start only the Axum REST/JSON server
async fn start_rest_server(state: Arc<regions::RegionsState>, port: u16) -> Result<()> {
    let app = api::build_router(state);

    let addr = format!("0.0.0.0:{}", port);
    tracing::info!(
        port = port,
        "REST server listening on http://127.0.0.1:{}",
        port
    );
    tracing::info!(
        port = port,
        "Swagger UI: http://127.0.0.1:{}/swagger-ui/",
        port
    );

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    Ok(())
}

/// Start only the Arrow Flight gRPC server
///
/// #336: Flight handlers dispatch per-action to the right region via
/// `dispatch_for_point` / `dispatch_for_pair`. Mixed-region batches
/// return FAILED_PRECONDITION (the gRPC analogue of REST 501).
async fn start_grpc_server(state: Arc<regions::RegionsState>, port: u16) -> Result<()> {
    let grpc_addr: std::net::SocketAddr = format!("0.0.0.0:{}", port).parse()?;
    tracing::info!(port = port, "gRPC Flight server listening on {}", grpc_addr);

    if state.len() > 1 {
        tracing::info!(
            n_regions = state.len(),
            "gRPC Flight multi-region: actions snap their first input point to pick the region; mixed-region batches return FAILED_PRECONDITION (#336)"
        );
    }
    // #292 Phase 3: pass the whole RegionsState; Flight resolves the
    // primary region per request so the default-lazy boot can keep
    // regions Pending until first query.
    let flight_svc = flight::build_flight_server(Arc::clone(&state));

    tonic::transport::Server::builder()
        .add_service(flight_svc)
        .serve_with_shutdown(grpc_addr, shutdown_signal())
        .await?;

    Ok(())
}
