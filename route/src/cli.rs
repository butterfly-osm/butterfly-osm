//! CLI commands for butterfly-route

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use std::path::{Path, PathBuf};

use crate::contraction;
use crate::customization;
use crate::ebg::{EbgConfig, build_ebg};
use crate::ingest::{IngestConfig, run_ingest};
use crate::nbg::{NbgConfig, build_nbg};
use crate::ordering;
use crate::ordering_lifted;
use crate::profile::{ProfileConfig, run_profiling};
use crate::profile_abi::Mode;
use crate::server;
use crate::validate::{
    Counts, LockFile, validate_step4, validate_step5, validate_step6, validate_step6_lifted,
    validate_step7, verify_lock_conditions,
};
use crate::weights;

/// Parse MODE=PATH pairs from CLI arguments, sorted alphabetically by mode name.
/// Returns (mode_name, mode_index, path) tuples with deterministic indices.
fn parse_mode_path_pairs(args: &[String], arg_name: &str) -> Result<Vec<(String, u8, PathBuf)>> {
    let mut pairs: Vec<(String, PathBuf)> = args
        .iter()
        .map(|s| {
            let (mode, path) = s.split_once('=').ok_or_else(|| {
                anyhow::anyhow!("Invalid --{} format '{}': expected MODE=PATH", arg_name, s)
            })?;
            Ok((mode.to_string(), PathBuf::from(path)))
        })
        .collect::<Result<Vec<_>>>()?;
    pairs.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(pairs
        .into_iter()
        .enumerate()
        .map(|(i, (name, path))| (name, i as u8, path))
        .collect())
}

/// Run the `extract-borders` subcommand: load each region's container,
/// extract border crossings, write JSON.
fn run_extract_borders(regions: &[PathBuf], out: &Path) -> Result<()> {
    use crate::server::border::extract_border_crossings;
    use crate::server::regions::RegionsState;

    anyhow::ensure!(
        regions.len() >= 2,
        "extract-borders requires at least 2 regions, got {}",
        regions.len()
    );
    tracing::info!(
        n = regions.len(),
        "extract-borders: loading {} regions",
        regions.len()
    );
    let started = std::time::Instant::now();
    let regions_state = RegionsState::load_from_paths(regions)?;
    tracing::info!(
        elapsed_ms = started.elapsed().as_millis() as u64,
        n_regions = regions_state.regions.len(),
        "extract-borders: regions loaded"
    );

    let pairs: Vec<(String, std::sync::Arc<crate::server::ServerState>)> = regions_state
        .regions
        .iter()
        .map(|r| (r.id.clone(), r.state()))
        .collect();

    let extract_started = std::time::Instant::now();
    let crossings = extract_border_crossings(&pairs);
    tracing::info!(
        n = crossings.len(),
        elapsed_ms = extract_started.elapsed().as_millis() as u64,
        "extract-borders: extracted crossings"
    );

    if let Some(first) = crossings.first() {
        tracing::info!(
            region_a = %first.region_a,
            node_a = first.node_a,
            lat_a = first.lat_a,
            lon_a = first.lon_a,
            region_b = %first.region_b,
            node_b = first.node_b,
            lat_b = first.lat_b,
            lon_b = first.lon_b,
            edge_distance_m = first.edge_distance_m,
            "first border crossing sample"
        );
    }

    #[derive(serde::Serialize)]
    struct CrossingJson<'a> {
        region_a: &'a str,
        node_a: u32,
        lat_a: f64,
        lon_a: f64,
        region_b: &'a str,
        node_b: u32,
        lat_b: f64,
        lon_b: f64,
        edge_distance_m: f64,
    }
    let json: Vec<CrossingJson<'_>> = crossings
        .iter()
        .map(|c| CrossingJson {
            region_a: &c.region_a,
            node_a: c.node_a,
            lat_a: c.lat_a,
            lon_a: c.lon_a,
            region_b: &c.region_b,
            node_b: c.node_b,
            lat_b: c.lat_b,
            lon_b: c.lon_b,
            edge_distance_m: c.edge_distance_m,
        })
        .collect();
    let bytes = serde_json::to_vec_pretty(&json)?;
    std::fs::write(out, &bytes)
        .with_context(|| format!("writing borders JSON to {}", out.display()))?;
    println!(
        "extract-borders: wrote {} crossings to {}",
        crossings.len(),
        out.display()
    );
    Ok(())
}

/// Run the `build-overlay` subcommand: load regions, extract borders,
/// build the cross-region overlay, persist to a `.butterfly` container.
fn run_build_overlay(regions: &[PathBuf], modes: Option<&str>, out: &Path) -> Result<()> {
    use crate::server::border::extract_border_crossings;
    use crate::server::overlay::build_overlay_in_memory;
    use crate::server::regions::RegionsState;

    anyhow::ensure!(
        regions.len() >= 2,
        "build-overlay requires at least 2 regions, got {}",
        regions.len()
    );
    tracing::info!(n = regions.len(), "build-overlay: loading regions");
    let regions_state = RegionsState::load_from_paths(regions)?;

    let pairs: Vec<(String, std::sync::Arc<crate::server::ServerState>)> = regions_state
        .regions
        .iter()
        .map(|r| (r.id.clone(), r.state()))
        .collect();

    let mode_list: Vec<String> = match modes {
        Some(s) => s
            .split(',')
            .map(|m| m.trim().to_lowercase())
            .filter(|m| !m.is_empty())
            .collect(),
        None => {
            let mut common: Vec<String> = pairs[0].1.mode_names.clone();
            for (_id, st) in &pairs[1..] {
                common.retain(|m| st.mode_names.contains(m));
            }
            common
        }
    };
    anyhow::ensure!(
        !mode_list.is_empty(),
        "no modes selected for build-overlay (intersection of regions is empty)"
    );
    tracing::info!(modes = ?mode_list, "build-overlay: mode list");

    tracing::info!("build-overlay: extracting borders");
    let extract_started = std::time::Instant::now();
    let crossings = extract_border_crossings(&pairs);
    tracing::info!(
        n = crossings.len(),
        elapsed_ms = extract_started.elapsed().as_millis() as u64,
        "build-overlay: borders extracted"
    );

    tracing::info!("build-overlay: building matrix (this is the slow step)");
    let matrix_started = std::time::Instant::now();
    let cluster = build_overlay_in_memory(&pairs, &crossings, &mode_list)?;
    tracing::info!(
        elapsed_s = matrix_started.elapsed().as_secs_f64(),
        "build-overlay: matrix built"
    );

    tracing::info!(out = %out.display(), "build-overlay: writing container");
    cluster.write_to_path(out)?;
    println!(
        "build-overlay: wrote overlay to {} ({} crossings, {} regions, modes {:?})",
        out.display(),
        crossings.len(),
        cluster.region_order.len(),
        cluster.modes
    );
    Ok(())
}

/// Resolve a mode name to a Mode by discovering modes from a data directory.
/// The directory should contain mode-specific files (way_attrs.*.bin, w.*.u32, or filtered.*.ebg).
fn resolve_mode(mode_name: &str, data_dir: &Path) -> Result<Mode> {
    let discovered = Mode::discover_from_dir(data_dir);
    if discovered.is_empty() {
        anyhow::bail!(
            "No modes found in {}. Expected way_attrs.*.bin, w.*.u32, or filtered.*.ebg files.",
            data_dir.display()
        );
    }
    match discovered.iter().find(|(name, _)| name == mode_name) {
        Some((_, idx)) => Ok(Mode(*idx)),
        None => {
            let available: Vec<&str> = discovered.iter().map(|(n, _)| n.as_str()).collect();
            anyhow::bail!(
                "Unknown mode '{}'. Available modes: {:?}",
                mode_name,
                available
            );
        }
    }
}

#[derive(Parser)]
#[command(name = "butterfly-route")]
#[command(about = "High-performance OSM routing engine", long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Step 1: Ingest OSM PBF into immutable artifacts
    Step1Ingest {
        /// Input OSM PBF file
        #[arg(short, long)]
        input: PathBuf,

        /// Output directory for artifacts
        #[arg(short, long)]
        outdir: PathBuf,

        /// Number of threads (currently unused, kept for future)
        #[arg(short, long, default_value = "8")]
        threads: usize,

        /// Verify only (don't write, just check CRCs)
        #[arg(long)]
        verify_only: bool,
    },

    /// Step 2: Generate per-mode attributes via routing profiles
    Step2Profile {
        /// Path to ways.raw from Step 1
        #[arg(long)]
        ways: PathBuf,

        /// Path to relations.raw from Step 1
        #[arg(long)]
        relations: PathBuf,

        /// Directory containing *.model.json files
        #[arg(long)]
        models_dir: PathBuf,

        /// Density classifier: `osm-tag` (default, deterministic, no extra
        /// data) or `cdis-parquet` (proprietary plug-in, not implemented).
        #[arg(long, default_value = "osm-tag")]
        density_classifier: String,

        /// Output directory for way_attrs.*.bin and turn_rules.*.bin
        #[arg(short, long)]
        outdir: PathBuf,
    },

    /// Step 3: Build node-based graph (NBG) from Step 1 and Step 2
    Step3Nbg {
        /// Path to nodes.sa from Step 1
        #[arg(long)]
        nodes: PathBuf,

        /// Path to ways.raw from Step 1
        #[arg(long)]
        ways: PathBuf,

        /// Per-mode way_attrs paths as mode=path pairs (e.g. --way-attrs car=way_attrs.car.bin --way-attrs bike=way_attrs.bike.bin)
        #[arg(long = "way-attrs", value_name = "MODE=PATH")]
        way_attrs: Vec<String>,

        /// Output directory for nbg.csr, nbg.geo, nbg.node_map
        #[arg(short, long)]
        outdir: PathBuf,
    },

    /// Step 4: Build edge-based graph (EBG) with turn expansion
    Step4Ebg {
        /// Path to nbg.csr from Step 3
        #[arg(long)]
        nbg_csr: PathBuf,

        /// Path to nbg.geo from Step 3
        #[arg(long)]
        nbg_geo: PathBuf,

        /// Path to nbg.node_map from Step 3
        #[arg(long)]
        nbg_node_map: PathBuf,

        /// Path to node_signals.bin from Step 1 (optional)
        #[arg(long)]
        node_signals: Option<PathBuf>,

        /// Per-mode way_attrs paths as mode=path pairs (e.g. --way-attrs car=way_attrs.car.bin)
        #[arg(long = "way-attrs", value_name = "MODE=PATH")]
        way_attrs: Vec<String>,

        /// Per-mode turn_rules paths as mode=path pairs (e.g. --turn-rules car=turn_rules.car.bin)
        #[arg(long = "turn-rules", value_name = "MODE=PATH")]
        turn_rules: Vec<String>,

        /// Output directory for ebg.nodes, ebg.csr, ebg.turn_table
        #[arg(short, long)]
        outdir: PathBuf,
    },

    /// Step 5: Generate per-mode weights & masks
    Step5Weights {
        /// Path to ebg.nodes from Step 4
        #[arg(long)]
        ebg_nodes: PathBuf,

        /// Path to ebg.csr from Step 4
        #[arg(long)]
        ebg_csr: PathBuf,

        /// Path to ebg.turn_table from Step 4
        #[arg(long)]
        turn_table: PathBuf,

        /// Path to nbg.geo from Step 3
        #[arg(long)]
        nbg_geo: PathBuf,

        /// Per-mode way_attrs paths as mode=path pairs (e.g. --way-attrs car=way_attrs.car.bin)
        #[arg(long = "way-attrs", value_name = "MODE=PATH")]
        way_attrs: Vec<String>,

        /// Output directory for w.*.u32, t.*.u32, mask.*.bitset
        #[arg(short, long)]
        outdir: PathBuf,
    },

    /// Step 6: Generate per-mode CCH ordering on filtered EBG via nested dissection
    Step6Order {
        /// Path to filtered.*.ebg from Step 5
        #[arg(long)]
        filtered_ebg: PathBuf,

        /// Path to ebg.nodes from Step 4
        #[arg(long)]
        ebg_nodes: PathBuf,

        /// Path to nbg.geo from Step 3 (for coordinates)
        #[arg(long)]
        nbg_geo: PathBuf,

        /// Mode name (discovered from way_attrs.*.bin files in data dir)
        #[arg(long)]
        mode: String,

        /// Output directory for order.*.ebg
        #[arg(short, long)]
        outdir: PathBuf,

        /// Leaf threshold for recursion (default: 8192)
        #[arg(long, default_value = "8192")]
        leaf_threshold: usize,

        /// Balance epsilon (default: 0.05)
        #[arg(long, default_value = "0.05")]
        balance_eps: f32,
    },

    /// Step 6 (Lifted): Generate CCH ordering via NBG ND + lift to EBG
    ///
    /// This is the CORRECT approach for CCH: compute nested dissection on the
    /// physical node graph (NBG), then lift to edge-states with block ranks.
    /// This preserves separator quality unlike ordering on the EBG directly.
    Step6Lifted {
        /// Path to nbg.csr from Step 3
        #[arg(long)]
        nbg_csr: PathBuf,

        /// Path to nbg.geo from Step 3
        #[arg(long)]
        nbg_geo: PathBuf,

        /// Path to ebg.nodes from Step 4
        #[arg(long)]
        ebg_nodes: PathBuf,

        /// Path to ebg.csr from Step 4
        #[arg(long)]
        ebg_csr: PathBuf,

        /// Path to filtered.*.ebg from Step 5
        #[arg(long)]
        filtered_ebg: PathBuf,

        /// Mode name (discovered from way_attrs.*.bin files in data dir)
        #[arg(long)]
        mode: String,

        /// Output directory
        #[arg(short, long)]
        outdir: PathBuf,

        /// Leaf threshold for ND recursion (default: 8192)
        #[arg(long, default_value = "8192")]
        leaf_threshold: usize,
    },

    /// Step 7: Build per-mode CCH topology via contraction on filtered EBG
    Step7Contract {
        /// Path to filtered.*.ebg from Step 5
        #[arg(long)]
        filtered_ebg: PathBuf,

        /// Path to order.*.ebg from Step 6
        #[arg(long)]
        order: PathBuf,

        /// Path to w.*.u32 weights file from Step 5 (for metric-aware witness search)
        #[arg(long)]
        weights: PathBuf,

        /// Path to t.*.u32 turn penalties file from Step 5 (for metric-aware witness search)
        #[arg(long)]
        turns: PathBuf,

        /// Mode name (discovered from way_attrs.*.bin files in data dir)
        #[arg(long)]
        mode: String,

        /// Output directory for cch.*.topo
        #[arg(short, long)]
        outdir: PathBuf,
    },

    /// Step 8: Customize per-mode CCH with weights. Optional `--traffic`
    /// switches into a fast traffic recustomization that scales edge weights
    /// by per-density-class speed factors and emits `cch.w.<mode>_<variant>.u32`.
    Step8Customize {
        /// Path to cch.*.topo from Step 7
        #[arg(long)]
        cch_topo: PathBuf,

        /// Path to filtered.*.ebg from Step 5
        #[arg(long)]
        filtered_ebg: PathBuf,

        /// Path to order.*.ebg from Step 6
        #[arg(long)]
        order: PathBuf,

        /// Path to w.*.u32 weights file from Step 5
        #[arg(long)]
        weights: PathBuf,

        /// Path to t.*.u32 turn penalties file from Step 5
        #[arg(long)]
        turns: PathBuf,

        /// Path to ebg.nodes from Step 4 (for distance weights)
        #[arg(long)]
        ebg_nodes: PathBuf,

        /// Mode name (discovered from way_attrs.*.bin files in data dir)
        #[arg(long)]
        mode: String,

        /// Output directory for cch.w.*.u32 and cch.d.*.u32
        #[arg(short, long)]
        outdir: PathBuf,

        /// OPTIONAL: path to a `*.traffic.json` profile. When set, performs a
        /// fast traffic recustomization that scales edge weights per density
        /// class and writes `cch.w.<mode>_<variant>.u32` (no distance file —
        /// distance is physical). Requires `--way-attrs` and `--nbg-geo`.
        #[arg(long)]
        traffic: Option<PathBuf>,

        /// REQUIRED with `--traffic`: path to `way_attrs.<mode>.bin` from
        /// step 2. Used to look up per-way `density_class`.
        #[arg(long)]
        way_attrs: Option<PathBuf>,

        /// REQUIRED with `--traffic`: path to `nbg.geo` from step 3. Used to
        /// map EBG nodes back to their first OSM way id.
        #[arg(long)]
        nbg_geo: Option<PathBuf>,

        /// DEVELOPMENT-ONLY: skip triangle relaxation. Produces INCORRECT
        /// (over-estimated) routing durations — only use for benchmark
        /// experiments. Without `--traffic` this flag has no effect.
        #[arg(long, hide = true)]
        skip_triangle_relax: bool,
    },

    /// Download (refresh) GTFS transit feeds into `<data>/transit/gtfs/`.
    ///
    /// Transit feeds are refreshed at rebuild time — same model as the
    /// OSM PBF. Run this on a cron or as part of a rebuild pipeline,
    /// then restart the server.
    ///
    /// Reads `<data>/transit/transit.toml` if present; otherwise uses
    /// the default Belgium feed set (SNCB, De Lijn, TEC, STIB).
    TransitFetch {
        /// Data directory (the one you pass to `serve --data-dir`).
        #[arg(short, long)]
        data_dir: PathBuf,

        /// Also download one-shot GTFS-RT trip-updates snapshots for
        /// every feed that configures an `rt_url`.
        #[arg(long)]
        realtime: bool,
    },

    /// Pack a `data_dir/step{1..8}/` tree into a single `*.butterfly`
    /// container. The container holds every per-step artefact plus a
    /// section directory + per-section CRCs, ready for a single
    /// `serve --data <file>` mmap load.
    Pack {
        /// Source data directory (the one with `step1/`, `step2/`, ... ).
        #[arg(short, long)]
        data_dir: PathBuf,

        /// Output container path (e.g. `belgium.butterfly`).
        #[arg(short, long)]
        out: PathBuf,

        /// Override which step subdir names to look for. Default uses
        /// the same `find_step_dir`-style fuzzy match used by `serve`.
        #[arg(long)]
        step_prefix: Option<String>,

        /// Region identifier embedded in the container manifest
        /// (e.g. `BE`, `LU`, `FR`). Used by `serve --data-dir` to
        /// dispatch queries across multiple regions. Default: `BE`
        /// (the historical Belgium-only baseline). Allowed characters:
        /// `[A-Z0-9_-]`, max 16 chars; lowercase input is upper-cased.
        #[arg(long)]
        region: Option<String>,
    },

    /// Show the section directory of a `*.butterfly` container.
    /// Optionally re-verify per-section CRCs (`--verify`) or the full
    /// file CRC (`--verify-full`).
    Inspect {
        /// Path to a `*.butterfly` container.
        path: PathBuf,

        /// Verify each section's CRC by reading the bytes back.
        #[arg(long)]
        verify: bool,

        /// Verify the whole-file CRC. Slow on multi-GB containers.
        #[arg(long)]
        verify_full: bool,
    },

    /// Inverse of `pack`: extract every section in a `*.butterfly`
    /// container back to a `step{N}/<file>` tree under `--out`.
    /// Useful for round-trip tests and for feeding `serve --data-dir`
    /// with the unpacked tree until the in-place container loader
    /// (Phase 5) lands.
    Unpack {
        /// Path to a `*.butterfly` container.
        #[arg(short, long)]
        path: PathBuf,

        /// Output directory. Must not already exist.
        #[arg(short, long)]
        out: PathBuf,
    },

    /// Issue #146: empirical sharing analysis between mode pairs in a
    /// `*.butterfly` container. Loads each mode's accessibility mask +
    /// filtered-EBG arc set + topology section header, computes node
    /// and arc Jaccard overlap for every pair, and emits a JSON report
    /// to stdout (with a human-readable summary on stderr).
    ///
    /// Output is the empirical input to the #146 acceptance decision
    /// — whether two modes' CCH topologies share enough structure that
    /// bundling them (one shared topology + per-mode customised
    /// weights) pays off vs the per-mode baseline. The tool projects a
    /// linear-scaling estimate; the ground-truth measurement still
    /// requires actually rebuilding step5/6/7 on the union, which is
    /// out of scope for this command.
    TopologyDiff {
        /// Path to a `*.butterfly` container.
        #[arg(short, long)]
        path: PathBuf,

        /// Comma-separated list of modes to compare. If omitted, every
        /// mode in the container is compared pairwise.
        #[arg(long)]
        modes: Option<String>,
    },

    /// Step 9: Start query server
    Serve {
        /// Directory containing all step outputs (step3/, step4/, etc.).
        /// Mutually exclusive with `--data`.
        #[arg(short, long, conflicts_with = "data")]
        data_dir: Option<PathBuf>,

        /// Path to a single `.butterfly` container produced by `pack`.
        /// Loads via mmap; mutually exclusive with `--data-dir`.
        #[arg(long, conflicts_with = "data_dir")]
        data: Option<PathBuf>,

        /// Port for REST/HTTP server (default: find free port starting from 8080)
        #[arg(short, long)]
        port: Option<u16>,

        /// Port for Arrow Flight gRPC server (default: REST port + 1)
        #[arg(long)]
        grpc_port: Option<u16>,

        /// Transport mode: rest, grpc, or both (default: both)
        #[arg(long, default_value = "both")]
        transport: String,

        /// Load only specific modes (comma-separated). Default: all discovered modes.
        /// Example: --modes car,bike
        #[arg(long)]
        modes: Option<String>,

        /// Load only specific regions (comma-separated). Default: every
        /// `*.butterfly` container in `--data-dir` is loaded.
        /// Example: `--regions BE,LU`. Ignored when `--data` is used
        /// (single-container mode is implicitly one region).
        #[arg(long)]
        regions: Option<String>,

        /// Log format: "text" (default) or "json"
        #[arg(long, default_value = "text")]
        log_format: String,

        /// Emit RSS / RssAnon / RssFile checkpoints (parsed from
        /// `/proc/self/smaps_rollup`) at every boot phase: shared
        /// section load, each per-mode bundle load, global spatial
        /// index, each per-mode spatial index, and `/health` first
        /// becomes ready. Lines are tagged `RSS_CHECKPOINT phase=...
        /// total_kb=N anon_kb=M file_kb=K` so they can be grepped out
        /// of the structured log stream.
        ///
        /// Also enabled by setting `BUTTERFLY_RSS_CHECKPOINTS=1`.
        ///
        /// This instrumentation is the foundation for the
        /// #153/#154/#155 measurement discipline; it stays in the
        /// codebase as a supported flag, not as a one-shot diagnostic.
        #[arg(long, default_value = "false")]
        rss_checkpoints: bool,

        /// #160: walk every section's CRC at boot (legacy behaviour).
        /// Mutually exclusive with `--warmup-on-boot`. Default off, in
        /// which case verification is deferred to first access of each
        /// section via the lazy-CRC gate. Use this for environments
        /// that prefer the pre-#160 fail-fast-on-boot semantics over a
        /// fast first byte at the listener.
        #[arg(long, default_value = "false", conflicts_with = "warmup_on_boot")]
        eager_verify: bool,

        /// #160: kick off a background CRC walk for every still-
        /// `Unverified` section right after boot completes. Matches the
        /// total-coverage of `--eager-verify` without blocking the
        /// listener. Mutually exclusive with `--eager-verify`.
        #[arg(long, default_value = "false")]
        warmup_on_boot: bool,

        /// #91 Phase 2: cross-region overlay container. When supplied,
        /// cross-region P2P queries are served via the overlay matrix
        /// instead of returning 501. Build the overlay with
        /// `butterfly-route build-overlay`.
        #[arg(long)]
        overlay: Option<PathBuf>,
    },

    /// #91 Phase 2: extract cross-region border crossings from a list
    /// of per-region containers. Writes a JSON file describing every
    /// matched border-node pair (one EBG node id per region plus its
    /// lat/lon and the haversine distance between the two endpoints).
    /// Used as input to `build-overlay`.
    ExtractBorders {
        /// One or more `.butterfly` containers (one per region).
        #[arg(long = "regions", value_name = "PATH", required = true, num_args = 1..)]
        regions: Vec<PathBuf>,

        /// Output JSON file.
        #[arg(short, long)]
        out: PathBuf,
    },

    /// #91 Phase 2: build a cross-region overlay container from a list
    /// of per-region containers. Extracts border crossings, runs
    /// per-region CCH P2P to populate the border-to-border matrix per
    /// mode, and writes a single `.butterfly` overlay container.
    BuildOverlay {
        /// One or more `.butterfly` containers (one per region).
        #[arg(long = "regions", value_name = "PATH", required = true, num_args = 1..)]
        regions: Vec<PathBuf>,

        /// Modes to include in the overlay (comma-separated). Default:
        /// every mode that all regions carry.
        #[arg(long)]
        modes: Option<String>,

        /// Output `.butterfly` overlay container.
        #[arg(short, long)]
        out: PathBuf,
    },

    /// Validate CCH correctness by comparing bidirectional CCH vs CCH-Dijkstra
    ValidateCch {
        /// Path to cch.*.topo from Step 7
        #[arg(long)]
        cch_topo: PathBuf,

        /// Path to cch.w.*.u32 from Step 8
        #[arg(long)]
        cch_weights: PathBuf,

        /// Path to order.*.ebg from Step 6
        #[arg(long)]
        order: PathBuf,

        /// Mode name (discovered from way_attrs.*.bin files in data dir)
        #[arg(long)]
        mode: String,

        /// Number of random query pairs (default: 50000)
        #[arg(long, default_value = "50000")]
        n_pairs: usize,

        /// Random seed (default: 42424242)
        #[arg(long, default_value = "42424242")]
        seed: u64,

        /// Output file for failures (optional)
        #[arg(long)]
        failures_file: Option<PathBuf>,
    },

    /// Run targeted regression tests for CCH edge cases
    RegressionCch {
        /// Path to cch.*.topo from Step 7
        #[arg(long)]
        cch_topo: PathBuf,

        /// Path to cch.w.*.u32 from Step 8
        #[arg(long)]
        cch_weights: PathBuf,

        /// Path to order.*.ebg from Step 6
        #[arg(long)]
        order: PathBuf,

        /// Mode name (discovered from way_attrs.*.bin files in data dir)
        #[arg(long)]
        mode: String,
    },

    /// Validate graph/weight invariants for CCH correctness
    ValidateInvariants {
        /// Path to cch.*.topo from Step 7
        #[arg(long)]
        cch_topo: PathBuf,

        /// Path to cch.w.*.u32 from Step 8
        #[arg(long)]
        cch_weights: PathBuf,

        /// Path to order.*.ebg from Step 6
        #[arg(long)]
        order: PathBuf,

        /// Mode name (discovered from way_attrs.*.bin files in data dir)
        #[arg(long)]
        mode: String,
    },

    /// Bounded Dijkstra for isochrone (range query)
    RangeCch {
        /// Path to cch.*.topo from Step 7
        #[arg(long)]
        cch_topo: PathBuf,

        /// Path to cch.w.*.u32 from Step 8
        #[arg(long)]
        cch_weights: PathBuf,

        /// Path to order.*.ebg from Step 6
        #[arg(long)]
        order: PathBuf,

        /// Origin node ID (EBG node, not OSM ID)
        #[arg(long)]
        origin_node: u32,

        /// Time threshold in milliseconds
        #[arg(long)]
        threshold_ms: u32,

        /// Mode name (discovered from way_attrs.*.bin files in data dir)
        #[arg(long)]
        mode: String,
    },

    /// Validate range query properties (monotonicity, equivalence, P2P consistency)
    ValidateRange {
        /// Path to cch.*.topo from Step 7
        #[arg(long)]
        cch_topo: PathBuf,

        /// Path to cch.w.*.u32 from Step 8
        #[arg(long)]
        cch_weights: PathBuf,

        /// Path to order.*.ebg from Step 6
        #[arg(long)]
        order: PathBuf,

        /// Origin node ID (EBG node, not OSM ID)
        #[arg(long)]
        origin_node: u32,

        /// Mode name (discovered from way_attrs.*.bin files in data dir)
        #[arg(long)]
        mode: String,
    },

    /// PHAST-based range query (fast one-to-many)
    PhastRange {
        /// Path to cch.*.topo from Step 7
        #[arg(long)]
        cch_topo: PathBuf,

        /// Path to cch.w.*.u32 from Step 8
        #[arg(long)]
        cch_weights: PathBuf,

        /// Path to order.*.ebg from Step 6
        #[arg(long)]
        order: PathBuf,

        /// Origin node ID (EBG node, not OSM ID)
        #[arg(long)]
        origin_node: u32,

        /// Time threshold in milliseconds
        #[arg(long)]
        threshold_ms: u32,

        /// Mode name (discovered from way_attrs.*.bin files in data dir)
        #[arg(long)]
        mode: String,
    },

    /// Validate PHAST correctness against naive Dijkstra
    ValidatePhast {
        /// Path to cch.*.topo from Step 7
        #[arg(long)]
        cch_topo: PathBuf,

        /// Path to cch.w.*.u32 from Step 8
        #[arg(long)]
        cch_weights: PathBuf,

        /// Path to order.*.ebg from Step 6
        #[arg(long)]
        order: PathBuf,

        /// Origin node ID (EBG node, not OSM ID)
        #[arg(long)]
        origin_node: u32,

        /// Time threshold in milliseconds
        #[arg(long)]
        threshold_ms: u32,

        /// Mode name (discovered from way_attrs.*.bin files in data dir)
        #[arg(long)]
        mode: String,
    },

    /// Validate block-gated PHAST against active-set PHAST
    ValidateBlockGated {
        /// Path to cch.*.topo from Step 7
        #[arg(long)]
        cch_topo: PathBuf,

        /// Path to cch.w.*.u32 from Step 8
        #[arg(long)]
        cch_weights: PathBuf,

        /// Path to order.*.ebg from Step 6
        #[arg(long)]
        order: PathBuf,

        /// Comma-separated origin node IDs (EBG nodes)
        #[arg(long, default_value = "0,1000,10000,100000")]
        origins: String,

        /// Comma-separated time thresholds in milliseconds
        #[arg(long, default_value = "60000,300000,600000")]
        thresholds: String,
    },

    /// Extract frontier on base graph (real road segments, not CCH shortcuts)
    ExtractFrontier {
        /// Path to cch.*.topo from Step 7
        #[arg(long)]
        cch_topo: PathBuf,

        /// Path to cch.w.*.u32 from Step 8
        #[arg(long)]
        cch_weights: PathBuf,

        /// Path to order.*.ebg from Step 6
        #[arg(long)]
        order: PathBuf,

        /// Path to filtered.*.ebg from Step 5
        #[arg(long)]
        filtered_ebg: PathBuf,

        /// Path to ebg.nodes from Step 4
        #[arg(long)]
        ebg_nodes: PathBuf,

        /// Path to nbg.geo from Step 3
        #[arg(long)]
        nbg_geo: PathBuf,

        /// Path to w.*.u32 (base edge weights) from Step 5
        #[arg(long)]
        base_weights: PathBuf,

        /// Origin node ID (filtered EBG node, not OSM ID)
        #[arg(long)]
        origin_node: u32,

        /// Time threshold in milliseconds
        #[arg(long)]
        threshold_ms: u32,

        /// Mode name (discovered from way_attrs.*.bin files in data dir)
        #[arg(long)]
        mode: String,

        /// Optional: export frontier to GeoJSON file
        #[arg(long)]
        geojson_out: Option<PathBuf>,
    },

    /// Generate isochrone polygon (full pipeline: PHAST → frontier → contour)
    Isochrone {
        /// Path to cch.*.topo from Step 7
        #[arg(long)]
        cch_topo: PathBuf,

        /// Path to cch.w.*.u32 from Step 8
        #[arg(long)]
        cch_weights: PathBuf,

        /// Path to order.*.ebg from Step 6
        #[arg(long)]
        order: PathBuf,

        /// Path to filtered.*.ebg from Step 5
        #[arg(long)]
        filtered_ebg: PathBuf,

        /// Path to ebg.nodes from Step 4
        #[arg(long)]
        ebg_nodes: PathBuf,

        /// Path to nbg.geo from Step 3
        #[arg(long)]
        nbg_geo: PathBuf,

        /// Path to w.*.u32 (base edge weights) from Step 5
        #[arg(long)]
        base_weights: PathBuf,

        /// Origin node ID (filtered EBG node, not OSM ID)
        #[arg(long)]
        origin_node: u32,

        /// Time threshold in milliseconds
        #[arg(long)]
        threshold_ms: u32,

        /// Mode name (discovered from way_attrs.*.bin files in data dir)
        #[arg(long)]
        mode: String,

        /// Output GeoJSON file
        #[arg(long)]
        output: PathBuf,

        /// Cell size in meters (default: mode-dependent)
        #[arg(long)]
        cell_size: Option<f64>,
    },

    /// Step 6 (Hybrid): Generate CCH ordering on hybrid state graph
    Step6Hybrid {
        /// Path to hybrid.<mode>.state from Step 5.5
        #[arg(long)]
        hybrid_state: PathBuf,

        /// Path to nbg.geo from Step 3 (for coordinates)
        #[arg(long)]
        nbg_geo: PathBuf,

        /// Mode name (discovered from way_attrs.*.bin files in data dir)
        #[arg(long)]
        mode: String,

        /// Output directory for order.hybrid.<mode>.ebg
        #[arg(short, long)]
        outdir: PathBuf,

        /// Leaf threshold for recursion (default: 8192)
        #[arg(long, default_value = "8192")]
        leaf_threshold: usize,

        /// Balance epsilon (default: 0.05)
        #[arg(long, default_value = "0.05")]
        balance_eps: f32,

        /// Use graph-based partitioning instead of geometry-based
        /// Enable this for equivalence-class hybrid where coordinate-based ND fails
        #[arg(long, default_value = "false")]
        graph_partition: bool,

        /// Densifier threshold: states with in×out > threshold are forced to late ranks
        /// This prevents fill-in explosion from high-degree nodes
        /// Use densifier-analysis command to find appropriate threshold (e.g., 50 or 100)
        #[arg(long, default_value = "0")]
        densifier_threshold: usize,
    },

    /// Step 7 (Hybrid): Build CCH topology via contraction on hybrid state graph
    Step7Hybrid {
        /// Path to hybrid.<mode>.state from Step 5.5
        #[arg(long)]
        hybrid_state: PathBuf,

        /// Path to order.hybrid.<mode>.ebg from Step 6 Hybrid
        #[arg(long)]
        order: PathBuf,

        /// Mode name (discovered from way_attrs.*.bin files in data dir)
        #[arg(long)]
        mode: String,

        /// Output directory for cch.hybrid.<mode>.topo
        #[arg(short, long)]
        outdir: PathBuf,
    },

    /// Build Node-Based CH on NBG (for junction expansion approach)
    ///
    /// This builds a contraction hierarchy on the node-based graph (1.9M nodes)
    /// instead of the edge-based graph (5M nodes). Combined with junction
    /// expansion at query time, this provides exact turn handling with 2-3x
    /// less overhead than edge-based CCH.
    BuildNbgCh {
        /// Path to nbg.csr from Step 3
        #[arg(long)]
        nbg_csr: PathBuf,

        /// Path to nbg.geo from Step 3
        #[arg(long)]
        nbg_geo: PathBuf,

        /// Leaf threshold for ND recursion (default: 8192)
        #[arg(long, default_value = "8192")]
        leaf_threshold: usize,

        /// Balance epsilon (default: 0.05)
        #[arg(long, default_value = "0.05")]
        balance_eps: f32,

        /// Run matrix benchmark after building
        #[arg(long)]
        benchmark: bool,

        /// Validate correctness against Dijkstra ground truth
        #[arg(long)]
        validate: bool,

        /// Number of validation tests (default: 1000)
        #[arg(long, default_value = "1000")]
        validate_tests: usize,
    },

    /// Step 8 (Hybrid): Customize CCH with weights from hybrid state graph
    Step8Hybrid {
        /// Path to cch.hybrid.<mode>.topo from Step 7 Hybrid
        #[arg(long)]
        cch_topo: PathBuf,

        /// Path to hybrid.<mode>.state from Step 5.5
        #[arg(long)]
        hybrid_state: PathBuf,

        /// Mode name (discovered from way_attrs.*.bin files in data dir)
        #[arg(long)]
        mode: String,

        /// Output directory for cch.w.hybrid.<mode>.u32
        #[arg(short, long)]
        outdir: PathBuf,
    },
}

impl Cli {
    pub fn run(self) -> Result<()> {
        match self.command {
            Commands::Step1Ingest {
                input,
                outdir,
                threads: _,
                verify_only,
            } => {
                if verify_only {
                    // Verify mode: check existing files
                    let nodes_sa_path = outdir.join("nodes.sa");
                    let nodes_si_path = outdir.join("nodes.si");
                    let ways_path = outdir.join("ways.raw");
                    let relations_path = outdir.join("relations.raw");

                    verify_lock_conditions(
                        &nodes_sa_path,
                        &nodes_si_path,
                        &ways_path,
                        &relations_path,
                    )?;
                } else {
                    // Ingest mode: run the pipeline
                    let config = IngestConfig {
                        input: input.clone(),
                        outdir: outdir.clone(),
                    };

                    let result = run_ingest(config)?;

                    // Verify the output
                    println!();
                    verify_lock_conditions(
                        &result.nodes_sa_file,
                        &result.nodes_si_file,
                        &result.ways_file,
                        &result.relations_file,
                    )?;

                    // Generate lock file
                    println!();
                    let lock = LockFile::create(
                        &input,
                        &result.nodes_sa_file,
                        &result.nodes_si_file,
                        &result.ways_file,
                        &result.relations_file,
                        Counts {
                            nodes: result.nodes_count,
                            ways: result.ways_count,
                            relations: result.relations_count,
                        },
                    )?;

                    let lock_path = outdir.join("step1.lock.json");
                    lock.write(&lock_path)?;

                    println!();
                    println!("🎉 Success! All lock conditions passed.");
                    println!("📋 Lock file: {}", lock_path.display());
                }

                Ok(())
            }
            Commands::Step2Profile {
                ways,
                relations,
                models_dir,
                density_classifier,
                outdir,
            } => {
                let classifier = crate::density::DensityClassifier::parse(&density_classifier)?;
                let config = ProfileConfig {
                    ways_path: ways,
                    relations_path: relations,
                    models_dir,
                    outdir,
                    density_classifier: classifier,
                };

                run_profiling(config)?;
                Ok(())
            }
            Commands::Step3Nbg {
                nodes,
                ways,
                way_attrs,
                outdir,
            } => {
                let wa_parsed = parse_mode_path_pairs(&way_attrs, "way-attrs")?;
                let way_attrs_paths: Vec<(String, PathBuf)> = wa_parsed
                    .into_iter()
                    .map(|(name, _, path)| (name, path))
                    .collect();

                let config = NbgConfig {
                    nodes_sa_path: nodes,
                    ways_path: ways,
                    way_attrs_paths,
                    outdir: outdir.clone(),
                };

                let result = build_nbg(config)?;

                // Verify lock conditions
                println!();
                crate::validate::verify_step3_lock_conditions(
                    &result.csr_path,
                    &result.geo_path,
                    &result.node_map_path,
                )?;

                // Generate lock file
                println!();
                println!("🔒 Generating Step 3 lock file...");

                let components = crate::validate::step3::compute_component_stats(&result.csr_path)?;

                let lock = crate::validate::Step3LockFile::create(
                    &result.csr_path,
                    &result.geo_path,
                    &result.node_map_path,
                    result.n_nodes,
                    result.n_edges_und,
                    components,
                    0, // RSS tracking would require build-time instrumentation
                )?;

                let lock_path = outdir.join("step3.lock.json");
                lock.write(&lock_path)?;
                println!("  ✓ Wrote {}", lock_path.display());

                println!();
                println!("🎉 Success! All lock conditions passed.");
                println!("📋 Lock file: {}", lock_path.display());

                Ok(())
            }
            Commands::Step4Ebg {
                nbg_csr,
                nbg_geo,
                nbg_node_map,
                node_signals,
                way_attrs,
                turn_rules,
                outdir,
            } => {
                let wa_parsed = parse_mode_path_pairs(&way_attrs, "way-attrs")?;
                let tr_parsed = parse_mode_path_pairs(&turn_rules, "turn-rules")?;

                // Validate that way_attrs and turn_rules have the same set of modes
                let wa_modes: Vec<&str> = wa_parsed.iter().map(|(n, _, _)| n.as_str()).collect();
                let tr_modes: Vec<&str> = tr_parsed.iter().map(|(n, _, _)| n.as_str()).collect();
                if wa_modes != tr_modes {
                    anyhow::bail!(
                        "Mismatched modes: --way-attrs has {:?}, --turn-rules has {:?}",
                        wa_modes,
                        tr_modes
                    );
                }

                // Default to data directory sibling of nbg_csr if not provided
                let signals_path = node_signals.clone().unwrap_or_else(|| {
                    nbg_csr
                        .parent()
                        .unwrap_or(Path::new("."))
                        .join("node_signals.bin")
                });

                // Build dynamic EbgModeConfig list
                let modes: Vec<crate::ebg::EbgModeConfig> = wa_parsed
                    .iter()
                    .zip(tr_parsed.iter())
                    .map(
                        |((name, idx, wa_path), (_, _, tr_path))| crate::ebg::EbgModeConfig {
                            mode_name: name.clone(),
                            mode_index: *idx,
                            way_attrs_path: wa_path.clone(),
                            turn_rules_path: tr_path.clone(),
                        },
                    )
                    .collect();

                let config = EbgConfig {
                    nbg_csr_path: nbg_csr.clone(),
                    nbg_geo_path: nbg_geo.clone(),
                    nbg_node_map_path: nbg_node_map.clone(),
                    node_signals_path: signals_path,
                    modes: modes.clone(),
                    outdir: outdir.clone(),
                };

                let result = build_ebg(config)?;

                // Run validation and generate lock file
                println!();
                let step4_mode_inputs: Vec<crate::validate::step4::Step4ModeInput> = modes
                    .iter()
                    .zip(tr_parsed.iter())
                    .map(
                        |(m, (_, _, tr_path))| crate::validate::step4::Step4ModeInput {
                            mode_name: m.mode_name.clone(),
                            mode_index: m.mode_index,
                            way_attrs_path: m.way_attrs_path.clone(),
                            turn_rules_path: tr_path.clone(),
                        },
                    )
                    .collect();
                let lock_file = validate_step4(
                    &result.nodes_path,
                    &result.csr_path,
                    &result.turn_table_path,
                    &nbg_csr,
                    &nbg_geo,
                    &nbg_node_map,
                    &step4_mode_inputs,
                    result.build_time_ms,
                )?;

                let lock_path = outdir.join("step4.lock.json");
                let lock_json = serde_json::to_string_pretty(&lock_file)?;
                std::fs::write(&lock_path, lock_json)?;

                println!();
                println!("✅ EBG validation complete!");
                println!("📋 Lock file: {}", lock_path.display());

                Ok(())
            }
            Commands::Step5Weights {
                ebg_nodes,
                ebg_csr,
                turn_table,
                nbg_geo,
                way_attrs,
                outdir,
            } => {
                // Parse mode=path pairs from CLI
                let wa_raw: Vec<(String, PathBuf)> = way_attrs
                    .iter()
                    .map(|s| {
                        let (mode, path) = s.split_once('=').ok_or_else(|| {
                            anyhow::anyhow!(
                                "Invalid --way-attrs format '{}': expected MODE=PATH",
                                s
                            )
                        })?;
                        Ok((mode.to_string(), PathBuf::from(path)))
                    })
                    .collect::<Result<Vec<_>>>()?;

                // Discover ALL modes from the step2 directory to get correct global indices.
                // The turn table (from step4) uses mode indices based on ALL modes sorted
                // alphabetically. Step5 MUST use the same indices even when processing a
                // subset of modes, otherwise the mode_mask bit check in the filtered EBG
                // will use the wrong bit.
                let step2_dir = wa_raw
                    .first()
                    .and_then(|(_, p)| p.parent())
                    .ok_or_else(|| {
                        anyhow::anyhow!("Cannot determine step2 directory from way-attrs paths")
                    })?;
                let all_modes = Mode::discover_from_dir(step2_dir);
                anyhow::ensure!(
                    !all_modes.is_empty(),
                    "No modes found in {}. Expected way_attrs.*.bin files.",
                    step2_dir.display()
                );

                // Build mode inputs with GLOBAL indices from discovery
                let mode_inputs: Vec<weights::Step5ModeInput> = wa_raw
                    .iter()
                    .map(|(name, path)| {
                        let global_idx = all_modes
                            .iter()
                            .find(|(n, _)| n == name)
                            .map(|(_, idx)| *idx)
                            .ok_or_else(|| {
                                anyhow::anyhow!(
                                    "Mode '{}' not found in discovered modes {:?}",
                                    name,
                                    all_modes
                                        .iter()
                                        .map(|(n, _)| n.as_str())
                                        .collect::<Vec<_>>()
                                )
                            })?;
                        Ok(weights::Step5ModeInput {
                            mode_name: name.clone(),
                            mode_index: global_idx,
                            way_attrs_path: path.clone(),
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                let result = weights::generate_weights(
                    &ebg_nodes,
                    &ebg_csr,
                    &turn_table,
                    &nbg_geo,
                    &mode_inputs,
                    &outdir,
                )?;

                // Run validation and generate lock file
                println!();
                let way_attrs_by_name: std::collections::HashMap<String, PathBuf> = wa_raw
                    .iter()
                    .map(|(name, path)| (name.clone(), path.clone()))
                    .collect();
                let lock_file = validate_step5(
                    &result,
                    &ebg_nodes,
                    &ebg_csr,
                    &turn_table,
                    &nbg_geo,
                    &way_attrs_by_name,
                )?;

                let lock_path = outdir.join("step5.lock.json");
                let lock_json = serde_json::to_string_pretty(&lock_file)?;
                std::fs::write(&lock_path, lock_json)?;

                println!();
                println!("✅ Step 5 weights validation complete!");
                println!("📋 Lock file: {}", lock_path.display());

                Ok(())
            }
            Commands::Step6Order {
                filtered_ebg,
                ebg_nodes,
                nbg_geo,
                mode,
                outdir,
                leaf_threshold,
                balance_eps,
            } => {
                // Parse mode — discover from filtered_ebg's parent (step5 dir)
                let mode_name = mode.to_lowercase();
                let step5_dir = filtered_ebg.parent().unwrap_or(Path::new("."));
                let mode = resolve_mode(&mode_name, step5_dir)?;

                let config = ordering::Step6Config {
                    filtered_ebg_path: filtered_ebg.clone(),
                    ebg_nodes_path: ebg_nodes,
                    nbg_geo_path: nbg_geo,
                    mode,
                    mode_name: mode_name.clone(),
                    outdir: outdir.clone(),
                    leaf_threshold,
                    balance_eps,
                };

                let result = ordering::generate_ordering(config)?;

                // Run validation and generate lock file
                println!();
                let lock_file = validate_step6(&result, &filtered_ebg)?;

                let mode_name = &result.mode_name;
                let lock_path = outdir.join(format!("step6.{}.lock.json", mode_name));
                let lock_json = serde_json::to_string_pretty(&lock_file)?;
                std::fs::write(&lock_path, lock_json)?;

                println!();
                println!("✅ Step 6 ordering complete for {} mode!", mode_name);
                println!("📋 Lock file: {}", lock_path.display());

                Ok(())
            }
            Commands::Step6Lifted {
                nbg_csr,
                nbg_geo,
                ebg_nodes,
                ebg_csr,
                filtered_ebg,
                mode,
                outdir,
                leaf_threshold,
            } => {
                // Parse mode — discover from filtered_ebg's parent (step5 dir)
                let mode_name_str = mode.to_lowercase();
                let step5_dir = filtered_ebg.parent().unwrap_or(Path::new("."));
                let mode = resolve_mode(&mode_name_str, step5_dir)?;

                let config = ordering_lifted::Step6LiftedConfig {
                    nbg_csr_path: nbg_csr,
                    nbg_geo_path: nbg_geo,
                    ebg_nodes_path: ebg_nodes,
                    ebg_csr_path: ebg_csr,
                    filtered_ebg_path: filtered_ebg.clone(),
                    mode,
                    mode_name: mode_name_str,
                    outdir: outdir.clone(),
                    leaf_threshold,
                };

                let result = ordering_lifted::generate_lifted_ordering(config)?;

                // Generate lock file (reuse step6 validation for ordering format)
                println!();
                let lock_file = validate_step6_lifted(&result, &filtered_ebg)?;

                let mode_name = &result.mode_name;
                let lock_path = outdir.join(format!("step6.lifted.{}.lock.json", mode_name));
                let lock_json = serde_json::to_string_pretty(&lock_file)?;
                std::fs::write(&lock_path, lock_json)?;

                println!();
                println!(
                    "✅ Step 6 (Lifted) ordering complete for {} mode!",
                    mode_name
                );
                println!("📋 Lock file: {}", lock_path.display());

                Ok(())
            }
            Commands::Step7Contract {
                filtered_ebg,
                order,
                weights,
                turns,
                mode,
                outdir,
            } => {
                // Parse mode — discover from filtered_ebg's parent (step5 dir)
                let mode_name_str = mode.to_lowercase();
                let step5_dir = filtered_ebg.parent().unwrap_or(Path::new("."));
                let mode = resolve_mode(&mode_name_str, step5_dir)?;

                let config = contraction::Step7Config {
                    filtered_ebg_path: filtered_ebg.clone(),
                    order_path: order.clone(),
                    weights_path: weights,
                    turns_path: turns,
                    mode,
                    mode_name: mode_name_str,
                    outdir: outdir.clone(),
                };

                let result = contraction::build_cch_topology(config)?;

                // Run validation and generate lock file
                println!();
                let lock_file = validate_step7(&result, &filtered_ebg, &order)?;

                let mode_name = &result.mode_name;
                let lock_path = outdir.join(format!("step7.{}.lock.json", mode_name));
                let lock_json = serde_json::to_string_pretty(&lock_file)?;
                std::fs::write(&lock_path, lock_json)?;

                println!();
                println!("✅ Step 7 CCH contraction complete for {} mode!", mode_name);
                println!("📋 Lock file: {}", lock_path.display());

                Ok(())
            }
            Commands::Step8Customize {
                cch_topo,
                filtered_ebg,
                order,
                weights,
                turns,
                ebg_nodes,
                mode,
                outdir,
                traffic,
                way_attrs,
                nbg_geo,
                skip_triangle_relax,
            } => {
                // Parse mode — discover from filtered_ebg's parent (step5 dir)
                let mode_name_str = mode.to_lowercase();
                let step5_dir = filtered_ebg.parent().unwrap_or(Path::new("."));
                let mode = resolve_mode(&mode_name_str, step5_dir)?;

                let traffic_cfg = match traffic {
                    Some(traffic_path) => {
                        let way_attrs_path = way_attrs.ok_or_else(|| {
                            anyhow::anyhow!("--traffic requires --way-attrs <PATH>")
                        })?;
                        let nbg_geo_path = nbg_geo.ok_or_else(|| {
                            anyhow::anyhow!("--traffic requires --nbg-geo <PATH>")
                        })?;
                        let profile = crate::traffic::TrafficProfile::load(&traffic_path)?;
                        // Validate base_model matches the mode we're customizing.
                        if profile.base_model != mode_name_str {
                            println!(
                                "⚠️  warning: traffic profile base_model='{}' but customizing mode='{}'. Proceeding.",
                                profile.base_model, mode_name_str
                            );
                        }
                        if skip_triangle_relax {
                            eprintln!(
                                "WARNING: --skip-triangle-relax enabled. The resulting weights \
                                 produce INCORRECT (over-estimated) routing durations and must \
                                 NOT be served to users. This flag is for bench experiments only."
                            );
                        }
                        Some(customization::TrafficCustomization {
                            profile,
                            way_attrs_path,
                            nbg_geo_path,
                            skip_triangle_relax,
                        })
                    }
                    None => None,
                };

                let config = customization::Step8Config {
                    cch_topo_path: cch_topo,
                    filtered_ebg_path: filtered_ebg,
                    order_path: order,
                    weights_path: weights,
                    turns_path: turns,
                    ebg_nodes_path: ebg_nodes,
                    mode,
                    mode_name: mode_name_str.clone(),
                    outdir: outdir.clone(),
                    traffic: traffic_cfg,
                };

                let traffic_variant = config.traffic.as_ref().map(|t| t.profile.name.clone());
                let result = customization::customize_cch(config)?;

                // Generate lock file
                let mode_name = &result.mode_name;
                let lock_basename = match &traffic_variant {
                    Some(v) => format!("step8.{}_{}.lock.json", mode_name, v),
                    None => format!("step8.{}.lock.json", mode_name),
                };
                let lock = serde_json::json!({
                    "mode": mode_name,
                    "traffic_variant": traffic_variant,
                    "output_path": result.output_path.display().to_string(),
                    "distance_output_path": result.distance_output_path.display().to_string(),
                    "n_up_edges": result.n_up_edges,
                    "n_down_edges": result.n_down_edges,
                    "customize_time_ms": result.customize_time_ms,
                    "created_at_utc": chrono::Utc::now().to_rfc3339(),
                });

                let lock_path = outdir.join(lock_basename);
                let lock_json = serde_json::to_string_pretty(&lock)?;
                std::fs::write(&lock_path, lock_json)?;

                println!();
                println!("✅ Step 8 CCH customization complete!");
                println!("📋 Lock file: {}", lock_path.display());

                Ok(())
            }
            Commands::TransitFetch { data_dir, realtime } => {
                // Load the transit config (uses default Belgium feeds if
                // no transit.toml is present, but still requires the
                // `transit/` directory to exist so operators opt in).
                let cfg_dir = data_dir.join("transit");
                std::fs::create_dir_all(&cfg_dir)
                    .with_context(|| format!("creating transit dir {}", cfg_dir.display()))?;
                let cfg = crate::transit::config::load(&data_dir)?.ok_or_else(|| {
                    anyhow::anyhow!(
                        "could not load transit config under {} (did `mkdir -p transit` fail?)",
                        data_dir.display()
                    )
                })?;
                println!(
                    "transit-fetch: {} feed(s) -> {}",
                    cfg.feeds.len(),
                    cfg.gtfs_dir().display()
                );
                for feed in &cfg.feeds {
                    println!("  - {} ({})", feed.id, feed.url);
                }

                let rt = tokio::runtime::Runtime::new()?;
                let reports = rt.block_on(crate::transit::feeds::fetch_all(&cfg, realtime))?;
                let mut ok = 0usize;
                let mut fail = 0usize;
                for r in &reports {
                    println!("  {}", crate::transit::feeds::format_report(r));
                    match r.static_outcome {
                        crate::transit::feeds::FeedFetchOutcome::Failed { .. } => fail += 1,
                        _ => ok += 1,
                    }
                }
                println!("transit-fetch: {ok} ok, {fail} failed");
                if fail > 0 && ok == 0 {
                    anyhow::bail!("every feed failed to download");
                }
                Ok(())
            }
            Commands::Pack {
                data_dir,
                out,
                step_prefix,
                region,
            } => crate::pack::pack(&data_dir, &out, step_prefix.as_deref(), region.as_deref()),
            Commands::Inspect {
                path,
                verify,
                verify_full,
            } => crate::pack::inspect(&path, verify, verify_full),
            Commands::Unpack { path, out } => crate::pack::unpack(&path, &out),
            Commands::TopologyDiff { path, modes } => {
                crate::pack::topology_diff(&path, modes.as_deref())
            }
            Commands::Serve {
                data_dir,
                data,
                port,
                grpc_port,
                transport,
                modes,
                regions,
                log_format,
                rss_checkpoints,
                eager_verify,
                warmup_on_boot,
                overlay,
            } => {
                // Initialize structured logging for the serve command
                server::init_tracing(&log_format);

                // Either CLI flag OR env var BUTTERFLY_RSS_CHECKPOINTS=1
                // turns on the checkpoint instrumentation.
                let rss_checkpoints = rss_checkpoints
                    || std::env::var("BUTTERFLY_RSS_CHECKPOINTS")
                        .map(|v| matches!(v.as_str(), "1" | "true" | "yes" | "on"))
                        .unwrap_or(false);
                if rss_checkpoints {
                    crate::server::rss::set_enabled(true);
                    crate::server::rss::checkpoint("startup");
                }

                let transport_mode = server::Transport::parse(&transport)?;
                let mode_filter = modes.map(|s| {
                    s.split(',')
                        .map(|m| m.trim().to_lowercase())
                        .filter(|m| !m.is_empty())
                        .collect::<Vec<String>>()
                });
                let region_filter: Option<Vec<String>> = match regions {
                    Some(s) => {
                        let parts: Vec<String> = s
                            .split(',')
                            .map(|r| r.trim())
                            .filter(|r| !r.is_empty())
                            .map(crate::pack::normalize_region_id)
                            .collect::<Result<Vec<_>, _>>()?;
                        if parts.is_empty() { None } else { Some(parts) }
                    }
                    None => None,
                };

                let source_holder: PathBuf;
                let source = match (data_dir, data) {
                    (Some(dir), None) => {
                        source_holder = dir;
                        server::DataSource::Directory(&source_holder)
                    }
                    (None, Some(file)) => {
                        source_holder = file;
                        server::DataSource::Container(&source_holder)
                    }
                    (Some(_), Some(_)) => {
                        anyhow::bail!("--data-dir and --data are mutually exclusive")
                    }
                    (None, None) => {
                        anyhow::bail!("one of --data-dir or --data is required")
                    }
                };

                let load_options = crate::server::state::LoadOptions {
                    eager_verify,
                    warmup_on_boot,
                };

                // Create tokio runtime
                let rt = tokio::runtime::Runtime::new()?;
                rt.block_on(server::serve(
                    source,
                    port,
                    grpc_port,
                    transport_mode,
                    mode_filter.as_deref(),
                    region_filter.as_deref(),
                    &load_options,
                    overlay.as_deref(),
                ))?;
                Ok(())
            }
            Commands::ExtractBorders { regions, out } => run_extract_borders(&regions, &out),
            Commands::BuildOverlay {
                regions,
                modes,
                out,
            } => run_build_overlay(&regions, modes.as_deref(), &out),
            Commands::ValidateCch {
                cch_topo,
                cch_weights,
                order,
                mode,
                n_pairs,
                seed,
                failures_file,
            } => {
                // Parse mode
                let mode_name = mode.to_lowercase();

                let (result, failures) = crate::validate::validate_cch_correctness(
                    &cch_topo,
                    &cch_weights,
                    &order,
                    n_pairs,
                    seed,
                    &mode_name,
                )?;

                // Write failures to file if requested
                if let Some(path) = failures_file {
                    use std::io::Write;
                    let mut f = std::fs::File::create(&path)?;
                    writeln!(f, "src,dst,bidi_cost,baseline_cost,diff")?;
                    for failure in &failures {
                        let diff = (failure.bidi_cost as i64) - (failure.baseline_cost as i64);
                        writeln!(
                            f,
                            "{},{},{},{},{}",
                            failure.src,
                            failure.dst,
                            failure.bidi_cost,
                            failure.baseline_cost,
                            diff
                        )?;
                    }
                    println!("\nFailures written to: {}", path.display());
                }

                if result.mismatches > 0 {
                    anyhow::bail!("Validation failed with {} mismatches", result.mismatches);
                }

                Ok(())
            }
            Commands::RegressionCch {
                cch_topo,
                cch_weights,
                order,
                mode,
            } => {
                let mode_name = mode.to_lowercase();

                let results = crate::validate::run_regression_tests(
                    &cch_topo,
                    &cch_weights,
                    &order,
                    &mode_name,
                )?;

                let failed_count = results.iter().filter(|r| !r.passed).count();
                if failed_count > 0 {
                    anyhow::bail!("Regression tests failed: {} failures", failed_count);
                }

                Ok(())
            }
            Commands::ValidateInvariants {
                cch_topo,
                cch_weights,
                order,
                mode,
            } => {
                let mode_name = mode.to_lowercase();

                let result = crate::validate::validate_invariants(
                    &cch_topo,
                    &cch_weights,
                    &order,
                    &mode_name,
                )?;

                if !result.passed {
                    anyhow::bail!(
                        "Invariant validation failed with {} errors",
                        result.errors.len()
                    );
                }

                Ok(())
            }
            Commands::RangeCch {
                cch_topo,
                cch_weights,
                order,
                origin_node,
                threshold_ms,
                mode,
            } => {
                let mode_name = mode.to_lowercase();
                let data_dir = cch_topo.parent().unwrap_or(Path::new("."));
                let mode = resolve_mode(&mode_name, data_dir)?;

                let result = crate::range::run_range_query(
                    &cch_topo,
                    &cch_weights,
                    &order,
                    origin_node,
                    threshold_ms,
                    mode,
                )?;

                // Success if no errors in verification
                let engine =
                    crate::range::RangeEngine::load(&cch_topo, &cch_weights, &order, mode)?;
                let errors = engine.verify(&result, origin_node, threshold_ms);
                if !errors.is_empty() {
                    anyhow::bail!(
                        "Range query verification failed with {} errors",
                        errors.len()
                    );
                }

                Ok(())
            }
            Commands::ValidateRange {
                cch_topo,
                cch_weights,
                order,
                origin_node,
                mode,
            } => {
                let mode_name = mode.to_lowercase();
                let data_dir = cch_topo.parent().unwrap_or(Path::new("."));
                let mode = resolve_mode(&mode_name, data_dir)?;

                crate::range::run_range_validation(
                    &cch_topo,
                    &cch_weights,
                    &order,
                    origin_node,
                    mode,
                )?;

                Ok(())
            }
            Commands::PhastRange {
                cch_topo,
                cch_weights,
                order,
                origin_node,
                threshold_ms,
                mode,
            } => {
                let mode_name = mode.to_lowercase();
                let data_dir = cch_topo.parent().unwrap_or(Path::new("."));
                let mode = resolve_mode(&mode_name, data_dir)?;

                crate::range::run_phast_query(
                    &cch_topo,
                    &cch_weights,
                    &order,
                    origin_node,
                    threshold_ms,
                    mode,
                )?;

                Ok(())
            }
            Commands::ValidatePhast {
                cch_topo,
                cch_weights,
                order,
                origin_node,
                threshold_ms,
                mode,
            } => {
                let mode_name = mode.to_lowercase();
                let data_dir = cch_topo.parent().unwrap_or(Path::new("."));
                let mode = resolve_mode(&mode_name, data_dir)?;

                crate::range::validate_phast(
                    &cch_topo,
                    &cch_weights,
                    &order,
                    origin_node,
                    threshold_ms,
                    mode,
                )?;

                Ok(())
            }
            Commands::ValidateBlockGated {
                cch_topo,
                cch_weights,
                order,
                origins,
                thresholds,
            } => {
                // Parse origins
                let origins: Vec<u32> = origins
                    .split(',')
                    .filter_map(|s| s.trim().parse().ok())
                    .collect();

                // Parse thresholds
                let thresholds: Vec<u32> = thresholds
                    .split(',')
                    .filter_map(|s| s.trim().parse().ok())
                    .collect();

                crate::range::validate_block_gated_phast(
                    &cch_topo,
                    &cch_weights,
                    &order,
                    &origins,
                    &thresholds,
                )?;

                Ok(())
            }
            Commands::ExtractFrontier {
                cch_topo,
                cch_weights,
                order,
                filtered_ebg,
                ebg_nodes,
                nbg_geo,
                base_weights,
                origin_node,
                threshold_ms,
                mode,
                geojson_out,
            } => {
                // Accept any mode name — validation happens when loading data files
                let mode_name = mode.to_lowercase();

                // First run PHAST to get distances
                println!("Running PHAST to compute distances...");
                let phast_engine =
                    crate::range::PhastEngine::load(&cch_topo, &cch_weights, &order)?;
                let phast_result = phast_engine.query_bounded(origin_node, threshold_ms);
                println!(
                    "  ✓ PHAST complete: {} reachable nodes in {} ms",
                    phast_result.n_reachable, phast_result.stats.total_time_ms
                );

                // Then extract frontier on base graph
                let cut_points = crate::range::run_frontier_extraction(
                    &filtered_ebg,
                    &ebg_nodes,
                    &nbg_geo,
                    &base_weights,
                    &phast_result.dist,
                    threshold_ms,
                    &mode_name,
                )?;

                println!(
                    "\n✅ Frontier extraction complete: {} cut points",
                    cut_points.len()
                );

                // Export to GeoJSON if requested
                if let Some(geojson_path) = geojson_out {
                    crate::range::frontier::export_geojson(&cut_points, &geojson_path)?;
                    println!("  Exported to: {}", geojson_path.display());
                }

                Ok(())
            }
            Commands::Isochrone {
                cch_topo,
                cch_weights,
                order,
                filtered_ebg,
                ebg_nodes,
                nbg_geo,
                base_weights,
                origin_node,
                threshold_ms,
                mode,
                output,
                cell_size,
            } => {
                let mode_name = mode.to_lowercase();
                let data_dir = cch_topo.parent().unwrap_or(Path::new("."));
                let _mode = resolve_mode(&mode_name, data_dir)?;

                println!("\n🗺️  Isochrone Generation ({} mode)", mode_name);
                println!("  Origin: node {}", origin_node);
                println!(
                    "  Threshold: {} ms ({:.1} min)",
                    threshold_ms,
                    threshold_ms as f64 / 60_000.0
                );

                // Step 1: PHAST distances
                println!("\n[1/4] Running PHAST...");
                let phast_engine =
                    crate::range::PhastEngine::load(&cch_topo, &cch_weights, &order)?;
                let phast_result = phast_engine.query_bounded(origin_node, threshold_ms);
                println!(
                    "  ✓ {} reachable nodes in {} ms",
                    phast_result.n_reachable, phast_result.stats.total_time_ms
                );

                // Step 2: Extract reachable road segments
                println!("\n[2/4] Extracting reachable road segments...");
                let extractor = crate::range::FrontierExtractor::load(
                    &filtered_ebg,
                    &ebg_nodes,
                    &nbg_geo,
                    &base_weights,
                )?;
                let segments =
                    extractor.extract_reachable_segments(&phast_result.dist, threshold_ms);
                println!("  ✓ {} reachable road segments", segments.len());

                // Step 3: Generate contour (sparse tile rasterization + boundary tracing)
                println!("\n[3/4] Generating contour...");
                let config = if let Some(size) = cell_size {
                    crate::range::SparseContourConfig::custom(size, size)
                } else {
                    crate::range::SparseContourConfig::for_mode_name(&mode_name)
                };

                println!(
                    "  Sparse: {}m cells, {}m simplify, {} dilation, {} erosion",
                    config.cell_size_m,
                    config.simplify_tolerance_m,
                    config.dilation_rounds,
                    config.erosion_rounds
                );

                let sparse_result = crate::range::generate_sparse_contour(&segments, &config)?;
                let active_tiles = sparse_result.stats.active_tiles_after_morphology;

                let contour = crate::range::ContourResult {
                    outer_ring: sparse_result.outer_ring,
                    holes: sparse_result.holes,
                    stats: crate::range::ContourStats {
                        input_segments: sparse_result.stats.input_segments,
                        grid_cols: 0,
                        grid_rows: 0,
                        filled_cells: sparse_result.stats.total_cells_set,
                        contour_vertices_before_simplify: sparse_result
                            .stats
                            .contour_vertices_before_simplify,
                        contour_vertices_after_simplify: sparse_result
                            .stats
                            .contour_vertices_after_simplify,
                        elapsed_ms: (sparse_result.stats.stamp_time_us
                            + sparse_result.stats.morphology_time_us
                            + sparse_result.stats.contour_time_us
                            + sparse_result.stats.simplify_time_us)
                            / 1000,
                    },
                };

                println!(
                    "  ✓ {} tiles, {} filled cells → {} vertices (before simplify: {})",
                    active_tiles,
                    contour.stats.filled_cells,
                    contour.stats.contour_vertices_after_simplify,
                    contour.stats.contour_vertices_before_simplify
                );

                // Step 4: Export
                println!("\n[4/4] Exporting GeoJSON...");
                crate::range::export_contour_geojson(&contour, &output)?;

                let file_size = std::fs::metadata(&output)?.len();
                println!(
                    "  ✓ Saved to: {} ({:.1} KB)",
                    output.display(),
                    file_size as f64 / 1024.0
                );

                println!("\n=== ISOCHRONE COMPLETE ===");
                println!(
                    "  Total vertices: {}",
                    contour.stats.contour_vertices_after_simplify
                );
                println!("  Processing time: {} ms", contour.stats.elapsed_ms);

                Ok(())
            }
            Commands::Step6Hybrid {
                hybrid_state,
                nbg_geo,
                mode,
                outdir,
                leaf_threshold,
                balance_eps,
                graph_partition,
                densifier_threshold,
            } => {
                let mode_name_str = mode.to_lowercase();
                let hybrid_dir = hybrid_state.parent().unwrap_or(Path::new("."));
                let mode_enum = resolve_mode(&mode_name_str, hybrid_dir)?;

                let config = ordering::Step6HybridConfig {
                    hybrid_state_path: hybrid_state.clone(),
                    nbg_geo_path: nbg_geo,
                    mode: mode_enum,
                    mode_name: mode_name_str,
                    outdir: outdir.clone(),
                    leaf_threshold,
                    balance_eps,
                    use_graph_partition: graph_partition,
                    densifier_threshold,
                };

                let result = ordering::generate_ordering_hybrid(config)?;

                // Generate lock file
                let mode_name = &result.mode_name;
                let lock = serde_json::json!({
                    "mode": mode_name,
                    "graph_type": "hybrid",
                    "order_path": result.order_path.display().to_string(),
                    "n_nodes": result.n_nodes,
                    "n_components": result.n_components,
                    "tree_depth": result.tree_depth,
                    "build_time_ms": result.build_time_ms,
                    "created_at_utc": chrono::Utc::now().to_rfc3339(),
                });

                let lock_path = outdir.join(format!("step6.hybrid.{}.lock.json", mode_name));
                let lock_json = serde_json::to_string_pretty(&lock)?;
                std::fs::write(&lock_path, lock_json)?;

                println!();
                println!("✅ Step 6 (Hybrid) ordering complete!");
                println!("📋 Lock file: {}", lock_path.display());

                Ok(())
            }
            Commands::Step7Hybrid {
                hybrid_state,
                order,
                mode,
                outdir,
            } => {
                let mode_name_str = mode.to_lowercase();
                let hybrid_dir = hybrid_state.parent().unwrap_or(Path::new("."));
                let mode_enum = resolve_mode(&mode_name_str, hybrid_dir)?;

                let config = contraction::Step7HybridConfig {
                    hybrid_state_path: hybrid_state.clone(),
                    order_path: order.clone(),
                    mode: mode_enum,
                    mode_name: mode_name_str,
                    outdir: outdir.clone(),
                };

                let result = contraction::build_cch_topology_hybrid(config)?;

                // Generate lock file
                let mode_name = &result.mode_name;
                let lock = serde_json::json!({
                    "mode": mode_name,
                    "graph_type": "hybrid",
                    "topo_path": result.topo_path.display().to_string(),
                    "n_nodes": result.n_nodes,
                    "n_original_arcs": result.n_original_arcs,
                    "n_shortcuts": result.n_shortcuts,
                    "n_up_edges": result.n_up_edges,
                    "n_down_edges": result.n_down_edges,
                    "build_time_ms": result.build_time_ms,
                    "created_at_utc": chrono::Utc::now().to_rfc3339(),
                });

                let lock_path = outdir.join(format!("step7.hybrid.{}.lock.json", mode_name));
                let lock_json = serde_json::to_string_pretty(&lock)?;
                std::fs::write(&lock_path, lock_json)?;

                println!();
                println!("✅ Step 7 (Hybrid) CCH contraction complete!");
                println!("📋 Lock file: {}", lock_path.display());

                Ok(())
            }
            Commands::BuildNbgCh {
                nbg_csr,
                nbg_geo,
                leaf_threshold,
                balance_eps,
                benchmark,
                validate,
                validate_tests,
            } => {
                use crate::formats::{NbgCsrFile, NbgGeoFile};
                use crate::nbg_ch::{
                    NbgBucketM2M, compute_nbg_ordering, contract_nbg, validate_matrix,
                    validate_nbg_ch,
                };

                println!("\n=== BUILD NODE-BASED CH ===\n");

                // Load NBG CSR
                println!("[1/3] Loading NBG CSR...");
                let nbg_csr_data = NbgCsrFile::read(&nbg_csr)?;
                println!(
                    "  {} nodes, {} edges (undirected)",
                    nbg_csr_data.n_nodes, nbg_csr_data.n_edges_und
                );

                // Load NBG Geo
                println!("[2/3] Loading NBG Geo...");
                let nbg_geo_data = NbgGeoFile::read(&nbg_geo)?;
                println!("  {} edges", nbg_geo_data.n_edges_und);

                // Compute ordering
                println!("\n[Ordering] Computing nested dissection ordering...");
                let start_order = std::time::Instant::now();
                let ordering = compute_nbg_ordering(
                    &nbg_csr_data,
                    &nbg_geo_data,
                    leaf_threshold,
                    balance_eps,
                )?;
                let order_time = start_order.elapsed().as_millis();
                println!(
                    "  Ordering complete: {} nodes, {} components, max depth {}",
                    ordering.n_nodes, ordering.n_components, ordering.max_depth
                );
                println!("  Ordering time: {} ms", order_time);

                // Contract with witness search
                println!("\n[Contraction] Contracting NBG with witness search...");
                let start_contract = std::time::Instant::now();
                let topo = contract_nbg(&nbg_csr_data, &nbg_geo_data, &ordering)?;
                let contract_time = start_contract.elapsed().as_millis();

                println!("\n=== NBG CH COMPLETE ===");
                println!("  Nodes:      {}", topo.n_nodes);
                println!("  UP edges:   {}", topo.n_up_edges);
                println!("  DOWN edges: {}", topo.n_down_edges);
                println!("  Shortcuts:  {}", topo.n_shortcuts);
                println!("  Ordering time:    {} ms", order_time);
                println!("  Contraction time: {} ms", contract_time);

                // Compare with EBG CCH
                println!("\n=== COMPARISON WITH EBG CCH ===");
                println!("  EBG CCH: ~5M nodes, ~30M shortcuts (typical)");
                println!(
                    "  NBG CH:  {}M nodes, {}M shortcuts",
                    topo.n_nodes as f64 / 1_000_000.0,
                    topo.n_shortcuts as f64 / 1_000_000.0
                );
                println!(
                    "  Expected speedup: ~{:.1}x fewer nodes to search",
                    5_000_000.0 / topo.n_nodes as f64
                );

                // Run benchmark if requested
                if benchmark {
                    println!("\n=== MATRIX BENCHMARK ===\n");

                    let engine = NbgBucketM2M::new(&topo);

                    // Generate random source/target pairs
                    use rand::prelude::*;
                    let mut rng = rand::rng();
                    let n_nodes = topo.n_nodes;

                    for size in [10, 25, 50, 100] {
                        let sources: Vec<u32> =
                            (0..size).map(|_| rng.random_range(0..n_nodes)).collect();
                        let targets: Vec<u32> =
                            (0..size).map(|_| rng.random_range(0..n_nodes)).collect();

                        // Warmup
                        let _ = engine.compute(&sources, &targets);

                        // Timed runs
                        let n_runs = 5;
                        let mut times = Vec::new();

                        for _ in 0..n_runs {
                            let start = std::time::Instant::now();
                            let (_matrix, _stats) = engine.compute(&sources, &targets);
                            let elapsed = start.elapsed().as_millis() as u64;
                            times.push(elapsed);
                        }

                        let avg = times.iter().sum::<u64>() / n_runs;
                        let min = *times.iter().min().unwrap();
                        let max = *times.iter().max().unwrap();

                        println!(
                            "  {}×{}: avg {}ms, min {}ms, max {}ms",
                            size, size, avg, min, max
                        );
                    }

                    println!("\n  Compare with OSRM (sequential CH):");
                    println!("    10×10:   ~4ms");
                    println!("    25×25:   ~9ms");
                    println!("    50×50:   ~19ms");
                    println!("    100×100: ~35ms");
                }

                // Run validation if requested
                if validate {
                    println!("\n=== VALIDATION AGAINST DIJKSTRA ===\n");

                    // Validate single queries
                    let result = validate_nbg_ch(
                        &nbg_csr_data,
                        &nbg_geo_data,
                        &topo,
                        validate_tests,
                        42, // Fixed seed for reproducibility
                    );
                    result.print();

                    if !result.is_valid() {
                        anyhow::bail!("NBG CH validation FAILED! Results are incorrect.");
                    }

                    // Also validate a small matrix to catch matrix-specific bugs
                    println!("\n--- Matrix Validation ---");
                    let matrix_result = validate_matrix(
                        &nbg_csr_data,
                        &nbg_geo_data,
                        &topo,
                        50,  // 50x50 matrix = 2500 tests
                        123, // Different seed
                    );
                    matrix_result.print();

                    if !matrix_result.is_valid() {
                        anyhow::bail!("NBG CH matrix validation FAILED! Results are incorrect.");
                    }
                }

                Ok(())
            }
            Commands::Step8Hybrid {
                cch_topo,
                hybrid_state,
                mode,
                outdir,
            } => {
                let mode_name_str = mode.to_lowercase();
                let hybrid_dir = hybrid_state.parent().unwrap_or(Path::new("."));
                let mode_enum = resolve_mode(&mode_name_str, hybrid_dir)?;

                let config = customization::Step8HybridConfig {
                    cch_topo_path: cch_topo,
                    hybrid_state_path: hybrid_state,
                    mode: mode_enum,
                    mode_name: mode_name_str,
                    outdir: outdir.clone(),
                };

                let result = customization::customize_cch_hybrid(config)?;

                // Generate lock file
                let mode_name = &result.mode_name;
                let lock = serde_json::json!({
                    "mode": mode_name,
                    "graph_type": "hybrid",
                    "output_path": result.output_path.display().to_string(),
                    "n_up_edges": result.n_up_edges,
                    "n_down_edges": result.n_down_edges,
                    "customize_time_ms": result.customize_time_ms,
                    "created_at_utc": chrono::Utc::now().to_rfc3339(),
                });

                let lock_path = outdir.join(format!("step8.hybrid.{}.lock.json", mode_name));
                let lock_json = serde_json::to_string_pretty(&lock)?;
                std::fs::write(&lock_path, lock_json)?;

                println!();
                println!("✅ Step 8 (Hybrid) CCH customization complete!");
                println!("📋 Lock file: {}", lock_path.display());

                Ok(())
            }
        }
    }
}
