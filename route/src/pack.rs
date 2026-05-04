//! Pack / inspect for the unified `butterfly.dat` container.
//!
//! Reads a `data_dir/step{1..8}/` tree and emits a single file with
//! every artefact + per-section CRCs + a directory at the tail. See
//! `formats::butterfly_dat` for the exact byte layout.
//!
//! Decisions:
//! * Section names use a stable string scheme so the loader does not
//!   have to know the file system layout. `step1/nodes.sa`,
//!   `step5/filtered.<mode>.ebg`, `step8/cch.w.<mode>.u32`, etc.
//! * The pack walks the source tree by *globbing* per-step
//!   directories, so newly-added files (e.g. traffic-customised
//!   weight files from #84) are picked up automatically as long as
//!   they follow the `cch.w.*.u32` / `cch.d.*.u32` filename pattern
//!   in `step8/`.
//! * Optional inputs (e.g. `node_signals.bin`, mode-mask bitsets) are
//!   skipped silently if absent.

use anyhow::{Context, Result};
use std::path::{Path, PathBuf};

use crate::formats::butterfly_dat::{Container, ContainerWriter, SectionKind};
use crate::formats::edge_geom::{
    EdgeGeomOffsets, EdgeGeomOffsetsFile, EdgeGeomPoints, EdgeGeomPointsFile,
};
use crate::formats::mode_index::{ModeIndex, ModeIndexFile, ModeIndexKind};
use crate::formats::snap_index::{SnapGridFile, SnapMaskFile, SnapPointsFile};
use crate::formats::{
    CchTopoFile, CchWeightsFile, EbgNodesFile, FilteredEbgFile, NbgGeoFile, OrderEbgFile,
};
use crate::matrix::bucket_ch::{
    DownAdjFlat, DownAdjFlatFile, DownReverseAdjFlat, DownReverseAdjFlatFile, UpAdjFlat,
    UpAdjFlatFile,
};
use crate::server::snap_index::{DEFAULT_CELL_LOG2, SnapBuilderMode, build_snap_index};
use std::borrow::Cow;

/// Section name for the JSON manifest that lists modes + bundle ids.
/// Lives at the top of the `shared/` namespace so legacy tooling can
/// ignore it (it has the synthetic `Unknown` kind on disk).
const MANIFEST_NAME: &str = "shared/manifest.json";

/// Resolve a step subdirectory the same way the server does:
/// exact match first, then any directory whose name starts with
/// `step{N}` (alphabetically lowest).
fn find_step_dir(data_dir: &Path, step: &str) -> Result<PathBuf> {
    let exact = data_dir.join(step);
    if exact.exists() {
        return Ok(exact);
    }
    let mut matches: Vec<PathBuf> = Vec::new();
    for entry in
        std::fs::read_dir(data_dir).with_context(|| format!("reading {}", data_dir.display()))?
    {
        let entry = entry?;
        let name = entry.file_name();
        let s = name.to_string_lossy();
        if s.starts_with(step) && entry.file_type()?.is_dir() {
            matches.push(entry.path());
        }
    }
    matches.sort();
    matches
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("could not find {} dir under {}", step, data_dir.display()))
}

/// Append a section if the file exists; silently skip otherwise.
/// Logs the size on append so the operator sees what was packed.
fn maybe_append(
    w: &mut ContainerWriter,
    kind: SectionKind,
    name: &str,
    path: &Path,
) -> Result<bool> {
    if !path.exists() {
        return Ok(false);
    }
    let size = std::fs::metadata(path)?.len();
    println!(
        "  + [{:>5} MiB] {:<28} <- {}",
        size / (1024 * 1024),
        name,
        path.display()
    );
    w.append_file(kind, name, path)
        .with_context(|| format!("packing {} from {}", name, path.display()))?;
    Ok(true)
}

/// Append a section synthesised in memory (e.g. a packed flat). Logs
/// the size and the section name so the operator sees what was packed.
fn append_encoded(
    w: &mut ContainerWriter,
    kind: SectionKind,
    name: &str,
    bytes: Vec<u8>,
) -> Result<()> {
    println!(
        "  + [{:>5} MiB] {:<28} <- (built in pack)",
        bytes.len() / (1024 * 1024),
        name,
    );
    w.append_bytes(kind, name, &bytes)
        .with_context(|| format!("packing synthesised section {}", name))
}

/// Glob a directory for files matching `prefix.*.suffix`. Returns the
/// embedded mode token together with the absolute path. Sorted by
/// mode name for determinism.
fn glob_per_mode(dir: &Path, prefix: &str, suffix: &str) -> Result<Vec<(String, PathBuf)>> {
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    let prefix = format!("{}.", prefix);
    for entry in std::fs::read_dir(dir).with_context(|| format!("reading {}", dir.display()))? {
        let entry = entry?;
        let n = entry.file_name();
        let s = n.to_string_lossy();
        if !s.starts_with(&prefix) || !s.ends_with(suffix) {
            continue;
        }
        let mode = &s[prefix.len()..s.len() - suffix.len()];
        if mode.is_empty() {
            continue;
        }
        out.push((mode.to_string(), entry.path()));
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(out)
}

/// Implementation of the `pack` subcommand.
pub fn pack(
    data_dir: &Path,
    out: &Path,
    step_prefix: Option<&str>,
    region: Option<&str>,
) -> Result<()> {
    let region_id = normalize_region_id(region.unwrap_or(DEFAULT_REGION_ID))?;
    println!(
        "packing {} → {} (region={})",
        data_dir.display(),
        out.display(),
        region_id
    );

    let step1 = find_step_dir(data_dir, step_prefix.unwrap_or("step1"))?;
    let step2 = find_step_dir(data_dir, step_prefix.unwrap_or("step2"))?;
    let step3 = find_step_dir(data_dir, step_prefix.unwrap_or("step3"))?;
    let step4 = find_step_dir(data_dir, step_prefix.unwrap_or("step4"))?;
    let step5 = find_step_dir(data_dir, step_prefix.unwrap_or("step5"))?;
    let step6 = find_step_dir(data_dir, step_prefix.unwrap_or("step6"))?;
    let step7 = find_step_dir(data_dir, step_prefix.unwrap_or("step7"))?;
    let step8 = find_step_dir(data_dir, step_prefix.unwrap_or("step8"))?;

    if let Some(parent) = out.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }
    let mut w = ContainerWriter::create(out)?;

    // ---- Shared global tables (mode-agnostic) -----------------------
    // Step 1 ingest output.
    maybe_append(
        &mut w,
        SectionKind::NodesSa,
        "shared/step1.nodes.sa",
        &step1.join("nodes.sa"),
    )?;
    maybe_append(
        &mut w,
        SectionKind::NodesSi,
        "shared/step1.nodes.si",
        &step1.join("nodes.si"),
    )?;
    maybe_append(
        &mut w,
        SectionKind::WaysRaw,
        "shared/step1.ways.raw",
        &step1.join("ways.raw"),
    )?;
    maybe_append(
        &mut w,
        SectionKind::RelationsRaw,
        "shared/step1.relations.raw",
        &step1.join("relations.raw"),
    )?;
    maybe_append(
        &mut w,
        SectionKind::NodeSignals,
        "shared/step1.node_signals.bin",
        &step1.join("node_signals.bin"),
    )?;
    // NBG (build-time intermediate, but the geo + node_map are read at
    // server startup — keep them in `shared/`).
    maybe_append(
        &mut w,
        SectionKind::NbgCsr,
        "shared/nbg.csr",
        &step3.join("nbg.csr"),
    )?;
    maybe_append(
        &mut w,
        SectionKind::NbgGeo,
        "shared/nbg.geo",
        &step3.join("nbg.geo"),
    )?;
    maybe_append(
        &mut w,
        SectionKind::NbgNodeMap,
        "shared/nbg.node_map",
        &step3.join("nbg.node_map"),
    )?;
    // EBG (mode-agnostic).
    maybe_append(
        &mut w,
        SectionKind::EbgNodes,
        "shared/ebg.nodes",
        &step4.join("ebg.nodes"),
    )?;
    maybe_append(
        &mut w,
        SectionKind::EbgCsr,
        "shared/ebg.csr",
        &step4.join("ebg.csr"),
    )?;
    maybe_append(
        &mut w,
        SectionKind::EbgTurnTable,
        "shared/ebg.turn_table",
        &step4.join("ebg.turn_table"),
    )?;

    // ---- Per-mode bundles -------------------------------------------
    // Modes are discovered from `step5/w.<mode>.u32` to match the
    // server's `discover_modes()` rule. We keep step2 way_attrs +
    // turn_rules under the per-mode bundle: they are consumed mode-
    // by-mode at server startup (e.g. exclude flags for `car`).
    let mut modes: Vec<String> = glob_per_mode(&step5, "w", ".u32")?
        .into_iter()
        .map(|(m, _)| m)
        .collect();
    modes.sort();
    modes.dedup();

    for mode in &modes {
        // step2 attrs/rules live with the mode they belong to.
        let way_attrs = step2.join(format!("way_attrs.{}.bin", mode));
        maybe_append(
            &mut w,
            SectionKind::WayAttrs,
            &format!("mode/{}/way_attrs", mode),
            &way_attrs,
        )?;
        let turn_rules = step2.join(format!("turn_rules.{}.bin", mode));
        maybe_append(
            &mut w,
            SectionKind::TurnRules,
            &format!("mode/{}/turn_rules", mode),
            &turn_rules,
        )?;
        // step5: filtered EBG, weights, mask.
        let filtered = step5.join(format!("filtered.{}.ebg", mode));
        maybe_append(
            &mut w,
            SectionKind::FilteredEbg,
            &format!("mode/{}/filtered_ebg", mode),
            &filtered,
        )?;
        let weights_time = step5.join(format!("w.{}.u32", mode));
        maybe_append(
            &mut w,
            SectionKind::NodeWeightsTime,
            &format!("mode/{}/node_weights.time", mode),
            &weights_time,
        )?;
        let weights_turn = step5.join(format!("t.{}.u32", mode));
        maybe_append(
            &mut w,
            SectionKind::NodeWeightsTurn,
            &format!("mode/{}/node_weights.turn", mode),
            &weights_turn,
        )?;
        let mask = step5.join(format!("mask.{}.bitset", mode));
        maybe_append(
            &mut w,
            SectionKind::ModeMask,
            &format!("mode/{}/mask", mode),
            &mask,
        )?;
        // step6 ordering. Lifted variants are intentionally skipped.
        let order = step6.join(format!("order.{}.ebg", mode));
        maybe_append(
            &mut w,
            SectionKind::OrderEbg,
            &format!("mode/{}/order", mode),
            &order,
        )?;
        // ---- Server-only mapping sections (#153) -------------------
        // `orig_to_rank` and `filtered_to_original` are derived from
        // the per-step `filtered.<mode>.ebg` + `order.<mode>.ebg` files
        // we just packed. They let the server drop both legacy structs
        // from RSS at boot time.
        //
        // We only build them when both sources exist on disk. If they
        // exist but parse fails we hard-fail the pack instead of
        // silently shipping a "new" container that drops the server
        // onto the legacy fallback — the regression would only surface
        // at server boot. If the files are absent (older build), we
        // skip the sections; the fallback path in `state.rs` keeps old
        // containers working.
        let filtered_path = step5.join(format!("filtered.{}.ebg", mode));
        let order_path = step6.join(format!("order.{}.ebg", mode));
        if filtered_path.exists() && order_path.exists() {
            let filtered_ebg = FilteredEbgFile::read(&filtered_path).with_context(|| {
                format!(
                    "parsing filtered.{mode}.ebg for #153 mapping sections (file present but unreadable)"
                )
            })?;
            let order_data = OrderEbgFile::read(&order_path).with_context(|| {
                format!(
                    "parsing order.{mode}.ebg for #153 mapping sections (file present but unreadable)"
                )
            })?;
            {
                let n_orig = filtered_ebg.n_original_nodes as usize;
                let n_filt = filtered_ebg.n_filtered_nodes as usize;
                anyhow::ensure!(
                    order_data.n_nodes as usize == n_filt,
                    "order.{0}.ebg n_nodes ({1}) != filtered.{0}.ebg n_filtered_nodes ({2})",
                    mode,
                    order_data.n_nodes,
                    n_filt
                );

                // orig_to_rank[orig_id] = perm[original_to_filtered[orig_id]]
                // or u32::MAX if the original node is not in the filtered subgraph.
                let mut orig_to_rank: Vec<u32> = vec![u32::MAX; n_orig];
                for (orig_id, &filt_id) in filtered_ebg.original_to_filtered.iter().enumerate() {
                    if filt_id != u32::MAX {
                        let rank = order_data.perm[filt_id as usize];
                        orig_to_rank[orig_id] = rank;
                    }
                }

                let mode_byte = filtered_ebg.mode.0;
                let inputs_sha: [u8; 16] = filtered_ebg.inputs_sha[..16]
                    .try_into()
                    .expect("filtered_ebg inputs_sha is 32 bytes; first 16 used");

                let o2r = ModeIndex {
                    kind: ModeIndexKind::OrigToRank,
                    mode: mode_byte,
                    inputs_sha,
                    data: Cow::Owned(orig_to_rank),
                };
                append_encoded(
                    &mut w,
                    SectionKind::OrigToRank,
                    &format!("mode/{}/orig_to_rank", mode),
                    ModeIndexFile::encode(&o2r),
                )?;
                drop(o2r);

                // filtered_to_original — copy of filtered_ebg.filtered_to_original.
                let f2o_data: Vec<u32> = filtered_ebg.filtered_to_original.to_vec();
                let f2o = ModeIndex {
                    kind: ModeIndexKind::FilteredToOriginal,
                    mode: mode_byte,
                    inputs_sha,
                    data: Cow::Owned(f2o_data),
                };
                append_encoded(
                    &mut w,
                    SectionKind::FilteredToOriginal,
                    &format!("mode/{}/filtered_to_original", mode),
                    ModeIndexFile::encode(&f2o),
                )?;
            }
        }

        // step7 topology. As of #151 the v4 layout pads every variable-
        // length u32 array to a u64 boundary, so the server reads it
        // zero-copy out of the mmap'd container.
        let topo = step7.join(format!("cch.{}.topo", mode));
        maybe_append(
            &mut w,
            SectionKind::CchTopo,
            &format!("mode/{}/topo", mode),
            &topo,
        )?;
        // step8 customised weights.
        let cch_w = step8.join(format!("cch.w.{}.u32", mode));
        maybe_append(
            &mut w,
            SectionKind::CchWeightsTime,
            &format!("mode/{}/weights.time", mode),
            &cch_w,
        )?;
        let cch_d = step8.join(format!("cch.d.{}.u32", mode));
        maybe_append(
            &mut w,
            SectionKind::CchWeightsDist,
            &format!("mode/{}/weights.dist", mode),
            &cch_d,
        )?;

        // ---- Pre-built flat adjacencies (#150) -----------------------
        // Flats are built once at pack time from (cch_topo, cch_weights),
        // serialised into the container, and mmap'd directly at server
        // boot. This is the architectural pivot that bounds idle RSS to
        // the working set rather than the dataset size.
        //
        // We build a mode at a time and drop the in-memory copies before
        // moving to the next mode, so the pack memory footprint stays
        // bounded by one mode's worth of flats (~1.5 GB peak on Belgium).
        let topo_path = step7.join(format!("cch.{}.topo", mode));
        let cch_w_path = step8.join(format!("cch.w.{}.u32", mode));
        let cch_d_path = step8.join(format!("cch.d.{}.u32", mode));
        if topo_path.exists() && cch_w_path.exists() && cch_d_path.exists() {
            // Load topo + both weight metrics for this mode. If parsing
            // fails (e.g. synthetic test inputs), log a warning and skip
            // flats for this mode — pack still succeeds; the server can
            // fall back to building flats at boot from these same files.
            let topo_res = CchTopoFile::read(&topo_path);
            let time_res = CchWeightsFile::read(&cch_w_path);
            let dist_res = CchWeightsFile::read(&cch_d_path);
            match (topo_res, time_res, dist_res) {
                (Ok(cch_topo), Ok(cch_time), Ok(cch_dist)) => {
                    // TIME flats: UP and DOWN-REV carry topo_edge_idx (the
                    // routing hot path needs it for parent-pointer unpacking);
                    // forward-DOWN does not.
                    let up_time = UpAdjFlat::build_with(&cch_topo, &cch_time, true);
                    append_encoded(
                        &mut w,
                        SectionKind::UpAdjFlat,
                        &format!("mode/{}/up_adj_flat.time", mode),
                        UpAdjFlatFile::encode(&up_time),
                    )?;
                    drop(up_time);

                    let drev_time = DownReverseAdjFlat::build_with(&cch_topo, &cch_time, true);
                    append_encoded(
                        &mut w,
                        SectionKind::DownReverseAdjFlat,
                        &format!("mode/{}/down_reverse_adj_flat.time", mode),
                        DownReverseAdjFlatFile::encode(&drev_time),
                    )?;
                    drop(drev_time);

                    let dadj_time = DownAdjFlat::build(&cch_topo, &cch_time);
                    append_encoded(
                        &mut w,
                        SectionKind::DownAdjFlat,
                        &format!("mode/{}/down_adj_flat.time", mode),
                        DownAdjFlatFile::encode(&dadj_time),
                    )?;
                    drop(dadj_time);

                    // DIST flats: only PHAST forward + isodistance use them;
                    // no topo back-ref needed.
                    let up_dist = UpAdjFlat::build(&cch_topo, &cch_dist);
                    append_encoded(
                        &mut w,
                        SectionKind::UpAdjFlat,
                        &format!("mode/{}/up_adj_flat.dist", mode),
                        UpAdjFlatFile::encode(&up_dist),
                    )?;
                    drop(up_dist);

                    let drev_dist = DownReverseAdjFlat::build(&cch_topo, &cch_dist);
                    append_encoded(
                        &mut w,
                        SectionKind::DownReverseAdjFlat,
                        &format!("mode/{}/down_reverse_adj_flat.dist", mode),
                        DownReverseAdjFlatFile::encode(&drev_dist),
                    )?;
                    drop(drev_dist);

                    let dadj_dist = DownAdjFlat::build(&cch_topo, &cch_dist);
                    append_encoded(
                        &mut w,
                        SectionKind::DownAdjFlat,
                        &format!("mode/{}/down_adj_flat.dist", mode),
                        DownAdjFlatFile::encode(&dadj_dist),
                    )?;
                    drop(dadj_dist);
                }
                (topo_r, time_r, dist_r) => {
                    let why = topo_r
                        .err()
                        .map(|e| format!("topo: {e}"))
                        .or_else(|| time_r.err().map(|e| format!("weights.time: {e}")))
                        .or_else(|| dist_r.err().map(|e| format!("weights.dist: {e}")))
                        .unwrap_or_else(|| "unknown".to_string());
                    eprintln!(
                        "  ! [skip flats] mode={} ({}); server will rebuild on boot",
                        mode, why
                    );
                }
            }
        }
    }

    // ---- Packed snap index (#154) ----------------------------------
    // Build the shared snap_points + snap_grid arrays from ebg_nodes +
    // nbg_geo, plus one snap_mask per mode (derived from
    // filtered_ebg.filtered_to_original). Emit all three section kinds.
    //
    // If any of the inputs is missing or malformed, the whole snap
    // index emission is skipped — the server's back-compat path will
    // build the legacy rstar at boot.
    if let Err(e) = pack_snap_index(&mut w, &step3, &step4, &step5, &modes) {
        eprintln!(
            "  ! [skip snap_index] {}; server will build rstar at boot",
            e
        );
    }

    // ---- Flat edge geometry (#155) ---------------------------------
    // Derive shared/edge_geom_offsets + shared/edge_geom_points from
    // the heap nbg.geo polylines. This replaces the heap Vec<Vec<_>>
    // shape on the serve path with mmap-backed flat arrays. If
    // nbg.geo is missing or malformed we skip the section emission;
    // the server falls back to building EdgeGeometry from the legacy
    // heap polylines at boot.
    if let Err(e) = pack_edge_geometry(&mut w, &step3) {
        eprintln!(
            "  ! [skip edge_geom] {}; server will build flat geometry from heap polylines at boot",
            e
        );
    }

    // ---- Manifest ---------------------------------------------------
    // Lists the modes packed and their bundle ids. For now, every mode
    // is a singleton bundle (bundle_id == mode_name); the topology-
    // groups follow-up (#146) will let multiple modes share one bundle.
    // The manifest is a JSON object so future fields land cleanly.
    let manifest = build_manifest(&modes, &region_id);
    w.append_bytes(SectionKind::Unknown, MANIFEST_NAME, manifest.as_bytes())?;

    let n_sec = w.len();
    w.finalize()?;

    let final_size = std::fs::metadata(out)?.len();
    println!(
        "wrote {} sections, {:.2} GiB → {}",
        n_sec,
        final_size as f64 / (1024.0 * 1024.0 * 1024.0),
        out.display()
    );
    Ok(())
}

/// Build and append the packed snap index sections (#154):
///
/// * `shared/snap_points` — flat array of `PackedPoint` derived from
///   ebg_nodes + nbg_geo (same 50 m dedup rule as the legacy
///   `SpatialIndex::build`).
/// * `shared/snap_grid` — uniform-grid CSR over the points.
/// * `mode/<m>/snap_mask` — one per mode, marking sample-array indices
///   that are snap-eligible for that mode. Derived from
///   `filtered.<mode>.ebg::filtered_to_original` (same SCC-filtered
///   accessibility set the legacy `mode_data.mask` uses).
///
/// Returns Err if any required input file is missing or fails to parse —
/// the caller logs and skips the section emission, which leaves the
/// container compatible with the back-compat fallback in `state.rs`.
fn pack_snap_index(
    w: &mut ContainerWriter,
    step3: &Path,
    step4: &Path,
    step5: &Path,
    modes: &[String],
) -> Result<()> {
    let ebg_nodes_path = step4.join("ebg.nodes");
    let nbg_geo_path = step3.join("nbg.geo");
    if !ebg_nodes_path.exists() {
        anyhow::bail!("ebg.nodes missing at {}", ebg_nodes_path.display());
    }
    if !nbg_geo_path.exists() {
        anyhow::bail!("nbg.geo missing at {}", nbg_geo_path.display());
    }

    let ebg_nodes = EbgNodesFile::read(&ebg_nodes_path)
        .with_context(|| format!("reading {}", ebg_nodes_path.display()))?;
    let nbg_geo = NbgGeoFile::read(&nbg_geo_path)
        .with_context(|| format!("reading {}", nbg_geo_path.display()))?;

    // Build per-mode EBG-id-indexed `[u64]` masks from the SCC-filtered
    // EBG. This is exactly what `state.rs::load_mode_data` does at boot
    // for the legacy path; replicating it here so the packed snap_mask
    // matches the legacy snap behaviour bit-for-bit.
    let n_original = ebg_nodes.n_nodes as usize;
    let n_words = n_original.div_ceil(64);

    struct ModeWork {
        name: String,
        mode_byte: u8,
        mask: Vec<u64>,
        inputs_sha: [u8; 16],
    }
    let mut mode_work: Vec<ModeWork> = Vec::with_capacity(modes.len());
    for mode in modes {
        let filtered_path = step5.join(format!("filtered.{}.ebg", mode));
        if !filtered_path.exists() {
            anyhow::bail!(
                "filtered.{}.ebg missing at {}",
                mode,
                filtered_path.display()
            );
        }
        let filtered_ebg = FilteredEbgFile::read(&filtered_path)
            .with_context(|| format!("reading {}", filtered_path.display()))?;
        anyhow::ensure!(
            filtered_ebg.n_original_nodes as usize == n_original,
            "filtered.{}.ebg n_original_nodes ({}) != ebg.nodes n_nodes ({})",
            mode,
            filtered_ebg.n_original_nodes,
            n_original
        );
        let mut bits = vec![0u64; n_words];
        for &orig_id in filtered_ebg.filtered_to_original.iter() {
            let word = orig_id as usize / 64;
            let bit = orig_id as usize % 64;
            bits[word] |= 1u64 << bit;
        }
        let mode_byte = filtered_ebg.mode.0;
        let inputs_sha: [u8; 16] = filtered_ebg.inputs_sha[..16]
            .try_into()
            .expect("filtered_ebg inputs_sha has at least 16 bytes");
        mode_work.push(ModeWork {
            name: mode.clone(),
            mode_byte,
            mask: bits,
            inputs_sha,
        });
    }

    // Build snap_index from ebg_nodes + nbg_geo + the per-mode masks.
    let builder_modes: Vec<SnapBuilderMode<'_>> = mode_work
        .iter()
        .map(|m| SnapBuilderMode {
            mode_byte: m.mode_byte,
            mask: &m.mask,
            inputs_sha: m.inputs_sha,
        })
        .collect();
    let built = build_snap_index(&ebg_nodes, &nbg_geo, &builder_modes, DEFAULT_CELL_LOG2);
    println!(
        "  + [{:>5} MiB] {:<28} <- (snap_points, {} samples, cell_log2={})",
        SnapPointsFile::encode(&built.points).len() / (1024 * 1024),
        "shared/snap_points",
        built.points.points.len(),
        built.points.cell_log2,
    );

    // Re-encode for emission. (`encode` is deterministic — re-encoding
    // is cheap and avoids holding two copies in memory.)
    let pts_bytes = SnapPointsFile::encode(&built.points);
    w.append_bytes(SectionKind::SnapPoints, "shared/snap_points", &pts_bytes)
        .with_context(|| "packing shared/snap_points".to_string())?;
    drop(pts_bytes);

    let grid_bytes = SnapGridFile::encode(&built.grid);
    println!(
        "  + [{:>5} MiB] {:<28} <- (snap_grid, {}x{} cells)",
        grid_bytes.len() / (1024 * 1024),
        "shared/snap_grid",
        built.grid.n_cells_x,
        built.grid.n_cells_y,
    );
    w.append_bytes(SectionKind::SnapGrid, "shared/snap_grid", &grid_bytes)
        .with_context(|| "packing shared/snap_grid".to_string())?;
    drop(grid_bytes);

    for (mw, mask) in mode_work.iter().zip(built.masks.iter()) {
        let mask_bytes = SnapMaskFile::encode(mask);
        println!(
            "  + [{:>5} KiB] {:<28} <- (snap_mask, {} samples)",
            mask_bytes.len() / 1024,
            format!("mode/{}/snap_mask", mw.name),
            mask.n_points,
        );
        let section_name = format!("mode/{}/snap_mask", mw.name);
        w.append_bytes(SectionKind::SnapModeMask, &section_name, &mask_bytes)
            .with_context(|| format!("packing {}", section_name))?;
    }

    Ok(())
}

/// Build and append the flat edge geometry sections (#155):
///
/// * `shared/edge_geom_offsets` — `[u32; n_edges + 1]` cumulative
///   point counts. CSR convention: `offsets[i]..offsets[i+1]` is the
///   half-open vertex range for edge `i`.
/// * `shared/edge_geom_points` — `[i32; 2 * n_points]` interleaved
///   `(lon_e7, lat_e7)` pairs in `nbg.geo` source order. Bytes are
///   stable byte-for-byte vs `nbg.geo`'s polyline blob.
///
/// Returns Err if the input file is missing or fails to parse — the
/// caller logs and skips the section emission, leaving the container
/// compatible with the back-compat fallback in `state.rs`.
fn pack_edge_geometry(w: &mut ContainerWriter, step3: &Path) -> Result<()> {
    let nbg_geo_path = step3.join("nbg.geo");
    if !nbg_geo_path.exists() {
        anyhow::bail!("nbg.geo missing at {}", nbg_geo_path.display());
    }

    let nbg_geo = NbgGeoFile::read(&nbg_geo_path)
        .with_context(|| format!("reading {}", nbg_geo_path.display()))?;

    // ---- 1. Build offsets + points in a single pass --------------------
    let n_edges = nbg_geo.polylines.len();
    let mut offsets: Vec<u32> = Vec::with_capacity(n_edges + 1);
    // Estimate: ~30 M vertices on Belgium → 240 MB. Pre-size conservatively.
    let est_pts = nbg_geo
        .polylines
        .iter()
        .map(|p| p.lat_fxp.len())
        .sum::<usize>();
    let mut points: Vec<i32> = Vec::with_capacity(est_pts.checked_mul(2).unwrap_or(0));

    let mut cumulative: u32 = 0;
    let mut bbox_min_lon = i32::MAX;
    let mut bbox_min_lat = i32::MAX;
    let mut bbox_max_lon = i32::MIN;
    let mut bbox_max_lat = i32::MIN;

    for poly in &nbg_geo.polylines {
        offsets.push(cumulative);
        let n = poly.lat_fxp.len();
        // The legacy NbgGeo guarantees lat_fxp.len() == lon_fxp.len() per
        // edge; defend against malformed data anyway.
        anyhow::ensure!(
            n == poly.lon_fxp.len(),
            "polyline has mismatched lat/lon lengths ({} vs {})",
            n,
            poly.lon_fxp.len()
        );
        for i in 0..n {
            let lon = poly.lon_fxp[i];
            let lat = poly.lat_fxp[i];
            points.push(lon);
            points.push(lat);
            if lon < bbox_min_lon {
                bbox_min_lon = lon;
            }
            if lon > bbox_max_lon {
                bbox_max_lon = lon;
            }
            if lat < bbox_min_lat {
                bbox_min_lat = lat;
            }
            if lat > bbox_max_lat {
                bbox_max_lat = lat;
            }
        }
        cumulative = cumulative
            .checked_add(n as u32)
            .ok_or_else(|| anyhow::anyhow!("edge geometry total point count exceeds u32::MAX"))?;
    }
    offsets.push(cumulative);
    let n_points: u32 = cumulative;

    if points.is_empty() {
        // No polylines anywhere — leave bbox at zero rather than the
        // sentinel min/max values.
        bbox_min_lon = 0;
        bbox_min_lat = 0;
        bbox_max_lon = 0;
        bbox_max_lat = 0;
    }

    // ---- 2. Round-trip sanity check (build → parse) --------------------
    // Catches encoder regressions before they hit serve callers. Cheap on
    // pack time (one CRC pass) but invaluable when iterating on the
    // format.
    let off_struct = EdgeGeomOffsets {
        n_edges: n_edges as u32,
        n_points,
        offsets: Cow::Owned(offsets),
    };
    let pts_struct = EdgeGeomPoints {
        n_points,
        bbox_min_lon,
        bbox_min_lat,
        bbox_max_lon,
        bbox_max_lat,
        points: Cow::Owned(points),
    };

    let off_bytes = EdgeGeomOffsetsFile::encode(&off_struct);
    let pts_bytes = EdgeGeomPointsFile::encode(&pts_struct);

    // Parse them back and confirm the polyline at one sample edge round-
    // trips byte-identically vs the source.
    let parsed_off = EdgeGeomOffsetsFile::read_from_bytes(&off_bytes)
        .with_context(|| "edge_geom_offsets failed self-roundtrip")?;
    let parsed_pts = EdgeGeomPointsFile::read_from_bytes(&pts_bytes)
        .with_context(|| "edge_geom_points failed self-roundtrip")?;
    anyhow::ensure!(
        parsed_off.n_edges as usize == n_edges,
        "edge_geom_offsets roundtrip n_edges mismatch"
    );
    anyhow::ensure!(
        parsed_pts.points.len() == 2 * n_points as usize,
        "edge_geom_points roundtrip point-count mismatch"
    );
    if !nbg_geo.polylines.is_empty() {
        // Pick a non-empty polyline if any exist.
        if let Some((edge_id, src_poly)) = nbg_geo
            .polylines
            .iter()
            .enumerate()
            .find(|(_, p)| !p.lat_fxp.is_empty())
        {
            let s = parsed_off.offsets[edge_id] as usize;
            let e = parsed_off.offsets[edge_id + 1] as usize;
            anyhow::ensure!(
                e - s == src_poly.lat_fxp.len(),
                "round-trip vertex count mismatch on edge {} ({} vs {})",
                edge_id,
                e - s,
                src_poly.lat_fxp.len()
            );
            for i in 0..(e - s) {
                let lon = parsed_pts.points[(s + i) * 2];
                let lat = parsed_pts.points[(s + i) * 2 + 1];
                anyhow::ensure!(
                    lon == src_poly.lon_fxp[i] && lat == src_poly.lat_fxp[i],
                    "round-trip vertex mismatch at edge {} vertex {}",
                    edge_id,
                    i
                );
            }
        }
    }

    // ---- 3. Emit both sections -----------------------------------------
    println!(
        "  + [{:>5} MiB] {:<28} <- (edge_geom_offsets, n_edges={})",
        off_bytes.len() / (1024 * 1024),
        "shared/edge_geom_offsets",
        n_edges,
    );
    w.append_bytes(
        SectionKind::EdgeGeomOffsets,
        "shared/edge_geom_offsets",
        &off_bytes,
    )
    .with_context(|| "packing shared/edge_geom_offsets".to_string())?;

    println!(
        "  + [{:>5} MiB] {:<28} <- (edge_geom_points, n_points={}, bbox=[{},{}]..[{},{}])",
        pts_bytes.len() / (1024 * 1024),
        "shared/edge_geom_points",
        n_points,
        bbox_min_lon,
        bbox_min_lat,
        bbox_max_lon,
        bbox_max_lat,
    );
    w.append_bytes(
        SectionKind::EdgeGeomPoints,
        "shared/edge_geom_points",
        &pts_bytes,
    )
    .with_context(|| "packing shared/edge_geom_points".to_string())?;

    Ok(())
}

/// Default region identifier when a container was packed without an
/// explicit `--region` flag (or read from a legacy container that
/// pre-dates region tagging).
///
/// Belgium was the canonical demonstration dataset before #91, so the
/// fallback is `"BE"` — the only legacy `baseline.butterfly` files in
/// existence are Belgium builds, and tagging them as such keeps the
/// multi-region loader compatible without forcing a re-pack.
pub const DEFAULT_REGION_ID: &str = "BE";

/// Normalise a region id: trim whitespace, uppercase. Returns an error
/// if the result is empty or contains characters outside the safe set
/// `[A-Z0-9_-]`. Region ids are used as path-safe map keys and
/// Prometheus label values, so the safe set is tight on purpose.
pub fn normalize_region_id(raw: &str) -> Result<String> {
    let trimmed = raw.trim();
    anyhow::ensure!(!trimmed.is_empty(), "region id must not be empty");
    let upper = trimmed.to_ascii_uppercase();
    for ch in upper.chars() {
        let ok = ch.is_ascii_uppercase() || ch.is_ascii_digit() || ch == '_' || ch == '-';
        anyhow::ensure!(
            ok,
            "region id '{}' contains illegal character '{}' (allowed: A-Z 0-9 _ -)",
            raw,
            ch
        );
    }
    anyhow::ensure!(
        upper.len() <= 16,
        "region id '{}' too long ({} chars, max 16)",
        raw,
        upper.len()
    );
    Ok(upper)
}

/// Build the JSON manifest payload listing the packed modes + region id.
/// The JSON shape is deliberately small + extensible: arrays of strings,
/// every mode mapped to a bundle id equal to its name (one bundle per
/// mode is the only shape this ticket ships; #146 generalises to
/// N-mode-per- bundle). Unknown JSON fields round-trip through `unpack`
/// because the section is byte-copied.
///
/// Schema (v1):
/// ```json
/// {
///   "version": 1,
///   "region_id": "BE",
///   "modes": ["bike", "car", "foot", "truck"],
///   "bundles": { "bike": ["bike"], "car": ["car"], ... }
/// }
/// ```
///
/// The `region_id` field is additive — readers that ignore it still
/// parse the file correctly, and pre-#91 containers without the field
/// fall back to [`DEFAULT_REGION_ID`] (`BE`).
fn build_manifest(modes: &[String], region_id: &str) -> String {
    use std::fmt::Write;
    let region_esc = region_id.replace('"', "\\\"");
    let mut s = String::from("{\n  \"version\": 1,\n  \"region_id\": \"");
    s.push_str(&region_esc);
    s.push_str("\",\n  \"modes\": [");
    for (i, m) in modes.iter().enumerate() {
        if i > 0 {
            s.push_str(", ");
        }
        let _ = write!(s, "\"{}\"", m.replace('"', "\\\""));
    }
    s.push_str("],\n  \"bundles\": {");
    for (i, m) in modes.iter().enumerate() {
        if i > 0 {
            s.push_str(", ");
        }
        let esc = m.replace('"', "\\\"");
        let _ = write!(s, "\"{0}\": [\"{0}\"]", esc);
    }
    s.push_str("}\n}\n");
    s
}

/// Best-effort parse of `region_id` out of a container's
/// `shared/manifest.json`. Returns [`DEFAULT_REGION_ID`] for legacy
/// containers (no manifest, or manifest missing the field).
///
/// We deliberately do NOT pull in `serde_json` for this — the
/// manifest is a tiny stable-shape JSON document, and the full
/// `serde_json` round-trip dependency cost is not justified for one
/// string field. The needle is `"region_id"\s*:\s*"<value>"`. Falls
/// back to default on any parse failure rather than rejecting the
/// container.
pub fn manifest_region_id(manifest_bytes: &[u8]) -> String {
    let text = match std::str::from_utf8(manifest_bytes) {
        Ok(s) => s,
        Err(_) => return DEFAULT_REGION_ID.to_string(),
    };
    if let Some(idx) = text.find("\"region_id\"") {
        let rest = &text[idx + "\"region_id\"".len()..];
        // Skip whitespace and the colon.
        let rest = rest.trim_start();
        let rest = match rest.strip_prefix(':') {
            Some(r) => r.trim_start(),
            None => return DEFAULT_REGION_ID.to_string(),
        };
        // Expect a quoted string.
        let rest = match rest.strip_prefix('"') {
            Some(r) => r,
            None => return DEFAULT_REGION_ID.to_string(),
        };
        // Read until the next unescaped quote.
        let mut out = String::new();
        let chars = rest.chars();
        let mut escaped = false;
        for c in chars {
            if escaped {
                out.push(c);
                escaped = false;
                continue;
            }
            match c {
                '\\' => escaped = true,
                '"' => {
                    return normalize_region_id(&out)
                        .unwrap_or_else(|_| DEFAULT_REGION_ID.to_string());
                }
                _ => out.push(c),
            }
        }
    }
    DEFAULT_REGION_ID.to_string()
}

/// Map a `SectionEntry` back to the on-disk path inside a `step{N}/`
/// tree, mirroring what `pack` consumed. Handles both the new
/// `shared/`+`mode/<m>/...` schema and the legacy `stepN/...` schema
/// from earlier container builds, so old containers still round-trip.
fn path_for_section(out_dir: &Path, name: &str) -> Option<PathBuf> {
    // ---- New schema -------------------------------------------------
    if name == MANIFEST_NAME {
        return Some(out_dir.join("manifest.json"));
    }
    if let Some(rest) = name.strip_prefix("shared/") {
        // shared/step1.<file> → step1/<file>
        if let Some(file) = rest.strip_prefix("step1.") {
            return Some(out_dir.join("step1").join(file));
        }
        // shared/nbg.<x> → step3/nbg.<x>
        if let Some(_n) = rest.strip_prefix("nbg.") {
            return Some(out_dir.join("step3").join(rest));
        }
        // shared/ebg.<x> → step4/ebg.<x>
        if let Some(_n) = rest.strip_prefix("ebg.") {
            return Some(out_dir.join("step4").join(rest));
        }
        return None;
    }
    if let Some(rest) = name.strip_prefix("mode/") {
        let slash = rest.find('/')?;
        let mode = &rest[..slash];
        let leaf = &rest[slash + 1..];
        return match leaf {
            "way_attrs" => Some(
                out_dir
                    .join("step2")
                    .join(format!("way_attrs.{}.bin", mode)),
            ),
            "turn_rules" => Some(
                out_dir
                    .join("step2")
                    .join(format!("turn_rules.{}.bin", mode)),
            ),
            "filtered_ebg" => Some(out_dir.join("step5").join(format!("filtered.{}.ebg", mode))),
            "node_weights.time" => Some(out_dir.join("step5").join(format!("w.{}.u32", mode))),
            "node_weights.turn" => Some(out_dir.join("step5").join(format!("t.{}.u32", mode))),
            "mask" => Some(out_dir.join("step5").join(format!("mask.{}.bitset", mode))),
            "order" => Some(out_dir.join("step6").join(format!("order.{}.ebg", mode))),
            "topo" => Some(out_dir.join("step7").join(format!("cch.{}.topo", mode))),
            "weights.time" => Some(out_dir.join("step8").join(format!("cch.w.{}.u32", mode))),
            "weights.dist" => Some(out_dir.join("step8").join(format!("cch.d.{}.u32", mode))),
            _ => None,
        };
    }

    // ---- Legacy `stepN/...` schema (older containers) ---------------
    legacy_path_for_section(out_dir, name)
}

fn legacy_path_for_section(out_dir: &Path, name: &str) -> Option<PathBuf> {
    // The mapping mirrors `pack` exactly. Any section we do not
    // recognise is left out; `unpack` reports it as a warning.
    if let Some(rest) = name.strip_prefix("step1/") {
        return Some(out_dir.join("step1").join(rest));
    }
    if let Some(rest) = name.strip_prefix("step2/") {
        // step2 sections are `way_attrs.<mode>` / `turn_rules.<mode>`;
        // re-add the `.bin` suffix the input directory used.
        if let Some(mode) = rest.strip_prefix("way_attrs.") {
            return Some(
                out_dir
                    .join("step2")
                    .join(format!("way_attrs.{}.bin", mode)),
            );
        }
        if let Some(mode) = rest.strip_prefix("turn_rules.") {
            return Some(
                out_dir
                    .join("step2")
                    .join(format!("turn_rules.{}.bin", mode)),
            );
        }
        return None;
    }
    if let Some(rest) = name.strip_prefix("step3/") {
        return Some(out_dir.join("step3").join(rest));
    }
    if let Some(rest) = name.strip_prefix("step4/") {
        return Some(out_dir.join("step4").join(rest));
    }
    if let Some(rest) = name.strip_prefix("step5/") {
        // Restore the trailing extension: filtered.<mode> -> filtered.<mode>.ebg, etc.
        if let Some(mode) = rest.strip_prefix("filtered.") {
            return Some(out_dir.join("step5").join(format!("filtered.{}.ebg", mode)));
        }
        if let Some(mode) = rest.strip_prefix("w.") {
            return Some(out_dir.join("step5").join(format!("w.{}.u32", mode)));
        }
        if let Some(mode) = rest.strip_prefix("t.") {
            return Some(out_dir.join("step5").join(format!("t.{}.u32", mode)));
        }
        if let Some(mode) = rest.strip_prefix("mask.") {
            return Some(out_dir.join("step5").join(format!("mask.{}.bitset", mode)));
        }
        return None;
    }
    if let Some(rest) = name.strip_prefix("step6/") {
        if let Some(mode) = rest.strip_prefix("order.") {
            return Some(out_dir.join("step6").join(format!("order.{}.ebg", mode)));
        }
        return None;
    }
    if let Some(rest) = name.strip_prefix("step7/") {
        if let Some(mode) = rest.strip_prefix("cch.") {
            return Some(out_dir.join("step7").join(format!("cch.{}.topo", mode)));
        }
        return None;
    }
    if let Some(rest) = name.strip_prefix("step8/") {
        if let Some(mode) = rest.strip_prefix("cch.w.") {
            return Some(out_dir.join("step8").join(format!("cch.w.{}.u32", mode)));
        }
        if let Some(mode) = rest.strip_prefix("cch.d.") {
            return Some(out_dir.join("step8").join(format!("cch.d.{}.u32", mode)));
        }
        return None;
    }
    None
}

/// Implementation of the `unpack` subcommand. Inverse of `pack`: writes
/// every section back to the canonical `step{N}/file` path under
/// `out_dir`. Validates each section's CRC during the copy.
///
/// `out_dir` must not exist (so the inverse mapping is unambiguous).
pub fn unpack(path: &Path, out_dir: &Path) -> Result<()> {
    if out_dir.exists() {
        anyhow::bail!(
            "output directory {} already exists; refusing to overwrite",
            out_dir.display()
        );
    }
    std::fs::create_dir_all(out_dir)?;

    let c = Container::open(path)?;
    println!(
        "unpacking {} ({} sections) → {}",
        path.display(),
        c.n_sections,
        out_dir.display()
    );

    for sec in &c.sections {
        // Flat adjacency sections (#150) are synthesised at pack time
        // and don't round-trip through step{N}/ — the next pack will
        // rebuild them from cch_topo + cch_weights. Skip them here so
        // unpack stays a faithful inverse of the on-disk inputs.
        if matches!(
            sec.kind,
            SectionKind::UpAdjFlat
                | SectionKind::DownAdjFlat
                | SectionKind::DownReverseAdjFlat
                | SectionKind::OrigToRank
                | SectionKind::FilteredToOriginal
                | SectionKind::SnapPoints
                | SectionKind::SnapGrid
                | SectionKind::SnapModeMask
        ) {
            println!("  -- (skip synthesised) {}", sec.name);
            continue;
        }
        let out_path = path_for_section(out_dir, &sec.name).ok_or_else(|| {
            anyhow::anyhow!(
                "section '{}' does not match the standard step{{N}}/... layout; \
                 cannot map back to a file path",
                sec.name
            )
        })?;
        if let Some(parent) = out_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let bytes = c.read_section_verified(path, sec)?;
        std::fs::write(&out_path, &bytes)?;
        println!(
            "  -> [{:>5} MiB] {:<32} -> {}",
            bytes.len() / (1024 * 1024),
            sec.name,
            out_path.display()
        );
    }
    println!("OK");
    Ok(())
}

/// Implementation of the `inspect` subcommand.
pub fn inspect(path: &Path, verify: bool, verify_full: bool) -> Result<()> {
    let c = Container::open(path)?;
    println!(
        "{} (version {}, {} sections, dir@{}+{}b)",
        path.display(),
        c.version,
        c.n_sections,
        c.dir_offset,
        c.dir_len,
    );
    println!(
        "{:<6} {:<28} {:<32} {:>14} {:>14} {:>16}",
        "idx", "kind", "name", "offset", "length", "crc"
    );
    for (i, sec) in c.sections.iter().enumerate() {
        println!(
            "{:<6} {:<28} {:<32} {:>14} {:>14} 0x{:016X}",
            i,
            sec.kind.label(),
            sec.name,
            sec.offset,
            sec.len,
            sec.crc,
        );
    }

    if verify {
        println!();
        println!("verifying {} per-section CRCs ...", c.n_sections);
        for sec in &c.sections {
            let _ = c.read_section_verified(path, sec)?;
        }
        println!("OK");
    }
    if verify_full {
        println!();
        println!("verifying full-file CRC ...");
        c.verify_file_crc(path)?;
        println!("OK");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::formats::filtered_ebg::FilteredEbg;
    use crate::formats::order_ebg::OrderEbg;
    use crate::profile_abi::Mode;
    use std::borrow::Cow;
    use std::fs;
    use tempfile::TempDir;

    fn write_file(p: &Path, body: &[u8]) -> Result<()> {
        if let Some(parent) = p.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(p, body)?;
        Ok(())
    }

    /// Write a minimal but parse-valid filtered.<mode>.ebg.
    ///
    /// #157's Copilot fix turned the previous soft-skip on parse error
    /// into a hard-fail (the soft-skip would silently produce a "new"
    /// container that drops the server onto the legacy fallback at
    /// boot — a regression that only surfaced at run time). The synth
    /// fixture therefore has to write real headers, not byte-string
    /// placeholders.
    fn write_filtered_ebg(p: &Path, mode: Mode) -> Result<()> {
        if let Some(parent) = p.parent() {
            fs::create_dir_all(parent)?;
        }
        let data = FilteredEbg {
            mode,
            n_filtered_nodes: 0,
            n_filtered_arcs: 0,
            n_original_nodes: 0,
            inputs_sha: [0u8; 32],
            offsets: Cow::Owned(vec![0u64]), // n_filtered_nodes + 1 = 1 entry
            heads: Cow::Owned(vec![]),
            original_arc_idx: Cow::Owned(vec![]),
            filtered_to_original: Cow::Owned(vec![]),
            original_to_filtered: Cow::Owned(vec![]),
        };
        crate::formats::FilteredEbgFile::write(p, &data)?;
        Ok(())
    }

    /// Write a minimal but parse-valid order.<mode>.ebg.
    fn write_order_ebg(p: &Path) -> Result<()> {
        if let Some(parent) = p.parent() {
            fs::create_dir_all(parent)?;
        }
        let data = OrderEbg {
            n_nodes: 0,
            inputs_sha: [0u8; 32],
            perm: vec![],
            inv_perm: vec![],
        };
        crate::formats::OrderEbgFile::write(p, &data)?;
        Ok(())
    }

    /// Build a synthetic data dir with a couple of files in step1/
    /// and step5/ + step8/ so we can prove the per-mode globbing works.
    fn synth_dir() -> Result<TempDir> {
        let tmp = TempDir::new()?;
        let root = tmp.path();

        write_file(&root.join("step1").join("nodes.sa"), b"sa-bytes")?;
        write_file(&root.join("step1").join("nodes.si"), b"si-bytes")?;
        write_file(&root.join("step1").join("ways.raw"), b"ways-raw")?;
        write_file(&root.join("step1").join("relations.raw"), b"rel-raw")?;
        write_file(&root.join("step3").join("nbg.csr"), b"csr")?;
        write_file(&root.join("step3").join("nbg.geo"), b"geo")?;
        write_file(&root.join("step3").join("nbg.node_map"), b"map")?;
        write_file(&root.join("step4").join("ebg.nodes"), b"en")?;
        write_file(&root.join("step4").join("ebg.csr"), b"ec")?;
        write_file(&root.join("step4").join("ebg.turn_table"), b"tt")?;

        // Per-mode samples for step2 / step5 / step6 / step7 / step8.
        write_file(&root.join("step2").join("way_attrs.car.bin"), b"wa-car")?;
        write_file(&root.join("step2").join("way_attrs.bike.bin"), b"wa-bike")?;
        write_file(&root.join("step2").join("turn_rules.car.bin"), b"tr-car")?;
        write_file(&root.join("step2").join("turn_rules.bike.bin"), b"tr-bike")?;

        // filtered.<mode>.ebg + order.<mode>.ebg need real binary headers
        // because #157 hard-fails the pack on parse error (see Copilot
        // review on PR #157 — silent skip → legacy fallback at boot).
        // Mode index here is arbitrary: synth fixtures don't run the
        // model discovery path, the byte just gets round-tripped.
        write_filtered_ebg(&root.join("step5").join("filtered.car.ebg"), Mode(1))?;
        write_filtered_ebg(&root.join("step5").join("filtered.bike.ebg"), Mode(0))?;
        write_file(&root.join("step5").join("w.car.u32"), b"wcar")?;
        write_file(&root.join("step5").join("w.bike.u32"), b"wbike")?;
        write_file(&root.join("step5").join("t.car.u32"), b"tcar")?;
        write_file(&root.join("step5").join("t.bike.u32"), b"tbike")?;
        write_file(&root.join("step5").join("mask.car.bitset"), b"mc")?;
        write_file(&root.join("step5").join("mask.bike.bitset"), b"mb")?;

        write_order_ebg(&root.join("step6").join("order.car.ebg"))?;
        write_order_ebg(&root.join("step6").join("order.bike.ebg"))?;
        // Lifted variants must be skipped.
        write_file(
            &root.join("step6").join("order.lifted.car.ebg"),
            b"o-lifted",
        )?;

        write_file(&root.join("step7").join("cch.car.topo"), b"cch-car")?;
        write_file(&root.join("step7").join("cch.bike.topo"), b"cch-bike")?;

        write_file(&root.join("step8").join("cch.w.car.u32"), b"wcar-cch")?;
        write_file(&root.join("step8").join("cch.w.bike.u32"), b"wbike-cch")?;
        write_file(&root.join("step8").join("cch.d.car.u32"), b"dcar-cch")?;
        // Future #84 traffic-customised file: pack must accept it
        // without knowing what `car_p3` means.
        write_file(&root.join("step8").join("cch.w.car_p3.u32"), b"wcarp3")?;

        Ok(tmp)
    }

    #[test]
    fn pack_synth_then_inspect() -> Result<()> {
        let tmp = synth_dir()?;
        let out = tmp.path().join("test.butterfly");
        pack(tmp.path(), &out, None, None)?;
        let c = Container::open(&out)?;

        // shared global tables
        assert!(c.get("shared/step1.nodes.sa").is_some());
        assert!(c.get("shared/step1.nodes.si").is_some());
        assert!(c.get("shared/step1.ways.raw").is_some());
        assert!(c.get("shared/step1.relations.raw").is_some());
        // node_signals optional, missing is OK
        assert!(c.get("shared/step1.node_signals.bin").is_none());
        assert!(c.get("shared/nbg.csr").is_some());
        assert!(c.get("shared/ebg.nodes").is_some());

        // mode bundles (sorted alphabetically by mode)
        assert_eq!(c.list_modes(), vec!["bike".to_string(), "car".to_string()]);
        let way_attrs: Vec<&str> = c
            .iter_kind(SectionKind::WayAttrs)
            .map(|s| s.name.as_str())
            .collect();
        assert_eq!(way_attrs, vec!["mode/bike/way_attrs", "mode/car/way_attrs"]);

        // Lifted ordering must NOT appear under any mode bundle.
        let orders: Vec<&str> = c
            .iter_kind(SectionKind::OrderEbg)
            .map(|s| s.name.as_str())
            .collect();
        assert_eq!(orders, vec!["mode/bike/order", "mode/car/order"]);

        // sections_with_prefix walks bundles cleanly.
        let car_sections: Vec<&str> = c
            .sections_with_prefix("mode/car/")
            .map(|s| s.name.as_str())
            .collect();
        assert!(car_sections.contains(&"mode/car/topo"));
        assert!(car_sections.contains(&"mode/car/weights.time"));
        assert!(car_sections.contains(&"mode/car/order"));

        // Manifest is present and parseable as JSON-ish (we don't pull
        // in serde_json just for this assertion; substring is enough).
        let manifest = c.get(MANIFEST_NAME).expect("manifest missing");
        let mbytes = c.read_section_verified(&out, manifest)?;
        let mtxt = std::str::from_utf8(&mbytes).unwrap();
        assert!(mtxt.contains("\"modes\""));
        assert!(mtxt.contains("\"car\""));
        assert!(mtxt.contains("\"bike\""));

        // CRCs verify end-to-end.
        c.verify_file_crc(&out)?;
        for sec in &c.sections {
            let bytes = c.read_section_verified(&out, sec)?;
            if sec.name == "shared/step1.nodes.sa" {
                assert_eq!(&bytes, b"sa-bytes");
            }
        }
        Ok(())
    }

    #[test]
    fn inspect_runs_clean() -> Result<()> {
        let tmp = synth_dir()?;
        let out = tmp.path().join("test.butterfly");
        pack(tmp.path(), &out, None, None)?;
        // No assertions on stdout here; we just want the call path to
        // not panic on a real pack output.
        inspect(&out, true, true)?;
        Ok(())
    }

    #[test]
    fn unpack_is_byte_for_byte_round_trip() -> Result<()> {
        let tmp = synth_dir()?;
        let container = tmp.path().join("rt.butterfly");
        pack(tmp.path(), &container, None, None)?;

        let unpacked = tmp.path().join("rt-out");
        unpack(&container, &unpacked)?;

        // Spot-check a handful of files for byte equality.
        let pairs: &[(&str, &str)] = &[
            ("step1/nodes.sa", "step1/nodes.sa"),
            ("step1/ways.raw", "step1/ways.raw"),
            ("step2/way_attrs.car.bin", "step2/way_attrs.car.bin"),
            ("step2/turn_rules.bike.bin", "step2/turn_rules.bike.bin"),
            ("step5/filtered.car.ebg", "step5/filtered.car.ebg"),
            ("step6/order.bike.ebg", "step6/order.bike.ebg"),
            ("step7/cch.car.topo", "step7/cch.car.topo"),
            ("step8/cch.w.car.u32", "step8/cch.w.car.u32"),
        ];
        for (src, dst) in pairs {
            let original = fs::read(tmp.path().join(src))?;
            let restored = fs::read(unpacked.join(dst))?;
            assert_eq!(original, restored, "byte mismatch for {} ↔ {}", src, dst);
        }

        // Files that pack skipped (lifted) must NOT show up in the
        // unpacked tree.
        assert!(!unpacked.join("step6/order.lifted.car.ebg").exists());
        Ok(())
    }

    #[test]
    fn unpack_refuses_existing_dir() -> Result<()> {
        let tmp = synth_dir()?;
        let container = tmp.path().join("rt.butterfly");
        pack(tmp.path(), &container, None, None)?;

        let existing = tmp.path().join("already-here");
        fs::create_dir_all(&existing)?;
        let res = unpack(&container, &existing);
        assert!(res.is_err());
        Ok(())
    }
}
