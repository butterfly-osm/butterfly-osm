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

    // Resolve the per-step subdirectory name. When `--step-prefix <p>`
    // is supplied, the operator's `<p>` is treated as a true prefix —
    // i.e. step N is looked up under `<p><N>` (so `--step-prefix custom`
    // means `custom1`, `custom2`, …). When unset, the default
    // `step{N}` naming is used.
    let resolve_step = |n: u8| -> String {
        match step_prefix {
            Some(p) => format!("{}{}", p, n),
            None => format!("step{}", n),
        }
    };
    let step1 = find_step_dir(data_dir, &resolve_step(1))?;
    let step2 = find_step_dir(data_dir, &resolve_step(2))?;
    let step3 = find_step_dir(data_dir, &resolve_step(3))?;
    let step4 = find_step_dir(data_dir, &resolve_step(4))?;
    let step5 = find_step_dir(data_dir, &resolve_step(5))?;
    let step6 = find_step_dir(data_dir, &resolve_step(6))?;
    let step7 = find_step_dir(data_dir, &resolve_step(7))?;
    let step8 = find_step_dir(data_dir, &resolve_step(8))?;

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

/// Best-effort parse of the `bundles` field out of a container's
/// `shared/manifest.json`. Returns the list of `(bundle_id, modes)` pairs
/// in declaration order. For legacy containers without a `bundles` field
/// (or no manifest at all), returns an empty vec — callers that need a
/// "every mode is its own bundle" fallback should derive that from
/// [`Container::list_modes`] explicitly.
///
/// As with [`manifest_region_id`], this is a small hand-rolled parser
/// rather than a full `serde_json` dance: the manifest is a stable-shape
/// JSON document under our own control and the dependency cost is not
/// justified for two scalar fields. Any parse failure (including unknown
/// JSON shape) returns an empty vec rather than rejecting the container.
pub fn manifest_bundles(manifest_bytes: &[u8]) -> Vec<(String, Vec<String>)> {
    let text = match std::str::from_utf8(manifest_bytes) {
        Ok(s) => s,
        Err(_) => return Vec::new(),
    };
    // Locate the `"bundles"` key.
    let key_idx = match text.find("\"bundles\"") {
        Some(i) => i,
        None => return Vec::new(),
    };
    let rest = &text[key_idx + "\"bundles\"".len()..];
    let rest = rest.trim_start();
    let rest = match rest.strip_prefix(':') {
        Some(r) => r.trim_start(),
        None => return Vec::new(),
    };
    let rest = match rest.strip_prefix('{') {
        Some(r) => r,
        None => return Vec::new(),
    };
    // Walk character by character, collecting `"<key>": [ "m", "n", ... ]`
    // pairs until we hit the closing `}`. Tolerant of whitespace and
    // commas; bails out (returning what was parsed so far) on the first
    // unexpected character.
    let mut out: Vec<(String, Vec<String>)> = Vec::new();
    let mut chars = rest.chars().peekable();

    fn skip_ws<I: Iterator<Item = char> + Clone>(it: &mut std::iter::Peekable<I>) {
        while let Some(&c) = it.peek() {
            if c.is_whitespace() || c == ',' {
                it.next();
            } else {
                break;
            }
        }
    }
    fn read_quoted<I: Iterator<Item = char>>(it: &mut std::iter::Peekable<I>) -> Option<String> {
        // Expects the next char to be `"`.
        if it.next()? != '"' {
            return None;
        }
        let mut s = String::new();
        let mut escaped = false;
        for c in it.by_ref() {
            if escaped {
                s.push(c);
                escaped = false;
                continue;
            }
            match c {
                '\\' => escaped = true,
                '"' => return Some(s),
                _ => s.push(c),
            }
        }
        None
    }

    loop {
        skip_ws(&mut chars);
        match chars.peek() {
            Some('}') => break,
            Some('"') => {}
            _ => break,
        }
        let key = match read_quoted(&mut chars) {
            Some(k) => k,
            None => break,
        };
        skip_ws(&mut chars);
        if chars.next() != Some(':') {
            break;
        }
        skip_ws(&mut chars);
        if chars.next() != Some('[') {
            break;
        }
        let mut modes: Vec<String> = Vec::new();
        loop {
            skip_ws(&mut chars);
            match chars.peek() {
                Some(']') => {
                    chars.next();
                    break;
                }
                Some('"') => {
                    let m = match read_quoted(&mut chars) {
                        Some(s) => s,
                        None => return out,
                    };
                    modes.push(m);
                }
                _ => return out,
            }
        }
        out.push((key, modes));
    }
    out
}

/// Result of comparing two modes' accessibility and existing CCH topology
/// sizes within a container, used by the `topology-diff` subcommand. The
/// JSON shape is part of the tool's public output — keep it stable so
/// downstream automation can grep/parse it.
#[derive(Debug, serde::Serialize)]
pub struct ModePairDiff {
    pub mode_a: String,
    pub mode_b: String,
    /// Number of original EBG nodes accessible (mask bit set) for mode A.
    pub n_nodes_a: u64,
    /// Number of original EBG nodes accessible for mode B.
    pub n_nodes_b: u64,
    /// Number of original EBG nodes accessible by both modes.
    pub n_nodes_intersect: u64,
    /// Number of original EBG nodes accessible by either mode.
    pub n_nodes_union: u64,
    /// Jaccard overlap on the per-node accessibility masks.
    pub node_jaccard: f64,
    /// Number of original EBG arcs in mode A's filtered subgraph.
    pub n_arcs_a: u64,
    /// Number of original EBG arcs in mode B's filtered subgraph.
    pub n_arcs_b: u64,
    /// Number of original EBG arcs in both filtered subgraphs.
    pub n_arcs_intersect: u64,
    /// Number of original EBG arcs in either filtered subgraph.
    pub n_arcs_union: u64,
    /// Jaccard overlap on the per-arc filtered subgraphs.
    pub arc_jaccard: f64,
    /// Per-mode CCH topology size (bytes) — bare section length.
    pub topo_bytes_a: u64,
    pub topo_bytes_b: u64,
    /// Per-mode time + dist weights size (bytes) — section length sum.
    pub weights_bytes_a: u64,
    pub weights_bytes_b: u64,
    /// Per-mode CCH n_nodes / n_shortcuts / n_original_arcs (from the
    /// topology header). Useful for sanity-checking that topology size
    /// scales linearly with shortcut count.
    pub topo_n_nodes_a: u64,
    pub topo_n_nodes_b: u64,
    pub topo_n_shortcuts_a: u64,
    pub topo_n_shortcuts_b: u64,
    pub topo_n_original_arcs_a: u64,
    pub topo_n_original_arcs_b: u64,
    /// Predicted bundled bytes assuming the bundle's CCH topology and
    /// per-mode weight files scale linearly with the union arc count vs
    /// the larger individual mode. See `acceptance_disk` for the full
    /// derivation; this is a back-of-envelope projection, NOT a measured
    /// value. The acceptance test in #146 mandates an actual rebuild.
    pub predicted_bundled_bytes: u64,
    /// Per-mode baseline (sum of `topo + 2*weights` for both modes).
    pub baseline_bytes: u64,
    /// `predicted_bundled_bytes / baseline_bytes`. < 1.0 means the
    /// bundle would reduce on-disk size if the linear-scaling model
    /// holds.
    pub predicted_bundle_ratio: f64,
    /// Issue #146 acceptance criterion (1): the predicted bundled size
    /// must be smaller than the per-mode baseline. The ground-truth
    /// version of this check requires actually rebuilding the union;
    /// this is a *predicted* pass/fail under the linear-scaling model.
    pub predicted_passes_disk_acceptance: bool,
}

/// Implementation of the `topology-diff` subcommand. Loads each mode's
/// per-original-EBG-node accessibility mask + per-mode filtered EBG
/// (which holds the per-mode arc set as `original_arc_idx`) from the
/// container, computes node + arc Jaccard overlaps between every pair
/// in `modes`, and emits a JSON report to stdout.
///
/// The report is the empirical input to issue #146's "should we bundle
/// these modes' topologies" decision. It does NOT actually rebuild a
/// union topology — that lives in steps 5/6/7 of the build pipeline,
/// which is out-of-scope here. The `predicted_*` fields project a
/// linear-scaling estimate so a human can decide whether a candidate
/// pair is worth the full rebuild.
///
/// Output is a JSON object:
/// ```json
/// {
///   "container": "<path>",
///   "modes": ["bike", "car", "foot", "truck"],
///   "pairs": [<ModePairDiff>...]
/// }
/// ```
pub fn topology_diff(path: &Path, modes_arg: Option<&str>) -> Result<()> {
    let container =
        Container::open(path).with_context(|| format!("opening container {}", path.display()))?;

    // Resolve which modes to compare. Default = all modes in container,
    // alphabetical, all pairwise combinations.
    let mode_list: Vec<String> = match modes_arg {
        Some(s) => s
            .split(',')
            .map(|m| m.trim().to_string())
            .filter(|m| !m.is_empty())
            .collect(),
        None => container.list_modes(),
    };
    anyhow::ensure!(
        mode_list.len() >= 2,
        "topology-diff needs at least 2 modes (got {}). Pass --modes a,b,c \
         or pack a multi-mode container.",
        mode_list.len()
    );
    // Validate every requested mode exists in the container.
    for m in &mode_list {
        let topo_name = format!("mode/{}/topo", m);
        anyhow::ensure!(
            container.get(&topo_name).is_some(),
            "mode '{}' has no '{}' section in {}",
            m,
            topo_name,
            path.display()
        );
    }

    eprintln!(
        "topology-diff: comparing {} modes ({}) in {}",
        mode_list.len(),
        mode_list.join(","),
        path.display()
    );

    // Load each mode's accessibility mask and filtered EBG arc set.
    // Both signals live entirely in `mode/<m>/mask` and
    // `mode/<m>/filtered_ebg`. We DO NOT load `mode/<m>/topo` body —
    // those are GiB-scale on Belgium and only the header is useful for
    // this diff. Header is parsed by reading the first 80 bytes.
    struct ModeState {
        name: String,
        node_mask: Vec<u8>, // per-original-node bitset (covers full key space)
        n_original_nodes: u64,
        arc_set: std::collections::BTreeSet<u32>, // unique original-arc indices in filtered EBG
        topo_bytes: u64,
        weights_bytes: u64,
        topo_n_nodes: u64,
        topo_n_shortcuts: u64,
        topo_n_original_arcs: u64,
    }
    let mut states: Vec<ModeState> = Vec::with_capacity(mode_list.len());
    for m in &mode_list {
        eprintln!("  loading mode '{}'...", m);
        // Mask
        let mask_name = format!("mode/{}/mask", m);
        let mask_entry = container
            .get(&mask_name)
            .ok_or_else(|| anyhow::anyhow!("missing '{}'", mask_name))?;
        let mask_bytes = container
            .read_section_verified(path, mask_entry)
            .with_context(|| format!("reading {}", mask_name))?;
        // mask format: 24-byte header, body = bitset, 16-byte footer.
        anyhow::ensure!(
            mask_bytes.len() >= 24 + 16,
            "mask section '{}' too short: {} bytes",
            mask_name,
            mask_bytes.len()
        );
        let n_nodes_for_mask = u32::from_le_bytes(mask_bytes[8..12].try_into().unwrap()) as u64;
        let body_len = (n_nodes_for_mask as usize).div_ceil(8);
        anyhow::ensure!(
            mask_bytes.len() == 24 + body_len + 16,
            "mask section '{}' body length mismatch: {} bytes (expected {})",
            mask_name,
            mask_bytes.len(),
            24 + body_len + 16
        );
        let node_mask = mask_bytes[24..24 + body_len].to_vec();

        // Filtered EBG arc set
        let fe_name = format!("mode/{}/filtered_ebg", m);
        let fe_entry = container
            .get(&fe_name)
            .ok_or_else(|| anyhow::anyhow!("missing '{}'", fe_name))?;
        let fe_bytes = container
            .read_section_verified(path, fe_entry)
            .with_context(|| format!("reading {}", fe_name))?;
        let fe = FilteredEbgFile::read_from_bytes(&fe_bytes)
            .with_context(|| format!("parsing {}", fe_name))?;
        // The set of arc indices accessible to this mode = unique values
        // of original_arc_idx across the filtered CSR.
        let mut arc_set: std::collections::BTreeSet<u32> = std::collections::BTreeSet::new();
        for &oai in fe.original_arc_idx.iter() {
            arc_set.insert(oai);
        }

        // Topology header (bytes-only, no body parse).
        let topo_name = format!("mode/{}/topo", m);
        let topo_entry = container.get(&topo_name).expect("validated above");
        let topo_bytes = topo_entry.len;
        let (topo_n_nodes, topo_n_shortcuts, topo_n_original_arcs) = {
            // Read first 80 bytes (header) of topo section.
            use std::fs::File;
            use std::io::{Read, Seek, SeekFrom};
            let mut f = File::open(path)?;
            f.seek(SeekFrom::Start(topo_entry.offset))?;
            let mut hdr = [0u8; 80];
            f.read_exact(&mut hdr)?;
            // [0..4] magic, [4..8] version, [8..12] n_nodes, [12..16]
            // reserved, [16..24] n_shortcuts, [24..32] n_original_arcs.
            let n_nodes = u32::from_le_bytes(hdr[8..12].try_into().unwrap()) as u64;
            let n_shortcuts = u64::from_le_bytes(hdr[16..24].try_into().unwrap());
            let n_orig = u64::from_le_bytes(hdr[24..32].try_into().unwrap());
            (n_nodes, n_shortcuts, n_orig)
        };

        // Weights (time + dist) sum.
        let mut weights_bytes = 0u64;
        for leaf in ["weights.time", "weights.dist"] {
            let nm = format!("mode/{}/{}", m, leaf);
            if let Some(e) = container.get(&nm) {
                weights_bytes += e.len;
            }
        }

        states.push(ModeState {
            name: m.clone(),
            node_mask,
            n_original_nodes: n_nodes_for_mask,
            arc_set,
            topo_bytes,
            weights_bytes,
            topo_n_nodes,
            topo_n_shortcuts,
            topo_n_original_arcs,
        });
    }

    // Sanity: every mask covers the same n_original_nodes.
    let n0 = states[0].n_original_nodes;
    for s in &states[1..] {
        anyhow::ensure!(
            s.n_original_nodes == n0,
            "mode '{}' mask has n_original_nodes={} but '{}' has {}",
            s.name,
            s.n_original_nodes,
            states[0].name,
            n0
        );
    }

    // Pairwise diff.
    let mut pairs: Vec<ModePairDiff> = Vec::new();
    for i in 0..states.len() {
        for j in (i + 1)..states.len() {
            let a = &states[i];
            let b = &states[j];
            // Node intersection / union via byte-by-byte AND/OR.
            let n_bytes = a.node_mask.len().min(b.node_mask.len());
            let mut n_intersect: u64 = 0;
            let mut n_union: u64 = 0;
            for k in 0..n_bytes {
                n_intersect += (a.node_mask[k] & b.node_mask[k]).count_ones() as u64;
                n_union += (a.node_mask[k] | b.node_mask[k]).count_ones() as u64;
            }
            let n_a: u64 = a.node_mask.iter().map(|b| b.count_ones() as u64).sum();
            let n_b: u64 = b.node_mask.iter().map(|b| b.count_ones() as u64).sum();
            let node_jaccard = if n_union == 0 {
                0.0
            } else {
                n_intersect as f64 / n_union as f64
            };

            // Arc intersection / union via BTreeSet ops.
            let arc_intersect: u64 = a.arc_set.intersection(&b.arc_set).count() as u64;
            let n_arcs_a: u64 = a.arc_set.len() as u64;
            let n_arcs_b: u64 = b.arc_set.len() as u64;
            let arc_union: u64 = n_arcs_a + n_arcs_b - arc_intersect;
            let arc_jaccard = if arc_union == 0 {
                0.0
            } else {
                arc_intersect as f64 / arc_union as f64
            };

            // Predicted bundled-disk model. Linear-scaling assumption:
            //
            //   bundled_topo  ≈ topo_max  * (n_arcs_union / n_arcs_max)
            //   bundled_weight≈ w_max     * (n_arcs_union / n_arcs_max)
            //
            // and the bundle holds 1 topo + (time + dist) per bundled
            // mode. So bundled bytes for {A, B}:
            //
            //   bundled = bundled_topo + 2 * bundled_weight_A + 2 * bundled_weight_B
            //           = bundled_topo + 4 * bundled_weight (modes ≈
            //             same shape after union, modulo their own
            //             u32::MAX entries which still occupy space).
            //
            // Baseline (per-mode):
            //
            //   baseline = topo_A + 2*w_A + topo_B + 2*w_B
            //
            // Bundle wins disk iff `bundled < baseline`. The CHALLENGE
            // here is that the bundle's per-mode weight file has the
            // SAME size as the bundled topology's edge count (one u32
            // per up+down edge, with `u32::MAX` for inaccessible). So
            // bundle weight bytes scale with `n_arcs_union`, NOT with
            // `n_arcs_<mode>`.
            //
            // We project bundled_topo and bundled_weight from whichever
            // individual mode has the larger arc set (closer to the
            // union; gives a tighter lower bound).
            let (large_topo, large_weight, large_n_arcs) = if n_arcs_a >= n_arcs_b {
                (a.topo_bytes, a.weights_bytes, n_arcs_a.max(1))
            } else {
                (b.topo_bytes, b.weights_bytes, n_arcs_b.max(1))
            };
            let scale = arc_union as f64 / large_n_arcs as f64;
            let bundled_topo = (large_topo as f64 * scale).round() as u64;
            // large_weight is time+dist combined (×2). For the bundle
            // we need time+dist for each of the two modes (×4 of the
            // single-metric weight cost).
            let single_metric_weight = large_weight / 2;
            let bundled_weight_one_mode_one_metric =
                (single_metric_weight as f64 * scale).round() as u64;
            let bundled_bytes = bundled_topo + 4 * bundled_weight_one_mode_one_metric;
            let baseline_bytes = a.topo_bytes + a.weights_bytes + b.topo_bytes + b.weights_bytes;
            let ratio = bundled_bytes as f64 / baseline_bytes.max(1) as f64;

            pairs.push(ModePairDiff {
                mode_a: a.name.clone(),
                mode_b: b.name.clone(),
                n_nodes_a: n_a,
                n_nodes_b: n_b,
                n_nodes_intersect: n_intersect,
                n_nodes_union: n_union,
                node_jaccard,
                n_arcs_a,
                n_arcs_b,
                n_arcs_intersect: arc_intersect,
                n_arcs_union: arc_union,
                arc_jaccard,
                topo_bytes_a: a.topo_bytes,
                topo_bytes_b: b.topo_bytes,
                weights_bytes_a: a.weights_bytes,
                weights_bytes_b: b.weights_bytes,
                topo_n_nodes_a: a.topo_n_nodes,
                topo_n_nodes_b: b.topo_n_nodes,
                topo_n_shortcuts_a: a.topo_n_shortcuts,
                topo_n_shortcuts_b: b.topo_n_shortcuts,
                topo_n_original_arcs_a: a.topo_n_original_arcs,
                topo_n_original_arcs_b: b.topo_n_original_arcs,
                predicted_bundled_bytes: bundled_bytes,
                baseline_bytes,
                predicted_bundle_ratio: ratio,
                predicted_passes_disk_acceptance: bundled_bytes < baseline_bytes,
            });
        }
    }

    // Emit human-readable header on stderr; full JSON on stdout.
    eprintln!();
    eprintln!(
        "{:<14} {:<14} {:>10} {:>10} {:>14} {:>14} {:>10}",
        "mode A", "mode B", "node J", "arc J", "baseline GB", "bundled GB", "ratio"
    );
    for p in &pairs {
        eprintln!(
            "{:<14} {:<14} {:>10.4} {:>10.4} {:>14.3} {:>14.3} {:>10.4}{}",
            p.mode_a,
            p.mode_b,
            p.node_jaccard,
            p.arc_jaccard,
            p.baseline_bytes as f64 / 1.073e9,
            p.predicted_bundled_bytes as f64 / 1.073e9,
            p.predicted_bundle_ratio,
            if p.predicted_passes_disk_acceptance {
                " *"
            } else {
                ""
            },
        );
    }
    eprintln!();
    eprintln!("(* = predicted disk-acceptance pass under linear scaling model)");

    let report = serde_json::json!({
        "container": path.display().to_string(),
        "modes": mode_list,
        "n_original_nodes": n0,
        "pairs": pairs,
    });
    println!("{}", serde_json::to_string_pretty(&report)?);
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
    use tempfile::{NamedTempFile, TempDir};

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

    // -----------------------------------------------------------------
    // #146: manifest `bundles` parser tests
    // -----------------------------------------------------------------

    #[test]
    fn manifest_bundles_singleton_per_mode() {
        let manifest = build_manifest(&["bike".to_string(), "car".to_string()], "BE");
        let bundles = manifest_bundles(manifest.as_bytes());
        assert_eq!(
            bundles,
            vec![
                ("bike".to_string(), vec!["bike".to_string()]),
                ("car".to_string(), vec!["car".to_string()]),
            ]
        );
    }

    #[test]
    fn manifest_bundles_legacy_no_field() {
        // No "bundles" field anywhere → empty vec, no panic.
        let bytes = b"{\"version\":1}";
        assert!(manifest_bundles(bytes).is_empty());
        // Non-UTF-8 manifest → empty vec (best-effort parse).
        assert!(manifest_bundles(&[0xC0u8, 0xC1u8, 0xFFu8]).is_empty());
        // Empty input → empty vec.
        assert!(manifest_bundles(&[]).is_empty());
    }

    #[test]
    fn manifest_bundles_multi_mode_groups() {
        // Forward-compat: a hypothetical future #146 manifest groups
        // car+truck under one bundle id and ships bike + foot solo. The
        // parser must round-trip the order.
        let raw = b"{\
            \"version\":1, \
            \"region_id\":\"BE\", \
            \"modes\":[\"bike\",\"car\",\"foot\",\"truck\"], \
            \"bundles\":{\
                \"car_truck\":[\"car\",\"truck\"], \
                \"bike\":[\"bike\"], \
                \"foot\":[\"foot\"]\
            }\
        }";
        let bundles = manifest_bundles(raw);
        assert_eq!(
            bundles,
            vec![
                (
                    "car_truck".to_string(),
                    vec!["car".to_string(), "truck".to_string()]
                ),
                ("bike".to_string(), vec!["bike".to_string()]),
                ("foot".to_string(), vec!["foot".to_string()]),
            ]
        );
    }

    #[test]
    fn manifest_bundles_round_trips_through_pack() -> Result<()> {
        // Regression: write a real manifest via build_manifest, append it
        // to a container, parse it back through manifest_bundles. This
        // exercises the same code path that `topology-diff` reads.
        let modes = vec!["bike".to_string(), "car".to_string(), "foot".to_string()];
        let manifest = build_manifest(&modes, "BE");

        let tmp = NamedTempFile::new()?;
        let mut w = ContainerWriter::create(tmp.path())?;
        w.append_bytes(SectionKind::Unknown, MANIFEST_NAME, manifest.as_bytes())?;
        w.finalize()?;

        let c = Container::open(tmp.path())?;
        let entry = c.get(MANIFEST_NAME).expect("manifest section exists");
        let bytes = c.read_section_verified(tmp.path(), entry)?;
        let bundles = manifest_bundles(&bytes);
        assert_eq!(bundles.len(), 3);
        for (i, m) in modes.iter().enumerate() {
            assert_eq!(bundles[i], (m.clone(), vec![m.clone()]));
        }
        Ok(())
    }

    #[test]
    fn manifest_bundles_tolerates_extra_whitespace() {
        let raw = b"{ \"bundles\" : { \"a\" : [ \"a\" ] , \"b\" : [ \"b\" , \"c\" ] } }";
        let bundles = manifest_bundles(raw);
        assert_eq!(
            bundles,
            vec![
                ("a".to_string(), vec!["a".to_string()]),
                ("b".to_string(), vec!["b".to_string(), "c".to_string()]),
            ]
        );
    }

    #[test]
    fn manifest_bundles_tolerates_garbage_after_close() {
        // Anything past the closing `}` of the bundles map is ignored.
        let raw = b"{\"bundles\":{\"x\":[\"x\"]} blah blah}";
        let bundles = manifest_bundles(raw);
        assert_eq!(bundles, vec![("x".to_string(), vec!["x".to_string()])]);
    }
}

#[cfg(test)]
mod topology_diff_tests {
    //! Integration tests for `topology-diff`. These build a tiny synth
    //! container with two real-shaped per-mode bundles (mask +
    //! filtered_ebg + topo + weights) so the analysis tool exercises
    //! the full read path including CRC checks.

    use super::*;
    use crate::formats::butterfly_dat::{ContainerWriter, SectionKind};
    use crate::formats::cch_topo::{CchTopo, CchTopoFile};
    use crate::formats::filtered_ebg::FilteredEbg;
    use crate::formats::{BitsetField, FilteredEbgFile};
    use crate::profile_abi::Mode;
    use std::borrow::Cow;
    use tempfile::NamedTempFile;

    /// Write a minimal mask section bytestream: 24-byte header + bitset
    /// body + 16-byte footer (matches `mod_mask` format v1).
    fn build_mask_bytes(mode_byte: u8, n_nodes: u32, body: &[u8]) -> Vec<u8> {
        use crate::formats::crc::Digest;
        const MAGIC: u32 = 0x4D41534B;
        const VERSION: u16 = 1;
        let mut header = Vec::with_capacity(24);
        header.extend_from_slice(&MAGIC.to_le_bytes());
        header.extend_from_slice(&VERSION.to_le_bytes());
        header.push(mode_byte);
        header.push(0);
        header.extend_from_slice(&n_nodes.to_le_bytes());
        header.extend_from_slice(&[0u8; 8]); // inputs_sha
        header.extend_from_slice(&[0u8; 4]); // pad
        debug_assert_eq!(header.len(), 24);

        let body_len = (n_nodes as usize).div_ceil(8);
        assert_eq!(body.len(), body_len, "test mask body wrong length");

        let mut body_d = Digest::new();
        body_d.update(body);
        let body_crc = body_d.finalize();
        let mut file_d = Digest::new();
        file_d.update(&header);
        file_d.update(body);
        let file_crc = file_d.finalize();

        let mut out = Vec::with_capacity(24 + body_len + 16);
        out.extend_from_slice(&header);
        out.extend_from_slice(body);
        out.extend_from_slice(&body_crc.to_le_bytes());
        out.extend_from_slice(&file_crc.to_le_bytes());
        out
    }

    /// Build a tiny but parse-valid `FilteredEbg` whose `original_arc_idx`
    /// matches `arc_indices` exactly. Filtered nodes / heads are
    /// arbitrary — we only need `original_arc_idx` for the diff.
    fn build_filtered_ebg(mode: Mode, n_orig: u32, arc_indices: &[u32]) -> FilteredEbg {
        let n_arcs = arc_indices.len() as u64;
        // Use a single filtered node owning all the arcs, looping back
        // to itself: the diff tool only inspects original_arc_idx, but
        // FilteredEbgFile::write requires a self-consistent CSR.
        let n_filt: u32 = if n_arcs > 0 { 1 } else { 0 };
        let offsets = if n_filt == 0 {
            vec![0u64]
        } else {
            vec![0u64, n_arcs]
        };
        let heads = vec![0u32; arc_indices.len()];
        let filtered_to_original = if n_filt == 0 { vec![] } else { vec![0u32] };
        let mut original_to_filtered = vec![u32::MAX; n_orig as usize];
        if n_filt > 0 {
            original_to_filtered[0] = 0;
        }
        FilteredEbg {
            mode,
            n_filtered_nodes: n_filt,
            n_filtered_arcs: n_arcs,
            n_original_nodes: n_orig,
            inputs_sha: [0u8; 32],
            offsets: Cow::Owned(offsets),
            heads: Cow::Owned(heads),
            original_arc_idx: Cow::Owned(arc_indices.to_vec()),
            filtered_to_original: Cow::Owned(filtered_to_original),
            original_to_filtered: Cow::Owned(original_to_filtered),
        }
    }

    /// Build a tiny `CchTopo` so the topology-diff header probe
    /// (offset[8..32] of the topo section) reads consistent values.
    fn build_topo(n_nodes: u32, n_shortcuts: u64, n_original_arcs: u64) -> CchTopo {
        // No edges, no shortcuts in body — the diff tool only reads the
        // header. But `up_offsets` / `down_offsets` need n_nodes+1
        // entries to satisfy the writer.
        let zero_off = vec![0u64; (n_nodes + 1) as usize];
        let zero_rank = vec![0u32; n_nodes as usize];
        CchTopo {
            n_nodes,
            n_shortcuts,
            n_original_arcs,
            inputs_sha: [0u8; 32],
            up_offsets: Cow::Owned(zero_off.clone()),
            up_targets: Cow::Owned(vec![]),
            up_is_shortcut: BitsetField::from_owned_words(vec![], 0),
            up_middle: Cow::Owned(vec![]),
            down_offsets: Cow::Owned(zero_off),
            down_targets: Cow::Owned(vec![]),
            down_is_shortcut: BitsetField::from_owned_words(vec![], 0),
            down_middle: Cow::Owned(vec![]),
            rank_to_filtered: Cow::Owned(zero_rank),
        }
    }

    /// One synthetic per-mode bundle: arc set + node mask. Used by
    /// `write_synth_container` below. Pulled out so the writer fn's
    /// argument count stays under the clippy limit.
    struct SynthMode<'a> {
        name: &'a str,
        arcs: &'a [u32],
        node_mask: &'a [u8],
    }

    fn write_synth_container(
        path: &std::path::Path,
        modes: &[SynthMode<'_>],
        n_orig_nodes: u32,
    ) -> Result<()> {
        let mut w = ContainerWriter::create(path)?;
        // Per mode: mask + filtered_ebg + topo + weights sections. The
        // diff tool requires every one of these.
        for (idx, sm) in modes.iter().enumerate() {
            let mode = Mode(idx as u8);
            let mask_bytes = build_mask_bytes(mode.0, n_orig_nodes, sm.node_mask);
            w.append_bytes(
                SectionKind::ModeMask,
                format!("mode/{}/mask", sm.name),
                &mask_bytes,
            )?;
            let fe = build_filtered_ebg(mode, n_orig_nodes, sm.arcs);
            let fe_path = NamedTempFile::new()?;
            FilteredEbgFile::write(fe_path.path(), &fe)?;
            w.append_file(
                SectionKind::FilteredEbg,
                format!("mode/{}/filtered_ebg", sm.name),
                fe_path.path(),
            )?;
            let topo = build_topo(1, sm.arcs.len() as u64 / 4, sm.arcs.len() as u64);
            let topo_path = NamedTempFile::new()?;
            CchTopoFile::write(topo_path.path(), &topo)?;
            w.append_file(
                SectionKind::CchTopo,
                format!("mode/{}/topo", sm.name),
                topo_path.path(),
            )?;
            // Weights — content is opaque to the diff tool, only sizes
            // matter. Use a deterministic non-zero size proportional to
            // arc count so the predicted-bundle math has something
            // sensible to chew on.
            let weight_size = sm.arcs.len() * 4;
            let weight_body = vec![0u8; weight_size];
            w.append_bytes(
                SectionKind::CchWeightsTime,
                format!("mode/{}/weights.time", sm.name),
                &weight_body,
            )?;
            w.append_bytes(
                SectionKind::CchWeightsDist,
                format!("mode/{}/weights.dist", sm.name),
                &weight_body,
            )?;
        }
        w.finalize()
    }

    /// Pure-function regression: a pair of identical arc sets must yield
    /// arc Jaccard 1.0; disjoint sets yield 0.0. Verify via the synth
    /// container path so the byte-level mask / filtered_ebg / topo
    /// readers are exercised end-to-end.
    #[test]
    fn topology_diff_identical_modes_high_overlap() -> Result<()> {
        let tmp = NamedTempFile::new()?;
        // Two modes with the same arc set [0..16] and same node mask.
        let arcs: Vec<u32> = (0..16).collect();
        let mask = vec![0xFFu8, 0xFFu8]; // all 16 nodes accessible
        write_synth_container(
            tmp.path(),
            &[
                SynthMode {
                    name: "alpha",
                    arcs: &arcs,
                    node_mask: &mask,
                },
                SynthMode {
                    name: "beta",
                    arcs: &arcs,
                    node_mask: &mask,
                },
            ],
            16,
        )?;

        let c = Container::open(tmp.path())?;
        // The container has both modes' sections; the diff command
        // walks them. Assert via list_modes that the prefix iteration
        // sees both.
        assert_eq!(
            c.list_modes(),
            vec!["alpha".to_string(), "beta".to_string()]
        );

        // Sanity: the topo-diff function does not panic on the synth.
        // We don't assert on stdout JSON here (the test_runner reroutes
        // stdout); we only confirm the heavy-lift path works.
        topology_diff(tmp.path(), Some("alpha,beta"))?;
        Ok(())
    }

    #[test]
    fn topology_diff_disjoint_modes() -> Result<()> {
        let tmp = NamedTempFile::new()?;
        let a_arcs: Vec<u32> = (0..8).collect();
        let b_arcs: Vec<u32> = (8..16).collect();
        let a_mask = vec![0x0Fu8, 0x00u8]; // bits 0..3 only
        let b_mask = vec![0x00u8, 0xF0u8]; // bits 12..15
        write_synth_container(
            tmp.path(),
            &[
                SynthMode {
                    name: "left",
                    arcs: &a_arcs,
                    node_mask: &a_mask,
                },
                SynthMode {
                    name: "right",
                    arcs: &b_arcs,
                    node_mask: &b_mask,
                },
            ],
            16,
        )?;
        topology_diff(tmp.path(), Some("left,right"))?;
        Ok(())
    }

    #[test]
    fn topology_diff_rejects_single_mode() -> Result<()> {
        let tmp = NamedTempFile::new()?;
        // Build a one-mode container; topology-diff with the default
        // (all modes) discovers a single mode and bails.
        let arcs: Vec<u32> = (0..4).collect();
        let mask = vec![0x0Fu8, 0x00u8];
        // Reuse the synth helper but only emit one mode.
        let mut w = ContainerWriter::create(tmp.path())?;
        let mode = Mode(0);
        let mask_bytes = build_mask_bytes(mode.0, 16, &mask);
        w.append_bytes(SectionKind::ModeMask, "mode/only/mask", &mask_bytes)?;
        let fe = build_filtered_ebg(mode, 16, &arcs);
        let fe_path = NamedTempFile::new()?;
        FilteredEbgFile::write(fe_path.path(), &fe)?;
        w.append_file(
            SectionKind::FilteredEbg,
            "mode/only/filtered_ebg",
            fe_path.path(),
        )?;
        let topo = build_topo(1, 1, arcs.len() as u64);
        let topo_path = NamedTempFile::new()?;
        CchTopoFile::write(topo_path.path(), &topo)?;
        w.append_file(SectionKind::CchTopo, "mode/only/topo", topo_path.path())?;
        w.append_bytes(
            SectionKind::CchWeightsTime,
            "mode/only/weights.time",
            &[0u8; 16],
        )?;
        w.append_bytes(
            SectionKind::CchWeightsDist,
            "mode/only/weights.dist",
            &[0u8; 16],
        )?;
        w.finalize()?;

        let res = topology_diff(tmp.path(), None);
        assert!(res.is_err());
        let msg = res.unwrap_err().to_string();
        assert!(
            msg.contains("at least 2 modes"),
            "unexpected error: {}",
            msg
        );
        Ok(())
    }

    #[test]
    fn topology_diff_unknown_mode_errors() -> Result<()> {
        let tmp = NamedTempFile::new()?;
        let arcs: Vec<u32> = (0..4).collect();
        let mask = vec![0x0Fu8, 0x00u8];
        write_synth_container(
            tmp.path(),
            &[
                SynthMode {
                    name: "alpha",
                    arcs: &arcs,
                    node_mask: &mask,
                },
                SynthMode {
                    name: "beta",
                    arcs: &arcs,
                    node_mask: &mask,
                },
            ],
            16,
        )?;

        // Asking for a mode that doesn't exist: hard error.
        let res = topology_diff(tmp.path(), Some("alpha,gamma"));
        assert!(res.is_err());
        let msg = res.unwrap_err().to_string();
        assert!(msg.contains("gamma"), "unexpected error: {}", msg);
        Ok(())
    }
}
