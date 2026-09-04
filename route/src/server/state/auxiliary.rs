//! Boot phase 4: the auxiliary data — road names, per-edge exclude
//! flags, distance node weights, elevation, flat edge geometry and the
//! per-edge OSM id chains (#578).
//!
//! Everything here is read once at boot and never re-read on the query
//! path. Split out of the monolithic loader along its own phase
//! banners; the `rss::checkpoint` calls stay with the code they measure.
//!
//! The file is named `auxiliary.rs`, not `aux.rs`: `AUX` is a reserved
//! device name on Windows and a file called `aux.rs` cannot be checked
//! out there.

use anyhow::{Context, Result};
use std::collections::HashMap;
use std::path::Path;

use super::shared::{Sections, SharedTables};
use super::{WayNames, exclude};
use crate::formats::WaysFile;
use crate::server::edge_geom::EdgeGeometry;
use crate::server::elevation::ElevationData;

/// The boot-time auxiliary tables, in the order the loader builds them.
pub(super) struct AuxData {
    pub way_names: WayNames,
    pub edge_exclude_flags: Vec<u8>,
    pub node_weights_dist: Vec<u32>,
    pub elevation: Option<ElevationData>,
    pub edge_geom: EdgeGeometry,
    pub edge_osm: crate::server::edge_osm::EdgeOsmChains,
}

/// Container boot phase: load every auxiliary table.
pub(super) fn load_auxiliary(
    sec: &Sections<'_>,
    shared: &SharedTables,
    discovered_modes: &[String],
    container_path: &Path,
) -> Result<AuxData> {
    let ebg_nodes = &shared.ebg_nodes;
    let container = sec.container;
    let mmap_for_bytes = sec.mmap;

    // ---- Road names ---------------------------------------------
    // #282: prefer the compact mmap-backed `shared/way_names_idx`
    // section (~5-10 KB heap on Belgium, scales to ~3 GiB saved at
    // planet scale). Fall back to the legacy
    // `shared/step1.ways.raw` HashMap build (~30-50 MB heap on
    // Belgium) for containers that pre-date the index.
    tracing::info!("Loading road names from container...");
    // PR #324 review: do NOT call `verify_now` here. A synchronous
    // verify walks the full ~19 MiB way_names_idx body, paging it in
    // at boot, which defeats the demand-paged-mmap goal of the lazy
    // index. The lazy header (magic / version / sizes) is still
    // validated by `read_from_mmap_unverified` below; the body CRC
    // stays deferred. Operators that want eager body CRC can opt in
    // via `--warmup-on-boot` or `--eager-verify`, which the
    // existing `LazyContainer::spawn_warmup` path already covers.
    let way_names = if let Some(entry) = container.get("shared/way_names_idx") {
        let off = entry.offset as usize;
        let len = entry.len as usize;
        let idx = crate::formats::way_names_idx::read_from_mmap_unverified(
            std::sync::Arc::clone(mmap_for_bytes),
            off,
            len,
        )?;
        tracing::info!(
            source = "shared/way_names_idx",
            named_roads = idx.len(),
            "loaded road names (mmap-backed, body CRC deferred to warmup/eager flags)"
        );
        WayNames::Idx(idx)
    } else if let Some(ways_bytes) = sec.optional("shared/step1.ways.raw")? {
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
    let edge_exclude_flags = if let Some(attr_bytes) = sec.optional(&attrs_section)? {
        let attrs = crate::formats::way_attrs::read_all_from_bytes(attr_bytes)?;
        let flags = exclude::build_edge_exclude_flags_from_attrs(ebg_nodes, &attrs)?;
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
    // `mmap_for_bytes[..]` directly. Do NOT route through
    // `Sections::optional(..)` because that calls
    // `lazy.verify_now(name)`, which would force a full CRC walk
    // (and page-in) of every other mode's way_attrs at boot —
    // defeating lazy verification. `madvise(MADV_DONTNEED)` is
    // safe on unverified bytes: it evicts resident pages (a no-op
    // on pages that were never faulted in).
    for other_mode in discovered_modes {
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

    // #297: EBG `length_m` is now metres (was `length_mm`).
    let node_weights_dist: Vec<u32> = ebg_nodes.nodes.iter().map(|n| n.length_m).collect();

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
    // The dispatch matches the `has_flat_edge_geom` check the shared
    // phase used for the NBG geo edges-only loader: if the sections
    // existed at open time they're still there now, so the back-compat
    // branch is for old containers that loaded the full NbgGeo.
    let edge_geom = if shared.has_flat_edge_geom {
        let eg = try_load_edge_geometry(sec)?.ok_or_else(|| {
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
        EdgeGeometry::from_legacy_polylines(&shared.nbg_geo)
    };
    crate::server::rss::checkpoint("load.edge_geom");

    // #460: per-edge OSM node id chains — optional sections (absent
    // in pre-#460 containers; edges_flow then emits NBG-endpoint
    // rows and logs once).
    let edge_osm = match try_load_edge_osm(sec)? {
        Some(chains) => {
            tracing::info!("loaded per-edge OSM id chains zero-copy (#460)");
            chains
        }
        None => {
            tracing::warn!(
                "edge_osm sections missing — edges_flow emits NBG-endpoint rows \
                 (re-run step3 + pack for per-OSM-segment expansion, #460)"
            );
            crate::server::edge_osm::EdgeOsmChains::empty()
        }
    };
    crate::server::rss::checkpoint("load.edge_osm");

    Ok(AuxData {
        way_names,
        edge_exclude_flags,
        node_weights_dist,
        elevation,
        edge_geom,
        edge_osm,
    })
}

/// Find the best way_attrs file for exclude flags (directory-tree path).
/// Prefers "car" if available, otherwise uses the first available mode.
pub(super) fn find_way_attrs_path(
    step2_dir: &Path,
    modes: &[String],
) -> Option<std::path::PathBuf> {
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

/// Load road names from ways.raw (step1 output).
/// Uses streaming to avoid loading all way data into memory at once.
/// Returns way_id → name mapping for all ways that have a "name" tag.
pub(super) fn load_way_names(step1_dir: &Path) -> Result<HashMap<i64, String>> {
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

/// Same as [`load_way_names`] but reads from an in-memory ways.raw byte
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

/// #460: load the optional per-edge OSM id chain sections. `Ok(None)`
/// when the container pre-dates them.
fn try_load_edge_osm(sec: &Sections<'_>) -> Result<Option<crate::server::edge_osm::EdgeOsmChains>> {
    use crate::formats::edge_osm::{EdgeOsmIdsFile, EdgeOsmOffsetsFile};

    let container = sec.container;
    let mmap = sec.mmap;

    let off_entry = match container.get("shared/edge_osm_offsets") {
        Some(e) => e,
        None => return Ok(None),
    };
    let ids_entry = match container.get("shared/edge_osm_ids") {
        Some(e) => e,
        None => return Ok(None),
    };
    sec.lazy.verify_now("shared/edge_osm_offsets")?;
    sec.lazy.verify_now("shared/edge_osm_ids")?;

    let off = EdgeOsmOffsetsFile::read_from_mmap_unverified(
        std::sync::Arc::clone(mmap),
        off_entry.offset as usize,
        off_entry.len as usize,
    )
    .with_context(|| "reading shared/edge_osm_offsets zero-copy")?;
    let ids = EdgeOsmIdsFile::read_from_mmap_unverified(
        std::sync::Arc::clone(mmap),
        ids_entry.offset as usize,
        ids_entry.len as usize,
    )
    .with_context(|| "reading shared/edge_osm_ids zero-copy")?;

    let chains = crate::server::edge_osm::EdgeOsmChains::from_sections(off, ids)
        .with_context(|| "stitching edge_osm sections")?;
    Ok(Some(chains))
}

/// Try to load the flat edge geometry sections (#155) zero-copy from a
/// container. Returns `Ok(None)` if either section is missing — caller
/// falls back to building from the heap polylines.
///
/// #160: per-section CRC verification is gated by `LazyContainer`.
fn try_load_edge_geometry(sec: &Sections<'_>) -> Result<Option<EdgeGeometry>> {
    use crate::formats::edge_geom::{EdgeGeomOffsetsFile, EdgeGeomPointsFile};

    let container = sec.container;
    let mmap = sec.mmap;

    let off_entry = match container.get("shared/edge_geom_offsets") {
        Some(e) => e,
        None => return Ok(None),
    };
    let pts_entry = match container.get("shared/edge_geom_points") {
        Some(e) => e,
        None => return Ok(None),
    };
    // #161: drive lazy CRC verification through LazyContainer.
    sec.lazy.verify_now("shared/edge_geom_offsets")?;
    sec.lazy.verify_now("shared/edge_geom_points")?;

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
