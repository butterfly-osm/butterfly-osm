//! Boot phase 3: the packed snap index (#154), split out of the
//! monolithic loader in #578.
//!
//! One shared point array + uniform-grid CSR + per-mode bitmaps. The
//! container path reads the prebuilt sections zero-copy; the
//! directory-tree path (and containers that pre-date #154) build the
//! same structure in heap memory.

use anyhow::{Context, Result};

use super::modes::ModeTables;
use super::shared::{Sections, SharedTables};
use crate::server::snap_index::{
    DEFAULT_CELL_LOG2, PackedSnapIndex, SnapBuilderMode, build_snap_index,
};

/// Build a packed snap index in heap memory from the loaded EBG + NBG
/// + per-mode masks. Used by:
///   - the directory-tree loader (always),
///   - the container loader's back-compat path when the new sections
///     are absent.
///
/// The resulting masks are aligned to `modes.names`, i.e. local-mode
/// position in `modes.data`. On the container path with the prebuilt
/// sections, `modes.names` order matches the container's mode-section
/// emission order, which matches the global mode-byte alphabetical
/// order — see [`try_load_packed_snap_index`] for the constraint.
pub(super) fn build_packed_snap_index_inmem(
    ebg_nodes: &crate::formats::EbgNodes,
    nbg_geo: &crate::formats::NbgGeo,
    modes: &ModeTables,
) -> PackedSnapIndex {
    let builder_modes: Vec<SnapBuilderMode<'_>> = modes
        .datas()
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
        n_modes = modes.names.len(),
        "snap index built in memory"
    );
    PackedSnapIndex {
        points: built.points,
        grid: built.grid,
        masks: built.masks,
    }
}

/// Container boot phase: prefer the mmap-backed sections, fall back to
/// building the legacy structure in heap memory when the container
/// pre-dates #154.
pub(super) fn load_or_build(
    sec: &Sections<'_>,
    shared: &SharedTables,
    modes: &ModeTables,
) -> Result<PackedSnapIndex> {
    let idx = match try_load_packed_snap_index(sec, &modes.names)? {
        Some(idx) => {
            tracing::info!(
                n_points = idx.n_indexed(),
                "loaded packed snap index zero-copy"
            );
            crate::server::rss::checkpoint("spatial.global");
            for name in &modes.names {
                crate::server::rss::checkpoint(&format!("spatial.mode.{}", name));
            }
            idx
        }
        None => {
            tracing::warn!(
                "packed snap index sections missing; building rstar at boot \
                     (this container pre-dates #154 — re-pack to drop ~1 GB anon)"
            );
            let idx = build_packed_snap_index_inmem(&shared.ebg_nodes, &shared.nbg_geo, modes);
            crate::server::rss::checkpoint("spatial.global");
            for name in &modes.names {
                crate::server::rss::checkpoint(&format!("spatial.mode.{}", name));
            }
            idx
        }
    };
    Ok(idx)
}

/// Try to load a packed snap index zero-copy from a container.
/// Returns `Ok(None)` if any of the required sections is missing —
/// caller falls back to the in-memory builder.
///
/// #160: per-section CRC verification is gated by the `LazyContainer`
/// in `ServerState`, not here. We only resolve byte ranges; body
/// pages stay cold until snap-index queries traverse them (or warmup
/// walks them off the request path).
fn try_load_packed_snap_index(
    sec: &Sections<'_>,
    mode_names: &[String],
) -> Result<Option<PackedSnapIndex>> {
    use crate::formats::snap_index::{SnapGridFile, SnapMaskFile, SnapPointsFile};

    let container = sec.container;
    let mmap = sec.mmap;
    let lazy = sec.lazy;

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
