//! Boot phase 1: the container section accessors + the shared graph
//! tables every mode reads (#578).
//!
//! Split out of the 980-line `load_from_container_with_options` along
//! its own phase banners. The phase is self-contained: it takes the
//! opened container and hands back the tables the later phases consume.

use anyhow::Result;
use std::sync::Arc;

use crate::formats::butterfly_dat::Container;
use crate::formats::lazy_verify::LazyContainer;
use crate::formats::{
    EbgCsr, EbgCsrFile, EbgNodes, EbgNodesFile, NbgGeo, NbgGeoFile, NbgNodeMapFile,
};

/// The three handles every boot phase needs to resolve a container
/// section: the section table, the live mmap, and the lazy-CRC gate.
///
/// Replaces the three closures the monolithic loader captured
/// (`section_arc` / `section_bytes` / `optional_section`) with the same
/// three operations as methods, so the phase functions can be plain
/// free functions.
pub(super) struct Sections<'a> {
    pub container: &'a Container,
    pub mmap: &'a Arc<memmap2::Mmap>,
    pub lazy: &'a Arc<LazyContainer>,
}

impl<'a> Sections<'a> {
    pub fn new(
        container: &'a Container,
        mmap: &'a Arc<memmap2::Mmap>,
        lazy: &'a Arc<LazyContainer>,
    ) -> Self {
        Self {
            container,
            mmap,
            lazy,
        }
    }

    /// Returns `(Arc<Mmap>, byte_offset, byte_len)` for the
    /// `read_from_mmap_unverified` path. Cloning the Arc is cheap
    /// (atomic inc). Each format reader holds its own clone so the
    /// mapping stays alive as long as any reader does — when
    /// `ServerState` drops, every reader's `ArcCow` drops, refcount
    /// hits 0, `munmap` fires.
    pub fn arc(&self, name: &str) -> Result<(Arc<memmap2::Mmap>, usize, usize)> {
        let entry = self
            .container
            .get(name)
            .ok_or_else(|| anyhow::anyhow!("missing required section '{}'", name))?;
        let off = entry.offset as usize;
        let len = entry.len as usize;
        // Use checked_add so a malformed container with
        // pathologically large offset+len cannot wrap usize and
        // bypass the bounds check.
        let _end = off.checked_add(len).ok_or_else(|| {
            anyhow::anyhow!(
                "section '{}' offset+len overflows usize (off={}, len={})",
                name,
                off,
                len
            )
        })?;
        anyhow::ensure!(
            off + len <= self.mmap.len(),
            "section '{}' bytes [{},{}) exceed mmap len {}",
            name,
            off,
            off + len,
            self.mmap.len()
        );
        // Drive lazy CRC verification through LazyContainer. The
        // first call to `verify_now` walks the section body once;
        // subsequent calls observe `Verified` and short-circuit.
        // This both updates `butterfly_route_sections_*` metrics
        // and lets format readers skip their own body CRC walk
        // via the `_unverified` entry points.
        self.lazy.verify_now(name)?;
        Ok((Arc::clone(self.mmap), off, len))
    }

    /// Byte-slice accessor borrowed from the live `Arc<Mmap>`.
    /// Used by `madvise(DONTNEED)` callers and the non-zero-copy
    /// readers that still consume `&[u8]` directly (NbgGeoFile,
    /// WaysFile, way_attrs, mod_weights).
    pub fn bytes(&self, name: &str) -> Result<&'a [u8]> {
        let entry = self
            .container
            .get(name)
            .ok_or_else(|| anyhow::anyhow!("missing required section '{}'", name))?;
        let off = entry.offset as usize;
        let len = entry.len as usize;
        anyhow::ensure!(
            off + len <= self.mmap.len(),
            "section '{}' bytes [{},{}) exceed mmap len {}",
            name,
            off,
            off + len,
            self.mmap.len()
        );
        self.lazy.verify_now(name)?;
        Ok(&self.mmap[off..off + len])
    }

    /// Like [`Self::bytes`] but `Ok(None)` when the section is absent.
    pub fn optional(&self, name: &str) -> Result<Option<&'a [u8]>> {
        match self.container.get(name) {
            Some(entry) => {
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
                    off + len <= self.mmap.len(),
                    "section '{}' bytes [{},{}) exceed mmap len {}",
                    name,
                    off,
                    off + len,
                    self.mmap.len()
                );
                self.lazy.verify_now(name)?;
                Ok(Some(&self.mmap[off..off + len]))
            }
            None => Ok(None),
        }
    }
}

/// The mode-independent graph tables: the edge-based graph, the NBG
/// geometry, and the NBG→OSM node id map.
pub(super) struct SharedTables {
    pub ebg_nodes: EbgNodes,
    pub ebg_csr: EbgCsr,
    pub nbg_geo: NbgGeo,
    /// Whether the container carries the #155 flat edge-geometry
    /// sections. Decided here (it drives the edges-only NBG geo read)
    /// and reused by the auxiliary phase, which must take the same
    /// branch.
    pub has_flat_edge_geom: bool,
    pub nbg_node_to_osm: Vec<i64>,
}

/// Load the shared graph tables from an opened container.
pub(super) fn load_shared_tables(sec: &Sections<'_>) -> Result<SharedTables> {
    // #152: ebg.nodes / ebg.csr are now read zero-copy. The
    // numeric arrays (`nodes`, `offsets`, `heads`, `turn_idx`)
    // borrow straight from the mmap, so we save ~250 MB of heap
    // on Belgium that the legacy owning-Vec readers used to copy.
    tracing::info!("Loading EBG nodes (zero-copy)...");
    // #161: LazyContainer already CRC-verified the section bytes;
    // skip the per-format CRC walk to avoid paging the body twice.
    let (m, off, len) = sec.arc("shared/ebg.nodes")?;
    let ebg_nodes = EbgNodesFile::read_from_mmap_unverified(m, off, len)?;
    tracing::info!(nodes = ebg_nodes.n_nodes, "loaded EBG nodes");

    tracing::info!("Loading EBG CSR (zero-copy)...");
    let (m, off, len) = sec.arc("shared/ebg.csr")?;
    let ebg_csr_bytes = &sec.mmap[off..off + len];
    let ebg_csr = EbgCsrFile::read_from_mmap_unverified(m, off, len)?;
    tracing::info!(arcs = ebg_csr.n_arcs, "loaded EBG CSR");
    // #152: ebg.csr is build/validate-only at serve time. The only
    // field any handler reads is `n_arcs` (a u64 in the header used
    // by /health). The body arrays (offsets, heads, turn_idx) are
    // touched by validate/step4 + ordering/contraction, none of
    // which run on the serve path. Drop the file pages from RSS;
    // the borrowed ArcCow slices stay valid (the Arc<Mmap> is still
    // alive) and a rare cold reader pages them back at fault cost.
    if let Err(e) = crate::formats::mmap::madvise_dontneed(ebg_csr_bytes) {
        tracing::warn!(
            section = "shared/ebg.csr",
            error = %e,
            "madvise(DONTNEED) on ebg.csr failed; ignoring"
        );
    } else {
        tracing::info!(
            section = "shared/ebg.csr",
            bytes = ebg_csr_bytes.len(),
            "madvise(DONTNEED) on cold ebg.csr section"
        );
    }

    // ---- NBG geo ----
    // If the container carries the flat edge geometry sections (#155),
    // we read NBG geo edges-only and let the polyline body stay on
    // disk. The new sections back the serve-path geometry hot
    // consumers; nothing downstream reads `nbg_geo.polylines` once
    // EdgeGeometry is wired below.
    let nbg_geo_section = sec.bytes("shared/nbg.geo")?;
    let has_flat_edge_geom = sec.container.get("shared/edge_geom_offsets").is_some()
        && sec.container.get("shared/edge_geom_points").is_some();
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
    let nbg_node_map = NbgNodeMapFile::read_map_from_bytes(sec.bytes("shared/nbg.node_map")?)?;
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

    Ok(SharedTables {
        ebg_nodes,
        ebg_csr,
        nbg_geo,
        has_flat_edge_geom,
        nbg_node_to_osm,
    })
}
