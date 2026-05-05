//! Cross-region overlay container (#91 Phase 2).
//!
//! The overlay is a single `.butterfly` container that holds:
//!
//! - **Border-node table** — every (region, ebg_node) endpoint of an
//!   extracted cross-region edge. Section kind
//!   [`SectionKind::OverlayBorderNodes`].
//! - **Crossings table** — pairs of border nodes (one per region) plus
//!   the haversine distance between them. Section kind
//!   [`SectionKind::OverlayCrossings`].
//! - **Per-(src, dst, mode) matrix** — row-major `[u32]` of CCH P2P
//!   weights from each border node in the src region to each border
//!   node in the dst region. Section kind [`SectionKind::OverlayMatrix`].
//! - **Manifest** — JSON with the region list, mode list, build
//!   provenance hash, and the per-matrix shape so a reader can locate
//!   each section without scanning the directory. Section kind
//!   [`SectionKind::OverlayManifest`].
//!
//! The container reuses [`crate::formats::butterfly_dat`] so it gets the
//! same CRC, alignment, and mmap guarantees as the per-region road
//! container.
//!
//! # In-memory representation
//!
//! [`OverlayCluster`] is the loaded shape. Border nodes are split per
//! region for O(1) "give me region X's border nodes" lookups. The
//! matrix is stored per `(src_region, dst_region, mode)` triple as a
//! row-major flat `Vec<u32>` so the cross-region coordinator can pluck
//! `dist[src_idx * n_dst_borders + dst_idx]` in cache-friendly order.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use super::border::BorderCrossing;
use super::query::CchQuery;
use super::state::ServerState;
use crate::formats::butterfly_dat::{Container, ContainerWriter, SectionKind};
use crate::profile_abi::Mode;

/// Region id type. Owned String so the overlay can outlive any borrowed
/// reference to the road state's region table.
pub type RegionId = String;

/// Index into [`OverlayCluster::borders`] for a region. Used as a
/// per-region row/column coordinate in the overlay matrix.
pub type BorderIdx = u32;

/// One border-node endpoint inside a region. Kept in a per-region
/// `Vec<BorderNode>`; its position in the vec is its `BorderIdx`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BorderNode {
    /// Original EBG node id in the region. Cross-region routing turns
    /// this into a CCH rank via `ModeData::orig_to_rank`.
    pub ebg_node: u32,
    /// Latitude of the snap sample used to extract this border node
    /// (degrees). Stored so the cross-region path can stitch geometry
    /// without re-snapping.
    pub lat: f64,
    /// Longitude of the snap sample (degrees).
    pub lon: f64,
}

/// One canonical cross-region crossing: A and B border-node indices in
/// their respective regions, plus the haversine traversal cost.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Crossing {
    /// Index into `OverlayCluster::borders[region_a]`.
    pub a_idx: BorderIdx,
    /// Index into `OverlayCluster::borders[region_b]`.
    pub b_idx: BorderIdx,
    /// Haversine distance, metres.
    pub edge_distance_m: f64,
}

/// Loaded overlay cluster.
///
/// `borders[r]` is the per-region border-node table, indexed by
/// `BorderIdx`.
///
/// `crossings[(a, b)]` is the canonical (a < b lexicographically) list
/// of crossings between regions `a` and `b`. The lookup is symmetric:
/// pass either ordering, the load path stores only the canonical one
/// but exposes both orderings via [`OverlayCluster::crossings_between`].
///
/// `matrices[(src, dst, mode)]` is a row-major
/// `[BorderIdx_src][BorderIdx_dst]` flat array. `u32::MAX` = unreachable.
#[derive(Debug)]
pub struct OverlayCluster {
    pub region_order: Vec<RegionId>,
    pub modes: Vec<String>,
    pub borders: HashMap<RegionId, Vec<BorderNode>>,
    pub crossings: HashMap<(RegionId, RegionId), Vec<Crossing>>,
    /// `(src_region, dst_region, mode_name)` → row-major flat matrix.
    pub matrices: HashMap<(RegionId, RegionId, String), Vec<u32>>,
}

impl OverlayCluster {
    /// Borrow the canonical (a ≤ b) crossing list for an unordered
    /// region pair. Returns an empty slice if the pair has no crossings.
    pub fn crossings_between(&self, a: &str, b: &str) -> &[Crossing] {
        if a == b {
            return &[];
        }
        let key_canon = if a <= b {
            (a.to_string(), b.to_string())
        } else {
            (b.to_string(), a.to_string())
        };
        self.crossings
            .get(&key_canon)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// Borrow the row-major matrix for `(src, dst, mode)` if it exists.
    pub fn matrix(&self, src: &str, dst: &str, mode: &str) -> Option<&[u32]> {
        self.matrices
            .get(&(src.to_string(), dst.to_string(), mode.to_string()))
            .map(|v| v.as_slice())
    }

    /// Borrow the per-region border-node table.
    pub fn region_borders(&self, region: &str) -> &[BorderNode] {
        self.borders
            .get(region)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// Number of regions in the overlay.
    pub fn n_regions(&self) -> usize {
        self.region_order.len()
    }
}

/// On-disk manifest. JSON-serialised as the `OverlayManifest` section.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OverlayManifest {
    pub version: u32,
    pub region_order: Vec<String>,
    pub modes: Vec<String>,
    /// Per-region border-node count. Used at load time to slice the
    /// flat `OverlayBorderNodes` body into per-region tables.
    pub border_counts: HashMap<String, u32>,
    /// Number of canonical crossings (rows in `OverlayCrossings`).
    pub n_crossings: u32,
    /// Build provenance: SHA-256 (truncated to 16 bytes hex) of the
    /// border-node table content. Useful as a sanity check against
    /// stale per-region containers, since regenerating per-region data
    /// would change the EBG node id space and invalidate the overlay.
    pub provenance: String,
}

const OVERLAY_MANIFEST_VERSION: u32 = 1;

/// On-disk record for one border node. 24 bytes. We do **not** persist
/// the region id here — the manifest's `border_counts` slices the body
/// into per-region runs in `region_order`.
#[repr(C)]
#[derive(Debug, Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
struct BorderNodeRecord {
    ebg_node: u32,
    _pad0: u32,
    lat_e7: i32,
    lon_e7: i32,
    _pad1: u64,
}

const _: () = assert!(std::mem::size_of::<BorderNodeRecord>() == 24);

/// On-disk record for one canonical crossing. 24 bytes.
#[repr(C)]
#[derive(Debug, Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
struct CrossingRecord {
    region_a_idx: u32,
    region_b_idx: u32,
    a_border_idx: u32,
    b_border_idx: u32,
    edge_distance_m: f64,
}

const _: () = assert!(std::mem::size_of::<CrossingRecord>() == 24);

/// Build an [`OverlayCluster`] entirely in memory from a list of
/// regions and the extracted border crossings.
///
/// The matrix-build step runs CCH P2P queries per `(src_region,
/// dst_region, mode)` triple. Each call costs `n_src_borders ×
/// n_dst_borders` bidirectional Dijkstra searches; on Belgium ↔
/// Luxembourg this is ~14 k × 14 k × n_modes ≈ **2 × 10⁹** queries per
/// mode, which takes hours and is expected to be done offline. The
/// runtime hot path consults the prebuilt matrix.
pub fn build_overlay_in_memory(
    regions: &[(RegionId, Arc<ServerState>)],
    crossings: &[BorderCrossing],
    modes: &[String],
) -> Result<OverlayCluster> {
    // ---- Group border nodes by region ------------------------------
    // Use a deterministic order: regions sorted by id. Border nodes
    // within each region are indexed in first-seen order so the
    // resulting BorderIdx is stable across overlay rebuilds.
    let mut region_order: Vec<RegionId> = regions.iter().map(|(id, _)| id.clone()).collect();
    region_order.sort();
    region_order.dedup();

    let mut borders: HashMap<RegionId, Vec<BorderNode>> = HashMap::new();
    let mut node_idx: HashMap<(RegionId, u32), BorderIdx> = HashMap::new();

    let record_endpoint =
        |region: &str,
         node_a: u32,
         lat: f64,
         lon: f64,
         borders: &mut HashMap<RegionId, Vec<BorderNode>>,
         node_idx: &mut HashMap<(RegionId, u32), BorderIdx>| {
            let key = (region.to_string(), node_a);
            if let std::collections::hash_map::Entry::Vacant(e) = node_idx.entry(key.clone()) {
                let region_borders = borders.entry(region.to_string()).or_default();
                let idx = region_borders.len() as BorderIdx;
                region_borders.push(BorderNode {
                    ebg_node: node_a,
                    lat,
                    lon,
                });
                e.insert(idx);
            }
        };

    for c in crossings {
        record_endpoint(
            &c.region_a,
            c.node_a,
            c.lat_a,
            c.lon_a,
            &mut borders,
            &mut node_idx,
        );
        record_endpoint(
            &c.region_b,
            c.node_b,
            c.lat_b,
            c.lon_b,
            &mut borders,
            &mut node_idx,
        );
    }

    // ---- Build crossings map (canonical region pair as key) --------
    let mut canon_crossings: HashMap<(RegionId, RegionId), Vec<Crossing>> = HashMap::new();
    for c in crossings {
        let (canon_a_id, canon_b_id, swap) = if c.region_a <= c.region_b {
            (c.region_a.clone(), c.region_b.clone(), false)
        } else {
            (c.region_b.clone(), c.region_a.clone(), true)
        };
        let a_idx = if swap {
            node_idx[&(c.region_b.clone(), c.node_b)]
        } else {
            node_idx[&(c.region_a.clone(), c.node_a)]
        };
        let b_idx = if swap {
            node_idx[&(c.region_a.clone(), c.node_a)]
        } else {
            node_idx[&(c.region_b.clone(), c.node_b)]
        };
        canon_crossings
            .entry((canon_a_id, canon_b_id))
            .or_default()
            .push(Crossing {
                a_idx,
                b_idx,
                edge_distance_m: c.edge_distance_m,
            });
    }

    // Deterministic intra-key ordering: by (a_idx, b_idx).
    for v in canon_crossings.values_mut() {
        v.sort_by_key(|c| (c.a_idx, c.b_idx));
    }

    // ---- Build per-(src, dst, mode) matrix --------------------------
    let mut matrices: HashMap<(RegionId, RegionId, String), Vec<u32>> = HashMap::new();

    let region_state: HashMap<RegionId, Arc<ServerState>> = regions
        .iter()
        .map(|(id, s)| (id.clone(), Arc::clone(s)))
        .collect();

    for src_region in &region_order {
        for dst_region in &region_order {
            if src_region == dst_region {
                continue;
            }
            let src_state = region_state
                .get(src_region)
                .ok_or_else(|| anyhow::anyhow!("missing state for region {}", src_region))?;
            let dst_state = region_state
                .get(dst_region)
                .ok_or_else(|| anyhow::anyhow!("missing state for region {}", dst_region))?;

            let src_borders = borders.get(src_region).map(|v| v.as_slice()).unwrap_or(&[]);
            let dst_borders = borders.get(dst_region).map(|v| v.as_slice()).unwrap_or(&[]);

            for mode_name in modes {
                let src_mode_idx =
                    src_state
                        .mode_lookup
                        .get(mode_name)
                        .copied()
                        .ok_or_else(|| {
                            anyhow::anyhow!(
                                "region {} does not carry mode {}",
                                src_region,
                                mode_name
                            )
                        })?;
                let dst_mode_idx =
                    dst_state
                        .mode_lookup
                        .get(mode_name)
                        .copied()
                        .ok_or_else(|| {
                            anyhow::anyhow!(
                                "region {} does not carry mode {}",
                                dst_region,
                                mode_name
                            )
                        })?;
                // matrix[i][j] = src_state.cch(border_i_rank → ???). We
                // need *every* border in the src region to *every*
                // border in the src region, then the inter-region edge
                // is folded by the coordinator. Wait — that's wrong.
                //
                // The matrix shape is `n_src × n_dst` because:
                //   d(s_in_src → t_in_dst) = min over (i, j) of
                //       d(s → src_border_i)            [src CCH]
                //     + d(src_border_i → dst_border_j) [matrix entry]
                //     + d(dst_border_j → t)            [dst CCH]
                //
                // The middle term decomposes further:
                //   d(src_border_i → dst_border_j) = min over crossings (k_i, k_j) of
                //       d(src_border_i → src_border_k_i)  [src CCH]
                //     + edge_k                            [haversine cost]
                //     + d(dst_border_k_j → dst_border_j)  [dst CCH]
                //
                // Precomputing the middle term means: for each src
                // border_i, run 1-to-N CCH P2P to all *other* src
                // borders (call it L_src[i][k]); for each dst border_j,
                // run 1-to-N CCH P2P from all dst borders k' to j
                // (call it L_dst[k'][j]); then matrix[i][j] = min over
                // crossings of L_src[i][k] + edge[k] + L_dst[k'][j].
                //
                // We pre-compute L_src and L_dst as the matrices; the
                // coordinator then combines them with the crossings
                // table at query time. The "matrix" stored on disk is
                // therefore the per-(src, mode) "border × border in
                // same region" table — *not* an inter-region matrix.
                //
                // BUT — as written below, we store an n_src × n_dst
                // dense matrix that already folds the crossings in.
                // That's what minimizes runtime work (one lookup per
                // (src_border, dst_border) pair) at the cost of a much
                // bigger build. For small overlays (≤100 border nodes
                // per region) that's fine; for BE↔LU (≈14k borders)
                // we should switch to the L_src/L_dst shape. We do the
                // dense shape for correctness now and document the
                // tradeoff in design.md.
                let mut row_major = vec![u32::MAX; src_borders.len() * dst_borders.len()];

                if src_borders.is_empty() || dst_borders.is_empty() {
                    matrices.insert(
                        (src_region.clone(), dst_region.clone(), mode_name.clone()),
                        row_major,
                    );
                    continue;
                }

                // Find the canonical crossings list for this region pair.
                let pair_key = if src_region <= dst_region {
                    (src_region.clone(), dst_region.clone())
                } else {
                    (dst_region.clone(), src_region.clone())
                };
                let pair_crossings = canon_crossings
                    .get(&pair_key)
                    .map(|v| v.as_slice())
                    .unwrap_or(&[]);

                if pair_crossings.is_empty() {
                    matrices.insert(
                        (src_region.clone(), dst_region.clone(), mode_name.clone()),
                        row_major,
                    );
                    continue;
                }

                // L_src: for every src border i, distance to every src
                // border k. Same metric as the routing weights —
                // deciseconds for time. Run a single CchQuery per src
                // border (1-to-N).
                let src_query = CchQuery::new(src_state, Mode(src_mode_idx));
                let dst_query = CchQuery::new(dst_state, Mode(dst_mode_idx));
                let src_mode_data = src_state.get_mode(Mode(src_mode_idx));
                let dst_mode_data = dst_state.get_mode(Mode(dst_mode_idx));

                // Translate every src border ebg_node → src CCH rank.
                let src_ranks: Vec<u32> = src_borders
                    .iter()
                    .map(|b| src_mode_data.orig_to_rank[b.ebg_node as usize])
                    .collect();
                let dst_ranks: Vec<u32> = dst_borders
                    .iter()
                    .map(|b| dst_mode_data.orig_to_rank[b.ebg_node as usize])
                    .collect();

                // For each crossing (a_idx in src, b_idx in dst), pre-extract
                // the (rank_in_src, rank_in_dst, edge_cost_dsec).
                //
                // We need to know which side of the crossing belongs to which
                // region. The canonical key is (min, max), so:
                //   if src_region == pair_key.0, then a_idx is in src
                //   else                              a_idx is in dst
                let (src_is_canon_a, _swap) = if src_region <= dst_region {
                    (true, false)
                } else {
                    (false, true)
                };

                struct ResolvedCrossing {
                    src_border_idx: BorderIdx,
                    src_rank: u32,
                    dst_border_idx: BorderIdx,
                    dst_rank: u32,
                    /// Inter-region cost in deciseconds (for time metric)
                    /// or millimetres (for distance). For now we use a
                    /// simple haversine-→-time conversion at 5 km/h
                    /// (foot) / 25 km/h (bike) / 50 km/h (car); see
                    /// build_inter_region_cost.
                    cost_dsec: u32,
                }

                let mut resolved: Vec<ResolvedCrossing> = Vec::with_capacity(pair_crossings.len());
                for c in pair_crossings {
                    let (sb_idx, db_idx) = if src_is_canon_a {
                        (c.a_idx, c.b_idx)
                    } else {
                        (c.b_idx, c.a_idx)
                    };
                    if (sb_idx as usize) >= src_ranks.len() || (db_idx as usize) >= dst_ranks.len()
                    {
                        continue;
                    }
                    let sr = src_ranks[sb_idx as usize];
                    let dr = dst_ranks[db_idx as usize];
                    if sr == u32::MAX || dr == u32::MAX {
                        continue;
                    }
                    resolved.push(ResolvedCrossing {
                        src_border_idx: sb_idx,
                        src_rank: sr,
                        dst_border_idx: db_idx,
                        dst_rank: dr,
                        cost_dsec: build_inter_region_cost(c.edge_distance_m, mode_name),
                    });
                }

                // L_src[i][k] = src CCH dist from src_border_i to src_border_k
                let mut l_src: Vec<u32> = vec![u32::MAX; src_borders.len() * resolved.len()];
                let resolved_src_ranks: Vec<u32> = resolved.iter().map(|r| r.src_rank).collect();
                for i in 0..src_borders.len() {
                    let i_rank = src_ranks[i];
                    if i_rank == u32::MAX {
                        continue;
                    }
                    let row = src_query.distances_one_to_many(i_rank, &resolved_src_ranks);
                    for (k, d) in row.into_iter().enumerate() {
                        l_src[i * resolved.len() + k] = d.unwrap_or(u32::MAX);
                    }
                }

                // L_dst[k'][j] = dst CCH dist from dst_border_k' to dst_border_j.
                // We compute from dst_border_k' to every dst_border_j.
                let mut l_dst: Vec<u32> = vec![u32::MAX; resolved.len() * dst_borders.len()];
                let resolved_dst_ranks: Vec<u32> = resolved.iter().map(|r| r.dst_rank).collect();
                for (kp, k_rank) in resolved_dst_ranks.iter().enumerate() {
                    if *k_rank == u32::MAX {
                        continue;
                    }
                    let row = dst_query.distances_one_to_many(*k_rank, &dst_ranks);
                    for (j, d) in row.into_iter().enumerate() {
                        l_dst[kp * dst_borders.len() + j] = d.unwrap_or(u32::MAX);
                    }
                }

                // Combine: matrix[i][j] = min over k of
                //     L_src[i][k] + cost[k] + L_dst[k][j]
                let n_dst = dst_borders.len();
                let n_k = resolved.len();
                for i in 0..src_borders.len() {
                    for j in 0..n_dst {
                        let mut best = u32::MAX;
                        for k in 0..n_k {
                            let l1 = l_src[i * n_k + k];
                            let l2 = l_dst[k * n_dst + j];
                            if l1 == u32::MAX || l2 == u32::MAX {
                                continue;
                            }
                            let c = resolved[k].cost_dsec;
                            let cand = l1.saturating_add(c).saturating_add(l2);
                            if cand < best {
                                best = cand;
                            }
                        }
                        let _ = resolved[0].src_border_idx; // silence unused
                        let _ = resolved[0].dst_border_idx; // silence unused
                        row_major[i * n_dst + j] = best;
                    }
                }

                matrices.insert(
                    (src_region.clone(), dst_region.clone(), mode_name.clone()),
                    row_major,
                );
            }
        }
    }

    Ok(OverlayCluster {
        region_order,
        modes: modes.to_vec(),
        borders,
        crossings: canon_crossings,
        matrices,
    })
}

/// Convert haversine metres → mode-specific deciseconds for the
/// inter-region traversal cost. This is a fixed-speed approximation
/// because the crossing edge isn't in any region's CCH; we conservatively
/// assume the lowest typical speed for the mode.
fn build_inter_region_cost(distance_m: f64, mode: &str) -> u32 {
    let speed_mps = match mode {
        "foot" => 1.4,   // ~5 km/h
        "bike" => 4.2,   // ~15 km/h (urban)
        "truck" => 11.1, // ~40 km/h
        _ => 13.9,       // ~50 km/h (car default)
    };
    let secs = distance_m / speed_mps;
    let dsec = (secs * 10.0).ceil();
    if dsec.is_finite() && dsec >= 0.0 && dsec <= u32::MAX as f64 {
        dsec as u32
    } else {
        u32::MAX
    }
}

impl OverlayCluster {
    /// Persist this overlay to a `.butterfly` container.
    pub fn write_to_path(&self, path: &Path) -> Result<()> {
        let mut writer = ContainerWriter::create(path).context("creating overlay container")?;

        // ---- Manifest ------------------------------------------------
        let mut border_counts = HashMap::new();
        let mut all_border_records: Vec<BorderNodeRecord> = Vec::new();
        // Iterate per region in `region_order` so the on-disk body is
        // segmented in a deterministic order.
        for region in &self.region_order {
            let region_borders = self
                .borders
                .get(region)
                .map(|v| v.as_slice())
                .unwrap_or(&[]);
            border_counts.insert(region.clone(), region_borders.len() as u32);
            for b in region_borders {
                all_border_records.push(BorderNodeRecord {
                    ebg_node: b.ebg_node,
                    _pad0: 0,
                    lat_e7: (b.lat * 1e7).round() as i32,
                    lon_e7: (b.lon * 1e7).round() as i32,
                    _pad1: 0,
                });
            }
        }

        // Crossings: enumerate canonical pairs in deterministic order.
        let mut all_crossing_records: Vec<CrossingRecord> = Vec::new();
        let region_idx: HashMap<&str, u32> = self
            .region_order
            .iter()
            .enumerate()
            .map(|(i, r)| (r.as_str(), i as u32))
            .collect();
        let mut canon_pairs: Vec<&(RegionId, RegionId)> = self.crossings.keys().collect();
        canon_pairs.sort();
        for pair in canon_pairs {
            let recs = &self.crossings[pair];
            for c in recs {
                all_crossing_records.push(CrossingRecord {
                    region_a_idx: region_idx[pair.0.as_str()],
                    region_b_idx: region_idx[pair.1.as_str()],
                    a_border_idx: c.a_idx,
                    b_border_idx: c.b_idx,
                    edge_distance_m: c.edge_distance_m,
                });
            }
        }

        let provenance = compute_provenance(&all_border_records);
        let manifest = OverlayManifest {
            version: OVERLAY_MANIFEST_VERSION,
            region_order: self.region_order.clone(),
            modes: self.modes.clone(),
            border_counts,
            n_crossings: all_crossing_records.len() as u32,
            provenance,
        };
        let manifest_bytes = serde_json::to_vec(&manifest)?;

        writer.append_bytes(
            SectionKind::OverlayManifest,
            "overlay/manifest.json",
            &manifest_bytes,
        )?;

        let border_bytes: &[u8] = bytemuck::cast_slice(&all_border_records);
        writer.append_bytes(
            SectionKind::OverlayBorderNodes,
            "overlay/border_nodes",
            border_bytes,
        )?;

        let crossing_bytes: &[u8] = bytemuck::cast_slice(&all_crossing_records);
        writer.append_bytes(
            SectionKind::OverlayCrossings,
            "overlay/crossings",
            crossing_bytes,
        )?;

        // Matrices, one section per (src, dst, mode).
        let mut keys: Vec<&(RegionId, RegionId, String)> = self.matrices.keys().collect();
        keys.sort();
        for key in keys {
            let m = &self.matrices[key];
            let name = format!("overlay/matrix/{}/{}/{}", key.0, key.1, key.2);
            let bytes: &[u8] = bytemuck::cast_slice(m);
            writer.append_bytes(SectionKind::OverlayMatrix, name, bytes)?;
        }

        writer.finalize()?;
        Ok(())
    }

    /// Load an overlay container from disk.
    pub fn load(path: &Path) -> Result<Arc<Self>> {
        let container = Container::open(path).context("opening overlay container")?;

        // ---- Manifest ------------------------------------------------
        let manifest_entry = container
            .get("overlay/manifest.json")
            .ok_or_else(|| anyhow::anyhow!("overlay container missing manifest"))?;
        let manifest_bytes = container.read_section_verified(path, manifest_entry)?;
        let manifest: OverlayManifest = serde_json::from_slice(&manifest_bytes)?;
        anyhow::ensure!(
            manifest.version == OVERLAY_MANIFEST_VERSION,
            "overlay manifest version mismatch (got {}, expected {})",
            manifest.version,
            OVERLAY_MANIFEST_VERSION
        );

        // ---- Border nodes -------------------------------------------
        let border_entry = container
            .get("overlay/border_nodes")
            .ok_or_else(|| anyhow::anyhow!("overlay container missing border_nodes"))?;
        let border_bytes = container.read_section_verified(path, border_entry)?;
        let border_records: &[BorderNodeRecord] = bytemuck::cast_slice(&border_bytes);

        let mut borders: HashMap<RegionId, Vec<BorderNode>> = HashMap::new();
        let mut cursor = 0usize;
        for region in &manifest.region_order {
            let n =
                *manifest.border_counts.get(region).ok_or_else(|| {
                    anyhow::anyhow!("manifest missing border count for {}", region)
                })? as usize;
            let slice = &border_records[cursor..cursor + n];
            let region_borders: Vec<BorderNode> = slice
                .iter()
                .map(|r| BorderNode {
                    ebg_node: r.ebg_node,
                    lat: r.lat_e7 as f64 / 1e7,
                    lon: r.lon_e7 as f64 / 1e7,
                })
                .collect();
            borders.insert(region.clone(), region_borders);
            cursor += n;
        }

        // ---- Crossings ----------------------------------------------
        let crossing_entry = container
            .get("overlay/crossings")
            .ok_or_else(|| anyhow::anyhow!("overlay container missing crossings"))?;
        let crossing_bytes = container.read_section_verified(path, crossing_entry)?;
        let crossing_records: &[CrossingRecord] = bytemuck::cast_slice(&crossing_bytes);

        let mut canon_crossings: HashMap<(RegionId, RegionId), Vec<Crossing>> = HashMap::new();
        for r in crossing_records {
            let a = manifest
                .region_order
                .get(r.region_a_idx as usize)
                .ok_or_else(|| anyhow::anyhow!("crossing has out-of-range region_a_idx"))?
                .clone();
            let b = manifest
                .region_order
                .get(r.region_b_idx as usize)
                .ok_or_else(|| anyhow::anyhow!("crossing has out-of-range region_b_idx"))?
                .clone();
            canon_crossings.entry((a, b)).or_default().push(Crossing {
                a_idx: r.a_border_idx,
                b_idx: r.b_border_idx,
                edge_distance_m: r.edge_distance_m,
            });
        }

        // ---- Matrices ----------------------------------------------
        let mut matrices: HashMap<(RegionId, RegionId, String), Vec<u32>> = HashMap::new();
        for sec in container.iter_kind(SectionKind::OverlayMatrix) {
            // Name shape: "overlay/matrix/<src>/<dst>/<mode>"
            let parts: Vec<&str> = sec.name.split('/').collect();
            anyhow::ensure!(
                parts.len() == 5 && parts[0] == "overlay" && parts[1] == "matrix",
                "unexpected matrix section name: {}",
                sec.name
            );
            let src = parts[2].to_string();
            let dst = parts[3].to_string();
            let mode = parts[4].to_string();

            let bytes = container.read_section_verified(path, sec)?;
            // Body is a flat [u32]. Length must equal n_src × n_dst.
            anyhow::ensure!(
                bytes.len() % 4 == 0,
                "matrix section {} has non-u32-aligned body",
                sec.name
            );
            let m: Vec<u32> = bytes
                .chunks_exact(4)
                .map(|c| u32::from_le_bytes([c[0], c[1], c[2], c[3]]))
                .collect();
            let n_src = borders.get(&src).map(|v| v.len()).unwrap_or(0);
            let n_dst = borders.get(&dst).map(|v| v.len()).unwrap_or(0);
            anyhow::ensure!(
                m.len() == n_src * n_dst,
                "matrix section {} length {} != n_src {} × n_dst {}",
                sec.name,
                m.len(),
                n_src,
                n_dst
            );
            matrices.insert((src, dst, mode), m);
        }

        Ok(Arc::new(OverlayCluster {
            region_order: manifest.region_order,
            modes: manifest.modes,
            borders,
            crossings: canon_crossings,
            matrices,
        }))
    }
}

fn compute_provenance(records: &[BorderNodeRecord]) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    for r in records {
        h.update(r.ebg_node.to_le_bytes());
        h.update(r.lat_e7.to_le_bytes());
        h.update(r.lon_e7.to_le_bytes());
    }
    let digest = h.finalize();
    digest[..16].iter().map(|b| format!("{:02x}", b)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    fn synth_cluster() -> OverlayCluster {
        let mut borders: HashMap<RegionId, Vec<BorderNode>> = HashMap::new();
        borders.insert(
            "A".to_string(),
            vec![
                BorderNode {
                    ebg_node: 100,
                    lat: 49.5,
                    lon: 5.5,
                },
                BorderNode {
                    ebg_node: 101,
                    lat: 49.6,
                    lon: 5.6,
                },
            ],
        );
        borders.insert(
            "B".to_string(),
            vec![BorderNode {
                ebg_node: 200,
                lat: 49.5001,
                lon: 5.5001,
            }],
        );

        let mut crossings = HashMap::new();
        crossings.insert(
            ("A".to_string(), "B".to_string()),
            vec![Crossing {
                a_idx: 0,
                b_idx: 0,
                edge_distance_m: 12.34,
            }],
        );

        let mut matrices = HashMap::new();
        matrices.insert(
            ("A".to_string(), "B".to_string(), "car".to_string()),
            vec![100u32, 200u32],
        );
        matrices.insert(
            ("B".to_string(), "A".to_string(), "car".to_string()),
            vec![300u32, 400u32],
        );

        OverlayCluster {
            region_order: vec!["A".to_string(), "B".to_string()],
            modes: vec!["car".to_string()],
            borders,
            crossings,
            matrices,
        }
    }

    #[test]
    fn roundtrip_overlay_container() -> Result<()> {
        let cluster = synth_cluster();
        let tmp = NamedTempFile::new()?;
        cluster.write_to_path(tmp.path())?;

        let loaded = OverlayCluster::load(tmp.path())?;
        assert_eq!(loaded.region_order, cluster.region_order);
        assert_eq!(loaded.modes, cluster.modes);
        assert_eq!(loaded.borders["A"].len(), 2);
        assert_eq!(loaded.borders["B"].len(), 1);
        assert_eq!(loaded.borders["A"][0].ebg_node, 100);
        assert_eq!(loaded.borders["B"][0].ebg_node, 200);
        let cs = loaded.crossings_between("A", "B");
        assert_eq!(cs.len(), 1);
        assert_eq!(cs[0].a_idx, 0);
        assert_eq!(cs[0].b_idx, 0);
        let m = loaded.matrix("A", "B", "car").expect("A→B car");
        assert_eq!(m, &[100u32, 200u32]);
        let m_rev = loaded.matrix("B", "A", "car").expect("B→A car");
        assert_eq!(m_rev, &[300u32, 400u32]);
        Ok(())
    }

    #[test]
    fn crossings_between_is_symmetric() {
        let cluster = synth_cluster();
        let ab = cluster.crossings_between("A", "B");
        let ba = cluster.crossings_between("B", "A");
        assert_eq!(ab.len(), 1);
        assert_eq!(ab.len(), ba.len());
        // Same key (A, B) under either ordering.
        assert_eq!(ab[0].a_idx, ba[0].a_idx);
    }

    #[test]
    fn missing_matrix_is_none() {
        let cluster = synth_cluster();
        assert!(cluster.matrix("A", "C", "car").is_none());
        assert!(cluster.matrix("A", "B", "bike").is_none());
    }

    #[test]
    fn inter_region_cost_uses_mode_speed() {
        let car = build_inter_region_cost(100.0, "car");
        let foot = build_inter_region_cost(100.0, "foot");
        // Foot is slower so cost should be higher.
        assert!(foot > car);
        let zero = build_inter_region_cost(0.0, "car");
        assert_eq!(zero, 0);
    }
}
