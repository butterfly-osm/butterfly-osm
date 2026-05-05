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
//!   weights from each *representative* border node in the src region
//!   to each *representative* border node in the dst region. Section
//!   kind [`SectionKind::OverlayMatrix`].
//! - **Per-region cluster map** — mapping from every border in the region
//!   to the index of its representative (within the same region). Used
//!   at query time to translate the chosen border into a matrix row/
//!   column. Section kind [`SectionKind::OverlayClusterMap`].
//! - **Manifest** — JSON with the region list, mode list, build
//!   provenance hash, the per-matrix shape, and the per-region count of
//!   representatives so a reader can locate each section without
//!   scanning the directory. Section kind [`SectionKind::OverlayManifest`].
//!
//! The container reuses [`crate::formats::butterfly_dat`] so it gets the
//! same CRC, alignment, and mmap guarantees as the per-region road
//! container.
//!
//! # Two-level pruning
//!
//! Before #91 Phase 2's optimisation pass, the overlay matrix was an
//! `n × m` grid where `n, m` were the full per-region border counts —
//! ~8 010 for BE+LU. That sized the build at ~7 days/mode/direction.
//! The optimisation pass clusters spatially-co-located borders into
//! "representatives" via [`super::border::prune_border_set`]. The matrix
//! is then `n_rep × m_rep` (typically ~50–200 representatives), and the
//! cluster map records which representative each non-rep border maps
//! to so the runtime coordinator can still answer queries that snap to
//! any border node.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use super::border::BorderCrossing;
use super::state::ServerState;
use crate::formats::butterfly_dat::{Container, ContainerWriter, SectionKind};
use crate::matrix::bucket_ch::table_bucket_parallel;
use crate::profile_abi::Mode;

/// Region id type. Owned String so the overlay can outlive any borrowed
/// reference to the road state's region table.
pub type RegionId = String;

/// Index into [`OverlayCluster::borders`] for a region. Used as a
/// per-region row/column coordinate in the overlay matrix.
pub type BorderIdx = u32;

/// Default merge radius for [`super::border::prune_border_set`]. Two
/// crossings within this many metres on **both** A-side and B-side are
/// merged into the same cluster. 250 m is large enough to collapse the
/// dense BE↔LU border (mostly within ~100 m of one another along the
/// same physical road) and small enough to keep cross-corridor borders
/// distinct.
pub const DEFAULT_MERGE_THRESHOLD_M: f64 = 250.0;

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
/// `cluster_maps[r]` is the per-region cluster map: for each border in
/// `borders[r]`, its index in `representatives[r]`. The matrix stores
/// distances between representatives only; non-rep borders look up
/// their representative through this map.
///
/// `representatives[r]` lists the per-region representative borders,
/// indexed by the entries of `cluster_maps[r]`. The matrix's rows
/// (resp. columns) for a `(src, dst, mode)` triple correspond to these
/// representatives, in this order.
///
/// `crossings[(a, b)]` is the canonical (a < b lexicographically) list
/// of crossings between regions `a` and `b`. The lookup is symmetric:
/// pass either ordering, the load path stores only the canonical one
/// but exposes both orderings via [`OverlayCluster::crossings_between`].
///
/// `matrices[(src, dst, mode)]` is a row-major
/// `[representatives_src.len()][representatives_dst.len()]` flat array.
/// `u32::MAX` = unreachable.
#[derive(Debug)]
pub struct OverlayCluster {
    pub region_order: Vec<RegionId>,
    pub modes: Vec<String>,
    pub borders: HashMap<RegionId, Vec<BorderNode>>,
    pub representatives: HashMap<RegionId, Vec<BorderNode>>,
    pub cluster_maps: HashMap<RegionId, Vec<u32>>,
    pub crossings: HashMap<(RegionId, RegionId), Vec<Crossing>>,
    /// `(src_region, dst_region, mode_name)` → row-major flat matrix
    /// over per-region representatives.
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
    ///
    /// The matrix indexes per-region *representatives*, not per-region
    /// *borders*. Use [`Self::cluster_map`] to translate a `BorderIdx`
    /// to its representative row/column.
    pub fn matrix(&self, src: &str, dst: &str, mode: &str) -> Option<&[u32]> {
        // Borrow-key lookup avoids the per-call `String` allocation that
        // the previous implementation incurred (Copilot finding #10).
        // We materialise a `(&str, &str, &str)` key into the equivalent
        // owned form only on miss in the slow path.
        for (k, v) in &self.matrices {
            if k.0 == src && k.1 == dst && k.2 == mode {
                return Some(v.as_slice());
            }
        }
        None
    }

    /// Borrow the per-region border-node table.
    pub fn region_borders(&self, region: &str) -> &[BorderNode] {
        self.borders
            .get(region)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// Borrow the per-region representative-border table. Each entry is
    /// a row (or column, depending on which side of the matrix the
    /// region sits) of the overlay matrix.
    pub fn region_representatives(&self, region: &str) -> &[BorderNode] {
        self.representatives
            .get(region)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// Borrow the per-region cluster map: `cluster_map(region)[i]` is
    /// the representative index in `representatives[region]` for border
    /// `i` in `borders[region]`. Empty slice if the region is unknown.
    pub fn cluster_map(&self, region: &str) -> &[u32] {
        self.cluster_maps
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
    /// Per-region representative-border count. Sliced from the same
    /// `OverlayBorderNodes` body, immediately after the per-region
    /// border block.
    #[serde(default)]
    pub representative_counts: HashMap<String, u32>,
    /// Number of canonical crossings (rows in `OverlayCrossings`).
    pub n_crossings: u32,
    /// Build provenance: SHA-256 (truncated to 16 bytes hex) of the
    /// border-node table content. Useful as a sanity check against
    /// stale per-region containers, since regenerating per-region data
    /// would change the EBG node id space and invalidate the overlay.
    pub provenance: String,
}

const OVERLAY_MANIFEST_VERSION: u32 = 2;

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
/// # Optimisations applied
///
/// - **Pruned border set** (#91 Phase 2 optimisation): the per-region
///   border list is spatially clustered via
///   [`super::border::prune_border_set`] (default radius
///   [`DEFAULT_MERGE_THRESHOLD_M`]). The matrix is built only over
///   representatives.
/// - **Batched parallel CCH**: per (src, dst, mode), a single
///   [`table_bucket_parallel`] call computes all `n_rep_src × n_rep_dst`
///   distances in one pass. The bucket-M2M algorithm runs forward
///   searches from each src rep, reverse searches from each dst rep,
///   and joins their meeting points — collapsing the previous
///   `O(n_rep_src × n_rep_dst)` independent CCH P2P calls into one
///   parallelised batch. With `n_rep ≈ 100` (BE↔LU clustering) this
///   makes the matrix-build `O(seconds)` instead of `O(weeks)`.
pub fn build_overlay_in_memory(
    regions: &[(RegionId, Arc<ServerState>)],
    crossings: &[BorderCrossing],
    modes: &[String],
) -> Result<OverlayCluster> {
    build_overlay_in_memory_with_threshold(regions, crossings, modes, DEFAULT_MERGE_THRESHOLD_M)
}

/// Same as [`build_overlay_in_memory`] but with a configurable cluster
/// merge threshold. Use 0.0 (or any value smaller than typical sample
/// spacing) to disable clustering — every border becomes its own
/// representative.
pub fn build_overlay_in_memory_with_threshold(
    regions: &[(RegionId, Arc<ServerState>)],
    crossings: &[BorderCrossing],
    modes: &[String],
    merge_threshold_m: f64,
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

    // ---- Spatial clustering: per-region cluster maps + reps --------
    //
    // The cluster map is computed *per region* by clustering that
    // region's borders alone. Each region's border list lives in
    // first-seen order (above). We feed each region's borders through
    // a thin shim onto `prune_border_set` by building synthetic
    // BorderCrossings (region-against-self) so the clustering reuses
    // the exact same haversine logic.
    let mut representatives: HashMap<RegionId, Vec<BorderNode>> = HashMap::new();
    let mut cluster_maps: HashMap<RegionId, Vec<u32>> = HashMap::new();
    for region in &region_order {
        let region_borders = borders.get(region).map(|v| v.as_slice()).unwrap_or(&[]);
        let synth: Vec<BorderCrossing> = region_borders
            .iter()
            .map(|b| BorderCrossing {
                region_a: region.clone(),
                node_a: b.ebg_node,
                lat_a: b.lat,
                lon_a: b.lon,
                region_b: region.clone(),
                node_b: b.ebg_node,
                lat_b: b.lat,
                lon_b: b.lon,
                edge_distance_m: 0.0,
            })
            .collect();
        let (rep_synth, map) = super::border::prune_border_set(&synth, merge_threshold_m);
        let reps_nodes: Vec<BorderNode> = rep_synth
            .iter()
            .map(|c| BorderNode {
                ebg_node: c.node_a,
                lat: c.lat_a,
                lon: c.lon_a,
            })
            .collect();
        representatives.insert(region.clone(), reps_nodes);
        cluster_maps.insert(region.clone(), map);
    }

    // ---- Build per-(src, dst, mode) matrix on representatives ------
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

            let src_reps = representatives
                .get(src_region)
                .map(|v| v.as_slice())
                .unwrap_or(&[]);
            let dst_reps = representatives
                .get(dst_region)
                .map(|v| v.as_slice())
                .unwrap_or(&[]);

            for mode_name in modes {
                let n_src = src_reps.len();
                let n_dst = dst_reps.len();
                if n_src == 0 || n_dst == 0 {
                    matrices.insert(
                        (src_region.clone(), dst_region.clone(), mode_name.clone()),
                        Vec::new(),
                    );
                    continue;
                }

                let src_mode_idx = *src_state.mode_lookup.get(mode_name).ok_or_else(|| {
                    anyhow::anyhow!("region {} does not carry mode {}", src_region, mode_name)
                })?;
                let dst_mode_idx = *dst_state.mode_lookup.get(mode_name).ok_or_else(|| {
                    anyhow::anyhow!("region {} does not carry mode {}", dst_region, mode_name)
                })?;
                let src_mode_data = src_state.get_mode(Mode(src_mode_idx));
                let dst_mode_data = dst_state.get_mode(Mode(dst_mode_idx));

                // Translate every src/dst representative ebg_node → CCH rank.
                let src_rep_ranks: Vec<u32> = src_reps
                    .iter()
                    .map(|b| src_mode_data.orig_to_rank[b.ebg_node as usize])
                    .collect();
                let dst_rep_ranks: Vec<u32> = dst_reps
                    .iter()
                    .map(|b| dst_mode_data.orig_to_rank[b.ebg_node as usize])
                    .collect();

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

                let row_major = if pair_crossings.is_empty() {
                    vec![u32::MAX; n_src * n_dst]
                } else {
                    build_matrix_with_buckets(
                        src_state,
                        Mode(src_mode_idx),
                        &src_rep_ranks,
                        dst_state,
                        Mode(dst_mode_idx),
                        &dst_rep_ranks,
                        src_region,
                        dst_region,
                        pair_crossings,
                        cluster_maps
                            .get(src_region)
                            .map(|v| v.as_slice())
                            .unwrap_or(&[]),
                        cluster_maps
                            .get(dst_region)
                            .map(|v| v.as_slice())
                            .unwrap_or(&[]),
                        mode_name,
                    )
                };

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
        representatives,
        cluster_maps,
        crossings: canon_crossings,
        matrices,
    })
}

/// Build the dense `n_src × n_dst` representative-to-representative
/// distance matrix in *one* parallelised batch using bucket many-to-many
/// CH (the same engine that powers the production `/table` endpoint).
///
/// # Algorithm
///
/// `dist(rep_i → rep_j) = min over crossings (k_a, k_b) of`
/// `   src_dist(rep_i → src_rep[cluster(k_a)])`
/// `   + edge_cost(k_a → k_b)`
/// `   + dst_dist(dst_rep[cluster(k_b)] → rep_j)`
///
/// The two CCH terms are batched M2M calls; the middle term is a
/// per-crossing `O(1)` lookup. The combiner is a triple loop over
/// `(rep_i, rep_j, crossing)` — typically `100 × 100 × 8 010 = 80 M`
/// adds, well under a second.
#[allow(clippy::too_many_arguments)]
fn build_matrix_with_buckets(
    src_state: &Arc<ServerState>,
    src_mode: Mode,
    src_rep_ranks: &[u32],
    dst_state: &Arc<ServerState>,
    dst_mode: Mode,
    dst_rep_ranks: &[u32],
    src_region: &str,
    dst_region: &str,
    pair_crossings: &[Crossing],
    src_cluster_map: &[u32],
    dst_cluster_map: &[u32],
    mode_name: &str,
) -> Vec<u32> {
    let n_src = src_rep_ranks.len();
    let n_dst = dst_rep_ranks.len();

    // Identify which side of the canonical pair the src region is on.
    // `pair_crossings` were stored under canonical key `(min, max)`; if
    // src_region is the "smaller", a_idx is in src and b_idx is in dst.
    let src_is_canon_a = src_region <= dst_region;

    // For every crossing, translate the per-region BorderIdx into a
    // representative index using the per-region cluster_map. Drop
    // crossings whose ranks are unreachable for this mode (orig_to_rank
    // returns u32::MAX for non-CCH-carried EBG nodes).
    struct ResolvedCrossing {
        src_rep_idx: u32,
        src_rep_rank: u32,
        dst_rep_idx: u32,
        dst_rep_rank: u32,
        cost_dsec: u32,
    }

    let mut resolved: Vec<ResolvedCrossing> = Vec::with_capacity(pair_crossings.len());
    for c in pair_crossings {
        let (sb_idx, db_idx) = if src_is_canon_a {
            (c.a_idx as usize, c.b_idx as usize)
        } else {
            (c.b_idx as usize, c.a_idx as usize)
        };
        if sb_idx >= src_cluster_map.len() || db_idx >= dst_cluster_map.len() {
            continue;
        }
        let s_rep = src_cluster_map[sb_idx] as usize;
        let d_rep = dst_cluster_map[db_idx] as usize;
        if s_rep >= n_src || d_rep >= n_dst {
            continue;
        }
        let s_rank = src_rep_ranks[s_rep];
        let d_rank = dst_rep_ranks[d_rep];
        if s_rank == u32::MAX || d_rank == u32::MAX {
            continue;
        }
        resolved.push(ResolvedCrossing {
            src_rep_idx: s_rep as u32,
            src_rep_rank: s_rank,
            dst_rep_idx: d_rep as u32,
            dst_rep_rank: d_rank,
            cost_dsec: build_inter_region_cost(c.edge_distance_m, mode_name),
        });
    }

    if resolved.is_empty() {
        return vec![u32::MAX; n_src * n_dst];
    }

    // Deduplicated list of ranks we actually need from each side. The
    // batched CCH below only needs the unique source/target ranks; we
    // index back into the resolved-crossings list via a small lookup.
    let mut src_unique_ranks: Vec<u32> = resolved.iter().map(|r| r.src_rep_rank).collect();
    src_unique_ranks.sort_unstable();
    src_unique_ranks.dedup();
    let mut dst_unique_ranks: Vec<u32> = resolved.iter().map(|r| r.dst_rep_rank).collect();
    dst_unique_ranks.sort_unstable();
    dst_unique_ranks.dedup();

    // L_src[i][k_unique] = src_dist(rep_i_rank → src_unique_ranks[k_unique]).
    // Built via one parallelised batched-bucket M2M call.
    //
    // We filter u32::MAX ranks out before calling the bucket M2M
    // engine — internally the search uses the rank as an array index,
    // so u32::MAX would walk past `dist_fwd.len()` and panic. A
    // u32::MAX rank means the representative is not in the mode CCH
    // (footpath-only border for car mode etc); we treat its row in
    // the L_src table as fully unreachable.
    let src_mode_data = src_state.get_mode(src_mode);
    let n_src_nodes = src_mode_data.cch_topo.n_nodes as usize;
    let mut valid_src_idx: Vec<usize> = Vec::with_capacity(n_src);
    let mut valid_src_ranks_for_bucket: Vec<u32> = Vec::with_capacity(n_src);
    for (i, &r) in src_rep_ranks.iter().enumerate() {
        if r != u32::MAX {
            valid_src_idx.push(i);
            valid_src_ranks_for_bucket.push(r);
        }
    }
    let mut l_src: Vec<u32> = vec![u32::MAX; n_src * src_unique_ranks.len()];
    if !valid_src_ranks_for_bucket.is_empty() && !src_unique_ranks.is_empty() {
        let (sub, _src_stats) = table_bucket_parallel(
            n_src_nodes,
            &src_mode_data.up_adj_flat,
            &src_mode_data.down_rev_flat,
            &valid_src_ranks_for_bucket,
            &src_unique_ranks,
        );
        // Expand the n_valid × n_src_unique sub-matrix back into the
        // n_src × n_src_unique L_src layout.
        let stride = src_unique_ranks.len();
        for (k, src_i) in valid_src_idx.iter().enumerate() {
            let row_src = &sub[k * stride..(k + 1) * stride];
            l_src[*src_i * stride..*src_i * stride + stride].copy_from_slice(row_src);
        }
    }

    // L_dst[k_unique][j] = dst_dist(dst_unique_ranks[k_unique] → rep_j_rank).
    // Same shape, dst CCH this time. The "from each crossing endpoint
    // to every dst rep" loop is exactly the bucket M2M case.
    let dst_mode_data = dst_state.get_mode(dst_mode);
    let n_dst_nodes = dst_mode_data.cch_topo.n_nodes as usize;
    let mut valid_dst_idx: Vec<usize> = Vec::with_capacity(n_dst);
    let mut valid_dst_ranks_for_bucket: Vec<u32> = Vec::with_capacity(n_dst);
    for (j, &r) in dst_rep_ranks.iter().enumerate() {
        if r != u32::MAX {
            valid_dst_idx.push(j);
            valid_dst_ranks_for_bucket.push(r);
        }
    }
    let mut l_dst: Vec<u32> = vec![u32::MAX; dst_unique_ranks.len() * n_dst];
    if !dst_unique_ranks.is_empty() && !valid_dst_ranks_for_bucket.is_empty() {
        let (sub, _dst_stats) = table_bucket_parallel(
            n_dst_nodes,
            &dst_mode_data.up_adj_flat,
            &dst_mode_data.down_rev_flat,
            &dst_unique_ranks,
            &valid_dst_ranks_for_bucket,
        );
        // sub is row-major [n_dst_unique][n_valid_dst]; expand into
        // l_dst row-major [n_dst_unique][n_dst].
        let sub_stride = valid_dst_ranks_for_bucket.len();
        for k_unique in 0..dst_unique_ranks.len() {
            for (k, dst_j) in valid_dst_idx.iter().enumerate() {
                l_dst[k_unique * n_dst + *dst_j] = sub[k_unique * sub_stride + k];
            }
        }
    }

    // Build per-rank → unique-index lookups so the combiner's inner
    // loop is a flat array index, not a HashMap.
    let mut src_unique_idx: HashMap<u32, u32> = HashMap::with_capacity(src_unique_ranks.len());
    for (i, &r) in src_unique_ranks.iter().enumerate() {
        src_unique_idx.insert(r, i as u32);
    }
    let mut dst_unique_idx: HashMap<u32, u32> = HashMap::with_capacity(dst_unique_ranks.len());
    for (i, &r) in dst_unique_ranks.iter().enumerate() {
        dst_unique_idx.insert(r, i as u32);
    }

    // Pre-resolve each crossing's (src_unique_k, dst_unique_k, cost).
    struct Triplet {
        src_uk: u32,
        dst_uk: u32,
        cost: u32,
    }
    let triplets: Vec<Triplet> = resolved
        .iter()
        .filter_map(|r| {
            let suk = src_unique_idx.get(&r.src_rep_rank).copied()?;
            let duk = dst_unique_idx.get(&r.dst_rep_rank).copied()?;
            Some(Triplet {
                src_uk: suk,
                dst_uk: duk,
                cost: r.cost_dsec,
            })
        })
        .collect();
    // Suppress unused-field warnings on the resolved fields not read here.
    let _ = resolved.first().map(|r| (r.src_rep_idx, r.dst_rep_idx));

    let n_src_unique = src_unique_ranks.len();
    // n_dst_unique not needed — l_dst's row stride is n_dst (the dst
    // representative count, since dst_rep_ranks is the *targets*
    // argument to the bucket M2M call).

    let mut row_major = vec![u32::MAX; n_src * n_dst];
    for i in 0..n_src {
        for j in 0..n_dst {
            let mut best = u32::MAX;
            for t in &triplets {
                let l1 = l_src[i * n_src_unique + t.src_uk as usize];
                if l1 == u32::MAX {
                    continue;
                }
                let l2 = l_dst[t.dst_uk as usize * n_dst + j];
                if l2 == u32::MAX {
                    continue;
                }
                let cand = l1.saturating_add(t.cost).saturating_add(l2);
                if cand < best {
                    best = cand;
                }
            }
            row_major[i * n_dst + j] = best;
        }
    }
    row_major
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
        let mut representative_counts = HashMap::new();
        let mut all_border_records: Vec<BorderNodeRecord> = Vec::new();
        // The on-disk body lays out each region's full border list,
        // immediately followed by its representative list, in
        // `region_order`. The manifest's `border_counts` and
        // `representative_counts` slice it back out at load time.
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
            let region_reps = self
                .representatives
                .get(region)
                .map(|v| v.as_slice())
                .unwrap_or(&[]);
            representative_counts.insert(region.clone(), region_reps.len() as u32);
            for b in region_reps {
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
            representative_counts,
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

        // Per-region cluster maps (one section per region, deterministic
        // little-endian u32 layout — see Copilot finding #3).
        for region in &self.region_order {
            let map = self
                .cluster_maps
                .get(region)
                .map(|v| v.as_slice())
                .unwrap_or(&[]);
            let mut bytes: Vec<u8> = Vec::with_capacity(map.len() * 4);
            for &v in map {
                bytes.extend_from_slice(&v.to_le_bytes());
            }
            let name = format!("overlay/cluster_map/{}", region);
            writer.append_bytes(SectionKind::OverlayClusterMap, name, &bytes)?;
        }

        // Matrices, one section per (src, dst, mode). Explicit
        // little-endian u32 layout to be portable across host
        // endianness (Copilot finding #3).
        let mut keys: Vec<&(RegionId, RegionId, String)> = self.matrices.keys().collect();
        keys.sort();
        for key in keys {
            let m = &self.matrices[key];
            let name = format!("overlay/matrix/{}/{}/{}", key.0, key.1, key.2);
            let mut bytes: Vec<u8> = Vec::with_capacity(m.len() * 4);
            for &v in m {
                bytes.extend_from_slice(&v.to_le_bytes());
            }
            writer.append_bytes(SectionKind::OverlayMatrix, name, &bytes)?;
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
        // try_cast_slice surfaces alignment/length errors as a real
        // anyhow chain instead of panicking (Copilot findings #6, #8).
        let border_records: &[BorderNodeRecord] = bytemuck::try_cast_slice(&border_bytes)
            .map_err(|e| anyhow::anyhow!("border_nodes section malformed: {}", e))?;

        let mut borders: HashMap<RegionId, Vec<BorderNode>> = HashMap::new();
        let mut representatives: HashMap<RegionId, Vec<BorderNode>> = HashMap::new();
        let mut cursor = 0usize;
        for region in &manifest.region_order {
            let n_borders =
                *manifest.border_counts.get(region).ok_or_else(|| {
                    anyhow::anyhow!("manifest missing border count for {}", region)
                })? as usize;
            let n_reps = *manifest
                .representative_counts
                .get(region)
                .unwrap_or(&(n_borders as u32)) as usize;
            anyhow::ensure!(
                cursor + n_borders + n_reps <= border_records.len(),
                "overlay/border_nodes truncated for region {}",
                region
            );
            let border_slice = &border_records[cursor..cursor + n_borders];
            let region_borders: Vec<BorderNode> = border_slice
                .iter()
                .map(|r| BorderNode {
                    ebg_node: r.ebg_node,
                    lat: r.lat_e7 as f64 / 1e7,
                    lon: r.lon_e7 as f64 / 1e7,
                })
                .collect();
            borders.insert(region.clone(), region_borders);
            cursor += n_borders;
            let rep_slice = &border_records[cursor..cursor + n_reps];
            let region_reps: Vec<BorderNode> = rep_slice
                .iter()
                .map(|r| BorderNode {
                    ebg_node: r.ebg_node,
                    lat: r.lat_e7 as f64 / 1e7,
                    lon: r.lon_e7 as f64 / 1e7,
                })
                .collect();
            representatives.insert(region.clone(), region_reps);
            cursor += n_reps;
        }

        // ---- Crossings ----------------------------------------------
        let crossing_entry = container
            .get("overlay/crossings")
            .ok_or_else(|| anyhow::anyhow!("overlay container missing crossings"))?;
        let crossing_bytes = container.read_section_verified(path, crossing_entry)?;
        let crossing_records: &[CrossingRecord] = bytemuck::try_cast_slice(&crossing_bytes)
            .map_err(|e| anyhow::anyhow!("crossings section malformed: {}", e))?;

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

        // ---- Cluster maps -------------------------------------------
        let mut cluster_maps: HashMap<RegionId, Vec<u32>> = HashMap::new();
        for sec in container.iter_kind(SectionKind::OverlayClusterMap) {
            // Name shape: "overlay/cluster_map/<region>"
            let parts: Vec<&str> = sec.name.split('/').collect();
            anyhow::ensure!(
                parts.len() == 3 && parts[0] == "overlay" && parts[1] == "cluster_map",
                "unexpected cluster_map section name: {}",
                sec.name
            );
            let region = parts[2].to_string();
            let bytes = container.read_section_verified(path, sec)?;
            anyhow::ensure!(
                bytes.len() % 4 == 0,
                "cluster_map section {} has non-u32-aligned body",
                sec.name
            );
            let m: Vec<u32> = bytes
                .chunks_exact(4)
                .map(|c| u32::from_le_bytes([c[0], c[1], c[2], c[3]]))
                .collect();
            let n_borders = borders.get(&region).map(|v| v.len()).unwrap_or(0);
            anyhow::ensure!(
                m.len() == n_borders,
                "cluster_map for {} has length {} but expected {} borders",
                region,
                m.len(),
                n_borders
            );
            cluster_maps.insert(region, m);
        }
        // Backfill identity cluster maps for regions where the section
        // is missing (forward-compatibility with v1 overlays).
        for region in &manifest.region_order {
            cluster_maps.entry(region.clone()).or_insert_with(|| {
                (0..borders.get(region).map(|v| v.len()).unwrap_or(0) as u32).collect()
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
            // Body is a flat little-endian [u32].
            anyhow::ensure!(
                bytes.len() % 4 == 0,
                "matrix section {} has non-u32-aligned body",
                sec.name
            );
            let m: Vec<u32> = bytes
                .chunks_exact(4)
                .map(|c| u32::from_le_bytes([c[0], c[1], c[2], c[3]]))
                .collect();
            let n_src = representatives.get(&src).map(|v| v.len()).unwrap_or(0);
            let n_dst = representatives.get(&dst).map(|v| v.len()).unwrap_or(0);
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
            representatives,
            cluster_maps,
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
        // Identity cluster maps (each border is its own representative).
        let mut representatives = HashMap::new();
        representatives.insert("A".to_string(), borders["A"].clone());
        representatives.insert("B".to_string(), borders["B"].clone());
        let mut cluster_maps = HashMap::new();
        cluster_maps.insert("A".to_string(), vec![0u32, 1]);
        cluster_maps.insert("B".to_string(), vec![0u32]);

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
            representatives,
            cluster_maps,
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
        assert_eq!(loaded.cluster_maps["A"], vec![0u32, 1]);
        assert_eq!(loaded.representatives["A"].len(), 2);
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

    #[test]
    fn malformed_overlay_returns_error_not_panic() {
        // Random bytes that aren't a valid butterfly container.
        let tmp = NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), b"not a butterfly container").unwrap();
        let result = OverlayCluster::load(tmp.path());
        assert!(result.is_err(), "load on garbage bytes must Err, not panic");
    }
}
