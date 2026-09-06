//! #460: per-NBG-edge OSM node ID chains on the serve path.
//!
//! Wraps the `shared/edge_osm_offsets` + `shared/edge_osm_ids` sections
//! (see [`crate::formats::edge_osm`]) and resolves any **EBG node**
//! (a directed NBG edge) to its **per-OSM-segment** `(from, to)` id
//! pairs — the granularity per-segment reference tables join on. At NBG
//! granularity ~49% of `edges_flow` mass keyed to junction pairs absent
//! from such tables; this is the server-side expansion that closes it.
//!
//! Direction handling mirrors the geometry subsystem: the stored chain
//! is in the NBG edge's canonical u→v order, both directed EBG twins
//! share the same `geom_idx`, and direction is resolved by comparing the
//! chain's endpoints against the EBG node's tail OSM id — no extra
//! per-EBG storage.

use crate::formats::edge_osm::{EdgeOsmIds, EdgeOsmOffsets};
use crate::formats::mmap::ArcCow;

/// Per-edge OSM id chains, CSR over the same NBG undirected edge space as
/// [`super::edge_geom::EdgeGeometry`] (`EbgNode.geom_idx`).
pub struct EdgeOsmChains {
    /// Length `n_edges + 1`; empty (len 0) ⇒ chains absent.
    offsets: ArcCow<u32>,
    ids: ArcCow<i64>,
}

impl EdgeOsmChains {
    /// The "absent" sentinel — old containers without the sections.
    pub fn empty() -> Self {
        Self {
            offsets: ArcCow::from_vec(Vec::new()),
            ids: ArcCow::from_vec(Vec::new()),
        }
    }

    pub fn from_sections(off: EdgeOsmOffsets, ids: EdgeOsmIds) -> anyhow::Result<Self> {
        anyhow::ensure!(
            off.n_ids == ids.n_ids,
            "edge_osm_offsets.n_ids ({}) != edge_osm_ids.n_ids ({})",
            off.n_ids,
            ids.n_ids
        );
        anyhow::ensure!(
            ids.ids.len() == ids.n_ids as usize,
            "edge_osm_ids body length {} != n_ids {}",
            ids.ids.len(),
            ids.n_ids
        );
        anyhow::ensure!(
            off.offsets.len() == off.n_edges as usize + 1,
            "edge_osm_offsets length {} != n_edges + 1 ({})",
            off.offsets.len(),
            off.n_edges as usize + 1
        );
        Ok(Self {
            offsets: off.offsets,
            ids: ids.ids,
        })
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.offsets.as_slice().is_empty()
    }

    /// The id chain for NBG undirected edge `geom_idx`, canonical u→v
    /// order. `None` when chains are absent or the index is out of range.
    #[inline]
    pub fn chain(&self, geom_idx: u32) -> Option<&[i64]> {
        let off = self.offsets.as_slice();
        let i = geom_idx as usize;
        if i + 1 >= off.len() {
            return None;
        }
        let (s, e) = (off[i] as usize, off[i + 1] as usize);
        self.ids.as_slice().get(s..e)
    }

    /// Resolve a DIRECTED traversal of NBG edge `geom_idx` to its OSM
    /// segment pairs, oriented so the first pair starts at `osm_tail`
    /// (the traversal's entry junction).
    ///
    /// Returns `None` when chains are absent, the chain is shorter than
    /// 2 ids, or `osm_tail` matches neither chain endpoint (defensive —
    /// indicates id/geometry disagreement; callers fall back to
    /// NBG-endpoint emission). The forward case yields
    /// `(chain[i], chain[i+1])`; the reverse case yields
    /// `(chain[i+1], chain[i])` walking from the chain's end.
    pub fn directed_segments(&self, geom_idx: u32, osm_tail: i64) -> Option<DirectedSegments<'_>> {
        let chain = self.chain(geom_idx)?;
        if chain.len() < 2 {
            return None;
        }
        let forward = if chain[0] == osm_tail {
            true
        } else if chain[chain.len() - 1] == osm_tail {
            false
        } else {
            return None;
        };
        Some(DirectedSegments {
            chain,
            forward,
            i: 0,
        })
    }
}

/// Iterator over a directed edge's per-OSM-segment `(from, to)` pairs.
pub struct DirectedSegments<'a> {
    chain: &'a [i64],
    forward: bool,
    i: usize,
}

impl Iterator for DirectedSegments<'_> {
    type Item = (i64, i64);

    #[inline]
    fn next(&mut self) -> Option<(i64, i64)> {
        let n = self.chain.len();
        if self.i + 1 >= n {
            return None;
        }
        let out = if self.forward {
            (self.chain[self.i], self.chain[self.i + 1])
        } else {
            (self.chain[n - 1 - self.i], self.chain[n - 2 - self.i])
        };
        self.i += 1;
        Some(out)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let left = (self.chain.len() - 1).saturating_sub(self.i);
        (left, Some(left))
    }
}

impl ExactSizeIterator for DirectedSegments<'_> {}

/// The OSM node ids a path of EBG edges runs through, in order, with the
/// junction shared by two consecutive edges emitted once (#606).
///
/// This is what `GET /route?annotations=nodes` serves. The chain of an
/// edge carries every OSM node on it — junctions and shape points alike —
/// so the result lines up with the route geometry, not with the per-edge
/// `duration` / `distance` / `speed` arrays; that is also how OSRM shapes
/// its `nodes` annotation.
///
/// Containers that pre-date the id chains (and the rare edge whose chain
/// endpoints disagree with its EBG tail) fall back to that edge's two NBG
/// junction ids — still OSM ids, just without the intermediate shape
/// nodes. `0` marks an id the container cannot resolve.
pub fn osm_nodes_along_path(
    chains: &EdgeOsmChains,
    ebg_nodes: &[crate::formats::ebg_nodes::EbgNode],
    nbg_node_to_osm: &[i64],
    path: &[u32],
) -> Vec<i64> {
    let osm_of = |nbg: u32| nbg_node_to_osm.get(nbg as usize).copied().unwrap_or(0);
    let mut out: Vec<i64> = Vec::with_capacity(path.len() + 1);
    for &ebg_id in path {
        let Some(node) = ebg_nodes.get(ebg_id as usize) else {
            continue;
        };
        let tail = osm_of(node.tail_nbg);
        match chains.directed_segments(node.geom_idx, tail) {
            Some(segments) => {
                for (a, b) in segments {
                    if out.last() != Some(&a) {
                        out.push(a);
                    }
                    out.push(b);
                }
            }
            None => {
                if out.last() != Some(&tail) {
                    out.push(tail);
                }
                out.push(osm_of(node.head_nbg));
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chains() -> EdgeOsmChains {
        // edge 0: [10, 11, 12]; edge 1: [12, 13]; edge 2: [20] (degenerate)
        EdgeOsmChains {
            offsets: ArcCow::from_vec(vec![0, 3, 5, 6]),
            ids: ArcCow::from_vec(vec![10, 11, 12, 12, 13, 20]),
        }
    }

    #[test]
    fn forward_segments() {
        let c = chains();
        let segs: Vec<_> = c.directed_segments(0, 10).unwrap().collect();
        assert_eq!(segs, vec![(10, 11), (11, 12)]);
    }

    #[test]
    fn reverse_segments() {
        let c = chains();
        let segs: Vec<_> = c.directed_segments(0, 12).unwrap().collect();
        assert_eq!(segs, vec![(12, 11), (11, 10)]);
    }

    #[test]
    fn tail_mismatch_is_none() {
        let c = chains();
        assert!(c.directed_segments(0, 99).is_none());
    }

    #[test]
    fn degenerate_chain_is_none() {
        let c = chains();
        assert!(c.directed_segments(2, 20).is_none());
    }

    #[test]
    fn out_of_range_is_none() {
        let c = chains();
        assert!(c.chain(3).is_none());
        assert!(c.directed_segments(3, 10).is_none());
    }

    #[test]
    fn empty_sentinel() {
        let c = EdgeOsmChains::empty();
        assert!(c.is_empty());
        assert!(c.chain(0).is_none());
    }

    #[test]
    fn exact_size() {
        let c = chains();
        let it = c.directed_segments(0, 10).unwrap();
        assert_eq!(it.len(), 2);
    }

    /// EBG node: `geom_idx` names the undirected NBG edge, `tail_nbg` /
    /// `head_nbg` its two junctions in the traversal's direction.
    fn ebg(geom_idx: u32, tail_nbg: u32, head_nbg: u32) -> crate::formats::ebg_nodes::EbgNode {
        crate::formats::ebg_nodes::EbgNode {
            tail_nbg,
            head_nbg,
            geom_idx,
            length_m: 100,
            class_bits: 0,
            primary_way: 0,
        }
    }

    /// nbg node 0 -> OSM 10, 1 -> 12, 2 -> 13, 3 -> 20.
    const NBG_TO_OSM: [i64; 4] = [10, 12, 13, 20];

    #[test]
    fn path_nodes_are_osm_ids_including_shape_points() {
        // Edge 0 forward (chain 10,11,12) then edge 1 forward (12,13).
        let path_nodes = osm_nodes_along_path(
            &chains(),
            &[ebg(0, 0, 1), ebg(1, 1, 2)],
            &NBG_TO_OSM,
            &[0, 1],
        );
        assert_eq!(path_nodes, vec![10, 11, 12, 13]);
    }

    #[test]
    fn path_nodes_follow_the_direction_of_travel() {
        // Edge 0 traversed head-to-tail: the chain reverses.
        let path_nodes = osm_nodes_along_path(&chains(), &[ebg(0, 1, 0)], &NBG_TO_OSM, &[0]);
        assert_eq!(path_nodes, vec![12, 11, 10]);
    }

    #[test]
    fn a_missing_chain_still_yields_osm_junction_ids() {
        // Edge 2's chain is degenerate (one id), so the NBG junctions
        // answer — OSM ids either way, never an internal id.
        let path_nodes = osm_nodes_along_path(&chains(), &[ebg(2, 0, 3)], &NBG_TO_OSM, &[0]);
        assert_eq!(path_nodes, vec![10, 20]);

        let absent = EdgeOsmChains::empty();
        let path_nodes = osm_nodes_along_path(&absent, &[ebg(2, 0, 3)], &NBG_TO_OSM, &[0]);
        assert_eq!(path_nodes, vec![10, 20]);
    }

    #[test]
    fn an_empty_path_yields_no_nodes() {
        assert!(osm_nodes_along_path(&chains(), &[], &NBG_TO_OSM, &[]).is_empty());
    }
}
