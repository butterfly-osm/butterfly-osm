//! Synthetic 2-region cross-region overlay test.
//!
//! Builds a hand-rolled grid of nodes split across two pseudo-regions,
//! places three border crossings between them, computes the brute-force
//! union-graph shortest paths via Dijkstra (the *oracle*), and verifies
//! that the cross-region overlay coordinator's combinatorial kernel
//! produces identical results for **every** (src, tgt) pair.
//!
//! This is the algorithm-correctness regression net for #91 Phase 2.
//! It does NOT exercise the full `solve_cross_region` (which would
//! require building two real `ServerState` instances), only the
//! combinatorial picker `pick_best_border_pair`. The picker is the
//! component that determines the final answer once the per-region
//! distances are available; the per-region distances are produced by
//! standard CCH P2P which is independently tested in
//! `query.rs::tests`.
//!
//! # Synthetic graph layout
//!
//! Two 3×3 grids ("regions A and B"), 4-connected, edge weight = 1
//! per traversal. Three border crossings link them with prescribed
//! costs:
//!   - A.node(2,1) ↔ B.node(0,1)  cost 5
//!   - A.node(2,2) ↔ B.node(0,2)  cost 3
//!   - A.node(2,0) ↔ B.node(0,0)  cost 7
//!
//! The oracle is a Dijkstra over the union of both grids plus the three
//! border edges. The picker is fed:
//!   - dist_src[i] = oracle distance from src to A's i-th border node
//!     (where i indexes A's border list)
//!   - dist_tgt[j] = oracle distance from B's j-th border node to tgt
//!     (within B only)
//!   - matrix[i][j] = oracle distance from A's i-th border node to
//!     B's j-th border node (across the union)
//!
//! Both the oracle and the picker should agree on every (src, tgt)
//! pair where src ∈ A and tgt ∈ B.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};

use butterfly_route::server::cross_region::pick_best_border_pair;

/// Node identifier in the synthetic graph. The high bit encodes the
/// region (0 = A, 1 = B); the low bits encode (row, col) in the 3×3 grid.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
struct N(u32);

impl N {
    fn new(region: u8, row: u8, col: u8) -> Self {
        N(((region as u32) << 16) | ((row as u32) << 8) | (col as u32))
    }
    fn region(self) -> u8 {
        (self.0 >> 16) as u8
    }
}

const GRID: u8 = 3;

/// Build the union graph: an undirected adjacency list with edge
/// weights, covering both regions and the three prescribed border
/// crossings.
fn union_graph() -> HashMap<N, Vec<(N, u32)>> {
    let mut adj: HashMap<N, Vec<(N, u32)>> = HashMap::new();
    // Per-region 4-connected grid edges, weight 1.
    for region in 0..2u8 {
        for r in 0..GRID {
            for c in 0..GRID {
                let n = N::new(region, r, c);
                if r + 1 < GRID {
                    let n2 = N::new(region, r + 1, c);
                    adj.entry(n).or_default().push((n2, 1));
                    adj.entry(n2).or_default().push((n, 1));
                }
                if c + 1 < GRID {
                    let n2 = N::new(region, r, c + 1);
                    adj.entry(n).or_default().push((n2, 1));
                    adj.entry(n2).or_default().push((n, 1));
                }
            }
        }
    }
    // Border crossings: A's row=2 (rightmost in A) → B's row=0 (leftmost in B).
    let crossings = [
        (N::new(0, 2, 1), N::new(1, 0, 1), 5u32),
        (N::new(0, 2, 2), N::new(1, 0, 2), 3),
        (N::new(0, 2, 0), N::new(1, 0, 0), 7),
    ];
    for (a, b, w) in crossings {
        adj.entry(a).or_default().push((b, w));
        adj.entry(b).or_default().push((a, w));
    }
    adj
}

/// Dijkstra over the union graph. Returns u32::MAX for unreachable.
fn dijkstra_union(adj: &HashMap<N, Vec<(N, u32)>>, src: N) -> HashMap<N, u32> {
    let mut dist: HashMap<N, u32> = HashMap::new();
    let mut pq: BinaryHeap<Reverse<(u32, N)>> = BinaryHeap::new();
    dist.insert(src, 0);
    pq.push(Reverse((0, src)));
    while let Some(Reverse((d, u))) = pq.pop() {
        if let Some(&best) = dist.get(&u)
            && d > best
        {
            continue;
        }
        if let Some(neigh) = adj.get(&u) {
            for &(v, w) in neigh {
                let new_d = d.saturating_add(w);
                let beat = match dist.get(&v) {
                    Some(&prev) => new_d < prev,
                    None => true,
                };
                if beat {
                    dist.insert(v, new_d);
                    pq.push(Reverse((new_d, v)));
                }
            }
        }
    }
    dist
}

/// Same as `dijkstra_union` but restricted to one region (the union
/// adjacency is filtered on the fly to only include nodes in `region`).
fn dijkstra_region(adj: &HashMap<N, Vec<(N, u32)>>, src: N, region: u8) -> HashMap<N, u32> {
    let mut dist: HashMap<N, u32> = HashMap::new();
    let mut pq: BinaryHeap<Reverse<(u32, N)>> = BinaryHeap::new();
    if src.region() != region {
        return dist;
    }
    dist.insert(src, 0);
    pq.push(Reverse((0, src)));
    while let Some(Reverse((d, u))) = pq.pop() {
        if let Some(&best) = dist.get(&u)
            && d > best
        {
            continue;
        }
        if let Some(neigh) = adj.get(&u) {
            for &(v, w) in neigh {
                if v.region() != region {
                    continue;
                }
                let new_d = d.saturating_add(w);
                let beat = match dist.get(&v) {
                    Some(&prev) => new_d < prev,
                    None => true,
                };
                if beat {
                    dist.insert(v, new_d);
                    pq.push(Reverse((new_d, v)));
                }
            }
        }
    }
    dist
}

fn all_nodes_in_region(region: u8) -> Vec<N> {
    let mut v = Vec::with_capacity(9);
    for r in 0..GRID {
        for c in 0..GRID {
            v.push(N::new(region, r, c));
        }
    }
    v
}

fn border_nodes_a() -> Vec<N> {
    vec![N::new(0, 2, 0), N::new(0, 2, 1), N::new(0, 2, 2)]
}

fn border_nodes_b() -> Vec<N> {
    vec![N::new(1, 0, 0), N::new(1, 0, 1), N::new(1, 0, 2)]
}

#[test]
fn synthetic_fixture_agrees_with_oracle_on_all_pairs() {
    let adj = union_graph();
    let borders_a = border_nodes_a();
    let borders_b = border_nodes_b();

    // Build the dense overlay matrix from oracle distances:
    //   matrix[i][j] = union_dist(borders_a[i] → borders_b[j])
    let mut matrix: Vec<u32> = vec![u32::MAX; borders_a.len() * borders_b.len()];
    for (i, a) in borders_a.iter().enumerate() {
        let dist_a = dijkstra_union(&adj, *a);
        for (j, b) in borders_b.iter().enumerate() {
            let d = dist_a.get(b).copied().unwrap_or(u32::MAX);
            matrix[i * borders_b.len() + j] = d;
        }
    }

    let n_a = all_nodes_in_region(0);
    let n_b = all_nodes_in_region(1);

    let mut pair_count = 0;
    let mut mismatches: Vec<String> = Vec::new();

    for &src in &n_a {
        let oracle_from_src = dijkstra_union(&adj, src);
        let region_dist_src = dijkstra_region(&adj, src, 0);

        // Per-region distances from src to every A border (the picker
        // input dist_src).
        let dist_src: Vec<u32> = borders_a
            .iter()
            .map(|b| region_dist_src.get(b).copied().unwrap_or(u32::MAX))
            .collect();

        for &tgt in &n_b {
            // Per-region distances from every B border to tgt (the
            // picker input dist_tgt). Restrict to B.
            let dist_tgt: Vec<u32> = borders_b
                .iter()
                .map(|b| {
                    let region_dist_b = dijkstra_region(&adj, *b, 1);
                    region_dist_b.get(&tgt).copied().unwrap_or(u32::MAX)
                })
                .collect();

            let picker = pick_best_border_pair(&dist_src, &matrix, borders_b.len(), &dist_tgt);

            let oracle = oracle_from_src.get(&tgt).copied().unwrap_or(u32::MAX);

            match (picker, oracle) {
                (None, u32::MAX) => {}
                (Some((picker_total, _, _)), oracle_d) if oracle_d != u32::MAX => {
                    if picker_total != oracle_d {
                        mismatches.push(format!(
                            "src=({:?}) tgt=({:?}): picker={} oracle={}",
                            src, tgt, picker_total, oracle_d
                        ));
                    }
                }
                _ => {
                    mismatches.push(format!(
                        "src=({:?}) tgt=({:?}): picker={:?} oracle={}",
                        src, tgt, picker, oracle
                    ));
                }
            }
            pair_count += 1;
        }
    }

    assert!(
        mismatches.is_empty(),
        "{} of {} pairs disagreed with oracle:\n{}",
        mismatches.len(),
        pair_count,
        mismatches.join("\n")
    );
    // 9 src × 9 tgt = 81 pairs, all checked.
    assert_eq!(pair_count, 81);
}

#[test]
fn picker_handles_unreachable_paths() {
    // dist_src all u32::MAX → no result.
    let dist_src = vec![u32::MAX; 3];
    let matrix = vec![1u32; 9];
    let dist_tgt = vec![1u32; 3];
    let picker = pick_best_border_pair(&dist_src, &matrix, 3, &dist_tgt);
    assert_eq!(picker, None);

    // matrix all u32::MAX → no result.
    let dist_src = vec![1u32; 3];
    let matrix = vec![u32::MAX; 9];
    let picker = pick_best_border_pair(&dist_src, &matrix, 3, &dist_tgt);
    assert_eq!(picker, None);

    // dist_tgt all u32::MAX → no result.
    let dist_src = vec![1u32; 3];
    let matrix = vec![1u32; 9];
    let dist_tgt = vec![u32::MAX; 3];
    let picker = pick_best_border_pair(&dist_src, &matrix, 3, &dist_tgt);
    assert_eq!(picker, None);
}

#[test]
fn pruning_collapses_dense_borders_without_breaking_picker() {
    // Synthetic test for the pruned-border-set optimisation: 10 src
    // borders that cluster down to 3 representatives, with the
    // matrix indexed at the *representative* level. The picker must
    // still pick the same minimum-cost border pair.
    //
    // Layout (per src cluster):
    //   cluster 0: src borders 0..3 → rep 0, all share the same
    //              `dist_src` (the access leg lands at the cluster).
    //   cluster 1: src borders 3..7 → rep 1
    //   cluster 2: src borders 7..10 → rep 2
    //
    // Per-rep dist_src: [10, 20, 30] (the "best" cluster is 0).
    // Matrix (3 src reps × 2 dst reps): [[100, 200], [50, 60], [99, 99]]
    // dist_tgt (per dst rep): [1, 2]
    //
    // Best at rep level: i=1, j=0, total = 20 + 50 + 1 = 71.
    let dist_src = vec![10u32, 20, 30];
    let matrix = vec![100u32, 200, 50, 60, 99, 99];
    let dist_tgt = vec![1u32, 2];
    let (total, i, j) = pick_best_border_pair(&dist_src, &matrix, 2, &dist_tgt).unwrap();
    assert_eq!(i, 1);
    assert_eq!(j, 0);
    assert_eq!(total, 71);

    // Sanity-check the cluster_map shape from prune_border_set against
    // a hand-rolled set of 10 border crossings that fall into 3
    // well-separated lat clusters. Verifies that the prune helper's
    // determinism matches what the matrix builder expects.
    use butterfly_route::server::border::{BorderCrossing, prune_border_set};
    let lats = [
        49.5000, 49.5001, 49.5002, // cluster 0 (~0–22 m apart)
        49.6000, 49.6001, 49.6002, 49.6003, // cluster 1
        49.7000, 49.7001, 49.7002, // cluster 2
    ];
    let cs: Vec<_> = lats
        .iter()
        .enumerate()
        .map(|(i, &lat)| BorderCrossing {
            region_a: "A".into(),
            node_a: 100 + i as u32,
            lat_a: lat,
            lon_a: 5.5,
            region_b: "B".into(),
            node_b: 200 + i as u32,
            lat_b: lat + 1e-4,
            lon_b: 5.5,
            edge_distance_m: 11.0,
        })
        .collect();
    let (reps, map) = prune_border_set(&cs, 100.0);
    assert_eq!(reps.len(), 3);
    assert_eq!(map.len(), 10);
    // Each cluster's members all share the same id.
    for window in [&map[0..3], &map[3..7], &map[7..10]] {
        let first = window[0];
        assert!(window.iter().all(|&x| x == first));
    }
    // Cluster ids are dense 0..3.
    let mut unique: Vec<u32> = map.clone();
    unique.sort_unstable();
    unique.dedup();
    assert_eq!(unique, vec![0u32, 1, 2]);
}

#[test]
fn picker_finds_the_minimum_combination() {
    // dist_src = [10, 20, 30]
    // dist_tgt = [1, 2, 3]
    // matrix = [[100, 200, 300], [50, 60, 70], [99, 99, 99]]
    // Best should be: i=1 (20), j=0 (50), tgt[0]=1 → 71
    //   alt:         i=0, j=0   = 10 + 100 + 1 = 111
    //   alt:         i=2, j=0   = 30 + 99 + 1 = 130
    let dist_src = vec![10u32, 20, 30];
    let matrix = vec![100u32, 200, 300, 50, 60, 70, 99, 99, 99];
    let dist_tgt = vec![1u32, 2, 3];
    let (total, i, j) = pick_best_border_pair(&dist_src, &matrix, 3, &dist_tgt).unwrap();
    assert_eq!(i, 1);
    assert_eq!(j, 0);
    assert_eq!(total, 71);
}
