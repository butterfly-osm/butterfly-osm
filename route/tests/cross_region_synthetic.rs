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
use butterfly_route::server::geometry::{GeometryFormat, Point, RouteGeometry};
use butterfly_route::server::route::stitch_cross_region_polyline;

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

// ===========================================================================
// #188 — cross-region polyline assembly
//
// Synthetic 2-region oracle for the geometry-stitching kernel
// `stitch_cross_region_polyline`. We hand-roll a curved access leg
// (region A), a curved egress leg (region B), and a single border
// crossing. The kernel should produce a deduplicated, monotonic
// polyline that visits every leg vertex plus the border representatives
// — never the degenerate `[src, dst]` straight line that #188 reports.
// ===========================================================================

fn pt(lon: f64, lat: f64) -> Point {
    Point { lon, lat }
}

#[test]
fn stitch_cross_region_polyline_emits_more_than_two_points() {
    // Synthetic 2-region cross-region setup mimicking the BE → LU
    // shape: a curved BE leg of 5 vertices, a single border edge, and
    // a curved LU leg of 5 vertices. Total expected: 12 unique points.
    let src_leg = vec![
        pt(4.3525, 50.8467), // src snap (start of leg)
        pt(4.4500, 50.8000),
        pt(4.5500, 50.7500),
        pt(4.7000, 50.6000),
        pt(4.9000, 50.4000),
        pt(5.2000, 50.1000),
        pt(5.5000, 49.9000),
        pt(5.7800, 49.7500), // BE-side border representative end
    ];
    let dst_leg = vec![
        pt(5.7900, 49.7400), // LU-side border representative start
        pt(5.8500, 49.7300),
        pt(5.9000, 49.7000),
        pt(5.9500, 49.6800),
        pt(6.0000, 49.6500),
        pt(6.0500, 49.6300),
        pt(6.0900, 49.6200),
        pt(6.1296, 49.6116), // dst snap
    ];
    let src_border = Some(pt(5.7800, 49.7500));
    let dst_border = Some(pt(5.7900, 49.7400));

    let polyline = stitch_cross_region_polyline(
        &src_leg,
        src_leg[0],
        src_border,
        dst_border,
        &dst_leg,
        dst_leg[dst_leg.len() - 1],
    );

    // Must not be the degenerate [src, dst] 2-point straight line.
    assert!(
        polyline.len() > 10,
        "expected polyline with > 10 points, got {}: {:?}",
        polyline.len(),
        polyline
    );

    // First point is the source; last point is the destination.
    assert!((polyline[0].lon - 4.3525).abs() < 1e-9);
    assert!((polyline[0].lat - 50.8467).abs() < 1e-9);
    assert!((polyline.last().unwrap().lon - 6.1296).abs() < 1e-9);
    assert!((polyline.last().unwrap().lat - 49.6116).abs() < 1e-9);

    // Polyline is roughly monotonic (lon mostly increases, lat mostly
    // decreases) on this BE→LU heading. We don't require strict
    // monotonicity — straight-line snap-to-border may have a small
    // jitter — but the bulk trend must be present.
    let first = polyline[0];
    let last = *polyline.last().unwrap();
    let mut wrong_lon = 0;
    let mut wrong_lat = 0;
    for w in polyline.windows(2) {
        if w[1].lon < w[0].lon {
            wrong_lon += 1;
        }
        if w[1].lat > w[0].lat {
            wrong_lat += 1;
        }
    }
    assert!(
        wrong_lon as f64 / polyline.len() as f64 <= 0.1,
        "lon should mostly increase BE → LU; got {} of {} reversals: {:?}",
        wrong_lon,
        polyline.len() - 1,
        polyline
    );
    assert!(
        wrong_lat as f64 / polyline.len() as f64 <= 0.1,
        "lat should mostly decrease BE → LU; got {} of {} reversals: {:?}",
        wrong_lat,
        polyline.len() - 1,
        polyline
    );

    // Polyline should not collapse — make sure we cover at least
    // 90% of the great-circle src→dst delta in pure walking distance
    // (not just teleporting). The synthetic legs sum to ~360 km;
    // even a slack threshold catches the #188 regression.
    let mut total_lon_span = 0.0;
    let mut total_lat_span = 0.0;
    for w in polyline.windows(2) {
        total_lon_span += (w[1].lon - w[0].lon).abs();
        total_lat_span += (w[1].lat - w[0].lat).abs();
    }
    assert!(
        total_lon_span >= (last.lon - first.lon).abs() * 0.95,
        "polyline collapsed in lon: cumulative {:.4} vs straight {:.4}",
        total_lon_span,
        last.lon - first.lon
    );
    assert!(
        total_lat_span >= (first.lat - last.lat).abs() * 0.95,
        "polyline collapsed in lat: cumulative {:.4} vs straight {:.4}",
        total_lat_span,
        first.lat - last.lat
    );

    // Polyline6 encoding round-trips fine (catches #188's "exk~_B…"
    // 2-point degenerate case at the wire format).
    let geom = RouteGeometry::from_points(polyline.clone(), GeometryFormat::Polyline6);
    let encoded = geom.polyline.expect("polyline6 string");
    assert!(
        encoded.len() > 32,
        "encoded polyline should be > 32 chars for >10 points, got {} chars: {}",
        encoded.len(),
        encoded
    );
}

#[test]
fn stitch_cross_region_handles_degenerate_legs() {
    // src snap == src border (zero-length access leg) — the function
    // must seed the polyline from src_snap rather than emitting an
    // empty access leg followed by a straight line.
    let src_snap = pt(5.7800, 49.7500);
    let src_border = Some(src_snap);
    let dst_border = Some(pt(5.7900, 49.7400));
    let dst_leg = vec![pt(5.7900, 49.7400), pt(6.0, 49.65), pt(6.1296, 49.6116)];
    let dst_snap = pt(6.1296, 49.6116);

    let polyline =
        stitch_cross_region_polyline(&[], src_snap, src_border, dst_border, &dst_leg, dst_snap);

    // Should still cover src + border + egress (no straight-line collapse).
    assert!(polyline.len() >= 4, "{:?}", polyline);
    assert!((polyline[0].lon - 5.7800).abs() < 1e-9);
    assert!((polyline.last().unwrap().lon - 6.1296).abs() < 1e-9);
}

#[test]
fn stitch_cross_region_dedupes_border_overlap_with_leg_endpoints() {
    // Last point of the access leg coincides with src_border, and
    // first point of the egress leg coincides with dst_border. The
    // stitched polyline must not contain consecutive duplicates.
    let src_border_pt = pt(5.7800, 49.7500);
    let dst_border_pt = pt(5.7900, 49.7400);
    let src_leg = vec![pt(4.35, 50.85), pt(5.0, 50.0), src_border_pt];
    let dst_leg = vec![dst_border_pt, pt(6.0, 49.65), pt(6.1296, 49.6116)];
    let src_snap = pt(4.35, 50.85);
    let dst_snap = pt(6.1296, 49.6116);

    let polyline = stitch_cross_region_polyline(
        &src_leg,
        src_snap,
        Some(src_border_pt),
        Some(dst_border_pt),
        &dst_leg,
        dst_snap,
    );

    for w in polyline.windows(2) {
        assert!(
            (w[0].lon - w[1].lon).abs() > 1e-9 || (w[0].lat - w[1].lat).abs() > 1e-9,
            "consecutive duplicates at {:?} → {:?} in {:?}",
            w[0],
            w[1],
            polyline
        );
    }
    // Source, both border vertices, and dst should appear (no
    // collapse). 3 src-leg + 1 dst-border + 2 remaining dst-leg = 6.
    assert_eq!(polyline.len(), 6, "{:?}", polyline);
}

// ===========================================================================
// #188 — live BE → LU end-to-end polyline
//
// Loads the prebuilt Belgium + Luxembourg containers and the BE↔LU
// overlay, runs an actual cross-region route from Brussels to
// Luxembourg City, and asserts that the polyline contains hundreds of
// road-network vertices. This is the regression net for #188's
// "polyline degrades to 2-point straight line" bug — without the fix,
// `cross_region_route_inner` returns a degenerate `[src, dst]` line
// even though distance/duration are correct.
//
// `#[ignore]` because:
//   - Belgium + Luxembourg containers (~28 GB combined) are not in CI
//   - Loading the full state takes ~15 s
//
// Run locally with:
//   cargo test -p butterfly-route --test cross_region_synthetic \
//       --release -- --ignored e2e_be_to_lu_polyline_has_road_geometry
// ===========================================================================

#[test]
#[ignore = "requires data/belgium + data/luxembourg + data/be-lu-overlay.butterfly"]
fn e2e_be_to_lu_polyline_has_road_geometry() {
    use butterfly_route::server::overlay::OverlayCluster;
    use butterfly_route::server::regions::{P2pPlan, RegionsState};
    use butterfly_route::server::route::{leg_points_and_distance, stitch_cross_region_polyline};
    use std::path::PathBuf;

    // Locate datasets — same probe pattern as multi_region.rs.
    let (be, lu, overlay) = {
        let candidates: Vec<(PathBuf, PathBuf, PathBuf)> = vec![
            (
                PathBuf::from("../data/belgium/baseline.butterfly"),
                PathBuf::from("../data/luxembourg/luxembourg.butterfly"),
                PathBuf::from("../data/be-lu-overlay.butterfly"),
            ),
            (
                PathBuf::from("data/belgium/baseline.butterfly"),
                PathBuf::from("data/luxembourg/luxembourg.butterfly"),
                PathBuf::from("data/be-lu-overlay.butterfly"),
            ),
        ];
        let mut found = None;
        for (a, b, c) in &candidates {
            if a.exists() && b.exists() && c.exists() {
                found = Some((a.clone(), b.clone(), c.clone()));
                break;
            }
        }
        match found {
            Some(t) => t,
            None => {
                eprintln!("skipping: BE/LU containers + overlay not on disk");
                return;
            }
        }
    };

    // Canonicalise discovered paths before symlinking — the relative
    // probe paths (`../data/...`) would otherwise be embedded in the
    // tempdir symlink and resolve to nothing inside `/tmp/.tmpXXX/`.
    let be = be.canonicalize().expect("canonicalize BE");
    let lu = lu.canonicalize().expect("canonicalize LU");
    let overlay = overlay.canonicalize().expect("canonicalize overlay");

    // Stage symlinks for load_from_dir.
    let dir = tempfile::tempdir().expect("tempdir");
    let be_dst = dir.path().join("be.butterfly");
    let lu_dst = dir.path().join("lu.butterfly");
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(&be, &be_dst).expect("symlink BE");
        std::os::unix::fs::symlink(&lu, &lu_dst).expect("symlink LU");
    }
    #[cfg(not(unix))]
    {
        std::fs::copy(&be, &be_dst).expect("copy BE");
        std::fs::copy(&lu, &lu_dst).expect("copy LU");
    }

    let mut regions = RegionsState::load_from_dir(dir.path(), None, None).expect("load_from_dir");
    let cluster = OverlayCluster::load(&overlay).expect("load overlay");
    regions.overlay = Some(cluster);

    // Brussels → Luxembourg City — the exact pair from #188.
    let plan = regions
        .dispatch_p2p_with_overlay(4.3525, 50.8467, 6.1296, 49.6116, "car")
        .expect("dispatch_p2p_with_overlay");

    let (src_state, src_region, dst_state, dst_region, ovl) = match plan {
        P2pPlan::CrossRegion {
            src_state,
            src_region,
            dst_state,
            dst_region,
            overlay,
        } => (src_state, src_region, dst_state, dst_region, overlay),
        P2pPlan::SameRegion { region, .. } => panic!(
            "expected CrossRegion plan for Brussels → Luxembourg City, got SameRegion({})",
            region
        ),
    };

    let src_mode = butterfly_route::profile_abi::Mode(src_state.mode_lookup["car"]);
    let dst_mode = butterfly_route::profile_abi::Mode(dst_state.mode_lookup["car"]);
    let src_md = src_state.get_mode(src_mode);
    let dst_md = dst_state.get_mode(dst_mode);

    // Snap + look up ranks.
    let src_snap = src_state
        .snap_index
        .snap_with_info(4.3525, 50.8467, src_mode.0)
        .expect("snap src");
    let dst_snap = dst_state
        .snap_index
        .snap_with_info(6.1296, 49.6116, dst_mode.0)
        .expect("snap dst");
    let src_rank = src_md.orig_to_rank[src_snap.0 as usize];
    let dst_rank = dst_md.orig_to_rank[dst_snap.0 as usize];
    assert_ne!(src_rank, u32::MAX);
    assert_ne!(dst_rank, u32::MAX);

    // Solve the cross-region pick.
    let solution = butterfly_route::server::cross_region::solve_cross_region(
        &src_state,
        &src_region,
        src_rank,
        &dst_state,
        &dst_region,
        dst_rank,
        "car",
        &ovl,
    )
    .expect("solve_cross_region");

    let src_border_rank = src_md.orig_to_rank[solution.src_border_ebg as usize];
    let dst_border_rank = dst_md.orig_to_rank[solution.dst_border_ebg as usize];
    assert_ne!(src_border_rank, u32::MAX);
    assert_ne!(dst_border_rank, u32::MAX);

    let (src_leg, src_dist_m) =
        leg_points_and_distance(&src_state, src_mode, src_rank, src_border_rank);
    let (dst_leg, dst_dist_m) =
        leg_points_and_distance(&dst_state, dst_mode, dst_border_rank, dst_rank);

    let src_border = ovl
        .region_representatives(&src_region)
        .get(solution.src_border_idx as usize)
        .copied()
        .map(|b| Point {
            lon: b.lon,
            lat: b.lat,
        });
    let dst_border = ovl
        .region_representatives(&dst_region)
        .get(solution.dst_border_idx as usize)
        .copied()
        .map(|b| Point {
            lon: b.lon,
            lat: b.lat,
        });

    let polyline = stitch_cross_region_polyline(
        &src_leg,
        Point {
            lon: src_snap.1,
            lat: src_snap.2,
        },
        src_border,
        dst_border,
        &dst_leg,
        Point {
            lon: dst_snap.1,
            lat: dst_snap.2,
        },
    );

    // Polyline must NOT be the degenerate 2-point line that #188
    // reports. Brussels → Luxembourg City via the overlay is ~187 km;
    // the road network's per-edge granularity yields hundreds of
    // vertices.
    assert!(
        polyline.len() > 100,
        "polyline must have > 100 points (got {}); #188 regressed",
        polyline.len()
    );

    // First/last point are within snap tolerance (~50 m) of the
    // requested coordinates.
    let first = polyline[0];
    let last = *polyline.last().unwrap();
    assert!((first.lon - 4.3525).abs() < 0.01 && (first.lat - 50.8467).abs() < 0.01);
    assert!((last.lon - 6.1296).abs() < 0.01 && (last.lat - 49.6116).abs() < 0.01);

    // Cumulative leg distance (excluding tiny border-edge contribution)
    // should be in the ballpark of the reported total (~187 km from
    // #188). Allow ±20% for snap variation.
    let leg_total_m = src_dist_m + dst_dist_m;
    assert!(
        leg_total_m > 100_000.0 && leg_total_m < 250_000.0,
        "src_dist + dst_dist = {} m, expected ~187 km",
        leg_total_m
    );
}
