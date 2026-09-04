//! Topology-only invariants on a synthetic lattice — no artifact, no
//! network, runs in CI (#587).
//!
//! Until now "exactly ONE simple polygon", "the pin is inside it",
//! "contours nest" and "route agrees with table" were asserted only by
//! `bench/postdeploy_gate.py`, i.e. AFTER a deploy, against Belgium.
//! A merge could break any of them and CI would stay green.
//!
//! This suite builds a road network in memory — a lattice of streets
//! with real coordinates, a real edge-based graph over it, one-way rows
//! so the metric is genuinely directed — and drives the PRODUCTION code
//! paths over it:
//!
//! * `server::geometry::build_isochrone_geometry_sparse` (the isochrone /
//!   catchment-hull surface) for the polygon invariants;
//! * step 6 ordering + step 7 contraction + step 8 in-memory
//!   customization for the hierarchy, then `range`'s PHAST, `matrix`'s
//!   bucket M2M and `server::query`'s bidirectional search for the
//!   route ≡ table ≡ isochrone agreement.
//!
//! Ground truth is an independent Dijkstra over the raw edge-based graph
//! written in this file — deliberately NOT the engine's own builder, so
//! an error shared by two engine paths cannot hide (the lesson of #542:
//! a network-vs-polygon check whose two sides came from the same wrong
//! builder could never see under-reach).

use std::collections::BinaryHeap;
use std::path::Path;

use butterfly_route::formats::ebg_nodes::{EbgNode, EbgNodes};
use butterfly_route::formats::mmap::ArcCow;
use butterfly_route::formats::nbg_geo::{NbgEdge, NbgGeo, PolyLine};
use butterfly_route::formats::{CchTopo, CchWeights, FilteredEbg};
use butterfly_route::model::types::Mode;
use butterfly_route::range::{ContourResult, encode_polygon_wkb};
use butterfly_route::server::edge_geom::EdgeGeometry;
use butterfly_route::server::geometry::{
    Point, ReachModel, build_isochrone_geometry_sparse, build_route_points_into,
};

// ---------------------------------------------------------------------
// The synthetic road network.
// ---------------------------------------------------------------------

/// Metres per degree — the flat-earth constants the contour rasteriser
/// itself uses, so "200 m apart" in this file is 200 m to it too.
const M_PER_DEG_LAT: f64 = 110_540.0;
const M_PER_DEG_LON: f64 = 111_320.0;

/// A `dim x dim` lattice of intersections, `spacing_m` apart, every
/// street segment costing `edge_cost_s` to traverse.
///
/// Every `oneway_row_period`-th row runs eastbound only. Columns stay
/// bidirectional, so the graph is still strongly connected — but
/// `d(s → t) != d(t → s)` for many pairs, which is the point: a
/// symmetric fixture cannot catch a reversed-adjacency bug.
struct Lattice {
    dim: usize,
    spacing_m: f64,
    edge_cost_s: u32,
    oneway_row_period: usize,
    origin_lon: f64,
    origin_lat: f64,
    /// Inclusive `(r0, r1, c0, c1)` block with no streets at all — a park,
    /// a lake, a rail yard. Big enough to survive the closing, so the
    /// reachable raster genuinely has an interior void and the tracer
    /// genuinely finds a hole ring to drop.
    void_block: Option<(usize, usize, usize, usize)>,
    /// `(from_row, from_col, to_row, to_col, cost_s)` shortcut with NO
    /// geometry. `reachable_polylines` skips geometry-less edges, so the
    /// far end becomes a reachable-but-undrawn island: a second raster
    /// component, which the product rule says must not be served.
    express: Option<(usize, usize, usize, usize, u32)>,
}

/// Directed street segment (an EBG node) as this fixture sees it.
#[derive(Clone, Copy)]
struct Segment {
    tail: u32,
    head: u32,
    /// Undirected street this segment is one of the two twins of.
    street: u32,
}

struct Network {
    lat: Lattice,
    /// Intersection coordinates, indexed by NBG node id.
    coords: Vec<(f64, f64)>,
    segments: Vec<Segment>,
    /// EBG CSR over `segments`.
    offsets: Vec<u64>,
    heads: Vec<u32>,
    /// Traversal cost of each segment, in seconds. Index = EBG node id.
    node_weights: Vec<u32>,
    /// Turn penalty per EBG arc. Zero here: step 7's witness search does
    /// not see turn penalties (#423), so a fixture with non-zero turns
    /// would be testing that known approximation, not the query engines.
    turn_penalties: Vec<u32>,
    ebg_nodes: EbgNodes,
    nbg_geo: NbgGeo,
    edge_geom: EdgeGeometry,
}

impl Lattice {
    fn node_id(&self, row: usize, col: usize) -> u32 {
        (row * self.dim + col) as u32
    }

    fn build(self) -> Network {
        let dim = self.dim;
        let dlat = self.spacing_m / M_PER_DEG_LAT;
        let dlon = self.spacing_m / (M_PER_DEG_LON * self.origin_lat.to_radians().cos());

        let mut coords = Vec::with_capacity(dim * dim);
        for row in 0..dim {
            for col in 0..dim {
                coords.push((
                    self.origin_lon + (col as f64 - dim as f64 / 2.0) * dlon,
                    self.origin_lat + (row as f64 - dim as f64 / 2.0) * dlat,
                ));
            }
        }

        // Undirected streets, then their two directed twins. A street on a
        // one-way row keeps only its eastbound twin.
        let mut segments: Vec<Segment> = Vec::new();
        let mut polylines: Vec<PolyLine> = Vec::new();
        let mut nbg_edges: Vec<NbgEdge> = Vec::new();

        let push_street = |u: u32,
                           v: u32,
                           forward_only: bool,
                           segs: &mut Vec<Segment>,
                           polys: &mut Vec<PolyLine>,
                           edges: &mut Vec<NbgEdge>| {
            let street = polys.len() as u32;
            let (ulon, ulat) = coords[u as usize];
            let (vlon, vlat) = coords[v as usize];
            polys.push(PolyLine {
                lat_fxp: vec![(ulat * 1e7) as i32, (vlat * 1e7) as i32],
                lon_fxp: vec![(ulon * 1e7) as i32, (vlon * 1e7) as i32],
            });
            edges.push(NbgEdge {
                u_node: u,
                v_node: v,
                length_mm: (self.spacing_m * 1000.0) as u32,
                bearing_deci_deg: u16::MAX,
                n_poly_pts: 2,
                poly_off: 0,
                first_osm_way_id: street as i64,
                flags: 0,
            });
            segs.push(Segment {
                tail: u,
                head: v,
                street,
            });
            if !forward_only {
                segs.push(Segment {
                    tail: v,
                    head: u,
                    street,
                });
            }
        };

        let voided = |row: usize, col: usize| match self.void_block {
            Some((r0, r1, c0, c1)) => (r0..=r1).contains(&row) && (c0..=c1).contains(&col),
            None => false,
        };

        for row in 0..dim {
            let oneway = self.oneway_row_period > 0 && row % self.oneway_row_period == 0;
            for col in 0..dim - 1 {
                if voided(row, col) || voided(row, col + 1) {
                    continue;
                }
                push_street(
                    self.node_id(row, col),
                    self.node_id(row, col + 1),
                    oneway,
                    &mut segments,
                    &mut polylines,
                    &mut nbg_edges,
                );
            }
        }
        for row in 0..dim - 1 {
            for col in 0..dim {
                if voided(row, col) || voided(row + 1, col) {
                    continue;
                }
                push_street(
                    self.node_id(row, col),
                    self.node_id(row + 1, col),
                    false,
                    &mut segments,
                    &mut polylines,
                    &mut nbg_edges,
                );
            }
        }

        // The geometry-less express link, appended last so its street id is
        // distinct from every drawn one.
        let express_segments = if let Some((r0, c0, r1, c1, _)) = self.express {
            let street = polylines.len() as u32;
            polylines.push(PolyLine {
                lat_fxp: Vec::new(),
                lon_fxp: Vec::new(),
            });
            nbg_edges.push(NbgEdge {
                u_node: self.node_id(r0, c0),
                v_node: self.node_id(r1, c1),
                length_mm: 0,
                bearing_deci_deg: u16::MAX,
                n_poly_pts: 0,
                poly_off: 0,
                first_osm_way_id: street as i64,
                flags: 0,
            });
            segments.push(Segment {
                tail: self.node_id(r0, c0),
                head: self.node_id(r1, c1),
                street,
            });
            vec![segments.len() - 1]
        } else {
            Vec::new()
        };

        // EBG arcs: segment e -> segment f when head(e) == tail(f), minus
        // the U-turn back onto the same street.
        let mut by_tail: Vec<Vec<u32>> = vec![Vec::new(); dim * dim];
        for (idx, seg) in segments.iter().enumerate() {
            by_tail[seg.tail as usize].push(idx as u32);
        }
        let mut offsets = Vec::with_capacity(segments.len() + 1);
        let mut heads: Vec<u32> = Vec::new();
        offsets.push(0u64);
        for seg in &segments {
            for &succ in &by_tail[seg.head as usize] {
                if segments[succ as usize].street != seg.street {
                    heads.push(succ);
                }
            }
            offsets.push(heads.len() as u64);
        }

        let mut node_weights = vec![self.edge_cost_s; segments.len()];
        if let (Some((_, _, _, _, cost)), Some(&idx)) = (self.express, express_segments.first()) {
            node_weights[idx] = cost;
        }
        let turn_penalties = vec![0u32; heads.len()];

        let ebg_nodes = EbgNodes {
            n_nodes: segments.len() as u32,
            created_unix: 0,
            inputs_sha: [0u8; 32],
            nodes: ArcCow::from_vec(
                segments
                    .iter()
                    .map(|s| EbgNode {
                        tail_nbg: s.tail,
                        head_nbg: s.head,
                        geom_idx: s.street,
                        length_m: self.spacing_m as u32,
                        class_bits: 0,
                        primary_way: s.street,
                    })
                    .collect(),
            ),
        };

        let nbg_geo = NbgGeo {
            n_edges_und: polylines.len() as u64,
            edges: nbg_edges,
            polylines,
        };
        let edge_geom = EdgeGeometry::from_legacy_polylines(&nbg_geo);

        Network {
            lat: self,
            coords,
            segments,
            offsets,
            heads,
            node_weights,
            turn_penalties,
            ebg_nodes,
            nbg_geo,
            edge_geom,
        }
    }
}

impl Network {
    fn n_segments(&self) -> usize {
        self.segments.len()
    }

    /// A stable way to name a start edge: the first segment leaving
    /// intersection `(row, col)`.
    fn segment_from(&self, row: usize, col: usize) -> u32 {
        let node = self.lat.node_id(row, col);
        self.segments
            .iter()
            .position(|s| s.tail == node)
            .expect("every intersection has at least one outgoing segment") as u32
    }

    /// Independent ground truth. Labels follow the engine's convention:
    /// `dist[e]` is the arrival time at the HEAD of segment `e`, the seed
    /// segment costs 0, and traversing arc `e -> f` costs
    /// `w(f) + turn(e -> f)`.
    fn reference_dijkstra(&self, seed: u32) -> Vec<u32> {
        let mut dist = vec![u32::MAX; self.n_segments()];
        let mut heap: BinaryHeap<std::cmp::Reverse<(u32, u32)>> = BinaryHeap::new();
        dist[seed as usize] = 0;
        heap.push(std::cmp::Reverse((0, seed)));
        while let Some(std::cmp::Reverse((d, e))) = heap.pop() {
            if d > dist[e as usize] {
                continue;
            }
            let start = self.offsets[e as usize] as usize;
            let end = self.offsets[e as usize + 1] as usize;
            for slot in start..end {
                let f = self.heads[slot];
                let nd = d + self.node_weights[f as usize] + self.turn_penalties[slot];
                if nd < dist[f as usize] {
                    dist[f as usize] = nd;
                    heap.push(std::cmp::Reverse((nd, f)));
                }
            }
        }
        dist
    }

    /// `(ebg_id, label)` pairs within `threshold_s`, the shape
    /// `build_isochrone_geometry_sparse` consumes.
    fn settled_within(&self, dist: &[u32], threshold_s: u32) -> Vec<(u32, u32)> {
        dist.iter()
            .enumerate()
            .filter(|&(_, &d)| d <= threshold_s)
            .map(|(i, &d)| (i as u32, d))
            .collect()
    }

    /// Start point of segment `e`, in (lon, lat).
    fn segment_start(&self, e: u32) -> (f64, f64) {
        self.coords[self.segments[e as usize].tail as usize]
    }

    /// A `FilteredEbg` over the whole network — every segment is
    /// accessible, so filtered ids equal original ids.
    fn filtered_ebg(&self) -> FilteredEbg {
        let n = self.n_segments();
        FilteredEbg {
            mode: Mode(0),
            n_filtered_nodes: n as u32,
            n_filtered_arcs: self.heads.len() as u64,
            n_original_nodes: n as u32,
            inputs_sha: [0u8; 32],
            offsets: ArcCow::from_vec(self.offsets.clone()),
            heads: ArcCow::from_vec(self.heads.clone()),
            original_arc_idx: ArcCow::from_vec((0..self.heads.len() as u32).collect()),
            filtered_to_original: ArcCow::from_vec((0..n as u32).collect()),
            original_to_filtered: ArcCow::from_vec((0..n as u32).collect()),
        }
    }
}

// ---------------------------------------------------------------------
// Polygon helpers (assertions only — nothing here is under test).
// ---------------------------------------------------------------------

/// Decode a little-endian WKB Polygon and return its rings. Panics with a
/// precise message on anything that is not exactly one Polygon — a
/// MultiPolygon here is a FAILURE, not a shape to be tolerated (#497).
fn wkb_polygon_rings(wkb: &[u8]) -> Vec<Vec<(f64, f64)>> {
    assert_eq!(wkb[0], 1, "WKB must be little-endian");
    let u32_at = |off: usize| u32::from_le_bytes(wkb[off..off + 4].try_into().unwrap());
    let f64_at = |off: usize| f64::from_le_bytes(wkb[off..off + 8].try_into().unwrap());
    let geom_type = u32_at(1);
    assert_eq!(
        geom_type, 3,
        "served geometry is WKB type {geom_type}, not a Polygon (3);          a MultiPolygon (6) means the one-polygon rule broke"
    );
    let n_rings = u32_at(5) as usize;
    let mut off = 9;
    let mut rings = Vec::with_capacity(n_rings);
    for _ in 0..n_rings {
        let n_pts = u32_at(off) as usize;
        off += 4;
        let mut ring = Vec::with_capacity(n_pts);
        for _ in 0..n_pts {
            ring.push((f64_at(off), f64_at(off + 8)));
            off += 16;
        }
        rings.push(ring);
    }
    assert_eq!(off, wkb.len(), "trailing bytes after the WKB Polygon");
    rings
}

fn ring_is_closed(ring: &[(f64, f64)]) -> bool {
    ring.len() >= 4 && ring[0] == ring[ring.len() - 1]
}

/// Signed area x2 in degree space; positive == counter-clockwise.
fn signed_area2(ring: &[(f64, f64)]) -> f64 {
    let mut acc = 0.0;
    for w in ring.windows(2) {
        acc += (w[1].0 - w[0].0) * (w[1].1 + w[0].1);
    }
    -acc
}

/// Ray casting. `true` when (lon, lat) is inside `ring`.
fn point_in_ring(ring: &[(f64, f64)], lon: f64, lat: f64) -> bool {
    let mut inside = false;
    let n = ring.len();
    let mut j = n - 1;
    for i in 0..n {
        let (xi, yi) = ring[i];
        let (xj, yj) = ring[j];
        if (yi > lat) != (yj > lat) && lon < (xj - xi) * (lat - yi) / (yj - yi) + xi {
            inside = !inside;
        }
        j = i;
    }
    inside
}

/// Shortest distance in metres from (lon, lat) to the ring's boundary.
fn dist_to_ring_m(ring: &[(f64, f64)], lon: f64, lat: f64, lat_ref: f64) -> f64 {
    let kx = M_PER_DEG_LON * lat_ref.to_radians().cos();
    let ky = M_PER_DEG_LAT;
    let mut best = f64::INFINITY;
    for w in ring.windows(2) {
        let (ax, ay) = ((w[0].0 - lon) * kx, (w[0].1 - lat) * ky);
        let (bx, by) = ((w[1].0 - lon) * kx, (w[1].1 - lat) * ky);
        let (dx, dy) = (bx - ax, by - ay);
        let len2 = dx * dx + dy * dy;
        let t = if len2 <= f64::EPSILON {
            0.0
        } else {
            (-(ax * dx + ay * dy) / len2).clamp(0.0, 1.0)
        };
        let (px, py) = (ax + t * dx, ay + t * dy);
        best = best.min((px * px + py * py).sqrt());
    }
    best
}

/// No vertex may repeat except the closing one, and no `a,b,a` spur may
/// survive — the two ways a traced ring stops being a simple polygon.
fn ring_is_simple(ring: &[(f64, f64)]) -> Result<(), String> {
    let body = &ring[..ring.len() - 1];
    for i in 0..body.len() {
        for j in i + 1..body.len() {
            if body[i] == body[j] {
                return Err(format!("vertex {i} repeats at {j}: {:?}", body[i]));
            }
        }
    }
    for w in ring.windows(3) {
        if w[0] == w[2] {
            return Err(format!("a,b,a spur at {:?}", w[1]));
        }
    }
    Ok(())
}

/// The one contour the engine serves for `threshold_s`, as its outer ring.
fn serve_contour_pinned(
    net: &Network,
    dist: &[u32],
    seed: u32,
    threshold_s: u32,
    pin: Option<(f64, f64)>,
) -> Vec<(f64, f64)> {
    let settled = net.settled_within(dist, threshold_s);
    let anchor = net.segment_start(seed);
    let polys = build_isochrone_geometry_sparse(
        &settled,
        threshold_s,
        &net.node_weights,
        &net.ebg_nodes,
        &net.edge_geom,
        "car",
        Some(anchor),
        pin.or(Some(anchor)),
        // Whole reached edges only: the frontier fragments are the
        // handler's business (`depart_frontier`), and this suite is about
        // the surface the fragments feed into.
        &ReachModel::Depart { frontier: &[] },
    );
    assert_eq!(
        polys.len(),
        1,
        "an isochrone is ONE simple polygon by definition (#497); \
         threshold {threshold_s} s served {} of them",
        polys.len()
    );
    assert!(
        polys[0].holes.is_empty(),
        "threshold {threshold_s} s served {} hole(s); the product rule is \
         one simple polygon, no holes",
        polys[0].holes.len()
    );
    // Assert on what is actually SERVED, not on the intermediate struct:
    // the WKB encoder is what /isochrone, /isochrone/bulk, the Flight
    // `isochrone` action and the catchment hulls all go through.
    let wkb = encode_polygon_wkb(&ContourResult::from_topology(polys))
        .expect("the served contour must encode to WKB");
    let rings = wkb_polygon_rings(&wkb);
    assert_eq!(
        rings.len(),
        1,
        "threshold {threshold_s} s: served WKB has {} rings; one simple \
         polygon means exactly one",
        rings.len()
    );
    rings.into_iter().next().unwrap()
}

fn serve_contour(net: &Network, dist: &[u32], seed: u32, threshold_s: u32) -> Vec<(f64, f64)> {
    serve_contour_pinned(net, dist, seed, threshold_s, None)
}

fn geometry_lattice() -> Network {
    // 200 m block spacing, 30 s per block. From the middle intersection
    // the 300 / 600 / 1200 s contours sit at 10 / 20 / 40 blocks, all
    // inside a 41 x 41 lattice.
    Lattice {
        dim: 41,
        spacing_m: 200.0,
        edge_cost_s: 30,
        oneway_row_period: 5,
        origin_lon: 4.35,
        origin_lat: 50.85,
        void_block: None,
        express: None,
    }
    .build()
}

/// The same lattice with the two shapes the "ONE simple polygon" rule
/// exists for: a 1.2 km void inside the reachable area (an interior ring
/// the tracer WILL find and must drop) and a geometry-less shortcut to the
/// far corner (a reachable-but-undrawn island, i.e. a second raster
/// component that must not be served).
fn pathological_lattice() -> Network {
    Lattice {
        dim: 41,
        spacing_m: 200.0,
        edge_cost_s: 30,
        oneway_row_period: 5,
        origin_lon: 4.35,
        origin_lat: 50.85,
        void_block: Some((23, 28, 23, 28)),
        express: Some((20, 20, 1, 1, 30)),
    }
    .build()
}

// ---------------------------------------------------------------------
// Polygon invariants — the post-deploy gate's `gate_isochrone_topology`,
// minus the data.
// ---------------------------------------------------------------------

#[test]
fn served_contour_is_one_simple_ccw_polygon() {
    let net = geometry_lattice();
    let seed = net.segment_from(20, 20);
    let dist = net.reference_dijkstra(seed);

    for threshold in [300u32, 600, 1200] {
        let ring = serve_contour(&net, &dist, seed, threshold);
        assert!(
            ring_is_closed(&ring),
            "threshold {threshold} s: ring is not a closed polygon ({} points)",
            ring.len()
        );
        if let Err(why) = ring_is_simple(&ring) {
            panic!("threshold {threshold} s: ring is not simple — {why}");
        }
        assert!(
            signed_area2(&ring) > 0.0,
            "threshold {threshold} s: outer ring must be CCW"
        );
    }
}

/// A void inside the reachable area and a detached reachable island are
/// exactly what `keep_holes: false` and the single-component truncation
/// exist for. Serving either as a hole or as a MultiPolygon breaks the
/// product rule (#497), so both must be gone from the WKB.
#[test]
fn a_void_and_a_detached_island_still_serve_one_simple_polygon() {
    let net = pathological_lattice();
    let seed = net.segment_from(20, 20);
    let dist = net.reference_dijkstra(seed);

    // The fixture must actually pose the problem, or the test is theatre.
    let far = net.segment_from(1, 1);
    assert!(
        dist[far as usize] <= 300,
        "the express link did not make the far corner reachable within 300 s \
         (label {})",
        dist[far as usize]
    );
    let void_centre = net.lat.node_id(25, 25);
    assert!(
        !net.segments.iter().any(|s| s.tail == void_centre),
        "the void block still has streets in it"
    );

    for threshold in [300u32, 600, 1200] {
        let ring = serve_contour(&net, &dist, seed, threshold);
        assert!(
            ring_is_closed(&ring),
            "threshold {threshold} s: ring is not closed ({} points)",
            ring.len()
        );
        if let Err(why) = ring_is_simple(&ring) {
            panic!("threshold {threshold} s: ring is not simple — {why}");
        }
        assert!(
            signed_area2(&ring) > 0.0,
            "threshold {threshold} s: outer ring must be CCW"
        );
        // The served component is the origin's, always.
        let (olon, olat) = net.segment_start(seed);
        assert!(
            point_in_ring(&ring, olon, olat),
            "threshold {threshold} s: the served component is not the origin's"
        );
    }
}

#[test]
fn origin_and_pin_are_inside_the_served_contour() {
    let net = geometry_lattice();
    let seed = net.segment_from(20, 20);
    let dist = net.reference_dijkstra(seed);
    let (olon, olat) = net.segment_start(seed);
    let kx = M_PER_DEG_LON * olat.to_radians().cos();

    for threshold in [300u32, 600, 1200] {
        let ring = serve_contour(&net, &dist, seed, threshold);
        assert!(
            point_in_ring(&ring, olon, olat),
            "threshold {threshold} s: the snapped origin is outside its own isochrone"
        );
        // #535: a pin up to 500 m off the network is stamped through the
        // access leg and must land inside too.
        for (dx_m, dy_m) in [(120.0, 0.0), (-120.0, 0.0), (0.0, 120.0), (0.0, -120.0)] {
            let plon = olon + dx_m / kx;
            let plat = olat + dy_m / M_PER_DEG_LAT;
            let ring = serve_contour_pinned(&net, &dist, seed, threshold, Some((plon, plat)));
            assert!(
                point_in_ring(&ring, plon, plat),
                "threshold {threshold} s: pin {dx_m},{dy_m} m from the snap is outside \
                 its own isochrone (#535)"
            );
        }
    }
}

#[test]
fn contours_nest_300_in_600_in_1200() {
    let net = geometry_lattice();
    let seed = net.segment_from(20, 20);
    let dist = net.reference_dijkstra(seed);
    let (_, olat) = net.segment_start(seed);

    let rings: Vec<(u32, Vec<(f64, f64)>)> = [300u32, 600, 1200]
        .into_iter()
        .map(|t| (t, serve_contour(&net, &dist, seed, t)))
        .collect();

    for pair in rings.windows(2) {
        let (t_in, inner) = &pair[0];
        let (t_out, outer) = &pair[1];

        // Raster quantisation: the outer contour is rasterised on coarser
        // cells (30 m below 600 s, 60 m below 3600 s) and simplified with a
        // matching tolerance, so a vertex of the inner ring can fall just
        // outside the outer one where both hug the same street. Budget one
        // coarse cell plus its simplification tolerance; beyond that it is a
        // real containment break, not quantisation.
        let slack_m = 120.0;
        let mut worst = 0.0f64;
        let mut n_out = 0usize;
        for &(lon, lat) in inner.iter() {
            if !point_in_ring(outer, lon, lat) {
                let d = dist_to_ring_m(outer, lon, lat, olat);
                worst = worst.max(d);
                if d > slack_m {
                    n_out += 1;
                }
            }
        }
        assert_eq!(
            n_out, 0,
            "{t_in} s contour escapes the {t_out} s contour at {n_out} vertices \
             (worst {worst:.0} m outside, budget {slack_m:.0} m)"
        );

        // Nesting is not just containment: the bigger threshold must
        // actually enclose more ground.
        assert!(
            signed_area2(outer) > signed_area2(inner),
            "{t_out} s contour is not larger than the {t_in} s one"
        );
    }
}

/// `gate_isochrone_topology`'s coverage clause, data-free: at most 1.5 %
/// of the network the engine itself says is reachable may sit more than
/// 150 m outside the polygon it serves.
///
/// This is the assertion the #542 "stamp counted the edge weight twice"
/// bug would have tripped: every fast boundary edge was cut one weight
/// short, so reachable road ended up outside the drawn shape.
#[test]
fn served_contour_covers_the_reachable_network() {
    let net = geometry_lattice();
    let seed = net.segment_from(20, 20);
    let dist = net.reference_dijkstra(seed);
    let (_, olat) = net.segment_start(seed);

    for threshold in [300u32, 600, 1200] {
        let ring = serve_contour(&net, &dist, seed, threshold);
        let reachable = net.settled_within(&dist, threshold);
        assert!(
            reachable.len() > 100,
            "threshold {threshold} s reached only {} segments — the fixture is \
             not exercising anything",
            reachable.len()
        );

        let mut outside = 0usize;
        let mut worst = 0.0f64;
        for &(seg, _) in &reachable {
            // Both endpoints of every reachable street, the same set the
            // gate walks over the served network.
            for node in [
                net.segments[seg as usize].tail,
                net.segments[seg as usize].head,
            ] {
                let (lon, lat) = net.coords[node as usize];
                if !point_in_ring(&ring, lon, lat) {
                    let d = dist_to_ring_m(&ring, lon, lat, olat);
                    worst = worst.max(d);
                    if d > 150.0 {
                        outside += 1;
                    }
                }
            }
        }
        let total = reachable.len() * 2;
        let pct = 100.0 * outside as f64 / total as f64;
        assert!(
            pct <= 1.5,
            "threshold {threshold} s: {pct:.2} % of the reachable network \
             ({outside}/{total} points) is more than 150 m outside the served \
             polygon (worst {worst:.0} m); the budget is 1.5 %"
        );
    }
}

#[test]
fn reach_is_monotone_in_the_threshold() {
    let net = geometry_lattice();
    let seed = net.segment_from(20, 20);
    let dist = net.reference_dijkstra(seed);

    let mut prev: Vec<u32> = Vec::new();
    for threshold in [300u32, 600, 1200] {
        let now: Vec<u32> = net
            .settled_within(&dist, threshold)
            .into_iter()
            .map(|(e, _)| e)
            .collect();
        for e in &prev {
            assert!(
                now.contains(e),
                "segment {e} reachable within a smaller threshold but not within {threshold} s"
            );
        }
        assert!(
            now.len() > prev.len(),
            "threshold {threshold} s did not reach further than the previous one"
        );
        prev = now;
    }
}

// ---------------------------------------------------------------------
// Hierarchy invariants — route ≡ table ≡ isochrone, all on the same
// synthetic CCH built by the real steps 6, 7 and 8.
// ---------------------------------------------------------------------

/// A lattice small enough that a debug-profile contraction is instant, and
/// still big enough for the hierarchy to have real depth.
fn hierarchy_lattice() -> Network {
    Lattice {
        dim: 13,
        spacing_m: 200.0,
        edge_cost_s: 30,
        oneway_row_period: 4,
        origin_lon: 4.35,
        origin_lat: 50.85,
        void_block: None,
        express: None,
    }
    .build()
}

struct Hierarchy {
    topo: CchTopo,
    weights: CchWeights,
    /// Kept alive: step 6 / 7 wrote into it and step 7's topo was read
    /// back from it.
    _dir: tempfile::TempDir,
}

/// Run the production steps 6 → 8 over the synthetic network.
fn contract(net: &Network) -> Hierarchy {
    use butterfly_route::formats::{
        EbgNodesFile, FilteredEbgFile, ModTurns, ModWeights, NbgGeoFile, mod_turns, mod_weights,
    };

    let dir = tempfile::tempdir().expect("tempdir");
    let p = |name: &str| dir.path().join(name);

    let filtered = net.filtered_ebg();
    FilteredEbgFile::write(p("filtered.ebg"), &filtered).expect("write filtered ebg");
    EbgNodesFile::write(p("ebg.nodes"), &net.ebg_nodes).expect("write ebg nodes");
    NbgGeoFile::write(p("nbg.geo"), &net.nbg_geo).expect("write nbg geo");
    mod_weights::write(
        p("w.car.u32"),
        &ModWeights {
            mode: Mode(0),
            weights: std::borrow::Cow::Owned(net.node_weights.clone()),
            inputs_sha: [0u8; 16],
        },
    )
    .expect("write weights");
    mod_turns::write(
        p("t.car.u32"),
        &ModTurns {
            mode: Mode(0),
            penalties: net.turn_penalties.clone(),
            inputs_sha: [0u8; 16],
        },
    )
    .expect("write turns");

    butterfly_route::ordering::generate_ordering(butterfly_route::ordering::Step6Config {
        filtered_ebg_path: p("filtered.ebg"),
        ebg_nodes_path: p("ebg.nodes"),
        nbg_geo_path: p("nbg.geo"),
        mode: Mode(0),
        mode_name: "car".to_string(),
        outdir: dir.path().to_path_buf(),
        leaf_threshold: 16,
        balance_eps: 0.2,
    })
    .expect("step 6 ordering");

    let step7 = butterfly_route::contraction::build_cch_topology(
        butterfly_route::contraction::Step7Config {
            filtered_ebg_path: p("filtered.ebg"),
            order_path: order_path(dir.path()),
            weights_path: p("w.car.u32"),
            turns_path: p("t.car.u32"),
            mode: Mode(0),
            mode_name: "car".to_string(),
            outdir: dir.path().to_path_buf(),
        },
    )
    .expect("step 7 contraction");

    let topo = butterfly_route::formats::CchTopoFile::read(&step7.topo_path).expect("read topo");
    let (weights, _) = butterfly_route::customization::customize_cch_time_in_memory(
        &topo,
        &filtered,
        &net.node_weights,
        &net.turn_penalties,
    )
    .expect("step 8 customization");

    Hierarchy {
        topo,
        weights,
        _dir: dir,
    }
}

/// Step 6 names its output by mode; find it rather than hard-coding.
fn order_path(dir: &Path) -> std::path::PathBuf {
    std::fs::read_dir(dir)
        .expect("read outdir")
        .flatten()
        .map(|e| e.path())
        .find(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("order") && n.contains("car"))
        })
        .expect("step 6 must have written an order file")
}

/// rank -> segment and its inverse. This fixture's filtered ids are its
/// original ids, so `rank_to_filtered` maps rank straight to segment.
fn rank_maps(h: &Hierarchy, n: usize) -> (Vec<u32>, Vec<u32>) {
    let rank_to_seg: Vec<u32> = (0..n).map(|r| h.topo.rank_to_filtered[r]).collect();
    let mut seg_to_rank = vec![u32::MAX; n];
    for (rank, &seg) in rank_to_seg.iter().enumerate() {
        seg_to_rank[seg as usize] = rank as u32;
    }
    (rank_to_seg, seg_to_rank)
}

#[test]
fn phast_table_and_route_all_agree_with_an_independent_dijkstra() {
    use butterfly_route::matrix::bucket_ch::{
        DownAdjFlat, DownReverseAdjFlat, UpAdjFlat, table_bucket_full_flat,
    };
    use butterfly_route::range::phast_seeded::run_phast_bounded_fast;
    use butterfly_route::server::query::CchQuery;

    let net = hierarchy_lattice();
    let h = contract(&net);
    let n = net.n_segments();
    assert_eq!(
        h.topo.n_nodes as usize, n,
        "hierarchy must cover every segment"
    );

    let up = UpAdjFlat::build_with(&h.topo, &h.weights, true);
    let down_rev = DownReverseAdjFlat::build_with(&h.topo, &h.weights, true);
    let down = DownAdjFlat::build(&h.topo, &h.weights);
    let (rank_to_seg, seg_to_rank) = rank_maps(&h, n);

    // Sources and targets spread across the lattice, including rows that
    // are one-way, so the directed case is covered.
    let sources: Vec<u32> = [(0usize, 0usize), (6, 6), (4, 9), (12, 12), (8, 1)]
        .into_iter()
        .map(|(r, c)| net.segment_from(r, c))
        .collect();
    let targets: Vec<u32> = [(1usize, 11usize), (6, 6), (11, 2), (0, 12), (9, 7)]
        .into_iter()
        .map(|(r, c)| net.segment_from(r, c))
        .collect();

    let src_ranks: Vec<u32> = sources.iter().map(|&s| seg_to_rank[s as usize]).collect();
    let tgt_ranks: Vec<u32> = targets.iter().map(|&t| seg_to_rank[t as usize]).collect();

    let (table, _) = table_bucket_full_flat(n, &up, &down_rev, &src_ranks, &tgt_ranks);
    let query = CchQuery::with_custom_weights(&h.topo, &up, &down_rev, &h.weights);

    let mut asymmetric_pairs = 0usize;
    for (si, &src) in sources.iter().enumerate() {
        let truth = net.reference_dijkstra(src);

        // 1. PHAST — the isochrone engine — must reproduce the whole field.
        let phast = run_phast_bounded_fast(&up, &down, src_ranks[si], u32::MAX - 1, Mode(0));
        let mut phast_by_seg = vec![u32::MAX; n];
        for (rank, d) in phast {
            phast_by_seg[rank_to_seg[rank as usize] as usize] = d;
        }
        for seg in 0..n {
            assert_eq!(
                phast_by_seg[seg], truth[seg],
                "PHAST disagrees with the reference Dijkstra from segment {src} to {seg}"
            );
        }

        for (ti, &tgt) in targets.iter().enumerate() {
            let expected = truth[tgt as usize];

            // 2. Table — the matrix engine.
            let cell = table[si * targets.len() + ti];
            assert_eq!(
                cell, expected,
                "table disagrees with the reference Dijkstra for {src} -> {tgt}"
            );

            // 3. Route — the bidirectional P2P engine. Zero tolerance:
            // route, table and isochrone read the same hierarchy.
            let routed = query.distance(src_ranks[si], tgt_ranks[ti]);
            let want = (expected != u32::MAX).then_some(expected);
            assert_eq!(
                routed, want,
                "route disagrees with the reference Dijkstra for {src} -> {tgt}"
            );

            if expected != u32::MAX {
                let back = net.reference_dijkstra(tgt)[src as usize];
                if back != expected {
                    asymmetric_pairs += 1;
                    let routed_back = query.distance(tgt_ranks[ti], src_ranks[si]);
                    assert_eq!(
                        routed_back,
                        (back != u32::MAX).then_some(back),
                        "route lost the directed asymmetry for {tgt} -> {src}"
                    );
                }
            }
        }
    }

    assert!(
        asymmetric_pairs > 0,
        "the fixture's one-way rows produced no asymmetric pair — the directed \
         semantics are not actually being exercised"
    );
}

#[test]
fn isochrone_reach_matches_the_table_on_the_synthetic_hierarchy() {
    use butterfly_route::matrix::bucket_ch::{
        DownAdjFlat, DownReverseAdjFlat, UpAdjFlat, table_bucket_full_flat,
    };
    use butterfly_route::range::phast_seeded::run_phast_bounded_fast;

    // `gate_isochrone_reach_truth`, data-free: /table is the independent
    // truth for what the isochrone claims to reach.
    let net = hierarchy_lattice();
    let h = contract(&net);
    let n = net.n_segments();

    let up = UpAdjFlat::build(&h.topo, &h.weights);
    let down_rev = DownReverseAdjFlat::build(&h.topo, &h.weights);
    let down = DownAdjFlat::build(&h.topo, &h.weights);
    let (_, seg_to_rank) = rank_maps(&h, n);

    let src_rank = seg_to_rank[net.segment_from(6, 6) as usize];
    let threshold = 240u32;

    let reached: Vec<u32> = run_phast_bounded_fast(&up, &down, src_rank, threshold, Mode(0))
        .into_iter()
        .filter(|&(_, d)| d <= threshold)
        .map(|(rank, _)| rank)
        .collect();
    assert!(
        !reached.is_empty(),
        "a {threshold} s isochrone reached nothing on a lattice of {n} segments"
    );

    let all_ranks: Vec<u32> = (0..n as u32).collect();
    let (row, _) = table_bucket_full_flat(n, &up, &down_rev, &[src_rank], &all_ranks);

    for (rank, &table_s) in row.iter().enumerate().take(n) {
        let within_table = table_s <= threshold;
        let within_iso = reached.contains(&(rank as u32));
        assert_eq!(
            within_iso, within_table,
            "segment at rank {rank}: isochrone says reachable={within_iso}, \
             table says {table_s} s"
        );
    }
}

/// #567: `/table` and `/trip` used to each hand-roll the K-best combo
/// escalation; they now share `snap_kbest::cell_with_kbest_fallback`.
/// Its bounded rule — a recovered cell is accepted ONLY when the pair's
/// travel time is ≤ the caller's threshold — is the guard `/trip`'s copy
/// never had, and it must hold on BOTH channels: under a bound the time
/// query gates the distance one, so a distance-only recovery cannot slip
/// past the threshold either.
///
/// The fixture drives both channels off the same time metric — the rule
/// under test is the gating, not the units.
#[test]
fn bounded_cell_fallback_never_recovers_past_the_threshold() {
    use butterfly_route::matrix::bucket_ch::{DownReverseAdjFlat, UpAdjFlat};
    use butterfly_route::server::query::CchQuery;
    use butterfly_route::server::snap_kbest::{
        DEFAULT_MAX_FALLBACK_COMBOS, cell_with_kbest_fallback,
    };

    let net = hierarchy_lattice();
    let h = contract(&net);
    let n = net.n_segments();
    let up = UpAdjFlat::build_with(&h.topo, &h.weights, true);
    let down_rev = DownReverseAdjFlat::build_with(&h.topo, &h.weights, true);
    let (_rank_to_seg, seg_to_rank) = rank_maps(&h, n);
    let query = CchQuery::with_custom_weights(&h.topo, &up, &down_rev, &h.weights);

    let src = net.segment_from(0, 0);
    let dst = net.segment_from(11, 9);
    // Ground truth from the independent Dijkstra in this file, not from
    // the engine path under test.
    let truth = net.reference_dijkstra(src)[dst as usize];
    assert!(
        truth != u32::MAX && truth > 1,
        "the fixture pair must be reachable and non-trivial, got {truth}"
    );

    let src_rank = seg_to_rank[src as usize];
    let dst_rank = seg_to_rank[dst as usize];
    // Candidate lists that force the escalation past combo (0, 0): the
    // first source candidate IS the destination, so (0, 0) is skipped and
    // only (1, 0) can connect. This is a recovered cell, not a primary.
    let src_ranks = vec![dst_rank, src_rank];
    let dst_ranks = vec![dst_rank];

    let unbounded = cell_with_kbest_fallback(
        Some(&query),
        Some(&query),
        &src_ranks,
        &dst_ranks,
        true,
        true,
        None,
        DEFAULT_MAX_FALLBACK_COMBOS,
    );
    assert_eq!(
        unbounded.time,
        Some(truth),
        "unbounded: the escalation must recover the reference cost"
    );
    assert_eq!(
        unbounded.distance,
        Some(truth),
        "unbounded: the distance channel must recover too"
    );

    let at_bound = cell_with_kbest_fallback(
        Some(&query),
        Some(&query),
        &src_ranks,
        &dst_ranks,
        true,
        true,
        Some(truth),
        DEFAULT_MAX_FALLBACK_COMBOS,
    );
    assert_eq!(
        at_bound.time,
        Some(truth),
        "the bound is inclusive: a cell exactly at the threshold stays recovered"
    );
    assert_eq!(
        at_bound.distance,
        Some(truth),
        "the bound is inclusive on the distance channel too"
    );

    let past_bound = cell_with_kbest_fallback(
        Some(&query),
        Some(&query),
        &src_ranks,
        &dst_ranks,
        true,
        true,
        Some(truth - 1),
        DEFAULT_MAX_FALLBACK_COMBOS,
    );
    assert_eq!(
        past_bound.time, None,
        "a cell whose travel time exceeds the bound must NOT be recovered"
    );
    assert_eq!(
        past_bound.distance, None,
        "the time bound must gate the DISTANCE channel too"
    );
}

// ---------------------------------------------------------------------
// Route geometry — the polyline a caller draws must BE the route the
// same call reports.
// ---------------------------------------------------------------------

/// Planar length of a polyline, in metres, under the same flat-earth
/// constants the fixture used to place its intersections. Nothing here is
/// under test — this is the ruler.
fn polyline_length_m(points: &[Point], lat_ref: f64) -> f64 {
    let m_per_deg_lon = M_PER_DEG_LON * lat_ref.to_radians().cos();
    points
        .windows(2)
        .map(|w| {
            let dx = (w[1].lon - w[0].lon) * m_per_deg_lon;
            let dy = (w[1].lat - w[0].lat) * M_PER_DEG_LAT;
            (dx * dx + dy * dy).sqrt()
        })
        .sum()
}

/// One lattice per traversal shape. As far as route geometry is
/// concerned modes differ in exactly ONE thing: how much of the network
/// is bidirectional. A car network has one-way rows; a bike network has
/// fewer; a foot network has none at all, so EVERY street is traversed
/// against its stored polyline orientation by half the routes.
///
/// The geometry builder takes no mode — which is precisely why the bug
/// this test guards presented as "driving is fine, walking is 2x": the
/// more bidirectional the network, the more traversals run against the
/// stored orientation, and each one drawn forward anyway is an
/// out-and-back that adds its own length twice.
fn traversal_lattice(oneway_row_period: usize) -> Network {
    Lattice {
        dim: 13,
        spacing_m: 200.0,
        edge_cost_s: 30,
        oneway_row_period,
        origin_lon: 4.35,
        origin_lat: 50.85,
        void_block: None,
        express: None,
    }
    .build()
}

/// A polyline whose length is ~2x the distance the same response reports
/// is what a consumer sees as "the route drawn twice on different
/// paths"; anything derived from the geometry (a midpoint at
/// `length / 2`, an overlap ratio) is wrong with it. The engine's own
/// `distance_m` is the sum of the traversed edges' `length_m`, so the
/// polyline it emits for that same edge sequence must measure the same.
///
/// This drives `build_route_points_into` — the ONE builder behind both
/// `/route` and the Flight `route_batch` batch surface (`route_batch`'s
/// per-pair `distance_m` and `geometry_wkb` are this function's return
/// value and this function's points). Only the endpoint seeding differs
/// between the two, and seeding picks *which* start rank, not how the
/// path between them is drawn.
#[test]
fn route_geometry_length_matches_the_reported_distance_on_every_traversal_shape() {
    use butterfly_route::matrix::bucket_ch::{DownReverseAdjFlat, UpAdjFlat};
    use butterfly_route::server::query::CchQuery;

    // (label, one-way row period). 0 = every street two-way.
    for (shape, oneway_row_period) in [("drive", 4usize), ("cycle", 7), ("walk", 0)] {
        let net = traversal_lattice(oneway_row_period);
        let h = contract(&net);
        let n = net.n_segments();
        // `build_with(.., true)` keeps the topo edge index the parent
        // chains are recorded in — the unpack needs it.
        let up = UpAdjFlat::build_with(&h.topo, &h.weights, true);
        let down_rev = DownReverseAdjFlat::build_with(&h.topo, &h.weights, true);
        let (_, seg_to_rank) = rank_maps(&h, n);
        let query = CchQuery::with_custom_weights(&h.topo, &up, &down_rev, &h.weights);
        let identity: Vec<u32> = (0..n as u32).collect();

        // Corners, edges and interior, both directions, so long routes
        // and short ones are both covered.
        let waypoints = [
            (0usize, 0usize),
            (0, 12),
            (12, 0),
            (12, 12),
            (6, 6),
            (3, 9),
            (9, 3),
            (7, 1),
            (1, 7),
            (5, 6),
        ];

        let mut rank_path = Vec::new();
        let mut ebg_path = Vec::new();
        let mut points = Vec::new();
        let mut pairs_checked = 0usize;
        let mut against_stored = 0usize;
        let mut total_edges = 0usize;

        for (i, &(sr, sc)) in waypoints.iter().enumerate() {
            for &(dr, dc) in waypoints.iter().skip(i + 1) {
                for (src_cell, dst_cell) in [((sr, sc), (dr, dc)), ((dr, dc), (sr, sc))] {
                    let src = net.segment_from(src_cell.0, src_cell.1);
                    let dst = net.segment_from(dst_cell.0, dst_cell.1);
                    if src == dst {
                        continue;
                    }
                    let src_rank = seg_to_rank[src as usize];
                    let dst_rank = seg_to_rank[dst as usize];
                    let Some(result) = query.query(src_rank, dst_rank) else {
                        continue;
                    };

                    let distance_m = build_route_points_into(
                        &h.topo,
                        &h.weights,
                        &identity,
                        &net.ebg_nodes,
                        &net.edge_geom,
                        &result.forward_parent,
                        &result.backward_parent,
                        src_rank,
                        &mut rank_path,
                        &mut ebg_path,
                        &mut points,
                    );
                    pairs_checked += 1;

                    let route = format!("[{shape}] {src} -> {dst}");

                    // The reported distance is the path it claims to have
                    // walked: every street in this fixture is exactly
                    // `spacing_m` long.
                    assert_eq!(
                        distance_m,
                        ebg_path.len() as f64 * net.lat.spacing_m,
                        "{route}: reported distance is not the length of the {} \
                         edges it unpacked",
                        ebg_path.len()
                    );

                    // The polyline is that same path, drawn once.
                    let drawn = polyline_length_m(&points, net.lat.origin_lat);
                    // Fixed-point coordinates (1e-7 deg ~ 1 cm) are the
                    // only slack there is; ONE edge drawn against its
                    // traversal would add 200 m.
                    let tol = 0.5f64.max(1e-4 * distance_m);
                    assert!(
                        (drawn - distance_m).abs() <= tol,
                        "{route}: polyline measures {drawn:.1} m but the same \
                         response reports {distance_m:.1} m over {} edges \
                         (tolerance {tol:.2} m) — the drawn route is not the \
                         route that was costed",
                        ebg_path.len()
                    );

                    // A doubled polyline can also hide as a teleport
                    // between two consecutive vertices; no step in this
                    // lattice is ever longer than one street.
                    for (k, w) in points.windows(2).enumerate() {
                        let step = polyline_length_m(w, net.lat.origin_lat);
                        assert!(
                            step <= net.lat.spacing_m * 1.001,
                            "{route}: vertex {k} jumps {step:.1} m, longer than the \
                             {} m streets this network is made of",
                            net.lat.spacing_m
                        );
                    }

                    // It starts where the route starts and ends where it ends.
                    let head_seg = net.segments[*ebg_path.last().unwrap() as usize];
                    let start = net.segment_start(ebg_path[0]);
                    let end = net.coords[head_seg.head as usize];
                    let first = *points.first().expect("a route has geometry");
                    let last = *points.last().expect("a route has geometry");
                    for (which, got, want) in [
                        ("start", (first.lon, first.lat), start),
                        ("end", (last.lon, last.lat), end),
                    ] {
                        let off = polyline_length_m(
                            &[
                                Point {
                                    lon: got.0,
                                    lat: got.1,
                                },
                                Point {
                                    lon: want.0,
                                    lat: want.1,
                                },
                            ],
                            net.lat.origin_lat,
                        );
                        assert!(
                            off <= 0.5,
                            "{route}: polyline {which} is {off:.1} m from the \
                             {which} of the path it was built from"
                        );
                    }

                    // Teeth: count the traversals that run against the
                    // stored polyline orientation. Those are the ones a
                    // forward-only builder draws backwards.
                    for &e in ebg_path.iter() {
                        let seg = net.segments[e as usize];
                        total_edges += 1;
                        if net.nbg_geo.edges[seg.street as usize].u_node != seg.tail {
                            against_stored += 1;
                        }
                    }
                }
            }
        }

        assert!(
            pairs_checked >= 50,
            "[{shape}] only {pairs_checked} pairs routed — the fixture is not \
             exercising the geometry builder"
        );
        assert!(
            against_stored * 4 >= total_edges,
            "[{shape}] only {against_stored} of {total_edges} traversed edges run \
             against their stored polyline orientation — this fixture cannot see a \
             builder that appends every edge forward"
        );
    }
}
