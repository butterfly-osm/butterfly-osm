//! In-memory access type for flat edge geometry (#155).
//!
//! [`EdgeGeometry`] wraps the two `formats::edge_geom` sections — the CSR
//! offset table and the interleaved point body — and exposes a borrowed
//! [`EdgePolyline`] view per edge id. The view type defers the i32 → f64
//! conversion to per-vertex calls so hot consumers that only need the
//! first / last point pay one divide instead of N.
//!
//! On the serve path the underlying [`ArcCow`] slices borrow directly
//! from the mmap'd container, so `polyline(edge_id)` is a cache-line-
//! aligned `&[i32]` view with zero allocation. The legacy fallback
//! (containers that pre-date #155, or directory-tree boots) flattens
//! the heap `Vec<PolyLine>` into the same layout in memory once at
//! boot and runs through the same accessors.
//!
//! The shape is deliberately the smallest delta from the legacy
//! `polyline.lat_fxp[i] / polyline.lon_fxp[i]` access pattern: every
//! consumer migrates to `let poly = state.edge_geom.polyline(edge_id);`
//! plus per-vertex `poly.at(i)` / `poly.at_e7(i)` reads.

use crate::formats::edge_geom::{EdgeGeomOffsets, EdgeGeomPoints};
use crate::formats::mmap::ArcCow;

/// Flat, mmap-friendly edge geometry. Replaces the heap-resident
/// `Vec<PolyLine>` shape inside `NbgGeo` on the serve path.
pub struct EdgeGeometry {
    /// Cumulative POINT counts per edge. Length = `n_edges + 1`.
    /// `offsets[i]..offsets[i+1]` is the half-open range of vertex
    /// indices for edge `i`.
    offsets: ArcCow<u32>,
    /// Interleaved `(lon_e7, lat_e7)` pairs. Length = `2 * n_points`.
    points: ArcCow<i32>,
}

impl EdgeGeometry {
    /// Build from on-disk sections (mmap-backed or owning, depending on
    /// the [`ArcCow`] shape carried by the parsed structs).
    pub fn from_sections(off: EdgeGeomOffsets, pts: EdgeGeomPoints) -> anyhow::Result<Self> {
        anyhow::ensure!(
            off.n_points == pts.n_points,
            "edge_geom_offsets.n_points ({}) != edge_geom_points.n_points ({})",
            off.n_points,
            pts.n_points
        );
        anyhow::ensure!(
            pts.points.len() == 2 * pts.n_points as usize,
            "edge_geom_points body length {} != 2 * n_points {}",
            pts.points.len(),
            pts.n_points
        );
        anyhow::ensure!(
            off.offsets.len() == off.n_edges as usize + 1,
            "edge_geom_offsets length {} != n_edges + 1 ({})",
            off.offsets.len(),
            off.n_edges as usize + 1
        );
        Ok(Self {
            offsets: off.offsets,
            points: pts.points,
        })
    }

    /// Build from the legacy heap `NbgGeo` shape. Used by the back-compat
    /// fallback when a container pre-dates #155 (or by the directory-
    /// tree boot path which always synthesises in memory).
    ///
    /// This eagerly flattens the nested `Vec<Vec<i32>>` into the flat
    /// arena. The heap cost is the same total bytes as the legacy
    /// `polylines` field; the wins are limited to the dropped per-Vec
    /// header overhead. Re-pack the container to land the full RSS
    /// drop.
    pub fn from_legacy_polylines(geo: &crate::formats::NbgGeo) -> Self {
        let n_edges = geo.polylines.len();
        let mut offsets: Vec<u32> = Vec::with_capacity(n_edges + 1);
        let total_pts: usize = geo.polylines.iter().map(|p| p.lat_fxp.len()).sum();
        let mut points: Vec<i32> = Vec::with_capacity(total_pts.saturating_mul(2));

        let mut cumulative: u32 = 0;
        for poly in &geo.polylines {
            offsets.push(cumulative);
            let n = poly.lat_fxp.len();
            // Defensive — same invariant the on-disk format enforces.
            assert_eq!(
                n,
                poly.lon_fxp.len(),
                "legacy polyline has mismatched lat/lon lengths"
            );
            for i in 0..n {
                points.push(poly.lon_fxp[i]);
                points.push(poly.lat_fxp[i]);
            }
            cumulative = cumulative.saturating_add(n as u32);
        }
        offsets.push(cumulative);

        Self {
            offsets: ArcCow::from_vec(offsets),
            points: ArcCow::from_vec(points),
        }
    }

    /// Number of edges. Equivalent to `nbg_geo.polylines.len()`.
    #[inline]
    pub fn n_edges(&self) -> usize {
        self.offsets.len().saturating_sub(1)
    }

    /// Total number of polyline vertices across all edges.
    #[inline]
    pub fn n_points(&self) -> usize {
        self.points.len() / 2
    }

    /// Borrow the polyline for edge `edge_id`. Returns an empty view if
    /// `edge_id` is out of range — this matches the legacy
    /// `if geom_idx < polylines.len() { ... }` guard pattern at every
    /// call site, so callers get the same default behaviour without
    /// explicit length checks.
    #[inline]
    pub fn polyline(&self, edge_id: u32) -> EdgePolyline<'_> {
        let i = edge_id as usize;
        if i + 1 >= self.offsets.len() {
            return EdgePolyline::EMPTY;
        }
        let start = self.offsets[i] as usize;
        let end = self.offsets[i + 1] as usize;
        // The edge_geom_offsets parse step verifies monotonicity and
        // bounds, so this slice index is well-formed.
        let pts = &self.points[start * 2..end * 2];
        EdgePolyline {
            pts_lon_lat_e7: pts,
        }
    }

    /// Borrow the polyline for an `edge_id` already known to be a
    /// `usize`. Avoids the u32→usize widen at the call site.
    #[inline]
    pub fn polyline_at(&self, edge_id: usize) -> EdgePolyline<'_> {
        if edge_id + 1 >= self.offsets.len() {
            return EdgePolyline::EMPTY;
        }
        let start = self.offsets[edge_id] as usize;
        let end = self.offsets[edge_id + 1] as usize;
        let pts = &self.points[start * 2..end * 2];
        EdgePolyline {
            pts_lon_lat_e7: pts,
        }
    }
}

/// Borrowed view over a single edge's polyline. Cheap to copy — it's just
/// a `&[i32]` slice over the interleaved `(lon_e7, lat_e7)` body.
#[derive(Clone, Copy)]
pub struct EdgePolyline<'a> {
    /// Interleaved `(lon_e7, lat_e7)` pairs. Length is always even and
    /// equals `2 * vertex_count`.
    pts_lon_lat_e7: &'a [i32],
}

impl<'a> EdgePolyline<'a> {
    /// An empty polyline. Returned when `edge_id` is out of range or
    /// the source polyline had zero vertices.
    pub const EMPTY: Self = Self {
        pts_lon_lat_e7: &[],
    };

    /// Number of vertices in this polyline.
    #[inline]
    pub fn len(&self) -> usize {
        self.pts_lon_lat_e7.len() / 2
    }

    /// True iff the polyline has zero vertices.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.pts_lon_lat_e7.is_empty()
    }

    /// `(lon_e7, lat_e7)` at vertex index `i`. Panics on out-of-range.
    #[inline]
    pub fn at_e7(&self, i: usize) -> (i32, i32) {
        let lon = self.pts_lon_lat_e7[i * 2];
        let lat = self.pts_lon_lat_e7[i * 2 + 1];
        (lon, lat)
    }

    /// `(lon, lat)` in degrees at vertex index `i`. Panics on
    /// out-of-range.
    #[inline]
    pub fn at(&self, i: usize) -> (f64, f64) {
        let (lon, lat) = self.at_e7(i);
        (lon as f64 / 1e7, lat as f64 / 1e7)
    }

    /// `(lat, lon)` in i32-e7 fixed point at vertex `i`. Some legacy
    /// consumers (isochrone segments) want lat-first ordering.
    #[inline]
    pub fn at_lat_lon_e7(&self, i: usize) -> (i32, i32) {
        let (lon, lat) = self.at_e7(i);
        (lat, lon)
    }

    /// Lazy iterator over `(lon, lat)` pairs in degrees.
    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = (f64, f64)> + 'a {
        self.pts_lon_lat_e7
            .chunks_exact(2)
            .map(|c| (c[0] as f64 / 1e7, c[1] as f64 / 1e7))
    }

    /// Lazy iterator over `(lon_e7, lat_e7)` in i32 fixed-point.
    #[inline]
    pub fn iter_e7(&self) -> impl Iterator<Item = (i32, i32)> + 'a {
        self.pts_lon_lat_e7.chunks_exact(2).map(|c| (c[0], c[1]))
    }

    /// Lazy iterator over `(lat_e7, lon_e7)` in i32 fixed-point. Used by
    /// the isochrone polygon stamper which wants lat-first.
    #[inline]
    pub fn iter_lat_lon_e7(&self) -> impl Iterator<Item = (i32, i32)> + 'a {
        self.pts_lon_lat_e7.chunks_exact(2).map(|c| (c[1], c[0]))
    }

    /// Owned `Vec<(f64, f64)>` of (lon, lat) pairs. Allocates — for the
    /// rare consumer that genuinely needs an owned vec.
    pub fn to_vec_lon_lat(&self) -> Vec<(f64, f64)> {
        self.iter().collect()
    }
}

impl<'a> std::fmt::Debug for EdgePolyline<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EdgePolyline")
            .field("len", &self.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::formats::edge_geom::{
        EdgeGeomOffsets, EdgeGeomOffsetsFile, EdgeGeomPoints, EdgeGeomPointsFile,
    };

    fn fixture() -> EdgeGeometry {
        // 4 edges with point counts [3, 0, 5, 2] → cumulative [0,3,3,8,10].
        // Use distinguishable lon/lat: lon = 100 + 1000*v_idx, lat = 500 + 1000*v_idx.
        let offsets = vec![0u32, 3, 3, 8, 10];
        let mut points = Vec::new();
        for v in 0..10i32 {
            points.push(100 + v * 1000); // lon_e7
            points.push(500 + v * 1000); // lat_e7
        }
        let off = EdgeGeomOffsets {
            n_edges: 4,
            n_points: 10,
            offsets: ArcCow::from_vec(offsets),
        };
        let pts = EdgeGeomPoints {
            n_points: 10,
            bbox_min_lon: 100,
            bbox_min_lat: 500,
            bbox_max_lon: 9_100,
            bbox_max_lat: 9_500,
            points: ArcCow::from_vec(points),
        };
        EdgeGeometry::from_sections(off, pts).expect("valid fixture")
    }

    #[test]
    fn polyline_returns_correct_range() {
        let g = fixture();
        assert_eq!(g.n_edges(), 4);
        assert_eq!(g.n_points(), 10);
        let p0 = g.polyline(0);
        assert_eq!(p0.len(), 3);
        let p1 = g.polyline(1);
        assert!(p1.is_empty());
        let p2 = g.polyline(2);
        assert_eq!(p2.len(), 5);
        let p3 = g.polyline(3);
        assert_eq!(p3.len(), 2);
    }

    #[test]
    fn polyline_at_lookup() {
        let g = fixture();
        let p2 = g.polyline(2);
        assert_eq!(p2.at_e7(0), (100 + 3 * 1000, 500 + 3 * 1000));
        assert_eq!(p2.at_e7(4), (100 + 7 * 1000, 500 + 7 * 1000));
        let (lon, lat) = p2.at(0);
        assert!((lon - (100 + 3 * 1000) as f64 / 1e7).abs() < 1e-12);
        assert!((lat - (500 + 3 * 1000) as f64 / 1e7).abs() < 1e-12);
    }

    #[test]
    fn polyline_iter_yields_all_vertices() {
        let g = fixture();
        let p2 = g.polyline(2);
        let v: Vec<_> = p2.iter_e7().collect();
        assert_eq!(v.len(), 5);
        assert_eq!(v[0], (100 + 3 * 1000, 500 + 3 * 1000));
        assert_eq!(v[4], (100 + 7 * 1000, 500 + 7 * 1000));
    }

    #[test]
    fn out_of_range_returns_empty() {
        let g = fixture();
        assert!(g.polyline(99).is_empty());
        assert!(g.polyline(u32::MAX).is_empty());
    }

    #[test]
    fn empty_polyline_iterators_are_empty() {
        let g = fixture();
        let p1 = g.polyline(1);
        assert_eq!(p1.iter().count(), 0);
        assert_eq!(p1.iter_e7().count(), 0);
        assert_eq!(p1.iter_lat_lon_e7().count(), 0);
        assert!(p1.to_vec_lon_lat().is_empty());
    }

    #[test]
    fn lat_lon_views_are_swapped() {
        let g = fixture();
        let p3 = g.polyline(3);
        let (lon, lat) = p3.at_e7(0);
        let (lat2, lon2) = p3.at_lat_lon_e7(0);
        assert_eq!(lon, lon2);
        assert_eq!(lat, lat2);
    }

    #[test]
    fn from_legacy_polylines_matches_from_sections() {
        // Build a legacy NbgGeo with the same content as the fixture
        // and confirm `polyline(i)` returns identical bytes.
        use crate::formats::NbgGeo;
        use crate::formats::nbg_geo::{NbgEdge, PolyLine};

        let polylines = vec![
            PolyLine {
                // edge 0: 3 points
                lat_fxp: vec![500, 1500, 2500],
                lon_fxp: vec![100, 1100, 2100],
            },
            PolyLine {
                // edge 1: empty
                lat_fxp: vec![],
                lon_fxp: vec![],
            },
            PolyLine {
                // edge 2: 5 points
                lat_fxp: vec![3500, 4500, 5500, 6500, 7500],
                lon_fxp: vec![3100, 4100, 5100, 6100, 7100],
            },
            PolyLine {
                // edge 3: 2 points
                lat_fxp: vec![8500, 9500],
                lon_fxp: vec![8100, 9100],
            },
        ];
        let edges: Vec<NbgEdge> = (0..4)
            .map(|_| NbgEdge {
                u_node: 0,
                v_node: 0,
                length_mm: 0,
                bearing_deci_deg: 0,
                n_poly_pts: 0,
                poly_off: 0,
                first_osm_way_id: 0,
                flags: 0,
            })
            .collect();
        let geo = NbgGeo {
            n_edges_und: 4,
            edges,
            polylines,
        };
        let from_legacy = EdgeGeometry::from_legacy_polylines(&geo);
        let from_sections = fixture();

        assert_eq!(from_legacy.n_edges(), from_sections.n_edges());
        assert_eq!(from_legacy.n_points(), from_sections.n_points());
        for e in 0..4u32 {
            let a = from_legacy.polyline(e);
            let b = from_sections.polyline(e);
            assert_eq!(a.len(), b.len(), "len mismatch at edge {}", e);
            for i in 0..a.len() {
                assert_eq!(a.at_e7(i), b.at_e7(i), "vertex mismatch e={} i={}", e, i);
            }
        }
    }

    #[test]
    fn round_trip_through_section_bytes() {
        // Build → encode → parse → wrap → query: end-to-end
        // sanity that the format and access type compose.
        let g = fixture();
        // Pull the structs back out via a re-encode using the
        // public format API.
        let off = EdgeGeomOffsets {
            n_edges: g.n_edges() as u32,
            n_points: g.n_points() as u32,
            offsets: ArcCow::from_vec((0..=g.n_edges()).map(|i| g.offsets[i]).collect()),
        };
        let pts = EdgeGeomPoints {
            n_points: g.n_points() as u32,
            bbox_min_lon: 100,
            bbox_min_lat: 500,
            bbox_max_lon: 9_100,
            bbox_max_lat: 9_500,
            points: ArcCow::from_vec(g.points.as_slice().to_vec()),
        };
        let off_bytes = EdgeGeomOffsetsFile::encode(&off);
        let pts_bytes = EdgeGeomPointsFile::encode(&pts);
        let parsed_off = EdgeGeomOffsetsFile::read_from_bytes(&off_bytes).unwrap();
        let parsed_pts = EdgeGeomPointsFile::read_from_bytes(&pts_bytes).unwrap();
        let g2 = EdgeGeometry::from_sections(parsed_off, parsed_pts).unwrap();
        for e in 0..4u32 {
            let a = g.polyline(e);
            let b = g2.polyline(e);
            assert_eq!(a.len(), b.len());
            for i in 0..a.len() {
                assert_eq!(a.at_e7(i), b.at_e7(i));
            }
        }
    }
}
