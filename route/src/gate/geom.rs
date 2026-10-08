//! Geometry — ONE implementation of each primitive, the same arithmetic as
//! the Python gate (haversine on a 6 371 000 m sphere, planar shoelace,
//! metres-per-degree scaling for ring distances, stdlib-style WKB parsing).

pub type Pt = (f64, f64);

pub fn haversine_m(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    let r = 6_371_000.0;
    let p1 = lat1.to_radians();
    let p2 = lat2.to_radians();
    let a = ((p2 - p1) / 2.0).sin().powi(2)
        + p1.cos() * p2.cos() * ((lon2 - lon1).to_radians() / 2.0).sin().powi(2);
    2.0 * r * a.sqrt().asin()
}

pub fn polyline_len_m(coords: &[Pt]) -> f64 {
    coords
        .windows(2)
        .map(|w| haversine_m(w[0].0, w[0].1, w[1].0, w[1].1))
        .sum::<f64>()
        + 0.0
}

/// Google polyline with 6-decimal precision → (lon, lat) points.
pub fn decode_polyline6(s: &str) -> Vec<Pt> {
    let b = s.as_bytes();
    let mut coords = Vec::new();
    let (mut idx, mut lat, mut lon) = (0usize, 0i64, 0i64);
    while idx < b.len() {
        for which in 0..2 {
            let (mut shift, mut result) = (0u32, 0i64);
            loop {
                if idx >= b.len() {
                    return coords;
                }
                let c = b[idx] as i64 - 63;
                idx += 1;
                result |= (c & 0x1F) << shift;
                shift += 5;
                if c < 0x20 {
                    break;
                }
            }
            let d = if result & 1 == 1 {
                !(result >> 1)
            } else {
                result >> 1
            };
            if which == 0 {
                lat += d;
            } else {
                lon += d;
            }
        }
        coords.push((lon as f64 / 1e6, lat as f64 / 1e6));
    }
    coords
}

/// Even-odd point-in-polygon; closure-tolerant (a repeated closing vertex is
/// never crossed).
pub fn point_in_ring(pt: Pt, ring: &[Pt]) -> bool {
    let (x, y) = pt;
    let mut inside = false;
    if ring.is_empty() {
        return false;
    }
    let mut j = ring.len() - 1;
    for i in 0..ring.len() {
        let (xi, yi) = ring[i];
        let (xj, yj) = ring[j];
        if (yi > y) != (yj > y) && x < (xj - xi) * (y - yi) / (yj - yi) + xi {
            inside = !inside;
        }
        j = i;
    }
    inside
}

/// Signed shoelace (×2): >0 = CCW, <0 = CW.
pub fn ring_area2(ring: &[Pt]) -> f64 {
    let n = ring.len();
    let mut s = 0.0;
    for i in 0..n {
        let (x1, y1) = ring[i];
        let (x2, y2) = ring[(i + 1) % n];
        s += x1 * y2 - x2 * y1;
    }
    s
}

/// Unsigned planar area (degrees²) — only ever compared to another one.
pub fn ring_area(ring: &[Pt]) -> f64 {
    ring_area2(ring).abs() / 2.0
}

/// Metres from `p` to the nearest SIDE of `ring`.
pub fn dist_to_ring_m(p: Pt, ring: &[Pt]) -> f64 {
    let kx = 111_320.0 * p.1.to_radians().cos();
    let ky = 110_540.0;
    let mut best = f64::INFINITY;
    for w in ring.windows(2) {
        let (ax, ay) = ((w[0].0 - p.0) * kx, (w[0].1 - p.1) * ky);
        let (bx, by) = ((w[1].0 - p.0) * kx, (w[1].1 - p.1) * ky);
        let (dx, dy) = (bx - ax, by - ay);
        let l2 = dx * dx + dy * dy;
        let t = if l2 == 0.0 {
            0.0
        } else {
            (-(ax * dx + ay * dy) / l2).clamp(0.0, 1.0)
        };
        let d = (ax + t * dx).hypot(ay + t * dy);
        if d < best {
            best = d;
        }
    }
    best
}

fn rd_u32(buf: &[u8], off: usize, le: bool) -> Option<u32> {
    let b: [u8; 4] = buf.get(off..off + 4)?.try_into().ok()?;
    Some(if le {
        u32::from_le_bytes(b)
    } else {
        u32::from_be_bytes(b)
    })
}

fn rd_f64(buf: &[u8], off: usize, le: bool) -> Option<f64> {
    let b: [u8; 8] = buf.get(off..off + 8)?.try_into().ok()?;
    Some(if le {
        f64::from_le_bytes(b)
    } else {
        f64::from_be_bytes(b)
    })
}

/// Geometry type byte of a WKB blob (3 = Polygon, 6 = MultiPolygon, 2 = LineString).
pub fn wkb_type(buf: &[u8]) -> Option<u32> {
    let le = *buf.first()? == 1;
    Some(rd_u32(buf, 1, le)? & 0xFF)
}

fn rd_poly(buf: &[u8], mut off: usize, le: bool) -> Option<(Vec<Vec<Pt>>, usize)> {
    let nrings = rd_u32(buf, off, le)? as usize;
    off += 4;
    let mut rings = Vec::with_capacity(nrings);
    for _ in 0..nrings {
        let npts = rd_u32(buf, off, le)? as usize;
        off += 4;
        let mut ring = Vec::with_capacity(npts);
        for i in 0..npts {
            ring.push((
                rd_f64(buf, off + 16 * i, le)?,
                rd_f64(buf, off + 16 * i + 8, le)?,
            ));
        }
        off += 16 * npts;
        rings.push(ring);
    }
    Some((rings, off))
}

/// WKB Polygon (3) or MultiPolygon (6) → `[[outer, hole, …], …]` of (lon, lat) rings.
pub fn wkb_polygons(buf: &[u8]) -> Vec<Vec<Vec<Pt>>> {
    let Some(&e) = buf.first() else {
        return vec![];
    };
    let le = e == 1;
    let Some(t) = rd_u32(buf, 1, le).map(|v| v & 0xFF) else {
        return vec![];
    };
    if t == 3 {
        return rd_poly(buf, 5, le)
            .map(|(r, _)| vec![r])
            .unwrap_or_default();
    }
    if t == 6 {
        let Some(n) = rd_u32(buf, 5, le) else {
            return vec![];
        };
        let mut off = 9;
        let mut polys = Vec::new();
        for _ in 0..n {
            let Some(&e2) = buf.get(off) else {
                return polys;
            };
            let le2 = e2 == 1;
            match rd_u32(buf, off + 1, le2).map(|v| v & 0xFF) {
                Some(3) => {}
                _ => return polys,
            }
            match rd_poly(buf, off + 5, le2) {
                Some((rings, next)) => {
                    polys.push(rings);
                    off = next;
                }
                None => return polys,
            }
        }
        return polys;
    }
    vec![]
}

/// Length (metres) of a WKB LineString, or None when the blob is not one.
pub fn wkb_linestring_len_m(buf: &[u8]) -> Option<f64> {
    if buf.len() < 9 {
        return None;
    }
    let le = buf[0] == 1;
    if rd_u32(buf, 1, le)? & 0xFF != 2 {
        return None;
    }
    let npts = rd_u32(buf, 5, le)? as usize;
    let mut off = 9;
    let mut pts = Vec::with_capacity(npts);
    for _ in 0..npts {
        if off + 16 > buf.len() {
            break;
        }
        pts.push((rd_f64(buf, off, le)?, rd_f64(buf, off + 8, le)?));
        off += 16;
    }
    Some(polyline_len_m(&pts))
}

/// In-radius exactly as the engine decides it (`nbg::EARTH_RADIUS_M`).
pub fn within_km(a: Pt, b: Pt, km: f64) -> bool {
    let r = 6_371_008.8;
    let p1 = a.1.to_radians();
    let p2 = b.1.to_radians();
    let h = ((p2 - p1) / 2.0).sin().powi(2)
        + p1.cos() * p2.cos() * ((b.0 - a.0).to_radians() / 2.0).sin().powi(2);
    2.0 * r * h.sqrt().asin() <= km * 1000.0
}

/// Python's `round(x, n)`: the correctly rounded decimal (ties to even on the
/// exact binary value) — what `format!("{:.n}")` produces too.
pub fn round_to(x: f64, n: usize) -> f64 {
    format!("{x:.n$}").parse().unwrap_or(x)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn polyline6_roundtrip_known_vector() {
        // Google's documented example at 1e5 precision scaled to 1e6: a
        // single-point line encodes (38.5, -120.2) → here checked by decode
        // of an encoding produced by the engine's own encoder format.
        let pts = decode_polyline6("_p~iF~ps|U");
        assert_eq!(pts.len(), 1);
        assert!((pts[0].1 - 3.85).abs() < 1e-9 && (pts[0].0 + 12.02).abs() < 1e-9);
    }

    #[test]
    fn ring_primitives() {
        let sq = [(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0)];
        assert!(ring_area2(&sq) > 0.0);
        assert!(point_in_ring((0.5, 0.5), &sq));
        assert!(!point_in_ring((1.5, 0.5), &sq));
        let closed = [(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)];
        assert!(point_in_ring((0.5, 0.5), &closed));
    }

    #[test]
    fn wkb_polygon_parses() {
        let mut b = vec![1u8];
        b.extend_from_slice(&3u32.to_le_bytes());
        b.extend_from_slice(&1u32.to_le_bytes());
        b.extend_from_slice(&4u32.to_le_bytes());
        for (x, y) in [(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 0.0)] {
            b.extend_from_slice(&f64::to_le_bytes(x));
            b.extend_from_slice(&f64::to_le_bytes(y));
        }
        let p = wkb_polygons(&b);
        assert_eq!(p.len(), 1);
        assert_eq!(p[0][0].len(), 4);
        assert_eq!(wkb_type(&b), Some(3));
    }
}
