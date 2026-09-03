//! Contour clip masks (2026-09-03, #541).
//!
//! `clip=<name>` intersects the served isochrone with
//! `<data>/clip/<name>.geojson` — a GeoJSON Polygon or MultiPolygon in WGS84
//! (e.g. a national boundary shipped by the deploy tooling under a generic
//! name such as `country`). It is a geometric mask applied to the FINAL
//! contour: reachability is untouched. The result stays ONE simple polygon
//! (product rule): the piece containing the origin, else the largest; holes
//! are dropped. Masks are parsed once per process and cached.

use geo::{Area, BooleanOps, Contains, Coord, LineString, MultiPolygon, Point, Polygon};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use crate::range::contour::ContourPolygon;

pub struct ClipMask {
    pub name: String,
    pub mp: MultiPolygon<f64>,
    /// (min_lon, min_lat, max_lon, max_lat)
    pub bbox: (f64, f64, f64, f64),
}

static MASKS: OnceLock<Mutex<HashMap<String, Arc<ClipMask>>>> = OnceLock::new();

/// Mask names are file stems under `<data>/clip/`: lowercase, digits, `_`, `-`.
pub fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 32
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-')
}

/// Load (or fetch from the cache) `<data_dir>/clip/<name>.geojson`.
pub fn load(data_dir: &str, name: &str) -> Result<Arc<ClipMask>, String> {
    if !valid_name(name) {
        return Err(format!("invalid clip mask name '{name}'"));
    }
    // `data_dir` is the served path: a directory, or the `.butterfly` container
    // FILE — sidecars (edge speeds, srtm, clip masks) then live beside it.
    let base = std::path::Path::new(data_dir);
    let dir = if base.is_file() {
        base.parent().unwrap_or(base)
    } else {
        base
    };
    let path = dir
        .join("clip")
        .join(format!("{name}.geojson"))
        .to_string_lossy()
        .into_owned();
    let masks = MASKS.get_or_init(|| Mutex::new(HashMap::new()));
    if let Some(m) = masks.lock().expect("clip mask cache").get(&path) {
        return Ok(Arc::clone(m));
    }
    let text = std::fs::read_to_string(&path)
        .map_err(|e| format!("clip mask '{name}' not available on this server ({e})"))?;
    let v: serde_json::Value = serde_json::from_str(&text)
        .map_err(|e| format!("clip mask '{name}': invalid JSON: {e}"))?;
    let mp = parse_geometry(&v)
        .ok_or_else(|| format!("clip mask '{name}': expected a GeoJSON Polygon or MultiPolygon"))?;
    let mut bbox = (
        f64::INFINITY,
        f64::INFINITY,
        f64::NEG_INFINITY,
        f64::NEG_INFINITY,
    );
    for p in &mp.0 {
        for c in p.exterior().coords() {
            bbox.0 = bbox.0.min(c.x);
            bbox.1 = bbox.1.min(c.y);
            bbox.2 = bbox.2.max(c.x);
            bbox.3 = bbox.3.max(c.y);
        }
    }
    let m = Arc::new(ClipMask {
        name: name.to_string(),
        mp,
        bbox,
    });
    masks
        .lock()
        .expect("clip mask cache")
        .insert(path, Arc::clone(&m));
    Ok(m)
}

fn parse_geometry(v: &serde_json::Value) -> Option<MultiPolygon<f64>> {
    let g = match v["type"].as_str()? {
        "Feature" => &v["geometry"],
        "FeatureCollection" => &v["features"][0]["geometry"],
        _ => v,
    };
    fn ring(a: &serde_json::Value) -> Option<LineString<f64>> {
        let pts = a
            .as_array()?
            .iter()
            .map(|c| {
                Some(Coord {
                    x: c[0].as_f64()?,
                    y: c[1].as_f64()?,
                })
            })
            .collect::<Option<Vec<_>>>()?;
        (pts.len() >= 4).then(|| LineString::from(pts))
    }
    fn poly(p: &serde_json::Value) -> Option<Polygon<f64>> {
        let rings = p.as_array()?;
        let mut it = rings.iter();
        let ext = ring(it.next()?)?;
        let holes = it.map(ring).collect::<Option<Vec<_>>>()?;
        Some(Polygon::new(ext, holes))
    }
    match g["type"].as_str()? {
        "Polygon" => Some(MultiPolygon(vec![poly(&g["coordinates"])?])),
        "MultiPolygon" => Some(MultiPolygon(
            g["coordinates"]
                .as_array()?
                .iter()
                .map(poly)
                .collect::<Option<Vec<_>>>()?,
        )),
        _ => None,
    }
}

/// Intersect a (lon, lat) outer ring with the mask → one closed CCW ring, or
/// `None` when nothing of the contour lies inside the mask.
pub fn clip_ring(
    mask: &ClipMask,
    outer: &[(f64, f64)],
    origin: Option<(f64, f64)>,
) -> Option<Vec<(f64, f64)>> {
    if outer.len() < 4 {
        return None;
    }
    let poly = Polygon::new(
        LineString::from(
            outer
                .iter()
                .map(|&(x, y)| Coord { x, y })
                .collect::<Vec<_>>(),
        ),
        vec![],
    );
    let inter = poly.intersection(&mask.mp);
    let pick = origin
        .and_then(|(x, y)| inter.0.iter().find(|p| p.contains(&Point::new(x, y))))
        .or_else(|| {
            inter.0.iter().max_by(|a, b| {
                a.unsigned_area()
                    .partial_cmp(&b.unsigned_area())
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
        })?;
    let mut ring: Vec<(f64, f64)> = pick.exterior().coords().map(|c| (c.x, c.y)).collect();
    if ring.len() < 4 {
        return None;
    }
    if ring.first() != ring.last() {
        ring.push(ring[0]);
    }
    let area2: f64 = ring
        .windows(2)
        .map(|w| w[0].0 * w[1].1 - w[1].0 * w[0].1)
        .sum();
    if area2 < 0.0 {
        ring.reverse();
    }
    Some(ring)
}

/// Apply the mask to a served topology (one simple polygon in, one out).
pub fn apply(
    mask: Option<&ClipMask>,
    mut topology: Vec<ContourPolygon>,
    origin: Option<(f64, f64)>,
) -> Vec<ContourPolygon> {
    let Some(mask) = mask else {
        return topology;
    };
    topology.truncate(1);
    match topology.pop() {
        Some(p) => match clip_ring(mask, &p.outer, origin) {
            Some(outer) => vec![ContourPolygon {
                outer,
                holes: Vec::new(),
            }],
            None => Vec::new(),
        },
        None => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn square(x0: f64, y0: f64, x1: f64, y1: f64) -> Vec<(f64, f64)> {
        vec![(x0, y0), (x1, y0), (x1, y1), (x0, y1), (x0, y0)]
    }

    fn mask(mp: MultiPolygon<f64>) -> ClipMask {
        ClipMask {
            name: "t".into(),
            mp,
            bbox: (0.0, 0.0, 0.0, 0.0),
        }
    }

    fn poly(ring: &[(f64, f64)]) -> Polygon<f64> {
        Polygon::new(
            LineString::from(
                ring.iter()
                    .map(|&(x, y)| Coord { x, y })
                    .collect::<Vec<_>>(),
            ),
            vec![],
        )
    }

    #[test]
    fn clip_keeps_the_piece_with_the_origin_and_ccw() {
        // mask = two disjoint squares; contour = a wide rectangle crossing both
        let m = mask(MultiPolygon(vec![
            poly(&square(0.0, 0.0, 1.0, 1.0)),
            poly(&square(2.0, 0.0, 5.0, 1.0)),
        ]));
        let contour = square(-1.0, 0.2, 6.0, 0.8);
        let r = clip_ring(&m, &contour, Some((0.5, 0.5))).unwrap();
        let xs: Vec<f64> = r.iter().map(|p| p.0).collect();
        assert!(
            xs.iter().all(|&x| (0.0..=1.0).contains(&x)),
            "origin piece is the small square: {xs:?}"
        );
        let area2: f64 = r
            .windows(2)
            .map(|w| w[0].0 * w[1].1 - w[1].0 * w[0].1)
            .sum();
        assert!(area2 > 0.0, "CCW");
        // no origin → largest piece
        let r2 = clip_ring(&m, &contour, None).unwrap();
        assert!(r2.iter().any(|p| p.0 > 2.0));
    }

    #[test]
    fn clip_outside_mask_yields_nothing() {
        let m = mask(MultiPolygon(vec![poly(&square(10.0, 10.0, 11.0, 11.0))]));
        assert!(clip_ring(&m, &square(0.0, 0.0, 1.0, 1.0), Some((0.5, 0.5))).is_none());
    }

    #[test]
    fn geojson_polygon_and_feature_parse() {
        let v: serde_json::Value = serde_json::from_str(
            r#"{"type":"Feature","geometry":{"type":"Polygon","coordinates":[[[0,0],[1,0],[1,1],[0,1],[0,0]]]}}"#,
        )
        .unwrap();
        assert_eq!(parse_geometry(&v).unwrap().0.len(), 1);
        assert!(valid_name("country") && !valid_name("../x") && !valid_name("Country"));
    }
}
