//! Avoid polygon feature: penalize edges inside user-defined polygons.
//!
//! At query time, finds all EBG edges whose midpoints fall inside the given
//! polygons, builds temporary exclude flags, and recustomizes the CCH weights.
//!
//! The R-tree spatial index provides O(log n) bounding-box prefiltering,
//! followed by O(v) ray-casting point-in-polygon for each candidate edge.

use geo::{Contains, Coord, Point, Polygon};

use crate::formats::cch_weights::CchWeights;

use super::exclude::{self, ExcludeWeights};
use super::spatial::SpatialIndex;
use super::state::{ModeData, ServerState};

/// Bit flag for avoid-polygon edges (bit 3, distinct from toll/ferry/motorway bits 0-2).
const AVOID_BIT: u8 = 8;

/// Parsed avoid polygon: a geo::Polygon for containment testing plus its AABB.
#[derive(Debug)]
struct AvoidPolygon {
    poly: Polygon<f64>,
    min_lon: f64,
    min_lat: f64,
    max_lon: f64,
    max_lat: f64,
}

/// Parse avoid_polygons JSON: array of polygon rings.
///
/// Accepted formats:
/// - Single polygon: `[[lon,lat],[lon,lat],...]`
/// - Multiple polygons: `[[[lon,lat],...],[[lon,lat],...]]`
///
/// Each ring must have >= 3 distinct points. Auto-closed if last != first.
fn parse_avoid_polygons(json_str: &str) -> Result<Vec<AvoidPolygon>, String> {
    let val: serde_json::Value =
        serde_json::from_str(json_str).map_err(|e| format!("invalid avoid JSON: {e}"))?;

    let arr = val
        .as_array()
        .ok_or_else(|| "avoid_polygons must be a JSON array".to_string())?;

    if arr.is_empty() {
        return Err("avoid_polygons array is empty".to_string());
    }

    // Detect format: single polygon vs multiple polygons
    // Single polygon: first element is [lon, lat] (array of 2 numbers)
    // Multiple polygons: first element is [[lon, lat], ...] (array of arrays)
    let is_single = arr[0]
        .as_array()
        .is_some_and(|inner| inner.len() == 2 && inner[0].is_number());

    let rings: Vec<&serde_json::Value> = if is_single {
        vec![&val]
    } else {
        arr.iter().collect()
    };

    let mut polygons = Vec::with_capacity(rings.len());
    for (i, ring_val) in rings.iter().enumerate() {
        let ring = ring_val
            .as_array()
            .ok_or_else(|| format!("avoid_polygons[{i}] must be a coordinate array"))?;

        if ring.len() < 3 {
            return Err(format!(
                "avoid_polygons[{i}] must have at least 3 points, got {}",
                ring.len()
            ));
        }

        let mut coords: Vec<Coord<f64>> = ring
            .iter()
            .enumerate()
            .map(|(j, pt)| {
                let arr = pt
                    .as_array()
                    .ok_or_else(|| format!("avoid_polygons[{i}][{j}] must be [lon, lat]"))?;
                if arr.len() != 2 {
                    return Err(format!(
                        "avoid_polygons[{i}][{j}] must be [lon, lat], got {} elements",
                        arr.len()
                    ));
                }
                let lon = arr[0]
                    .as_f64()
                    .ok_or_else(|| format!("avoid_polygons[{i}][{j}][0] must be a number"))?;
                let lat = arr[1]
                    .as_f64()
                    .ok_or_else(|| format!("avoid_polygons[{i}][{j}][1] must be a number"))?;
                Ok(Coord { x: lon, y: lat })
            })
            .collect::<Result<Vec<_>, String>>()?;

        // Auto-close ring if needed
        if coords.first() != coords.last() {
            coords.push(coords[0]);
        }

        // Compute bounding box
        let min_lon = coords.iter().map(|c| c.x).fold(f64::INFINITY, f64::min);
        let max_lon = coords.iter().map(|c| c.x).fold(f64::NEG_INFINITY, f64::max);
        let min_lat = coords.iter().map(|c| c.y).fold(f64::INFINITY, f64::min);
        let max_lat = coords.iter().map(|c| c.y).fold(f64::NEG_INFINITY, f64::max);

        let poly = Polygon::new(coords.into(), vec![]);

        polygons.push(AvoidPolygon {
            poly,
            min_lon,
            min_lat,
            max_lon,
            max_lat,
        });
    }

    Ok(polygons)
}

/// Find all EBG edges whose midpoints fall inside the given avoid polygons.
/// Returns a Vec<u8> indexed by original EBG edge ID, with AVOID_BIT set for avoided edges.
fn find_avoided_edges(
    spatial_index: &SpatialIndex,
    polygons: &[AvoidPolygon],
    n_edges: usize,
) -> Vec<u8> {
    let mut flags = vec![0u8; n_edges];

    for poly in polygons {
        // Query R-tree for edges in polygon bounding box
        for point in
            spatial_index.edges_in_envelope(poly.min_lon, poly.min_lat, poly.max_lon, poly.max_lat)
        {
            let ebg_id = point.ebg_id as usize;
            if ebg_id >= n_edges {
                continue;
            }
            // Already flagged?
            if (flags[ebg_id] & AVOID_BIT) != 0 {
                continue;
            }
            // Point-in-polygon test
            let pt = Point::new(point.coords[0], point.coords[1]);
            if poly.poly.contains(&pt) {
                flags[ebg_id] |= AVOID_BIT;
            }
        }
    }

    flags
}

/// Build a snap mask that excludes edges inside avoid polygons.
/// Combines with optional exclude mask.
pub fn build_avoid_mask(
    base_mask: &[u64],
    avoid_flags: &[u8],
    exclude_flags: Option<(&[u8], u8)>, // (edge_exclude_flags, exclude_mask) if exclude is also active
) -> Vec<u64> {
    base_mask
        .iter()
        .enumerate()
        .map(|(word_idx, &word)| {
            let mut filtered = word;
            for bit in 0..64 {
                let edge_id = word_idx * 64 + bit;
                if edge_id < avoid_flags.len() && (avoid_flags[edge_id] & AVOID_BIT) != 0 {
                    filtered &= !(1u64 << bit);
                }
                // Also clear exclude bits if applicable
                if let Some((exc_flags, exc_mask)) = exclude_flags
                    && edge_id < exc_flags.len()
                    && (exc_flags[edge_id] & exc_mask) != 0
                {
                    filtered &= !(1u64 << bit);
                }
            }
            filtered
        })
        .collect()
}

/// Parse avoid polygons and find avoided edges (shared helper).
///
/// Returns (avoid_flags, polygon_count, avoided_edge_count).
fn prepare_avoid_flags(
    state: &ServerState,
    avoid_json: &str,
    exclude_mask: Option<u8>,
) -> Result<(Vec<u8>, usize, usize), String> {
    let polygons = parse_avoid_polygons(avoid_json)?;

    let n_edges = state.ebg_nodes.n_nodes as usize;
    let mut avoid_flags = find_avoided_edges(&state.spatial_index, &polygons, n_edges);

    let avoided_count = avoid_flags.iter().filter(|&&f| f != 0).count();
    if avoided_count == 0 {
        return Err("no edges found inside avoid polygon(s)".to_string());
    }

    // Merge with exclude flags if both are specified
    if let Some(exc_mask) = exclude_mask {
        for (i, flag) in avoid_flags.iter_mut().enumerate() {
            if i < state.edge_exclude_flags.len() && (state.edge_exclude_flags[i] & exc_mask) != 0 {
                *flag |= AVOID_BIT;
            }
        }
    }

    let poly_count = polygons.len();
    Ok((avoid_flags, poly_count, avoided_count))
}

/// Compute time-only avoid weights for P2P route queries.
///
/// Skips distance recustomization and flat adjacency builds (~2x faster).
/// Returns (time_weights, avoid_flags) for use with `CchQuery::with_custom_weights`.
pub fn compute_avoid_weights_time_only(
    state: &ServerState,
    mode_data: &ModeData,
    avoid_json: &str,
    exclude_mask: Option<u8>,
) -> Result<(CchWeights, Vec<u8>), String> {
    let start = std::time::Instant::now();

    let (avoid_flags, poly_count, avoided_count) =
        prepare_avoid_flags(state, avoid_json, exclude_mask)?;

    let time_weights = exclude::compute_exclude_weights_time_only(
        &mode_data.cch_topo,
        &mode_data.cch_weights,
        &avoid_flags,
        AVOID_BIT,
        &mode_data.filtered_ebg.filtered_to_original,
    );

    tracing::info!(
        polygons = poly_count,
        avoided_edges = avoided_count,
        elapsed_ms = start.elapsed().as_millis(),
        "computed avoid weights (time-only)"
    );

    Ok((time_weights, avoid_flags))
}

/// Compute full avoid weights (time + distance + flat adjacencies).
///
/// For PHAST-based endpoints (isochrones, matrices, trip).
/// If `exclude_mask` is also specified, both avoid and exclude flags are merged.
pub fn compute_avoid_weights(
    state: &ServerState,
    mode_data: &ModeData,
    avoid_json: &str,
    exclude_mask: Option<u8>,
) -> Result<(ExcludeWeights, Vec<u8>), String> {
    let start = std::time::Instant::now();

    let (avoid_flags, poly_count, avoided_count) =
        prepare_avoid_flags(state, avoid_json, exclude_mask)?;

    let weights = exclude::compute_exclude_weights(
        &mode_data.cch_topo,
        &mode_data.cch_weights,
        &mode_data.cch_weights_dist,
        &avoid_flags,
        AVOID_BIT,
        &mode_data.filtered_ebg.filtered_to_original,
    );

    tracing::info!(
        polygons = poly_count,
        avoided_edges = avoided_count,
        elapsed_ms = start.elapsed().as_millis(),
        "computed avoid weights"
    );

    Ok((weights, avoid_flags))
}

/// Parse an optional avoid_polygons parameter.
/// Returns `None` if the parameter is absent or empty.
pub fn parse_avoid_option(avoid: &Option<String>) -> Result<Option<String>, String> {
    match avoid {
        Some(s) if !s.trim().is_empty() => Ok(Some(s.clone())),
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_single_polygon() {
        let json = "[[4.35,50.85],[4.36,50.85],[4.36,50.86],[4.35,50.86]]";
        let polys = parse_avoid_polygons(json).unwrap();
        assert_eq!(polys.len(), 1);
    }

    #[test]
    fn test_parse_multiple_polygons() {
        let json = r#"[
            [[4.35,50.85],[4.36,50.85],[4.36,50.86],[4.35,50.86]],
            [[4.40,50.90],[4.41,50.90],[4.41,50.91],[4.40,50.91]]
        ]"#;
        let polys = parse_avoid_polygons(json).unwrap();
        assert_eq!(polys.len(), 2);
    }

    #[test]
    fn test_parse_auto_close() {
        // Not closed — should auto-close
        let json = "[[4.35,50.85],[4.36,50.85],[4.36,50.86]]";
        let polys = parse_avoid_polygons(json).unwrap();
        assert_eq!(polys.len(), 1);
    }

    #[test]
    fn test_parse_too_few_points() {
        let json = "[[4.35,50.85],[4.36,50.85]]";
        let err = parse_avoid_polygons(json).unwrap_err();
        assert!(err.contains("at least 3"));
    }

    #[test]
    fn test_parse_invalid_json() {
        let err = parse_avoid_polygons("not json").unwrap_err();
        assert!(err.contains("invalid avoid JSON"));
    }

    #[test]
    fn test_parse_empty_array() {
        let err = parse_avoid_polygons("[]").unwrap_err();
        assert!(err.contains("empty"));
    }

    #[test]
    fn test_parse_avoid_option_empty() {
        assert!(parse_avoid_option(&None).unwrap().is_none());
        assert!(parse_avoid_option(&Some(String::new())).unwrap().is_none());
        assert!(
            parse_avoid_option(&Some("  ".to_string()))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn test_parse_avoid_option_valid() {
        let val = parse_avoid_option(&Some(
            "[[4.35,50.85],[4.36,50.85],[4.36,50.86]]".to_string(),
        ))
        .unwrap();
        assert!(val.is_some());
    }
}
