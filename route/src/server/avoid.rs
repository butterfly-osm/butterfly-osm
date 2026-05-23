//! Avoid polygon feature: penalize edges inside user-defined polygons.
//!
//! At query time, finds all EBG edges whose midpoints fall inside the given
//! polygons, builds temporary exclude flags, and recustomizes the CCH weights.
//!
//! The R-tree spatial index provides O(log n) bounding-box prefiltering,
//! followed by O(v) ray-casting point-in-polygon for each candidate edge.
//!
//! Recustomization is expensive (~30 s on Belgium even for a tiny polygon
//! because the bottom-up rebuilds every shortcut weight). To make repeat
//! queries cheap, we cache the recustomized weights keyed by
//! (mode, polygon_hash, exclude_mask). Cache capacity is bounded so
//! memory stays predictable — each entry is ~100-200 MB on Belgium. See
//! `AvoidWeightCache` below.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use geo::{Contains, Coord, Point, Polygon};
use parking_lot::RwLock;

use super::exclude::{self, ExcludeWeights};
use super::snap_index::PackedSnapIndex;
use super::state::{ModeData, ServerState};

/// Default LRU capacity. Each full entry is ~100-200 MB on Belgium, so 8
/// entries cap memory at ~1.6 GB. Override at boot via the
/// `BUTTERFLY_AVOID_CACHE_CAP` env var.
pub const DEFAULT_AVOID_CACHE_CAP: usize = 8;

/// Cache key for a recustomized weight set. The polygon hash collapses
/// the polygon JSON to 64 bits; clients querying with byte-identical
/// JSON hit the cache. Different polygons hash to different keys, so
/// there is no risk of returning the wrong weights — at worst we miss.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct AvoidKey {
    mode_idx: u8,
    exclude_mask: u8,
    polygon_hash: u64,
}

/// Value cached for an `AvoidKey`. Holds the full weight set (time +
/// distance + flat adjacencies) plus the avoid flags so /route, /table,
/// /isochrone, /trip can all reuse the same recustomization.
pub struct AvoidEntry {
    pub weights: ExcludeWeights,
    pub flags: Vec<u8>,
}

struct AvoidCacheInner {
    map: HashMap<AvoidKey, (Arc<AvoidEntry>, u64)>, // (entry, last-touched generation)
    generation: u64,
    capacity: usize,
    hits: u64,
    misses: u64,
}

/// Bounded LRU keyed by (mode, polygon_hash, exclude_mask). Single
/// `RwLock` for the whole cache — reads are an `Arc::clone` so the
/// lock is released quickly. Writes do an O(capacity) scan for the
/// least-recently-used slot when full.
pub struct AvoidWeightCache {
    inner: RwLock<AvoidCacheInner>,
}

impl AvoidWeightCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: RwLock::new(AvoidCacheInner {
                map: HashMap::with_capacity(capacity.max(1)),
                generation: 0,
                capacity: capacity.max(1),
                hits: 0,
                misses: 0,
            }),
        }
    }

    fn get(&self, key: &AvoidKey) -> Option<Arc<AvoidEntry>> {
        // Fast path: read lock + key presence check.
        let present = self.inner.read().map.contains_key(key);
        if !present {
            return None;
        }
        // Slow path: write lock so we can bump the LRU generation
        // stamp atomically with the read.
        let mut inner = self.inner.write();
        let new_gen = inner.generation.wrapping_add(1);
        if let Some((entry, gen_stamp)) = inner.map.get_mut(key) {
            *gen_stamp = new_gen;
            let entry_clone = Arc::clone(entry);
            inner.generation = new_gen;
            inner.hits += 1;
            return Some(entry_clone);
        }
        None
    }

    fn insert(&self, key: AvoidKey, entry: Arc<AvoidEntry>) {
        let mut inner = self.inner.write();
        inner.misses += 1;
        // Evict LRU if at capacity and the new key isn't already present.
        if !inner.map.contains_key(&key)
            && inner.map.len() >= inner.capacity
            && let Some(victim) = inner
                .map
                .iter()
                .min_by_key(|(_, (_, g))| *g)
                .map(|(k, _)| *k)
        {
            inner.map.remove(&victim);
        }
        inner.generation = inner.generation.wrapping_add(1);
        let gen_stamp = inner.generation;
        inner.map.insert(key, (entry, gen_stamp));
    }

    /// (hits, misses, current size, capacity) — surfaced for the
    /// /health endpoint or operational visibility.
    pub fn stats(&self) -> (u64, u64, usize, usize) {
        let inner = self.inner.read();
        (inner.hits, inner.misses, inner.map.len(), inner.capacity)
    }
}

impl Default for AvoidWeightCache {
    fn default() -> Self {
        let cap = std::env::var("BUTTERFLY_AVOID_CACHE_CAP")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(DEFAULT_AVOID_CACHE_CAP);
        Self::new(cap)
    }
}

/// Hash an avoid_polygons JSON payload after canonicalising it (#243).
///
/// Canonicalisation steps:
///   1. Parse the JSON into rings of `(i64, i64)` vertices, each
///      coordinate quantised to 6 decimals (lon × 1e6, lat × 1e6, then
///      `round() as i64`). 6 decimals ≈ 10 cm precision.
///   2. Strip any duplicate trailing closing vertex (rings are then
///      stored open).
///   3. Rotate each ring to its lexicographically minimal cyclic
///      rotation — picks the start that yields the smallest FULL
///      sequence, not just the smallest first vertex (degenerate
///      when multiple vertices share the min value).
///   4. Sort polygons by the entire canonical ring sequence so
///      multi-polygon orderings collapse and ties resolve
///      deterministically.
///   5. Hash the resulting canonical byte stream.
///
/// Falls back to a raw-bytes hash if parsing fails — the cache will
/// then miss as before, but the route handler's error path still
/// surfaces the parse error.
fn hash_polygon_json(s: &str) -> u64 {
    let mut h = rustc_hash::FxHasher::default();
    match canonicalize_polygons(s) {
        Some(canon) => canon.hash(&mut h),
        None => s.as_bytes().hash(&mut h),
    }
    h.finish()
}

/// Parse + canonicalise a polygon payload into a sortable byte vec.
///
/// Returns `None` if the JSON shape is invalid; callers fall back to a
/// raw-bytes hash so a malformed polygon still has a deterministic
/// cache key (it will lose every time at the parse step downstream).
fn canonicalize_polygons(s: &str) -> Option<Vec<u8>> {
    let val: serde_json::Value = serde_json::from_str(s).ok()?;
    let arr = val.as_array()?;
    if arr.is_empty() {
        return None;
    }
    let is_single = arr[0]
        .as_array()
        .is_some_and(|inner| inner.len() == 2 && inner[0].is_number());
    let rings_json: Vec<&serde_json::Value> = if is_single {
        vec![&val]
    } else {
        arr.iter().collect()
    };

    let mut rings: Vec<Vec<(i64, i64)>> = Vec::with_capacity(rings_json.len());
    for ring_val in &rings_json {
        let ring = ring_val.as_array()?;
        let mut pts: Vec<(i64, i64)> = Vec::with_capacity(ring.len());
        for pt in ring {
            let coord = pt.as_array()?;
            if coord.len() != 2 {
                return None;
            }
            let lon = coord[0].as_f64()?;
            let lat = coord[1].as_f64()?;
            // 6 decimals ≈ 10 cm precision. Scale to integer for stable
            // hashing without f64 representation quirks.
            let lon_q = (lon * 1_000_000.0).round() as i64;
            let lat_q = (lat * 1_000_000.0).round() as i64;
            pts.push((lon_q, lat_q));
        }
        if pts.len() < 3 {
            return None;
        }
        // Drop duplicate closing vertex if present.
        if pts.first() == pts.last() {
            pts.pop();
        }
        // Rotate ring to its lexicographically minimal cyclic rotation.
        // Naive `min_by_key` returns the FIRST index of the smallest
        // vertex, which breaks rotation-independence when the same
        // vertex value appears multiple times in the ring. Instead we
        // pick the rotation start that yields the lexicographically
        // smallest full sequence. For typical polygons (≤ ~100 verts)
        // the O(n²) candidate enumeration is trivial; for pathological
        // rings the upgrade to Booth's algorithm is mechanical.
        if !pts.is_empty() {
            let n = pts.len();
            let mut best = 0usize;
            for cand in 1..n {
                for k in 0..n {
                    let a = pts[(best + k) % n];
                    let b = pts[(cand + k) % n];
                    if b < a {
                        best = cand;
                        break;
                    } else if b > a {
                        break;
                    }
                }
            }
            pts.rotate_left(best);
        }
        rings.push(pts);
    }

    // Multi-polygon order: sort by the FULL canonical ring sequence
    // (not just first vertex). Two rings whose canonical first vertex
    // collides need a stable tie-break to keep multi-polygon hashing
    // deterministic.
    rings.sort_unstable();

    // Serialise to a deterministic byte stream.
    let mut out = Vec::with_capacity(rings.iter().map(|r| r.len() * 16).sum::<usize>() + 8);
    out.extend_from_slice(&(rings.len() as u32).to_le_bytes());
    for ring in &rings {
        out.extend_from_slice(&(ring.len() as u32).to_le_bytes());
        for (lon, lat) in ring {
            out.extend_from_slice(&lon.to_le_bytes());
            out.extend_from_slice(&lat.to_le_bytes());
        }
    }
    Some(out)
}

#[cfg(test)]
mod canon_tests {
    use super::canonicalize_polygons;

    #[test]
    fn whitespace_independent() {
        let a = "[[4.32,50.92],[4.50,50.92],[4.50,51.15],[4.32,51.15]]";
        let b = "[[4.32, 50.92], [4.50, 50.92], [4.50, 51.15], [4.32, 51.15]]";
        assert_eq!(canonicalize_polygons(a), canonicalize_polygons(b));
    }

    #[test]
    fn precision_independent_at_6dp() {
        let a = "[[4.32,50.92],[4.50,50.92],[4.50,51.15],[4.32,51.15]]";
        let b =
            "[[4.320000,50.920000],[4.500000,50.920000],[4.500000,51.150000],[4.320000,51.150000]]";
        assert_eq!(canonicalize_polygons(a), canonicalize_polygons(b));
    }

    #[test]
    fn closing_vertex_independent() {
        let a = "[[4.32,50.92],[4.50,50.92],[4.50,51.15],[4.32,51.15]]";
        let b = "[[4.32,50.92],[4.50,50.92],[4.50,51.15],[4.32,51.15],[4.32,50.92]]";
        assert_eq!(canonicalize_polygons(a), canonicalize_polygons(b));
    }

    #[test]
    fn ring_rotation_independent() {
        let a = "[[4.32,50.92],[4.50,50.92],[4.50,51.15],[4.32,51.15]]";
        // Same ring, rotated start.
        let b = "[[4.50,51.15],[4.32,51.15],[4.32,50.92],[4.50,50.92]]";
        assert_eq!(canonicalize_polygons(a), canonicalize_polygons(b));
    }

    #[test]
    fn different_polygons_differ() {
        let a = "[[4.32,50.92],[4.50,50.92],[4.50,51.15],[4.32,51.15]]";
        let b = "[[4.32,50.92],[4.50,50.92],[4.50,51.20],[4.32,51.20]]";
        assert_ne!(canonicalize_polygons(a), canonicalize_polygons(b));
    }

    #[test]
    fn multi_polygon_order_independent() {
        let p1 = "[[4.32,50.92],[4.50,50.92],[4.50,51.15],[4.32,51.15]]";
        let p2 = "[[5.10,50.50],[5.30,50.50],[5.30,50.70],[5.10,50.70]]";
        let a = format!("[{},{}]", p1, p2);
        let b = format!("[{},{}]", p2, p1);
        assert_eq!(canonicalize_polygons(&a), canonicalize_polygons(&b));
    }

    #[test]
    fn duplicate_min_vertex_rotation_independent() {
        let a = "[[1.0,1.0],[2.0,3.0],[1.0,1.0],[4.0,5.0]]";
        let b = "[[1.0,1.0],[4.0,5.0],[1.0,1.0],[2.0,3.0]]";
        assert_eq!(canonicalize_polygons(a), canonicalize_polygons(b));
    }
}

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
    snap_index: &PackedSnapIndex,
    polygons: &[AvoidPolygon],
    n_edges: usize,
) -> Vec<u8> {
    let mut flags = vec![0u8; n_edges];

    for poly in polygons {
        // Query packed grid for samples in the polygon bounding box.
        let samples =
            snap_index.samples_in_envelope(poly.min_lon, poly.min_lat, poly.max_lon, poly.max_lat);
        for s in samples {
            let ebg_id = s.ebg_id as usize;
            if ebg_id >= n_edges {
                continue;
            }
            // Already flagged?
            if (flags[ebg_id] & AVOID_BIT) != 0 {
                continue;
            }
            // Point-in-polygon test
            let pt = Point::new(s.lon, s.lat);
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
    let mut avoid_flags = find_avoided_edges(&state.snap_index, &polygons, n_edges);

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

/// Compute (or read from cache) the FULL avoid-weight set for a
/// `(mode, polygon_hash, exclude_mask)` key. The full set is
/// shareable between /route, /table, /isochrone, /trip — first caller
/// pays the ~30 s recustomization cost; subsequent callers on the same
/// key return in ~µs.
///
/// Concurrent identical misses both compute and the second insertion
/// silently overwrites — we accept the duplicate work in exchange for
/// dead-simple lock semantics (no per-key Mutex / OnceCell).
fn get_or_compute_avoid_entry(
    state: &ServerState,
    mode_data: &ModeData,
    mode_idx: u8,
    avoid_json: &str,
    exclude_mask: Option<u8>,
) -> Result<Arc<AvoidEntry>, String> {
    let polygon_hash = hash_polygon_json(avoid_json);
    let key = AvoidKey {
        mode_idx,
        exclude_mask: exclude_mask.unwrap_or(0),
        polygon_hash,
    };

    if let Some(entry) = state.avoid_cache.get(&key) {
        tracing::debug!(
            mode_idx,
            exclude_mask = key.exclude_mask,
            polygon_hash,
            "avoid weights cache HIT"
        );
        return Ok(entry);
    }

    let start = std::time::Instant::now();
    let (avoid_flags, poly_count, avoided_count) =
        prepare_avoid_flags(state, avoid_json, exclude_mask)?;
    let weights = exclude::compute_exclude_weights(
        &mode_data.cch_topo,
        &mode_data.cch_weights,
        &mode_data.cch_weights_dist,
        &avoid_flags,
        AVOID_BIT,
        &mode_data.filtered_to_original,
    );
    tracing::info!(
        mode_idx,
        polygons = poly_count,
        avoided_edges = avoided_count,
        elapsed_ms = start.elapsed().as_millis(),
        "computed avoid weights (cache MISS, stored)"
    );

    let entry = Arc::new(AvoidEntry {
        weights,
        flags: avoid_flags,
    });
    state.avoid_cache.insert(key, Arc::clone(&entry));
    Ok(entry)
}

/// Compute (or cache-fetch) the avoid weight set for /route, /table,
/// /isochrone, /trip, /matching. Returns the full `Arc<AvoidEntry>`
/// directly — callers borrow fields they need via deref. This avoids
/// the ~100-400 MB deep clone on cache hits that owned-return forced.
///
/// Both time-only (P2P /route) and full (PHAST batch) consumers go
/// through this single entry point. Time-only callers read
/// `entry.weights.time_weights`; full callers read `entry.weights`.
pub fn compute_avoid_weights(
    state: &ServerState,
    mode_data: &ModeData,
    avoid_json: &str,
    exclude_mask: Option<u8>,
) -> Result<Arc<AvoidEntry>, String> {
    let mode_idx = mode_index_in_state(state, mode_data)? as u8;
    get_or_compute_avoid_entry(state, mode_data, mode_idx, avoid_json, exclude_mask)
}

/// Compatibility shim: /route only needs the time field but reuses
/// the cache via the unified entry. Same `Arc<AvoidEntry>` shape.
pub fn compute_avoid_weights_time_only(
    state: &ServerState,
    mode_data: &ModeData,
    avoid_json: &str,
    exclude_mask: Option<u8>,
) -> Result<Arc<AvoidEntry>, String> {
    compute_avoid_weights(state, mode_data, avoid_json, exclude_mask)
}

/// Look up the mode index by comparing the `ModeData` pointer against
/// the state's mode list. Avoids threading an explicit index through
/// the existing call sites.
fn mode_index_in_state(state: &ServerState, mode_data: &ModeData) -> Result<usize, String> {
    for (i, m) in state.modes.iter().enumerate() {
        if std::ptr::eq(m, mode_data) {
            return Ok(i);
        }
    }
    Err("internal error: ModeData not registered in ServerState".to_string())
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
