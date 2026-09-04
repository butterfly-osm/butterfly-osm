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
use crate::profile_abi::Mode;

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

/// Booth's algorithm — O(n) lexicographically minimal cyclic rotation.
///
/// Returns the starting index `k` such that `s.rotate_left(k)` yields
/// the lex-smallest rotation of `s`. Reference:
///   K. S. Booth, "Lexicographically least circular substrings", 1980.
///
/// Operates on the doubled sequence implicitly via `failure[]` of size 2n.
/// Used by `canonicalize_polygons` so the avoid-cache hit rate isn't
/// gated on polygon vertex count (#243 follow-up).
fn booth_minimal_rotation<T: Ord + Copy>(s: &[T]) -> usize {
    let n = s.len();
    if n <= 1 {
        return 0;
    }
    // failure[i] = -1 sentinel encoded as i+1 = 0 means "unset".
    // Storing failure as `Vec<isize>` with -1 sentinel keeps the code
    // readable; size is 2n which is fine for polygon vertex counts.
    let mut failure: Vec<isize> = vec![-1; 2 * n];
    let mut k: usize = 0;
    for j in 1..2 * n {
        let mut i = failure[j - k - 1];
        while i != -1 && s[(j) % n] != s[(k as isize + i + 1) as usize % n] {
            if s[(j) % n] < s[(k as isize + i + 1) as usize % n] {
                k = (j as isize - i - 1) as usize;
            }
            i = failure[i as usize];
        }
        if i == -1 && s[(j) % n] != s[(k as isize + i + 1) as usize % n] {
            if s[(j) % n] < s[(k as isize + i + 1) as usize % n] {
                k = j;
            }
            failure[j - k] = -1;
        } else {
            failure[j - k] = i + 1;
        }
    }
    k % n
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
        // Rotate ring to its lexicographically minimal cyclic rotation
        // via Booth's algorithm — O(n) time and O(n) space. The naive
        // "smallest vertex first" approach breaks rotation-independence
        // when the same vertex value appears multiple times. A full
        // O(n²) sequence-compare also works but degrades on large
        // polygons; Booth's stays linear regardless of polygon size.
        if !pts.is_empty() {
            let start = booth_minimal_rotation(&pts);
            pts.rotate_left(start);
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
    use super::{booth_minimal_rotation, canonicalize_polygons};

    /// O(n²) reference: try every rotation, pick the lex-smallest.
    /// Used to cross-check Booth's algorithm on tricky inputs.
    fn naive_minimal_rotation<T: Ord + Copy>(s: &[T]) -> usize {
        let n = s.len();
        if n <= 1 {
            return 0;
        }
        let mut best = 0usize;
        for cand in 1..n {
            for k in 0..n {
                let a = s[(best + k) % n];
                let b = s[(cand + k) % n];
                if b < a {
                    best = cand;
                    break;
                } else if b > a {
                    break;
                }
            }
        }
        best
    }

    fn booth_matches_naive<T: Ord + Copy + std::fmt::Debug>(s: &[T]) {
        let b = booth_minimal_rotation(s);
        let n = naive_minimal_rotation(s);
        // Both rotations must produce the same full sequence — different
        // starting indices are allowed when the input has rotational
        // symmetry (e.g. all-equal vertices).
        let n_len = s.len();
        let booth_seq: Vec<T> = (0..n_len).map(|k| s[(b + k) % n_len]).collect();
        let naive_seq: Vec<T> = (0..n_len).map(|k| s[(n + k) % n_len]).collect();
        assert_eq!(booth_seq, naive_seq, "Booth mismatch on {:?}", s);
    }

    #[test]
    fn booth_basic() {
        booth_matches_naive::<u32>(&[]);
        booth_matches_naive(&[5u32]);
        booth_matches_naive(&[3u32, 1, 2]);
        booth_matches_naive(&[1u32, 2, 3, 4]);
        booth_matches_naive(&[4u32, 3, 2, 1]);
    }

    #[test]
    fn booth_repeated_patterns() {
        booth_matches_naive(&[1u32, 2, 1, 2, 1, 2]);
        booth_matches_naive(&[1u32, 1, 1, 1]);
        booth_matches_naive(&[2u32, 1, 2, 1]);
    }

    #[test]
    fn booth_multiple_identical_minima() {
        booth_matches_naive(&[1u32, 5, 1, 3]);
        booth_matches_naive(&[1u32, 5, 1, 3, 1, 2]);
        booth_matches_naive(&[1u32, 9, 1, 9, 1, 9]);
    }

    #[test]
    fn booth_random_smoke() {
        // Hand-picked tricky sequences.
        let cases: &[&[u32]] = &[
            &[7, 3, 9, 3, 7, 1, 2, 1],
            &[5, 5, 4, 5, 5, 4, 5],
            &[10, 1, 10, 2, 10, 1, 10, 2, 10, 1],
            &[1, 2, 1, 3, 1, 2, 1, 4],
            &[9, 8, 7, 6, 5, 4, 3, 2, 1, 0],
            &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
        ];
        for s in cases {
            booth_matches_naive(s);
        }
    }

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
///
/// Private since #566: the handlers no longer build masks by hand, they
/// call [`resolve_weights`].
fn build_avoid_mask(
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
/// Reached only via [`resolve_weights`] since #566.
fn compute_avoid_weights(
    state: &ServerState,
    mode_data: &ModeData,
    avoid_json: &str,
    exclude_mask: Option<u8>,
) -> Result<Arc<AvoidEntry>, String> {
    let mode_idx = mode_index_in_state(state, mode_data)? as u8;
    get_or_compute_avoid_entry(state, mode_data, mode_idx, avoid_json, exclude_mask)
}

/// Look up the mode index by comparing the `ModeData` pointer against
/// the state's mode list. Avoids threading an explicit index through
/// the existing call sites.
///
/// #402: `state.modes` is now `Vec<ModeSlot>`, where each slot wraps the
/// `Arc<ModeData>` behind a `RwLock`. We peek the read lock and compare
/// the `Arc`'s inner pointer to identify the slot owning `mode_data`.
fn mode_index_in_state(state: &ServerState, mode_data: &ModeData) -> Result<usize, String> {
    for (i, m) in state.modes.iter().enumerate() {
        let r = m.state.read();
        if let Some(arc) = r.as_ref()
            && std::ptr::eq(std::sync::Arc::as_ptr(arc), mode_data as *const ModeData)
        {
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

// ===========================================================================
// #566 / #561 — ONE resolution of `exclude` + `avoid_polygons` per query.
// ===========================================================================

/// The resolved `exclude=` / `avoid_polygons=` plan for ONE query (#566).
///
/// Every query surface (/route, /table, /trip, /isochrone,
/// /isochrone/bulk, /match) needs the same four things, derived by the
/// same priority rule — **avoid wins over exclude**, because the avoid
/// recustomization already folds the exclude flags into its own weights:
///
/// * `snap_mask` — the edge bitset snapping is allowed to pick from;
/// * the avoid recustomization, when an avoid polygon is active;
/// * the exclude recustomization, when `exclude=` is the ONLY option;
/// * `exclude_mask` — the parsed bits, still needed by callers that pick
///   a legacy (non-phantom) seeding flow when custom weights are in play.
///
/// Two properties this type exists to keep true everywhere at once:
///
/// 1. **`snap_mask` is `Cow::Borrowed(&mode_data.mask)` when neither
///    option is present** — the 99 % path. `/table` and `/isochrone/bulk`
///    used to clone the whole bitset there (#561: one bit per EBG node,
///    `ceil(n / 64)` words — 80 639 words = 630 KiB for Belgium's
///    5 160 848, per request, on the two highest-throughput surfaces)
///    while `/route` borrowed it. One resolution, one answer.
/// 2. **The exclude weights resolve on FIRST USE, not at plan time.**
///    `ServerState::get_exclude_weights` is a multi-second
///    recustomization on a cold cache, and `/route` reaches several early
///    returns (degenerate same-edge snap, snap failure, GPX shortcut)
///    before it ever needs them — it therefore computed them late, by
///    hand, ~400 lines below the mask. The `OnceLock` keeps `/route`
///    exactly as lazy as it was and makes the other five lazy too.
pub struct WeightPlan<'a> {
    /// Edge bitset for snapping. Borrowed unless an option filters it.
    pub snap_mask: std::borrow::Cow<'a, [u64]>,
    /// Parsed `exclude=` bits (`None` when absent or empty).
    pub exclude_mask: Option<u8>,
    avoid_entry: Option<Arc<AvoidEntry>>,
    /// `Some` only when exclude is active AND avoid is not — i.e. exactly
    /// when the exclude weight set is the one the query must run on.
    exclude_weights_mask: Option<u8>,
    exclude_weights: std::sync::OnceLock<Option<Arc<ExcludeWeights>>>,
    state: &'a ServerState,
    mode: Mode,
}

impl<'a> WeightPlan<'a> {
    /// True when neither option is active: base weights, base mask, and
    /// the phantom-seeded fast paths are allowed to run (their partial
    /// edge costs assume base weights).
    pub fn is_base(&self) -> bool {
        self.avoid_entry.is_none() && self.exclude_mask.is_none()
    }

    /// True when an avoid polygon produced a recustomized weight set.
    pub fn has_avoid(&self) -> bool {
        self.avoid_entry.is_some()
    }

    /// The custom weight set this query must run on, or `None` for the
    /// mode's base weights. THE priority rule (avoid > exclude), in one
    /// place. Resolving the exclude branch may recustomize on a cold
    /// cache, so call it only where the weights are actually needed.
    pub fn weights(&self) -> Option<&ExcludeWeights> {
        if let Some(ref entry) = self.avoid_entry {
            return Some(&entry.weights);
        }
        self.exclude_weights
            .get_or_init(|| {
                self.exclude_weights_mask
                    .map(|exc| self.state.get_exclude_weights(self.mode, exc))
            })
            .as_deref()
    }

    /// Time metric of [`WeightPlan::weights`].
    pub fn time_weights(&self) -> Option<&crate::formats::CchWeights> {
        self.weights().map(|w| &w.time_weights)
    }
}

/// Pick the snap mask for one query — THE #561 decision, in one place.
///
/// `Cow::Borrowed(base_mask)` unless an option actually filters it, so
/// the 99 % path (no `avoid_polygons`, no `exclude`) copies nothing.
/// Owned otherwise: the avoid build already folds the exclude flags in,
/// which is why the two arms never compose.
fn select_snap_mask<'a>(
    base_mask: &'a [u64],
    edge_exclude_flags: &[u8],
    avoid_flags: Option<&[u8]>,
    exclude_mask: Option<u8>,
) -> std::borrow::Cow<'a, [u64]> {
    match (avoid_flags, exclude_mask) {
        (Some(flags), exc) => std::borrow::Cow::Owned(build_avoid_mask(
            base_mask,
            flags,
            exc.map(|e| (edge_exclude_flags, e)),
        )),
        (None, Some(exc)) => std::borrow::Cow::Owned(exclude::build_exclude_mask(
            base_mask,
            edge_exclude_flags,
            exc,
        )),
        (None, None) => std::borrow::Cow::Borrowed(base_mask),
    }
}

/// Plan for a query with no (usable) avoid polygon. Infallible: the
/// exclude bits are already parsed and the mask build cannot fail.
fn plan_without_avoid<'a>(
    state: &'a ServerState,
    mode_data: &'a ModeData,
    mode: Mode,
    exclude_mask: Option<u8>,
) -> WeightPlan<'a> {
    WeightPlan {
        snap_mask: select_snap_mask(
            &mode_data.mask,
            &state.edge_exclude_flags,
            None,
            exclude_mask,
        ),
        exclude_mask,
        avoid_entry: None,
        exclude_weights_mask: exclude_mask,
        exclude_weights: std::sync::OnceLock::new(),
        state,
        mode,
    }
}

/// Plan for a query whose avoid polygon must be honoured. The avoid
/// recustomization already folds `exclude_mask` in, so the exclude
/// weight set is never consulted on this branch.
fn plan_with_avoid<'a>(
    state: &'a ServerState,
    mode_data: &'a ModeData,
    mode: Mode,
    exclude_mask: Option<u8>,
    avoid_json: &str,
) -> Result<WeightPlan<'a>, String> {
    let entry = compute_avoid_weights(state, mode_data, avoid_json, exclude_mask)?;
    let snap_mask = select_snap_mask(
        &mode_data.mask,
        &state.edge_exclude_flags,
        Some(&entry.flags),
        exclude_mask,
    );
    Ok(WeightPlan {
        snap_mask,
        exclude_mask,
        avoid_entry: Some(entry),
        exclude_weights_mask: None,
        exclude_weights: std::sync::OnceLock::new(),
        state,
        mode,
    })
}

/// Resolve the weight plan for one query (#566).
///
/// Inputs are the ALREADY-PARSED options: every surface renders a parse
/// error in its own response shape (`{"error": …}` on /route, /table,
/// /isochrone; `{"code","message"}` on /trip, /match) and /route must
/// reject a bad `exclude=` token before it validates `uncertainty=`, so
/// the two `parse_*_option` calls stay at the call sites where their
/// 400s belong. Everything after them — the avoid recustomization, the
/// snap mask, the avoid-over-exclude priority — is here, once.
///
/// `mode_data` must be `state.get_mode(mode)`: the avoid cache keys on
/// the mode slot that owns it.
pub fn resolve_weights<'a>(
    state: &'a ServerState,
    mode_data: &'a ModeData,
    mode: Mode,
    exclude_mask: Option<u8>,
    avoid_json: Option<&str>,
) -> Result<WeightPlan<'a>, String> {
    match avoid_json {
        Some(json) => plan_with_avoid(state, mode_data, mode, exclude_mask, json),
        None => Ok(plan_without_avoid(state, mode_data, mode, exclude_mask)),
    }
}

/// Same plan, but an unusable avoid polygon is DROPPED instead of
/// failing the request.
///
/// This is not a nicety: `/trip` and `/match` have always resolved their
/// avoid polygon inside `spawn_blocking` with a bare
/// `compute_avoid_weights(..).ok()`, so a polygon that covers no edge
/// (or fails to parse past the emptiness check) silently degrades to a
/// plain query there, while `/route`, `/table` and `/isochrone` answer
/// 400. Consolidation preserves that difference rather than trading it
/// away; see #566 for the divergence.
pub fn resolve_weights_lenient_avoid<'a>(
    state: &'a ServerState,
    mode_data: &'a ModeData,
    mode: Mode,
    exclude_mask: Option<u8>,
    avoid_json: Option<&str>,
) -> WeightPlan<'a> {
    if let Some(json) = avoid_json
        && let Ok(plan) = plan_with_avoid(state, mode_data, mode, exclude_mask, json)
    {
        return plan;
    }
    plan_without_avoid(state, mode_data, mode, exclude_mask)
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

/// #566 / #561 — the six query surfaces resolve through ONE plan.
///
/// Two properties are locked here because both have already drifted
/// once: the snap mask must be BORROWED on the no-option path (#561),
/// and the six surfaces must derive the same plan from the same
/// parameters (#566).
///
/// The first test runs anywhere; the other three need the Belgium
/// artifact (#587) and are deliberately the expensive kind — between
/// them they force one `exclude=` and two `avoid_polygons=`
/// recustomizations, i.e. ~5 min for the module in a debug build. That
/// IS the branch under test: a plan that resolves the wrong weight set
/// is only visible once the weight set exists.
#[cfg(test)]
mod weight_plan_tests {
    use super::*;
    use crate::server::exclude::EXCLUDE_TOLL;
    use std::borrow::Cow;

    /// One skip line for this whole module (#587).
    const SCOPE: &str = "avoid::weight_plan_tests";

    /// A 0.02° box (~1.4 x 2.2 km) over central Brussels. Public
    /// geography, and dense enough that `prepare_avoid_flags` finds
    /// edges inside it.
    const USABLE_AVOID: &str = "[[4.34,50.84],[4.36,50.84],[4.36,50.86],[4.34,50.86]]";

    /// A box in the North Sea, ~40 km off Ostend: syntactically valid,
    /// contains no road. This is the input on which the six surfaces
    /// legitimately DISAGREE — see [`only_trip_and_match_ignore_an_unusable_avoid_polygon`].
    const UNUSABLE_AVOID: &str = "[[2.40,51.60],[2.50,51.60],[2.50,51.70],[2.40,51.70]]";

    /// How a surface answers an avoid polygon it cannot honour.
    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    enum AvoidPolicy {
        /// 400 with the resolver's message: /route, /table, /isochrone,
        /// /isochrone/bulk.
        Reject,
        /// Drop the polygon and answer a plain query: /trip, /match.
        Ignore,
    }

    /// THE six call sites, as rows. `/isochrone` appears twice because
    /// `isochrone_handler.rs` resolves twice — once for the single
    /// query, once inside `isochrone_bulk_sync`.
    const SURFACES: &[(&str, AvoidPolicy)] = &[
        ("/route", AvoidPolicy::Reject),
        ("/table", AvoidPolicy::Reject),
        ("/isochrone", AvoidPolicy::Reject),
        ("/isochrone/bulk", AvoidPolicy::Reject),
        ("/trip", AvoidPolicy::Ignore),
        ("/match", AvoidPolicy::Ignore),
    ];

    /// The resolution one surface performs, selected by its policy —
    /// the only thing that differs between the six call sites.
    fn resolve_as<'a>(
        policy: AvoidPolicy,
        state: &'a ServerState,
        mode_data: &'a ModeData,
        mode: Mode,
        exclude_mask: Option<u8>,
        avoid_json: Option<&str>,
    ) -> Result<WeightPlan<'a>, String> {
        match policy {
            AvoidPolicy::Reject => {
                resolve_weights(state, mode_data, mode, exclude_mask, avoid_json)
            }
            AvoidPolicy::Ignore => Ok(resolve_weights_lenient_avoid(
                state,
                mode_data,
                mode,
                exclude_mask,
                avoid_json,
            )),
        }
    }

    /// #561, on the pure decision itself: `Cow::Borrowed` EXACTLY when
    /// neither option filters the mask, `Cow::Owned` with the right bits
    /// cleared otherwise. Runs without the Belgium artifact.
    #[test]
    fn snap_mask_is_borrowed_only_when_no_option_filters_it() {
        // One 64-edge word, edges 0..=3 usable. Edge 0 is tolled;
        // edge 1 falls inside the avoid polygon.
        let base: Vec<u64> = vec![0b1111];
        let mut exclude_flags = vec![0u8; 64];
        exclude_flags[0] = EXCLUDE_TOLL;
        let mut avoid_flags = vec![0u8; 64];
        avoid_flags[1] = AVOID_BIT;

        let neither = select_snap_mask(&base, &exclude_flags, None, None);
        assert!(
            matches!(neither, Cow::Borrowed(_)),
            "#561: the no-option path must not copy the edge bitset"
        );
        assert!(
            std::ptr::eq(neither.as_ref().as_ptr(), base.as_ptr()),
            "#561: the borrow must be the mode's own mask, not a fresh buffer"
        );
        assert_eq!(neither.as_ref(), base.as_slice());

        // Every combination that DOES filter, and the bits that survive it.
        struct Filtered<'a> {
            avoid: Option<&'a [u8]>,
            exclude: Option<u8>,
            survivors: u64,
            why: &'a str,
        }
        let owned = [
            Filtered {
                avoid: None,
                exclude: Some(EXCLUDE_TOLL),
                survivors: 0b1110,
                why: "exclude clears the tolled edge",
            },
            Filtered {
                avoid: Some(&avoid_flags),
                exclude: None,
                survivors: 0b1101,
                why: "avoid clears the edge inside the polygon",
            },
            Filtered {
                avoid: Some(&avoid_flags),
                exclude: Some(EXCLUDE_TOLL),
                survivors: 0b1100,
                why: "avoid folds the exclude bits in, so both edges go",
            },
        ];
        for case in &owned {
            let mask = select_snap_mask(&base, &exclude_flags, case.avoid, case.exclude);
            assert!(
                matches!(mask, Cow::Owned(_)),
                "a filtered mask must be owned ({})",
                case.why
            );
            assert_eq!(mask.as_ref(), &[case.survivors], "{}", case.why);
        }
    }

    /// #561 again, one level up: the plan `/table` and `/isochrone/bulk`
    /// actually build. Both used to clone `mode_data.mask` here.
    #[test]
    fn weight_plan_borrows_the_mode_mask_on_the_common_path() {
        let Some(state) = crate::testutil::belgium_state(SCOPE) else {
            return;
        };
        let mode = Mode(0);
        let mode_data = state.get_mode(mode);

        let base = resolve_weights(&state, &mode_data, mode, None, None).unwrap();
        assert!(
            matches!(base.snap_mask, Cow::Borrowed(_)),
            "#561: no avoid, no exclude — nothing to copy"
        );
        assert!(
            std::ptr::eq(base.snap_mask.as_ptr(), mode_data.mask.as_ptr()),
            "#561: the plan must borrow the mode's own mask"
        );
        assert!(base.is_base());
        assert!(!base.has_avoid());
        assert!(base.weights().is_none(), "base weights, no recustomization");

        let excluded = resolve_weights(&state, &mode_data, mode, Some(EXCLUDE_TOLL), None).unwrap();
        assert!(
            matches!(excluded.snap_mask, Cow::Owned(_)),
            "exclude filters the mask, so it must be owned"
        );
        assert!(!excluded.is_base());
        assert!(!excluded.has_avoid());
        assert_eq!(excluded.exclude_mask, Some(EXCLUDE_TOLL));
        assert!(
            excluded.weights().is_some(),
            "exclude alone selects the exclude weight set"
        );
    }

    /// #566: the six surfaces derive the SAME plan from the same
    /// parameters — the snap mask, the ownedness, the priority rule and
    /// the resulting weight set.
    #[test]
    fn every_surface_resolves_the_same_plan() {
        let Some(state) = crate::testutil::belgium_state(SCOPE) else {
            return;
        };
        let mode = Mode(0);
        let mode_data = state.get_mode(mode);

        // (label, exclude, avoid). The avoid rows share one cache key
        // per (mode, exclude) pair, so the recustomization runs once and
        // the other five surfaces read the cached entry.
        let cases: &[(&str, Option<u8>, Option<&str>)] = &[
            ("no option", None, None),
            ("exclude only", Some(EXCLUDE_TOLL), None),
            ("avoid only", None, Some(USABLE_AVOID)),
            ("avoid + exclude", Some(EXCLUDE_TOLL), Some(USABLE_AVOID)),
        ];

        for &(label, exclude, avoid) in cases {
            let (first_name, first_policy) = SURFACES[0];
            let expect = resolve_as(first_policy, &state, &mode_data, mode, exclude, avoid)
                .unwrap_or_else(|e| panic!("{first_name} failed to resolve {label}: {e}"));
            let expect_weights = expect.weights().map(|w| w as *const ExcludeWeights);

            for &(name, policy) in &SURFACES[1..] {
                let got = resolve_as(policy, &state, &mode_data, mode, exclude, avoid)
                    .unwrap_or_else(|e| panic!("{name} failed to resolve {label}: {e}"));
                assert_eq!(
                    got.is_base(),
                    expect.is_base(),
                    "{name} vs {first_name}: is_base disagrees on '{label}'"
                );
                assert_eq!(
                    got.has_avoid(),
                    expect.has_avoid(),
                    "{name} vs {first_name}: has_avoid disagrees on '{label}'"
                );
                assert_eq!(
                    got.exclude_mask, expect.exclude_mask,
                    "{name} vs {first_name}: exclude_mask disagrees on '{label}'"
                );
                assert_eq!(
                    matches!(got.snap_mask, Cow::Owned(_)),
                    matches!(expect.snap_mask, Cow::Owned(_)),
                    "{name} vs {first_name}: mask ownedness disagrees on '{label}'"
                );
                assert_eq!(
                    got.snap_mask.as_ref(),
                    expect.snap_mask.as_ref(),
                    "{name} vs {first_name}: snap mask disagrees on '{label}'"
                );
                assert_eq!(
                    got.weights().map(|w| w as *const ExcludeWeights),
                    expect_weights,
                    "{name} vs {first_name}: resolved weight set disagrees on '{label}' \
                     (the avoid-over-exclude priority, or the cache key)"
                );
            }

            // The priority rule itself, asserted once per case.
            match (exclude, avoid) {
                (None, None) => assert!(expect.weights().is_none(), "'{label}': base weights"),
                (_, Some(_)) => assert!(
                    expect.has_avoid() && expect.weights().is_some(),
                    "'{label}': avoid wins — its recustomization already folds exclude in"
                ),
                (Some(_), None) => assert!(
                    !expect.has_avoid() && expect.weights().is_some(),
                    "'{label}': exclude alone selects the exclude weight set"
                ),
            }
        }
    }

    /// The ONE divergence between the six, found while consolidating and
    /// deliberately kept: `/trip` and `/match` have always resolved their
    /// avoid polygon with a bare `.ok()` inside `spawn_blocking`, so a
    /// polygon that covers no edge degrades to a plain query there while
    /// the other four answer 400. Locked, not traded away (#566).
    #[test]
    fn only_trip_and_match_ignore_an_unusable_avoid_polygon() {
        let Some(state) = crate::testutil::belgium_state(SCOPE) else {
            return;
        };
        let mode = Mode(0);
        let mode_data = state.get_mode(mode);

        for &(name, policy) in SURFACES {
            let got = resolve_as(policy, &state, &mode_data, mode, None, Some(UNUSABLE_AVOID));
            match policy {
                AvoidPolicy::Reject => {
                    let err = got.err().unwrap_or_else(|| {
                        panic!("{name} must reject an avoid polygon that covers no edge")
                    });
                    assert_eq!(
                        err, "no edges found inside avoid polygon(s)",
                        "{name}: the 400 body must not drift"
                    );
                }
                AvoidPolicy::Ignore => {
                    let plan = got.unwrap_or_else(|_| {
                        panic!("{name} must degrade to a plain query, not fail")
                    });
                    assert!(
                        plan.is_base(),
                        "{name}: the dropped polygon leaves a base plan"
                    );
                    assert!(
                        matches!(plan.snap_mask, Cow::Borrowed(_)),
                        "{name}: and a base plan borrows its mask (#561)"
                    );
                }
            }
        }
    }
}
