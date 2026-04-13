//! Euclidean pre-filter for matrix and catchment queries.
//!
//! When a `radius_km` parameter is supplied to /table, /table/stream, Arrow Flight
//! `matrix`, or /catchment, this module computes a per-source list of reachable
//! targets (or none-filter) so the routing layer can short-circuit pairs that
//! are provably too far to be of interest.
//!
//! The filter uses great-circle distance (haversine). A longitude-sorted target
//! index lets us binary-search the longitude band implied by `radius_km / (111.32 * cos(lat))`
//! before running the exact haversine check — which keeps the cost effectively
//! proportional to the number of *kept* pairs rather than N×M.
//!
//! Correctness note: pairs dropped by the filter are emitted as `u32::MAX`
//! (unreachable) in the final matrix. The routing layer preserves this by
//! applying a `neighbor_mask` after the M2M solve — see `table.rs` and
//! `catchment.rs` for the call sites.

use crate::nbg::haversine_distance;

/// Parsed `radius_km` parameter as received from a JSON request body.
///
/// The parameter accepts:
/// - omitted / null / 0 / "" → [`RadiusParam::None`]
/// - positive number ("50.0" or 50) → [`RadiusParam::Km`]
/// - string "auto" (case-insensitive) → [`RadiusParam::Auto`]
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RadiusParam {
    /// No filter applied.
    None,
    /// Server-computed radius (p95 of pairwise haversine distances × 1.1).
    Auto,
    /// Explicit kilometre hard cap.
    Km(f64),
}

/// Parse a `radius_km` JSON value into a [`RadiusParam`].
///
/// Accepts both string and number forms. An unrecognised string, non-finite
/// value, or a non-positive number collapses to `None` (i.e. "no filter").
pub fn parse_radius(raw: Option<&serde_json::Value>) -> RadiusParam {
    let Some(v) = raw else {
        return RadiusParam::None;
    };
    match v {
        serde_json::Value::Null => RadiusParam::None,
        serde_json::Value::Number(n) => {
            if let Some(f) = n.as_f64() {
                if f.is_finite() && f > 0.0 {
                    RadiusParam::Km(f)
                } else {
                    RadiusParam::None
                }
            } else {
                RadiusParam::None
            }
        }
        serde_json::Value::String(s) => {
            let trimmed = s.trim();
            if trimmed.is_empty() || trimmed == "0" {
                return RadiusParam::None;
            }
            if trimmed.eq_ignore_ascii_case("auto") {
                return RadiusParam::Auto;
            }
            match trimmed.parse::<f64>() {
                Ok(f) if f.is_finite() && f > 0.0 => RadiusParam::Km(f),
                _ => RadiusParam::None,
            }
        }
        _ => RadiusParam::None,
    }
}

/// Maximum auto-radius in km. Anything beyond this is a nonsense query.
const MAX_AUTO_RADIUS_KM: f64 = 1000.0;

/// Compute p95 of pairwise source→target great-circle distances, multiplied by 1.1.
///
/// Uses a sample-based estimate when N×M is too large to enumerate exactly.
/// Returns 0 if either list is empty.
pub fn auto_radius_km(sources: &[(f64, f64)], targets: &[(f64, f64)]) -> f64 {
    if sources.is_empty() || targets.is_empty() {
        return 0.0;
    }

    // Limit the number of pairs we collect — the exact cap is arbitrary but
    // must be large enough for a stable percentile. 200k samples is overkill
    // for statistical purposes but keeps us under 10ms of CPU even in the
    // pathological case.
    const SAMPLE_CAP: usize = 200_000;
    let n_pairs_full = sources.len().saturating_mul(targets.len());
    let mut distances_km: Vec<f64> = Vec::with_capacity(n_pairs_full.min(SAMPLE_CAP));

    if n_pairs_full <= SAMPLE_CAP {
        for &(slon, slat) in sources {
            for &(tlon, tlat) in targets {
                let m = haversine_distance(slat, slon, tlat, tlon);
                distances_km.push(m / 1000.0);
            }
        }
    } else {
        // Stratified stride sample: deterministic, no RNG.
        let stride = n_pairs_full.div_ceil(SAMPLE_CAP);
        let mut i = 0usize;
        while i < n_pairs_full {
            let si = i / targets.len();
            let ti = i % targets.len();
            let (slon, slat) = sources[si];
            let (tlon, tlat) = targets[ti];
            let m = haversine_distance(slat, slon, tlat, tlon);
            distances_km.push(m / 1000.0);
            i = i.saturating_add(stride);
        }
    }

    if distances_km.is_empty() {
        return 0.0;
    }

    // p95 via partial sort. We cap the index at `n-2` (when n >= 2) so the
    // auto radius is always strictly below the observed maximum — without
    // this, tiny samples where p95 lands on the max lead to "nothing gets
    // pruned, so what was the point?" behaviour. The ×1.1 slack already
    // absorbs the resulting underestimate.
    distances_km.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let n = distances_km.len();
    let mut idx = ((n as f64) * 0.95).floor() as usize;
    idx = idx.min(n - 1);
    if n >= 2 {
        idx = idx.min(n - 2);
    }
    let p95 = distances_km[idx];
    (p95 * 1.1).min(MAX_AUTO_RADIUS_KM)
}

/// For each source, return the sorted indices of targets within `radius_km`.
///
/// Coordinates are `(lon, lat)`. The algorithm sorts targets by longitude once,
/// then for each source derives the longitude half-width `radius_km / (111.32 * cos(lat))`
/// and binary-searches that band before applying an exact haversine check.
/// This runs in roughly `O(N log M + N·K)` where `K` is the average number of
/// targets in-band.
pub fn build_neighbors(
    sources: &[(f64, f64)],
    targets: &[(f64, f64)],
    radius_km: f64,
) -> Vec<Vec<u32>> {
    let n_sources = sources.len();
    let n_targets = targets.len();

    if n_sources == 0 || !radius_km.is_finite() || radius_km <= 0.0 {
        return vec![Vec::new(); n_sources];
    }

    if n_targets == 0 {
        return vec![Vec::new(); n_sources];
    }

    // Sort target indices by longitude for band lookup.
    let mut order: Vec<u32> = (0..n_targets as u32).collect();
    order.sort_by(|&a, &b| {
        targets[a as usize]
            .0
            .partial_cmp(&targets[b as usize].0)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let sorted_lons: Vec<f64> = order.iter().map(|&i| targets[i as usize].0).collect();

    let radius_m = radius_km * 1000.0;

    let mut result: Vec<Vec<u32>> = Vec::with_capacity(n_sources);
    for &(slon, slat) in sources {
        // Longitude half-width. At high latitudes cos(lat) -> 0 so we must guard.
        let cos_lat = slat.to_radians().cos().abs();
        let lon_half_deg = if cos_lat < 1e-9 {
            // Near the poles every target could be within radius → no pruning.
            360.0
        } else {
            (radius_km / (111.32 * cos_lat)).min(360.0)
        };

        let lo = slon - lon_half_deg;
        let hi = slon + lon_half_deg;

        // Binary search the sorted-lon array for the [lo, hi] slice.
        // We use `partition_point` because it matches our ordering comparator.
        let start = sorted_lons.partition_point(|&x| x < lo);
        let end = sorted_lons.partition_point(|&x| x <= hi);

        let mut row: Vec<u32> = Vec::new();
        if start < end {
            for &tgt_idx in &order[start..end] {
                let (tlon, tlat) = targets[tgt_idx as usize];
                let d = haversine_distance(slat, slon, tlat, tlon);
                if d <= radius_m {
                    row.push(tgt_idx);
                }
            }
        }

        // If the query crosses the antimeridian (lo < -180 or hi > 180), scan
        // the wrap-around band. This handles wide radii near the date line.
        if lon_half_deg < 360.0 && (lo < -180.0 || hi > 180.0) {
            let (wrap_lo, wrap_hi) = if lo < -180.0 {
                (lo + 360.0, 180.0)
            } else {
                (-180.0, hi - 360.0)
            };
            let ws = sorted_lons.partition_point(|&x| x < wrap_lo);
            let we = sorted_lons.partition_point(|&x| x <= wrap_hi);
            for &tgt_idx in &order[ws..we] {
                // Skip targets already added in the primary band.
                if row.contains(&tgt_idx) {
                    continue;
                }
                let (tlon, tlat) = targets[tgt_idx as usize];
                let d = haversine_distance(slat, slon, tlat, tlon);
                if d <= radius_m {
                    row.push(tgt_idx);
                }
            }
        }

        row.sort_unstable();
        result.push(row);
    }

    result
}

/// Reasonable mode→average-speed lookup (m/s) used by the optional bounded
/// PHAST path. `m/s * seconds = metres` so the returned decisecond bound is
/// `radius_m * 10 / speed_mps`.
///
/// These values intentionally err on the fast side so the bound never cuts off
/// legitimate routes. Unknown modes fall back to 5 m/s.
pub fn decisecond_bound_for_radius(mode_name: &str, radius_km: f64) -> u32 {
    let speed_mps = match mode_name.to_ascii_lowercase().as_str() {
        "car" => 22.0_f64,
        "truck" => 18.0,
        "bike" => 4.0,
        "foot" => 1.2,
        _ => 5.0,
    };
    let radius_m = radius_km * 1000.0;
    // deciseconds (= 0.1 s)
    let ds = (radius_m / speed_mps) * 10.0;
    if !ds.is_finite() || ds <= 0.0 {
        return u32::MAX;
    }
    if ds >= u32::MAX as f64 {
        u32::MAX
    } else {
        ds.ceil() as u32
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parse_radius_none_variants() {
        assert_eq!(parse_radius(None), RadiusParam::None);
        assert_eq!(parse_radius(Some(&json!(null))), RadiusParam::None);
        assert_eq!(parse_radius(Some(&json!(""))), RadiusParam::None);
        assert_eq!(parse_radius(Some(&json!("0"))), RadiusParam::None);
        assert_eq!(parse_radius(Some(&json!(0))), RadiusParam::None);
        assert_eq!(parse_radius(Some(&json!(-5.0))), RadiusParam::None);
    }

    #[test]
    fn parse_radius_numeric() {
        match parse_radius(Some(&json!(50))) {
            RadiusParam::Km(v) => assert!((v - 50.0).abs() < 1e-9),
            _ => panic!("expected Km"),
        }
        match parse_radius(Some(&json!("25.5"))) {
            RadiusParam::Km(v) => assert!((v - 25.5).abs() < 1e-9),
            _ => panic!("expected Km"),
        }
    }

    #[test]
    fn parse_radius_auto_case_insensitive() {
        assert_eq!(parse_radius(Some(&json!("auto"))), RadiusParam::Auto);
        assert_eq!(parse_radius(Some(&json!("AUTO"))), RadiusParam::Auto);
        assert_eq!(parse_radius(Some(&json!(" Auto "))), RadiusParam::Auto);
    }

    #[test]
    fn build_neighbors_empty_inputs() {
        let empty: Vec<(f64, f64)> = Vec::new();
        let result = build_neighbors(&empty, &empty, 10.0);
        assert!(result.is_empty());

        let sources = vec![(4.35, 50.85)];
        let result = build_neighbors(&sources, &empty, 10.0);
        assert_eq!(result.len(), 1);
        assert!(result[0].is_empty());

        let result = build_neighbors(&empty, &sources, 10.0);
        assert!(result.is_empty());
    }

    #[test]
    fn build_neighbors_single_source_all_within() {
        let sources = vec![(4.35, 50.85)];
        // All points within ~5km of Brussels.
        let targets = vec![(4.36, 50.86), (4.34, 50.84), (4.35, 50.85)];
        let result = build_neighbors(&sources, &targets, 10.0);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], vec![0, 1, 2]);
    }

    #[test]
    fn build_neighbors_none_within() {
        let sources = vec![(4.35, 50.85)]; // Brussels
        let targets = vec![(2.35, 48.86), (13.40, 52.52)]; // Paris, Berlin
        let result = build_neighbors(&sources, &targets, 50.0);
        assert_eq!(result.len(), 1);
        assert!(result[0].is_empty());
    }

    #[test]
    fn build_neighbors_mixed_distances() {
        let sources = vec![(4.35, 50.85)]; // Brussels
        let targets = vec![
            (4.35, 50.85), // 0 km (self)
            (4.86, 50.47), // Leuven ~50 km
            (3.71, 51.05), // Ghent ~50 km
            (5.57, 50.63), // Liège ~90 km
        ];
        // Radius 70 km should include the self/Leuven/Ghent cluster but not Liège.
        let result = build_neighbors(&sources, &targets, 70.0);
        assert_eq!(result.len(), 1);
        assert!(result[0].contains(&0), "self must always be in");
        assert!(result[0].contains(&1), "Leuven (~50km) must be within 70km");
        assert!(result[0].contains(&2), "Ghent (~50km) must be within 70km");
        assert!(
            !result[0].contains(&3),
            "Liege (~90km) must not be within 70km"
        );
    }

    #[test]
    fn build_neighbors_longitude_wrap_does_not_crash() {
        // Points near the antimeridian. This mostly checks we don't panic on
        // lon_half going out of [-180, 180].
        let sources = vec![(179.9, 0.0)];
        let targets = vec![(-179.9, 0.0), (179.8, 0.0), (0.0, 0.0)];
        // ~22 km across the date line; -179.9/179.9 should cluster.
        let result = build_neighbors(&sources, &targets, 50.0);
        assert_eq!(result.len(), 1);
        // 179.8 is ~11 km away and must be included.
        assert!(result[0].contains(&1));
    }

    #[test]
    fn build_neighbors_antipodal() {
        let sources = vec![(0.0, 0.0)];
        let targets = vec![(180.0, 0.0)];
        // Half circumference ≈ 20 015 km — far beyond any sane radius.
        let result = build_neighbors(&sources, &targets, 100.0);
        assert_eq!(result.len(), 1);
        assert!(result[0].is_empty());
    }

    #[test]
    fn auto_radius_scales_with_input() {
        // Two clusters 100 km apart. The auto radius is p95-capped-to-(n-2)
        // × 1.1, which for {1, 90} lands on ~1.1 km — strictly below the
        // far point so at least one target gets pruned.
        let sources = vec![(4.35, 50.85)];
        let targets = vec![(4.36, 50.86), (5.57, 50.63)]; // ~1 km, ~90 km
        let r = auto_radius_km(&sources, &targets);
        assert!(r > 0.0 && r < 90.0, "got {}", r);
    }

    #[test]
    fn auto_radius_prunes_farthest_with_4_points() {
        // Realistic "distant outlier" shape — the tail must be excluded.
        let sources = vec![(4.35, 50.85)]; // Brussels
        let targets = vec![
            (4.35, 50.85), // self ~0 km
            (4.86, 50.47), // Leuven ~55 km
            (3.71, 51.05), // Ghent ~50 km
            (5.57, 50.63), // Liège ~90 km
        ];
        let r = auto_radius_km(&sources, &targets);
        // Must be strictly below the farthest pair (~90 km).
        assert!(r > 0.0 && r < 90.0, "got {}", r);
    }

    #[test]
    fn auto_radius_empty_is_zero() {
        let empty: Vec<(f64, f64)> = Vec::new();
        assert_eq!(auto_radius_km(&empty, &empty), 0.0);
    }

    #[test]
    fn decisecond_bound_sane() {
        // 50 km by car at 22 m/s ≈ 2273 s → 22 727 deciseconds.
        let ds = decisecond_bound_for_radius("car", 50.0);
        assert!(ds > 20_000 && ds < 30_000, "got {}", ds);
        // Unknown mode falls back to 5 m/s.
        let ds_default = decisecond_bound_for_radius("unicorn", 5.0);
        assert!(ds_default > 9_000 && ds_default < 11_000);
    }
}
