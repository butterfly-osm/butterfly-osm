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
#[derive(Debug, Clone, PartialEq)]
pub enum RadiusParam {
    /// No filter applied.
    None,
    /// Server-computed radius (p95 of pairwise haversine distances × 1.1).
    Auto,
    /// Explicit kilometre hard cap (same radius for every origin).
    Km(f64),
    /// #531: one kilometre cap PER ORIGIN (`len == origins`). Gravity-model
    /// capture radii that vary per origin, pruned tighter server-side than a
    /// single global radius (or the client's bucket-and-multi-call workaround).
    PerOrigin(Vec<f64>),
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
        serde_json::Value::Array(arr) => {
            // Per-origin radii. A non-positive / non-finite / null entry means
            // "no filter for THIS origin" (represented as +inf, so build keeps
            // every target for it). An empty array collapses to no filter.
            if arr.is_empty() {
                return RadiusParam::None;
            }
            let radii: Vec<f64> = arr
                .iter()
                .map(|v| match v.as_f64() {
                    Some(f) if f.is_finite() && f > 0.0 => f,
                    _ => f64::INFINITY,
                })
                .collect();
            RadiusParam::PerOrigin(radii)
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

/// Largest number of `(source, target)` entries a neighbour mask may hold.
///
/// The mask is a `Vec<Vec<u32>>` built up front and kept for the whole
/// request — on the streamed matrix path it is the ONLY structure that
/// still scales with `S x T`, because everything else is either tiled or
/// streamed. Two ordinary-looking requests fill it without limit: a
/// generous `radius_km` over a large `S x T`, and a per-origin array of
/// "no filter" entries, each of which materialises every target index for
/// its origin. Several hundred thousand origins against a few thousand
/// destinations — the shape the sparse matrix path is documented for —
/// reaches billions of entries and takes the process down.
///
/// 100 million entries is ~400 MB of `u32`. It is far above any radius
/// that actually prunes (a mask that big is not filtering anything), and
/// far below an out-of-memory kill on either deployment. Over it, the
/// request is refused with the count and the limit rather than allocated.
pub const MAX_NEIGHBOR_ENTRIES: usize = 100_000_000;

/// The refusal, shared so both matrix surfaces word it identically.
fn neighbor_budget_error(entries: usize, budget: usize) -> String {
    format!(
        "radius_km neighbour set too large: over {budget} in-radius (source, target) \
         pairs (reached {entries}). Reduce radius_km or split the request into \
         smaller batches"
    )
}

/// Build the per-source neighbour mask for a `radius_km` request AND report
/// the largest radius in play (`None` ⇒ no filter at all).
///
/// #602: ONE body for the Flight `matrix` action and REST `/table`. Both need
/// the mask AND the kilometre value behind it — the mask alone does not retain
/// it, and the compute bound below has to be derived from the same number the
/// mask was built from or the two surfaces prune differently.
pub fn build_radius_mask(
    param: RadiusParam,
    sources: &[(f64, f64)],
    targets: &[(f64, f64)],
) -> Result<(Option<Vec<Vec<u32>>>, Option<f64>), String> {
    match param {
        RadiusParam::None => Ok((None, None)),
        RadiusParam::Km(r) => Ok((Some(build_neighbors(sources, targets, r)?), Some(r))),
        RadiusParam::Auto => {
            let r = auto_radius_km(sources, targets);
            if r > 0.0 {
                Ok((Some(build_neighbors(sources, targets, r)?), Some(r)))
            } else {
                Ok((None, None))
            }
        }
        // #531: validated `len == origins` upstream. The widest origin sets
        // the compute bound — a tighter one is still masked at emit.
        RadiusParam::PerOrigin(radii) => {
            let max = radii
                .iter()
                .copied()
                .filter(|r| r.is_finite())
                .fold(None, |acc: Option<f64>, r| {
                    Some(acc.map_or(r, |a| a.max(r)))
                });
            let mask = build_neighbors_per_origin(sources, targets, &radii)?;
            Ok((Some(mask), max))
        }
    }
}

/// #538/#602: the conservative TRAVEL-TIME bound a `radius_km` implies, in
/// seconds.
///
/// Crow-fly kilometres cannot bound road time exactly, so the cap is
/// deliberately generous — a 1.8× detour driven at 36 km/h, plus a flat
/// allowance for the first and last mile — and every in-radius cell it still
/// cuts is recomputed EXACTLY by the caller's rescue pass. The two constants
/// trade rescue frequency against sweep pruning; they never trade
/// correctness, because nothing is served straight out of the bounded sweep
/// above the cap.
///
/// ONE body for the Flight `matrix` action and REST `/table` (#602): a caller
/// that sets a radius must get the same cells whichever transport it uses.
pub fn radius_compute_cap_s(radius_km: f64) -> u32 {
    const RADIUS_SEC_PER_KM: f64 = 180.0; // ≥ detour 1.8 at 36 km/h
    const RADIUS_BASE_S: f64 = 900.0;
    (radius_km * RADIUS_SEC_PER_KM + RADIUS_BASE_S)
        .ceil()
        .min(u32::MAX as f64) as u32
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
) -> Result<Vec<Vec<u32>>, String> {
    // Historical scalar contract: an invalid radius filters EVERYTHING (all
    // rows empty). Preserved explicitly so the wrapper can't inherit the
    // per-origin "inf = no filter" semantics (that path is array-only, #531).
    if !radius_km.is_finite() || radius_km <= 0.0 {
        return Ok(vec![Vec::new(); sources.len()]);
    }
    // Otherwise share the single per-origin implementation so the band-search
    // + antimeridian logic lives once.
    build_neighbors_per_origin(sources, targets, &vec![radius_km; sources.len()])
}

/// #531: like [`build_neighbors`] but with ONE radius per origin
/// (`radii.len() == sources.len()`). A non-finite / non-positive radius for an
/// origin means "no filter for that origin" (every target kept). Targets are
/// still sorted by longitude ONCE; each origin band-searches with its own
/// radius, so a tight origin prunes hard while a wide one keeps more — no
/// bucket-and-multi-call workaround needed.
pub fn build_neighbors_per_origin(
    sources: &[(f64, f64)],
    targets: &[(f64, f64)],
    radii: &[f64],
) -> Result<Vec<Vec<u32>>, String> {
    build_neighbors_within(sources, targets, radii, MAX_NEIGHBOR_ENTRIES)
}

/// The implementation, with the budget as a parameter so a test can drive
/// the refusal without allocating a hundred million entries to reach it.
fn build_neighbors_within(
    sources: &[(f64, f64)],
    targets: &[(f64, f64)],
    radii: &[f64],
    budget: usize,
) -> Result<Vec<Vec<u32>>, String> {
    let n_sources = sources.len();
    let n_targets = targets.len();

    if n_sources == 0 {
        return Ok(Vec::new());
    }
    debug_assert_eq!(radii.len(), n_sources, "radii must be one per source");

    if n_targets == 0 {
        return Ok(vec![Vec::new(); n_sources]);
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

    let mut result: Vec<Vec<u32>> = Vec::with_capacity(n_sources);
    // Counted as the rows are built, so a request over the budget is
    // refused at the row that crosses it — not after the whole mask has
    // been allocated.
    let mut entries = 0usize;
    for (si, &(slon, slat)) in sources.iter().enumerate() {
        let radius_km = radii.get(si).copied().unwrap_or(f64::INFINITY);
        // No-filter origin (inf / non-positive): keep every target for it.
        if !radius_km.is_finite() || radius_km <= 0.0 {
            entries += n_targets;
            if entries > budget {
                return Err(neighbor_budget_error(entries, budget));
            }
            result.push((0..n_targets as u32).collect());
            continue;
        }
        let radius_m = radius_km * 1000.0;
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
        entries += row.len();
        if entries > budget {
            return Err(neighbor_budget_error(entries, budget));
        }
        result.push(row);
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parse_radius_array_is_per_origin() {
        // #531: an array parses to PerOrigin; non-positive/null entries become
        // +inf ("no filter for this origin"); an empty array is None.
        match parse_radius(Some(&json!([1.5, 2.0, 0, null]))) {
            RadiusParam::PerOrigin(r) => {
                assert_eq!(r.len(), 4);
                assert_eq!(r[0], 1.5);
                assert_eq!(r[1], 2.0);
                assert!(r[2].is_infinite(), "0 -> no filter (inf)");
                assert!(r[3].is_infinite(), "null -> no filter (inf)");
            }
            other => panic!("want PerOrigin, got {other:?}"),
        }
        assert_eq!(parse_radius(Some(&json!([]))), RadiusParam::None);
    }

    #[test]
    fn per_origin_radius_prunes_each_origin_by_its_own_radius() {
        // Two origins at the SAME point; three targets at ~1.1, ~2.2, ~3.3 km
        // due east. Origin 0 gets a 1.5 km cap (keeps only the nearest),
        // origin 1 gets 3 km (keeps the two nearest). Same geometry, different
        // radius -> different neighbor sets, which a single global radius
        // cannot express (the whole point of #531).
        let o = (4.35, 50.85);
        let sources = vec![o, o];
        // 1 deg lon at 50.85N ~ 70.2 km, so +0.0157 deg ~ 1.1 km, etc.
        let targets = vec![
            (o.0 + 0.0157, o.1), // ~1.1 km
            (o.0 + 0.0314, o.1), // ~2.2 km
            (o.0 + 0.0471, o.1), // ~3.3 km
        ];
        let out = build_neighbors_per_origin(&sources, &targets, &[1.5, 3.0])
            .expect("fixture is within the neighbour budget");
        assert_eq!(out.len(), 2);
        // Origin 0 (1.5 km): only target 0.
        assert_eq!(
            out[0],
            vec![0u32],
            "1.5 km origin keeps only the ~1.1 km target"
        );
        // Origin 1 (3.0 km): targets 0 and 1, not the ~3.3 km one.
        assert_eq!(
            out[1],
            vec![0u32, 1u32],
            "3 km origin keeps the two nearest"
        );
    }

    #[test]
    fn per_origin_inf_radius_keeps_all_targets_for_that_origin() {
        let o = (4.35, 50.85);
        let sources = vec![o, o];
        let targets = vec![(o.0 + 0.2, o.1), (o.0 + 0.4, o.1)];
        // Origin 0 tight (0.5 km -> nothing); origin 1 inf (-> everything).
        let out = build_neighbors_per_origin(&sources, &targets, &[0.5, f64::INFINITY])
            .expect("fixture is within the neighbour budget");
        assert!(out[0].is_empty(), "tight origin prunes all far targets");
        assert_eq!(out[1], vec![0u32, 1u32], "inf origin keeps every target");
    }

    #[test]
    fn per_origin_scalar_wrapper_matches_uniform_array() {
        // build_neighbors(r) must equal build_neighbors_per_origin(vec![r; n]).
        let sources = vec![(4.35, 50.85), (4.40, 50.80), (5.0, 51.0)];
        let targets = vec![(4.36, 50.86), (4.50, 50.90), (5.2, 51.1)];
        let scalar = build_neighbors(&sources, &targets, 5.0)
            .expect("fixture is within the neighbour budget");
        let per = build_neighbors_per_origin(&sources, &targets, &[5.0, 5.0, 5.0])
            .expect("fixture is within the neighbour budget");
        assert_eq!(scalar, per, "uniform per-origin must equal the scalar path");
    }

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

    /// #540: the neighbour mask was the one `S x T` structure with no
    /// bound anywhere on the matrix path — including the streamed one,
    /// where every other allocation is tiled. It must be REFUSED, with
    /// the limit in the message, and refused at the row that crosses the
    /// budget rather than after the whole mask has been built.
    #[test]
    fn a_neighbour_mask_over_its_budget_is_refused_early() {
        let sources: Vec<(f64, f64)> = (0..40).map(|i| (4.35 + i as f64 * 0.001, 50.85)).collect();
        let targets: Vec<(f64, f64)> = (0..10).map(|i| (4.35 + i as f64 * 0.001, 50.85)).collect();

        // Every origin keeps every target: 40 x 10 = 400 entries.
        let wide = vec![f64::INFINITY; sources.len()];
        let full = build_neighbors_within(&sources, &targets, &wide, 400)
            .expect("exactly at the budget is allowed");
        assert_eq!(full.iter().map(Vec::len).sum::<usize>(), 400);

        let err = build_neighbors_within(&sources, &targets, &wide, 399)
            .expect_err("one entry over the budget must be refused");
        assert!(err.contains("399"), "{err} must state the limit");
        assert!(
            err.contains("radius_km"),
            "{err} must name the knob to turn"
        );

        // Early: the refusal reports the count at the crossing row, not
        // the size of the mask it never built.
        let err = build_neighbors_within(&sources, &targets, &wide, 100)
            .expect_err("well over the budget must be refused");
        assert!(
            err.contains("110"),
            "{err} should report the count at the row that crossed (110), \
             proving it stopped there and did not build all 400"
        );

        // A radius that genuinely prunes stays under the budget and is
        // answered: the guard bounds the mask, it does not disable it.
        let tight = vec![0.15f64; sources.len()];
        let pruned = build_neighbors_within(&sources, &targets, &tight, 400)
            .expect("a pruning radius is well inside the budget");
        assert!(
            pruned.iter().map(Vec::len).sum::<usize>() < 400,
            "the fixture radius must actually prune, or this proves nothing"
        );

        // And the production entry points carry the production budget.
        assert_eq!(MAX_NEIGHBOR_ENTRIES, 100_000_000);
        assert!(build_neighbors(&sources, &targets, 1.0).is_ok());
        assert!(build_neighbors_per_origin(&sources, &targets, &wide).is_ok());
    }

    #[test]
    fn build_neighbors_empty_inputs() {
        let empty: Vec<(f64, f64)> = Vec::new();
        let result =
            build_neighbors(&empty, &empty, 10.0).expect("fixture is within the neighbour budget");
        assert!(result.is_empty());

        let sources = vec![(4.35, 50.85)];
        let result = build_neighbors(&sources, &empty, 10.0)
            .expect("fixture is within the neighbour budget");
        assert_eq!(result.len(), 1);
        assert!(result[0].is_empty());

        let result = build_neighbors(&empty, &sources, 10.0)
            .expect("fixture is within the neighbour budget");
        assert!(result.is_empty());
    }

    #[test]
    fn build_neighbors_single_source_all_within() {
        let sources = vec![(4.35, 50.85)];
        // All points within ~5km of Brussels.
        let targets = vec![(4.36, 50.86), (4.34, 50.84), (4.35, 50.85)];
        let result = build_neighbors(&sources, &targets, 10.0)
            .expect("fixture is within the neighbour budget");
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], vec![0, 1, 2]);
    }

    #[test]
    fn build_neighbors_none_within() {
        let sources = vec![(4.35, 50.85)]; // Brussels
        let targets = vec![(2.35, 48.86), (13.40, 52.52)]; // Paris, Berlin
        let result = build_neighbors(&sources, &targets, 50.0)
            .expect("fixture is within the neighbour budget");
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
        let result = build_neighbors(&sources, &targets, 70.0)
            .expect("fixture is within the neighbour budget");
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
        let result = build_neighbors(&sources, &targets, 50.0)
            .expect("fixture is within the neighbour budget");
        assert_eq!(result.len(), 1);
        // 179.8 is ~11 km away and must be included.
        assert!(result[0].contains(&1));
    }

    #[test]
    fn build_neighbors_antipodal() {
        let sources = vec![(0.0, 0.0)];
        let targets = vec![(180.0, 0.0)];
        // Half circumference ≈ 20 015 km — far beyond any sane radius.
        let result = build_neighbors(&sources, &targets, 100.0)
            .expect("fixture is within the neighbour budget");
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
}
