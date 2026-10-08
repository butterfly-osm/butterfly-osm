//! Gates — level vs the reference sets (share `ref_trip_routes`) and bands.

use std::collections::HashMap;

use serde_json::{Value, json};

use super::GateFail;
use super::ctx::{
    Ctx, REFS_PREFIX, RefRoute, check, check_errors, f0, mean, median, num, outlier_frac, pct,
    refs_path, skip, tup,
};
use super::flight::{col_bytes, col_f64, col_str, col_u64, rows};
use super::gates_rest::GateResult;
use super::geom::ring_area;
use super::http::pyf;

const BANDS_SKIP_REASON: &str = "/health reports bands:false — the loaded speeds table has no best/worst columns (no table is staged since the licensed provider's data was retired, 2026-09-28); every uncertainty=bands request answers 400";

fn pf(t: &HashMap<String, String>, k: &str) -> Option<f64> {
    t.get(k).and_then(|v| v.parse::<f64>().ok())
}

pub fn gate_ground_truth(ctx: &Ctx, trips_path: &str, checks: &str) -> GateResult {
    println!("== ground truth: reference trips ({trips_path}, {checks}) ==");
    let rec = ctx.ref_trip_routes(trips_path)?;
    let (rows, res) = (&rec.0, &rec.1);
    let t = &ctx.t;
    let mut ok_res: Vec<(f64, f64)> = Vec::new();
    let mut errors = 0usize;
    for (r, trip) in res.iter().zip(rows) {
        let Some(r) = r else {
            errors += 1;
            continue;
        };
        match (pf(trip, "ref_min"), pf(trip, "ref_km")) {
            (Some(ref_min), Some(ref_km)) if ref_min > 0.0 && ref_km > 0.0 => {
                ok_res.push((r.min / ref_min, r.km / ref_km))
            }
            _ => errors += 1,
        }
    }
    if ok_res.is_empty() {
        return Ok(check(
            "trip errors",
            false,
            &format!("{errors} of {} trips unusable", rows.len()),
        ));
    }
    let lfl = t.like_for_like_km_tol;
    let dur: Vec<f64> = ok_res
        .iter()
        .filter(|x| checks != "duration" || (x.1 - 1.0).abs() <= lfl)
        .map(|x| x.0)
        .collect();
    let dist: Vec<f64> = ok_res.iter().map(|x| x.1).collect();
    let (outliers, out_frac) = outlier_frac(&dist, 0.85, 1.2);
    let mut passed = check(
        "trip errors",
        errors <= t.max_errors,
        &format!("{errors} (max {})", t.max_errors),
    );
    if checks == "all" || checks == "duration" {
        let (p50d, p90d) = (pct(&dur, 0.5), pct(&dur, 0.9));
        passed &= check(
            "duration p50",
            t.dur_p50.0 <= p50d && p50d <= t.dur_p50.1,
            &format!("{p50d:.3} (bounds {})", tup(t.dur_p50)),
        );
        passed &= check(
            "duration p90",
            p90d <= t.dur_p90_max,
            &format!("{p90d:.3} (max {})", pyf(t.dur_p90_max)),
        );
    }
    if checks == "all" || checks == "distance" {
        let (p50m, p90m) = (pct(&dist, 0.5), pct(&dist, 0.9));
        passed &= check(
            "distance p50",
            t.dist_p50.0 <= p50m && p50m <= t.dist_p50.1,
            &format!("{p50m:.3} (bounds {})", tup(t.dist_p50)),
        );
        passed &= check(
            "distance p90",
            p90m <= t.dist_p90_max,
            &format!("{p90m:.3} (max {})", pyf(t.dist_p90_max)),
        );
        passed &= check(
            "distance outliers",
            out_frac <= t.dist_outliers_frac,
            &format!(
                "{outliers}/{} = {out_frac:.3} (max {})",
                dist.len(),
                pyf(t.dist_outliers_frac)
            ),
        );
    }
    if !dur.is_empty() {
        println!(
            "  stats: dur mean={:.3} p05={:.3} p95={:.3} | dist mean={:.3} p05={:.3} p95={:.3}",
            mean(&dur),
            pct(&dur, 0.05),
            pct(&dur, 0.95),
            mean(&dist),
            pct(&dist, 0.05),
            pct(&dist, 0.95)
        );
    }
    Ok(passed)
}

fn route_choice_stats(
    rows: &[HashMap<String, String>],
    res: &[Option<RefRoute>],
) -> (Vec<f64>, usize) {
    let mut ratios = Vec::new();
    let mut errors = 0usize;
    for (trip, r) in rows.iter().zip(res) {
        match (pf(trip, "ref_km"), r) {
            (Some(ref_km), Some(r)) if ref_km > 0.0 => ratios.push(r.km / ref_km),
            _ => errors += 1,
        }
    }
    (ratios, errors)
}

pub fn gate_route_choice(ctx: &Ctx, trips_path: &str) -> GateResult {
    let t = &ctx.t;
    let (hi, bound) = (t.dist_p90_max, t.choice_divergent_frac);
    println!("== route choice: engine route length vs the observed route ({trips_path}, #545) ==");
    let rec = ctx.ref_trip_routes(trips_path)?;
    let (ratios, errors) = route_choice_stats(&rec.0, &rec.1);
    let mut passed = check_errors(t, "route choice", errors, None);
    if ratios.len() < t.band_min_trips {
        return Ok(check(
            "route choice: usable pairs",
            false,
            &format!("{} (need {})", ratios.len(), t.band_min_trips),
        ));
    }
    let n_div = ratios.iter().filter(|&&x| x >= hi).count();
    let frac = n_div as f64 / ratios.len() as f64;
    passed &= check(
        &format!("share of pairs at or beyond {hi:.2}x the observed length"),
        frac <= bound,
        &format!(
            "{n_div}/{} = {frac:.3} (max {}) — KNOWN DEFECT #545: ~0.19 today. The bound is a ceiling on FURTHER degradation, not an endorsement of the current value; ratchet it down when #545 is fixed.",
            ratios.len(),
            pyf(bound)
        ),
    );
    println!(
        "  stats: length ratio p50={:.3} p90={:.3} p95={:.3} mean={:.3}",
        pct(&ratios, 0.5),
        pct(&ratios, 0.9),
        pct(&ratios, 0.95),
        mean(&ratios)
    );
    println!(
        "  attribution (weights vs engine) is bench/route_choice.py --compare <base-weights instance>; a single-instance gate cannot carry that control — see this gate's docstring."
    );
    Ok(passed)
}

pub fn gate_bands(ctx: &Ctx, refs_prefix_override: Option<&str>) -> GateResult {
    println!("== best / typical / worst bands: every API, ordering, level (2026-09-03) ==");
    if !ctx.bands_served() {
        skip(BANDS_SKIP_REASON);
        return Ok(true);
    }
    let t = &ctx.t;
    let mut passed = true;
    let pairs: [((f64, f64), (f64, f64)); 4] = [
        ((4.3517, 50.8503), (4.4025, 51.2194)),
        ((4.85, 50.55), (4.79, 50.60)),
        ((3.7174, 51.0543), (3.65, 51.02)),
        ((5.65, 50.1), (5.60, 50.15)),
    ];
    let (mut ok_route, mut n_route) = (true, 0usize);
    for (a, b) in &pairs {
        match ctx.route_json(
            a.0,
            a.1,
            b.0,
            b.1,
            "car",
            60,
            &[("uncertainty", "bands".into())],
        ) {
            Ok(r) => {
                let bt = r.get("duration_best_s").and_then(Value::as_f64);
                let tp = r
                    .get("duration_s")
                    .and_then(Value::as_f64)
                    .unwrap_or(f64::NAN);
                let wt = r.get("duration_worst_s").and_then(Value::as_f64);
                n_route += 1;
                ok_route &= matches!((bt, wt), (Some(bt), Some(wt)) if bt != 0.0 && wt != 0.0 && bt <= tp + 0.5 && tp <= wt + 0.5);
            }
            Err(e) => {
                ok_route = false;
                println!("    /route bands: {e}");
            }
        }
    }
    passed &= check(
        "/route uncertainty=bands: duration_best_s ≤ duration_s ≤ duration_worst_s",
        ok_route && n_route == pairs.len(),
        &format!("{n_route} pairs"),
    );
    let pts: [[f64; 2]; 5] = [
        [4.3517, 50.8503],
        [4.4025, 51.2194],
        [3.7174, 51.0543],
        [4.85, 50.55],
        [5.65, 50.1],
    ];
    let mut ok_table = true;
    for (shape, origins, dests) in [
        ("1×n", &pts[..1], &pts[..]),
        ("n×1", &pts[..], &pts[..1]),
        ("n×n", &pts[..], &pts[..]),
    ] {
        match ctx.table(
            origins,
            dests,
            "car",
            120,
            &[
                ("annotations", json!("duration")),
                ("uncertainty", json!("bands")),
            ],
        ) {
            Ok(r) => {
                let (d, bt, wt) = (
                    r.get("durations"),
                    r.get("durations_best"),
                    r.get("durations_worst"),
                );
                let empty = |v: Option<&Value>| {
                    v.is_none_or(|x| x.is_null() || x.as_array().is_some_and(|a| a.is_empty()))
                };
                if empty(bt) || empty(wt) {
                    ok_table = false;
                    println!("    /table {shape}: no band grids");
                    continue;
                }
                let (d, bt, wt) = (d.unwrap(), bt.unwrap(), wt.unwrap());
                for i in 0..origins.len() {
                    for j in 0..dests.len() {
                        let cell =
                            |g: &Value| g.pointer(&format!("/{i}/{j}")).and_then(Value::as_f64);
                        if let Some(x) = cell(d) {
                            let (b, w) =
                                (cell(bt).unwrap_or(f64::NAN), cell(wt).unwrap_or(f64::NAN));
                            if !(b <= x + 0.5 && x <= w + 0.5) {
                                ok_table = false;
                            }
                        }
                    }
                }
            }
            Err(e) => {
                ok_table = false;
                println!("    /table {shape}: {e}");
            }
        }
    }
    passed &= check(
        "/table uncertainty=bands (1×n, n×1, n×n): best ≤ typical ≤ worst per cell",
        ok_table,
        "3 shapes",
    );
    let (ok_trip, detail) = match ctx.http.post_json(
        &ctx.url("/trip"),
        &json!({"points": pts[..4], "mode": "car", "uncertainty": "bands"}),
        120,
    ) {
        Ok(r) => {
            let tr = r
                .get("trips")
                .and_then(Value::as_array)
                .and_then(|a| a.first())
                .cloned()
                .unwrap_or(r.clone());
            let bt = tr.get("duration_best").and_then(Value::as_f64);
            let tp = tr
                .get("duration_s")
                .and_then(Value::as_f64)
                .or_else(|| tr.get("duration").and_then(Value::as_f64));
            let wt = tr.get("duration_worst").and_then(Value::as_f64);
            let ok = matches!((bt, tp, wt), (Some(b), Some(p), Some(w)) if b != 0.0 && w != 0.0 && p != 0.0 && b <= p + 0.5 && p <= w + 0.5);
            let detail = if ok {
                format!(
                    "best {} ≤ typical {} ≤ worst {}",
                    f0(bt.unwrap()),
                    f0(tp.unwrap()),
                    f0(wt.unwrap())
                )
            } else {
                let s =
                    |v: Option<&Value>| v.map(|x| x.to_string()).unwrap_or_else(|| "None".into());
                format!(
                    "{{'duration_best': {}, 'duration_s': {}, 'duration_worst': {}}}",
                    s(tr.get("duration_best")),
                    s(tr.get("duration_s")),
                    s(tr.get("duration_worst"))
                )
            };
            (ok, detail)
        }
        Err(e) => (false, e.to_string()),
    };
    passed &= check(
        "/trip uncertainty=bands: duration_best ≤ duration ≤ duration_worst",
        ok_trip,
        &detail,
    );
    let (mut ok_iso, mut n_iso) = (true, 0usize);
    for (lon, lat) in [(4.3517, 50.8503), (4.85, 50.55)] {
        match ctx.http.json(
            &ctx.url(&format!(
                "/isochrone?lon={}&lat={}&time_s=600&mode=car&uncertainty=bands&geometries=geojson",
                pyf(lon),
                pyf(lat)
            )),
            120,
        ) {
            Ok(r) => {
                let mut areas: HashMap<String, f64> = HashMap::new();
                for f in r
                    .get("contours")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default()
                {
                    let g = f.get("geometry").cloned().unwrap_or(Value::Null);
                    if g.get("type").and_then(Value::as_str) == Some("Polygon") {
                        let ring: Vec<(f64, f64)> = g
                            .pointer("/coordinates/0")
                            .and_then(Value::as_array)
                            .map(|a| {
                                a.iter()
                                    .filter_map(|p| {
                                        Some((p.get(0)?.as_f64()?, p.get(1)?.as_f64()?))
                                    })
                                    .collect()
                            })
                            .unwrap_or_default();
                        let band = f
                            .get("band")
                            .and_then(Value::as_str)
                            .unwrap_or("typical")
                            .to_string();
                        areas.insert(band, ring_area(&ring));
                    }
                }
                n_iso += 1;
                let have = ["best", "typical", "worst"]
                    .iter()
                    .all(|k| areas.contains_key(*k));
                if !(have
                    && areas["best"] >= areas["typical"] * 0.999
                    && areas["typical"] >= areas["worst"] * 0.999)
                {
                    ok_iso = false;
                    let mut ks: Vec<&String> = areas.keys().collect();
                    ks.sort();
                    println!(
                        "    /isochrone bands areas: {{{}}}",
                        ks.iter()
                            .map(|k| format!("'{k}': {}", areas[*k]))
                            .collect::<Vec<_>>()
                            .join(", ")
                    );
                }
            }
            Err(e) => {
                ok_iso = false;
                println!("    /isochrone bands: {e}");
            }
        }
    }
    passed &= check(
        "/isochrone uncertainty=bands: best ⊇ typical ⊇ worst (areas)",
        ok_iso && n_iso == 2,
        &format!("{n_iso} origins"),
    );

    if !ctx.flight {
        skip("Flight bands: --no-flight");
    } else {
        let m = ctx.do_get(
            "matrix",
            "car",
            &json!({"origins": pts, "destinations": pts, "uncertainty": "bands"}),
        )?;
        let mut bands: Vec<String> = Vec::new();
        let mut piv: HashMap<(u64, u64), HashMap<String, f64>> = HashMap::new();
        let has_band = rows(&m)
            .next()
            .is_some_and(|(b, _)| super::flight::has_col(b, "band"));
        for (b, i) in rows(&m) {
            let band = col_str(b, "band", i).unwrap_or("").to_string();
            if !bands.contains(&band) {
                bands.push(band.clone());
            }
            let (Some(s), Some(t_), Some(d)) = (
                col_u64(b, "source_idx", i),
                col_u64(b, "target_idx", i),
                col_f64(b, "duration_ms", i),
            ) else {
                continue;
            };
            piv.entry((s, t_)).or_default().insert(band, d);
        }
        bands.sort();
        let mut okm = has_band
            && bands == ["best", "typical", "worst"]
            && m.num_rows() == 3 * pts.len() * pts.len();
        if okm {
            okm = piv.values().all(
                |c| match (c.get("best"), c.get("typical"), c.get("worst")) {
                    (Some(b), Some(t_), Some(w)) => *b <= t_ + 1.0 && *t_ <= w + 1.0,
                    _ => false,
                },
            );
        }
        passed &= check(
            "Flight matrix uncertainty=bands: band column, 3 passes, best ≤ typical ≤ worst",
            okm,
            &format!("{} rows", m.num_rows()),
        );
        let pairs_j: Vec<[f64; 4]> = pairs.iter().map(|(a, b)| [a.0, a.1, b.0, b.1]).collect();
        let rb = ctx.do_get(
            "route_batch",
            "car",
            &json!({"pairs": pairs_j, "uncertainty": "bands"}),
        )?;
        let has_band_rb = rows(&rb)
            .next()
            .is_some_and(|(b, _)| super::flight::has_col(b, "band"));
        let mut okr = has_band_rb && rb.num_rows() == 3 * pairs.len();
        if okr {
            let mut piv: HashMap<u64, HashMap<String, f64>> = HashMap::new();
            for (b, i) in rows(&rb) {
                if let (Some(p), Some(band), Some(d)) = (
                    col_u64(b, "pair_idx", i),
                    col_str(b, "band", i),
                    col_f64(b, "duration_s", i),
                ) {
                    piv.entry(p).or_default().insert(band.to_string(), d);
                }
            }
            okr = piv.values().all(
                |c| match (c.get("best"), c.get("typical"), c.get("worst")) {
                    (Some(b), Some(t_), Some(w)) => *b <= t_ + 0.5 && *t_ <= w + 0.5,
                    _ => false,
                },
            );
        }
        passed &= check(
            "Flight route_batch uncertainty=bands: band column, best ≤ typical ≤ worst",
            okr,
            &format!("{} rows", rb.num_rows()),
        );
        let iso = ctx.do_get(
            "isochrone",
            "car",
            &json!({"lon": 4.85, "lat": 50.55, "intervals": [600], "uncertainty": "bands"}),
        )?;
        let has_band_iso = rows(&iso)
            .next()
            .is_some_and(|(b, _)| super::flight::has_col(b, "band"));
        let mut oki = has_band_iso && iso.num_rows() == 3;
        if oki {
            let got: std::collections::BTreeSet<String> = rows(&iso)
                .filter(|(b, i)| col_bytes(b, "polygon_wkb", *i).is_some_and(|w| !w.is_empty()))
                .filter_map(|(b, i)| col_str(b, "band", i).map(String::from))
                .collect();
            oki = ["best", "typical", "worst"]
                .iter()
                .all(|k| got.contains(*k));
        }
        passed &= check(
            "Flight isochrone uncertainty=bands: one polygon per band",
            oki,
            &format!("{} rows", iso.num_rows()),
        );
    }

    // (c) level per profile against its time-stamped reference set.
    let refs_prefix = match refs_path(ctx.refs_dir.as_deref(), REFS_PREFIX, refs_prefix_override) {
        Ok(p) => p,
        Err(super::ctx::RefsErr::Retired(e)) => {
            skip(&format!("band levels vs reference: {e}"));
            return Ok(passed);
        }
        Err(super::ctx::RefsErr::Unavailable(e)) => return Err(GateFail::Unavailable(e)),
    };
    let (lo, hi) = t.band_level;
    for (name, field) in [
        ("typical", "min"),
        ("best", "best_min"),
        ("worst", "worst_min"),
    ] {
        let path = format!("{refs_prefix}_{name}.csv");
        let rec = match ctx.ref_trip_routes(&path) {
            Ok(r) => r,
            Err(e) => {
                passed &= check(
                    &format!("{name} level vs reference"),
                    false,
                    &format!("cannot read reference set: {e}"),
                );
                continue;
            }
        };
        let (trips, res) = (&rec.0, &rec.1);
        let pick = |r: &RefRoute| match field {
            "min" => r.min,
            "best_min" => r.best_min,
            _ => r.worst_min,
        };
        let mut ratios: Vec<f64> = res
            .iter()
            .zip(trips)
            .filter(|(r, trip)| ctx.like_for_like(r.as_ref(), trip))
            .filter_map(|(r, trip)| {
                let r = r.as_ref()?;
                let v = pick(r);
                let ref_min = pf(trip, "ref_min")?;
                (v > 0.0 && ref_min > 0.0).then_some(v / ref_min)
            })
            .collect();
        ratios.sort_by(|a, b| a.total_cmp(b));
        if ratios.len() < t.band_min_trips {
            passed &= check(
                &format!("{name} level vs reference"),
                false,
                &format!(
                    "only {} like-for-like trips (need {})",
                    ratios.len(),
                    t.band_min_trips
                ),
            );
            continue;
        }
        let med = median(&ratios);
        passed &= check(
            &format!(
                "{name}: median(engine/{name} reference) in [{}, {}] (like-for-like routes)",
                pyf(lo),
                pyf(hi)
            ),
            lo <= med && med <= hi,
            &format!(
                "{med:.3} (p10 {:.3}, p90 {:.3}, n={})",
                ratios[ratios.len() / 10],
                ratios[9 * ratios.len() / 10],
                ratios.len()
            ),
        );
        if name != "typical" {
            continue;
        }
        let (rlo, rhi) = t.band_regional;
        for (rname, bx, both) in [
            ("Brussels-internal", (4.25, 50.76, 4.50, 50.92), true),
            ("coast (West Flanders)", (2.50, 51.00, 3.35, 51.40), false),
        ] {
            let inside = |trip: &HashMap<String, String>, k1: &str, k2: &str| -> bool {
                match (pf(trip, k1), pf(trip, k2)) {
                    (Some(x), Some(y)) => bx.0 <= x && x <= bx.2 && bx.1 <= y && y <= bx.3,
                    _ => false,
                }
            };
            let sel: Vec<(&RefRoute, &HashMap<String, String>)> = res
                .iter()
                .zip(trips)
                .filter(|(r, trip)| ctx.like_for_like(r.as_ref(), trip))
                .filter_map(|(r, trip)| r.as_ref().map(|r| (r, trip)))
                .filter(|(_, trip)| {
                    let a = inside(trip, "long_1", "lat_1");
                    let b = inside(trip, "long_2", "lat_2");
                    if both { a && b } else { a || b }
                })
                .collect();
            if sel.len() >= t.band_min_regional {
                let mr = median(
                    &sel.iter()
                        .filter_map(|(r, trip)| pf(trip, "ref_min").map(|m| r.min / m))
                        .collect::<Vec<_>>(),
                );
                passed &= check(
                    &format!(
                        "typical: {rname} like-for-like pairs in [{}, {}] (#543)",
                        pyf(rlo),
                        pyf(rhi)
                    ),
                    rlo <= mr && mr <= rhi,
                    &format!("{mr:.3} (n={})", sel.len()),
                );
            } else {
                println!(
                    "    ({rname} typical pairs: {} — not enough to check)",
                    sel.len()
                );
            }
        }
        let wb: Vec<f64> = res
            .iter()
            .zip(trips)
            .filter(|(r, trip)| ctx.like_for_like(r.as_ref(), trip))
            .filter_map(|(r, _)| r.as_ref())
            .filter(|r| r.best_min > 0.0)
            .map(|r| r.worst_min / r.best_min)
            .collect();
        if !wb.is_empty() {
            let ms = median(&wb);
            passed &= check(
                &format!(
                    "spread: median(worst/best) over the typical trips ≥ {}",
                    pyf(t.band_spread_min)
                ),
                ms >= t.band_spread_min,
                &format!("{ms:.3}"),
            );
        }
    }
    Ok(passed)
}

#[allow(dead_code)]
fn _unused(v: &Value) -> Option<f64> {
    num(v, "x").ok()
}
