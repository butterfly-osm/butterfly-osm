//! Gates — routing invariants, matrix/table, surface coverage, storm.
//!
//! `6.283` appears where the Python gate wrote it: the samples must be the
//! same numbers, so the literal stays (not `TAU`).
#![allow(clippy::approx_constant)]

use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::{Value, json};

use super::ctx::{
    Ctx, FIXTURES, ISO_POINTS, MAX_U32, SUBLINEAR_PLANS, check, check_errors, check_plan, f0, num,
    par_map, parse_matrix_plan, pct, skip,
};
use super::flight::{col_f64, col_u64, f64_table, rows};
use super::geom::{decode_polyline6, haversine_m, polyline_len_m, round_to, within_km};
use super::http::{GResult, is_no_route, pyf};
use super::pyrandom::PyRandom;

pub type GateResult = Result<bool, super::GateFail>;

/// /route with geometry + annotations → (duration_s, distance_m, polyline
/// length, Σann distance, Σann duration).
fn route_geometry_report(
    ctx: &Ctx,
    olon: f64,
    olat: f64,
    dlon: f64,
    dlat: f64,
    mode: &str,
) -> GResult<(f64, f64, Option<f64>, f64, f64)> {
    let d = ctx.route_json(
        olon,
        olat,
        dlon,
        dlat,
        mode,
        60,
        &[
            ("geometries", "polyline6".into()),
            ("annotations", "distance,duration".into()),
        ],
    )?;
    let geom = d.get("geometry").cloned().unwrap_or(Value::Null);
    let poly = geom
        .get("polyline")
        .or_else(|| geom.get("coordinates_polyline6"))
        .and_then(Value::as_str)
        .unwrap_or("");
    let ann = d.get("annotations").cloned().unwrap_or(Value::Null);
    let sum = |k: &str| -> f64 {
        ann.get(k)
            .and_then(Value::as_array)
            .map(|a| a.iter().filter_map(Value::as_f64).sum::<f64>() + 0.0)
            .unwrap_or(0.0)
    };
    Ok((
        num(&d, "duration_s")?,
        num(&d, "distance_m")?,
        if poly.is_empty() {
            None
        } else {
            Some(polyline_len_m(&decode_polyline6(poly)))
        },
        sum("distance"),
        sum("duration"),
    ))
}

pub fn gate_fixtures(ctx: &Ctx) -> GateResult {
    println!("== sentinel pairs (#502/#503) — invariant checks, no expected constants ==");
    let t = &ctx.t;
    let mut passed = true;
    let (lo_kmh, hi_kmh) = t.car_speed_kmh;
    for &(name, olon, olat, dlon, dlat) in FIXTURES {
        let (dur_s, dist_m, geom_m, ann_dist, ann_dur) =
            match route_geometry_report(ctx, olon, olat, dlon, dlat, "car") {
                Ok(r) => r,
                Err(e) => {
                    passed &= check(name, false, &format!("request failed: {e}"));
                    continue;
                }
            };
        let crow = haversine_m(olon, olat, dlon, dlat);
        let detour = dist_m / crow.max(1.0);
        let kmh = dist_m / dur_s.max(0.001) * 3.6;
        let ok_detour = detour <= t.sentinel_max_detour;
        let ok_speed = lo_kmh <= kmh && kmh <= hi_kmh;
        let ok_geom = geom_m.is_none_or(|g| (g - dist_m).abs() <= dist_m * t.geom_consistency_tol);
        let ok_ann = ann_dist == 0.0
            || ((ann_dist - dist_m).abs() <= dist_m * t.geom_consistency_tol
                && (ann_dur - dur_s).abs() <= dur_s * t.ann_duration_tol);
        let gtxt = geom_m
            .map(|g| format!("{}m", f0(g)))
            .unwrap_or_else(|| "n/a".into());
        passed &= check(
            name,
            ok_detour && ok_speed && ok_geom && ok_ann,
            &format!(
                "{}s/{}m detour×{detour:.2}(≤{}) {}km/h geom={gtxt} annΣ={}m/{}s",
                f0(dur_s),
                f0(dist_m),
                pyf(t.sentinel_max_detour),
                f0(kmh),
                f0(ann_dist),
                f0(ann_dur)
            ),
        );
    }
    Ok(passed)
}

pub fn gate_mode_coherence(ctx: &Ctx) -> GateResult {
    println!("== foot/bike geometry ≡ distance ≡ annotations + plausible speed (#522/#493) ==");
    let t = &ctx.t;
    let mut passed = true;
    for mode in ["foot", "bike"] {
        let (lo_kmh, hi_kmh) = if mode == "foot" {
            t.foot_speed_kmh
        } else {
            t.bike_speed_kmh
        };
        for &(name, olon, olat, dlon, dlat) in FIXTURES {
            let label = format!("{mode} {name}");
            let (dur_s, dist_m, geom_m, ann_dist, _) =
                match route_geometry_report(ctx, olon, olat, dlon, dlat, mode) {
                    Ok(r) => r,
                    Err(e) => {
                        passed &= check(&label, false, &format!("request failed: {e}"));
                        continue;
                    }
                };
            if dist_m <= 0.0 || dur_s <= 0.0 {
                passed &= check(
                    &label,
                    false,
                    &format!("degenerate {}m/{}s", pyf(dist_m), pyf(dur_s)),
                );
                continue;
            }
            let kmh = dist_m / dur_s * 3.6;
            let ok_speed = lo_kmh <= kmh && kmh <= hi_kmh;
            let ok_geom =
                geom_m.is_none_or(|g| (g - dist_m).abs() <= dist_m * t.geom_consistency_tol);
            let ok_ann =
                ann_dist == 0.0 || (ann_dist - dist_m).abs() <= dist_m * t.geom_consistency_tol;
            let gtxt = geom_m
                .map(|g| format!("{}m", f0(g)))
                .unwrap_or_else(|| "n/a".into());
            passed &= check(
                &label,
                ok_speed && ok_geom && ok_ann,
                &format!(
                    "{}s/{}m {kmh:.1}km/h (bound {}-{}) geom={gtxt} annΣ={}m",
                    f0(dur_s),
                    f0(dist_m),
                    f0(lo_kmh),
                    f0(hi_kmh),
                    f0(ann_dist)
                ),
            );
        }
    }
    Ok(passed)
}

enum Probe<T> {
    Ok(T),
    Unroutable,
    Error,
}

fn classify<T>(r: GResult<T>) -> Probe<T> {
    match r {
        Ok(v) => Probe::Ok(v),
        Err(e) if is_no_route(&e) => Probe::Unroutable,
        Err(_) => Probe::Error,
    }
}

fn py_tuple4(p: (f64, f64, f64, f64)) -> String {
    format!("({}, {}, {}, {})", pyf(p.0), pyf(p.1), pyf(p.2), pyf(p.3))
}

pub fn gate_symmetry(ctx: &Ctx) -> GateResult {
    let n_pairs = 150;
    println!("== symmetry invariant ({n_pairs} seeded random pairs) ==");
    let t = &ctx.t;
    let mut rng = PyRandom::new(99);
    let pairs: Vec<(f64, f64, f64, f64)> = (0..n_pairs)
        .map(|_| {
            let a = round_to(rng.uniform(3.0, 6.2), 5);
            let b = round_to(rng.uniform(49.6, 51.4), 5);
            let c = round_to(rng.uniform(3.0, 6.2), 5);
            let d = round_to(rng.uniform(49.6, 51.4), 5);
            (a, b, c, d)
        })
        .collect();
    let res = par_map(&pairs, 16, |&(a, b, c, d)| {
        classify(
            ctx.route(a, b, c, d, "car")
                .and_then(|f| ctx.route(c, d, a, b, "car").map(|r| (f.0, r.0))),
        )
    });
    let mut violations = Vec::new();
    let (mut tested, mut errors, mut unroutable) = (0usize, 0usize, 0usize);
    let mut worst = 1.0f64;
    for (p, r) in pairs.iter().zip(res) {
        let (f, rv) = match r {
            Probe::Error => {
                errors += 1;
                continue;
            }
            Probe::Unroutable => {
                unroutable += 1;
                continue;
            }
            Probe::Ok(v) => v,
        };
        if f < 60.0 {
            continue;
        }
        tested += 1;
        let ratio = f.max(rv) / f.min(rv).max(1.0);
        worst = worst.max(ratio);
        if ratio > t.symmetry_ratio_max {
            violations.push((ratio, *p));
        }
    }
    for v in violations.iter().take(5) {
        println!("    violation: ratio {:.2} @ {}", v.0, py_tuple4(v.1));
    }
    let mut passed = check(
        "fwd/rev symmetry",
        violations.len() <= t.symmetry_violations_max && tested >= 50,
        &format!(
            "{tested} pairs, {} >{}x, worst {worst:.2}",
            violations.len(),
            pyf(t.symmetry_ratio_max)
        ),
    );
    passed &= check_errors(t, "symmetry", errors, Some(unroutable));
    Ok(passed)
}

pub fn gate_route_table_agreement(ctx: &Ctx) -> GateResult {
    let (n_uniform, n_close) = (50usize, 150usize);
    println!(
        "== route ≡ table agreement: {n_uniform} uniform + {n_close} close (50-400 m) pairs =="
    );
    let t = &ctx.t;
    let tol = t.consistency_tolerance_s;
    let mut rng_u = PyRandom::new(7);
    let uniform: Vec<(f64, f64, f64, f64)> = (0..n_uniform)
        .map(|_| {
            let a = round_to(rng_u.uniform(3.5, 5.8), 5);
            let b = round_to(rng_u.uniform(50.2, 51.2), 5);
            let c = round_to(rng_u.uniform(3.5, 5.8), 5);
            let d = round_to(rng_u.uniform(50.2, 51.2), 5);
            (a, b, c, d)
        })
        .collect();
    let mut rng_c = PyRandom::new(123);
    let close: Vec<(f64, f64, f64, f64)> = (0..n_close)
        .map(|_| {
            let lon = rng_c.uniform(3.5, 5.8);
            let lat = rng_c.uniform(50.3, 51.2);
            let d = rng_c.uniform(0.0005, 0.004);
            let a = rng_c.uniform(0.0, 6.283);
            (
                round_to(lon, 6),
                round_to(lat, 6),
                round_to(lon + d * a.cos(), 6),
                round_to(lat + d * a.sin(), 6),
            )
        })
        .collect();
    let probe = |p: &(f64, f64, f64, f64)| -> Probe<(f64, Option<f64>)> {
        classify((|| {
            let (dur_r, _) = ctx.route(p.0, p.1, p.2, p.3, "car")?;
            let tab = ctx.table(
                &[[p.0, p.1]],
                &[[p.2, p.3]],
                "car",
                120,
                &[("annotations", json!("duration"))],
            )?;
            let cell = tab.pointer("/durations/0/0").and_then(Value::as_f64);
            Ok((dur_r, cell))
        })())
    };
    let res_u = par_map(&uniform, 16, probe);
    let res_c = par_map(&close, 16, probe);
    let measure = |res: &[Probe<(f64, Option<f64>)>]| {
        let (mut tested, mut errors, mut unroutable, mut mism, mut zeros) = (0, 0, 0, 0, 0);
        let mut worst = 0.0f64;
        for r in res {
            match r {
                Probe::Error => errors += 1,
                Probe::Unroutable => unroutable += 1,
                Probe::Ok((dur_r, Some(dur_t))) => {
                    tested += 1;
                    let delta = (dur_r - dur_t).abs();
                    worst = worst.max(delta);
                    if delta > tol {
                        mism += 1;
                    }
                    if (*dur_r < 1.0 && *dur_t > 10.0) || (*dur_t < 1.0 && *dur_r > 10.0) {
                        zeros += 1;
                    }
                }
                Probe::Ok((_, None)) => {}
            }
        }
        (tested, errors, unroutable, mism, zeros, worst)
    };
    let (t_u, e_u, u_u, m_u, z_u, w_u) = measure(&res_u);
    let (t_c, e_c, u_c, m_c, z_c, w_c) = measure(&res_c);
    let max_mism = t.close_pair_mismatch_max;
    let mut passed = check(
        "uniform pairs: route == table",
        w_u <= tol && z_u == 0 && t_u >= 8.max(n_uniform / 2),
        &format!(
            "{t_u} pairs, {z_u} zero-bugs, {m_u} >{}s, worst delta {w_u:.1}s (max {}s)",
            pyf(tol),
            pyf(tol)
        ),
    );
    passed &= check(
        "close pairs: route == table",
        z_c == 0 && m_c <= max_mism && t_c >= 80,
        &format!(
            "{t_c} pairs, {z_c} zero-bugs, {m_c} >{}s (max {max_mism}), worst {w_c:.1}s",
            pyf(tol)
        ),
    );
    passed &= check_errors(t, "route≡table", e_u + e_c, Some(u_u + u_c));
    Ok(passed)
}

fn city_pairs() -> Vec<(String, f64, f64, f64, f64)> {
    let mut out = Vec::new();
    let mut i = 0;
    while i + 1 < ISO_POINTS.len() {
        let a = ISO_POINTS[i];
        let b = ISO_POINTS[i + 1];
        out.push((format!("{}→{}", a.0, b.0), a.1, a.2, b.1, b.2));
        i += 2;
    }
    out
}

pub fn gate_one_way_routable(ctx: &Ctx) -> GateResult {
    println!("== car one-way routability: no directional 404 (#197) ==");
    let pairs = city_pairs();
    let res = par_map(&pairs, 16, |p| {
        let fwd = ctx.route(p.1, p.2, p.3, p.4, "car").is_ok();
        let rev = ctx.route(p.3, p.4, p.1, p.2, "car").is_ok();
        (fwd, rev)
    });
    let fails: Vec<String> = pairs
        .iter()
        .zip(res)
        .filter(|(_, (f, r))| !(*f && *r))
        .map(|(p, (f, r))| format!("{} (fwd={} rev={})", p.0, pybool(f), pybool(r)))
        .collect();
    let n_ok = pairs.len() - fails.len();
    for f in fails.iter().take(5) {
        println!("    directional gap: {f}");
    }
    Ok(check(
        "both directions route",
        n_ok == pairs.len(),
        &format!("{n_ok}/{} pairs route both ways", pairs.len()),
    ))
}

fn pybool(b: bool) -> &'static str {
    if b { "True" } else { "False" }
}

pub fn gate_graph_holes(ctx: &Ctx) -> GateResult {
    println!("== car-vs-foot detour parity: graph holes (#503/#478) ==");
    let t = &ctx.t;
    let mut rng = PyRandom::new(478);
    let pairs: Vec<(f64, f64, f64, f64)> = (0..60)
        .map(|_| {
            let lon = rng.uniform(3.5, 5.8);
            let lat = rng.uniform(50.3, 51.2);
            let d = rng.uniform(0.01, 0.05);
            let a = rng.uniform(0.0, 6.283);
            (lon, lat, lon + d * a.cos(), lat + d * a.sin())
        })
        .collect();
    let res = par_map(&pairs, 16, |p| {
        classify(
            ctx.route(p.0, p.1, p.2, p.3, "car")
                .and_then(|c| ctx.route(p.0, p.1, p.2, p.3, "foot").map(|f| (c.1, f.1))),
        )
    });
    let (mut tested, mut errors, mut unroutable) = (0usize, 0usize, 0usize);
    let mut holes = Vec::new();
    let mut worst = 0.0f64;
    for (p, r) in pairs.iter().zip(res) {
        let (cdist, fdist) = match r {
            Probe::Error => {
                errors += 1;
                continue;
            }
            Probe::Unroutable => {
                unroutable += 1;
                continue;
            }
            Probe::Ok(v) => v,
        };
        if fdist <= 1.0 {
            continue;
        }
        tested += 1;
        let ratio = cdist / fdist;
        worst = worst.max(ratio);
        if ratio > t.car_foot_detour_max {
            holes.push((round_to(p.0, 4), round_to(p.1, 4), round_to(ratio, 1)));
        }
    }
    for h in holes.iter().take(5) {
        println!(
            "    car/foot hole: ({}, {}, {})",
            pyf(h.0),
            pyf(h.1),
            pyf(h.2)
        );
    }
    let mut passed = check(
        &format!("car detour ≤ {}× foot", f0(t.car_foot_detour_max)),
        holes.len() <= t.car_foot_holes_max,
        &format!("{} holes of {tested} pairs, worst ×{worst:.1}", holes.len()),
    );
    passed &= check_errors(t, "graph holes", errors, Some(unroutable));
    Ok(passed)
}

const CORRIDORS: &[(&str, f64, f64, f64, f64)] = &[
    ("Bxl→Antwerp (A1/E19)", 4.3517, 50.8503, 4.4025, 51.2194),
    ("Bxl→Liège (E40)", 4.3517, 50.8503, 5.5671, 50.6326),
    ("Bxl→Arlon (E411)", 4.3517, 50.8503, 5.8109, 49.6833),
];

pub fn gate_motorway_speed_floor(ctx: &Ctx) -> GateResult {
    println!("== motorway corridor speed floor (#450) ==");
    let floor = ctx.t.motorway_floor_kmh;
    let mut passed = true;
    for &(name, olon, olat, dlon, dlat) in CORRIDORS {
        match ctx.route(olon, olat, dlon, dlat, "car") {
            Ok((dur, dist)) => {
                let kmh = dist / dur.max(0.001) * 3.6;
                passed &= check(
                    name,
                    kmh >= floor,
                    &format!("{} km/h (floor {})", f0(kmh), f0(floor)),
                );
            }
            Err(e) => passed &= check(name, false, &format!("route failed: {e}")),
        }
    }
    Ok(passed)
}

/// Share of a /route's LENGTH on edges whose `classes` annotation carries `cls`.
pub fn class_share(d: &Value, cls: &str) -> Option<f64> {
    let ann = d.get("annotations")?;
    let dist = ann.get("distance")?.as_array()?;
    let classes = ann.get("classes")?.as_array()?;
    if dist.len() != classes.len() {
        return None;
    }
    let total: f64 = dist.iter().filter_map(Value::as_f64).sum::<f64>() + 0.0;
    if total <= 0.0 {
        return None;
    }
    let on: f64 = dist
        .iter()
        .zip(classes)
        .filter(|(_, c)| c.as_str().is_some_and(|s| s.split(',').any(|x| x == cls)))
        .filter_map(|(x, _)| x.as_f64())
        .sum::<f64>()
        + 0.0;
    Some(on / total)
}

pub fn gate_exclude_motorway(ctx: &Ctx) -> GateResult {
    println!("== exclude=motorway is strict (#606) ==");
    let t = &ctx.t;
    let corridors: Vec<(&str, f64, f64, f64, f64)> = CORRIDORS
        .iter()
        .copied()
        .chain(std::iter::once((
            "Mechelen→Antwerp (E19)",
            4.4800,
            51.0259,
            4.4025,
            51.2194,
        )))
        .collect();
    let share_min = t.exclude_corridor_class_share_min;
    let cell_tol = t.matrix_cell_tol;
    let mut passed = true;
    for (name, olon, olat, dlon, dlat) in corridors {
        let ann = "distance,duration,classes".to_string();
        let r = (|| -> GResult<(Value, Value, Value, Option<f64>)> {
            let plain = ctx.route_json(
                olon,
                olat,
                dlon,
                dlat,
                "car",
                600,
                &[("annotations", ann.clone())],
            )?;
            let excl = ctx.route_json(
                olon,
                olat,
                dlon,
                dlat,
                "car",
                600,
                &[("annotations", ann.clone()), ("exclude", "motorway".into())],
            )?;
            let more = ctx.route_json(
                olon,
                olat,
                dlon,
                dlat,
                "car",
                600,
                &[
                    ("annotations", ann.clone()),
                    ("exclude", "motorway,toll,ferry".into()),
                ],
            )?;
            let cell = ctx
                .table(
                    &[[olon, olat]],
                    &[[dlon, dlat]],
                    "car",
                    600,
                    &[
                        ("exclude", json!("motorway")),
                        ("annotations", json!("duration")),
                    ],
                )?
                .pointer("/durations/0/0")
                .and_then(Value::as_f64);
            Ok((plain, excl, more, cell))
        })();
        let (plain, excl, more, cell) = match r {
            Ok(v) => v,
            Err(e) => {
                passed &= check(name, false, &format!("request failed: {e}"));
                continue;
            }
        };
        let d0 = num(&plain, "duration_s").unwrap_or(f64::NAN);
        let d1 = num(&excl, "duration_s").unwrap_or(f64::NAN);
        let d2 = num(&more, "duration_s").unwrap_or(f64::NAN);
        passed &= check(
            &format!("{name}: a restriction is never faster"),
            d1 >= d0,
            &format!(
                "{}s -> {}s ({:+.1}%)",
                f0(d0),
                f0(d1),
                100.0 * (d1 / d0 - 1.0)
            ),
        );
        passed &= check(
            &format!("{name}: the mask is monotone"),
            d2 >= d1,
            &format!("motorway {}s -> +toll,ferry {}s", f0(d1), f0(d2)),
        );
        let ok_cell = cell.is_some_and(|c| (c - d1).abs() <= (d1 * cell_tol).max(1.0));
        passed &= check(
            &format!("{name}: /table agrees under the same mask"),
            ok_cell,
            &format!(
                "/route {}s vs /table {}s",
                f0(d1),
                cell.map(f0).unwrap_or_else(|| "None".into())
            ),
        );
        let s0 = class_share(&plain, "motorway");
        let s1 = class_share(&excl, "motorway");
        let s2 = ["motorway", "toll", "ferry"]
            .iter()
            .map(|c| class_share(&more, c))
            .collect::<Option<Vec<f64>>>()
            .map(|v| v.into_iter().fold(f64::NEG_INFINITY, f64::max));
        let (Some(s0), Some(s1), Some(s2)) = (s0, s1, s2) else {
            passed &= check(
                &format!("{name}: class annotations"),
                false,
                "no classes/distance annotations returned",
            );
            continue;
        };
        passed &= check(
            &format!("{name}: the corridor IS motorway"),
            s0 >= share_min,
            &format!(
                "{}% of its length on motorway-class edges (>= {}%)",
                f0(100.0 * s0),
                f0(100.0 * share_min)
            ),
        );
        passed &= check(
            &format!("{name}: the excluded route carries NO motorway edge"),
            s1 == 0.0,
            &format!(
                "{:.2}% of its length on motorway-class edges (unrestricted: {}%)",
                100.0 * s1,
                f0(100.0 * s0)
            ),
        );
        passed &= check(
            &format!("{name}: under motorway,toll,ferry no edge of any excluded class"),
            s2 == 0.0,
            &format!("max class share {:.2}%", 100.0 * s2),
        );
    }
    Ok(passed)
}

// ---------------------------------------------------------------------------
// Ticket invariants — the map IS the check
// ---------------------------------------------------------------------------
const TICKET_GATES: &[(&str, &[&str])] = &[
    (
        "#495/#497",
        &["isochrone_upper_bound", "isochrone_topology"],
    ),
    ("#535", &["isochrone_topology"]),
    ("#536", &["catchment_containment"]),
    ("#541", &[]),
    (
        "#542",
        &["isochrone_topology", "isochrone_reach_truth", "graph_holes"],
    ),
    ("#543", &["bands"]),
    ("#545", &["route_choice"]),
    ("#605", &["route_batch_agrees_with_route"]),
    ("#606", &["exclude_motorway"]),
    ("#613", &["isochrone_transports_agree"]),
];

fn ticket_note(ticket: &str) -> &'static str {
    match ticket {
        "#535" => {
            "isochrone off-centre / pin not covered: the pin is in (or within pin_near_ring_m of) the ONE polygon from a pedestrian centre"
        }
        "#536" => "square lasso, missing clients: the road hull is the threshold isochrone",
        "#541" => {
            "clip to the border is PRESENTATION — done consumer-side; the engine stays generic, so there is deliberately no engine gate"
        }
        "#542" => {
            "islands / confetti: one simple polygon, faithful to the engine's reach, no graph holes"
        }
        "#543" => {
            "isochrones too big vs a traffic-aware reference: typical/best/worst levels, like-for-like, never more than 2 % fast"
        }
        "#605" => {
            "batch route \u{2260} /route on short pairs: the two surfaces return the same duration and distance, on a sample that still hits the shared-snap case"
        }
        "#545" => {
            "route CHOICE has no check: durations are judged like-for-like (which drops the\n            divergent pairs) and route length only on the legacy corridor set, so a calibration\n            can collapse choice onto the motorways with every gate green"
        }
        "#606" => {
            "exclude=motorway close to a no-op: a restriction is never faster, the mask is monotone, /route == /table under it, and the corridor is left"
        }
        "#613" => {
            "the machine isochrone could not exclude anything: both transports serve the same bytes under an exclusion, and the exclusion actually moves the polygon"
        }
        "#495/#497" => "size / foot origin: max reach <= v_max x time, snapped origin contained",
        _ => "",
    }
}

fn matrix_plan_gates_run_before_the_matrix_heavy_ones(order: &[&str]) -> bool {
    let heavy = ["isodistance_truth"];
    let pos = |n: &str| order.iter().position(|x| *x == n);
    let plan_gate = pos("lopsided_matrix");
    let mut ok = true;
    for name in heavy {
        let (Some(p), Some(pg)) = (pos(name), plan_gate) else {
            continue;
        };
        ok &= check(
            &format!("{name} runs after lopsided_matrix"),
            p > pg,
            "the plan assertions read a MEASURED router; heavy matrix probes must not teach it first",
        );
    }
    ok
}

pub fn gate_ticket_invariants(order: &[&str]) -> GateResult {
    println!("== ticket invariants: every user ticket delegates to a REGISTERED gate ==");
    let registered: BTreeSet<&str> = order.iter().copied().collect();
    let mut passed = matrix_plan_gates_run_before_the_matrix_heavy_ones(order);
    // `sorted(TICKET_GATES)`: Python sorts the ticket strings lexically.
    let mut tickets: Vec<&(&str, &[&str])> = TICKET_GATES.iter().collect();
    tickets.sort_by(|a, b| a.0.cmp(b.0));
    for (ticket, gates) in tickets {
        let note = ticket_note(ticket);
        if gates.is_empty() {
            println!("  [SKIP] {ticket}: {note}");
            continue;
        }
        let missing: Vec<&str> = gates
            .iter()
            .copied()
            .filter(|g| !registered.contains(g))
            .collect();
        let detail = if missing.is_empty() {
            note.to_string()
        } else {
            format!(
                "NOT REGISTERED: [{}] — {note}",
                missing
                    .iter()
                    .map(|m| format!("'{m}'"))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        passed &= check(
            &format!("{ticket} -> {}", gates.join(", ")),
            missing.is_empty(),
            &detail,
        );
    }
    Ok(passed)
}

// ---------------------------------------------------------------------------
// Matrix / table
// ---------------------------------------------------------------------------
pub fn gate_lopsided(ctx: &Ctx) -> GateResult {
    println!("== lopsided matrix: the SERVED plan + route==table (#526/#527/#594) ==");
    let t = &ctx.t;
    let mut rng = PyRandom::new(31);
    let origin = (4.3517, 50.8503);
    let dests: Vec<[f64; 2]> = (0..800)
        .map(|_| {
            let a = origin.0 + rng.uniform(-0.25, 0.25);
            let b = origin.1 + rng.uniform(-0.15, 0.15);
            [a, b]
        })
        .collect();
    let timed = |dsts: &[[f64; 2]], extra: &[(&str, Value)]| -> GResult<(Value, String, f64)> {
        let t0 = Instant::now();
        let (r, plan) = ctx.table_with_plan(&[[origin.0, origin.1]], dsts, "foot", 300, extra)?;
        Ok((r, plan, t0.elapsed().as_secs_f64()))
    };
    timed(&dests[..50], &[])?;
    let (big, plan_1xn, tb) = timed(&dests, &[])?;
    let (_small, _p, ts) = timed(&dests[..50], &[])?;
    let mut passed = check_plan("1x800 lopsided", &plan_1xn, SUBLINEAR_PLANS);
    println!(
        "    (wall clock, informational: 1x800 {tb:.2}s vs 1x50 {ts:.2}s = x{:.1})",
        tb / ts.max(1e-3)
    );
    let rev_origins: Vec<[f64; 2]> = (0..2000)
        .map(|_| {
            let a = origin.0 + rng.uniform(-0.25, 0.25);
            let b = origin.1 + rng.uniform(-0.15, 0.15);
            [a, b]
        })
        .collect();
    let t0 = Instant::now();
    let (_rev, plan_nx1) = ctx.table_with_plan(&rev_origins, &[dests[0]], "foot", 300, &[])?;
    passed &= check_plan("2000x1 reverse-lopsided", &plan_nx1, SUBLINEAR_PLANS);
    println!(
        "    (wall clock, informational: 2000x1 {:.2}s)",
        t0.elapsed().as_secs_f64()
    );
    let square: Vec<[f64; 2]> = (0..40)
        .map(|_| {
            let a = origin.0 + rng.uniform(-0.1, 0.1);
            let b = origin.1 + rng.uniform(-0.06, 0.06);
            [a, b]
        })
        .collect();
    let (_sq, plan_sq) = ctx.table_with_plan(&square, &square, "foot", 300, &[])?;
    passed &= check_plan("40x40 balanced", &plan_sq, &["bucket"]);

    let compare =
        |tab: &Value, idxs: &[usize], channel: &str, relative: bool| -> (usize, usize, f64) {
            let ds: Vec<f64> = par_map(idxs, 16, |&i| {
                let v = tab
                    .pointer(&format!("/{channel}/0/{i}"))
                    .and_then(Value::as_f64)?;
                tab.pointer(&format!("/durations/0/{i}"))
                    .and_then(Value::as_f64)?;
                let (dur_r, dist_r) = ctx
                    .route(origin.0, origin.1, dests[i][0], dests[i][1], "foot")
                    .ok()?;
                Some(if relative {
                    (v - dist_r).abs() / dist_r.max(1.0)
                } else {
                    (v - dur_r).abs()
                })
            })
            .into_iter()
            .flatten()
            .collect();
            let tol = if relative {
                t.matrix_cell_tol
            } else {
                t.consistency_tolerance_s
            };
            (
                ds.len(),
                ds.iter().filter(|&&d| d > tol).count(),
                ds.iter().cloned().fold(0.0, f64::max),
            )
        };
    let idx1 = rng.sample_range(800, 25);
    let (checked, mism, worst) = compare(&big, &idx1, "durations", false);
    passed &= check(
        "lopsided route==table",
        mism == 0 && checked >= 15,
        &format!("{checked} cells sampled, {mism} mismatches, worst {worst:.1}s"),
    );
    let (dd, plan_2ch, _) = timed(&dests, &[("annotations", json!("duration,distance"))])?;
    passed &= check_plan("1x800 lopsided, 2-channel", &plan_2ch, SUBLINEAR_PLANS);
    let idx2 = rng.sample_range(800, 25);
    let (dchecked, dmis, dworst) = compare(&dd, &idx2, "distances", true);
    passed &= check(
        "lopsided 2-channel distance==route",
        dmis == 0 && dchecked >= 15,
        &format!(
            "{dchecked} cells, {dmis} mismatches, worst {:.2}%",
            dworst * 100.0
        ),
    );
    if !ctx.flight {
        skip("Flight matrix plan: --no-flight");
    } else {
        let dec = ctx.do_get(
            "matrix",
            "foot",
            &json!({"origins": [[origin.0, origin.1]], "destinations": dests}),
        )?;
        let plan = parse_matrix_plan(
            dec.meta
                .as_ref()
                .and_then(|m| m.get("plan"))
                .and_then(Value::as_str),
        );
        passed &= check_plan("Flight matrix 1x800 lopsided", &plan, SUBLINEAR_PLANS);
    }
    Ok(passed)
}

fn distance_channel_vs_route(
    ctx: &Ctx,
    mode: &str,
) -> GResult<Option<(usize, usize, f64, String)>> {
    let mut rng = PyRandom::new(528);
    let o = (4.3517, 50.8503);
    let dests: Vec<[f64; 2]> = (0..200)
        .map(|_| {
            let a = o.0 + rng.uniform(-0.3, 0.3);
            let b = o.1 + rng.uniform(-0.2, 0.2);
            [a, b]
        })
        .collect();
    let (tab, plan) = match ctx.table_with_plan(
        &[[o.0, o.1]],
        &dests,
        mode,
        200,
        &[("annotations", json!("duration,distance"))],
    ) {
        Ok(v) => v,
        Err(e) if is_no_route(&e) => return Ok(None),
        Err(e) => return Err(e),
    };
    let cell_tol = ctx.t.matrix_cell_tol;
    let (mut mism, mut checked) = (0usize, 0usize);
    let mut worst = 0.0f64;
    for i in rng.sample_range(200, 30) {
        let Some(m) = tab
            .pointer(&format!("/distances/0/{i}"))
            .and_then(Value::as_f64)
        else {
            continue;
        };
        let Ok((_, dist_r)) = ctx.route(o.0, o.1, dests[i][0], dests[i][1], mode) else {
            continue;
        };
        if dist_r < 1.0 {
            continue;
        }
        checked += 1;
        let rel = (m - dist_r).abs() / dist_r;
        worst = worst.max(rel);
        if rel > cell_tol {
            mism += 1;
        }
    }
    Ok(Some((checked, mism, worst, plan)))
}

pub fn gate_recustomized_distance(ctx: &Ctx) -> GateResult {
    println!("== recustomized-mode 2-channel distance==route (#528/#529) ==");
    let mut passed = true;
    for mode in ["car", "car_nodir"] {
        match distance_channel_vs_route(ctx, mode)? {
            None => skip(&format!(
                "{mode} 2-channel distance==route: mode not served"
            )),
            Some((checked, mism, worst, plan)) => {
                passed &= check(
                    &format!("{mode} 2-channel distance==route"),
                    mism == 0 && checked >= 20,
                    &format!(
                        "{checked} cells, {mism} mismatches, worst {:.2}%, plan {plan}",
                        worst * 100.0
                    ),
                );
            }
        }
    }
    Ok(passed)
}

fn pylist(v: &[usize]) -> String {
    format!(
        "[{}]",
        v.iter()
            .map(|x| x.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    )
}

pub fn gate_radius_prune(ctx: &Ctx) -> GateResult {
    println!("== radius_km prunes far cells (scalar + per-origin, #531) ==");
    let o = [4.35, 50.85];
    let dests = [
        [o[0] + 0.0157, o[1]],
        [o[0] + 0.0314, o[1]],
        [o[0] + 0.0471, o[1]],
    ];
    let durations = |origins: &[[f64; 2]], radius: Value| -> GResult<Value> {
        Ok(ctx
            .table(
                origins,
                &dests,
                "foot",
                120,
                &[("annotations", json!("duration")), ("radius_km", radius)],
            )?
            .get("durations")
            .cloned()
            .unwrap_or(Value::Null))
    };
    let kept = |row: &Value| -> Vec<usize> {
        row.as_array()
            .map(|r| {
                r.iter()
                    .enumerate()
                    .filter(|(_, v)| !v.is_null())
                    .map(|(i, _)| i)
                    .collect()
            })
            .unwrap_or_default()
    };
    let scalar = kept(&durations(&[o], json!(1.5))?[0]);
    let ok_scalar = check(
        "scalar radius_km=1.5 prunes",
        scalar == vec![0],
        &format!(
            "kept {} (want [0] — the ~2.2/3.3 km targets pruned)",
            pylist(&scalar)
        ),
    );
    let per = durations(&[o, o, o], json!([1.5, 3.0, 0]))?;
    let rows: Vec<Vec<usize>> = (0..3).map(|i| kept(&per[i])).collect();
    let want = vec![vec![0], vec![0, 1], vec![0, 1, 2]];
    let ok_per = check(
        "per-origin radius_km prunes each origin",
        rows == want,
        &format!(
            "kept [{}] (want [[0],[0,1],[0,1,2]])",
            rows.iter()
                .map(|r| pylist(r))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    );
    Ok(ok_scalar && ok_per)
}

pub fn gate_radius_exactness(ctx: &Ctx) -> GateResult {
    println!("== /table radius_km: pruned == unpruned inside the radius (#602) ==");
    let mut rng = PyRandom::new(602);
    let origins: Vec<[f64; 2]> = (0..60)
        .map(|_| {
            let a = round_to(rng.uniform(3.4, 5.4), 6);
            let b = round_to(rng.uniform(50.5, 51.2), 6);
            [a, b]
        })
        .collect();
    let dests: Vec<[f64; 2]> = (0..220)
        .map(|_| {
            let a = round_to(rng.uniform(3.4, 5.4), 6);
            let b = round_to(rng.uniform(50.5, 51.2), 6);
            [a, b]
        })
        .collect();
    let r_km = 20.0;
    let mut passed = true;
    for mode in ["car", "foot"] {
        let run = |extra: &[(&str, Value)]| -> GResult<Value> {
            let mut e: Vec<(&str, Value)> = vec![("annotations", json!("duration,distance"))];
            e.extend(extra.iter().cloned());
            ctx.table(&origins, &dests, mode, 900, &e)
        };
        let full = run(&[])?;
        let pruned = run(&[("radius_km", json!(r_km))])?;
        let cell =
            |v: &Value, k: &str, i: usize, j: usize| v.pointer(&format!("/{k}/{i}/{j}")).cloned();
        let has_dist = full.get("distances").is_some_and(|v| !v.is_null())
            && pruned.get("distances").is_some_and(|v| !v.is_null());
        let (mut kept, mut mism, mut dropped) = (0usize, 0usize, 0usize);
        for i in 0..origins.len() {
            for j in 0..dests.len() {
                let pd = cell(&pruned, "durations", i, j);
                if pd.as_ref().is_none_or(Value::is_null) {
                    continue;
                }
                kept += 1;
                let fd = cell(&full, "durations", i, j);
                if fd.as_ref().is_none_or(Value::is_null) {
                    dropped += 1;
                } else if fd != pd
                    || (has_dist
                        && cell(&full, "distances", i, j) != cell(&pruned, "distances", i, j))
                {
                    mism += 1;
                }
            }
        }
        let loc = |v: &Value, k: &str, i: usize| -> Option<(f64, f64)> {
            let a = v.pointer(&format!("/{k}/{i}/location"))?.as_array()?;
            Some((a.first()?.as_f64()?, a.get(1)?.as_f64()?))
        };
        let mut lost = 0usize;
        for i in 0..origins.len() {
            for j in 0..dests.len() {
                let fd = cell(&full, "durations", i, j);
                let pd = cell(&pruned, "durations", i, j);
                if fd.as_ref().is_some_and(|v| !v.is_null())
                    && pd.as_ref().is_none_or(Value::is_null)
                    && let (Some(so), Some(sd)) =
                        (loc(&pruned, "origins", i), loc(&pruned, "destinations", j))
                    && within_km(so, sd, r_km)
                {
                    lost += 1;
                }
            }
        }
        let total = origins.len() * dests.len();
        passed &= check(
            &format!("{mode}: radius actually prunes"),
            0 < kept && kept < total,
            &format!("{kept}/{total} cells kept"),
        );
        passed &= check(
            &format!("{mode}: every kept cell identical to the unpruned run"),
            mism == 0 && dropped == 0,
            &format!("{mism} value mismatches, {dropped} cells the unpruned run did not have"),
        );
        passed &= check(
            &format!("{mode}: no in-radius cell lost to the compute bound"),
            lost == 0,
            &format!(
                "{lost} cells inside {} km present unpruned, missing pruned (the #602 rescue failed)",
                pyf(r_km)
            ),
        );
    }
    Ok(passed)
}

// ---------------------------------------------------------------------------
// Transit feeds, endpoint smoke, storm
// ---------------------------------------------------------------------------
const BELGIUM_TRANSIT_FEEDS: &[&str] = &["sncb", "delijn", "tec", "stib"];

fn pystr_list(v: &[String]) -> String {
    format!(
        "[{}]",
        v.iter()
            .map(|s| format!("'{s}'"))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

pub fn gate_transit_feeds(ctx: &Ctx) -> GateResult {
    println!("== transit feeds: every declared operator is in the loaded timetable (#628) ==");
    let h = ctx.http.json(&ctx.url("/health"), 30)?;
    let status = h.get("transit").and_then(Value::as_str);
    if status != Some("loaded") {
        let shown = status
            .map(|s| format!("'{s}'"))
            .unwrap_or_else(|| "None".into());
        skip(&format!(
            "transit is {shown} on this deployment — nothing to hold to"
        ));
        return Ok(true);
    }
    let feeds = h.get("transit_feeds").cloned().unwrap_or(json!({}));
    let strs = |k: &str| -> Vec<String> {
        feeds
            .get(k)
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default()
    };
    let loaded: BTreeSet<String> = strs("loaded").into_iter().collect();
    let missing = strs("missing");
    let excluded: BTreeSet<String> = feeds
        .get("excluded")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|e| e.get("id").and_then(Value::as_str).map(String::from))
                .collect()
        })
        .unwrap_or_default();
    let mut passed = check(
        "no configured feed is missing from the loaded timetable",
        missing.is_empty(),
        &format!("missing={}", pystr_list(&missing)),
    );
    let want: BTreeSet<String> = BELGIUM_TRANSIT_FEEDS
        .iter()
        .map(|s| s.to_string())
        .filter(|s| !excluded.contains(s))
        .collect();
    let sorted = |s: &BTreeSet<String>| pystr_list(&s.iter().cloned().collect::<Vec<_>>());
    passed &= check(
        "every declared Belgian operator is loaded",
        want.is_subset(&loaded),
        &format!(
            "loaded={} excluded={} want={}",
            sorted(&loaded),
            sorted(&excluded),
            sorted(&want)
        ),
    );
    Ok(passed)
}

type ProbeSpec = (&'static str, String, Option<Value>);

fn rest_probes() -> HashMap<&'static str, ProbeSpec> {
    let o = (4.3517, 50.8503);
    let d = (4.4025, 51.2194);
    let trace = json!([
        [4.3517, 50.8503],
        [4.3537, 50.8513],
        [4.3557, 50.8523],
        [4.3577, 50.8533]
    ]);
    HashMap::from([
        ("/health", ("GET", "/health".to_string(), None)),
        ("/version", ("GET", "/version".to_string(), None)),
        ("/regions", ("GET", "/regions".to_string(), None)),
        (
            "/route",
            (
                "GET",
                format!(
                    "/route?origin_lon={}&origin_lat={}&destination_lon={}&destination_lat={}&mode=car",
                    pyf(o.0),
                    pyf(o.1),
                    pyf(d.0),
                    pyf(d.1)
                ),
                None,
            ),
        ),
        (
            "/nearest",
            (
                "GET",
                format!("/nearest?lon={}&lat={}&mode=car", pyf(o.0), pyf(o.1)),
                None,
            ),
        ),
        (
            "/isochrone",
            (
                "GET",
                format!(
                    "/isochrone?lon={}&lat={}&time_s=300&mode=car",
                    pyf(o.0),
                    pyf(o.1)
                ),
                None,
            ),
        ),
        (
            "/height",
            (
                "GET",
                format!(
                    "/height?coordinates={},{}|{},{}",
                    pyf(o.0),
                    pyf(o.1),
                    pyf(d.0),
                    pyf(d.1)
                ),
                None,
            ),
        ),
        (
            "/transit",
            (
                "GET",
                format!(
                    "/transit?origin_lon={}&origin_lat={}&destination_lon={}&destination_lat={}",
                    pyf(o.0),
                    pyf(o.1),
                    pyf(d.0),
                    pyf(d.1)
                ),
                None,
            ),
        ),
        (
            "/table",
            (
                "POST",
                "/table".to_string(),
                Some(
                    json!({"origins": [[o.0, o.1], [d.0, d.1]], "destinations": [[o.0, o.1], [d.0, d.1]], "mode": "car", "annotations": "duration,distance"}),
                ),
            ),
        ),
        (
            "/trip",
            (
                "POST",
                "/trip".to_string(),
                Some(
                    json!({"points": [[o.0, o.1], [d.0, d.1], [4.35, 50.90]], "mode": "car", "round_trip": true}),
                ),
            ),
        ),
        (
            "/match",
            (
                "POST",
                "/match".to_string(),
                Some(json!({"points": trace, "mode": "car", "geometry": "polyline6"})),
            ),
        ),
        (
            "/catchment",
            (
                "POST",
                "/catchment".to_string(),
                Some(
                    json!({"mode": "car", "hull_shape": "road", "percentiles": [50], "remove_outliers": false, "stores": [{"id": "s1", "lon": o.0, "lat": o.1}], "clients": [{"lon": 4.36, "lat": 50.86}, {"lon": 4.34, "lat": 50.84}, {"lon": 4.40, "lat": 50.88}]}),
                ),
            ),
        ),
    ])
}

const REST_INPUTLESS_PATHS: &[&str] = &["/health", "/version", "/regions"];

fn rest_invalid_probes() -> HashMap<&'static str, ProbeSpec> {
    let bad = (999.0, 50.8503);
    let d = (4.4025, 51.2194);
    HashMap::from([
        (
            "/route",
            (
                "GET",
                format!(
                    "/route?origin_lon={}&origin_lat={}&destination_lon={}&destination_lat={}&mode=car",
                    pyf(bad.0),
                    pyf(bad.1),
                    pyf(d.0),
                    pyf(d.1)
                ),
                None,
            ),
        ),
        (
            "/nearest",
            (
                "GET",
                format!("/nearest?lon={}&lat={}&mode=car", pyf(bad.0), pyf(bad.1)),
                None,
            ),
        ),
        (
            "/isochrone",
            (
                "GET",
                format!(
                    "/isochrone?lon={}&lat={}&time_s=300&mode=car",
                    pyf(bad.0),
                    pyf(bad.1)
                ),
                None,
            ),
        ),
        (
            "/height",
            (
                "GET",
                format!("/height?coordinates={},{}", pyf(bad.0), pyf(bad.1)),
                None,
            ),
        ),
        (
            "/transit",
            (
                "GET",
                format!(
                    "/transit?origin_lon={}&origin_lat={}&destination_lon={}&destination_lat={}",
                    pyf(bad.0),
                    pyf(bad.1),
                    pyf(d.0),
                    pyf(d.1)
                ),
                None,
            ),
        ),
        (
            "/table",
            (
                "POST",
                "/table".to_string(),
                Some(
                    json!({"origins": [[bad.0, bad.1]], "destinations": [[d.0, d.1]], "mode": "car", "annotations": "duration"}),
                ),
            ),
        ),
        (
            "/trip",
            (
                "POST",
                "/trip".to_string(),
                Some(json!({"points": [[bad.0, bad.1], [d.0, d.1]], "mode": "car"})),
            ),
        ),
        (
            "/match",
            (
                "POST",
                "/match".to_string(),
                Some(json!({"points": [[bad.0, bad.1], [d.0, d.1]], "mode": "car"})),
            ),
        ),
        (
            "/catchment",
            (
                "POST",
                "/catchment".to_string(),
                Some(
                    json!({"mode": "car", "hull_shape": "road", "percentiles": [50], "remove_outliers": false, "stores": [{"id": "s1", "lon": bad.0, "lat": bad.1}], "clients": [{"lon": d.0, "lat": d.1}]}),
                ),
            ),
        ),
    ])
}

fn rest_probe_skip(path: &str, status: u16) -> Option<&'static str> {
    match (path, status) {
        ("/height", 404) => {
            Some("not mounted — <data>/srtm/ absent; lean containers 404 by design")
        }
        ("/transit", 503) => Some("transit subsystem not loaded (no transit/ directory)"),
        ("/transit", 404) => Some("no journey for the probe pair — a valid documented answer"),
        _ => None,
    }
}

fn pyrepr_bytes(b: &[u8]) -> String {
    // Python's bytes repr for a short prefix — printable ASCII as is, the
    // rest as \xNN; enough for an error body excerpt.
    let mut s = String::from("b'");
    for &c in b {
        match c {
            b'\'' => s.push_str("\\'"),
            b'\\' => s.push_str("\\\\"),
            b'\n' => s.push_str("\\n"),
            b'\r' => s.push_str("\\r"),
            b'\t' => s.push_str("\\t"),
            0x20..=0x7e => s.push(c as char),
            _ => s.push_str(&format!("\\x{c:02x}")),
        }
    }
    s.push('\'');
    s
}

pub fn gate_all_endpoints_smoke(ctx: &Ctx) -> GateResult {
    println!("== all-endpoints smoke: every OpenAPI-documented REST path + every Flight action ==");
    let mut passed = true;
    let o = (4.3517, 50.8503);
    let d = (4.4025, 51.2194);
    let doc = match ctx.http.json(&ctx.url("/api-docs/openapi.json"), 60) {
        Ok(d) => d,
        Err(e) => {
            return Ok(check(
                "openapi document readable",
                false,
                &format!("GateErr: {e}"),
            ));
        }
    };
    let mut documented: Vec<String> = doc
        .get("paths")
        .and_then(Value::as_object)
        .map(|m| m.keys().cloned().collect())
        .unwrap_or_default();
    documented.sort();
    let probes = rest_probes();
    let undocumented: Vec<String> = {
        let mut v: Vec<String> = probes
            .keys()
            .filter(|p| !documented.iter().any(|d| d == *p))
            .map(|s| s.to_string())
            .collect();
        v.sort();
        v
    };
    let unprobed: Vec<String> = documented
        .iter()
        .filter(|p| !probes.contains_key(p.as_str()))
        .cloned()
        .collect();
    passed &= check(
        "openapi paths == probe table (drift alarm)",
        undocumented.is_empty() && unprobed.is_empty(),
        &format!(
            "{} documented paths{}{}",
            documented.len(),
            if unprobed.is_empty() {
                String::new()
            } else {
                format!(
                    "; DRIFT — documented but NOT probed: {}",
                    pystr_list(&unprobed)
                )
            },
            if undocumented.is_empty() {
                String::new()
            } else {
                format!(
                    "; DRIFT — probed but no longer documented: {}",
                    pystr_list(&undocumented)
                )
            }
        ),
    );
    for path in &documented {
        let Some((method, target, body)) = probes.get(path.as_str()) else {
            continue;
        };
        let (status, ctype, payload) =
            match ctx
                .http
                .status(&ctx.url(target), method, body.as_ref(), 120)
            {
                Ok(r) => r,
                Err(e) => {
                    passed &= check(
                        &format!("REST {method} {path}"),
                        false,
                        &format!("GateErr: {e}"),
                    );
                    continue;
                }
            };
        if let Some(why) = rest_probe_skip(path, status) {
            skip(&format!("REST {method} {path}: {status} — {why}"));
            continue;
        }
        let mut ok = (200..300).contains(&status) && !payload.is_empty();
        let mut ctype_shown = ctype.clone();
        if ok
            && ctype.to_lowercase().contains("json")
            && let Err(e) = serde_json::from_slice::<Value>(&payload)
        {
            ok = false;
            ctype_shown = format!("{ctype} (undecodable: {e})");
        }
        let mut detail = format!(
            "{status} {} {}B",
            ctype_shown
                .split(';')
                .next()
                .filter(|s| !s.is_empty())
                .unwrap_or("?"),
            payload.len()
        );
        if !ok {
            detail += &format!(" body={}", pyrepr_bytes(&payload[..payload.len().min(160)]));
        }
        passed &= check(&format!("REST {method} {path}"), ok, &detail);
    }
    let invalid = rest_invalid_probes();
    let covered: Vec<String> = documented
        .iter()
        .filter(|p| !REST_INPUTLESS_PATHS.contains(&p.as_str()))
        .cloned()
        .collect();
    let missing_invalid: Vec<String> = covered
        .iter()
        .filter(|p| !invalid.contains_key(p.as_str()))
        .cloned()
        .collect();
    passed &= check(
        "every input-taking path has an invalid probe",
        missing_invalid.is_empty(),
        &format!(
            "{} probes{}",
            invalid.len(),
            if missing_invalid.is_empty() {
                String::new()
            } else {
                format!("; MISSING: {}", pystr_list(&missing_invalid))
            }
        ),
    );
    for path in &covered {
        let Some((method, target, body)) = invalid.get(path.as_str()) else {
            continue;
        };
        let (status, _ctype, payload) =
            match ctx.http.status(&ctx.url(target), method, body.as_ref(), 60) {
                Ok(r) => r,
                Err(e) => {
                    passed &= check(
                        &format!("REST {method} {path} (invalid)"),
                        false,
                        &format!("GateErr: {e}"),
                    );
                    continue;
                }
            };
        if let Some(why) = rest_probe_skip(path, status) {
            skip(&format!("REST {method} {path} (invalid): {status} — {why}"));
            continue;
        }
        let mut detail = format!("{status}");
        let mut ok = (400..500).contains(&status);
        if !ok {
            detail += &format!(
                " not a 4xx; body={}",
                pyrepr_bytes(&payload[..payload.len().min(160)])
            );
        } else {
            match serde_json::from_slice::<Value>(&payload) {
                Err(e) => {
                    ok = false;
                    detail = format!("{status} undecodable JSON: {e}");
                }
                Ok(doc_body) => {
                    ok = doc_body
                        .as_object()
                        .and_then(|m| m.get("error"))
                        .and_then(Value::as_str)
                        .is_some_and(|s| !s.is_empty());
                    let keys = match doc_body.as_object() {
                        Some(m) => {
                            let mut k: Vec<&String> = m.keys().collect();
                            k.sort();
                            format!(
                                "[{}]",
                                k.iter()
                                    .map(|x| format!("'{x}'"))
                                    .collect::<Vec<_>>()
                                    .join(", ")
                            )
                        }
                        None => match &doc_body {
                            Value::Array(_) => "list".into(),
                            Value::String(_) => "str".into(),
                            Value::Number(_) => "int".into(),
                            Value::Bool(_) => "bool".into(),
                            _ => "NoneType".into(),
                        },
                    };
                    detail = format!("{status} keys={keys}");
                    if !ok {
                        detail += " — no documented `error` field";
                    }
                }
            }
        }
        passed &= check(
            &format!("REST {method} {path} (invalid) carries `error`"),
            ok,
            &detail,
        );
    }
    if !ctx.flight {
        skip("Flight actions: --no-flight");
        return Ok(passed);
    }
    let pairs = json!([[o.0, o.1, d.0, d.1]]);
    let do_get_ok = |action: &str, params: Value| -> bool {
        match ctx.do_get(action, "car", &params) {
            Ok(dec) => check(
                &format!("Flight {action}"),
                true,
                &format!("{} rows", dec.num_rows()),
            ),
            Err(e) => check(
                &format!("Flight {action}"),
                false,
                &trunc(&e.to_string(), 80),
            ),
        }
    };
    passed &= do_get_ok(
        "matrix",
        json!({"origins": [[o.0, o.1]], "destinations": [[d.0, d.1]]}),
    );
    passed &= do_get_ok("route_batch", json!({"pairs": pairs}));
    passed &= do_get_ok("edges_batch", json!({"pairs": pairs}));
    passed &= do_get_ok(
        "isochrone",
        json!({"lon": o.0, "lat": o.1, "intervals": [300]}),
    );
    match ctx.do_get(
        "transit_bulk",
        "transit",
        &json!({"queries": [{"origin_lon": o.0, "origin_lat": o.1, "destination_lon": d.0, "destination_lat": d.1}]}),
    ) {
        Ok(_) => passed &= check("Flight transit_bulk", true, "ok"),
        Err(e) => {
            let msg = e.to_string();
            if msg.contains("not loaded") || msg.contains("FailedPrecondition") || msg.to_lowercase().contains("transit") {
                skip("Flight transit_bulk: transit subsystem not loaded");
            } else {
                passed &= check("Flight transit_bulk", false, &trunc(&msg, 80));
            }
        }
    }
    let catchment_cmd = format!(
        "catchment:car:{}",
        r#"{"percentiles": [50], "hull_shape": "isochrone", "remove_outliers": false}"#
    );
    let catchment_tbl = f64_table(
        &[
            ("store_lon", vec![o.0]),
            ("store_lat", vec![o.1]),
            ("client_lon", vec![d.0]),
            ("client_lat", vec![d.1]),
        ],
        &[("store_id", vec!["s1".to_string()])],
    );
    let flow_tbl = f64_table(
        &[
            ("src_lon", vec![o.0]),
            ("src_lat", vec![o.1]),
            ("dst_lon", vec![d.0]),
            ("dst_lat", vec![d.1]),
        ],
        &[],
    );
    for (label, cmd, tbl) in [
        ("catchment", catchment_cmd.into_bytes(), catchment_tbl),
        ("edges_flow", b"edges_flow:car".to_vec(), flow_tbl),
    ] {
        match ctx.do_exchange(&cmd, tbl) {
            Ok(_) => passed &= check(&format!("Flight {label}"), true, "ok"),
            Err(e) => {
                passed &= check(
                    &format!("Flight {label}"),
                    false,
                    &trunc(&e.to_string(), 80),
                )
            }
        }
    }
    Ok(passed)
}

pub fn trunc(s: &str, n: usize) -> String {
    s.chars().take(n).collect()
}

pub const EDGES_FLOW_MAX_PAIRS: usize = 500_000;

pub fn gate_edges_flow_storm(ctx: &Ctx) -> GateResult {
    println!(
        "== edges_flow storm: /health stays fast under a heavy exchange, bound enforced (#631) =="
    );
    let rng = Arc::new(Mutex::new(PyRandom::new(631)));
    let table = |n: usize| -> arrow::array::RecordBatch {
        let mut r = rng.lock().unwrap();
        let mut src = Vec::with_capacity(n);
        for _ in 0..n {
            let a = r.uniform(4.30, 4.70);
            let b = r.uniform(50.80, 51.20);
            src.push((a, b));
        }
        let mut dst = Vec::with_capacity(n);
        for _ in 0..n {
            let a = r.uniform(4.30, 4.70);
            let b = r.uniform(50.80, 51.20);
            dst.push((a, b));
        }
        f64_table(
            &[
                ("src_lon", src.iter().map(|p| p.0).collect()),
                ("src_lat", src.iter().map(|p| p.1).collect()),
                ("dst_lon", dst.iter().map(|p| p.0).collect()),
                ("dst_lat", dst.iter().map(|p| p.1).collect()),
                ("weight", vec![1.0; n]),
            ],
            &[],
        )
    };
    let exchange = |n: usize| -> GResult<usize> {
        let t = table(n);
        Ok(ctx.do_exchange(b"edges_flow:car", t)?.num_rows())
    };
    let mut passed = true;
    match exchange(EDGES_FLOW_MAX_PAIRS + 1) {
        Ok(_) => {
            passed &= check(
                &format!("edges_flow refuses {} pairs", EDGES_FLOW_MAX_PAIRS + 1),
                false,
                "accepted",
            )
        }
        Err(e) => {
            let msg = e.to_string();
            passed &= check(
                &format!(
                    "edges_flow refuses {} pairs and says to chunk",
                    EDGES_FLOW_MAX_PAIRS + 1
                ),
                msg.contains("chunk") && msg.contains(&EDGES_FLOW_MAX_PAIRS.to_string()),
                &trunc(&msg, 140),
            );
        }
    }
    let stop = Arc::new(AtomicBool::new(false));
    let lat: Arc<Mutex<Vec<f64>>> = Arc::new(Mutex::new(Vec::new()));
    let iso: Arc<Mutex<Vec<f64>>> = Arc::new(Mutex::new(Vec::new()));
    let poll = |path: &'static str, store: Arc<Mutex<Vec<f64>>>, timeout: u64| {
        let stop = Arc::clone(&stop);
        let url = ctx.url(path);
        move || {
            let client = reqwest::blocking::Client::new();
            while !stop.load(Ordering::Relaxed) {
                let t0 = Instant::now();
                let r = client
                    .get(&url)
                    .timeout(Duration::from_secs(timeout))
                    .send()
                    .and_then(|r| r.error_for_status())
                    .and_then(|r| r.bytes());
                store.lock().unwrap().push(if r.is_ok() {
                    t0.elapsed().as_secs_f64()
                } else {
                    99.0
                });
                let deadline = Instant::now() + Duration::from_secs(1);
                while Instant::now() < deadline && !stop.load(Ordering::Relaxed) {
                    std::thread::sleep(Duration::from_millis(20));
                }
            }
        }
    };
    let t0 = Instant::now();
    let (result, err) = std::thread::scope(|s| {
        let h1 = s.spawn(poll("/health", Arc::clone(&lat), 10));
        let h2 = s.spawn(poll(
            "/isochrone?lon=4.3517&lat=50.8503&time_s=300&mode=car",
            Arc::clone(&iso),
            10,
        ));
        let r = exchange(60_000);
        stop.store(true, Ordering::Relaxed);
        let _ = h1.join();
        let _ = h2.join();
        match r {
            Ok(rows) => (Some(rows), None),
            Err(e) => (None, Some(trunc(&e.to_string(), 160))),
        }
    });
    passed &= check(
        "edges_flow 60 000 pairs completes",
        result.is_some_and(|r| r > 0),
        &format!(
            "{} rows in {:.1}s{}",
            result.unwrap_or(0),
            t0.elapsed().as_secs_f64(),
            err.map(|e| format!(" ({e})")).unwrap_or_default()
        ),
    );
    let lat = lat.lock().unwrap().clone();
    let worst = lat.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    let worst = if lat.is_empty() { 99.0 } else { worst };
    passed &= check(
        "/health answered under 1 s at every poll during the exchange",
        !lat.is_empty() && worst < 1.0,
        &format!("{} polls, worst {worst:.3}s", lat.len()),
    );
    let iso = iso.lock().unwrap().clone();
    let worst_iso = iso.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    let worst_iso = if iso.is_empty() { 99.0 } else { worst_iso };
    passed &= check(
        "a 5-min isochrone answered under 2 s at every poll during the exchange",
        !iso.is_empty() && worst_iso < 2.0,
        &format!("{} polls, worst {worst_iso:.3}s", iso.len()),
    );
    Ok(passed)
}

// Shared by the Flight gates: {(source_idx, target_idx): duration_ms} + rows.
pub type MatrixCells = HashMap<(u64, u64), u64>;

pub fn flight_matrix_cells(ctx: &Ctx, mode: &str, params: &Value) -> GResult<(MatrixCells, usize)> {
    let dec = ctx.do_get("matrix", mode, params)?;
    let mut cells = HashMap::new();
    for (b, i) in rows(&dec) {
        let (Some(s), Some(t), Some(d)) = (
            col_u64(b, "source_idx", i),
            col_u64(b, "target_idx", i),
            col_u64(b, "duration_ms", i),
        ) else {
            continue;
        };
        cells.insert((s, t), d);
    }
    let n = dec.num_rows();
    Ok((cells, n))
}

#[allow(dead_code)]
fn _unused(v: &[f64]) -> f64 {
    let _ = col_f64;
    let _ = MAX_U32;
    pct(v, 0.5)
}
