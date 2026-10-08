//! Gates — isochrone geometry, catchment, isochrone transports.

use std::collections::HashMap;

use serde_json::{Value, json};

use super::ctx::{
    Ctx, FIXTURES, ISO_POINTS, IsoBundle, PEDESTRIAN_CENTRES, check, check_errors, f0, num,
    rings_of,
};
use super::flight::{col_bytes, col_u64, f64_table, rows};
use super::gates_rest::{GateResult, trunc};
use super::geom::{
    Pt, decode_polyline6, dist_to_ring_m, haversine_m, point_in_ring, ring_area, ring_area2,
    wkb_polygons, wkb_type,
};
use super::http::{GResult, GateErr, is_no_route, pyf, urlencode};
use super::pyrandom::PyRandom;

/// Python's `f"{x:.2e}"`: two-digit, signed exponent.
fn pye2(x: f64) -> String {
    let s = format!("{x:.2e}");
    match s.split_once('e') {
        Some((m, e)) => {
            let (sign, digits) = match e.strip_prefix('-') {
                Some(d) => ("-", d),
                None => ("+", e),
            };
            format!("{m}e{sign}{digits:0>2}")
        }
        None => s,
    }
}

fn body(ring: &[Pt]) -> &[Pt] {
    if ring.is_empty() {
        ring
    } else {
        &ring[..ring.len() - 1]
    }
}

fn why_join(why: &[String], n: usize) -> String {
    why.iter().take(n).cloned().collect::<Vec<_>>().join("; ")
}

pub fn gate_isochrone_topology(ctx: &Ctx) -> GateResult {
    println!(
        "== isochrone topology: ONE simple polygon, no spurs, faithful to the network (2026-09-03) =="
    );
    let t = &ctx.t;
    let (far_m, far_frac, near_m, snap_max) = (
        t.topology_outside_m,
        t.topology_outside_frac,
        t.pin_near_ring_m,
        t.pin_snap_max_m,
    );
    let mut passed = true;
    let origins: Vec<(&str, f64, f64, bool)> = ISO_POINTS
        .iter()
        .map(|&(n, lo, la)| (n, lo, la, false))
        .chain(
            PEDESTRIAN_CENTRES
                .iter()
                .map(|&(n, lo, la)| (n, lo, la, true)),
        )
        .collect();
    for (mode, time_s) in [("car", 600u64), ("foot", 1800u64)] {
        let (mut n_ok, mut n, mut far_total, mut verts_total) = (0usize, 0usize, 0usize, 0usize);
        let (mut pin_ok, mut n_pin) = (0usize, 0usize);
        let mut details: Vec<String> = Vec::new();
        for &(name, lon, lat, is_centre) in &origins {
            let b = IsoBundle::new(ctx, lon, lat, mode, time_s, "depart", "time_s");
            let fetched = (|| -> GResult<_> {
                Ok((
                    b.wkb()?,
                    b.polys()?,
                    b.snap()?,
                    b.network()?,
                    b.geojson()?,
                    b.rings()?,
                ))
            })();
            let (wkb, polys, sp, net, gj, p6) = match fetched {
                Ok(v) => v,
                Err(e) => {
                    details.push(format!("{name}: {e}"));
                    continue;
                }
            };
            n += 1;
            let mut why: Vec<String> = Vec::new();
            if polys.is_empty() {
                why.push("no polygon in the WKB".into());
            } else if wkb_type(&wkb) != Some(3) || polys.len() != 1 {
                why.push(format!(
                    "WKB is not a single Polygon ({} parts)",
                    polys.len()
                ));
            } else if polys[0].len() != 1 {
                why.push(format!("polygon has {} hole(s)", polys[0].len() - 1));
            }
            for (pi, rings) in polys.iter().enumerate() {
                for (ri, ring) in rings.iter().enumerate() {
                    if ring.len() < 4 || ring.first() != ring.last() {
                        why.push(format!("p{pi}r{ri}: not a closed ring of ≥4 points"));
                    }
                    let bd = body(ring);
                    let m = bd.len();
                    if m > 0 && (0..m).any(|i| bd[i] == bd[(i + 1) % m]) {
                        why.push(format!("p{pi}r{ri}: consecutive duplicate vertex"));
                    }
                    if m > 0 && (0..m).any(|i| bd[i] == bd[(i + 2) % m]) {
                        why.push(format!("p{pi}r{ri}: zero-width spur (a,b,a)"));
                    }
                    let a2 = ring_area2(bd);
                    if ri == 0 && a2 <= 0.0 {
                        why.push(format!("p{pi}: outer ring not CCW"));
                    }
                    if ri > 0 {
                        if a2 >= 0.0 {
                            why.push(format!("p{pi}r{ri}: hole not CW"));
                        }
                        if !bd.is_empty() && !point_in_ring(bd[0], body(&rings[0])) {
                            why.push(format!("p{pi}r{ri}: hole outside its outer ring"));
                        }
                        if point_in_ring(sp, bd) {
                            why.push(format!("p{pi}r{ri}: hole contains the origin"));
                        }
                    }
                }
            }
            if p6.len() != 1 {
                why.push(format!(
                    "polyline6: {} contour rings for one time_s (want 1)",
                    p6.len()
                ));
            }
            for (ri, r) in p6.iter().enumerate() {
                if r.len() < 4 || r.first() != r.last() {
                    why.push(format!("polyline6 r{ri}: ring not closed (r[0] != r[-1])"));
                } else if ring_area2(body(r)) <= 0.0 {
                    why.push(format!("polyline6 r{ri}: ring not CCW"));
                }
            }
            if !polys.is_empty() && !point_in_ring(sp, body(&polys[0][0])) {
                why.push("origin not in the primary polygon".into());
            }
            let pts: Vec<Pt> = net.iter().flatten().copied().step_by(3).collect();
            let mut far = 0usize;
            for &p in &pts {
                let inside = polys.iter().any(|rings| {
                    point_in_ring(p, body(&rings[0]))
                        && !rings[1..].iter().any(|h| point_in_ring(p, body(h)))
                });
                if inside {
                    continue;
                }
                let d = polys
                    .iter()
                    .map(|rings| dist_to_ring_m(p, &rings[0]))
                    .fold(1e9, f64::min);
                if d > far_m {
                    far += 1;
                }
            }
            far_total += far;
            verts_total += pts.len();
            if !pts.is_empty() && far as f64 / pts.len() as f64 > far_frac {
                why.push(format!(
                    "{far}/{} reachable vertices > {} m outside",
                    pts.len(),
                    f0(far_m)
                ));
            }
            for (pi, rings) in polys.iter().enumerate().skip(1) {
                if !pts.iter().any(|&p| point_in_ring(p, body(&rings[0]))) {
                    why.push(format!("p{pi}: component without any reachable network"));
                    break;
                }
            }
            let g = gj.pointer("/contours/0/geometry").filter(|g| !g.is_null());
            match g {
                None => why.push("geojson: no `geometry` object".into()),
                Some(g) => {
                    let coords = g.get("coordinates").and_then(Value::as_array);
                    let gr = if g.get("type").and_then(Value::as_str) == Some("MultiPolygon") {
                        coords
                            .map(|cs| cs.iter().map(|p| p.as_array().map_or(0, |a| a.len())).sum())
                            .unwrap_or(0)
                    } else {
                        coords.map_or(0, |c| c.len())
                    };
                    let wr: usize = polys.iter().map(|r| r.len()).sum();
                    if gr != wr {
                        why.push(format!("geojson rings {gr} != wkb rings {wr}"));
                    }
                }
            }
            if is_centre && !polys.is_empty() {
                n_pin += 1;
                let ring = &polys[0][0];
                let snap_m = haversine_m(lon, lat, sp.0, sp.1);
                let covered = point_in_ring((lon, lat), body(ring))
                    || dist_to_ring_m((lon, lat), ring) <= near_m;
                if covered && snap_m <= snap_max {
                    pin_ok += 1;
                } else {
                    why.push(format!(
                        "#535: pin inside/near={} snap {} m (max {} m)",
                        if covered { "True" } else { "False" },
                        f0(snap_m),
                        f0(snap_max)
                    ));
                }
            }
            if why.is_empty() {
                n_ok += 1;
            } else {
                details.push(format!("{name}: {}", why_join(&why, 3)));
            }
        }
        for d in details.iter().take(6) {
            println!("    {d}");
        }
        passed &= check(
            &format!("{mode} {time_s}s: valid topology at every origin"),
            n > 0 && n_ok == n,
            &format!(
                "{n_ok}/{n} origins; network vertices > {} m outside: {far_total}/{verts_total} ({:.2}%)",
                f0(far_m),
                100.0 * far_total as f64 / verts_total.max(1) as f64
            ),
        );
        passed &= check(
            &format!(
                "#535: {mode} {time_s}s isochrone from a pedestrian centre contains the pin (one polygon, snap ≤ {} m)",
                f0(snap_max)
            ),
            n_pin == PEDESTRIAN_CENTRES.len() && pin_ok == n_pin,
            &format!("{pin_ok}/{} centres", PEDESTRIAN_CENTRES.len()),
        );
    }
    Ok(passed)
}

fn durations_grid(
    ctx: &Ctx,
    origins: &[[f64; 2]],
    dests: &[[f64; 2]],
    annotation: &str,
) -> GResult<Value> {
    let key = if annotation == "distance" {
        "distances"
    } else {
        "durations"
    };
    Ok(ctx
        .table(
            origins,
            dests,
            "car",
            120,
            &[("annotations", json!(annotation))],
        )?
        .get(key)
        .cloned()
        .unwrap_or(Value::Null))
}

fn row_values(grid: &Value, i: usize) -> Vec<f64> {
    grid.get(i)
        .and_then(Value::as_array)
        .map(|r| r.iter().filter_map(Value::as_f64).collect())
        .unwrap_or_default()
}

fn col_values(grid: &Value, j: usize) -> Vec<f64> {
    grid.as_array()
        .map(|rs| {
            rs.iter()
                .filter_map(|r| r.get(j).and_then(Value::as_f64))
                .collect()
        })
        .unwrap_or_default()
}

pub fn gate_isochrone_reach_truth(ctx: &Ctx) -> GateResult {
    println!("== isochrone ≡ engine reach (/table truth, depart + arrive) (2026-09-03) ==");
    let t = &ctx.t;
    let mut passed = true;
    let (mode, big_t) = ("car", 600u64);
    let far_m = t.topology_outside_m;
    for direction in ["depart", "arrive"] {
        let (mut n_in, mut n_in_over, mut n_out, mut n_out_reached) =
            (0usize, 0usize, 0usize, 0usize);
        let mut worst_out: Option<(f64, &str)> = None;
        let mut details = Vec::new();
        for &(name, lon, lat) in ISO_POINTS {
            let b = IsoBundle::new(ctx, lon, lat, mode, big_t, direction, "time_s");
            let wide = IsoBundle::new(
                ctx,
                lon,
                lat,
                mode,
                (big_t as f64 * 1.4) as u64,
                direction,
                "time_s",
            );
            let (ring, net, big) = match (|| -> GResult<_> {
                let polys = b.polys()?;
                let ring = polys
                    .first()
                    .and_then(|p| p.first())
                    .cloned()
                    .ok_or_else(|| GateErr::Other("list index out of range".into()))?;
                Ok((ring, b.network()?, wide.network()?))
            })() {
                Ok(v) => v,
                Err(e) => {
                    details.push(format!("{name}: {e}"));
                    continue;
                }
            };
            let mut rnd = PyRandom::new(7);
            let mut segs: Vec<&Vec<Pt>> = net.iter().filter(|s| s.len() >= 2).collect();
            rnd.shuffle(&mut segs);
            segs.truncate(150);
            let mut ends: Vec<Pt> = segs.iter().map(|s| s[s.len() - 1]).collect();
            ends.extend(segs.iter().map(|s| {
                let (a, bq) = (s[s.len() - 2], s[s.len() - 1]);
                (a.0 + (bq.0 - a.0) * 0.5, a.1 + (bq.1 - a.1) * 0.5)
            }));
            let mut pts: Vec<Pt> = big.iter().flatten().copied().collect();
            rnd.shuffle(&mut pts);
            let mut far = Vec::new();
            for p in pts {
                if far.len() >= 150 {
                    break;
                }
                if point_in_ring(p, body(&ring)) {
                    continue;
                }
                if dist_to_ring_m(p, &ring) > far_m {
                    far.push(p);
                }
            }
            let ends_a: Vec<[f64; 2]> = ends.iter().map(|p| [p.0, p.1]).collect();
            let far_a: Vec<[f64; 2]> = far.iter().map(|p| [p.0, p.1]).collect();
            let (d_in, d_out) = if direction == "depart" {
                let g = durations_grid(ctx, &[[lon, lat]], &ends_a, "duration")?;
                let out = if far_a.is_empty() {
                    vec![]
                } else {
                    row_values(&durations_grid(ctx, &[[lon, lat]], &far_a, "duration")?, 0)
                };
                (row_values(&g, 0), out)
            } else {
                let g = durations_grid(ctx, &ends_a, &[[lon, lat]], "duration")?;
                let out = if far_a.is_empty() {
                    vec![]
                } else {
                    col_values(&durations_grid(ctx, &far_a, &[[lon, lat]], "duration")?, 0)
                };
                (col_values(&g, 0), out)
            };
            n_in += d_in.len();
            n_in_over += d_in
                .iter()
                .filter(|&&x| x > t.reach_in_tol * big_t as f64)
                .count();
            n_out += d_out.len();
            let reached: Vec<f64> = d_out
                .iter()
                .copied()
                .filter(|&x| x <= t.reach_out_tol * big_t as f64)
                .collect();
            n_out_reached += reached.len();
            if !reached.is_empty() {
                let m = reached.iter().cloned().fold(f64::INFINITY, f64::min);
                if worst_out.is_none_or(|w| m < w.0) {
                    worst_out = Some((m, name));
                }
                details.push(format!(
                    "{name}: {}/{} outside road points reachable ≤ {}T (min {} s)",
                    reached.len(),
                    d_out.len(),
                    pyf(t.reach_out_tol),
                    f0(m)
                ));
            }
        }
        for d in details.iter().take(4) {
            println!("    {d}");
        }
        passed &= check(
            &format!(
                "{direction} {big_t}s: served network reachable within {}T",
                pyf(t.reach_in_tol)
            ),
            n_in > 0 && n_in_over <= 1.max((n_in as f64 * t.reach_in_over_frac) as usize),
            &format!("{}/{n_in} vertices", n_in - n_in_over),
        );
        passed &= check(
            &format!(
                "{direction} {big_t}s: nothing reachable ≤ {}T lies > {} m outside",
                pyf(t.reach_out_tol),
                f0(far_m)
            ),
            n_out_reached <= 1.max((n_out as f64 * t.reach_out_frac) as usize),
            &format!(
                "{n_out_reached}/{n_out} road points{}",
                worst_out
                    .map(|(m, nm)| format!(", earliest {} s at {nm}", f0(m)))
                    .unwrap_or_default()
            ),
        );
    }
    Ok(passed)
}

/// True when every road /table would seed for `p` lies on `polyline`.
fn snap_unambiguous(ctx: &Ctx, p: Pt, polyline: &[Pt], mode: &str) -> bool {
    let Ok(j) = ctx.http.json(
        &ctx.url(&format!(
            "/nearest?lon={}&lat={}&mode={mode}&number=8",
            pyf(p.0),
            pyf(p.1)
        )),
        30,
    ) else {
        return false;
    };
    let Some(w) = j.get("waypoints").and_then(Value::as_array) else {
        return false;
    };
    if w.is_empty() {
        return false;
    }
    let dist = |x: &Value| {
        x.get("distance")
            .and_then(Value::as_f64)
            .unwrap_or(f64::INFINITY)
    };
    let d_min = w.iter().map(dist).fold(f64::INFINITY, f64::min);
    let slack = (d_min + 20.0).max(d_min * 1.2);
    let kx = p.1.to_radians().cos() * 111_320.0;
    let ky = 110_540.0;
    let on_polyline = |q: Pt| -> bool {
        for s in polyline.windows(2) {
            let (ax, ay) = ((s[0].0 - q.0) * kx, (s[0].1 - q.1) * ky);
            let (bx, by) = ((s[1].0 - q.0) * kx, (s[1].1 - q.1) * ky);
            let (dx, dy) = (bx - ax, by - ay);
            let l2 = dx * dx + dy * dy;
            let u = if l2 == 0.0 {
                0.0
            } else {
                (-(ax * dx + ay * dy) / l2).clamp(0.0, 1.0)
            };
            if (ax + u * dx).hypot(ay + u * dy) <= 1.0 {
                return true;
            }
        }
        false
    };
    if w.len() >= 8 && w.iter().all(|x| dist(x) <= slack) {
        return false;
    }
    let own_len = w[0].get("edge_length_m").cloned();
    w.iter().all(|x| {
        if dist(x) > slack {
            return true;
        }
        let loc = x
            .get("location")
            .and_then(Value::as_array)
            .and_then(|a| Some((a.first()?.as_f64()?, a.get(1)?.as_f64()?)));
        loc.is_some_and(on_polyline) && x.get("edge_length_m").cloned() == own_len
    })
}

pub fn gate_isodistance_truth(ctx: &Ctx) -> GateResult {
    println!("== isodistance == length along the time-shortest path (/table truth) (#612) ==");
    let t = &ctx.t;
    let mut passed = true;
    let (mode, l_m) = ("car", 5000u64);
    let far_m = t.topology_outside_m;
    for direction in ["depart", "arrive"] {
        struct P<'a> {
            name: &'a str,
            origin: [f64; 2],
            ends: Vec<Pt>,
            far: Vec<Pt>,
        }
        let mut probe: Vec<P> = Vec::new();
        let mut details: Vec<String> = Vec::new();
        for &(name, lon, lat) in ISO_POINTS {
            let b = IsoBundle::new(ctx, lon, lat, mode, l_m, direction, "distance_m");
            let wide = IsoBundle::new(
                ctx,
                lon,
                lat,
                mode,
                (l_m as f64 * 1.4) as u64,
                direction,
                "distance_m",
            );
            let (ring, net, big) = match (|| -> GResult<_> {
                let polys = b.polys()?;
                let ring = polys
                    .first()
                    .and_then(|p| p.first())
                    .cloned()
                    .ok_or_else(|| GateErr::Other("list index out of range".into()))?;
                Ok((ring, b.network()?, wide.network()?))
            })() {
                Ok(v) => v,
                Err(e) => {
                    details.push(format!("{name}: {e}"));
                    continue;
                }
            };
            let mut rnd = PyRandom::new(7);
            let mut segs: Vec<&Vec<Pt>> = net.iter().filter(|s| s.len() >= 2).collect();
            rnd.shuffle(&mut segs);
            segs.truncate(100);
            let mut ends: Vec<Pt> = segs.iter().map(|s| s[s.len() - 2]).collect();
            ends.extend(segs.iter().map(|s| {
                let (a, bq) = (s[s.len() - 2], s[s.len() - 1]);
                ((a.0 + bq.0) / 2.0, (a.1 + bq.1) / 2.0)
            }));
            let seg_for: Vec<&Vec<Pt>> = segs.iter().chain(segs.iter()).copied().collect();
            let ends: Vec<Pt> = ends
                .into_iter()
                .zip(seg_for)
                .filter(|(p, s)| snap_unambiguous(ctx, *p, s, mode))
                .map(|(p, _)| p)
                .collect();
            let mut pts: Vec<Pt> = big.iter().flatten().copied().collect();
            rnd.shuffle(&mut pts);
            let mut far = Vec::new();
            for p in pts {
                if far.len() >= 150 {
                    break;
                }
                if point_in_ring(p, body(&ring)) {
                    continue;
                }
                if dist_to_ring_m(p, &ring) > far_m {
                    far.push(p);
                }
            }
            probe.push(P {
                name,
                origin: [lon, lat],
                ends,
                far,
            });
        }
        let origins: Vec<[f64; 2]> = probe.iter().map(|p| p.origin).collect();
        let mut points: Vec<[f64; 2]> = Vec::new();
        let mut spans = Vec::new();
        for p in &probe {
            spans.push((points.len(), p.ends.len(), p.far.len()));
            points.extend(p.ends.iter().map(|e| [e.0, e.1]));
            points.extend(p.far.iter().map(|e| [e.0, e.1]));
        }
        if origins.is_empty() || points.is_empty() {
            let d = if details.is_empty() {
                "no origins sampled".to_string()
            } else {
                why_join(&details, 3)
            };
            passed &= check(
                &format!("{direction} {l_m}m: isodistance probes built"),
                false,
                &d,
            );
            continue;
        }
        let grid = if direction == "depart" {
            durations_grid(ctx, &origins, &points, "distance")?
        } else {
            durations_grid(ctx, &points, &origins, "distance")?
        };
        let row_of = |i: usize| -> Vec<Option<f64>> {
            if direction == "depart" {
                grid.get(i)
                    .and_then(Value::as_array)
                    .map(|r| r.iter().map(Value::as_f64).collect())
                    .unwrap_or_default()
            } else {
                grid.as_array()
                    .map(|rs| {
                        rs.iter()
                            .map(|r| r.get(i).and_then(Value::as_f64))
                            .collect()
                    })
                    .unwrap_or_default()
            }
        };
        let (mut n_in, mut n_in_over, mut n_out, mut n_out_reached) =
            (0usize, 0usize, 0usize, 0usize);
        let mut worst_out: Option<(f64, &str)> = None;
        for (i, p) in probe.iter().enumerate() {
            let row = row_of(i);
            let (off, n_e, n_f) = spans[i];
            let d_in: Vec<f64> = row.iter().skip(off).take(n_e).flatten().copied().collect();
            let d_out: Vec<f64> = row
                .iter()
                .skip(off + n_e)
                .take(n_f)
                .flatten()
                .copied()
                .collect();
            n_in += d_in.len();
            n_in_over += d_in
                .iter()
                .filter(|&&x| x > t.reach_in_tol * l_m as f64)
                .count();
            n_out += d_out.len();
            let reached: Vec<f64> = d_out
                .iter()
                .copied()
                .filter(|&x| x <= t.reach_out_tol * l_m as f64)
                .collect();
            n_out_reached += reached.len();
            if !reached.is_empty() {
                let m = reached.iter().cloned().fold(f64::INFINITY, f64::min);
                if worst_out.is_none_or(|w| m < w.0) {
                    worst_out = Some((m, p.name));
                }
                details.push(format!(
                    "{}: {}/{} outside road points within {}L (min {} m)",
                    p.name,
                    reached.len(),
                    d_out.len(),
                    pyf(t.reach_out_tol),
                    f0(m)
                ));
            }
        }
        for d in details.iter().take(4) {
            println!("    {d}");
        }
        passed &= check(
            &format!(
                "{direction} {l_m}m: served network within {}L by /table distance (exact, #620)",
                pyf(t.reach_in_tol)
            ),
            n_in > 0 && n_in_over <= t.iso_len_in_over_max,
            &format!(
                "{}/{n_in} unambiguous points (last vertex before the cut + midpoints)",
                n_in - n_in_over
            ),
        );
        passed &= check(
            &format!(
                "{direction} {l_m}m: nothing within {}L lies > {} m outside",
                pyf(t.reach_out_tol),
                f0(far_m)
            ),
            n_out > 0 && n_out_reached <= 1.max((n_out as f64 * t.reach_out_frac) as usize),
            &format!(
                "{n_out_reached}/{n_out} road points{}",
                worst_out
                    .map(|(m, nm)| format!(", nearest {} m at {nm}", f0(m)))
                    .unwrap_or_default()
            ),
        );
    }
    // (c) product rule for a distance threshold
    let (mut n, mut n_ok) = (0usize, 0usize);
    let mut why_all = Vec::new();
    for &(name, lon, lat) in ISO_POINTS {
        let b = IsoBundle::new(ctx, lon, lat, mode, l_m, "depart", "distance_m");
        let (wkb, polys, sp) = match (|| -> GResult<_> { Ok((b.wkb()?, b.polys()?, b.snap()?)) })()
        {
            Ok(v) => v,
            Err(e) => {
                why_all.push(format!("{name}: {e}"));
                continue;
            }
        };
        n += 1;
        let mut why = Vec::new();
        if polys.is_empty() {
            why.push("no polygon in the WKB".to_string());
        } else if wkb_type(&wkb) != Some(3) || polys.len() != 1 {
            why.push(format!(
                "WKB is not a single Polygon ({} parts)",
                polys.len()
            ));
        } else if polys[0].len() != 1 {
            why.push(format!("polygon has {} hole(s)", polys[0].len() - 1));
        } else {
            let ring = &polys[0][0];
            if ring.len() < 4 || ring.first() != ring.last() {
                why.push("outer ring not closed / < 4 points".into());
            } else if ring_area2(body(ring)) <= 0.0 {
                why.push("outer ring not CCW".into());
            } else if !point_in_ring(sp, body(ring)) {
                why.push("snapped origin outside its own polygon".into());
            }
        }
        if why.is_empty() {
            n_ok += 1;
        } else {
            why_all.push(format!("{name}: {}", why.join("; ")));
        }
    }
    for d in why_all.iter().take(4) {
        println!("    {d}");
    }
    passed &= check(
        &format!("{l_m}m: ONE simple CCW polygon containing the snapped origin"),
        n > 0 && n_ok == n,
        &format!("{n_ok}/{n} origins"),
    );
    // (d) contours_m labels + nesting
    let (lon, lat) = (ISO_POINTS[0].1, ISO_POINTS[0].2);
    let js = ctx.http.json(
        &ctx.url(&format!(
            "/isochrone?lon={}&lat={}&mode={mode}&contours_m=2500,5000",
            pyf(lon),
            pyf(lat)
        )),
        30,
    )?;
    let cs: Vec<Value> = js
        .get("contours")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let dm: Vec<Option<i64>> = cs
        .iter()
        .map(|c| c.get("distance_m").and_then(Value::as_i64))
        .collect();
    let labelled = cs.len() == 2
        && dm == vec![Some(2500), Some(5000)]
        && cs
            .iter()
            .all(|c| c.get("time_s").is_none_or(Value::is_null));
    let shown: Vec<String> = cs
        .iter()
        .map(|c| {
            let d = c
                .get("distance_m")
                .filter(|v| !v.is_null())
                .map(|v| v.to_string())
                .unwrap_or_else(|| "None".into());
            let s = c
                .get("time_s")
                .filter(|v| !v.is_null())
                .map(|v| v.to_string())
                .unwrap_or_else(|| "None".into());
            format!("({d}, {s})")
        })
        .collect();
    passed &= check(
        "contours_m: two contours labelled in metres, never seconds",
        labelled,
        &format!("[{}]", shown.join(", ")),
    );
    if cs.len() == 2 {
        let rings: Vec<Vec<Pt>> = rings_of(&js);
        let nested = rings.len() == 2 && {
            let inner = body(&rings[0]);
            let outer = body(&rings[1]);
            inner.iter().filter(|&&p| point_in_ring(p, outer)).count() as f64
                >= t.iso_nest_tol * inner.len() as f64
        };
        passed &= check(
            "contours_m: the 5 km contour contains the 2.5 km one",
            nested,
            &format!("{} rings", rings.len()),
        );
    }
    let o = [ISO_POINTS[0].1, ISO_POINTS[0].2];
    let d = [ISO_POINTS[1].1, ISO_POINTS[1].2];
    let d_only = ctx
        .table(&[o], &[d], mode, 120, &[("annotations", json!("distance"))])?
        .pointer("/distances/0/0")
        .and_then(Value::as_f64);
    let d_both = ctx
        .table(
            &[o],
            &[d],
            mode,
            120,
            &[("annotations", json!("duration,distance"))],
        )?
        .pointer("/distances/0/0")
        .and_then(Value::as_f64);
    let d_route = num(
        &ctx.route_json(o[0], o[1], d[0], d[1], mode, 60, &[])?,
        "distance_m",
    )?;
    let same = matches!((d_only, d_both), (Some(a), Some(b)) if (a - b).abs() < 1.0 && (a - d_route).abs() <= (2.0f64).max(0.001 * d_route));
    let show = |v: Option<f64>| v.map(pyf).unwrap_or_else(|| "None".into());
    passed &= check(
        "/table annotations=distance is the same metres as duration,distance and /route",
        same,
        &format!(
            "distance-only {} m, duration+distance {} m, /route {} m",
            show(d_only),
            show(d_both),
            f0(d_route)
        ),
    );
    let (code, _ct, raw) = ctx.http.status(
        &ctx.url(&format!(
            "/isochrone?lon={}&lat={}&mode={mode}&distance_m={l_m}&exclude=motorway",
            pyf(lon),
            pyf(lat)
        )),
        "GET",
        None,
        120,
    )?;
    let body_s = String::from_utf8_lossy(&raw).into_owned();
    passed &= check(
        "distance_m + exclude is refused, not answered on stale metres",
        code == 400 && body_s.contains("length-along-time"),
        &format!("HTTP {code}: {}", trunc(&body_s, 140)),
    );
    Ok(passed)
}

pub fn gate_isochrone_upper_bound(ctx: &Ctx) -> GateResult {
    println!("== isochrone upper bound + nested monotonicity (#430/#431) ==");
    let t = &ctx.t;
    let (slack, nest_tol) = (t.iso_reach_slack, t.iso_nest_tol);
    let mut passed = true;
    for mode in ["car", "foot"] {
        let vmax: f64 = if mode == "car" { 36.1 } else { 1.9 };
        let time_s: u64 = if mode == "car" { 600 } else { 1800 };
        let (mut reach_ok, mut nest_ok, mut n) = (0usize, 0usize, 0usize);
        for &(_name, lon, lat) in ISO_POINTS {
            let b = IsoBundle::new(ctx, lon, lat, mode, time_s, "depart", "time_s");
            let (sp, rings) = match (|| -> GResult<_> {
                Ok((b.snap()?, b.contour_rings(&[time_s / 2, time_s])?))
            })() {
                Ok(v) => v,
                Err(_) => continue,
            };
            if rings.is_empty() {
                continue;
            }
            n += 1;
            let reach: Vec<f64> = rings
                .iter()
                .map(|r| {
                    r.iter()
                        .map(|v| haversine_m(sp.0, sp.1, v.0, v.1))
                        .fold(0.0, f64::max)
                })
                .collect();
            let outer_reach = *reach.last().unwrap();
            if outer_reach <= vmax * time_s as f64 * slack {
                reach_ok += 1;
            }
            if rings.len() >= 2 {
                if outer_reach >= reach[0] * nest_tol {
                    nest_ok += 1;
                }
            } else {
                nest_ok += 1;
            }
        }
        passed &= check(
            &format!("{mode}: max reach ≤ v_max×time"),
            n > 0 && reach_ok == n,
            &format!(
                "{reach_ok}/{n} within {vmax:.1}m/s×{time_s}s×{}",
                pyf(slack)
            ),
        );
        passed &= check(
            &format!("{mode}: contours nest (600⊇300)"),
            n > 0 && nest_ok == n,
            &format!("{nest_ok}/{n}"),
        );
    }
    Ok(passed)
}

pub fn gate_isochrone_transports_agree(ctx: &Ctx) -> GateResult {
    println!("== Flight isochrone == /isochrone, same request, same bytes (#613) ==");
    let origins: [(&str, f64, f64); 3] = [
        ("Brussels", 4.3517, 50.8503),
        ("Liege", 5.5671, 50.6326),
        ("Bruges", 3.2247, 51.2089),
    ];
    let ring = "[[[4.42, 50.85], [4.43, 50.85], [4.43, 50.86], [4.42, 50.86]]]".to_string();
    let options: Vec<(&str, Vec<(&str, String)>)> = vec![
        ("exclude=motorway", vec![("exclude", "motorway".into())]),
        (
            "exclude=motorway,toll,ferry",
            vec![("exclude", "motorway,toll,ferry".into())],
        ),
        ("avoid_polygons", vec![("avoid_polygons", ring.clone())]),
    ];
    let rest_wkb = |lon: f64,
                    lat: f64,
                    direction: &str,
                    key: &str,
                    value: u64,
                    opts: &[(&str, String)]|
     -> GResult<Vec<u8>> {
        let mut q: Vec<(&str, String)> = vec![
            ("lon", pyf(lon)),
            ("lat", pyf(lat)),
            ("mode", "car".into()),
            ("direction", direction.into()),
            (key, value.to_string()),
        ];
        q.extend(opts.iter().cloned());
        ctx.http.bytes(
            &ctx.url(&format!("/isochrone?{}", urlencode(&q))),
            900,
            Some("application/octet-stream"),
        )
    };
    let flight_wkb = |lon: f64,
                      lat: f64,
                      direction: &str,
                      key: &str,
                      values: &[u64],
                      opts: &[(&str, String)]|
     -> GResult<Vec<Option<Vec<u8>>>> {
        let mut params = json!({"lon": lon, "lat": lat, "direction": direction});
        params[key] = json!(values);
        for (k, v) in opts {
            params[*k] = json!(v);
        }
        let dec = ctx.do_get("isochrone", "car", &params)?;
        Ok(rows(&dec)
            .map(|(b, i)| col_bytes(b, "polygon_wkb", i).map(|x| x.to_vec()))
            .collect())
    };
    let (mut passed, mut n, mut errors) = (true, 0usize, 0usize);
    let mut moved: HashMap<&str, usize> = options.iter().map(|(l, _)| (*l, 0)).collect();
    for (name, lon, lat) in origins {
        for direction in ["depart", "arrive"] {
            let mut base_wkb: HashMap<u64, Vec<u8>> = HashMap::new();
            for t in [600u64, 1800] {
                match rest_wkb(lon, lat, direction, "time_s", t, &[]) {
                    Ok(w) => {
                        base_wkb.insert(t, w);
                    }
                    Err(e) => {
                        errors += 1;
                        println!("  [warn] {name}/{direction}/{t}s baseline: {e}");
                    }
                }
            }
            for (label, opts) in &options {
                let tag = format!("{name}/{direction}/{label}");
                let r: GResult<()> = (|| {
                    for t in [600u64, 1800] {
                        let rw = rest_wkb(lon, lat, direction, "time_s", t, opts)?;
                        let f = flight_wkb(lon, lat, direction, "intervals", &[t], opts)?;
                        n += 1;
                        let same = f.len() == 1 && f[0].as_deref() == Some(rw.as_slice());
                        passed &= check(
                            &format!("{tag} {t}s: same bytes"),
                            same,
                            &format!(
                                "REST {} B vs Flight {} B",
                                rw.len(),
                                f.first().and_then(|x| x.as_ref()).map_or(0, |x| x.len())
                            ),
                        );
                        if base_wkb.get(&t).is_some_and(|bw| *bw != rw) {
                            *moved.get_mut(label).unwrap() += 1;
                        }
                    }
                    let multi = [600u64, 1800];
                    let f = flight_wkb(lon, lat, direction, "intervals", &multi, opts)?;
                    n += 1;
                    if f.len() != multi.len() {
                        passed &= check(
                            &format!("{tag} multi: one row per threshold"),
                            false,
                            &format!("{} rows for {} thresholds", f.len(), multi.len()),
                        );
                    } else {
                        for (row, t) in f.iter().zip(multi) {
                            let rw = rest_wkb(lon, lat, direction, "time_s", t, opts)?;
                            passed &= check(
                                &format!("{tag} multi@{t}s: same bytes"),
                                row.as_deref() == Some(rw.as_slice()),
                                &format!("{} B", row.as_ref().map_or(0, |x| x.len())),
                            );
                        }
                    }
                    let rest_refused = match rest_wkb(lon, lat, direction, "distance_m", 5000, opts)
                    {
                        Err(GateErr::Http { body, .. }) => body,
                        _ => String::new(),
                    };
                    let flight_refused =
                        match flight_wkb(lon, lat, direction, "intervals_m", &[5000], opts) {
                            Err(e) => e.to_string(),
                            Ok(_) => String::new(),
                        };
                    passed &= check(
                        &format!("{tag}: both refuse a distance threshold"),
                        rest_refused.contains("length-along-time")
                            && flight_refused.contains("length-along-time"),
                        &format!(
                            "REST '{}' / Flight '{}'",
                            trunc(&rest_refused, 60),
                            trunc(&flight_refused, 60)
                        ),
                    );
                    Ok(())
                })();
                if let Err(e) = r {
                    errors += 1;
                    passed &= check(
                        &format!("{tag}: request failed"),
                        false,
                        &trunc(&e.to_string(), 200),
                    );
                }
            }
        }
    }
    for (label, _) in &options {
        let k = moved[label];
        passed &= check(
            &format!("{label} actually moves the polygon"),
            k > 0,
            &format!(
                "{k}/{} single-contour cases differ from the same request without it",
                2 * origins.len() * 2
            ),
        );
    }
    passed &= check_errors(&ctx.t, "isochrone transports", errors, None);
    println!("  ({n} paired requests)");
    Ok(passed)
}

pub fn gate_flight_isochrone_batch(ctx: &Ctx) -> GateResult {
    println!(
        "== Flight isochrone batch == origins × intervals, same bytes as the single calls (#624) =="
    );
    let mut passed = true;
    let origins: Vec<(f64, f64)> = ISO_POINTS[..4].iter().map(|p| (p.1, p.2)).collect();
    let intervals = [300u64, 600];
    let origins_j: Vec<[f64; 2]> = origins.iter().map(|o| [o.0, o.1]).collect();
    let tb = match ctx.do_get(
        "isochrone",
        "car",
        &json!({"origins": origins_j, "intervals": intervals}),
    ) {
        Ok(d) => d,
        Err(e) => {
            return Ok(check(
                "Flight isochrone batch answers",
                false,
                &trunc(&e.to_string(), 120),
            ));
        }
    };
    let mut map: HashMap<(u64, u64), Option<Vec<u8>>> = HashMap::new();
    for (b, i) in rows(&tb) {
        if let (Some(o), Some(t)) = (col_u64(b, "origin_idx", i), col_u64(b, "interval_s", i)) {
            map.insert((o, t), col_bytes(b, "polygon_wkb", i).map(|x| x.to_vec()));
        }
    }
    let n = origins.len() * intervals.len();
    passed &= check(
        "batch: one row per origin × interval",
        map.len() == n && tb.num_rows() == map.len(),
        &format!(
            "{} rows for {}×{}",
            tb.num_rows(),
            origins.len(),
            intervals.len()
        ),
    );
    let (mut same_single, mut same_rest) = (0usize, 0usize);
    for (i, (lon, lat)) in origins.iter().enumerate() {
        let one = ctx.do_get(
            "isochrone",
            "car",
            &json!({"lon": lon, "lat": lat, "intervals": intervals}),
        )?;
        for (b, j) in rows(&one) {
            let t = col_u64(b, "interval_s", j).unwrap_or(0);
            let w = col_bytes(b, "polygon_wkb", j).map(|x| x.to_vec());
            if map.get(&(i as u64, t)) == Some(&w) {
                same_single += 1;
            }
        }
        for t in intervals {
            let q = urlencode(&[
                ("lon", pyf(*lon)),
                ("lat", pyf(*lat)),
                ("mode", "car".into()),
                ("time_s", t.to_string()),
            ]);
            let r = ctx.http.bytes(
                &ctx.url(&format!("/isochrone?{q}")),
                300,
                Some("application/octet-stream"),
            )?;
            if map.get(&(i as u64, t)) == Some(&Some(r)) {
                same_rest += 1;
            }
        }
    }
    passed &= check(
        "batch ≡ N single Flight calls, byte for byte",
        same_single == n,
        &format!("{same_single}/{n}"),
    );
    passed &= check(
        "batch ≡ REST /isochrone, byte for byte",
        same_rest == n,
        &format!("{same_rest}/{n}"),
    );
    let sea = ctx.do_get(
        "isochrone",
        "car",
        &json!({"origins": [[origins[0].0, origins[0].1], [2.5, 51.6]], "intervals": intervals}),
    )?;
    let mut nulls: Vec<u64> = rows(&sea)
        .filter(|(b, j)| col_bytes(b, "polygon_wkb", *j).is_none())
        .filter_map(|(b, j)| col_u64(b, "origin_idx", j))
        .collect();
    let n_nulls = nulls.len();
    nulls.sort_unstable();
    nulls.dedup();
    passed &= check(
        "an unsnappable origin yields NULL rows, never a drop",
        sea.num_rows() == 2 * intervals.len() && nulls == vec![1] && n_nulls == intervals.len(),
        &format!(
            "{} rows, null origins [{}]",
            sea.num_rows(),
            nulls
                .iter()
                .map(|x| x.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        ),
    );
    Ok(passed)
}

pub fn gate_catchment_containment(ctx: &Ctx) -> GateResult {
    println!("== catchment: road hull covers its percentile + nests (#536) ==");
    let mut ok = true;
    let store = (4.4025, 51.2194);
    let mut rng = PyRandom::new(536);
    let clients: Vec<Pt> = (0..300)
        .map(|_| {
            let a = store.0 + rng.uniform(-0.12, 0.12);
            let b = store.1 + rng.uniform(-0.08, 0.08);
            (a, b)
        })
        .collect();
    let n = clients.len();
    let tbl = f64_table(
        &[
            ("store_lon", vec![store.0; n]),
            ("store_lat", vec![store.1; n]),
            ("client_lon", clients.iter().map(|c| c.0).collect()),
            ("client_lat", clients.iter().map(|c| c.1).collect()),
        ],
        &[("store_id", vec!["s".to_string(); n])],
    );
    let cmd = format!(
        "catchment:car:{}",
        r#"{"percentiles": [50, 80], "hull_shape": "road", "remove_outliers": false}"#
    );
    let dec = match ctx.do_exchange(cmd.as_bytes(), tbl) {
        Ok(d) => d,
        Err(e) => {
            return Ok(check(
                "catchment road hull",
                false,
                &trunc(&e.to_string(), 100),
            ));
        }
    };
    struct Row {
        percentile: f64,
        covered: u64,
        total: u64,
        wkb: Vec<u8>,
    }
    let mut rs: Vec<Row> = rows(&dec)
        .map(|(b, i)| Row {
            percentile: col_u64(b, "percentile", i)
                .map(|x| x as f64)
                .or_else(|| super::flight::col_f64(b, "percentile", i))
                .unwrap_or(0.0),
            covered: col_u64(b, "clients_covered", i).unwrap_or(0),
            total: col_u64(b, "clients_total", i).unwrap_or(0),
            wkb: col_bytes(b, "polygon_wkb", i)
                .map(|x| x.to_vec())
                .unwrap_or_default(),
        })
        .collect();
    rs.sort_by(|a, b| a.percentile.total_cmp(&b.percentile));
    let min_v = ctx.t.catchment_min_vertices;
    let mut rings: HashMap<u64, Vec<Pt>> = HashMap::new();
    for row in &rs {
        let p = row.percentile;
        ok &= check(
            &format!("p{}: all within-threshold clients covered", f0(p)),
            row.covered == row.total && row.total > 0,
            &format!("{}/{}", row.covered, row.total),
        );
        let rr = wkb_polygons(&row.wkb);
        let nv = rr.first().and_then(|r| r.first()).map_or(0, |r| r.len());
        ok &= check(
            &format!("p{}: polygon parses + road-contour vertex count", f0(p)),
            !rr.is_empty() && nv > min_v,
            &format!("{nv} vertices (the retired sector lasso capped at 18 extremes)"),
        );
        rings.insert(
            p.round() as u64,
            rr.first()
                .and_then(|r| r.first())
                .cloned()
                .unwrap_or_default(),
        );
    }
    if let (Some(r50), Some(r80)) = (
        rings.get(&50).filter(|r| !r.is_empty()),
        rings.get(&80).filter(|r| !r.is_empty()),
    ) {
        let reach = |ring: &[Pt], bearing_deg: f64| -> f64 {
            let b = bearing_deg.to_radians();
            let (ux, uy) = (b.sin(), b.cos());
            let mx = store.1.to_radians().cos() * 111_320.0;
            ring.iter()
                .map(|v| (v.0 - store.0) * mx * ux + (v.1 - store.1) * 111_320.0 * uy)
                .fold(f64::NEG_INFINITY, f64::max)
        };
        let bad: Vec<u32> = (0..360)
            .step_by(45)
            .filter(|&br| reach(r80, br as f64) < reach(r50, br as f64) * 0.98)
            .collect();
        ok &= check(
            "nesting: p80 reach >= p50 reach in all directions",
            bad.is_empty(),
            &if bad.is_empty() {
                "8/8 directions monotone".to_string()
            } else {
                format!(
                    "violated bearings: [{}]",
                    bad.iter()
                        .map(|b| b.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            },
        );
        ok &= check(
            "nesting: area(p80) >= area(p50)",
            ring_area(r80) >= ring_area(r50),
            &format!("{} vs {}", pye2(ring_area(r80)), pye2(ring_area(r50))),
        );
    }
    Ok(ok)
}

#[allow(dead_code)]
fn _unused() {
    let _ = decode_polyline6;
    let _ = is_no_route;
    let _ = FIXTURES;
}
