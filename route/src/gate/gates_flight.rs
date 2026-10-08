//! Gates — Flight matrix / route_batch / edges_batch / completeness.
#![allow(clippy::approx_constant)]

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use serde_json::{Value, json};

use super::ctx::{Ctx, FIXTURES, ISO_POINTS, MAX_U32, check, check_errors, f0, pct};
use super::flight::{col_bytes, col_f64, col_u64, column_names, f64_table, rows};
use super::gates_rest::{GateResult, flight_matrix_cells, trunc};
use super::geom::{round_to, wkb_linestring_len_m};
use super::http::{GResult, is_no_route};
use super::pyrandom::PyRandom;

/// ONE decoded >1M-cell matrix stream: everything any consumer needs.
pub struct StreamedMatrix {
    pub batches: usize,
    pub empties: usize,
    pub rows: usize,
    pub sentinels: usize,
    pub dist_max: usize,
    pub dist_sample: Option<(u64, u64)>,
    pub cells: Vec<(u64, u64, u64, u64)>,
    pub trailer: Option<Value>,
    pub cells_total: usize,
}

fn streaming_grid() -> Vec<[f64; 2]> {
    let lons: Vec<f64> = (0..34).map(|i| 3.6 + 0.0606 * i as f64).collect();
    let lats: Vec<f64> = (0..31).map(|j| 50.50 + 0.020 * j as f64).collect();
    let mut g = Vec::with_capacity(34 * 31);
    for lo in &lons {
        for la in &lats {
            g.push([round_to(*lo, 5), round_to(*la, 5)]);
        }
    }
    g
}

pub fn streaming_params() -> Value {
    let grid = streaming_grid();
    json!({"origins": grid, "destinations": grid, "radius_km": 6, "sparse": true})
}

fn stream_matrix_once(ctx: &Ctx, mode: &str, params: &Value) -> GResult<StreamedMatrix> {
    let dec = ctx.do_get("matrix", mode, params)?;
    let (mut batches, mut empties, mut rows_n, mut sentinels, mut dist_max) =
        (0usize, 0usize, 0usize, 0usize, 0usize);
    let mut dist_sample = None;
    let mut cells = Vec::new();
    for b in &dec.batches {
        batches += 1;
        if b.num_rows() == 0 {
            empties += 1;
        }
        for k in 0..b.num_rows() {
            rows_n += 1;
            let du = col_u64(b, "duration_ms", k).unwrap_or(MAX_U32);
            let di = col_u64(b, "distance_m", k).unwrap_or(MAX_U32);
            let src = col_u64(b, "source_idx", k).unwrap_or(0);
            let tgt = col_u64(b, "target_idx", k).unwrap_or(0);
            if du == MAX_U32 {
                sentinels += 1;
                continue;
            }
            if di == MAX_U32 {
                dist_max += 1;
                if dist_sample.is_none() {
                    dist_sample = Some((src, tgt));
                }
            } else if src != tgt && cells.len() < 8 && rows_n % 137 == 0 {
                cells.push((src, tgt, du, di));
            }
        }
    }
    let n = params["origins"].as_array().map_or(0, |a| a.len())
        * params["destinations"].as_array().map_or(0, |a| a.len());
    Ok(StreamedMatrix {
        batches,
        empties,
        rows: rows_n,
        sentinels,
        dist_max,
        dist_sample,
        cells,
        trailer: dec.meta,
        cells_total: n,
    })
}

/// ONE decoded stream per (mode, params); a failure is memoised and re-raised
/// to every consumer.
pub fn streamed_matrix(
    ctx: &Ctx,
    mode: &str,
    params: &Value,
) -> Result<Arc<Result<StreamedMatrix, String>>, String> {
    let key = format!("{}|{mode}|{}", ctx.base, params);
    let mut g = ctx.streamed.lock().unwrap();
    if !g.contains_key(&key) {
        let r = stream_matrix_once(ctx, mode, params).map_err(|e| format!("GateErr: {e}"));
        g.insert(key.clone(), Arc::new(r));
    }
    let rec = Arc::clone(&g[&key]);
    match rec.as_ref() {
        Ok(_) => Ok(rec),
        Err(e) => Err(e.clone()),
    }
}

pub fn gate_bounded_matrix_exactness(ctx: &Ctx) -> GateResult {
    println!("== seeded bounded matrix == unbounded filtered (#534/#415) ==");
    let mut pts: Vec<[f64; 2]> = ISO_POINTS.iter().map(|p| [p.1, p.2]).collect();
    pts.extend(FIXTURES.iter().map(|f| [f.3, f.4]));
    let matrix = |mode: &str, max_minutes: Option<u64>| -> GResult<HashMap<(u64, u64), u64>> {
        let mut params = json!({"origins": pts, "destinations": pts});
        if let Some(m) = max_minutes {
            params["max_minutes"] = json!(m);
        }
        Ok(flight_matrix_cells(ctx, mode, &params)?.0)
    };
    let mut passed = true;
    for mode in ["car", "foot"] {
        let unb = matrix(mode, None)?;
        let t_min = 15u64;
        let thr_ms = t_min * 60 * 1000;
        let bnd = matrix(mode, Some(t_min))?;
        let in_bound: HashMap<(u64, u64), u64> = unb
            .iter()
            .filter(|(_, v)| **v != MAX_U32 && **v <= thr_ms)
            .map(|(k, v)| (*k, *v))
            .collect();
        let missing = in_bound
            .keys()
            .filter(|k| *bnd.get(k).unwrap_or(&MAX_U32) == MAX_U32)
            .count();
        let wrong = in_bound
            .iter()
            .filter(|(k, v)| *bnd.get(k).unwrap_or(&MAX_U32) != MAX_U32 && bnd[k] != **v)
            .count();
        passed &= check(
            &format!("{mode}: fixture exercises the bound"),
            !in_bound.is_empty(),
            &format!("{} cells ≤ {t_min}min", in_bound.len()),
        );
        passed &= check(
            &format!("{mode}: no in-bound cell falsely dropped"),
            missing == 0,
            &format!("{missing} in-bound cells came back u32::MAX (#534 forward-bound bug)"),
        );
        passed &= check(
            &format!("{mode}: in-bound values identical to unbounded"),
            wrong == 0,
            &format!("{wrong} cells differ from the unbounded value"),
        );
    }
    Ok(passed)
}

pub fn gate_matrix_distance_consistency(ctx: &Ctx) -> GateResult {
    println!("== streamed matrix distance_m computed (not column-wide MAX) (#534) ==");
    let params = streaming_params();
    let grid = streaming_grid();
    let cell_tol = ctx.t.matrix_cell_tol;
    let mut passed = true;
    for mode in ["car", "foot"] {
        let rec = match streamed_matrix(ctx, mode, &params) {
            Ok(r) => r,
            Err(e) => {
                passed &= check(&format!("{mode}: streamed matrix decoded"), false, &e);
                continue;
            }
        };
        let rec = rec.as_ref().as_ref().unwrap();
        passed &= check(
            &format!("{mode}: streamed path returns rows"),
            rec.rows > 1000,
            &format!("{} reachable rows over {}² cells", rec.rows, grid.len()),
        );
        passed &= check(
            &format!("{mode}: every reachable cell has a distance"),
            rec.dist_max == 0,
            &format!(
                "{}/{} rows have duration but distance_m==MAX (#534 column-wide MAX){}",
                rec.dist_max,
                rec.rows,
                rec.dist_sample
                    .map(|(s, t)| format!(" e.g. ({s}, {t})"))
                    .unwrap_or_default()
            ),
        );
        let (mut bad, mut worst) = (0usize, 0.0f64);
        for &(si, ti, dur_ms, dist_m) in &rec.cells {
            let (o, d) = (grid[si as usize], grid[ti as usize]);
            let Ok((dur_r, dist_r)) = ctx.route(o[0], o[1], d[0], d[1], mode) else {
                continue;
            };
            let dur_ok = (dur_ms as f64 / 1000.0 - dur_r).abs() <= (dur_r * cell_tol).max(1.0);
            let dist_ok = (dist_m as f64 - dist_r).abs() <= (dist_r * cell_tol).max(5.0);
            worst = worst.max((dist_m as f64 - dist_r).abs() / dist_r.max(1.0));
            if !(dur_ok && dist_ok) {
                bad += 1;
            }
        }
        passed &= check(
            &format!("{mode}: streamed cell values == /route"),
            bad == 0 && !rec.cells.is_empty(),
            &format!(
                "{} cells checked, {bad} mismatch (worst dist {:.2}%)",
                rec.cells.len(),
                worst * 100.0
            ),
        );
    }
    Ok(passed)
}

pub fn gate_matrix_sparse(ctx: &Ctx) -> GateResult {
    println!("== Flight matrix sparse == dense minus sentinels (#532) ==");
    let pts: Vec<[f64; 2]> = ISO_POINTS.iter().map(|p| [p.1, p.2]).collect();
    let fetch = |sparse: bool| {
        flight_matrix_cells(
            ctx,
            "car",
            &json!({"origins": pts, "destinations": pts, "radius_km": 20, "sparse": sparse}),
        )
    };
    let (dense, dense_n) = fetch(false)?;
    let (sp, _sp_n) = fetch(true)?;
    let n = pts.len();
    let dense_real: HashMap<(u64, u64), u64> = dense
        .iter()
        .filter(|(_, v)| **v != MAX_U32)
        .map(|(k, v)| (*k, *v))
        .collect();
    let mut passed = check(
        "dense is full grid",
        dense_n == n * n,
        &format!("{dense_n} rows (expect {})", n * n),
    );
    passed &= check(
        "fixture actually prunes",
        !dense_real.is_empty() && dense_real.len() < dense_n,
        &format!(
            "{} sentinels, {} real of {dense_n}",
            dense_n - dense_real.len(),
            dense_real.len()
        ),
    );
    let leaked = sp.values().filter(|&&v| v == MAX_U32).count();
    passed &= check(
        "sparse emits no sentinels",
        leaked == 0,
        &format!("{leaked} sentinel rows leaked"),
    );
    let sk: HashSet<&(u64, u64)> = sp.keys().collect();
    let dk: HashSet<&(u64, u64)> = dense_real.keys().collect();
    passed &= check(
        "sparse keys == dense non-sentinel keys",
        sk == dk,
        &format!("sparse {} vs dense-real {}", sp.len(), dense_real.len()),
    );
    passed &= check(
        "sparse values identical to dense",
        dense_real.iter().all(|(k, v)| sp.get(k) == Some(v)),
        "all surviving pairs match dense",
    );
    Ok(passed)
}

pub fn gate_matrix_sparse_streaming(ctx: &Ctx) -> GateResult {
    println!("== Flight matrix sparse STREAMING path (>1M cells, #532) ==");
    let params = streaming_params();
    let rec = match streamed_matrix(ctx, "car", &params) {
        Ok(r) => r,
        Err(e) => return Ok(check("streamed matrix decoded", false, &e)),
    };
    let rec = rec.as_ref().as_ref().unwrap();
    let cells = rec.cells_total;
    let mut passed = check(
        "took the streaming path",
        rec.batches > 1,
        &format!("{} batches for {cells} cells", rec.batches),
    );
    passed &= check(
        "no sentinels streamed",
        rec.sentinels == 0,
        &format!("{} sentinel rows", rec.sentinels),
    );
    passed &= check(
        "no empty batches streamed",
        rec.empties == 0,
        &format!("{} empty batches", rec.empties),
    );
    passed &= check(
        "sparse << dense",
        rec.rows > 0 && rec.rows < cells / 2,
        &format!(
            "{} rows of {cells} cells ({:.1}% dropped)",
            rec.rows,
            100.0 * (1.0 - rec.rows as f64 / cells as f64)
        ),
    );
    Ok(passed)
}

fn meta_str(meta: &Option<Value>) -> String {
    match meta {
        None => "None".to_string(),
        Some(v) => pydict(v),
    }
}

/// Python's dict repr for a flat JSON object (sorted keys are not required;
/// serde preserves the server's order like Python's json.loads).
/// The order the engine writes its trailer keys in (`serde_json` without
/// `preserve_order` sorts them; Python keeps the wire order).
const TRAILER_KEY_ORDER: &[&str] = &[
    "complete",
    "total_rows",
    "contract",
    "plan",
    "n_pairs",
    "n_unreachable",
    "total_weight_in",
    "total_weight_assigned",
];

fn pydict(v: &Value) -> String {
    match v {
        Value::Object(m) => {
            let mut keys: Vec<&String> = m.keys().collect();
            keys.sort_by_key(|k| {
                TRAILER_KEY_ORDER
                    .iter()
                    .position(|x| x == k)
                    .unwrap_or(usize::MAX)
            });
            format!(
                "{{{}}}",
                keys.iter()
                    .map(|k| format!("'{k}': {}", pyval(&m[*k])))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        }
        other => pyval(other),
    }
}

fn pyval(v: &Value) -> String {
    match v {
        Value::Null => "None".into(),
        Value::Bool(b) => {
            if *b {
                "True".into()
            } else {
                "False".into()
            }
        }
        Value::String(s) => format!("'{s}'"),
        Value::Array(a) => format!("[{}]", a.iter().map(pyval).collect::<Vec<_>>().join(", ")),
        Value::Object(_) => pydict(v),
        Value::Number(n) => n.to_string(),
    }
}

pub fn gate_flight_completeness(ctx: &Ctx) -> GateResult {
    println!(
        "== Flight completeness trailer: matrix (dense/sparse/streaming), route_batch, edges_batch, isochrone, edges_flow (#533/#532) =="
    );
    let pts: Vec<[f64; 2]> = ISO_POINTS.iter().map(|p| [p.1, p.2]).collect();
    let pairs: Vec<[f64; 4]> = FIXTURES.iter().map(|f| [f.1, f.2, f.3, f.4]).collect();
    let judge =
        |label: &str, rows_n: usize, meta: &Option<Value>, want_contract: Option<&str>| -> bool {
            let ms = meta_str(meta);
            let mut p = check(
                &format!("{label}: trailer present"),
                meta.is_some(),
                &format!("meta={ms}"),
            );
            p &= check(
                &format!("{label}: complete:true"),
                meta.as_ref()
                    .and_then(|m| m.get("complete"))
                    .and_then(Value::as_bool)
                    == Some(true),
                &format!("meta={ms}"),
            );
            p &= check(
                &format!("{label}: total_rows=={rows_n} decoded"),
                meta.as_ref()
                    .and_then(|m| m.get("total_rows"))
                    .and_then(Value::as_u64)
                    == Some(rows_n as u64),
                &format!("meta={ms}"),
            );
            if let Some(w) = want_contract {
                p &= check(
                    &format!("{label}: contract={w}"),
                    meta.as_ref()
                        .and_then(|m| m.get("contract"))
                        .and_then(Value::as_str)
                        == Some(w),
                    &format!("meta={ms}"),
                );
            }
            p
        };
    let probe = |label: &str,
                 action: &str,
                 params: Value,
                 mode: &str,
                 want: Option<&str>|
     -> GResult<bool> {
        let dec = ctx.do_get(action, mode, &params)?;
        Ok(judge(label, dec.num_rows(), &dec.meta, want))
    };
    let mut passed = probe(
        "matrix dense",
        "matrix",
        json!({"origins": pts, "destinations": pts}),
        "car",
        Some("dense"),
    )?;
    passed &= probe(
        "matrix sparse",
        "matrix",
        json!({"origins": pts, "destinations": pts, "radius_km": 20, "sparse": true}),
        "car",
        Some("sparse"),
    )?;
    match streamed_matrix(ctx, "car", &streaming_params()) {
        Ok(rec) => {
            let rec = rec.as_ref().as_ref().unwrap();
            passed &= judge(
                "matrix streaming sparse",
                rec.rows,
                &rec.trailer,
                Some("sparse"),
            );
        }
        Err(e) => passed &= check("matrix streaming sparse: trailer present", false, &e),
    }
    passed &= probe(
        "route_batch",
        "route_batch",
        json!({"pairs": pairs}),
        "car",
        None,
    )?;
    passed &= probe(
        "edges_batch",
        "edges_batch",
        json!({"pairs": pairs}),
        "car",
        None,
    )?;
    let iso = json!({"lon": ISO_POINTS[0].1, "lat": ISO_POINTS[0].2, "intervals": [600]});
    passed &= probe("isochrone", "isochrone", iso.clone(), "car", None)?;
    if ctx.bands_served() {
        let mut ib = iso.clone();
        ib["uncertainty"] = json!("bands");
        passed &= probe("isochrone bands", "isochrone", ib, "car", None)?;
    } else {
        super::ctx::skip(
            "isochrone bands: /health reports bands:false — the loaded speeds table has no best/worst columns (no table is staged since the licensed provider's data was retired, 2026-09-28); every uncertainty=bands request answers 400",
        );
    }
    let tbl = f64_table(
        &[
            ("src_lon", pairs.iter().map(|p| p[0]).collect()),
            ("src_lat", pairs.iter().map(|p| p[1]).collect()),
            ("dst_lon", pairs.iter().map(|p| p[2]).collect()),
            ("dst_lat", pairs.iter().map(|p| p[3]).collect()),
        ],
        &[],
    );
    let dec = ctx.do_exchange(b"edges_flow:car", tbl)?;
    passed &= check(
        "edges_flow: complete:true summary",
        dec.meta
            .as_ref()
            .and_then(|m| m.get("complete"))
            .and_then(Value::as_bool)
            == Some(true),
        &format!("meta={}", meta_str(&dec.meta)),
    );
    Ok(passed)
}

pub fn gate_edges_batch(ctx: &Ctx) -> GateResult {
    println!("== edges_batch vs /route (ticket fixtures) ==");
    let pairs: Vec<[f64; 4]> = FIXTURES.iter().map(|f| [f.1, f.2, f.3, f.4]).collect();
    let dec = ctx.do_get("edges_batch", "car", &json!({"pairs": pairs}))?;
    let mut sums: HashMap<u64, f64> = HashMap::new();
    for (b, i) in rows(&dec) {
        if let (Some(k), Some(du)) = (col_u64(b, "query_idx", i), col_u64(b, "duration_ms", i)) {
            *sums.entry(k).or_insert(0.0) += du as f64 / 1000.0;
        }
    }
    let (lo, hi) = ctx.t.edges_sum_bounds;
    let mut passed = true;
    for (idx, f) in FIXTURES.iter().enumerate() {
        let got = sums.get(&(idx as u64)).copied();
        let (exp, _) = ctx.route(f.1, f.2, f.3, f.4, "car")?;
        let ok = got.is_some_and(|g| exp * lo <= g && g <= exp * hi);
        passed &= check(
            &format!("{} edges", f.0),
            ok,
            &match got {
                Some(g) if g != 0.0 => format!("sum {}s (route {}s)", f0(g), f0(exp)),
                _ => "no rows".to_string(),
            },
        );
    }
    Ok(passed)
}

fn pick_col<'a>(names: &'a [String], a: &'a str, b: &'a str) -> &'a str {
    if names.iter().any(|n| n == a) { a } else { b }
}

pub fn gate_route_batch_geometry(ctx: &Ctx) -> GateResult {
    println!("== route_batch foot/bike geometry_wkb ≈ distance (#493) ==");
    let pairs: Vec<[f64; 4]> = FIXTURES.iter().map(|f| [f.1, f.2, f.3, f.4]).collect();
    let tol = ctx.t.wkb_len_tol;
    let mut passed = true;
    for mode in ["foot", "bike"] {
        let dec = ctx.do_get("route_batch", mode, &json!({"pairs": pairs}))?;
        let names = column_names(&dec);
        let dist_col = pick_col(&names, "distance_m", "distance_meters");
        let wkb_col = pick_col(&names, "geometry_wkb", "polyline_wkb");
        if !names.iter().any(|n| n == dist_col) || !names.iter().any(|n| n == wkb_col) {
            passed &= check(
                &format!("{mode} schema"),
                false,
                &format!(
                    "cols=[{}]",
                    names
                        .iter()
                        .map(|n| format!("'{n}'"))
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            );
            continue;
        }
        let (mut bad, mut worst) = (0usize, 0.0f64);
        for (b, i) in rows(&dec) {
            let (Some(d), Some(w)) = (col_f64(b, dist_col, i), col_bytes(b, wkb_col, i)) else {
                continue;
            };
            if d <= 0.0 {
                continue;
            }
            let Some(glen) = wkb_linestring_len_m(w) else {
                continue;
            };
            worst = worst.max((glen / d - 1.0).abs());
            if (glen - d).abs() > d * tol {
                bad += 1;
            }
        }
        passed &= check(
            &format!("{mode}: wkb length ≈ distance_m"),
            bad == 0,
            &format!(
                "{bad} rows off >{}% (worst {:.1}%)",
                f0(tol * 100.0),
                worst * 100.0
            ),
        );
    }
    Ok(passed)
}

fn short_shared_snap_pairs(seed: u32, n_per_centre: usize) -> Vec<[f64; 4]> {
    let mut rng = PyRandom::new(seed);
    let mut pairs = Vec::new();
    for p in ISO_POINTS {
        let (clon, clat) = (p.1, p.2);
        for _ in 0..n_per_centre {
            let lon = clon + rng.uniform(-0.03, 0.03);
            let lat = clat + rng.uniform(-0.02, 0.02);
            let d_m = rng.uniform(20.0, 1200.0);
            let a = rng.uniform(0.0, 2.0 * std::f64::consts::PI);
            pairs.push([
                round_to(lon, 6),
                round_to(lat, 6),
                round_to(
                    lon + d_m * a.cos() / (111_320.0 * lat.to_radians().cos()),
                    6,
                ),
                round_to(lat + d_m * a.sin() / 110_540.0, 6),
            ]);
        }
    }
    pairs
}

pub fn gate_route_batch_agrees_with_route(ctx: &Ctx) -> GateResult {
    println!("== route_batch == /route, same pair, same answer (#605) ==");
    let pairs = short_shared_snap_pairs(605, 14);
    let mut passed = true;
    for mode in ["car", "foot", "bike"] {
        let dec = ctx.do_get("route_batch", mode, &json!({"pairs": pairs}))?;
        let names = column_names(&dec);
        let dc = pick_col(&names, "distance_m", "distance_meters");
        let du = pick_col(&names, "duration_s", "duration_seconds");
        let mut bad: Vec<(u64, f64, f64, f64, f64)> = Vec::new();
        let (mut shared, mut compared, mut errors) = (0usize, 0usize, 0usize);
        for (b, i) in rows(&dec) {
            let Some(pi) = col_u64(b, "pair_idx", i) else {
                continue;
            };
            let p = pairs[pi as usize];
            let r = match ctx.route_json(
                p[0],
                p[1],
                p[2],
                p[3],
                mode,
                60,
                &[("debug", "true".into())],
            ) {
                Ok(r) => r,
                Err(e) => {
                    if !is_no_route(&e) {
                        errors += 1;
                    }
                    continue;
                }
            };
            let dbg = r.get("debug").cloned().unwrap_or(Value::Null);
            if dbg.pointer("/src_snapped/ebg_node_id").is_some()
                && dbg.pointer("/src_snapped/ebg_node_id")
                    == dbg.pointer("/dst_snapped/ebg_node_id")
            {
                shared += 1;
            }
            compared += 1;
            let (Some(bd), Some(bm)) = (col_f64(b, du, i), col_f64(b, dc, i)) else {
                continue;
            };
            let (rd, rm) = (
                r.get("duration_s")
                    .and_then(Value::as_f64)
                    .unwrap_or(f64::NAN),
                r.get("distance_m")
                    .and_then(Value::as_f64)
                    .unwrap_or(f64::NAN),
            );
            if (bd - rd).abs() > 0.5 || (bm - rm).abs() > 1.0 {
                bad.push((pi, rd, rm, bd, bm));
            }
        }
        passed &= check(
            &format!("{mode}: same duration and distance"),
            bad.is_empty(),
            &format!(
                "{}/{compared} pairs disagree{}",
                bad.len(),
                bad.first()
                    .map(|b| format!(
                        " (worst: /route {}s {}m vs batch {}s {}m)",
                        f0(b.1),
                        f0(b.2),
                        f0(b.3),
                        f0(b.4)
                    ))
                    .unwrap_or_default()
            ),
        );
        passed &= check(
            &format!("{mode}: sample still hits the shared-snap case"),
            shared > 0,
            &format!("{shared}/{compared} pairs snap both ends to one edge"),
        );
        passed &= check_errors(&ctx.t, mode, errors, None);
    }
    Ok(passed)
}

pub fn gate_route_batch_max_meters(ctx: &Ctx) -> GateResult {
    println!("== route_batch max_meters prune == unbounded ≤ B (#482/#487) ==");
    let mut rng = PyRandom::new(482);
    let pairs: Vec<[f64; 4]> = (0..120)
        .map(|_| {
            let lon = rng.uniform(3.6, 5.6);
            let lat = rng.uniform(50.5, 51.1);
            let dd = rng.uniform(0.01, 0.06);
            let a = rng.uniform(0.0, 6.283);
            [
                lon,
                lat,
                round_to(lon + dd * a.cos(), 6),
                round_to(lat + dd * a.sin(), 6),
            ]
        })
        .collect();
    let run = |extra: Option<f64>| -> GResult<HashMap<u64, f64>> {
        let mut params = json!({"pairs": pairs});
        if let Some(b) = extra {
            params["max_meters"] = json!(b);
        }
        let dec = ctx.do_get("route_batch", "car", &params)?;
        let names = column_names(&dec);
        let dc = pick_col(&names, "distance_m", "distance_meters");
        Ok(rows(&dec)
            .filter_map(|(b, i)| Some((col_u64(b, "pair_idx", i)?, col_f64(b, dc, i)?)))
            .collect())
    };
    let unb = run(None)?;
    let vals: Vec<f64> = unb.values().copied().collect();
    let bound = pct(&vals, 0.5).floor();
    let bnd = run(Some(bound))?;
    let expected: BTreeSet<u64> = unb
        .iter()
        .filter(|(_, v)| **v <= bound)
        .map(|(k, _)| *k)
        .collect();
    let got: BTreeSet<u64> = bnd.keys().copied().collect();
    let over = bnd.values().filter(|&&v| v > bound).count();
    let mut passed = check(
        "bound actually prunes",
        !got.is_empty() && got.len() < unb.len(),
        &format!("{}/{} kept (B={}m)", got.len(), unb.len(), f0(bound)),
    );
    passed &= check(
        "bounded set == unbounded ≤ B",
        got == expected,
        &format!("got {} vs expected {}", got.len(), expected.len()),
    );
    passed &= check(
        "every returned pair ≤ B",
        over == 0,
        &format!("{over} over-bound leaked"),
    );
    Ok(passed)
}

#[allow(dead_code)]
fn _unused(s: &str) -> String {
    trunc(s, 1)
}
