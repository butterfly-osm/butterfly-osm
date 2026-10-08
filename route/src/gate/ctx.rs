//! Shared run context: thresholds, the HTTP and Flight clients, the memoised
//! fetches (snap, isochrone bundles, reference routes, streamed matrices) and
//! the `check` printer whose lines ARE the gate's output contract.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use serde_json::{Value, json};

use super::flight::{Decoded, Flights};
use super::geom::{Pt, decode_polyline6, wkb_polygons};
use super::http::{GResult, GateErr, Http, pyf, urlencode};

pub const MAX_U32: u64 = 4_294_967_295;

// ---------------------------------------------------------------------------
// Thresholds — ONE table, printed sorted so a PASS line reads against its bound.
// ---------------------------------------------------------------------------
#[derive(Clone, Debug)]
pub struct Thresholds {
    pub never_fast: f64,
    pub tol: f64,
    pub slack_level: f64,
    pub slack_regional: f64,
    pub dur_p90_max: f64,
    pub choice_divergent_frac: f64,
    pub dist_p50: (f64, f64),
    pub dist_p90_max: f64,
    pub dist_outliers_frac: f64,
    pub like_for_like_km_tol: f64,
    pub symmetry_ratio_max: f64,
    pub symmetry_violations_max: usize,
    pub consistency_tolerance_s: f64,
    pub close_pair_mismatch_max: usize,
    pub max_errors: usize,
    pub band_spread_min: f64,
    pub band_min_trips: usize,
    pub band_min_regional: usize,
    pub iso_reach_slack: f64,
    pub iso_nest_tol: f64,
    pub topology_outside_m: f64,
    pub topology_outside_frac: f64,
    pub reach_in_tol: f64,
    pub iso_len_in_over_max: usize,
    pub reach_in_over_frac: f64,
    pub reach_out_tol: f64,
    pub reach_out_frac: f64,
    pub pin_near_ring_m: f64,
    pub pin_snap_max_m: f64,
    pub geom_consistency_tol: f64,
    pub ann_duration_tol: f64,
    pub sentinel_max_detour: f64,
    pub car_speed_kmh: (f64, f64),
    pub foot_speed_kmh: (f64, f64),
    pub bike_speed_kmh: (f64, f64),
    pub motorway_floor_kmh: f64,
    pub exclude_corridor_class_share_min: f64,
    pub car_foot_detour_max: f64,
    pub car_foot_holes_max: usize,
    pub matrix_cell_tol: f64,
    pub wkb_len_tol: f64,
    pub edges_sum_bounds: (f64, f64),
    pub catchment_min_vertices: usize,
    // derived
    pub band_level: (f64, f64),
    pub band_regional: (f64, f64),
    pub dur_p50: (f64, f64),
}

impl Default for Thresholds {
    fn default() -> Self {
        let mut t = Thresholds {
            never_fast: 0.98,
            tol: 0.06,
            slack_level: 0.03,
            slack_regional: 0.06,
            dur_p90_max: 1.30,
            choice_divergent_frac: 0.25,
            dist_p50: (0.97, 1.06),
            dist_p90_max: 1.20,
            dist_outliers_frac: 0.08,
            like_for_like_km_tol: 0.10,
            symmetry_ratio_max: 1.5,
            symmetry_violations_max: 0,
            consistency_tolerance_s: 3.0,
            close_pair_mismatch_max: 2,
            max_errors: 5,
            band_spread_min: 1.10,
            band_min_trips: 100,
            band_min_regional: 10,
            iso_reach_slack: 1.20,
            iso_nest_tol: 0.98,
            topology_outside_m: 150.0,
            topology_outside_frac: 0.015,
            reach_in_tol: 1.02,
            iso_len_in_over_max: 0,
            reach_in_over_frac: 0.01,
            reach_out_tol: 0.99,
            reach_out_frac: 0.005,
            pin_near_ring_m: 30.0,
            pin_snap_max_m: 300.0,
            geom_consistency_tol: 0.03,
            ann_duration_tol: 0.15,
            sentinel_max_detour: 8.0,
            car_speed_kmh: (15.0, 135.0),
            foot_speed_kmh: (2.0, 8.0),
            bike_speed_kmh: (5.0, 32.0),
            motorway_floor_kmh: 50.0,
            exclude_corridor_class_share_min: 0.25,
            car_foot_detour_max: 3.0,
            car_foot_holes_max: 2,
            matrix_cell_tol: 0.02,
            wkb_len_tol: 0.05,
            edges_sum_bounds: (0.9, 1.45),
            catchment_min_vertices: 50,
            band_level: (0.0, 0.0),
            band_regional: (0.0, 0.0),
            dur_p50: (0.0, 0.0),
        };
        t.derive_level_bounds();
        t
    }
}

fn round3(x: f64) -> f64 {
    super::geom::round_to(x, 3)
}

impl Thresholds {
    /// `tol` is stored, the three level bounds are derived — never the reverse.
    pub fn derive_level_bounds(&mut self) {
        let lo = self.never_fast;
        self.band_level = (lo, round3(1.0 + self.tol + self.slack_level));
        self.band_regional = (lo, round3(1.0 + self.tol + self.slack_regional));
        self.dur_p50 = self.band_regional;
    }

    /// `windows.json` beside the reference sets overrides the level
    /// tolerances; an unreadable file is fatal (a broken config must not
    /// silently loosen the gate).
    pub fn apply_windows(&mut self, refs_dir: Option<&Path>) {
        let Some(dir) = refs_dir else {
            return;
        };
        let path = dir.join("windows.json");
        if !path.exists() {
            return;
        }
        let w: Value = match std::fs::read(&path)
            .map_err(|e| e.to_string())
            .and_then(|b| serde_json::from_slice(&b).map_err(|e| e.to_string()))
        {
            Ok(v) => v,
            Err(e) => {
                eprintln!("windows.json at {} is unreadable: {e}", path.display());
                std::process::exit(2);
            }
        };
        let f = |k: &str| w.get(k).and_then(Value::as_f64);
        if let Some(v) = f("never_fast") {
            self.never_fast = v;
        }
        if let Some(v) = f("tol") {
            self.tol = v;
        }
        if let Some(v) = f("slack_level") {
            self.slack_level = v;
        }
        if let Some(v) = f("slack_regional") {
            self.slack_regional = v;
        }
        if let Some(v) = f("match_tol") {
            self.like_for_like_km_tol = v;
        }
        self.derive_level_bounds();
        println!(
            "[windows] {}: never_fast {}, tol {}, slack {}/{}, match_tol {} → dur_p50 {}, band_level {}, band_regional {}",
            path.display(),
            pyf(self.never_fast),
            pyf(self.tol),
            pyf(self.slack_level),
            pyf(self.slack_regional),
            pyf(self.like_for_like_km_tol),
            tup(self.dur_p50),
            tup(self.band_level),
            tup(self.band_regional)
        );
    }

    /// Every resolved threshold, sorted by name, Python-repr style.
    pub fn print(&self) {
        println!("thresholds:");
        let mut rows: Vec<(&str, String)> = vec![
            ("ann_duration_tol", pyf(self.ann_duration_tol)),
            ("band_level", tup(self.band_level)),
            ("band_min_regional", self.band_min_regional.to_string()),
            ("band_min_trips", self.band_min_trips.to_string()),
            ("band_regional", tup(self.band_regional)),
            ("band_spread_min", pyf(self.band_spread_min)),
            ("bike_speed_kmh", tup(self.bike_speed_kmh)),
            ("car_foot_detour_max", pyf(self.car_foot_detour_max)),
            ("car_foot_holes_max", self.car_foot_holes_max.to_string()),
            ("car_speed_kmh", tup(self.car_speed_kmh)),
            (
                "catchment_min_vertices",
                self.catchment_min_vertices.to_string(),
            ),
            ("choice_divergent_frac", pyf(self.choice_divergent_frac)),
            (
                "close_pair_mismatch_max",
                self.close_pair_mismatch_max.to_string(),
            ),
            ("consistency_tolerance_s", pyf(self.consistency_tolerance_s)),
            ("dist_outliers_frac", pyf(self.dist_outliers_frac)),
            ("dist_p50", tup(self.dist_p50)),
            ("dist_p90_max", pyf(self.dist_p90_max)),
            ("dur_p50", tup(self.dur_p50)),
            ("dur_p90_max", pyf(self.dur_p90_max)),
            ("edges_sum_bounds", tup(self.edges_sum_bounds)),
            (
                "exclude_corridor_class_share_min",
                pyf(self.exclude_corridor_class_share_min),
            ),
            ("foot_speed_kmh", tup(self.foot_speed_kmh)),
            ("geom_consistency_tol", pyf(self.geom_consistency_tol)),
            ("iso_len_in_over_max", self.iso_len_in_over_max.to_string()),
            ("iso_nest_tol", pyf(self.iso_nest_tol)),
            ("iso_reach_slack", pyf(self.iso_reach_slack)),
            ("like_for_like_km_tol", pyf(self.like_for_like_km_tol)),
            ("matrix_cell_tol", pyf(self.matrix_cell_tol)),
            ("max_errors", self.max_errors.to_string()),
            ("motorway_floor_kmh", pyf(self.motorway_floor_kmh)),
            ("never_fast", pyf(self.never_fast)),
            ("pin_near_ring_m", pyf(self.pin_near_ring_m)),
            ("pin_snap_max_m", pyf(self.pin_snap_max_m)),
            ("reach_in_over_frac", pyf(self.reach_in_over_frac)),
            ("reach_in_tol", pyf(self.reach_in_tol)),
            ("reach_out_frac", pyf(self.reach_out_frac)),
            ("reach_out_tol", pyf(self.reach_out_tol)),
            ("sentinel_max_detour", pyf(self.sentinel_max_detour)),
            ("slack_level", pyf(self.slack_level)),
            ("slack_regional", pyf(self.slack_regional)),
            ("symmetry_ratio_max", pyf(self.symmetry_ratio_max)),
            (
                "symmetry_violations_max",
                self.symmetry_violations_max.to_string(),
            ),
            ("tol", pyf(self.tol)),
            ("topology_outside_frac", pyf(self.topology_outside_frac)),
            ("topology_outside_m", pyf(self.topology_outside_m)),
            ("wkb_len_tol", pyf(self.wkb_len_tol)),
        ];
        rows.sort_by(|a, b| a.0.cmp(b.0));
        for (k, v) in rows {
            println!("  {k} = {v}");
        }
    }
}

pub fn tup(t: (f64, f64)) -> String {
    format!("({}, {})", pyf(t.0), pyf(t.1))
}

// ---------------------------------------------------------------------------
// check — the output contract
// ---------------------------------------------------------------------------
pub fn check(name: &str, ok: bool, detail: &str) -> bool {
    println!("  [{}] {name}: {detail}", if ok { "PASS" } else { "FAIL" });
    ok
}

pub fn skip(detail: &str) {
    println!("  [SKIP] {detail}");
}

pub fn check_errors(t: &Thresholds, label: &str, errors: usize, unroutable: Option<usize>) -> bool {
    let extra = match unroutable {
        Some(u) => format!(", {u} unroutable (not an error)"),
        None => String::new(),
    };
    check(
        &format!("{label}: request errors"),
        errors <= t.max_errors,
        &format!("{errors} (max {}){extra}", t.max_errors),
    )
}

/// `pct(xs, q)`: sorted, index `min(int(len*q), len-1)`.
pub fn pct(xs: &[f64], q: f64) -> f64 {
    let mut v = xs.to_vec();
    v.sort_by(|a, b| a.total_cmp(b));
    let i = ((v.len() as f64 * q) as usize).min(v.len().saturating_sub(1));
    v[i]
}

pub fn mean(xs: &[f64]) -> f64 {
    xs.iter().sum::<f64>() / xs.len() as f64
}

/// `statistics.median`: mean of the two middle values for even n.
pub fn median(xs: &[f64]) -> f64 {
    let mut v = xs.to_vec();
    v.sort_by(|a, b| a.total_cmp(b));
    let n = v.len();
    if n % 2 == 1 {
        v[n / 2]
    } else {
        (v[n / 2 - 1] + v[n / 2]) / 2.0
    }
}

/// (count, share) of ratios outside [lo, hi].
pub fn outlier_frac(ratios: &[f64], lo: f64, hi: f64) -> (usize, f64) {
    let n = ratios.iter().filter(|&&r| r < lo || r > hi).count();
    (
        n,
        if ratios.is_empty() {
            0.0
        } else {
            n as f64 / ratios.len() as f64
        },
    )
}

/// Python's `f"{x:.0f}"` etc.: `format!("{:.0}")` rounds half to even on the
/// exact binary value, as CPython does.
pub fn f0(x: f64) -> String {
    format!("{x:.0}")
}

// ---------------------------------------------------------------------------
// Reference sets
// ---------------------------------------------------------------------------
pub const DEFAULT_TRIPS: &str = "od_typical.csv";
pub const LEGACY_TRIPS_DISTANCE: &str = "od.csv";
pub const REFS_PREFIX: &str = "od";
pub const REFS_RETIRED_REASON: &str = "no reference trip sets (BUTTERFLY_REFS_DIR unset): the licensed provider's historic times were retired on 2026-09-28 — the level, bands and route-choice gates need a clean reference set to run; export BUTTERFLY_REFS_DIR to the directory holding one";

#[derive(Debug)]
pub enum RefsErr {
    /// `$BUTTERFLY_REFS_DIR` unset: SKIP by name.
    Retired(String),
    /// Set but not a directory: FAIL by name.
    Unavailable(String),
}

impl std::fmt::Display for RefsErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RefsErr::Retired(s) | RefsErr::Unavailable(s) => write!(f, "{s}"),
        }
    }
}

pub fn require_refs_dir(refs_dir: Option<&Path>) -> Result<PathBuf, RefsErr> {
    match refs_dir {
        None => Err(RefsErr::Retired(format!(
            "{REFS_RETIRED_REASON} ({REFS_PREFIX}_{{typical,best,worst}}.csv, {LEGACY_TRIPS_DISTANCE}, optional windows.json)."
        ))),
        Some(d) if !d.is_dir() => Err(RefsErr::Unavailable(format!(
            "BUTTERFLY_REFS_DIR={:?} is not a directory.",
            d.display().to_string()
        ))),
        Some(d) => Ok(d.to_path_buf()),
    }
}

/// `refs_path(name, override)`: an override wins as typed, else under the refs dir.
pub fn refs_path(
    refs_dir: Option<&Path>,
    name: &str,
    override_: Option<&str>,
) -> Result<String, RefsErr> {
    if let Some(o) = override_ {
        return Ok(o.to_string());
    }
    Ok(require_refs_dir(refs_dir)?
        .join(name)
        .to_string_lossy()
        .into_owned())
}

// ---------------------------------------------------------------------------
// Fixtures / origins
// ---------------------------------------------------------------------------
pub const FIXTURES: &[(&str, f64, f64, f64, f64)] = &[
    ("Berloz #503", 5.211554, 50.709124, 5.211383, 50.698323),
    ("Heers #503", 5.307080, 50.751610, 5.293005, 50.752418),
    ("Robertville #502", 6.008464, 50.428652, 6.022535, 50.428452),
];

pub const ISO_POINTS: &[(&str, f64, f64)] = &[
    ("Brussels", 4.3517, 50.8503),
    ("Antwerp", 4.4025, 51.2194),
    ("Rixensart", 4.5286, 50.7115),
    ("Robertville #502", 6.008464, 50.428652),
    ("Heers #503", 5.30708, 50.75161),
    ("rural WB", 4.85, 50.55),
    ("Ardennes", 5.65, 50.10),
    ("coast", 2.95, 51.20),
    ("Ghent", 3.7174, 51.0543),
    ("Berloz #503", 5.211554, 50.709124),
];

pub const PEDESTRIAN_CENTRES: &[(&str, f64, f64)] = &[
    ("Namur centre", 4.8667, 50.4632),
    ("Ghent Korenmarkt", 3.7234, 51.0543),
    ("Leuven Grote Markt", 4.7009, 50.8792),
];

pub const MATRIX_PLAN_HEADER: &str = "x-butterfly-matrix-plan";
pub const MATRIX_PLANS: &[&str] = &["bucket", "phast_fwd", "phast_rev", "mixed"];
pub const SUBLINEAR_PLANS: &[&str] = &["phast_fwd", "phast_rev"];

/// Normalise a reported plan: missing / unknown come back as markers no
/// plan check can accept.
pub fn parse_matrix_plan(value: Option<&str>) -> String {
    match value {
        None => "<missing>".to_string(),
        Some(v) => {
            let v = v.trim();
            if MATRIX_PLANS.contains(&v) {
                v.to_string()
            } else {
                format!("<unknown:{v}>")
            }
        }
    }
}

pub fn check_plan(label: &str, got: &str, want: &[&str]) -> bool {
    let want_list = format!(
        "[{}]",
        want.iter()
            .map(|w| format!("'{w}'"))
            .collect::<Vec<_>>()
            .join(", ")
    );
    check(
        &format!("{label}: plan"),
        want.contains(&got),
        &format!("server reported '{got}', expected one of {want_list}"),
    )
}

// ---------------------------------------------------------------------------
// The context
// ---------------------------------------------------------------------------
pub struct RefRoute {
    pub min: f64,
    pub best_min: f64,
    pub worst_min: f64,
    pub km: f64,
}

pub type RefRows = Vec<HashMap<String, String>>;
pub type RefRoutes = Arc<(RefRows, Vec<Option<RefRoute>>)>;
pub type StreamedMemo = HashMap<String, Arc<Result<super::gates_flight::StreamedMatrix, String>>>;

pub struct Ctx {
    pub base: String,
    pub t: Thresholds,
    pub flight: bool,
    pub flight_base: Option<String>,
    pub refs_dir: Option<PathBuf>,
    pub http: Http,
    pub flights: Flights,
    bands: Mutex<Option<bool>>,
    snap_cache: Mutex<HashMap<String, Pt>>,
    ref_routes: Mutex<HashMap<String, RefRoutes>>,
    pub streamed: Mutex<StreamedMemo>,
}

impl Ctx {
    pub fn new(
        base: String,
        t: Thresholds,
        flight: bool,
        flight_base: Option<String>,
        refs_dir: Option<PathBuf>,
    ) -> Self {
        Ctx {
            base,
            t,
            flight,
            flight_base,
            refs_dir,
            http: Http::new(),
            flights: Flights::new(),
            bands: Mutex::new(None),
            snap_cache: Mutex::new(HashMap::new()),
            ref_routes: Mutex::new(HashMap::new()),
            streamed: Mutex::new(HashMap::new()),
        }
    }

    pub fn url(&self, path_and_query: &str) -> String {
        format!("{}{}", self.base, path_and_query)
    }

    /// Flight port convention: REST port + 1, unless overridden.
    pub fn flight_uri(&self) -> String {
        if let Some(f) = &self.flight_base {
            return f.clone();
        }
        let u = reqwest::Url::parse(&self.base).ok();
        let host = u
            .as_ref()
            .and_then(|u| u.host_str().map(|h| h.to_string()))
            .unwrap_or_else(|| "localhost".to_string());
        let port = u.as_ref().and_then(|u| u.port()).unwrap_or(8080);
        format!("grpc://{host}:{}", port + 1)
    }

    pub fn do_get(&self, action: &str, mode: &str, params: &Value) -> GResult<Decoded> {
        self.flights
            .do_get(&self.flight_uri(), action, mode, params)
    }

    pub fn do_exchange(&self, cmd: &[u8], table: arrow::array::RecordBatch) -> GResult<Decoded> {
        self.flights.do_exchange(&self.flight_uri(), cmd, table)
    }

    /// Whether the engine serves `uncertainty=bands`, read ONCE from /health.
    pub fn bands_served(&self) -> bool {
        let mut g = self.bands.lock().unwrap();
        if let Some(b) = *g {
            return b;
        }
        let served = self
            .http
            .json(&self.url("/health"), 30)
            .ok()
            .and_then(|h| h.get("bands").map(|v| v.as_bool().unwrap_or(true)))
            .unwrap_or(true);
        *g = Some(served);
        served
    }

    // ---- REST helpers --------------------------------------------------
    pub fn route_json(
        &self,
        olon: f64,
        olat: f64,
        dlon: f64,
        dlat: f64,
        mode: &str,
        timeout: u64,
        extra: &[(&str, String)],
    ) -> GResult<Value> {
        let mut q: Vec<(&str, String)> = vec![
            ("origin_lon", pyf(olon)),
            ("origin_lat", pyf(olat)),
            ("destination_lon", pyf(dlon)),
            ("destination_lat", pyf(dlat)),
            ("mode", mode.to_string()),
        ];
        q.extend(extra.iter().map(|(k, v)| (*k, v.clone())));
        self.http
            .json(&self.url(&format!("/route?{}", urlencode(&q))), timeout)
    }

    /// (duration_s, distance_m)
    pub fn route(
        &self,
        olon: f64,
        olat: f64,
        dlon: f64,
        dlat: f64,
        mode: &str,
    ) -> GResult<(f64, f64)> {
        let d = self.route_json(olon, olat, dlon, dlat, mode, 60, &[])?;
        Ok((num(&d, "duration_s")?, num(&d, "distance_m")?))
    }

    pub fn table_with_plan(
        &self,
        origins: &[[f64; 2]],
        destinations: &[[f64; 2]],
        mode: &str,
        timeout: u64,
        extra: &[(&str, Value)],
    ) -> GResult<(Value, String)> {
        let mut payload = json!({"origins": origins, "destinations": destinations, "mode": mode});
        for (k, v) in extra {
            payload[*k] = v.clone();
        }
        let (body, headers) =
            self.http
                .post_json_with_headers(&self.url("/table"), &payload, timeout)?;
        let plan = parse_matrix_plan(
            headers
                .get(MATRIX_PLAN_HEADER)
                .and_then(|v| v.to_str().ok()),
        );
        Ok((body, plan))
    }

    pub fn table(
        &self,
        origins: &[[f64; 2]],
        destinations: &[[f64; 2]],
        mode: &str,
        timeout: u64,
        extra: &[(&str, Value)],
    ) -> GResult<Value> {
        Ok(self
            .table_with_plan(origins, destinations, mode, timeout, extra)?
            .0)
    }

    pub fn snap_point(&self, lon: f64, lat: f64, mode: &str) -> GResult<Pt> {
        let key = format!("{}|{}|{}|{}", self.base, pyf(lon), pyf(lat), mode);
        if let Some(p) = self.snap_cache.lock().unwrap().get(&key) {
            return Ok(*p);
        }
        let j = self.http.json(
            &self.url(&format!(
                "/nearest?lon={}&lat={}&mode={mode}",
                pyf(lon),
                pyf(lat)
            )),
            30,
        )?;
        let loc = j
            .pointer("/waypoints/0/location")
            .and_then(Value::as_array)
            .ok_or_else(|| GateErr::Other("no waypoints".into()))?;
        let p = (
            loc.first().and_then(Value::as_f64).unwrap_or(f64::NAN),
            loc.get(1).and_then(Value::as_f64).unwrap_or(f64::NAN),
        );
        self.snap_cache.lock().unwrap().insert(key, p);
        Ok(p)
    }

    /// Route every reference trip ONCE (with bands when served) and memoise.
    pub fn ref_trip_routes(&self, path: &str) -> GResult<RefRoutes> {
        let key = format!("{}|{}", self.base, path);
        if let Some(r) = self.ref_routes.lock().unwrap().get(&key) {
            return Ok(Arc::clone(r));
        }
        let rows = ref_trips(path)?;
        let extra: Vec<(&str, String)> = if self.bands_served() {
            vec![("uncertainty", "bands".to_string())]
        } else {
            vec![]
        };
        let res: Vec<Option<RefRoute>> = par_map(&rows, 16, |t| {
            let f = |k: &str| t.get(k).and_then(|v| v.parse::<f64>().ok());
            let (Some(a), Some(b), Some(c), Some(d)) =
                (f("long_1"), f("lat_1"), f("long_2"), f("lat_2"))
            else {
                return None;
            };
            let d = self.route_json(a, b, c, d, "car", 60, &extra).ok()?;
            Some(RefRoute {
                min: num(&d, "duration_s").ok()? / 60.0,
                best_min: d
                    .get("duration_best_s")
                    .and_then(Value::as_f64)
                    .unwrap_or(0.0)
                    / 60.0,
                worst_min: d
                    .get("duration_worst_s")
                    .and_then(Value::as_f64)
                    .unwrap_or(0.0)
                    / 60.0,
                km: num(&d, "distance_m").ok()? / 1000.0,
            })
        });
        let rec = Arc::new((rows, res));
        self.ref_routes
            .lock()
            .unwrap()
            .insert(key, Arc::clone(&rec));
        Ok(rec)
    }

    pub fn like_for_like(&self, r: Option<&RefRoute>, t: &HashMap<String, String>) -> bool {
        let Some(r) = r else {
            return false;
        };
        let km = t.get("ref_km").and_then(|v| {
            if v.is_empty() {
                Some(0.0)
            } else {
                v.parse::<f64>().ok()
            }
        });
        match km {
            Some(km) if km > 0.0 => (r.km / km - 1.0).abs() <= self.t.like_for_like_km_tol,
            _ => false,
        }
    }
}

/// Reference trips CSV → rows as maps.
pub fn ref_trips(path: &str) -> GResult<RefRows> {
    let mut rdr = csv::Reader::from_path(path)
        .map_err(|e| GateErr::Other(format!("cannot read reference set {path}: {e}")))?;
    let headers = rdr
        .headers()
        .map_err(|e| GateErr::Other(e.to_string()))?
        .clone();
    let mut rows = Vec::new();
    for rec in rdr.records() {
        let rec = rec.map_err(|e| GateErr::Other(e.to_string()))?;
        let mut m = HashMap::new();
        for (h, v) in headers.iter().zip(rec.iter()) {
            m.insert(h.to_string(), v.to_string());
        }
        rows.push(m);
    }
    Ok(rows)
}

pub fn num(v: &Value, key: &str) -> GResult<f64> {
    v.get(key)
        .and_then(Value::as_f64)
        .ok_or_else(|| GateErr::Other(format!("missing numeric field '{key}'")))
}

/// `ThreadPoolExecutor(16).map`: order-preserving parallel map.
pub fn par_map<T: Sync, R: Send>(
    items: &[T],
    workers: usize,
    f: impl Fn(&T) -> R + Sync,
) -> Vec<R> {
    let n = items.len();
    let next = std::sync::atomic::AtomicUsize::new(0);
    let results: Vec<Mutex<Option<R>>> = (0..n).map(|_| Mutex::new(None)).collect();
    std::thread::scope(|s| {
        for _ in 0..workers.min(n.max(1)) {
            s.spawn(|| {
                loop {
                    let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if i >= n {
                        break;
                    }
                    let r = f(&items[i]);
                    *results[i].lock().unwrap() = Some(r);
                }
            });
        }
    });
    results
        .into_iter()
        .map(|m| m.into_inner().unwrap().expect("every item computed"))
        .collect()
}

// ---------------------------------------------------------------------------
// Isochrone bundle — ONE fetch per (origin, mode, threshold, direction)
// ---------------------------------------------------------------------------
pub struct IsoBundle<'a> {
    ctx: &'a Ctx,
    pub lon: f64,
    pub lat: f64,
    pub mode: String,
    pub time_s: u64,
    pub direction: String,
    pub param: String,
    json: Mutex<HashMap<String, GResult<Value>>>,
    wkb: Mutex<Option<GResult<Vec<u8>>>>,
    contours: Mutex<HashMap<String, GResult<Vec<Vec<Pt>>>>>,
}

impl<'a> IsoBundle<'a> {
    pub fn new(
        ctx: &'a Ctx,
        lon: f64,
        lat: f64,
        mode: &str,
        time_s: u64,
        direction: &str,
        param: &str,
    ) -> Self {
        IsoBundle {
            ctx,
            lon,
            lat,
            mode: mode.to_string(),
            time_s,
            direction: direction.to_string(),
            param: param.to_string(),
            json: Mutex::new(HashMap::new()),
            wkb: Mutex::new(None),
            contours: Mutex::new(HashMap::new()),
        }
    }

    pub fn q(&self) -> String {
        format!(
            "lon={}&lat={}&mode={}&direction={}&{}={}",
            pyf(self.lon),
            pyf(self.lat),
            self.mode,
            self.direction,
            self.param,
            self.time_s
        )
    }

    fn json_extra(&self, extra: &str) -> GResult<Value> {
        let mut g = self.json.lock().unwrap();
        if !g.contains_key(extra) {
            let r = self.ctx.http.json(
                &self.ctx.url(&format!("/isochrone?{}{extra}", self.q())),
                120,
            );
            g.insert(extra.to_string(), r);
        }
        g[extra].clone()
    }

    pub fn snap(&self) -> GResult<Pt> {
        self.ctx.snap_point(self.lon, self.lat, &self.mode)
    }

    pub fn wkb(&self) -> GResult<Vec<u8>> {
        let mut g = self.wkb.lock().unwrap();
        if g.is_none() {
            *g = Some(self.ctx.http.bytes(
                &self.ctx.url(&format!("/isochrone?{}", self.q())),
                120,
                Some("application/octet-stream"),
            ));
        }
        g.as_ref().unwrap().clone()
    }

    pub fn polys(&self) -> GResult<Vec<Vec<Vec<Pt>>>> {
        Ok(wkb_polygons(&self.wkb()?))
    }

    pub fn json(&self) -> GResult<Value> {
        self.json_extra("")
    }

    /// Outer rings of the JSON `contours[].polygon` (polyline6), request order.
    pub fn rings(&self) -> GResult<Vec<Vec<Pt>>> {
        Ok(rings_of(&self.json()?))
    }

    pub fn network(&self) -> GResult<Vec<Vec<Pt>>> {
        let j = self.json_extra("&include=network")?;
        Ok(network_of(&j))
    }

    pub fn geojson(&self) -> GResult<Value> {
        self.json_extra("&geometries=geojson")
    }

    pub fn contour_rings(&self, times: &[u64]) -> GResult<Vec<Vec<Pt>>> {
        let key = times
            .iter()
            .map(|t| t.to_string())
            .collect::<Vec<_>>()
            .join(",");
        let mut g = self.contours.lock().unwrap();
        if !g.contains_key(&key) {
            let r = self
                .ctx
                .http
                .json(
                    &self.ctx.url(&format!(
                        "/isochrone?lon={}&lat={}&mode={}&direction={}&contours={key}",
                        pyf(self.lon),
                        pyf(self.lat),
                        self.mode,
                        self.direction
                    )),
                    120,
                )
                .map(|j| rings_of(&j));
            g.insert(key.clone(), r);
        }
        g[&key].clone()
    }
}

pub fn rings_of(payload: &Value) -> Vec<Vec<Pt>> {
    payload
        .get("contours")
        .and_then(Value::as_array)
        .map(|cs| {
            cs.iter()
                .filter_map(|c| c.get("polygon").and_then(Value::as_str))
                .filter(|s| !s.is_empty())
                .map(decode_polyline6)
                .collect()
        })
        .unwrap_or_default()
}

pub fn network_of(j: &Value) -> Vec<Vec<Pt>> {
    j.get("network")
        .and_then(Value::as_array)
        .map(|segs| {
            segs.iter()
                .map(|seg| {
                    seg.as_array()
                        .map(|pts| {
                            pts.iter()
                                .filter_map(|p| {
                                    let a = p.as_array()?;
                                    Some((a.first()?.as_f64()?, a.get(1)?.as_f64()?))
                                })
                                .collect()
                        })
                        .unwrap_or_default()
                })
                .collect()
        })
        .unwrap_or_default()
}
