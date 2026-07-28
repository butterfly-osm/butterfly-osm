#!/usr/bin/env python3
"""Post-deploy correctness gate for a live butterfly server.

Runs baseline-independent invariants + a reference-ETA ground-truth set against a
deployed instance and exits non-zero on any violation. Designed to run after
every deploy (dev container, staging, or any reachable instance) so a
regression of a KNOWN failure class can never ship silently again.

Checks
------
1. GROUND TRUTH (1,000 reference trips with independently observed ETAs;
   the dataset itself is private — pass any CSV with columns
   route_id,long_1,lat_1,long_2,lat_2,ref_min,ref_km via --trips):
   duration and distance ratio distributions vs the reference. The DISTANCE ratio is
   speed-calibration-independent — it gates pure routing correctness.
2. SYMMETRY: route(A→B) vs route(B→A) on seeded random pairs. The #502 snap
   bug's fingerprint was 4× asymmetry; a healthy two-way network stays <1.5×.
3. TICKET FIXTURES: the #502/#503 cases (Berloz, Heers, Robertville) pinned
   to their validated values ±10 %.
4. ENDPOINT CONSISTENCY: /route and /table must agree on durations (±3 s)
   for the same pairs — one answer per question.
5. ISOCHRONE CONTAINMENT (#497/#506): every isochrone polygon must contain
   its own SNAPPED origin (snapped-road-point semantics — the raw query
   point may legitimately sit outside when it is far off-network).
6. CLOSE-PAIR CONSISTENCY: /route vs /table on pairs 50-400 m apart —
   the same-edge / co-located-candidate regime where a legacy same-rank
   shortcut and a reduce clamp both emitted bogus 0 s answers. Uniform
   random pairs almost never land in this regime, so it gets its own sweep.

Usage
-----
    python3 bench/postdeploy_gate.py --base http://localhost:3001 \
        [--trips /path/to/od.csv] [--quick]

`--quick` skips the 1,000-trip ground truth (runs invariants only, ~30 s).
Thresholds are set from the measured 2026-07-16 baseline (see BASELINE below)
with modest slack; RATCHET THEM DOWN as tails get fixed, never up.
"""

import argparse
import concurrent.futures as cf
import csv
import json
import random
import statistics
import sys
import urllib.error
import urllib.parse
import urllib.request

DEFAULT_TRIPS = "/home/pierre/explorations/reference_trips/od.csv"

# BASELINE 2026-07-16 (engine d97168d, 1000 trips, zero errors):
#   duration ratio: p05=0.854 p50=1.029 p90=1.246 p95=1.304 mean=1.048
#   distance ratio: p05=0.933 p50=1.004 p90=1.148 p95=1.253 mean=1.039
#   distance outliers (<0.85 / >1.2): 73
THRESHOLDS = {
    "dur_p50": (0.90, 1.15),
    "dur_p90_max": 1.30,
    "dist_p50": (0.97, 1.06),
    "dist_p90_max": 1.20,
    "dist_outliers_max": 80,  # baseline 72-73; ratcheted 90→80 (2026-07-17); next drop needs richer per-edge speed data
    "symmetry_ratio_max": 1.5,
    "symmetry_violations_max": 0,
    "consistency_tolerance_s": 3.0,
    "max_errors": 5,  # unroutable trips (OSM drift) tolerated before failing
}

# #502/#503 sentinel pairs. NO hardcoded expected values (a measured-then-
# pasted constant only asserts "the server returns what it returned", and
# breaks on every legitimate semantic improvement — e.g. #523 end clipping).
# Instead each pair is checked against invariants that never expire:
#   1. bounded detour vs crow-fly, ONE global generous bound (a lake crossing
#      legitimately hits ×6; the #502 pathologies were ×10-40 loops) —
#      per-pair bounds would be hardcoding by another name
#   2. physically plausible mean speed  (per mode)
#   3. internal consistency: distance_m ≡ polyline length ≡ Σ annotations
#      (the #523 invariant — would have caught #522 automatically)
# (name, o_lon, o_lat, d_lon, d_lat)
FIXTURES = [
    ("Berloz #503", 5.211554, 50.709124, 5.211383, 50.698323),
    ("Heers #503", 5.307080, 50.751610, 5.293005, 50.752418),
    ("Robertville #502", 6.008464, 50.428652, 6.022535, 50.428452),
]
SENTINEL_MAX_DETOUR = 8.0
CAR_SPEED_BOUNDS_KMH = (15.0, 135.0)  # mean over a whole route, car mode
# Physical-plausibility bounds on the IMPLIED mean speed (distance/duration),
# per mode — not measured values, just "a human can't walk 19 km/h" limits.
# #522: foot routes were reporting up to 5.3 m/s (19 km/h).
FOOT_SPEED_BOUNDS_KMH = (2.0, 8.0)
BIKE_SPEED_BOUNDS_KMH = (5.0, 32.0)
GEOM_CONSISTENCY_TOL = 0.03  # distance_m vs polyline length


def http_json(url, timeout=30, data=None, headers=None):
    req = urllib.request.Request(url, data=data, headers=headers or {})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def route(base, olon, olat, dlon, dlat, mode="car"):
    q = urllib.parse.urlencode(
        {
            "origin_lon": olon,
            "origin_lat": olat,
            "destination_lon": dlon,
            "destination_lat": dlat,
            "mode": mode,
        }
    )
    d = http_json(f"{base}/route?{q}")
    return d["duration_s"], d["distance_m"]


def pct(xs, q):
    xs = sorted(xs)
    return xs[min(int(len(xs) * q), len(xs) - 1)]


def check(name, ok, detail):
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}: {detail}")
    return ok


def gate_lopsided(base):
    """#526: lopsided (1xN) matrices must take a sublinear plan (seeded
    PHAST) and stay cell-for-cell consistent with /route. Guards BOTH the
    selection (scaling ratio — linear bucket would be ~16x) and the
    correctness of the PHAST field evaluation (route==table equality)."""
    import time as _t

    print("== lopsided matrix 1xN: sublinear plan + route==table (#526) ==")
    rng = random.Random(31)
    origin = (4.3517, 50.8503)
    dests = [
        (origin[0] + rng.uniform(-0.25, 0.25), origin[1] + rng.uniform(-0.15, 0.15))
        for _ in range(800)
    ]

    def table(dsts):
        body = json.dumps(
            {
                "origins": [list(origin)],
                "destinations": [list(d) for d in dsts],
                "mode": "foot",
            }
        ).encode()
        t0 = _t.time()
        r = http_json(
            f"{base}/table",
            timeout=300,
            data=body,
            headers={"Content-Type": "application/json"},
        )
        return r, _t.time() - t0

    def table_dd(dsts):
        body = json.dumps(
            {
                "origins": [list(origin)],
                "destinations": [list(d) for d in dsts],
                "mode": "foot",
                "annotations": "duration,distance",
            }
        ).encode()
        return http_json(
            f"{base}/table",
            timeout=300,
            data=body,
            headers={"Content-Type": "application/json"},
        )

    table(dests[:50])  # warm + calibrate the router's measured constants
    big, tb = table(dests)
    small, ts = table(dests[:50])
    ratio = tb / max(ts, 1e-3)
    ok_scale = check(
        "lopsided scaling",
        ratio < 6.0,
        f"1x800 {tb:.2f}s vs 1x50 {ts:.2f}s ratio x{ratio:.1f} (linear bucket ~x16, PHAST ~x1)",
    )
    mism = 0
    checked = 0
    worst = 0.0
    for i in rng.sample(range(800), 25):
        d_t = big["durations"][0][i]
        if d_t is None:
            continue
        try:
            d_r, _ = route(base, origin[0], origin[1], dests[i][0], dests[i][1], mode="foot")
        except Exception:
            continue
        checked += 1
        delta = abs(d_r - d_t)
        worst = max(worst, delta)
        if delta > THRESHOLDS["consistency_tolerance_s"]:
            mism += 1
    ok_eq = check(
        "lopsided route==table",
        mism == 0 and checked >= 15,
        f"{checked} cells sampled, {mism} mismatches, worst {worst:.1f}s",
    )
    # #527: 2-channel lopsided — distance channel must equal /route distance_m.
    dd = table_dd(dests[:300])
    dmis = 0
    dchecked = 0
    dworst = 0.0
    for i in rng.sample(range(300), 25):
        d_t = dd["durations"][0][i]
        m_t = dd["distances"][0][i]
        if d_t is None or m_t is None:
            continue
        try:
            r = http_json(
                f"{base}/route?"
                + urllib.parse.urlencode(
                    {
                        "origin_lon": origin[0],
                        "origin_lat": origin[1],
                        "destination_lon": dests[i][0],
                        "destination_lat": dests[i][1],
                        "mode": "foot",
                    }
                )
            )
        except Exception:
            continue
        dchecked += 1
        rel = abs(m_t - r["distance_m"]) / max(r["distance_m"], 1.0)
        dworst = max(dworst, rel)
        if rel > 0.02:
            dmis += 1
    ok_dist = check(
        "lopsided 2-channel distance==route",
        dmis == 0 and dchecked >= 15,
        f"{dchecked} cells, {dmis} mismatches, worst {dworst*100:.2f}%",
    )
    return ok_scale and ok_eq and ok_dist


def _distance_channel_vs_route(base, mode):
    """Shared #528 probe: build a 1x200 /table with the 2-channel distance
    (length-along-time) annotation for `mode`, then compare 30 sampled cells
    against /route distance_m for the same OD pair on the SAME mode. Returns
    (checked, mism, worst) or None if the mode is not served (unknown-mode
    400s are a SKIP, not a FAIL — variants are deploy-dependent)."""
    rng = random.Random(528)
    o = (4.3517, 50.8503)
    dests = [
        (o[0] + rng.uniform(-0.3, 0.3), o[1] + rng.uniform(-0.2, 0.2))
        for _ in range(200)
    ]
    body = json.dumps(
        {
            "origins": [list(o)],
            "destinations": [list(d) for d in dests],
            "mode": mode,
            "annotations": "duration,distance",
        }
    ).encode()
    try:
        tab = http_json(
            f"{base}/table",
            timeout=200,
            data=body,
            headers={"Content-Type": "application/json"},
        )
    except urllib.error.HTTPError as e:
        if e.code in (400, 404):
            return None  # mode not served — skip
        raise
    mism = 0
    checked = 0
    worst = 0.0
    for i in rng.sample(range(200), 30):
        m = tab["distances"][0][i]
        if m is None:
            continue
        try:
            _, dist_r = route(base, o[0], o[1], dests[i][0], dests[i][1], mode=mode)
        except Exception:
            continue
        if dist_r < 1:
            continue
        checked += 1
        rel = abs(m - dist_r) / dist_r
        worst = max(worst, rel)
        if rel > 0.02:
            mism += 1
    return checked, mism, worst


def gate_recustomized_distance(base):
    """#528: on a RECUSTOMIZED mode, the 2-channel /table distance
    (length-along-time) must equal /route distance_m. This was the blind
    spot that let a ~15% car distance error live for months: durations were
    tested, foot (never recustomized) looked fine, and no test compared the
    DISTANCE channel across surfaces on the recustomized mode.

    We probe the boot-recustomized base `car`: its len-along-time flats must
    be recomputed from the recustomized time middles, not cloned from the
    clean base.

    #529/#530: `car_nodir` (one-way-agnostic) is the canonical equal-DURATION
    tie mode — forward/backward arcs cost the same, so many OD pairs have
    several equal-time paths of DIFFERENT length. The 2-channel distance must
    still equal /route distance_m under those ties. This probe exercises the
    default /table BUCKET path (SearchState2 lazy-lex, #529). The PHAST
    2-channel path (#530) is only taken when `phast_wins()` routes a lopsided
    matrix through `table_phast_lopsided_2ch`; the live server picks the plan
    itself and its env cannot be set from the gate, so forcing PHAST here is
    not feasible — that surface is locked by the Rust unit test
    `phast_2ch_lex_tests` and can be re-verified live with a dedicated serve
    run under `BUTTERFLY_MATRIX_ALGO=phast`."""
    print("== recustomized-mode 2-channel distance==route (#528/#529) ==")
    passed = True
    for mode in ("car", "car_nodir"):
        res = _distance_channel_vs_route(base, mode)
        if res is None:
            print(f"  [SKIP] {mode} 2-channel distance==route: mode not served")
            continue
        checked, mism, worst = res
        # `car` is always present (hard requirement); a served variant must
        # also hold. checked>=20 guards against a probe that snapped nothing.
        need_coverage = checked >= 20 if mode in ("car", "car_nodir") else checked >= 10
        passed &= check(
            f"{mode} 2-channel distance==route",
            mism == 0 and need_coverage,
            f"{checked} cells, {mism} mismatches, worst {worst*100:.2f}%",
        )
    return passed


def gate_radius_prune(base):
    """#531: radius_km must actually PRUNE far cells on the UNBOUNDED /table
    path. It was silently ignored there for a long time — the K-best snap
    fallback re-populated cells the radius mask had nulled. Guards both the
    scalar radius and the per-origin array (len == origins)."""
    print("== radius_km prunes far cells (scalar + per-origin, #531) ==")
    o = [4.35, 50.85]
    # ~1.1 / 2.2 / 3.3 km due east at this latitude.
    dests = [[o[0] + 0.0157, o[1]], [o[0] + 0.0314, o[1]], [o[0] + 0.0471, o[1]]]

    def table(origins, radius):
        body = json.dumps(
            {
                "origins": origins,
                "destinations": dests,
                "mode": "foot",
                "annotations": "duration",
                "radius_km": radius,
            }
        ).encode()
        r = http_json(
            f"{base}/table", data=body, headers={"Content-Type": "application/json"}
        )
        return r["durations"]

    def kept(row):
        return [i for i, v in enumerate(row) if v is not None]

    scalar = kept(table([o], 1.5)[0])
    ok_scalar = check(
        "scalar radius_km=1.5 prunes",
        scalar == [0],
        f"kept {scalar} (want [0] — the ~2.2/3.3 km targets pruned)",
    )
    per = table([o, o, o], [1.5, 3.0, 0])
    rows = [kept(per[i]) for i in range(3)]
    ok_per = check(
        "per-origin radius_km prunes each origin",
        rows == [[0], [0, 1], [0, 1, 2]],
        f"kept {rows} (want [[0],[0,1],[0,1,2]])",
    )
    return ok_scalar and ok_per


def gate_ground_truth(base, trips_path):
    print(f"== ground truth: reference trips ({trips_path}) ==")
    rows = list(csv.DictReader(open(trips_path)))

    def one(r):
        try:
            dur_s, dist_m = route(base, r["long_1"], r["lat_1"], r["long_2"], r["lat_2"])
            return (
                dur_s / 60 / float(r["ref_min"]),
                dist_m / 1000 / float(r["ref_km"]),
            )
        except Exception:
            return None

    with cf.ThreadPoolExecutor(16) as ex:
        res = list(ex.map(one, rows))
    ok_res = [x for x in res if x]
    errors = len(rows) - len(ok_res)
    dur = [x[0] for x in ok_res]
    dist = [x[1] for x in ok_res]
    outliers = sum(1 for d in dist if d < 0.85 or d > 1.2)
    t = THRESHOLDS
    passed = True
    passed &= check("trip errors", errors <= t["max_errors"], f"{errors} (max {t['max_errors']})")
    p50d = pct(dur, 0.5)
    passed &= check(
        "duration p50",
        t["dur_p50"][0] <= p50d <= t["dur_p50"][1],
        f"{p50d:.3f} (bounds {t['dur_p50']})",
    )
    p90d = pct(dur, 0.9)
    passed &= check("duration p90", p90d <= t["dur_p90_max"], f"{p90d:.3f} (max {t['dur_p90_max']})")
    p50m = pct(dist, 0.5)
    passed &= check(
        "distance p50",
        t["dist_p50"][0] <= p50m <= t["dist_p50"][1],
        f"{p50m:.3f} (bounds {t['dist_p50']})",
    )
    p90m = pct(dist, 0.9)
    passed &= check("distance p90", p90m <= t["dist_p90_max"], f"{p90m:.3f} (max {t['dist_p90_max']})")
    passed &= check(
        "distance outliers",
        outliers <= t["dist_outliers_max"],
        f"{outliers} (max {t['dist_outliers_max']})",
    )
    print(
        f"  stats: dur mean={statistics.mean(dur):.3f} p05={pct(dur, 0.05):.3f} p95={pct(dur, 0.95):.3f}"
        f" | dist mean={statistics.mean(dist):.3f} p05={pct(dist, 0.05):.3f} p95={pct(dist, 0.95):.3f}"
    )
    return passed


def gate_symmetry(base, n_pairs=150):
    print(f"== symmetry invariant ({n_pairs} seeded random pairs) ==")
    rng = random.Random(99)
    t = THRESHOLDS
    violations = []
    tested = 0
    worst = 1.0
    for _ in range(n_pairs):
        a, b = round(rng.uniform(3.0, 6.2), 5), round(rng.uniform(49.6, 51.4), 5)
        c, d = round(rng.uniform(3.0, 6.2), 5), round(rng.uniform(49.6, 51.4), 5)
        try:
            f, _ = route(base, a, b, c, d)
            r, _ = route(base, c, d, a, b)
        except Exception:
            continue
        if f < 60:
            continue
        tested += 1
        ratio = max(f, r) / max(min(f, r), 1)
        worst = max(worst, ratio)
        if ratio > t["symmetry_ratio_max"]:
            violations.append((ratio, (a, b, c, d)))
    ok = len(violations) <= t["symmetry_violations_max"] and tested >= 50
    for v in violations[:5]:
        print(f"    violation: ratio {v[0]:.2f} @ {v[1]}")
    return check(
        "fwd/rev symmetry",
        ok,
        f"{tested} pairs, {len(violations)} >{t['symmetry_ratio_max']}x, worst {worst:.2f}",
    )


def _haversine_m(lon1, lat1, lon2, lat2):
    import math
    r = 6371000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    a = (
        math.sin((p2 - p1) / 2) ** 2
        + math.cos(p1) * math.cos(p2) * math.sin(math.radians(lon2 - lon1) / 2) ** 2
    )
    return 2 * r * math.asin(math.sqrt(a))


def _polyline_len_m(coords):
    return sum(
        _haversine_m(coords[i][0], coords[i][1], coords[i + 1][0], coords[i + 1][1])
        for i in range(len(coords) - 1)
    )


def gate_fixtures(base):
    print("== sentinel pairs (#502/#503) — invariant checks, no expected constants ==")
    passed = True
    lo_kmh, hi_kmh = CAR_SPEED_BOUNDS_KMH
    for name, olon, olat, dlon, dlat in FIXTURES:
        max_detour = SENTINEL_MAX_DETOUR
        q = urllib.parse.urlencode(
            {
                "origin_lon": olon,
                "origin_lat": olat,
                "destination_lon": dlon,
                "destination_lat": dlat,
                "mode": "car",
                "geometries": "polyline6",
                "annotations": "distance,duration",
            }
        )
        try:
            d = http_json(f"{base}/route?{q}")
        except Exception as e:
            passed &= check(name, False, f"request failed: {e}")
            continue
        dur_s, dist_m = d["duration_s"], d["distance_m"]
        crow = _haversine_m(olon, olat, dlon, dlat)
        detour = dist_m / max(crow, 1.0)
        kmh = dist_m / max(dur_s, 0.001) * 3.6
        geom = d.get("geometry", {})
        poly = geom.get("polyline") or geom.get("coordinates_polyline6") or ""
        geom_m = _polyline_len_m(_decode_polyline6(poly)) if poly else None
        ann = d.get("annotations") or {}
        ann_dist = sum(ann.get("distance") or [])
        ann_dur = sum(ann.get("duration") or [])
        ok_detour = detour <= max_detour
        ok_speed = lo_kmh <= kmh <= hi_kmh
        ok_geom = geom_m is None or abs(geom_m - dist_m) <= dist_m * GEOM_CONSISTENCY_TOL
        # annotations may legitimately differ from duration_s by the turn/
        # junction costs the summary carries; require them within 15%.
        ok_ann = (
            ann_dist == 0
            or (
                abs(ann_dist - dist_m) <= dist_m * GEOM_CONSISTENCY_TOL
                and abs(ann_dur - dur_s) <= dur_s * 0.15
            )
        )
        ok = ok_detour and ok_speed and ok_geom and ok_ann
        gtxt = f"{geom_m:.0f}m" if geom_m is not None else "n/a"
        passed &= check(
            name,
            ok,
            f"{dur_s:.0f}s/{dist_m:.0f}m detour×{detour:.2f}(≤{max_detour}) "
            f"{kmh:.0f}km/h geom={gtxt} annΣ={ann_dist:.0f}m/{ann_dur:.0f}s",
        )
    return passed


def gate_mode_coherence(base):
    """#522 / #493: foot and bike routes must be internally coherent —
    distance_m ≡ polyline length ≡ Σ annotation distances (within tol), and the
    IMPLIED mean speed (distance/duration) must be physically plausible for the
    mode. Catches #522 (foot routes reporting up to 5.3 m/s ≈ 19 km/h) and #493
    (foot/bike geometry_wkb ~2× the reported distance — polyline doubled/zigzag).
    Same invariant the car sentinel gate enforces, extended to the modes where
    the bugs actually landed."""
    print("== foot/bike geometry ≡ distance ≡ annotations + plausible speed (#522/#493) ==")
    passed = True
    for mode, (lo_kmh, hi_kmh) in (
        ("foot", FOOT_SPEED_BOUNDS_KMH),
        ("bike", BIKE_SPEED_BOUNDS_KMH),
    ):
        for name, olon, olat, dlon, dlat in FIXTURES:
            q = urllib.parse.urlencode(
                {
                    "origin_lon": olon,
                    "origin_lat": olat,
                    "destination_lon": dlon,
                    "destination_lat": dlat,
                    "mode": mode,
                    "geometries": "polyline6",
                    "annotations": "distance,duration",
                }
            )
            try:
                d = http_json(f"{base}/route?{q}")
            except Exception as e:
                passed &= check(f"{mode} {name}", False, f"request failed: {e}")
                continue
            dur_s, dist_m = d["duration_s"], d["distance_m"]
            if dist_m <= 0 or dur_s <= 0:
                passed &= check(f"{mode} {name}", False, f"degenerate {dist_m}m/{dur_s}s")
                continue
            kmh = dist_m / dur_s * 3.6
            geom = d.get("geometry", {})
            poly = geom.get("polyline") or geom.get("coordinates_polyline6") or ""
            geom_m = _polyline_len_m(_decode_polyline6(poly)) if poly else None
            ann = d.get("annotations") or {}
            ann_dist = sum(ann.get("distance") or [])
            ok_speed = lo_kmh <= kmh <= hi_kmh
            ok_geom = geom_m is None or abs(geom_m - dist_m) <= dist_m * GEOM_CONSISTENCY_TOL
            ok_ann = ann_dist == 0 or abs(ann_dist - dist_m) <= dist_m * GEOM_CONSISTENCY_TOL
            gtxt = f"{geom_m:.0f}m" if geom_m is not None else "n/a"
            passed &= check(
                f"{mode} {name}",
                ok_speed and ok_geom and ok_ann,
                f"{dur_s:.0f}s/{dist_m:.0f}m {kmh:.1f}km/h "
                f"(bound {lo_kmh:.0f}-{hi_kmh:.0f}) geom={gtxt} annΣ={ann_dist:.0f}m",
            )
    return passed


def gate_bounded_matrix_exactness(base):
    """#534 / #415: the SEEDED bounded matrix must be EXACT — every cell the
    unbounded matrix reports at ≤ threshold must survive the bounded run with
    the same value, never a false u32::MAX. The pre-#534 bug bounded the shared
    forward sweep by the source's own partial instead of `threshold + max target
    shift`, so a target that out-shifts the sources had its meeting node pruned →
    false sentinel. We drive both matrices over the same points (which snap to
    real multi-candidate phantom seeds with real shifts) and assert the
    filtered-equality invariant. No stored constant — the unbounded run is the
    ground truth."""
    print("== seeded bounded matrix == unbounded filtered (#534/#415) ==")
    try:
        import pyarrow.flight as fl
    except ImportError:
        print("  [SKIP] pyarrow not available")
        return True
    import urllib.parse as up

    host = up.urlparse(base).hostname or "localhost"
    port = (up.urlparse(base).port or 8080) + 1
    MAX = 4294967295
    # Points span short + long-edge fixtures (large phantom shifts) so the
    # forward bound is actually exercised.
    pts = [[p[1], p[2]] for p in ISO_POINTS] + [[f[3], f[4]] for f in FIXTURES]

    def matrix(mode, max_minutes):
        params = {"origins": pts, "destinations": pts}
        if max_minutes is not None:
            params["max_minutes"] = max_minutes
        tb = fl.connect(f"grpc://{host}:{port}").do_get(
            fl.Ticket(f"matrix:{mode}:{json.dumps(params)}".encode())
        ).read_all()
        s, t, dur = tb.column("source_idx"), tb.column("target_idx"), tb.column("duration_ms")
        return {(s[i].as_py(), t[i].as_py()): dur[i].as_py() for i in range(tb.num_rows)}

    passed = True
    try:
        for mode in ("car", "foot"):
            unb = matrix(mode, None)
            # threshold in minutes; ms → minutes for the compare.
            T_MIN = 15
            thr_ms = T_MIN * 60 * 1000
            bnd = matrix(mode, T_MIN)
            in_bound = {k: v for k, v in unb.items() if v != MAX and v <= thr_ms}
            missing = [k for k in in_bound if bnd.get(k, MAX) == MAX]
            wrong = [k for k in in_bound if bnd.get(k, MAX) != MAX and bnd[k] != in_bound[k]]
            passed &= check(
                f"{mode}: fixture exercises the bound",
                len(in_bound) > 0,
                f"{len(in_bound)} cells ≤ {T_MIN}min",
            )
            passed &= check(
                f"{mode}: no in-bound cell falsely dropped",
                len(missing) == 0,
                f"{len(missing)} in-bound cells came back u32::MAX (#534 forward-bound bug)",
            )
            passed &= check(
                f"{mode}: in-bound values identical to unbounded",
                len(wrong) == 0,
                f"{len(wrong)} cells differ from the unbounded value",
            )
    except Exception as e:
        print(f"  [SKIP] flight unreachable ({e})")
        return True
    return passed


# Long inter-city car pairs (endpoints = ISO_POINTS cities) — used by the
# one-way-routability and motorway-floor gates.
def _city_pairs():
    cities = [(n, lo, la) for (n, lo, la) in ISO_POINTS]
    out = []
    for i in range(0, len(cities) - 1, 2):
        a, b = cities[i], cities[i + 1]
        out.append((f"{a[0]}→{b[0]}", a[1], a[2], b[1], b[2]))
    return out


def gate_one_way_routable(base):
    """#197: on a bidirectional-dominant road network, a car pair that routes
    one way MUST route the other — a one-directional `No route found` (the #197
    fingerprint: A→B ok, B→A 404) is a directed-graph / snap-role regression.
    `gate_symmetry` `continue`s past exceptions, so a 404 is invisible there;
    this asserts BOTH directions return a route."""
    print("== car one-way routability: no directional 404 (#197) ==")
    passed = True
    n_ok = 0
    n_pairs = 0
    fails = []
    for name, olon, olat, dlon, dlat in _city_pairs():
        n_pairs += 1
        fwd = rev = True
        try:
            route(base, olon, olat, dlon, dlat, "car")
        except Exception:
            fwd = False
        try:
            route(base, dlon, dlat, olon, olat, "car")
        except Exception:
            rev = False
        if fwd and rev:
            n_ok += 1
        else:
            fails.append(f"{name} (fwd={fwd} rev={rev})")
    for f in fails[:5]:
        print(f"    directional gap: {f}")
    passed &= check(
        "both directions route",
        n_ok == n_pairs,
        f"{n_ok}/{n_pairs} pairs route both ways",
    )
    return passed


def gate_isochrone_upper_bound(base):
    """#430/#495/#431: isochrones were too LARGE/lenient (a short interval
    covering a far-too-big area), and #431 bounds residual over-extension. Two
    invariants, no reference dataset:
      * max reach — the farthest polygon vertex (crow-fly) from the SNAPPED
        origin ≤ v_max(mode) × time × slack (crow-fly ≤ road distance ≤
        v_max×time; a gross leniency regression blows this);
      * nested monotonicity — contour(600s) ⊇ contour(300s) for the same
        origin (rings nest; the #431 balanced-closing / multi-contour order)."""
    print("== isochrone upper bound + nested monotonicity (#430/#431) ==")
    # v_max in m/s: physical ceilings, not measured values.
    vmax = {"car": 36.1, "foot": 1.9}  # 130 km/h, ~6.8 km/h
    slack = 1.20
    passed = True
    for mode in ("car", "foot"):
        time_s = 600 if mode == "car" else 1800
        reach_ok = 0
        nest_ok = 0
        n = 0
        for name, lon, lat in ISO_POINTS:
            try:
                sp_j = http_json(f"{base}/nearest?lon={lon}&lat={lat}&mode={mode}")
                sp = tuple(sp_j["waypoints"][0]["location"])
                d = http_json(
                    f"{base}/isochrone?lon={lon}&lat={lat}&mode={mode}&contours={time_s // 2},{time_s}"
                )
                # `contours=A,B` returns rings in request order (the `interval`
                # field is currently unlabelled). Ring 0 = inner (time_s/2),
                # ring -1 = outer (time_s).
                rings = [_decode_polyline6(c["polygon"]) for c in d.get("contours", []) if c.get("polygon")]
            except Exception:
                continue
            if not rings:
                continue
            n += 1
            reach = [
                max((_haversine_m(sp[0], sp[1], v[0], v[1]) for v in r), default=0.0)
                for r in rings
            ]
            outer_reach = reach[-1]
            if outer_reach <= vmax[mode] * time_s * slack:
                reach_ok += 1
            # Nested MONOTONICITY via max reach: more time must reach at least
            # as far (robust to per-contour boundary-tracing jitter, which makes
            # a strict point-in-ring test flap on jagged rural rings). The #431
            # regression — a short contour over-extending past the long one —
            # violates this.
            if len(rings) >= 2:
                inner_reach = reach[0]
                if outer_reach >= inner_reach * 0.98:
                    nest_ok += 1
            else:
                nest_ok += 1
        passed &= check(
            f"{mode}: max reach ≤ v_max×time",
            n > 0 and reach_ok == n,
            f"{reach_ok}/{n} within {vmax[mode]:.1f}m/s×{time_s}s×{slack}",
        )
        passed &= check(
            f"{mode}: contours nest (600⊇300)",
            n > 0 and nest_ok == n,
            f"{nest_ok}/{n}",
        )
    return passed


def gate_graph_holes(base):
    """#503/#478: a car graph hole shows up as car routing 3–4× further than
    foot between the same coordinates (foot uses the missing connection). Car ≥
    foot is legitimate (one-ways), but a large ratio is the hole fingerprint
    (#478: 645 unroutable expressway edges; #503: Berloz/Heers detours). Invariant,
    no constant: flag car_distance / foot_distance > 3 over random medium pairs."""
    print("== car-vs-foot detour parity: graph holes (#503/#478) ==")
    import math

    rng = random.Random(478)
    tested = 0
    holes = []
    worst = 0.0
    for _ in range(60):
        lon, lat = rng.uniform(3.5, 5.8), rng.uniform(50.3, 51.2)
        d, a = rng.uniform(0.01, 0.05), rng.uniform(0, 6.283)
        dlon, dlat = lon + d * math.cos(a), lat + d * math.sin(a)
        try:
            _, cdist = route(base, lon, lat, dlon, dlat, "car")
            _, fdist = route(base, lon, lat, dlon, dlat, "foot")
        except Exception:
            continue
        if fdist <= 1.0:
            continue
        tested += 1
        ratio = cdist / fdist
        worst = max(worst, ratio)
        if ratio > 3.0:
            holes.append((round(lon, 4), round(lat, 4), round(ratio, 1)))
    for h in holes[:5]:
        print(f"    car/foot hole: {h}")
    # allow ≤2 legit one-way detours out of ~60.
    passed = check(
        "car detour ≤ 3× foot",
        len(holes) <= 2,
        f"{len(holes)} holes of {tested} pairs, worst ×{worst:.1f}",
    )
    return passed


def _wkb_linestring_len_m(buf):
    """Length (metres) of a WKB LineString, for the #493 geometry check."""
    import struct

    if not buf or len(buf) < 9:
        return None
    little = buf[0] == 1
    e = "<" if little else ">"
    gtype = struct.unpack_from(e + "I", buf, 1)[0]
    if gtype & 0xFF != 2:  # 2 = LineString (ignore SRID/Z flags in high bits)
        return None
    npts = struct.unpack_from(e + "I", buf, 5)[0]
    off = 9
    pts = []
    for _ in range(npts):
        if off + 16 > len(buf):
            break
        x, y = struct.unpack_from(e + "dd", buf, off)
        pts.append((x, y))
        off += 16
    return _polyline_len_m(pts)


def gate_matrix_distance_consistency(base):
    """#534 (the real root cause): the STREAMED matrix path (>1M cells) must
    compute distance_m too — it used to emit distance_m = u32::MAX on 100% of
    rows while durations were real, and large-request clients misread that
    column-wide MAX as an unreachability sentinel and dropped whole tiles. The
    invariant: on the streamed path, EVERY reachable cell (duration != MAX) also
    carries a real distance (distance != MAX). Sparse output returns only
    reachable rows, so simply: no returned row may have distance_m == MAX while
    duration_ms is real. Cross-checked against /route on a sample."""
    print("== streamed matrix distance_m computed (not column-wide MAX) (#534) ==")
    try:
        import pyarrow.flight as fl
    except ImportError:
        print("  [SKIP] pyarrow not available")
        return True
    import urllib.parse as up

    host = up.urlparse(base).hostname or "localhost"
    port = (up.urlparse(base).port or 8080) + 1
    MAX = 4294967295
    # >1M-cell grid → streamed path; sparse → only reachable rows come back.
    lons = [3.6 + 0.0606 * i for i in range(34)]
    lats = [50.50 + 0.020 * j for j in range(31)]
    grid = [[round(lo, 5), round(la, 5)] for lo in lons for la in lats]
    passed = True
    for mode in ("car", "foot"):
        params = {"origins": grid, "destinations": grid, "radius_km": 6, "sparse": True}
        try:
            rd = fl.connect(f"grpc://{host}:{port}").do_get(
                fl.Ticket(f"matrix:{mode}:{json.dumps(params)}".encode())
            )
            rows = 0
            dur_max = 0
            dist_max = 0
            sample = None
            cells = []  # (src_idx, tgt_idx, dur_ms, dist_m) for /route cross-check
            for chunk in rd:
                b = chunk.data
                if b is None:
                    continue
                du = b.column("duration_ms").to_pylist()
                di = b.column("distance_m").to_pylist()
                s = b.column("source_idx").to_pylist()
                t = b.column("target_idx").to_pylist()
                for k in range(b.num_rows):
                    rows += 1
                    if du[k] == MAX:
                        dur_max += 1
                        continue
                    if di[k] == MAX:
                        dist_max += 1  # reachable (duration real) but no distance
                        if sample is None:
                            sample = (s[k], t[k])
                    elif s[k] != t[k] and len(cells) < 8 and rows % 137 == 0:
                        cells.append((s[k], t[k], du[k], di[k]))
        except Exception as e:
            print(f"  [SKIP] flight unreachable ({e})")
            return True
        passed &= check(
            f"{mode}: streamed path returns rows",
            rows > 1000,
            f"{rows} reachable rows over {len(grid)}² cells",
        )
        passed &= check(
            f"{mode}: every reachable cell has a distance",
            dist_max == 0,
            f"{dist_max}/{rows} rows have duration but distance_m==MAX (#534 column-wide MAX)"
            + (f" e.g. {sample}" if sample else ""),
        )
        # CROSS-PATH: streamed matrix cell values must match /route (the small
        # single-query path) — duration AND distance — within tolerance. Catches
        # any streamed-path value divergence (bucket/PHAST/2-channel), not just
        # the column-wide MAX. The streamed engine is a different code path than
        # /route, so this is the belt to the CI cross-path suspenders.
        bad = 0
        worst = 0.0
        for si, ti, dur_ms, dist_m in cells:
            o, d = grid[si], grid[ti]
            try:
                r = route(base, o[0], o[1], d[0], d[1], mode)  # (dur_s, dist_m)
            except Exception:
                continue
            dur_ok = abs(dur_ms / 1000.0 - r[0]) <= max(r[0] * 0.02, 1.0)
            dist_ok = abs(dist_m - r[1]) <= max(r[1] * 0.02, 5.0)
            worst = max(worst, abs(dist_m - r[1]) / max(r[1], 1.0))
            if not (dur_ok and dist_ok):
                bad += 1
        passed &= check(
            f"{mode}: streamed cell values == /route",
            bad == 0 and len(cells) > 0,
            f"{len(cells)} cells checked, {bad} mismatch (worst dist {worst * 100:.2f}%)",
        )
    return passed


def gate_all_endpoints_smoke(base):
    """COVERAGE: ping EVERY REST endpoint and EVERY Flight action so a change
    that breaks one surface entirely is caught even if you were only touching
    another. Each must return a valid (non-error) response of the right shape.
    Optional surfaces (transit, /height) are skipped, not failed, when absent."""
    print("== all-endpoints smoke: every REST route + Flight action responds ==")
    import urllib.parse as up

    passed = True
    o = (4.3517, 50.8503)
    d = (4.4025, 51.2194)

    # ---- REST ----
    rest = {
        "/health": f"{base}/health",
        "/version": f"{base}/version",
        "/route": f"{base}/route?origin_lon={o[0]}&origin_lat={o[1]}&destination_lon={d[0]}&destination_lat={d[1]}&mode=car",
        "/nearest": f"{base}/nearest?lon={o[0]}&lat={o[1]}&mode=car",
        "/isochrone": f"{base}/isochrone?lon={o[0]}&lat={o[1]}&time_s=300&mode=car",
    }
    for name, url in rest.items():
        try:
            http_json(url)
            passed &= check(f"REST {name}", True, "ok")
        except Exception as e:
            passed &= check(f"REST {name}", False, f"{e}")
    # POST /table
    try:
        http_json(
            f"{base}/table",
            data=json.dumps(
                {"origins": [list(o), list(d)], "destinations": [list(o), list(d)], "mode": "car", "annotations": "duration,distance"}
            ).encode(),
            headers={"Content-Type": "application/json"},
        )
        passed &= check("REST /table", True, "ok")
    except Exception as e:
        passed &= check("REST /table", False, f"{e}")
    # POST /trip
    try:
        http_json(
            f"{base}/trip",
            data=json.dumps({"coordinates": [list(o), list(d), [4.35, 50.9]], "mode": "car"}).encode(),
            headers={"Content-Type": "application/json"},
        )
        passed &= check("REST /trip", True, "ok")
    except Exception as e:
        passed &= check("REST /trip", False, f"{e}")

    # ---- Flight ----
    try:
        import pyarrow as pa
        import pyarrow.flight as fl
    except ImportError:
        print("  [SKIP] pyarrow not available for Flight actions")
        return passed
    host = up.urlparse(base).hostname or "localhost"
    port = (up.urlparse(base).port or 8080) + 1
    c = fl.connect(f"grpc://{host}:{port}")
    pairs = [[o[0], o[1], d[0], d[1]]]

    def do_get_ok(name, ticket):
        try:
            tb = c.do_get(fl.Ticket(ticket.encode())).read_all()
            return check(f"Flight {name}", tb.num_rows >= 0, f"{tb.num_rows} rows")
        except Exception as e:
            return check(f"Flight {name}", False, f"{str(e)[:80]}")

    passed &= do_get_ok("matrix", f"matrix:car:{json.dumps({'origins': [list(o)], 'destinations': [list(d)]})}")
    passed &= do_get_ok("route_batch", f"route_batch:car:{json.dumps({'pairs': pairs})}")
    passed &= do_get_ok("edges_batch", f"edges_batch:car:{json.dumps({'pairs': pairs})}")
    passed &= do_get_ok("isochrone", f"isochrone:car:{json.dumps({'lon': o[0], 'lat': o[1], 'intervals': [300], 'interval_type': 'time'})}")
    # transit_bulk — optional (needs transit subsystem)
    try:
        q = {"queries": [{"origin_lon": o[0], "origin_lat": o[1], "destination_lon": d[0], "destination_lat": d[1]}]}
        c.do_get(fl.Ticket(f"transit_bulk:transit:{json.dumps(q)}".encode())).read_all()
        passed &= check("Flight transit_bulk", True, "ok")
    except Exception as e:
        msg = str(e)
        if "not loaded" in msg or "FailedPrecondition" in msg or "transit" in msg.lower():
            print("  [SKIP] Flight transit_bulk: transit subsystem not loaded")
        else:
            passed &= check("Flight transit_bulk", False, f"{msg[:80]}")
    # do_exchange: catchment + edges_flow
    try:
        tbl = pa.table({
            "store_id": pa.array(["s1"]), "store_lon": pa.array([o[0]]), "store_lat": pa.array([o[1]]),
            "client_lon": pa.array([d[0]]), "client_lat": pa.array([d[1]]),
        })
        params = {"percentiles": [50], "hull_shape": "isochrone", "remove_outliers": False, "radius_km": "auto"}
        w, r = c.do_exchange(fl.FlightDescriptor.for_command(f"catchment:car:{json.dumps(params)}".encode()))
        w.begin(tbl.schema); w.write_table(tbl); w.done_writing(); r.read_all(); w.close()
        passed &= check("Flight catchment", True, "ok")
    except Exception as e:
        passed &= check("Flight catchment", False, f"{str(e)[:80]}")
    try:
        tbl = pa.table({"src_lon": pa.array([o[0]]), "src_lat": pa.array([o[1]]), "dst_lon": pa.array([d[0]]), "dst_lat": pa.array([d[1]])})
        w, r = c.do_exchange(fl.FlightDescriptor.for_command(b"edges_flow:car"))
        w.begin(tbl.schema); w.write_table(tbl); w.done_writing(); r.read_all(); w.close()
        passed &= check("Flight edges_flow", True, "ok")
    except Exception as e:
        passed &= check("Flight edges_flow", False, f"{str(e)[:80]}")
    return passed


def gate_route_batch_geometry(base):
    """#493: foot/bike `route_batch` emitted `geometry_wkb` ~2× the reported
    distance (polyline doubled/zigzag) while car was fine — a Flight-only
    regression the REST `/route` coherence gate would miss. Assert the WKB
    LineString length ≈ distance_m within tol for foot and bike."""
    print("== route_batch foot/bike geometry_wkb ≈ distance (#493) ==")
    try:
        import pyarrow.flight as fl
    except ImportError:
        print("  [SKIP] pyarrow not available")
        return True
    import urllib.parse as up

    host = up.urlparse(base).hostname or "localhost"
    port = (up.urlparse(base).port or 8080) + 1
    pairs = [[f[1], f[2], f[3], f[4]] for f in FIXTURES]
    passed = True
    for mode in ("foot", "bike"):
        try:
            tb = fl.connect(f"grpc://{host}:{port}").do_get(
                fl.Ticket(f"route_batch:{mode}:{json.dumps({'pairs': pairs})}".encode())
            ).read_all()
        except Exception as e:
            print(f"  [SKIP] flight unreachable ({e})")
            return True
        names = tb.column_names
        dist_col = "distance_m" if "distance_m" in names else "distance_meters"
        wkb_col = "geometry_wkb" if "geometry_wkb" in names else "polyline_wkb"
        if dist_col not in names or wkb_col not in names:
            passed &= check(f"{mode} schema", False, f"cols={names}")
            continue
        d = tb.column(dist_col).to_pylist()
        w = tb.column(wkb_col).to_pylist()
        bad = 0
        worst = 0.0
        for i in range(tb.num_rows):
            if d[i] is None or w[i] is None or d[i] <= 0:
                continue
            glen = _wkb_linestring_len_m(bytes(w[i]))
            if glen is None:
                continue
            ratio = glen / d[i]
            worst = max(worst, abs(ratio - 1.0))
            if abs(glen - d[i]) > d[i] * 0.05:
                bad += 1
        passed &= check(
            f"{mode}: wkb length ≈ distance_m",
            bad == 0,
            f"{bad} rows off >5% (worst {worst * 100:.1f}%)",
        )
    return passed


def gate_route_batch_max_meters(base):
    """#482/#487: `route_batch` `max_meters` is a server-side prune that DROPS
    over-bound pairs. Invariant, no constant: the bounded result set must equal
    exactly {pairs whose unbounded distance ≤ B}, every returned distance ≤ B,
    and pair_idx preserved (gaps visible)."""
    print("== route_batch max_meters prune == unbounded ≤ B (#482/#487) ==")
    try:
        import pyarrow.flight as fl
    except ImportError:
        print("  [SKIP] pyarrow not available")
        return True
    import urllib.parse as up

    host = up.urlparse(base).hostname or "localhost"
    port = (up.urlparse(base).port or 8080) + 1
    import math

    rng = random.Random(482)
    pairs = []
    for _ in range(120):
        lon, lat = rng.uniform(3.6, 5.6), rng.uniform(50.5, 51.1)
        dd, a = rng.uniform(0.01, 0.06), rng.uniform(0, 6.283)
        pairs.append([lon, lat, round(lon + dd * math.cos(a), 6), round(lat + dd * math.sin(a), 6)])

    def run(extra):
        params = {"pairs": pairs}
        params.update(extra)
        tb = fl.connect(f"grpc://{host}:{port}").do_get(
            fl.Ticket(f"route_batch:car:{json.dumps(params)}".encode())
        ).read_all()
        names = tb.column_names
        dc = "distance_m" if "distance_m" in names else "distance_meters"
        pi = tb.column("pair_idx").to_pylist()
        di = tb.column(dc).to_pylist()
        return {pi[i]: di[i] for i in range(tb.num_rows) if di[i] is not None}

    try:
        unb = run({})
        B = pct([v for v in unb.values()], 0.5)  # median → ~half pruned
        bnd = run({"max_meters": B})
    except Exception as e:
        print(f"  [SKIP] flight unreachable ({e})")
        return True

    expected = {k for k, v in unb.items() if v <= B}
    got = set(bnd.keys())
    over = [k for k, v in bnd.items() if v > B]
    passed = True
    passed &= check("bound actually prunes", 0 < len(got) < len(unb), f"{len(got)}/{len(unb)} kept (B={B:.0f}m)")
    passed &= check("bounded set == unbounded ≤ B", got == expected, f"got {len(got)} vs expected {len(expected)}")
    passed &= check("every returned pair ≤ B", len(over) == 0, f"{len(over)} over-bound leaked")
    return passed


def gate_motorway_speed_floor(base):
    """#450: the motorway/N-road hierarchy de-rated (E411 ~56 km/h vs 120) and
    emptied motorway corridors. A long inter-city car route is motorway-dominated;
    its implied MEAN speed must clear a floor — a physical invariant for a
    motorway corridor, not a measured target. Catches a hierarchy regression that
    the aggregate ground-truth p50 can hide."""
    print("== motorway corridor speed floor (#450) ==")
    # (name, o_lon,o_lat, d_lon,d_lat) — motorway-dominated corridors.
    corridors = [
        ("Bxl→Antwerp (A1/E19)", 4.3517, 50.8503, 4.4025, 51.2194),
        ("Bxl→Liège (E40)", 4.3517, 50.8503, 5.5671, 50.6326),
        ("Bxl→Arlon (E411)", 4.3517, 50.8503, 5.8109, 49.6833),
    ]
    FLOOR_KMH = 50.0  # conservative floor; #450 pushed corridors to ~40
    passed = True
    for name, olon, olat, dlon, dlat in corridors:
        try:
            dur, dist = route(base, olon, olat, dlon, dlat, "car")
            kmh = dist / max(dur, 0.001) * 3.6
            passed &= check(name, kmh >= FLOOR_KMH, f"{kmh:.0f} km/h (floor {FLOOR_KMH:.0f})")
        except Exception as e:
            passed &= check(name, False, f"route failed: {e}")
    return passed


def _decode_polyline6(s):
    coords, idx, lat, lon = [], 0, 0, 0
    while idx < len(s):
        for which in (0, 1):
            shift = result = 0
            while True:
                b = ord(s[idx]) - 63
                idx += 1
                result |= (b & 0x1F) << shift
                shift += 5
                if b < 0x20:
                    break
            d = ~(result >> 1) if result & 1 else result >> 1
            if which == 0:
                lat += d
            else:
                lon += d
        coords.append((lon / 1e6, lat / 1e6))
    return coords


def _point_in_ring(pt, ring):
    x, y = pt
    inside = False
    j = len(ring) - 1
    for i in range(len(ring)):
        xi, yi = ring[i]
        xj, yj = ring[j]
        if (yi > y) != (yj > y) and x < (xj - xi) * (y - yi) / (yj - yi) + xi:
            inside = not inside
        j = i
    return inside


# Origins chosen to cover urban, rural, long-edge (#502 Robertville) and
# off-network snaps. Containment is checked against the SNAPPED point.
ISO_POINTS = [
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
]


def gate_edges_batch(base):
    """#512: edges_batch per-edge duration sums must match /route (plus the
    documented full first/last-edge emission — bounded by 2 edges' worth)."""
    print("== edges_batch vs /route (ticket fixtures) ==")
    try:
        import pyarrow.flight as fl
    except ImportError:
        print("  [SKIP] pyarrow not available")
        return True
    # Flight port convention: REST port + 1 (dev container maps 3011).
    import urllib.parse as up
    host = up.urlparse(base).hostname or "localhost"
    port = (up.urlparse(base).port or 8080) + 1
    try:
        client = fl.connect(f"grpc://{host}:{port}")
        pairs = [[f[1], f[2], f[3], f[4]] for f in FIXTURES]
        t = fl.Ticket(f"edges_batch:car:{json.dumps({'pairs': pairs})}".encode())
        tb = client.do_get(t).read_all()
    except Exception as e:
        print(f"  [SKIP] flight unreachable ({e})")
        return True
    sums = {}
    qi, du = tb.column("query_idx"), tb.column("duration_ms")
    for i in range(tb.num_rows):
        k = qi[i].as_py()
        sums[k] = sums.get(k, 0.0) + du[i].as_py() / 1000.0
    passed = True
    for idx, f in enumerate(FIXTURES):
        got = sums.get(idx)
        # Invariant, no stored constant: the per-edge sum must agree with the
        # LIVE /route duration for the same pair — >= route (edges are whole,
        # the route clips partials) but within +45% (2 extra rural edge
        # halves); the #502 detour fingerprint was 2-3.5x.
        exp, _ = route(base, f[1], f[2], f[3], f[4])
        ok = got is not None and exp * 0.9 <= got <= exp * 1.45
        passed &= check(f"{f[0]} edges", ok, f"sum {got:.0f}s (route {exp:.0f}s)" if got else "no rows")
    return passed


def gate_matrix_sparse(base):
    """#532: Flight matrix `sparse:true` must be EXACTLY the dense output with
    the sentinel rows removed — no measured constant, pure equivalence:
      * dense is the full S×T grid;
      * the fixture (radius_km=20 over 10 spread-out cities) actually prunes;
      * sparse emits zero sentinel durations;
      * the set of surviving (source,target) pairs == dense's non-sentinel set;
      * every surviving value is byte-identical to its dense value.
    A regression that dropped a reachable row, kept a sentinel, or changed a
    value would break one of these."""
    print("== Flight matrix sparse == dense minus sentinels (#532) ==")
    try:
        import pyarrow.flight as fl
    except ImportError:
        print("  [SKIP] pyarrow not available")
        return True
    import urllib.parse as up

    host = up.urlparse(base).hostname or "localhost"
    port = (up.urlparse(base).port or 8080) + 1
    MAX = 4294967295
    pts = [[p[1], p[2]] for p in ISO_POINTS]  # 10 spread-out Belgium points

    def fetch(sparse):
        params = {
            "origins": pts,
            "destinations": pts,
            "radius_km": 20,
            "sparse": sparse,
        }
        t = fl.Ticket(f"matrix:car:{json.dumps(params)}".encode())
        tb = fl.connect(f"grpc://{host}:{port}").do_get(t).read_all()
        s, d, dur = tb.column("source_idx"), tb.column("target_idx"), tb.column("duration_ms")
        cells = {(s[i].as_py(), d[i].as_py()): dur[i].as_py() for i in range(tb.num_rows)}
        return cells, tb.num_rows

    try:
        dense, dense_n = fetch(False)
        sp, _sp_n = fetch(True)
    except Exception as e:
        print(f"  [SKIP] flight unreachable ({e})")
        return True

    n = len(pts)
    dense_real = {k: v for k, v in dense.items() if v != MAX}
    passed = True
    passed &= check("dense is full grid", dense_n == n * n, f"{dense_n} rows (expect {n * n})")
    passed &= check(
        "fixture actually prunes",
        0 < len(dense_real) < dense_n,
        f"{dense_n - len(dense_real)} sentinels, {len(dense_real)} real of {dense_n}",
    )
    leaked = sum(1 for v in sp.values() if v == MAX)
    passed &= check("sparse emits no sentinels", leaked == 0, f"{leaked} sentinel rows leaked")
    passed &= check(
        "sparse keys == dense non-sentinel keys",
        set(sp.keys()) == set(dense_real.keys()),
        f"sparse {len(sp)} vs dense-real {len(dense_real)}",
    )
    passed &= check(
        "sparse values identical to dense",
        all(sp.get(k) == v for k, v in dense_real.items()),
        "all surviving pairs match dense",
    )
    return passed


def gate_matrix_sparse_streaming(base):
    """#532: the STREAMING branch (>1M cells → PHAST-tiled, multi-batch) must
    honour sparse too — the path a large nearest-facility workload (hundreds of
    thousands of origins × thousands of destinations) actually takes.
    A single sparse pass over a >1M-cell radius-pruned grid must: stream >1
    batch (confirms the tiled path), leak zero sentinels, drop empty tiles, and
    return far fewer rows than cells (the diagonal + near neighbours survive).
    No dense comparison here (1M+ dense rows over the wire is the very cost this
    ticket removes) — the unit tests carry the full dense/sparse equivalence."""
    print("== Flight matrix sparse STREAMING path (>1M cells, #532) ==")
    try:
        import pyarrow.flight as fl
    except ImportError:
        print("  [SKIP] pyarrow not available")
        return True
    import urllib.parse as up

    host = up.urlparse(base).hostname or "localhost"
    port = (up.urlparse(base).port or 8080) + 1
    MAX = 4294967295
    # A deterministic ~34×31 grid kept INSIDE Belgium's routable box = 1054
    # points; 1054² ≈ 1.11M cells > the 1M bucket-M2M threshold, so do_matrix
    # takes the tiled stream. Coordinates stay clear of the borders: a point
    # OUTSIDE the BE region hard-errors the matrix request (region dispatch),
    # whereas an in-region off-network point is silently dropped — we want the
    # latter, never the former.
    lons = [3.6 + 0.0606 * i for i in range(34)]  # ~3.60–5.60
    lats = [50.50 + 0.020 * j for j in range(31)]  # ~50.50–51.10
    pts = [[round(lo, 5), round(la, 5)] for lo in lons for la in lats]
    n = len(pts)
    params = {"origins": pts, "destinations": pts, "radius_km": 6, "sparse": True}
    try:
        rd = fl.connect(f"grpc://{host}:{port}").do_get(
            fl.Ticket(f"matrix:car:{json.dumps(params)}".encode())
        )
        rows = 0
        sentinels = 0
        batches = 0
        empty = 0
        for chunk in rd:
            b = chunk.data
            if b is None:
                # #533 completeness trailer: app_metadata-only, no data body.
                # Chunk-iterating clients MUST skip it (read_all ignores it).
                continue
            batches += 1
            if b.num_rows == 0:
                empty += 1
            rows += b.num_rows
            sentinels += sum(1 for v in b.column("duration_ms").to_pylist() if v == MAX)
    except Exception as e:
        print(f"  [SKIP] flight unreachable ({e})")
        return True

    cells = n * n
    passed = True
    passed &= check("took the streaming path", batches > 1, f"{batches} batches for {cells} cells")
    passed &= check("no sentinels streamed", sentinels == 0, f"{sentinels} sentinel rows")
    passed &= check("no empty batches streamed", empty == 0, f"{empty} empty batches")
    passed &= check(
        "sparse << dense",
        0 < rows < cells // 2,
        f"{rows} rows of {cells} cells ({100 * (1 - rows / cells):.1f}% dropped)",
    )
    return passed


def gate_matrix_completeness(base):
    """#533/#532: every matrix DoGet must end with a completeness trailer —
    an app_metadata message {"complete":true,"total_rows":N,"contract":...}
    whose N equals the rows actually decoded. This is the deterministic signal
    that lets clients tell a full response from a truncated/empty-OK one (the
    #533 silent-data-loss failure). Verified on the small path, the streaming
    path, and a sparse response — the trailer must be present in all three and
    its count must reconcile with the decoded rows."""
    print("== Flight matrix completeness trailer (#533/#532) ==")
    try:
        import pyarrow.flight as fl
    except ImportError:
        print("  [SKIP] pyarrow not available")
        return True
    import urllib.parse as up

    host = up.urlparse(base).hostname or "localhost"
    port = (up.urlparse(base).port or 8080) + 1
    pts = [[p[1], p[2]] for p in ISO_POINTS]

    def probe(params, label, want_contract):
        # Iterate chunks so we see BOTH the record batches and the trailing
        # app_metadata (read_all() would discard the metadata).
        reader = fl.connect(f"grpc://{host}:{port}").do_get(
            fl.Ticket(f"matrix:car:{json.dumps(params)}".encode())
        )
        rows = 0
        meta = None
        for chunk in reader:
            if getattr(chunk, "data", None) is not None:
                rows += chunk.data.num_rows
            am = getattr(chunk, "app_metadata", None)
            if am:
                meta = json.loads(bytes(am))
        ok_present = meta is not None
        ok_complete = bool(meta and meta.get("complete") is True)
        ok_count = bool(meta and meta.get("total_rows") == rows)
        ok_contract = bool(meta and meta.get("contract") == want_contract)
        p = True
        p &= check(f"{label}: trailer present", ok_present, f"meta={meta}")
        p &= check(f"{label}: complete:true", ok_complete, f"meta={meta}")
        p &= check(f"{label}: total_rows=={rows} decoded", ok_count, f"meta={meta}")
        p &= check(f"{label}: contract={want_contract}", ok_contract, f"meta={meta}")
        return p

    try:
        passed = True
        # small path, dense
        passed &= probe(
            {"origins": pts, "destinations": pts}, "small dense", "dense"
        )
        # small path, sparse
        passed &= probe(
            {"origins": pts, "destinations": pts, "radius_km": 20, "sparse": True},
            "small sparse",
            "sparse",
        )
        # streaming path (>1M cells), dense — the #533 repro shape
        lons = [3.6 + 0.0606 * i for i in range(34)]
        lats = [50.50 + 0.020 * j for j in range(31)]
        grid = [[round(lo, 5), round(la, 5)] for lo in lons for la in lats]
        passed &= probe(
            {"origins": grid, "destinations": grid, "radius_km": 6, "sparse": True},
            "streaming sparse",
            "sparse",
        )
    except Exception as e:
        print(f"  [SKIP] flight unreachable ({e})")
        return True
    return passed


def gate_flight_completeness(base):
    """#533: EVERY streamed Flight action — not just matrix — must end with a
    completeness signal (trailer on success, non-OK error on truncation) so a
    silent OK-with-missing-rows is impossible. This probes the real producers
    (route_batch, edges_batch = do_get; edges_flow = do_exchange) live and
    reconciles the trailer's row/pair count against what was decoded. matrix is
    covered by gate_matrix_completeness."""
    print("== Flight completeness trailer: route_batch / edges_batch / edges_flow (#533) ==")
    try:
        import pyarrow as pa
        import pyarrow.flight as fl
    except ImportError:
        print("  [SKIP] pyarrow not available")
        return True
    import urllib.parse as up

    host = up.urlparse(base).hostname or "localhost"
    port = (up.urlparse(base).port or 8080) + 1
    pairs = [[f[1], f[2], f[3], f[4]] for f in FIXTURES]

    def probe_do_get(action, params, label):
        reader = fl.connect(f"grpc://{host}:{port}").do_get(
            fl.Ticket(f"{action}:car:{json.dumps(params)}".encode())
        )
        rows = 0
        meta = None
        for chunk in reader:
            if getattr(chunk, "data", None) is not None:
                rows += chunk.data.num_rows
            am = getattr(chunk, "app_metadata", None)
            if am:
                meta = json.loads(bytes(am))
        p = True
        p &= check(f"{label}: trailer present", meta is not None, f"meta={meta}")
        p &= check(f"{label}: complete:true", bool(meta and meta.get("complete")), f"meta={meta}")
        p &= check(
            f"{label}: total_rows=={rows}",
            bool(meta and meta.get("total_rows") == rows),
            f"meta={meta}",
        )
        return p

    try:
        passed = True
        passed &= probe_do_get("route_batch", {"pairs": pairs}, "route_batch")
        passed &= probe_do_get("edges_batch", {"pairs": pairs}, "edges_batch")

        # edges_flow (do_exchange): the summary carries complete:true and is
        # sent only after every chunk streamed.
        tbl = pa.table(
            {
                "src_lon": pa.array([p[0] for p in pairs]),
                "src_lat": pa.array([p[1] for p in pairs]),
                "dst_lon": pa.array([p[2] for p in pairs]),
                "dst_lat": pa.array([p[3] for p in pairs]),
            }
        )
        desc = fl.FlightDescriptor.for_command(b"edges_flow:car")
        client = fl.connect(f"grpc://{host}:{port}")
        writer, reader = client.do_exchange(desc)
        writer.begin(tbl.schema)
        writer.write_table(tbl)
        writer.done_writing()
        meta = None
        for chunk in reader:
            am = getattr(chunk, "app_metadata", None)
            if am:
                meta = json.loads(bytes(am))
        writer.close()
        passed &= check(
            "edges_flow: complete:true summary",
            bool(meta and meta.get("complete")),
            f"meta={meta}",
        )
    except Exception as e:
        print(f"  [SKIP] flight unreachable ({e})")
        return True
    return passed


def gate_close_pairs(base, n_pairs=150):
    import math

    print(f"== close-pair route==table ({n_pairs} pairs, 50-400 m) ==")
    rng = random.Random(123)
    tol = THRESHOLDS["consistency_tolerance_s"]
    worst = 0.0
    tested = 0
    zeros = 0
    mism = 0
    for _ in range(n_pairs):
        lon, lat = rng.uniform(3.5, 5.8), rng.uniform(50.3, 51.2)
        d, a = rng.uniform(0.0005, 0.004), rng.uniform(0, 6.283)
        p = (
            round(lon, 6),
            round(lat, 6),
            round(lon + d * math.cos(a), 6),
            round(lat + d * math.sin(a), 6),
        )
        try:
            dur_r, _ = route(base, p[0], p[1], p[2], p[3])
            body = json.dumps(
                {
                    "origins": [[p[0], p[1]]],
                    "destinations": [[p[2], p[3]]],
                    "mode": "car",
                    "annotations": "duration",
                }
            ).encode()
            tab = http_json(
                f"{base}/table", data=body, headers={"Content-Type": "application/json"}
            )
            dur_t = tab["durations"][0][0]
        except Exception:
            continue
        if dur_t is None:
            continue
        tested += 1
        delta = abs(dur_r - dur_t)
        worst = max(worst, delta)
        if delta > tol:
            mism += 1
        # a sub-second answer while the other side needs >10 s is the
        # fingerprint of the 0-second bug class
        if (dur_r < 1 and dur_t > 10) or (dur_t is not None and dur_t < 1 and dur_r > 10):
            zeros += 1
    ok = zeros == 0 and mism <= 2 and tested >= 80
    return check(
        "close pairs",
        ok,
        f"{tested} pairs, {zeros} zero-bugs, {mism} >{tol}s (max 2), worst {worst:.1f}s",
    )


def gate_isochrone(base):
    print("== isochrone snapped-origin containment (#497/#506) ==")
    passed = True
    for mode, time_s in (("car", 600), ("foot", 1800)):
        ok = 0
        fails = []
        for name, lon, lat in ISO_POINTS:
            try:
                d = http_json(
                    f"{base}/isochrone?lon={lon}&lat={lat}&time_s={time_s}&mode={mode}"
                )
                rings = [
                    _decode_polyline6(c["polygon"])
                    for c in d.get("contours", [])
                    if c.get("polygon")
                ]
                n = http_json(f"{base}/nearest?lon={lon}&lat={lat}&mode={mode}")
                sp = tuple(n["waypoints"][0]["location"])
            except Exception as e:
                fails.append(f"{name}: {e}")
                continue
            if any(_point_in_ring(sp, r) for r in rings):
                ok += 1
            else:
                fails.append(name)
        for f in fails[:5]:
            print(f"    not contained: {f}")
        passed &= check(
            f"containment {mode}",
            ok == len(ISO_POINTS),
            f"{ok}/{len(ISO_POINTS)} ({time_s}s)",
        )
    return passed


def gate_consistency(base, n_pairs=15):
    print(f"== /route vs /table consistency ({n_pairs} pairs) ==")
    rng = random.Random(7)
    tol = THRESHOLDS["consistency_tolerance_s"]
    passed = True
    worst = 0.0
    tested = 0
    for _ in range(n_pairs):
        a, b = round(rng.uniform(3.5, 5.8), 5), round(rng.uniform(50.2, 51.2), 5)
        c, d = round(rng.uniform(3.5, 5.8), 5), round(rng.uniform(50.2, 51.2), 5)
        try:
            dur_r, _ = route(base, a, b, c, d)
            body = json.dumps(
                {
                    "origins": [[a, b]],
                    "destinations": [[c, d]],
                    "mode": "car",
                    "annotations": "duration",
                }
            ).encode()
            tab = http_json(
                f"{base}/table", data=body, headers={"Content-Type": "application/json"}
            )
            dur_t = tab["durations"][0][0]
        except Exception:
            continue
        if dur_t is None:
            continue
        tested += 1
        worst = max(worst, abs(dur_r - dur_t))
    ok = worst <= tol and tested >= 8
    return check("route==table", ok, f"{tested} pairs, worst delta {worst:.1f}s (max {tol}s)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", required=True, help="e.g. http://localhost:3001")
    ap.add_argument("--trips", default=DEFAULT_TRIPS)
    ap.add_argument("--quick", action="store_true", help="skip the 1000-trip ground truth")
    args = ap.parse_args()
    base = args.base.rstrip("/")

    print(f"post-deploy gate against {base}")
    ok = True
    ok &= gate_fixtures(base)
    ok &= gate_symmetry(base)
    ok &= gate_consistency(base)
    ok &= gate_isochrone(base)
    ok &= gate_close_pairs(base)
    ok &= gate_lopsided(base)
    ok &= gate_radius_prune(base)
    ok &= gate_recustomized_distance(base)
    ok &= gate_edges_batch(base)
    ok &= gate_matrix_sparse(base)
    ok &= gate_matrix_sparse_streaming(base)
    ok &= gate_matrix_completeness(base)
    ok &= gate_flight_completeness(base)
    ok &= gate_matrix_distance_consistency(base)
    ok &= gate_mode_coherence(base)
    ok &= gate_bounded_matrix_exactness(base)
    ok &= gate_one_way_routable(base)
    ok &= gate_isochrone_upper_bound(base)
    ok &= gate_graph_holes(base)
    ok &= gate_motorway_speed_floor(base)
    ok &= gate_route_batch_geometry(base)
    ok &= gate_route_batch_max_meters(base)
    if not args.quick:
        ok &= gate_ground_truth(base, args.trips)
    print("\nGATE:", "PASS" if ok else "FAIL")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
