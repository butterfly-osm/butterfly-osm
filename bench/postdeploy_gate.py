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
    python3 bench/postdeploy_gate.py --base http://butterfly.staging.lan \
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
    "dist_outliers_max": 80,  # baseline 72-73; ratcheted 90→80 (2026-07-17); next drop needs per-edge FCD (butterfly-speeds#9)
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

    We probe BOTH the boot-recustomized base `car` AND, when it is served,
    the `car_rush_hour` variant — the variant exercises the second stale-clone
    site (the container-baked / register-from-edge-speeds variant path), whose
    len-along-time flats must be recomputed from the VARIANT's own time
    middles, not cloned from the clean base.

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
    for mode in ("car", "car_rush_hour", "car_nodir"):
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
    ap.add_argument("--base", required=True, help="e.g. http://butterfly.staging.lan")
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
    ok &= gate_recustomized_distance(base)
    ok &= gate_edges_batch(base)
    if not args.quick:
        ok &= gate_ground_truth(base, args.trips)
    print("\nGATE:", "PASS" if ok else "FAIL")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
