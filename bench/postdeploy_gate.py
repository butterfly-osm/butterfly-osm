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
1b. ROUTE CHOICE (#545): on the SAME time-stamped set, the share of pairs
   whose engine route is materially longer than the route the observations
   were made on. Duration is judged like-for-like, which drops exactly those
   pairs; without this check a calibration change can collapse route choice
   onto the motorway network with every other gate green.
2. SYMMETRY: route(A→B) vs route(B→A) on seeded random pairs. The #502 snap
   bug's fingerprint was 4× asymmetry; a healthy two-way network stays <1.5×.
3. TICKET FIXTURES: the #502/#503 cases (Berloz, Heers, Robertville) checked
   against invariants that never expire (no pasted constants).
4. ENDPOINT AGREEMENT: /route and /table must agree on durations (±3 s) for
   the same pairs — one answer per question. TWO samplers in ONE gate (#550):
   uniform long pairs AND close pairs 50-400 m apart, the same-edge /
   co-located-candidate regime where a legacy same-rank shortcut and a reduce
   clamp both emitted bogus 0 s answers and which uniform sampling never
   reaches.
5. ISOCHRONE TOPOLOGY + CONTAINMENT (#497/#506/#535/#542): ONE simple
   polygon, closed CCW polyline6 ring, containing its own SNAPPED origin
   (snapped-road-point semantics — the raw query point may legitimately sit
   outside when it is far off-network); from a pedestrian centre the PIN
   itself is in or ≤ 30 m from the ring (#535). Car 600 s and foot 1800 s.
6. SURFACE COVERAGE: every REST path the server's own OpenAPI document lists
   is probed (a documented path with no probe, or a probe for a path the
   server no longer documents, is DRIFT and fails), and every Flight action
   answers. Documented-optional surfaces (/height without SRTM, transit
   without a feed) SKIP with their status and reason.

Fail-loud rules (#550)
----------------------
* pyarrow/pandas are REQUIRED: a runner without them used to green-light a
  deploy with every Flight invariant unchecked. Missing → FAIL at preflight,
  unless you explicitly pass `--no-flight` (then the Flight gates print SKIP).
* an unreachable Flight endpoint is a FAIL, not a skip.
* request exceptions are COUNTED, not swallowed: more than
  THRESHOLDS["max_errors"] transport errors in a sampling gate fails it.
  An HTTP 400/404 "no route" is a legitimate answer for an off-network or
  out-of-region point and is counted separately — gate_one_way_routable is
  the gate that owns directional 404s.

Usage
-----
    BUTTERFLY_REFS_DIR=/path/to/reference-trips \
    python3 bench/postdeploy_gate.py --base http://localhost:3001 \
        [--trips /path/to/od.csv] [--quick] [--no-flight] \
        [--flight-base grpc://host:port]
    python3 bench/postdeploy_gate.py --list-gates    # names only, exit 0
    python3 bench/test_postdeploy_gate.py            # offline unit checks

`--quick` skips the 1,000-trip ground truth (runs invariants only, ~30 s).
`$BUTTERFLY_REFS_DIR` is REQUIRED by the gates that read reference trips
(#589) — there is no default path. It is resolved when such a gate RUNS, so
`--help`, `--list-gates` and the unit tests need no environment; unset, those
gates FAIL by name instead of taking the process down.

Expensive server work is fetched ONCE and shared (#550/#572): `iso_bundle`
per isochrone query, `ref_trip_routes` per reference set, `streamed_matrix`
per >1M-cell Flight matrix request. A memoised FAILURE is re-raised to every
consumer, so a gate that runs second can never turn it into a silent PASS.
Thresholds live in ONE table (THRESHOLDS below); RATCHET THEM DOWN as tails
get fixed, never up. Every threshold is a ratio, a bound derived from ONE
tolerance, or a structural invariant — never a measured-then-pasted count.
"""

import argparse
import collections
import concurrent.futures as cf
import csv
import functools
import json
import math
import os
import random
import statistics
import struct
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

# Reference trip sets are generic CSV inputs (route_id,long_1,lat_1,long_2,
# lat_2,ref_min,ref_km) staged by the deploy tooling into $BUTTERFLY_REFS_DIR.
# 2026-09-03: durations are judged on the TIME-STAMPED typical set (weekday
# 07-19 h observed historic times); the old od.csv (1 000 long trips, no hour)
# is free-flow and let a free-flow engine pass at p50 1.0.
# #589: NO default directory — the old hardcoded fallback path was never
# created by anything, so a mis-staged runner failed deep inside a gate on a
# missing CSV instead of saying which variable was unset. The
# directory is resolved LAZILY, by `refs_path()`, when a refs-dependent gate
# actually runs: `--help`, `--list-gates` and the offline unit tests need no
# environment at all, and the three gates that do need it FAIL (loudly, naming
# the variable) instead of taking the process down at import time.
REFS_DIR = os.environ.get("BUTTERFLY_REFS_DIR")
DEFAULT_TRIPS = "od_typical.csv"  # under $BUTTERFLY_REFS_DIR
# Route-choice reference: the 1 000 long inter-city trips. Their ref_min is
# free-flow (no hour) so DURATIONS are judged on od_typical; their ref_km is
# a solid route-length truth (motorway-dominated), which the ~40-min regional
# od_typical pairs are not (the reference router and the engine pick
# different regional routes on ~17 % of them, identically on a free-flow
# engine — see #545).
LEGACY_TRIPS_DISTANCE = "od.csv"  # under $BUTTERFLY_REFS_DIR
REFS_PREFIX = "od"  # <prefix>_{typical,best,worst}.csv under $BUTTERFLY_REFS_DIR


class RefsUnavailable(RuntimeError):
    """#589: `$BUTTERFLY_REFS_DIR` is unset or not a directory. Raised by
    `refs_path()`, i.e. only from a gate that actually needs the reference
    trips — main() turns it into that gate's FAIL line, so the operator sees
    WHICH gates were skipped and WHY, and the rest of the suite still runs."""


def require_refs_dir(refs_dir=None):
    """The resolved reference directory, or RefsUnavailable naming the env
    variable. Never a default path: nothing creates one."""
    refs_dir = REFS_DIR if refs_dir is None else refs_dir
    if not refs_dir:
        raise RefsUnavailable(
            "BUTTERFLY_REFS_DIR is not set — export it to the directory holding the reference "
            f"trip sets ({REFS_PREFIX}_{{typical,best,worst}}.csv, {LEGACY_TRIPS_DISTANCE}, "
            "optional windows.json).")
    if not os.path.isdir(refs_dir):
        raise RefsUnavailable(f"BUTTERFLY_REFS_DIR={refs_dir!r} is not a directory.")
    return refs_dir


def refs_path(name, override=None):
    """Resolve a reference input. `override` (a --trips / --refs-prefix value)
    wins as given — an absolute or relative path the operator typed; otherwise
    `name` is joined onto $BUTTERFLY_REFS_DIR. Called at GATE RUN TIME, never
    at argparse-default time (that is the #589 crash: `--list-gates` died in
    `os.path.join(None, "od")` before printing a single name)."""
    if override:
        return override
    return os.path.join(require_refs_dir(), name)


# BASELINE 2026-07-16 (engine d97168d, 1000 trips, zero errors):
#   duration ratio: p05=0.854 p50=1.029 p90=1.246 p95=1.304 mean=1.048
#   distance ratio: p05=0.933 p50=1.004 p90=1.148 p95=1.253 mean=1.039
#   distance outliers (<0.85 / >1.2): 73 of 1000 = 7.3 %
#
# ONE table for every tolerance in this file (#550) — no gate hardcodes a
# bound of its own. Three kinds of entry, nothing else (#589):
#   * a RATIO of a sample (trip-set-size independent);
#   * a bound DERIVED from the level tolerance below (`derive_level_bounds`);
#   * a structural INVARIANT (a lower bound the engine's design guarantees).
THRESHOLDS = {
    # --- level (ground truth + bands): ONE tolerance, every bound derived ---
    # Pierre 2026-09-03 ("mieux vaut trop lent que trop rapide"): never more
    # than 2 % fast; up to `tol` + slack slow. windows.json (staged beside the
    # reference sets by the deploy tooling) may override these four; the
    # bounds `dur_p50`, `band_level`, `band_regional` are ALWAYS derived from
    # them by derive_level_bounds() — never stored, never inverted back.
    "never_fast": 0.98,  # lower bound of every level ratio (engine / reference)
    "tol": 0.06,  # level tolerance: like-for-like median may be this much slow ...
    "slack_level": 0.03,  # ... + this slack per profile level (best / typical / worst) → 1.09
    "slack_regional": 0.06,  # ... + this slack per region and on the ~40-min typical set → 1.12
    # --- ground truth (reference trips) ---
    "dur_p90_max": 1.30,
    # --- route CHOICE (#545) — see gate_route_choice for how it was chosen ---
    # A CEILING on further degradation, not a measurement: today's value on
    # the time-stamped set is ~0.186 and IS the defect. Ratchet DOWN when
    # #545 is fixed; a bound at today's value would bless it.
    "choice_divergent_frac": 0.25,
    "dist_p50": (0.97, 1.06),
    "dist_p90_max": 1.20,
    "dist_outliers_frac": 0.08,  # share of trips with distance ratio <0.85 or >1.2 (baseline 7.3 %)
    "like_for_like_km_tol": 0.10,  # pinned corridors: compare only same-length routes
    # --- sampling gates ---
    "symmetry_ratio_max": 1.5,
    "symmetry_violations_max": 0,
    "consistency_tolerance_s": 3.0,
    "close_pair_mismatch_max": 2,
    "max_errors": 5,  # transport errors / unroutable trips tolerated before failing
    # --- bands (#543) — `band_level` / `band_regional` are derived (see above) ---
    "band_spread_min": 1.10,  # median(worst/best) over the typical trips
    "band_min_trips": 100,  # like-for-like trips needed to judge a level
    "band_min_regional": 10,
    # --- isochrone geometry ---
    "iso_reach_slack": 1.20,  # max vertex reach ≤ v_max × T × slack
    "iso_nest_tol": 0.98,  # outer contour reach ≥ inner × tol
    "topology_outside_m": 150.0,  # a network vertex counts "outside" beyond this
    # INVARIANT (ratio, #589): share of the engine's OWN reachable network
    # vertices left > topology_outside_m outside the polygon. Detached reach
    # under ~300 m across is deliberately not drawn (crumb filter), which is
    # ~1.1 % at rural origins, ~0 % urban; anything above 1.5 % is under-draw.
    "topology_outside_frac": 0.015,
    "reach_in_tol": 1.02,  # served-network vertices reachable within 1.02 T
    "reach_in_over_frac": 0.01,  # ≤1 % may exceed it
    # 0.95 -> 0.99 (2026-09-04, measured on Belgium after #544). #544 fixed the
    # arrive field but deliberately left this knob alone: it is shared with the
    # depart direction and with the crumb filter's separate budget, and nobody
    # had measured the real-data residual. Measured now, car, T=600 s, 1500
    # sampled road points > 150 m outside the polygon per direction:
    #   arrive — earliest reachable far point 1.0217 T; NOTHING outside the
    #            polygon is reachable within T at all, so 0/1500 at every
    #            tolerance up to 1.00 T;
    #   depart — earliest 0.8867 T (one detached stub at Berloz, exactly the
    #            crumb the filter declines to draw); the next is 1.0133 T, so
    #            1/1500 at 0.99 T against a budget of 7.
    # One knob still serves both directions — the measurement says a split is
    # not needed. Not raised to 1.00: the depart frontier needs its own cell of
    # headroom, and the crumb filter keeps its separate 1.5 % budget in
    # gate_isochrone_topology.
    "reach_out_tol": 0.99,  # nothing reachable ≤0.99 T may lie outside
    "reach_out_frac": 0.005,  # ≤0.5 % tolerated
    "pin_near_ring_m": 30.0,  # #535: pin inside, or ≤30 m from the ring
    "pin_snap_max_m": 300.0,  # car-free centres snap 100-200 m
    # --- other invariants ---
    "geom_consistency_tol": 0.03,  # distance_m vs polyline length vs Σ annotations
    "ann_duration_tol": 0.15,  # Σ annotation duration vs duration_s (turn costs)
    "sentinel_max_detour": 8.0,  # bounded detour vs crow-fly
    "car_speed_kmh": (15.0, 135.0),
    "foot_speed_kmh": (2.0, 8.0),  # #522: foot routes reported up to 19 km/h
    "bike_speed_kmh": (5.0, 32.0),
    "motorway_floor_kmh": 50.0,
    # #606 `exclude=motorway`. `fast_share_floor_kmh` is the speed above which
    # the engine's own per-edge annotation means "unobstructed high-speed
    # link"; `exclude_fast_share_ratio` bounds how much of THAT share may
    # survive the exclusion, as a fraction of the SAME route's unrestricted
    # share — a ratio, so it encodes no level and moves with the profile.
    # Belgium keeps a real floor of non-motorway expressway (`trunk` at
    # 120 km/h, e.g. the N4), so the honest bound is small-but-nonzero:
    # measured 0.9-10.7 % after the fix against ~92 % before it, and the
    # bound sits between with >2x headroom on both sides.
    "fast_share_floor_kmh": 100.0,
    "exclude_fast_share_ratio": 0.25,
    "car_foot_detour_max": 3.0,
    "car_foot_holes_max": 2,
    "matrix_cell_tol": 0.02,  # streamed / 2-channel cell vs /route
    "wkb_len_tol": 0.05,  # route_batch geometry_wkb vs distance_m
    "edges_sum_bounds": (0.9, 1.45),  # Σ per-edge duration vs /route
    # #594: there is NO wall-clock threshold for the matrix plan any more.
    # gate_lopsided asserts the plan the SERVER REPORTS for the request it
    # just made (`x-butterfly-matrix-plan` on /table, `plan` in the Flight
    # matrix trailer) — the branch `phast_dir` actually took. Wall clock is
    # still printed as context, and is decisive for nothing.
    # INVARIANT (structural lower bound, #589): a road-following contour at a
    # 5-decimal Douglas-Peucker tolerance has hundreds of vertices; the
    # retired sector lasso (#536) could never emit more than 18. Not a
    # measured level — any value between the two shapes separates them.
    "catchment_min_vertices": 50,
}


def derive_level_bounds():
    """#589: the three level bounds come from ONE tolerance and named slack
    constants — `tol` is stored, the bounds are derived, never the reverse
    (the old code inverted `band_level[1] - 1.03` to recover `tol`, so a
    change to either literal silently moved the other)."""
    t = THRESHOLDS
    lo, tol = t["never_fast"], t["tol"]
    t["band_level"] = (lo, round(1.0 + tol + t["slack_level"], 3))  # per-profile median (#543)
    t["band_regional"] = (lo, round(1.0 + tol + t["slack_regional"], 3))  # per-region median, typical
    # The typical reference set IS regional ~40-min pairs → the same bound.
    t["dur_p50"] = t["band_regional"]
    return t


def _apply_windows_config(refs_dir=None):
    """One source of truth for the level tolerances (2026-09-04): when the
    deploy tooling stages `windows.json` beside the reference sets, its
    `never_fast` / `tol` / `slack_level` / `slack_regional` / `match_tol`
    override the defaults in THRESHOLDS — the same numbers the speed
    pipeline and the weekly window report use. Bounds are then re-derived."""
    refs_dir = REFS_DIR if refs_dir is None else refs_dir
    path = os.path.join(refs_dir, "windows.json") if refs_dir else None
    if not path or not os.path.exists(path):
        return derive_level_bounds()
    try:
        with open(path) as f:
            w = json.load(f)
    except Exception as ex:  # a broken config must not silently loosen the gate
        raise SystemExit(f"windows.json at {path} is unreadable: {ex}")
    for key in ("never_fast", "tol", "slack_level", "slack_regional"):
        if key in w:
            THRESHOLDS[key] = float(w[key])
    if "match_tol" in w:
        THRESHOLDS["like_for_like_km_tol"] = float(w["match_tol"])
    t = derive_level_bounds()
    print(f"[windows] {path}: never_fast {t['never_fast']}, tol {t['tol']}, slack {t['slack_level']}/"
          f"{t['slack_regional']}, match_tol {t['like_for_like_km_tol']} → dur_p50 {t['dur_p50']}, "
          f"band_level {t['band_level']}, band_regional {t['band_regional']}")
    return t


def print_thresholds():
    """#589 guard: every resolved threshold is printed, so a PASS line can be
    read against the bound that produced it."""
    print("thresholds:")
    for k in sorted(THRESHOLDS):
        print(f"  {k} = {THRESHOLDS[k]}")


# Import-time: derive the level bounds from the stored tolerance so THRESHOLDS
# is complete for any importer (the offline unit tests included). Reading
# `windows.json` is a LIVE-RUN step done by main() — it prints, and
# `--list-gates` must stay a bare list of names for the CI smoke diff.
derive_level_bounds()

MAX_U32 = 4294967295

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
FIXTURES = [("Berloz #503", 5.211554, 50.709124, 5.211383, 50.698323),
    ("Heers #503", 5.307080, 50.751610, 5.293005, 50.752418),
    ("Robertville #502", 6.008464, 50.428652, 6.022535, 50.428452) ]

# Origins chosen to cover urban, rural, long-edge (#502 Robertville) and
# off-network snaps. Containment is checked against the SNAPPED point.
ISO_POINTS = [("Brussels", 4.3517, 50.8503), ("Antwerp", 4.4025, 51.2194), ("Rixensart", 4.5286, 50.7115),
    ("Robertville #502", 6.008464, 50.428652),
    ("Heers #503", 5.30708, 50.75161),
    ("rural WB", 4.85, 50.55),
    ("Ardennes", 5.65, 50.10),
    ("coast", 2.95, 51.20),
    ("Ghent", 3.7174, 51.0543),
    ("Berloz #503", 5.211554, 50.709124) ]

# #535: pedestrian city centres — the pin sits in a car-free core, so the snap
# lands on the ring road and the polygon used to be drawn off-centre, leaving
# the pin outside. Checked alongside ISO_POINTS in gate_isochrone_topology.
PEDESTRIAN_CENTRES = [("Namur centre", 4.8667, 50.4632), ("Ghent Korenmarkt", 3.7234, 51.0543),
    ("Leuven Grote Markt", 4.7009, 50.8792) ]

# Runtime switches, set once by main().
CONFIG = {"flight": True, "flight_base": None}


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------
def http_bytes(url, timeout=120, headers=None):
    req = urllib.request.Request(url, headers=headers or {})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def http_json(url, timeout=30, data=None, headers=None):
    req = urllib.request.Request(url, data=data, headers=headers or {})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def post_json(url, payload, timeout=120):
    return http_json(url, timeout=timeout, data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"})


def http_status(url, method="GET", body=None, timeout=120):
    """(status, content_type, bytes). An HTTP error status is RETURNED, not
    raised — the endpoint smoke judges statuses (404 = optional surface,
    503 = subsystem not loaded). Transport failures still raise."""
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
        headers={"Content-Type": "application/json"} if data is not None else {})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.headers.get("Content-Type", "") or "", r.read()
    except urllib.error.HTTPError as e:
        ctype = e.headers.get("Content-Type", "") if e.headers else ""
        return e.code, ctype or "", e.read()


def route_json(base, olon, olat, dlon, dlat, mode="car", timeout=60, **extra):
    q = {
        "origin_lon": olon,
        "origin_lat": olat,
        "destination_lon": dlon,
        "destination_lat": dlat,
        "mode": mode}
    q.update(extra)
    return http_json(f"{base}/route?{urllib.parse.urlencode(q)}", timeout=timeout)


def route(base, olon, olat, dlon, dlat, mode="car"):
    d = route_json(base, olon, olat, dlon, dlat, mode)
    return d["duration_s"], d["distance_m"]


def table(base, origins, destinations, mode="car", timeout=120, **extra):
    return table_with_plan(base, origins, destinations, mode, timeout, **extra)[0]


# #594: the matrix plan the server reports for THIS request. `/table` sets the
# header, the Flight `matrix` completeness trailer carries the same value under
# `plan`. Closed set — anything else is a drift the gate must fail on.
MATRIX_PLAN_HEADER = "x-butterfly-matrix-plan"
MATRIX_PLANS = ("bucket", "phast_fwd", "phast_rev", "mixed")
# The sublinear plans: one seeded PHAST field per endpoint of the SHORT side
# (#526 forward, #527 reverse), instead of ~(S+T) bucket sweeps.
SUBLINEAR_PLANS = ("phast_fwd", "phast_rev")


def parse_matrix_plan(value):
    """Normalise a reported plan. A missing header (an old build, or a surface
    that dropped the report) and an unrecognised value both come back as
    markers that no plan check can accept — never silently as "fine"."""
    if value is None:
        return "<missing>"
    v = value.strip()
    return v if v in MATRIX_PLANS else f"<unknown:{v}>"


def table_with_plan(base, origins, destinations, mode="car", timeout=120, **extra):
    """(matrix, plan) — the plan is the server's own report of the engine it
    ran for this request (#594), not an inference from timing."""
    payload = {"origins": origins, "destinations": destinations, "mode": mode}
    payload.update(extra)
    req = urllib.request.Request(f"{base}/table", data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        body = json.loads(r.read())
        return body, parse_matrix_plan(r.headers.get(MATRIX_PLAN_HEADER))


def check_plan(label, got, want):
    """FAIL unless the server reported one of `want`. #594: this replaced a
    wall-clock WARN — the plan is a fact the engine states, so it is decisive."""
    return check(f"{label}: plan", got in want,
        f"server reported {got!r}, expected one of {list(want)}")


def is_no_route(exc):
    """A 400/404 is the server SAYING "no route / off network" — a legitimate
    answer for a random point, not a transport failure. Everything else
    (timeout, connection reset, 5xx, malformed body) is an error and counts
    against THRESHOLDS["max_errors"]."""
    return isinstance(exc, urllib.error.HTTPError) and exc.code in (400, 404)


def pct(xs, q):
    xs = sorted(xs)
    return xs[min(int(len(xs) * q), len(xs) - 1)]


def outlier_frac(ratios, lo=0.85, hi=1.2):
    """(count, share) of ratios outside [lo, hi] — the share is what the gate
    judges (#589: trip-set-size independent)."""
    n = sum(1 for r in ratios if r < lo or r > hi)
    return n, (n / len(ratios) if ratios else 0.0)


def check(name, ok, detail):
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}: {detail}")
    return ok


# #594: there is no `warn()` helper any more. Its only caller was the lopsided
# wall-clock ratio, now replaced by an assertion on the plan the server reports
# (`check_plan`). A helper whose whole purpose is "print, never fail" is an
# invitation to downgrade the next flaky check instead of making it
# deterministic. Context worth printing is printed with `print`.


def check_errors(label, errors, unroutable=None):
    """#550: sampling gates used to `continue` past every exception. Count
    them and FAIL past the tolerated budget."""
    t = THRESHOLDS["max_errors"]
    extra = f", {unroutable} unroutable (not an error)" if unroutable is not None else ""
    return check(f"{label}: request errors", errors <= t, f"{errors} (max {t}){extra}")


# ---------------------------------------------------------------------------
# Geometry — ONE implementation of each primitive (#550)
# ---------------------------------------------------------------------------
def haversine_m(lon1, lat1, lon2, lat2):
    r = 6371000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    a = (math.sin((p2 - p1) / 2) ** 2
        + math.cos(p1) * math.cos(p2) * math.sin(math.radians(lon2 - lon1) / 2) ** 2)
    return 2 * r * math.asin(math.sqrt(a))


def polyline_len_m(coords):
    return sum(haversine_m(coords[i][0], coords[i][1], coords[i + 1][0], coords[i + 1][1])
        for i in range(len(coords) - 1))


def decode_polyline6(s):
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


def point_in_ring(pt, ring):
    """Even-odd point-in-polygon. Closure-TOLERANT (#589): a closed ring
    (first vertex repeated last) gives the same answer as the open one — the
    duplicate closing edge has yi == yj and is never crossed. Callers pass
    ring[:-1] by convention; passing the closed ring is not a bug."""
    x, y = pt[0], pt[1]
    inside = False
    j = len(ring) - 1
    for i in range(len(ring)):
        xi, yi = ring[i]
        xj, yj = ring[j]
        if (yi > y) != (yj > y) and x < (xj - xi) * (y - yi) / (yj - yi) + xi:
            inside = not inside
        j = i
    return inside


def ring_area2(ring):
    """Signed shoelace (×2): >0 = CCW, <0 = CW."""
    s = 0.0
    n = len(ring)
    for i in range(n):
        x1, y1 = ring[i]
        x2, y2 = ring[(i + 1) % n]
        s += x1 * y2 - x2 * y1
    return s


def ring_area(ring):
    """Unsigned planar area (degrees²) — only ever compared to another one."""
    return abs(ring_area2(ring)) / 2.0


def dist_to_ring_m(p, ring):
    """Metres from (lon, lat) `p` to the nearest SIDE of `ring` — not to its
    vertices: after Douglas-Peucker a straight 450 m side has no vertex near a
    road that grazes it, and a vertex-distance reports ~200 m for a point
    5 m off the boundary."""
    kx = 111_320.0 * math.cos(math.radians(p[1]))
    ky = 110_540.0
    best = float("inf")
    for i in range(len(ring) - 1):
        ax, ay = (ring[i][0] - p[0]) * kx, (ring[i][1] - p[1]) * ky
        bx, by = (ring[i + 1][0] - p[0]) * kx, (ring[i + 1][1] - p[1]) * ky
        dx, dy = bx - ax, by - ay
        l2 = dx * dx + dy * dy
        t = 0.0 if l2 == 0.0 else max(0.0, min(1.0, -(ax * dx + ay * dy) / l2))
        d = math.hypot(ax + t * dx, ay + t * dy)
        if d < best:
            best = d
    return best


def wkb_type(buf):
    e = "<" if buf[0] == 1 else ">"
    return struct.unpack_from(e + "I", buf, 1)[0] & 0xFF


def wkb_polygons(buf):
    """Parse WKB Polygon (3) or MultiPolygon (6) into
    [[outer, hole, hole, ...], ...] of (lon, lat) rings. Stdlib-only."""

    def rd_poly(off, e):
        nrings = struct.unpack_from(e + "I", buf, off)[0]
        off += 4
        rings = []
        for _ in range(nrings):
            npts = struct.unpack_from(e + "I", buf, off)[0]
            off += 4
            ring = [struct.unpack_from(e + "dd", buf, off + 16 * i) for i in range(npts)]
            off += 16 * npts
            rings.append(ring)
        return rings, off

    e = "<" if buf[0] == 1 else ">"
    gtype = struct.unpack_from(e + "I", buf, 1)[0] & 0xFF
    if gtype == 3:
        return [rd_poly(5, e)[0]]
    if gtype == 6:
        n = struct.unpack_from(e + "I", buf, 5)[0]
        off = 9
        polys = []
        for _ in range(n):
            e2 = "<" if buf[off] == 1 else ">"
            t2 = struct.unpack_from(e2 + "I", buf, off + 1)[0] & 0xFF
            assert t2 == 3, f"MultiPolygon part of type {t2}"
            rings, off = rd_poly(off + 5, e2)
            polys.append(rings)
        return polys
    return []


def wkb_linestring_len_m(buf):
    """Length (metres) of a WKB LineString, for the #493 geometry check."""
    if not buf or len(buf) < 9:
        return None
    e = "<" if buf[0] == 1 else ">"
    if struct.unpack_from(e + "I", buf, 1)[0] & 0xFF != 2:  # 2 = LineString
        return None
    npts = struct.unpack_from(e + "I", buf, 5)[0]
    off = 9
    pts = []
    for _ in range(npts):
        if off + 16 > len(buf):
            break
        pts.append(struct.unpack_from(e + "dd", buf, off))
        off += 16
    return polyline_len_m(pts)


# ---------------------------------------------------------------------------
# Arrow Flight — ONE client, ONE port convention (#550)
# ---------------------------------------------------------------------------
_FLIGHT_CLIENTS = {}


def require_pyarrow():
    """Preflight: return the list of missing modules. Missing pyarrow/pandas
    used to make eleven gates return PASS without checking anything."""
    missing = []
    for mod in ("pyarrow", "pyarrow.flight", "pandas"):
        try:
            __import__(mod)
        except ImportError as e:
            missing.append(f"{mod} ({e})")
    return missing


def flight_enabled():
    return CONFIG["flight"]


def flight_uri(base):
    """Flight port convention: REST port + 1 (dev container maps 3011).
    Overridable with --flight-base for deploys that map it elsewhere."""
    if CONFIG["flight_base"]:
        return CONFIG["flight_base"]
    u = urllib.parse.urlparse(base)
    return f"grpc://{u.hostname or 'localhost'}:{(u.port or 8080) + 1}"


def flight_client(base):
    import pyarrow.flight as fl

    uri = flight_uri(base)
    if uri not in _FLIGHT_CLIENTS:
        _FLIGHT_CLIENTS[uri] = fl.FlightClient(uri)
    return _FLIGHT_CLIENTS[uri]


def flight_reader(base, action, mode, params):
    import pyarrow.flight as fl

    ticket = f"{action}:{mode}:{json.dumps(params)}".encode()
    return flight_client(base).do_get(fl.Ticket(ticket))


def flight_table(base, action, mode, params):
    return flight_reader(base, action, mode, params).read_all()


def flight_rows_meta(base, action, mode, params):
    """Iterate chunks (read_all() would discard the app_metadata trailer) and
    return (decoded_rows, trailer_dict_or_None). A chunk with data=None is the
    #533 completeness trailer — chunk-iterating clients MUST skip its body."""
    rows = 0
    meta = None
    for chunk in flight_reader(base, action, mode, params):
        if getattr(chunk, "data", None) is not None:
            rows += chunk.data.num_rows
        am = getattr(chunk, "app_metadata", None)
        if am:
            meta = json.loads(bytes(am))
    return rows, meta


def _exchange(base, command, tbl):
    """do_exchange (catchment, edges_flow): send `tbl`, drain the reply.
    Returns (rows as dicts, trailing app_metadata dict or None)."""
    import pyarrow.flight as fl

    writer, reader = flight_client(base).do_exchange(fl.FlightDescriptor.for_command(command))
    writer.begin(tbl.schema)
    writer.write_table(tbl)
    writer.done_writing()
    rows, meta = [], None
    for chunk in reader:
        if getattr(chunk, "data", None) is not None:
            rows.extend(chunk.data.to_pylist())
        am = getattr(chunk, "app_metadata", None)
        if am:
            meta = json.loads(bytes(am))
    writer.close()
    return rows, meta


def flight_matrix_cells(base, mode, params):
    """{(source_idx, target_idx): duration_ms} plus the row count."""
    tb = flight_table(base, "matrix", mode, params)
    s, t, dur = tb.column("source_idx"), tb.column("target_idx"), tb.column("duration_ms")
    cells = {(s[i].as_py(), t[i].as_py()): dur[i].as_py() for i in range(tb.num_rows)}
    return cells, tb.num_rows


# ---------------------------------------------------------------------------
# Isochrone bundle — ONE fetch per (origin, mode, time, direction) (#550)
# ---------------------------------------------------------------------------
_SNAP_CACHE = {}
_ISO_CACHE = {}


def snap_point(base, lon, lat, mode):
    key = (base, lon, lat, mode)
    if key not in _SNAP_CACHE:
        j = http_json(f"{base}/nearest?lon={lon}&lat={lat}&mode={mode}")
        _SNAP_CACHE[key] = tuple(j["waypoints"][0]["location"])
    return _SNAP_CACHE[key]


class IsoBundle:
    """Memoised view of ONE isochrone query: WKB polygons, JSON contours, the
    engine's own reachable network, the GeoJSON geometry and the snapped
    origin. Five gates assert against the same responses; before #550 each
    re-fetched them."""

    def __init__(self, base, lon, lat, mode, time_s, direction):
        self.base, self.lon, self.lat = base, lon, lat
        self.mode, self.time_s, self.direction = mode, time_s, direction
        self._memo = {}

    def _cached(self, key, fn):
        if key not in self._memo:
            self._memo[key] = fn()
        return self._memo[key]

    @property
    def q(self):
        return (f"lon={self.lon}&lat={self.lat}&mode={self.mode}"
            f"&direction={self.direction}&time_s={self.time_s}")

    def _json(self, extra=""):
        return self._cached(("json", extra),
            lambda: http_json(f"{self.base}/isochrone?{self.q}{extra}", timeout=120))

    @staticmethod
    def _rings(payload):
        """Outer rings of the JSON `contours[].polygon` (polyline6), in
        REQUEST order."""
        return [decode_polyline6(c["polygon"]) for c in payload.get("contours", []) if c.get("polygon")]

    @property
    def snap(self):
        return snap_point(self.base, self.lon, self.lat, self.mode)

    @property
    def wkb(self):
        return self._cached("wkb", lambda: http_bytes(
            f"{self.base}/isochrone?{self.q}", headers={"Accept": "application/octet-stream"}))

    @property
    def polys(self):
        return self._cached("polys", lambda: wkb_polygons(self.wkb))

    @property
    def json(self):
        return self._json()

    @property
    def rings(self):
        return self._rings(self._json())

    @property
    def network(self):
        return self._json("&include=network").get("network", [])

    @property
    def geojson(self):
        return self._json("&geometries=geojson")

    def contour_rings(self, times):
        """Multi-contour request (`contours=a,b`) — a DIFFERENT query shape
        from `time_s`, so it does not reuse `json`."""
        return self._cached(("contours", tuple(times)), lambda: self._rings(http_json(
            f"{self.base}/isochrone?lon={self.lon}&lat={self.lat}&mode={self.mode}"
            f"&direction={self.direction}&contours={','.join(str(t) for t in times)}",
            timeout=120)))


def iso_bundle(base, lon, lat, mode, time_s, direction="depart"):
    key = (base, lon, lat, mode, time_s, direction)
    if key not in _ISO_CACHE:
        _ISO_CACHE[key] = IsoBundle(base, lon, lat, mode, time_s, direction)
    return _ISO_CACHE[key]


# ---------------------------------------------------------------------------
# Reference trips — routed ONCE, shared by gate_bands and gate_ground_truth
# ---------------------------------------------------------------------------
_REF_ROUTES = {}


def ref_trips(path):
    """Reference trips CSV (local file under $BUTTERFLY_REFS_DIR) → dicts."""
    with open(path) as f:
        return list(csv.DictReader(f))


def ref_trip_routes(base, path):
    """Route every reference trip ONCE with `uncertainty=bands` and memoise:
    gate_bands needs the three band durations, gate_ground_truth needs the
    typical duration + distance — the same /route call (#550)."""
    key = (base, path)
    if key in _REF_ROUTES:
        return _REF_ROUTES[key]
    rows = ref_trips(path)

    def one(t):
        try:
            d = route_json(base, t["long_1"], t["lat_1"], t["long_2"], t["lat_2"], mode="car", timeout=60,
                uncertainty="bands")
        except Exception:
            return None
        return {
            "min": d["duration_s"] / 60.0,
            "best_min": (d.get("duration_best_s") or 0.0) / 60.0,
            "worst_min": (d.get("duration_worst_s") or 0.0) / 60.0,
            "km": d["distance_m"] / 1000.0}

    with cf.ThreadPoolExecutor(16) as ex:
        res = list(ex.map(one, rows))
    _REF_ROUTES[key] = (rows, res)
    return rows, res


def like_for_like(r, t):
    """The reference OD trips are PINNED corridors: only compare where the
    engine's free route has the same length (±10 %) — elsewhere a faster
    engine time is a better route, not a level error (Brussels, 2026-09-03)."""
    if r is None:
        return False
    try:
        km = float(t.get("ref_km") or 0)
    except (TypeError, ValueError):
        return False
    return km > 0 and abs(r["km"] / km - 1.0) <= THRESHOLDS["like_for_like_km_tol"]


# ---------------------------------------------------------------------------
# Gates — routing invariants
# ---------------------------------------------------------------------------
def _route_geometry_report(base, olon, olat, dlon, dlat, mode):
    """/route with geometry + annotations → the numbers every coherence check
    needs: (duration_s, distance_m, polyline length, Σann distance, Σann
    duration). Raises on transport failure."""
    d = route_json(base, olon, olat, dlon, dlat, mode=mode, geometries="polyline6",
        annotations="distance,duration")
    geom = d.get("geometry", {})
    poly = geom.get("polyline") or geom.get("coordinates_polyline6") or ""
    ann = d.get("annotations") or {}
    return (d["duration_s"], d["distance_m"], polyline_len_m(decode_polyline6(poly)) if poly else None,
        sum(ann.get("distance") or []),
        sum(ann.get("duration") or []))


def gate_fixtures(base):
    print("== sentinel pairs (#502/#503) — invariant checks, no expected constants ==")
    passed = True
    lo_kmh, hi_kmh = THRESHOLDS["car_speed_kmh"]
    max_detour = THRESHOLDS["sentinel_max_detour"]
    gtol = THRESHOLDS["geom_consistency_tol"]
    for name, olon, olat, dlon, dlat in FIXTURES:
        try:
            dur_s, dist_m, geom_m, ann_dist, ann_dur = _route_geometry_report(
                base, olon, olat, dlon, dlat, "car")
        except Exception as e:
            passed &= check(name, False, f"request failed: {e}")
            continue
        crow = haversine_m(olon, olat, dlon, dlat)
        detour = dist_m / max(crow, 1.0)
        kmh = dist_m / max(dur_s, 0.001) * 3.6
        ok_detour = detour <= max_detour
        ok_speed = lo_kmh <= kmh <= hi_kmh
        ok_geom = geom_m is None or abs(geom_m - dist_m) <= dist_m * gtol
        # annotations may legitimately differ from duration_s by the turn/
        # junction costs the summary carries; require them within 15%.
        ok_ann = ann_dist == 0 or (abs(ann_dist - dist_m) <= dist_m * gtol
            and abs(ann_dur - dur_s) <= dur_s * THRESHOLDS["ann_duration_tol"])
        gtxt = f"{geom_m:.0f}m" if geom_m is not None else "n/a"
        passed &= check(name, ok_detour and ok_speed and ok_geom and ok_ann,
            f"{dur_s:.0f}s/{dist_m:.0f}m detour×{detour:.2f}(≤{max_detour}) "
            f"{kmh:.0f}km/h geom={gtxt} annΣ={ann_dist:.0f}m/{ann_dur:.0f}s")
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
    gtol = THRESHOLDS["geom_consistency_tol"]
    for mode in ("foot", "bike"):
        lo_kmh, hi_kmh = THRESHOLDS[f"{mode}_speed_kmh"]
        for name, olon, olat, dlon, dlat in FIXTURES:
            try:
                dur_s, dist_m, geom_m, ann_dist, _ = _route_geometry_report(
                    base, olon, olat, dlon, dlat, mode)
            except Exception as e:
                passed &= check(f"{mode} {name}", False, f"request failed: {e}")
                continue
            if dist_m <= 0 or dur_s <= 0:
                passed &= check(f"{mode} {name}", False, f"degenerate {dist_m}m/{dur_s}s")
                continue
            kmh = dist_m / dur_s * 3.6
            ok_speed = lo_kmh <= kmh <= hi_kmh
            ok_geom = geom_m is None or abs(geom_m - dist_m) <= dist_m * gtol
            ok_ann = ann_dist == 0 or abs(ann_dist - dist_m) <= dist_m * gtol
            gtxt = f"{geom_m:.0f}m" if geom_m is not None else "n/a"
            passed &= check(f"{mode} {name}", ok_speed and ok_geom and ok_ann,
                f"{dur_s:.0f}s/{dist_m:.0f}m {kmh:.1f}km/h "
                f"(bound {lo_kmh:.0f}-{hi_kmh:.0f}) geom={gtxt} annΣ={ann_dist:.0f}m")
    return passed


def gate_symmetry(base, n_pairs=150):
    print(f"== symmetry invariant ({n_pairs} seeded random pairs) ==")
    rng = random.Random(99)
    t = THRESHOLDS
    pairs = [(round(rng.uniform(3.0, 6.2), 5), round(rng.uniform(49.6, 51.4), 5),
              round(rng.uniform(3.0, 6.2), 5), round(rng.uniform(49.6, 51.4), 5))
             for _ in range(n_pairs)]

    def probe(p):
        a, b, c, d = p
        try:
            f, _ = route(base, a, b, c, d)
            r, _ = route(base, c, d, a, b)
        except Exception as e:
            return ("unroutable" if is_no_route(e) else "error", None, None)
        return ("ok", f, r)

    with cf.ThreadPoolExecutor(16) as ex:
        res = list(ex.map(probe, pairs))
    violations = []
    tested = errors = unroutable = 0
    worst = 1.0
    for p, (kind, f, r) in zip(pairs, res):
        if kind == "error":
            errors += 1
            continue
        if kind == "unroutable":
            unroutable += 1
            continue
        if f < 60:
            continue
        tested += 1
        ratio = max(f, r) / max(min(f, r), 1)
        worst = max(worst, ratio)
        if ratio > t["symmetry_ratio_max"]:
            violations.append((ratio, p))
    for v in violations[:5]:
        print(f"    violation: ratio {v[0]:.2f} @ {v[1]}")
    passed = check("fwd/rev symmetry", len(violations) <= t["symmetry_violations_max"] and tested >= 50,
                   f"{tested} pairs, {len(violations)} >{t['symmetry_ratio_max']}x, worst {worst:.2f}")
    passed &= check_errors("symmetry", errors, unroutable)
    return passed


def gate_route_table_agreement(base, n_uniform=50, n_close=150):
    """MERGED gate_consistency + gate_close_pairs (#550): ONE route≡table
    agreement gate with two samplers, run in parallel.

    * uniform: long random pairs — /route and /table must return the SAME
      duration (worst |Δ| ≤ 3 s). One answer per question.
    * close (50-400 m): the same-edge / co-located-candidate regime where a
      legacy same-rank shortcut and a reduce clamp both emitted bogus 0 s
      answers. Uniform random pairs almost never land in it.
    """
    print(f"== route ≡ table agreement: {n_uniform} uniform + {n_close} close (50-400 m) pairs ==")
    tol = THRESHOLDS["consistency_tolerance_s"]
    rng_u = random.Random(7)
    uniform = [(round(rng_u.uniform(3.5, 5.8), 5), round(rng_u.uniform(50.2, 51.2), 5),
                round(rng_u.uniform(3.5, 5.8), 5), round(rng_u.uniform(50.2, 51.2), 5))
               for _ in range(n_uniform)]
    rng_c = random.Random(123)
    close = []
    for _ in range(n_close):
        lon, lat = rng_c.uniform(3.5, 5.8), rng_c.uniform(50.3, 51.2)
        d, a = rng_c.uniform(0.0005, 0.004), rng_c.uniform(0, 6.283)
        close.append((round(lon, 6), round(lat, 6),
                      round(lon + d * math.cos(a), 6), round(lat + d * math.sin(a), 6)))

    def probe(p):
        try:
            dur_r, _ = route(base, p[0], p[1], p[2], p[3])
            tab = table(base, [[p[0], p[1]]], [[p[2], p[3]]], annotations="duration")
            return ("ok", dur_r, tab["durations"][0][0])
        except Exception as e:
            return ("unroutable" if is_no_route(e) else "error", None, None)

    with cf.ThreadPoolExecutor(16) as ex:
        res_u = list(ex.map(probe, uniform))
        res_c = list(ex.map(probe, close))

    def measure(res):
        tested = errors = unroutable = mism = zeros = 0
        worst = 0.0
        for kind, dur_r, dur_t in res:
            if kind == "error":
                errors += 1
                continue
            if kind == "unroutable":
                unroutable += 1
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
            if (dur_r < 1 and dur_t > 10) or (dur_t < 1 and dur_r > 10):
                zeros += 1
        return tested, errors, unroutable, mism, zeros, worst

    t_u, e_u, u_u, m_u, z_u, w_u = measure(res_u)
    t_c, e_c, u_c, m_c, z_c, w_c = measure(res_c)
    max_mism = THRESHOLDS["close_pair_mismatch_max"]
    passed = check("uniform pairs: route == table",
                   w_u <= tol and z_u == 0 and t_u >= max(8, n_uniform // 2),
                   f"{t_u} pairs, {z_u} zero-bugs, {m_u} >{tol}s, worst delta {w_u:.1f}s (max {tol}s)")
    passed &= check("close pairs: route == table", z_c == 0 and m_c <= max_mism and t_c >= 80,
                    f"{t_c} pairs, {z_c} zero-bugs, {m_c} >{tol}s (max {max_mism}), worst {w_c:.1f}s")
    passed &= check_errors("route≡table", e_u + e_c, u_u + u_c)
    return passed


def _city_pairs():
    """Long inter-city car pairs (endpoints = ISO_POINTS cities)."""
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
    `gate_symmetry` counts a 404 as "unroutable", so a DIRECTIONAL 404 is
    invisible there; this asserts BOTH directions return a route."""
    print("== car one-way routability: no directional 404 (#197) ==")

    def probe(p):
        _, olon, olat, dlon, dlat = p
        out = []
        for a, b, c, d in ((olon, olat, dlon, dlat), (dlon, dlat, olon, olat)):
            try:
                route(base, a, b, c, d, "car")
                out.append(True)
            except Exception:
                out.append(False)
        return tuple(out)

    pairs = _city_pairs()
    with cf.ThreadPoolExecutor(16) as ex:
        res = list(ex.map(probe, pairs))
    fails = [f"{p[0]} (fwd={fwd} rev={rev})"
        for p, (fwd, rev) in zip(pairs, res)
        if not (fwd and rev)]
    n_ok = len(pairs) - len(fails)
    for f in fails[:5]:
        print(f"    directional gap: {f}")
    return check("both directions route", n_ok == len(pairs), f"{n_ok}/{len(pairs)} pairs route both ways")


def gate_graph_holes(base):
    """#503/#478: a car graph hole shows up as car routing 3–4× further than
    foot between the same coordinates (foot uses the missing connection). Car ≥
    foot is legitimate (one-ways), but a large ratio is the hole fingerprint
    (#478: 645 unroutable expressway edges; #503: Berloz/Heers detours). Invariant,
    no constant: flag car_distance / foot_distance > 3 over random medium pairs."""
    print("== car-vs-foot detour parity: graph holes (#503/#478) ==")
    rng = random.Random(478)
    pairs = []
    for _ in range(60):
        lon, lat = rng.uniform(3.5, 5.8), rng.uniform(50.3, 51.2)
        d, a = rng.uniform(0.01, 0.05), rng.uniform(0, 6.283)
        pairs.append((lon, lat, lon + d * math.cos(a), lat + d * math.sin(a)))

    def probe(p):
        try:
            _, cdist = route(base, p[0], p[1], p[2], p[3], "car")
            _, fdist = route(base, p[0], p[1], p[2], p[3], "foot")
        except Exception as e:
            return ("unroutable" if is_no_route(e) else "error", None, None)
        return ("ok", cdist, fdist)

    with cf.ThreadPoolExecutor(16) as ex:
        res = list(ex.map(probe, pairs))
    tested = errors = unroutable = 0
    holes = []
    worst = 0.0
    for p, (kind, cdist, fdist) in zip(pairs, res):
        if kind == "error":
            errors += 1
            continue
        if kind == "unroutable":
            unroutable += 1
            continue
        if fdist <= 1.0:
            continue
        tested += 1
        ratio = cdist / fdist
        worst = max(worst, ratio)
        if ratio > THRESHOLDS["car_foot_detour_max"]:
            holes.append((round(p[0], 4), round(p[1], 4), round(ratio, 1)))
    for h in holes[:5]:
        print(f"    car/foot hole: {h}")
    # allow ≤2 legit one-way detours out of ~60.
    passed = check(f"car detour ≤ {THRESHOLDS['car_foot_detour_max']:.0f}× foot",
        len(holes) <= THRESHOLDS["car_foot_holes_max"],
        f"{len(holes)} holes of {tested} pairs, worst ×{worst:.1f}")
    passed &= check_errors("graph holes", errors, unroutable)
    return passed


def gate_motorway_speed_floor(base):
    """#450: the motorway/N-road hierarchy de-rated (E411 ~56 km/h vs 120) and
    emptied motorway corridors. A long inter-city car route is motorway-dominated;
    its implied MEAN speed must clear a floor — a physical invariant for a
    motorway corridor, not a measured target. Catches a hierarchy regression that
    the aggregate ground-truth p50 can hide."""
    print("== motorway corridor speed floor (#450) ==")
    corridors = [("Bxl→Antwerp (A1/E19)", 4.3517, 50.8503, 4.4025, 51.2194),
        ("Bxl→Liège (E40)", 4.3517, 50.8503, 5.5671, 50.6326),
        ("Bxl→Arlon (E411)", 4.3517, 50.8503, 5.8109, 49.6833) ]
    floor = THRESHOLDS["motorway_floor_kmh"]
    passed = True
    for name, olon, olat, dlon, dlat in corridors:
        try:
            dur, dist = route(base, olon, olat, dlon, dlat, "car")
        except Exception as e:
            passed &= check(name, False, f"route failed: {e}")
            continue
        kmh = dist / max(dur, 0.001) * 3.6
        passed &= check(name, kmh >= floor, f"{kmh:.0f} km/h (floor {floor:.0f})")
    return passed


def gate_exclude_motorway(base):
    """#606: `exclude=motorway` was close to a no-op — it moved an inter-city
    duration by under 4 % and left an 18.6 km "motorway-free" route 80 % on
    links at 90 km/h or more, because the recustomization seeded every CCH
    edge with its build-time weight, which no exclusion can raise; a shortcut
    that summarised a motorway corridor kept the corridor.

    Four checks on the corridors `gate_motorway_speed_floor` already asserts
    ARE motorway-dominated (that gate fails if their unrestricted mean speed
    drops below `motorway_floor_kmh`), so "the motorway is the route" is not
    an assumption here — it is a neighbouring gate's assertion:

      1. EXACT, no constant — a restriction can never be FASTER. Before the
         fix the 18.6 km hop came back 0.3 % QUICKER with the option on, which
         is impossible for a strict subgraph and is the cheapest possible
         signature of a recustomization that did not take.
      2. EXACT, no constant — the mask is monotone: adding `toll,ferry` on top
         of `motorway` can only cost more time, never less.
      3. EXACT, no constant — `/route` and `/table` must agree under the SAME
         mask. Two independent engines (bidirectional CCH, bucket M2M) read
         the same recustomized weights, so a half-applied mask shows up as a
         disagreement.
      4. The corridor is actually abandoned. The unrestricted route runs
         mostly on links the engine itself annotates at motorway speed; the
         restricted one must not. The bound is RELATIVE — a fraction of the
         SAME route's unrestricted share — so it encodes no level and moves
         with the profile. Belgium leaves a real floor of non-motorway
         expressway (the N4 to Arlon is `trunk` at 120 km/h and is correctly
         NOT excluded), which is why the check is a ratio and not "zero":
         measured 0.9-10.7 % of the unrestricted share after the fix, ~92 %
         before it.
    """
    print("== exclude=motorway is strict (#606) ==")
    corridors = [("Bxl→Antwerp (A1/E19)", 4.3517, 50.8503, 4.4025, 51.2194),
        ("Bxl→Liège (E40)", 4.3517, 50.8503, 5.5671, 50.6326),
        ("Bxl→Arlon (E411)", 4.3517, 50.8503, 5.8109, 49.6833)]
    floor_kmh = THRESHOLDS["fast_share_floor_kmh"]
    max_ratio = THRESHOLDS["exclude_fast_share_ratio"]
    cell_tol = THRESHOLDS["matrix_cell_tol"]
    passed = True

    def fast_share(d):
        """Share of route LENGTH the engine annotates at >= floor_kmh."""
        ann = d.get("annotations") or {}
        dist, spd = ann.get("distance") or [], ann.get("speed") or []
        total = sum(dist)
        if total <= 0:
            return None
        return sum(x for x, s in zip(dist, spd) if s >= floor_kmh) / total

    for name, olon, olat, dlon, dlat in corridors:
        try:
            ann = "distance,duration,speed"
            plain = route_json(base, olon, olat, dlon, dlat, annotations=ann, timeout=600)
            excl = route_json(base, olon, olat, dlon, dlat, annotations=ann,
                exclude="motorway", timeout=600)
            more = route_json(base, olon, olat, dlon, dlat, annotations=ann,
                exclude="motorway,toll,ferry", timeout=600)
            cell = table(base, [[olon, olat]], [[dlon, dlat]], exclude="motorway",
                annotations="duration", timeout=600)["durations"][0][0]
        except Exception as e:
            passed &= check(name, False, f"request failed: {e}")
            continue

        d0, d1, d2 = plain["duration_s"], excl["duration_s"], more["duration_s"]
        passed &= check(f"{name}: a restriction is never faster",
            d1 >= d0, f"{d0:.0f}s -> {d1:.0f}s ({100 * (d1 / d0 - 1):+.1f}%)")
        passed &= check(f"{name}: the mask is monotone",
            d2 >= d1, f"motorway {d1:.0f}s -> +toll,ferry {d2:.0f}s")
        ok_cell = cell is not None and abs(cell - d1) <= max(d1 * cell_tol, 1.0)
        passed &= check(f"{name}: /table agrees under the same mask",
            ok_cell, f"/route {d1:.0f}s vs /table {cell if cell is None else f'{cell:.0f}'}s")

        s0, s1 = fast_share(plain), fast_share(excl)
        if s0 is None or s1 is None:
            passed &= check(f"{name}: fast-link share", False, "no annotations returned")
            continue
        # The corridor must be motorway-dominated for the check to mean
        # anything — gate_motorway_speed_floor asserts the same thing by speed.
        passed &= check(f"{name}: the corridor IS motorway",
            s0 >= 0.25, f"{100 * s0:.0f}% of its length at >= {floor_kmh:.0f} km/h")
        passed &= check(f"{name}: the excluded route leaves it",
            s1 <= s0 * max_ratio,
            f"{100 * s1:.1f}% at >= {floor_kmh:.0f} km/h "
            f"(<= {100 * s0 * max_ratio:.1f}% = {max_ratio:g} x the unrestricted {100 * s0:.0f}%)")
    return passed


# ---------------------------------------------------------------------------
# Gates — isochrone geometry (all share iso_bundle's fetches, #550)
# ---------------------------------------------------------------------------
def gate_isochrone_topology(base):
    """2026-09-03 (#535/#542 root cause): the contour used to keep ONE ring —
    every hole and every detached reachable component was silently dropped,
    and 1-cell corridors traced as zero-width spikes. Invariants, no measured
    constants:
      * PRODUCT RULE — the WKB is ONE simple Polygon: never a MultiPolygon,
        never a hole (an isochrone is one polygon by definition);
      * every ring is closed, has ≥ 4 points, no consecutive duplicates, no
        immediate backtrack (a,b,a) — the zero-width-spur signature;
      * outer rings CCW, holes CW (RFC 7946), each hole strictly inside its
        outer ring and never containing the origin;
      * the polyline6 `polygon` string of the JSON surface decodes to the SAME
        contract — a CLOSED, CCW ring (#589: the 0719f14 contract had no gate);
      * the snapped origin lies in the PRIMARY (first) polygon — #497/#506
        containment, folded in from the former `gate_isochrone` (#572);
      * from a pedestrian city centre the PIN ITSELF is inside the polygon or
        within `pin_near_ring_m` of it, on a snap no further than
        `pin_snap_max_m` (#535 — folded in from gate_ticket_invariants, #572);
      * self-consistency: the engine's own reachable network (include=network)
        is represented — at most 1.5% of its vertices lie > 150 m outside every
        polygon (sub-300 m detached stubs are deliberately not drawn);
      * every EXTRA component holds reachable network (confetti guard);
      * `geometries=geojson` carries a `geometry` object whose ring count
        matches the WKB.
    Both modes (#572): car 600 s and foot 1800 s — the pedestrian surface has
    its own tracer regime (dense short edges) and used to be checked only for
    containment."""
    print("== isochrone topology: ONE simple polygon, no spurs, faithful to the network (2026-09-03) ==")
    far_m = THRESHOLDS["topology_outside_m"]
    far_frac = THRESHOLDS["topology_outside_frac"]
    near_m = THRESHOLDS["pin_near_ring_m"]
    snap_max = THRESHOLDS["pin_snap_max_m"]
    passed = True
    origins = ([(nm, lo, la, False) for nm, lo, la in ISO_POINTS]
        + [(nm, lo, la, True) for nm, lo, la in PEDESTRIAN_CENTRES])
    for mode, time_s in (("car", 600), ("foot", 1800)):
        n_ok = n = far_total = verts_total = 0
        pin_ok = n_pin = 0
        details = []
        for name, lon, lat, is_centre in origins:
            b = iso_bundle(base, lon, lat, mode, time_s)
            try:
                wkb, polys, sp, net, gj = b.wkb, b.polys, b.snap, b.network, b.geojson
                p6 = b.rings
            except Exception as ex:
                details.append(f"{name}: {ex}")
                continue
            n += 1
            why = []  # non-empty ⇒ this origin FAILS
            # Product rule (2026-09-03): an isochrone IS one simple polygon —
            # a MultiPolygon of fragments or a polygon with holes is a defect.
            if not polys:
                why.append("no polygon in the WKB")
            elif wkb_type(wkb) != 3 or len(polys) != 1:
                why.append(f"WKB is not a single Polygon ({len(polys)} parts)")
            elif len(polys[0]) != 1:
                why.append(f"polygon has {len(polys[0]) - 1} hole(s)")
            for pi, rings in enumerate(polys):
                for ri, ring in enumerate(rings):
                    if len(ring) < 4 or ring[0] != ring[-1]:
                        why.append(f"p{pi}r{ri}: not a closed ring of ≥4 points")
                    body = ring[:-1]
                    if any(body[i] == body[(i + 1) % len(body)] for i in range(len(body))):
                        why.append(f"p{pi}r{ri}: consecutive duplicate vertex")
                    if any(body[i] == body[(i + 2) % len(body)] for i in range(len(body))):
                        why.append(f"p{pi}r{ri}: zero-width spur (a,b,a)")
                    a2 = ring_area2(body)
                    if ri == 0 and a2 <= 0:
                        why.append(f"p{pi}: outer ring not CCW")
                    if ri > 0:
                        if a2 >= 0:
                            why.append(f"p{pi}r{ri}: hole not CW")
                        if not point_in_ring(body[0], rings[0][:-1]):
                            why.append(f"p{pi}r{ri}: hole outside its outer ring")
                        if point_in_ring(sp, body):
                            why.append(f"p{pi}r{ri}: hole contains the origin")
            # #589: the JSON polyline6 `polygon` carries the SAME ring contract
            # as the WKB (closed, CCW) — 0719f14 shipped it, nothing checked it,
            # and a consumer decoding polyline6 sees only this surface.
            if len(p6) != 1:
                why.append(f"polyline6: {len(p6)} contour rings for one time_s (want 1)")
            for ri, r in enumerate(p6):
                if len(r) < 4 or r[0] != r[-1]:
                    why.append(f"polyline6 r{ri}: ring not closed (r[0] != r[-1])")
                elif ring_area2(r[:-1]) <= 0:
                    why.append(f"polyline6 r{ri}: ring not CCW")
            if polys and not point_in_ring(sp, polys[0][0][:-1]):
                why.append("origin not in the primary polygon")
            # self-consistency vs the engine's own reachable network
            pts = [tuple(p) for seg in net for p in seg][::3]
            far = 0
            for p in pts:
                inside = any(point_in_ring(p, rings[0][:-1])
                    and not any(point_in_ring(p, h[:-1]) for h in rings[1:])
                    for rings in polys)
                if inside:
                    continue
                if min((dist_to_ring_m(p, rings[0]) for rings in polys), default=1e9) > far_m:
                    far += 1
            far_total += far
            verts_total += len(pts)
            # 1.5 %: detached reach smaller than ~300 m across (crumb filter,
            # COMPONENT_MIN_AREA_CELLS) is deliberately not drawn — measured
            # 1.07-1.12 % at rural origins, 0.0-0.2 % urban. The pre-fix engine
            # lost 0.62 % beyond 150 m AND 9.57 % within 150 m of the boundary.
            if pts and far / len(pts) > far_frac:
                why.append(f"{far}/{len(pts)} reachable vertices > {far_m:.0f} m outside")
            # every EXTRA component must hold reachable network — a polygon with
            # no reachable road inside is confetti (a mis-oriented frontier
            # fragment, #542), not a place you drove to.
            for pi, rings in enumerate(polys[1:], start=1):
                if not any(point_in_ring(p, rings[0][:-1]) for p in pts):
                    why.append(f"p{pi}: component without any reachable network")
                    break
            g = (gj.get("contours") or [{}])[0].get("geometry")
            if not g:
                why.append("geojson: no `geometry` object")
            else:
                gr = (sum(len(p) for p in g["coordinates"]) if g["type"] == "MultiPolygon"
                      else len(g["coordinates"]))
                wr = sum(len(rings) for rings in polys)
                if gr != wr:
                    why.append(f"geojson rings {gr} != wkb rings {wr}")
            # #535: at a pedestrian centre the pin is off-network — the polygon
            # must still cover it (the pin -> snap access leg is stamped).
            if is_centre and polys:
                n_pin += 1
                ring = polys[0][0]
                snap_m = haversine_m(lon, lat, sp[0], sp[1])
                covered = (point_in_ring((lon, lat), ring[:-1])
                    or dist_to_ring_m((lon, lat), ring) <= near_m)
                if covered and snap_m <= snap_max:
                    pin_ok += 1
                else:
                    why.append(f"#535: pin inside/near={covered} snap {snap_m:.0f} m "
                               f"(max {snap_max:.0f} m)")
            if why:
                details.append(f"{name}: " + "; ".join(why[:3]))
            else:
                n_ok += 1
        for d in details[:6]:
            print(f"    {d}")
        passed &= check(f"{mode} {time_s}s: valid topology at every origin", n > 0 and n_ok == n,
            f"{n_ok}/{n} origins; network vertices > {far_m:.0f} m outside: "
            f"{far_total}/{verts_total} ({100.0 * far_total / max(verts_total, 1):.2f}%)")
        passed &= check(
            f"#535: {mode} {time_s}s isochrone from a pedestrian centre contains the pin "
            f"(one polygon, snap ≤ {snap_max:.0f} m)",
            n_pin == len(PEDESTRIAN_CENTRES) and pin_ok == n_pin,
            f"{pin_ok}/{len(PEDESTRIAN_CENTRES)} centres")
    return passed


def gate_isochrone_reach_truth(base):
    """2026-09-03: the polygon must reproduce the ENGINE's reach — both ways —
    with `/table` as the independent truth (found via #543: PHAST labels are
    HEAD arrivals; the stamp counted an edge's weight twice, cut every fast
    boundary edge one weight early and never drew the true frontier: 4-6 % of
    rural road points >150 m outside the polygon were reachable within T).
      (a) inside: the exposed vertices of the served network (last point of
          each polyline) are reachable within 1.02 T (≥ 99 %; the rest is
          /table snapping onto a neighbouring edge);
      (b) outside: road points > 150 m outside the polygon (taken from the
          1.4 T network) are NOT reachable within 0.95 T (≤ 0.5 %).
    depart = origin→point, arrive = point→origin."""
    print("== isochrone ≡ engine reach (/table truth, depart + arrive) (2026-09-03) ==")
    passed = True
    mode, T = "car", 600
    t = THRESHOLDS
    far_m = t["topology_outside_m"]

    def durations(origins, dests):
        return table(base, origins, dests, mode=mode, annotations="duration")["durations"]

    for direction in ("depart", "arrive"):
        n_in = n_in_over = n_out = n_out_reached = 0
        worst_out = None
        details = []
        for name, lon, lat in ISO_POINTS:
            b = iso_bundle(base, lon, lat, mode, T, direction)
            wide = iso_bundle(base, lon, lat, mode, int(T * 1.4), direction)
            try:
                ring = b.polys[0][0]
                net = b.network
                big = wide.network
            except Exception as ex:
                details.append(f"{name}: {ex}")
                continue
            rnd = random.Random(7)
            ends = [tuple(s[-1]) for s in net]
            rnd.shuffle(ends)
            ends = ends[:150]
            pts = [tuple(p) for s in big for p in s]
            rnd.shuffle(pts)
            far = []
            for p in pts:
                if len(far) >= 150:
                    break
                if point_in_ring(p, ring[:-1]):
                    continue
                if dist_to_ring_m(p, ring) > far_m:
                    far.append(p)
            if direction == "depart":
                d_in = durations([[lon, lat]], [list(e) for e in ends])[0]
                d_out = durations([[lon, lat]], [list(p) for p in far])[0] if far else []
            else:
                d_in = [row[0] for row in durations([list(e) for e in ends], [[lon, lat]])]
                d_out = [row[0] for row in durations([list(p) for p in far], [[lon, lat]])] if far else []
            d_in = [x for x in d_in if x is not None]
            d_out = [x for x in d_out if x is not None]
            n_in += len(d_in)
            n_in_over += sum(1 for x in d_in if x > t["reach_in_tol"] * T)
            n_out += len(d_out)
            reached = [x for x in d_out if x <= t["reach_out_tol"] * T]
            n_out_reached += len(reached)
            if reached:
                m = min(reached)
                if worst_out is None or m < worst_out[0]:
                    worst_out = (m, name)
                details.append(f"{name}: {len(reached)}/{len(d_out)} outside road points reachable "
                               f"≤ {t['reach_out_tol']}T (min {m:.0f} s)")
        for d in details[:4]:
            print(f"    {d}")
        passed &= check(f"{direction} {T}s: served network reachable within {t['reach_in_tol']}T",
                        n_in > 0 and n_in_over <= max(1, int(n_in * t["reach_in_over_frac"])),
                        f"{n_in - n_in_over}/{n_in} vertices")
        passed &= check(
            f"{direction} {T}s: nothing reachable ≤ {t['reach_out_tol']}T lies > {far_m:.0f} m outside",
            n_out_reached <= max(1, int(n_out * t["reach_out_frac"])),
            f"{n_out_reached}/{n_out} road points"
            + (f", earliest {worst_out[0]:.0f} s at {worst_out[1]}" if worst_out else ""))
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
    slack = THRESHOLDS["iso_reach_slack"]
    nest_tol = THRESHOLDS["iso_nest_tol"]
    passed = True
    for mode in ("car", "foot"):
        time_s = 600 if mode == "car" else 1800
        reach_ok = nest_ok = n = 0
        for name, lon, lat in ISO_POINTS:
            b = iso_bundle(base, lon, lat, mode, time_s)
            try:
                sp = b.snap
                # `contours=A,B` returns rings in request order (the `interval`
                # field is currently unlabelled). Ring 0 = inner (time_s/2),
                # ring -1 = outer (time_s).
                rings = b.contour_rings((time_s // 2, time_s))
            except Exception:
                continue
            if not rings:
                continue
            n += 1
            reach = [max((haversine_m(sp[0], sp[1], v[0], v[1]) for v in r), default=0.0)
                for r in rings]
            outer_reach = reach[-1]
            if outer_reach <= vmax[mode] * time_s * slack:
                reach_ok += 1
            # Nested MONOTONICITY via max reach: more time must reach at least
            # as far (robust to per-contour boundary-tracing jitter, which makes
            # a strict point-in-ring test flap on jagged rural rings). The #431
            # regression — a short contour over-extending past the long one —
            # violates this.
            if len(rings) >= 2:
                if outer_reach >= reach[0] * nest_tol:
                    nest_ok += 1
            else:
                nest_ok += 1
        passed &= check(f"{mode}: max reach ≤ v_max×time", n > 0 and reach_ok == n,
            f"{reach_ok}/{n} within {vmax[mode]:.1f}m/s×{time_s}s×{slack}")
        passed &= check(f"{mode}: contours nest (600⊇300)", n > 0 and nest_ok == n, f"{nest_ok}/{n}")
    return passed


# One user ticket -> the gate(s) that ARE its invariant (2026-09-03). The map
# is the documentation AND the check: gate_ticket_invariants fails if a gate
# named here is not in the registry, so folding or renaming a gate can never
# silently drop a user-visible guarantee. An EMPTY tuple means "deliberately
# not an engine invariant" and must carry its reason in TICKET_NOTES.
TICKET_GATES = {
    "#535": ("isochrone_topology",),
    "#536": ("catchment_containment",),
    "#541": (),
    "#542": ("isochrone_topology", "isochrone_reach_truth", "graph_holes"),
    "#543": ("bands",),
    "#605": ("route_batch_agrees_with_route",),
    "#545": ("route_choice",),
    "#606": ("exclude_motorway",),
    "#495/#497": ("isochrone_upper_bound", "isochrone_topology"),
}
TICKET_NOTES = {
    "#535": "isochrone off-centre / pin not covered: the pin is in (or within "
            "pin_near_ring_m of) the ONE polygon from a pedestrian centre",
    "#536": "square lasso, missing clients: the road hull is the threshold isochrone",
    "#541": "clip to the border is PRESENTATION — done consumer-side; the engine stays "
            "generic, so there is deliberately no engine gate",
    "#542": "islands / confetti: one simple polygon, faithful to the engine's reach, "
            "no graph holes",
    "#543": "isochrones too big vs a traffic-aware reference: typical/best/worst levels, "
            "like-for-like, never more than 2 % fast",
    "#605": "batch route \u2260 /route on short pairs: the two surfaces return the same "
            "duration and distance, on a sample that still hits the shared-snap case",
    "#545": "route CHOICE has no check: durations are judged like-for-like (which drops the\n            divergent pairs) and route length only on the legacy corridor set, so a calibration\n            can collapse choice onto the motorways with every gate green",
    "#606": "exclude=motorway close to a no-op: a restriction is never faster, the mask "
            "is monotone, /route == /table under it, and the corridor is left",
    "#495/#497": "size / foot origin: max reach <= v_max x time, snapped origin contained",
}


def gate_ticket_invariants(base):
    """One named invariant per user ticket, so none of them can regress
    silently. This gate does NOT re-measure anything (#572: the #535 pin probe
    it used to run is now inside gate_isochrone_topology, where the same
    isochrone is already fetched) — it asserts the MAP itself: every gate a
    ticket delegates to is registered and will run. Fold a gate away, rename
    it, or drop it from build_gates and this fails by ticket number."""
    del base  # structural check: no server call (the delegated gates make them)
    print("== ticket invariants: every user ticket delegates to a REGISTERED gate ==")
    registered = set(gate_names())
    passed = True
    for ticket in sorted(TICKET_GATES):
        gates = TICKET_GATES[ticket]
        note = TICKET_NOTES[ticket]
        if not gates:
            print(f"  [SKIP] {ticket}: {note}")
            continue
        missing = [g for g in gates if g not in registered]
        passed &= check(f"{ticket} -> {', '.join(gates)}", not missing,
            note if not missing else f"NOT REGISTERED: {missing} — {note}")
    return passed


# ---------------------------------------------------------------------------
# Gates — level vs the reference sets (share ref_trip_routes, #550)
# ---------------------------------------------------------------------------
def gate_ground_truth(base, trips_path, checks="all"):
    """checks: "all" | "duration" (errors + duration only) | "distance".
    Routes come from ref_trip_routes — the SAME /route responses gate_bands
    uses for its `typical` level, so each reference trip is routed once."""
    print(f"== ground truth: reference trips ({trips_path}, {checks}) ==")
    rows, res = ref_trip_routes(base, trips_path)
    t = THRESHOLDS
    ok_res = []
    errors = 0
    for r, trip in zip(res, rows):
        if r is None:
            errors += 1
            continue
        try:
            ref_min, ref_km = float(trip["ref_min"]), float(trip["ref_km"])
            if ref_min <= 0 or ref_km <= 0:
                raise ValueError("non-positive reference")
            ok_res.append((r["min"] / ref_min, r["km"] / ref_km))
        except (KeyError, TypeError, ValueError):
            errors += 1
    if not ok_res:
        return check("trip errors", False, f"{errors} of {len(rows)} trips unusable")
    # Duration is judged like-for-like (2026-09-03): the reference OD trips are
    # pinned corridors, so a trip whose engine route length is >10 % off the
    # measured one compares two different routes, not two levels.
    lfl = t["like_for_like_km_tol"]
    dur = [x[0] for x in ok_res if checks != "duration" or abs(x[1] - 1.0) <= lfl]
    dist = [x[1] for x in ok_res]
    outliers, out_frac = outlier_frac(dist)
    # #550: errors are checked on EVERY invocation (the distance pass used to
    # skip the count entirely).
    passed = check("trip errors", errors <= t["max_errors"], f"{errors} (max {t['max_errors']})")
    if checks in ("all", "duration"):
        p50d, p90d = pct(dur, 0.5), pct(dur, 0.9)
        passed &= check("duration p50", t["dur_p50"][0] <= p50d <= t["dur_p50"][1],
                        f"{p50d:.3f} (bounds {t['dur_p50']})")
        passed &= check("duration p90", p90d <= t["dur_p90_max"], f"{p90d:.3f} (max {t['dur_p90_max']})")
    if checks in ("all", "distance"):
        p50m, p90m = pct(dist, 0.5), pct(dist, 0.9)
        passed &= check("distance p50", t["dist_p50"][0] <= p50m <= t["dist_p50"][1],
                        f"{p50m:.3f} (bounds {t['dist_p50']})")
        passed &= check("distance p90", p90m <= t["dist_p90_max"], f"{p90m:.3f} (max {t['dist_p90_max']})")
        # #589: a RATIO of the trip set — an absolute count passed vacuously
        # on a shrunken CSV.
        passed &= check("distance outliers", out_frac <= t["dist_outliers_frac"],
                        f"{outliers}/{len(dist)} = {out_frac:.3f} (max {t['dist_outliers_frac']})")
    print(f"  stats: dur mean={statistics.mean(dur):.3f} p05={pct(dur, 0.05):.3f} p95={pct(dur, 0.95):.3f}"
        f" | dist mean={statistics.mean(dist):.3f} p05={pct(dist, 0.05):.3f} p95={pct(dist, 0.95):.3f}")
    return passed


def route_choice_stats(rows, res):
    """(length ratios, errors) for every reference pair: the engine's route
    length over the length the observations were actually made on.

    Pure — `rows`/`res` are whatever `ref_trip_routes` returned, so this is
    unit-testable with no server.

    There is deliberately NO `like_for_like` filter here. That filter keeps
    only the pairs whose engine route length is within ±10 % of the observed
    one, i.e. it DROPS exactly the pairs this statistic is about; it is the
    right thing for a LEVEL comparison (a duration measured on another route
    is not a level error) and it is precisely why the duration gate could not
    see #545.
    """
    ratios, errors = [], 0
    for trip, r in zip(rows, res):
        try:
            ref_km = float(trip["ref_km"])
            if r is None or ref_km <= 0:
                raise ValueError("unroutable trip or non-positive reference length")
            ratios.append(r["km"] / ref_km)
        except (KeyError, TypeError, ValueError):
            errors += 1
    return ratios, errors


def gate_route_choice(base, trips_path):
    """Route CHOICE on the TIME-STAMPED reference set (#545).

    The hole this closes: `gate_ground_truth` judges DURATION on the
    time-stamped set (like-for-like, so divergent pairs are dropped) and
    route LENGTH only on the legacy long-trip set — motorway-dominated
    corridors where a calibration change barely moves the choice. Nothing
    looked at route length on the regional pairs, so a calibration that made
    local roads relatively steeper could collapse route choice onto the
    motorway network with every gate green. That is #545, and it went
    unnoticed until it was investigated by hand.

    The statistic is the one `bench/route_choice.py` prints: the share of
    reference pairs whose engine route is at least `dist_p90_max` (1.20×) the
    observed length. The multiple is NOT a new constant — 1.20 is already
    this file's definition of "materially longer than the observed route"
    (`dist_p90_max`, and `outlier_frac`'s upper bound).

    HOW THE BOUND WAS CHOSEN. Today's value is ~0.186; that IS the defect, so
    the bound cannot be it. It cannot be much tighter either — a gate that
    fails on every deploy gets switched off, and a WARN is not allowed here.
    So the bound is the point at which divergence stops being a tail and
    becomes the engine's normal behaviour: ONE PAIR IN FOUR. It has a meaning
    that does not depend on any measurement, it leaves the current defect
    ~6 points of headroom (a third worse than today fails), and #545's own
    collapse was several times that in size. It is a CEILING on further
    degradation and says so on every run; the file's standing rule applies —
    ratchet it DOWN when #545 is fixed, never up.

    THE CONTROL ROW IS NOT PRACTICAL HERE, and that is a deliberate decision.
    `bench/route_choice.py`'s strength is `--compare`: the same pairs on the
    same build's UNCALIBRATED base weights, where divergence is ~zero, which
    separates a weights regression from an engine regression. A post-deploy
    gate is given ONE `--base`, and that control cannot be had from it:
      * the container's clean base weights are the hidden `car_freeflow`
        slot, deliberately absent from `mode_lookup` (ONE public car
        profile). Publishing it so a gate can read it would change the API
        to make a test convenient — the wrong direction;
      * `uncertainty=bands` is not a control: best/worst are recustomized
        from the SAME edge table by the same pass, so they share the route-
        choice shape and differ only in level;
      * anything else means a SECOND instance booted without the calibration
        input — two containers on two ports, a deploy-side arrangement, not
        something a gate handed one URL can conjure.
    So the isolation stays in `bench/route_choice.py`, where a second base is
    a natural flag and the operator is already looking at both. This gate
    detects the degradation; that script attributes it.

    Cost: zero extra server work — `ref_trip_routes` already routed every
    pair for `gate_ground_truth` and `gate_bands` (#550).
    """
    t = THRESHOLDS
    hi, bound = t["dist_p90_max"], t["choice_divergent_frac"]
    print(f"== route choice: engine route length vs the observed route ({trips_path}, #545) ==")
    rows, res = ref_trip_routes(base, trips_path)
    ratios, errors = route_choice_stats(rows, res)
    passed = check_errors("route choice", errors)
    # A shrunken or mis-staged reference set must not pass vacuously.
    if len(ratios) < t["band_min_trips"]:
        return check("route choice: usable pairs", False,
                     f"{len(ratios)} (need {t['band_min_trips']})")
    n_div = sum(1 for x in ratios if x >= hi)
    frac = n_div / len(ratios)
    passed &= check(f"share of pairs at or beyond {hi:.2f}x the observed length",
                    frac <= bound,
                    f"{n_div}/{len(ratios)} = {frac:.3f} (max {bound}) — KNOWN DEFECT #545: "
                    f"~0.19 today. The bound is a ceiling on FURTHER degradation, not an "
                    f"endorsement of the current value; ratchet it down when #545 is fixed.")
    print(f"  stats: length ratio p50={pct(ratios, 0.5):.3f} p90={pct(ratios, 0.9):.3f} "
          f"p95={pct(ratios, 0.95):.3f} mean={statistics.mean(ratios):.3f}")
    print("  attribution (weights vs engine) is bench/route_choice.py --compare <base-weights "
          "instance>; a single-instance gate cannot carry that control — see this gate's docstring.")
    return passed


def gate_bands(base, refs_prefix):
    """best / typical / worst (2026-09-03). ONE public car profile = typical
    (weekday 07-19 h), two opt-in bands on the same artefact: best = nights
    (free-flow), worst = weekday peaks. Invariants:
      (a) every API serves the bands on request: REST /route, /table (1×n,
          n×1, n×n), /trip, /isochrone; Flight matrix, route_batch,
          isochrone (`band` column, typical rows first);
      (b) ordering with a REAL spread: best ≤ typical ≤ worst per cell /
          route; isochrone best ⊇ typical ⊇ worst (areas); median
          worst/best over the reference trips ≥ 1.10;
      (c) level: median(engine/reference) within the band bounds for each
          profile against its own TIME-STAMPED reference set (observed
          historic times in the same window; the old 1 000 trips carried no
          hour and were free-flow), and typical within the regional bounds on
          Brussels-internal / coast pairs.
    """
    print("== best / typical / worst bands: every API, ordering, level (2026-09-03) ==")
    passed = True
    t = THRESHOLDS

    # ---- (a)+(b) REST surfaces on a few fixture pairs
    pairs = [((4.3517, 50.8503), (4.4025, 51.2194)), ((4.85, 50.55), (4.79, 50.60)),
             ((3.7174, 51.0543), (3.65, 51.02)), ((5.65, 50.1), (5.60, 50.15))]
    ok_route, n_route = True, 0
    for a, b in pairs:
        try:
            r = route_json(base, a[0], a[1], b[0], b[1], mode="car", uncertainty="bands")
            bt, tp, wt = r.get("duration_best_s"), r.get("duration_s"), r.get("duration_worst_s")
            n_route += 1
            ok_route &= bool(bt and wt and bt <= tp + 0.5 and tp <= wt + 0.5)
        except Exception as ex:
            ok_route = False
            print(f"    /route bands: {ex}")
    passed &= check("/route uncertainty=bands: duration_best_s ≤ duration_s ≤ duration_worst_s",
                    ok_route and n_route == len(pairs), f"{n_route} pairs")
    # /table 1×n, n×1, n×n
    pts = [[4.3517, 50.8503], [4.4025, 51.2194], [3.7174, 51.0543], [4.85, 50.55], [5.65, 50.1]]
    ok_table = True
    for shape, origins, dests in (("1×n", pts[:1], pts), ("n×1", pts, pts[:1]), ("n×n", pts, pts)):
        try:
            r = table(base, origins, dests, annotations="duration", uncertainty="bands")
            d, bt, wt = r["durations"], r.get("durations_best"), r.get("durations_worst")
            if not bt or not wt:
                ok_table = False
                print(f"    /table {shape}: no band grids")
                continue
            for i in range(len(origins)):
                for j in range(len(dests)):
                    if d[i][j] is not None and not (bt[i][j] <= d[i][j] + 0.5 and d[i][j] <= wt[i][j] + 0.5):
                        ok_table = False
        except Exception as ex:
            ok_table = False
            print(f"    /table {shape}: {ex}")
    passed &= check("/table uncertainty=bands (1×n, n×1, n×n): best ≤ typical ≤ worst per cell",
                    ok_table, "3 shapes")
    # /trip
    try:
        r = post_json(f"{base}/trip", {"points": pts[:4], "mode": "car", "uncertainty": "bands"})
        tr = (r.get("trips") or [r])[0]  # OSRM-shaped: {code, waypoints, trips:[{duration,...}]}
        bt, tp, wt = tr.get("duration_best"), tr.get("duration_s") or tr.get("duration"), tr.get("duration_worst")
        ok_trip = bool(bt and wt and tp and bt <= tp + 0.5 and tp <= wt + 0.5)
        detail = (f"best {bt:.0f} ≤ typical {tp:.0f} ≤ worst {wt:.0f}" if ok_trip
                  else str({k: tr.get(k) for k in ("duration_best", "duration_s", "duration_worst")}))
    except Exception as ex:
        ok_trip, detail = False, str(ex)
    passed &= check("/trip uncertainty=bands: duration_best ≤ duration ≤ duration_worst", ok_trip, detail)
    # /isochrone: areas ordered
    ok_iso, n_iso = True, 0
    for lon, lat in ((4.3517, 50.8503), (4.85, 50.55)):
        try:
            r = http_json(f"{base}/isochrone?lon={lon}&lat={lat}&time_s=600&mode=car"
                          "&uncertainty=bands&geometries=geojson", timeout=120)
            areas = {}
            for f in r.get("contours") or []:
                g = f.get("geometry") or {}
                if g.get("type") == "Polygon":
                    areas[f.get("band") or "typical"] = ring_area(g["coordinates"][0])
            n_iso += 1
            if not (areas.keys() >= {"best", "typical", "worst"}
                    and areas["best"] >= areas["typical"] * 0.999
                    and areas["typical"] >= areas["worst"] * 0.999):
                ok_iso = False
                print(f"    /isochrone bands areas: {areas}")
        except Exception as ex:
            ok_iso = False
            print(f"    /isochrone bands: {ex}")
    passed &= check("/isochrone uncertainty=bands: best ⊇ typical ⊇ worst (areas)",
                    ok_iso and n_iso == 2, f"{n_iso} origins")

    # ---- Flight (same client / port convention as every other Flight gate)
    if not flight_enabled():
        print("  [SKIP] Flight bands: --no-flight")
    else:
        def get(action, params):
            return flight_table(base, action, "car", params).to_pandas()

        m = get("matrix", {"origins": pts, "destinations": pts, "uncertainty": "bands"})
        okm = ("band" in m.columns and sorted(m["band"].unique()) == ["best", "typical", "worst"]
               and len(m) == 3 * len(pts) ** 2)
        if okm:
            piv = m.pivot_table(index=["source_idx", "target_idx"], columns="band", values="duration_ms")
            okm = bool(((piv["best"] <= piv["typical"] + 1) & (piv["typical"] <= piv["worst"] + 1)).all())
        passed &= check("Flight matrix uncertainty=bands: band column, 3 passes, best ≤ typical ≤ worst",
                        okm, f"{len(m)} rows")
        rb = get("route_batch", {"pairs": [[a[0], a[1], b[0], b[1]] for a, b in pairs],
                                 "uncertainty": "bands"})
        okr = "band" in rb.columns and len(rb) == 3 * len(pairs)
        if okr:
            piv = rb.pivot_table(index="pair_idx", columns="band", values="duration_s")
            okr = bool(((piv["best"] <= piv["typical"] + 0.5) & (piv["typical"] <= piv["worst"] + 0.5)).all())
        passed &= check("Flight route_batch uncertainty=bands: band column, best ≤ typical ≤ worst",
                        okr, f"{len(rb)} rows")
        iso = get("isochrone", {"lon": 4.85, "lat": 50.55, "intervals": [600], "uncertainty": "bands"})
        oki = "band" in iso.columns and len(iso) == 3
        if oki:
            # one NON-EMPTY polygon per band
            oki = {row["band"] for _, row in iso.iterrows() if len(row["polygon_wkb"] or b"")} >= {
                "best", "typical", "worst"}
        passed &= check("Flight isochrone uncertainty=bands: one polygon per band", oki, f"{len(iso)} rows")

    # ---- (c) level per profile against its time-stamped reference set
    lo, hi = t["band_level"]
    for name, field in (("typical", "min"), ("best", "best_min"), ("worst", "worst_min")):
        path = f"{refs_prefix}_{name}.csv"
        try:
            trips, res = ref_trip_routes(base, path)
        except Exception as ex:
            passed &= check(f"{name} level vs reference", False, f"cannot read reference set: {ex}")
            continue
        ratios = sorted(r[field] / float(trip["ref_min"])
            for r, trip in zip(res, trips)
            if like_for_like(r, trip) and r[field] > 0 and float(trip["ref_min"]) > 0)
        if len(ratios) < t["band_min_trips"]:
            passed &= check(f"{name} level vs reference", False,
                            f"only {len(ratios)} like-for-like trips (need {t['band_min_trips']})")
            continue
        med = statistics.median(ratios)
        # Pierre 2026-09-03: "mieux vaut trop lent que trop rapide" — never more
        # than 2 % fast, up to 9 % slow (the anchor lands the median at +3 %).
        passed &= check(f"{name}: median(engine/{name} reference) in [{lo}, {hi}] (like-for-like routes)",
                        lo <= med <= hi,
                        f"{med:.3f} (p10 {ratios[len(ratios) // 10]:.3f}, "
                        f"p90 {ratios[9 * len(ratios) // 10]:.3f}, n={len(ratios)})")
        if name != "typical":
            continue
        # Regional levels (#543: "too optimistic, especially Brussels"): the
        # national median can hide a region. One end inside the box is enough
        # for the coast (trips leave it); both for Brussels.
        rlo, rhi = t["band_regional"]
        for rname, box, both in (("Brussels-internal", (4.25, 50.76, 4.50, 50.92), True),
                                 ("coast (West Flanders)", (2.50, 51.00, 3.35, 51.40), False)):
            x0, y0, x1, y1 = box

            def inside(trip, k1, k2, x0=x0, y0=y0, x1=x1, y1=y1):
                return x0 <= float(trip[k1]) <= x1 and y0 <= float(trip[k2]) <= y1

            sel = [(r, trip) for r, trip in zip(res, trips) if like_for_like(r, trip)
                   and ((inside(trip, "long_1", "lat_1") and inside(trip, "long_2", "lat_2")) if both
                        else (inside(trip, "long_1", "lat_1") or inside(trip, "long_2", "lat_2")))]
            if len(sel) >= t["band_min_regional"]:
                mr = statistics.median(r["min"] / float(trip["ref_min"]) for r, trip in sel)
                passed &= check(f"typical: {rname} like-for-like pairs in [{rlo}, {rhi}] (#543)",
                                rlo <= mr <= rhi, f"{mr:.3f} (n={len(sel)})")
            else:
                print(f"    ({rname} typical pairs: {len(sel)} — not enough to check)")
        wb = [r["worst_min"] / r["best_min"] for r, trip in zip(res, trips)
              if like_for_like(r, trip) and r["best_min"] > 0]
        if wb:
            ms = statistics.median(wb)
            passed &= check(f"spread: median(worst/best) over the typical trips ≥ {t['band_spread_min']}",
                            ms >= t["band_spread_min"], f"{ms:.3f}")
    return passed


# ---------------------------------------------------------------------------
# Gates — matrix / table
# ---------------------------------------------------------------------------
def gate_lopsided(base):
    """#526/#527: a lopsided matrix (1xN or Nx1) must be SERVED by the
    sublinear seeded-PHAST plan, and stay cell-for-cell consistent with
    /route on both channels.

    #594 — the selection is asserted on the plan the SERVER REPORTS for the
    request the gate just made: `x-butterfly-matrix-plan` on /table and
    `plan` in the Flight `matrix` completeness trailer, both written at the
    branch site in `phast_dir`'s dispatch. That is a fact the engine states,
    so it is a hard FAIL. It replaces a wall-clock scaling ratio that had to
    be a WARN because a loaded runner flaps it; wall clock is still measured
    and PRINTED, and is decisive for nothing.

    Both plans are exercised BY SHAPE, which is the only way against a live
    server: `BUTTERFLY_MATRIX_ALGO=bucket|phast` is read ONCE at the first
    matrix call and then frozen for the life of the process (the `OnceLock`
    in `phast_dir`; docs/ENGINEERING.md), so a running server cannot be
    flipped from here — no pretending otherwise. Instead the router's own
    cost model decides, and the shapes below sit ~2 orders of magnitude
    apart in it: min(S,T) full scans vs (S+T) sweeps means 1xN and Nx1 take
    one field, while a balanced NxN would need N of them and takes the
    bucket. Asserting all three covers both branches per request."""
    print("== lopsided matrix: the SERVED plan + route==table (#526/#527/#594) ==")
    rng = random.Random(31)
    origin = (4.3517, 50.8503)
    dests = [(origin[0] + rng.uniform(-0.25, 0.25), origin[1] + rng.uniform(-0.15, 0.15))
             for _ in range(800)]

    def timed(dsts, **kw):
        t0 = time.time()
        r, plan = table_with_plan(base, [list(origin)], [list(d) for d in dsts],
                                  mode="foot", timeout=300, **kw)
        return r, plan, time.time() - t0

    timed(dests[:50])  # warm + calibrate the router's measured constants
    big, plan_1xn, tb = timed(dests)
    _small, _p, ts = timed(dests[:50])
    passed = check_plan("1x800 lopsided", plan_1xn, SUBLINEAR_PLANS)
    # Context only (#594): no threshold, no verdict. A sublinear plan stays
    # ~x1-3 against the 1x50 baseline, the linear bucket ~x16 — useful when
    # reading a failure, never the reason for one.
    print(f"    (wall clock, informational: 1x800 {tb:.2f}s vs 1x50 {ts:.2f}s "
          f"= x{tb / max(ts, 1e-3):.1f})")

    # #527 REVERSE lopsided: many sources, one target → one reverse field per
    # target. 2000 sources keeps the decision decisive even on a server whose
    # reverse-scan cell has never been measured (the router's structural
    # fallback prices a reverse field at 2 x n_nodes relaxations, so the
    # sweep side must be well past it).
    rev_origins = [[origin[0] + rng.uniform(-0.25, 0.25), origin[1] + rng.uniform(-0.15, 0.15)]
                   for _ in range(2000)]
    t0 = time.time()
    _rev, plan_nx1 = table_with_plan(base, rev_origins, [list(dests[0])], mode="foot", timeout=300)
    passed &= check_plan("2000x1 reverse-lopsided", plan_nx1, SUBLINEAR_PLANS)
    print(f"    (wall clock, informational: 2000x1 {time.time() - t0:.2f}s)")

    # The other branch: a balanced shape must take the bucket. N fields would
    # cost N full scans against 2N sweeps — the cost model is never close here.
    square = [[origin[0] + rng.uniform(-0.1, 0.1), origin[1] + rng.uniform(-0.06, 0.06)]
              for _ in range(40)]
    _sq, plan_sq = table_with_plan(base, square, square, mode="foot", timeout=300)
    passed &= check_plan("40x40 balanced", plan_sq, ("bucket",))

    def compare(tab, idxs, channel, relative):
        """Sampled table cells vs /route on the SAME mode → (checked, over-tol
        count, worst). `relative` picks the distance channel (relative error)
        over the duration channel (absolute seconds)."""
        def one(i):
            v = tab[channel][0][i]
            if v is None or tab["durations"][0][i] is None:
                return None
            try:
                dur_r, dist_r = route(base, origin[0], origin[1], dests[i][0], dests[i][1], mode="foot")
            except Exception:
                return None
            return abs(v - dist_r) / max(dist_r, 1.0) if relative else abs(v - dur_r)

        with cf.ThreadPoolExecutor(16) as ex:
            ds = [d for d in ex.map(one, idxs) if d is not None]
        tol = THRESHOLDS["matrix_cell_tol"] if relative else THRESHOLDS["consistency_tolerance_s"]
        return len(ds), sum(1 for d in ds if d > tol), max(ds, default=0.0)

    checked, mism, worst = compare(big, rng.sample(range(800), 25), "durations", False)
    passed &= check("lopsided route==table", mism == 0 and checked >= 15,
                    f"{checked} cells sampled, {mism} mismatches, worst {worst:.1f}s")
    # #527: 2-channel lopsided — distance channel must equal /route distance_m,
    # and the 2-channel router must reach the SAME sublinear plan (#562: it
    # costs on its OWN cell set, so a 1-channel-only regression would hide
    # here). The shape must therefore be decisive on a COLD 2-channel cell
    # set, exactly like the reverse probe above: with no 2-channel PHAST scan
    # ever measured, the router prices a forward field at n_nodes relaxations
    # against sweeps of n_nodes/400, so it only takes the field past
    # S + T > 400. A 1x300 probe (301) sits BELOW that and legitimately
    # planned `bucket` on a freshly booted server, while passing on any server
    # that had already served one 2-channel field — an order-dependent flake,
    # not an engine fact. 1x800 clears the structural crossover ~2x, the same
    # margin the 1-channel probe above has.
    dd, plan_2ch, _ = timed(dests, annotations="duration,distance")
    passed &= check_plan("1x800 lopsided, 2-channel", plan_2ch, SUBLINEAR_PLANS)
    dchecked, dmis, dworst = compare(dd, rng.sample(range(800), 25), "distances", True)
    passed &= check("lopsided 2-channel distance==route", dmis == 0 and dchecked >= 15,
                    f"{dchecked} cells, {dmis} mismatches, worst {dworst * 100:.2f}%")

    # The same assertion on the machine-facing surface: the Flight `matrix`
    # completeness trailer carries the plan too, so a regression that only
    # dropped the Flight report cannot pass on the REST one.
    if not flight_enabled():
        print("  [SKIP] Flight matrix plan: --no-flight")
    else:
        _rows, meta = flight_rows_meta(base, "matrix", "foot",
            {"origins": [list(origin)], "destinations": [list(d) for d in dests]})
        passed &= check_plan("Flight matrix 1x800 lopsided",
                             parse_matrix_plan((meta or {}).get("plan")), SUBLINEAR_PLANS)
    return passed


def _distance_channel_vs_route(base, mode):
    """Shared #528 probe: build a 1x200 /table with the 2-channel distance
    (length-along-time) annotation for `mode`, then compare 30 sampled cells
    against /route distance_m for the same OD pair on the SAME mode. Returns
    (checked, mism, worst, plan) or None if the mode is not served
    (unknown-mode 400s are a SKIP, not a FAIL — variants are deploy-dependent).

    #594: `plan` is the plan the server reported for the probe. It is REPORTED,
    not asserted — this gate is about the distance channel, and which plan it
    happens to exercise is a fact worth printing rather than a bound."""
    rng = random.Random(528)
    o = (4.3517, 50.8503)
    dests = [(o[0] + rng.uniform(-0.3, 0.3), o[1] + rng.uniform(-0.2, 0.2)) for _ in range(200)]
    try:
        tab, plan = table_with_plan(base, [list(o)], [list(d) for d in dests], mode=mode,
            timeout=200, annotations="duration,distance")
    except urllib.error.HTTPError as e:
        if e.code in (400, 404):
            return None  # mode not served — skip
        raise
    cell_tol = THRESHOLDS["matrix_cell_tol"]
    mism = checked = 0
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
        if rel > cell_tol:
            mism += 1
    return checked, mism, worst, plan


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
    still equal /route distance_m under those ties, on WHICHEVER 2-channel
    plan the router picks for the probe's 1x200 shape — the plan is printed
    (#594) rather than assumed: `BUTTERFLY_MATRIX_ALGO` is read once at the
    first matrix call and frozen for the life of the process, so this gate
    cannot pin one side. Which shape takes which plan is gate_lopsided's
    assertion; the 2-channel PHAST lex semantics (#530) are locked by the
    Rust unit test `phast_2ch_lex_tests`."""
    print("== recustomized-mode 2-channel distance==route (#528/#529) ==")
    passed = True
    for mode in ("car", "car_nodir"):
        res = _distance_channel_vs_route(base, mode)
        if res is None:
            print(f"  [SKIP] {mode} 2-channel distance==route: mode not served")
            continue
        checked, mism, worst, plan = res
        # `car` is always present (hard requirement); a served variant must
        # also hold. checked>=20 guards against a probe that snapped nothing.
        passed &= check(f"{mode} 2-channel distance==route", mism == 0 and checked >= 20,
            f"{checked} cells, {mism} mismatches, worst {worst * 100:.2f}%, plan {plan}")
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

    def durations(origins, radius):
        return table(base, origins, dests, mode="foot", annotations="duration", radius_km=radius,
        )["durations"]

    def kept(row):
        return [i for i, v in enumerate(row) if v is not None]

    scalar = kept(durations([o], 1.5)[0])
    ok_scalar = check("scalar radius_km=1.5 prunes", scalar == [0],
        f"kept {scalar} (want [0] — the ~2.2/3.3 km targets pruned)")
    per = durations([o, o, o], [1.5, 3.0, 0])
    rows = [kept(per[i]) for i in range(3)]
    ok_per = check("per-origin radius_km prunes each origin", rows == [[0], [0, 1], [0, 1, 2]],
        f"kept {rows} (want [[0],[0,1],[0,1,2]])")
    return ok_scalar and ok_per


def gate_radius_exactness(base):
    """#602: `radius_km` on REST /table prunes the COMPUTE (an effective time
    bound derived from the radius, plus an exact rescue), where it used to run
    the full N×M and null the pruned pairs at emit. The port is only correct if
    the rescue recovers everything the bound cut: the SAME pair set with and
    without a radius must agree cell for cell on everything inside the radius,
    duration AND distance. The unpruned run is the ground truth — no stored
    constant. Also reports the wall-clock the pruning buys."""
    print("== /table radius_km: pruned == unpruned inside the radius (#602) ==")
    rng = random.Random(602)
    # A spread of origins with clustered destinations, so a radius prunes a
    # large majority of the product but keeps a substantial in-radius set.
    origins = [[round(rng.uniform(3.4, 5.4), 6), round(rng.uniform(50.5, 51.2), 6)]
               for _ in range(60)]
    dests = [[round(rng.uniform(3.4, 5.4), 6), round(rng.uniform(50.5, 51.2), 6)]
             for _ in range(220)]
    R = 20.0
    passed = True
    for mode in ("car", "foot"):
        def run(extra):
            return table(base, origins, dests, mode=mode, annotations="duration,distance",
                         timeout=900, **extra)

        full = run({})
        pruned = run({"radius_km": R})
        fd, pd = full["durations"], pruned["durations"]
        fm, pm = full.get("distances"), pruned.get("distances")
        kept = mism = dropped = 0
        for i in range(len(origins)):
            for j in range(len(dests)):
                if pd[i][j] is None:
                    continue
                kept += 1
                if fd[i][j] is None:
                    dropped += 1
                elif fd[i][j] != pd[i][j]:
                    mism += 1
                elif fm and pm and fm[i][j] != pm[i][j]:
                    mism += 1
        # Everything the unpruned run reports inside the radius must survive.
        # The radius is measured on the SNAPPED endpoints — the same
        # coordinates the engine builds its neighbour mask from — so a pair
        # whose raw coordinates sit just inside 20 km but whose snapped ones
        # sit just outside is not a lost cell, it is correctly out of radius.
        so = [w["location"] for w in pruned["origins"]]
        sd = [w["location"] for w in pruned["destinations"]]
        lost = 0
        for i in range(len(origins)):
            for j in range(len(dests)):
                if fd[i][j] is not None and pd[i][j] is None and _within_km(
                        so[i], sd[j], R):
                    lost += 1
        # No timing here on purpose: two sequential calls over the same points
        # are not a fair comparison (the second runs warm), and this gate's job
        # is exactness. The speedup is measured separately, interleaved.
        passed &= check(f"{mode}: radius actually prunes",
                        0 < kept < len(origins) * len(dests),
                        f"{kept}/{len(origins) * len(dests)} cells kept")
        passed &= check(f"{mode}: every kept cell identical to the unpruned run",
                        mism == 0 and dropped == 0,
                        f"{mism} value mismatches, {dropped} cells the unpruned run "
                        "did not have")
        passed &= check(f"{mode}: no in-radius cell lost to the compute bound",
                        lost == 0,
                        f"{lost} cells inside {R} km present unpruned, missing pruned "
                        "(the #602 rescue failed)")
    return passed


def _within_km(a, b, km):
    """In-radius exactly as the engine decides it: same haversine, same earth
    radius (`nbg::EARTH_RADIUS_M`). The generic `haversine_m` above uses a
    round 6 371 000 m, which differs by 1.4e-6 — 28 mm at 20 km — and that is
    enough to disagree about a pair sitting on the boundary and report a
    correctly-pruned cell as a lost one."""
    r = 6371008.8
    p1, p2 = math.radians(a[1]), math.radians(b[1])
    h = (math.sin((p2 - p1) / 2) ** 2
        + math.cos(p1) * math.cos(p2) * math.sin(math.radians(b[0] - a[0]) / 2) ** 2)
    return 2 * r * math.asin(math.sqrt(h)) <= km * 1000.0


@functools.lru_cache(maxsize=1)
def _streaming_grid():
    """A deterministic ~34×31 grid kept INSIDE Belgium's routable box = 1054
    points; 1054² ≈ 1.11M cells > the 1M bucket-M2M threshold, so do_matrix
    takes the tiled stream. Coordinates stay clear of the borders: a point
    OUTSIDE the BE region hard-errors the matrix request (region dispatch),
    whereas an in-region off-network point is silently dropped — we want the
    latter, never the former."""
    lons = [3.6 + 0.0606 * i for i in range(34)]  # ~3.60–5.60
    lats = [50.50 + 0.020 * j for j in range(31)]  # ~50.50–51.10
    return tuple((round(lo, 5), round(la, 5)) for lo in lons for la in lats)


def streaming_params(radius_km=6):
    """The ONE >1M-cell sparse request shape (#572). Three gates used to build
    it independently and, being byte-identical, paid for the same 1.1M-cell
    pass three times — the memo below keys on this dict."""
    grid = [list(p) for p in _streaming_grid()]
    return {"origins": grid, "destinations": grid, "radius_km": radius_km, "sparse": True}


# One decoded >1M-cell matrix stream: everything any consumer needs, in ONE
# pass. `dist_sample` is the first reachable-but-distance-MAX cell (#534),
# `cells` a deterministic sample of (src, tgt, duration_ms, distance_m) for the
# /route cross-check, `trailer` the #533 completeness metadata.
StreamedMatrix = collections.namedtuple(
    "StreamedMatrix", "batches empties rows sentinels dist_max dist_sample cells trailer cells_total")

_STREAMED_MATRIX = {}


def _stream_matrix_once(base, mode, params):
    batches = empties = rows = sentinels = dist_max = 0
    dist_sample, trailer, cells = None, None, []
    for chunk in flight_reader(base, "matrix", mode, params):
        am = getattr(chunk, "app_metadata", None)
        if am:
            trailer = json.loads(bytes(am))
        b = getattr(chunk, "data", None)
        if b is None:
            # #533 completeness trailer: app_metadata only, no data body.
            # Chunk-iterating clients MUST skip it (read_all ignores it).
            continue
        batches += 1
        if b.num_rows == 0:
            empties += 1
        du = b.column("duration_ms").to_pylist()
        di = b.column("distance_m").to_pylist()
        src = b.column("source_idx").to_pylist()
        tgt = b.column("target_idx").to_pylist()
        for k in range(b.num_rows):
            rows += 1
            if du[k] == MAX_U32:
                sentinels += 1
                continue
            if di[k] == MAX_U32:
                dist_max += 1  # reachable (duration real) but no distance
                if dist_sample is None:
                    dist_sample = (src[k], tgt[k])
            elif src[k] != tgt[k] and len(cells) < 8 and rows % 137 == 0:
                cells.append((src[k], tgt[k], du[k], di[k]))
    n = len(params["origins"]) * len(params["destinations"])
    return StreamedMatrix(batches, empties, rows, sentinels, dist_max, dist_sample, cells, trailer, n)


def streamed_matrix(base, mode, params):
    """#572: ONE decoded Flight `matrix` stream per (base, mode, params),
    shared by gate_matrix_distance_consistency (car + foot),
    gate_matrix_sparse_streaming and gate_flight_completeness — the same
    1054² ≈ 1.11M-cell request was previously issued four times, minutes of
    server work for identical bytes.

    A FAILURE is memoised too, and re-raised on every call: each consumer
    still catches it and prints its own FAIL line, so a broken stream never
    turns into a silent PASS for the gates that ran second."""
    key = (base, mode, json.dumps(params, sort_keys=True))
    if key not in _STREAMED_MATRIX:
        try:
            _STREAMED_MATRIX[key] = _stream_matrix_once(base, mode, params)
        except Exception as ex:  # noqa: BLE001 — re-raised below, per consumer
            _STREAMED_MATRIX[key] = ex
    rec = _STREAMED_MATRIX[key]
    if isinstance(rec, Exception):
        raise rec
    return rec


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
    # Points span short + long-edge fixtures (large phantom shifts) so the
    # forward bound is actually exercised.
    pts = [[p[1], p[2]] for p in ISO_POINTS] + [[f[3], f[4]] for f in FIXTURES]

    def matrix(mode, max_minutes):
        params = {"origins": pts, "destinations": pts}
        if max_minutes is not None:
            params["max_minutes"] = max_minutes
        return flight_matrix_cells(base, mode, params)[0]

    passed = True
    for mode in ("car", "foot"):
        unb = matrix(mode, None)
        # threshold in minutes; ms → minutes for the compare.
        T_MIN = 15
        thr_ms = T_MIN * 60 * 1000
        bnd = matrix(mode, T_MIN)
        in_bound = {k: v for k, v in unb.items() if v != MAX_U32 and v <= thr_ms}
        missing = [k for k in in_bound if bnd.get(k, MAX_U32) == MAX_U32]
        wrong = [k for k in in_bound if bnd.get(k, MAX_U32) != MAX_U32 and bnd[k] != in_bound[k]]
        passed &= check(f"{mode}: fixture exercises the bound", len(in_bound) > 0,
                        f"{len(in_bound)} cells ≤ {T_MIN}min")
        passed &= check(f"{mode}: no in-bound cell falsely dropped", len(missing) == 0,
                        f"{len(missing)} in-bound cells came back u32::MAX (#534 forward-bound bug)")
        passed &= check(f"{mode}: in-bound values identical to unbounded", len(wrong) == 0,
                        f"{len(wrong)} cells differ from the unbounded value")
    return passed


def gate_matrix_distance_consistency(base):
    """#534 (the real root cause): the STREAMED matrix path (>1M cells) must
    compute distance_m too — it used to emit distance_m = u32::MAX on 100% of
    rows while durations were real, and large-request clients misread that
    column-wide MAX as an unreachability sentinel and dropped whole tiles. The
    invariant: on the streamed path, EVERY reachable cell (duration != MAX) also
    carries a real distance (distance != MAX). Sparse output returns only
    reachable rows, so simply: no returned row may have distance_m == MAX while
    duration_ms is real. Cross-checked against /route on a sample.

    #572: the stream itself comes from `streamed_matrix` — the same decoded
    pass gate_matrix_sparse_streaming and gate_flight_completeness assert on."""
    print("== streamed matrix distance_m computed (not column-wide MAX) (#534) ==")
    params = streaming_params()
    grid = params["origins"]
    cell_tol = THRESHOLDS["matrix_cell_tol"]
    passed = True
    for mode in ("car", "foot"):
        try:
            rec = streamed_matrix(base, mode, params)
        except Exception as ex:
            passed &= check(f"{mode}: streamed matrix decoded", False, f"{type(ex).__name__}: {ex}")
            continue
        passed &= check(f"{mode}: streamed path returns rows", rec.rows > 1000,
                        f"{rec.rows} reachable rows over {len(grid)}² cells")
        passed &= check(f"{mode}: every reachable cell has a distance", rec.dist_max == 0,
            f"{rec.dist_max}/{rec.rows} rows have duration but distance_m==MAX "
            "(#534 column-wide MAX)" + (f" e.g. {rec.dist_sample}" if rec.dist_sample else ""))
        # CROSS-PATH: streamed matrix cell values must match /route (the small
        # single-query path) — duration AND distance — within tolerance. Catches
        # any streamed-path value divergence (bucket/PHAST/2-channel), not just
        # the column-wide MAX. The streamed engine is a different code path than
        # /route, so this is the belt to the CI cross-path suspenders.
        bad = 0
        worst = 0.0
        for si, ti, dur_ms, dist_m in rec.cells:
            o, d = grid[si], grid[ti]
            try:
                r = route(base, o[0], o[1], d[0], d[1], mode)  # (dur_s, dist_m)
            except Exception:
                continue
            dur_ok = abs(dur_ms / 1000.0 - r[0]) <= max(r[0] * cell_tol, 1.0)
            dist_ok = abs(dist_m - r[1]) <= max(r[1] * cell_tol, 5.0)
            worst = max(worst, abs(dist_m - r[1]) / max(r[1], 1.0))
            if not (dur_ok and dist_ok):
                bad += 1
        passed &= check(f"{mode}: streamed cell values == /route", bad == 0 and len(rec.cells) > 0,
                        f"{len(rec.cells)} cells checked, {bad} mismatch (worst dist {worst * 100:.2f}%)")
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
    pts = [[p[1], p[2]] for p in ISO_POINTS]  # 10 spread-out Belgium points

    def fetch(sparse):
        return flight_matrix_cells(base, "car",
            {"origins": pts, "destinations": pts, "radius_km": 20, "sparse": sparse})

    dense, dense_n = fetch(False)
    sp, _sp_n = fetch(True)
    n = len(pts)
    dense_real = {k: v for k, v in dense.items() if v != MAX_U32}
    passed = check("dense is full grid", dense_n == n * n, f"{dense_n} rows (expect {n * n})")
    passed &= check("fixture actually prunes", 0 < len(dense_real) < dense_n,
        f"{dense_n - len(dense_real)} sentinels, {len(dense_real)} real of {dense_n}")
    leaked = sum(1 for v in sp.values() if v == MAX_U32)
    passed &= check("sparse emits no sentinels", leaked == 0, f"{leaked} sentinel rows leaked")
    passed &= check("sparse keys == dense non-sentinel keys", set(sp.keys()) == set(dense_real.keys()),
        f"sparse {len(sp)} vs dense-real {len(dense_real)}")
    passed &= check("sparse values identical to dense", all(sp.get(k) == v for k, v in dense_real.items()),
        "all surviving pairs match dense")
    return passed


def gate_matrix_sparse_streaming(base):
    """#532: the STREAMING branch (>1M cells → PHAST-tiled, multi-batch) must
    honour sparse too — the path a large nearest-facility workload (hundreds of
    thousands of origins × thousands of destinations) actually takes.
    A single sparse pass over a >1M-cell radius-pruned grid must: stream >1
    batch (confirms the tiled path), leak zero sentinels, drop empty tiles, and
    return far fewer rows than cells (the diagonal + near neighbours survive).
    No dense comparison here (1M+ dense rows over the wire is the very cost this
    ticket removes) — the unit tests carry the full dense/sparse equivalence.

    #572: the pass is `streamed_matrix`'s, shared with the distance-consistency
    and completeness gates."""
    print("== Flight matrix sparse STREAMING path (>1M cells, #532) ==")
    params = streaming_params()
    try:
        rec = streamed_matrix(base, "car", params)
    except Exception as ex:
        return check("streamed matrix decoded", False, f"{type(ex).__name__}: {ex}")
    cells = rec.cells_total
    passed = check("took the streaming path", rec.batches > 1,
        f"{rec.batches} batches for {cells} cells")
    passed &= check("no sentinels streamed", rec.sentinels == 0, f"{rec.sentinels} sentinel rows")
    passed &= check("no empty batches streamed", rec.empties == 0, f"{rec.empties} empty batches")
    passed &= check("sparse << dense", 0 < rec.rows < cells // 2,
        f"{rec.rows} rows of {cells} cells ({100 * (1 - rec.rows / cells):.1f}% dropped)")
    return passed


def gate_flight_completeness(base):
    """#533/#532: EVERY streamed Flight action must end with a completeness
    signal — an app_metadata trailer {"complete":true,"total_rows":N,...} whose
    N equals the rows actually decoded (a non-OK error on truncation). That is
    the deterministic way a client tells a full response from a truncated or
    empty-OK one; without it, silent data loss is indistinguishable from "no
    results".

    #572 merges the former `gate_matrix_completeness` in: ONE gate covers every
    producer — matrix dense / sparse / streaming (do_get), route_batch,
    edges_batch, isochrone (do_get) and edges_flow (do_exchange). The streaming
    probe reuses `streamed_matrix`'s decoded pass instead of issuing a fourth
    1.1M-cell request."""
    print("== Flight completeness trailer: matrix (dense/sparse/streaming), route_batch, "
        "edges_batch, isochrone, edges_flow (#533/#532) ==")
    import pyarrow as pa

    pts = [[p[1], p[2]] for p in ISO_POINTS]
    pairs = [[f[1], f[2], f[3], f[4]] for f in FIXTURES]

    def judge(label, rows, meta, want_contract=None, note=""):
        p = check(f"{label}: trailer present", meta is not None, f"meta={meta}{note}")
        p &= check(f"{label}: complete:true", bool(meta and meta.get("complete") is True),
            f"meta={meta}{note}")
        p &= check(f"{label}: total_rows=={rows} decoded",
            bool(meta and meta.get("total_rows") == rows), f"meta={meta}{note}")
        if want_contract is not None:
            p &= check(f"{label}: contract={want_contract}",
                bool(meta and meta.get("contract") == want_contract), f"meta={meta}{note}")
        return p

    def probe(label, action, params, mode="car", want_contract=None, note=""):
        rows, meta = flight_rows_meta(base, action, mode, params)
        return judge(label, rows, meta, want_contract, note)

    passed = probe("matrix dense", "matrix", {"origins": pts, "destinations": pts},
        want_contract="dense")
    passed &= probe("matrix sparse", "matrix",
        {"origins": pts, "destinations": pts, "radius_km": 20, "sparse": True},
        want_contract="sparse")
    # streaming path (>1M cells) — the #533 repro shape, decoded ONCE (#572)
    try:
        rec = streamed_matrix(base, "car", streaming_params())
        passed &= judge("matrix streaming sparse", rec.rows, rec.trailer, "sparse")
    except Exception as ex:
        passed &= check("matrix streaming sparse: trailer present", False,
            f"{type(ex).__name__}: {ex}")
    passed &= probe("route_batch", "route_batch", {"pairs": pairs})
    passed &= probe("edges_batch", "edges_batch", {"pairs": pairs})
    # isochrone: the gap this merge exposed, closed server-side by #560 — the
    # action now encodes through the same `completed_flight_stream` as every
    # other do_get arm. Probed plain AND banded: the banded path chains three
    # passes and may only claim complete when all three finished.
    iso = {"lon": ISO_POINTS[0][1], "lat": ISO_POINTS[0][2], "intervals": [600]}
    passed &= probe("isochrone", "isochrone", iso)
    passed &= probe("isochrone bands", "isochrone", {**iso, "uncertainty": "bands"})
    # edges_flow (do_exchange): the summary carries complete:true and is
    # sent only after every chunk streamed.
    tbl = pa.table({"src_lon": pa.array([p[0] for p in pairs]), "src_lat": pa.array([p[1] for p in pairs]),
                    "dst_lon": pa.array([p[2] for p in pairs]), "dst_lat": pa.array([p[3] for p in pairs])})
    _rows, meta = _exchange(base, b"edges_flow:car", tbl)
    passed &= check("edges_flow: complete:true summary", bool(meta and meta.get("complete")),
        f"meta={meta}")
    return passed


def gate_edges_batch(base):
    """#512: edges_batch per-edge duration sums must match /route (plus the
    documented full first/last-edge emission — bounded by 2 edges' worth)."""
    print("== edges_batch vs /route (ticket fixtures) ==")
    pairs = [[f[1], f[2], f[3], f[4]] for f in FIXTURES]
    tb = flight_table(base, "edges_batch", "car", {"pairs": pairs})
    sums = {}
    qi, du = tb.column("query_idx"), tb.column("duration_ms")
    for i in range(tb.num_rows):
        k = qi[i].as_py()
        sums[k] = sums.get(k, 0.0) + du[i].as_py() / 1000.0
    lo, hi = THRESHOLDS["edges_sum_bounds"]
    passed = True
    for idx, f in enumerate(FIXTURES):
        got = sums.get(idx)
        # Invariant, no stored constant: the per-edge sum must agree with the
        # LIVE /route duration for the same pair — >= route (edges are whole,
        # the route clips partials) but within +45% (2 extra rural edge
        # halves); the #502 detour fingerprint was 2-3.5x.
        exp, _ = route(base, f[1], f[2], f[3], f[4])
        ok = got is not None and exp * lo <= got <= exp * hi
        passed &= check(f"{f[0]} edges", ok, f"sum {got:.0f}s (route {exp:.0f}s)" if got else "no rows")
    return passed


def gate_route_batch_geometry(base):
    """#493: foot/bike `route_batch` emitted `geometry_wkb` ~2× the reported
    distance (polyline doubled/zigzag) while car was fine — a Flight-only
    regression the REST `/route` coherence gate would miss. Assert the WKB
    LineString length ≈ distance_m within tol for foot and bike."""
    print("== route_batch foot/bike geometry_wkb ≈ distance (#493) ==")
    pairs = [[f[1], f[2], f[3], f[4]] for f in FIXTURES]
    tol = THRESHOLDS["wkb_len_tol"]
    passed = True
    for mode in ("foot", "bike"):
        tb = flight_table(base, "route_batch", mode, {"pairs": pairs})
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
            glen = wkb_linestring_len_m(bytes(w[i]))
            if glen is None:
                continue
            worst = max(worst, abs(glen / d[i] - 1.0))
            if abs(glen - d[i]) > d[i] * tol:
                bad += 1
        passed &= check(f"{mode}: wkb length ≈ distance_m", bad == 0,
            f"{bad} rows off >{tol * 100:.0f}% (worst {worst * 100:.1f}%)")
    return passed


def short_shared_snap_pairs(seed, n_per_centre=14):
    """Short pairs (20 m … 1.2 km) around the fixture centres — the shape that
    makes two snapped endpoints share candidate edges. Deterministic."""
    rng = random.Random(seed)
    centres = [(p[1], p[2]) for p in ISO_POINTS]
    pairs = []
    for clon, clat in centres:
        for _ in range(n_per_centre):
            lon = clon + rng.uniform(-0.03, 0.03)
            lat = clat + rng.uniform(-0.02, 0.02)
            d_m = rng.uniform(20, 1200)
            a = rng.uniform(0, 2 * math.pi)
            pairs.append([
                round(lon, 6), round(lat, 6),
                round(lon + d_m * math.cos(a) / (111320.0 * math.cos(math.radians(lat))), 6),
                round(lat + d_m * math.sin(a) / 110540.0, 6)])
    return pairs


def gate_route_batch_agrees_with_route(base):
    """#605: the SAME pair must get the SAME route from `/route` and from the
    Flight `route_batch` batch surface — same duration, same distance.

    They drifted because the phantom tier was hand-copied: `/route` carried
    the same-edge direct-move recovery and the batch pair driver did not, so a
    pair whose two snaps land on ONE edge came back from the batch as a loop
    around the block — measured at 52 of 1 080 short pairs across the three
    modes, up to several minutes apart. Asserted, not measured: zero
    disagreement, at metre / half-second resolution.

    The sample is deliberately SHORT, and the gate FAILS if it stops
    containing shared-snap pairs — a sample that no longer exercises the case
    would pass this check for the wrong reason."""
    print("== route_batch == /route, same pair, same answer (#605) ==")
    pairs = short_shared_snap_pairs(605)
    passed = True
    for mode in ("car", "foot", "bike"):
        tb = flight_table(base, "route_batch", mode, {"pairs": pairs})
        names = tb.column_names
        dc = "distance_m" if "distance_m" in names else "distance_meters"
        du = "duration_s" if "duration_s" in names else "duration_seconds"
        pi = tb.column("pair_idx").to_pylist()
        bd = tb.column(du).to_pylist()
        bm = tb.column(dc).to_pylist()
        bad, shared, compared, errors = [], 0, 0, 0
        for i in range(tb.num_rows):
            p = pairs[pi[i]]
            try:
                r = route_json(base, p[0], p[1], p[2], p[3], mode, debug="true")
            except Exception as e:  # noqa: BLE001
                errors += 0 if is_no_route(e) else 1
                continue
            dbg = r.get("debug") or {}
            if dbg.get("src_snapped", {}).get("ebg_node_id") == \
               dbg.get("dst_snapped", {}).get("ebg_node_id"):
                shared += 1
            compared += 1
            if bd[i] is None or bm[i] is None:
                continue
            if abs(bd[i] - r["duration_s"]) > 0.5 or abs(bm[i] - r["distance_m"]) > 1.0:
                bad.append((pi[i], r["duration_s"], r["distance_m"], bd[i], bm[i]))
        passed &= check(f"{mode}: same duration and distance", not bad,
            f"{len(bad)}/{compared} pairs disagree" + (
                f" (worst: /route {bad[0][1]:.0f}s {bad[0][2]:.0f}m vs "
                f"batch {bad[0][3]:.0f}s {bad[0][4]:.0f}m)" if bad else ""))
        passed &= check(f"{mode}: sample still hits the shared-snap case",
            shared > 0, f"{shared}/{compared} pairs snap both ends to one edge")
        passed &= check_errors(f"{mode}", errors)
    return passed


def gate_route_batch_max_meters(base):
    """#482/#487: `route_batch` `max_meters` is a server-side prune that DROPS
    over-bound pairs. Invariant, no constant: the bounded result set must equal
    exactly {pairs whose unbounded distance ≤ B}, every returned distance ≤ B,
    and pair_idx preserved (gaps visible)."""
    print("== route_batch max_meters prune == unbounded ≤ B (#482/#487) ==")
    rng = random.Random(482)
    pairs = []
    for _ in range(120):
        lon, lat = rng.uniform(3.6, 5.6), rng.uniform(50.5, 51.1)
        dd, a = rng.uniform(0.01, 0.06), rng.uniform(0, 6.283)
        pairs.append([lon, lat, round(lon + dd * math.cos(a), 6), round(lat + dd * math.sin(a), 6)])

    def run(extra):
        params = {"pairs": pairs}
        params.update(extra)
        tb = flight_table(base, "route_batch", "car", params)
        names = tb.column_names
        dc = "distance_m" if "distance_m" in names else "distance_meters"
        pi = tb.column("pair_idx").to_pylist()
        di = tb.column(dc).to_pylist()
        return {pi[i]: di[i] for i in range(tb.num_rows) if di[i] is not None}

    unb = run({})
    # Integral bound: the engine rounds `max_meters` to whole metres, so a
    # fractional B would make "distance <= B" here and "distance <= round(B)"
    # there disagree about the one pair whose distance IS the median (measured
    # 2026-09-04: B = 5276.355 m, pair 38 at exactly 5276.355 m, correctly
    # dropped by the engine and wrongly expected here). Flooring removes the
    # ambiguity without loosening anything — the set equality below is still
    # exact.
    B = float(math.floor(pct(list(unb.values()), 0.5)))  # median → ~half pruned
    bnd = run({"max_meters": B})
    expected = {k for k, v in unb.items() if v <= B}
    got = set(bnd.keys())
    over = [k for k, v in bnd.items() if v > B]
    passed = check(
        "bound actually prunes", 0 < len(got) < len(unb), f"{len(got)}/{len(unb)} kept (B={B:.0f}m)")
    passed &= check(
        "bounded set == unbounded ≤ B", got == expected, f"got {len(got)} vs expected {len(expected)}")
    passed &= check("every returned pair ≤ B", len(over) == 0, f"{len(over)} over-bound leaked")
    return passed


def gate_catchment_containment(base):
    """#536: hull_shape "road" must be the threshold isochrone — every
    within-percentile client counted covered (the old sector lasso silently
    excluded up to ~7% of them), rings must nest across percentiles, and the
    polygon must be a real road-following contour (far more vertices than the
    18-sector lasso could ever emit)."""
    print("== catchment: road hull covers its percentile + nests (#536) ==")
    import pyarrow as pa

    ok = True
    store = (4.4025, 51.2194)  # Antwerp
    rng = random.Random(536)
    clients = [(store[0] + rng.uniform(-0.12, 0.12), store[1] + rng.uniform(-0.08, 0.08))
               for _ in range(300)]
    n = len(clients)
    tbl = pa.table({"store_id": pa.array(["s"] * n),
                    "store_lon": pa.array([store[0]] * n), "store_lat": pa.array([store[1]] * n),
                    "client_lon": pa.array([c[0] for c in clients]),
                    "client_lat": pa.array([c[1] for c in clients])})
    # Since #596 the Flight `catchment` action takes the SAME parameter set as
    # REST /catchment (percentiles / hull_shape / remove_outliers / radius_km),
    # and since #564 it rejects anything else instead of ignoring it. This
    # fixture deliberately sends NO radius_km: the assertion below is that every
    # within-threshold client is covered, which a pre-filter would confound.
    params = {"percentiles": [50, 80], "hull_shape": "road", "remove_outliers": False}
    try:
        rows, _meta = _exchange(base, f"catchment:car:{json.dumps(params)}".encode(), tbl)
    except Exception as e:
        return check("catchment road hull", False, f"{str(e)[:100]}")
    rows.sort(key=lambda x: x["percentile"])
    min_v = THRESHOLDS["catchment_min_vertices"]
    rings = {}
    for row in rows:
        p = row["percentile"]
        ok &= check(f"p{p:.0f}: all within-threshold clients covered",
            row["clients_covered"] == row["clients_total"] and row["clients_total"] > 0,
            f"{row['clients_covered']}/{row['clients_total']}")
        rr = wkb_polygons(bytes(row["polygon_wkb"]))
        ok &= check(f"p{p:.0f}: polygon parses + road-contour vertex count",
            bool(rr) and len(rr[0][0]) > min_v,
            f"{len(rr[0][0]) if rr else 0} vertices (the retired sector lasso capped at 18 extremes)")
        rings[p] = rr[0][0] if rr else []
    if rings.get(50.0) and rings.get(80.0):
        # Vertex-in-ring is too strict against contour-simplification jitter
        # (same lesson as gate_isochrone_upper_bound): assert directional
        # max-reach monotonicity + area ordering instead — jitter-proof, still
        # catches any gross inversion.
        def reach(ring, bearing_deg):
            b = math.radians(bearing_deg)
            ux, uy = math.sin(b), math.cos(b)
            mx = math.cos(math.radians(store[1])) * 111320.0
            return max((v[0] - store[0]) * mx * ux + (v[1] - store[1]) * 111320.0 * uy for v in ring)

        bad = [br
            for br in range(0, 360, 45)
            if reach(rings[80.0], br) < reach(rings[50.0], br) * 0.98]
        ok &= check("nesting: p80 reach >= p50 reach in all directions", not bad,
            f"violated bearings: {bad}" if bad else "8/8 directions monotone")
        ok &= check("nesting: area(p80) >= area(p50)", ring_area(rings[80.0]) >= ring_area(rings[50.0]),
            f"{ring_area(rings[80.0]):.2e} vs {ring_area(rings[50.0]):.2e}")
    return ok


# #572: ONE probe per documented REST path. The gate iterates
# `GET /api-docs/openapi.json` — proven identical to the router's MOUNTED_PATHS
# by the `openapi_parity` unit test in route/src/server/api.rs — so a path
# added to (or removed from) the server without a probe here is DRIFT and fails
# the gate. Before this, the smoke pinged 7 of the 14 mounted paths and adding
# an endpoint silently added an untested surface.
# Each entry: (method, path-with-query, JSON body or None).
def rest_probes():
    o = (4.3517, 50.8503)  # Brussels
    d = (4.4025, 51.2194)  # Antwerp
    trace = [[4.3517, 50.8503], [4.3537, 50.8513], [4.3557, 50.8523], [4.3577, 50.8533]]
    return {
        "/health": ("GET", "/health", None),
        "/version": ("GET", "/version", None),
        "/regions": ("GET", "/regions", None),
        "/route": ("GET", f"/route?origin_lon={o[0]}&origin_lat={o[1]}"
                          f"&destination_lon={d[0]}&destination_lat={d[1]}&mode=car", None),
        "/nearest": ("GET", f"/nearest?lon={o[0]}&lat={o[1]}&mode=car", None),
        "/isochrone": ("GET", f"/isochrone?lon={o[0]}&lat={o[1]}&time_s=300&mode=car", None),
        "/height": ("GET", f"/height?coordinates={o[0]},{o[1]}|{d[0]},{d[1]}", None),
        "/transit": ("GET", f"/transit?origin_lon={o[0]}&origin_lat={o[1]}"
                            f"&destination_lon={d[0]}&destination_lat={d[1]}", None),
        "/table": ("POST", "/table", {"origins": [list(o), list(d)],
                                      "destinations": [list(o), list(d)],
                                      "mode": "car", "annotations": "duration,distance"}),
        "/trip": ("POST", "/trip", {"points": [list(o), list(d), [4.35, 50.90]], "mode": "car",
                                    "round_trip": True}),
        "/match": ("POST", "/match", {"points": trace, "mode": "car", "geometry": "polyline6"}),
        "/catchment": ("POST", "/catchment", {
            "mode": "car", "hull_shape": "road", "percentiles": [50], "remove_outliers": False,
            "stores": [{"id": "s1", "lon": o[0], "lat": o[1]}],
            "clients": [{"lon": 4.36, "lat": 50.86}, {"lon": 4.34, "lat": 50.84},
                        {"lon": 4.40, "lat": 50.88}]}),
        "/isochrone/bulk": ("POST", "/isochrone/bulk",
                            {"origins": [list(o), list(d)], "time_s": 300, "mode": "car"}),
        "/transit/bulk": ("POST", "/transit/bulk", {"queries": [
            {"origin_lon": o[0], "origin_lat": o[1],
             "destination_lon": d[0], "destination_lat": d[1]}]}),
    }


# Paths that take no request input at all: nothing to refuse, so they have
# no invalid probe. Mirrors `INPUTLESS_PATHS` in route/src/server/api.rs.
REST_INPUTLESS_PATHS = ("/health", "/version", "/regions")


def rest_invalid_probes():
    """One deliberately invalid request per input-taking path (#576): the
    same shape as `rest_probes()`, with longitude 999 where a coordinate
    belongs. Every one of them must be refused with a 4xx carrying the
    documented `error` field — that is the shape check `/trip` and `/match`
    were missing when they answered `{code, message}` instead."""
    bad = (999.0, 50.8503)
    d = (4.4025, 51.2194)
    return {
        "/route": ("GET", f"/route?origin_lon={bad[0]}&origin_lat={bad[1]}"
                          f"&destination_lon={d[0]}&destination_lat={d[1]}&mode=car", None),
        "/nearest": ("GET", f"/nearest?lon={bad[0]}&lat={bad[1]}&mode=car", None),
        "/isochrone": ("GET", f"/isochrone?lon={bad[0]}&lat={bad[1]}&time_s=300&mode=car", None),
        "/height": ("GET", f"/height?coordinates={bad[0]},{bad[1]}", None),
        "/transit": ("GET", f"/transit?origin_lon={bad[0]}&origin_lat={bad[1]}"
                            f"&destination_lon={d[0]}&destination_lat={d[1]}", None),
        "/table": ("POST", "/table", {"origins": [list(bad)], "destinations": [list(d)],
                                      "mode": "car", "annotations": "duration"}),
        "/trip": ("POST", "/trip", {"points": [list(bad), list(d)], "mode": "car"}),
        "/match": ("POST", "/match", {"points": [list(bad), list(d)], "mode": "car"}),
        "/catchment": ("POST", "/catchment", {
            "mode": "car", "hull_shape": "road", "percentiles": [50], "remove_outliers": False,
            "stores": [{"id": "s1", "lon": bad[0], "lat": bad[1]}],
            "clients": [{"lon": d[0], "lat": d[1]}]}),
        "/isochrone/bulk": ("POST", "/isochrone/bulk",
                            {"origins": [list(bad)], "time_s": 300, "mode": "car"}),
        "/transit/bulk": ("POST", "/transit/bulk", {"queries": [
            {"origin_lon": bad[0], "origin_lat": bad[1],
             "destination_lon": d[0], "destination_lat": d[1]}]}),
    }


# status -> why it is a SKIP rather than a FAIL. ONLY documented-optional
# surfaces belong here; anything else must answer 2xx.
REST_PROBE_SKIPS = {
    "/height": {404: "not mounted — <data>/srtm/ absent; lean containers 404 by design"},
    "/transit": {503: "transit subsystem not loaded (no transit/ directory)",
                 404: "no journey for the probe pair — a valid documented answer"},
    "/transit/bulk": {503: "transit subsystem not loaded (no transit/ directory)"},
}


def gate_all_endpoints_smoke(base):
    """COVERAGE: ping EVERY documented REST endpoint and EVERY Flight action so
    a change that breaks one surface entirely is caught even if you were only
    touching another. The REST list is not written here — it is READ from the
    server's own OpenAPI document (#572), so the gate cannot drift behind the
    router. Optional surfaces (/height without SRTM, transit without a feed)
    are SKIPPED with their status and reason, never silently passed."""
    print("== all-endpoints smoke: every OpenAPI-documented REST path + every Flight action ==")
    passed = True
    o = (4.3517, 50.8503)
    d = (4.4025, 51.2194)

    # ---- REST, driven by the server's own OpenAPI document ----
    try:
        doc = http_json(f"{base}/api-docs/openapi.json", timeout=60)
        documented = sorted((doc.get("paths") or {}).keys())
    except Exception as ex:
        return check("openapi document readable", False, f"{type(ex).__name__}: {ex}")
    probes = rest_probes()
    undocumented = [p for p in probes if p not in documented]
    unprobed = [p for p in documented if p not in probes]
    passed &= check("openapi paths == probe table (drift alarm)",
        not undocumented and not unprobed,
        f"{len(documented)} documented paths"
        + (f"; DRIFT — documented but NOT probed: {unprobed}" if unprobed else "")
        + (f"; DRIFT — probed but no longer documented: {undocumented}" if undocumented else ""))
    for path in documented:
        spec = probes.get(path)
        if spec is None:
            continue  # already reported as drift above
        method, target, body = spec
        try:
            status, ctype, payload = http_status(f"{base}{target}", method=method, body=body,
                                                 timeout=120)
        except Exception as ex:
            passed &= check(f"REST {method} {path}", False, f"{type(ex).__name__}: {ex}")
            continue
        skips = REST_PROBE_SKIPS.get(path, {})
        if status in skips:
            print(f"  [SKIP] REST {method} {path}: {status} — {skips[status]}")
            continue
        ok = 200 <= status < 300 and len(payload) > 0
        if ok and "json" in ctype.lower():
            try:
                json.loads(payload)
            except Exception as ex:
                ok = False
                ctype = f"{ctype} (undecodable: {ex})"
        detail = f"{status} {ctype.split(';')[0] or '?'} {len(payload)}B"
        if not ok:
            detail += f" body={payload[:160]!r}"
        passed &= check(f"REST {method} {path}", ok, detail)

    # ---- One INVALID request per input-taking path (#576) ----
    # The refusal shape is part of the API: every endpoint answers 4xx with
    # the documented `error` field. `/trip` and `/match` served `{code,
    # message}` instead until #576, and nothing here noticed.
    invalid = rest_invalid_probes()
    covered = sorted(set(documented) - set(REST_INPUTLESS_PATHS))
    missing_invalid = [p for p in covered if p not in invalid]
    passed &= check("every input-taking path has an invalid probe",
        not missing_invalid, f"{len(invalid)} probes"
        + (f"; MISSING: {missing_invalid}" if missing_invalid else ""))
    for path in covered:
        spec = invalid.get(path)
        if spec is None:
            continue  # already reported above
        method, target, body = spec
        try:
            status, ctype, payload = http_status(f"{base}{target}", method=method, body=body,
                                                 timeout=60)
        except Exception as ex:
            passed &= check(f"REST {method} {path} (invalid)", False, f"{type(ex).__name__}: {ex}")
            continue
        skips = REST_PROBE_SKIPS.get(path, {})
        if status in skips:
            print(f"  [SKIP] REST {method} {path} (invalid): {status} — {skips[status]}")
            continue
        detail = f"{status}"
        ok = 400 <= status < 500
        if not ok:
            detail += f" not a 4xx; body={payload[:160]!r}"
        else:
            try:
                doc_body = json.loads(payload)
            except Exception as ex:
                ok, detail = False, f"{status} undecodable JSON: {ex}"
            else:
                ok = isinstance(doc_body, dict) and isinstance(doc_body.get("error"), str) \
                    and bool(doc_body["error"])
                keys = sorted(doc_body) if isinstance(doc_body, dict) else type(doc_body).__name__
                detail = f"{status} keys={keys}"
                if not ok:
                    detail += " — no documented `error` field"
        passed &= check(f"REST {method} {path} (invalid) carries `error`", ok, detail)

    # ---- Flight ----
    if not flight_enabled():
        print("  [SKIP] Flight actions: --no-flight")
        return passed
    import pyarrow as pa

    pairs = [[o[0], o[1], d[0], d[1]]]

    def do_get_ok(action, params, mode="car"):
        try:
            tb = flight_table(base, action, mode, params)
            return check(f"Flight {action}", tb.num_rows >= 0, f"{tb.num_rows} rows")
        except Exception as e:
            return check(f"Flight {action}", False, f"{str(e)[:80]}")

    passed &= do_get_ok("matrix", {"origins": [list(o)], "destinations": [list(d)]})
    passed &= do_get_ok("route_batch", {"pairs": pairs})
    passed &= do_get_ok("edges_batch", {"pairs": pairs})
    passed &= do_get_ok("isochrone", {"lon": o[0], "lat": o[1], "intervals": [300]})
    # transit_bulk — optional (needs transit subsystem)
    try:
        flight_table(base, "transit_bulk", "transit", {"queries": [
            {"origin_lon": o[0], "origin_lat": o[1], "destination_lon": d[0], "destination_lat": d[1]}]})
        passed &= check("Flight transit_bulk", True, "ok")
    except Exception as e:
        msg = str(e)
        if "not loaded" in msg or "FailedPrecondition" in msg or "transit" in msg.lower():
            print("  [SKIP] Flight transit_bulk: transit subsystem not loaded")
        else:
            passed &= check("Flight transit_bulk", False, f"{msg[:80]}")
    # do_exchange: catchment + edges_flow
    # percentiles / hull_shape / remove_outliers only — see gate_catchment_containment.
    catchment_params = {"percentiles": [50], "hull_shape": "isochrone",
                        "remove_outliers": False}
    for label, command, tbl in (("catchment", f"catchment:car:{json.dumps(catchment_params)}".encode(),
         pa.table({"store_id": pa.array(["s1"]), "store_lon": pa.array([o[0]]),
                   "store_lat": pa.array([o[1]]), "client_lon": pa.array([d[0]]),
                   "client_lat": pa.array([d[1]])})),
        ("edges_flow", b"edges_flow:car",
         pa.table({"src_lon": pa.array([o[0]]), "src_lat": pa.array([o[1]]),
                   "dst_lon": pa.array([d[0]]), "dst_lat": pa.array([d[1]])})) ):
        try:
            _exchange(base, command, tbl)
            passed &= check(f"Flight {label}", True, "ok")
        except Exception as e:
            passed &= check(f"Flight {label}", False, f"{str(e)[:80]}")
    return passed


# ---------------------------------------------------------------------------
# Registry + entrypoint
# ---------------------------------------------------------------------------
# Attribute defaults for a gate registry built WITHOUT argparse (gate_names,
# the offline unit tests). Every path-valued option is None = "resolve under
# $BUTTERFLY_REFS_DIR when the gate runs" (#589).
GATE_ARGS_DEFAULTS = {"base": None, "quick": False, "trips": None, "distance_trips": None,
    "refs_prefix": None, "no_flight": False, "flight_base": None}


def gate_names(args=None):
    """Every registered gate name. Builds the registry with placeholder args —
    the thunks are never called, so no reference set and no server is touched.
    `gate_ticket_invariants` asserts its ticket map against this."""
    return [name for name, _flight, _thunk in
            build_gates(args or argparse.Namespace(**GATE_ARGS_DEFAULTS))]


def build_gates(args):
    """(name, needs_flight, thunk). ONE list — `--list-gates` prints it, main
    runs it, CI smoke-tests it."""
    b = getattr(args, "base", None)
    gates = [
        ("fixtures", False, lambda: gate_fixtures(b)),
        ("symmetry", False, lambda: gate_symmetry(b)),
        ("route_table_agreement", False, lambda: gate_route_table_agreement(b)),
        ("isochrone_topology", False, lambda: gate_isochrone_topology(b)),
        ("isochrone_reach_truth", False, lambda: gate_isochrone_reach_truth(b)),
        ("isochrone_upper_bound", False, lambda: gate_isochrone_upper_bound(b)),
        ("bands", False, lambda: gate_bands(b, refs_path(REFS_PREFIX, args.refs_prefix))),
        ("ticket_invariants", False, lambda: gate_ticket_invariants(b)),
        ("lopsided_matrix", False, lambda: gate_lopsided(b)),
        ("radius_prune", False, lambda: gate_radius_prune(b)),
        ("radius_exactness", False, lambda: gate_radius_exactness(b)),
        ("recustomized_distance", False, lambda: gate_recustomized_distance(b)),
        ("mode_coherence", False, lambda: gate_mode_coherence(b)),
        ("one_way_routable", False, lambda: gate_one_way_routable(b)),
        ("graph_holes", False, lambda: gate_graph_holes(b)),
        ("motorway_speed_floor", False, lambda: gate_motorway_speed_floor(b)),
        ("exclude_motorway", False, lambda: gate_exclude_motorway(b)),
        ("edges_batch", True, lambda: gate_edges_batch(b)),
        ("matrix_sparse", True, lambda: gate_matrix_sparse(b)),
        ("matrix_sparse_streaming", True, lambda: gate_matrix_sparse_streaming(b)),
        ("flight_completeness", True, lambda: gate_flight_completeness(b)),
        ("matrix_distance_consistency", True, lambda: gate_matrix_distance_consistency(b)),
        ("bounded_matrix_exactness", True, lambda: gate_bounded_matrix_exactness(b)),
        ("route_batch_geometry", True, lambda: gate_route_batch_geometry(b)),
        ("route_batch_agrees_with_route", True, lambda: gate_route_batch_agrees_with_route(b)),
        ("route_batch_max_meters", True, lambda: gate_route_batch_max_meters(b)),
        ("catchment_containment", True, lambda: gate_catchment_containment(b)),
        ("all_endpoints_smoke", False, lambda: gate_all_endpoints_smoke(b)),
    ]
    if not args.quick:
        gates.append(("ground_truth_duration", False,
                      lambda: gate_ground_truth(b, refs_path(DEFAULT_TRIPS, args.trips), "duration")))
        gates.append(("ground_truth_distance", False,
                      lambda: gate_ground_truth(b, refs_path(LEGACY_TRIPS_DISTANCE, args.distance_trips),
                                                "distance")))
        # #545: route CHOICE on the TIME-STAMPED set. Shares ref_trip_routes
        # with ground_truth_duration, so it costs no extra server work.
        gates.append(("route_choice", False,
                      lambda: gate_route_choice(b, refs_path(DEFAULT_TRIPS, args.trips))))
    return gates


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", help="e.g. http://localhost:3001")
    # #589: NO argparse default may touch $BUTTERFLY_REFS_DIR — an unset
    # variable used to crash `--list-gates` and `--help` in os.path.join(None,
    # ...). Unset here means "resolve under $BUTTERFLY_REFS_DIR when the gate
    # runs" (refs_path); a value given wins as typed.
    ap.add_argument("--trips", default=None,
        help=f"duration reference set (default: {DEFAULT_TRIPS} under $BUTTERFLY_REFS_DIR)")
    ap.add_argument("--distance-trips", default=None,
        help=f"route-length reference set (default: {LEGACY_TRIPS_DISTANCE} under $BUTTERFLY_REFS_DIR)")
    ap.add_argument("--refs-prefix", default=None,
        help="time-stamped reference sets <prefix>_{typical,best,worst}.csv "
             f"(default prefix: {REFS_PREFIX!r} under $BUTTERFLY_REFS_DIR)")
    ap.add_argument("--quick", action="store_true", help="skip the 1000-trip ground truth")
    ap.add_argument("--no-flight", action="store_true",
        help="skip every Arrow Flight gate (the ONLY way to run without pyarrow/pandas)")
    ap.add_argument("--flight-base", help="override the Flight URI (default: REST host, port+1)")
    ap.add_argument("--list-gates", action="store_true", help="print the gate names and exit 0 (CI smoke)")
    args = ap.parse_args()

    if args.list_gates:
        for name, needs_flight, _ in build_gates(args):
            print(f"{name}{' [flight]' if needs_flight else ''}")
        sys.exit(0)
    if not args.base:
        ap.error("--base is required")

    CONFIG["flight"] = not args.no_flight
    CONFIG["flight_base"] = args.flight_base
    base = args.base.rstrip("/")
    args.base = base

    # PREFLIGHT (#550): eleven gates used to return PASS on ImportError, so a
    # runner without pyarrow green-lit a deploy with every Flight invariant
    # unchecked. Missing deps are now a FAIL unless --no-flight says so.
    if CONFIG["flight"]:
        missing = require_pyarrow()
        if missing:
            print("post-deploy gate PREFLIGHT FAILED — Flight gates cannot run:")
            for m in missing:
                print(f"  missing: {m}")
            print("  install pyarrow + pandas, or pass --no-flight to skip those gates knowingly.")
            print("\nGATE: FAIL")
            sys.exit(1)

    print(f"post-deploy gate against {base}" + ("" if CONFIG["flight"] else " (--no-flight)"))
    _apply_windows_config()  # staged tolerances override the defaults, then re-derive
    print_thresholds()  # #589 guard: read every PASS line against the bound that produced it
    ok = True
    t0 = time.time()
    for name, needs_flight, fn in build_gates(args):
        if needs_flight and not flight_enabled():
            print(f"== {name} ==")
            print("  [SKIP] --no-flight")
            continue
        started = time.time()
        try:
            ok &= bool(fn())
        except RefsUnavailable as e:
            # #589: a refs-dependent gate cannot run — FAIL it by name (never a
            # silent skip, never a process-wide SystemExit) and keep going.
            ok &= check(name, False, str(e))
        except Exception as e:
            ok &= check(name, False, f"raised {type(e).__name__}: {str(e)[:200]}")
        print(f"  ({name}: {time.time() - started:.1f}s)")
    print(f"\nGATE: {'PASS' if ok else 'FAIL'} ({time.time() - t0:.1f}s)")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
