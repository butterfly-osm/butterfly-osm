#!/usr/bin/env python3
"""Route-CHOICE diagnostic against the time-stamped reference trips (#545).

The post-deploy gate judges LEVEL (engine duration vs the observed duration).
This script judges CHOICE: how far the engine's route is from the route the
observations were actually made on, which the gate deliberately does not gate
on `od_typical.csv` (see `gate_ground_truth(..., checks=)`).

It is read-only: it issues `GET /route` and, optionally, OSRM `route/v1` — it
never writes to a server and never changes engine behaviour.

    BUTTERFLY_REFS_DIR=/path/to/reference-trips \
    python3 bench/route_choice.py http://localhost:3903 \
        --label calibrated \
        --compare http://localhost:3904 --compare-label base-weights \
        --osrm http://localhost:5051

`$BUTTERFLY_REFS_DIR` holds the reference CSVs (`route_id, long_1, lat_1,
long_2, lat_2, ref_min, ref_km, n_obs`); they are never committed here.

What it prints:

1. one row per engine/weight set: route length and duration against the
   observed ones, and the share of pairs whose route is >= 1.2x the observed
   length (the #545 statistic);
2. the divergent set's shape - trip length, city proximity, the share of the
   route on fast links, endpoint snap distance and the turn-cost share, so
   the usual alternative explanations can be read off directly;
3. with `--compare`, the per-corridor slow-down one weight set applies
   relative to the other, bucketed by the comparison route's fast-link share
   - i.e. how much steeper one weight set is on local roads than on trunk
   roads, which is what decides between a direct route and a fast detour.
"""
import argparse
import concurrent.futures as cf
import csv
import json
import math
import os
import sys
import urllib.request

# Belgian city centres, only for reporting WHERE divergence concentrates.
CITIES = {
    "Brussels": (4.3517, 50.8503), "Antwerp": (4.4025, 51.2194),
    "Ghent": (3.7174, 51.0543), "Liege": (5.5797, 50.6326),
    "Charleroi": (4.4446, 50.4108), "Bruges": (3.2247, 51.2093),
    "Namur": (4.8720, 50.4674), "Hasselt": (5.3378, 50.9307),
}
DIVERGENT = 1.20  # route length / observed length at or above which we count a pair
FAST_KMH = 90.0   # link speed at or above which a link counts as "fast" (motorway/trunk)
MIN_EDGE_M = 50.0 # shorter edges have integer-second weights: their implied speed is noise


def pct(xs, p):
    xs = sorted(xs)
    if not xs:
        return float("nan")
    k = (len(xs) - 1) * p
    lo, hi = math.floor(k), math.ceil(k)
    return xs[lo] if lo == hi else xs[lo] + (xs[hi] - xs[lo]) * (k - lo)


def haversine_km(lon1, lat1, lon2, lat2):
    p1, p2 = math.radians(lat1), math.radians(lat2)
    a = (math.sin((p2 - p1) / 2) ** 2
         + math.cos(p1) * math.cos(p2) * math.sin(math.radians(lon2 - lon1) / 2) ** 2)
    return 2 * 6371.0 * math.asin(math.sqrt(a))


def http_json(url, timeout=180):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.loads(r.read())


def refs_path(name):
    d = os.environ.get("BUTTERFLY_REFS_DIR")
    if not d or not os.path.isdir(d):
        sys.exit("BUTTERFLY_REFS_DIR is not set to the reference-trips directory")
    return os.path.join(d, name)


def butterfly_route(base, t):
    d = http_json(f"{base}/route?origin_lon={t['long_1']}&origin_lat={t['lat_1']}"
                  f"&destination_lon={t['long_2']}&destination_lat={t['lat_2']}&mode=car"
                  f"&annotations=distance,duration,speed&debug=true&geometries=points")
    ann = d["annotations"]
    long_edges = [(m, s) for m, s in zip(ann["distance"], ann["speed"]) if m >= MIN_EDGE_M]
    tot = sum(m for m, _ in long_edges) or 1.0
    dbg = d.get("debug") or {}
    return {
        "km": d["distance_m"] / 1000.0,
        "dur_s": d["duration_s"],
        "fast_share": sum(m for m, s in long_edges if s >= FAST_KMH) / tot,
        "turn_share": (d["duration_s"] - sum(ann["duration"])) / max(d["duration_s"], 1.0),
        "snap_m": max((dbg.get("src_snapped") or {}).get("snap_distance_m") or 0.0,
                      (dbg.get("dst_snapped") or {}).get("snap_distance_m") or 0.0),
    }


def osrm_route(base, t):
    d = http_json(f"{base}/route/v1/driving/{t['long_1']},{t['lat_1']};"
                  f"{t['long_2']},{t['lat_2']}?overview=false", timeout=60)
    r = d["routes"][0]
    return {"km": r["distance"] / 1000.0, "dur_s": r["duration"],
            "fast_share": None, "turn_share": None, "snap_m": None}


def run(fn, base, trips, workers=8):
    def one(t):
        try:
            return fn(base, t)
        except Exception as e:  # a failed pair is reported, never silently dropped
            return {"err": str(e)}
    with cf.ThreadPoolExecutor(workers) as ex:
        return list(ex.map(one, trips))


def level_row(label, trips, res):
    ok = [(t, r) for t, r in zip(trips, res) if "km" in r]
    dist = [r["km"] / float(t["ref_km"]) for t, r in ok]
    dur = [r["dur_s"] / 60.0 / float(t["ref_min"]) for t, r in ok]
    bad = sum(1 for x in dist if x >= DIVERGENT)
    print(f"  {label:<26s} {len(ok):4d} {pct(dist,.5):8.3f} {pct(dist,.9):8.3f} "
          f"{bad:5d} {bad/max(len(dist),1):8.1%} {pct(dur,.5):9.3f}")
    return {t["route_id"] for t, r in ok if r["km"] / float(t["ref_km"]) >= DIVERGENT}


def describe(trips, res, divergent_ids):
    by_id = {t["route_id"]: (t, r) for t, r in zip(trips, res) if "km" in r}
    div = [by_id[i] for i in divergent_ids if i in by_id]
    rest = [v for k, v in by_id.items() if k not in divergent_ids]
    if not div:
        return
    print(f"\n== the divergent set ({len(div)} pairs) vs the rest ({len(rest)}) ==")
    fields = [
        ("observed length km", lambda t, r: float(t["ref_km"])),
        ("route km / observed km", lambda t, r: r["km"] / float(t["ref_km"])),
        ("route duration / observed", lambda t, r: r["dur_s"] / 60.0 / float(t["ref_min"])),
        ("detour vs crow (engine)", lambda t, r: r["km"] / max(haversine_km(
            float(t["long_1"]), float(t["lat_1"]), float(t["long_2"]), float(t["lat_2"])), .001)),
        ("detour vs crow (observed)", lambda t, r: float(t["ref_km"]) / max(haversine_km(
            float(t["long_1"]), float(t["lat_1"]), float(t["long_2"]), float(t["lat_2"])), .001)),
        (f"length share >= {FAST_KMH:.0f} km/h", lambda t, r: r["fast_share"]),
        ("turn cost / duration", lambda t, r: r["turn_share"]),
        ("endpoint snap m", lambda t, r: r["snap_m"]),
        ("observations per pair", lambda t, r: float(t.get("n_obs") or 0)),
    ]
    print(f"  {'':30s} {'divergent p25/p50/p75':>28s}   {'rest p25/p50/p75':>28s}")
    for name, f in fields:
        a = [f(t, r) for t, r in div if f(t, r) is not None]
        b = [f(t, r) for t, r in rest if f(t, r) is not None]
        if not a or not b:
            continue
        print(f"  {name:30s} {pct(a,.25):8.3f}/{pct(a,.5):8.3f}/{pct(a,.75):8.3f}   "
              f"{pct(b,.25):8.3f}/{pct(b,.5):8.3f}/{pct(b,.75):8.3f}")

    print("\n  divergence rate by trip length (observed km):")
    for lo, hi in ((0, 20), (20, 35), (35, 50), (50, 1e9)):
        g = [(t, r) for t, r in by_id.values() if lo <= float(t["ref_km"]) < hi]
        if len(g) < 5:
            continue
        n = sum(1 for t, _ in g if t["route_id"] in divergent_ids)
        print(f"    {lo:3.0f}-{hi if hi < 1e9 else 999:4.0f} km: n={len(g):3d} "
              f"divergent={n:3d} ({n/len(g):5.1%})")
    print("  divergence rate by city proximity (either endpoint within 15 km):")
    for city, (lo, la) in CITIES.items():
        g = [(t, r) for t, r in by_id.values()
             if min(haversine_km(lo, la, float(t["long_1"]), float(t["lat_1"])),
                    haversine_km(lo, la, float(t["long_2"]), float(t["lat_2"]))) <= 15]
        if len(g) < 5:
            continue
        n = sum(1 for t, _ in g if t["route_id"] in divergent_ids)
        print(f"    {city:10s} n={len(g):3d} divergent={n:3d} ({n/len(g):5.1%})")


def compare_slowdown(trips, res_a, res_b, label_a, label_b):
    """How much slower is weight set A than weight set B, per corridor type?
    Only pairs where both pick a route of the same length are compared, so the
    ratio is a property of the WEIGHTS and not of two different routes."""
    rows = []
    for t, a, b in zip(trips, res_a, res_b):
        if "km" not in a or "km" not in b or b["fast_share"] is None:
            continue
        if abs(a["km"] / max(b["km"], 1e-9) - 1.0) > 0.05:
            continue
        rows.append((b["fast_share"], a["dur_s"] / max(b["dur_s"], 1e-9)))
    if len(rows) < 20:
        print(f"\n== {label_a} vs {label_b}: only {len(rows)} same-length pairs, skipped ==")
        return
    print(f"\n== per-corridor slow-down of '{label_a}' over '{label_b}' "
          f"({len(rows)} same-length pairs) ==")
    print(f"  {label_b} route share >= {FAST_KMH:.0f} km/h      n     p25    p50    p75")
    med = {}
    for lo, hi in ((0, .15), (.15, .40), (.40, .70), (.70, 1.01)):
        g = [v for f, v in rows if lo <= f < hi]
        if len(g) < 5:
            continue
        med[(lo, hi)] = pct(g, .5)
        print(f"    {lo:.2f}-{hi:.2f}                        {len(g):4d}  "
              f"{pct(g,.25):6.3f} {pct(g,.5):6.3f} {pct(g,.75):6.3f}")
    local = min((k for k in med if k[0] < .40), default=None)
    trunk = max((k for k in med if k[0] >= .40), default=None)
    if local and trunk and med[trunk] > 0:
        print(f"  relative penalty on local corridors vs trunk corridors: "
              f"{med[local]/med[trunk]:.3f}")
        print("  (a value well above 1 means the two corridors are no longer costed on the "
              "same scale, which is what moves route CHOICE onto the fast detour)")


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("base", help="butterfly base URL, e.g. http://localhost:3903")
    ap.add_argument("--label", default="engine")
    ap.add_argument("--compare", help="second butterfly URL (e.g. same build, other weights)")
    ap.add_argument("--compare-label", default="compare")
    ap.add_argument("--osrm", help="OSRM base URL, as an independent control")
    ap.add_argument("--trips", default="od_typical.csv",
                    help="reference CSV under $BUTTERFLY_REFS_DIR")
    args = ap.parse_args()

    trips = list(csv.DictReader(open(refs_path(args.trips))))
    print(f"== route choice vs {args.trips} ({len(trips)} pairs) ==")
    print(f"  {'engine / weights':<26s} {'n':>4s} {'km p50':>8s} {'km p90':>8s} "
          f"{'>=1.2x':>5s} {'':>8s} {'dur p50':>9s}")

    res = run(butterfly_route, args.base, trips)
    div = level_row(args.label, trips, res)
    errs = [r["err"] for r in res if "err" in r]
    if errs:
        print(f"  !! {len(errs)} pairs failed, first: {errs[0]}")

    res_cmp = None
    if args.compare:
        res_cmp = run(butterfly_route, args.compare, trips)
        div_cmp = level_row(args.compare_label, trips, res_cmp)
        print(f"  divergent on both: {len(div & div_cmp)}, "
              f"only '{args.label}': {len(div - div_cmp)}, "
              f"only '{args.compare_label}': {len(div_cmp - div)}")
    if args.osrm:
        level_row("OSRM CH (control)", trips, run(osrm_route, args.osrm, trips))

    describe(trips, res, div)
    if res_cmp:
        compare_slowdown(trips, res, res_cmp, args.label, args.compare_label)


if __name__ == "__main__":
    main()
