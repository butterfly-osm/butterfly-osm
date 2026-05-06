#!/usr/bin/env python3
import json
import sys
import urllib.request

SRC_LON, SRC_LAT = 4.3517, 50.8503  # Brussels
DST_LON, DST_LAT = 4.4017, 51.2194  # Antwerp

cases = [
    ("Freeflow (mode=car)", f"mode=car"),
    ("Freeflow via traffic=freeflow", f"mode=car&traffic=freeflow"),
    ("Off-peak", f"mode=car&traffic=offpeak"),
    ("Rush hour", f"mode=car&traffic=rush_hour"),
    ("Direct mode car_rush_hour", f"mode=car_rush_hour"),
    ("Direct mode car_offpeak", f"mode=car_offpeak"),
    ("Direct mode car_freeflow", f"mode=car_freeflow"),
]

results = {}
for label, suffix in cases:
    url = (
        f"http://localhost:18800/route?"
        f"src_lon={SRC_LON}&src_lat={SRC_LAT}&"
        f"dst_lon={DST_LON}&dst_lat={DST_LAT}&{suffix}"
    )
    try:
        with urllib.request.urlopen(url, timeout=30) as resp:
            data = json.loads(resp.read().decode())
        # Response shape: {duration_s, distance_m, geometry?, steps?}
        if "routes" in data:
            route = data["routes"][0]
        else:
            route = data
        dur = route["duration_s"]
        dist = route["distance_m"]
        n_steps = len(route.get("steps") or [])
        n_geom_pts = 0
        geom = route.get("geometry")
        if isinstance(geom, dict):
            if "polyline" in geom and isinstance(geom["polyline"], str):
                n_geom_pts = len(geom["polyline"])  # crude size
            elif "coordinates" in geom:
                n_geom_pts = len(geom["coordinates"])
        print(
            f"  {label:34s} duration_s={dur:7.1f} distance_m={dist:7.0f} steps={n_steps} polyline_chars={n_geom_pts}"
        )
        results[label] = dur
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        print(f"  {label:34s} HTTP {e.code}: {body[:200]}")
    except Exception as e:
        print(f"  {label:34s} error: {e}")

# Sanity: rush_hour > offpeak > freeflow
ff = results.get("Freeflow (mode=car)", 0.0)
op = results.get("Off-peak", 0.0)
rh = results.get("Rush hour", 0.0)
print()
print(f"Freeflow:  {ff:.1f}s")
print(f"Offpeak:   {op:.1f}s ({(op/ff - 1) * 100:+.1f}%)" if ff > 0 else f"Offpeak: {op:.1f}s")
print(f"Rush hour: {rh:.1f}s ({(rh/ff - 1) * 100:+.1f}%)" if ff > 0 else f"Rush hour: {rh:.1f}s")
if ff > 0:
    pct = (rh / ff - 1) * 100
    if pct < 5:
        print("WARNING: rush_hour duration not noticeably longer than freeflow!")
    else:
        print(f"OK — rush_hour is {pct:.1f}% longer than freeflow")

# Test that an unknown variant rejects
url = (
    f"http://localhost:18800/route?"
    f"src_lon={SRC_LON}&src_lat={SRC_LAT}&"
    f"dst_lon={DST_LON}&dst_lat={DST_LAT}&mode=car&traffic=does_not_exist"
)
try:
    with urllib.request.urlopen(url, timeout=10) as resp:
        print("UNKNOWN variant: unexpected 200")
except urllib.error.HTTPError as e:
    body = e.read().decode()
    if e.code == 400 and "Unknown traffic variant" in body:
        print(f"OK — unknown traffic variant correctly rejected (400): {body[:120]}")
    else:
        print(f"UNKNOWN variant: HTTP {e.code}: {body[:200]}")

# Save for results dir
with open("/tmp/smoke_results.json", "w") as f:
    json.dump(
        {
            "freeflow_s": ff,
            "offpeak_s": op,
            "rush_hour_s": rh,
            "rush_pct_increase": (rh / ff - 1) * 100 if ff > 0 else None,
            "all_durations": results,
        },
        f,
        indent=2,
    )
