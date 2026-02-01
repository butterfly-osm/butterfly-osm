#!/usr/bin/env python3
"""
Valhalla vs Butterfly Isochrone Benchmark

Compares isochrone generation performance between:
- Valhalla (port 8002)
- Butterfly (computed separately via CLI)
"""

import subprocess
import time
import json
import statistics
from typing import List, Tuple

# Test locations (Belgium)
LOCATIONS = [
    ("Brussels Center", 4.3517, 50.8503),
    ("Antwerp Center", 4.4025, 51.2194),
    ("Ghent Center", 3.7174, 51.0543),
    ("Liège Center", 5.5796, 50.6326),
    ("Charleroi", 4.4447, 50.4108),
    ("E40/E19 Junction", 4.4167, 50.8833),
    ("Ring Brussels S", 4.3500, 50.7833),
    ("Near Netherlands", 4.2833, 51.3500),
    ("Near France", 4.0833, 49.5667),
]

# Thresholds in minutes
THRESHOLDS = [5, 10, 30, 60]

VALHALLA_URL = "http://localhost:8002/isochrone"

def benchmark_valhalla(lon: float, lat: float, time_minutes: int, n_runs: int = 3) -> Tuple[float, int]:
    """
    Run Valhalla isochrone and return (avg_ms, n_vertices)
    """
    import urllib.parse
    params = json.dumps({
        "locations": [{"lat": lat, "lon": lon}],
        "costing": "auto",
        "contours": [{"time": time_minutes}],
        "polygons": True
    })
    url = f"{VALHALLA_URL}?json={urllib.parse.quote(params)}"

    times = []
    n_vertices = 0

    for _ in range(n_runs):
        start = time.perf_counter()
        try:
            result = subprocess.run(
                ["curl", "-s", url],
                capture_output=True,
                text=True,
                timeout=60
            )
            elapsed_ms = (time.perf_counter() - start) * 1000
            times.append(elapsed_ms)

            if result.returncode == 0:
                data = json.loads(result.stdout)
                # Count vertices in the polygon
                if "features" in data and len(data["features"]) > 0:
                    geom = data["features"][0].get("geometry", {})
                    coords = geom.get("coordinates", [[]])
                    if geom.get("type") == "Polygon":
                        n_vertices = len(coords[0]) if coords else 0
                    elif geom.get("type") == "MultiPolygon":
                        n_vertices = sum(len(ring[0]) for ring in coords if ring)
        except Exception as e:
            times.append(60000)  # timeout
            print(f"Error: {e}")

    return statistics.mean(times) if times else 60000, n_vertices

def main():
    print("=" * 80)
    print("  VALHALLA ISOCHRONE BENCHMARK (Belgium)")
    print("=" * 80)
    print()

    # Run benchmarks
    results = {}

    print(f"{'Location':<20} | {'5 min':>10} | {'10 min':>10} | {'30 min':>10} | {'60 min':>10} | {'Verts':>6}")
    print("-" * 80)

    for name, lon, lat in LOCATIONS:
        row = [name]
        last_verts = 0

        for t in THRESHOLDS:
            avg_ms, verts = benchmark_valhalla(lon, lat, t)
            row.append(f"{avg_ms:>7.0f}ms")
            last_verts = verts

            key = (name, t)
            results[key] = avg_ms

        row.append(f"{last_verts:>6}")
        print(f"{row[0]:<20} | {row[1]:>10} | {row[2]:>10} | {row[3]:>10} | {row[4]:>10} | {row[5]:>6}")

    print("-" * 80)
    print()

    # Summary statistics
    print("VALHALLA Summary by threshold:")
    for t in THRESHOLDS:
        times = [results[(name, t)] for name, _, _ in LOCATIONS if (name, t) in results]
        if times:
            print(f"  {t:2d} min: avg={statistics.mean(times):>6.0f}ms, "
                  f"min={min(times):>5.0f}ms, max={max(times):>6.0f}ms")

    print()
    print("=" * 80)
    print("  BUTTERFLY Results (from pathological-origins benchmark)")
    print("=" * 80)
    print()

    # Butterfly results from our benchmark
    butterfly_car = {
        ("Brussels Center", 5): 7, ("Brussels Center", 10): 6, ("Brussels Center", 30): 65, ("Brussels Center", 60): 333,
        ("Antwerp Center", 5): 3, ("Antwerp Center", 10): 8, ("Antwerp Center", 30): 54, ("Antwerp Center", 60): 204,
        ("Ghent Center", 5): 4, ("Ghent Center", 10): 8, ("Ghent Center", 30): 115, ("Ghent Center", 60): 419,
        ("Liège Center", 5): 3, ("Liège Center", 10): 17, ("Liège Center", 30): 143, ("Liège Center", 60): 473,
        ("Charleroi", 5): 5, ("Charleroi", 10): 10, ("Charleroi", 30): 68, ("Charleroi", 60): 212,
        ("E40/E19 Junction", 5): 3, ("E40/E19 Junction", 10): 6, ("E40/E19 Junction", 30): 46, ("E40/E19 Junction", 60): 190,
        ("Ring Brussels S", 5): 3, ("Ring Brussels S", 10): 5, ("Ring Brussels S", 30): 78, ("Ring Brussels S", 60): 332,
        ("Near Netherlands", 5): 3, ("Near Netherlands", 10): 5, ("Near Netherlands", 30): 54, ("Near Netherlands", 60): 209,
        ("Near France", 5): 3, ("Near France", 10): 7, ("Near France", 30): 83, ("Near France", 60): 343,
    }

    print("BUTTERFLY Summary (car mode):")
    for t in THRESHOLDS:
        times = [butterfly_car[(name, t)] for name, _, _ in LOCATIONS if (name, t) in butterfly_car]
        if times:
            print(f"  {t:2d} min: avg={statistics.mean(times):>6.0f}ms, "
                  f"min={min(times):>5.0f}ms, max={max(times):>6.0f}ms")

    print()
    print("=" * 80)
    print("  HEAD-TO-HEAD COMPARISON")
    print("=" * 80)
    print()
    print(f"{'Threshold':>10} | {'Valhalla avg':>12} | {'Butterfly avg':>13} | {'Winner':>10} | {'Ratio':>8}")
    print("-" * 60)

    for t in THRESHOLDS:
        v_times = [results[(name, t)] for name, _, _ in LOCATIONS if (name, t) in results]
        b_times = [butterfly_car[(name, t)] for name, _, _ in LOCATIONS if (name, t) in butterfly_car]
        v_avg = statistics.mean(v_times) if v_times else 0
        b_avg = statistics.mean(b_times) if b_times else 0

        if b_avg < v_avg:
            winner = "Butterfly"
            ratio = f"{v_avg/b_avg:.1f}x"
        else:
            winner = "Valhalla"
            ratio = f"{b_avg/v_avg:.1f}x"

        print(f"{t:>7} min | {v_avg:>10.0f}ms | {b_avg:>11.0f}ms | {winner:>10} | {ratio:>8}")

    print("-" * 60)
    print()

if __name__ == "__main__":
    main()
