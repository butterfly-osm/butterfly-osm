#!/usr/bin/env python3
"""
Compare Butterfly vs OSRM matrix quality and speed.
Uses fixed coordinates to ensure apples-to-apples comparison.
"""
import requests
import json
import time
import sys

# Fixed test coordinates in Belgium (known to be on roads)
TEST_COORDS = [
    (4.3517, 50.8503),   # Brussels center
    (4.4025, 50.8449),   # Brussels east
    (3.7250, 51.0543),   # Ghent
    (4.4028, 51.2194),   # Antwerp
    (5.5796, 50.6326),   # Liège
    (4.4449, 50.4108),   # Charleroi
    (3.2247, 51.2093),   # Bruges
    (4.7008, 50.8798),   # Leuven
    (5.9714, 50.9386),   # Aachen border
    (4.8673, 50.4679),   # Namur
]

OSRM_URL = "http://localhost:5050"
BUTTERFLY_URL = "http://localhost:8080"

def test_osrm_matrix(coords):
    """Query OSRM /table endpoint"""
    coords_str = ";".join(f"{lon},{lat}" for lon, lat in coords)
    url = f"{OSRM_URL}/table/v1/driving/{coords_str}"

    start = time.perf_counter()
    resp = requests.get(url, timeout=30)
    elapsed_ms = (time.perf_counter() - start) * 1000

    if resp.status_code != 200:
        print(f"OSRM error: {resp.status_code}")
        return None, elapsed_ms

    data = resp.json()
    if data.get("code") != "Ok":
        print(f"OSRM error: {data}")
        return None, elapsed_ms

    # Durations are in seconds
    durations = data.get("durations", [])
    return durations, elapsed_ms

def test_butterfly_matrix(coords):
    """Query Butterfly /matrix endpoint"""
    # Construct query params
    src_lons = ",".join(str(lon) for lon, lat in coords)
    src_lats = ",".join(str(lat) for lon, lat in coords)
    dst_lons = src_lons
    dst_lats = src_lats

    url = f"{BUTTERFLY_URL}/matrix?src_lons={src_lons}&src_lats={src_lats}&dst_lons={dst_lons}&dst_lats={dst_lats}&mode=car"

    start = time.perf_counter()
    resp = requests.get(url, timeout=30)
    elapsed_ms = (time.perf_counter() - start) * 1000

    if resp.status_code != 200:
        print(f"Butterfly error: {resp.status_code} - {resp.text[:200]}")
        return None, elapsed_ms

    data = resp.json()
    # Butterfly returns durations_s as flat array
    durations_flat = data.get("durations_s", [])
    n = len(coords)
    # Reshape to matrix
    durations = [durations_flat[i*n:(i+1)*n] for i in range(n)]
    return durations, elapsed_ms

def compare_matrices(osrm_mat, butterfly_mat, coords):
    """Compare two duration matrices"""
    n = len(coords)
    max_diff = 0
    max_diff_pct = 0
    diffs = []

    for i in range(n):
        for j in range(n):
            osrm_val = osrm_mat[i][j] if osrm_mat[i][j] else float('inf')
            bf_val = butterfly_mat[i][j] if butterfly_mat[i][j] else float('inf')

            if osrm_val == float('inf') and bf_val == float('inf'):
                continue
            if osrm_val == float('inf') or bf_val == float('inf'):
                diffs.append((i, j, osrm_val, bf_val, "INF mismatch"))
                continue

            diff = abs(osrm_val - bf_val)
            pct = (diff / osrm_val * 100) if osrm_val > 0 else 0

            if diff > max_diff:
                max_diff = diff
            if pct > max_diff_pct:
                max_diff_pct = pct

            if pct > 10:  # Flag >10% differences
                diffs.append((i, j, osrm_val, bf_val, f"{pct:.1f}%"))

    return max_diff, max_diff_pct, diffs

def main():
    print("=" * 70)
    print("MATRIX QUALITY & SPEED COMPARISON: Butterfly vs OSRM CH")
    print("=" * 70)
    print()

    # Test with different sizes
    for size in [5, 10]:
        coords = TEST_COORDS[:size]
        print(f"Testing {size}x{size} matrix ({size} coordinates)")
        print("-" * 50)

        # OSRM
        osrm_mat, osrm_time = test_osrm_matrix(coords)
        if osrm_mat:
            print(f"  OSRM:      {osrm_time:.1f}ms")
        else:
            print(f"  OSRM:      FAILED ({osrm_time:.1f}ms)")
            continue

        # Butterfly
        bf_mat, bf_time = test_butterfly_matrix(coords)
        if bf_mat:
            print(f"  Butterfly: {bf_time:.1f}ms")
        else:
            print(f"  Butterfly: FAILED ({bf_time:.1f}ms)")
            continue

        # Compare
        max_diff, max_diff_pct, diffs = compare_matrices(osrm_mat, bf_mat, coords)
        print(f"  Max diff:  {max_diff:.1f}s ({max_diff_pct:.1f}%)")

        if diffs:
            print(f"  Warnings:  {len(diffs)} cells with >10% difference")
            for i, j, osrm_val, bf_val, note in diffs[:3]:
                print(f"    [{i},{j}]: OSRM={osrm_val:.1f}s, Butterfly={bf_val:.1f}s ({note})")
        else:
            print(f"  Quality:   ✓ All cells within 10%")

        # Sample diagonal values
        print(f"  Sample diagonal (should be ~0):")
        for i in range(min(3, size)):
            print(f"    [{i},{i}]: OSRM={osrm_mat[i][i]:.1f}s, Butterfly={bf_mat[i][i]:.1f}s")

        print()

    # Speed benchmark with warm-up
    print("=" * 70)
    print("SPEED BENCHMARK (5 runs each, warmed up)")
    print("=" * 70)
    print()

    coords = TEST_COORDS[:10]

    # Warm up
    for _ in range(2):
        test_osrm_matrix(coords)
        test_butterfly_matrix(coords)

    osrm_times = []
    bf_times = []

    for _ in range(5):
        _, t = test_osrm_matrix(coords)
        osrm_times.append(t)
        _, t = test_butterfly_matrix(coords)
        bf_times.append(t)

    print(f"10x10 Matrix (5 runs):")
    print(f"  OSRM:      avg={sum(osrm_times)/5:.1f}ms, min={min(osrm_times):.1f}ms, max={max(osrm_times):.1f}ms")
    print(f"  Butterfly: avg={sum(bf_times)/5:.1f}ms, min={min(bf_times):.1f}ms, max={max(bf_times):.1f}ms")

    ratio = sum(bf_times) / sum(osrm_times) if sum(osrm_times) > 0 else 0
    if ratio > 1:
        print(f"  Butterfly is {ratio:.1f}x SLOWER than OSRM")
    else:
        print(f"  Butterfly is {1/ratio:.1f}x FASTER than OSRM")

if __name__ == "__main__":
    main()
