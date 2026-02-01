#!/usr/bin/env python3
"""
Comprehensive Matrix Benchmark: OSRM CH vs OSRM MLD vs Butterfly EBG-CCH

Tests:
1. Direct /table comparison up to OSRM limits
2. Large matrix (10000x10000) using each system's optimal approach:
   - OSRM: Tiled requests (due to limits)
   - Butterfly: Batched PHAST + Arrow streaming
"""

import requests
import time
import random
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

# Belgium bounding box
MIN_LAT, MAX_LAT = 49.5, 51.5
MIN_LON, MAX_LON = 2.5, 6.4

def random_coords(n, seed=42):
    """Generate n random coordinates in Belgium"""
    random.seed(seed)
    coords = []
    for _ in range(n):
        lat = random.uniform(MIN_LAT, MAX_LAT)
        lon = random.uniform(MIN_LON, MAX_LON)
        coords.append((lon, lat))
    return coords

def osrm_table(port, sources, targets, timeout=60):
    """
    OSRM /table query
    Returns (time_ms, n_results) or (None, error_msg)
    """
    all_coords = sources + targets
    coords_str = ";".join(f"{lon},{lat}" for lon, lat in all_coords)

    src_indices = ";".join(str(i) for i in range(len(sources)))
    dst_indices = ";".join(str(i) for i in range(len(sources), len(all_coords)))

    url = f"http://localhost:{port}/table/v1/driving/{coords_str}?sources={src_indices}&destinations={dst_indices}"

    try:
        start = time.perf_counter()
        resp = requests.get(url, timeout=timeout)
        elapsed_ms = (time.perf_counter() - start) * 1000

        if resp.status_code == 200:
            data = resp.json()
            if data.get('code') == 'Ok':
                n_results = len(sources) * len(targets)
                return elapsed_ms, n_results
            else:
                return None, f"OSRM error: {data.get('code')}"
        else:
            return None, f"HTTP {resp.status_code}"
    except Exception as e:
        return None, str(e)

def osrm_tiled_table(port, sources, targets, tile_size=100):
    """
    OSRM tiled table for large matrices (chunks into tile_size x tile_size)
    Returns (total_time_ms, n_results)
    """
    total_time = 0
    n_results = 0

    # Tile sources and targets
    for src_start in range(0, len(sources), tile_size):
        src_end = min(src_start + tile_size, len(sources))
        src_chunk = sources[src_start:src_end]

        for dst_start in range(0, len(targets), tile_size):
            dst_end = min(dst_start + tile_size, len(targets))
            dst_chunk = targets[dst_start:dst_end]

            result = osrm_table(port, src_chunk, dst_chunk)
            if result[0] is not None:
                total_time += result[0]
                n_results += result[1]
            else:
                return None, f"Tile failed: {result[1]}"

    return total_time, n_results

def butterfly_matrix(sources, targets, timeout=60):
    """
    Butterfly /matrix endpoint (one-to-many, need to loop sources)
    Returns (time_ms, n_results)
    """
    total_time = 0
    n_results = 0

    dst_lats = ",".join(str(lat) for lon, lat in targets)
    dst_lons = ",".join(str(lon) for lon, lat in targets)

    for lon, lat in sources:
        url = f"http://localhost:3030/matrix?mode=car&src_lat={lat}&src_lon={lon}&dst_lats={dst_lats}&dst_lons={dst_lons}"

        try:
            start = time.perf_counter()
            resp = requests.get(url, timeout=timeout)
            elapsed_ms = (time.perf_counter() - start) * 1000

            if resp.status_code == 200:
                total_time += elapsed_ms
                n_results += len(targets)
            else:
                return None, f"HTTP {resp.status_code}"
        except Exception as e:
            return None, str(e)

    return total_time, n_results

def butterfly_bulk_matrix(sources, targets, timeout=300):
    """
    Butterfly /matrix/bulk endpoint with Arrow streaming
    Returns (time_ms, n_results)
    """
    src_lats = [lat for lon, lat in sources]
    src_lons = [lon for lon, lat in sources]
    dst_lats = [lat for lon, lat in targets]
    dst_lons = [lon for lon, lat in targets]

    payload = {
        "mode": "car",
        "src_lats": src_lats,
        "src_lons": src_lons,
        "dst_lats": dst_lats,
        "dst_lons": dst_lons,
    }

    url = "http://localhost:3030/matrix/bulk"

    try:
        start = time.perf_counter()
        resp = requests.post(url, json=payload, timeout=timeout, stream=True)

        # Stream and discard response to measure full transfer time
        total_bytes = 0
        for chunk in resp.iter_content(chunk_size=65536):
            total_bytes += len(chunk)

        elapsed_ms = (time.perf_counter() - start) * 1000

        if resp.status_code == 200:
            n_results = len(sources) * len(targets)
            return elapsed_ms, n_results, total_bytes
        else:
            return None, f"HTTP {resp.status_code}", 0
    except Exception as e:
        return None, str(e), 0

def run_warmup(port):
    """Warmup queries"""
    coords = random_coords(10, seed=999)
    for _ in range(3):
        osrm_table(port, coords[:5], coords[5:])

def benchmark_direct_table():
    """Benchmark direct /table comparison"""
    print("\n" + "="*80)
    print("DIRECT /table BENCHMARK (OSRM CH vs OSRM MLD)")
    print("="*80)

    # Warmup
    print("\nWarming up...")
    run_warmup(5050)
    run_warmup(5051)

    sizes = [10, 25, 50, 100, 200, 500]

    print(f"\n{'Size':>10} | {'OSRM CH':>12} | {'OSRM MLD':>12} | {'CH/MLD':>8} | {'Winner':>8}")
    print("-"*70)

    results = []
    for n in sizes:
        sources = random_coords(n, seed=n)
        targets = random_coords(n, seed=n+1000)

        # Run 3 times, take median
        ch_times = []
        mld_times = []

        for _ in range(3):
            ch_result = osrm_table(5050, sources, targets)
            mld_result = osrm_table(5051, sources, targets)

            if ch_result[0]:
                ch_times.append(ch_result[0])
            if mld_result[0]:
                mld_times.append(mld_result[0])

        ch_time = sorted(ch_times)[len(ch_times)//2] if ch_times else None
        mld_time = sorted(mld_times)[len(mld_times)//2] if mld_times else None

        if ch_time and mld_time:
            ratio = ch_time / mld_time
            winner = "CH" if ratio < 1 else "MLD"
            print(f"{n}x{n:>6} | {ch_time:>10.1f}ms | {mld_time:>10.1f}ms | {ratio:>7.2f}x | {winner:>8}")
            results.append((n, ch_time, mld_time))
        else:
            print(f"{n}x{n:>6} | {'N/A':>12} | {'N/A':>12} | {'N/A':>8} | {'N/A':>8}")

    return results

def benchmark_large_matrix():
    """Benchmark large matrix (10000x10000) with optimal strategies"""
    print("\n" + "="*80)
    print("LARGE MATRIX BENCHMARK (10000x10000 = 100M distances)")
    print("="*80)

    n = 10000
    sources = random_coords(n, seed=12345)
    targets = random_coords(n, seed=67890)

    print(f"\nMatrix size: {n}x{n} = {n*n:,} distances")

    # Strategy 1: OSRM CH tiled (100x100 tiles = 10000 requests)
    print("\n--- OSRM CH (tiled 100x100) ---")
    n_tiles = (n // 100) ** 2
    print(f"Tiles: {n_tiles} requests of 100x100")

    # Sample timing for a few tiles
    sample_times = []
    for i in range(5):
        src_chunk = sources[i*100:(i+1)*100]
        dst_chunk = targets[i*100:(i+1)*100]
        result = osrm_table(5050, src_chunk, dst_chunk)
        if result[0]:
            sample_times.append(result[0])

    if sample_times:
        avg_tile_time = sum(sample_times) / len(sample_times)
        estimated_total_ch = avg_tile_time * n_tiles
        print(f"Sample tile time: {avg_tile_time:.1f}ms")
        print(f"Estimated total (sequential): {estimated_total_ch/1000:.1f}s")
        print(f"Estimated total (10 parallel): {estimated_total_ch/10000:.1f}s")

    # Strategy 2: OSRM MLD tiled
    print("\n--- OSRM MLD (tiled 100x100) ---")
    sample_times_mld = []
    for i in range(5):
        src_chunk = sources[i*100:(i+1)*100]
        dst_chunk = targets[i*100:(i+1)*100]
        result = osrm_table(5051, src_chunk, dst_chunk)
        if result[0]:
            sample_times_mld.append(result[0])

    if sample_times_mld:
        avg_tile_time_mld = sum(sample_times_mld) / len(sample_times_mld)
        estimated_total_mld = avg_tile_time_mld * n_tiles
        print(f"Sample tile time: {avg_tile_time_mld:.1f}ms")
        print(f"Estimated total (sequential): {estimated_total_mld/1000:.1f}s")
        print(f"Estimated total (10 parallel): {estimated_total_mld/10000:.1f}s")

    # Strategy 3: Butterfly bulk (if available)
    print("\n--- Butterfly EBG-CCH (checking /matrix/bulk) ---")

    # First check if bulk endpoint exists
    try:
        # Test with small matrix first
        test_result = butterfly_bulk_matrix(sources[:10], targets[:10], timeout=10)
        if test_result[0]:
            print(f"Bulk endpoint available")
            # Estimate from small test
            small_time = test_result[0]
            small_size = 100  # 10x10
            # Scaling is roughly O(sources) for PHAST
            estimated_butterfly = small_time * (n / 10)
            print(f"Small test (10x10): {small_time:.1f}ms")
            print(f"Estimated 10000x10000: {estimated_butterfly/1000:.1f}s (linear scaling assumption)")
        else:
            print(f"Bulk endpoint error: {test_result[1]}")
    except Exception as e:
        print(f"Bulk endpoint not available: {e}")

    # Strategy 4: Butterfly one-to-many (PHAST per source)
    print("\n--- Butterfly EBG-CCH (one-to-many PHAST) ---")
    # Test a few one-to-many queries
    sample_phast_times = []
    for i in range(5):
        lon, lat = sources[i]
        dst_lats = ",".join(str(lat) for lon, lat in targets[:1000])
        dst_lons = ",".join(str(lon) for lon, lat in targets[:1000])
        url = f"http://localhost:3030/matrix?mode=car&src_lat={lat}&src_lon={lon}&dst_lats={dst_lats}&dst_lons={dst_lons}"

        try:
            start = time.perf_counter()
            resp = requests.get(url, timeout=30)
            elapsed = (time.perf_counter() - start) * 1000
            if resp.status_code == 200:
                sample_phast_times.append(elapsed)
        except:
            pass

    if sample_phast_times:
        avg_phast_time = sum(sample_phast_times) / len(sample_phast_times)
        # For 10000x10000: need 10000 one-to-many queries
        estimated_phast = avg_phast_time * n
        print(f"Sample one-to-1000 time: {avg_phast_time:.1f}ms")
        print(f"Estimated 10000 x one-to-10000 (sequential): {estimated_phast/1000:.1f}s")
        print(f"Estimated (8 parallel): {estimated_phast/8000:.1f}s")

def main():
    print("Matrix Benchmark Suite")
    print("OSRM CH (port 5050) vs OSRM MLD (port 5051) vs Butterfly (port 3030)")

    # Check services
    print("\nChecking services...")

    services = [
        ("OSRM CH", 5050, "/table/v1/driving/4.3,50.8;4.4,50.9"),
        ("OSRM MLD", 5051, "/table/v1/driving/4.3,50.8;4.4,50.9"),
        ("Butterfly", 3030, "/health"),
    ]

    for name, port, path in services:
        try:
            resp = requests.get(f"http://localhost:{port}{path}", timeout=5)
            status = "OK" if resp.status_code == 200 else f"HTTP {resp.status_code}"
        except Exception as e:
            status = f"FAIL: {e}"
        print(f"  {name}: {status}")

    # Run benchmarks
    benchmark_direct_table()
    benchmark_large_matrix()

if __name__ == "__main__":
    main()
