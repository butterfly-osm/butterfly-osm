#!/usr/bin/env python3
"""
Comprehensive Matrix Benchmark v2

Tests 500x500 and estimates 10000x10000 using each system's optimal approach:
- OSRM CH/MLD: Direct /table (with increased limits)
- Butterfly: One-to-many /matrix calls (which handle snapping)

For throughput comparison:
- OSRM: Can parallelize /table calls
- Butterfly: Can parallelize one-to-many calls + use /matrix/stream for Arrow output
"""

import requests
import time
import random
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
import statistics

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

def osrm_table(port, sources, targets, timeout=120):
    """OSRM /table query with coordinates"""
    all_coords = sources + targets
    coords_str = ";".join(f"{lon},{lat}" for lon, lat in all_coords)

    src_indices = ";".join(str(i) for i in range(len(sources)))
    dst_indices = ";".join(str(i) for i in range(len(sources), len(all_coords)))

    url = f"http://localhost:{port}/table/v1/driving/{coords_str}?sources={src_indices}&destinations={dst_indices}"

    start = time.perf_counter()
    try:
        resp = requests.get(url, timeout=timeout)
        elapsed_ms = (time.perf_counter() - start) * 1000

        if resp.status_code == 200:
            data = resp.json()
            if data.get('code') == 'Ok':
                return elapsed_ms, len(sources) * len(targets), None
            else:
                return None, 0, f"OSRM: {data.get('code')} - {data.get('message', '')}"
        else:
            return None, 0, f"HTTP {resp.status_code}"
    except Exception as e:
        return None, 0, str(e)

def butterfly_one_to_many(source, targets, timeout=60):
    """Butterfly /matrix one-to-many query"""
    lon, lat = source
    dst_lats = ",".join(str(t[1]) for t in targets)
    dst_lons = ",".join(str(t[0]) for t in targets)

    url = f"http://localhost:3030/matrix?mode=car&src_lat={lat}&src_lon={lon}&dst_lats={dst_lats}&dst_lons={dst_lons}"

    start = time.perf_counter()
    try:
        resp = requests.get(url, timeout=timeout)
        elapsed_ms = (time.perf_counter() - start) * 1000

        if resp.status_code == 200:
            return elapsed_ms, len(targets), None
        else:
            return None, 0, f"HTTP {resp.status_code}"
    except Exception as e:
        return None, 0, str(e)

def butterfly_matrix_sequential(sources, targets, timeout=300):
    """Butterfly matrix via sequential one-to-many calls"""
    total_time = 0
    total_results = 0

    for source in sources:
        result = butterfly_one_to_many(source, targets, timeout=timeout/len(sources))
        if result[0] is not None:
            total_time += result[0]
            total_results += result[1]
        else:
            return None, 0, result[2]

    return total_time, total_results, None

def butterfly_matrix_parallel(sources, targets, n_workers=8, timeout=300):
    """Butterfly matrix via parallel one-to-many calls"""
    start = time.perf_counter()
    total_results = 0

    with ThreadPoolExecutor(max_workers=n_workers) as executor:
        futures = {executor.submit(butterfly_one_to_many, src, targets, timeout/len(sources)): src
                   for src in sources}

        for future in as_completed(futures):
            result = future.result()
            if result[0] is not None:
                total_results += result[1]
            else:
                return None, 0, result[2]

    elapsed_ms = (time.perf_counter() - start) * 1000
    return elapsed_ms, total_results, None

def osrm_tiled_parallel(port, sources, targets, tile_size=100, n_workers=10):
    """OSRM tiled with parallel requests"""
    start = time.perf_counter()
    total_results = 0

    # Generate tiles
    tiles = []
    for src_start in range(0, len(sources), tile_size):
        src_end = min(src_start + tile_size, len(sources))
        src_chunk = sources[src_start:src_end]

        for dst_start in range(0, len(targets), tile_size):
            dst_end = min(dst_start + tile_size, len(targets))
            dst_chunk = targets[dst_start:dst_end]
            tiles.append((src_chunk, dst_chunk))

    with ThreadPoolExecutor(max_workers=n_workers) as executor:
        futures = {executor.submit(osrm_table, port, tile[0], tile[1]): tile for tile in tiles}

        for future in as_completed(futures):
            result = future.result()
            if result[0] is not None:
                total_results += result[1]
            else:
                return None, 0, result[2]

    elapsed_ms = (time.perf_counter() - start) * 1000
    return elapsed_ms, total_results, None

def run_benchmark(name, func, *args, n_runs=3):
    """Run benchmark multiple times and report stats"""
    times = []
    results = 0

    for i in range(n_runs):
        result = func(*args)
        if result[0] is not None:
            times.append(result[0])
            results = result[1]
        else:
            print(f"  {name} run {i+1} failed: {result[2]}")

    if times:
        return {
            'name': name,
            'median_ms': statistics.median(times),
            'min_ms': min(times),
            'max_ms': max(times),
            'n_results': results,
        }
    return None

def main():
    print("="*80)
    print("MATRIX BENCHMARK v2: OSRM CH vs OSRM MLD vs Butterfly EBG-CCH")
    print("="*80)

    # Check services
    print("\nChecking services...")
    for name, port in [("OSRM CH", 5050), ("OSRM MLD", 5051), ("Butterfly", 3030)]:
        try:
            if port == 3030:
                resp = requests.get(f"http://localhost:{port}/health", timeout=5)
            else:
                resp = requests.get(f"http://localhost:{port}/table/v1/driving/4.3,50.8;4.4,50.9", timeout=5)
            status = "OK" if resp.status_code == 200 else f"HTTP {resp.status_code}"
        except Exception as e:
            status = f"FAIL"
        print(f"  {name}: {status}")

    # ==================== 500x500 BENCHMARK ====================
    print("\n" + "="*80)
    print("500×500 MATRIX BENCHMARK (250,000 distances)")
    print("="*80)

    sources_500 = random_coords(500, seed=500)
    targets_500 = random_coords(500, seed=501)

    print("\n--- OSRM CH (direct /table) ---")
    ch_500 = run_benchmark("OSRM CH 500x500", osrm_table, 5050, sources_500, targets_500)
    if ch_500:
        print(f"  Median: {ch_500['median_ms']:.0f}ms, Range: [{ch_500['min_ms']:.0f}, {ch_500['max_ms']:.0f}]ms")
        print(f"  Throughput: {ch_500['n_results']/ch_500['median_ms']*1000:.0f} distances/sec")

    print("\n--- OSRM MLD (direct /table) ---")
    mld_500 = run_benchmark("OSRM MLD 500x500", osrm_table, 5051, sources_500, targets_500)
    if mld_500:
        print(f"  Median: {mld_500['median_ms']:.0f}ms, Range: [{mld_500['min_ms']:.0f}, {mld_500['max_ms']:.0f}]ms")
        print(f"  Throughput: {mld_500['n_results']/mld_500['median_ms']*1000:.0f} distances/sec")

    print("\n--- Butterfly (500 one-to-many, sequential) ---")
    bf_500_seq = run_benchmark("Butterfly 500x500 seq", butterfly_matrix_sequential, sources_500, targets_500, n_runs=1)
    if bf_500_seq:
        print(f"  Total time: {bf_500_seq['median_ms']:.0f}ms")
        print(f"  Per-source avg: {bf_500_seq['median_ms']/500:.1f}ms")
        print(f"  Throughput: {bf_500_seq['n_results']/bf_500_seq['median_ms']*1000:.0f} distances/sec")

    print("\n--- Butterfly (500 one-to-many, 8 parallel) ---")
    bf_500_par = run_benchmark("Butterfly 500x500 par", butterfly_matrix_parallel, sources_500, targets_500, 8, n_runs=1)
    if bf_500_par:
        print(f"  Wall time: {bf_500_par['median_ms']:.0f}ms")
        print(f"  Throughput: {bf_500_par['n_results']/bf_500_par['median_ms']*1000:.0f} distances/sec")

    # Summary
    print("\n--- 500×500 SUMMARY ---")
    if ch_500 and mld_500 and bf_500_par:
        results = [
            ("OSRM CH", ch_500['median_ms']),
            ("OSRM MLD", mld_500['median_ms']),
            ("Butterfly (8∥)", bf_500_par['median_ms']),
        ]
        results.sort(key=lambda x: x[1])

        print(f"{'Rank':<6} {'System':<20} {'Time':>10} {'vs Winner':>12}")
        print("-"*50)
        for i, (name, t) in enumerate(results):
            ratio = t / results[0][1]
            print(f"{i+1:<6} {name:<20} {t:>8.0f}ms {ratio:>10.1f}x")

    # ==================== 10000x10000 ESTIMATION ====================
    print("\n" + "="*80)
    print("10000×10000 MATRIX ESTIMATION (100M distances)")
    print("="*80)

    # Sample for estimation
    sources_sample = random_coords(100, seed=10000)
    targets_sample = random_coords(100, seed=10001)

    print("\n--- OSRM CH ---")
    # 100x100 tiles = 10000 tiles for 10000x10000
    ch_tile = run_benchmark("CH 100x100 tile", osrm_table, 5050, sources_sample, targets_sample, n_runs=5)
    if ch_tile:
        n_tiles = (10000 // 100) ** 2  # 10000 tiles
        seq_estimate = ch_tile['median_ms'] * n_tiles / 1000
        par_estimate = seq_estimate / 10  # 10 parallel workers
        print(f"  Sample 100×100 tile: {ch_tile['median_ms']:.1f}ms")
        print(f"  Tiles needed: {n_tiles}")
        print(f"  Sequential estimate: {seq_estimate:.0f}s")
        print(f"  Parallel (10 workers): {par_estimate:.0f}s")

    print("\n--- OSRM MLD ---")
    mld_tile = run_benchmark("MLD 100x100 tile", osrm_table, 5051, sources_sample, targets_sample, n_runs=5)
    if mld_tile:
        seq_estimate = mld_tile['median_ms'] * n_tiles / 1000
        par_estimate = seq_estimate / 10
        print(f"  Sample 100×100 tile: {mld_tile['median_ms']:.1f}ms")
        print(f"  Sequential estimate: {seq_estimate:.0f}s")
        print(f"  Parallel (10 workers): {par_estimate:.0f}s")

    print("\n--- Butterfly ---")
    # Sample one-to-1000 timing
    sample_src = sources_sample[0]
    targets_1000 = random_coords(1000, seed=1000)

    times = []
    for _ in range(5):
        result = butterfly_one_to_many(sample_src, targets_1000)
        if result[0]:
            times.append(result[0])

    if times:
        median_1to1000 = statistics.median(times)
        # For 10000x10000: 10000 one-to-10000 calls
        # Estimate scales ~linearly with targets (PHAST downward scan)
        scaling_factor = 10000 / 1000  # 10x more targets
        time_per_source = median_1to1000 * scaling_factor
        seq_estimate = time_per_source * 10000 / 1000
        par_estimate = seq_estimate / 8  # 8 parallel workers

        print(f"  Sample one-to-1000: {median_1to1000:.1f}ms")
        print(f"  Estimated one-to-10000: {time_per_source:.0f}ms")
        print(f"  Sequential estimate (10000 sources): {seq_estimate:.0f}s")
        print(f"  Parallel (8 workers): {par_estimate:.0f}s")

    # Final comparison
    print("\n" + "="*80)
    print("ESTIMATED THROUGHPUT COMPARISON (100M distances)")
    print("="*80)

    estimates = []
    if ch_tile:
        ch_par_time = ch_tile['median_ms'] * n_tiles / 10 / 1000  # 10 parallel
        estimates.append(("OSRM CH (10∥)", ch_par_time, 100_000_000 / ch_par_time))
    if mld_tile:
        mld_par_time = mld_tile['median_ms'] * n_tiles / 10 / 1000
        estimates.append(("OSRM MLD (10∥)", mld_par_time, 100_000_000 / mld_par_time))
    if times:
        bf_par_time = seq_estimate / 8
        estimates.append(("Butterfly (8∥)", bf_par_time, 100_000_000 / bf_par_time))

    estimates.sort(key=lambda x: x[1])

    print(f"\n{'System':<25} {'Time':>12} {'Throughput':>18}")
    print("-"*60)
    for name, t, throughput in estimates:
        print(f"{name:<25} {t:>10.0f}s {throughput/1_000_000:>15.1f}M/s")

if __name__ == "__main__":
    main()
