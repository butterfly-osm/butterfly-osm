#!/usr/bin/env python3
"""Profile 100K isochrones via /isochrone endpoint"""

import requests
import random
import time
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Belgium bounding box
LON_MIN, LON_MAX = 2.5, 6.4
LAT_MIN, LAT_MAX = 49.5, 51.5

# Stats
stats_lock = threading.Lock()
completed = 0
errors = 0
total_time_ms = 0

def generate_origins(n):
    random.seed(42)
    return [(
        round(random.uniform(LON_MIN, LON_MAX), 6),
        round(random.uniform(LAT_MIN, LAT_MAX), 6)
    ) for _ in range(n)]

def run_isochrone(origin, time_s=300, mode="car"):
    global completed, errors, total_time_ms
    lon, lat = origin
    try:
        start = time.time()
        resp = requests.get(
            "http://localhost:8080/isochrone",
            params={
                "lon": lon,
                "lat": lat,
                "time_s": time_s,  # seconds, not minutes
                "mode": mode
            },
            timeout=30
        )
        elapsed_ms = (time.time() - start) * 1000

        with stats_lock:
            if resp.status_code == 200:
                completed += 1
                total_time_ms += elapsed_ms
            else:
                errors += 1
        return resp.status_code, elapsed_ms
    except Exception as e:
        with stats_lock:
            errors += 1
        return None, 0

def main():
    n_isochrones = 100000
    n_workers = 32  # Parallel workers
    time_s = 300  # 5-minute isochrones (in seconds)

    print(f"Generating {n_isochrones:,} random origins...")
    origins = generate_origins(n_isochrones)

    print(f"Running {n_isochrones:,} isochrones with {n_workers} workers...")
    print(f"Threshold: {time_s}s ({time_s//60} min), Mode: car")

    start = time.time()
    last_report = start

    with ThreadPoolExecutor(max_workers=n_workers) as executor:
        futures = [executor.submit(run_isochrone, origin, time_s) for origin in origins]

        for i, future in enumerate(as_completed(futures)):
            now = time.time()
            if now - last_report >= 5.0:  # Report every 5 seconds
                elapsed = now - start
                rate = completed / elapsed if elapsed > 0 else 0
                avg_ms = total_time_ms / completed if completed > 0 else 0
                print(f"  Progress: {completed:,}/{n_isochrones:,} ({100*completed/n_isochrones:.1f}%), "
                      f"{rate:.0f}/sec, avg={avg_ms:.1f}ms, errors={errors}")
                last_report = now

    elapsed = time.time() - start

    print(f"\n=== Results ===")
    print(f"Completed: {completed:,}")
    print(f"Errors: {errors:,}")
    print(f"Total time: {elapsed:.1f}s")
    print(f"Throughput: {completed / elapsed:.0f} isochrones/sec")
    if completed > 0:
        print(f"Avg latency: {total_time_ms / completed:.1f}ms")
        print(f"Effective latency (incl concurrency): {1000 * elapsed / completed:.2f}ms")

if __name__ == "__main__":
    main()
