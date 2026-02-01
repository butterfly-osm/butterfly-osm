#!/usr/bin/env python3
"""
OSRM Sanity Parity Suite

Compares Butterfly vs OSRM on 10K random query pairs to:
1. Track drift bounds
2. Calculate correlation coefficient
3. Flag queries where |butterfly - osrm| > 20%

This is a trust-building tool to verify Butterfly produces reasonable results.
"""

import requests
import random
import time
import sys
import json
import statistics
import math
from dataclasses import dataclass
from typing import Optional, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

# Belgium bounding box
MIN_LAT, MAX_LAT = 49.5, 51.5
MIN_LON, MAX_LON = 2.5, 6.4

OSRM_URL = "http://localhost:5050"
BUTTERFLY_URL = "http://localhost:8080"

@dataclass
class QueryResult:
    """Result from a single route query."""
    src: Tuple[float, float]  # (lon, lat)
    dst: Tuple[float, float]  # (lon, lat)
    osrm_duration_s: Optional[float]
    butterfly_duration_s: Optional[float]
    osrm_distance_m: Optional[float]
    butterfly_distance_m: Optional[float]
    osrm_error: Optional[str]
    butterfly_error: Optional[str]

    @property
    def both_valid(self) -> bool:
        return (self.osrm_duration_s is not None and
                self.butterfly_duration_s is not None and
                self.osrm_duration_s > 0)

    @property
    def duration_diff_pct(self) -> Optional[float]:
        if not self.both_valid:
            return None
        return abs(self.butterfly_duration_s - self.osrm_duration_s) / self.osrm_duration_s * 100

    @property
    def duration_ratio(self) -> Optional[float]:
        if not self.both_valid:
            return None
        return self.butterfly_duration_s / self.osrm_duration_s


def random_coord() -> Tuple[float, float]:
    """Generate random coordinate in Belgium."""
    lat = random.uniform(MIN_LAT, MAX_LAT)
    lon = random.uniform(MIN_LON, MAX_LON)
    return (lon, lat)


def query_osrm_route(src: Tuple[float, float], dst: Tuple[float, float]) -> Tuple[Optional[float], Optional[float], Optional[str]]:
    """Query OSRM /route endpoint. Returns (duration_s, distance_m, error)."""
    url = f"{OSRM_URL}/route/v1/driving/{src[0]},{src[1]};{dst[0]},{dst[1]}"
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code != 200:
            return None, None, f"HTTP {resp.status_code}"
        data = resp.json()
        if data.get("code") != "Ok":
            return None, None, data.get("code", "Unknown error")
        routes = data.get("routes", [])
        if not routes:
            return None, None, "No routes"
        route = routes[0]
        return route.get("duration"), route.get("distance"), None
    except Exception as e:
        return None, None, str(e)


def query_butterfly_route(src: Tuple[float, float], dst: Tuple[float, float]) -> Tuple[Optional[float], Optional[float], Optional[str]]:
    """Query Butterfly /route endpoint. Returns (duration_s, distance_m, error)."""
    url = f"{BUTTERFLY_URL}/route?src_lon={src[0]}&src_lat={src[1]}&dst_lon={dst[0]}&dst_lat={dst[1]}&mode=car"
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code != 200:
            try:
                data = resp.json()
                return None, None, data.get("error", f"HTTP {resp.status_code}")
            except:
                return None, None, f"HTTP {resp.status_code}"
        data = resp.json()
        return data.get("duration_s"), data.get("distance_m"), None
    except Exception as e:
        return None, None, str(e)


def run_single_comparison(idx: int) -> QueryResult:
    """Run a single random comparison."""
    src = random_coord()
    dst = random_coord()

    osrm_dur, osrm_dist, osrm_err = query_osrm_route(src, dst)
    bf_dur, bf_dist, bf_err = query_butterfly_route(src, dst)

    return QueryResult(
        src=src,
        dst=dst,
        osrm_duration_s=osrm_dur,
        butterfly_duration_s=bf_dur,
        osrm_distance_m=osrm_dist,
        butterfly_distance_m=bf_dist,
        osrm_error=osrm_err,
        butterfly_error=bf_err,
    )


def calculate_correlation(x: List[float], y: List[float]) -> float:
    """Calculate Pearson correlation coefficient."""
    if len(x) < 2 or len(x) != len(y):
        return 0.0

    n = len(x)
    mean_x = sum(x) / n
    mean_y = sum(y) / n

    num = sum((xi - mean_x) * (yi - mean_y) for xi, yi in zip(x, y))
    denom_x = math.sqrt(sum((xi - mean_x) ** 2 for xi in x))
    denom_y = math.sqrt(sum((yi - mean_y) ** 2 for yi in y))

    if denom_x == 0 or denom_y == 0:
        return 0.0

    return num / (denom_x * denom_y)


def run_parity_suite(n_queries: int = 10000, n_workers: int = 16, save_flagged: bool = True):
    """Run the full parity suite."""
    print("=" * 70)
    print("OSRM SANITY PARITY SUITE")
    print("=" * 70)
    print(f"Target: {n_queries} random route queries")
    print(f"Workers: {n_workers}")
    print()

    # Check services are up
    print("Checking services...")
    try:
        r = requests.get(f"{OSRM_URL}/health", timeout=5)
        print(f"  OSRM ({OSRM_URL}): ", end="")
        # OSRM doesn't have /health, try a simple route
        r = requests.get(f"{OSRM_URL}/route/v1/driving/4.35,50.85;4.40,50.86", timeout=5)
        if r.status_code == 200:
            print("OK")
        else:
            print(f"Warning - status {r.status_code}")
    except Exception as e:
        print(f"FAILED - {e}")
        print("\nPlease start OSRM: docker run -t -i -p 5050:5000 -v \"${PWD}/data:/data\" osrm/osrm-backend osrm-routed --algorithm ch /data/belgium.osrm")
        sys.exit(1)

    try:
        r = requests.get(f"{BUTTERFLY_URL}/health", timeout=5)
        print(f"  Butterfly ({BUTTERFLY_URL}): ", end="")
        if r.status_code == 200:
            print("OK")
        else:
            print(f"Warning - status {r.status_code}")
    except Exception as e:
        print(f"FAILED - {e}")
        print("\nPlease start Butterfly: ./target/release/butterfly-route serve --data-dir ./data/belgium")
        sys.exit(1)

    print()
    print("Running comparisons...")

    results: List[QueryResult] = []
    start_time = time.perf_counter()

    with ThreadPoolExecutor(max_workers=n_workers) as executor:
        futures = [executor.submit(run_single_comparison, i) for i in range(n_queries)]

        completed = 0
        for future in as_completed(futures):
            results.append(future.result())
            completed += 1
            if completed % 500 == 0:
                elapsed = time.perf_counter() - start_time
                rate = completed / elapsed
                eta = (n_queries - completed) / rate if rate > 0 else 0
                print(f"  Progress: {completed}/{n_queries} ({rate:.1f}/sec, ETA: {eta:.0f}s)")

    total_time = time.perf_counter() - start_time
    print(f"\nCompleted {n_queries} queries in {total_time:.1f}s ({n_queries/total_time:.1f}/sec)")
    print()

    # Analyze results
    print("=" * 70)
    print("ANALYSIS")
    print("=" * 70)
    print()

    # Count categories
    both_valid = [r for r in results if r.both_valid]
    osrm_only = [r for r in results if r.osrm_duration_s is not None and r.butterfly_duration_s is None]
    bf_only = [r for r in results if r.osrm_duration_s is None and r.butterfly_duration_s is not None]
    both_failed = [r for r in results if r.osrm_duration_s is None and r.butterfly_duration_s is None]

    print(f"Query outcomes:")
    print(f"  Both succeeded:      {len(both_valid):,} ({100*len(both_valid)/n_queries:.1f}%)")
    print(f"  OSRM only:           {len(osrm_only):,} ({100*len(osrm_only)/n_queries:.1f}%)")
    print(f"  Butterfly only:      {len(bf_only):,} ({100*len(bf_only)/n_queries:.1f}%)")
    print(f"  Both failed:         {len(both_failed):,} ({100*len(both_failed)/n_queries:.1f}%)")
    print()

    if not both_valid:
        print("ERROR: No valid comparisons. Cannot compute statistics.")
        return

    # Duration comparison
    osrm_durations = [r.osrm_duration_s for r in both_valid]
    bf_durations = [r.butterfly_duration_s for r in both_valid]
    duration_diffs = [r.duration_diff_pct for r in both_valid]
    duration_ratios = [r.duration_ratio for r in both_valid]

    print("Duration comparison (both succeeded):")
    print(f"  OSRM mean:           {statistics.mean(osrm_durations):.1f}s")
    print(f"  Butterfly mean:      {statistics.mean(bf_durations):.1f}s")
    print(f"  Mean ratio (bf/osrm): {statistics.mean(duration_ratios):.3f}")
    print(f"  Median ratio:        {statistics.median(duration_ratios):.3f}")
    print()

    # Drift bounds
    print("Drift bounds (|bf - osrm| / osrm * 100%):")
    print(f"  Min drift:           {min(duration_diffs):.2f}%")
    print(f"  Max drift:           {max(duration_diffs):.2f}%")
    print(f"  Mean drift:          {statistics.mean(duration_diffs):.2f}%")
    print(f"  Median drift:        {statistics.median(duration_diffs):.2f}%")
    print(f"  Std dev:             {statistics.stdev(duration_diffs):.2f}%")
    print()

    # Distribution of drift
    drift_buckets = [0, 5, 10, 20, 50, 100, float('inf')]
    print("Drift distribution:")
    for i in range(len(drift_buckets) - 1):
        low, high = drift_buckets[i], drift_buckets[i+1]
        count = sum(1 for d in duration_diffs if low <= d < high)
        pct = 100 * count / len(duration_diffs)
        label = f"{low}-{high}%" if high != float('inf') else f">{low}%"
        bar = "█" * int(pct / 2)
        print(f"  {label:10s}: {count:5d} ({pct:5.1f}%) {bar}")
    print()

    # Correlation
    correlation = calculate_correlation(osrm_durations, bf_durations)
    print(f"Correlation coefficient: {correlation:.4f}")
    if correlation > 0.99:
        print("  → Excellent agreement")
    elif correlation > 0.95:
        print("  → Good agreement")
    elif correlation > 0.90:
        print("  → Moderate agreement")
    else:
        print("  → Poor agreement - investigate!")
    print()

    # Flag queries with >20% drift
    flagged = [r for r in both_valid if r.duration_diff_pct > 20]
    print(f"Flagged queries (>20% drift): {len(flagged)} ({100*len(flagged)/len(both_valid):.1f}%)")

    if flagged and save_flagged:
        # Save flagged queries for investigation
        flagged_data = []
        for r in flagged:
            flagged_data.append({
                "src": list(r.src),
                "dst": list(r.dst),
                "osrm_duration_s": r.osrm_duration_s,
                "butterfly_duration_s": r.butterfly_duration_s,
                "osrm_distance_m": r.osrm_distance_m,
                "butterfly_distance_m": r.butterfly_distance_m,
                "drift_pct": r.duration_diff_pct,
            })

        flagged_path = "scripts/osrm_parity_flagged.json"
        with open(flagged_path, "w") as f:
            json.dump(flagged_data, f, indent=2)
        print(f"  Saved to: {flagged_path}")

        # Show worst examples
        flagged_sorted = sorted(flagged, key=lambda r: r.duration_diff_pct, reverse=True)
        print("\n  Top 5 worst drifts:")
        for i, r in enumerate(flagged_sorted[:5]):
            print(f"    {i+1}. {r.src[0]:.4f},{r.src[1]:.4f} → {r.dst[0]:.4f},{r.dst[1]:.4f}")
            print(f"       OSRM: {r.osrm_duration_s:.1f}s, Butterfly: {r.butterfly_duration_s:.1f}s ({r.duration_diff_pct:.1f}%)")

    print()

    # Investigate systematic bias
    print("Systematic bias check:")
    bf_faster = sum(1 for r in both_valid if r.butterfly_duration_s < r.osrm_duration_s)
    bf_slower = len(both_valid) - bf_faster
    print(f"  Butterfly faster: {bf_faster} ({100*bf_faster/len(both_valid):.1f}%)")
    print(f"  Butterfly slower: {bf_slower} ({100*bf_slower/len(both_valid):.1f}%)")

    if bf_slower > bf_faster * 1.5:
        print("  → Butterfly tends to report LONGER routes")
        print("  → This is expected: exact turn penalties add time")
    elif bf_faster > bf_slower * 1.5:
        print("  → Butterfly tends to report SHORTER routes")
        print("  → Investigate: may be missing penalties")
    else:
        print("  → No significant systematic bias")

    print()
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)

    passed = True
    issues = []

    if correlation < 0.95:
        passed = False
        issues.append(f"Low correlation: {correlation:.4f} < 0.95")

    if len(flagged) / len(both_valid) > 0.10:
        passed = False
        issues.append(f"Too many flagged queries: {100*len(flagged)/len(both_valid):.1f}% > 10%")

    if statistics.mean(duration_diffs) > 15:
        passed = False
        issues.append(f"High mean drift: {statistics.mean(duration_diffs):.1f}% > 15%")

    if passed:
        print("✅ PARITY CHECK PASSED")
        print("   Butterfly results are consistent with OSRM")
    else:
        print("❌ PARITY CHECK FAILED")
        for issue in issues:
            print(f"   - {issue}")

    return {
        "n_queries": n_queries,
        "both_valid": len(both_valid),
        "correlation": correlation,
        "mean_drift_pct": statistics.mean(duration_diffs),
        "flagged_count": len(flagged),
        "flagged_pct": 100 * len(flagged) / len(both_valid),
        "passed": passed,
    }


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="OSRM Sanity Parity Suite")
    parser.add_argument("-n", "--n-queries", type=int, default=10000, help="Number of random queries")
    parser.add_argument("-w", "--workers", type=int, default=16, help="Number of parallel workers")
    parser.add_argument("--no-save", action="store_true", help="Don't save flagged queries to file")
    args = parser.parse_args()

    run_parity_suite(args.n_queries, args.workers, not args.no_save)
