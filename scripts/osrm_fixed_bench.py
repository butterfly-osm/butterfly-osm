#!/usr/bin/env python3
"""
OSRM benchmark with fixed coordinates (no random snapping overhead).
Compares with Butterfly bucket-m2m benchmark using same location types.
"""
import requests
import time

OSRM_URL = "http://localhost:5050/table/v1/driving/"

# Fixed coordinates on major roads in Belgium (pre-validated to snap quickly)
FIXED_COORDS = [
    (4.3517, 50.8503),   # Brussels center
    (4.4025, 50.8449),   # Brussels east
    (3.7250, 51.0543),   # Ghent
    (4.4028, 51.2194),   # Antwerp
    (5.5796, 50.6326),   # Liège
    (4.4449, 50.4108),   # Charleroi
    (3.2247, 51.2093),   # Bruges
    (4.7008, 50.8798),   # Leuven
    (5.9714, 50.9386),   # Near Aachen
    (4.8673, 50.4679),   # Namur
    (4.0334, 50.6414),   # Mons area
    (4.6186, 50.5256),   # E411 junction
    (4.2500, 50.7500),   # South Brussels
    (4.5000, 50.9000),   # E40 area
    (3.9000, 50.8000),   # Halle area
    (4.1500, 51.0000),   # Dendermonde area
    (5.0000, 50.7000),   # Huy area
    (5.3000, 50.8500),   # Near Verviers
    (3.5000, 50.9000),   # Aalst area
    (4.8000, 51.0000),   # Mechelen area
    (4.9500, 50.5500),   # Andenne area
    (5.1000, 50.4000),   # Dinant area
    (4.3000, 50.3000),   # Binche area
    (3.8500, 50.5000),   # Tournai area
    (5.8000, 50.5000),   # Spa area
    # Add more for larger matrices
    (4.2000, 50.9500),
    (4.6000, 50.6000),
    (3.6000, 50.7500),
    (5.2000, 51.0000),
    (4.0000, 51.1000),
    (5.4000, 50.4500),
    (4.7500, 50.3500),
    (3.4000, 51.0500),
    (5.0500, 50.9500),
    (4.3500, 50.5500),
    (3.9500, 50.9500),
    (5.6500, 50.4000),
    (4.1000, 50.4500),
    (4.9000, 50.8000),
    (3.7500, 50.8500),
    (5.1500, 50.5500),
    (4.5500, 50.7000),
    (4.0500, 50.6500),
    (5.3500, 50.6500),
    (4.2500, 51.0500),
    (3.5500, 51.1000),
    (5.7500, 50.7500),
    (4.8500, 50.4000),
    (4.4500, 50.9500),
    (3.3500, 50.8500),
]

def bench_osrm(n, runs=5):
    """Benchmark OSRM with first n coordinates"""
    coords = FIXED_COORDS[:n]
    coords_str = ";".join(f"{lon},{lat}" for lon, lat in coords)
    url = f"{OSRM_URL}{coords_str}"

    times = []
    for r in range(runs):
        start = time.perf_counter()
        resp = requests.get(url, timeout=60)
        elapsed = (time.perf_counter() - start) * 1000

        if resp.status_code == 200:
            data = resp.json()
            if data.get("code") == "Ok":
                times.append(elapsed)
            else:
                print(f"  Run {r}: OSRM error: {data.get('code')}")
        else:
            print(f"  Run {r}: HTTP {resp.status_code}")

    if times:
        return sum(times)/len(times), min(times), max(times)
    return None, None, None

def main():
    print("=" * 60)
    print("OSRM CH Matrix Benchmark (Fixed Coordinates)")
    print("=" * 60)
    print()
    print("Using pre-validated coordinates on major Belgian roads.")
    print("This avoids random snapping variance for fair comparison.")
    print()

    for size in [10, 25, 50]:
        if size > len(FIXED_COORDS):
            print(f"{size}x{size}: Skipped (need {size} coords, have {len(FIXED_COORDS)})")
            continue

        avg, mn, mx = bench_osrm(size, runs=5)
        if avg:
            print(f"{size}x{size}: avg={avg:.1f}ms, min={mn:.1f}ms, max={mx:.1f}ms")
        else:
            print(f"{size}x{size}: FAILED")

    print()
    print("Compare with Butterfly bucket-m2m benchmark:")
    print("  ./target/release/butterfly-bench bucket-m2m --data-dir ./data/belgium")
    print()
    print("Note: Butterfly benchmark uses random pre-snapped node IDs,")
    print("which is equivalent to fixed coordinates (no snapping overhead).")

if __name__ == "__main__":
    main()
