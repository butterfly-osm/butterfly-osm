import requests
import time
import random

# Belgium bounding box
MIN_LAT, MAX_LAT = 49.5, 51.5
MIN_LON, MAX_LON = 2.5, 6.4

OSRM_URL = "http://localhost:5050/table/v1/driving/"

def random_coords(n):
    coords = []
    for _ in range(n):
        lat = random.uniform(MIN_LAT, MAX_LAT)
        lon = random.uniform(MIN_LON, MAX_LON)
        coords.append(f"{lon},{lat}")
    return coords

def bench_matrix(n_sources, n_targets, runs=5):
    times = []
    for _ in range(runs):
        sources = random_coords(n_sources)
        targets = random_coords(n_targets)
        
        all_coords = sources + targets
        coords_str = ";".join(all_coords)
        
        sources_idx = ";".join(str(i) for i in range(n_sources))
        targets_idx = ";".join(str(i) for i in range(n_sources, n_sources + n_targets))
        
        url = f"{OSRM_URL}{coords_str}?sources={sources_idx}&destinations={targets_idx}"
        
        start = time.perf_counter()
        resp = requests.get(url)
        elapsed = (time.perf_counter() - start) * 1000
        
        if resp.status_code == 200:
            times.append(elapsed)
    
    if times:
        avg = sum(times) / len(times)
        return avg, min(times), max(times)
    return None, None, None

print("OSRM CH Matrix Benchmark (Belgium)")
print("=" * 50)

for size in [10, 25, 50, 100]:
    avg, mn, mx = bench_matrix(size, size, runs=5)
    if avg:
        print(f"{size}x{size}: avg={avg:.1f}ms, min={mn:.1f}ms, max={mx:.1f}ms")
    else:
        print(f"{size}x{size}: FAILED")
