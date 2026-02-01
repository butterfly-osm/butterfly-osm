#!/usr/bin/env python3
"""Quick test of Arrow streaming."""

import requests
import random
import time

random.seed(42)

# Generate small test
sources = [[random.uniform(2.5, 6.4), random.uniform(49.5, 51.5)] for _ in range(1000)]
destinations = [[random.uniform(2.5, 6.4), random.uniform(49.5, 51.5)] for _ in range(1000)]

payload = {
    "sources": sources,
    "destinations": destinations,
    "mode": "car",
    "src_tile_size": 1000,
    "dst_tile_size": 1000,
}

print("Testing 1000x1000...")
start = time.perf_counter()
resp = requests.post("http://127.0.0.1:8080/table/stream", json=payload, timeout=60)
end = time.perf_counter()

print(f"Status: {resp.status_code}")
print(f"Size: {len(resp.content)} bytes")
print(f"Time: {end - start:.2f}s")
