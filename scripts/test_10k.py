#!/usr/bin/env python3
"""Test 10k×10k matrix."""

import requests
import random
import time
import sys

random.seed(42)

n = 10000
print(f"Generating {n}x{n} coordinates...")
sources = [[random.uniform(2.5, 6.4), random.uniform(49.5, 51.5)] for _ in range(n)]
destinations = [[random.uniform(2.5, 6.4), random.uniform(49.5, 51.5)] for _ in range(n)]

payload = {
    "sources": sources,
    "destinations": destinations,
    "mode": "car",
    "src_tile_size": 1000,
    "dst_tile_size": 1000,
}

print("Sending request...")
sys.stdout.flush()

start = time.perf_counter()
resp = requests.post("http://127.0.0.1:8080/table/stream", json=payload, stream=True, timeout=600)

first_byte = None
chunks = []
for chunk in resp.iter_content(chunk_size=64*1024):
    if first_byte is None:
        first_byte = time.perf_counter()
        print(f"First byte: {first_byte - start:.2f}s")
        sys.stdout.flush()
    chunks.append(chunk)

end = time.perf_counter()
total_data = b''.join(chunks)

print(f"Status: {resp.status_code}")
print(f"Size: {len(total_data):,} bytes ({len(total_data)/1024/1024:.1f} MB)")
print(f"Total time: {end - start:.2f}s")
print(f"Throughput: {100_000_000 / (end - start):,.0f} distances/sec")
