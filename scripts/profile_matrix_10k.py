#!/usr/bin/env python3
"""Profile 10k×10k matrix via /table/stream endpoint"""

import requests
import random
import time
import sys

# Belgium bounding box
LON_MIN, LON_MAX = 2.5, 6.4
LAT_MIN, LAT_MAX = 49.5, 51.5

def generate_coords(n):
    random.seed(42)
    return [[
        round(random.uniform(LON_MIN, LON_MAX), 6),
        round(random.uniform(LAT_MIN, LAT_MAX), 6)
    ] for _ in range(n)]

def main():
    n = 10000
    print(f"Generating {n}x{n} coordinates...")
    sources = generate_coords(n)
    destinations = generate_coords(n)

    payload = {
        "sources": sources,
        "destinations": destinations,
        "mode": "car",
        "src_tile_size": 1000,
        "dst_tile_size": 1000
    }

    print(f"Payload size: ~{len(str(payload)) / 1024 / 1024:.1f} MB")
    print("Sending request...")

    start = time.time()
    resp = requests.post(
        "http://localhost:8080/table/stream",
        json=payload,
        stream=True,
        timeout=600
    )

    first_byte_time = None
    total_bytes = 0
    chunks = 0

    for chunk in resp.iter_content(chunk_size=64*1024):
        if first_byte_time is None:
            first_byte_time = time.time() - start
            print(f"First byte: {first_byte_time:.2f}s")
        total_bytes += len(chunk)
        chunks += 1
        if chunks % 100 == 0:
            elapsed = time.time() - start
            print(f"  ... {total_bytes / 1024 / 1024:.1f} MB received ({elapsed:.1f}s)")

    elapsed = time.time() - start
    print(f"\n=== Results ===")
    print(f"Status: {resp.status_code}")
    print(f"Size: {total_bytes:,} bytes ({total_bytes / 1024 / 1024:.1f} MB)")
    print(f"Total time: {elapsed:.2f}s")
    print(f"Expected distances: {n*n:,}")
    print(f"Throughput: {n*n / elapsed:,.0f} distances/sec")

if __name__ == "__main__":
    main()
