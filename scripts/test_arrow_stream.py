#!/usr/bin/env python3
"""Test Arrow streaming endpoint for large matrices."""

import requests
import random
import time
import pyarrow.ipc as ipc
import io
import sys
import struct

# Belgium bounding box
BELGIUM_BBOX = {
    "min_lon": 2.5,
    "max_lon": 6.4,
    "min_lat": 49.5,
    "max_lat": 51.5,
}

def generate_random_coords(n):
    """Generate random coordinates within Belgium."""
    coords = []
    for _ in range(n):
        lon = random.uniform(BELGIUM_BBOX["min_lon"], BELGIUM_BBOX["max_lon"])
        lat = random.uniform(BELGIUM_BBOX["min_lat"], BELGIUM_BBOX["max_lat"])
        coords.append([lon, lat])
    return coords

def parse_concatenated_ipc_streams(data):
    """Parse concatenated Arrow IPC streams."""
    offset = 0
    total_distances = 0
    tile_count = 0
    reachable_count = 0

    while offset < len(data):
        # Try to find the next IPC stream
        # Arrow IPC stream starts with magic bytes 0xFFFFFFFF followed by schema message
        if offset + 8 > len(data):
            break

        # Check for Arrow magic (0xFFFFFFFF followed by message length)
        magic = struct.unpack_from('<I', data, offset)[0]
        if magic != 0xFFFFFFFF:
            offset += 1
            continue

        try:
            # Try to parse an IPC stream starting at this offset
            stream_data = data[offset:]
            reader = ipc.open_stream(io.BytesIO(stream_data))

            stream_bytes_read = 0
            for batch in reader:
                n_rows = batch.num_rows

                # Parse each tile's binary blob
                src_lens = batch.column('src_block_len').to_pylist()
                dst_lens = batch.column('dst_block_len').to_pylist()
                durations_blobs = batch.column('durations_ms')

                for i in range(n_rows):
                    tile_count += 1
                    src_len = src_lens[i]
                    dst_len = dst_lens[i]
                    blob = durations_blobs[i].as_py()
                    n_distances = src_len * dst_len
                    total_distances += n_distances

                    # Sample a few to count reachable (don't count all for speed)
                    sample_size = min(100, n_distances)
                    sample_reachable = 0
                    for j in range(sample_size):
                        d = struct.unpack_from('<I', blob, j * 4)[0]
                        if d != 0xFFFFFFFF:
                            sample_reachable += 1
                    reachable_count += int(sample_reachable * n_distances / sample_size)

                    if tile_count <= 3:
                        print(f"  Tile {tile_count}: {src_len}x{dst_len} = {n_distances:,} distances")

            # Estimate bytes consumed - find next magic or end
            next_offset = offset + 100  # Skip at least some bytes
            while next_offset < len(data) - 4:
                if struct.unpack_from('<I', data, next_offset)[0] == 0xFFFFFFFF:
                    # Check if this looks like a new stream (has a reasonable length after)
                    if next_offset + 8 <= len(data):
                        msg_len = struct.unpack_from('<I', data, next_offset + 4)[0]
                        if msg_len > 0 and msg_len < 10000:  # Reasonable schema size
                            break
                next_offset += 1

            if next_offset >= len(data) - 4:
                break
            offset = next_offset

        except Exception as e:
            # Move past this position and try again
            offset += 1

    return tile_count, total_distances, reachable_count


def run_benchmark(n_sources, n_destinations):
    """Run Arrow streaming benchmark."""
    print(f"\n{'='*60}")
    print(f"Arrow Streaming: {n_sources}x{n_destinations} matrix")
    print(f"Expected output: {n_sources * n_destinations:,} distances")
    print(f"{'='*60}")

    # Generate coordinates
    print("Generating coordinates...")
    sources = generate_random_coords(n_sources)
    destinations = generate_random_coords(n_destinations)

    payload = {
        "sources": sources,
        "destinations": destinations,
        "mode": "car",
        "src_tile_size": 1000,
        "dst_tile_size": 1000,
    }

    # Calculate expected tiles
    n_src_tiles = (n_sources + 999) // 1000
    n_dst_tiles = (n_destinations + 999) // 1000
    total_tiles = n_src_tiles * n_dst_tiles
    print(f"Expected tiles: {n_src_tiles} x {n_dst_tiles} = {total_tiles}")

    url = "http://127.0.0.1:8080/table/stream"

    print(f"Sending request...")
    start = time.perf_counter()

    try:
        # Use a very long timeout for large matrices
        resp = requests.post(url, json=payload, stream=True, timeout=3600)

        if resp.status_code != 200:
            print(f"Error: {resp.status_code} - {resp.text[:500]}")
            return

        # Read and parse Arrow IPC streams as they arrive
        first_byte_time = None
        last_report = start

        # Read all data first (chunked transfer)
        chunks = []
        for chunk in resp.iter_content(chunk_size=64*1024):
            if first_byte_time is None:
                first_byte_time = time.perf_counter()
                print(f"First byte received after {first_byte_time - start:.2f}s")
            chunks.append(chunk)

            # Progress report every 10 seconds
            now = time.perf_counter()
            if now - last_report > 10:
                mb = sum(len(c) for c in chunks) / 1024 / 1024
                print(f"  ... received {mb:.1f} MB so far ({now - start:.1f}s)")
                last_report = now

        end = time.perf_counter()
        total_data = b''.join(chunks)
        print(f"Total data received: {len(total_data):,} bytes ({len(total_data)/1024/1024:.1f} MB)")

        # Parse all concatenated IPC streams
        print("Parsing Arrow data...")
        tile_count, total_distances, reachable_count = parse_concatenated_ipc_streams(total_data)

        elapsed = end - start
        throughput = total_distances / elapsed if elapsed > 0 else 0
        reachable_pct = 100 * reachable_count / total_distances if total_distances > 0 else 0

        print(f"\n--- Results ---")
        print(f"Total tiles: {tile_count}")
        print(f"Total distances: {total_distances:,}")
        print(f"Reachable (estimated): {reachable_count:,} ({reachable_pct:.1f}%)")
        print(f"Total time: {elapsed:.2f}s")
        print(f"Throughput: {throughput:,.0f} distances/sec")

    except requests.exceptions.Timeout:
        print("Request timed out after 3600s")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    random.seed(42)

    # Test sizes
    if len(sys.argv) > 1:
        if sys.argv[1] == "--large":
            sizes = [(10000, 10000), (50000, 50000)]
        elif sys.argv[1] == "--50k":
            sizes = [(50000, 50000)]
        else:
            sizes = [(10000, 10000)]
    else:
        sizes = [(10000, 10000)]

    for n_src, n_dst in sizes:
        run_benchmark(n_src, n_dst)
