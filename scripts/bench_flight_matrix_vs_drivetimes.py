#!/usr/bin/env python3
"""
Bench butterfly-route Flight matrix vs drivetimes (libosrm-via-Flight) matrix.

Both speak Arrow Flight; ticket format differs.

  - butterfly-route: ticket = b'matrix:{"mode":"car","sources":[[lon,lat]...],"destinations":[[lon,lat]...]}'
  - drivetimes:      ticket = b'matrix:car:{"sources":[[lon,lat]...],"targets":[[lon,lat]...]}'

Methodology: 10 runs per (engine, size), report sorted min/p50/max
(deduplicates client-side jitter). Same random points (seed=42) for both
engines per size, so output sizes are directly comparable.

Usage:
  python3 scripts/bench_flight_matrix_vs_drivetimes.py [--bf-port 13002] [--dt-port 50051]
"""

import argparse
import json
import random
import sys
import time

try:
    import pyarrow.flight as flight
except ImportError:
    print("ERR  pyarrow not installed. `pip install pyarrow`", file=sys.stderr)
    sys.exit(2)

# Tight Belgium bbox — land only, avoids the North Sea coast where
# random points hit water and snap fails. Both engines compute every
# cell so row counts match.
LON_MIN, LON_MAX = 3.6, 5.6
LAT_MIN, LAT_MAX = 50.2, 51.1


def random_pts(n, seed=42):
    rng = random.Random(seed)
    return [
        [round(rng.uniform(LON_MIN, LON_MAX), 6),
         round(rng.uniform(LAT_MIN, LAT_MAX), 6)]
        for _ in range(n)
    ]


def call_butterfly(client, pts):
    """Butterfly Flight matrix — ticket = 'matrix:car:{json}'.
    See route/src/server/flight.rs::parse_ticket — server splits on the
    first two colons; the profile is encoded in the ticket, NOT in JSON.
    """
    body = json.dumps({"sources": pts, "destinations": pts})
    ticket = flight.Ticket(("matrix:car:" + body).encode())
    t0 = time.perf_counter_ns()
    tbl = client.do_get(ticket).read_all()
    dt_ms = (time.perf_counter_ns() - t0) / 1_000_000
    return dt_ms, tbl.num_rows


def call_drivetimes(client, pts):
    """drivetimes Flight matrix — ticket = matrix:{profile}:{json}."""
    body = json.dumps({"sources": pts, "targets": pts})
    ticket = flight.Ticket(("matrix:car:" + body).encode())
    t0 = time.perf_counter_ns()
    tbl = client.do_get(ticket).read_all()
    dt_ms = (time.perf_counter_ns() - t0) / 1_000_000
    return dt_ms, tbl.num_rows


def run_one(label, fn, pts, warm=3, runs=10):
    for _ in range(warm):
        try:
            fn(pts)
        except Exception:
            pass
    times = []
    rows = None
    for _ in range(runs):
        try:
            t, r = fn(pts)
            times.append(t)
            rows = r
        except Exception as e:
            return None, None, f"{type(e).__name__}: {e}"
    times.sort()
    p50 = times[len(times) // 2]
    return p50, rows, None


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--bf-port", type=int, default=13002,
                   help="butterfly-route gRPC Flight port")
    p.add_argument("--dt-port", type=int, default=50051,
                   help="drivetimes Flight port")
    p.add_argument("--sizes", type=int, nargs="+",
                   default=[10, 25, 50, 100, 250, 500, 1000])
    args = p.parse_args()

    bf = flight.connect(f"grpc://127.0.0.1:{args.bf_port}")
    dt = flight.connect(f"grpc://127.0.0.1:{args.dt_port}")

    print(f"Butterfly Flight matrix (mode=car, dur+dist) vs drivetimes (libosrm CH) Flight matrix")
    print(f"Belgium random points, seed=42, 10 runs sorted, p50 ms")
    print()
    print(f"{'size':>6} | {'Butterfly':>11} | {'drivetimes':>11} | {'ratio':>7} | rows")
    print(f"{'-'*6}-+-{'-'*11}-+-{'-'*11}-+-{'-'*7}-+------")

    for n in args.sizes:
        pts = random_pts(n)

        bf_p50, bf_rows, bf_err = run_one("butterfly", lambda p: call_butterfly(bf, p), pts)
        dt_p50, dt_rows, dt_err = run_one("drivetimes", lambda p: call_drivetimes(dt, p), pts)

        if bf_err or dt_err:
            print(f"{n}x{n}: butterfly={bf_err or 'ok'} drivetimes={dt_err or 'ok'}")
            continue

        ratio = dt_p50 / bf_p50  # >1 means Butterfly faster
        winner = "▲" if ratio > 1 else "▼" if ratio < 0.95 else "="
        print(f"{n:4}x{n:<4} | {bf_p50:9.1f}ms | {dt_p50:9.1f}ms | {ratio:5.2f}x {winner} | bf={bf_rows} dt={dt_rows}")


if __name__ == "__main__":
    main()
