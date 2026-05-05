#!/usr/bin/env python3
"""
Geocode benchmark driver.

Usage:
    python bench.py --engine nominatim --queries queries/belgium.tsv \
        --concurrency 1,4,16 --output results/

Engines:
    - nominatim: hits http://localhost:8080/search (Nominatim's API)
    - butterfly: hits http://localhost:3001/geocode (future, not built yet)
    - photon: hits http://localhost:2322/api (future)

Metrics:
    - p50, p95, p99 latency
    - throughput (queries/sec) at concurrency 1, 4, 16
    - recall@1: fraction of queries where the top-1 result is within
      100 m of the gold coordinate
    - top-1 distance distribution: median and p95 distance to gold

Output:
    - results/<engine>-<concurrency>.jsonl: one row per query with
      (query_id, latency_ms, top1_lat, top1_lon, distance_to_gold_m)
    - results/<engine>-summary.md: pretty markdown summary
"""

import argparse
import csv
import json
import math
import os
import statistics
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

try:
    import requests  # type: ignore
except ImportError:
    print("error: pip install -r requirements.txt", file=sys.stderr)
    sys.exit(1)


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Distance between two WGS84 points in meters."""
    R = 6_371_000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def load_queries(path: Path):
    """Load TSV of (query_id, query_text, gold_lat, gold_lon, quality_class)."""
    rows = []
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            try:
                rows.append({
                    "query_id": row["query_id"],
                    "query_text": row["query_text"],
                    "gold_lat": float(row["gold_lat"]),
                    "gold_lon": float(row["gold_lon"]),
                    "quality_class": row["quality_class"],
                })
            except (KeyError, ValueError) as e:
                print(f"skipping malformed row: {row} ({e})", file=sys.stderr)
    return rows


def query_nominatim(session: requests.Session, query_text: str, base_url: str) -> dict:
    """Send one query to Nominatim. Returns dict with latency, top1 coords."""
    t0 = time.perf_counter()
    try:
        r = session.get(
            f"{base_url}/search",
            params={"q": query_text, "format": "json", "limit": 1, "addressdetails": 0},
            timeout=10.0,
        )
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        return {
            "latency_ms": (time.perf_counter() - t0) * 1000.0,
            "top1_lat": None,
            "top1_lon": None,
            "error": str(e),
        }
    latency_ms = (time.perf_counter() - t0) * 1000.0
    if not data:
        return {"latency_ms": latency_ms, "top1_lat": None, "top1_lon": None, "error": "no_result"}
    return {
        "latency_ms": latency_ms,
        "top1_lat": float(data[0]["lat"]),
        "top1_lon": float(data[0]["lon"]),
        "error": None,
    }


def query_butterfly(session: requests.Session, query_text: str, base_url: str) -> dict:
    """Send one query to butterfly-geocode's REST API. Returns dict with
    latency, top1 coords, and topk (top-5 lat/lon pairs for diagnostic
    parity with the previously-shipped butterfly-run/ files).
    """
    t0 = time.perf_counter()
    try:
        r = session.get(
            f"{base_url}/geocode",
            params={"q": query_text, "limit": 5},
            timeout=10.0,
        )
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        return {
            "latency_ms": (time.perf_counter() - t0) * 1000.0,
            "top1_lat": None,
            "top1_lon": None,
            "topk": [],
            "error": str(e),
        }
    latency_ms = (time.perf_counter() - t0) * 1000.0
    results = data.get("results") or []
    if not results:
        return {
            "latency_ms": latency_ms,
            "top1_lat": None,
            "top1_lon": None,
            "topk": [],
            "error": "no_result",
        }
    topk = [[float(it["lat"]), float(it["lon"])] for it in results[:5]]
    return {
        "latency_ms": latency_ms,
        "top1_lat": topk[0][0],
        "top1_lon": topk[0][1],
        "topk": topk,
        "error": None,
    }


def run_one_concurrency(engine_fn, queries, concurrency: int, base_url: str, qps_cap: float = 0.0):
    """Run all queries through `engine_fn` at the given concurrency.

    `qps_cap > 0` paces submission to that global QPS — needed when
    butterfly-geocode's admission layer enforces a per-IP token-bucket
    that would otherwise reject the bench traffic with 429s. Set to
    0 to disable pacing.
    """
    results = []
    session_local = threading.local()
    rate_lock = threading.Lock()
    next_slot = [time.perf_counter()]
    interval = 1.0 / qps_cap if qps_cap > 0 else 0.0

    def get_session():
        s = getattr(session_local, "s", None)
        if s is None:
            s = requests.Session()
            session_local.s = s
        return s

    def maybe_throttle():
        if interval <= 0.0:
            return
        # Block until our slot opens up. Holding the lock across the
        # sleep is intentional: it serialises submission so concurrency
        # > 1 still respects the global pacing cap (admission's
        # token bucket is per-IP, not per-connection).
        with rate_lock:
            now = time.perf_counter()
            slot = next_slot[0]
            if now < slot:
                wait_for = slot - now
                time.sleep(wait_for)
                now = time.perf_counter()
            next_slot[0] = now + interval

    def task(q):
        maybe_throttle()
        s = get_session()
        out = engine_fn(s, q["query_text"], base_url)
        out["query_id"] = q["query_id"]
        out["query_text"] = q["query_text"]
        out["quality_class"] = q["quality_class"]
        if out.get("top1_lat") is not None and out.get("top1_lon") is not None:
            out["distance_to_gold_m"] = haversine_m(
                q["gold_lat"], q["gold_lon"], out["top1_lat"], out["top1_lon"]
            )
        else:
            out["distance_to_gold_m"] = None
        topk = out.get("topk")
        if topk:
            out["best_distance_top5_m"] = min(
                haversine_m(q["gold_lat"], q["gold_lon"], lat, lon)
                for (lat, lon) in topk
            )
        return out

    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = [pool.submit(task, q) for q in queries]
        for fut in as_completed(futures):
            results.append(fut.result())
    elapsed = time.perf_counter() - t0
    throughput = len(queries) / elapsed if elapsed > 0 else 0.0
    return results, throughput


def summarize(results):
    """Compute recall@1, distance percentiles, latency percentiles."""
    successful = [r for r in results if r.get("error") is None]
    n = len(results)
    n_ok = len(successful)
    latencies = [r["latency_ms"] for r in successful]
    distances = [r["distance_to_gold_m"] for r in successful if r["distance_to_gold_m"] is not None]
    recall_at_1 = sum(1 for d in distances if d <= 100.0) / n if n > 0 else 0.0

    def pct(xs, p):
        if not xs:
            return None
        xs = sorted(xs)
        idx = max(0, min(len(xs) - 1, int(round(p / 100.0 * (len(xs) - 1)))))
        return xs[idx]

    return {
        "n_queries": n,
        "n_successful": n_ok,
        "recall_at_1_100m": recall_at_1,
        "latency_ms_p50": pct(latencies, 50),
        "latency_ms_p95": pct(latencies, 95),
        "latency_ms_p99": pct(latencies, 99),
        "latency_ms_mean": statistics.mean(latencies) if latencies else None,
        "distance_m_p50": pct(distances, 50),
        "distance_m_p95": pct(distances, 95),
    }


ENGINES = {
    "nominatim": (query_nominatim, "http://localhost:8080"),
    "butterfly": (query_butterfly, "http://localhost:3001"),
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--engine", choices=list(ENGINES.keys()), default="nominatim")
    ap.add_argument("--queries", type=Path, default=Path("queries/belgium.tsv"))
    ap.add_argument("--concurrency", default="1,4,16")
    ap.add_argument("--output", type=Path, default=Path("results"))
    ap.add_argument("--base-url", default=None, help="override default base URL for engine")
    ap.add_argument(
        "--qps-cap",
        type=float,
        default=0.0,
        help=(
            "client-side pacing cap (queries per second). 0 disables pacing. "
            "Required for the butterfly engine because the admission layer "
            "applies a per-IP token bucket (default 25/s steady, 50 burst); "
            "set to 20 to stay under it."
        ),
    )
    args = ap.parse_args()

    args.output.mkdir(parents=True, exist_ok=True)
    queries = load_queries(args.queries)
    if not queries:
        print(f"error: no queries loaded from {args.queries}", file=sys.stderr)
        sys.exit(1)
    print(f"loaded {len(queries)} queries from {args.queries}")

    engine_fn, default_base_url = ENGINES[args.engine]
    base_url = args.base_url or default_base_url

    summary_md = [f"# {args.engine} bench results\n", f"Queries: {len(queries)}", ""]
    for c in [int(x) for x in args.concurrency.split(",")]:
        print(f"\n=== concurrency={c} ===")
        results, throughput = run_one_concurrency(
            engine_fn, queries, c, base_url, qps_cap=args.qps_cap
        )
        s = summarize(results)
        s["throughput_qps"] = throughput
        s["concurrency"] = c
        out_path = args.output / f"{args.engine}-c{c}.jsonl"
        with out_path.open("w", encoding="utf-8") as f:
            for r in results:
                f.write(json.dumps(r) + "\n")
        print(f"  wrote {out_path}")
        def _fmt(v):
            return f"{v:.1f}" if isinstance(v, (int, float)) else "n/a"

        print(f"  recall@1 (100m): {s['recall_at_1_100m']:.3f}")
        print(f"  throughput: {throughput:.1f} qps")
        print(
            f"  p50/p95/p99: {_fmt(s['latency_ms_p50'])} / {_fmt(s['latency_ms_p95'])} "
            f"/ {_fmt(s['latency_ms_p99'])} ms"
        )
        summary_md.append(
            f"## concurrency={c}\n"
            f"- throughput: {throughput:.1f} qps\n"
            f"- recall@1 (100 m): {s['recall_at_1_100m']:.3f}\n"
            f"- latency p50 / p95 / p99: {_fmt(s['latency_ms_p50'])} / "
            f"{_fmt(s['latency_ms_p95'])} / {_fmt(s['latency_ms_p99'])} ms\n"
            f"- distance p50 / p95: {s['distance_m_p50']} / {s['distance_m_p95']} m\n"
        )
    summary_path = args.output / f"{args.engine}-summary.md"
    summary_path.write_text("\n".join(summary_md), encoding="utf-8")
    print(f"\nsummary: {summary_path}")


if __name__ == "__main__":
    main()
