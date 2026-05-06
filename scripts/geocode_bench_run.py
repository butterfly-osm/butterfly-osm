#!/usr/bin/env python3
"""Wrapper around bench/geocode/bench.py that produces a clean
summary.json comparing butterfly (with the chosen parser/model) against
the local Nominatim instance.

Computes top-1 and top-5 accuracy at a configurable distance threshold
(default 100 m) plus p50/p95/p99 latency. The base bench script
already records `topk` per row, which is all we need.

Usage:
    python3 scripts/geocode_bench_run.py \
        --queries bench/geocode/queries/belgium.tsv \
        --output-dir bench/geocode/results/2026-05-06-gpu-prod \
        --butterfly-base-url http://localhost:3003 \
        --nominatim-base-url http://localhost:8080 \
        --concurrency 4 \
        --limit 5
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
import subprocess
import sys
from pathlib import Path


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6_371_000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = (math.sin(dphi / 2) ** 2
         + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2)
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def run_bench(repo: Path, engine: str, base_url: str, queries: Path,
              concurrency: int, limit: int, output: Path,
              qps_cap: int) -> Path:
    cmd = [
        sys.executable,
        str(repo / "bench" / "geocode" / "bench.py"),
        "--engine", engine,
        "--queries", str(queries),
        "--concurrency", str(concurrency),
        "--output", str(output),
        "--base-url", base_url,
        "--limit", str(limit),
        "--qps-cap", str(qps_cap),
    ]
    print(f"[bench] running {' '.join(cmd)}")
    result = subprocess.run(cmd)
    if result.returncode != 0:
        raise SystemExit(f"bench.py failed with code {result.returncode}")
    # bench.py writes <output>/<engine>-c<concurrency>.jsonl
    # plus <output>/<engine>-summary.md (one for the whole run).
    out_path = output / f"{engine}-c{concurrency}.jsonl"
    if not out_path.exists():
        raise SystemExit(f"missing expected bench output {out_path}")
    return out_path


def load_jsonl(path: Path) -> list[dict]:
    rows = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def summarise(rows: list[dict], radius_m: float) -> dict:
    n_total = len(rows)
    n_top1 = 0
    n_top5 = 0
    n_no_result = 0
    latencies = []
    for r in rows:
        if r.get("error") and r["error"] not in (None, ""):
            n_no_result += 1
        latencies.append(r.get("latency_ms", 0.0))
        gold_lat = r.get("gold_lat")
        gold_lon = r.get("gold_lon")
        topk = r.get("topk") or []
        if gold_lat is None or gold_lon is None:
            continue
        if not topk:
            continue
        # top-1 hit
        d1 = haversine_m(gold_lat, gold_lon, topk[0][0], topk[0][1])
        if d1 <= radius_m:
            n_top1 += 1
        # top-5 hit (any of top 5 within radius)
        for lat, lon in topk[:5]:
            if haversine_m(gold_lat, gold_lon, lat, lon) <= radius_m:
                n_top5 += 1
                break
    latencies.sort()
    def pct(p: float) -> float:
        if not latencies:
            return float("nan")
        i = max(0, min(len(latencies) - 1, int(round(p / 100 * (len(latencies) - 1)))))
        return latencies[i]
    return {
        "n_total": n_total,
        "n_no_result": n_no_result,
        "top1_recall": n_top1 / n_total if n_total else float("nan"),
        "top5_recall": n_top5 / n_total if n_total else float("nan"),
        "p50_ms": pct(50),
        "p95_ms": pct(95),
        "p99_ms": pct(99),
        "mean_ms": statistics.fmean(latencies) if latencies else float("nan"),
        "radius_m": radius_m,
    }


def attach_gold(rows: list[dict], queries_path: Path) -> list[dict]:
    """The bench.py output stores query_id + topk; gold lat/lon must be
    pulled from the query TSV again."""
    gold = {}
    import csv
    with open(queries_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            gold[row["query_id"]] = (
                float(row["gold_lat"]),
                float(row["gold_lon"]),
            )
    out = []
    for r in rows:
        qid = r.get("query_id")
        if qid in gold:
            r["gold_lat"], r["gold_lon"] = gold[qid]
        out.append(r)
    return out


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--queries", required=True)
    p.add_argument("--output-dir", required=True)
    p.add_argument("--butterfly-base-url", default="http://localhost:3003")
    p.add_argument("--nominatim-base-url", default="http://localhost:8080")
    p.add_argument("--concurrency", type=int, default=4)
    p.add_argument("--limit", type=int, default=5)
    p.add_argument("--qps-cap-butterfly", type=int, default=20)
    p.add_argument("--qps-cap-nominatim", type=int, default=0)
    p.add_argument("--radius-m", type=float, default=100.0)
    p.add_argument("--skip-nominatim", action="store_true",
                   help="Skip the Nominatim baseline run.")
    p.add_argument("--skip-butterfly", action="store_true",
                   help="Skip the butterfly run.")
    p.add_argument("--label", default="2026-05-06-gpu-prod",
                   help="Label written into summary.json.")
    args = p.parse_args()

    repo = Path(__file__).resolve().parents[1]
    queries = Path(args.queries).resolve()
    out_dir = Path(args.output_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    summary = {
        "label": args.label,
        "queries": str(queries),
        "concurrency": args.concurrency,
        "limit": args.limit,
        "radius_m": args.radius_m,
    }

    if not args.skip_butterfly:
        bf_jsonl = run_bench(repo, "butterfly", args.butterfly_base_url,
                             queries, args.concurrency, args.limit, out_dir,
                             args.qps_cap_butterfly)
        rows = attach_gold(load_jsonl(bf_jsonl), queries)
        summary["butterfly"] = summarise(rows, args.radius_m)
        # Re-write enriched JSONL.
        with open(bf_jsonl, "w") as f:
            for r in rows:
                f.write(json.dumps(r) + "\n")

    if not args.skip_nominatim:
        nom_jsonl = run_bench(repo, "nominatim", args.nominatim_base_url,
                              queries, args.concurrency, args.limit, out_dir,
                              args.qps_cap_nominatim)
        rows = attach_gold(load_jsonl(nom_jsonl), queries)
        summary["nominatim"] = summarise(rows, args.radius_m)
        with open(nom_jsonl, "w") as f:
            for r in rows:
                f.write(json.dumps(r) + "\n")

    summary_path = out_dir / "summary.json"
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2)
    print(f"\n[bench] summary written → {summary_path}")
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
