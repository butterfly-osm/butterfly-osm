#!/usr/bin/env python3
"""Multi-country geocode benchmark.

Walks the per-country query TSVs in `bench/geocode/queries/` and runs
each through:
  - Butterfly geocoder (multi-shard server on `--butterfly-base`)
  - Nominatim (optional, if `--nominatim-base` is reachable)

Emits per-country summary.json + a top-level summary.md aggregating
recall@1 / recall@5 / latency p50/p95 across all countries, and a
per-country breakdown comparing the two engines.

Usage:
    python3 scripts/geocode_multi_country_bench.py \
        --queries-dir bench/geocode/queries \
        --out-dir bench/geocode/results/2026-05-06-multi-country \
        --butterfly-base http://localhost:31000 \
        --nominatim-base http://localhost:8080 \
        --concurrency 4 --limit 5 --radius-m 100
"""
import argparse
import json
import math
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

try:
    import requests
except ImportError:
    print("error: pip install requests", file=sys.stderr)
    sys.exit(1)


COUNTRIES = {
    # iso2 -> tsv basename
    "AT": "austria",
    "AU": "australia",
    "BE": "belgium",
    "BR": "brazil",
    "CH": "switzerland",
    "DE": "germany",
    "ES": "spain",
    "FR": "france",
    "GB": "great-britain",
    "IN": "india",
    "IT": "italy",
    "JP": "japan",
    "LU": "luxembourg",
    "NL": "netherlands",
    "US": "united-states",
}


def haversine_m(lat1, lon1, lat2, lon2):
    R = 6_371_000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlam / 2) ** 2
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def load_queries(path):
    out = []
    with open(path) as f:
        next(f)  # skip header
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 5:
                continue
            try:
                out.append({
                    "query_id": parts[0],
                    "query_text": parts[1],
                    "gold_lat": float(parts[2]),
                    "gold_lon": float(parts[3]),
                    "quality_class": parts[4],
                })
            except ValueError:
                continue
    return out


def run_butterfly(session, query_text, country, base_url, limit):
    t0 = time.perf_counter()
    try:
        r = session.get(
            f"{base_url}/geocode",
            params={"q": query_text, "country": country, "limit": limit},
            timeout=10.0,
        )
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        return {"latency_ms": (time.perf_counter() - t0) * 1000.0, "topk": [], "error": str(e)}
    latency_ms = (time.perf_counter() - t0) * 1000.0
    results = data.get("results", []) or []
    topk = []
    for r in results[:limit]:
        topk.append({"lat": r.get("lat"), "lon": r.get("lon")})
    return {"latency_ms": latency_ms, "topk": topk}


def run_nominatim(session, query_text, country, base_url, limit):
    t0 = time.perf_counter()
    try:
        params = {"q": query_text, "format": "json", "limit": limit, "addressdetails": 0}
        if country:
            params["countrycodes"] = country.lower()
        r = session.get(f"{base_url}/search", params=params, timeout=15.0)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        return {"latency_ms": (time.perf_counter() - t0) * 1000.0, "topk": [], "error": str(e)}
    latency_ms = (time.perf_counter() - t0) * 1000.0
    topk = []
    for r in data[:limit]:
        try:
            topk.append({"lat": float(r["lat"]), "lon": float(r["lon"])})
        except Exception:
            continue
    return {"latency_ms": latency_ms, "topk": topk}


def evaluate(rows, queries, radius_m):
    n = len(queries)
    n_no_result = 0
    top1_ok = 0
    top5_ok = 0
    latencies = []
    for q, r in zip(queries, rows):
        latencies.append(r["latency_ms"])
        topk = r["topk"]
        if not topk:
            n_no_result += 1
            continue
        if topk[0].get("lat") is None:
            n_no_result += 1
            continue
        d1 = haversine_m(q["gold_lat"], q["gold_lon"], topk[0]["lat"], topk[0]["lon"])
        if d1 <= radius_m:
            top1_ok += 1
        # top-5
        for k in topk[:5]:
            if k.get("lat") is None:
                continue
            d = haversine_m(q["gold_lat"], q["gold_lon"], k["lat"], k["lon"])
            if d <= radius_m:
                top5_ok += 1
                break
    p50 = statistics.median(latencies) if latencies else 0.0
    sl = sorted(latencies)
    p95 = sl[int(0.95 * len(sl))] if sl else 0.0
    p99 = sl[int(0.99 * len(sl))] if sl else 0.0
    mean = statistics.mean(latencies) if latencies else 0.0
    return {
        "n_total": n,
        "n_no_result": n_no_result,
        "top1_recall": top1_ok / n if n else 0.0,
        "top5_recall": top5_ok / n if n else 0.0,
        "p50_ms": p50, "p95_ms": p95, "p99_ms": p99, "mean_ms": mean,
    }


def bench_one(iso, queries, base_url, kind, concurrency, limit):
    """Run all queries for one country against one engine."""
    sess = requests.Session()
    rows = [None] * len(queries)
    fn = run_butterfly if kind == "butterfly" else run_nominatim
    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        futures = {ex.submit(fn, sess, q["query_text"], iso, base_url, limit): i for i, q in enumerate(queries)}
        for f in futures:
            i = futures[f]
            rows[i] = f.result()
    return rows


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--queries-dir", required=True)
    p.add_argument("--out-dir", required=True)
    p.add_argument("--butterfly-base", default="http://localhost:31000")
    p.add_argument("--nominatim-base", default=None,
                   help="Optional. Skip Nominatim if absent.")
    p.add_argument("--countries", default=",".join(COUNTRIES.keys()),
                   help="Comma-separated ISO2 codes to bench (default all 15)")
    p.add_argument("--concurrency", type=int, default=4)
    p.add_argument("--limit", type=int, default=5)
    p.add_argument("--radius-m", type=float, default=100.0)
    args = p.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    queries_dir = Path(args.queries_dir)

    iso_list = [s.strip().upper() for s in args.countries.split(",") if s.strip()]
    summary = {
        "label": out_dir.name,
        "concurrency": args.concurrency,
        "limit": args.limit,
        "radius_m": args.radius_m,
        "butterfly_base": args.butterfly_base,
        "nominatim_base": args.nominatim_base,
        "per_country": {},
    }

    nominatim_avail = False
    if args.nominatim_base:
        try:
            requests.get(f"{args.nominatim_base}/search", params={"q": "test", "format": "json"}, timeout=3.0)
            nominatim_avail = True
        except Exception:
            print(f"[bench] nominatim at {args.nominatim_base} unreachable — skipping", file=sys.stderr)

    for iso in iso_list:
        tsv_name = COUNTRIES.get(iso)
        if not tsv_name:
            print(f"[bench] skip unknown country {iso}", file=sys.stderr)
            continue
        tsv_path = queries_dir / f"{tsv_name}.tsv"
        if not tsv_path.exists():
            print(f"[bench] skip {iso}: no queries at {tsv_path}", file=sys.stderr)
            continue
        queries = load_queries(tsv_path)
        if not queries:
            print(f"[bench] skip {iso}: empty queries", file=sys.stderr)
            continue

        print(f"[bench] {iso}: {len(queries)} queries", file=sys.stderr)
        country_dir = out_dir / iso
        country_dir.mkdir(parents=True, exist_ok=True)

        # Butterfly
        bf_rows = bench_one(iso, queries, args.butterfly_base, "butterfly", args.concurrency, args.limit)
        bf_summary = evaluate(bf_rows, queries, args.radius_m)
        with open(country_dir / "butterfly-rows.jsonl", "w") as f:
            for q, r in zip(queries, bf_rows):
                f.write(json.dumps({"q": q, "r": r}) + "\n")
        print(f"[bench]   BF top1={bf_summary['top1_recall']:.3f} top5={bf_summary['top5_recall']:.3f} p50={bf_summary['p50_ms']:.1f}ms", file=sys.stderr)

        # Nominatim (optional)
        nom_summary = None
        if nominatim_avail:
            nom_rows = bench_one(iso, queries, args.nominatim_base, "nominatim", args.concurrency, args.limit)
            nom_summary = evaluate(nom_rows, queries, args.radius_m)
            with open(country_dir / "nominatim-rows.jsonl", "w") as f:
                for q, r in zip(queries, nom_rows):
                    f.write(json.dumps({"q": q, "r": r}) + "\n")
            print(f"[bench]   Nom top1={nom_summary['top1_recall']:.3f} top5={nom_summary['top5_recall']:.3f} p50={nom_summary['p50_ms']:.1f}ms", file=sys.stderr)

        country_summary = {"butterfly": bf_summary, "nominatim": nom_summary}
        with open(country_dir / "summary.json", "w") as f:
            json.dump(country_summary, f, indent=2)
        summary["per_country"][iso] = country_summary

    # Top-level summary
    with open(out_dir / "summary.json", "w") as f:
        json.dump(summary, f, indent=2)

    # Markdown summary
    lines = []
    lines.append(f"# Multi-country geocode bench")
    lines.append("")
    lines.append(f"- Label: {summary['label']}")
    lines.append(f"- Concurrency: {summary['concurrency']}, limit: {summary['limit']}, radius: {summary['radius_m']}m")
    lines.append(f"- Butterfly: {summary['butterfly_base']}")
    if nominatim_avail:
        lines.append(f"- Nominatim: {summary['nominatim_base']}")
    else:
        lines.append(f"- Nominatim: not used")
    lines.append("")
    lines.append("## Per-country recall and latency")
    lines.append("")
    if nominatim_avail:
        lines.append("| ISO | n  | BF top1 | Nom top1 | BF top5 | Nom top5 | BF p50 (ms) | Nom p50 (ms) |")
        lines.append("|:----|:---|:--------|:---------|:--------|:---------|:------------|:-------------|")
        for iso, c in summary["per_country"].items():
            bf, nom = c["butterfly"], c["nominatim"]
            lines.append(
                f"| {iso} | {bf['n_total']} | {bf['top1_recall']:.3f} | {nom['top1_recall']:.3f} "
                f"| {bf['top5_recall']:.3f} | {nom['top5_recall']:.3f} "
                f"| {bf['p50_ms']:.1f} | {nom['p50_ms']:.1f} |"
            )
    else:
        lines.append("| ISO | n  | BF top1 | BF top5 | BF p50 (ms) |")
        lines.append("|:----|:---|:--------|:--------|:------------|")
        for iso, c in summary["per_country"].items():
            bf = c["butterfly"]
            lines.append(
                f"| {iso} | {bf['n_total']} | {bf['top1_recall']:.3f} "
                f"| {bf['top5_recall']:.3f} | {bf['p50_ms']:.1f} |"
            )

    # Aggregate
    bf_top1 = [c["butterfly"]["top1_recall"] for c in summary["per_country"].values()]
    bf_top5 = [c["butterfly"]["top5_recall"] for c in summary["per_country"].values()]
    if bf_top1:
        lines.append("")
        lines.append(f"**Mean BF top1 across countries: {statistics.mean(bf_top1):.3f}**")
        lines.append(f"**Mean BF top5 across countries: {statistics.mean(bf_top5):.3f}**")
        if nominatim_avail:
            nom_top1 = [c["nominatim"]["top1_recall"] for c in summary["per_country"].values()]
            nom_top5 = [c["nominatim"]["top5_recall"] for c in summary["per_country"].values()]
            lines.append(f"**Mean Nom top1 across countries: {statistics.mean(nom_top1):.3f}**")
            lines.append(f"**Mean Nom top5 across countries: {statistics.mean(nom_top5):.3f}**")

    with open(out_dir / "summary.md", "w") as f:
        f.write("\n".join(lines) + "\n")
    print(f"[bench] wrote {out_dir/'summary.json'} + {out_dir/'summary.md'}", file=sys.stderr)


if __name__ == "__main__":
    main()
