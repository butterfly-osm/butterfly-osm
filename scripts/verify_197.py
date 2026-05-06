#!/usr/bin/env python3
"""Verification script for fix #197 — directional snap asymmetry.

Re-runs every car pair from the 2026-05-06 correctness sweep where
butterfly returned 404 ("No route found") and OSRM successfully
routed. Hits the local Butterfly /route on the configured port and
reports how many of the previously-failing pairs now succeed.

Usage:
    python3 scripts/verify_197.py [--port 3010] [--out PATH]
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import urllib.error
import urllib.request


def load_failing_pairs(path: Path) -> list[dict]:
    out: list[dict] = []
    with path.open() as fh:
        for line in fh:
            r = json.loads(line)
            if r.get("butterfly_distance_m") is None and r.get("osrm_distance_m") is not None:
                out.append(r)
    return out


def query(port: int, pair: dict, timeout: float = 30.0) -> tuple[bool, dict | None]:
    url = (
        f"http://localhost:{port}/route?"
        f"src_lon={pair['src_lon']}&src_lat={pair['src_lat']}"
        f"&dst_lon={pair['dst_lon']}&dst_lat={pair['dst_lat']}"
        f"&mode=car"
    )
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            data = json.loads(resp.read())
            # Butterfly /route returns {distance_m, duration_s, geometry, ...}
            # at the top level on success; an {error: ...} body on failure.
            if "distance_m" in data:
                return True, {
                    "distance": data.get("distance_m"),
                    "duration": data.get("duration_s"),
                }
            if "routes" in data and data.get("routes"):
                # Defensive: support OSRM-shape responses if they ever
                # ship.
                return True, {
                    "distance": data["routes"][0].get("distance"),
                    "duration": data["routes"][0].get("duration"),
                }
            return False, {"error": data.get("error", "no routes / no distance_m")}
    except urllib.error.HTTPError as e:
        try:
            body = json.loads(e.read())
            return False, {"error": body.get("error", str(e)), "status": e.code}
        except Exception:
            return False, {"error": str(e), "status": e.code}
    except Exception as e:
        return False, {"error": str(e)}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=3010)
    ap.add_argument(
        "--results",
        default="bench/route/results/correctness-sweep-2026-05-06/results-car.jsonl",
    )
    ap.add_argument(
        "--out",
        default="bench/route/results/correctness-sweep-2026-05-06/197-verification.json",
    )
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument(
        "--threshold",
        type=float,
        default=98.0,
        help="Fix-rate percent threshold for exit code 0; below this, exit 1. "
        "Default 98.0 reflects the residual ~1.3%% of pairs that snap >5km from "
        "the mode-filtered SCC (true topology disconnect, not snap asymmetry).",
    )
    args = ap.parse_args()

    pairs_path = Path(args.results)
    if not pairs_path.exists():
        print(f"Results file not found: {pairs_path}", file=sys.stderr)
        return 2

    failing = load_failing_pairs(pairs_path)
    print(f"Loaded {len(failing)} previously-failing car pairs", file=sys.stderr)

    # Sanity check the server.
    try:
        with urllib.request.urlopen(f"http://localhost:{args.port}/health", timeout=5) as r:
            _ = r.read()
    except Exception as e:
        print(f"Server health check failed on port {args.port}: {e}", file=sys.stderr)
        return 2

    successes: list[dict] = []
    still_failing: list[dict] = []

    started = time.time()

    def run(p: dict) -> tuple[dict, bool, dict | None]:
        ok, info = query(args.port, p)
        return p, ok, info

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futs = [pool.submit(run, p) for p in failing]
        last_log = time.time()
        for i, fut in enumerate(as_completed(futs), 1):
            p, ok, info = fut.result()
            if ok:
                successes.append({**p, "fixed_distance_m": info["distance"]})
            else:
                still_failing.append({**p, "still_error": info})
            if time.time() - last_log > 5:
                pct = (len(successes) * 100.0 / max(1, i))
                print(
                    f"  progress: {i}/{len(failing)} pairs, fixed={len(successes)} ({pct:.1f}%)",
                    file=sys.stderr,
                )
                last_log = time.time()

    elapsed = time.time() - started
    n_total = len(failing)
    n_fixed = len(successes)
    n_still = len(still_failing)
    pct = (n_fixed * 100.0 / max(1, n_total))

    summary = {
        "issue": 197,
        "results_file": str(pairs_path),
        "port": args.port,
        "n_pairs_previously_failing": n_total,
        "n_fixed": n_fixed,
        "n_still_failing": n_still,
        "fix_rate_pct": round(pct, 2),
        "elapsed_s": round(elapsed, 1),
        "still_failing_sample": still_failing[:25],
    }

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))
    print(f"\nWrote {out_path}", file=sys.stderr)

    return 0 if pct >= args.threshold else 1


if __name__ == "__main__":
    raise SystemExit(main())
