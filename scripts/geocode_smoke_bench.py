#!/usr/bin/env python3
"""Smoke bench for the post-libpostal recall+rerank pipeline (#205).

Boots a fresh `butterfly-geocode serve` against a Belgium shard
sibling to a recall-fst sidecar, fires 100 fixture queries, and
reports:

  - recall@10 (gold record id appears in top-10)
  - rerank top-1 accuracy (gold record id is top-1)
  - p50 / p95 latency

Output is written to
`bench/geocode/results/2026-05-06-recall-rerank-fixture/summary.json`
so CI can diff it across changes.

Usage:

    python3 scripts/geocode_smoke_bench.py \\
        --shard geocode/regions/belgium.bfgs \\
        --rerank geocode/data/models/rerank-belgium-prod.gbdt \\
        --out bench/geocode/results/2026-05-06-recall-rerank-fixture
"""

import argparse
import json
import os
import random
import socket
import statistics
import subprocess
import sys
import time
import urllib.parse
import urllib.request


def find_free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def wait_until_ready(host: str, port: int, timeout_s: float = 30.0) -> bool:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(f"http://{host}:{port}/health", timeout=1.5) as r:
                if r.status == 200:
                    return True
        except Exception:
            pass
        time.sleep(0.25)
    return False


def fixture_queries() -> list[dict]:
    """100 hand-curated Belgium queries spanning Brussels, Antwerp, Gent,
    Liège, Brugge, Leuven, Charleroi, plus typo / abbreviation /
    dropped-field perturbations.

    Each entry is `{"q": "...", "lat": ..., "lon": ...}`. The bench
    measures top-1 accuracy by the great-circle distance between the
    top-1 result and the gold lat/lon (≤ 200 m = correct).
    """
    return [
        # ------- Brussels (canonical clean queries) --------
        {"q": "Rue de la Loi 16 1000 Bruxelles", "lat": 50.8467, "lon": 4.3673},
        {"q": "Avenue Louise 100 1050 Ixelles", "lat": 50.8323, "lon": 4.3690},
        {"q": "Boulevard Anspach 1 1000 Bruxelles", "lat": 50.848, "lon": 4.351},
        {"q": "Place Sainte-Catherine 1 1000 Bruxelles", "lat": 50.852, "lon": 4.351},
        {"q": "Rue Neuve 1 1000 Bruxelles", "lat": 50.851, "lon": 4.355},
        {"q": "Avenue de Tervueren 1 1040 Etterbeek", "lat": 50.838, "lon": 4.388},
        {"q": "Boulevard Adolphe Max 17 1000 Bruxelles", "lat": 50.852, "lon": 4.355},
        {"q": "Rue de la Régence 3 1000 Bruxelles", "lat": 50.842, "lon": 4.358},
        {"q": "Avenue de la Toison d'Or 5 1050 Ixelles", "lat": 50.838, "lon": 4.358},
        {"q": "Chaussée d'Ixelles 100 1050 Ixelles", "lat": 50.838, "lon": 4.367},
        # ------- Antwerp --------
        {"q": "Grote Markt 1 2000 Antwerpen", "lat": 51.221, "lon": 4.400},
        {"q": "Meir 50 2000 Antwerpen", "lat": 51.218, "lon": 4.408},
        {"q": "Hoogstraat 15 2000 Antwerpen", "lat": 51.220, "lon": 4.401},
        {"q": "De Keyserlei 1 2018 Antwerpen", "lat": 51.218, "lon": 4.420},
        {"q": "Sint-Jansvliet 1 2000 Antwerpen", "lat": 51.222, "lon": 4.397},
        {"q": "Lange Nieuwstraat 100 2000 Antwerpen", "lat": 51.221, "lon": 4.403},
        # ------- Gent --------
        {"q": "Korenmarkt 1 9000 Gent", "lat": 51.054, "lon": 3.724},
        {"q": "Vrijdagmarkt 1 9000 Gent", "lat": 51.058, "lon": 3.724},
        {"q": "Veldstraat 50 9000 Gent", "lat": 51.052, "lon": 3.724},
        {"q": "Sint-Baafsplein 1 9000 Gent", "lat": 51.053, "lon": 3.726},
        {"q": "Kortrijksesteenweg 100 9000 Gent", "lat": 51.040, "lon": 3.713},
        # ------- Liège --------
        {"q": "Place Saint-Lambert 1 4000 Liège", "lat": 50.645, "lon": 5.574},
        {"q": "Rue Léopold 1 4000 Liège", "lat": 50.642, "lon": 5.572},
        {"q": "Boulevard d'Avroy 1 4000 Liège", "lat": 50.638, "lon": 5.574},
        {"q": "Rue Saint-Gilles 1 4000 Liège", "lat": 50.640, "lon": 5.567},
        # ------- Brugge --------
        {"q": "Markt 1 8000 Brugge", "lat": 51.208, "lon": 3.224},
        {"q": "Burg 1 8000 Brugge", "lat": 51.208, "lon": 3.227},
        {"q": "Steenstraat 1 8000 Brugge", "lat": 51.207, "lon": 3.221},
        # ------- Leuven --------
        {"q": "Bondgenotenlaan 1 3000 Leuven", "lat": 50.881, "lon": 4.704},
        {"q": "Grote Markt 1 3000 Leuven", "lat": 50.879, "lon": 4.701},
        {"q": "Naamsestraat 1 3000 Leuven", "lat": 50.877, "lon": 4.701},
        # ------- Charleroi --------
        {"q": "Place Charles II 1 6000 Charleroi", "lat": 50.412, "lon": 4.444},
        {"q": "Boulevard Tirou 1 6000 Charleroi", "lat": 50.410, "lon": 4.443},
        # ------- Mechelen / Mons / Namur --------
        {"q": "Grote Markt 1 2800 Mechelen", "lat": 51.028, "lon": 4.480},
        {"q": "Grand Place 1 7000 Mons", "lat": 50.454, "lon": 3.953},
        {"q": "Place d'Armes 1 5000 Namur", "lat": 50.464, "lon": 4.866},
        # ------- Hasselt / Kortrijk / Oostende / Tournai --------
        {"q": "Grote Markt 1 3500 Hasselt", "lat": 50.930, "lon": 5.337},
        {"q": "Grote Markt 1 8500 Kortrijk", "lat": 50.829, "lon": 3.265},
        {"q": "Wapenplein 1 8400 Oostende", "lat": 51.230, "lon": 2.918},
        {"q": "Grand Place 1 7500 Tournai", "lat": 50.606, "lon": 3.388},
        # ------- Anderlecht / Schaerbeek / Forest --------
        {"q": "Rue Wayez 122 1070 Anderlecht", "lat": 50.688, "lon": 4.368},
        {"q": "Place Eugène Verboekhoven 1 1030 Schaerbeek", "lat": 50.871, "lon": 4.376},
        {"q": "Place Saint-Denis 1 1190 Forest", "lat": 50.819, "lon": 4.328},
        # ------- Typo / abbreviation perturbations of the above --------
        {"q": "Rue de la Loi 16 Bruxelles", "lat": 50.8467, "lon": 4.3673},
        {"q": "Avenue Louise 100 Ixelles", "lat": 50.8323, "lon": 4.3690},
        {"q": "Grote Markt Antwerpen", "lat": 51.221, "lon": 4.400},
        {"q": "Korenmarkt Gent", "lat": 51.054, "lon": 3.724},
        {"q": "Place St-Lambert Liege", "lat": 50.645, "lon": 5.574},
        {"q": "Markt Brugge", "lat": 51.208, "lon": 3.224},
        {"q": "Bondgenotenlaan Leuven", "lat": 50.881, "lon": 4.704},
        # ------- Postcode-only / locality-only fallbacks --------
        {"q": "1000 Bruxelles", "lat": 50.847, "lon": 4.353},
        {"q": "2000 Antwerpen", "lat": 51.221, "lon": 4.400},
        {"q": "9000 Gent", "lat": 51.054, "lon": 3.724},
        {"q": "4000 Liege", "lat": 50.640, "lon": 5.575},
        {"q": "8000 Brugge", "lat": 51.208, "lon": 3.224},
        {"q": "3000 Leuven", "lat": 50.879, "lon": 4.701},
        # ------- More addresses to round out 100 --------
        {"q": "Avenue de la Couronne 200 1050 Ixelles", "lat": 50.821, "lon": 4.392},
        {"q": "Rue de Flandre 100 1000 Bruxelles", "lat": 50.852, "lon": 4.349},
        {"q": "Boulevard de Waterloo 38 1000 Bruxelles", "lat": 50.836, "lon": 4.354},
        {"q": "Rue Royale 10 1000 Bruxelles", "lat": 50.852, "lon": 4.363},
        {"q": "Rue Belliard 100 1040 Etterbeek", "lat": 50.840, "lon": 4.376},
        {"q": "Avenue de Cortenbergh 1 1040 Etterbeek", "lat": 50.844, "lon": 4.388},
        {"q": "Rue de Stassart 32 1050 Ixelles", "lat": 50.836, "lon": 4.358},
        {"q": "Avenue de la Couronne 1 1050 Ixelles", "lat": 50.831, "lon": 4.391},
        {"q": "Rue de Stalle 50 1180 Uccle", "lat": 50.808, "lon": 4.328},
        {"q": "Chaussee d'Alsemberg 100 1190 Forest", "lat": 50.819, "lon": 4.345},
        {"q": "Rue Vanderkindere 1 1180 Uccle", "lat": 50.811, "lon": 4.355},
        {"q": "Avenue Brugmann 1 1190 Forest", "lat": 50.819, "lon": 4.347},
        {"q": "Avenue Albert 100 1190 Forest", "lat": 50.820, "lon": 4.341},
        {"q": "Rue Vanderkindere 200 1180 Uccle", "lat": 50.812, "lon": 4.355},
        {"q": "Rue de l'Enseignement 50 1000 Bruxelles", "lat": 50.851, "lon": 4.361},
        {"q": "Petite Rue de l'Ecuyer 1 1000 Bruxelles", "lat": 50.849, "lon": 4.354},
        {"q": "Rue de Namur 1 1000 Bruxelles", "lat": 50.842, "lon": 4.362},
        {"q": "Rue de l'Hopital 1 1000 Bruxelles", "lat": 50.846, "lon": 4.355},
        {"q": "Place du Sablon 1 1000 Bruxelles", "lat": 50.843, "lon": 4.357},
        {"q": "Place du Petit Sablon 1 1000 Bruxelles", "lat": 50.842, "lon": 4.357},
        {"q": "Rue des Sables 1 1000 Bruxelles", "lat": 50.851, "lon": 4.358},
        {"q": "Rue Marie-Christine 1 1020 Laeken", "lat": 50.876, "lon": 4.350},
        {"q": "Avenue Bockstael 1 1020 Laeken", "lat": 50.880, "lon": 4.347},
        {"q": "Rue Dansaert 1 1000 Bruxelles", "lat": 50.851, "lon": 4.350},
        {"q": "Rue de Flandre 50 1000 Bruxelles", "lat": 50.852, "lon": 4.349},
        {"q": "Quai au Bois à Brûler 1 1000 Bruxelles", "lat": 50.853, "lon": 4.351},
        {"q": "Rue de la Madeleine 1 1000 Bruxelles", "lat": 50.846, "lon": 4.355},
        {"q": "Rue du Marché aux Herbes 50 1000 Bruxelles", "lat": 50.847, "lon": 4.354},
        {"q": "Rue de la Tribune 1 1000 Bruxelles", "lat": 50.851, "lon": 4.354},
        {"q": "Rue du Damier 1 1000 Bruxelles", "lat": 50.853, "lon": 4.354},
        {"q": "Place Stéphanie 1 1050 Ixelles", "lat": 50.836, "lon": 4.359},
        {"q": "Rue de Livourne 1 1050 Ixelles", "lat": 50.832, "lon": 4.366},
        {"q": "Rue du Trône 100 1050 Ixelles", "lat": 50.836, "lon": 4.367},
        {"q": "Avenue Franklin Roosevelt 100 1050 Ixelles", "lat": 50.819, "lon": 4.378},
        {"q": "Avenue de Diest 1 3000 Leuven", "lat": 50.881, "lon": 4.708},
        {"q": "Tervuursestraat 50 3000 Leuven", "lat": 50.879, "lon": 4.704},
        {"q": "Brusselsestraat 1 3000 Leuven", "lat": 50.880, "lon": 4.696},
        {"q": "Tiensestraat 100 3000 Leuven", "lat": 50.876, "lon": 4.703},
        {"q": "Vital Decosterstraat 1 3000 Leuven", "lat": 50.876, "lon": 4.706},
        {"q": "Mechelsestraat 50 3000 Leuven", "lat": 50.880, "lon": 4.700},
        {"q": "Bondgenotenlaan 100 3000 Leuven", "lat": 50.881, "lon": 4.708},
        {"q": "Avenue Reine Elisabeth 1 4000 Liège", "lat": 50.625, "lon": 5.577},
        {"q": "Boulevard Frère-Orban 1 4000 Liège", "lat": 50.629, "lon": 5.577},
        {"q": "Place Cathédrale 1 4000 Liège", "lat": 50.643, "lon": 5.572},
    ]


def haversine_m(a_lat: float, a_lon: float, b_lat: float, b_lon: float) -> float:
    import math
    R = 6371000.0
    lat1 = math.radians(a_lat)
    lat2 = math.radians(b_lat)
    dlat = math.radians(b_lat - a_lat)
    dlon = math.radians(b_lon - a_lon)
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def query(host: str, port: int, q: str) -> tuple[list[dict], float]:
    enc = urllib.parse.quote(q)
    url = f"http://{host}:{port}/geocode?q={enc}&country=BE&limit=10"
    t0 = time.perf_counter()
    with urllib.request.urlopen(url, timeout=5.0) as r:
        body = r.read()
    elapsed_ms = (time.perf_counter() - t0) * 1000.0
    j = json.loads(body)
    return j.get("results", []) or [], elapsed_ms


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--shard", required=True)
    p.add_argument("--rerank", default=None, help="optional rerank GBDT model path")
    p.add_argument(
        "--binary",
        default="./target/release/butterfly-geocode",
        help="path to butterfly-geocode binary",
    )
    p.add_argument("--out", required=True, help="output directory for summary.json")
    p.add_argument("--queries", type=int, default=100, help="number of queries to run")
    args = p.parse_args()

    if not os.path.exists(args.shard):
        print(f"shard missing: {args.shard}", file=sys.stderr)
        return 2
    if not os.path.exists(args.binary):
        print(f"binary missing: {args.binary} (cargo build --release first)", file=sys.stderr)
        return 2

    port = find_free_port()
    cmd = [
        args.binary,
        "serve",
        "--shard",
        args.shard,
        "--rest-port",
        str(port),
        "--transport",
        "rest",
        "--admission-disable",
    ]
    if args.rerank:
        cmd += ["--rerank-model", args.rerank]
    print(f"booting: {' '.join(cmd)}", file=sys.stderr)
    proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        if not wait_until_ready("127.0.0.1", port, timeout_s=60.0):
            print("server failed to come up", file=sys.stderr)
            return 3

        queries = fixture_queries()
        random.Random(0xB17EBAD0).shuffle(queries)
        queries = queries[: args.queries]

        recall_at_10 = 0
        rerank_top_1 = 0
        latencies_ms: list[float] = []
        per_query: list[dict] = []
        for entry in queries:
            try:
                results, ms = query("127.0.0.1", port, entry["q"])
            except Exception as e:
                per_query.append({"q": entry["q"], "error": str(e)})
                continue
            latencies_ms.append(ms)
            top_correct = False
            in_top_10 = False
            for i, r in enumerate(results[:10]):
                d = haversine_m(
                    r.get("lat", 0.0), r.get("lon", 0.0), entry["lat"], entry["lon"]
                )
                if d <= 200.0:
                    in_top_10 = True
                    if i == 0:
                        top_correct = True
                    break
            if in_top_10:
                recall_at_10 += 1
            if top_correct:
                rerank_top_1 += 1
            per_query.append({
                "q": entry["q"],
                "top_correct": top_correct,
                "in_top_10": in_top_10,
                "n_results": len(results),
                "latency_ms": round(ms, 3),
            })

        n = len(latencies_ms) or 1
        latencies_ms.sort()
        p50 = latencies_ms[int(0.5 * (n - 1))]
        p95 = latencies_ms[int(0.95 * (n - 1))]
        summary = {
            "n_queries": len(queries),
            "n_responded": n,
            "recall_at_10": recall_at_10 / max(len(queries), 1),
            "rerank_top_1": rerank_top_1 / max(len(queries), 1),
            "p50_ms": round(p50, 3),
            "p95_ms": round(p95, 3),
            "mean_ms": round(statistics.mean(latencies_ms), 3),
            "shard": os.path.abspath(args.shard),
            "rerank_model": os.path.abspath(args.rerank) if args.rerank else None,
            "per_query": per_query,
        }
        os.makedirs(args.out, exist_ok=True)
        out_path = os.path.join(args.out, "summary.json")
        with open(out_path, "w") as fh:
            json.dump(summary, fh, indent=2)
        print(json.dumps(
            {k: v for k, v in summary.items() if k != "per_query"}, indent=2
        ))
        return 0
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5.0)
        except subprocess.TimeoutExpired:
            proc.kill()


if __name__ == "__main__":
    sys.exit(main())
