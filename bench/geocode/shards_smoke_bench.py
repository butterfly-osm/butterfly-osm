#!/usr/bin/env python3
"""Smoke bench: production shard coverage vs Nominatim.

Per-country plan:
  - Boot a single-shard butterfly-geocode server on port 31000+i.
  - Run a tiny sample of well-known queries through both Butterfly and
    Nominatim (https://nominatim.openstreetmap.org/, public usage policy).
  - Record top-1 lat/lon agreement (within 1 km) and per-engine median
    latency.

This is NOT a full accuracy bench (#88 + #89 cover that). It only
proves: (1) every shipped shard answers, (2) the answer is in the
right ballpark.

If $NOMINATIM_BASE is unset and the public endpoint is unreachable,
the script still emits the Butterfly half of the bench so the artefact
exists — Nominatim columns become "skipped".

Usage:
  python3 bench/geocode/shards_smoke_bench.py \\
      --shards-dir geocode/data/shards \\
      --binary    target/release/butterfly-geocode \\
      --out       bench/geocode/results/2026-05-06-shards-coverage
"""

import argparse
import json
import math
import os
import shutil
import socket
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

# Per-country smoke queries. These are well-known central addresses;
# the goal is "did any reasonable result come back," not "exactly match
# that address," so the ground truth is "near the city centre."
# (lat, lon) reference is the city centre, used for distance check.
QUERIES = {
    "AT": [("Stephansplatz 1 Wien", 48.2085, 16.3725)],
    "AU": [("Bourke Street 1 Melbourne", -37.8136, 144.9631)],
    "BE": [("Rue Wayez 122 Anderlecht", 50.8358, 4.3111)],
    "BR": [("Avenida Paulista 1000 São Paulo", -23.5613, -46.6565)],
    "CH": [("Bahnhofstrasse 1 Zürich", 47.3769, 8.5417)],
    "DE": [("Unter den Linden 1 10117 Berlin", 52.5170, 13.3889)],
    "ES": [("Gran Via 1 Madrid", 40.4202, -3.7059)],
    "FR": [("Rue de Rivoli 1 75001 Paris", 48.8585, 2.3358)],
    "GB": [("Oxford Street 1 London", 51.5145, -0.1413)],
    "IN": [("MG Road 1 Bangalore", 12.9756, 77.6094)],
    "IT": [("Via del Corso 1 Roma", 41.9012, 12.4806)],
    "JP": [("名古屋市中区 1", 35.1815, 136.9066)],
    "LU": [("Place d'Armes 1 Luxembourg", 49.6111, 6.1299)],
    "NL": [("1012JS Dam 1 Amsterdam", 52.3733, 4.8937)],
    "US": [("Pennsylvania Avenue 1 Washington DC", 38.8975, -77.0364)],
}


def haversine_km(a_lat, a_lon, b_lat, b_lon):
    """Great-circle distance in kilometres."""
    R = 6371.0
    p1, p2 = math.radians(a_lat), math.radians(b_lat)
    dp = math.radians(b_lat - a_lat)
    dl = math.radians(b_lon - a_lon)
    h = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(min(1.0, math.sqrt(h)))


def is_port_listening(port, host="127.0.0.1"):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.5)
        try:
            s.connect((host, port))
            return True
        except OSError:
            return False


def boot_server(binary, shard, port):
    proc = subprocess.Popen(
        [
            binary, "serve",
            "--shard", str(shard),
            "--rest-port", str(port),
            "--grpc-port", str(port + 1),
            "--transport", "rest",
            "--log-format", "text",
        ],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    for _ in range(60):
        if is_port_listening(port):
            return proc
        time.sleep(0.5)
    proc.terminate()
    raise RuntimeError(f"server on {port} did not come up")


def query_butterfly(port, q, country):
    url = (
        f"http://127.0.0.1:{port}/geocode?"
        f"q={urllib.parse.quote(q)}&country={country}&limit=1"
    )
    t0 = time.perf_counter()
    with urllib.request.urlopen(url, timeout=10) as r:
        data = json.loads(r.read())
    dt_ms = (time.perf_counter() - t0) * 1000
    if not data.get("results"):
        return None, dt_ms
    r0 = data["results"][0]
    return (r0["lat"], r0["lon"]), dt_ms


def query_nominatim(base, q, country):
    url = (
        f"{base.rstrip('/')}/search?"
        f"q={urllib.parse.quote(q)}&countrycodes={country.lower()}&"
        f"format=json&limit=1&addressdetails=0"
    )
    req = urllib.request.Request(
        url, headers={"User-Agent": "butterfly-geocode-smoke-bench/0.1"}
    )
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read())
    except Exception:
        return None, (time.perf_counter() - t0) * 1000
    dt_ms = (time.perf_counter() - t0) * 1000
    if not data:
        return None, dt_ms
    return (float(data[0]["lat"]), float(data[0]["lon"])), dt_ms


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--shards-dir", required=True)
    ap.add_argument("--binary", default="./target/release/butterfly-geocode")
    ap.add_argument("--out", required=True)
    ap.add_argument("--nominatim-base",
                    default=os.environ.get(
                        "NOMINATIM_BASE",
                        "https://nominatim.openstreetmap.org",
                    ))
    args = ap.parse_args()

    shards_dir = Path(args.shards_dir)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    # Probe Nominatim once.
    have_nominatim = False
    try:
        req = urllib.request.Request(
            f"{args.nominatim_base.rstrip('/')}/status",
            headers={"User-Agent": "butterfly-geocode-smoke-bench/0.1"},
        )
        with urllib.request.urlopen(req, timeout=5) as r:
            r.read()
        have_nominatim = True
    except Exception as e:
        print(f"[warn] Nominatim probe failed ({e}); skipping comparison",
              file=sys.stderr)

    rows = []
    for shard_path in sorted(shards_dir.glob("*.bfgs")):
        # Pick country code from filename; we ship "<iso2>.bfgs" plus
        # variants like "lu-oa-osm-merged.bfgs". The leading 2 letters
        # before the first hyphen / dot are the country code.
        stem = shard_path.stem
        iso2 = stem.split("-")[0].split(".")[0].upper()
        if len(iso2) != 2:
            print(f"[skip] {shard_path}: cannot infer ISO2 from filename")
            continue
        if iso2 not in QUERIES:
            print(f"[skip] {shard_path}: no smoke query defined for {iso2}")
            continue
        # Skip merged variants; the canonical shard already covers the
        # country (and we don't want to double-count).
        if "-merged" in stem or "-oa-osm" in stem:
            continue

        port = 30100 + (ord(iso2[0]) * 7 + ord(iso2[1])) % 4000
        print(f"[info] booting {iso2} on :{port}", file=sys.stderr)
        try:
            proc = boot_server(args.binary, shard_path, port)
        except Exception as e:
            print(f"[fail] {iso2} server boot: {e}", file=sys.stderr)
            continue

        try:
            for q, ref_lat, ref_lon in QUERIES[iso2]:
                bf, bf_ms = query_butterfly(port, q, iso2)
                nm = nm_ms = None
                if have_nominatim:
                    nm, nm_ms = query_nominatim(args.nominatim_base, q, iso2)
                    # Public Nominatim has a 1 req/s usage policy.
                    time.sleep(1.1)
                row = {
                    "iso2": iso2,
                    "shard": str(shard_path.name),
                    "shard_size_mb": round(
                        shard_path.stat().st_size / 1048576, 1
                    ),
                    "query": q,
                    "ref_lat": ref_lat,
                    "ref_lon": ref_lon,
                    "butterfly_lat": bf[0] if bf else None,
                    "butterfly_lon": bf[1] if bf else None,
                    "butterfly_ms": round(bf_ms, 1),
                    "butterfly_km_from_ref": (
                        round(haversine_km(ref_lat, ref_lon, bf[0], bf[1]), 2)
                        if bf else None
                    ),
                    "nominatim_lat": nm[0] if nm else None,
                    "nominatim_lon": nm[1] if nm else None,
                    "nominatim_ms": round(nm_ms, 1) if nm_ms else None,
                    "nominatim_km_from_ref": (
                        round(haversine_km(ref_lat, ref_lon, nm[0], nm[1]), 2)
                        if nm else None
                    ),
                    "agreement_km": (
                        round(haversine_km(bf[0], bf[1], nm[0], nm[1]), 2)
                        if bf and nm else None
                    ),
                }
                rows.append(row)
                print(json.dumps(row), file=sys.stderr)
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()

    (out / "rows.jsonl").write_text(
        "\n".join(json.dumps(r) for r in rows) + "\n"
    )

    # Summary table.
    with (out / "summary.md").open("w") as f:
        f.write("# Shard coverage smoke bench\n\n")
        f.write(f"Generated: {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}\n\n")
        f.write(f"Nominatim available: {have_nominatim}\n\n")
        f.write("| ISO2 | Shard MB | Query | BF (km from ref) | Nominatim (km from ref) | BF↔Nominatim (km) | BF ms | Nom ms |\n")
        f.write("|------|----------|-------|------------------|------------------------|-------------------|-------|--------|\n")
        for r in rows:
            f.write(
                f"| {r['iso2']} | {r['shard_size_mb']} | {r['query'][:32]} | "
                f"{r['butterfly_km_from_ref']} | {r['nominatim_km_from_ref']} | "
                f"{r['agreement_km']} | {r['butterfly_ms']} | {r['nominatim_ms']} |\n"
            )

    print(f"\nWrote {out}/rows.jsonl and {out}/summary.md", file=sys.stderr)


if __name__ == "__main__":
    main()
