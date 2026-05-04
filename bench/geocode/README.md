# Geocode Benchmark Infrastructure

Competitive benchmarking for `butterfly-geocode` against external geocoding
baselines. Mirrors the existing `/route` vs OSRM and `/isochrone` vs Valhalla
policy described in `CLAUDE.md` (section "Benchmark Comparison Policy").

## Status

This is **scaffolding** — issued by the research / preparation agent track.
The geocoder itself does not exist yet. The contents of this directory are:

- `docker-compose.yml` — pinned Nominatim image with Belgium PBF mount
- `queries/belgium.tsv` — 1000-row reference query set (mixed quality)
- `bench.py` — Python benchmark client (concurrency 1 / 4 / 16, recall@1 metric)
- `README.md` — this file

## Methodology

The bench reads `queries/belgium.tsv`, sends each query to the configured
geocoder endpoint, records:

1. **Latency** per request (p50, p95, p99)
2. **Throughput** at concurrency levels 1, 4, 16
3. **Recall@1**: how often the top-1 result lat/lon is within 100 m of the gold
   coordinate annotated on the query
4. **Top-1 distance distribution**: median and p95 distance from gold

Cross-engine output: a JSONL file per run (`results/<engine>-<concurrency>.jsonl`)
plus a markdown summary. Compare engines side-by-side with `compare.py` (TODO,
not in MVP).

## Query set construction

`queries/belgium.tsv` is bootstrapped from a sample of OSM `addr:*` nodes —
rows have:

```
query_id<TAB>query_text<TAB>gold_lat<TAB>gold_lon<TAB>quality_class
```

`quality_class` is one of `clean | abbreviated | typo | reordered |
partial`. The sample covers all five classes roughly equally so the recall
metric exercises the full quality range.

The current bootstrap query set in this commit is a **provisional 30-row
seed**. Once the geocode-training corpus generator (in `geocode-training/
corpus-gen/`) is run on Belgium, we will derive the full 1000-row set from
the same gold OSM nodes — that way we can correlate parser performance to
geocode performance without two different ground-truth pipelines.

## Pinned versions

- Nominatim: `mediagis/nominatim:4.5` (image digest pinned in the
  docker-compose hash on first import; check `docker compose images` after
  first start)
- Belgium PBF: from Geofabrik, expected at `data/belgium.pbf`. The
  parallel data-prep agent owns this file.

## Setup

First-time Nominatim import takes ~30 minutes on Belgium. After that, restarts
are seconds.

```bash
# Pre-flight: verify compose syntax (works without Docker daemon)
cd bench/geocode
docker compose -f docker-compose.yml config > /dev/null && echo "compose OK"

# Start Nominatim (will import on first run)
docker compose -f docker-compose.yml up -d

# Wait for import (watch logs)
docker logs -f butterfly-bench-nominatim

# Smoke test
curl 'http://localhost:8080/search?q=Rue+Wayez+122+Anderlecht&format=json' | head -200

# Run the bench (Python 3.11+, install requirements)
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python bench.py --engine nominatim --concurrency 1,4,16 --queries queries/belgium.tsv
```

## Future comparators

Documented but **not stood up** in MVP — these get their own docker-compose
fragments when we move past Nominatim:

- **Pelias** (Mapzen-derived, Elasticsearch-backed). Stronger fuzzy matching,
  multi-language. Compose at `pelias/`.
- **Photon** (Komoot, Elasticsearch+OSM tags). Much faster than Nominatim,
  weaker on house-number resolution. Compose at `photon/`.
- **MOTIS-geocode** (academic, multi-modal). Smaller install footprint,
  designed for transit. Compose at `motis-geocode/`.

Each will land in this directory with the same shape: `docker-compose.yml`,
`bench.py` adapter for its API, and a section in this README.

## What this directory is NOT

It is not the geocoder's own benchmark. The geocoder will own its own bench
harness (in `geocode/src/bench/` once that crate exists), parallel to
`route/src/bench/`. **This directory is the comparator infrastructure**.
