<p align="center">
  <img src="images/butterfly_logo_900kb.jpg" width="280" alt="Butterfly-OSM logo" />
</p>

# Butterfly-OSM

Production-grade routing engine and OSM toolkit, in Rust.

Exact turn-aware edge-based CCH for driving, walking, cycling, and trucking;
a full RAPTOR + ULTRA multimodal transit stack with merged GTFS and
NeTEx-EPIP feeds; REST + Arrow Flight gRPC; Belgium-latest deployed today,
faster than OSRM at scale.

[![License: AGPL v3+](https://img.shields.io/badge/license-AGPL--3.0--or--later-blue.svg)](LICENSE)

## At a glance

- **Matrix 10k×10k**: 18.3 s in-process (1.8× faster than OSRM CH at 32.9 s).
- **Flight gRPC matrix 50k×50k**: 9.61 min (parity with the historical `/table/stream` baseline; OSRM cannot run it).
- **`/isochrone` 30-min**: 5 ms p50; bulk endpoint sustains **1 526 iso/sec**.
- **`/route?avoid_polygons=...`**: ~780 ms cold MISS, ~22 ms warm HIT (incremental recustomization + LRU cache, #240).
- **`/transit` single warm**: 35 ms p50; `/transit/bulk` sustains 311 q/s on varied queries.
- **Coverage**: 4 modes (car, bike, foot, truck) × 4 merged transit feeds (SNCB, De Lijn, TEC, STIB).
- Belgium artifact `data/belgium/baseline.butterfly` deployed to the production `belgium-latest` container.

## Quickstart

```bash
docker build -t butterfly-route .
docker run -d --name butterfly -p 3001:8080 -p 3002:8081 \
  -v "${PWD}/data/belgium:/data" butterfly-route
curl "http://localhost:3001/route?src_lon=4.3517&src_lat=50.8503&dst_lon=4.4025&dst_lat=51.2194&mode=car"
```

Boot is ~30-40 s for the road graph alone, ~3 min with transit feeds. See
[Quickstart](docs/quickstart.md) for the full walk-through including the
upstream `butterfly-dl` + step1–step8 pipeline.

## Workspace

```
butterfly-osm/
├── butterfly-common/   # shared error types + utilities
├── dl/                 # butterfly-dl — OSM downloader (<1GB RAM for any file size)
└── route/              # butterfly-route — routing engine + transit
```

Support directories: `bench/` (regression and competitor benches),
`scripts/` (OSRM/Valhalla harnesses), `data/` (Belgium artifacts), `traffic/`
(per-density-class speed profiles for #84 recustomization), `models/`
(declarative `*.model.json` cost models, Q-Sprint).

## Features

### Point-to-point routing
- Exact turn restrictions (edge-based CCH state is a directed edge id).
- Multiple alternatives (penalty-based).
- `exclude=toll,ferry,motorway` via CCH recustomization with sparse triangle relaxation.
- `avoid_polygons=...` — incremental recustomization seeded by polygon-flagged base edges (#240) plus a bounded LRU cache keyed by canonicalised polygon hash.
- Traffic-aware variants (`?traffic=rush_hour`, #84): 5-bucket density classification, per-class speed factors, separate `cch.w.<mode>_<variant>.u32` weight set.
- Turn-by-turn steps with road names from 754K named-roads index.
- Bearing hints (`bearings=angle,range`).

### Matrices
- Bucket many-to-many CH for sparse `S × T` (small `POST /table`, low-latency).
- K-lane batched PHAST + L3-aware source tiling (#190) for large matrices — 10k×10k in 18.3 s in-process.
- Arrow Flight gRPC `matrix` action for 50k×50k+ over the wire, with parallel K-best snap (#232).
- Per-row Arrow IPC streaming, cooperative cancellation on client disconnect.

### Isochrones
- Block-gated PHAST downward scan (18× faster than naive linear scan).
- `direction=depart|arrive` (reverse isochrones).
- `contours=300,600,1200` (multi-contour) and `distance_m=...` (isodistance).
- GeoJSON or WKB output; CCW outer rings, 5-decimal precision.
- `POST /isochrone/bulk` length-prefixed WKB stream.

### Multimodal transit
- RAPTOR rounds over a merged `Timetable` (GTFS + NeTEx-EPIP via streaming `quick-xml` parser, Lambert-93 → WGS84 reprojection).
- ULTRA-preprocessed stop-to-stop transfer graph (66 512 stops, 668 K edges on Belgium).
- Cross-feed equivalence bridges (SNCB ↔ STIB, SNCB ↔ De Lijn) and same-station parent-child transfers, injected before ULTRA dominance restriction.
- `GET /transit` JSON, `POST /transit/bulk` (up to 100 K queries/call), Flight `transit_bulk` action (up to 500 K queries/call).
- NeTEx calendar fallback: if the active day set is empty (stale publication), remap to the same weekday in the latest published period.

### Other queries
- `POST /trip` — TSP/trip optimization (nearest-neighbor + 2-opt + or-opt).
- `GET /nearest` — K-best snap with connectivity-aware role masks (#197).
- `GET /height` — SRTM DEM elevation.
- Flight `edges_batch` — unnested per-edge path output with OSM node ids (flow analytics, emissions inventory, network vulnerability).
- Flight `catchment` — per-store catchment hulls via DoExchange.

### Operational
- `/health` with uptime, per-region node/edge counts, lazy-CRC verification status, and `avoid_cache` stats (hits/misses/size/capacity per region, #242).
- `/metrics` (Prometheus): latency histograms, per-section verification counters, `avoid_cache` gauges.
- Graceful shutdown (SIGINT + SIGTERM), 120s request timeout, 600s streaming timeout, gzip+brotli compression, panic recovery (`CatchPanicLayer`), input validation, multi-region serving (#91).

## Performance (Belgium, vs OSRM CH)

In-process bucket M2M, parallel, 8 threads (`butterfly-bench bucket-m2m --parallel`):

| Size       | OSRM CH | Butterfly | Ratio          |
|------------|---------|-----------|----------------|
| 50×50      | 17 ms   | 12 ms     | 1.4× FASTER    |
| 100×100    | 35 ms   | 32 ms     | 1.1× FASTER    |
| 1000×1000  | 684 ms  | 268 ms    | 2.56× FASTER   |
| 5000×5000  | 8.0 s   | 5.5 s     | 1.45× FASTER   |
| 10000×10000| 32.9 s  | **18.3 s**| **1.8× FASTER**|

Edge-based CCH has ~2.5× more states than OSRM's node-based CH (~5M EBG
nodes vs ~1.9M), and butterfly handles turn restrictions exactly where OSRM
approximates them — we beat OSRM despite the extra work. Small `N` (<25)
still loses to OSRM's sequential shape because rayon thread dispatch isn't
amortised over so few cells; see closed issue #191 for the analysis.

Flight gRPC end-to-end (includes Arrow IPC framing, full network roundtrip,
parallel K-best snap):

| Size      | Cells | Time      | Throughput |
|-----------|-------|-----------|------------|
| 1k × 1k   | 1 M   | 3.61 s    | 277 K c/s  |
| 10k × 10k | 100 M | 35.5 s    | 2.8 M c/s  |
| 50k × 50k | 2.5 B | **9.61 min** | 4.3 M c/s  |

Bench source: `bench/route/results/2026-05-22-post-snap-kbest/REPORT.md`.

## Architecture

```mermaid
flowchart LR
    subgraph data[Data sources]
        PBF[Belgium PBF]
        GTFS[GTFS feeds<br/>SNCB · De Lijn · TEC]
        NETEX[NeTEx-EPIP<br/>STIB]
    end

    subgraph pipeline[Build pipeline<br/>step1 → step8]
        ING[step1-ingest<br/>step2-profile]
        EBG[step3-nbg → step4-ebg<br/>edge-based graph]
        CCH[step5-weights → step6-order<br/>step7-contract → step8-customize]
    end

    subgraph serve[Serve]
        REST[REST :3001<br/>route · matrix · isochrone<br/>transit · trip · nearest · height]
        FLIGHT[Flight gRPC :3002<br/>matrix · route_batch<br/>isochrone · catchment<br/>transit_bulk · edges_batch]
    end

    PBF --> ING --> EBG --> CCH --> REST
    CCH --> FLIGHT
    GTFS --> REST
    NETEX --> REST
    GTFS --> FLIGHT
    NETEX --> FLIGHT
```

The edge-based CCH (EBG) is the **single source of truth**: routes,
matrices, isochrones, transit access/egress legs, and avoid-polygon
recustomization all run on the same hierarchy with the same weights. That
invariant is what keeps them mutually consistent. See
[Architecture](docs/architecture.md) for the full pipeline, PHAST and
bucket M2M internals, the avoid_polygons fast path (#240), and the transit
subsystem.

## Documentation

- [Quickstart](docs/quickstart.md) — Docker + first `/route` in 60 seconds.
- [API reference](docs/api.md) — REST + Flight endpoint catalog, query parameters, response shapes.
- [Deployment](docs/deployment.md) — env vars, `/health`, `/metrics`, Prometheus, multi-region.
- [Architecture](docs/architecture.md) — edge-based CCH, pipeline steps, avoid_polygons internals, transit subsystem.
- [Troubleshooting](docs/troubleshooting.md) — boot failures, snap errors, CRC mismatches, avoid-cache miss rate.

Swagger UI is live at `http://<host>:3001/swagger-ui/` when the server is
running. `CLAUDE.md` carries the in-tree engineering notes and benchmarking
playbooks.

## Contributing

Performance claims must be benchmarked against OSRM (matrices) and Valhalla
(isochrones) on Belgium. The harnesses live under `scripts/` and
`bench/route/`. Workspace lints are enforced as errors (warnings included);
run `cargo clippy --workspace --all-targets --all-features` and
`cargo fmt --all` before opening a PR. Submission implies AGPL-3.0-or-later
licensing per `CONTRIBUTING.md`.

See [CONTRIBUTING.md](CONTRIBUTING.md) for full guidelines.

## License

AGPL-3.0-or-later — applies to every crate in the workspace
(`butterfly-common`, `butterfly-dl`, `butterfly-route`). Network-deployed
forks must publish source per AGPL §13. See [LICENSE](LICENSE) for the
canonical FSF text.

## Status

- **butterfly-route**: production-ready; `belgium-latest` container deployed.
- **butterfly-dl**: production-ready; 79% faster than aria2 on Geofabrik downloads, <1 GB RAM for any file size up to the 81 GB planet.
- **Geocoder**: shelved (see issue #254, tag `geocode-shelved-2026-05-23`).

Built by Pierre &lt;pierre@warnier.net&gt; for the broader OpenStreetMap
community.
