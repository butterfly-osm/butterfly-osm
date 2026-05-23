# Quickstart

Once you have a baked Belgium routing container (or step1–step8 tree) on
disk, get a server answering `/route` requests in 60 seconds.

The cold path (download Geofabrik PBF, build the binary, run steps 1–8)
is a one-time ~10–30 min job. Everything below assumes that's done.

## 1. Download Belgium data

```bash
mkdir -p data/belgium && cd data/belgium
cargo install butterfly-dl   # or grab a release binary from GitHub
butterfly-dl europe/belgium belgium.osm.pbf
```

This pulls ~654 MB from Geofabrik. A `.sha256` sidecar is written next to the PBF; subsequent runs short-circuit if the hash already matches.

## 2. Build the Docker image

```bash
cd /path/to/butterfly-osm
docker build -t butterfly-route .
```

No pre-built images are published yet — build locally. The locally-
built runtime layer is ~200 MB (slim Debian + the static-ish Rust
binary + curl + certs); the multi-stage builder layer is fat but
never shipped. The 26 GB figure cited elsewhere refers to the mounted
Belgium data volume (the packed `*.butterfly` container), not the
image. Build takes 5–10 min cold, seconds warm.

## 3. Run the server

You still need to run steps 1-8 of the pipeline against `belgium.osm.pbf` to produce the routing artifacts under `data/belgium/`. See `architecture.md` for the pipeline. Once `data/belgium/` contains the step outputs:

```bash
docker run -d --name butterfly \
  -p 3001:8080 \
  -v "${PWD}/data/belgium:/data" \
  butterfly-route
```

Boot takes 30-40 s for the road graph alone (~5M EBG nodes + 754k named roads + spatial index). With the transit subsystem enabled (4 Belgian feeds + ULTRA transfer graph), expect ~3 min. Watch `docker logs -f butterfly` until you see `listening on 0.0.0.0:8080`.

## 4. First /route call

Brussels (Grand-Place) to Antwerp (Centraal), car profile:

```bash
curl "http://localhost:3001/route?src_lon=4.3517&src_lat=50.8503&dst_lon=4.4025&dst_lat=51.2194&mode=car"
```

Sample response (polyline truncated):

```json
{
  "duration_s": 2548.3,
  "distance_m": 47812.6,
  "geometry": {
    "polyline": "_p~iF~ps|U_ulLnnqC_mqNvxq`@..."
  }
}
```

`duration_s` is travel time in seconds; `distance_m` is meters. Default geometry is encoded polyline6. Pass `&geometry=geojson` for a coordinates array, `&steps=true` for turn-by-turn with road names.

## 5. Health check

```bash
curl http://localhost:3001/health
```

Returns uptime, per-region node/edge counts, mode list, CRC verification status, and per-region `avoid_cache` stats (hits/misses/size/capacity, see #242). Use it as your Docker `HEALTHCHECK` target — the Dockerfile already wires it.

## Next steps

- [API reference](api.md) — all REST endpoints, query parameters, response shapes
- [Deployment](deployment.md) — multi-region serving, Flight gRPC, Prometheus, graceful shutdown
- [Architecture](architecture.md) — the 8-step pipeline, edge-based CCH, PHAST, RAPTOR transit
- [Troubleshooting](troubleshooting.md) — boot failures, snap errors, CRC mismatches
- Swagger UI: `http://localhost:3001/swagger-ui/`
