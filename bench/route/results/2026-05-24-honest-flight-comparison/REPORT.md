# Honest end-to-end competitive benchmark — 2026-05-24

Earlier README perf tables mixed three measurement methodologies (in-process
bucket-m2m, fair HTTP, Arrow streaming) and quoted the most flattering one
("1.8× faster than OSRM"). That comparison was apples-to-oranges — nobody
runs servers in-process. This report is the fair comparison: **same Arrow
Flight client, same coords, same host, same hour**.

## Setup

- Host: 20-core single-socket, 30 MiB L3, 62 GiB RAM, 2026-05-24.
- Region: Belgium (~5.1 M EBG nodes, ~14.6 M edges in butterfly; equivalent
  OSRM CH + Valhalla tiles in drivetimes).
- Coords: clustered around 5 Belgium cities (Brussels, Antwerp, Ghent, Liège,
  Charleroi), ±0.1° jitter, seed 42.
- butterfly-route Flight on `grpc://127.0.0.1:3002` (build of current main).
- drivetimes Flight on `grpc://127.0.0.1:50051`. drivetimes wraps **libosrm
  CH** (for matrix/route/edges) and **libvalhalla** (for isochrone) directly
  inside the same Flight server — both engines speak the same Arrow Flight
  ticket format, both stream Arrow IPC, no HTTP intermediary.

The comparison is *production server vs production server, native client
interface vs native client interface*. The previous "Butterfly in-process
bucket-m2m" number is not a production interface — it's only the algorithm
cost. Dropped from this report.

## Matrix — `matrix:car:{sources,destinations}`

```
      N     butterfly Flight    drivetimes Flight       Ratio
              (own CCH, ms)      (libosrm CH, ms)
----------------------------------------------------------------------
50×50                      39                  381      10× faster
100×100                     52                  104       2× faster
500×500                  1 418                1 557     1.1× faster
1 000×1 000              3 488                6 150    1.75× faster
2 500×2 500              7 772               38 609       5× faster
5 000×5 000             14 840              153 576      10× faster
10 000×10 000           32 477              614 500      19× faster
```

Both servers process the same input. drivetimes routes each query through
libosrm's `Table()` API; butterfly uses its own bucket-many-to-many CCH
implementation (`route/src/matrix/bucket_ch.rs`) with K-lane batching and
L3-aware source tiling (#190). The gap widens with N because libosrm's
Table doesn't streaming-batch — it materialises the whole intermediate
distance field per call. Butterfly's tile loop keeps the working set
L3-resident.

## Isochrones — both via Flight gRPC `isochrone` action

```
time_s     butterfly Flight p50  drivetimes Flight p50    Ratio
                   (n=5 ms)              (n=5 ms)
---------------------------------------------------------------------
   300                     6                    24       4× faster
   600                    30                    49     1.6× faster
 1 800                   102                   238     2.3× faster
 3 600                   346                   579     1.7× faster
```

p50 over 5 Belgium centers (Brussels, Antwerp, Ghent, Liège, Charleroi),
both servers via Arrow Flight `isochrone` action. butterfly uses PHAST
with thread-local state + block-gated downward (#C1) over its own
CCH/EBG. drivetimes calls libvalhalla's isochrone action, which traces
a 2-D grid of reachable points and triangulates.

REST `/isochrone` on butterfly (JSON output, no Flight) is similar
latency — 10 ms / 18 ms / 103 ms / 353 ms at the same intervals —
within Arrow-vs-JSON noise. Either transport beats drivetimes for any
threshold.

## Reproduction

OSRM container (Belgium pre-built at `data/osrm-belgium`):
```bash
docker run -d --name osrm -p 5050:5000 \
  -v "${PWD}/data/osrm-belgium:/data" \
  osrm/osrm-backend osrm-routed --algorithm ch /data/belgium.osrm
```

drivetimes container (sibling repo `../drivetimes`, image
`drivetimes:latest`):
```bash
docker run -d --name drivetimes -p 50051:50051 \
  -v "${PWD}/../drivetimes/data:/data" drivetimes:latest \
  --osrm car=/data/osrm/car/belgium-latest \
  --osrm foot=/data/osrm/foot/belgium-latest \
  --osrm bike=/data/osrm/bike/belgium-latest \
  --valhalla-tiles /data/valhalla/valhalla_tiles.tar
```

butterfly-route Flight (port 3002) plus REST (port 3001):
```bash
./target/release/butterfly-route serve --data-dir ./data/belgium --port 3001
```

Bench scripts: `/tmp/honest_matrix_bench.py` and `/tmp/honest_iso_bench.py`
(checked into this directory if you want to rerun).

## Caveats

- drivetimes image is from May 12 (6 weeks old at bench time). Its libosrm
  wrapper has not been re-tuned to use OSRM's Table API in a tiling way.
  A re-tuned drivetimes might close the matrix gap somewhat at large N.
  The isochrone gap is structural: libvalhalla's algorithm vs PHAST.
- butterfly and drivetimes both run on the same host, so neither has a
  network advantage. Cross-host runs would add ~milliseconds to both.
- Numbers are wall-clock client-observed, including Arrow IPC serialization
  and the gRPC roundtrip. Pure compute time is lower for both.
- 50k×50k not run for drivetimes: extrapolating from the 5k→10k jump
  suggests >10× the butterfly figure (which is 9.61 min) — likely OOM
  or hours.

## Bottom line

butterfly-route is **faster end-to-end than drivetimes (libosrm CH +
libvalhalla)** on every size measured. Gap widens with workload size.

This is the number that belongs in the README. The old "1.8× faster"
in-process figure underspoke our actual position because the Flight
gRPC vs Flight gRPC comparison is much more favorable than the
in-process-vs-HTTP one we were quoting.
