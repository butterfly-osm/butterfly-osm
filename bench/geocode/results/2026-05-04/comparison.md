# Belgium geocoder benchmark — butterfly vs Nominatim

**Date**: 2026-05-04  
**Dataset**: 1000 mixed-quality Belgium addresses with gold (lat, lon)  
**Hardware**: shared dev box (not isolated)  
**Versions**: butterfly-geocode 2.0.0 (post #170), Nominatim 4.5 (mediagis/nominatim:4.5)

## Results

| Metric                   | butterfly       | Nominatim     | winner                |
|--------------------------|-----------------|---------------|-----------------------|
| Recall@1 (within 100 m)  | **0.470**       | **0.864**     | Nominatim (1.84×)     |
| Top-1 distance p50       | 66.9 m          | 0.0 m         | Nominatim             |
| Top-1 distance p95       | 4439.5 m        | 50.6 m        | Nominatim             |
| Throughput @ c=1         | 23.5 qps        | 56.4 qps      | Nominatim (2.4×)      |
| Throughput @ c=4         | 25.1 qps        | 246.2 qps     | Nominatim (9.8×)      |
| Throughput @ c=16        | 25.1 qps        | 354.1 qps     | Nominatim (14.1×)     |
| Latency p50 @ c=1        | **7.2 ms**      | 17.1 ms       | butterfly (2.4×)      |
| Latency p99 @ c=16       | 3032 ms         | 165.6 ms      | Nominatim (18×)       |

## Analysis

butterfly wins on **single-thread p50 latency** (7.2 ms vs 17.1 ms, 2.4× faster) but loses on every other axis.

**Recall gap (47% vs 86%)**: butterfly's MVP shard is built from OSM `addr:*` tags (~170 k Belgian records). Nominatim aggregates additional ranks (admin, postcode, locality centroids) and does not require a precise housenumber match. The recall gap is expected to close substantially when BOSA BeSt (~6.7 M Belgian authoritative addresses) ingestion lands (#173 (BOSA BeSt ingestion in PR #173)) — the BOSA shard is ~24× larger and authoritative.

**Concurrency saturation (25 qps regardless of concurrency)**: butterfly's throughput is essentially identical at c=1, c=4, c=16. Latency p50 at c=16 is **1004 ms** — over 100× the c=1 number — indicating a serial bottleneck. The control plane's spawn_blocking pool, the global allocator counting middleware, or contention on the R-tree's sync internals are likely candidates. **Filed as a follow-up**: [#172](https://github.com/butterfly-osm/butterfly-osm/issues/172).

**Distance precision (0.0 m vs 66.9 m)**: Nominatim returns the exact gold coordinate for the median of the test corpus because the test queries were derived from OSM nodes that Nominatim also indexes. butterfly returns the nearest housenumber on the same street, which is on average 67 m from the gold (typical inter-housenumber spacing for Belgian streets). Once BOSA lands, butterfly's median distance should drop to <10 m for any address present in BeSt.

## Honest characterization

butterfly-geocode in its current MVP shape is **not yet competitive** with Nominatim on Belgium. The architecture is correct (mmap-resident shards, control-plane-aware execution, GBDT reranking) but the data substrate is thin (OSM addr fallback) and the concurrency model has a serial bottleneck. Both are tracked.

## Next actions

- Land #173 (BOSA BeSt ingestion in PR #173) → expect recall@1 ≥ 0.85
- Investigate concurrency saturation — file GH issue with profiling data
- Re-run this bench after both → expected outcome: butterfly within 2× of Nominatim on every axis, ahead on single-thread p50
