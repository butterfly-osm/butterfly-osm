# butterfly-geocode

Deterministic Belgium-only geocoder for the butterfly-osm toolkit. **MVP / Phase 0** of the architecture in [butterfly-osm#96](https://github.com/butterfly-osm/butterfly-osm/issues/96).

## What this ships

- **Forward geocoding**: `GET /geocode?q=...&country=BE` — text → coordinates
- **Reverse geocoding**: `GET /geocode/reverse?lat=...&lon=...` — coordinates → address
- **Belgium address shard** (`BFGS` v1) built from OSM `addr:*` tags
- **Architectural type contracts from #96**: `ParsedQuery`, `ParseHypothesis`, `ExecutionBudget`, `Channel`, `ChannelRole`, `RetrievalPolicy`, retrieval operators (`Lookup`, `Intersect`, `Union`, `TopkMerge`, `Filter`, `Score`, `Cap`, `Sample`, `Downgrade`)
- **Recombination Invariant** (#96): canonicalize + dedup with stable commutative ordering, identity folding, redundancy collapse — `op.canonicalize().canonicalize() == op.canonicalize()`
- **Zero-Cost-on-Clean-Queries NFR** (#96): the `|hypotheses|==1, |countries|==1` path skips canonicalization, dedup, and dynamic dispatch
- **Multi-channel executor** with `lookup → intersect → cap → score`
- **Static cost model** compositional over the operator tree
- **REST API** with `Accept`-header content negotiation (`application/json` default, `application/geo+json` for the GeoJSON variant)
- **Prometheus metrics** at `/metrics`
- **Graceful shutdown** on SIGINT/SIGTERM

## Architecture map (#96 → this crate)

| #96 concept                    | Where it lives                              |
|--------------------------------|---------------------------------------------|
| `ParsedQuery`, `ParseHypothesis` | `src/types.rs`                            |
| `ExecutionBudget`              | `src/types.rs`                              |
| `Channel`, `ChannelRole`       | `src/geocoder/channels.rs`                  |
| `RetrievalPolicy`              | `src/types.rs`                              |
| Retrieval operators + canonicalization | `src/geocoder/program.rs`           |
| Static cost model              | `src/geocoder/cost.rs`                      |
| Multi-channel executor         | `src/geocoder/executor.rs`                  |
| Country routing (1st-class)    | `src/routing/`                              |
| Per-country shard              | `src/shard/`                                |
| OSM `addr:*` extractor         | `src/osm_extract/`                          |
| HTTP API                       | `src/server/`                               |

## What's deferred (still in #96/#97/#98)

- **Byte-level transformer parser** (#96 §Tagger, #98 Phase 2) — the heuristic in `parser/heuristic.rs` is the deterministic Phase 0 baseline that the trained transformer will replace. **NOT** #98 Phase 1 (which is the retrieval-aware beam search over transformer outputs).
- **GBDT confidence reranker** (#96 §Confidence Model)
- **Multi-country routing** (#96 §Country Routing) — `CountryId` is `non_exhaustive`, the cheap classifier returns a posterior shape; only `BE` is wired.
- **Cross-border shard co-location** (#96 §Cross-Border Shard Co-location)
- **Feedback operators** (`Downgrade`, `TopkMerge`, `Sample`) — types defined per #96, not invoked by the MVP executor
- **Admission-control fanout caps** (#97 §5)
- **mmap-backed reader** — current reader is heap-resident (~1.3 GB RSS for Belgium); a future ticket will switch to `memmap2` via butterfly-route's `formats/mmap.rs` wrappers (the only sanctioned `unsafe` carveout in the workspace)

## Build and run

```bash
# 1. Get the Belgium PBF (if not already there)
butterfly-dl belgium --only pbf -o data/belgium.pbf

# 2. Build the shard (~1 minute on Belgium scale)
cargo run --release -p butterfly-geocode -- build-shard \
    --pbf data/belgium.pbf \
    --out geocode/regions/belgium.bfgs

# 3. Boot the server
cargo run --release -p butterfly-geocode -- serve \
    --shard geocode/regions/belgium.bfgs \
    --port 3003

# 4. Try it
curl 'http://localhost:3003/geocode?q=Rue+Wayez+122+Anderlecht&country=BE'
curl 'http://localhost:3003/geocode/reverse?lat=50.8467&lon=4.3525'
curl -H 'Accept: application/geo+json' \
    'http://localhost:3003/geocode?q=Grote+Markt+Antwerpen'
```

## Testing

```bash
# Unit tests
cargo test -p butterfly-geocode

# Belgium end-to-end (requires built shard at regions/belgium.bfgs)
cargo test --release -p butterfly-geocode --test belgium_e2e -- --ignored
```

## API reference

### `GET /geocode`

| Param     | Type   | Default | Description                                      |
|-----------|--------|---------|--------------------------------------------------|
| `q`       | string | —       | The query (max 512 bytes, required)              |
| `country` | string | `BE`    | ISO 3166-1 alpha-2 (MVP: only `BE`)              |
| `limit`   | int    | 5       | Max results (1-50)                               |
| `include` | string | —       | `debug` to surface `reason_codes`                |

`Accept: application/json` (default) returns:

```json
{
  "query": "Rue Wayez 122 Anderlecht",
  "country": "BE",
  "count": 3,
  "results": [
    {
      "lat": 50.6883,
      "lon": 4.3680,
      "street": "Rue Wayez",
      "housenumber": "122",
      "postcode": "",
      "locality": "",
      "score": 1.85
    }
  ]
}
```

`Accept: application/geo+json` returns a GeoJSON `FeatureCollection`.

### `GET /geocode/reverse`

| Param      | Type  | Default | Description                          |
|------------|-------|---------|--------------------------------------|
| `lat`      | float | —       | Latitude (-90..90, required)         |
| `lon`      | float | —       | Longitude (-180..180, required)      |
| `radius_m` | float | 200     | Search radius in metres (1..50000)   |
| `limit`    | int   | 1       | Max results (1-50)                   |

### `GET /health`

```json
{
  "status": "ok",
  "version": "2.0.0",
  "uptime_seconds": 12,
  "record_count": 4026754
}
```

### `GET /metrics`

Prometheus exposition.

## Performance (Belgium, 4 026 754 addresses)

| Metric                                      | Value             |
|---------------------------------------------|-------------------|
| Forward `/geocode` p50                      | 1.5 ms            |
| Forward `/geocode` p99                      | 21 ms             |
| Reverse `/geocode/reverse` p50              | 2.3 ms            |
| Reverse `/geocode/reverse` p99              | 17 ms             |
| Server RSS (heap-loaded shard)              | ~1.3 GB           |
| Shard file size                             | 178 MB            |
| Shard build time (PBF → `.bfgs`)            | ~67 s             |

## Reason-code vocabulary

The executor attaches one or more reason codes to each result (visible
when the client passes `include=debug`):

- `POSTCODE_EXACT` — postcode matched exactly
- `STREET_EXACT` — normalized street name matched exactly
- `STREET_PARTIAL` — street is a substring match
- `STREET_FUZZY` — street matched via bounded edit-distance fallback
- `HOUSE_EXACT` — house number matched exactly (case-insensitive)
- `HOUSE_NEAR` — house number within ±2 (off-by-one tolerance)
- `LOCALITY_EXACT` — locality matched exactly
- `NEAREST` — reverse-geocode hit within the requested radius
- `NEAREST_OUT_OF_RADIUS` — radius miss; nearest record returned anyway
- `EXEC` — multi-hypothesis path generic match (no specific match flag)

## License

AGPL-3.0-or-later (matches the rest of the butterfly-osm workspace).
