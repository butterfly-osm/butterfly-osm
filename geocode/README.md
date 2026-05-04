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

## Execution control plane (#97)

The control plane sits between the parser and the executor and is the
operational guardrail layer. Every request flows through it.

### What it does

| Phase                          | Module                         | Purpose                                                          |
|--------------------------------|--------------------------------|------------------------------------------------------------------|
| Budget computation             | `control/budget.rs`            | Maps (confidence, fanout, cost) → `ExecutionBudget`              |
| Admission                      | `control/admission.rs`         | Token-bucket rate limiting (global + per-IP), 429 + `Retry-After`|
| Pre-execution check            | `control/budget::pre_execution_check` | Re-verifies static cost ceiling on executor entry         |
| Fanout safeguards              | `control/fanout.rs`            | Sequential + parallel-channel caps (per-query)                   |
| Country routing metrics        | `control/metrics_routing.rs`   | First-class subsystem (#97 §6)                                   |
| Channel + cost-calibration     | `control/metrics_channels.rs`  | Posting-list size, static-vs-feedback ratio, role-smoothness     |
| General per-query              | `control/metrics_general.rs`   | Tier counts, candidates, exhaustion                              |

### Budget tiers

```
Tight     → 1 country, 1 hypothesis, max 50 candidates, ceiling 256
Normal    → 2 countries, 3 hypotheses, max 200 candidates, ceiling 4 096
Wide      → 4 countries, 5 hypotheses, max 1 000 candidates, ceiling 65 536
Desperate → 4 countries, 5 hypotheses, max 5 000 candidates, ceiling 524 288
```

Tier selection inputs (see `BudgetPolicy` for the tunable thresholds):

- `global_confidence` from the parser
- has-postcode-and-house anchor (promotes Tight)
- country posterior entropy (entropy > 1 bit widens one tier)
- max parallel channel count per hypothesis (> 4 widens)
- lexical posting-list frequency (≥ `high_fanout_postings_threshold` widens)
- static cost over the ceiling (widens until it fits)

### Configuration knobs

All defaults plus their valid ranges are documented inline on the
config structs. Headline knobs:

| Knob                                              | Default        | Range                |
|---------------------------------------------------|----------------|----------------------|
| `BudgetPolicy::tight_confidence`                  | 0.85           | 0.0 - 1.0            |
| `BudgetPolicy::high_fanout_postings_threshold`    | 5 000          | 100 - 1 000 000      |
| `BudgetPolicy::static_cost_ceilings`              | (256, 4 096, 65 536, 524 288) | tier-keyed |
| `FanoutConfig::max_total_candidates`              | 5 000          | 100 - 100 000        |
| `FanoutConfig::max_query_time_ms`                 | 500            | 10 - 60 000          |
| `FanoutConfig::max_field_channels_per_hypothesis` | 4              | 1 - 6                |
| `FanoutConfig::max_blocker_empty_downgrades`      | 6              | 1 - 64               |
| `FanoutConfig::max_feedback_operator_firings`     | 8              | 1 - 64               |
| `AdmissionPolicy::global_capacity`                | 1 000          | 1 - 1 000 000        |
| `AdmissionPolicy::per_ip_capacity`                | 50             | 1 - 100 000          |
| `MetricsAlertThresholds::clean_query_overhead_p99`| 500 ns         | -                    |
| `MetricsAlertThresholds::clean_query_alloc_count` | 0              | exactly 0 — strict   |
| `MetricsAlertThresholds::static_vs_feedback_ratio`| 2.0            | -                    |

### Static-vs-feedback ratio (worked example)

The static cost is the **upper bound** the budget admits before
execution. The feedback cost is whatever extra work the executor
incurred at runtime via `Downgrade` operators (#96). The ratio
`feedback / static` is the calibration health metric:

- Ratio ≈ 0.0 — feedback never fires; static cost was a tight
  upper bound. This is the MVP today (no `Downgrade` invocations).
- Ratio ≈ 1.0 — for every unit of admitted work, the executor did
  one more unit chasing a downgrade. Tolerable.
- Ratio > 2.0 — alert. Either the country pack's role priors are
  miscalibrated (postcode-as-blocker firing on a shard with sparse
  postcode coverage) or the cost model under-estimates a popular
  channel.

Worked example, hypothetical:

```
static_cost     = 200   (planned: postcode → street → cap 64)
feedback_cost   = 500   (postcode came back empty → downgrade to scorer
                         → re-execute with street-as-blocker, which expanded
                         to 500 entries before cap)
ratio           = 2.5
```

Ratio 2.5 > threshold 2.0 → fires `clean_query` alert key (config
only; the alerting layer is upstream of this crate). The dashboard
should drill into `geocode_cost_feedback_firing_total{channel="postcode",country="BE"}`
to confirm the suspect channel.

### Metric vocabulary

All metrics use the `geocode_*` prefix. Highlights:

| Name                                          | Type      | Why it matters                                             |
|-----------------------------------------------|-----------|------------------------------------------------------------|
| `geocode_admission_admitted_total`            | counter   | Capacity headroom indicator                                |
| `geocode_admission_rejected_total`            | counter   | Inversely tracks tail latency                              |
| `geocode_query_tier_total{tier=...}`          | counter   | Distribution of budget tiers (clean queries → `tight`)     |
| `geocode_query_candidates`                    | histogram | Candidate count per query                                  |
| `geocode_query_budget_exhaustion_total`       | counter   | Queries that hit `max_total_candidates`                    |
| `geocode_country_router_confidence`           | histogram | Cheap classifier confidence (label `top_country`)          |
| `geocode_country_router_disagreement_total`   | counter   | Cheap-vs-neural disagreement (#97 §6)                      |
| `geocode_channel_posting_list_size`           | histogram | Per-(channel, country) posting-list size                   |
| `geocode_cost_static_vs_feedback_ratio`       | histogram | Calibration health (target ~1.0)                           |
| `geocode_recomb_collapse_rate`                | histogram | Hypothesis-dedup collapse fraction (#96 invariant)         |
| `geocode_cleanquery_overhead_seconds`         | histogram | **Zero-Cost-on-Clean-Queries canary** (target p99 ≤ 500ns) |
| `geocode_cleanquery_alloc_count`              | histogram | **Strict 0 — non-zero is a regression**                    |
| `geocode_cleanquery_share`                    | gauge     | Fraction of traffic on the clean path                      |

### Allocation NFR enforcement

The clean-query path **must not heap-allocate** beyond the unavoidable
output `Vec<GeocodedResult>`. The contract is enforced by
`tests/control_clean_query_alloc.rs` via a wrapping
`#[global_allocator]` that counts allocations between
`start_count()` / `stop_count()` markers. The test runs in CI and
fails the build the moment a regression slips in.

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
