# butterfly-geocode

Belgium-only geocoder for the butterfly-osm toolkit. Started as the deterministic Phase 0 baseline of [butterfly-osm#96](https://github.com/butterfly-osm/butterfly-osm/issues/96); now ships **two parser backends side-by-side** — the deterministic heuristic baseline AND a byte-level transformer with retrieval-aware decoding ([#98](https://github.com/butterfly-osm/butterfly-osm/issues/98) Phase 1).

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
| **Byte-level transformer (#96 §Tagger)** | **`src/tagger/`**                 |
| **#98 Phase 1 retrieval-aware decoding** | **`src/parser/decoding.rs`, `src/parser/beam.rs`, `src/parser/anchor.rs`** |
| **Neural parser backend**      | **`src/parser/neural.rs`**                  |
| **Parser backend trait**       | **`src/parser/mod.rs::ParserBackend`**      |

## Neural parser (#96 §Tagger + #98 Phase 1)

A byte-level transformer encoder + BIO tagging head + country-posterior head, implemented from scratch on [`candle_core`] (Apache-2.0, AGPL-compatible). The shipped tiny architecture:

| | |
|---|---|
| `d_model` | 64 |
| `n_layers` | 2 |
| `n_heads` | 4 (head_dim=16) |
| `d_ff` | 256 |
| Tokenizer | byte-level, vocab=260 (256 byte values + BOS/EOS/PAD/UNK) |
| Total parameters | ~120k |
| Safetensors size | 461 KB |

**This is a proof-of-life model**, trained on a synthetic Belgium corpus to validate that the training loop converges and inference is wired correctly end-to-end. A production-quality model needs the shard-agnostic augmentation strategy from #96 §Tagger plus a real OSM-derived corpus — both filed for follow-up.

#### #98 Phase 1: retrieval-aware decoding

Each piece of #98 Phase 1 ships:

| #98 Phase 1 sub-deliverable | Where it lives |
|---|---|
| **1.1** Hypothesis recombination via canonicalization | `src/parser/decoding.rs::decode` (calls `Op::canonicalize` from #96, dedups by canonical form, merges source-hypothesis scores via **max**) |
| **1.2** Adaptive beam width | `src/parser/beam.rs::adaptive_beam_width` (varies with local entropy + accumulated static-cost fraction; suppresses expansion when `cost ≥ 0.7 × ceiling`) |
| **1.3** Country-router prior | Consumed via `inference.country_posterior` in `src/parser/neural.rs::merge_country_candidates`; the beam does NOT re-derive country routing |
| **1.4** Anchor pruning with role-smoothness | `src/parser/anchor.rs` + `src/parser/beam.rs::apply_anchor_pruning` (within ε of trust → downweight, outside ε → hard-prune; ε=0.15 by default) |
| **1.5** Retrieval-utility heuristic scoring | `src/parser/decoding.rs::retrieval_utility_score` (penalizes high static cost, all-scorer policies, empty/oversized lookups; rewards strong blocker channels) |

#### Training the proof-of-life model

```bash
# Synthetic-corpus training (~5 min for 25 epochs on the tiny config)
cargo run --release -p butterfly-geocode -- train \
    --out geocode/data/models/belgium-tiny.safetensors \
    --synthetic 8192 --epochs 25 --batch-size 64 --learning-rate 0.003

# Or with a real corpus
cargo run --release -p butterfly-geocode -- train \
    --out geocode/data/models/my-model.safetensors \
    --corpus path/to/corpus.jsonl --epochs 50
```

Corpus JSONL format (one example per line):

```json
{"text":"Rue Wayez 122 1070 Anderlecht","country":"BE","spans":[
  {"field":"street","start":0,"end":9},
  {"field":"house","start":10,"end":13},
  {"field":"postcode","start":14,"end":18},
  {"field":"locality","start":19,"end":29}]}
```

Training emits a safetensors file plus a sidecar `<path>.config.json` carrying the architecture so the loader doesn't need hardcoded shapes.

#### Serving with the neural parser

```bash
cargo run --release -p butterfly-geocode -- serve \
    --shard geocode/regions/belgium.bfgs \
    --parser neural \
    --model geocode/data/models/belgium-tiny.safetensors
```

If the model file is missing or fails to load, the server **falls back to the heuristic backend with a warning**, so the neural path is never load-bearing for availability.

#### Convergence (proof-of-life run, 8192 synthetic examples, 25 epochs)

```
epoch=0  train_loss=1.5840 eval_loss=1.1515 bio_acc=0.5995 country_acc=1.0000
epoch=5  train_loss=0.7668 eval_loss=0.7466 bio_acc=0.7224 country_acc=1.0000
epoch=10 train_loss=0.6916 eval_loss=0.6779 bio_acc=0.7418 country_acc=1.0000
epoch=15 train_loss=0.6607 eval_loss=0.6507 bio_acc=0.7463 country_acc=1.0000
epoch=20 train_loss=0.6440 eval_loss=0.6372 bio_acc=0.7480 country_acc=1.0000
epoch=24 train_loss=0.6364 eval_loss=0.6289 bio_acc=0.7491 country_acc=1.0000
```

BIO accuracy plateaus around 75% on the synthetic corpus — that's the limit of a 120k-parameter architecture trained on 4 fixed sentence shapes without augmentation. Country accuracy is trivially 100% (only one country in the corpus). On Belgium e2e tests, the neural parser correctly resolves "Rue Wayez 122 Anderlecht" through the #98 Phase 1 decoding pipeline (see `tests/belgium_e2e.rs::neural_parser_resolves_rue_wayez_122_anderlecht`).

#### What's still deferred

- **#98 Phase 2 (learned objective)** — explicitly blocked on a labeled `(query → gold-address)` corpus; the spec itself defers it. Phase 2 will replace the Phase 1 heuristic retrieval-utility score with a learned function trained directly on geocode success.
- **Production-grade trained model** — the shipped `belgium-tiny.safetensors` is a proof-of-life. A real model needs ~2-4M params, OSM-derived training data, and #96 §Tagger's shard-agnostic augmentation strategy.
- **LoRA / regional adapters** — hooks noted in #96 §Tagger but not implemented.
- **Multi-country routing** — `n_countries=1` in the shipped config; the country head trivially predicts BE. The architecture extends cleanly when more countries land.

## Confidence + GBDT Reranking (#96 §Confidence Model)

A pure-Rust GBDT layer reranks the executor's candidates and assigns
each query an action tier (`accept` / `caution` / `review` / `reject`).

- **Library:** `gbdt = "0.1.3"` (per Agent D's PR #164 decision —
  pure Rust, single-binary deploy preserved).
- **Inference:** ~1.12 µs p50 / 1.81 µs p99 single-row predict;
  end-to-end rerank (executor + features + GBDT + thresholds) is
  ~212 µs p50 / ~220 µs p99 for a Brussels query on Belgium.
- **Feature schema:** 14 numeric features defined in
  `confidence::features::Features`, versioned via
  `Features::SCHEMA_VERSION`. Stable JSONL on-disk shape so training
  corpora survive code refactors.
- **Training:** pointwise `LogLikelyhood` loss. The
  `butterfly-geocode train-rerank` CLI subcommand reads a labelled
  JSONL corpus (`{"query": "...", "gold": {"lat": ..., "lon": ...,
  "housenumber": "..."}, ...}`) and emits a `gbdt`-format model file.
  When `--corpus` is omitted, the trainer synthesises a corpus by
  sampling records from the shard — useful as a Phase-0 bootstrap
  while #98 Phase 2 collects real telemetry.
- **Action thresholds (BE Phase-0 defaults):** `accept >= 0.85,
  caution >= 0.5, review >= 0.2, reject < 0.2`. Tunable via
  `ConfidenceConfig`.
- **Reason codes:** `RERANK_GBDT` on every reranked candidate;
  `HIGH_CONFIDENCE` / `LOW_CONFIDENCE` / `BELOW_THRESHOLD` for the
  top-1 tier; `STREET_WEAK`, `COUNTRY_UNCERTAIN`, `POSTCODE_EXACT`,
  `POSTCODE_MISMATCH` as per-candidate secondary signals.
- **No-model fallback:** when the server is started without
  `--rerank-model`, the executor returns its raw scores untouched and
  the API surfaces `confidence: "accept"`. Existing clients see no
  behavioural change.

```bash
# Train the reranker on Belgium (synthetic corpus from shard records).
cargo run --release -p butterfly-geocode -- train-rerank \
    --shard geocode/regions/belgium.bfgs \
    --out geocode/data/models/rerank-belgium-tiny.gbdt \
    --iterations 100 --max-depth 6 --synth-size 5000

# Serve with reranking enabled.
cargo run --release -p butterfly-geocode -- serve \
    --shard geocode/regions/belgium.bfgs \
    --rerank-model geocode/data/models/rerank-belgium-tiny.gbdt
```

The shipped model `geocode/data/models/rerank-belgium-tiny.gbdt` was
trained on the Belgium shard with synthetic queries (1000 corpus rows,
9764 (query, candidate) pairs, 50 trees / depth 5). Rank-1 hit rate on
the training set is ~88%. **Phase 2 retraining** with production
telemetry — once #98's beam-search parser ships and queries-with-gold
can be logged — is the user's follow-up; the architecture and CLI
landed in this layer support that without code changes.

## What's deferred (still in #96/#97/#98)

- **Byte-level transformer parser** (#96 §Tagger, #98 Phase 2) — the heuristic in `parser/heuristic.rs` is the deterministic Phase 0 baseline that the trained transformer will replace. **NOT** #98 Phase 1 (which is the retrieval-aware beam search over transformer outputs).
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
