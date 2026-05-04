# butterfly-geocode

Multi-country geocoder for the butterfly-osm toolkit. Started as the deterministic Phase 0 baseline of [butterfly-osm#96](https://github.com/butterfly-osm/butterfly-osm/issues/96); now ships **two parser backends side-by-side** — the deterministic heuristic baseline AND a byte-level transformer with retrieval-aware decoding ([#98](https://github.com/butterfly-osm/butterfly-osm/issues/98) Phase 1) — over **per-country shards** for cluster #1 + #2 (BE / FR / NL / LU / DE / AT / CH).

## What this ships

- **Per-country shards** (BFGS v3) for cluster #1 + #2 (BE / FR / NL / LU / DE / AT / CH) built from OSM `addr:*` tags via `butterfly-geocode build-shard --country <ISO2>`
- **Multi-shard server**: `butterfly-geocode serve --shard-dir <dir>` loads every `*.bfgs` in `<dir>` and routes forward queries via the lexical country classifier (returns a `(country, weight)` posterior) and reverse queries via lat/lon bbox membership
- **Cross-shard score normalization**: per-shard scores are scaled by the country posterior weight before merging, so an uncertain match against a "wrong" country is correctly demoted (see `executor::execute_across_shards`)
- **Forward geocoding**: `GET /geocode?q=...[&country=BE]` — text → coordinates
- **Reverse geocoding**: `GET /geocode/reverse?lat=...&lon=...[&country=BE]` — coordinates → address with bbox-based country dispatch
- **Per-country shards** built from OSM `addr:*` tags
- **Architectural type contracts from #96**: `ParsedQuery`, `ParseHypothesis`, `ExecutionBudget`, `Channel`, `ChannelRole`, `RetrievalPolicy`, retrieval operators (`Lookup`, `Intersect`, `Union`, `TopkMerge`, `Filter`, `Score`, `Cap`, `Sample`, `Downgrade`)
- **Recombination Invariant** (#96): canonicalize + dedup with stable commutative ordering, identity folding, redundancy collapse — `op.canonicalize().canonicalize() == op.canonicalize()`
- **Zero-Cost-on-Clean-Queries NFR** (#96): the `|hypotheses|==1, |countries|==1` path skips canonicalization, dedup, and dynamic dispatch
- **Multi-channel executor** with `lookup → intersect → cap → score`
- **Static cost model** compositional over the operator tree
- **REST API** with `Accept`-header content negotiation (`application/json` default, `application/geo+json` for the GeoJSON variant)
- **Prometheus metrics** at `/metrics`
- **Graceful shutdown** on SIGINT/SIGTERM with bounded drain timeout
- **Per-IP rate limit** (`tower_governor`, defaults: 100 req/s, burst 200) on top of the #97 cost-based admission gate
- **Permissive CORS** by default (operators behind a reverse proxy should narrow this)
- **Response compression** (gzip + brotli) negotiated via `Accept-Encoding`
- **Request body cap** (4 KB default — protects future POST endpoints)
- **Multi-stage Dockerfile** (`debian:trixie-slim` runtime, ~110 MB image)

## Docker

```bash
# Build (from the workspace root, NOT inside `geocode/`).
docker build -t butterfly-geocode:latest -f geocode/Dockerfile .

# Run with a shard mounted at /data/shard.bfgs. The image's CMD
# defaults to JSON logs; override with `--log-format text` for
# human-readable output.
docker run -d --name butterfly-geocode \
  -p 8080:8080 \
  -v /host/path/to/belgium.bfgs:/data/shard.bfgs:ro \
  butterfly-geocode:latest

# Health check.
curl http://localhost:8080/health

# Forward query.
curl 'http://localhost:8080/geocode?q=Rue+Wayez+122+Anderlecht&country=BE'

# Prometheus scrape.
curl http://localhost:8080/metrics

# Stop gracefully (SIGTERM → drains in-flight requests, defaults to a
# 30 s drain timeout — see `--shutdown-timeout-secs`).
docker stop butterfly-geocode

# Logs.
docker logs -f butterfly-geocode
```

The image runs as a non-root `butterfly` user. Bind-mount the shard as read-only (`:ro`).

## Production knobs (`butterfly-geocode serve`)

| Flag                          | Default       | Purpose                                                    |
|-------------------------------|---------------|------------------------------------------------------------|
| `--shard <PATH>`              | —             | Path to the BFGS shard file.                               |
| `--port <N>`                  | 3003          | TCP port to bind. Container defaults to 8080.              |
| `--host <IP>`                 | 0.0.0.0       | Bind address.                                              |
| `--log-format text\|json`     | text          | Tracing subscriber format. JSON in containers.             |
| `--rate-limit-per-sec <N>`    | 100           | Per-IP requests-per-second steady state.                   |
| `--rate-limit-burst <N>`      | 200           | Per-IP burst capacity.                                     |
| `--request-timeout-secs <N>`  | 30            | Server-side per-request timeout.                           |
| `--shutdown-timeout-secs <N>` | 30            | Max drain time after SIGTERM/SIGINT before forced exit.    |
| `--max-body-bytes <N>`        | 4096          | POST/PUT body cap. GETs are unaffected.                    |
| `--rerank-model <PATH>`       | — (off)       | Optional GBDT reranker model.                              |
| `--parser heuristic\|neural`  | heuristic     | Parser backend.                                            |
| `--model <PATH>`              | — (heuristic) | Required when `--parser=neural`.                           |

The HTTP-level rate limit (`tower_governor`) sits in front of the #97 cost-based admission control: governor drops abusive clients on raw request rate, admission gates on per-query work. Both are needed.

CORS is permissive by default (`Access-Control-Allow-Origin: *`, all methods, all headers). Production deployments with browser clients should narrow `Access-Control-Allow-Origin` via a reverse proxy that re-injects the policy.

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

## Multi-country support

The geocoder is built and deployed as **one shard per country**. Operators choose which country shards to build and which to serve.

| Country | ISO2 | Postcode shape  | Status                    |
|---------|------|-----------------|---------------------------|
| Belgium       | `BE` | `\d{4}`              | Verified (test dataset) |
| France        | `FR` | `\d{5}`              | OSM `addr:*` build-ready |
| Netherlands   | `NL` | `\d{4}\s?[A-Z]{2}`   | Verified (1.4 GB PBF)    |
| Luxembourg    | `LU` | `(L-)?\d{4}`         | Verified (47 MB PBF)     |
| Germany       | `DE` | `\d{5}`              | OSM `addr:*` build-ready (5 GB PBF — operator-supplied) |
| Austria       | `AT` | `\d{4}`              | OSM `addr:*` build-ready |
| Switzerland   | `CH` | `\d{4}`              | OSM `addr:*` build-ready |

### Build all shards in one pass

```bash
butterfly-geocode build-shards-all \
    --pbf-dir data \
    --out-dir geocode/regions/multi
# Looks for <country>.pbf or <iso2>.pbf in --pbf-dir for each
# supported country. Missing PBFs are skipped with a warning.
```

### Build one country at a time

```bash
butterfly-geocode build-shard \
    --pbf data/netherlands.pbf \
    --out geocode/regions/multi/nl.bfgs \
    --country NL
```

### Serve a multi-country deployment

```bash
butterfly-geocode serve \
    --shard-dir geocode/regions/multi \
    --port 3033
```

### Authoritative sources

Belgium ships with **first-class BOSA BeSt ingestion** as of 2026-05-04. Other countries still default to OSM `addr:*` tags pending their authoritative-source loaders (BAN for FR, BAG for NL, BD-Adresses for LU, BEV for AT, swisstopo for CH). See `geocode-data/SOURCES.md` for field mappings and the per-country importer roadmap.

OSM coverage varies materially: Netherlands is dense (≈9.9 M `addr:*`-tagged objects), Belgium OSM is dense (≈4.0 M), Luxembourg is sparse (≈170 K — most addresses live in BD-Adresses, not OSM). Operators with stricter coverage requirements should use the authoritative source where available.

## Authoritative source: BOSA BeSt (Belgium)

[BeSt Address](https://opendata.bosa.be/) is the Belgian Federal Public Service BOSA's open address dataset (~6.7 M physical addresses, monthly cadence, Belgian Open Data License — CC-BY-compatible). It has materially better coverage than OSM `addr:*` tags:

| Shard | Records | Unique postcodes | Unique streets |
|---|---|---|---|
| OSM-only (PBF tags) | 4 026 754 | 1 723 | 87 903 |
| BOSA-only (3 regional ZIPs merged, NL+FR aliases) | 10 667 558 | 1 145 | 95 704 |
| BOSA + OSM merged | 13 263 831 | 1 751 | 105 082 |

(BOSA's "1 145 unique postcodes" is the actual Belgian postcode universe; OSM's 1 723 includes typo'd / mis-tagged variants.)

### Build a BOSA shard

```bash
# 1. Fetch all three BOSA regional ZIPs (Flanders / Wallonia / Brussels)
butterfly-dl belgium --only addresses
#   → data/belgium/addresses/bosa-bevlg.zip   (~152 MB)
#   → data/belgium/addresses/bosa-bewal.zip   (~60 MB)
#   → data/belgium/addresses/bosa-bebru.zip   (~17 MB)

# 2. Build a per-region shard (~30 s each, opens the ZIP transparently)
butterfly-geocode build-shard --csv data/belgium/addresses/bosa-bevlg.zip \
    --out belgium-bosa-vlg.bfgs --country BE --source bosa
butterfly-geocode build-shard --csv data/belgium/addresses/bosa-bewal.zip \
    --out belgium-bosa-wal.bfgs --country BE --source bosa
butterfly-geocode build-shard --csv data/belgium/addresses/bosa-bebru.zip \
    --out belgium-bosa-bru.bfgs --country BE --source bosa

# 3. Merge into a single Belgium-wide BOSA shard
butterfly-geocode build-shard \
    --merge belgium-bosa-vlg.bfgs \
    --merge belgium-bosa-wal.bfgs \
    --merge belgium-bosa-bru.bfgs \
    --out belgium-bosa.bfgs --country BE
```

### Combine BOSA with OSM (merged shard)

BOSA has the addresses BOSA knows about. OSM knows about new buildings, recently mapped places, and a few address conventions BOSA doesn't index. The `--merge` mode combines both: where they agree on (postcode, street, housenumber) within ~30 m, the BOSA record wins (it is authoritative); where they disagree or only one has the address, both survive.

```bash
butterfly-geocode build-shard \
    --merge belgium-bosa.bfgs \
    --merge belgium-osm.bfgs \
    --out belgium-merged.bfgs --country BE
```

The per-record source byte (BFGS v4) survives the merge so `/geocode` results carry their provenance. Operators auditing geocode outputs can filter by source via the per-record source field.

### How the loader works

`geocode/src/sources/bosa.rs` streams the CSV directly out of the BOSA ZIP (no decompress-to-disk step). For each `current` row, it emits one `AddressRecord` per non-empty language (NL / FR / DE) — Brussels rows typically yield 2 records (FR + NL); Flanders yields 1 (NL); Wallonia 1 (FR); the German-Belgium DE column is set on the ~70 k records in the East-Cantons. Coordinates are read from `EPSG:4326_lat/lon` so no Lambert-72 reprojection is needed.

The `box_number` column folds into `housenumber` as `"475 bte RDC"` — Belgian box-number convention (apartment / floor identifier) preserved without a separate field.

## What's deferred (still in #96/#97/#98)

- **Byte-level transformer parser** (#96 §Tagger, #98 Phase 2) — the heuristic in `parser/heuristic.rs` is the deterministic Phase 0 baseline. The byte-level transformer + #98 Phase 1 retrieval-aware decoding ship in `parser/neural.rs`.
- **Authoritative-source ingestion** for **non-Belgium countries** (BAN for FR, BAG for NL, BD-Adresses for LU, BEV for AT, swisstopo for CH) — `geocode-data/SOURCES.md` has the URLs + field mappings ready. **Belgium BOSA BeSt ships in this release** (see "Authoritative source" section below).
- **Cross-border shard co-location** (#96 §Cross-Border Shard Co-location) — separate per-country shards instead. The layout-merge is a future optimization for the BE-FR-NL-LU-DE cluster.
- **Adapter layers (LoRA per region)** (#96 §Tagger) — needs trained models.
- **Feedback operators** (`Downgrade`, `TopkMerge`, `Sample`) — types defined per #96, not invoked by the MVP executor.

## Build and run (Belgium-only)

```bash
# 1. Get the Belgium PBF (if not already there)
butterfly-dl belgium --only pbf -o data/belgium.pbf

# 2. Build the shard (~1 minute on Belgium scale)
cargo run --release -p butterfly-geocode -- build-shard \
    --pbf data/belgium.pbf \
    --out geocode/regions/belgium.bfgs \
    --country BE

# 3. Boot the server (single-country mode)
cargo run --release -p butterfly-geocode -- serve \
    --shard geocode/regions/belgium.bfgs \
    --port 3003

# 4. Try it
curl 'http://localhost:3003/geocode?q=Rue+Wayez+122+Anderlecht&country=BE'
curl 'http://localhost:3003/geocode/reverse?lat=50.8467&lon=4.3525'
curl -H 'Accept: application/geo+json' \
    'http://localhost:3003/geocode?q=Grote+Markt+Antwerpen'
```

## Build and run (multi-country)

```bash
# 1. Get one PBF per country you want to deploy
curl -o data/luxembourg.pbf https://download.geofabrik.de/europe/luxembourg-latest.osm.pbf
curl -o data/netherlands.pbf https://download.geofabrik.de/europe/netherlands-latest.osm.pbf

# 2. Build a shard per country
mkdir -p geocode/regions/multi
butterfly-geocode build-shard --pbf data/belgium.pbf      --out geocode/regions/multi/be.bfgs --country BE
butterfly-geocode build-shard --pbf data/luxembourg.pbf   --out geocode/regions/multi/lu.bfgs --country LU
butterfly-geocode build-shard --pbf data/netherlands.pbf  --out geocode/regions/multi/nl.bfgs --country NL

# Or build everything in one pass:
butterfly-geocode build-shards-all --pbf-dir data --out-dir geocode/regions/multi

# 3. Boot the multi-shard server
butterfly-geocode serve --shard-dir geocode/regions/multi --port 3033

# 4. Pinned country (clean-query path)
curl 'http://localhost:3033/geocode?q=Damrak+1+1012+LP+Amsterdam&country=NL'

# 5. Auto-routed (classifier picks NL from "Damrak ... Amsterdam")
curl 'http://localhost:3033/geocode?q=Damrak+1+1012+LP+Amsterdam'

# 6. Reverse with bbox dispatch (50.8467,4.3525 → BE)
curl 'http://localhost:3033/geocode/reverse?lat=50.8467&lon=4.3525'

# 7. Health endpoint lists loaded countries
curl 'http://localhost:3033/health'
# {"status":"ok",...,"countries":["BE","LU","NL"]}
```

## Testing

```bash
# Multi-country end-to-end (requires shards in regions/multi/)
cargo test --release -p butterfly-geocode --test multi_country_e2e -- --ignored
```

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
  "record_count": 4026754,
  "shard_count": 1,
  "total_records": 4026754
}
```

`record_count` is retained for backwards compatibility and equals
`total_records` in single-shard mode. `shard_count` is plumbed through
ahead of multi-shard support so dashboards built today don't break
when #96 lands.

### `GET /metrics`

Prometheus exposition.

## gRPC Arrow Flight (#145)

The geocoder ships a **gRPC Arrow Flight** transport alongside REST, per the workspace transport policy ([#145](https://github.com/butterfly-osm/butterfly-osm/issues/145)). Bulk geocoding workloads — data-pipeline backfills, address-book uploads, ETL — hit REST limits hard (URL length, JSON parsing overhead, no native batching). Flight gives columnar Arrow IPC with backpressure, cancellation on disconnect, and a 10–100× throughput gain vs equivalent REST loops at scale.

### Default ports + transport selection

```bash
# Both transports (default)
butterfly-geocode serve --shard <path> --transport=both
# REST  → 0.0.0.0:3003
# gRPC  → 0.0.0.0:3004

# REST only (back-compat with single-port deployments)
butterfly-geocode serve --shard <path> --transport=rest --rest-port 3003

# gRPC only
butterfly-geocode serve --shard <path> --transport=grpc --grpc-port 3004
```

The legacy `--port` flag still works as the REST port (kept so existing run scripts keep working when transport defaults to `both`).

### Action: `geocode_batch`

Submit a batch via `DoExchange` — the canonical bulk path. The descriptor's `cmd` carries the action name + JSON params, and the request's FlightData stream carries the input RecordBatch:

- **Descriptor cmd**: `geocode_batch[:<params_json>]`
  - `limit` (u32, default 5) — top-K per query (only the top-1 is emitted in the output row today)
  - `include_debug` (bool, default false) — emit `reason_codes`
  - `group_by_country` (bool, default false) — group by country before rayon dispatch (improves per-country cache locality)
- **Input schema**:
  - `query: Utf8` (required, non-null)
  - `country: Utf8` (nullable, ISO 3166-1 alpha-2)
- **Output schema** (one row per input query, `query_idx` ascending):
  - `query_idx: UInt32` — original input row index
  - `lat: Float64` (nullable — null on no-result)
  - `lon: Float64` (nullable)
  - `score: Float32` (nullable)
  - `confidence: Utf8` — one of `accept` / `caution` / `review` / `reject` / `empty`
  - `street: Utf8` (nullable)
  - `housenumber: Utf8` (nullable)
  - `postcode: Utf8` (nullable)
  - `locality: Utf8` (nullable)
  - `country: Utf8` (nullable)
  - `reason_codes: List<Utf8>` (always present; empty unless `include_debug=true`)

Up to 500 000 queries per call. Output is streamed in 1024-row RecordBatch chunks so latency-to-first-byte is low and the server's resident set stays bounded under heavy load. Cooperative cancellation: when the client drops the response stream, processing stops within ~one chunk.

### CLI: `flight-batch`

A built-in client subcommand for ops + smoke testing:

```bash
# JSONL input: one {"query": "...", "country": "..."} per line
echo '{"query":"Rue de la Loi 16, 1000 Bruxelles","country":"BE"}'  > queries.jsonl
echo '{"query":"Grand-Place 1 Bruxelles"}'                          >> queries.jsonl

butterfly-geocode flight-batch \
  --endpoint http://localhost:3004 \
  --queries  queries.jsonl \
  --output   results.arrow \
  --limit    3
```

The output is an **Arrow IPC stream** — load it directly with pyarrow / pandas / polars / DataFusion without a JSON parsing round-trip:

```python
import pyarrow.ipc as ipc
with ipc.open_stream("results.arrow") as r:
    table = r.read_all()
    print(table.to_pandas())
```

### Why this is symmetric with REST

Per the [#145 transport policy](https://github.com/butterfly-osm/butterfly-osm/issues/145), every server route must be reachable via REST OR gRPC. The geocoder's batch use case is bulk-Arrow-shaped — encoding 500 k JSON request bodies would dominate the response time — so Flight is the canonical bulk endpoint. Single-query lookups go through REST (`GET /geocode`); bulk goes through Flight. No transport mixing on either server: REST stays JSON, Flight stays Arrow.

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
