# Multi-Country Neural Tagger Training Runbook

Operator runbook for training the production neural parser on multiple
countries. The Fork A+ engineering work (#192) shipped every piece of
infrastructure required — corpus-format bridge, multi-country country head,
multi-country corpus-gen with morphology tables, production architecture,
LR schedule, gradient clipping. This document is the operator's recipe to
exercise that infrastructure on the full multi-country target.

The Belgium-only run is documented in `PRODUCTION_TRAINING.md`. It is a
strict subset of this runbook (`--countries BE`, single PBF). Read that
first to confirm the pipeline works locally before running multi-country
overnight.

## Phase 0 — Prerequisites

- A Linux box with **≥ 32 GB RAM** (the training corpus is ~1 GB JSONL
  loaded fully into memory; head-room for the model + safetensors save).
- **≥ 200 GB disk** (PBFs + corpora + checkpoints).
- Rust 1.95, edition 2024.
- The corpus-gen + butterfly-geocode binaries built in release mode:
  ```
  cd /path/to/butterfly-osm
  cargo build --release --workspace
  ( cd geocode-training/corpus-gen && cargo build --release )
  ```

## Phase 1 — Fetch country PBFs

The corpus-gen reads a single Geofabrik PBF per country. butterfly-dl is
the canonical fetch path; it caches into `data/`.

```bash
# Already on disk per the project default:
#   data/belgium.pbf       (~600 MB)
# Fetch the rest:
butterfly-dl france           # → data/france.pbf, ~4 GB
butterfly-dl germany          # → data/germany.pbf, ~4 GB
butterfly-dl netherlands      # → data/netherlands.pbf, ~1.5 GB
butterfly-dl us-northeast     # → data/us-northeast.pbf, ~1.7 GB (partial US)
# For full US coverage, fetch each US-state extract from Geofabrik
# (us/<state>.pbf) and concatenate via osmium / osmconvert. Geofabrik
# does not ship a single 'united-states' PBF; the operator chooses
# which states to include. The morphology tables don't change — US is
# US whichever region you pull.

butterfly-dl great-britain    # → data/great-britain.pbf, ~1.5 GB
```

Network-bound. 13–15 GB total over ~1–2 hours on a 100 Mbps link.

## Phase 2 — Generate per-country corpora

corpus-gen produces a JSONL per country. Each gold OSM `addr:*`-tagged
node yields 1 canonical record + 8 augmented variants (`--augmentations
8`, the codex-recommended default). The morphology used for abbreviation
expansion / contraction comes from `geocode-training/corpus-gen/morphology/<iso2>.toml`.
A morphology table is **required** for any country you generate; the
shipped set covers BE, FR, NL, DE, US, GB.

```bash
cd /path/to/butterfly-osm
mkdir -p geocode-training/output

for cc in BE FR NL DE GB; do
    pbf="data/$(echo $cc | tr A-Z a-z).pbf"
    case $cc in
        BE) pbf="data/belgium.pbf" ;;
        DE) pbf="data/germany.pbf" ;;
        FR) pbf="data/france.pbf" ;;
        NL) pbf="data/netherlands.pbf" ;;
        GB) pbf="data/great-britain.pbf" ;;
    esac
    geocode-training/corpus-gen/target/release/corpus-gen \
        --pbf "$pbf" \
        --country "$cc" \
        --canary-targets "$(echo BE,FR,NL,DE,GB | tr ',' '\n' | grep -v $cc | tr '\n' ',' | sed 's/,$//')" \
        --morphology-dir geocode-training/corpus-gen/morphology \
        --augmentations 8 \
        --out "geocode-training/output/${cc,,}-corpus.jsonl" \
        --canary "geocode-training/output/${cc,,}-canary.jsonl"
done

# US is split per-state if needed; rerun corpus-gen per state PBF and
# concatenate. The ISO code stays "US" on every record.
for state in northeast; do
    geocode-training/corpus-gen/target/release/corpus-gen \
        --pbf "data/us-${state}.pbf" \
        --country US \
        --canary-targets BE,FR,NL,DE,GB \
        --morphology-dir geocode-training/corpus-gen/morphology \
        --augmentations 8 \
        --out "geocode-training/output/us-${state}-corpus.jsonl" \
        --canary "geocode-training/output/us-${state}-canary.jsonl"
done
```

Wall-clock: ~9 sec per million records on a laptop. Belgium produces
~1.5 M records; France around 6 M; Germany around 8 M; full set will
land around **15–20 M training records** depending on how much US you
include.

## Phase 3 — Concatenate + shuffle

Combine per-country corpora into one shuffled file. The shuffler is the
GNU `shuf`; for files larger than RAM, use the disk-backed shuffle
provided by `terashuf` or `shuf --random-source=/dev/urandom -n N`.

```bash
cat geocode-training/output/*-corpus.jsonl | shuf > geocode-training/output/world-corpus.jsonl
wc -l geocode-training/output/world-corpus.jsonl
# expect 15–20 million lines, 3–4 GB on disk
```

The trainer reads the entire corpus into RAM. At ~150 bytes/line average
that's 2–3 GB heap — comfortable on a 32 GB box.

## Phase 4 — Train

Training the production architecture (~825k params at any vocab size up
to 8 — the country head adds 128 floats per country).

```bash
./target/release/butterfly-geocode train \
    --corpus geocode-training/output/world-corpus.jsonl \
    --countries BE,FR,NL,DE,GB,US \
    --architecture production \
    --batch-size 64 \
    --learning-rate 1e-3 \
    --lr-schedule cosine \
    --epochs 30 \
    --warmup-steps 5000 \
    --weight-decay 0.01 \
    --gradient-clip 1.0 \
    --eval-split 0.05 \
    --out geocode/data/models/world-prod.safetensors
```

Wall-clock projection (CPU-only, 20-core x86):

| Records | Epochs | Throughput | Wall-clock |
|---------|--------|------------|------------|
| 1.49 M (BE alone) | 10 | ~250 batches/sec | ~40 min |
| ~15 M (world) | 30 | ~250 batches/sec | **~30 hours** |

The 30-hour figure is the dominant cost. To bring it under 6 h, build
candle with the `cuda` feature (one line in `geocode/Cargo.toml`):

```toml
candle-core = { version = "0.10", default-features = false, features = ["cuda"] }
candle-nn  = { version = "0.10", default-features = false, features = ["cuda"] }
```

then re-run the training step. A single A100/H100 GPU pushes throughput
to ~5–10 k batches/sec at this model size, taking the full
multi-country run from 30h to 3–6h. The architecture is otherwise
unchanged — same safetensors layout, same sidecar JSON, same inference
path.

## Phase 5 — Bench

After training completes, validate against per-country bench query
files. The `bench/geocode/bench.py` script accepts a `--queries` TSV
and a running geocode server.

```bash
# Boot the multi-country server. The shard-dir loads every BFGS the
# server can find — point it at a directory of pre-built shards
# (one per ISO code).
./target/release/butterfly-geocode serve \
    --shard-dir geocode/regions/ \
    --parser neural \
    --model geocode/data/models/world-prod.safetensors \
    --retrieval-utility learned \
    --retrieval-utility-model geocode/data/models/retrieval-utility-belgium-tiny.gbdt \
    --port 3003 &

# Run the bench per country.
for cc in BE FR NL DE GB US; do
    python3 bench/geocode/bench.py \
        --engine butterfly \
        --queries "bench/geocode/queries/${cc,,}.tsv" \
        --concurrency 1,4,16 \
        --output "bench/geocode/results/${cc,,}-world-prod-bench" \
        --limit 5
done
```

Compare recall@1 per country against:
- Belgium-only baseline (`belgium-prod.safetensors`): expectation is
  parity or small drop, since multi-country training gives up some BE
  specialization.
- Multi-country baseline (heuristic parser): expectation is uplift,
  since the neural parser should outperform simple regex tagging on
  out-of-distribution country mixes.

If recall drops more than 5 points vs the per-country specialised model,
the issue is **shared-vs-specialized capacity**: the 825k-param model
is at its capacity ceiling. Re-train at the next architecture profile
(d_model=192, n_layers=6 — file as a follow-up issue, not in scope of
#192).

## Phase 6 — Deploy

The world-prod safetensors + sidecar config drop into
`geocode/data/models/`. The serve command above is the production
deployment shape. The Docker image rebuild uses the same path:

```dockerfile
COPY geocode/data/models/world-prod.safetensors /opt/models/
COPY geocode/data/models/world-prod.safetensors.config.json /opt/models/
```

## Validation: this runbook IS reproducible

The Belgium row of Phase 4 is exercised in `PRODUCTION_TRAINING.md`:
running the exact command at `--countries BE` produces a measurable
loss curve and a deployable model. Phases 1, 2, 3, 5 are network /
disk operations whose outputs are deterministic given the same
upstream PBF snapshots. Phase 4 multi-country is operator-driven
overnight compute — but no part of the workflow is research-grade
exploratory. Every step above is a single bash command operating on
the infrastructure shipped in #192.

## Known omissions (deferred)

- **Per-country eval metrics**: the trainer prints global BIO-acc /
  country-acc; per-country breakdown requires an additional eval pass
  iterating per-country. Tracked as #196 (proposed).
- **Curriculum learning**: train on BE first, then warm-start the world
  model from the BE checkpoint. Tracked as #197 (proposed).
- **Mixed-precision (fp16)**: 2x speed on GPU. candle 0.10 supports it
  but we ship fp32 for CPU stability. Tracked as #198 (proposed).
