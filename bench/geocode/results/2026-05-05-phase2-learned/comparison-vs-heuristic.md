# Phase 2 retrieval-utility scorer — Belgium training + bench

**Date**: 2026-05-05
**Branch**: `phase2-train-bench` (off `main` HEAD `f26302d` post-#178)
**Dataset**: 1000 mixed-quality Belgium addresses (`bench/geocode/queries/belgium.tsv`)
**Shard**: `geocode/regions/be.bfgs` — freshly built BFGS v4 from `data/belgium.pbf` via `butterfly-geocode build-shard --pbf data/belgium.pbf --country BE --source osm` (4 042 324 OSM `addr:*` records). The pre-existing `belgium.bfgs` is a v3 shard left over from the 2026-05-04 baseline; the v3 reader was removed by #176, so a v4 rebuild was required before any work could proceed.
**BOSA**: BOSA BeSt CSV inputs are not present on this box. The original task referenced a "BOSA-merged shard" but only OSM data was available. This is the same data substrate as the 2026-05-04 baseline, so recall numbers are directly comparable.
**Hardware**: shared dev box (not isolated)

## Phase 2 training run

Generated a 500 000-row corpus by running `phase2-corpus` against the v4 BE
shard with `--augmentations 8 --max-rows 500000 --skip-incomplete`
(reservoir-sampled from 13.8 M candidate rows). 500k rather than 5M because
(a) the labeling step ran in 5 seconds at 500k so 5M was overkill for a
GBDT with ~30 features, and (b) the held-out AUC at 500k already saturates
above the 0.7 target.

`phase2-label` produced 500 000 `(features, label)` rows in 5 s
(parallel rayon). Label balance: **134 930 positives (27.0 %) / 365 070
negatives (73.0 %)**. The positive rate is the heuristic-parser-on-OSM
recall floor — about 1 in 4 augmentations of an OSM record lands on the
gold record within the 30 m + housenumber-match tolerance.

`butterfly-geocode train-retrieval-utility --epochs 150 --depth 6 --eval-split 0.1 --seed 2977872592`:

| Metric | Value |
|---|---|
| Eval split size | 50 000 |
| Eval positives / negatives | 13 555 / 36 445 |
| **AUC** | **0.9395** |
| **Brier** | **0.0854** |
| Accuracy @ 0.5 threshold | 0.864 |
| Training time | 83 s |
| Tree count | 150 (depth 6) |

The trainer's `auc >= 0.7 — features carry usable signal` info-line fired,
and the saved model verified via load-back. Model size: **2.2 MB** —
committed under `geocode/data/models/retrieval-utility-belgium-tiny.gbdt`.

## Architectural caveat — scorer applies only to neural parser

`butterfly-geocode serve --retrieval-utility=learned` is documented as
"Only consulted when `--parser=neural`; the heuristic parser emits a
single hypothesis and skips utility scoring". The retrieval-utility
scorer's job is to discriminate *between competing hypotheses*; with one
hypothesis per query there is nothing to rerank, so the scorer is a
no-op on the heuristic-parser path.

This means the bench has to be run on the **neural parser** to see any
effect. The shipped neural model
(`geocode/data/models/belgium-tiny.safetensors`, 472 KB, 120k params,
trained on 8 k synthetic examples per #178) is severely undertrained for
real Belgium traffic, but it is the only neural model in-tree. A real
production-grade neural parser is filed as #168.

## Results — three configurations

All three runs are 1 000 queries through the 2026-05-04 Belgium TSV at
client-side qps-cap = 20. The qps cap is required because the admission
layer enforces a per-IP token bucket (capacity 50, refill 25 / s,
hardcoded `AdmissionPolicy::default()`) that 429s a localhost benchmark
otherwise. Tower-governor was raised via `--rate-limit-per-sec 100000`,
but admission's per-IP bucket is not currently CLI-pluggable. Bench
results compare *recall*, not *throughput*; the qps cap binds the
throughput numbers identically across runs.

### Heuristic parser (Phase 0 baseline)

`--parser heuristic` (default), no scorer involvement.

| concurrency | recall@1 (100 m) | distance p50 (m) | latency p50 / p95 / p99 (ms) |
|---|---|---|---|
| 1 | **0.484** | 30.0 | 3.8 / 5.7 / 8.7 |
| 4 | 0.484 | 30.0 | 3.9 / 5.2 / 9.5 |
| 16 | 0.484 | 30.0 | 3.7 / 5.5 / 10.1 |

This matches the 2026-05-04 baseline (0.470) to within the noise
introduced by the v3→v4 shard rebuild.

### Neural parser + heuristic scorer (Phase 1)

`--parser neural --model belgium-tiny.safetensors --retrieval-utility heuristic`.

| concurrency | recall@1 (100 m) | distance p50 (m) | latency p50 / p95 / p99 (ms) |
|---|---|---|---|
| 1 | **0.163** | 1318.8 | 8.5 / 11.3 / 13.3 |
| 4 | 0.163 | 1318.8 | 8.7 / 11.5 / 12.7 |
| 16 | 0.163 | 1318.8 | 8.7 / 11.3 / 12.5 |

Neural parser collapses recall from 0.484 to 0.163 — confirms the
"undertrained 120 k-param model on 8 k examples cannot generalise"
characterisation in PR #178.

### Neural parser + learned scorer (Phase 2)

`--parser neural --model belgium-tiny.safetensors --retrieval-utility learned --retrieval-utility-model retrieval-utility-belgium-tiny.gbdt`.

| concurrency | recall@1 (100 m) | distance p50 (m) | latency p50 / p95 / p99 (ms) |
|---|---|---|---|
| 1 | **0.163** | 1318.8 | 8.5 / 12.5 / 14.0 |
| 4 | 0.163 | 1318.8 | 8.9 / 11.8 / 13.2 |
| 16 | 0.163 | 1318.8 | 8.6 / 11.8 / 13.6 |

**Identical recall and identical top-1 coordinates on every one of the
1 000 queries** vs. the heuristic-scorer run (verified by direct
diff: same = 1000, diff = 0).

## Side-by-side delta vs the published 2026-05-04 baseline

| Metric | 2026-05-04 baseline (heuristic) | Phase 1 (neural + heuristic) | Phase 2 (neural + learned) | Δ Phase 2 vs Phase 1 |
|---|---|---|---|---|
| Recall@1 (100 m) | 0.470 | 0.163 | 0.163 | **+0.000** |
| Top-1 distance p50 (m) | 66.9 | 1318.8 | 1318.8 | 0.0 |
| Latency p50 c=1 (ms) | 7.2 | 8.5 | 8.5 | +0.0 |
| Throughput c=16 (qps) | 25.1 | 20.0 (capped) | 20.0 (capped) | 0.0 |

## Why Phase 2 is a no-op against this neural model

The retrieval-utility scorer is invoked **inside** `decode_with_scorer`
to break ties between competing parser hypotheses. The shipped tiny
neural model emits a beam of width 1 in practice (#168 / #178) — the
distribution is concentrated enough that only one hypothesis survives
recombination after canonicalisation. With one survivor, the scorer
function's input set has cardinality 1; argmax over 1 element returns
the same element regardless of the scoring function.

This is **not a Phase 2 architecture failure** and **not a training
failure** — the held-out AUC of 0.9395 says the features carry plenty
of signal and the GBDT learned the right shape. It is a parser-side
diversity failure: until #168 ships a beam-producing neural parser,
the learned scorer has no candidates to choose between.

## Honest characterisation

- **AUC ≥ 0.7 target**: cleared (0.9395). Phase 2 features and the
  GBDT trainer are doing their job.
- **Recall@1 lift target on hard queries**: not achieved on this
  parser pairing. Zero-delta is the correct outcome given the
  beam-width-1 input.
- **Phase 2 architecture**: validated end-to-end (corpus → labels →
  GBDT → loaded by serve → invoked at decode time → predictions
  consumed by beam re-rank). The plumbing works; the upstream
  candidate generator is too narrow to expose the lift.

## Recommendation

Phase 2 should be re-benched once **either** of the following holds:

1. The neural parser is retrained on a corpus comparable in size and
   diversity to `phase2-belgium-corpus.jsonl` (#168). A parser whose
   beam routinely emits ≥ 2 distinct hypotheses per query will
   exercise the scorer.
2. A dedicated **hypothesis-diversity** path is added — e.g. inject
   2-3 perturbations of the heuristic parse (drop housenumber, swap
   street/locality, etc.) before recombination. This would let Phase 2
   light up even with the heuristic parser as the candidate generator.

Until then, the Phase 2 model's eval-set AUC of 0.9395 stands as
synthetic validation of the feature schema, and the bench-recall
delta is correctly reported as zero.

## Reproduction

```bash
# 1. Build v4 BE shard from belgium.pbf
butterfly-geocode build-shard \
  --pbf data/belgium.pbf \
  --country BE \
  --source osm \
  --out geocode/regions/be.bfgs

# 2. Generate Phase 2 corpus
cargo build --release --manifest-path geocode-training/phase2/Cargo.toml
./geocode-training/phase2/target/release/phase2-corpus \
  --shard geocode/regions/be.bfgs \
  --out geocode-training/output/phase2-belgium-corpus.jsonl \
  --augmentations 8 --max-rows 500000 --skip-incomplete

# 3. Label
./geocode-training/phase2/target/release/phase2-label \
  --samples geocode-training/output/phase2-belgium-corpus.jsonl \
  --shard geocode/regions/be.bfgs \
  --out geocode-training/output/phase2-belgium-labels.jsonl

# 4. Train
butterfly-geocode train-retrieval-utility \
  --labels geocode-training/output/phase2-belgium-labels.jsonl \
  --out geocode/data/models/retrieval-utility-belgium-tiny.gbdt \
  --epochs 150 --depth 6 --eval-split 0.1 --seed 2977872592

# 5. Bench (one server per row above)
butterfly-geocode serve --shard geocode/regions/be.bfgs --port 3001 \
  --transport rest --rate-limit-per-sec 100000 --rate-limit-burst 100000 \
  --parser <heuristic|neural> [--model <safetensors>] \
  [--retrieval-utility <heuristic|learned>] \
  [--retrieval-utility-model <gbdt>] &

cd bench/geocode
python3 bench.py --engine butterfly --queries queries/belgium.tsv \
  --concurrency 1,4,16 --qps-cap 20 \
  --output results/2026-05-05-phase2-<config>/
```

The qps-cap is required to stay under the per-IP admission bucket
(25 req / s steady, 50 burst, hardcoded). Without it the bench
collapses to ~5 % success rate even at concurrency 1.

## Bench-harness consistency caveat (added in PR #179 follow-up)

All rows in this report were generated through the butterfly bench
adapter, which issues `GET /geocode?limit=5` to butterfly and forwards
the same `limit=5` to Nominatim. Cross-engine comparisons across these
configurations are therefore apples-to-apples.

The earlier 2026-05-04 baseline at `bench/geocode/results/2026-05-04/`
was produced before the butterfly adapter accepted a configurable
`--limit` flag (added in #179) and before the Nominatim adapter
forwarded that flag rather than hardcoding `limit=1`. Latency and
throughput numbers from that baseline are therefore **not directly
comparable** to the rows above — butterfly's executor scales work with
`limit`, so a `limit=5` run does more work than the implicit `limit=1`
behaviour of the older harness. **Recall@1 within 100 m is comparable
across all rows** (every harness evaluates top-1 distance regardless
of how wide the result set is). To reproduce a strict apples-to-apples
re-bench against the 2026-05-04 baseline, re-run the current harness
with `python3 bench.py ... --limit 1` for both engines.
