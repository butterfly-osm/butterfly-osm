# geocode-training

Training corpus tooling for `butterfly-geocode` (#96 Phase 2 prerequisite,
#98 Phase 2 corpus dependency).

This directory does NOT train anything. It produces the (text, BIO,
country) supervision corpus that the future Phase 2 trainer will consume.
The trainer itself is out of scope — it lands when the geocode crate is
mature enough to define a proper trainer pipeline.

## Layout

- `corpus-gen/` — Rust binary, reads OSM PBF, emits JSONL training
  samples. Standalone crate (own `[workspace]` table).
- `fixtures/` — committed sample outputs for code review.
  - `belgium-sample.jsonl` — 1001 training records produced by running
    `corpus-gen` on `data/belgium.pbf` with `--sample 1000`.
  - `belgium-canary-sample.jsonl` — held-out cross-shard canary records
    produced in the same run.
- `README.md` — this file.

## Augmentation strategies

`corpus-gen/src/augment.rs` implements 10 strategies:

| Index | Kind | What it does |
|---|---|---|
| 0 | `reorder_postcode_first` | "1070 Anderlecht Rue Wayez 122" |
| 1 | `drop_postcode` | "Rue Wayez 122, Anderlecht" |
| 2 | `drop_city` | "Rue Wayez 122, 1070" |
| 3 | `abbr_contract` | "R. Wayez 122, 1070 Anderlecht" |
| 4 | `abbr_expand` | "Boulevard ..." (when the gold has "Bd.") |
| 5 | `case_upper` | "RUE WAYEZ 122, 1070 ANDERLECHT" |
| 6 | `case_lower` | "rue wayez 122, 1070 anderlecht" |
| 7 | `ws_noise` | extra commas, double spaces, em-dashes |
| 8 | `typo_injection` | one-character ASCII edit inside a span |
| 9 | `combined_case_abbr` | abbr + case combined |

Plus held-out cross-shard canaries:

- `canary_be_as_fr` — Dutch street type ("straat") rewritten to French
  ("rue") to test that a parser trained on BE doesn't memorize Dutch
  morphology.
- `canary_be_as_nl` — French street type ("rue") rewritten to Dutch
  ("straat") for the inverse test.

Canary records are written to a SEPARATE file and held out from training.

## Running

The corpus generator depends on a Belgium OSM PBF. The parallel
data-prep agent owns `dl/regions/` and `data/`; that PBF is expected
at `data/belgium.pbf`.

```bash
cd geocode-training/corpus-gen
cargo build --release
./target/release/corpus-gen \
    --pbf ../../data/belgium.pbf \
    --out /tmp/corpus-belgium.jsonl \
    --canary /tmp/canary-belgium.jsonl \
    --bench-queries ../../bench/geocode/queries/belgium.tsv \
    --country BE \
    --augmentations 10
```

Belgium full run (169K gold records, 1.82M training records, 3.3K canary
records) takes ~9 seconds on a laptop.

## Validation

The corpus is byte-aligned: every JSONL record's `bio_labels` length
equals the byte length of `text`. The fixture passes this check (1001 ok,
0 bad). Validation script:

```bash
python3 -c "
import json
ok=bad=0
for line in open('geocode-training/fixtures/belgium-sample.jsonl'):
  r = json.loads(line)
  if len(r['text'].encode('utf-8')) != len(r['bio_labels']):
    print('LENGTH MISMATCH:', r['text'])
    bad += 1
  else:
    ok += 1
print('ok:', ok, 'bad:', bad)
"
```

## Open design points

See `geocode-research/CORPUS_DESIGN_NOTES.md` for the open items the
Phase 2 trainer should address — country-head loss masking on ambiguous
variants, lower N=6 default, provenance-based labeling for online
augmentation.

## What this is NOT

- It is not the trainer. The trainer is Phase 2 of #98 and lands later.
- It is not the dataset. The dataset is the JSONL output of running this
  binary on the per-country PBF that the parallel data-prep agent
  maintains.
- It is not the parser. The parser is in `geocode/` (parallel agent's
  territory) and consumes the trained model artifact.
