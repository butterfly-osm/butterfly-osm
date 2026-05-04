# geocode-research

Research and future-phase preparation work for `butterfly-geocode`
(GitHub issues #96, #97, #98). The geocode crate itself is built by a
parallel agent; this directory is the research scaffold.

## Layout

- `ML_STACK_DECISION.md` — candle vs burn comparison, measured latency,
  final pick (candle), reasoning. Read first.
- `GBDT_DECISION.md` — gbdt vs lightgbm3 / xgboost-rs / linfa-trees /
  smartcore, measured latency, final pick (gbdt).
- `EXTERNAL_REVIEW.md` — verbatim codex and gemini outputs. Source of
  truth for the disagreement-and-resolution table at the top.
- `CORPUS_DESIGN_NOTES.md` — open design points for the Phase 2
  trainer, with codex/gemini recommendations consumed.
- `DEAD_ENDS.md` — directions explored and discarded.
- `PROMPT_ML_STACK.txt`, `PROMPT_CORPUS.txt` — the consultation prompts.
- `scratch-candle/`, `scratch-burn/`, `scratch-gbdt/` — minimal
  benchmark binaries. Each has its own `[workspace]` table, opts out
  of the butterfly-osm workspace, and runs standalone.

Output files (not committed):

- `gemini-ml.txt`, `gemini-corpus.txt`, `codex-ml.txt`, `codex-corpus.txt`
  — raw consultation outputs (committed; small, useful for audit).
- `*.err` — consultation stderr; useful for diagnosing quota failures.

## Reproducing the benchmarks

```bash
# ML stack
cd geocode-research/scratch-candle && cargo build --release && taskset -c 0 ./target/release/scratch-candle
cd geocode-research/scratch-burn && cargo build --release && taskset -c 0 ./target/release/scratch-burn

# GBDT
cd geocode-research/scratch-gbdt && cargo build --release && taskset -c 0 ./target/release/scratch-gbdt
```

## Final calls

| Decision | Pick | Source |
|---|---|---|
| Pure-Rust ML framework | **candle 0.10** | `ML_STACK_DECISION.md` |
| Pure-Rust GBDT | **gbdt 0.1.3** | `GBDT_DECISION.md` |
| Augmentation count default | **10** in tool, **6** recommended for trainer | `CORPUS_DESIGN_NOTES.md` |
| Country-head supervision | reduced-weight loss on ambiguous variants | `CORPUS_DESIGN_NOTES.md` |
| ML escape hatch | hand-rolled inference if candle + int8 + MKL still misses target | `ML_STACK_DECISION.md` |
