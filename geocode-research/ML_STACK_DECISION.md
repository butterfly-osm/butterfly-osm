# ML Stack Decision: candle-rs vs burn

## Decision

**Use `candle` (huggingface/candle) for the production geocode tagger.**

This is the recommendation the parser implementer should follow when issue
#96 Phase 1 starts. The reasoning, the measured numbers, and the dissenting
view are all below — read the whole document before challenging the call.

## Measured latency (this machine, Linux x86_64, single CPU thread)

Both scratch projects implement the same shape: byte-level transformer
encoder, 4 layers, d=96, 4 heads, FFN=256, seq=64, vocab=256, plus a 9-way
BIO head and a 200-way country head. **399,729 trainable parameters**
(~1.5 MB f32 weights). No native BLAS — `ndarray` for burn, candle's default
CPU backend for candle. Synthetic input, 1000 forward passes after a
50-iteration warm-up.

| Framework | p50 | p90 | p99 | mean | source |
|-----------|-----|-----|-----|------|--------|
| candle 0.10 | 1123 µs | 1171 µs | 1206 µs | 1130 µs | `geocode-research/scratch-candle/` |
| burn 0.20 (ndarray) | 1773 µs | 2078 µs | 9022 µs | 2060 µs | `geocode-research/scratch-burn/` |

**Candle is 1.6x faster at p50 and 7.5x faster at p99.** The p99 spread on
burn is reproducible and points at allocator / scratch-buffer churn; we did
not chase it down because the median already settles the question for this
workload.

### Honest caveats

- **Neither hits the 50 µs p99 target from #96.** We measured a 400K-param
  model in pure-Rust matmul; the spec talks about a 2-4M-param model with
  int8/4-bit quantization. The gap to <50 µs at full size is ~50x. At the
  measured per-layer cost (~280 µs candle), reaching the target requires
  either: (a) a much smaller model, (b) MKL/Accelerate-backed matmul with
  hot threads pinned, (c) custom Rust inference code, or (d) all three.
  This is a **#96 design item, not a framework choice item** — both
  frameworks land in the same order of magnitude on this CPU.
- We did not bench with MKL on candle or with `ndarray-blas-openblas` on
  burn. Doing so will narrow the gap on both sides; it will not flip the
  ordering. Codex's review (verbatim in `EXTERNAL_REVIEW.md`) confirms
  candle has the more believable CPU-quantized path on x86 (MKL +
  `QMatMul` already used in production-ish code) and Apple (Accelerate).
- The burn build pulls in twice as many transitive crates and takes ~30s
  to compile a fresh release binary. Candle: ~12s. This matters for CI.
- Candle's API churns slightly more between point releases. Burn's API
  churns much more (we hit two API drifts inside the 0.20 series during
  this scratch project — `squeeze` signature change, `.int()` ambiguity).
  Both ship semver point releases; neither is at a "stable" 1.0 contract.

## Side-by-side comparison

| Aspect | candle 0.10 | burn 0.20 | Winner |
|---|---|---|---|
| Inference p50 (this bench, no native BLAS) | 1.1 ms | 1.8 ms | **candle** |
| Inference p99 (this bench) | 1.2 ms | 9.0 ms | **candle** |
| Quantization story (int8/4-bit) | `QMatMul` + GGML/GGUF, real CPU q4/q8 path used by quantized LLM code | `Quantizer` API supports 8/4/2-bit PTQ; docs say "currently in active development" and "supported on some backends" | **candle** (deployable today) |
| Built-in transformer encoder | No — hand-roll attention from `Linear`, softmax, matmul | Yes — `TransformerEncoder` + `TransformerEncoderLayer` ship in `burn::nn::transformer` | **burn** (less code, less risk in training-side) |
| Multi-head loss + grad accumulation | Manual: own `Optimizer`, own `GradStore` | Built-in `GradientsAccumulator`, `CrossEntropyLoss`, full training crate (`burn-train`) with the Burn Book documenting the path | **burn** |
| Mixed-precision training | Not first-class | Not first-class either — both punt to backend-specific support | tie |
| WASM target | Yes, candle has WASM examples | Yes, burn-wgpu hits WASM | tie |
| Compile time (release, fresh) | ~12 s | ~30 s | **candle** |
| Number of transitive deps | smaller | larger | **candle** |
| AGPL-3.0-or-later compatibility | dual MIT/Apache-2.0 | dual MIT/Apache-2.0 | tie (both fine) |
| Maintenance velocity (Nov 2025–May 2026) | 0.9.2 (Jan 26), 0.10.0/0.10.2 (Mar/Apr 26) | 0.19.1 (Nov 25), 0.20.0 (Jan 26), 0.20.1 (Jan 26), 0.21.0-pre.* (Apr 26) | tie (both alive) |
| Single-binary deploy story | yes, vendors fine | yes, vendors fine | tie |
| Documentation quality | thinner, source-driven | better book, better tutorials | **burn** |

## Reasoning for the call

The decision is driven by **deploy risk**, not training velocity. Issue #96
puts the parser on the production hot path at 50-100k addr/sec. The
constraints that matter for that role are:

1. **Inference latency on CPU**, where candle wins by 1.6x at p50 and 7.5x
   at p99 on the apples-to-apples scratch bench. A regression on inference
   latency hits the throughput contract hard and is hard to roll back.
2. **A working int8/4-bit quantization path today**, where candle's
   `QMatMul` is already used in production by the quantized LLM models in
   `candle-transformers`. Burn's PTQ docs say "currently in active
   development" — that is the wrong sentence for a load-bearing component.
3. **A small, stable surface**. Burn has a wider, prettier surface but
   churns more between point releases. Candle has a narrower surface, less
   ergonomic, but more stable for a thing that has to run untouched in
   production for months.

Burn wins clearly on the **training side** — built-in transformer encoder,
gradient accumulator, training book, less hand-rolling. Codex's review
(below, verbatim) calls this out as well: "**Candle wins inference and
deploy risk; Burn wins training ergonomics**."

For this project, deploy-side certainty matters more than training-loop
ergonomics. We will hand-roll a 60-line transformer encoder block in
candle (the scratch project already has one — see
`geocode-research/scratch-candle/src/main.rs`); the trainer's complexity
is a one-time engineering cost paid offline, not a hot-path cost paid on
every customer query.

## Dissenting view

It is reasonable to flip this decision to burn IF one of the following
becomes true:

- The training pipeline (#98 Phase 2) lands first and the training-side
  ergonomic gap dominates the implementation cost. In that case, train in
  burn, export to candle for inference (via safetensors round-trip — the
  weight tensors are interchangeable; the architecture has to be
  re-instantiated, but the encoder shape is small). `burn-candle` exists
  but is alpha; it is not the right plumbing today.
- candle's quantized matmul on aarch64 is found to be substantially
  worse than burn's. We did not measure this — we were on x86_64.

Both contingencies are a "re-evaluate before #96 Phase 4 starts"
checkpoint, not a reason to flip the call now.

## Third option: don't pick a framework, write custom inference

Codex floated this and it is the right escape hatch to keep open. For a
2-4M-param model with fixed shape and known quantization scheme, a
hand-written inference path (fixed-shape byte embeddings, hand-written
encoder block, weight-only q4/q8 linear layers, no framework overhead
outside tensor math) has the highest probability of hitting the <50 µs
p99 target. It costs the most engineering up front and pays back at
deploy time.

**Recommendation for the parser implementer**: ship Phase 1 on candle.
If the production p99 latency lands above the throughput contract by more
than 2x, the next move is **not** to switch to burn — it is to harden the
candle path with int8 weights + MKL on x86 + Accelerate on macOS + thread
pinning, and only after that to consider hand-written inference for the
hot-path encoder block.

## Reproducing the measurements

```bash
# candle scratch
cd geocode-research/scratch-candle
cargo build --release
taskset -c 0 ./target/release/scratch-candle

# burn scratch
cd geocode-research/scratch-burn
cargo build --release
taskset -c 0 ./target/release/scratch-burn
```

Both projects opt out of the butterfly-osm workspace via empty `[workspace]`
tables in their Cargo.toml so they don't get pulled into `cargo test
--workspace`.

## External review

Verbatim outputs in `geocode-research/EXTERNAL_REVIEW.md`. Summary:

- **Codex**: pick candle for deployment-critical path; quantization story
  is more believable in candle today; burn is "release-disciplined and
  more documented" but has more architectural churn. **Agrees with this
  document's call.**
- **Gemini** (gemini-2.5-flash, after gemini-2.5-pro hit quota): exploration
  loop, did not produce a definitive recommendation in the time window.
  See `EXTERNAL_REVIEW.md` for the (truncated) output.
