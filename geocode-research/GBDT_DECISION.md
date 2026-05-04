# GBDT Library Decision

## Decision

**Use `gbdt` (pure-Rust) for the geocode reranker.**

Stay on pure-Rust until measured production data shows it is the
bottleneck. If we must move, move to **`lightgbm3`** (LightGBM C++
bindings) — but only after we have evidence the pure-Rust path doesn't
fit the budget, and only after we accept the build-system tax (cmake +
OpenMP) it imposes on the single-binary deploy story committed in #96.

## Measured latency (this machine, Linux x86_64, single CPU thread)

`geocode-research/scratch-gbdt/` builds a 100-tree, depth-6 GBDT on
synthetic 20-feature samples and measures inference latency for both the
single-row predict path (the reranker hot path) and a batched path.

| Path | latency |
|---|---|
| Single-row `predict()` p50 | **1.12 µs** |
| Single-row `predict()` p99 | **1.81 µs** |
| Single-row mean | 1.18 µs |
| Batched 1000 rows | 472 ns / row (~2.1 M rows/sec) |
| Train 100 trees, 5000 samples | 261 ms |

Reranker budget per query (#96): ~10 µs end-to-end. At 1.12 µs per
candidate, we can rerank ~9 candidates per query while keeping the GBDT
inside its slice of the budget. That fits the typical hypothesis fanout
(post-dedup, per #96 Recombination Invariant) for a clean query.

## Comparison

| Library | Type | Bench (single-row) | Build deps | License | Verdict |
|---|---|---|---|---|---|
| **gbdt** 0.1.3 | pure Rust | **1.12 µs p50** (measured) | rust only | Apache-2.0 | **pick** |
| **lightgbm3** 1.0.10 | LightGBM C++ FFI | not measured here; LightGBM CPU predict on a 100×6 tree is ~0.5-1 µs/row in published numbers | cmake + OpenMP runtime + C++ toolchain | MIT (binding); MIT (LightGBM) | escape hatch |
| **xgboost-rs** | XGBoost C++ FFI | similar order to lightgbm3 | cmake + C++; less actively maintained binding | Apache-2.0 (binding) | not recommended |
| **linfa-trees** 0.7 | pure Rust | not measured; documented as "less mature"; no native GBDT (single decision trees only) | rust only | MIT/Apache-2.0 | does not fit (decision trees, not boosted ensemble) |
| **smartcore** 0.5 | pure Rust | not measured; broader algorithm zoo, slower than `gbdt` for tree ensembles per published comparisons | rust only | Apache-2.0 | not recommended (broader scope, less optimized for this specific task) |

We did **not** bench `lightgbm3` on this machine because:

1. The first decision is "pure Rust vs C++ FFI". `gbdt` already meets the
   per-candidate budget (1.12 µs vs 10 µs slice). There is no algorithmic
   reason to add a C++ build-system dependency.
2. The `lightgbm3` install requires cmake, OpenMP, libstdc++ headers, and
   a working C++ toolchain on every build host. This is a 30-second job
   on Debian 13 and a multi-day job on a customer's locked-down CI.
3. The single-binary deploy story committed in #96 is load-bearing for
   the geocoder's value proposition. Forcing customers to ship a
   LightGBM `.so` alongside our binary erodes that.

**Bench `lightgbm3` only if** the production reranker is observed to be
the bottleneck in geocode queries AND `gbdt` is the slowest layer. Until
then, the build-system cost is not justified by the latency win.

## Reasoning

Three priorities, ranked:

1. **Single-binary deploy** — biggest reason to favor pure Rust.
   `gbdt` compiles without external dependencies; `lightgbm3` doesn't.
2. **Per-candidate latency** — `gbdt` 1.12 µs already fits the per-query
   budget; LightGBM is faster but the difference is dwarfed by the
   surrounding query cost.
3. **Training-side ergonomics** — `gbdt` has a smaller API surface and
   no native categorical-feature support. The reranker features in
   #96 (resolution success, anchor coverage, channel role distribution,
   country posterior) are all numeric, so categorical handling does not
   matter for this use case.

`smartcore` was passed over because it is broader-but-slower for the
specific tree-ensemble task. `linfa-trees` does not actually have a GBDT
implementation — only single-tree CART. Both were eliminated on shape,
not benchmarked.

## What this does NOT decide

- The GBDT serialization format. `gbdt` has its own JSON-ish format;
  `lightgbm3` reads/writes LightGBM text format. If we ever want to
  hand-tune the model in Python (using the LightGBM training pipeline)
  and ship to Rust for inference, we MUST move to `lightgbm3`. That is a
  separate decision, deferred to the time the trainer pipeline lands
  (#98 Phase 2).
- The training pipeline. The MVP geocoder will train the reranker
  offline using whatever path the parser implementer prefers. The
  inference-side library choice (this document) does not constrain the
  training-side choice.

## Reproducing the measurements

```bash
cd geocode-research/scratch-gbdt
cargo build --release
taskset -c 0 ./target/release/scratch-gbdt
```

Standalone crate (own `[workspace]` table, opts out of butterfly-osm).
