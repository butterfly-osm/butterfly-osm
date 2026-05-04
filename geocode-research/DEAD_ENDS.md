# Dead Ends

Research directions explored and discarded. Captured here so the next
contributor doesn't waste time re-running the same fences.

## 1. lightgbm-rs (note the dash)

There is no crate named `lightgbm-rs` on crates.io. The active LightGBM
binding is **`lightgbm3`** (1.0.10 as of this writeup). The CLAUDE.md task
description mentioned `lightgbm-rs` — that was a name confusion. The
GBDT decision document references `lightgbm3` correctly.

## 2. xgboost-rs

We did not bench it. It exists, it works, but it has the same C++ build
dependency (cmake, OpenMP) as `lightgbm3`. Once we accept that cost,
LightGBM has stronger CPU inference per published numbers. So
xgboost-rs is dominated.

## 3. linfa-trees

Not a GBDT — only single-tree CART. Eliminated on shape, not benched.
The crate is fine; it does not implement boosted ensembles.

## 4. burn-candle ("train in burn, run in candle")

Looks attractive on paper: pretty training surface in burn, fast
inference in candle. The crate `burn-candle` is documented on crates.io
as **alpha**, "usable for some use cases, like inference," "not all
operations supported." This is the wrong sentence for a load-bearing
component. Path discarded; documented in `ML_STACK_DECISION.md`
dissenting view as a re-evaluation point but not a Phase 1 path.

## 5. Hand-rolled inference (skip both frameworks)

Not a dead end, but explicitly deferred. Documented as the third option
in `ML_STACK_DECISION.md`. The escape hatch is open if the production
candle path lands above the latency contract by more than 2x even with
int8 + MKL + thread pinning.

## 6. gemini-2.5-pro for ML stack consultation

Hit HTTP 429 quota exhaustion on first attempt. Switched to
gemini-2.5-flash for the retry; the corpus retry produced a useful
review, but the ML stack retry spent the full 200s window in
tool-use-exploration without producing a recommendation. Codex's review
stands alone for the ML stack call. Captured verbatim in
`EXTERNAL_REVIEW.md`.

## 7. Mixed-language swap gated by multilingual zone

The current `canary.rs` BE-as-FR / BE-as-NL rewrite is NOT gated by
multilingual zone — it fires on any street whose name happens to
contain "straat" or "rue". For Belgium that mostly works because the
language regions are reasonably well-correlated with street naming. For
the production canary, we should consult `addr:city` against the
trilingual-zone municipality list and only fire there. Not blocking for
research scaffolding; tracked in `CORPUS_DESIGN_NOTES.md` open
question.
