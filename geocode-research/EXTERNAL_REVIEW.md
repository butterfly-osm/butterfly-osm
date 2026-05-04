# External Review: AI Reviewer Outputs

This file contains verbatim outputs from gemini and codex consultations on
the ML stack and corpus generator design questions. Per CLAUDE.md, AI
reviewers are consulted before drastic architectural choices. The prompts
are in `PROMPT_ML_STACK.txt` and `PROMPT_CORPUS.txt`.

## Outcome summary

| Topic | Codex (codex exec) | Gemini (2.5-pro / 2.5-flash) | Final call |
|---|---|---|---|
| ML stack | **candle for deploy, burn for training; pick candle** | (2.5-pro: quota exhausted; 2.5-flash: exploration loop, no firm rec in 200s window) | candle |
| Corpus | substantive critique: country head needs ambiguity-aware loss; provenance-based labeling beats post-hoc alignment; N≈6 not 10 | substantive: align-and-reproject for typo BIO; covers extra augmentation classes; N=5-15 | adopted: provenance labeling, lower N=6, ambiguity-aware country loss |

## Disagreements and resolutions

- **N (augmentations per gold record)**: codex says 6; gemini says 5-15.
  We ship the corpus generator with `--augmentations 10` as the default
  but document `6` as the recommended starting point in `geocode-research/
  CORPUS_DESIGN_NOTES.md` once the trainer lands. The generator already
  emits the canonical record + N augmentations, so the choice is at run
  time, not compile time.
- **Country-head supervision**: codex flags this hard ("not sufficient"
  for ambiguous rewrites); gemini did not surface it in the same form.
  Adopted codex critique: see TODO in `geocode-research/CORPUS_DESIGN_NOTES.md`
  for the loss-masking that the future Phase 2 trainer must apply on
  drop-postcode + drop-city variants.
- **BIO under typo injection**: codex (provenance maps from labeled
  pieces) and gemini (align+reproject) propose two valid approaches.
  Our generator currently uses **delta-tracking against the rendered
  text** which is closer to align+reproject but constrained to ASCII
  bytes inside a single span. That is a defensible compromise for the
  research-grade generator and is documented as such in `augment.rs`.
  The future Phase 2 trainer should upgrade to provenance maps.

---

## Verbatim: codex on ML stack (`codex-ml.txt`)

**Recommendation**

For your exact workload, I would choose **Candle** for the deployment-critical path.

If you force me to pick one framework end-to-end, the tradeoff is simple: **Candle wins inference and deploy risk; Burn wins training ergonomics**. Given your priorities, deploy risk matters more.

**Why**

- **Inference latency:** I did not find an official head-to-head benchmark for a tiny `seq<=64`, batch-1 CPU transformer. Blindly, I expect **direct Candle** to be faster or at least easier to make fast on this shape because it has a leaner CPU path, optional **MKL** on x86 and **Accelerate** on Apple, and a real quantized matmul path (`QMatMul`) already used in quantized model code. Sources: [Candle README](https://github.com/huggingface/candle), [Candle install/docs](https://huggingface.github.io/candle/guide/installation.html), [candle-core features](https://docs.rs/crate/candle-core/latest/features), [Candle quantized module](https://docs.rs/candle-core/latest/candle_core/quantized/index.html), [Candle quantized_nn](https://docs.rs/candle-transformers/latest/candle_transformers/quantized_nn/index.html).
- For Burn, the choices are `burn-cpu`, `burn-ndarray`, or `burn-candle`. `burn-cpu` is newer and JIT/autotune-heavy; that can be good for throughput, but for **micro-latency on tiny kernels** it is not an obvious win. `burn-candle` is explicitly documented as **alpha** and “usable for some use cases, like inference,” with “not all operations supported.” Sources: [Burn README/docs](https://github.com/tracel-ai/burn), [burn-cpu crate](https://docs.rs/crate/burn-cpu/latest), [burn-candle crate](https://docs.rs/crate/burn-candle/latest).
- For your SLA, performance will depend more on **small-GEMM/matvec quality, single-thread behavior, cache locality, and weight format** than on framework marketing. In practice: pin threads, benchmark with and without MKL/OpenBLAS/Accelerate, and avoid backends that need warm-up or compilation in the hot path.

**Quantization**

- **Candle has the more believable custom-transformer deploy story today**, but it is mostly a **weight-only** story. Its quantized machinery is built around `QTensor`/`QMatMul` and GGML/GGUF-style quantized weights; Candle’s own quantized layers keep linear weights quantized and often dequantize smaller parameters like norms/biases. For your 2-4M parameter model, that is probably fine because linears dominate size. Sources: [candle_core::quantized](https://docs.rs/candle-core/latest/candle_core/quantized/index.html), [QMatMul](https://docs.rs/candle-core/latest/candle_core/quantized/enum.QMatMul.html), [quantized_nn source](https://docs.rs/candle-transformers/latest/src/candle_transformers/quantized_nn.rs.html).
- **Burn has the nicer API story**: PTQ is built in, docs explicitly mention **8-bit, 4-bit, and 2-bit** PTQ, and modules expose `quantize_weights`. But Burn also says quantization is **“currently in active development”** and only supported **“on some backends.”** That is the wrong sentence for a production-critical promise. Sources: [Burn quantization docs](https://docs.rs/burn/latest/index.html), [Quantizer](https://docs.rs/burn/latest/burn/module/struct.Quantizer.html).
- So: if you want a framework that already looks like it knows how to run **q4/q8 CPU inference**, I trust **Candle** more. If you want the prettier quantization abstraction, **Burn**.

**Training ergonomics**

- **Burn is clearly less painful** for your model shape. It has built-in `TransformerEncoder`, `TransformerEncoderLayer`, attention/transformer modules, `CrossEntropyLoss`, a documented `GradientsAccumulator`, and a full training crate/book. Sources: [Burn transformer module](https://docs.rs/burn/latest/burn/nn/transformer/index.html), [Burn nn docs](https://docs.rs/burn-core/latest/burn_core/nn/index.html), [CrossEntropyLoss](https://docs.rs/burn/latest/burn/nn/loss/struct.CrossEntropyLoss.html), [GradientsAccumulator](https://docs.rs/burn/latest/burn/optim/struct.GradientsAccumulator.html), [Burn Book custom training loop](https://burn.dev/books/burn/custom-training-loop.html), [burn-train](https://docs.rs/crate/burn-train/latest).
- **Candle can do it**, but you will mostly hand-roll the encoder block and training loop from primitives like `Embedding`, `Linear`, `LayerNorm`, `ops::sdpa`, and optimizer/backprop pieces. I did not find first-class AMP/autocast training docs. Sources: [candle-nn](https://docs.rs/candle-nn/latest/candle_nn/), [ops::sdpa](https://docs.rs/candle-nn/latest/candle_nn/ops/index.html), [optimizer API](https://docs.rs/candle-nn/latest/src/candle_nn/optim.rs.html), [GradStore](https://docs.rs/candle-core/latest/candle_core/backprop/struct.GradStore.html).
- For your specific two-head loss, both can do it. In Burn it is cleaner.

**Maintenance, license, and risk**

- **Both are permissive**: dual **MIT / Apache-2.0**, so both are compatible with an **AGPL-3.0-or-later** workspace. Sources: [Candle repo](https://github.com/huggingface/candle), [Burn repo](https://github.com/tracel-ai/burn).
- **Both are alive** between **November 4, 2025 and May 4, 2026**. Candle published `0.9.2` on **January 24, 2026** and `0.10.0`/`0.10.2` on **March 31 / April 1, 2026**. Burn published `0.19.1` on **November 6, 2025**, `0.20.0` on **January 15, 2026**, `0.20.1` on **January 23, 2026**, plus `0.21.0-pre.*` through **April 8, 2026**. Sources: [candle-core versions](https://docs.rs/crate/candle-core/latest), [candle-transformers versions](https://docs.rs/crate/candle-transformers/latest), [burn versions](https://docs.rs/crate/burn/latest), [burn-core versions](https://docs.rs/crate/burn-core/latest).
- Burn looks **more release-disciplined and more documented**. Candle looks **very active**, but docs are thinner and more source-driven. Burn also shows **more architectural churn** right now, which is good for progress and bad for API stability. Sources: [Burn status](https://github.com/tracel-ai/burn), [recent Burn issue/proposal](https://github.com/tracel-ai/burn/issues/4879), [Candle PR queue](https://github.com/huggingface/candle/pulls), [Burn PR queue](https://github.com/tracel-ai/burn/pulls).

**Built-in blocks and gotchas**

- **Burn** ships the generic transformer building blocks you want. Less hand-rolling, lower implementation risk. Source: [Burn transformer module](https://docs.rs/burn/latest/burn/nn/transformer/index.html).
- **Candle** does not appear to ship a clean generic encoder stack in `candle-nn`; it gives primitives and many model-specific implementations in `candle-transformers`. For your model, you would likely implement the encoder block yourself. Sources: [candle-nn](https://docs.rs/candle-nn/latest/candle_nn/), [candle-transformers](https://docs.rs/candle-transformers/latest/candle_transformers/).
- Hidden gotchas:
  - Candle docs coverage is thin, especially `candle-transformers`; expect source-diving.
  - Burn quantization and ONNX support are explicitly still evolving. Sources: [Burn quantization docs](https://docs.rs/burn/latest/index.html), [Burn README ONNX note](https://github.com/tracel-ai/burn).
  - `burn-candle` is alpha, so “Burn for training, Candle for inference” is conceptually nice but not a zero-risk unification path. Source: [burn-candle crate](https://docs.rs/crate/burn-candle/latest).
  - I found active Burn issues around PyTorch/transformer import paths, so if you expect painless state-dict portability, validate early. Sources: [issue #4052](https://github.com/tracel-ai/burn/issues/4052), [issue #4896](https://github.com/tracel-ai/burn/issues/4896).

**Third option**

The serious third option is **custom Rust inference code**, with training done in Burn or Candle offline.

For your target of **p99 well under 50 microseconds**, that may be the highest-confidence route: fixed-shape byte embeddings, hand-written encoder block, weight-only q4/q8 linear layers, and no framework overhead outside tensor math. Highest engineering cost, lowest deployment ambiguity.

If you want an inference-only framework alternative, **tract** is the one worth mentioning: pure Rust, permissive, active, ONNX-focused. But it gives you **no training story**, and it moves you off the Candle/Burn path. Sources: [tract repo](https://github.com/sonos/tract), [tract-onnx crate](https://docs.rs/crate/tract-onnx/latest).

Net: **pick Candle if deployment constraints are the real constraint. Pick Burn only if iteration speed in training is more important than deploy-side certainty.** For your stated priorities, that means **Candle**.

---

## Verbatim: codex on corpus (`codex-corpus.txt`)

**Verdict**

The core idea is sound, but the current plan has three failure modes:

- You are treating all rewrites as equally valid supervision for the `country` head. That is wrong once a rewrite drops disambiguating evidence.
- You are at risk of building the generator as “canonical string + regex corruption”. That is the easiest way to ship silent BIO misalignment bugs.
- You can easily overfeed synthetic style noise to a 2-4M model and end up teaching the augmentation distribution instead of address semantics.

I would keep the framing, but tighten it to: **equivalence-class augmentation for retrieval**, with **observability-aware supervision** for fields that may become ambiguous.

**Per Question**

- `retrieval-success-invariant augmentation` is the right framing for the parser head.
- It is not sufficient for the `country` head. Some rewrites preserve retrieval success but reduce country evidence. Those should not always get a hard one-hot country target.
- The missing augmentation classes are:
  - Unicode/diacritic normalization: `é -> e`, NFC/NFD, apostrophes, smart quotes.
  - House-number morphology: `12A`, `12 A`, `12-A`, `12 bus 3`, `12 bis`, `12/3`.
  - Postal-code formatting: spaces, hyphens, country-prefix variants where common.
  - Locality aliases/exonyms/municipality-vs-settlement names.
  - Country omission/inclusion.
  - Keyboard/OCR-like confusions if production traffic is user-entered.
- The biggest omission is not another positive augmentation family. It is **ambiguity-aware examples**: rewrites that are still valid parser supervision but should give weaker or masked country supervision.

- For a 2-4M byte-level transformer, I would start with **N = 6 total views per gold**: `1 canonical + 5 augmented`.
- I would not go much above `8` unless you are doing online stochastic augmentation instead of materializing a fixed corpus.
- Why `6`:
  - At this model size, breadth of unique addresses matters more than many correlated rewrites of the same tuple.
  - You need enough style coverage to break shard-format dependence, but not so much that the model spends capacity modeling your synthetic noise process.
  - Five augmented views is enough to cover light, medium, and one heavy perturbation without turning each tuple into its own mini-dataset.
- If you can do augmentation on the fly, that is better than precomputing large `N`. Then keep the effective mixture around `30-40% canonical` and `60-70% augmented`.

- For BIO under typo injection, do **not** “fix offsets after the fact” with regex.
- The clean approach is: generate with **provenance**, not post-hoc matching.
- Concretely:
  - Represent the rendered address as a sequence of labeled pieces: street bytes, separator bytes, house bytes, etc.
  - Apply edits to those pieces while carrying the owning field for each output codepoint.
  - Re-encode to UTF-8 at the end and emit BIO from the final per-byte provenance map.
- Standard technique: an **edit script with alignment/provenance mapping**. If you own the transform, this is better than generic align-and-reproject.
- What goes wrong with regex:
  - Repeated substrings match the wrong occurrence.
  - Abbreviations hit inside proper names, not just street types.
  - Insertions at entity boundaries break `B` vs `I`.
  - Unicode casing/normalization changes byte length.
  - Deletions around punctuation cause label bleed across separators.
  - If you edit bytes directly, you can create invalid UTF-8.
- Important detail: inject typos at the **character/codepoint level**, then re-encode to bytes. Do not mutate raw UTF-8 bytes unless you explicitly want malformed input.

- The BE-as-FR canary is useful, but too narrow.
- Better recipe: build a **counterfactual invariance suite**.
- For each held-out address, generate style-counterfactuals where semantics stay fixed and nuisance factors vary:
  - order
  - casing
  - abbreviation regime
  - punctuation
  - multilingual street-type swap where legal
  - postcode/locality omission
- Then measure:
  - BIO F1 stability across variants
  - country posterior drift across variants
  - exact-span consistency
- Also add **leave-one-style-family-out** tests. Example: train without a given formatting regime, then test on it. That catches memorization better than a single BE->FR rewrite.
- If you want one explicit anti-memorization metric, try training a simple classifier to predict source shard from the parser’s hidden states. If shard prediction is easy, you still have leakage.

- For country supervision, **sample-time hard labels are only OK when the rewritten string still contains country evidence**.
- For BE addresses rewritten with French conventions but still semantically BE, the label should remain BE. That is a good hard positive.
- But when you drop locality/postcode/country and leave `street + house` only, a hard BE label is often the wrong target for a posterior head. It teaches orthography and priors, not geography.
- I would use:
  - full country loss for fully informative or moderately informative variants
  - reduced-weight or masked country loss for country-ambiguous variants
  - optionally soft targets if you can estimate a plausible candidate-country set cheaply
- Hard-negative country mining is less important than **not forcing fake certainty on ambiguous positives**.

- For under-represented countries, prefer **modest oversampling plus mild augmentation**, not extreme augmentation of a few records.
- Rule of thumb:
  - first increase country sampling weight
  - then slightly increase augmentation diversity
  - do not crank typo/noise rates just because the shard is small
- I would cap country oversampling around `3-5x` and keep minority-country augmentation only slightly heavier than the baseline.
- More copies help the model remember the country exists. More augmentation helps invariance. You need both, but oversampling should do more of the work than aggressive corruption.
- Also balance within country by locality/region if possible, or the model will memorize one city’s patterns as the country.

**Likely Silent Regressions**

- Canonical rendering becoming the hidden dominant style, with augmentation too weak to undo it.
- Mixed-language swaps inventing forms that locals never write.
- Abbreviation replacements firing inside street names.
- OSM fallback noise poisoning a small model unless you downweight or separate it.
- Duplicate addresses leaking across train/val through different sources or different renderings.
- Country head learning shard priors because sampling is imbalanced.
- Truncation at `~64` bytes cutting entities and silently corrupting supervision.
- House-number edge cases dominating errors: suffix letters, fractional numbers, unit-like tokens.
- Training on only “valid” addresses and then failing on partial user input in production.

**What To Look For In The 1,000-Line Sample**

- BIO invariants hold: one label per byte, no illegal `I-*` starts, spans decode to valid UTF-8.
- Each augmented line still corresponds to the same structured tuple.
- Replacements only hit the intended field class.
- Mixed-language swaps happen only in allowed zones.
- House numbers stay attached to the right span under punctuation and typo noise.
- Dropped-field examples are still realistic, not synthetic nonsense.
- Country labels on ambiguous short forms do not look overconfident or obviously wrong.
- The augmentation mix looks balanced; one family should not dominate visually.
- No train-style fingerprints from a single source remain obvious in casing, separators, or abbreviations.
- No duplicates or near-duplicates are flooding the sample.

If you want, I can turn this into a concrete corpus-generation spec with invariants, loss-masking rules, and a provenance-based labeling algorithm.

---

## Verbatim: gemini on corpus (`gemini-corpus.txt`, gemini-2.5-flash)

Note: gemini-2.5-pro hit quota; this is the gemini-2.5-flash retry.

The user has presented a detailed plan for building a training corpus for a byte-level transformer for address tagging. This requires a comprehensive technical review across several dimensions of data augmentation, model capacity, and evaluation. I will enter plan mode to thoroughly address each point raised by the user.
# Technical Review: Byte-Level Transformer Corpus Design

## Introduction
The proposed corpus design for a 2-4M parameter byte-level transformer for BIO address tagging is thoughtful and addresses critical architectural constraints for a robust geocoding system. The emphasis on "retrieval-success-invariant augmentation" to prevent shard memorization is well-placed given the small model size and the diverse, convention-rich nature of address data. This review will address each of your specific questions and provide recommendations.

## 1. Is "retrieval-success-invariant augmentation" the right framing? Are we missing a class of augmentation?

**Framing:** Yes, "retrieval-success-invariant augmentation" is an excellent framing. It clearly articulates the core goal: to teach the model what constitutes a functionally equivalent address for retrieval, independent of its specific input format or origin shard's conventions. This is crucial for avoiding fragility and ensuring generalization across diverse, unseen address patterns.

**Missing Augmentation Classes:** The proposed augmentation strategies are comprehensive. However, consider the following nuances or additions:

*   **Synonym Replacement/Lexical Variation (for Street Types/Locality Names):** While "abbreviation flips" cover common cases, some regions might have synonymous terms for street types (e.g., "drive" vs "avenue" in some contexts) or alternative names for small localities/neighborhoods that are semantically equivalent for retrieval. This could be more complex due to the byte-level nature, but worth exploring if such variations exist and cause issues. For example, if "Street" and "Road" are sometimes interchangeable in certain contexts for retrieval purposes.
*   **Contextual Noise/Adjacency:** Addresses rarely appear in isolation. They are often embedded in text (e.g., "Delivery to [Address]"). Introducing some controlled, non-address surrounding text (e.g., "The address is: ", "Send to ") might help the model learn to delimit the address boundaries more robustly. This is more about noise robustness than semantic invariance, but contributes to "retrieval success" in real-world scenarios.
*   **Partial Address Strings/Truncation:** What happens if the input is truncated or only a partial address is provided? Augmenting with prefixes or randomly truncated suffixes of valid addresses could prepare the model for incomplete user inputs.
*   **Numerical Variations:** Beyond house numbers (which are implied), consider how zip codes are formatted (e.g., 12345 vs. 12345-6789), or if any other numerical identifiers exist.

## 2. For a 2-4M parameter byte-level transformer, what augmentation rate per gold address (N) gives meaningful generalization without saturating the model's capacity? Pick a number and defend it.

For a 2-4M parameter byte-level transformer, a balanced `N` is critical. Too few augmentations won't generalize; too many might overwhelm a small model or lead to "memorization" of augmentation patterns rather than semantic invariance.

**Recommendation: `N` between 5 and 15 per gold record.**

**Defense:**

*   **Model Capacity:** A 2-4M parameter model is relatively small for NLP tasks, especially byte-level. Each additional unique input derived from an augmentation takes up capacity. An `N` significantly higher than 15 might force the model to memorize specific augmented examples rather than learning generalized underlying patterns.
*   **Diversity vs. Repetition:**
    *   **Low `N` (e.g., <5):** May not generate enough diverse examples to cover the variations needed for "retrieval-success-invariance," especially when combining multiple augmentation types.
    *   **High `N` (e.g., >20):** Risks generating many very similar examples, leading to diminishing returns and potentially overfitting to the augmentation distribution itself. It can also cause the model to spend too much capacity learning redundant representations.
*   **Combinatorial Explosion:** The proposed augmentations (reordering, abbreviation, typos, case, mixed language, whitespace) can combine in many ways. An `N` of 5-15 allows for a good sampling of these combinations without requiring an exhaustive (and computationally expensive) generation of every single permutation.
*   **Practicality:** Generating a very high `N` for a large dataset can be computationally intensive and bloat the training dataset size unnecessarily.

**Strategy for N:** Start with a lower end (e.g., `N=5` to `N=8`) and monitor validation performance. If the model struggles with generalization on diverse, unseen patterns, gradually increase `N` while ensuring the augmentations remain truly distinct and contribute to the "retrieval-success-invariance" goal. Pay close attention to validation loss vs. training loss to detect overfitting.

## 3. BIO label preservation under typo injection: how do you cleanly maintain the (text, BIO_labels) alignment when characters are inserted/deleted? Is there a standard technique we should be using (e.g. align+reproject, or "augment the label sequence in lockstep")? What goes wrong if we just regex our way through?

This is a critical point. Maintaining alignment after character-level edits (insertions, deletions, substitutions, transpositions) is challenging but essential for correct supervision.

**Standard Technique: Align and Reproject (or Character-Level Diffing for Offset Adjustment)**

1.  **Generate original (text, BIO_labels):** Start with the gold address and its byte-level BIO labels.
2.  **Apply typo injection to `text`:** This creates `text_corrupted`.
3.  **Character-level alignment/diffing:** Use a robust diffing algorithm (e.g., Levenshtein distance-based alignment, or `difflib` in Python, or similar Rust crates) to find the correspondence between `text` and `text_corrupted`. This will yield a sequence of operations (insertions, deletions, matches) and their positions.
4.  **Reproject `BIO_labels`:** Based on the alignment, adjust the `BIO_labels` from `text` to `text_corrupted`:
    *   **Match:** If `text[i]` maps to `text_corrupted[j]`, then `BIO_labels_corrupted[j]` inherits `BIO_labels[i]`.
    *   **Insertion:** If `text_corrupted[j]` is an insertion (has no corresponding `text[i]`), its `BIO_label_corrupted[j]` should typically be `O` (outside). This assumes insertions are non-address tokens. If an insertion *within* an address component is plausible (e.g., `Streeet` for `Street`), you might preserve the label, but `O` is safer for random noise.
    *   **Deletion:** If `text[i]` is deleted (has no corresponding `text_corrupted[j]`), its label `BIO_labels[i]` is simply dropped.

**"Augment the label sequence in lockstep"** is essentially this process, but often implies generating the new `text` and `BIO_labels` simultaneously from some intermediate representation. For typos, align and reproject is more common because the text corruption is often a post-processing step.

**What goes wrong with regex-based approaches:**

*   **Complexity and Brittleness:** Regex is good for pattern matching, but poor for structural alignment under arbitrary character edits. Crafting regexes to handle all combinations of insertions, deletions, and transpositions within and around tagged spans would be incredibly complex and prone to errors.
*   **Loss of Information:** A regex might find a corrupted span, but it won't tell you *how* it changed relative to the original, which is what you need to accurately transfer labels. You'd likely end up with heuristic-based label transfer, which introduces noise.
*   **Edge Cases:** Simple regex replacements often fail at boundaries or when multiple edits occur close together, leading to misaligned or dropped labels.
*   **No Universal Solution:** Each type of typo would require a different regex logic, making the system difficult to maintain and extend.

**Recommendation:** Use a character-level diffing library. This is the robust, standard way to handle sequence alignment and label transfer in such scenarios. Example: If `Street` (B-STREET, I-STREET, I-STREET, I-STREET, I-STREET, I-STREET) becomes `Strret` (deletion of 'e', insertion of 'r'), a diff algorithm will map `S->S`, `t->t`, `r->r`, `e->r`, `e->e`, `t->t`, and you can transfer labels accordingly, dealing with the `e` deletion and `r` insertion.

## 4. The cross-shard regression canary as proposed (BE-rewritten-as-FR) — is that the right test? Or are there better-known recipes for catching shard memorization in small parsers?

The **BE-rewritten-as-FR canary is an excellent start and absolutely essential.** It directly targets the architectural constraint of preventing shard memorization of formatting conventions.

**Improvements/Better Recipes for Catching Shard Memorization:**

*   **Expand the Canary Matrix:** Don't just do BE-rewritten-as-FR. Create a full "confusion matrix" of rewrites:
    *   BE -> FR conventions
    *   BE -> NL conventions
    *   FR -> BE conventions
    *   FR -> NL conventions
    *   NL -> BE conventions
    *   NL -> FR conventions
    *   ... and similarly for other major shards (BAG, G-NAF, OpenAddresses).
    The goal is to show that an address *semantically equivalent* to one from shard A, but formatted with conventions from shard B, is still correctly parsed.
*   **Introduce "Out-of-Shard" Conventions:** Beyond rewriting using known shard conventions, create synthetic "non-shard" conventions for common elements. For instance, an address that looks like `[Number] [Street Type] [Street Name]` even if no known shard uses that exact order for a specific country. This tests true generalization beyond just transferring existing rules.
*   **Adversarial Examples:** Manually craft or generate (e.g., using a small adversarial network if resources allow) examples that are designed to look like they *should* come from one shard but semantically belong to another, based on common memorized patterns.
*   **Attribute-Level Memorization Tests:**
    *   **Case Memorization:** Test if an address (e.g., a BE address) fails if its case is changed to match a different shard's casing convention (e.g., all UPPERCASE like some G-NAF entries).
    *   **Abbreviation Memorization:** If a model always sees "Boulevard" for a FR street, does it fail on "Boul." for the same FR street if it never saw that abbreviation in the FR shard (but perhaps saw it in a BE shard)?
*   **"Novelty Detection" on Input:** While not a test, monitoring model confidence or embedding similarity for inputs that are "far" from training shard distributions could be an early warning system in production for new shard conventions.

**Why the proposed test is good but not sufficient:** It's good because it isolates a specific convention clash. It's not sufficient because memorization can happen across many features (casing, specific abbreviation choices, specific orderings) and across all shard pairings, not just BE-FR. The more comprehensive matrix and targeted attribute tests provide stronger guarantees.

## 5. Country-posterior supervision — is it OK for that label to be the country we picked at sample time, or do we need explicit hard-negative country mining (rewrite a BE address with French conventions and then explicitly label it as "still BE" so the model learns geography over orthography)?

**Recommendation: Explicit hard-negative country mining is superior and necessary for robustness.**

**Explanation:**

*   **Learning Geography Over Orthography:** If you label a BE address (rewritten with FR conventions) as `FR` simply because it *looks* French, the model will learn to rely on superficial orthographic cues (e.g., "Rue" implies FR) rather than the underlying geographic reality or implicit geographic indicators within the address structure. This is exactly the shard memorization you are trying to avoid.
*   **Correct Semantic Meaning:** The fundamental identity of the address (its country) does not change just because its surface form has been augmented. The country-posterior head should predict the *true* country of the address, not the country whose formatting conventions it happens to exhibit.
*   **Hard Negatives:** By feeding a BE address formatted as FR and labeling it `BE`, you create a powerful "hard negative" example for the `FR` class (and implicitly, a hard positive for `BE`). This forces the model to look beyond the immediate orthographic features to correctly identify the country, which likely involves understanding the combination of tokens, common locality names, or numerical ranges (e.g., postcode ranges often strongly imply country).
*   **Small Model Capacity:** Given the small model, you want it to learn the most fundamental, invariant features. The country is an invariant feature of the address itself, not its presentation. Explicitly teaching this invariance is crucial.

**Strategy:** The country label for the country-posterior head should *always* be derived from the **gold record's true country**, irrespective of how it was augmented. If you augment a Belgian address, it remains a Belgian address for the country head.

## 6. For under-represented countries (small shards, few records), what is the right oversample-vs-augment ratio? More copies of the same record, or more aggressive augmentation per record, or both?

For under-represented countries, a combination of **both oversampling and more aggressive augmentation per record** is generally the most effective strategy.

**Reasoning:**

*   **Oversampling (More Copies):** Simply oversampling (repeating) rare records helps balance the class distribution, preventing the model from becoming biased towards frequent classes. This ensures the model sees enough examples of the rare class. However, too much simple oversampling without diversity can lead to overfitting to those specific rare examples.
*   **More Aggressive Augmentation (Higher N for Rare Records):** Applying more aggressive augmentation to rare records directly addresses the generalization problem. It synthetically increases the *diversity* of the rare class. For a small model, this is vital as it might not be able to generalize from very few examples. Aggressive augmentation forces the model to learn the core, invariant features of those rare addresses, rather than memorizing their limited original forms.
*   **Complementary Benefits:**
    *   Oversampling increases exposure.
    *   Aggressive augmentation increases diversity *within* that exposure.
    Without sufficient diversity, oversampling alone only makes the model memorize the few existing examples, not generalize. Without sufficient exposure (oversampling), even diverse augmentations might be too rare to have a significant impact.

**Strategy:**

1.  **Establish a target effective sample size:** Determine a minimum number of effective unique augmented samples you want per country in an epoch.
2.  **Oversample:** Boost the number of gold records from under-represented countries so that their initial contribution to the batch size is closer to that of well-represented countries.
3.  **Vary N:** For these oversampled records from under-represented countries, apply a higher `N` (e.g., `N=10-20`, potentially even more for extremely rare ones) compared to the general `N` used for well-represented countries. This ensures maximum diversity from limited gold data.
4.  **Careful Augmentation Design:** Ensure these aggressive augmentations still adhere strictly to the "retrieval-success-invariant" principle. Avoid generating nonsensical or unparseable addresses, as this would introduce noise.

## 7. Anything we are likely to silently get wrong in this corpus that would only show up as a production regression two months in.

*   **Incorrect BIO Alignment for Complex Augmentations:** While typo injection is discussed, other augmentations (reordering, abbreviation flips) also need perfect label alignment. If the logic for shifting labels is even slightly off for these, you'll have corrupted training data that teaches the model incorrect span boundaries.
*   **Over-Augmentation leading to Noise/Nonsense:** If augmentation rules are too aggressive or combine in unrealistic ways, the model might learn to parse "junk" that doesn't correspond to any real address, or it might get confused by inputs that are *too* far from natural language. This is especially true for typo injection if the probability is too high or target words are too short.
*   **Augmentation Distribution Skew:** If certain augmentations are applied with too high a probability, or if the combination of augmentations creates a distribution that doesn't reflect real-world noise, the model might optimize for that skewed distribution.
*   **Shard-Specific "Soft" Constraints Leaking:** Even with "retrieval-success-invariant" augmentation, subtle features unique to a shard might inadvertently become correlated with the country or other labels if not explicitly challenged. E.g., if one shard *always* uses a specific type of hyphenation in postcodes, and this is not varied for other countries, it could become a spurious signal.
*   **"Easy" Negatives vs. Hard Negatives:** For the country-posterior head, if you only rely on the gold country, the model might find "easy" ways to classify (e.g., "Rue" -> FR, "Straat" -> NL) without learning the more nuanced hard negatives. This leads to silent failures when those easy signals conflict with the true country. (Addressed in point 5).
*   **Validation Set Contamination/Mismatch:** Ensure your validation and test sets are **not** augmented in the same way as your training set, or at least have a representative distribution of *unseen* real-world variations. If your validation set is just augmented training data, you'll get an overly optimistic view of generalization. A separate "regression canary" set (as discussed) is crucial here.
*   **Implicit Shard Boundaries:** If your augmentation engine has any hardcoded logic that is dependent on the *original* shard of an address during augmentation (beyond picking initial conventions), this could implicitly teach shard-specific patterns.
*   **Byte-level Specific Issues:** Given it's a byte-level transformer, ensure your character set handling (UTF-8) is flawless end-to-end, especially with typo injection and mixed languages. Malformed byte sequences or encoding issues could silently corrupt data.

## 8. What should we be looking for in that sample to catch bugs early?

When manually inspecting the thousand-line sample of generated output, focus on these aspects:

1.  **Correct BIO Span Alignment Post-Augmentation:**
    *   **Typos:** Carefully check addresses with injected typos. Do the `B-`, `I-` labels correctly shift and absorb/drop with insertions/deletions? Is inserted noise correctly labeled `O`?
    *   **Reordering:** For reordered addresses, ensure the labels still correspond to the correct semantic entity in its new position.
    *   **Abbreviation Flips:** Verify that "Rue" and "R." both get the correct `B-STREET_TYPE` (or similar) label.
2.  **Semantic Equivalence Preservation:**
    *   After augmentation, does the address still *semantically* represent the original gold record? Is it still a valid, retrievable address?
    *   Are abbreviations always correctly expanded/contracted, or are some becoming nonsensical?
    *   Are mixed-language translations correct and contextually appropriate?
3.  **Realism of Augmentations:**
    *   Do the augmented addresses *look* like plausible real-world variations?
    *   Are typo injections producing plausible (if incorrect) words, or just random character sequences that never occur?
    *   Is whitespace/punctuation noise realistic, or just chaotic? Overly chaotic noise can sometimes confuse rather than generalize.
4.  **Country Label Consistency (for country-posterior head):**
    *   For *every* augmented address, especially those rewritten with foreign conventions, verify that the associated country label is the **true country of origin**, not the country whose conventions were borrowed. This is the crucial check for hard-negative mining.
5.  **Diversity and Coverage:**
    *   For a given gold record, are the `N` augmented versions sufficiently distinct? Or are many of them very similar, indicating redundant augmentation?
    *   Are all planned augmentation types (reordering, abbreviation, typo, case, mixed language, whitespace) present and reasonably distributed?
    *   Are under-represented countries getting a good diversity of augmentations?
6.  **Edge Cases:**
    *   Addresses with single-character components (e.g., "A Street"). How do typos affect them?
    *   Addresses with many numbers or special characters.
    *   Empty fields (if allowed).
7.  **UTF-8 Integrity:**
    *   Are there any garbled characters or encoding errors, especially in mixed-language or typo-injected strings?
8.  **Expected Output Format:**
    *   Does the final output for each training sample (e.g., `(augmented_text_bytes, BIO_labels_bytes, country_label)`) conform exactly to what the model expects?

By focusing on these points during manual inspection, you can catch critical data generation bugs that would otherwise lead to silent model failures and difficult-to-debug regressions in production.

## Conclusion
The corpus design demonstrates a strong understanding of the challenges involved in training a small, robust address parser. The "retrieval-success-invariant augmentation" framework is excellent. Implementing careful alignment for BIO labels post-typo injection, expanding the cross-shard canary matrix, and critically, employing hard-negative country mining will significantly enhance the model's generalization capabilities and prevent shard memorization. Vigilant manual inspection of generated samples for semantic preservation, label alignment, and country consistency will be your best defense against production regressions.

---

## Verbatim: gemini on ML stack (`gemini-ml.txt`, gemini-2.5-flash, exploration loop)

gemini-2.5-pro hit quota on the original attempt. The gemini-2.5-flash retry
spent the full 200s window in tool-use exploration without producing a
definitive recommendation. Output captured below for completeness.

I will now analyze `candle` and `burn` based on your specified criteria. First, I need to list the project directory to check for any relevant existing files or configurations that might inform my recommendation.
The project directory contains `scratch-burn`, `scratch-candle`, and `scratch-gbdt`, each with `Cargo.toml` and `src/main.rs`, indicating existing work with these frameworks. I will begin by examining the `Cargo.toml` and `src/main.rs` files for `scratch-burn` and `scratch-candle` to understand their current configurations and code.
`scratch-burn/Cargo.toml` confirms `burn` 0.20 is configured with the pure-Rust `ndarray` backend under an AGPL-3.0-or-later license, fitting the "all-Rust" constraint. Next, I'll examine `scratch-burn/src/main.rs`.
This `main.rs` for `scratch-burn` clearly defines a byte-level transformer with BIO and country heads, aligning with the user's requirements. It uses `burn::backend::NdArray` and includes performance metrics (p50, p90, p99 latency) for the forward pass, employing `burn::nn::transformer::TransformerEncoder`. Model size is also calculated. Now, I'll examine `scratch-candle/Cargo.toml`.
`scratch-candle/Cargo.toml` specifies `candle-core` and `candle-nn` versions 0.10, under an AGPL-3.0-or-later license, confirming `candle` is in consideration. Next, I'll examine `scratch-candle/src/main.rs`.
I've analyzed `scratch-candle/src/main.rs`, noting its hand-rolled `SelfAttention` and `EncoderLayer` contrasting with `burn`'s built-in `TransformerEncoder`, while both use identical model parameters for a fair latency comparison. My plan now is to: 1. Execute `scratch-burn` and `scratch-candle` for latency measurements, then discuss performance factors. 2. Research quantization support (int8, 4-bit) for both frameworks. 3. Evaluate training ergonomics, including multi-head, BIO loss, gradient accumulation, and mixed-precision. 4. Assess maintenance velocity via GitHub activity. 5. Confirm license compatibility. 6. Elaborate on built-in transformer blocks. 7. Investigate hidden gotchas like compile times and ONNX export. 8. Finally, consider a third framework alternative if appropriate. I will begin by running the benchmarks.
I need to compile and run `scratch-burn` and `scratch-candle` to gather performance data. The `cargo run --release` command optimizes and executes the Rust code, which then outputs performance metrics. I'll start with `scratch-burn`.
I made an error in attempting to run the command again. Without direct execution, I'll provide a recommendation for `burn` and `candle` based on general knowledge and documentation, noting the lack of specific performance benchmarks. My updated plan includes researching inference latency (discussing general characteristics and influences like BLAS, intrinsics, and autovectorization, with the caveat of missing specific model data), quantization, training ergonomics, maintenance velocity via GitHub activity, licensing, built-in transformer blocks, potential gotchas (compile times, PyTorch compatibility, ONNX export), and alternative frameworks. I'll begin by researching inference latency and quantization.
Direct "burn ndarray vs. candle cpu" performance benchmarks are elusive. I'll research each framework independently. For inference latency, I'll investigate `burn`'s `ndarray` backend and `candle`'s CPU backend, focusing on pure Rust implementation, CPU intrinsics, and matrix multiplication handling. For quantization, I'll examine official documentation and discussions for both `burn` and `candle` regarding INT8/4-bit support for custom transformers. I'll begin with `burn`'s `ndarray` backend performance and quantization.
"Burn framework ndarray backend performance" yielded no results. I'll broaden my search for `burn`'s general CPU performance and `ndarray` backend details. Then, I'll research `candle`'s CPU performance. I'm starting with "burn framework cpu performance."
Direct searches for "burn framework cpu performance" are failing, so I'll shift strategy. Instead of benchmarks, I'll investigate the underlying numerical operation implementations for both `burn`'s `ndarray` backend and `candle`'s CPU backend. For `burn`, I'll research "rust ndarray performance" and `burn`'s backend architecture. For `candle`, I'll look into "candle cpu backend implementation" and its matrix multiplication methods. Starting with "rust ndarray performance."
My web searches for "rust ndarray performance" were unsuccessful. High-level queries aren't working. I'll pivot to more precise searches about `ndarray` and `candle`, focusing on SIMD and CPU optimizations. For `burn/ndarray`, I'll use "ndarray rust simd", "ndarray rust blas", "burn ndarray backend details", and "burn matrix multiplication optimization". For `candle/CPU`, I'll try "candle rust cpu optimization", "candle rust simd", "candle rust blas", and "candle-core matmul cpu implementation". I'll begin with "ndarray rust simd".
My "ndarray rust simd" search failed. Performance will now be discussed generally, without specific benchmarks, unless later research yields concrete data. I'm moving on to "Quantization story" for Burn and Candle, starting with "burn framework quantization int8" to find information on INT8/4-bit quantization, model compression, and low-precision inference.

---

## Verbatim: gemini errors (gemini-2.5-pro, original attempt)

Both calls failed with HTTP 429 "You have exhausted your capacity on this model."
The retry on gemini-2.5-flash succeeded for corpus and looped for ML stack.

```
Attempt 1 failed: You have exhausted your capacity on this model. Your quota will reset after 5s.. Retrying after 6320ms...
Attempt 1 failed: You have exhausted your capacity on this model. Your quota will reset after 6s.. Retrying after 7053ms...
Attempt 1 failed: You have exhausted your capacity on this model. Your quota will reset after 1s.. Retrying after 5191ms...
Attempt 1 failed: You have exhausted your capacity on this model. Your quota will reset after 1s.. Retrying after 5119ms...
Error executing tool run_shell_command: Tool execution denied by policy.
Attempt 1 failed: You have exhausted your capacity on this model. Your quota will reset after 0s.. Retrying after 5071ms...
Error executing tool run_shell_command: Tool execution denied by policy.
Attempt 1 failed: You have exhausted your capacity on this model. Your quota will reset after 2s.. Retrying after 5550ms...
Attempt 1 failed: You have exhausted your capacity on this model. Your quota will reset after 5s.. Retrying after 5511ms...
Attempt 1 failed: You have exhausted your capacity on this model. Your quota will reset after 5s.. Retrying after 5927ms...
Attempt 1 failed: You have exhausted your capacity on this model. Your quota will reset after 6s.. Retrying after 7612ms...
Attempt 1 failed: You have exhausted your capacity on this model. Your quota will reset after 3s.. Retrying after 5096ms...
Attempt 1 failed: You have exhausted your capacity on this model. Your quota will reset after 2s.. Retrying after 5448ms...
Attempt 1 failed: You have exhausted your capacity on this model. Your quota will reset after 2s.. Retrying after 5678ms...
Attempt 1 failed: You have exhausted your capacity on this model. Your quota will reset after 2s.. Retrying after 5460ms...
Attempt 1 failed: You have exhausted your capacity on this model. Your quota will reset after 3s.. Retrying after 5809ms...
```
