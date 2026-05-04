# Corpus Design Notes

Open design points the corpus generator implementer (and the future Phase 2
trainer in #98) should resolve before training. Anchored against the
external review in `EXTERNAL_REVIEW.md`.

## Current state (this PR)

- `geocode-training/corpus-gen/` is a working binary that reads a Belgium
  PBF and produces JSONL training samples.
- BIO labels are byte-aligned. Validated on the 1001-line fixture: zero
  byte-length mismatches between `text` and `bio_labels`.
- Augmentation strategies: 9 distinct rewrites + 1 combined (case + abbr).
- Cross-shard canary: BE-as-FR and BE-as-NL street-type swaps, written
  to a separate `canary.jsonl` and held out from training.
- Cli flag `--augmentations N` (default 10), `--bench-queries` to also
  emit the competitive bench query set.

Run on Belgium produces ~1.8M training records + 3.3K canary records in
about 9 seconds on a laptop.

## Open: country-head supervision (codex critique)

**Problem**: dropping postcode/locality from a BE address still leaves a
plausible BE record for the parser, but for the **country head**, the
record may now be country-ambiguous (the same string could plausibly
match FR, NL, or LU). The current generator stamps `country: "BE"`
unconditionally on every variant.

**Recommendation from codex**:

- Full country-head loss for fully informative variants (canonical, all
  fields present).
- Reduced-weight or masked country loss for country-ambiguous variants
  (drop-postcode, drop-city, mixed-language street swap).
- Optionally soft targets (BE: 0.6, FR: 0.2, NL: 0.2) when the rewrite is
  clearly ambiguous.

**Action**: this is a **trainer-side concern**, not a corpus-side one.
The corpus already records the augmentation kind in the `augmentation`
field of every record. The Phase 2 trainer should look up the
augmentation kind and weight or mask the country-head loss accordingly.

Concretely the trainer config should expose:

```
country_head_loss_weight = {
  "canonical": 1.0,
  "abbr_contract": 1.0,
  "abbr_expand": 1.0,
  "case_upper": 1.0,
  "case_lower": 1.0,
  "ws_noise": 1.0,
  "typo_injection": 1.0,
  "reorder_postcode_first": 1.0,
  "drop_postcode": 0.5,
  "drop_city": 0.3,
  "combined_case_abbr": 1.0,
  "canary_be_as_fr": 1.0,   # held-out, never seen at training
  "canary_be_as_nl": 1.0,   # held-out, never seen at training
}
```

The exact numbers are calibration knobs; the principle is "don't force
fake certainty on ambiguous positives".

## Open: lower N (codex says 6, we ship default 10)

**Codex**: at 2-4M params, breadth of unique addresses matters more than
many correlated rewrites of the same tuple. Recommends N=6 (1 canonical
+ 5 augmented).

**Gemini-flash**: recommends 5-15 with the caveat to start lower.

**Action**: corpus generator default stays at 10 because the marginal
cost of more augmentations is cheap (9 seconds for 1.8M records on
Belgium) and the trainer can downsample. The recommendation for the
Phase 2 trainer is to **train on N=6 first** and only increase if
evaluation shows under-generalization.

## Open: provenance-based labeling vs delta tracking

**Codex**: build a provenance map (each output codepoint owns a field
ID) when applying edits. This is more robust than mutating bytes and
re-deriving labels.

**Current implementation**: we track byte-level deltas across edits
within a single ASCII span and re-derive BIO from the (mutated text,
adjusted spans) pair. This works for ASCII edits inside a span and
falls back to "no typo" for non-ASCII. Acceptable for the research
generator. Documented in `augment.rs::typo_injection`.

**Action**: when the Phase 2 trainer cycle starts and we move
augmentation online (per codex's recommendation: "if you can do
augmentation on the fly, that is better than precomputing large N"),
upgrade to a provenance map. The codepoint-level edit script + BIO
projection is a small Rust crate, ~300 lines.

## Open: missing augmentation classes

Both reviewers flagged classes the current generator does NOT handle:

- **Unicode normalization** (`é` → `e`, NFC vs NFD, smart quotes,
  apostrophes). Belgium has `Wijnegem`-vs-`Wÿnegem`-style edge cases
  but they are rare.
- **House-number morphology**: `12A`, `12 A`, `12-A`, `12 bus 3`,
  `12 bis`, `12/3`. BOSA writes these consistently; users do not.
  Current generator preserves whatever OSM has, no canonicalization.
- **Postcode formatting**: BE postcodes are 4-digit (no ambiguity), but
  for FR/NL/LU/UK we will eventually need to handle hyphens, spaces,
  country prefixes.
- **Locality aliases / exonyms**: `Brussels` vs `Bruxelles` vs
  `Brussel`. Currently we use whatever `addr:city` says.
- **Country prefix omit/add**: ", Belgium" / ", BE" / nothing.
- **Truncation**: cutting an address at 64 bytes. The transformer's
  64-byte limit means we'll truncate at training time. The generator
  should produce explicit truncated examples so the model sees them.
- **Contextual noise**: "delivery to <address>" wrappings.

**Action**: defer to Phase 2 trainer landing. The generator is modular
(each strategy is one function in `augment.rs`), adding a new strategy
is ~30 lines. Track in #96 follow-up.

## Open: cross-shard canary is too narrow

**Codex**: "BE-as-FR is useful, but too narrow. Better recipe: a
counterfactual invariance suite with order, casing, abbreviation
regime, punctuation, multilingual swap, and dropped fields all varied
independently."

**Action**: defer. The current canary is a smoke test for shard
memorization; the full counterfactual suite is a Phase 2 evaluation
deliverable. The canary file format is the same as the training file
format, so the suite can be added by extending `canary.rs` with more
rewrite functions.

## Open: leave-one-style-family-out training

**Codex**: train without a given formatting regime, then test on it.
Catches memorization better than a fixed canary.

**Action**: trainer-side. Document the augmentation kind on every
record (already done) so the trainer can leave-one-out by filtering on
the `augmentation` field at training time.

## Open: linear classifier shard-prediction probe

**Codex**: train a linear classifier to predict source shard from the
parser's hidden states. If shard prediction is easy, leakage is real.

**Action**: trainer-side. Belgium MVP only has one shard (BOSA, plus
OSM addr nodes), so this probe needs at least two shards before it can
fire. Track for the second-country (France BAN or Netherlands BAG)
sprint.

## Sample inspection checklist

Per codex's "what to look for in the 1000-line sample":

- [x] BIO invariants hold (1 label per byte, validated by Python script
      against `belgium-sample.jsonl`: 1001 ok, 0 bad)
- [ ] Each augmented line still corresponds to the same structured
      tuple (visual spot check pending)
- [ ] Replacements only hit the intended field class (i.e.
      `abbr_contract` doesn't match "Rue" inside a proper name)
- [ ] Mixed-language swaps happen only in allowed zones (currently NOT
      gated by zone — known issue, fix in next sprint)
- [ ] House numbers stay attached to the right span under typo noise
- [ ] Dropped-field examples are still realistic
- [ ] No duplicates flooding the sample
- [ ] No train-style fingerprints (single-shard casing/separators) remain
