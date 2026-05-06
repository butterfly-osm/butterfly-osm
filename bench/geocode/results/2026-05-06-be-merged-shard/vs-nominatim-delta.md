# BE recall closure analysis — OA+OSM merged shard vs Nominatim

**Bench:** 1000 BE TSV queries, radius=100m, single-shard server with merged
`be.bfgs` (10.49M records, sha256
`cd879f0ba19f6b8be28a8e9ab271afd184cc945d939328769c4cb3245df372ed`).
Heuristic parser, no rerank model. Nominatim 4.5 on local Docker.

## Headline numbers

| Engine | top1 | top5 | p50 (ms) | p95 (ms) | no_result |
|---|---|---|---|---|---|
| Butterfly (OA+OSM merged) | **0.388** | 0.453 | 1.5 | 2.9 | 6 |
| Butterfly (OA-only baseline) | 0.329 | 0.381 | 3.6 | 4.7 | 25 |
| Nominatim | 0.830 | 0.848 | 29.2 | 107.2 | 133 |

**Closed gap:** +5.9 pp top1 from the data merge alone (0.329 → 0.388).
Below the 0.55 acceptance gate the spec asked for — see "Where the rest
of the gap lives" below for the explicit reason.

**No-result gain:** Butterfly never timing out vs Nominatim's 133 no-results
remains a real product win when the question is "does it return *something*".
Latency advantage 19× p50 also unchanged.

**Win/loss matrix at 100m radius:**

|   | Butterfly OK | Butterfly fail |
|---|---|---|
| Nominatim OK | 349 | 481 |
| Nominatim fail | 39 | 131 |

Butterfly wins on 39 queries Nominatim misses; Nominatim wins on 481
Butterfly misses; both lose on 131.

## Per-category breakdown

| category | n | OA-only top1 | OA+OSM top1 | Δpp | Nominatim top1 | gap pp |
|---|---|---|---|---|---|---|
| clean | 200 | 0.665 | **0.805** | +14.0 | 0.965 | 16.0 |
| typo | 200 | 0.420 | **0.500** | +8.0 | 0.495 | -0.5 (parity) |
| abbreviated | 200 | 0.250 | **0.305** | +5.5 | 0.845 | 54.0 |
| partial | 200 | 0.190 | **0.205** | +1.5 | 0.935 | 73.0 |
| reordered | 200 | 0.120 | **0.125** | +0.5 | 0.910 | 78.5 |

The merge moved exactly the categories the spec predicted: addresses
where the right `addr:*` row was in OSM but not in OpenAddresses.
**`clean` jumped 14 pp; `typo` jumped 8 pp.** Together those two are
80% of the headline +5.9 pp.

`abbreviated` / `partial` / `reordered` barely moved because for those
queries the right record IS already in the OA shard — the failure
mode is upstream parsing/scoring, not retrieval coverage.

## Recall vs radius (Butterfly merged)

| radius | overall | clean | typo | abbreviated | partial | reordered |
|---|---|---|---|---|---|---|
| 50 m | 0.350 | 0.775 | 0.470 | 0.285 | 0.135 | 0.085 |
| **100 m** | **0.388** | 0.805 | 0.500 | 0.305 | 0.205 | 0.125 |
| 250 m | 0.446 | 0.830 | 0.535 | 0.350 | 0.325 | 0.190 |
| 500 m | 0.518 | 0.845 | 0.590 | 0.435 | 0.420 | 0.300 |
| 1000 m | 0.604 | 0.895 | 0.640 | 0.560 | 0.545 | 0.380 |

At 1 km city-block radius we'd land at 60.4%; the bench's 100 m
threshold is precise-housenumber strict, which is the right product
metric, but it makes the parser/scorer gap visible and unavoidable.

## Where the rest of the gap lives

Failures by distance class (612 failures total, radius=100m):

- **6** no result at all — the shard genuinely doesn't index a match.
- **216** street-level miss (≤ 1 km from gold, median 414 m). Right
  street family found, wrong housenumber chosen.
- **390** city-level miss (> 1 km from gold). Right *street name*
  matched, but in the wrong locality.

Top failure facets (cat / facet → count):

- partial / no_postcode: 159 — query has no postcode; parser can't
  disambiguate `Sint-Jorisstraat` between Brugge and 60 other Belgian
  occurrences without postcode.
- abbreviated / has_abbreviation: 122 — `Kerkstr.` doesn't expand to
  `Kerkstraat` in the heuristic parser.
- reordered / has_diacritics: 42 — `4040 Herstal 1ère Avenue 50`
  (postcode-first ordering) confuses the heuristic regex parser.
- partial / has_diacritics: 31 (overlaps the no_postcode bucket).

These are all **parser/scoring** gaps, not data gaps:

1. **Abbreviation expansion** (`str.` → `straat`, `pl.` → `plein`,
   `bd.` → `boulevard`). Currently absent in the heuristic parser.
2. **Postcode-less locality grounding.** Without a postcode, the
   recall index returns 5+ candidate streets across BE; the heuristic
   scorer doesn't have a strong locality prior. Every other major
   geocoder runs locality matching as an explicit feature.
3. **Reordered query handling.** `4040 Herstal 1ère Avenue 50`
   parses cleanly only if the parser knows BE postcodes are 4-digit
   (the heuristic doesn't currently anchor on numeric tokens by length
   + country).
4. **Diacritic-tolerant matching.** `Bütgenbach` vs `Butgenbach` —
   the FST is folded but the postings layout doesn't always carry
   both forms with identical retrievability.

#220 (multi-country tagger v2) explicitly fixes #1, #3, #4 via the
neural tagger. #98 Phase 2 (learned retrieval-utility GBDT) explicitly
fixes #2. Neither is in scope for this PR — both ship via parallel
agents.

## Recommendations

This PR ships data-side closure. To reach 55% top1 (the spec's
acceptance gate) the rest of the closure has to come from:

1. **Land #220's tagger v2 neural model** — once `multi-country-large`
   training (in flight as of 2026-05-06 18:00) finishes, swap the
   server's `--parser heuristic` for `--parser neural --model
   geocode/data/models/multi-country-large.safetensors`. Past tagger
   eval shows abbreviated+reordered should jump 30-40 pp.
2. **Land #98 Phase 2 GBDT rerank** as the production scorer with
   explicit locality and postcode-presence features. The labels-from-
   bench corpus already exists.
3. **Diacritic policy review** — file follow-up issue: ensure
   `Bütgenbach`/`Butgenbach`/`butgenbach` all collapse to the same
   FST node *and* the same postings list.

Optional but cheap: ship a static abbreviation-expansion pass in the
heuristic parser (`Kerkstr.` → `Kerkstraat`, `Pl.` → `Plein`,
`Av.`/`Bd.` → French equivalents). One-day fix; might add another
3-5 pp on `abbreviated`.
