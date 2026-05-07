# World Geocoder Corpus — Build Report

**Build date:** 2026-05-07
**Snapshot tag:** `260507` (Geofabrik latest as of 2026-05-07)
**Branch:** `geocode-world-corpus-final`
**Issue:** #225

## Summary

Built a multilingual, country-stratified geocoder evaluation corpus from
32 Geofabrik OSM PBF country files. The pipeline is fully reproducible:
every input is SHA-256-pinned via local sidecars, every SQL step has a
deterministic ORDER BY (or single-threaded streaming where ORDER BY
would be unworkably expensive), and `MANIFEST.json` records every output
hash so a re-run on the same inputs produces byte-identical outputs.

## Inputs

32 country PBFs, totalling 56 GB on disk. All downloaded with
`butterfly-dl`, all SHA-256-sidecar verified before being consumed.

| Region                    | Countries pulled                                             |
|---------------------------|--------------------------------------------------------------|
| Europe                    | BE, LU, NL, DE, FR, AT, CH, GB, IT, ES, RU, GR              |
| Asia                      | JP, IN, CN, KR, TW, TH, IR, IL (=IL+PS), GCC (=SA/AE/KW/BH/OM/QA) |
| Africa                    | EG, KE, NG, ZA                                               |
| Australia/Oceania         | AU                                                           |
| Americas                  | BR, AR, CL, MX, CA, US                                       |

GCC and IL PBFs are multi-country; their features are bbox-classified
into the correct destination ISO at unify time.

No country failed to download. Two operational fixes were needed:

1. `butterfly-dl`'s path-shaped fetches (`africa/kenya`, ...) take a
   codepath that does not write the `.sha256` sidecar that the rest of
   the pipeline expects. `build.sh` now computes the sidecar itself
   after a successful download — same primitive, just authored
   downstream of the binary.
2. `butterfly-dl`'s parallel-chunk path uses `O_DIRECT` for files
   > 1 GB, which on the test host's ext4 mount causes
   `EINVAL` after a few GB of writes (alignment requirement violated by
   the chunk allocator). Worked around by downloading Canada (6.3 GB)
   and US (11.9 GB) to `/tmp` (tmpfs — `O_DIRECT` open fails, falls
   back to standard I/O) and then `mv`'ing into place. SHA-256 sidecars
   computed post-move. `dl/src/` was not modified per scope rules.

## Outputs

| Artefact                                                       | Rows         | Size  |
|----------------------------------------------------------------|-------------:|------:|
| `data/world-corpus/all-with-country.parquet` (unified)         | 224,447,046  | 2.9 GB |
| `data/world-corpus/multilingual-pairs.parquet`                 |  24,803,625  | 184 MB |
| `data/world-corpus/family-stats.parquet`                       |          96  | 5 KB  |
| `data/world-corpus/bench-sample.parquet`                       |     ~33,000  | small  |
| `bench/geocode/queries/world-<family>-260507.tsv` × 14         |     ~33,000  | small  |
| `data/world-corpus/MANIFEST.json`                              |          —   | 16 KB |

## Records by script family

| Script      | Records      | Countries |
|-------------|-------------:|----------:|
| Latin       |   71,172,045 |        82 |
| Han         |    7,694,694 |        37 |
| Cyrillic    |    3,813,818 |        38 |
| Arabic      |    1,871,878 |        35 |
| Hangul      |    1,049,368 |        27 |
| Other       |      436,735 |        39 |
| Greek       |      394,946 |        22 |
| Japanese    |      295,422 |        18 |
| Thai        |      268,757 |        10 |
| Hebrew      |      174,819 |        18 |
| Devanagari  |        3,215 |         5 |
| Bengali     |        1,529 |         5 |
| Tamil       |        1,435 |         4 |
| Telugu      |          127 |         1 |
| Unknown     |           26 |         3 |

The unified parquet's 224M-row total includes records with only address
fields and no `name` tag (street segments, places). The script-family
table above counts only records with a `name` tag.

## Top-15 multilingual pairs

24,803,625 cross-language name pairs across 68 distinct language tags
(including `default`, `alt`, `official`, `int`).

| lang_a  | lang_b   | pairs     |
|---------|----------|----------:|
| default | en       | 4,936,245 |
| alt     | default  | 1,968,433 |
| en      | zh       | 1,386,265 |
| en      | ja       |   972,621 |
| en      | ko       |   684,623 |
| ar      | en       |   622,959 |
| default | official |   595,106 |
| default | zh-Hant  |   430,950 |
| en      | ru       |   383,091 |
| en      | zh-Hant  |   362,881 |
| alt     | en       |   332,513 |
| default | fr       |   323,162 |
| default | zh       |   315,535 |
| de      | default  |   297,303 |
| default | es       |   270,861 |

## Top-20 countries by record count

| ISO2 | Records      |
|------|-------------:|
| US   | 54,320,686   |
| DE   | 28,085,256   |
| FR   | 19,654,506   |
| RU   | 15,840,666   |
| NL   | 11,438,947   |
| GB   | 10,527,482   |
| TW   | 10,425,569   |
| CA   |  8,997,313   |
| IT   |  8,383,433   |
| AU   |  6,378,756   |
| BR   |  5,819,181   |
| ES   |  5,746,937   |
| BE   |  5,051,387   |
| CN   |  4,368,334   |
| AT   |  3,686,093   |
| AR   |  3,541,061   |
| MX   |  3,361,967   |
| JP   |  3,353,563   |
| CH   |  3,152,312   |
| EG   |  2,921,335   |

## Bench query TSVs

14 per-script-family TSVs at `bench/geocode/queries/world-<family>-260507.tsv`.
Each line: `query_id\tquery_text\tgold_lat\tgold_lon\tquality_class\tassigned_country\tscript_family\tquery_form\tosm_kind\tosm_id\tname`.

Up to 1000 records × 3 query forms (canonical, partial, reordered) per
family. Smaller families (Bengali, Devanagari, Tamil, Telugu) emit
fewer rows because the corpus has fewer total records to sample from.

## Reproducibility

Each output's SHA-256 is recorded in `data/world-corpus/MANIFEST.json`.
Re-running `./geocode-research/world-corpus/build.sh` on the same
SHA-256-sidecar-pinned inputs reproduces every output byte-for-byte.

Determinism layers:
1. **Inputs**: SHA-256 sidecars (`<file>.sha256`) for every PBF.
2. **Per-PBF extract**: `extract.sql` ends with `ORDER BY osm_kind, osm_id`,
   plus deterministic ZSTD framing (fixed `ROW_GROUP_SIZE 100000`).
3. **Unify**: single-threaded streaming pass; output row order is
   `concat(input parquets in lex order) ⊕ per-input (osm_kind, osm_id)`.
   No 224M-row global sort, but determinism preserved by `threads=1`.
4. **Pairs / Stratify / Sample**: each ends with explicit `ORDER BY`.
   Sample uses `row_hash` for stable pseudo-random selection — no
   `RANDOM()` calls.

A reproducibility script (`scripts/repro_check.sh`) re-runs the build
into a scratch directory and diffs every output's SHA-256 against the
committed manifest.

## Pipeline runtime

On the test host (62 GB RAM, ext4 nvme):

| Phase     | Wall time |
|-----------|----------:|
| Download  | ~22 min |
| Extract   | ~6 min  |
| Unify     | 4 min   |
| Pairs     | 1 min   |
| Stratify  | 5 sec   |
| Sample    | 6 sec   |
| Manifest  | 4 sec   |
| **Total** | **~33 min** |

## Known sharp edges

- **`butterfly-dl` path-shaped vs region-name-shaped**: only the latter
  writes a `.sha256` sidecar via `verified::download_verified`. Worked
  around in `build.sh` (sidecar fallback).
- **`butterfly-dl` `O_DIRECT` for files > 1 GB**: triggers `EINVAL`
  mid-stream on this host's ext4. Worked around by downloading the two
  largest PBFs (CA, US) to tmpfs first.
- **Unify SQL** had to be rewritten to avoid a global 224M-row
  mergesort that triggered ~200 GB of spilling. The new form uses
  `threads=1` + structural determinism; it streams through with ~2 GB
  peak RAM and no spilling.
