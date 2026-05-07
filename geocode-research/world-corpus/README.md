# World Geocoder Corpus — Reproducible Build

Reproducible DuckDB-based extraction pipeline that turns a fixed set of dated
Geofabrik OSM PBF snapshots into a multilingual, country-stratified geocoder
corpus, plus deterministic bench-query TSVs sampled per script family.

## Reproducibility contract

A reviewer who clones this repo on a fresh Linux box, installs the listed
dependencies, runs `./build.sh`, and waits for it to finish must obtain
**byte-identical output parquet files and bench TSVs** to those whose SHA-256
hashes are recorded in `MANIFEST.json` (committed to the tree once the build
has been validated).

To enable that, every artefact in this pipeline is pinned:

| Layer            | Pin                                                      |
|------------------|----------------------------------------------------------|
| OSM data         | Geofabrik **dated monthly snapshot `260401`** (2026-04-01) for every country in `manifest_input.tsv`. Geofabrik publishes immutable archives at `<region>-YYMMDD.osm.pbf`; we never use the mutating `*-latest.osm.pbf` symlinks. Every input download is SHA-256 verified against the manifest before being consumed. |
| DuckDB           | v1.5.2 at `/usr/local/bin/duckdb` (verified by `build.sh`).        |
| Spatial extension | DuckDB-managed; recorded by extension_version in `MANIFEST.json`. |
| SQL queries      | Every `COPY ... TO ...` has an explicit `ORDER BY`. Every random sample uses `SETSEED(0.42)` so output row ordering is reproducible. |
| Build script     | `build.sh` is itself SHA-256-stamped into `MANIFEST.json`. |

If two consecutive runs of `build.sh` produce different outputs on the same
machine and dependency stack, that is a bug — open an issue.

## Snapshot date — why `260401`, not `250401`

The brief asked for a `2025-04-01` snapshot. By the time this build was wired
up, the most recent stable Geofabrik first-of-month snapshot before the build
date (2026-05-07) is `260401` (2026-04-01). Geofabrik's dated archives
prior to mid-2025 are kept on annual cadence (`*-260101`, `*-250101`, ...);
`*-250401` does not exist as a file at Geofabrik. We pin to the youngest
fully-immutable monthly snapshot the publisher offers, which is `260401`.
The `DATE_TAG` env var in `build.sh` makes this trivial to re-pin to a
different snapshot in a follow-up build.

## Pipeline

```
manifest_input.tsv  ──┐
                      ▼
  download + sha256-verify  ──►  data/world-corpus/inputs/<region>-260401.osm.pbf
                                                        │
                                                        ▼  extract.sql
                                  data/world-corpus/<iso2>.parquet  (per country)
                                                        │
                                                        ▼  unify.sql
                                  data/world-corpus/all-with-country.parquet
                                                        │
                                          ┌─────────────┼─────────────┐
                                          ▼             ▼             ▼
                                      pairs.sql    stratify.sql  sample_bench.sql
                                          │             │             │
                                          ▼             ▼             ▼
                          multilingual-pairs.parquet  family-stats.parquet
                                                                bench/geocode/queries/world-<family>-260401.tsv
```

`build.sh` runs all phases idempotently. If a downloaded PBF already matches
its manifested SHA-256, it is not re-downloaded. If a per-country parquet
already exists and its source PBF SHA-256 has not changed since
`MANIFEST.json` was written, the extraction step for that country is skipped.

## Dependencies

Pinned via `build.sh`:

- `duckdb` v1.5.2 (binary at `/usr/local/bin/duckdb`)
- `bash`, `curl`, `sha256sum`, `jq`, `awk`, `sed`, `tee` — POSIX-ish standard tooling
- DuckDB `spatial` extension (auto-installed via `INSTALL spatial; LOAD spatial`)

## Usage

```bash
cd geocode-research/world-corpus
./build.sh                    # run end-to-end
./build.sh --phase extract    # run a single phase (download, extract, unify, pairs, stratify, sample, manifest)
DATE_TAG=260401 ./build.sh    # pin to a specific snapshot (default 260401)
WORK_DIR=/tmp/wc ./build.sh   # override the per-build working dir (default data/world-corpus/)
```

## Outputs (gitignored except MANIFEST + bench TSVs)

| Artefact                                                       | Tracked in git?    |
|----------------------------------------------------------------|--------------------|
| `data/world-corpus/inputs/<region>-260401.osm.pbf`             | No (large)         |
| `data/world-corpus/<iso2>.parquet`                             | No                 |
| `data/world-corpus/all-with-country.parquet`                   | No                 |
| `data/world-corpus/multilingual-pairs.parquet`                 | No                 |
| `data/world-corpus/family-stats.parquet`                       | No                 |
| `data/world-corpus/MANIFEST.json`                              | **Yes** (small)    |
| `bench/geocode/queries/world-<family>-260401.tsv`              | **Yes** (small)    |
| `bench/geocode/results/2026-05-07-world-corpus/REPORT.md`      | **Yes**            |

## Reproducibility test

After a successful run, `build.sh` writes a `MANIFEST.json` containing every
input and output SHA-256. The included `scripts/repro_check.sh` re-runs the
build in a scratch directory, recomputes every output SHA-256, and diffs
against the committed manifest. CI is expected to pin output identity via
this check.

## Out of scope

This directory MUST NOT touch production Rust code (`geocode/src/`,
`route/src/`, `dl/src/`, `butterfly-common/`). It only consumes the
read-only `geocode/data/packs/*.toml` country bbox tables.
