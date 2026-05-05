# Geocoder Source License + Format Inventory

Tracks: #96 (butterfly-geocode design), #91 (multi-region foundation).

This is the **data-prep manifest** for butterfly-geocode. It catalogs the
authoritative open-data address sources for each country, their
license, and how the importer maps them into the geocode shard's
normalized `AddressRecord` schema.

## Canonical authoritative source: OpenAddresses

[**OpenAddresses**](https://openaddresses.io) is butterfly-geocode's primary
authoritative-source layer. OA federates ~600 M addresses across ~40
countries through one normalised schema, weekly cadence, predominantly
CC-BY / public-domain licensing.

OA is **the** ingestion path for v5 shards (PR #96 §Data Sources). Country-
specific authoritative datasets (BOSA for Belgium, BAN for France, BAG
for the Netherlands, BD-Adresses for Luxembourg, BEV for Austria,
swisstopo for Switzerland, G-NAF for Australia, …) ship **upstream** of
OpenAddresses: the OA pipeline ingests each one via a per-source manifest
and republishes a normalised feed. Going through OA gives butterfly-
geocode one ingestion code path, one normalised schema, one update
cadence — instead of N heterogeneous loaders.

Operators wanting maximum recency for a single country can still point
the loader directly at the upstream pack (e.g. a fresh BOSA ZIP); OA's
schema is intentionally close to the upstream's so the same loader
handles both.

## Why OpenAddresses, not per-country datasets

- **One code path.** v4 of butterfly-geocode shipped a Belgium-only
  BOSA loader; the audit was unanimous that scaling that to FR/NL/LU/
  DE/AT/CH/AU/US was N×Rust modules. OA collapses N to 1.
- **Normalised schema.** Every OA feature has the same properties
  (`number`, `street`, `unit`, `city`, `district`, `region`, `postcode`,
  `id`, `accuracy`). Per-country quirks (BOSA's `box_number`, BAN's
  `rep`, BAG's `huisletter`) are folded into `unit` upstream.
- **Predictable cadence.** OA's CI re-ingests upstream weekly; operators
  pin a job ID and refresh on a known schedule.
- **License clarity.** OA filters out share-alike-restricted upstream
  packs and surfaces only feeds that are unrestricted-use-with-
  attribution. Operators can audit the per-source license through
  the OA source manifests at <https://github.com/openaddresses/openaddresses>.

## OpenAddresses URL discovery

The current job ID for any country lives at:

```
https://batch.openaddresses.io/api/data?source=<cc>
```

This returns a JSON array of `{source, layer, job, size, ...}`. Filter
where `layer == "addresses"` and `source` starts with `<cc>/` to find
the country's packs. The download URL for a job is:

```
https://v2.openaddresses.io/batch-prod/job/<id>/source.geojson.gz
```

The on-disk format is gzipped GeoJSON-seq (one JSON Feature per line).
butterfly-geocode's loader at `geocode/src/sources/openaddresses.rs`
streams it row-by-row.

## Per-country status (verified live 2026-05-05)

| Country | OA national pack? | OA size (compressed) | Job ID | Upstream source(s) | Wired in `dl/regions/` |
|---|---|---|---|---|---|
| **BE** | per-region (3 × 2 lang) | 23 MB + 23 MB + 120 MB + 152 MB + 64 MB + 57 MB | 824413 / 824412 / 824410 / 824409 / 824406 / 824408 | BOSA BeSt | YES — `belgium.toml` |
| **FR** | yes | ~795 MB | 743120 | BAN | YES — `france.toml` |
| **NL** | yes | ~296 MB | 823433 | BAG | YES — `netherlands.toml` |
| **LU** | yes | ~6 MB | 823471 | BD-Adresses | YES — `luxembourg.toml` |
| **DE** | no — per-state | 14 packs, 6 MB to 120 MB each | 824008 + 13 others | various Bundesländer | YES — `germany.toml` |
| **AT** | yes | ~71 MB | 824498 | BEV Adressregister | YES — `austria.toml` |
| **CH** | yes | ~154 MB | 824053 | swisstopo | YES — `switzerland.toml` |
| **US** | no — per-state | 50+ statewide, 4 MB to 337 MB each | 802939 / 822656 / 475152 / 820314 / 413823 / 821003 / 821928 / 823077 / 488124 | per-state open data | YES — `united-states.toml` |
| **AU** | yes | ~833 MB | 824415 | G-NAF | YES — `australia.toml` |
| **BR** | no — per-state | 8 statewide, 165 MB to 1.3 GB each | 824365 + 7 others | per-state | YES — `brazil.toml` |
| **JP** | no — per-prefecture | 8 prefectures, 11 MB to 36 MB each | 823591 + 7 others | per-prefecture | YES — `japan.toml` |
| **GB** | no | — | — | (no OA coverage; OSM-only) | only PBF |
| **IN** | no | — | — | (no OA coverage; OSM-only) | only PBF — `india.toml` |

To refresh job IDs:

```bash
# Pick a country and dump its OA inventory.
curl -sL 'https://batch.openaddresses.io/api/data?source=be' \
  | jq '.[] | select(.layer == "addresses" and (.source|startswith("be/"))) | {source, job, size}'
```

Update `dl/regions/<country>.toml` `[[address]]` blocks with the new
`job/<id>/source.geojson.gz` URLs and re-run the download chain.

## Normalized record schema

All authoritative sources land into the same flat record. `AddressRecord`
is the geocode crate's storage shape:

```rust
struct AddressRecord {
    lat: f64,                   // WGS84
    lon: f64,                   // WGS84
    street: String,             // canonical street name
    locality: String,           // city / municipality name
    housenumber: String,        // alphanumeric — "12", "12A", "12bis", "10-12"
    postcode: String,           // canonical postcode (no whitespace, no country prefix)
    source: SourceTag,          // OpenAddresses | Osm
    source_id: Option<String>,  // upstream stable id; OA's `id` field where present
}
```

**Invariants:**

- Coordinates are WGS84. OpenAddresses does the reprojection upstream
  for sources that publish in projected CRS (Lambert-93 for BAN, RD New
  for BAG, MGI/Lambert for BEV, LV95 for swisstopo); butterfly-geocode
  reads `[lon, lat]` straight off OA's published GeoJSON-seq.
- Postcodes are stored as published, **without** country prefix. The
  parser knows postcode format per country at query time.
- Multilingual aliases (e.g. Brussels = Bruxelles = Brussel = Brüssel)
  are handled at the **alias-table** layer: one record per
  `(canonical locality, alias)` pair. The canonical locality is the
  `(country, postcode_first_two, locality_canonical_id)` tuple; each
  alias adds one row pointing back to that canonical id. Worked
  example for Brussels:
  ```
  canonical_locality: (BE, "10", id=42 → "Bruxelles")
  alias:              (BE, "10", id=42, alias="Brussel",  lang="nl")
  alias:              (BE, "10", id=42, alias="Brussels", lang="en")
  alias:              (BE, "10", id=42, alias="Brüssel",  lang="de")
  ```
  OpenAddresses publishes per-language packs (e.g. `be/bru/bosa-region-brussels-fr` and `…-nl`); operators build per-language shards and merge them via `--merge`. The alias-table layer lands as a follow-up so the merger collapses these into one canonical row + N alias rows. Until then, the merger keeps both records per (#173 first-wins tie-break) and lookups resolve via the canonical row's inverted index.
- House numbers stay strings. Numeric coercion is wrong — alphanumeric
  ("12A"), hyphenated ("10-12"), bis/ter/quater suffixes are all
  semantically real.
- Multi-language records are emitted by language-tagged OA sources
  (e.g. `be/bru/bosa-region-brussels-fr` and `…-nl`). Operators
  build per-language shards and merge them via `--merge`.

## OpenAddresses Feature schema

Per <https://github.com/openaddresses/openaddresses/wiki/Conform>:

```json
{
  "type": "Feature",
  "properties": {
    "hash": "e9bfa4b3f42f842d",
    "number": "475",
    "street": "Chaussée de Mons",
    "unit": "RDC",
    "city": "Anderlecht",
    "district": "",
    "region": "",
    "postcode": "1070",
    "id": "BE-BRU:615867",
    "accuracy": ""
  },
  "geometry": {
    "type": "Point",
    "coordinates": [4.31653, 50.83595]
  }
}
```

The loader maps:

- `street` → `AddressRecord.street`
- `number` + (space + `unit` if non-empty) → `AddressRecord.housenumber`
- `city` → `AddressRecord.locality`
- `postcode` → `AddressRecord.postcode`
- `coordinates[0]` → `AddressRecord.lon`, `coordinates[1]` → `AddressRecord.lat`
- `id` → `AddressRecord.source_id`

For languages: pick per-language packs at ingest time (`be/bru/bosa-region-brussels-fr` vs `…-nl`). The merger keeps both records until the alias-table layer lands.

Records with empty `street` or empty `housenumber` are dropped — they
aren't addressable by the parser. Records with non-Point geometry
(LineString, Polygon — rare in OA but possible if an operator points
the loader at non-address OA data) are also dropped.

## Smoke test results (2026-05-05)

| Shard | Source | Records | Build time | Unique postcodes | Unique streets |
|---|---|---|---|---|---|
| BE-BRU-FR | OA `be/bru/bosa-region-brussels-fr` (job 824413) | 841,990 | 6.55 s | 26 | 4,706 |
| US-DC | OA `us/dc/statewide` (job 802939) | 142,820 | 0.88 s | 115 | 1,821 |

Live geocode round trip:

```
$ curl 'http://localhost:3055/geocode?q=Rue+Wayez+122+Anderlecht'
{
  "query": "Rue Wayez 122 Anderlecht",
  "country": "BE",
  "confidence": "accept",
  "count": 5,
  "results": [
    {
      "lat": 50.83543,
      "lon": 4.31111,
      "street": "Rue Wayez",
      "housenumber": "122",
      "postcode": "1070",
      "locality": "Anderlecht",
      "country": "BE",
      "score": 1.8000001
    },
    ...
  ]
}
```

Full curl proofs in `geocode/data/proof/10-belgium-openaddresses.txt`
and `geocode/data/proof/11-us-dc-openaddresses.txt`.

## OSM fallback

Countries without OA coverage (today: GB, IN) fall back to OSM
`addr:*` tags. The same `osm_extract` two-pass extractor handles
every PBF, and the merge dedup picks OA records over OSM where both
exist for the same physical address.

## License compatibility

OpenAddresses publishes per-source license metadata at
<https://github.com/openaddresses/openaddresses/tree/master/sources>.
The OA pipeline filters out share-alike-restricted feeds — only
unrestricted-use-with-attribution sources reach `v2.openaddresses.io`.

For Belgium specifically: BOSA's BeSt is published under the Belgian
Open Data License (CC-BY compatible). The OA-republished feeds inherit
the upstream license; the OA contributor agreement requires upstream
license preservation.

A formal multi-jurisdiction license review is filed as a follow-up
ticket — the engineering decision here is to proceed under the
OA-mediated license clarity while that review runs.

## Future work: per-country authoritative source ingestion

If an operator needs a feed OA does not yet ingest, or wants to bypass
OA's update lag (typically days, occasionally weeks), the path forward
is a new module under `geocode/src/sources/`. The existing
[`Source`](../geocode/src/sources/mod.rs) trait was designed for this:
implement `stream()` for the new format, add a new `SourceTag` variant
(reserves a new code in the BFGS record byte — bumps the on-disk
version), and wire the CLI's `--source` dispatch.

This is filed as a post-#96 follow-up. The user-visible ergonomics
goal is to keep OA as the default, deferring to per-country loaders
only when OA is materially behind.
