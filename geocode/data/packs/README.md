# Country packs

Each `<iso2>.toml` in this directory is a **country pack** — the data
the geocoder needs to classify text against this country, build a
shard from this country's PBF, and dispatch reverse-geocode queries
that fall in this country's bbox.

Adding a country is a single TOML drop. No Rust changes. The
classifier, parser, bbox dispatcher, and shard builder all read from
this directory.

## Schema

```toml
[country]
iso2 = "JP"                    # ISO 3166-1 alpha-2, REQUIRED, validated
name = "Japan"                 # Human-friendly name, REQUIRED

[postcode]
# Regex matched against the raw query. Used by both the parser
# (extract postcode → blocker channel) and the classifier (postcode
# match → +3.0 log-evidence for this country). OPTIONAL — countries
# without a structured postcode (e.g. Ireland's Eircode is rare in
# OSM) can omit this section entirely.
regex = '\b\d{3}-?\d{4}\b'
position = "leading"           # leading | trailing | anywhere
canonicalize = "collapse_whitespace"  # none | collapse_whitespace | strip_country_prefix

[scripts]
# Unicode script signal for the classifier. The classifier counts
# how many input chars are in `dominant` vs `secondary` and adds
# fractional log-evidence (max +1.5 dominant, +0.6 secondary).
dominant = ["Han", "Hiragana", "Katakana"]
secondary = ["Latin"]

[lexical_cues]
# Per-cue boost added when the substring/word is present in the
# (lowercase) input. The cue list is the bulk of the classifier's
# discriminative power.
[[lexical_cues.match]]
substring = "都"               # exact bytes; matched as substring or word
boost = 2.5                    # log-evidence added to this country's score
kind = "substring"             # "substring" or "word"
[[lexical_cues.match]]
substring = "tokyo"
boost = 1.5
kind = "word"

[bbox]
# WGS84 land-area bbox, REQUIRED. Used by the reverse-geocode
# dispatcher to map (lat, lon) → country and as a tiebreak when
# multiple bboxes contain the point (smallest bbox wins).
min_lat = 24.00
max_lat = 45.60
min_lon = 122.90
max_lon = 153.00

[neighbours]
# Cross-border ambiguity hint (NOT used by the classifier yet —
# reserved for #96 §"Cross-Border Shard Co-location"). Empty for
# island countries.
codes = []

[source_priors]
# Per-source weighting for the shard merger. `osm` is OSM
# `addr:*` tags; `authoritative` is OpenAddresses (which ingests
# BAN / BAG / BeSt / G-NAF / state packs upstream — see
# `geocode-data/SOURCES.md`).
osm = 0.6
authoritative = 0.5

[osm_tags]
# Per-country OSM tag mapping. Most countries use the standard
# `addr:street` + `addr:housenumber` pattern; Japanese addresses
# come through `addr:full` because Japan doesn't number streets.
postcode = "addr:postcode"
street = "addr:full"           # JP override — defaults to "addr:street"
housenumber = "addr:housenumber"
city = "addr:city"
```

## Validation

Pack loading enforces:

- `[country].iso2` parses as a 2-uppercase-letter ISO 3166-1 alpha-2 code
- `[bbox]` lat/lon are in the valid WGS84 ranges and `min < max` on both axes
- `[postcode].regex` compiles
- `canonicalize` is one of `none`, `collapse_whitespace`, `strip_country_prefix`

A pack with any of the above wrong fails to load. The shipped 15
packs all pass these checks (`PackRegistry::shipped()` is unit-tested).

## Cookbook

### Adding a country

1. Pick the ISO 3166-1 alpha-2 code (e.g. `KE` for Kenya).
2. Copy the pack closest to your target (e.g. `gb.toml` for an
   English-language postcode-anchored country) into `<iso2>.toml`.
3. Update `[country]`, `[postcode]`, `[bbox]`.
4. Add 5-15 lexical cues. Major cities, distinctive street types,
   any unicode markers (script-distinctive characters get a free
   boost via `[scripts]`).
5. If the country uses authoritative data (not just OSM), update
   `geocode-data/SOURCES.md` and bump `[source_priors].authoritative`.
6. If the country's OSM tagging differs (Japan's block-based,
   Korea's road-name addresses), override `[osm_tags]`.
7. Add the pack to `PackRegistry::shipped()` in
   `geocode/src/routing/pack.rs` so it embeds in the binary.
8. Add a `tests/serve_the_world.rs::classifier_accuracy_60_inputs`
   row.

### Postcode regex tips

- **Word boundaries** (`\b`) are essential — without them, a 5-digit
  ZIP regex matches inside arbitrary numeric tokens.
- **Leading-digit constraints** disambiguate: `\b[1-9]\d{3}\b`
  excludes 0xxx forms that aren't valid for the country.
- **Test on real OSM data** — postcode formats published by UPU /
  national post offices sometimes differ from what's actually in OSM.

### Why TOML

Same parser butterfly-route uses for region indexes (`dl/regions/`).
Comment-friendly. Hand-editable. Operators can ship a country pack
override without recompiling the binary by mounting a directory and
running `butterfly-geocode serve --pack-dir /etc/geocode/packs`.

### `--pack-dir` semantics

The flag is additive over the shipped registry: every embedded pack
loads first; then every `*.toml` under `<pack-dir>` overlays the
matching ISO2. Files in `<pack-dir>` win on conflict. Missing files
leave the embedded pack untouched.

Example: to hot-patch BE's postcode regex without rebuilding:
```bash
mkdir -p /etc/geocode/packs
cp my-be-override.toml /etc/geocode/packs/be.toml
butterfly-geocode serve --shard-dir /data/shards --pack-dir /etc/geocode/packs
```
Bring the override TOML through the same validation gate as the
shipped packs (see "Validation" above) — a malformed override
**fails the boot** rather than silently degrading to the embedded
pack. Operators can keep partial coverage in the override directory
(only the countries they want to patch); the rest stay shipped.
