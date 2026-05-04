# Cross-Border Cluster Manifest

Tracks: #96 (butterfly-geocode design — *Cross-Border Shard Co-location*).

The geocoder design commits to **physical co-location of border-region
shard fragments** for predictable cross-border ambiguity clusters.
This is a data-layout decision, not a runtime-policy decision: the
shard byte layout is what makes the "country ∈ {A, B}" lookup
cheap, not a runtime branch.

This document defines:

1. The cluster membership and language landscape per cluster.
2. The postcode-format compatibility matrix per cluster.
3. The locality-name conflict examples each cluster needs to handle.
4. The concrete fragment-layout scheme the shard builder will use.

The corresponding country region indexes live in `dl/regions/`.
The authoritative source URLs and field mappings live in
`SOURCES.md` (this directory).

## Cluster #1 — BE / FR / NL / LU / DE (densest)

**Members:** Belgium, France, Netherlands, Luxembourg, Germany.

**Why this is a cluster:** The BENELUX-FR-DE quadripoint is the
densest cross-border-query region in Europe. Daily commuters cross
3+ borders, postal services route across NL→BE→LU regularly, and
freight tooling routinely receives partial addresses without a
country code.

### Languages

| Country | Official | Cross-border secondary |
|---|---|---|
| BE | nl, fr, de | (de in eastern cantons) |
| FR | fr | (small de in Alsace, nl in Flanders historical) |
| NL | nl | fy in Friesland |
| LU | lb, fr, de | en in business contexts |
| DE | de | — |

The geocoder cannot assume single-language input anywhere in this
cluster. A query like "Rue de la Gare, Liège" is FR-language but
firmly Belgian.

### Postcode formats

| Country | Format | Example | Conflict in cluster |
|---|---|---|---|
| BE | `\d{4}` | 1000 | Overlaps numerically with FR/AT/LU (all 4-5 digit) |
| FR | `\d{5}` | 75001 | Distinct length |
| NL | `\d{4} ?[A-Z]{2}` | 1011 AB | Distinct shape (alpha suffix) |
| LU | `L?-?\d{4}` | L-2453 / 2453 | Numerically overlaps with BE |
| DE | `\d{5}` | 10115 | Numerically overlaps with FR |

**Conflict rule:** A bare 4-digit postcode is ambiguous BE / LU / (AT).
A bare 5-digit postcode is ambiguous FR / DE / (ES / IT). The
parser's cheap classifier (#96 routing stage) cannot resolve from
postcode alone — it needs a script/lexical signal too.

### Locality-name conflicts (real examples)

| Canonical | Aliases / collisions |
|---|---|
| Brussels (BE) | Bruxelles (FR) / Brussel (NL) / Brüssel (DE) — all four are correct, all four show up in input |
| Liège (BE) | Lüttich (DE) / Luik (NL) — all valid, mixed-language signage in town |
| Antwerp (BE) | Antwerpen (NL/DE) / Anvers (FR) |
| Aachen (DE) | Aix-la-Chapelle (FR) / Aken (NL) — borders BE/NL |
| Maastricht (NL) | "Maestricht" archaic FR; same name in DE |
| Luxembourg City (LU) | Luxembourg (FR) / Lëtzebuerg (LB) / Luxemburg (DE) |
| Strasbourg (FR) | Straßburg (DE) — Alsace cross-border |
| Lille (FR) | Rijsel (NL) — used by Belgian Flemish speakers |

The alias table is a first-class shard input, not a CSV
post-processing step. Multilingual locality names appear in
authoritative sources (BeSt has `municipality_name_{nl,fr,de}`,
BAN has `nom_commune` only in fr but the shard layer joins
against an OSM-derived multilingual table).

### Multilingual signage convention

- BE: bilingual or trilingual signage by region (Flemish, Walloon,
  Brussels-Capital, German-speaking community). Street names appear
  in 2–3 languages on the same sign.
- LU: triple-language street signs (lb / fr / de), but databases
  typically store fr only; lb/de are alias-table responsibilities.
- DE/FR border (Alsace, Saar): predominantly fr or de with
  occasional dual-language signage.
- NL/BE: typically nl-only on each side, but Brussels and the
  Flemish/Walloon border is bilingual.

## Cluster #2 — AT / DE / CH (German-language)

**Members:** Austria, Germany, Switzerland.

**Why this is a cluster:** The DACH region shares a primary language
(de) with strong dialectal variation, partial postcode overlap, and
heavy cross-border commuter traffic (Bodensee, Salzburg/Bayern,
Basel/Lörrach).

### Languages

| Country | Official | Cross-border secondary |
|---|---|---|
| AT | de | (sl in Carinthia, hu in Burgenland) |
| DE | de | (da in Schleswig, nds regional) |
| CH | de, fr, it, rm | English business contexts |

Switzerland is multilingual within itself, and CH-DE is German but
with Swiss orthography (no ß, "Strasse" not "Straße").

### Postcode formats

| Country | Format | Example | Conflict in cluster |
|---|---|---|---|
| AT | `\d{4}` | 1010 | Numerically overlaps with CH/BE/LU |
| DE | `\d{5}` | 10115 | Distinct from AT/CH |
| CH | `\d{4}` | 8001 | Numerically overlaps with AT |

**Conflict rule:** AT and CH share 4-digit postcodes outright.
1xxx (AT: Vienna) overlaps 1xxx (CH: Lausanne). Disambiguation
requires locality, language script (ß vs ss), or country prefix.

### Locality-name conflicts

| Canonical | Aliases / collisions |
|---|---|
| Wien (AT) | Vienna (en) / Vienne (fr) — Vienne is also a city in FR |
| München (DE) | Munich (en) / Monaco (it) — disambiguates only by context |
| Köln (DE) | Cologne (en/fr) / Keulen (nl) |
| Zürich (CH) | Zurich (no umlaut) — common ASCII rendering |
| Bern (CH) | Berne (en/fr) — disambiguates only by context |
| Sankt Gallen (CH) | "St. Gallen" / "Saint-Gall" (fr) |

### Multilingual signage convention

- DE: monolingual de.
- AT: monolingual de (with regional dialectal terms in addresses
  rare but present in Carinthia, Tyrol).
- CH: language by canton — German cantons monolingual de (Swiss
  spelling), French cantons monolingual fr, Ticino monolingual it,
  Graubünden trilingual de/it/rm.

## Cluster #3 — ES / PT (Iberian)

**Members:** Spain, Portugal.

**Out of scope for this prep pass.** Documented for completeness;
region indexes will be added in a follow-up.

**Languages:** es, ca, gl, eu (ES); pt (PT). Cross-border traffic
heavy in Galicia/Norte (gl/pt are mutually intelligible).

**Postcode formats:** ES `\d{5}`, PT `\d{4}-\d{3}`. Numerically
distinguishable, no conflict.

**Authoritative sources:**

- ES: CNIG (Centro Nacional de Información Geográfica) Cartociudad
- PT: CTT (Correios de Portugal) PT-Postal — license restricted, not open

## Cluster #4 — Balkans (RS / HR / BA / SI / ME / MK)

**Members:** Serbia, Croatia, Bosnia & Herzegovina, Slovenia,
Montenegro, North Macedonia.

**Out of scope for this prep pass.** Documented for completeness.

**Why this is a cluster:** Mixed Latin/Cyrillic scripts, shared
locality-name space across post-Yugoslav successor states, weak
authoritative-data coverage.

**Postcode formats:** mostly 5-digit; SI and HR are distinguishable
by lexical / script signals, not by postcode alone.

## Cluster #5 — Arabic-script multilingual regions

**Out of scope for this prep pass.** Documented for completeness.

The geocoder design (#96) calls these out as a co-location cluster
because **the script itself** is the binding signal — Arabic input
benefits from being matched against the Arabic-script alias table
across ALL Arabic-speaking countries simultaneously, since
disambiguation by country happens AFTER fuzzy script matching, not
before.

## Fragment-layout scheme

The geocode shard's on-disk format (which lives in `geocode/src/shard/`)
will partition records by:

```
key = (cluster_id, country, postcode_first_two_digits)
```

with each `(cluster, country, pc2)` triple producing one **fragment**:
a contiguous record slab + its inverted-index slabs (postcode trie,
locality alias map, street trie, house-number sorted array). All
fragments belonging to the same `cluster_id` are laid out
back-to-back in the on-disk file so:

- Single-country queries: one fragment touched, ~hundreds of KB
  to a few MB depending on country/postcode density.
- Border-region cross-country queries within the same cluster:
  N fragments touched, but they are page-adjacent on disk and in
  the OS page cache, so the second fragment is essentially free
  once the first is hot.
- Cross-cluster queries (rare): N fragments touched at random
  offsets, paying full mmap-fault cost. This is the path the
  design accepts as expensive — the optimization is the common
  case, not the worst case.

### Why `(cluster, country, pc2)`?

- `cluster_id` first — primary memory-locality key. All fragments
  in cluster #1 are physically adjacent, regardless of country.
- `country` second — secondary key. Within cluster #1, all BE
  fragments come before all FR fragments etc., so a confirmed-
  country query lives in one contiguous range.
- `postcode_first_two_digits` third — granularity knob. 100
  fragments per country × 5 countries = 500 fragments per cluster.
  At ~6.7M BeSt records, that's ~67k records per BE fragment on
  average, which is small enough to fit in L3 for hot fragments
  and page-fault cheaply for cold ones.

### Why `pc2` and not country alone?

Without `pc2`-level partitioning, a single-country query against
France pulls in 26M records' worth of mmap pages even though the
query is interested in one Paris postcode. With `pc2`, the parser
hands the executor a postcode prior, the executor selects one
~260k-record fragment, and only that fragment is faulted in.

### Why not finer than `pc2`?

`pc3` (1000 fragments per country) over-fragments smaller countries
(LU has ~250k addresses across ~80 postcodes — `pc2` is already
~4 fragments per `pc2` value, `pc3` would be ~ten records per
fragment, defeating the slab benefit). `pc2` is the empirical
sweet spot from the typical-fragment-size target of 50k–500k
records.

### Shard-file structure (sketch)

```
geocode-{cluster_id}.shard
├── header                        — magic, version, cluster id, table of contents
├── alias_table                   — multilingual locality + street aliases (cluster-wide)
├── per-country sections (sorted by ISO code)
│   └── per-pc2 fragments (sorted by postcode prefix)
│       ├── records slab          — [AddressRecord; n], packed, fixed stride
│       ├── postcode index        — sparse FST or sorted-key array
│       ├── locality index        — alias-id → record-offset adjacency
│       ├── street trie           — fst::Map of canonical street name → offset list
│       └── housenumber array     — sorted (street_id, number) pairs for binary search
└── footer                        — CRC64 over body, magic
```

This is a sketch, not a binding spec. The on-disk format details
land in `geocode/src/shard/` once the geocode crate is past the
scaffolding stage. The commitment captured here is:

- One shard per cluster (not one per country).
- Within a shard, fragments ordered by `(country, pc2)`.
- Mmap-friendly fixed-stride records.
- Cluster-wide alias table (so BE↔FR↔NL↔DE locality aliases share
  storage rather than being duplicated per country).

### What the parser hands the executor

```rust
struct ShardLookup {
    cluster: ClusterId,
    countries: SmallVec<[CountryId; 4]>, // typically 1, sometimes 2-3 in border regions
    postcode_priors: SmallVec<[(CountryId, Pc2); 4]>,
}
```

The executor walks `countries` in posterior-confidence order;
for each, it picks the candidate `pc2` fragments from
`postcode_priors`. If `postcode_priors` is empty (no postcode in
the query), the executor falls back to all `pc2` fragments for the
country, which costs more page-cache but stays correct.
