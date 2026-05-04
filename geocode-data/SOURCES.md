# Geocoder Source License + Format Inventory

Tracks: #96 (butterfly-geocode design), #91 (multi-region foundation).

This is the **data-prep manifest** for butterfly-geocode. It catalogs the
authoritative open-data address sources for each country in cross-border
cluster #1 (BE / FR / NL / LU / DE) and cluster #2 (AT / DE / CH), the
license each one ships under, and the field mapping each importer will
use to land records into the geocode shard's normalized
`AddressRecord` schema.

The shard importer itself lives in `geocode/` and is being scaffolded
in parallel with this prep work. The intent here is that wiring an
authoritative source into the geocoder is a copy-paste from the
"Field mapping" sketches below, not a research task.

## Normalized record schema

All authoritative sources land into the same flat record. `AddressRecord`
is the geocode crate's storage shape:

```rust
struct AddressRecord {
    id: u64,                  // monotonic shard-local id
    country: CountryId,       // ISO-3166-1 alpha-2 (BE, FR, NL, LU, DE, AT, CH)
    lat: f64,                 // WGS84
    lon: f64,                 // WGS84
    postcode: Option<String>, // canonical postcode (no whitespace, no country prefix)
    locality: Option<String>, // city / municipality name as published
    street: Option<String>,   // canonical street name (no abbreviation expansion at ingest)
    housenumber: Option<String>, // alphanumeric — "12", "12A", "12bis", "10-12"
    source: SourceTag,        // BAN | BAG | BOSA | BD_ADRESSES | BEV | SWISSTOPO | OSM
    source_id: Option<String>, // upstream stable id where the source provides one
}
```

**Invariants:**

- Coordinates are WGS84. Sources publishing in projected CRS
  (Lambert-93 for BAN, RD New for BAG, MGI/Lambert for BEV,
  LV95 for swisstopo) are reprojected at ingest with `proj4rs`.
- Postcodes are stored as published, **without** country prefix. The
  parser knows postcode format per country at query time.
- Multilingual aliases (e.g. Brussels = Bruxelles = Brussel = Brüssel)
  are handled at the **alias-table** layer (one record per (canonical
  locality, alias)), not by duplicating `AddressRecord`s.
- House numbers stay strings. Numeric coercion is wrong — alphanumeric
  ("12A"), hyphenated ("10-12"), bis/ter/quater suffixes are all
  semantically real.

## Source inventory

### Belgium — BeSt Address (BOSA)

| Property | Value |
|---|---|
| Dataset name | BeSt Address (Belgium Standard Address) |
| Publisher | FPS BOSA — DG Digital Transformation |
| Landing page | https://data.gov.be/en/datasets/fpsbosa-dis-best-csv-deriv |
| Direct files (CSV, regional) | `https://opendata.bosa.be/download/best/openaddress-bevlg.zip` (Flanders), `openaddress-bewal.zip` (Wallonia), `openaddress-bebru.zip` (Brussels) |
| Direct file (XML, full) | `https://opendata.bosa.be/download/best/best-full-latest.xml.zip` |
| License | Belgian Open Data License (compatible with CC-BY) |
| Format | CSV (derived) + XML (canonical) |
| Record count | ~6.7 M addresses |
| Cadence | Monthly |

**Status:** Belgium MVP is being built by another agent. The current
`dl/regions/belgium.toml` does NOT include BeSt — adding it is a
follow-up tracked in this doc, not in scope for this prep pass.

**Field mapping sketch (CSV variant):**

```rust
// CSV columns: address_id, street_id, street_name_nl, street_name_fr,
//              street_name_de, house_number, box_number, postcode,
//              municipality_name_nl, municipality_name_fr,
//              municipality_name_de, lat, lon, ...
fn from_bosa_csv(row: &BosaRow, lang: Lang) -> AddressRecord {
    AddressRecord {
        id: next_id(),
        country: CountryId::BE,
        lat: row.lat,
        lon: row.lon,
        postcode: Some(row.postcode.trim().to_string()),
        locality: Some(pick_lang(lang, &row.muni_nl, &row.muni_fr, &row.muni_de)),
        street: Some(pick_lang(lang, &row.street_nl, &row.street_fr, &row.street_de)),
        housenumber: Some(format_belgian_number(&row.house_number, &row.box_number)),
        source: SourceTag::Bosa,
        source_id: Some(row.address_id.clone()),
    }
}
```

The `pick_lang` step is **not** an ingest-time choice — Belgium
needs every language as a queryable alias. The importer emits one
record per language per address, all sharing the same `source_id`,
linked through the alias table.

### France — Base Adresse Nationale (BAN)

| Property | Value |
|---|---|
| Dataset name | Base Adresse Nationale (BAN) |
| Publisher | DINUM / Etalab + IGN + La Poste |
| Landing page | https://adresse.data.gouv.fr/data/ban/adresses/latest/csv/ |
| Direct file (national) | https://adresse.data.gouv.fr/data/ban/adresses/latest/csv/adresses-france.csv.gz |
| Direct files (departmental) | https://adresse.data.gouv.fr/data/ban/adresses/latest/csv/adresses-{dep}.csv.gz |
| License | Licence Ouverte 2.0 (Etalab, ≈ CC-BY) |
| Format | gzip CSV, semicolon-separated, UTF-8, ~921 MB compressed |
| Record count | ~26 M addresses |
| Cadence | Daily |

**Field mapping sketch:**

```rust
// CSV columns: id;id_fantoir;numero;rep;nom_voie;code_postal;
//              code_insee;nom_commune;code_insee_ancienne_commune;
//              nom_ancienne_commune;x;y;lon;lat;type_position;
//              alias;nom_ld;libelle_acheminement;nom_afnor;source;
//              date_der_maj;certification_commune
fn from_ban(row: &BanRow) -> AddressRecord {
    AddressRecord {
        id: next_id(),
        country: CountryId::FR,
        lat: row.lat,
        lon: row.lon,
        postcode: Some(row.code_postal.clone()),
        locality: Some(row.nom_commune.clone()),
        street: Some(row.nom_voie.clone()),
        housenumber: format_fr_number(&row.numero, &row.rep), // "rep" = bis/ter/quater
        source: SourceTag::Ban,
        source_id: Some(row.id.clone()),
    }
}
```

`type_position` is a quality flag (`entrance`, `building`, `parcel`,
`interpolation`, `area`). The importer drops `area` (centroid-only)
records since the geocoder needs a real coordinate.

### Netherlands — BAG (Basisregistratie Adressen en Gebouwen)

| Property | Value |
|---|---|
| Dataset name | BAG — Basic Registration Addresses and Buildings |
| Publisher | Kadaster (via PDOK) |
| Landing page | https://www.kadaster.nl/-/gratis-download-bag-extract |
| Direct file | https://service.pdok.nl/kadaster/adressen/atom/v1_0/downloads/lvbag-extract-nl.zip |
| Atom feed (programmatic) | https://service.pdok.nl/kadaster/adressen/atom/v1_0/adressen.xml |
| License | CC0 (public domain) for the open extract |
| Format | GML / XML inside ZIP, ~2.8 GB compressed |
| Record count | ~9 M addresses |
| Cadence | Monthly (8th of the month) |

**Field mapping sketch (BAG GML, "verblijfsobject" object):**

```rust
// BAG ships as nested GML. The relevant entity for AddressRecord is
// `nummeraanduiding` (street + housenumber + postcode) joined to
// `verblijfsobject` (the unit, with geometry and locality via
// `woonplaats`).
fn from_bag(num: &Nummeraanduiding, obj: &Verblijfsobject, plaats: &Woonplaats) -> AddressRecord {
    let (lon, lat) = rd_to_wgs84(obj.geometrie.point); // RD New EPSG:28992 -> WGS84
    AddressRecord {
        id: next_id(),
        country: CountryId::NL,
        lat, lon,
        postcode: num.postcode.clone(),
        locality: Some(plaats.naam.clone()),
        street: Some(num.openbare_ruimte_naam.clone()),
        housenumber: Some(format_nl_number(num.huisnummer, &num.huisletter, &num.huisnummertoevoeging)),
        source: SourceTag::Bag,
        source_id: Some(num.identificatie.clone()),
    }
}
```

### Luxembourg — BD-Adresses

| Property | Value |
|---|---|
| Dataset name | Adresses géoréférencées (BD-L-AD / BD-Adresses) |
| Publisher | Administration du Cadastre et de la Topographie (ACT) |
| Landing page | https://data.public.lu/en/datasets/adresses-georeferencees-bd-adresses/ |
| API discovery | https://data.public.lu/api/1/datasets/adresses-georeferencees-bd-adresses/ |
| License | CC-BY 4.0 |
| Format | Shapefile / GeoJSON / CSV |
| Record count | ~250 k addresses |
| Cadence | ≈ monthly |

Direct file URLs on data.public.lu are content-hashed and rotate per
release; the importer resolves them through the dataset API at
fetch time.

**Field mapping sketch (CSV variant):**

```rust
// Columns (typical export): id, id_caclr, rue, numero, code_postal,
//                           localite, x_lambert, y_lambert
fn from_bdadresses(row: &BdadressesRow) -> AddressRecord {
    let (lon, lat) = lambert_lu_to_wgs84(row.x_lambert, row.y_lambert);
    AddressRecord {
        id: next_id(),
        country: CountryId::LU,
        lat, lon,
        postcode: Some(row.code_postal.clone()),
        locality: Some(row.localite.clone()),
        street: Some(row.rue.clone()),
        housenumber: Some(row.numero.clone()),
        source: SourceTag::BdAdresses,
        source_id: Some(row.id_caclr.clone()),
    }
}
```

### Germany — OSM fallback (no national authoritative source)

| Property | Value |
|---|---|
| Dataset name | OSM `addr:*` tags (national merge) |
| Publisher | OpenStreetMap contributors (via Geofabrik) |
| Direct file | https://download.geofabrik.de/europe/germany-latest.osm.pbf |
| License | ODbL 1.0 |
| Format | OSM PBF |
| Record count | ~25 M addr nodes (estimate) |
| Cadence | Geofabrik refreshes daily |

Germany has no nationally unified authoritative open address dataset.
State-level open datasets exist (NRW Geobasis, Berlin FIS-Broker,
Hamburg Transparenzportal, Brandenburg, Sachsen) but with
heterogeneous licensing and schemas. The geocoder ships an OSM
fallback for Germany; layering open state datasets on top is a
follow-up.

**Field mapping sketch (OSM `addr:*` extraction):**

```rust
// Filter PBF for nodes/ways with addr:housenumber set.
fn from_osm(elem: &OsmElement) -> Option<AddressRecord> {
    let tags = &elem.tags;
    let housenumber = tags.get("addr:housenumber")?.clone();
    let (lon, lat) = elem.centroid_wgs84();
    Some(AddressRecord {
        id: next_id(),
        country: CountryId::DE,
        lat, lon,
        postcode: tags.get("addr:postcode").cloned(),
        locality: tags.get("addr:city").cloned()
            .or_else(|| tags.get("addr:town").cloned())
            .or_else(|| tags.get("addr:village").cloned()),
        street: tags.get("addr:street").cloned(),
        housenumber: Some(housenumber),
        source: SourceTag::Osm,
        source_id: Some(format!("{}:{}", elem.kind, elem.id)),
    })
}
```

The same OSM extractor is the universal fallback for every country
that has no authoritative source — it runs on every PBF, and the
importer flags OSM-sourced records as lower-priority than
authoritative records when both exist for the same country.

### Austria — BEV Adressregister

| Property | Value |
|---|---|
| Dataset name | Österreichisches Adressregister |
| Publisher | Bundesamt für Eich- und Vermessungswesen (BEV) |
| Landing page | https://www.data.gv.at/katalog/dataset/adressregister-tagesaktuell |
| Metadata | https://data.bev.gv.at/geonetwork/srv/api/records/37d564f9-5d63-4760-aae6-29d3f98ee1b4 |
| Product page | https://www.bev.gv.at/Services/Produkte/Adressregister/Oesterreichisches-Adressregister.html |
| WFS | https://apps.bev.gv.at/bev.webservice/inspire?service=WFS&request=GetCapabilities&version=2.0.0 |
| License | CC-BY 4.0 Austria |
| Format | Zipped CSV (BEV shop, free with registration) + INSPIRE GML (WFS) |
| Record count | ~2.3 M addresses |
| Cadence | Continuous, full snapshot quarterly |

**URL verification surprise:** The BEV CSV that historically lived
at `bev.gv.at/pls/portal/.../Adresse_Relationale_Tabellen-Stichtagsdaten.zip`
(documented in the OSM wiki) does not resolve reliably from outside
the BEV portal as of 2026-05. The geocode importer will go through
the WFS endpoint or the adressregister.at static export, which is
the documented modern access path.

**Field mapping sketch (BEV relational CSV — ADRESSE.csv):**

```rust
// Columns: ADRCD, GKZ, OKZ, PLZ, STRASSE, HAUSNRTEXT,
//          HOFNAME, GADRID, RW, HW, EPSG, GEMNAM, ORTNAM
fn from_bev(row: &BevRow) -> AddressRecord {
    let (lon, lat) = mgi_to_wgs84(row.rw, row.hw, row.epsg);
    AddressRecord {
        id: next_id(),
        country: CountryId::AT,
        lat, lon,
        postcode: Some(format!("{:04}", row.plz)),
        locality: Some(row.ortnam.clone()), // "Ortschaft", more precise than Gemeinde
        street: Some(row.strasse.clone()),
        housenumber: Some(row.hausnrtext.clone()),
        source: SourceTag::Bev,
        source_id: Some(row.adrcd.clone()),
    }
}
```

### Switzerland — Amtliches Verzeichnis der Gebäudeadressen (swisstopo)

| Property | Value |
|---|---|
| Dataset name | Amtliches Verzeichnis der Gebäudeadressen (AmtlicheGebäudeadressen) |
| Publisher | Bundesamt für Landestopografie (swisstopo) |
| Landing page | https://opendata.swiss/de/dataset/amtliches-verzeichnis-der-gebaudeadressen |
| Product page | https://www.swisstopo.admin.ch/de/amtliches-verzeichnis-der-gebaeudeadressen |
| STAC API | https://data.geo.admin.ch/api/stac/v0.9/collections/ch.swisstopo.amtliches-gebaeudeadressverzeichnis |
| License | OGD (Open Government Data) — free use, attribution to swisstopo |
| Format | CSV / GDB / Interlis (XTF) |
| Record count | ~2.5 M addresses |
| Cadence | Weekly |

Direct file URLs are STAC-asset-hashed and rotate per release;
the importer resolves them via the STAC API at fetch time
(`/collections/<id>/items/<id>/assets/<asset>`).

**Field mapping sketch (swisstopo CSV — `building-addresses.csv`):**

```rust
// Columns (per swisstopo Gebaeudeadressen Technical Documentation):
//   EGAID, EDID, EGID, STN_LABEL, ADR_NUMBER, ZIP_LABEL,
//   COM_NAME, COM_FOSNR, COM_CANTON, BDG_EPSG, BDG_EAST, BDG_NORTH
fn from_swisstopo(row: &SwisstopoRow) -> AddressRecord {
    let (lon, lat) = lv95_to_wgs84(row.bdg_east, row.bdg_north);
    AddressRecord {
        id: next_id(),
        country: CountryId::CH,
        lat, lon,
        postcode: Some(row.zip_label.clone()),
        locality: Some(row.com_name.clone()),
        street: Some(row.stn_label.clone()),
        housenumber: Some(row.adr_number.clone()),
        source: SourceTag::Swisstopo,
        source_id: Some(row.egaid.clone()),
    }
}
```

## Open data deferred to a follow-up

These are out of scope for this prep pass but tracked here so the
geocode shard builder can pick them up once it lands:

- **Belgium BeSt CSV** — adding `[[addresses]] id="best-vlg" url="..."`
  etc. to `dl/regions/belgium.toml` once the geocode TOML schema
  knows how to parse address-source entries. Requires schema design
  in the geocode crate, not in butterfly-dl.
- **Germany state-level datasets** — NRW, Berlin, Hamburg, Brandenburg,
  Sachsen. Per-state license review needed.
- **Italy ANNCSU**, **Spain CNIG**, **Portugal CTT/INE** — cluster #3
  (ES/PT) and Italian extension. Out of cluster #1 + cluster #2 scope.
- **OpenAddresses** — global federation of state-by-state submissions,
  useful as a coverage-fill layer on top of authoritative + OSM.

## License compatibility

All listed authoritative sources are open data with attribution
requirements at most. None are share-alike-restricted in a way that
contaminates downstream geocode output. The OSM fallback (ODbL) is
share-alike for substantial extracts but the geocoder consumes
addr:* tags as factual data, which has been broadly understood as
non-contaminating for derived databases under ODbL §4.4 (Produced
Works exception). A formal license review is filed as a follow-up
ticket — the engineering decision here is to proceed under the
common interpretation while that review runs.
