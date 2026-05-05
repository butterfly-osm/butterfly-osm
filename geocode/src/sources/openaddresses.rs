//! OpenAddresses loader (#96 §"Data Sources").
//!
//! [OpenAddresses](https://openaddresses.io) is the canonical
//! authoritative-source layer for butterfly-geocode: ~600 M addresses
//! across ~40 countries, normalised through a single schema, weekly
//! cadence. Upstream OpenAddresses ingests national/regional open
//! datasets (BOSA for Belgium, BAN for France, BAG for the
//! Netherlands, BD-Adresses for Luxembourg, several state-level
//! datasets for the US, AU, BR, JP, …) and republishes them as
//! gzipped GeoJSON-seq (one JSON Feature per line):
//!
//! ```text
//! { "type":"Feature",
//!   "properties":{
//!     "hash":"e9bfa4b3f42f842d", "number":"475",
//!     "street":"Chaussée de Mons", "unit":"RDC",
//!     "city":"Anderlecht", "district":"", "region":"",
//!     "postcode":"1070", "id":"BE-BRU:615867", "accuracy":""
//!   },
//!   "geometry":{ "type":"Point", "coordinates":[4.31653,50.83595] } }
//! ```
//!
//! Coordinates are WGS84 `[lon, lat]` (per RFC 7946); no reprojection
//! needed. The loader streams the file row-by-row so memory stays
//! bounded at a few KB regardless of dataset size — the US-northeast
//! shard alone is ~30 GB uncompressed and would not fit in RAM.
//!
//! ## Format auto-detect
//!
//! Three on-disk layouts ship in the wild:
//!
//! - `*.geojson.gz` — the canonical processed cache from
//!   `https://v2.openaddresses.io/batch-prod/job/<id>/source.geojson.gz`.
//! - `*.zip` containing one or more `.geojson` / `.csv` entries —
//!   the legacy pre-2024 packaging, plus the upstream BOSA ZIPs which
//!   OpenAddresses imports unmodified for some Belgium sources.
//! - raw `*.geojson` / `*.geojsonseq` / `*.ndjson` / `*.csv` — for
//!   operators who pre-decompressed.
//!
//! The loader sniffs the magic bytes (gzip 0x1f 0x8b, zip 0x50 0x4b)
//! and dispatches accordingly.
//!
//! ## Per-language records
//!
//! OpenAddresses publishes language-specific sources separately
//! (e.g. `be/bru/bosa-region-brussels-fr.geojson.gz` and
//! `…-nl.geojson.gz`). Each emits records in **one** language. To get
//! multi-language aliases for Brussels/Wallonia/Flanders, the operator
//! fetches both, builds two single-language shards, and merges them
//! via `--merge` — same code path BOSA used in v4.

use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use flate2::read::GzDecoder;
use serde::Deserialize;

use crate::routing::CountryId;
use crate::shard::{AddressRecord, SourceTag};

use super::{Source, SourceProgress};

/// OpenAddresses streaming loader.
///
/// Construct with [`OpenAddressesSource::new`]; the path may point at
/// a `.geojson.gz`, `.zip`, raw `.geojson`/`.geojsonseq`/`.ndjson`, or
/// `.csv` file. Any other extension triggers magic-byte sniffing.
#[derive(Debug, Clone)]
pub struct OpenAddressesSource {
    path: PathBuf,
    /// Country tag attached to each emitted record. Operators with
    /// per-state/region OpenAddresses files (US, FR, DE, …) build
    /// per-state shards then merge them into one country shard.
    country: CountryId,
}

impl OpenAddressesSource {
    pub fn new(path: impl AsRef<Path>, country: CountryId) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            country,
        }
    }

    #[must_use]
    pub fn country(&self) -> CountryId {
        self.country
    }
}

impl Source for OpenAddressesSource {
    fn tag(&self) -> SourceTag {
        SourceTag::OpenAddresses
    }

    fn stream(
        &self,
        progress: &mut dyn FnMut(SourceProgress),
        emit: &mut dyn FnMut(AddressRecord),
    ) -> Result<()> {
        let path = &self.path;

        progress(SourceProgress::Phase {
            phase: "opening OpenAddresses input",
        });

        // Sniff the magic bytes so the loader works regardless of the
        // operator's local naming convention. We only peek the first 4
        // bytes — enough to disambiguate gzip (1f 8b), zip (50 4b 03 04),
        // and a plain `{` that signals raw GeoJSON-seq.
        let mut sniff_file =
            File::open(path).with_context(|| format!("opening {}", path.display()))?;
        let mut head = [0u8; 4];
        let n_head = read_up_to(&mut sniff_file, &mut head)?;
        drop(sniff_file);

        let kind = detect_kind(&head[..n_head], path);

        match kind {
            InputKind::GzGeojsonSeq => {
                progress(SourceProgress::Phase {
                    phase: "streaming gzipped GeoJSON-seq",
                });
                let f =
                    File::open(path).with_context(|| format!("re-opening {}", path.display()))?;
                let gz = GzDecoder::new(BufReader::with_capacity(1 << 20, f));
                stream_geojson_seq(gz, progress, emit)?;
            }
            InputKind::GzCsv => {
                progress(SourceProgress::Phase {
                    phase: "streaming gzipped CSV",
                });
                let f =
                    File::open(path).with_context(|| format!("re-opening {}", path.display()))?;
                let gz = GzDecoder::new(BufReader::with_capacity(1 << 20, f));
                stream_csv(gz, progress, emit)?;
            }
            InputKind::RawGeojsonSeq => {
                progress(SourceProgress::Phase {
                    phase: "streaming raw GeoJSON-seq",
                });
                let f =
                    File::open(path).with_context(|| format!("re-opening {}", path.display()))?;
                stream_geojson_seq(f, progress, emit)?;
            }
            InputKind::Csv => {
                progress(SourceProgress::Phase {
                    phase: "streaming OpenAddresses CSV",
                });
                let f =
                    File::open(path).with_context(|| format!("re-opening {}", path.display()))?;
                stream_csv(f, progress, emit)?;
            }
            InputKind::Zip => {
                progress(SourceProgress::Phase {
                    phase: "streaming first GeoJSON/CSV entry inside ZIP",
                });
                stream_zip(path, progress, emit)?;
            }
        }

        Ok(())
    }
}

/// On-disk layout choice produced by [`detect_kind`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InputKind {
    /// gzip-magic (1f 8b) — GeoJSON-seq inside.
    GzGeojsonSeq,
    /// gzip-magic (1f 8b) AND path extension marks the payload as
    /// CSV (`.csv.gz`, `.csv.gzip`). BAN-style feeds use this shape;
    /// the historical default of "gzip → GeoJSON-seq" mis-dispatched
    /// them and parsed every CSV row as JSON.
    GzCsv,
    /// zip-magic (50 4b 03 04) — find the first `.geojson*` or
    /// `.csv` entry inside.
    Zip,
    /// raw GeoJSON-seq (each line a `Feature` JSON).
    RawGeojsonSeq,
    /// CSV — extension `.csv` and no gzip/zip magic.
    Csv,
}

/// Decide which streaming path to use based on magic bytes first,
/// falling back to the file extension. Magic bytes win for the broad
/// "compressed/uncompressed/zip" decision; the file extension then
/// disambiguates CSV-vs-GeoJSON inside a gzip stream (we cannot peek
/// past the gzip header without spinning up an actual decoder, and
/// the extension is the canonical way operators distinguish the two
/// in OpenAddresses + BAN feeds).
fn detect_kind(head: &[u8], path: &Path) -> InputKind {
    let lower_path = path
        .to_str()
        .map(|s| s.to_ascii_lowercase())
        .unwrap_or_default();
    let is_gz = head.len() >= 2 && head[0] == 0x1f && head[1] == 0x8b;
    if is_gz {
        // Disambiguate gz-CSV from gz-GeoJSON-seq by the path
        // extension. BAN ships `.csv.gz`; OpenAddresses ships
        // `.geojson.gz`. Without this distinction the CSV path was
        // dispatched to the GeoJSON-seq reader, which treats every
        // CSV row as a JSON parse error and abort.
        if lower_path.ends_with(".csv.gz") || lower_path.ends_with(".csv.gzip") {
            return InputKind::GzCsv;
        }
        return InputKind::GzGeojsonSeq;
    }
    if head.len() >= 4 && head[0] == 0x50 && head[1] == 0x4b && head[2] == 0x03 && head[3] == 0x04 {
        return InputKind::Zip;
    }
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .map(|s| s.to_ascii_lowercase());
    match ext.as_deref() {
        Some("csv") => InputKind::Csv,
        // Otherwise treat as raw GeoJSON-seq — the streaming reader
        // ignores blank lines and bails on truly malformed payloads.
        _ => InputKind::RawGeojsonSeq,
    }
}

/// Read up to `dst.len()` bytes; tolerate short files (returns the
/// number of bytes actually read instead of erroring).
fn read_up_to<R: Read>(r: &mut R, dst: &mut [u8]) -> Result<usize> {
    let mut total = 0usize;
    while total < dst.len() {
        match r.read(&mut dst[total..]) {
            Ok(0) => break,
            Ok(n) => total += n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e.into()),
        }
    }
    Ok(total)
}

/// Streaming GeoJSON-seq reader. Skips blank lines, skips lines that
/// don't carry a usable address (empty street, no coordinate). Any
/// JSON parse error on a non-empty line is fatal — partial input
/// would silently undercount. OpenAddresses-published feeds are
/// well-formed; if a snapshot drifts, operators see the parse error
/// pointing at the failing line.
fn stream_geojson_seq<R: Read>(
    reader: R,
    progress: &mut dyn FnMut(SourceProgress),
    emit: &mut dyn FnMut(AddressRecord),
) -> Result<()> {
    let mut buf = BufReader::with_capacity(1 << 20, reader);
    let mut line = String::new();
    let mut rows_seen: u64 = 0;
    let mut records_emitted: u64 = 0;
    loop {
        line.clear();
        let n = buf.read_line(&mut line).context("reading GeoJSON-seq")?;
        if n == 0 {
            break;
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        rows_seen += 1;

        let feat: OaFeature = match serde_json::from_str(trimmed) {
            Ok(f) => f,
            Err(e) => {
                bail!(
                    "OpenAddresses GeoJSON-seq parse error at row {rows_seen}: {e}. \
                     Snippet: {snippet}",
                    snippet = &trimmed.chars().take(80).collect::<String>(),
                );
            }
        };
        if let Some(rec) = feature_to_record(&feat) {
            emit(rec);
            records_emitted += 1;
        }

        if rows_seen.is_multiple_of(100_000) {
            progress(SourceProgress::Records {
                rows_seen,
                records_emitted,
            });
        }
    }
    progress(SourceProgress::Records {
        rows_seen,
        records_emitted,
    });
    Ok(())
}

/// Streaming OpenAddresses CSV reader. The OA CSV spec is
/// `hash, lon, lat, number, street, unit, city, district, region,
///  postcode, id` — but per-source CSVs may add or omit columns
/// (the OA conform rules drive renames upstream). We resolve column
/// indices from the header and pull lon/lat/number/street/city/
/// postcode/id; missing columns degrade gracefully.
fn stream_csv<R: Read>(
    reader: R,
    progress: &mut dyn FnMut(SourceProgress),
    emit: &mut dyn FnMut(AddressRecord),
) -> Result<()> {
    let mut buf = BufReader::with_capacity(1 << 20, reader);
    let mut header_line = String::new();
    buf.read_line(&mut header_line)
        .context("reading OpenAddresses CSV header")?;
    if header_line.trim().is_empty() {
        bail!("OpenAddresses CSV has empty header");
    }
    let cols = parse_oa_csv_header(&header_line);

    let mut rows_seen: u64 = 0;
    let mut records_emitted: u64 = 0;
    let mut line = String::new();
    loop {
        line.clear();
        let n = buf
            .read_line(&mut line)
            .context("reading OpenAddresses CSV row")?;
        if n == 0 {
            break;
        }
        rows_seen += 1;
        let trimmed = line.trim_end_matches(['\r', '\n']);
        if trimmed.is_empty() {
            continue;
        }
        let fields = split_csv_row(trimmed);

        let lon = cols
            .lon
            .and_then(|i| fields.get(i))
            .and_then(|s| parse_lon(s));
        let lat = cols
            .lat
            .and_then(|i| fields.get(i))
            .and_then(|s| parse_lat(s));
        let (Some(lon), Some(lat)) = (lon, lat) else {
            continue;
        };
        let street = cols
            .street
            .and_then(|i| fields.get(i))
            .map(|s| s.trim().to_string())
            .unwrap_or_default();
        if street.is_empty() {
            continue;
        }
        let number = cols
            .number
            .and_then(|i| fields.get(i))
            .map(|s| s.trim().to_string())
            .unwrap_or_default();
        let unit = cols
            .unit
            .and_then(|i| fields.get(i))
            .map(|s| s.trim().to_string())
            .unwrap_or_default();
        let housenumber = format_oa_number(&number, &unit);
        let postcode = cols
            .postcode
            .and_then(|i| fields.get(i))
            .map(|s| normalize_oa_postcode(s.trim()))
            .unwrap_or_default();
        let city = cols
            .city
            .and_then(|i| fields.get(i))
            .map(|s| s.trim().to_string())
            .unwrap_or_default();
        let id = cols
            .id
            .and_then(|i| fields.get(i))
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());

        emit(AddressRecord {
            lat,
            lon,
            street,
            locality: city,
            housenumber,
            postcode,
            source: SourceTag::OpenAddresses,
            source_id: id,
        });
        records_emitted += 1;

        if rows_seen.is_multiple_of(100_000) {
            progress(SourceProgress::Records {
                rows_seen,
                records_emitted,
            });
        }
    }
    progress(SourceProgress::Records {
        rows_seen,
        records_emitted,
    });
    Ok(())
}

/// Streaming ZIP reader. Reads the first `.geojson*` / `.csv` entry
/// inside the archive. ZIP-wrapped OpenAddresses payloads exist
/// because some upstream sources (e.g. BOSA) ship as ZIPs and a few
/// OA pipelines republish them unwrapped.
fn stream_zip(
    path: &Path,
    progress: &mut dyn FnMut(SourceProgress),
    emit: &mut dyn FnMut(AddressRecord),
) -> Result<()> {
    let f = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let mut zip =
        zip::ZipArchive::new(f).with_context(|| format!("zip archive {}", path.display()))?;
    let mut chosen: Option<(usize, &'static str)> = None;
    for i in 0..zip.len() {
        let entry = zip
            .by_index(i)
            .with_context(|| format!("reading zip entry {i} in {}", path.display()))?;
        let name = entry.name().to_ascii_lowercase();
        if name.ends_with(".geojson")
            || name.ends_with(".geojsonseq")
            || name.ends_with(".geojsonl")
            || name.ends_with(".ndjson")
        {
            chosen = Some((i, "geojson"));
            break;
        }
        if name.ends_with(".csv") {
            chosen = Some((i, "csv"));
            // keep scanning — geojson is preferred over csv if both
            // exist in the archive, since OA's normalised feature has
            // strictly more fields than the legacy CSV.
        }
    }
    let (idx, kind) = chosen.ok_or_else(|| {
        anyhow::anyhow!(
            "no .geojson*/.ndjson/.csv entry inside zip {}",
            path.display()
        )
    })?;
    // Re-borrow because `entry` above held the archive borrow.
    let entry = zip
        .by_index(idx)
        .with_context(|| format!("re-opening zip entry {idx} in {}", path.display()))?;
    match kind {
        "geojson" => stream_geojson_seq(entry, progress, emit),
        "csv" => stream_csv(entry, progress, emit),
        _ => unreachable!("kind set above"),
    }
}

/// One OpenAddresses Feature. We deserialise just the fields we use —
/// `hash`, `district`, `region`, `accuracy` are accepted via
/// `#[serde(flatten)]` and dropped, so per-source extensions don't
/// trip the deserializer.
#[derive(Debug, Deserialize)]
struct OaFeature {
    #[serde(default)]
    properties: OaProperties,
    #[serde(default)]
    geometry: Option<OaGeometry>,
}

#[derive(Debug, Default, Deserialize)]
struct OaProperties {
    #[serde(default)]
    number: Option<String>,
    #[serde(default)]
    street: Option<String>,
    #[serde(default)]
    unit: Option<String>,
    #[serde(default)]
    city: Option<String>,
    #[serde(default)]
    postcode: Option<String>,
    #[serde(default)]
    id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OaGeometry {
    #[serde(rename = "type")]
    geom_type: Option<String>,
    /// `[lon, lat]` per RFC 7946 for Point. We only accept Point;
    /// LineString/Polygon coordinates are nested arrays so we accept
    /// `serde_json::Value` and inspect the shape — that way a
    /// LineString/Polygon doesn't blow up the parser, it just gets
    /// filtered out at `feature_to_record`.
    coordinates: Option<serde_json::Value>,
}

/// Map a Feature to an `AddressRecord`. Returns `None` when the row
/// isn't usable (no street, no/invalid coords, non-Point geometry).
fn feature_to_record(feat: &OaFeature) -> Option<AddressRecord> {
    let geom = feat.geometry.as_ref()?;
    if geom.geom_type.as_deref() != Some("Point") {
        return None;
    }
    let coords_arr = geom.coordinates.as_ref()?.as_array()?;
    if coords_arr.len() < 2 {
        return None;
    }
    let lon = coords_arr[0].as_f64()?;
    let lat = coords_arr[1].as_f64()?;
    if !(-180.0..=180.0).contains(&lon) || !(-90.0..=90.0).contains(&lat) {
        return None;
    }
    let street = feat
        .properties
        .street
        .as_deref()
        .map(str::trim)
        .unwrap_or_default();
    if street.is_empty() {
        return None;
    }
    let number = feat
        .properties
        .number
        .as_deref()
        .map(str::trim)
        .unwrap_or_default();
    let unit = feat
        .properties
        .unit
        .as_deref()
        .map(str::trim)
        .unwrap_or_default();
    let housenumber = format_oa_number(number, unit);
    if housenumber.is_empty() {
        // No housenumber → not an addressable point in butterfly's
        // geocoder model (we keyspace by housenumber to disambiguate
        // multi-building streets).
        return None;
    }
    let postcode_raw = feat
        .properties
        .postcode
        .as_deref()
        .map(str::trim)
        .unwrap_or_default();
    let postcode_owned = normalize_oa_postcode(postcode_raw);
    let postcode = postcode_owned.as_str();
    let city = feat
        .properties
        .city
        .as_deref()
        .map(str::trim)
        .unwrap_or_default();
    let source_id = feat
        .properties
        .id
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    Some(AddressRecord {
        lat,
        lon,
        street: street.to_string(),
        locality: city.to_string(),
        housenumber,
        postcode: postcode.to_string(),
        source: SourceTag::OpenAddresses,
        source_id,
    })
}

/// Normalise an OpenAddresses postcode value. Strips a trailing
/// `.0` / `.00` / … suffix that appears when upstream tooling encodes
/// numeric postcodes as JSON floats (cf. US-DC OA proof: `20001.0`).
/// Without this, the postcode anchor index keys on `"20001.0"` and
/// every query carrying the human form `20001` misses.
///
/// Decisions:
///
/// - Only strip when the suffix is `.0+` AND the remainder is all
///   ASCII digits. We don't want to mangle alphanumeric postcodes
///   like Canadian `K1A 0B1` or UK `SW1A 1AA`.
/// - We don't apply the country-pack `canonicalize_postcode` rule
///   here — the loader doesn't carry the pack, and the canonicalize
///   pass is applied later (at shard-build / query time, on both
///   sides of the comparison). This function only undoes the
///   float-encoding round-trip damage so the canonicalize pass sees
///   the operator-intended string.
fn normalize_oa_postcode(raw: &str) -> String {
    let trimmed = raw.trim();
    if let Some(idx) = trimmed.find('.') {
        let (left, right) = trimmed.split_at(idx);
        // `right` includes the leading dot; check the remainder.
        let after_dot = &right[1..];
        if !left.is_empty()
            && left.bytes().all(|b| b.is_ascii_digit())
            && !after_dot.is_empty()
            && after_dot.bytes().all(|b| b == b'0')
        {
            return left.to_string();
        }
    }
    trimmed.to_string()
}

/// Format an OpenAddresses housenumber: `number[ unit]` with a space
/// separator (e.g. `"475 RDC"`). We deliberately don't normalise to
/// any country-specific convention (`bte`, `bis`, `apt`) — the
/// upstream `unit` field is published in the source's local
/// convention, and the geocoder's normalisation layer folds the
/// variants at query time.
fn format_oa_number(number: &str, unit: &str) -> String {
    let n = number.trim();
    let u = unit.trim();
    if n.is_empty() && u.is_empty() {
        return String::new();
    }
    if u.is_empty() {
        return n.to_string();
    }
    if n.is_empty() {
        return u.to_string();
    }
    format!("{n} {u}")
}

/// CSV column resolver for OpenAddresses CSVs.
#[derive(Debug, Default)]
struct OaCsvCols {
    lon: Option<usize>,
    lat: Option<usize>,
    number: Option<usize>,
    street: Option<usize>,
    unit: Option<usize>,
    city: Option<usize>,
    postcode: Option<usize>,
    id: Option<usize>,
}

fn parse_oa_csv_header(header_line: &str) -> OaCsvCols {
    let fields: Vec<&str> = split_csv_row(header_line.trim_end_matches(['\r', '\n']))
        .into_iter()
        .collect();
    let lookup = |name: &str| {
        fields
            .iter()
            .position(|f| f.trim().eq_ignore_ascii_case(name))
    };
    OaCsvCols {
        lon: lookup("lon")
            .or_else(|| lookup("longitude"))
            .or_else(|| lookup("x")),
        lat: lookup("lat")
            .or_else(|| lookup("latitude"))
            .or_else(|| lookup("y")),
        number: lookup("number").or_else(|| lookup("housenumber")),
        street: lookup("street"),
        unit: lookup("unit"),
        city: lookup("city"),
        postcode: lookup("postcode").or_else(|| lookup("zip")),
        id: lookup("id"),
    }
}

/// Lightweight CSV-row split with quoted-field support. Mirrors the
/// BOSA loader's previous implementation — OpenAddresses CSVs are
/// generally well-formed with commas-only separators.
fn split_csv_row(line: &str) -> Vec<&str> {
    let trimmed = line.trim_end_matches(['\r', '\n']);
    let mut out = Vec::new();
    let bytes = trimmed.as_bytes();
    let mut start = 0usize;
    let mut in_quote = false;
    let mut i = 0usize;
    while i < bytes.len() {
        let b = bytes[i];
        match b {
            b'"' => in_quote = !in_quote,
            b',' if !in_quote => {
                out.push(slice_unquoted(&trimmed[start..i]));
                start = i + 1;
            }
            _ => {}
        }
        i += 1;
    }
    out.push(slice_unquoted(&trimmed[start..]));
    out
}

fn slice_unquoted(s: &str) -> &str {
    let s = s.trim_matches([' ', '\t']);
    if s.len() >= 2 && s.starts_with('"') && s.ends_with('"') {
        &s[1..s.len() - 1]
    } else {
        s
    }
}

fn parse_lat(s: &str) -> Option<f64> {
    let v: f64 = s.trim().parse().ok()?;
    if (-90.0..=90.0).contains(&v) {
        Some(v)
    } else {
        None
    }
}

fn parse_lon(s: &str) -> Option<f64> {
    let v: f64 = s.trim().parse().ok()?;
    if (-180.0..=180.0).contains(&v) {
        Some(v)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// Real-shape OpenAddresses GeoJSON-seq lifted from the live
    /// `be/bru/bosa-region-brussels-fr` job (verified 2026-05-05).
    /// 4 features: 3 valid, 1 with no street to exercise the skip path.
    const SAMPLE_GEOJSON: &str = concat!(
        r#"{"type":"Feature","properties":{"hash":"e9bfa4b3f42f842d","number":"475","street":"Chaussée de Mons","unit":"RDC","city":"Anderlecht","district":"","region":"","postcode":"1070","id":"BE-BRU:615867","accuracy":""},"geometry":{"type":"Point","coordinates":[4.31653,50.83595]}}"#,
        "\n",
        r#"{"type":"Feature","properties":{"hash":"85cad0bdc3514625","number":"603","street":"Chaussée de Mons","unit":"2eET","city":"Anderlecht","district":"","region":"","postcode":"1070","id":"BE-BRU:615868","accuracy":""},"geometry":{"type":"Point","coordinates":[4.31283,50.83327]}}"#,
        "\n",
        r#"{"type":"Feature","properties":{"hash":"d","number":"1","street":"Grand-Place","unit":"","city":"Bruxelles","district":"","region":"","postcode":"1000","id":"BE-BRU:99999","accuracy":""},"geometry":{"type":"Point","coordinates":[4.35251,50.84671]}}"#,
        "\n",
        r#"{"type":"Feature","properties":{"hash":"x","number":"","street":"","city":"","postcode":""},"geometry":{"type":"Point","coordinates":[4.0,50.0]}}"#,
        "\n",
    );

    fn write_sample(dir: &std::path::Path, contents: &str, name: &str) -> std::path::PathBuf {
        let p = dir.join(name);
        std::fs::write(&p, contents).unwrap();
        p
    }

    #[test]
    fn parses_raw_geojson_seq() {
        let dir = tempfile::tempdir().unwrap();
        let p = write_sample(dir.path(), SAMPLE_GEOJSON, "sample.geojson");
        let src = OpenAddressesSource::new(&p, CountryId::BE);
        let recs = super::super::collect_all(&src, |_| {}).unwrap();
        // 3 of 4 features are valid (the 4th has empty street).
        assert_eq!(recs.len(), 3, "expected 3 records, got {:#?}", recs);
        for r in &recs {
            assert_eq!(r.source, SourceTag::OpenAddresses);
            assert!(r.source_id.is_some(), "id should propagate");
            assert!(r.lat > 50.0 && r.lat < 51.0, "lat {}", r.lat);
            assert!(r.lon > 4.0 && r.lon < 5.0, "lon {}", r.lon);
        }
        let r0 = &recs[0];
        assert_eq!(r0.street, "Chaussée de Mons");
        assert_eq!(r0.housenumber, "475 RDC");
        assert_eq!(r0.postcode, "1070");
        assert_eq!(r0.locality, "Anderlecht");
        assert_eq!(r0.source_id.as_deref(), Some("BE-BRU:615867"));
    }

    #[test]
    fn parses_gzipped_geojson_seq() {
        use flate2::Compression;
        use flate2::write::GzEncoder;
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("sample.geojson.gz");
        let f = std::fs::File::create(&p).unwrap();
        let mut enc = GzEncoder::new(f, Compression::fast());
        enc.write_all(SAMPLE_GEOJSON.as_bytes()).unwrap();
        enc.finish().unwrap();
        let src = OpenAddressesSource::new(&p, CountryId::BE);
        let recs = super::super::collect_all(&src, |_| {}).unwrap();
        assert_eq!(recs.len(), 3);
    }

    #[test]
    fn parses_zip_with_inner_geojson() {
        use std::io::Write as _;
        use zip::write::SimpleFileOptions;
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("sample.zip");
        let f = std::fs::File::create(&p).unwrap();
        let mut zw = zip::ZipWriter::new(f);
        zw.start_file(
            "sample.geojson",
            SimpleFileOptions::default().compression_method(zip::CompressionMethod::Deflated),
        )
        .unwrap();
        zw.write_all(SAMPLE_GEOJSON.as_bytes()).unwrap();
        zw.finish().unwrap();
        let src = OpenAddressesSource::new(&p, CountryId::BE);
        let recs = super::super::collect_all(&src, |_| {}).unwrap();
        assert_eq!(recs.len(), 3);
    }

    #[test]
    fn parses_csv_variant() {
        let csv = "lon,lat,number,street,unit,city,district,region,postcode,id\n\
                   4.31653,50.83595,475,Chaussée de Mons,RDC,Anderlecht,,,1070,BE-BRU:615867\n\
                   4.31283,50.83327,603,Chaussée de Mons,2eET,Anderlecht,,,1070,BE-BRU:615868\n\
                   ,,,,,,,,,bad\n\
                   4.35251,50.84671,1,Grand-Place,,Bruxelles,,,1000,BE-BRU:99999\n";
        let dir = tempfile::tempdir().unwrap();
        let p = write_sample(dir.path(), csv, "sample.csv");
        let src = OpenAddressesSource::new(&p, CountryId::BE);
        let recs = super::super::collect_all(&src, |_| {}).unwrap();
        assert_eq!(recs.len(), 3, "expected 3 valid CSV rows, got {:#?}", recs);
        assert_eq!(recs[0].housenumber, "475 RDC");
        assert_eq!(recs[2].housenumber, "1");
    }

    #[test]
    fn invalid_lat_lon_skipped() {
        let bad = r#"{"type":"Feature","properties":{"number":"1","street":"X","postcode":"1000"},"geometry":{"type":"Point","coordinates":[999.0,50.0]}}"#;
        let dir = tempfile::tempdir().unwrap();
        let p = write_sample(dir.path(), bad, "bad.geojson");
        let src = OpenAddressesSource::new(&p, CountryId::BE);
        let recs = super::super::collect_all(&src, |_| {}).unwrap();
        assert_eq!(recs.len(), 0);
    }

    #[test]
    fn non_point_geometry_skipped() {
        let bad = r#"{"type":"Feature","properties":{"number":"1","street":"X","postcode":"1000"},"geometry":{"type":"LineString","coordinates":[[4.0,50.0],[4.1,50.1]]}}"#;
        let dir = tempfile::tempdir().unwrap();
        let p = write_sample(dir.path(), bad, "bad.geojson");
        let src = OpenAddressesSource::new(&p, CountryId::BE);
        let recs = super::super::collect_all(&src, |_| {}).unwrap();
        assert_eq!(recs.len(), 0);
    }

    #[test]
    fn missing_housenumber_skipped() {
        let no_num = r#"{"type":"Feature","properties":{"street":"Just A Street","city":"X","postcode":"1000"},"geometry":{"type":"Point","coordinates":[4.0,50.0]}}"#;
        let dir = tempfile::tempdir().unwrap();
        let p = write_sample(dir.path(), no_num, "no_num.geojson");
        let src = OpenAddressesSource::new(&p, CountryId::BE);
        let recs = super::super::collect_all(&src, |_| {}).unwrap();
        assert_eq!(recs.len(), 0);
    }

    #[test]
    fn format_oa_number_combines_fields() {
        assert_eq!(format_oa_number("475", "RDC"), "475 RDC");
        assert_eq!(format_oa_number("475", ""), "475");
        assert_eq!(format_oa_number("", "RDC"), "RDC");
        assert_eq!(format_oa_number("", ""), "");
    }

    #[test]
    fn detect_kind_dispatches_on_magic() {
        // gzip magic
        let head = [0x1f, 0x8b, 0x00, 0x00];
        assert!(matches!(
            detect_kind(&head, std::path::Path::new("x.bin")),
            InputKind::GzGeojsonSeq
        ));
        // zip magic
        let head = [0x50, 0x4b, 0x03, 0x04];
        assert!(matches!(
            detect_kind(&head, std::path::Path::new("x.bin")),
            InputKind::Zip
        ));
        // csv extension
        let head = [b'a', b',', b'b', b'\n'];
        assert!(matches!(
            detect_kind(&head, std::path::Path::new("x.csv")),
            InputKind::Csv
        ));
        // unknown ext + non-magic → raw geojson-seq
        let head = [b'{', b'}', 0, 0];
        assert!(matches!(
            detect_kind(&head, std::path::Path::new("x.json")),
            InputKind::RawGeojsonSeq
        ));
    }

    #[test]
    fn detect_kind_dispatches_csv_gz_separately_from_geojson_gz() {
        // gzip magic + .csv.gz extension → GzCsv (not GzGeojsonSeq).
        // Regression for Copilot review on PR #184: BAN-style feeds
        // were being routed to the GeoJSON-seq parser.
        let head = [0x1f, 0x8b, 0x00, 0x00];
        assert_eq!(
            detect_kind(&head, std::path::Path::new("ban-fr-75.csv.gz")),
            InputKind::GzCsv
        );
        assert_eq!(
            detect_kind(&head, std::path::Path::new("foo.csv.gzip")),
            InputKind::GzCsv
        );
        // .geojson.gz path stays on the GeoJSON-seq route.
        assert_eq!(
            detect_kind(&head, std::path::Path::new("oa-be-bru-fr.geojson.gz")),
            InputKind::GzGeojsonSeq
        );
    }

    #[test]
    fn normalize_postcode_strips_trailing_zero_decimal() {
        // US-DC OA proof: postcodes encoded as JSON floats round-trip
        // to "20001.0" / "20001.00" / etc. Strip the suffix so the
        // shard postcode index keys on the human form "20001".
        assert_eq!(normalize_oa_postcode("20001.0"), "20001");
        assert_eq!(normalize_oa_postcode("20001.00"), "20001");
        assert_eq!(normalize_oa_postcode("20001.000"), "20001");
        // Don't touch values that aren't numeric+.0 — Canadian
        // alphanumeric, UK with internal whitespace, fully-numeric.
        assert_eq!(normalize_oa_postcode("20001"), "20001");
        assert_eq!(normalize_oa_postcode("K1A 0B1"), "K1A 0B1");
        assert_eq!(normalize_oa_postcode("SW1A 1AA"), "SW1A 1AA");
        assert_eq!(normalize_oa_postcode("1234.AB"), "1234.AB");
        assert_eq!(normalize_oa_postcode(""), "");
    }

    #[test]
    fn tag_is_openaddresses() {
        let s = OpenAddressesSource::new("/nonexistent.geojson", CountryId::FR);
        assert_eq!(s.tag(), SourceTag::OpenAddresses);
        assert_eq!(s.country(), CountryId::FR);
    }
}
