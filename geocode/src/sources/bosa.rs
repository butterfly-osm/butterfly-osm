//! BOSA BeSt Address loader (Belgium, #96 §"Data Sources").
//!
//! BeSt Address is the Belgian Federal Public Service BOSA's
//! authoritative open-data address dataset (~6.7M records). It is
//! published as three regional CSV downloads (Flanders / Wallonia /
//! Brussels) plus a single national XML; this loader handles the CSV
//! variant. Both the raw CSV and the ZIP-wrapped CSV branch stream
//! rows row-by-row via `BufReader::read_line` — peak in-memory state
//! is one row plus the streaming buffers (~1 MB), so memory stays
//! bounded regardless of dataset size.
//!
//! The CSV header is:
//!
//! ```text
//! EPSG:31370_x,EPSG:31370_y,EPSG:4326_lat,EPSG:4326_lon,address_id,
//! box_number,house_number,municipality_id,municipality_name_de,
//! municipality_name_fr,municipality_name_nl,postcode,
//! postname_fr,postname_nl,street_id,streetname_de,streetname_fr,
//! streetname_nl,region_code,status
//! ```
//!
//! Coordinates are already published in WGS84 (`EPSG:4326_lat/lon`) so
//! no reprojection is needed — Lambert-72 (`EPSG:31370`) columns are
//! ignored. Records with `status != current` are dropped (BOSA marks
//! retired addresses with `retired`, which would pollute the shard).
//!
//! Per `geocode-data/SOURCES.md` Belgium needs every language as a
//! queryable alias (NL/FR/DE). The loader emits **one record per
//! language per address** so all three localities and street names
//! land in the shard's per-language inverted index. They all share
//! the same `(lat, lon, postcode, housenumber)` so the geocoder
//! answers any-language queries against the right physical place.
//!
//! ## URL
//!
//! Direct CSV downloads (verified 2026-05-04, see
//! `geocode-data/SOURCES.md`):
//!
//! - Flanders: `https://opendata.bosa.be/download/best/openaddress-bevlg.zip`
//! - Wallonia: `https://opendata.bosa.be/download/best/openaddress-bewal.zip`
//! - Brussels: `https://opendata.bosa.be/download/best/openaddress-bebru.zip`
//!
//! Each ZIP holds one CSV at the top level. The shard pipeline expects
//! the CSV to be unzipped before this loader runs — the CLI's
//! `--csv` flag points directly at the CSV file (or a `.zip`
//! containing exactly one CSV).

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};

use crate::routing::CountryId;
use crate::shard::{AddressRecord, SourceTag};

use super::{Source, SourceProgress};

/// BOSA CSV streaming loader.
#[derive(Debug, Clone)]
pub struct BosaCsvSource {
    path: PathBuf,
    /// Country tag attached to each emitted record. BOSA is BE-only
    /// in 2026 — the field is here for symmetry with future
    /// federated-source loaders that aren't single-country.
    country: CountryId,
}

impl BosaCsvSource {
    pub fn new(path: impl AsRef<Path>, country: CountryId) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            country,
        }
    }
}

impl Source for BosaCsvSource {
    fn tag(&self) -> SourceTag {
        SourceTag::Bosa
    }

    fn stream(
        &self,
        progress: &mut dyn FnMut(SourceProgress),
        emit: &mut dyn FnMut(AddressRecord),
    ) -> Result<()> {
        if self.country != CountryId::BE {
            bail!(
                "BOSA loader is BE-only; got {} — wire a per-country dispatch upstream",
                self.country.iso2()
            );
        }

        let path = &self.path;
        let is_zip = path
            .extension()
            .and_then(|e| e.to_str())
            .is_some_and(|e| e.eq_ignore_ascii_case("zip"));

        if is_zip {
            // ZIP branch: stream the first .csv entry directly off
            // the deflate decompressor. `ZipFile<'a>: Read` borrows
            // the archive, which keeps the entire 60-130 MB CSV from
            // having to be slurped into a Vec first.
            let f = File::open(path).with_context(|| format!("opening {}", path.display()))?;
            let mut zip = zip::ZipArchive::new(f)
                .with_context(|| format!("zip archive {}", path.display()))?;
            let mut csv_idx = None;
            for i in 0..zip.len() {
                let entry = zip
                    .by_index(i)
                    .with_context(|| format!("reading zip entry {i} in {}", path.display()))?;
                let name = entry.name().to_string();
                if name.to_ascii_lowercase().ends_with(".csv") {
                    csv_idx = Some((i, name));
                    break;
                }
            }
            let (idx, name) = csv_idx.ok_or_else(|| {
                anyhow::anyhow!("no .csv entry found inside zip {}", path.display())
            })?;
            let entry = zip
                .by_index(idx)
                .with_context(|| format!("re-opening zip entry {name} in {}", path.display()))?;
            let buf_reader = BufReader::with_capacity(1 << 20, entry);
            stream_csv(buf_reader, path, progress, emit)
        } else {
            let f = File::open(path).with_context(|| format!("opening {}", path.display()))?;
            let buf_reader = BufReader::with_capacity(1 << 20, f);
            stream_csv(buf_reader, path, progress, emit)
        }
    }
}

/// Common CSV streaming logic. Generic over the underlying `Read`
/// implementation so the ZIP path can pass a `ZipFile<'a>` directly
/// without buffering its entire contents.
fn stream_csv<R: BufRead>(
    mut buf_reader: R,
    path: &Path,
    progress: &mut dyn FnMut(SourceProgress),
    emit: &mut dyn FnMut(AddressRecord),
) -> Result<()> {
    progress(SourceProgress::Phase {
        phase: "parsing BOSA CSV header",
    });

    // Read the header line and resolve column indices.
    let mut header_line = String::new();
    buf_reader
        .read_line(&mut header_line)
        .context("reading BOSA CSV header")?;
    if header_line.trim().is_empty() {
        bail!("BOSA CSV at {} has empty header", path.display());
    }
    let cols = parse_header(&header_line)?;

    progress(SourceProgress::Phase {
        phase: "streaming BOSA CSV rows",
    });

    let mut rows_seen: u64 = 0;
    let mut records_emitted: u64 = 0;
    let mut line = String::new();

    loop {
        line.clear();
        let n = buf_reader
            .read_line(&mut line)
            .context("reading BOSA CSV row")?;
        if n == 0 {
            break;
        }
        rows_seen += 1;

        let fields = split_csv_row(&line);
        if fields.len() < cols.column_count {
            // Truncated/malformed line: skip but don't fail the
            // whole pass. BOSA is generally well-formed but we
            // don't want one bad row to abort a 6.7M-row import.
            continue;
        }

        let lat = parse_lat(fields[cols.lat]);
        let lon = parse_lon(fields[cols.lon]);
        let (Some(lat), Some(lon)) = (lat, lon) else {
            continue;
        };

        let status = fields[cols.status].trim();
        // Accept "current" or empty (occasionally missing in
        // older publications). Skip "retired" and any other
        // non-current state.
        if !status.is_empty() && !status.eq_ignore_ascii_case("current") {
            continue;
        }

        let postcode = fields[cols.postcode].trim();
        if postcode.is_empty() {
            continue;
        }

        let house_number = fields[cols.house_number].trim();
        let box_number = cols
            .box_number
            .and_then(|i| fields.get(i))
            .map(|s| s.trim())
            .unwrap_or("");
        let house = format_belgian_number(house_number, box_number);
        if house.is_empty() {
            continue;
        }

        let address_id = fields[cols.address_id].trim();

        // Per-language: BOSA Belgium publishes NL/FR/DE names.
        // We emit one record per non-empty language so all three
        // names are queryable. Same `source_id` so the merge dedup
        // can collapse them when needed.
        for lang in [LangCol::Nl, LangCol::Fr, LangCol::De] {
            let muni = fields[cols.muni(lang)].trim();
            let street = fields[cols.street(lang)].trim();
            if muni.is_empty() && street.is_empty() {
                continue;
            }
            emit(AddressRecord {
                lat,
                lon,
                street: street.to_string(),
                locality: muni.to_string(),
                housenumber: house.clone(),
                postcode: postcode.to_string(),
                source: SourceTag::Bosa,
                source_id: if address_id.is_empty() {
                    None
                } else {
                    Some(address_id.to_string())
                },
            });
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

/// Format a Belgian housenumber: `house[box]` with `bte` separator
/// when both are present. BOSA publishes box numbers separately
/// (apartment / box like `RDC` for ground-floor or `2eET` for second
/// floor); we fold them into a single field so the geocoder doesn't
/// need a separate "unit" channel for the BE MVP.
fn format_belgian_number(house: &str, box_number: &str) -> String {
    let h = house.trim();
    let b = box_number.trim();
    if h.is_empty() {
        return String::new();
    }
    if b.is_empty() {
        return h.to_string();
    }
    format!("{h} bte {b}")
}

#[derive(Debug, Clone, Copy)]
enum LangCol {
    Nl,
    Fr,
    De,
}

#[derive(Debug, Clone, Copy)]
struct ColumnIndices {
    lat: usize,
    lon: usize,
    address_id: usize,
    house_number: usize,
    /// `None` when the BOSA snapshot doesn't carry a `box_number`
    /// column. We optionally fold this into the housenumber when
    /// present (Brussels/Wallonia/Flanders all do); older / regional
    /// snapshots that omit it land with bare housenumbers and no
    /// runtime panic.
    box_number: Option<usize>,
    postcode: usize,
    muni_nl: usize,
    muni_fr: usize,
    muni_de: usize,
    street_nl: usize,
    street_fr: usize,
    street_de: usize,
    status: usize,
    /// Total column count; used to reject truncated rows.
    column_count: usize,
}

impl ColumnIndices {
    fn muni(&self, lang: LangCol) -> usize {
        match lang {
            LangCol::Nl => self.muni_nl,
            LangCol::Fr => self.muni_fr,
            LangCol::De => self.muni_de,
        }
    }
    fn street(&self, lang: LangCol) -> usize {
        match lang {
            LangCol::Nl => self.street_nl,
            LangCol::Fr => self.street_fr,
            LangCol::De => self.street_de,
        }
    }
}

fn parse_header(line: &str) -> Result<ColumnIndices> {
    let fields: Vec<&str> = line.trim().split(',').map(str::trim).collect();
    let lookup = |name: &str| fields.iter().position(|f| f.eq_ignore_ascii_case(name));
    let lat = lookup("EPSG:4326_lat").context("BOSA CSV missing EPSG:4326_lat column")?;
    let lon = lookup("EPSG:4326_lon").context("BOSA CSV missing EPSG:4326_lon column")?;
    let address_id = lookup("address_id").context("BOSA CSV missing address_id column")?;
    let house_number = lookup("house_number").context("BOSA CSV missing house_number column")?;
    // box_number is optional in some BOSA snapshots. Track presence
    // explicitly so we can skip the box-number folding rather than
    // index past the row's column count and panic.
    let box_number = lookup("box_number");
    let postcode = lookup("postcode").context("BOSA CSV missing postcode column")?;
    let muni_nl =
        lookup("municipality_name_nl").context("BOSA CSV missing municipality_name_nl column")?;
    let muni_fr =
        lookup("municipality_name_fr").context("BOSA CSV missing municipality_name_fr column")?;
    let muni_de =
        lookup("municipality_name_de").context("BOSA CSV missing municipality_name_de column")?;
    let street_nl = lookup("streetname_nl").context("BOSA CSV missing streetname_nl column")?;
    let street_fr = lookup("streetname_fr").context("BOSA CSV missing streetname_fr column")?;
    let street_de = lookup("streetname_de").context("BOSA CSV missing streetname_de column")?;
    let status = lookup("status").context("BOSA CSV missing status column")?;
    Ok(ColumnIndices {
        lat,
        lon,
        address_id,
        house_number,
        box_number,
        postcode,
        muni_nl,
        muni_fr,
        muni_de,
        street_nl,
        street_fr,
        street_de,
        status,
        column_count: fields.len(),
    })
}

/// Lightweight CSV-row split. BOSA does not quote any field with
/// commas in the data we've inspected (verified against Brussels +
/// Flanders snapshots, 2026-05-04), so a plain comma split is
/// correct. We still handle double-quoted fields defensively in case a
/// future snapshot starts emitting them.
///
/// Returns trimmed-of-newline string slices in original order.
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

/// Strip a single leading + trailing `"` if present. We don't unescape
/// `""` → `"` because BOSA doesn't emit it; if it ever does we'll
/// hear about it via the status filter dropping rows it can't read.
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

    /// 4-row sample lifted from the real Brussels download
    /// (`openaddress-bebru.zip`, downloaded 2026-05-04). Header +
    /// real-shaped data so the parser exercises BOSA's actual format.
    const SAMPLE_CSV: &str = "EPSG:31370_x,EPSG:31370_y,EPSG:4326_lat,EPSG:4326_lon,address_id,box_number,house_number,municipality_id,municipality_name_de,municipality_name_fr,municipality_name_nl,postcode,postname_fr,postname_nl,street_id,streetname_de,streetname_fr,streetname_nl,region_code,status\n\
146321.07800,169504.60900,50.83595,4.31653,615867,RDC,475,21001,,Anderlecht,Anderlecht,1070,,,4568,,Chaussée de Mons,Bergense Steenweg,BE-BRU,current\n\
146059.99800,169206.03000,50.83327,4.31283,615868,2eET,603,21001,,Anderlecht,Anderlecht,1070,,,4568,,Chaussée de Mons,Bergense Steenweg,BE-BRU,current\n\
000000,000000,50.84671,4.35251,99999,,1,21004,,Bruxelles,Brussel,1000,,,1,,Grand-Place,Grote Markt,BE-BRU,current\n\
000000,000000,50.84671,4.35252,77777,,2,21004,,Bruxelles,Brussel,1000,,,1,,Grand-Place,Grote Markt,BE-BRU,retired\n";

    fn write_sample(dir: &std::path::Path) -> std::path::PathBuf {
        let p = dir.join("sample.csv");
        std::fs::write(&p, SAMPLE_CSV).unwrap();
        p
    }

    #[test]
    fn parses_real_bosa_sample() {
        let dir = tempfile::tempdir().unwrap();
        let p = write_sample(dir.path());
        let src = BosaCsvSource::new(&p, CountryId::BE);
        let recs = super::super::collect_all(&src, |_| {}).unwrap();
        // 3 active rows × 2 non-empty languages each (NL+FR; DE is
        // empty in Brussels snapshots) = 6 records. The retired row
        // is dropped.
        assert_eq!(
            recs.len(),
            6,
            "expected 6 active-NL+FR records, got {}: {:#?}",
            recs.len(),
            recs
        );
        for r in &recs {
            assert_eq!(r.source, SourceTag::Bosa);
            assert!(r.source_id.is_some(), "address_id should propagate");
        }
        // Two records share address_id 615867 (NL + FR variants).
        let by_id: Vec<_> = recs
            .iter()
            .filter(|r| r.source_id.as_deref() == Some("615867"))
            .collect();
        assert_eq!(by_id.len(), 2);
        let langs: Vec<&str> = by_id.iter().map(|r| r.locality.as_str()).collect();
        assert!(langs.contains(&"Anderlecht"));
        // Box folds into housenumber.
        assert!(by_id.iter().all(|r| r.housenumber.contains("475 bte RDC")));
    }

    #[test]
    fn drops_retired_status() {
        let dir = tempfile::tempdir().unwrap();
        let p = write_sample(dir.path());
        let src = BosaCsvSource::new(&p, CountryId::BE);
        let recs = super::super::collect_all(&src, |_| {}).unwrap();
        // Address_id 77777 is the only retired one; should NOT appear.
        assert!(
            recs.iter().all(|r| r.source_id.as_deref() != Some("77777")),
            "retired address leaked through: {:#?}",
            recs.iter()
                .filter(|r| r.source_id.as_deref() == Some("77777"))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn rejects_non_be_country() {
        let dir = tempfile::tempdir().unwrap();
        let p = write_sample(dir.path());
        let src = BosaCsvSource::new(&p, CountryId::FR);
        let err = super::super::collect_all(&src, |_| {}).unwrap_err();
        assert!(format!("{err:#}").contains("BOSA loader is BE-only"));
    }

    #[test]
    fn missing_lat_lon_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("bad.csv");
        let bad = "EPSG:31370_x,EPSG:31370_y,EPSG:4326_lat,EPSG:4326_lon,address_id,box_number,house_number,municipality_id,municipality_name_de,municipality_name_fr,municipality_name_nl,postcode,postname_fr,postname_nl,street_id,streetname_de,streetname_fr,streetname_nl,region_code,status\n\
146321,169504,not_a_number,4.31653,615867,,475,21001,,A,A,1070,,,4568,,X,Y,BE-BRU,current\n";
        std::fs::write(&p, bad).unwrap();
        let src = BosaCsvSource::new(&p, CountryId::BE);
        let recs = super::super::collect_all(&src, |_| {}).unwrap();
        assert_eq!(recs.len(), 0, "rows with non-numeric lat must be dropped");
    }

    #[test]
    fn header_validation_rejects_missing_columns() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("bad_header.csv");
        std::fs::write(&p, "lat,lon,foo\n50,4,bar\n").unwrap();
        let src = BosaCsvSource::new(&p, CountryId::BE);
        let err = super::super::collect_all(&src, |_| {}).unwrap_err();
        assert!(format!("{err:#}").contains("BOSA CSV missing"));
    }

    #[test]
    fn format_belgian_number_with_box() {
        assert_eq!(format_belgian_number("475", "RDC"), "475 bte RDC");
        assert_eq!(format_belgian_number("475", ""), "475");
        assert_eq!(format_belgian_number(" ", "RDC"), "");
    }

    #[test]
    fn header_without_box_number_skips_box_folding() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("nobox.csv");
        // Header without `box_number`. Note: column count is one less.
        let csv = "EPSG:31370_x,EPSG:31370_y,EPSG:4326_lat,EPSG:4326_lon,address_id,house_number,municipality_id,municipality_name_de,municipality_name_fr,municipality_name_nl,postcode,postname_fr,postname_nl,street_id,streetname_de,streetname_fr,streetname_nl,region_code,status\n\
146321,169504,50.83595,4.31653,615867,475,21001,,Anderlecht,Anderlecht,1070,,,4568,,Chaussée de Mons,Bergense Steenweg,BE-BRU,current\n";
        std::fs::write(&p, csv).unwrap();
        let src = BosaCsvSource::new(&p, CountryId::BE);
        let recs = super::super::collect_all(&src, |_| {}).unwrap();
        assert!(!recs.is_empty(), "missing box_number column must not panic");
        for r in &recs {
            assert_eq!(r.housenumber, "475", "no box folding when column is absent");
        }
    }

    #[test]
    fn split_csv_row_handles_basic_commas() {
        let r = split_csv_row("a,b,c\n");
        assert_eq!(r, vec!["a", "b", "c"]);
    }

    #[test]
    fn split_csv_row_respects_quoted_commas() {
        let r = split_csv_row("a,\"b,c\",d\n");
        assert_eq!(r, vec!["a", "b,c", "d"]);
    }
}
