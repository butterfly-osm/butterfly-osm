//! Build a recall FST + postings payload + stats sidecar from a
//! [`Shard`] (#205).
//!
//! Iterates every record in the shard, generates the canonical
//! key(s), groups by key, sorts lex-ascending, then streams into
//! [`fst::MapBuilder`]. Postings are appended into a flat `u32`
//! payload in the same key order so the FST value can encode
//! `(offset_words << 24) | count`.
//!
//! KISS: 24-bit count + 40-bit offset is enough headroom for any
//! single-country shard we ship (max 4M records, ~8M postings worst
//! case if every record has 2 keys).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};

use super::stats::ShardRecallStats;
use super::{FST_EXT, POSTING_OA_FLAG, POSTINGS_EXT, STATS_EXT};
use crate::parser::normalize::normalize;
use crate::shard::SourceTag;
use crate::shard::reader::Shard;

/// Options controlling the build.
#[derive(Debug, Clone)]
pub struct BuildOptions {
    /// Emit a `place` key (locality alone) per record? Defaults to
    /// true. Set to false for shards with very generic locality names
    /// to avoid bloating the FST.
    pub emit_place_keys: bool,
    /// Skip records whose canonical key would be empty (no street,
    /// no postcode, no locality). Defaults to true.
    pub skip_empty_keys: bool,
}

impl Default for BuildOptions {
    fn default() -> Self {
        Self {
            emit_place_keys: true,
            skip_empty_keys: true,
        }
    }
}

/// Statistics returned after a successful build.
#[derive(Debug, Clone)]
pub struct BuildReport {
    pub fst_path: PathBuf,
    pub postings_path: PathBuf,
    pub stats_path: PathBuf,
    pub stats: ShardRecallStats,
}

/// Build a recall index for `shard`, writing sidecar files next to
/// `shard_path`.
///
/// Output paths:
/// - `<shard_path>.<FST_EXT>` — FST map keys → packed posting refs
/// - `<shard_path>.<POSTINGS_EXT>` — flat u32 postings
/// - `<shard_path>.<STATS_EXT>` — JSON stats
pub fn build_recall_index(
    shard_path: &Path,
    shard: &Shard,
    opts: &BuildOptions,
) -> Result<BuildReport> {
    // 1. Walk records, group postings by canonical key.
    //
    // BTreeMap keeps keys lex-sorted as-we-go which is what
    // `fst::MapBuilder` requires. This costs O(n log n) but the
    // constant factor is dwarfed by the BFGS scan itself.
    let mut buckets: BTreeMap<String, Vec<u32>> = BTreeMap::new();

    for id in 0..shard.record_count() as u32 {
        let Some(rec) = shard.record(id) else {
            continue;
        };

        let key_addr =
            canonical_address_key(&rec.street, &rec.housenumber, &rec.postcode, &rec.locality);
        let posting = encode_posting(id, rec.source);

        if !key_addr.is_empty() || !opts.skip_empty_keys {
            buckets.entry(key_addr.clone()).or_default().push(posting);
        }

        if opts.emit_place_keys {
            let key_place = normalize(&rec.locality);
            if !key_place.is_empty() && key_place != key_addr {
                buckets.entry(key_place).or_default().push(posting);
            }
        }
    }

    if buckets.is_empty() {
        return Err(anyhow!(
            "shard at {} produced no recall keys (every record had empty street/locality/postcode)",
            shard_path.display()
        ));
    }

    // 2. Stream into FST builder + postings payload.
    let fst_path = sidecar_path(shard_path, FST_EXT);
    let postings_path = sidecar_path(shard_path, POSTINGS_EXT);
    let stats_path = sidecar_path(shard_path, STATS_EXT);

    let fst_writer = std::io::BufWriter::new(
        std::fs::File::create(&fst_path)
            .with_context(|| format!("creating {}", fst_path.display()))?,
    );
    let mut postings_writer = std::io::BufWriter::new(
        std::fs::File::create(&postings_path)
            .with_context(|| format!("creating {}", postings_path.display()))?,
    );

    let mut builder = fst::MapBuilder::new(fst_writer)
        .map_err(|e| anyhow!("fst::MapBuilder::new failed: {e}"))?;

    let mut postings_offset_words: u64 = 0;
    let mut total_postings: u64 = 0;
    let mut posting_counts: Vec<u32> = Vec::with_capacity(buckets.len());
    let mut total_key_bytes: u64 = 0;

    use std::io::Write;
    for (key, mut postings) in buckets {
        // Dedup postings within a key — a single record could be
        // pushed twice if address-key happened to equal place-key
        // before the equality guard fired.
        postings.sort_unstable();
        postings.dedup();
        if postings.len() > 0x00FF_FFFF {
            return Err(anyhow!(
                "recall key {:?} has {} postings; exceeds 24-bit count limit. \
                 Split the shard or increase POSTING_COUNT_BITS.",
                key,
                postings.len()
            ));
        }
        let count = postings.len() as u64;
        let value = (postings_offset_words << 24) | count;
        builder
            .insert(key.as_bytes(), value)
            .map_err(|e| anyhow!("fst insert failed for key {:?}: {e}", key))?;

        for p in &postings {
            postings_writer
                .write_all(&p.to_le_bytes())
                .with_context(|| "writing postings")?;
        }
        postings_offset_words += count;
        total_postings += count;
        posting_counts.push(postings.len() as u32);
        total_key_bytes += key.len() as u64;
    }
    builder
        .finish()
        .map_err(|e| anyhow!("fst finish failed: {e}"))?;
    postings_writer
        .flush()
        .with_context(|| "flushing postings file")?;
    drop(postings_writer);

    // 3. Stats sidecar.
    let vocab_size = posting_counts.len();
    let avg_key_len = if vocab_size > 0 {
        total_key_bytes as f64 / vocab_size as f64
    } else {
        0.0
    };
    posting_counts.sort_unstable();
    let p50 = percentile(&posting_counts, 0.50);
    let p95 = percentile(&posting_counts, 0.95);

    let stats = ShardRecallStats {
        country_iso2: shard.country().iso2().to_string(),
        vocab_size,
        avg_key_len,
        p50_postings: p50,
        p95_postings: p95,
        total_postings,
        record_count: shard.record_count(),
    };

    let stats_json =
        serde_json::to_vec_pretty(&stats).with_context(|| "serializing recall stats")?;
    std::fs::write(&stats_path, &stats_json)
        .with_context(|| format!("writing {}", stats_path.display()))?;

    Ok(BuildReport {
        fst_path,
        postings_path,
        stats_path,
        stats,
    })
}

/// Encode a (record_id, source) pair into a single u32 posting word.
fn encode_posting(record_id: u32, source: SourceTag) -> u32 {
    let id = record_id & super::POSTING_ID_MASK;
    match source {
        SourceTag::OpenAddresses => id | POSTING_OA_FLAG,
        SourceTag::Osm => id,
    }
}

/// Build the canonical address key (street + house + postcode + locality).
#[must_use]
pub fn canonical_address_key(
    street: &str,
    housenumber: &str,
    postcode: &str,
    locality: &str,
) -> String {
    let mut buf = String::with_capacity(64);
    let mut first = true;
    for part in [street, housenumber, postcode, locality] {
        let n = normalize(part);
        if n.is_empty() {
            continue;
        }
        if !first {
            buf.push(' ');
        }
        buf.push_str(&n);
        first = false;
    }
    buf
}

fn sidecar_path(base: &Path, ext: &str) -> PathBuf {
    let parent = base.parent().unwrap_or_else(|| Path::new("."));
    let file_name = base
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("shard.bfgs");
    let stripped = file_name.strip_suffix(".bfgs").unwrap_or(file_name);
    parent.join(format!("{stripped}.{ext}"))
}

fn percentile(sorted: &[u32], q: f64) -> u32 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64 - 1.0) * q).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routing::CountryId;
    use crate::shard::AddressRecord;
    use crate::shard::builder::build_shard;
    use tempfile::tempdir;

    fn small_shard(dir: &Path) -> PathBuf {
        let p = dir.join("be.bfgs");
        let addrs = vec![
            AddressRecord {
                street: "Rue Wayez".into(),
                housenumber: "122".into(),
                postcode: "1070".into(),
                locality: "Anderlecht".into(),
                lat: 50.834,
                lon: 4.314,
                source: SourceTag::OpenAddresses,
                ..Default::default()
            },
            AddressRecord {
                street: "Grote Markt".into(),
                housenumber: "1".into(),
                postcode: "2000".into(),
                locality: "Antwerpen".into(),
                lat: 51.221,
                lon: 4.401,
                source: SourceTag::Osm,
                ..Default::default()
            },
        ];
        build_shard(&p, CountryId::BE, addrs).unwrap();
        p
    }

    #[test]
    fn build_recall_smoke() {
        let dir = tempdir().unwrap();
        let shard_path = small_shard(dir.path());
        let shard = Shard::open(&shard_path).unwrap();
        let report = build_recall_index(&shard_path, &shard, &BuildOptions::default()).unwrap();
        assert!(report.fst_path.exists());
        assert!(report.postings_path.exists());
        assert!(report.stats_path.exists());
        assert_eq!(report.stats.country_iso2, "BE");
        assert_eq!(report.stats.record_count, 2);
        // 2 address keys + 2 distinct place keys = 4 vocab entries.
        assert_eq!(report.stats.vocab_size, 4);
    }

    #[test]
    fn canonical_key_skips_empty_fields() {
        let k = canonical_address_key("Rue Wayez", "", "1070", "Anderlecht");
        assert_eq!(k, "rue wayez 1070 anderlecht");
        let k2 = canonical_address_key("", "", "", "Antwerpen");
        assert_eq!(k2, "antwerpen");
    }

    #[test]
    fn posting_encoding_round_trip() {
        let p = encode_posting(42, SourceTag::Osm);
        assert_eq!(p, 42);
        let p2 = encode_posting(42, SourceTag::OpenAddresses);
        assert_eq!(p2, 42 | POSTING_OA_FLAG);
        assert_eq!(p2 & super::super::POSTING_ID_MASK, 42);
    }
}
