//! Open and query a recall FST + postings + stats sidecar (#205).

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use fst::{IntoStreamer, Streamer};
use memmap2::Mmap;

use super::stats::ShardRecallStats;
use super::{FST_EXT, POSTINGS_EXT, POSTING_OA_FLAG, POSTING_ID_MASK, STATS_EXT};
use crate::shard::SourceTag;

/// One posting decoded from the recall payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Posting {
    pub record_id: u32,
    pub source: SourceTag,
}

/// Memory-mapped recall index for a single country shard.
///
/// Both the FST file and the postings payload are mmap'd; the FST is
/// owned via `fst::Map<Mmap>` and the postings as a `Arc<Mmap>`. Both
/// are zero-copy: lookups read directly from the kernel's file cache.
#[derive(Debug)]
pub struct RecallIndex {
    map: fst::Map<Mmap>,
    postings: Arc<Mmap>,
    stats: ShardRecallStats,
    /// Path to the source `.bfgs` shard (informational).
    pub shard_path: PathBuf,
}

impl RecallIndex {
    /// Open a recall index sitting next to a shard at `shard_path`.
    /// Looks for `<base>.recall.fst` / `<base>.recall.postings` /
    /// `<base>.recall.stats.json`.
    pub fn open(shard_path: &Path) -> Result<Self> {
        let parent = shard_path.parent().unwrap_or_else(|| Path::new("."));
        let file_name = shard_path
            .file_name()
            .and_then(|s| s.to_str())
            .ok_or_else(|| anyhow!("shard path has no file name: {}", shard_path.display()))?;
        let stripped = file_name.strip_suffix(".bfgs").unwrap_or(file_name);
        let fst_path = parent.join(format!("{stripped}.{FST_EXT}"));
        let postings_path = parent.join(format!("{stripped}.{POSTINGS_EXT}"));
        let stats_path = parent.join(format!("{stripped}.{STATS_EXT}"));

        let fst_file = std::fs::File::open(&fst_path)
            .with_context(|| format!("opening recall FST at {}", fst_path.display()))?;
        // SAFETY note: mmap is wrapped in `unsafe` upstream; we expose
        // it through the safe Mmap type. The file is opened read-only.
        let mmap = unsafe { Mmap::map(&fst_file) }
            .with_context(|| format!("mmapping recall FST at {}", fst_path.display()))?;
        let map = fst::Map::new(mmap)
            .map_err(|e| anyhow!("FST at {} is malformed: {e}", fst_path.display()))?;

        let postings_file = std::fs::File::open(&postings_path)
            .with_context(|| format!("opening recall postings at {}", postings_path.display()))?;
        let postings_mmap = unsafe { Mmap::map(&postings_file) }
            .with_context(|| format!("mmapping recall postings at {}", postings_path.display()))?;

        let stats_bytes = std::fs::read(&stats_path)
            .with_context(|| format!("reading recall stats at {}", stats_path.display()))?;
        let stats: ShardRecallStats = serde_json::from_slice(&stats_bytes)
            .with_context(|| format!("parsing recall stats at {}", stats_path.display()))?;

        Ok(Self {
            map,
            postings: Arc::new(postings_mmap),
            stats,
            shard_path: shard_path.to_path_buf(),
        })
    }

    #[must_use]
    pub fn stats(&self) -> &ShardRecallStats {
        &self.stats
    }

    /// Exact lookup. Returns the postings list for `key`, or empty if
    /// the key is not present.
    #[must_use]
    pub fn get(&self, key: &str) -> Vec<Posting> {
        let Some(value) = self.map.get(key.as_bytes()) else {
            return Vec::new();
        };
        self.decode_postings(value)
    }

    /// Iterate over every key beginning with `prefix` and emit each
    /// matching posting. Caller-controlled budget through
    /// `max_postings`.
    pub fn prefix(&self, prefix: &str, max_postings: usize) -> Vec<(String, Posting)> {
        let mut out: Vec<(String, Posting)> = Vec::new();
        if max_postings == 0 {
            return out;
        }
        let matcher = fst::automaton::Str::new(prefix).starts_with();
        let mut stream = self.map.search(matcher).into_stream();
        while let Some((key_bytes, value)) = stream.next() {
            let postings = self.decode_postings(value);
            // Cheap UTF-8 validation: keys are produced by normalize()
            // which only emits ASCII alphanumerics + '-' + ' ' + '\''.
            // Lossy is acceptable defensively.
            let key = String::from_utf8_lossy(key_bytes).into_owned();
            for p in postings {
                out.push((key.clone(), p));
                if out.len() >= max_postings {
                    return out;
                }
            }
        }
        out
    }

    /// Iterate over every key in lex order. Used when the input is so
    /// short that a prefix scan would still be too narrow — e.g.
    /// single-token place-name fallback. Bounded by `max_keys`.
    pub fn iter_all(&self, max_keys: usize) -> Vec<(String, Posting)> {
        let mut out: Vec<(String, Posting)> = Vec::new();
        if max_keys == 0 {
            return out;
        }
        let mut stream = self.map.stream();
        let mut keys_seen = 0usize;
        while let Some((key_bytes, value)) = stream.next() {
            let key = String::from_utf8_lossy(key_bytes).into_owned();
            for p in self.decode_postings(value) {
                out.push((key.clone(), p));
            }
            keys_seen += 1;
            if keys_seen >= max_keys {
                break;
            }
        }
        out
    }

    /// Number of keys in the FST.
    #[must_use]
    pub fn key_count(&self) -> usize {
        self.map.len()
    }

    fn decode_postings(&self, value: u64) -> Vec<Posting> {
        let count = (value & 0x00FF_FFFF) as usize;
        let offset_words = (value >> 24) as usize;
        let byte_start = offset_words * 4;
        let byte_end = byte_start + count * 4;
        let bytes: &[u8] = &self.postings;
        if byte_end > bytes.len() {
            // Defensive: a corrupt FST should not panic the server.
            return Vec::new();
        }
        let mut out = Vec::with_capacity(count);
        for i in 0..count {
            let off = byte_start + i * 4;
            let word = u32::from_le_bytes([
                bytes[off],
                bytes[off + 1],
                bytes[off + 2],
                bytes[off + 3],
            ]);
            let id = word & POSTING_ID_MASK;
            let source = if word & POSTING_OA_FLAG != 0 {
                SourceTag::OpenAddresses
            } else {
                SourceTag::Osm
            };
            out.push(Posting {
                record_id: id,
                source,
            });
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::super::build::{BuildOptions, build_recall_index};
    use super::*;
    use crate::routing::CountryId;
    use crate::shard::AddressRecord;
    use crate::shard::builder::build_shard;
    use crate::shard::reader::Shard;
    use tempfile::tempdir;

    fn fixture() -> (tempfile::TempDir, PathBuf) {
        let dir = tempdir().unwrap();
        let p = dir.path().join("be.bfgs");
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
        let shard = Shard::open(&p).unwrap();
        build_recall_index(&p, &shard, &BuildOptions::default()).unwrap();
        (dir, p)
    }

    #[test]
    fn open_and_lookup_exact() {
        let (_d, p) = fixture();
        let idx = RecallIndex::open(&p).unwrap();
        let postings = idx.get("rue wayez 122 1070 anderlecht");
        assert_eq!(postings.len(), 1);
        assert_eq!(postings[0].record_id, 0);
        assert_eq!(postings[0].source, SourceTag::OpenAddresses);
        assert_eq!(idx.stats().vocab_size, 4);
    }

    #[test]
    fn prefix_scan_finds_partial() {
        let (_d, p) = fixture();
        let idx = RecallIndex::open(&p).unwrap();
        let mut hits = idx.prefix("grote", 10);
        // Postings can come in any FST traversal order; sort for stability.
        hits.sort_by(|a, b| a.0.cmp(&b.0));
        assert!(!hits.is_empty(), "expected at least one prefix hit");
        assert!(hits.iter().any(|(k, _)| k.starts_with("grote")));
    }

    #[test]
    fn missing_key_returns_empty() {
        let (_d, p) = fixture();
        let idx = RecallIndex::open(&p).unwrap();
        assert!(idx.get("nonexistent street 9999").is_empty());
    }
}
