//! Streaming JSONL corpus reader for `multi-country-corpus-3p6m.jsonl`.

use serde::Deserialize;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

#[derive(Deserialize, Debug)]
pub struct CorpusRecord {
    pub text: String,
    pub country: String,
    #[serde(default)]
    pub augmentation: Option<String>,
}

/// Streaming iterator — yields one record per line. We don't load
/// 720 MB into RAM. Each record is parsed on demand.
pub struct CorpusReader {
    inner: BufReader<File>,
}

impl CorpusReader {
    pub fn open(path: &Path) -> std::io::Result<Self> {
        let f = File::open(path)?;
        // 4 MB buffer — disk sequential read on a dataset this size benefits.
        Ok(CorpusReader {
            inner: BufReader::with_capacity(4 * 1024 * 1024, f),
        })
    }

    pub fn into_iter(self) -> CorpusIter {
        CorpusIter {
            inner: self.inner,
            buf: String::new(),
        }
    }
}

pub struct CorpusIter {
    inner: BufReader<File>,
    buf: String,
}

impl Iterator for CorpusIter {
    type Item = std::io::Result<CorpusRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        self.buf.clear();
        match self.inner.read_line(&mut self.buf) {
            Ok(0) => None,
            Ok(_) => {
                let line = self.buf.trim_end_matches('\n');
                if line.is_empty() {
                    return self.next();
                }
                match serde_json::from_str::<CorpusRecord>(line) {
                    Ok(r) => Some(Ok(r)),
                    Err(e) => Some(Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        e.to_string(),
                    ))),
                }
            }
            Err(e) => Some(Err(e)),
        }
    }
}
