//! JSONL output writer.
//!
//! TrainRecord shape is the contract with the future Phase 2 trainer (#98).
//! Don't change field names without also updating the trainer.

use anyhow::Result;
use serde::Serialize;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

#[derive(Debug, Clone, Serialize)]
pub struct TrainRecord {
    pub text: String,
    /// One BIO label per BYTE of `text`. Length must equal `text.len()` (bytes).
    pub bio_labels: Vec<u8>,
    pub country: String,
    pub source_record_id: String,
    pub augmentation: String,
}

pub struct JsonlWriter {
    inner: BufWriter<File>,
}

impl JsonlWriter {
    pub fn new(path: &Path) -> Result<Self> {
        let f = File::create(path)?;
        Ok(Self {
            inner: BufWriter::with_capacity(64 * 1024, f),
        })
    }

    pub fn write(&mut self, record: &TrainRecord) -> Result<()> {
        // Sanity: bio_labels length must match text byte length.
        debug_assert_eq!(
            record.text.len(),
            record.bio_labels.len(),
            "BIO label / byte length mismatch"
        );
        let line = serde_json::to_string(record)?;
        self.inner.write_all(line.as_bytes())?;
        self.inner.write_all(b"\n")?;
        Ok(())
    }

    pub fn finish(mut self) -> Result<()> {
        self.inner.flush()?;
        Ok(())
    }
}
