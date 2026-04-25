//! Binary on-disk format for a precomputed [`TransferGraph`].
//!
//! ```text
//! +----------------------------------------------------------+
//! | u64   MAGIC            = b"BFTRNSF1"                     |
//! | u32   VERSION          = 1                               |
//! | u32   n_stops                                            |
//! | u64   n_edges                                            |
//! | [u8; 32] provenance (SHA-256)                            |
//! | [u32; n_stops + 1]  offsets                              |
//! | [(u32, u32); n_edges]  neighbours (stop_id, walk_s)      |
//! | [u8; 8]  BODY_CRC (CRC-64/XZ over everything above)      |
//! +----------------------------------------------------------+
//! ```
//!
//! The CRC protects the entire header+body; on mismatch the file is
//! treated as absent (and the caller rebuilds from scratch).

use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

use anyhow::{Context, Result, bail};
use crc::Crc;

use super::timetable::StopIdx;
use super::transfers::TransferGraph;

const MAGIC: [u8; 8] = *b"BFTRNSF1";
const VERSION: u32 = 1;

const CRC64: Crc<u64> = Crc::<u64>::new(&crc::CRC_64_XZ);

/// Write a transfer graph to `path` using the binary format.
///
/// Streams the header + offsets + neighbours directly through a
/// digest-updating BufWriter — no full-image memory copy. The CRC is
/// computed incrementally as bytes pass through. See issue #117.
pub fn write(path: &Path, graph: &TransferGraph) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating transit cache directory {}", parent.display()))?;
    }
    let tmp = path.with_extension("bin.tmp");
    let f = File::create(&tmp).with_context(|| format!("creating {}", tmp.display()))?;
    let mut w = BufWriter::new(f);

    let offsets = graph.offsets_raw();
    let neighbours = graph.neighbours_raw();
    let n_stops = (offsets.len() - 1) as u32;
    let n_edges = neighbours.len() as u64;

    // Incremental CRC over the streamed body. Each byte range is both
    // written to disk AND fed into the digest in the same step.
    let mut digest = CRC64.digest();

    fn emit<W: Write>(w: &mut W, d: &mut crc::Digest<'_, u64>, bytes: &[u8]) -> Result<()> {
        w.write_all(bytes).context("writing transit cache body")?;
        d.update(bytes);
        Ok(())
    }

    emit(&mut w, &mut digest, &MAGIC)?;
    emit(&mut w, &mut digest, &VERSION.to_le_bytes())?;
    emit(&mut w, &mut digest, &n_stops.to_le_bytes())?;
    emit(&mut w, &mut digest, &n_edges.to_le_bytes())?;
    emit(&mut w, &mut digest, &graph.provenance)?;

    // u32 offsets stream, explicit little-endian (file format is LE
    // throughout — see header writes above). Buffered into a single
    // `Vec<u8>` so we still call `emit` once per stream rather than
    // hammering the digest 4 bytes at a time.
    let mut offsets_bytes = Vec::with_capacity(offsets.len() * 4);
    for off in offsets {
        offsets_bytes.extend_from_slice(&off.to_le_bytes());
    }
    emit(&mut w, &mut digest, &offsets_bytes)?;

    // (u32, u32) neighbours stream, explicit little-endian.
    let mut neighbours_bytes = Vec::with_capacity(neighbours.len() * 8);
    for (a, b) in neighbours {
        neighbours_bytes.extend_from_slice(&a.to_le_bytes());
        neighbours_bytes.extend_from_slice(&b.to_le_bytes());
    }
    emit(&mut w, &mut digest, &neighbours_bytes)?;

    let crc = digest.finalize();
    w.write_all(&crc.to_le_bytes())
        .context("writing transit cache CRC")?;
    w.flush()?;
    drop(w);
    std::fs::rename(&tmp, path)
        .with_context(|| format!("renaming {} to {}", tmp.display(), path.display()))?;
    Ok(())
}

/// Read a cached transfer graph from `path`. Returns `Ok(None)` if the
/// file is missing, corrupt, or its provenance does not match.
pub fn read(path: &Path, expected_provenance: [u8; 32]) -> Result<Option<TransferGraph>> {
    if !path.exists() {
        return Ok(None);
    }
    let f = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let mut r = BufReader::new(f);
    let mut all = Vec::new();
    r.read_to_end(&mut all)?;
    if all.len() < 8 + 4 + 4 + 8 + 32 + 4 + 8 {
        tracing::warn!("transit cache {} too small — ignoring", path.display());
        return Ok(None);
    }

    let body_len = all.len() - 8;
    let body = &all[..body_len];
    let crc_bytes = &all[body_len..];
    let stored_crc = u64::from_le_bytes(crc_bytes.try_into().unwrap());
    let computed_crc = CRC64.checksum(body);
    if stored_crc != computed_crc {
        tracing::warn!("transit cache {} CRC mismatch — ignoring", path.display());
        return Ok(None);
    }

    let mut cursor = 0usize;
    let mut take = |n: usize| -> &[u8] {
        let s = &body[cursor..cursor + n];
        cursor += n;
        s
    };
    let magic = take(8);
    if magic != MAGIC {
        bail!("transit cache {} has wrong magic", path.display());
    }
    let version = u32::from_le_bytes(take(4).try_into().unwrap());
    if version != VERSION {
        tracing::warn!(
            "transit cache {} version {} != {} — ignoring",
            path.display(),
            version,
            VERSION
        );
        return Ok(None);
    }
    let n_stops = u32::from_le_bytes(take(4).try_into().unwrap()) as usize;
    let n_edges = u64::from_le_bytes(take(8).try_into().unwrap()) as usize;
    let mut provenance = [0u8; 32];
    provenance.copy_from_slice(take(32));
    if provenance != expected_provenance {
        tracing::info!(
            path = %path.display(),
            "transit cache provenance mismatch — will rebuild"
        );
        return Ok(None);
    }

    let mut offsets = Vec::with_capacity(n_stops + 1);
    for _ in 0..(n_stops + 1) {
        let b = take(4);
        offsets.push(u32::from_le_bytes(b.try_into().unwrap()));
    }
    let mut neighbours: Vec<(StopIdx, u32)> = Vec::with_capacity(n_edges);
    for _ in 0..n_edges {
        let s = u32::from_le_bytes(take(4).try_into().unwrap());
        let w = u32::from_le_bytes(take(4).try_into().unwrap());
        neighbours.push((s, w));
    }

    if cursor != body.len() {
        tracing::warn!("transit cache {} trailing bytes — ignoring", path.display());
        return Ok(None);
    }

    // The on-disk layout IS already CSR (offsets + neighbours), so we
    // skip the triples round-trip that `from_triples` would impose and
    // hand the arrays directly to the graph via `from_csr_parts`.
    // See issue #117.
    Ok(Some(TransferGraph::from_csr_parts(
        n_stops, offsets, neighbours, provenance,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn roundtrip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("transfers.bin");
        let mut g = TransferGraph::from_triples(
            4,
            vec![(0, 1, 30), (1, 0, 30), (1, 2, 40), (2, 1, 40), (2, 3, 50)],
        );
        g.provenance = [7u8; 32];
        write(&path, &g).unwrap();
        let loaded = read(&path, [7u8; 32]).unwrap().unwrap();
        let a: Vec<_> = loaded.neighbours(1).collect();
        assert_eq!(a, vec![(0, 30), (2, 40)]);
    }

    #[test]
    fn provenance_mismatch_rejects() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("transfers.bin");
        let g = TransferGraph::from_triples(2, vec![(0, 1, 10), (1, 0, 10)]);
        write(&path, &g).unwrap();
        assert!(read(&path, [9u8; 32]).unwrap().is_none());
    }
}
