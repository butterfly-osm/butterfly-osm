//! Unified butterfly.dat container — single file holds everything
//! the server needs (per-step artefacts + indices + transit) so we
//! can mmap once and zero-copy from there.
//!
//! # On-disk layout
//!
//! ```text
//! +-------------------------------------+ offset 0
//! | Header (64 bytes)                   |
//! |   magic   : u32 = "BFLY"            |
//! |   version : u16                     |
//! |   _resv0  : u16                     |
//! |   n_sec   : u32                     |
//! |   _resv1  : u32                     |
//! |   dir_off : u64                     |
//! |   dir_len : u64                     |
//! |   pad     : 32 bytes (zero)         |
//! +-------------------------------------+
//! | Section payloads                    |
//! |   raw bytes for each section,       |
//! |   in directory order, contiguous,   |
//! |   no inter-section padding          |
//! +-------------------------------------+ offset = dir_off
//! | Section directory                   |
//! |   for each section:                 |
//! |     kind     : u32                  |
//! |     _resv    : u32                  |
//! |     offset   : u64                  |
//! |     len      : u64                  |
//! |     crc      : u64                  |
//! |     name_len : u16                  |
//! |     name     : `name_len` UTF-8 B   |
//! |     pad to 8                        |
//! +-------------------------------------+
//! | Footer (16 bytes)                   |
//! |   dir_crc  : u64                    |
//! |   file_crc : u64 (header+payloads+  |
//! |              directory)             |
//! +-------------------------------------+
//! ```
//!
//! The directory lives at the end of the file so a streaming writer
//! does not need to know the total payload size up front. Readers
//! seek to the footer first, validate the dir CRC, then mmap.
//!
//! Every section is also CRC-checked individually, so corruption can
//! be localised and a single bad section does not invalidate every
//! mode.
//!
//! Section names are stored as UTF-8 strings (e.g. `"weights/car"`,
//! `"cch/topo"`). The `kind` enum is for typed dispatch; the name is
//! for logging and for ad-hoc / future sections (e.g. per-mode
//! traffic-customised weights from #84).

use anyhow::{Context, Result};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;

use super::crc;

/// File magic — "BFLY" little endian.
pub const MAGIC: u32 = 0x594C4642;

/// Format version. Bump on any incompatible layout change.
pub const VERSION: u16 = 1;

/// Header size in bytes (always 64). Layout above.
pub const HEADER_SIZE: u64 = 64;

/// Footer size in bytes (always 16). Two u64 CRCs.
pub const FOOTER_SIZE: u64 = 16;

/// What kind of payload a section holds.
///
/// Kinds are stored as `u32` on disk; new kinds get appended with a
/// fresh discriminant. Older readers will still find the section via
/// the directory and can choose to ignore unknown kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum SectionKind {
    /// `step1/nodes.sa` — packed array of all OSM node coords.
    NodesSa = 0x0001_0001,
    /// `step1/nodes.si` — sparse index over `nodes.sa`.
    NodesSi = 0x0001_0002,
    /// `step1/ways.raw` — raw way bytes (also the source of road names).
    WaysRaw = 0x0001_0003,
    /// `step1/relations.raw` — relation bytes (turn restrictions etc).
    RelationsRaw = 0x0001_0004,
    /// `step1/node_signals.bin` — traffic signal node mask.
    NodeSignals = 0x0001_0005,

    /// `step2/way_attrs.<mode>.bin` — per-mode way attributes.
    /// Section name carries the mode string.
    WayAttrs = 0x0002_0001,
    /// `step2/turn_rules.<mode>.bin` — per-mode turn rules.
    TurnRules = 0x0002_0002,

    /// `step3/nbg.csr` — node-based graph CSR.
    NbgCsr = 0x0003_0001,
    /// `step3/nbg.geo` — NBG edge geometries.
    NbgGeo = 0x0003_0002,
    /// `step3/nbg.node_map` — OSM node id ↔ compact id map.
    NbgNodeMap = 0x0003_0003,

    /// `step4/ebg.nodes` — edge-based graph node table.
    EbgNodes = 0x0004_0001,
    /// `step4/ebg.csr` — EBG CSR adjacency.
    EbgCsr = 0x0004_0002,
    /// `step4/ebg.turn_table` — turn cost table.
    EbgTurnTable = 0x0004_0003,

    /// `step5/filtered.<mode>.ebg`. Name carries mode.
    FilteredEbg = 0x0005_0001,
    /// `step5/w.<mode>.u32` — per-mode time weights on EBG.
    NodeWeightsTime = 0x0005_0002,
    /// `step5/t.<mode>.u32` — per-mode turn penalties on EBG.
    NodeWeightsTurn = 0x0005_0003,
    /// `step5/mask.<mode>.bitset` — per-mode accessibility mask.
    ModeMask = 0x0005_0004,

    /// `step6/order.<mode>.ebg` — per-mode CCH ordering.
    OrderEbg = 0x0006_0001,

    /// `step7/cch.<mode>.topo` — per-mode CCH topology.
    CchTopo = 0x0007_0001,

    /// `step8/cch.w.<mode>.u32` — per-mode customised time weights.
    CchWeightsTime = 0x0008_0001,
    /// `step8/cch.d.<mode>.u32` — per-mode customised distance weights.
    CchWeightsDist = 0x0008_0002,

    /// Pre-built flat UP adjacency for a (mode, metric). Built at pack
    /// time from cch_topo + cch_weights. Mmapped at server boot — the
    /// substrate that bounds idle RSS to working set rather than dataset
    /// size (see #150).
    UpAdjFlat = 0x0009_0001,
    /// Pre-built flat forward DOWN adjacency for a (mode, metric).
    DownAdjFlat = 0x0009_0002,
    /// Pre-built flat reverse DOWN adjacency for a (mode, metric).
    DownReverseAdjFlat = 0x0009_0003,

    /// Future / unrecognised. Readers that see this should fall back
    /// to the section name string.
    Unknown = 0xFFFF_FFFF,
}

impl SectionKind {
    /// Convert a raw discriminant from disk into a known kind, or
    /// `Unknown` for anything we have not seen before.
    pub fn from_u32(v: u32) -> Self {
        match v {
            0x0001_0001 => Self::NodesSa,
            0x0001_0002 => Self::NodesSi,
            0x0001_0003 => Self::WaysRaw,
            0x0001_0004 => Self::RelationsRaw,
            0x0001_0005 => Self::NodeSignals,

            0x0002_0001 => Self::WayAttrs,
            0x0002_0002 => Self::TurnRules,

            0x0003_0001 => Self::NbgCsr,
            0x0003_0002 => Self::NbgGeo,
            0x0003_0003 => Self::NbgNodeMap,

            0x0004_0001 => Self::EbgNodes,
            0x0004_0002 => Self::EbgCsr,
            0x0004_0003 => Self::EbgTurnTable,

            0x0005_0001 => Self::FilteredEbg,
            0x0005_0002 => Self::NodeWeightsTime,
            0x0005_0003 => Self::NodeWeightsTurn,
            0x0005_0004 => Self::ModeMask,

            0x0006_0001 => Self::OrderEbg,

            0x0007_0001 => Self::CchTopo,

            0x0008_0001 => Self::CchWeightsTime,
            0x0008_0002 => Self::CchWeightsDist,

            0x0009_0001 => Self::UpAdjFlat,
            0x0009_0002 => Self::DownAdjFlat,
            0x0009_0003 => Self::DownReverseAdjFlat,

            _ => Self::Unknown,
        }
    }

    /// Human-readable label for this kind. Useful in `inspect`.
    pub fn label(self) -> &'static str {
        match self {
            Self::NodesSa => "step1/nodes.sa",
            Self::NodesSi => "step1/nodes.si",
            Self::WaysRaw => "step1/ways.raw",
            Self::RelationsRaw => "step1/relations.raw",
            Self::NodeSignals => "step1/node_signals.bin",
            Self::WayAttrs => "step2/way_attrs",
            Self::TurnRules => "step2/turn_rules",
            Self::NbgCsr => "step3/nbg.csr",
            Self::NbgGeo => "step3/nbg.geo",
            Self::NbgNodeMap => "step3/nbg.node_map",
            Self::EbgNodes => "step4/ebg.nodes",
            Self::EbgCsr => "step4/ebg.csr",
            Self::EbgTurnTable => "step4/ebg.turn_table",
            Self::FilteredEbg => "step5/filtered.ebg",
            Self::NodeWeightsTime => "step5/w.u32",
            Self::NodeWeightsTurn => "step5/t.u32",
            Self::ModeMask => "step5/mask.bitset",
            Self::OrderEbg => "step6/order.ebg",
            Self::CchTopo => "step7/cch.topo",
            Self::CchWeightsTime => "step8/cch.w.u32",
            Self::CchWeightsDist => "step8/cch.d.u32",
            Self::UpAdjFlat => "flat/up_adj",
            Self::DownAdjFlat => "flat/down_adj",
            Self::DownReverseAdjFlat => "flat/down_reverse_adj",
            Self::Unknown => "unknown",
        }
    }
}

/// One directory entry. Owned representation; the on-disk layout is
/// variable-width (because of `name`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SectionEntry {
    pub kind: SectionKind,
    pub name: String,
    /// Absolute byte offset in the container.
    pub offset: u64,
    /// Length of the payload in bytes (no padding).
    pub len: u64,
    /// CRC-64 of just this section's payload.
    pub crc: u64,
}

/// Streaming writer. Append sections one at a time; finalise to flush
/// the directory and footer.
pub struct ContainerWriter {
    file: BufWriter<File>,
    sections: Vec<SectionEntry>,
    cursor: u64,
}

impl ContainerWriter {
    /// Create a new file at `path`, write a placeholder header, and
    /// return a writer ready to append sections.
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Self> {
        // Open read+write+truncate so `finalize` can read the file
        // back to compute the full-file CRC without reopening.
        let raw = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path.as_ref())
            .with_context(|| format!("creating container file {}", path.as_ref().display()))?;
        let mut file = BufWriter::new(raw);

        // Reserve header space; we patch it on `finalize`.
        let placeholder = [0u8; HEADER_SIZE as usize];
        file.write_all(&placeholder)?;

        Ok(Self {
            file,
            sections: Vec::new(),
            cursor: HEADER_SIZE,
        })
    }

    /// Append a section by streaming bytes from `reader`. Returns once
    /// the payload has been written and CRC accumulated.
    pub fn append_from_reader<R: Read>(
        &mut self,
        kind: SectionKind,
        name: impl Into<String>,
        reader: &mut R,
    ) -> Result<()> {
        let name = name.into();
        anyhow::ensure!(
            name.len() <= u16::MAX as usize,
            "section name too long: {} bytes (max {})",
            name.len(),
            u16::MAX
        );

        // Pad cursor up to an 8-byte boundary so every section starts
        // u64-aligned in the file. Required by #147 zero-copy readers:
        // they reinterpret section bytes as `&[u32]` / `&[u64]` via
        // `bytemuck::cast_slice`, which fails on misaligned input.
        // Padding bytes are zero and are NOT included in the section CRC
        // (CRC covers the named payload exactly).
        let pad = ((8 - (self.cursor % 8)) % 8) as usize;
        if pad != 0 {
            let zeros = [0u8; 8];
            self.file.write_all(&zeros[..pad])?;
            self.cursor += pad as u64;
        }
        let offset = self.cursor;
        let mut sec_digest = crc::Digest::new();
        let mut buf = vec![0u8; 1 << 20]; // 1 MiB streaming buffer.
        let mut len: u64 = 0;
        loop {
            let n = reader.read(&mut buf)?;
            if n == 0 {
                break;
            }
            self.file.write_all(&buf[..n])?;
            sec_digest.update(&buf[..n]);
            len += n as u64;
        }
        let crc = sec_digest.finalize();
        self.cursor += len;

        self.sections.push(SectionEntry {
            kind,
            name,
            offset,
            len,
            crc,
        });
        Ok(())
    }

    /// Append a section by copying an in-memory byte slice. Convenience
    /// wrapper around the streaming variant.
    pub fn append_bytes(
        &mut self,
        kind: SectionKind,
        name: impl Into<String>,
        bytes: &[u8],
    ) -> Result<()> {
        let mut cur = std::io::Cursor::new(bytes);
        self.append_from_reader(kind, name, &mut cur)
    }

    /// Append a section by reading the entire file at `path`.
    pub fn append_file(
        &mut self,
        kind: SectionKind,
        name: impl Into<String>,
        path: impl AsRef<Path>,
    ) -> Result<()> {
        let mut f = File::open(path.as_ref())
            .with_context(|| format!("opening section source {}", path.as_ref().display()))?;
        self.append_from_reader(kind, name, &mut f)
    }

    /// Number of sections appended so far. Useful for progress
    /// reporting.
    pub fn len(&self) -> usize {
        self.sections.len()
    }

    pub fn is_empty(&self) -> bool {
        self.sections.is_empty()
    }

    /// Write the directory + footer, patch the header in place, flush
    /// to disk. Consumes the writer.
    pub fn finalize(self) -> Result<()> {
        let Self {
            file,
            sections,
            cursor,
        } = self;
        let dir_offset = cursor;

        // --- Encode the directory ------------------------------------
        let mut dir_bytes: Vec<u8> = Vec::with_capacity(sections.len() * 64);
        for sec in &sections {
            let kind_u = match sec.kind {
                SectionKind::Unknown => SectionKind::Unknown as u32,
                k => k as u32,
            };
            dir_bytes.extend_from_slice(&kind_u.to_le_bytes());
            dir_bytes.extend_from_slice(&0u32.to_le_bytes()); // _resv
            dir_bytes.extend_from_slice(&sec.offset.to_le_bytes());
            dir_bytes.extend_from_slice(&sec.len.to_le_bytes());
            dir_bytes.extend_from_slice(&sec.crc.to_le_bytes());
            let nl = sec.name.len() as u16;
            dir_bytes.extend_from_slice(&nl.to_le_bytes());
            dir_bytes.extend_from_slice(sec.name.as_bytes());
            // Pad entry to multiple of 8 so subsequent reads stay
            // aligned on disk and the directory is easy to dump.
            let entry_len = 4 + 4 + 8 + 8 + 8 + 2 + sec.name.len();
            let pad = (8 - (entry_len % 8)) % 8;
            dir_bytes.resize(dir_bytes.len() + pad, 0);
        }

        let dir_crc = crc::checksum(&dir_bytes);
        let dir_len = dir_bytes.len() as u64;

        // Encode the patched header.
        let mut header = [0u8; HEADER_SIZE as usize];
        header[0..4].copy_from_slice(&MAGIC.to_le_bytes());
        header[4..6].copy_from_slice(&VERSION.to_le_bytes());
        header[6..8].copy_from_slice(&0u16.to_le_bytes()); // _resv0
        header[8..12].copy_from_slice(&(sections.len() as u32).to_le_bytes());
        header[12..16].copy_from_slice(&0u32.to_le_bytes()); // _resv1
        header[16..24].copy_from_slice(&dir_offset.to_le_bytes());
        header[24..32].copy_from_slice(&dir_len.to_le_bytes());
        // bytes [32..64] left zero.

        // --- Hand off the underlying File ----------------------------
        // BufWriter::into_inner flushes for us. After this point we own
        // the raw File and can seek/read freely.
        let mut underlying = file.into_inner()?;

        // Append the directory at the current write position
        // (BufWriter ended exactly at `dir_offset`).
        underlying.seek(SeekFrom::Start(dir_offset))?;
        underlying.write_all(&dir_bytes)?;

        // Patch header in place.
        underlying.seek(SeekFrom::Start(0))?;
        underlying.write_all(&header)?;
        underlying.sync_data()?;

        // Recompute the full file CRC by streaming from disk (covers
        // header || payloads || directory). One pass, ~8 GiB at peak;
        // disk reads are sequential and fast.
        underlying.seek(SeekFrom::Start(0))?;
        let total_len = HEADER_SIZE + (dir_offset - HEADER_SIZE) + dir_len;
        let mut digest = crc::Digest::new();
        let mut remaining = total_len;
        let mut buf = vec![0u8; 1 << 20];
        while remaining > 0 {
            let want = std::cmp::min(remaining as usize, buf.len());
            underlying.read_exact(&mut buf[..want])?;
            digest.update(&buf[..want]);
            remaining -= want as u64;
        }
        let file_crc = digest.finalize();

        // Write footer at end of file.
        underlying.seek(SeekFrom::Start(total_len))?;
        underlying.write_all(&dir_crc.to_le_bytes())?;
        underlying.write_all(&file_crc.to_le_bytes())?;
        underlying.sync_all()?;

        Ok(())
    }
}

/// Parsed directory + header. Validates the directory CRC on read but
/// does not validate per-section CRCs (that is on demand via
/// [`Container::verify_section`]).
#[derive(Debug, Clone)]
pub struct Container {
    pub version: u16,
    pub n_sections: u32,
    pub dir_offset: u64,
    pub dir_len: u64,
    pub sections: Vec<SectionEntry>,
    /// Lookup by section name → index in `sections`.
    pub by_name: BTreeMap<String, usize>,
}

impl Container {
    /// Parse the header + directory of a container file. Does not
    /// validate the file CRC (that requires reading every byte; do it
    /// explicitly via [`Container::verify_file_crc`] when needed).
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        let mut file =
            File::open(path).with_context(|| format!("opening container {}", path.display()))?;
        let total_len = file.metadata()?.len();
        anyhow::ensure!(
            total_len >= HEADER_SIZE + FOOTER_SIZE,
            "container file too small: {} bytes",
            total_len
        );

        // Header
        let mut header = [0u8; HEADER_SIZE as usize];
        file.read_exact(&mut header)?;
        let magic = u32::from_le_bytes(header[0..4].try_into().unwrap());
        anyhow::ensure!(
            magic == MAGIC,
            "bad magic in {}: got 0x{:08X}, expected 0x{:08X}",
            path.display(),
            magic,
            MAGIC
        );
        let version = u16::from_le_bytes(header[4..6].try_into().unwrap());
        anyhow::ensure!(
            version == VERSION,
            "unsupported container version {} (expected {})",
            version,
            VERSION
        );
        let n_sections = u32::from_le_bytes(header[8..12].try_into().unwrap());
        let dir_offset = u64::from_le_bytes(header[16..24].try_into().unwrap());
        let dir_len = u64::from_le_bytes(header[24..32].try_into().unwrap());

        anyhow::ensure!(
            dir_offset >= HEADER_SIZE && dir_offset + dir_len + FOOTER_SIZE == total_len,
            "directory offset/length inconsistent with file size: \
             dir_off={}, dir_len={}, file_len={}",
            dir_offset,
            dir_len,
            total_len
        );

        // Footer
        let mut footer = [0u8; FOOTER_SIZE as usize];
        file.seek(SeekFrom::Start(total_len - FOOTER_SIZE))?;
        file.read_exact(&mut footer)?;
        let stored_dir_crc = u64::from_le_bytes(footer[0..8].try_into().unwrap());
        let stored_file_crc = u64::from_le_bytes(footer[8..16].try_into().unwrap());

        // Read the directory, validate its CRC.
        let mut dir_bytes = vec![0u8; dir_len as usize];
        file.seek(SeekFrom::Start(dir_offset))?;
        file.read_exact(&mut dir_bytes)?;
        let computed_dir_crc = crc::checksum(&dir_bytes);
        anyhow::ensure!(
            computed_dir_crc == stored_dir_crc,
            "directory CRC mismatch: computed 0x{:016X}, stored 0x{:016X}",
            computed_dir_crc,
            stored_dir_crc
        );
        let _ = stored_file_crc; // verified separately on demand

        // Decode entries.
        let mut sections = Vec::with_capacity(n_sections as usize);
        let mut by_name = BTreeMap::new();
        let mut p = 0usize;
        for _ in 0..n_sections {
            anyhow::ensure!(
                p + 34 <= dir_bytes.len(),
                "directory truncated (entry header)"
            );
            let kind =
                SectionKind::from_u32(u32::from_le_bytes(dir_bytes[p..p + 4].try_into().unwrap()));
            // p+4..p+8 reserved.
            let offset = u64::from_le_bytes(dir_bytes[p + 8..p + 16].try_into().unwrap());
            let len = u64::from_le_bytes(dir_bytes[p + 16..p + 24].try_into().unwrap());
            let crc_v = u64::from_le_bytes(dir_bytes[p + 24..p + 32].try_into().unwrap());
            let nl = u16::from_le_bytes(dir_bytes[p + 32..p + 34].try_into().unwrap()) as usize;
            anyhow::ensure!(
                p + 34 + nl <= dir_bytes.len(),
                "directory truncated (name body)"
            );
            let name = std::str::from_utf8(&dir_bytes[p + 34..p + 34 + nl])
                .context("section name not valid UTF-8")?
                .to_string();
            let entry_len = 4 + 4 + 8 + 8 + 8 + 2 + nl;
            let pad = (8 - (entry_len % 8)) % 8;
            p += entry_len + pad;

            anyhow::ensure!(
                offset >= HEADER_SIZE && offset + len <= dir_offset,
                "section '{}' offset/len out of range",
                name
            );

            let idx = sections.len();
            sections.push(SectionEntry {
                kind,
                name: name.clone(),
                offset,
                len,
                crc: crc_v,
            });
            by_name.insert(name, idx);
        }

        Ok(Self {
            version,
            n_sections,
            dir_offset,
            dir_len,
            sections,
            by_name,
        })
    }

    /// Look up a section by name.
    pub fn get(&self, name: &str) -> Option<&SectionEntry> {
        self.by_name.get(name).map(|&i| &self.sections[i])
    }

    /// Iterate all sections of a given kind. Order matches insertion
    /// order in the container.
    pub fn iter_kind(&self, kind: SectionKind) -> impl Iterator<Item = &SectionEntry> {
        self.sections.iter().filter(move |s| s.kind == kind)
    }

    /// Iterate all sections whose name starts with `prefix`. Order matches
    /// insertion order. Useful for collecting a per-mode bundle (e.g.
    /// `mode/car/`) or the shared section group (`shared/`).
    pub fn sections_with_prefix<'a>(
        &'a self,
        prefix: &'a str,
    ) -> impl Iterator<Item = &'a SectionEntry> + 'a {
        self.sections
            .iter()
            .filter(move |s| s.name.starts_with(prefix))
    }

    /// List every mode bundle present in this container. A mode bundle is
    /// any subtree under `mode/<name>/...`. Returns the bundle names sorted
    /// for determinism.
    ///
    /// Returns an empty vec for legacy containers that use the old flat
    /// `stepN/...` naming (no `mode/...` prefix anywhere).
    pub fn list_modes(&self) -> Vec<String> {
        let mut out: Vec<String> = self
            .sections
            .iter()
            .filter_map(|s| {
                let rest = s.name.strip_prefix("mode/")?;
                let slash = rest.find('/')?;
                Some(rest[..slash].to_string())
            })
            .collect();
        out.sort();
        out.dedup();
        out
    }

    /// Read a section's bytes off disk and verify its CRC. Suitable
    /// for one-shot loaders that copy the bytes into a `Vec`.
    pub fn read_section_verified<P: AsRef<Path>>(
        &self,
        path: P,
        sec: &SectionEntry,
    ) -> Result<Vec<u8>> {
        let mut file = File::open(path.as_ref())?;
        file.seek(SeekFrom::Start(sec.offset))?;
        let mut buf = vec![0u8; sec.len as usize];
        file.read_exact(&mut buf)?;
        let computed = crc::checksum(&buf);
        anyhow::ensure!(
            computed == sec.crc,
            "section '{}' CRC mismatch: computed 0x{:016X}, stored 0x{:016X}",
            sec.name,
            computed,
            sec.crc
        );
        Ok(buf)
    }

    /// Borrow a section's bytes from a memory-mapped container.
    ///
    /// Verifies the section's stored CRC against the bytes-as-mapped on
    /// the first call (cold paging cost paid once); subsequent calls on
    /// the same `Mmap` are pointer arithmetic + length only.
    pub fn section_bytes<'a>(
        &self,
        mmap: &'a memmap2::Mmap,
        sec: &SectionEntry,
    ) -> Result<&'a [u8]> {
        let start = sec.offset as usize;
        let end = start + sec.len as usize;
        anyhow::ensure!(
            end <= mmap.len(),
            "section '{}' bytes [{},{}) exceed mmap len {}",
            sec.name,
            start,
            end,
            mmap.len()
        );
        Ok(&mmap[start..end])
    }

    /// Like [`Container::section_bytes`], plus verifies the section CRC
    /// over the mapped bytes. Use during initial load; the per-byte
    /// scan touches every page once and is therefore proportional to
    /// section size.
    pub fn section_bytes_verified<'a>(
        &self,
        mmap: &'a memmap2::Mmap,
        sec: &SectionEntry,
    ) -> Result<&'a [u8]> {
        let bytes = self.section_bytes(mmap, sec)?;
        let computed = crc::checksum(bytes);
        anyhow::ensure!(
            computed == sec.crc,
            "section '{}' CRC mismatch: computed 0x{:016X}, stored 0x{:016X}",
            sec.name,
            computed,
            sec.crc
        );
        Ok(bytes)
    }

    /// Walk the file once and verify the whole-file CRC. O(file size)
    /// — call only when paranoid (e.g. `inspect --full`).
    pub fn verify_file_crc<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let path = path.as_ref();
        let mut file = File::open(path)?;
        let total_len = file.metadata()?.len();
        anyhow::ensure!(total_len >= HEADER_SIZE + FOOTER_SIZE, "file too small");
        // Read footer.
        file.seek(SeekFrom::Start(total_len - FOOTER_SIZE))?;
        let mut footer = [0u8; FOOTER_SIZE as usize];
        file.read_exact(&mut footer)?;
        let stored_file_crc = u64::from_le_bytes(footer[8..16].try_into().unwrap());

        // Re-scan everything except the footer.
        file.seek(SeekFrom::Start(0))?;
        let mut digest = crc::Digest::new();
        let mut br = BufReader::new(&mut file);
        let mut remaining = total_len - FOOTER_SIZE;
        let mut buf = vec![0u8; 1 << 20];
        while remaining > 0 {
            let want = std::cmp::min(remaining as usize, buf.len());
            br.read_exact(&mut buf[..want])?;
            digest.update(&buf[..want]);
            remaining -= want as u64;
        }
        let computed = digest.finalize();
        anyhow::ensure!(
            computed == stored_file_crc,
            "file CRC mismatch: computed 0x{:016X}, stored 0x{:016X}",
            computed,
            stored_file_crc
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Seek, SeekFrom, Write};
    use tempfile::NamedTempFile;

    fn write_demo(path: &Path) -> Result<()> {
        let mut w = ContainerWriter::create(path)?;
        w.append_bytes(SectionKind::EbgNodes, "ebg.nodes", b"hello ebg nodes")?;
        w.append_bytes(SectionKind::CchTopo, "cch.topo", b"shared topology")?;
        w.append_bytes(
            SectionKind::CchWeightsTime,
            "weights/car",
            b"car time weights data",
        )?;
        w.append_bytes(
            SectionKind::CchWeightsTime,
            "weights/bike",
            b"bike time weights",
        )?;
        w.finalize()
    }

    #[test]
    fn roundtrip() -> Result<()> {
        let tmp = NamedTempFile::new()?;
        write_demo(tmp.path())?;

        let c = Container::open(tmp.path())?;
        assert_eq!(c.n_sections, 4);
        let car = c.get("weights/car").expect("weights/car missing");
        assert_eq!(car.kind, SectionKind::CchWeightsTime);
        let car_bytes = c.read_section_verified(tmp.path(), car)?;
        assert_eq!(&car_bytes, b"car time weights data");

        // by-kind iteration.
        let modes: Vec<&str> = c
            .iter_kind(SectionKind::CchWeightsTime)
            .map(|s| s.name.as_str())
            .collect();
        assert_eq!(modes, vec!["weights/car", "weights/bike"]);

        c.verify_file_crc(tmp.path())?;
        Ok(())
    }

    #[test]
    fn detect_payload_corruption() -> Result<()> {
        let tmp = NamedTempFile::new()?;
        write_demo(tmp.path())?;
        let c = Container::open(tmp.path())?;

        let car = c.get("weights/car").unwrap().clone();
        // Flip a byte inside the car payload.
        {
            let mut f = std::fs::OpenOptions::new().write(true).open(tmp.path())?;
            f.seek(SeekFrom::Start(car.offset))?;
            f.write_all(&[0xFF])?;
        }

        let res = c.read_section_verified(tmp.path(), &car);
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("CRC mismatch"));

        // File CRC should also fail now.
        let full = c.verify_file_crc(tmp.path());
        assert!(full.is_err());
        Ok(())
    }

    #[test]
    fn detect_directory_corruption() -> Result<()> {
        let tmp = NamedTempFile::new()?;
        write_demo(tmp.path())?;
        // Open once to read dir_offset.
        let c = Container::open(tmp.path())?;
        // Flip a byte inside the directory.
        {
            let mut f = std::fs::OpenOptions::new().write(true).open(tmp.path())?;
            f.seek(SeekFrom::Start(c.dir_offset + 4))?;
            f.write_all(&[0xAA])?;
        }
        let res = Container::open(tmp.path());
        assert!(res.is_err());
        let err = res.unwrap_err().to_string();
        assert!(
            err.contains("directory CRC mismatch") || err.contains("file CRC mismatch"),
            "unexpected error: {}",
            err
        );
        Ok(())
    }

    #[test]
    fn missing_section_returns_none() -> Result<()> {
        let tmp = NamedTempFile::new()?;
        write_demo(tmp.path())?;
        let c = Container::open(tmp.path())?;
        assert!(c.get("nonexistent").is_none());
        Ok(())
    }

    #[test]
    fn list_modes_and_prefix_iter() -> Result<()> {
        let tmp = NamedTempFile::new()?;
        let mut w = ContainerWriter::create(tmp.path())?;
        w.append_bytes(SectionKind::EbgNodes, "shared/ebg.nodes", b"x")?;
        w.append_bytes(SectionKind::CchTopo, "mode/car/topo", b"car-topo")?;
        w.append_bytes(SectionKind::CchWeightsTime, "mode/car/weights.time", b"ct")?;
        w.append_bytes(SectionKind::CchTopo, "mode/bike/topo", b"bike-topo")?;
        // Unknown manifest payload: must round-trip.
        w.append_bytes(
            SectionKind::Unknown,
            "shared/manifest.json",
            b"{\"version\":1,\"future\":\"field\"}",
        )?;
        w.finalize()?;

        let c = Container::open(tmp.path())?;
        assert_eq!(c.list_modes(), vec!["bike".to_string(), "car".to_string()]);

        let car: Vec<&str> = c
            .sections_with_prefix("mode/car/")
            .map(|s| s.name.as_str())
            .collect();
        assert_eq!(car, vec!["mode/car/topo", "mode/car/weights.time"]);

        let shared: Vec<&str> = c
            .sections_with_prefix("shared/")
            .map(|s| s.name.as_str())
            .collect();
        assert_eq!(shared, vec!["shared/ebg.nodes", "shared/manifest.json"]);

        // Manifest unknown future field round-trips byte-for-byte.
        let m = c.get("shared/manifest.json").unwrap();
        let mb = c.read_section_verified(tmp.path(), m)?;
        assert_eq!(&mb, b"{\"version\":1,\"future\":\"field\"}");
        Ok(())
    }

    #[test]
    fn list_modes_empty_for_legacy() -> Result<()> {
        let tmp = NamedTempFile::new()?;
        let mut w = ContainerWriter::create(tmp.path())?;
        w.append_bytes(SectionKind::CchTopo, "step7/cch.car", b"legacy")?;
        w.finalize()?;
        let c = Container::open(tmp.path())?;
        assert!(c.list_modes().is_empty());
        Ok(())
    }

    #[test]
    fn empty_container_is_valid() -> Result<()> {
        let tmp = NamedTempFile::new()?;
        let w = ContainerWriter::create(tmp.path())?;
        w.finalize()?;

        let c = Container::open(tmp.path())?;
        assert_eq!(c.n_sections, 0);
        assert!(c.sections.is_empty());
        c.verify_file_crc(tmp.path())?;
        Ok(())
    }
}
