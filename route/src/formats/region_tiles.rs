//! `shared/region_tiles` — coarse tile coverage set per region (#142).
//!
//! Stored as a sorted `Vec<u64>` of packed `(lat_cell, lon_cell)` tile
//! ids at a fixed 0.1° resolution. The set lists every coarse tile
//! that contains at least one `shared/snap_points` sample — i.e. every
//! tile the region's road network touches.
//!
//! # Use case
//!
//! Multi-region serve's `snap_winner` (after #292 Phase 4's bbox
//! pre-filter) needs a tighter "could this region contain the query
//! coord?" check. Bbox is conservative: BE's bbox fully contains LU,
//! so a query inside LU still passes BE's bbox check and forces BE's
//! lazy load if BE is `Pending`. Tile coverage is *tile-tight* —
//! adjacent countries false-positive only along their literal shared
//! border, which is the correct semantic ("a query right on the border
//! might plausibly snap into either region's road network").
//!
//! # Resolution
//!
//! `CELL_SIZE_E7 = 1_000_000` = 0.1° per tile. At 50°N latitude this
//! is ~11 km × ~7 km. Belgium's road network covers ~700 such tiles
//! (~5.5 KiB), Luxembourg ~50 (~0.4 KiB). For planet-scale containers
//! (~1.2M tiles ≈ 10 MiB), this stays under any reasonable budget.
//!
//! # On-disk layout
//!
//! ```text
//!   [u8;4]   MAGIC ("RGTL")
//!   u16      VERSION (1)
//!   u16      _pad
//!   u32      n_tiles
//!   [u8;28]  _pad (header pads to 40 B for u64 alignment)
//!   body:    [u64; n_tiles]  sorted ascending
//!   [u64;2]  footer: body_crc || file_crc
//! ```

use anyhow::Result;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::Arc;

use super::crc::Digest;
use super::mmap::ArcCow;

pub const REGION_TILES_MAGIC: u32 = 0x5247_544C; // "RGTL"
pub const REGION_TILES_VERSION: u16 = 1;
pub const HEADER_SIZE: usize = 40;
pub const FOOTER_SIZE: usize = 16;

/// 0.1° in i32-e7 fixed point. ~11 km × ~7 km tile at 50°N.
pub const CELL_SIZE_E7: i32 = 1_000_000;

const LAT_OFFSET_E7: i32 = 90 * 10_000_000;
const LON_OFFSET_E7: i32 = 180 * 10_000_000;

/// Pack a fixed-point lat/lon into a tile id.
#[inline]
pub fn tile_id_from_e7(lon_e7: i32, lat_e7: i32) -> u64 {
    let lon_cell = ((lon_e7 as i64 + LON_OFFSET_E7 as i64) / CELL_SIZE_E7 as i64) as u32;
    let lat_cell = ((lat_e7 as i64 + LAT_OFFSET_E7 as i64) / CELL_SIZE_E7 as i64) as u32;
    ((lat_cell as u64) << 32) | (lon_cell as u64)
}

/// Pack a float lat/lon into a tile id. Convenience for the runtime
/// query path (`snap_winner` has lat/lon as f64).
#[inline]
pub fn tile_id_from_f64(lon: f64, lat: f64) -> u64 {
    tile_id_from_e7((lon * 1e7) as i32, (lat * 1e7) as i32)
}

/// Parsed `shared/region_tiles` section.
#[derive(Debug, Clone)]
pub struct RegionTiles {
    pub n_tiles: u32,
    /// Sorted ascending. Binary-searchable.
    pub tiles: ArcCow<u64>,
}

impl RegionTiles {
    /// Query whether `(lon, lat)` lies in any covered tile, optionally
    /// expanded by `margin_tiles` neighbour rings on each side.
    /// `margin_tiles=0` checks exact tile membership; `margin_tiles=1`
    /// also accepts the 8 neighbouring tiles (3x3 block).
    ///
    /// `snap_winner` uses `margin_tiles=1` so a query near a tile edge
    /// that snaps into a neighbour's road segment still considers this
    /// region.
    pub fn contains_with_margin(&self, lon: f64, lat: f64, margin_tiles: i32) -> bool {
        let lon_e7 = (lon * 1e7) as i32;
        let lat_e7 = (lat * 1e7) as i32;
        let center_lon_cell =
            ((lon_e7 as i64 + LON_OFFSET_E7 as i64) / CELL_SIZE_E7 as i64) as i32;
        let center_lat_cell =
            ((lat_e7 as i64 + LAT_OFFSET_E7 as i64) / CELL_SIZE_E7 as i64) as i32;
        let slice = self.tiles.as_slice();
        for dy in -margin_tiles..=margin_tiles {
            for dx in -margin_tiles..=margin_tiles {
                let lon_cell = center_lon_cell + dx;
                let lat_cell = center_lat_cell + dy;
                if lon_cell < 0 || lat_cell < 0 {
                    continue;
                }
                let id = ((lat_cell as u64) << 32) | (lon_cell as u64);
                if slice.binary_search(&id).is_ok() {
                    return true;
                }
            }
        }
        false
    }

    pub fn len(&self) -> usize {
        self.n_tiles as usize
    }

    pub fn is_empty(&self) -> bool {
        self.n_tiles == 0
    }
}

pub struct RegionTilesFile;

impl RegionTilesFile {
    /// Encode header || sorted body || footer.
    pub fn encode(rt: &[u64]) -> Vec<u8> {
        debug_assert!(
            rt.windows(2).all(|w| w[0] <= w[1]),
            "RegionTilesFile::encode: input must be sorted ascending"
        );
        let n = rt.len();
        let body_len = n * std::mem::size_of::<u64>();
        let mut out = Vec::with_capacity(HEADER_SIZE + body_len + FOOTER_SIZE);
        out.extend_from_slice(&REGION_TILES_MAGIC.to_le_bytes());
        out.extend_from_slice(&REGION_TILES_VERSION.to_le_bytes());
        out.extend_from_slice(&0u16.to_le_bytes());
        out.extend_from_slice(&(n as u32).to_le_bytes());
        out.extend_from_slice(&[0u8; HEADER_SIZE - 12]);
        debug_assert_eq!(out.len(), HEADER_SIZE);
        out.extend_from_slice(bytemuck::cast_slice(rt));
        let body_crc = {
            let mut d = Digest::new();
            d.update(&out[HEADER_SIZE..HEADER_SIZE + body_len]);
            d.finalize()
        };
        let file_crc = {
            let mut d = Digest::new();
            d.update(&out[..HEADER_SIZE + body_len]);
            d.finalize()
        };
        out.extend_from_slice(&body_crc.to_le_bytes());
        out.extend_from_slice(&file_crc.to_le_bytes());
        out
    }

    pub fn write<P: AsRef<Path>>(path: P, tiles: &[u64]) -> Result<()> {
        let bytes = Self::encode(tiles);
        let mut w = BufWriter::new(File::create(path.as_ref())?);
        w.write_all(&bytes)?;
        w.flush()?;
        Ok(())
    }

    /// Owning reader: copies body into Vec<u64>.
    pub fn read_from_bytes(bytes: &[u8]) -> Result<RegionTiles> {
        let (n_tiles, body) = parse_header(bytes)?;
        let tiles: &[u64] = bytemuck::cast_slice(body);
        let mut body_d = Digest::new();
        body_d.update(body);
        let computed_body = body_d.finalize();
        let footer_off = HEADER_SIZE + body.len();
        let stored_body = u64::from_le_bytes(bytes[footer_off..footer_off + 8].try_into()?);
        anyhow::ensure!(
            computed_body == stored_body,
            "region_tiles body CRC mismatch: computed 0x{:016X}, stored 0x{:016X}",
            computed_body,
            stored_body
        );
        Ok(RegionTiles {
            n_tiles,
            tiles: ArcCow::from_vec(tiles.to_vec()),
        })
    }

    /// Production mmap-backed reader. Holds an `Arc<Mmap>` clone in the
    /// returned struct via `ArcCow::Mmap` so the mapping stays alive as
    /// long as the struct does.
    pub fn read_from_mmap_unverified(
        mmap: Arc<memmap2::Mmap>,
        byte_offset: usize,
        byte_len: usize,
    ) -> Result<RegionTiles> {
        anyhow::ensure!(
            byte_offset.saturating_add(byte_len) <= mmap.len(),
            "region_tiles section out of bounds: off={byte_offset} len={byte_len} mmap_len={}",
            mmap.len()
        );
        let n_tiles = {
            let bytes = &mmap[byte_offset..byte_offset + byte_len];
            parse_header(bytes)?.0
        };
        let body_byte_offset = byte_offset + HEADER_SIZE;
        let tiles = ArcCow::<u64>::from_mmap(mmap, body_byte_offset, n_tiles as usize)?;
        Ok(RegionTiles { n_tiles, tiles })
    }
}

fn parse_header(bytes: &[u8]) -> Result<(u32, &[u8])> {
    anyhow::ensure!(
        bytes.len() >= HEADER_SIZE + FOOTER_SIZE,
        "region_tiles too short: {} bytes",
        bytes.len()
    );
    let magic = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    anyhow::ensure!(
        magic == REGION_TILES_MAGIC,
        "Invalid magic in region_tiles: expected 0x{:08X}, got 0x{:08X}",
        REGION_TILES_MAGIC,
        magic
    );
    let version = u16::from_le_bytes([bytes[4], bytes[5]]);
    anyhow::ensure!(
        version == REGION_TILES_VERSION,
        "Unsupported region_tiles version {version}, expected {REGION_TILES_VERSION}",
    );
    let n_tiles = u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]);
    let expected_body = (n_tiles as usize) * std::mem::size_of::<u64>();
    anyhow::ensure!(
        bytes.len() == HEADER_SIZE + expected_body + FOOTER_SIZE,
        "region_tiles length mismatch: declared body {}, total {}",
        expected_body,
        bytes.len()
    );
    let body = &bytes[HEADER_SIZE..HEADER_SIZE + expected_body];
    Ok((n_tiles, body))
}

/// Build the sorted tile-id set from a slice of (lon_e7, lat_e7) sample
/// pairs. Used by the pack tool — call once per region after `snap_points`
/// is built.
pub fn build_from_snap_points<I: IntoIterator<Item = (i32, i32)>>(samples: I) -> Vec<u64> {
    let mut tiles: Vec<u64> = samples
        .into_iter()
        .map(|(lon_e7, lat_e7)| tile_id_from_e7(lon_e7, lat_e7))
        .collect();
    tiles.sort_unstable();
    tiles.dedup();
    tiles
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn tile_id_packing_brussels() {
        let id = tile_id_from_f64(4.35, 50.85);
        let lat_cell = (id >> 32) as u32;
        let lon_cell = (id & 0xFFFF_FFFF) as u32;
        assert_eq!(lat_cell, 1408);
        assert_eq!(lon_cell, 1843);
    }

    #[test]
    fn build_from_samples_sorts_and_dedups() {
        let samples = vec![
            (43_500_000, 508_500_000),
            (43_500_000, 508_500_000),
            (61_300_000, 496_100_000),
            (40_000_000, 510_000_000),
        ];
        let tiles = build_from_snap_points(samples);
        assert_eq!(tiles.len(), 3);
        assert!(tiles.windows(2).all(|w| w[0] < w[1]));
    }

    #[test]
    fn encode_decode_roundtrip() -> Result<()> {
        let tiles: Vec<u64> = (0..100).map(|i| i as u64 * 17).collect();
        let bytes = RegionTilesFile::encode(&tiles);
        let parsed = RegionTilesFile::read_from_bytes(&bytes)?;
        assert_eq!(parsed.n_tiles as usize, tiles.len());
        assert_eq!(parsed.tiles.as_slice(), tiles.as_slice());
        Ok(())
    }

    #[test]
    fn mmap_reader_zero_copy() -> Result<()> {
        let tiles: Vec<u64> = (0..50).map(|i| i as u64 * 31).collect();
        let bytes = RegionTilesFile::encode(&tiles);
        let tmp = NamedTempFile::new()?;
        std::fs::write(tmp.path(), &bytes)?;
        let mmap = super::super::mmap::map_readonly(tmp.path())?;
        let parsed = RegionTilesFile::read_from_mmap_unverified(mmap, 0, bytes.len())?;
        assert_eq!(parsed.tiles.as_slice(), tiles.as_slice());
        Ok(())
    }

    #[test]
    fn contains_with_margin_exact_and_neighbour() {
        let center = tile_id_from_f64(4.35, 50.85);
        let rt = RegionTiles {
            n_tiles: 1,
            tiles: ArcCow::from_vec(vec![center]),
        };
        assert!(rt.contains_with_margin(4.35, 50.85, 0));
        assert!(!rt.contains_with_margin(4.45, 50.85, 0));
        assert!(rt.contains_with_margin(4.45, 50.85, 1));
        assert!(!rt.contains_with_margin(5.0, 50.85, 1));
    }

    #[test]
    fn bad_magic_rejected() {
        let bytes = vec![0u8; HEADER_SIZE + FOOTER_SIZE];
        let err = RegionTilesFile::read_from_bytes(&bytes).unwrap_err();
        assert!(err.to_string().contains("magic"));
    }
}
