//! Profile ABI - Stable C-compatible interface for routing profiles
//!
//! This module defines the ABI between the routing engine and mode-specific profiles.
//! Modes are discovered at runtime from `*.model.json` files — no hardcoded mode names.

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct WayInput<'a> {
    /// Tag key IDs from ways.raw key dictionary
    pub kv_keys: &'a [u32],
    /// Tag value IDs from ways.raw value dictionary (parallel to kv_keys)
    pub kv_vals: &'a [u32],
    /// Optional key dictionary for string lookup
    pub key_dict: Option<&'a std::collections::HashMap<u32, String>>,
    /// Optional value dictionary for string lookup
    pub val_dict: Option<&'a std::collections::HashMap<u32, String>>,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct WayOutput {
    /// Can traverse forward (along way direction)
    pub access_fwd: bool,
    /// Can traverse reverse (against way direction)
    pub access_rev: bool,
    /// Oneway constraint: 0=no, 1=fwd, 2=rev, 3=both (rare)
    pub oneway: u8,
    /// Base speed in mm/s (integer)
    pub base_speed_mmps: u32,
    /// Surface class enum index (0 if unknown)
    pub surface_class: u16,
    /// Highway class enum index (required)
    pub highway_class: u16,
    /// Feature bit flags (toll, tunnel, bridge, ferry, etc.)
    pub class_bits: u32,
    /// Extra penalty per km in deciseconds (preference shaping)
    pub per_km_penalty_ds: u16,
    /// Constant penalty per edge entry in deciseconds
    pub const_penalty_ds: u32,
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct TurnInput<'a> {
    /// Relation tag key IDs from relations.raw key dictionary
    pub tags_keys: &'a [u32],
    /// Relation tag value IDs from relations.raw value dictionary
    pub tags_vals: &'a [u32],
    /// Optional key dictionary for string lookup
    pub key_dict: Option<&'a std::collections::HashMap<u32, String>>,
    /// Optional value dictionary for string lookup
    pub val_dict: Option<&'a std::collections::HashMap<u32, String>>,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TurnRuleKind {
    None = 0,
    Ban = 1,
    Only = 2,
    Penalty = 3,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TurnOutput {
    /// Kind of turn rule
    pub kind: TurnRuleKind,
    /// Bitmask of modes this applies to
    pub applies: u8,
    /// Bitmask of exceptions (same encoding as applies)
    pub except_mask: u8,
    /// Penalty in deciseconds (only for Penalty kind)
    pub penalty_ds: u32,
    /// True if conditional restrictions present
    pub is_time_dependent: bool,
}

impl Default for TurnOutput {
    fn default() -> Self {
        Self {
            kind: TurnRuleKind::None,
            applies: 0,
            except_mask: 0,
            penalty_ds: 0,
            is_time_dependent: false,
        }
    }
}

/// Maximum number of modes supported (hard limit for penalty arrays in turn table).
pub const MAX_MODES: usize = 8;

/// Dynamic mode identifier — a wrapper around the mode's alphabetical index (0..MAX_MODES-1).
///
/// Modes are discovered at pipeline time from `*.model.json` files, sorted alphabetically.
/// Mode is just an index — it does NOT carry its name. Use external lookup for names
/// (e.g., `state.mode_names[mode.index()]` in the server, or `modes[i].name` in the pipeline).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Mode(pub u8);

impl Mode {
    /// Bitmask for this mode: `1 << index`
    #[inline]
    pub fn bit(&self) -> u8 {
        1u8 << self.0
    }

    /// Index into arrays (0..MAX_MODES-1)
    #[inline]
    pub fn index(&self) -> usize {
        self.0 as usize
    }

    /// Raw u8 value
    #[inline]
    pub fn as_u8(&self) -> u8 {
        self.0
    }

    /// Construct from raw u8
    #[inline]
    pub fn from_u8(v: u8) -> Self {
        Mode(v)
    }

    /// Discover modes from a pipeline output directory by scanning for known file patterns.
    /// Tries `way_attrs.*.bin` (step2), then `w.*.u32` (step5), then `filtered.*.ebg` (step5).
    /// Returns (mode_name, mode_index) pairs sorted alphabetically.
    pub fn discover_from_dir(dir: &std::path::Path) -> Vec<(String, u8)> {
        let patterns: &[(&str, &str)] = &[
            ("way_attrs.", ".bin"),
            ("w.", ".u32"),
            ("filtered.", ".ebg"),
        ];

        for &(prefix, suffix) in patterns {
            let mut names: Vec<String> = Vec::new();
            if let Ok(entries) = std::fs::read_dir(dir) {
                for entry in entries.flatten() {
                    let fname = entry.file_name();
                    let fname_str = fname.to_string_lossy();
                    if let Some(rest) = fname_str.strip_prefix(prefix)
                        && let Some(mode_name) = rest.strip_suffix(suffix) {
                            names.push(mode_name.to_string());
                        }
                }
            }
            if !names.is_empty() {
                names.sort();
                return names
                    .into_iter()
                    .enumerate()
                    .map(|(idx, name)| (name, idx as u8))
                    .collect();
            }
        }

        Vec::new()
    }
}

/// Class bit positions for way features
pub mod class_bits {
    pub const ACCESS_FWD: u32 = 0;
    pub const ACCESS_REV: u32 = 1;
    pub const ONEWAY_SHIFT: u32 = 2; // bits 2-3 encode oneway
    pub const TOLL: u32 = 4;
    pub const FERRY: u32 = 5;
    pub const TUNNEL: u32 = 6;
    pub const BRIDGE: u32 = 7;
    pub const LINK: u32 = 8;
    pub const RESIDENTIAL: u32 = 9;
    pub const TRACK: u32 = 10;
    pub const CYCLEWAY: u32 = 11;
    pub const FOOTWAY: u32 = 12;
    pub const LIVING_STREET: u32 = 13;
    pub const SERVICE: u32 = 14;
    pub const CONSTRUCTION: u32 = 15;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_way_output_default() {
        let output = WayOutput::default();
        assert!(!output.access_fwd);
        assert!(!output.access_rev);
        assert_eq!(output.base_speed_mmps, 0);
    }

    #[test]
    fn test_turn_output_default() {
        let output = TurnOutput::default();
        assert_eq!(output.kind, TurnRuleKind::None);
        assert_eq!(output.applies, 0);
    }

    #[test]
    fn test_mode_struct() {
        let m = Mode(1);
        assert_eq!(m.index(), 1);
        assert_eq!(m.bit(), 0b10);
        assert_eq!(m.as_u8(), 1);
        assert_eq!(Mode::from_u8(3), Mode(3));
    }

    #[test]
    fn test_mode_bit_mask() {
        assert_eq!(Mode(0).bit(), 1);
        assert_eq!(Mode(1).bit(), 2);
        assert_eq!(Mode(2).bit(), 4);
        assert_eq!(Mode(7).bit(), 128);
    }

    #[test]
    fn test_discover_from_dir_empty() {
        let tmp = tempfile::TempDir::new().unwrap();
        let modes = Mode::discover_from_dir(tmp.path());
        assert!(modes.is_empty());
    }
}
