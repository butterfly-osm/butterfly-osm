///! Profile ABI - Stable C-compatible interface for routing profiles
///!
///! This module defines the ABI between the routing engine and mode-specific profiles.
///! Profiles can be compiled as cdylib or WASM with the same interface.

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct WayInput<'a> {
    /// Tag key IDs from ways.raw key dictionary
    pub kv_keys: &'a [u32],
    /// Tag value IDs from ways.raw value dictionary (parallel to kv_keys)
    pub kv_vals: &'a [u32],
}

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq)]
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

impl Default for WayOutput {
    fn default() -> Self {
        Self {
            access_fwd: false,
            access_rev: false,
            oneway: 0,
            base_speed_mmps: 0,
            surface_class: 0,
            highway_class: 0,
            class_bits: 0,
            per_km_penalty_ds: 0,
            const_penalty_ds: 0,
        }
    }
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct TurnInput<'a> {
    /// Relation tag key IDs from relations.raw key dictionary
    pub tags_keys: &'a [u32],
    /// Relation tag value IDs from relations.raw value dictionary
    pub tags_vals: &'a [u32],
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
    /// Bitmask of modes this applies to: bit0=car, bit1=bike, bit2=foot
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

/// Mode enumeration matching file formats
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Mode {
    Car = 0,
    Bike = 1,
    Foot = 2,
}

impl Mode {
    pub fn all() -> &'static [Mode] {
        &[Mode::Car, Mode::Bike, Mode::Foot]
    }

    pub fn name(&self) -> &'static str {
        match self {
            Mode::Car => "car",
            Mode::Bike => "bike",
            Mode::Foot => "foot",
        }
    }

    pub fn from_u8(v: u8) -> Option<Mode> {
        match v {
            0 => Some(Mode::Car),
            1 => Some(Mode::Bike),
            2 => Some(Mode::Foot),
            _ => None,
        }
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

/// Profile trait that profiles must implement
pub trait Profile {
    /// Get profile version (increment on ABI changes)
    fn version() -> u32;

    /// Process a way and return mode-specific attributes
    fn process_way(input: WayInput) -> WayOutput;

    /// Process a turn restriction relation
    fn process_turn(input: TurnInput) -> TurnOutput;
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
    fn test_mode_names() {
        assert_eq!(Mode::Car.name(), "car");
        assert_eq!(Mode::Bike.name(), "bike");
        assert_eq!(Mode::Foot.name(), "foot");
    }

    #[test]
    fn test_mode_from_u8() {
        assert_eq!(Mode::from_u8(0), Some(Mode::Car));
        assert_eq!(Mode::from_u8(1), Some(Mode::Bike));
        assert_eq!(Mode::from_u8(2), Some(Mode::Foot));
        assert_eq!(Mode::from_u8(3), None);
    }
}
