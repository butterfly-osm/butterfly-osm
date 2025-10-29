///! Binary file formats for Step 1 output

pub mod crc;

// New sorted array + sparse index format (replaces sparse bitmap)
pub mod nodes_sa;
pub mod nodes_si;

// Legacy format (deprecated - uses inefficient sparse bitmap)
#[deprecated(note = "Use nodes_sa + nodes_si instead - sparse bitmap wastes space")]
pub mod nodes;

pub mod ways;
pub mod relations;

// Legacy exports (deprecated)
#[deprecated(note = "Use nodes_sa + nodes_si modules directly")]
pub use nodes::NodesFile;

pub use ways::{Way, WaysFile};
pub use relations::{Member, MemberKind, Relation, RelationsFile};
