//! Butterfly-Route: High-performance OSM routing engine
//!
//! Step 1: PBF Ingest - Convert OSM PBF to immutable, deterministic artifacts
//! Step 2: Modal Profiling - Per-mode attributes and turn restrictions
//! Step 3: Node-Based Graph - Mode-agnostic road/ferry topology

pub mod formats;
pub mod ingest;
pub mod validate;
pub mod cli;
pub mod profile_abi;
pub mod profiles;
pub mod profile;
pub mod nbg;

pub use formats::{WaysFile, RelationsFile};
pub use validate::{LockFile, Step2LockFile};
pub use profile_abi::{Mode, WayInput, WayOutput, TurnInput, TurnOutput, TurnRuleKind, Profile};
pub use profiles::{CarProfile, BikeProfile, FootProfile};
