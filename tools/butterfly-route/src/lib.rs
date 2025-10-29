//! Butterfly-Route: High-performance OSM routing engine
//!
//! Step 1: PBF Ingest - Convert OSM PBF to immutable, deterministic artifacts
//! Step 2: Modal Profiling - Per-mode attributes and turn restrictions
//! Step 3: Node-Based Graph - Mode-agnostic road/ferry topology
//! Step 4: Edge-Based Graph - Turn-expanded graph with mode masks
//! Step 5: Per-mode weights & masks - Mode-specific traversal costs

pub mod formats;
pub mod ingest;
pub mod validate;
pub mod cli;
pub mod profile_abi;
pub mod profiles;
pub mod profile;
pub mod nbg;
pub mod ebg;
pub mod step5;

pub use formats::{WaysFile, RelationsFile};
pub use validate::{LockFile, Step2LockFile};
pub use profile_abi::{Mode, WayInput, WayOutput, TurnInput, TurnOutput, TurnRuleKind, Profile};
pub use profiles::{CarProfile, BikeProfile, FootProfile};
