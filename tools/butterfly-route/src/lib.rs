//! Butterfly-Route: High-performance OSM routing engine
//!
//! Pipeline:
//! - Step 1: PBF Ingest - Convert OSM PBF to immutable, deterministic artifacts
//! - Step 2: Modal Profiling - Per-mode attributes and turn restrictions
//! - Step 3: Node-Based Graph - Mode-agnostic road/ferry topology (build-time intermediate)
//! - Step 4: Edge-Based Graph - Turn-expanded graph with mode masks (THE routing graph)
//! - Step 5: Per-mode weights & masks - Mode-specific traversal costs on EBG
//! - Step 6: CCH ordering on EBG - Nested dissection for contraction
//! - Step 7: CCH contraction - Build hierarchy topology
//! - Step 8: Customization - Apply per-mode weights to shortcuts
//!
//! Key principle: Edge-based graph is the single source of truth for routing.
//! All queries (P2P, matrix, isochrone) use the same EBG-based CCH.

pub mod analysis;
pub mod cli;
pub mod ebg;
pub mod formats;
pub mod hybrid;
pub mod ingest;
pub mod matrix;
pub mod nbg;
pub mod nbg_ch;
pub mod profile;
pub mod profile_abi;
pub mod profiles;
pub mod range;
pub mod step5;
pub mod step6;
pub mod step6_lifted;
pub mod step7;
pub mod step8;
pub mod step9;
pub mod validate;

pub use formats::{RelationsFile, WaysFile};
pub use profile_abi::{Mode, Profile, TurnInput, TurnOutput, TurnRuleKind, WayInput, WayOutput};
pub use profiles::{BikeProfile, CarProfile, FootProfile};
pub use validate::{LockFile, Step2LockFile};
