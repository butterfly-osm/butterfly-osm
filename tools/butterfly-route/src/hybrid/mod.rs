//! Hybrid State Graph for Exact Turn-Aware Routing
//!
//! This module implements a hybrid state representation that dramatically reduces
//! the state count while maintaining exact turn cost semantics.
//!
//! ## The Problem
//!
//! Full edge-based graphs (EBG) create one state per directed edge, resulting in
//! ~2.6x state expansion vs node count. For Belgium:
//! - NBG nodes: 1.9M
//! - EBG nodes: 5.0M (2.6x expansion)
//!
//! This causes proportional slowdown in all graph algorithms.
//!
//! ## The Solution
//!
//! Only 0.3% of intersections have turn restrictions that depend on the incoming edge.
//! For the other 99.7%, we can collapse all incoming edge-states to a single node-state.
//!
//! ### State Types
//!
//! - **Node-state**: Represents "at NBG node v" (for simple nodes with no turn restrictions)
//! - **Edge-state**: Represents "arrived at NBG node v via edge u→v" (for complex nodes)
//!
//! ### Transitions
//!
//! From any state at node u to node v:
//! - If v is simple → transition to node-state(v)
//! - If v is complex → transition to edge-state(u→v)
//!
//! Turn restrictions are only checked when leaving a complex node (edge-state).
//!
//! ## Expected Impact
//!
//! - State count: 5.0M → ~1.9M (2.6x reduction)
//! - Edge count: proportional reduction
//! - Query performance: 2-3x faster (matching OSRM)

pub mod state_graph;
pub mod builder;
pub mod equivalence;
pub mod equiv_builder;

pub use state_graph::{HybridStateGraph, HybridState, HybridArc};
pub use builder::HybridGraphBuilder;
pub use equivalence::{analyze_equivalence_classes, EquivalenceAnalysis};
pub use equiv_builder::EquivHybridBuilder;
