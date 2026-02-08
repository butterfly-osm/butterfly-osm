//! Analysis utilities for understanding turn models and junction behavior
//!
//! This module provides tools to analyze when turns matter and how many
//! junctions need special handling for exact turn semantics.

pub mod turn_model;

pub use turn_model::{analyze_turn_model, TurnModelAnalysis};
