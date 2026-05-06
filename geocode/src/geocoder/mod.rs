//! Geocoder: post-libpostal recall + rerank pipeline (#205).
//!
//! Two retrieval steps, no parse intermediate:
//!
//! 1. [`recall`] — FST descent over per-country recall indexes.
//!    Cheap deterministic priors (postcode regex, country posterior,
//!    script detection) gate which country FSTs are visited; tagger
//!    BIO logits weight prefix expansion.
//! 2. [`rerank`] — GBDT scoring over recall candidates. Trained on
//!    perturbed OA gold + OSM-derived synthetic queries + bench
//!    query mix.
//!
//! [`executor`] is a thin orchestrator that calls recall then rerank
//! and returns ranked results; it carries no logic of its own.

pub mod executor;
pub mod recall;
pub mod rerank;
