//! Type contracts from butterfly-osm#96.
//!
//! These types form the public boundary between the parser (heuristic
//! today, neural in #98 Phase 2) and the executor.
//!
//! ## Zero-Cost-on-Clean-Queries (#96 NFR)
//!
//! When `hypotheses.len() == 1` and `country_candidates.len() == 1`,
//! every operation downstream of parsing (canonicalization, dedup,
//! cost estimation, role-legality clamping) must be O(1) and
//! allocation-free. The contract is tested in
//! [`crate::geocoder::executor`].

use serde::{Deserialize, Serialize};

use crate::geocoder::channels::{Channel, ChannelRole};
use crate::routing::CountryId;

/// Number of evidence channels defined in #96. Used to size dense
/// per-channel arrays so the |hypotheses|==1 path does not heap-allocate.
pub const N_CHANNELS: usize = 6;

/// Strictness of retrieval. `Exact` runs first; broader strictnesses
/// are only attempted if budget allows and the previous attempt
/// returned nothing (#96).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub enum Strictness {
    /// Exact case/diacritic-insensitive string match on canonical fields.
    #[default]
    Exact,
    /// Bounded edit-distance fuzzy match (rapidfuzz, distance ≤ 2).
    Fuzzy,
    /// Place-name fallback / wide search. Last resort.
    Desperate,
}

/// Bitmask over [`Channel`] indicating which fields are reliable
/// enough to use as blockers.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FieldMask(pub u8);

impl FieldMask {
    pub const NONE: Self = Self(0);

    #[must_use]
    pub fn with(self, ch: Channel) -> Self {
        Self(self.0 | (1u8 << ch.index()))
    }

    #[must_use]
    pub fn contains(self, ch: Channel) -> bool {
        (self.0 & (1u8 << ch.index())) != 0
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryFlags {
    pub had_postcode: bool,
    pub had_house_number: bool,
    pub had_locality: bool,
    pub stripped_country_suffix: bool,
}

/// Per-hypothesis retrieval policy: assigns a [`ChannelRole`] to each
/// channel.
///
/// Dense `[Option<ChannelRole>; N_CHANNELS]` layout — register-sized,
/// no heap allocation, canonicalization on a single policy is one
/// table lookup per the NFR.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetrievalPolicy {
    pub roles: [Option<ChannelRole>; N_CHANNELS],
}

impl RetrievalPolicy {
    #[must_use]
    pub fn from_pairs(pairs: &[(Channel, ChannelRole)]) -> Self {
        let mut roles: [Option<ChannelRole>; N_CHANNELS] = [None; N_CHANNELS];
        for &(ch, role) in pairs {
            roles[ch.index()] = Some(role);
        }
        Self { roles }
    }

    /// Belgium default: postcode as blocker, street as reducer,
    /// house-number as scorer, locality as scorer (#96 examples).
    pub fn belgium_default() -> Self {
        Self::from_pairs(&[
            (Channel::Postcode, ChannelRole::Blocker),
            (Channel::Street, ChannelRole::Reducer),
            (Channel::HouseNumber, ChannelRole::Scorer),
            (Channel::Locality, ChannelRole::Scorer),
        ])
    }

    #[must_use]
    pub fn role(&self, ch: Channel) -> Option<ChannelRole> {
        self.roles[ch.index()]
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ParseHypothesis {
    pub street_candidates: Vec<(String, f32)>,
    pub house_candidates: Vec<(String, f32)>,
    pub postcode_candidates: Vec<(String, f32)>,
    pub locality_candidates: Vec<(String, f32)>,
    pub unit_candidates: Vec<(String, f32)>,
    pub field_reliability: FieldMask,
    pub retrieval_policy: RetrievalPolicy,
    pub strictness: Strictness,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ExecutionBudget {
    pub max_countries: u8,
    pub max_hypotheses: u8,
    pub max_fuzzy_expansions: u16,
    pub max_total_candidates: u32,
    pub static_cost_ceiling: f32,
}

impl Default for ExecutionBudget {
    fn default() -> Self {
        Self {
            max_countries: 1,
            max_hypotheses: 1,
            max_fuzzy_expansions: 32,
            max_total_candidates: 50,
            static_cost_ceiling: 1024.0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParsedQuery {
    pub original_text: String,
    pub country_candidates: Vec<(CountryId, f32)>,
    pub hypotheses: Vec<ParseHypothesis>,
    pub global_confidence: f32,
    pub recovery_flags: RecoveryFlags,
    pub execution_budget: ExecutionBudget,
}

impl ParsedQuery {
    /// Detect the clean-query path used by the Zero-Cost-on-Clean-Queries
    /// NFR (#96): exactly one country candidate, exactly one hypothesis.
    #[inline]
    #[must_use]
    pub fn is_clean(&self) -> bool {
        self.country_candidates.len() == 1 && self.hypotheses.len() == 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn field_mask_round_trip() {
        let m = FieldMask::NONE
            .with(Channel::Postcode)
            .with(Channel::Street);
        assert!(m.contains(Channel::Postcode));
        assert!(m.contains(Channel::Street));
        assert!(!m.contains(Channel::HouseNumber));
    }

    #[test]
    fn retrieval_policy_be_default() {
        let p = RetrievalPolicy::belgium_default();
        assert_eq!(p.role(Channel::Postcode), Some(ChannelRole::Blocker));
        assert_eq!(p.role(Channel::Street), Some(ChannelRole::Reducer));
        assert_eq!(p.role(Channel::HouseNumber), Some(ChannelRole::Scorer));
        assert_eq!(p.role(Channel::Locality), Some(ChannelRole::Scorer));
        assert_eq!(p.role(Channel::Alias), None);
    }

    #[test]
    fn clean_query_detection() {
        let q = ParsedQuery {
            original_text: String::from("test"),
            country_candidates: vec![(CountryId::BE, 1.0)],
            hypotheses: vec![ParseHypothesis::default()],
            global_confidence: 1.0,
            recovery_flags: RecoveryFlags::default(),
            execution_budget: ExecutionBudget::default(),
        };
        assert!(q.is_clean());
    }
}
