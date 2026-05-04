//! Evidence channels and channel roles (#96 §Channel Roles, §Retrieval
//! Execution Model).
//!
//! Per #96, **role is a property of the (hypothesis, channel) pair,
//! not of the channel itself**. The same channel can be a blocker
//! for one hypothesis and a scorer for another. This module defines
//! the flat set of channels and the role enum; the (hypothesis,
//! channel) → role mapping lives on [`crate::types::RetrievalPolicy`].

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum Channel {
    Postcode,
    Locality,
    Street,
    HouseNumber,
    Alias,
    Transliteration,
}

impl Channel {
    #[inline]
    #[must_use]
    pub const fn index(self) -> usize {
        match self {
            Channel::Postcode => 0,
            Channel::Locality => 1,
            Channel::Street => 2,
            Channel::HouseNumber => 3,
            Channel::Alias => 4,
            Channel::Transliteration => 5,
        }
    }

    #[must_use]
    pub const fn all() -> [Channel; 6] {
        [
            Channel::Postcode,
            Channel::Locality,
            Channel::Street,
            Channel::HouseNumber,
            Channel::Alias,
            Channel::Transliteration,
        ]
    }
}

/// Role assigned to a (hypothesis, channel) pair (#96 §Channel Roles).
///
/// Ordered weakest → strongest. The order matters for role-smoothness
/// fall-back (#96 §Role-Smoothness Guarantee): if a blocker fails or
/// is oversized, the executor downgrades it to the next weaker role,
/// never the other way.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ChannelRole {
    Scorer,
    Reducer,
    Blocker,
}

impl ChannelRole {
    #[must_use]
    pub fn weaker(self) -> Option<ChannelRole> {
        match self {
            ChannelRole::Blocker => Some(ChannelRole::Reducer),
            ChannelRole::Reducer => Some(ChannelRole::Scorer),
            ChannelRole::Scorer => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn channel_indices_are_stable() {
        assert_eq!(Channel::Postcode.index(), 0);
        assert_eq!(Channel::Locality.index(), 1);
        assert_eq!(Channel::Street.index(), 2);
        assert_eq!(Channel::HouseNumber.index(), 3);
        assert_eq!(Channel::Alias.index(), 4);
        assert_eq!(Channel::Transliteration.index(), 5);
    }

    #[test]
    fn role_weakening_chain() {
        assert_eq!(ChannelRole::Blocker.weaker(), Some(ChannelRole::Reducer));
        assert_eq!(ChannelRole::Reducer.weaker(), Some(ChannelRole::Scorer));
        assert_eq!(ChannelRole::Scorer.weaker(), None);
    }

    #[test]
    fn role_ordering_is_weak_to_strong() {
        assert!(ChannelRole::Scorer < ChannelRole::Reducer);
        assert!(ChannelRole::Reducer < ChannelRole::Blocker);
    }
}
