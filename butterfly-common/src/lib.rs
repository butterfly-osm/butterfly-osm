//! Common utilities for the butterfly-osm toolkit

pub mod error;

pub use error::{Error, Result};

#[cfg(test)]
mod tests {
    use crate::error::suggest_correction;

    #[test]
    fn suggest_correction_returns_expected_country() {
        assert_eq!(
            suggest_correction("belgum"),
            Some("europe/belgium".to_string())
        );
    }
}
