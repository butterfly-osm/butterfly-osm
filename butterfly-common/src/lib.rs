//! Common utilities for the butterfly-osm toolkit

pub mod error;

pub use error::{Error, Result};

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        // Basic smoke test to ensure the library compiles
        let _result = 2 + 2;
        assert_eq!(_result, 4);
    }
}