//! Shared server state.

use std::time::Instant;

use crate::shard::reader::Shard;

#[derive(Debug)]
pub struct ServerState {
    pub shard: Shard,
    pub started_at: Instant,
    pub version: &'static str,
}

impl ServerState {
    pub fn new(shard: Shard) -> Self {
        Self {
            shard,
            started_at: Instant::now(),
            version: env!("CARGO_PKG_VERSION"),
        }
    }
}
