use anyhow::Result;
use clap::Parser;

use butterfly_route::cli::{Cli, Commands};

// #400 — lean-at-rest: use jemalloc as the global allocator. glibc's
// default arena keeps freed allocations in process, so the idle
// compactor's `drop(SearchState)` doesn't actually return RAM to the
// OS (VmRSS stays put even after the Vec body is freed). jemalloc
// aggressively unmaps cold heap regions via `madvise(DONTNEED)`,
// which is precisely the behaviour the lean-at-rest path needs.
//
// Picked over mimalloc for the longer production track record on
// long-running multi-thread Rust servers (Firefox, Redis, TiKV,
// ripgrep, Rustup all use jemalloc). The `#[global_allocator]`
// declaration is plain safe Rust; the unsafe FFI lives inside the
// `tikv-jemallocator` crate, behind the same encapsulation as
// `libc::madvise` and `bytemuck`.
//
// Set for ALL binaries built from this crate (serve, step CLIs,
// bench). Pipeline steps benefit incidentally: large transient
// allocations (e.g. step8 customise) return to the OS instead of
// staying pinned in arena.
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

// Note: jemalloc's default purge cadence is ~10s for dirty pages.
// That means RSS after the idle compactor fires takes ~10s to drop —
// acceptable for a "lean at rest" tool. Operators who want faster
// purging can set the standard jemalloc env var, e.g.:
//   MALLOC_CONF=background_thread:true,dirty_decay_ms:1000,muzzy_decay_ms:1000
// We don't bake this into the binary because the `malloc_conf` symbol
// requires `#[unsafe(no_mangle)]`, and the project rule forbids any
// unsafe attribute in OUR code (jemalloc's internal unsafe is fine —
// it lives inside `tikv-jemallocator`, like `libc`).

fn main() -> Result<()> {
    let cli = Cli::parse();

    // Use env_logger for pipeline steps (1-8), tracing is initialized
    // inside the Serve handler via init_tracing()
    if !matches!(cli.command, Commands::Serve { .. }) {
        env_logger::init();
    }

    cli.run()?;

    Ok(())
}
