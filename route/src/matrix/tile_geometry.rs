//! L3-aware tile geometry for monolithic matrix queries (#190)
//!
//! ## Problem
//!
//! `table_bucket_parallel` driven with a monolithic 10k×10k shape sits at the
//! DRAM-bandwidth floor (~33s on Belgium, 20-core box). The forward phase
//! produces ~10k bucket-arrays' worth of state, and the backward phase walks
//! a single huge `PrefixSumBuckets` whose hot working set is several
//! hundred megabytes — every relax in every backward worker pulls bucket
//! entries from DRAM.
//!
//! Production `/table/stream` already tiles the request to 1000×1000
//! sub-matrices in `server/table.rs` and stays L3-resident, hitting
//! ~24s end-to-end. The bench, however, drives the algorithm directly
//! at the monolithic shape — that's the gap this module closes.
//!
//! ## Strategy
//!
//! 1. Detect L2/L3 cache size at startup from
//!    `/sys/devices/system/cpu/cpu0/cache/index*` — these files are
//!    populated by the kernel from CPUID/ACPI on every Linux box.
//! 2. Detect NUMA topology from `/sys/devices/system/node/node*` —
//!    on single-socket boxes (the development host, most cloud VMs)
//!    NUMA pinning is a no-op so we skip the dependency on `hwloc` /
//!    `libnuma`. On multi-socket we currently document the
//!    deferral; pinning lands in a follow-up if real-world machines
//!    show contention.
//! 3. Pick a tile size such that the per-tile bucket working set
//!    (forward bucket items + per-tile result matrix slice) fits in
//!    a budget derived from per-core L2 — that way each rayon worker
//!    operates on its own L2-resident slab during the backward phase.
//!
//! ## Working-set accounting (per source tile of `S` sources)
//!
//! Forward phase produces `S × avg_visited` bucket items, each
//! 12 bytes. Backward phase reads from `PrefixSumBuckets` whose
//! total memory is ~`S × avg_visited × 8` bytes (SoA layout: u32
//! source_idx + u32 dist).
//!
//! On Belgium: `avg_visited ≈ n_nodes/400 ≈ 6000` for source tile
//! `S = 1000` → bucket items ~96 MB, SoA ~48 MB. Way bigger than
//! per-core L2 (3 MB on the dev host). The win comes from the
//! _backward_ phase: each backward worker reuses the same buckets
//! across many target searches, and shrinking `S` shrinks the
//! per-target bucket-walk cost roughly linearly.
//!
//! Heuristic: target `S` such that `S × avg_visited × 8 bytes` fits
//! in 4× shared L3, so multiple workers can share the working set
//! without thrashing. On a 30 MB L3 with 6000 avg_visited: target
//! `S ≈ 30 MB × 4 / (6000 × 8) ≈ 2500`. Floor at 1000 (production
//! default), ceiling at 4000 (bench-friendly), step in 500.

use std::sync::OnceLock;

/// Detected machine topology. Cached after first call.
#[derive(Debug, Clone, Copy)]
pub struct CacheTopology {
    /// Per-core L2 cache size in bytes (`index2` Unified, `shared_cpu_list` is one CPU).
    /// Falls back to 256 KiB (conservative x86_64 default) if detection fails.
    pub per_core_l2_bytes: usize,
    /// Shared L3 cache size in bytes (`index3` Unified). Falls back to 8 MiB.
    pub shared_l3_bytes: usize,
    /// Number of NUMA nodes on the system. 1 on single-socket / cloud VMs.
    pub numa_nodes: usize,
    /// Number of logical CPUs.
    pub n_cpus: usize,
}

impl CacheTopology {
    /// Conservative fallback for non-Linux or unreadable `/sys`.
    const fn fallback() -> Self {
        Self {
            per_core_l2_bytes: 256 * 1024,
            shared_l3_bytes: 8 * 1024 * 1024,
            numa_nodes: 1,
            n_cpus: 8,
        }
    }
}

static TOPOLOGY: OnceLock<CacheTopology> = OnceLock::new();

/// Detect the machine's cache + NUMA topology. Cached after first call.
///
/// Reads `/sys/devices/system/cpu/cpu0/cache/index*` for cache sizes and
/// `/sys/devices/system/node/node*` for NUMA topology. On non-Linux or
/// when sysfs is unavailable, returns conservative fallbacks.
///
/// Logs the detected topology + NUMA-pinning decision once at first call
/// (via `tracing::info`) so operators can see which tile geometry the
/// matrix engine has chosen.
pub fn detect_topology() -> CacheTopology {
    *TOPOLOGY.get_or_init(|| {
        let topo = detect_topology_uncached();
        // Log once. We use `eprintln!` rather than `tracing::info!` so
        // this surfaces in `butterfly-bench` runs even when the bench
        // harness hasn't installed a tracing subscriber.
        eprintln!(
            "[tile_geometry] detected: per_core_l2={} KiB, shared_l3={} MiB, numa_nodes={}, n_cpus={}, numa_pinning={}",
            topo.per_core_l2_bytes / 1024,
            topo.shared_l3_bytes / (1024 * 1024),
            topo.numa_nodes,
            topo.n_cpus,
            if topo.numa_nodes >= 2 {
                "considered (multi-socket — pinning code is a planned follow-up)"
            } else {
                "skipped (single-socket — no win from pinning)"
            },
        );
        topo
    })
}

fn detect_topology_uncached() -> CacheTopology {
    let mut topo = CacheTopology::fallback();

    // CPU count: trust `std::thread::available_parallelism`.
    if let Ok(n) = std::thread::available_parallelism() {
        topo.n_cpus = n.get();
    }

    // Walk /sys/devices/system/cpu/cpu0/cache/index*.
    let cache_root = "/sys/devices/system/cpu/cpu0/cache";
    if let Ok(entries) = std::fs::read_dir(cache_root) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if !name.starts_with("index") {
                continue;
            }
            let level = read_int_file(&format!("{}/{}/level", cache_root, name)).unwrap_or(0);
            let cache_type = std::fs::read_to_string(format!("{}/{}/type", cache_root, name))
                .unwrap_or_default()
                .trim()
                .to_string();
            // We only care about Data and Unified caches (skip Instruction).
            if cache_type == "Instruction" {
                continue;
            }
            let size = read_size_file(&format!("{}/{}/size", cache_root, name)).unwrap_or(0);
            if size == 0 {
                continue;
            }
            match level {
                2 => topo.per_core_l2_bytes = size,
                3 => topo.shared_l3_bytes = size,
                _ => {}
            }
        }
    }

    // NUMA: count node{N} dirs under /sys/devices/system/node.
    let mut numa = 0usize;
    if let Ok(entries) = std::fs::read_dir("/sys/devices/system/node") {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.starts_with("node")
                && name.len() > 4
                && name[4..].chars().all(|c| c.is_ascii_digit())
            {
                numa += 1;
            }
        }
    }
    if numa > 0 {
        topo.numa_nodes = numa;
    }

    topo
}

fn read_int_file(path: &str) -> Option<u32> {
    std::fs::read_to_string(path).ok()?.trim().parse().ok()
}

/// Read a `/sys` "size" file, accepting suffixes `K`, `M`, `G` (kernel format).
fn read_size_file(path: &str) -> Option<usize> {
    let s = std::fs::read_to_string(path).ok()?;
    let s = s.trim();
    let (num, mult): (&str, usize) = if let Some(stripped) = s.strip_suffix('K') {
        (stripped, 1024)
    } else if let Some(stripped) = s.strip_suffix('M') {
        (stripped, 1024 * 1024)
    } else if let Some(stripped) = s.strip_suffix('G') {
        (stripped, 1024 * 1024 * 1024)
    } else {
        (s, 1)
    };
    let n: usize = num.parse().ok()?;
    n.checked_mul(mult)
}

/// Pick the L3-aware source-tile size for monolithic matrix queries.
///
/// Returns `None` when `n_sources × n_targets` is small enough to fit in
/// shared L3 in a single shot — caller skips tiling. Returns `Some(tile)`
/// otherwise.
///
/// `avg_visited_per_search` is the per-source bucket fanout estimate
/// (already cached as `n_nodes / 400` clamped 500..=20_000 in the bucket
/// path).
pub fn pick_source_tile_size(
    n_sources: usize,
    n_targets: usize,
    avg_visited_per_search: usize,
) -> Option<usize> {
    let topo = detect_topology();

    // Working set for the backward phase through the buckets:
    //   `PrefixSumBuckets.dists` + `source_indices` =
    //   `S × avg_visited × 8 bytes` (SoA u32 source_idx + u32 dist)
    //
    // Per-tile result matrix is also written, but writes are streaming
    // (each backward search touches only the column for one target),
    // so we don't budget for it — only the bucket _read_ working set.
    //
    // We want the bucket working set under ~4× shared L3 so multiple
    // backward workers (one per rayon thread) can share L3 without
    // pulling everything from DRAM on every relax. The ×4 budget is
    // empirical: buckets are accessed in `node_id` order during the
    // backward Dijkstra, so prefetcher overlap means we tolerate some
    // overflow without becoming DRAM-bound.
    let l3_budget = topo.shared_l3_bytes.saturating_mul(4);
    let bytes_per_source = avg_visited_per_search.saturating_mul(8); // SoA
    if bytes_per_source == 0 {
        return None;
    }

    let bucket_cap = l3_budget / bytes_per_source;

    let _ = n_targets; // currently unused; kept in signature for future tuning

    // Floor the tile at 1000 — production /table/stream's default —
    // so we don't go below the empirically-tuned shape.
    // Ceiling at 4000 to bound worst-case bucket build memory.
    let tile = bucket_cap.clamp(1000, 4000);

    // Round to nearest 500 for cleaner block geometry.
    let tile = (tile / 500) * 500;
    let tile = tile.max(1000);

    if n_sources <= tile {
        // Whole problem already fits — no tiling needed.
        None
    } else {
        Some(tile)
    }
}

/// True if NUMA pinning could plausibly help (≥ 2 NUMA nodes detected).
///
/// On single-socket / cloud VMs (detected as 1 NUMA node) all memory
/// is local so pinning is a no-op — skip the dependency on `hwloc`.
pub fn should_consider_numa_pinning() -> bool {
    detect_topology().numa_nodes >= 2
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topology_returns_sane_values() {
        let topo = detect_topology();
        // Whatever the host, we should at least see plausible numbers.
        assert!(topo.per_core_l2_bytes >= 64 * 1024, "L2 too small");
        assert!(topo.shared_l3_bytes >= 1024 * 1024, "L3 too small");
        assert!(topo.numa_nodes >= 1, "NUMA nodes must be >= 1");
        assert!(topo.n_cpus >= 1, "Must have at least 1 CPU");
    }

    #[test]
    fn small_problem_skips_tiling() {
        // 1000×1000 should not tile (already fits production tile size).
        assert_eq!(pick_source_tile_size(1000, 1000, 6000), None);
    }

    #[test]
    fn large_problem_tiles() {
        // 10k×10k should tile.
        let tile = pick_source_tile_size(10_000, 10_000, 6000);
        assert!(tile.is_some(), "10k×10k must tile");
        let t = tile.unwrap();
        assert!((1000..=4000).contains(&t), "tile {} out of range", t);
        assert!(
            t.is_multiple_of(500),
            "tile {} should be a multiple of 500",
            t
        );
    }

    #[test]
    fn read_size_file_parses_kernel_suffixes() {
        // Smoke-test the parser with a temp file.
        let dir = std::env::temp_dir();
        let path = dir.join("butterfly_tile_geom_test.txt");
        std::fs::write(&path, "30720K\n").unwrap();
        let n = read_size_file(path.to_str().unwrap()).unwrap();
        assert_eq!(n, 30720 * 1024);
        std::fs::remove_file(&path).ok();
    }
}
