//! M7.1 - Thread Architecture: NUMA-aware high-performance serving

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::{self, ThreadId};
use crate::profiles::TransportProfile;

/// NUMA node identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NumaNode(pub u16);

impl NumaNode {
    pub fn new(id: u16) -> Self {
        Self(id)
    }

    /// Get the current thread's NUMA node
    pub fn current() -> Self {
        // In a real implementation, this would use libnuma or similar
        // For now, we'll use a simple CPU-based heuristic
        let cpu_id = get_current_cpu_id();
        Self((cpu_id / 4) as u16) // Assume 4 cores per NUMA node
    }

    /// Get total number of NUMA nodes in the system
    pub fn system_count() -> usize {
        // In a real implementation, this would query the system
        // For now, assume a typical dual-socket server
        std::env::var("BUTTERFLY_NUMA_NODES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(2)
    }
}

/// Per-thread memory arena for NUMA-aware allocation
#[derive(Debug)]
pub struct ThreadArena {
    pub thread_id: ThreadId,
    pub numa_node: NumaNode,
    pub allocation_size: AtomicUsize,
    pub peak_allocation: AtomicUsize,
    pub allocations_count: AtomicUsize,
}

impl ThreadArena {
    pub fn new() -> Self {
        let thread_id = thread::current().id();
        let numa_node = NumaNode::current();
        
        Self {
            thread_id,
            numa_node,
            allocation_size: AtomicUsize::new(0),
            peak_allocation: AtomicUsize::new(0),
            allocations_count: AtomicUsize::new(0),
        }
    }

    /// Allocate memory in this arena
    pub fn allocate(&self, size: usize) -> Result<(), String> {
        let current = self.allocation_size.fetch_add(size, Ordering::Relaxed);
        let new_size = current + size;
        
        // Update peak if necessary
        let mut peak = self.peak_allocation.load(Ordering::Relaxed);
        while peak < new_size {
            match self.peak_allocation.compare_exchange_weak(
                peak, 
                new_size, 
                Ordering::Relaxed, 
                Ordering::Relaxed
            ) {
                Ok(_) => break,
                Err(actual) => peak = actual,
            }
        }
        
        self.allocations_count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Deallocate memory from this arena
    pub fn deallocate(&self, size: usize) {
        self.allocation_size.fetch_sub(size, Ordering::Relaxed);
    }

    /// Get current allocation size
    pub fn current_allocation(&self) -> usize {
        self.allocation_size.load(Ordering::Relaxed)
    }

    /// Get peak allocation size
    pub fn peak_allocation(&self) -> usize {
        self.peak_allocation.load(Ordering::Relaxed)
    }

    /// Get total number of allocations
    pub fn allocation_count(&self) -> usize {
        self.allocations_count.load(Ordering::Relaxed)
    }

    /// Get arena statistics
    pub fn stats(&self) -> ThreadArenaStats {
        ThreadArenaStats {
            thread_id_hash: format!("{:?}", self.thread_id).chars().fold(0u64, |acc, c| acc.wrapping_mul(31).wrapping_add(c as u64)),
            numa_node: self.numa_node,
            current_allocation: self.current_allocation(),
            peak_allocation: self.peak_allocation(),
            allocation_count: self.allocation_count(),
        }
    }
}

// Thread-local storage for routing operations
thread_local! {
    static THREAD_ARENA: ThreadArena = ThreadArena::new();
}

/// Get the current thread's arena
pub fn current_arena() -> ThreadArenaStats {
    THREAD_ARENA.with(|arena| arena.stats())
}

/// Allocate memory in the current thread's arena
pub fn arena_allocate(size: usize) -> Result<(), String> {
    THREAD_ARENA.with(|arena| arena.allocate(size))
}

/// Deallocate memory from the current thread's arena
pub fn arena_deallocate(size: usize) {
    THREAD_ARENA.with(|arena| arena.deallocate(size))
}

/// Thread pool configuration for NUMA-aware scheduling
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThreadPoolConfig {
    pub threads_per_numa_node: usize,
    pub max_threads: usize,
    pub thread_stack_size: usize,
    pub numa_affinity_enabled: bool,
    pub lock_free_enabled: bool,
}

impl Default for ThreadPoolConfig {
    fn default() -> Self {
        Self {
            threads_per_numa_node: 4,
            max_threads: 16,
            thread_stack_size: 2 * 1024 * 1024, // 2MB stack
            numa_affinity_enabled: true,
            lock_free_enabled: true,
        }
    }
}

/// NUMA-aware thread pool for parallel serving
#[derive(Debug)]
pub struct NumaThreadPool {
    config: ThreadPoolConfig,
    threads: Vec<thread::JoinHandle<()>>,
    thread_arenas: Arc<std::sync::RwLock<HashMap<ThreadId, ThreadArenaStats>>>,
    active_threads: AtomicUsize,
    total_requests: AtomicUsize,
}

impl NumaThreadPool {
    pub fn new(config: ThreadPoolConfig) -> Self {
        Self {
            config,
            threads: Vec::new(),
            thread_arenas: Arc::new(std::sync::RwLock::new(HashMap::new())),
            active_threads: AtomicUsize::new(0),
            total_requests: AtomicUsize::new(0),
        }
    }

    /// Start the thread pool
    pub fn start(&mut self) -> Result<(), String> {
        let numa_nodes = NumaNode::system_count();
        let total_threads = (numa_nodes * self.config.threads_per_numa_node).min(self.config.max_threads);

        for thread_idx in 0..total_threads {
            let numa_node = NumaNode::new((thread_idx / self.config.threads_per_numa_node) as u16);
            let thread_arenas = Arc::clone(&self.thread_arenas);
            let stack_size = self.config.thread_stack_size;

            let handle = thread::Builder::new()
                .stack_size(stack_size)
                .name(format!("butterfly-worker-{}", thread_idx))
                .spawn(move || {
                    // Set NUMA affinity if enabled
                    if true { // config.numa_affinity_enabled
                        set_numa_affinity(numa_node);
                    }

                    // Create thread arena
                    let arena = ThreadArena::new();
                    let thread_id = thread::current().id();
                    
                    // Register arena
                    {
                        let mut arenas = thread_arenas.write().unwrap();
                        arenas.insert(thread_id, arena.stats());
                    }

                    // Worker loop (simplified for now)
                    loop {
                        // In a real implementation, this would handle work items
                        thread::sleep(std::time::Duration::from_millis(100));
                        
                        // Update arena stats periodically
                        {
                            let mut arenas = thread_arenas.write().unwrap();
                            arenas.insert(thread_id, arena.stats());
                        }
                    }
                })
                .map_err(|e| format!("Failed to spawn thread: {}", e))?;

            self.threads.push(handle);
        }

        self.active_threads.store(total_threads, Ordering::Relaxed);
        Ok(())
    }

    /// Get thread pool statistics
    pub fn stats(&self) -> ThreadPoolStats {
        let arenas = self.thread_arenas.read().unwrap();
        let total_allocation = arenas.values().map(|stats| stats.current_allocation).sum();
        let peak_allocation = arenas.values().map(|stats| stats.peak_allocation).max().unwrap_or(0);
        let total_allocations = arenas.values().map(|stats| stats.allocation_count).sum();

        let numa_distribution: HashMap<NumaNode, usize> = arenas
            .values()
            .fold(HashMap::new(), |mut acc, stats| {
                *acc.entry(stats.numa_node).or_insert(0) += 1;
                acc
            });

        ThreadPoolStats {
            active_threads: self.active_threads.load(Ordering::Relaxed),
            total_requests: self.total_requests.load(Ordering::Relaxed),
            total_allocation,
            peak_allocation,
            total_allocations,
            numa_distribution,
            config: self.config.clone(),
        }
    }

    /// Submit a request (increments counter)
    pub fn submit_request(&self) {
        self.total_requests.fetch_add(1, Ordering::Relaxed);
    }
}

/// Lock-free hot path for routing operations
pub struct LockFreeHotPath {
    enabled: bool,
    cache_hits: AtomicUsize,
    cache_misses: AtomicUsize,
    fast_path_hits: AtomicUsize,
    slow_path_hits: AtomicUsize,
}

impl LockFreeHotPath {
    pub fn new(enabled: bool) -> Self {
        Self {
            enabled,
            cache_hits: AtomicUsize::new(0),
            cache_misses: AtomicUsize::new(0),
            fast_path_hits: AtomicUsize::new(0),
            slow_path_hits: AtomicUsize::new(0),
        }
    }

    /// Execute a routing operation via lock-free hot path
    pub fn route<F, R>(&self, fast_path: F, slow_path: F) -> R 
    where 
        F: Fn() -> R,
        R: Clone,
    {
        if self.enabled {
            // Try fast path first (lock-free)
            if let Ok(result) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| fast_path())) {
                self.fast_path_hits.fetch_add(1, Ordering::Relaxed);
                return result;
            }
        }

        // Fall back to slow path
        self.slow_path_hits.fetch_add(1, Ordering::Relaxed);
        slow_path()
    }

    /// Record cache hit
    pub fn record_cache_hit(&self) {
        self.cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Record cache miss
    pub fn record_cache_miss(&self) {
        self.cache_misses.fetch_add(1, Ordering::Relaxed);
    }

    /// Get hot path statistics
    pub fn stats(&self) -> LockFreeStats {
        let cache_hits = self.cache_hits.load(Ordering::Relaxed);
        let cache_misses = self.cache_misses.load(Ordering::Relaxed);
        let total_cache_ops = cache_hits + cache_misses;
        let cache_hit_rate = if total_cache_ops > 0 {
            cache_hits as f64 / total_cache_ops as f64
        } else {
            0.0
        };

        let fast_path_hits = self.fast_path_hits.load(Ordering::Relaxed);
        let slow_path_hits = self.slow_path_hits.load(Ordering::Relaxed);
        let total_path_ops = fast_path_hits + slow_path_hits;
        let fast_path_rate = if total_path_ops > 0 {
            fast_path_hits as f64 / total_path_ops as f64
        } else {
            0.0
        };

        LockFreeStats {
            enabled: self.enabled,
            cache_hit_rate,
            fast_path_rate,
            cache_hits,
            cache_misses,
            fast_path_hits,
            slow_path_hits,
        }
    }
}

/// Thread architecture system combining all components
pub struct ThreadArchitectureSystem {
    pub thread_pool: NumaThreadPool,
    pub lock_free_hot_path: LockFreeHotPath,
    pub profile_affinities: HashMap<TransportProfile, NumaNode>,
}

impl ThreadArchitectureSystem {
    pub fn new(config: ThreadPoolConfig) -> Self {
        let lock_free_enabled = config.lock_free_enabled;
        let thread_pool = NumaThreadPool::new(config);
        let lock_free_hot_path = LockFreeHotPath::new(lock_free_enabled);

        // Assign profiles to NUMA nodes for better cache locality
        let mut profile_affinities = HashMap::new();
        let numa_count = NumaNode::system_count();
        let profiles = [TransportProfile::Car, TransportProfile::Bicycle, TransportProfile::Foot];
        
        for (i, profile) in profiles.iter().enumerate() {
            profile_affinities.insert(*profile, NumaNode::new((i % numa_count) as u16));
        }

        Self {
            thread_pool,
            lock_free_hot_path,
            profile_affinities,
        }
    }

    /// Start the thread architecture system
    pub fn start(&mut self) -> Result<(), String> {
        self.thread_pool.start()
    }

    /// Get preferred NUMA node for a profile
    pub fn preferred_numa_node(&self, profile: &TransportProfile) -> Option<NumaNode> {
        self.profile_affinities.get(profile).copied()
    }

    /// Submit a routing request
    pub fn submit_routing_request(&self, profile: TransportProfile) {
        self.thread_pool.submit_request();
        
        // Route to preferred NUMA node if possible
        if let Some(_numa_node) = self.preferred_numa_node(&profile) {
            // In a real implementation, would route to specific NUMA node
        }
    }

    /// Get comprehensive system statistics
    pub fn stats(&self) -> ThreadArchitectureStats {
        ThreadArchitectureStats {
            thread_pool: self.thread_pool.stats(),
            lock_free: self.lock_free_hot_path.stats(),
            profile_affinities: self.profile_affinities.clone(),
        }
    }
}

/// Statistics for thread arena
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThreadArenaStats {
    pub thread_id_hash: u64,
    pub numa_node: NumaNode,
    pub current_allocation: usize,
    pub peak_allocation: usize,
    pub allocation_count: usize,
}

/// Statistics for thread pool
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThreadPoolStats {
    pub active_threads: usize,
    pub total_requests: usize,
    pub total_allocation: usize,
    pub peak_allocation: usize,
    pub total_allocations: usize,
    pub numa_distribution: HashMap<NumaNode, usize>,
    pub config: ThreadPoolConfig,
}

/// Statistics for lock-free operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LockFreeStats {
    pub enabled: bool,
    pub cache_hit_rate: f64,
    pub fast_path_rate: f64,
    pub cache_hits: usize,
    pub cache_misses: usize,
    pub fast_path_hits: usize,
    pub slow_path_hits: usize,
}

/// Comprehensive thread architecture statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThreadArchitectureStats {
    pub thread_pool: ThreadPoolStats,
    pub lock_free: LockFreeStats,
    pub profile_affinities: HashMap<TransportProfile, NumaNode>,
}

/// Set NUMA affinity for current thread
fn set_numa_affinity(numa_node: NumaNode) {
    // In a real implementation, this would use libnuma
    // For now, this is a placeholder
    std::env::set_var("BUTTERFLY_THREAD_NUMA", numa_node.0.to_string());
}

/// Get current CPU ID (simplified implementation)
fn get_current_cpu_id() -> usize {
    // In a real implementation, this would use sched_getcpu() or similar
    // For now, use a simple hash of thread ID
    let thread_id = thread::current().id();
    format!("{:?}", thread_id).chars().fold(0usize, |acc, c| acc.wrapping_mul(31).wrapping_add(c as usize)) % 16
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_numa_node_creation() {
        let node = NumaNode::new(0);
        assert_eq!(node.0, 0);

        let current = NumaNode::current();
        assert!(current.0 < 16); // Reasonable upper bound

        let count = NumaNode::system_count();
        assert!(count >= 1 && count <= 8); // Reasonable range
    }

    #[test]
    fn test_thread_arena() {
        let arena = ThreadArena::new();
        
        assert_eq!(arena.current_allocation(), 0);
        assert_eq!(arena.peak_allocation(), 0);
        assert_eq!(arena.allocation_count(), 0);

        // Test allocation
        arena.allocate(1024).unwrap();
        assert_eq!(arena.current_allocation(), 1024);
        assert_eq!(arena.peak_allocation(), 1024);
        assert_eq!(arena.allocation_count(), 1);

        // Test another allocation
        arena.allocate(512).unwrap();
        assert_eq!(arena.current_allocation(), 1536);
        assert_eq!(arena.peak_allocation(), 1536);
        assert_eq!(arena.allocation_count(), 2);

        // Test deallocation
        arena.deallocate(512);
        assert_eq!(arena.current_allocation(), 1024);
        assert_eq!(arena.peak_allocation(), 1536); // Peak remains
        assert_eq!(arena.allocation_count(), 2);

        // Test stats
        let stats = arena.stats();
        assert_eq!(stats.current_allocation, 1024);
        assert_eq!(stats.peak_allocation, 1536);
        assert_eq!(stats.allocation_count, 2);
    }

    #[test]
    fn test_thread_local_arena() {
        // Test thread-local arena functions
        let initial_stats = current_arena();
        assert_eq!(initial_stats.current_allocation, 0);

        arena_allocate(2048).unwrap();
        let after_alloc = current_arena();
        assert_eq!(after_alloc.current_allocation, 2048);
        assert_eq!(after_alloc.allocation_count, 1);

        arena_deallocate(1024);
        let after_dealloc = current_arena();
        assert_eq!(after_dealloc.current_allocation, 1024);
    }

    #[test]
    fn test_thread_pool_config() {
        let config = ThreadPoolConfig::default();
        assert_eq!(config.threads_per_numa_node, 4);
        assert_eq!(config.max_threads, 16);
        assert_eq!(config.thread_stack_size, 2 * 1024 * 1024);
        assert!(config.numa_affinity_enabled);
        assert!(config.lock_free_enabled);
    }

    #[test]
    fn test_numa_thread_pool_creation() {
        let config = ThreadPoolConfig::default();
        let pool = NumaThreadPool::new(config.clone());
        
        let stats = pool.stats();
        assert_eq!(stats.active_threads, 0);
        assert_eq!(stats.total_requests, 0);
        assert_eq!(stats.config.max_threads, config.max_threads);
    }

    #[test]
    fn test_lock_free_hot_path() {
        let hot_path = LockFreeHotPath::new(true);
        
        let stats = hot_path.stats();
        assert!(stats.enabled);
        assert_eq!(stats.cache_hits, 0);
        assert_eq!(stats.fast_path_hits, 0);

        // Test cache operations
        hot_path.record_cache_hit();
        hot_path.record_cache_hit();
        hot_path.record_cache_miss();
        
        let stats = hot_path.stats();
        assert_eq!(stats.cache_hits, 2);
        assert_eq!(stats.cache_misses, 1);
        assert!((stats.cache_hit_rate - 2.0/3.0).abs() < 0.001);

        // Test routing operations
        let result = hot_path.route(|| 42, || 0);
        assert_eq!(result, 42);
        
        let stats = hot_path.stats();
        assert_eq!(stats.fast_path_hits, 1);
    }

    #[test]
    fn test_thread_architecture_system() {
        let config = ThreadPoolConfig::default();
        let system = ThreadArchitectureSystem::new(config);
        
        // Test profile affinities
        assert!(system.preferred_numa_node(&TransportProfile::Car).is_some());
        assert!(system.preferred_numa_node(&TransportProfile::Bicycle).is_some());
        assert!(system.preferred_numa_node(&TransportProfile::Foot).is_some());

        // Test request submission
        system.submit_routing_request(TransportProfile::Car);
        let stats = system.stats();
        assert_eq!(stats.thread_pool.total_requests, 1);
    }

    #[test]
    fn test_numa_distribution() {
        let config = ThreadPoolConfig::default();
        let system = ThreadArchitectureSystem::new(config);
        
        // Check that different profiles get different NUMA nodes
        let car_numa = system.preferred_numa_node(&TransportProfile::Car);
        let bike_numa = system.preferred_numa_node(&TransportProfile::Bicycle);
        let foot_numa = system.preferred_numa_node(&TransportProfile::Foot);
        
        assert!(car_numa.is_some());
        assert!(bike_numa.is_some());
        assert!(foot_numa.is_some());
        
        // With multiple NUMA nodes, profiles should be distributed
        let numa_count = NumaNode::system_count();
        if numa_count > 1 {
            let nodes = vec![car_numa, bike_numa, foot_numa];
            let unique_nodes: std::collections::HashSet<_> = nodes.into_iter().collect();
            assert!(unique_nodes.len() >= 2); // At least 2 different NUMA nodes used
        }
    }

    #[test]
    fn test_concurrent_arena_operations() {
        use std::sync::Arc;
        use std::thread;
        
        let arena = Arc::new(ThreadArena::new());
        let mut handles = vec![];
        
        for i in 0..4 {
            let arena_clone = Arc::clone(&arena);
            let handle = thread::spawn(move || {
                for j in 0..10 {
                    let size = (i + 1) * 100 + j * 10;
                    arena_clone.allocate(size).unwrap();
                    thread::sleep(std::time::Duration::from_millis(1));
                    arena_clone.deallocate(size);
                }
            });
            handles.push(handle);
        }
        
        for handle in handles {
            handle.join().unwrap();
        }
        
        let stats = arena.stats();
        assert_eq!(stats.current_allocation, 0); // All deallocated
        assert_eq!(stats.allocation_count, 40); // 4 threads × 10 allocations
        assert!(stats.peak_allocation > 0);
    }

    #[test] 
    fn test_lock_free_disabled() {
        let hot_path = LockFreeHotPath::new(false);
        let stats = hot_path.stats();
        assert!(!stats.enabled);
        
        // When disabled, should always use slow path
        let result = hot_path.route(|| 42, || 100);
        assert_eq!(result, 100);
        
        let stats = hot_path.stats();
        assert_eq!(stats.fast_path_hits, 0);
        assert_eq!(stats.slow_path_hits, 1);
    }
}