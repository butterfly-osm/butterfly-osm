//! Memory monitoring and management

use anyhow::Result;
use std::fs;

/// Get current process RSS (Resident Set Size) in MB
#[cfg(target_os = "linux")]
pub fn get_rss_mb() -> Result<f64> {
    let status = fs::read_to_string("/proc/self/status")?;
    for line in status.lines() {
        if line.starts_with("VmRSS:") {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 2 {
                let kb: f64 = parts[1].parse()?;
                return Ok(kb / 1024.0);
            }
        }
    }
    anyhow::bail!("Could not find VmRSS in /proc/self/status")
}

#[cfg(target_os = "macos")]
pub fn get_rss_mb() -> Result<f64> {
    use std::mem;
    use std::os::raw::c_int;
    
    #[repr(C)]
    struct RUsage {
        ru_utime: libc::timeval,
        ru_stime: libc::timeval,
        ru_maxrss: i64,
        // ... other fields we don't care about
    }
    
    unsafe {
        let mut usage: RUsage = mem::zeroed();
        let ret = libc::getrusage(libc::RUSAGE_SELF, &mut usage as *mut _ as *mut libc::rusage);
        if ret == 0 {
            // On macOS, ru_maxrss is in bytes
            Ok(usage.ru_maxrss as f64 / (1024.0 * 1024.0))
        } else {
            anyhow::bail!("getrusage failed")
        }
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub fn get_rss_mb() -> Result<f64> {
    // Fallback: return 0 if we can't determine RSS
    Ok(0.0)
}

/// Memory watchdog configuration
pub struct MemoryWatchdog {
    soft_limit_mb: f64,
    hard_limit_mb: f64,
    last_check_mb: f64,
}

impl MemoryWatchdog {
    pub fn new(max_mem_mb: usize) -> Self {
        let soft_limit_mb = max_mem_mb as f64;
        let hard_limit_mb = soft_limit_mb * 1.1; // 10% buffer
        
        Self {
            soft_limit_mb,
            hard_limit_mb,
            last_check_mb: 0.0,
        }
    }
    
    /// Check memory and return action to take
    pub fn check(&mut self) -> MemoryAction {
        match get_rss_mb() {
            Ok(rss_mb) => {
                self.last_check_mb = rss_mb;
                
                if rss_mb > self.hard_limit_mb {
                    log::error!("Memory hard limit exceeded: {:.1}MB > {:.1}MB", 
                        rss_mb, self.hard_limit_mb);
                    MemoryAction::Abort
                } else if rss_mb > self.soft_limit_mb {
                    log::warn!("Memory soft limit exceeded: {:.1}MB > {:.1}MB - forcing flush", 
                        rss_mb, self.soft_limit_mb);
                    MemoryAction::ForceFlush
                } else {
                    log::trace!("Memory usage: {:.1}MB / {:.1}MB", rss_mb, self.soft_limit_mb);
                    MemoryAction::Continue
                }
            }
            Err(e) => {
                log::debug!("Could not check memory: {}", e);
                MemoryAction::Continue
            }
        }
    }
    
    pub fn current_rss_mb(&self) -> f64 {
        self.last_check_mb
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MemoryAction {
    Continue,     // All good
    ForceFlush,   // Soft limit hit - flush batch
    Abort,        // Hard limit hit - abort processing
}