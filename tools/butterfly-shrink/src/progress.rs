//! Progress tracking with indicatif for clean terminal output
//!
//! Uses the same progress bar style as butterfly-dl to avoid spamming output.

use indicatif::{ProgressBar, ProgressStyle};
use std::sync::Arc;

/// Progress tracker using indicatif
pub struct Progress {
    pb: ProgressBar,
    name: String,
}

impl Progress {
    /// Create a new progress tracker
    pub fn new(name: impl Into<String>) -> Arc<Self> {
        let name = name.into();
        let pb = ProgressBar::new(0);
        
        // Use spinner initially until we know the total
        pb.set_style(
            ProgressStyle::default_spinner()
                .template("{spinner:.green} {msg}")
                .expect("Failed to create progress style")
        );
        pb.set_message(name.clone());
        
        Arc::new(Self { pb, name })
    }
    
    /// Create a progress bar with known total
    pub fn new_with_total(name: impl Into<String>, total: u64) -> Arc<Self> {
        let name = name.into();
        let pb = ProgressBar::new(total);
        
        pb.set_style(
            ProgressStyle::default_bar()
                .template("{msg}: {percent:>3}%|{wide_bar:.cyan/blue}| {human_pos}/{human_len} [{elapsed_precise}<{eta}, {per_sec}]")
                .expect("Failed to create progress style")
                .progress_chars("█▉▊▋▌▍▎▏ ")
        );
        pb.set_message(name.clone());
        
        Arc::new(Self { pb, name })
    }
    
    /// Set the total expected items
    pub fn set_total(&self, total: u64) {
        self.pb.set_length(total);
        
        // Switch from spinner to bar style when we know the total
        self.pb.set_style(
            ProgressStyle::default_bar()
                .template(&format!("{}: {{percent:>3}}%|{{wide_bar:.cyan/blue}}| {{human_pos}}/{{human_len}} [{{elapsed_precise}}<{{eta}}, {{per_sec}}]", self.name))
                .expect("Failed to create progress style")
                .progress_chars("█▉▊▋▌▍▎▏ ")
        );
    }
    
    /// Increment progress
    pub fn inc(&self, delta: u64) {
        self.pb.inc(delta);
    }
    
    /// Set current position
    pub fn set(&self, pos: u64) {
        self.pb.set_position(pos);
    }
    
    /// Update message
    pub fn set_message(&self, msg: impl Into<String>) {
        self.pb.set_message(msg.into());
    }
    
    /// Force a progress report (no-op with indicatif, it handles this)
    pub fn report(&self, _current: u64) {
        // indicatif handles reporting automatically
    }
    
    /// Finish and clear the progress bar
    pub fn finish(&self) {
        self.pb.finish_with_message(format!("{}: Complete!", self.name));
    }
    
    /// Finish with a custom message
    pub fn finish_with_message(&self, msg: impl Into<String>) {
        self.pb.finish_with_message(msg.into());
    }
    
    /// Abandon the progress bar (removes it from display)
    pub fn abandon(&self) {
        self.pb.abandon();
    }
}

/// Helper to temporarily suppress progress bars during batch operations
pub struct ProgressGuard {
    pb: Arc<Progress>,
}

impl ProgressGuard {
    pub fn new(pb: Arc<Progress>) -> Self {
        pb.pb.set_draw_target(indicatif::ProgressDrawTarget::hidden());
        Self { pb }
    }
}

impl Drop for ProgressGuard {
    fn drop(&mut self) {
        self.pb.pb.set_draw_target(indicatif::ProgressDrawTarget::stderr());
    }
}