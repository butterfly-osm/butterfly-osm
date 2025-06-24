//! CLI-specific progress handling for butterfly-dl
//!
//! Provides progress bar implementation for the command-line interface.

use indicatif::{ProgressBar, ProgressStyle};

/// Creates a progress bar for CLI display
pub fn create_progress_bar(total_size: u64) -> ProgressBar {
    let pb = ProgressBar::new(total_size);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({eta})")
            .expect("Failed to create progress style")
            .progress_chars("#>-")
    );
    pb
}

// Unused progress callback functions removed

/// Progress manager for complex download operations
pub struct ProgressManager {
    pub pb: ProgressBar,
}

impl ProgressManager {
    /// Create a new progress manager
    pub fn new(total_size: u64, message: &str) -> Self {
        let pb = create_progress_bar(total_size);
        
        // Print initial message to stderr
        eprintln!("{}", message);
        
        Self {
            pb,
        }
    }

    // Unused methods removed - ProgressManager is only used for creating progress bars
}