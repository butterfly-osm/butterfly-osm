//! CLI-specific progress handling for butterfly-dl
//!
//! Provides progress bar implementation for the command-line interface.

use indicatif::{ProgressBar, ProgressStyle};

/// Creates a progress bar for CLI display with enhanced information
pub fn create_progress_bar(total_size: u64) -> ProgressBar {
    let pb = ProgressBar::new(total_size);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({percent}%) {bytes_per_sec} ETA: {eta}")
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_progress_bar_template() {
        let pb = create_progress_bar(1000);
        
        // Verify the progress bar is created successfully
        assert_eq!(pb.length().unwrap(), 1000);
        
        // The progress bar should be created without panicking with the enhanced template
        // This verifies the template string is valid
        pb.set_position(100);
        pb.finish();
    }

    #[test]
    fn test_progress_manager_creation() {
        let manager = ProgressManager::new(500, "Test download");
        assert_eq!(manager.pb.length().unwrap(), 500);
    }
}