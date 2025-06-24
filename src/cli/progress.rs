//! CLI-specific progress handling for butterfly-dl
//!
//! Provides progress bar implementation for the command-line interface.

use indicatif::{ProgressBar, ProgressStyle};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

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

/// Creates a progress callback that updates a progress bar
pub fn create_progress_callback(pb: ProgressBar) -> impl Fn(u64, u64) + Send + Sync {
    move |downloaded: u64, total: u64| {
        pb.set_position(downloaded);
        if downloaded >= total {
            pb.finish_with_message("✅ Download completed!");
        }
    }
}

/// Creates a progress callback with custom completion message
pub fn create_progress_callback_with_message(
    pb: ProgressBar,
    completion_message: String,
) -> impl Fn(u64, u64) + Send + Sync {
    move |downloaded: u64, total: u64| {
        pb.set_position(downloaded);
        if downloaded >= total {
            pb.finish_with_message(completion_message.clone());
        }
    }
}

/// Progress manager for complex download operations
pub struct ProgressManager {
    pub pb: ProgressBar,
    completed: Arc<AtomicBool>,
}

impl ProgressManager {
    /// Create a new progress manager
    pub fn new(total_size: u64, message: &str) -> Self {
        let pb = create_progress_bar(total_size);
        
        // Print initial message to stderr
        eprintln!("{}", message);
        
        Self {
            pb,
            completed: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Get a progress callback for this manager
    pub fn callback(&self) -> impl Fn(u64, u64) + Send + Sync {
        let pb = self.pb.clone();
        let completed = Arc::clone(&self.completed);
        
        move |downloaded: u64, total: u64| {
            pb.set_position(downloaded);
            if downloaded >= total {
                completed.store(true, Ordering::Relaxed);
                pb.finish_with_message("✅ Download completed!");
            }
        }
    }

    /// Finish the progress bar with a custom message
    pub fn finish_with_message(&self, message: String) {
        self.completed.store(true, Ordering::Relaxed);
        self.pb.finish_with_message(message);
    }

    /// Check if the download is completed
    pub fn is_completed(&self) -> bool {
        self.completed.load(Ordering::Relaxed)
    }
}