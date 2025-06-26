//! Integration tests for butterfly-dl downloads
//! 
//! These tests verify that downloads can start successfully and then stop them
//! after a few seconds to avoid downloading large files during testing.
//!
//! Note: These tests are disabled during CI package verification to avoid 
//! network dependencies and compilation overhead during cargo publish.

use std::process::{Command, Stdio};
use std::time::{Duration, Instant};
use std::thread;

/// Helper to run a download command with timeout and capture output
fn test_download_starts(source: &str, timeout_secs: u64) -> Result<(String, String, bool), String> {
    // Use the pre-built binary to avoid Cargo lock contention in CI
    let binary_path = if std::path::Path::new("./target/debug/butterfly-dl").exists() {
        "./target/debug/butterfly-dl"
    } else if std::path::Path::new("./target/release/butterfly-dl").exists() {
        "./target/release/butterfly-dl"
    } else {
        // Fallback to cargo run if no pre-built binary exists
        return test_download_with_cargo_run(source, timeout_secs);
    };

    let mut cmd = Command::new(binary_path)
        .arg(source)
        .arg(format!("/tmp/test-{}.pbf", source.replace('/', "_")))
        .arg("--verbose")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("Failed to spawn command: {}", e))?;

    // Wait for timeout or process completion
    let start = Instant::now();
    let mut success = false;
    let mut stdout_output = String::new();
    let mut stderr_output = String::new();

    while start.elapsed() < Duration::from_secs(timeout_secs) {
        match cmd.try_wait() {
            Ok(Some(status)) => {
                // Process completed
                let output = cmd.wait_with_output()
                    .map_err(|e| format!("Failed to get output: {}", e))?;
                
                stdout_output = String::from_utf8_lossy(&output.stdout).to_string();
                stderr_output = String::from_utf8_lossy(&output.stderr).to_string();
                
                if status.success() {
                    success = true;
                } else {
                    return Err(format!("Process failed with status: {}\nStderr: {}", status, stderr_output));
                }
                return Ok((stdout_output, stderr_output, success));
            }
            Ok(None) => {
                // Process still running - check if download started
                thread::sleep(Duration::from_millis(100));
                
                // If we've been running for more than 2 seconds and no error, consider it a success
                if start.elapsed() > Duration::from_secs(2) {
                    success = true;
                    let _ = cmd.kill(); // Stop the download
                    if let Ok(output) = cmd.wait_with_output() {
                        stdout_output = String::from_utf8_lossy(&output.stdout).to_string();
                        stderr_output = String::from_utf8_lossy(&output.stderr).to_string();
                    }
                    return Ok((stdout_output, stderr_output, success));
                }
            }
            Err(e) => {
                let _ = cmd.kill();
                return Err(format!("Error checking process status: {}", e));
            }
        }
    }

    // Timeout reached
    let _ = cmd.kill();
    if let Ok(output) = cmd.wait_with_output() {
        stdout_output = String::from_utf8_lossy(&output.stdout).to_string();
        stderr_output = String::from_utf8_lossy(&output.stderr).to_string();
    }

    Ok((stdout_output, stderr_output, success))
}

/// Fallback function using cargo run (for local development)
fn test_download_with_cargo_run(source: &str, timeout_secs: u64) -> Result<(String, String, bool), String> {
    let mut cmd = Command::new("cargo")
        .arg("run")
        .arg("--bin")
        .arg("butterfly-dl")
        .arg("--")
        .arg(source)
        .arg(format!("/tmp/test-{}.pbf", source.replace('/', "_")))
        .arg("--verbose")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("Failed to spawn command: {}", e))?;

    // Wait for timeout or process completion
    let start = Instant::now();
    let mut success = false;
    let mut stdout_output = String::new();
    let mut stderr_output = String::new();

    while start.elapsed() < Duration::from_secs(timeout_secs) {
        match cmd.try_wait() {
            Ok(Some(status)) => {
                // Process completed
                let output = cmd.wait_with_output()
                    .map_err(|e| format!("Failed to get output: {}", e))?;
                
                stdout_output = String::from_utf8_lossy(&output.stdout).to_string();
                stderr_output = String::from_utf8_lossy(&output.stderr).to_string();
                
                if status.success() {
                    success = true;
                } else {
                    return Err(format!("Process failed with status: {}\nStderr: {}", status, stderr_output));
                }
                return Ok((stdout_output, stderr_output, success));
            }
            Ok(None) => {
                // Process still running - check if download started
                thread::sleep(Duration::from_millis(100));
                
                // If we've been running for more than 2 seconds and no error, consider it a success
                if start.elapsed() > Duration::from_secs(2) {
                    success = true;
                    let _ = cmd.kill(); // Stop the download
                    if let Ok(output) = cmd.wait_with_output() {
                        stdout_output = String::from_utf8_lossy(&output.stdout).to_string();
                        stderr_output = String::from_utf8_lossy(&output.stderr).to_string();
                    }
                    return Ok((stdout_output, stderr_output, success));
                }
            }
            Err(e) => {
                let _ = cmd.kill();
                return Err(format!("Error checking process status: {}", e));
            }
        }
    }

    // Timeout reached
    let _ = cmd.kill();
    if let Ok(output) = cmd.wait_with_output() {
        stdout_output = String::from_utf8_lossy(&output.stdout).to_string();
        stderr_output = String::from_utf8_lossy(&output.stderr).to_string();
    }

    Ok((stdout_output, stderr_output, success))
}

#[test]
#[cfg(not(feature = "ci-tests-disabled"))]
fn test_planet_download_starts() {
    println!("Testing planet download startup...");
    
    match test_download_starts("planet", 10) {
        Ok((stdout, stderr, success)) => {
            println!("Planet test completed:");
            println!("Stdout: {}", stdout);
            println!("Stderr: {}", stderr);
            
            if success {
                assert!(stderr.contains("Downloading planet") || stderr.contains("HTTP") || stderr.contains("planet"), 
                       "Expected planet download indicators in stderr: {}", stderr);
            } else {
                panic!("Planet download failed to start successfully");
            }
        }
        Err(e) => {
            panic!("Planet download test failed: {}", e);
        }
    }
}

#[test]
#[cfg(not(feature = "ci-tests-disabled"))]
fn test_europe_continent_download_starts() {
    println!("Testing Europe continent download startup...");
    
    match test_download_starts("europe", 10) {
        Ok((stdout, stderr, success)) => {
            println!("Europe test completed:");
            println!("Stdout: {}", stdout);
            println!("Stderr: {}", stderr);
            
            if success {
                assert!(stderr.contains("Downloading europe") || stderr.contains("HTTP") || stderr.contains("geofabrik"), 
                       "Expected Europe download indicators in stderr: {}", stderr);
            } else {
                panic!("Europe download failed to start successfully");
            }
        }
        Err(e) => {
            panic!("Europe download test failed: {}", e);
        }
    }
}

#[test]
#[cfg(not(feature = "ci-tests-disabled"))]
fn test_monaco_country_download_starts() {
    println!("Testing Monaco country download startup...");
    
    match test_download_starts("europe/monaco", 10) {
        Ok((stdout, stderr, success)) => {
            println!("Monaco test completed:");
            println!("Stdout: {}", stdout);
            println!("Stderr: {}", stderr);
            
            if success {
                assert!(stderr.contains("Downloading europe/monaco") || stderr.contains("HTTP") || stderr.contains("geofabrik"), 
                       "Expected Monaco download indicators in stderr: {}", stderr);
            } else {
                panic!("Monaco download failed to start successfully");
            }
        }
        Err(e) => {
            panic!("Monaco download test failed: {}", e);
        }
    }
}

#[test]
#[cfg(not(feature = "ci-tests-disabled"))]
fn test_invalid_continent_fails_gracefully() {
    println!("Testing invalid continent (invalid-continent) fails gracefully...");
    
    match test_download_starts("invalid-continent", 10) {
        Ok((stdout, stderr, success)) => {
            println!("Invalid continent test completed:");
            println!("Stdout: {}", stdout);
            println!("Stderr: {}", stderr);
            
            // This should fail, but gracefully
            assert!(!success, "Invalid continent download should fail since it's not a valid Geofabrik continent");
            assert!(stderr.contains("404") || stderr.contains("not found") || stderr.contains("HttpError"), 
                   "Expected 404 or not found error for invalid continent: {}", stderr);
        }
        Err(e) => {
            // This is expected - invalid continent should fail
            println!("Invalid continent correctly failed: {}", e);
            assert!(e.contains("404") || e.contains("not found"), 
                   "Expected 404 error for invalid continent: {}", e);
        }
    }
}

#[test]
#[cfg(not(feature = "ci-tests-disabled"))]
fn test_antarctica_continent_download_starts() {
    println!("Testing Antarctica continent download starts...");
    
    match test_download_starts("antarctica", 5) {
        Ok((stdout, stderr, success)) => {
            println!("Antarctica test completed:");
            println!("Stdout: {}", stdout);
            println!("Stderr: {}", stderr);
            
            assert!(success, "Antarctica download should succeed since it's a valid Geofabrik continent");
            assert!(stderr.contains("Downloading from HTTP"), "Should show HTTP download source");
            assert!(stderr.contains("antarctica"), "Should reference antarctica in output");
        }
        Err(e) => {
            panic!("Antarctica download test failed: {}", e);
        }
    }
}

#[test]
#[cfg(not(feature = "ci-tests-disabled"))]
fn test_valid_country_belgium_download_starts() {
    println!("Testing Belgium country download startup...");
    
    match test_download_starts("europe/belgium", 10) {
        Ok((stdout, stderr, success)) => {
            println!("Belgium test completed:");
            println!("Stdout: {}", stdout);
            println!("Stderr: {}", stderr);
            
            if success {
                assert!(stderr.contains("Downloading europe/belgium") || stderr.contains("HTTP") || stderr.contains("geofabrik"), 
                       "Expected Belgium download indicators in stderr: {}", stderr);
            } else {
                panic!("Belgium download failed to start successfully");
            }
        }
        Err(e) => {
            panic!("Belgium download test failed: {}", e);
        }
    }
}

/// Test dry run mode for all source types
#[test]
#[cfg(not(feature = "ci-tests-disabled"))]
fn test_dry_run_mode() {
    println!("Testing dry run mode for different sources...");
    
    // Use the same binary detection logic as other tests
    let binary_path = if std::path::Path::new("./target/debug/butterfly-dl").exists() {
        "./target/debug/butterfly-dl"
    } else if std::path::Path::new("./target/release/butterfly-dl").exists() {
        "./target/release/butterfly-dl"
    } else {
        panic!("No pre-built binary found. Run 'cargo build' first.");
    };
    
    let sources = ["planet", "europe", "europe/monaco", "europe/belgium"];
    
    for source in &sources {
        let output = Command::new(binary_path)
            .arg(source)
            .arg("--dry-run")
            .output()
            .expect("Failed to run dry-run command");
        
        let stderr = String::from_utf8_lossy(&output.stderr);
        println!("Dry run for {}: {}", source, stderr);
        
        assert!(output.status.success(), "Dry run should succeed for {}", source);
        assert!(stderr.contains("DRY RUN"), "Expected DRY RUN indicator for {}", source);
        assert!(stderr.contains(source), "Expected source name in output for {}", source);
    }
}

/// Cleanup function to remove any test files
#[cfg(test)]
mod cleanup {
    use std::fs;
    
    #[ctor::dtor]
    fn cleanup() {
        let _ = fs::remove_file("/tmp/test-planet.pbf");
        let _ = fs::remove_file("/tmp/test-europe.pbf");
        let _ = fs::remove_file("/tmp/test-europe_monaco.pbf");
        let _ = fs::remove_file("/tmp/test-europe_belgium.pbf");
        let _ = fs::remove_file("/tmp/test-antarctica.pbf");
    }
}