//! Integration tests for butterfly-dl downloads
//!
//! These tests verify that downloads can start successfully and then stop them
//! after a few seconds to avoid downloading large files during testing.
//!
//! Note: These tests are disabled during CI package verification to avoid
//! network dependencies and compilation overhead during cargo publish.

use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

/// Helper to run a download command with timeout and capture output
#[allow(dead_code)]
fn test_download_starts(source: &str, timeout_secs: u64) -> Result<(String, String, bool), String> {
    // Use the pre-built binary to avoid Cargo lock contention in CI
    let binary_name = if cfg!(windows) {
        "butterfly-dl.exe"
    } else {
        "butterfly-dl"
    };

    // Calculate workspace root (two levels up from package dir)
    let workspace_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let debug_binary = workspace_root
        .join("target")
        .join("debug")
        .join(binary_name);
    let release_binary = workspace_root
        .join("target")
        .join("release")
        .join(binary_name);

    let binary_path = if debug_binary.exists() {
        debug_binary.to_string_lossy().to_string()
    } else if release_binary.exists() {
        release_binary.to_string_lossy().to_string()
    } else {
        // Build the binary first to avoid chicken-and-egg problem
        let build_output = Command::new("cargo")
            .args(["build", "--bin", "butterfly-dl"])
            .current_dir(workspace_root)
            .output()
            .map_err(|e| format!("Failed to execute cargo build: {e}"))?;

        if !build_output.status.success() {
            return Err(format!(
                "Failed to build butterfly-dl: {}",
                String::from_utf8_lossy(&build_output.stderr)
            ));
        }

        debug_binary.to_string_lossy().to_string()
    };

    let mut cmd = Command::new(&binary_path)
        .arg(source)
        .arg({
            let temp_dir = if cfg!(windows) {
                std::env::temp_dir()
            } else {
                std::path::PathBuf::from("/tmp")
            };
            temp_dir
                .join(format!("test-{}.pbf", source.replace('/', "_")))
                .to_string_lossy()
                .to_string()
        })
        .arg("--verbose")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("Failed to spawn command: {e}"))?;

    // Wait for timeout or process completion
    let start = Instant::now();
    let mut success = false;
    let mut stdout_output = String::new();
    let mut stderr_output = String::new();

    while start.elapsed() < Duration::from_secs(timeout_secs) {
        match cmd.try_wait() {
            Ok(Some(status)) => {
                // Process completed
                let output = cmd
                    .wait_with_output()
                    .map_err(|e| format!("Failed to get output: {e}"))?;

                stdout_output = String::from_utf8_lossy(&output.stdout).to_string();
                stderr_output = String::from_utf8_lossy(&output.stderr).to_string();

                if status.success() {
                    success = true;
                } else {
                    return Err(format!(
                        "Process failed with status: {status}\nStderr: {stderr_output}"
                    ));
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
                return Err(format!("Error checking process status: {e}"));
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
#[allow(dead_code)]
fn test_download_with_cargo_run(
    source: &str,
    timeout_secs: u64,
) -> Result<(String, String, bool), String> {
    let mut cmd = Command::new("cargo")
        .arg("run")
        .arg("--bin")
        .arg("butterfly-dl")
        .arg("--")
        .arg(source)
        .arg({
            let temp_dir = if cfg!(windows) {
                std::env::temp_dir()
            } else {
                std::path::PathBuf::from("/tmp")
            };
            temp_dir
                .join(format!("test-{}.pbf", source.replace('/', "_")))
                .to_string_lossy()
                .to_string()
        })
        .arg("--verbose")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("Failed to spawn command: {e}"))?;

    // Wait for timeout or process completion
    let start = Instant::now();
    let mut success = false;
    let mut stdout_output = String::new();
    let mut stderr_output = String::new();

    while start.elapsed() < Duration::from_secs(timeout_secs) {
        match cmd.try_wait() {
            Ok(Some(status)) => {
                // Process completed
                let output = cmd
                    .wait_with_output()
                    .map_err(|e| format!("Failed to get output: {e}"))?;

                stdout_output = String::from_utf8_lossy(&output.stdout).to_string();
                stderr_output = String::from_utf8_lossy(&output.stderr).to_string();

                if status.success() {
                    success = true;
                } else {
                    return Err(format!(
                        "Process failed with status: {status}\nStderr: {stderr_output}"
                    ));
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
                return Err(format!("Error checking process status: {e}"));
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
                assert!(
                    stderr.contains("Downloading planet")
                        || stderr.contains("HTTP")
                        || stderr.contains("planet"),
                    "Expected planet download indicators in stderr: {stderr}"
                );
            } else {
                panic!("Planet download failed to start successfully");
            }
        }
        Err(e) => {
            panic!("Planet download test failed: {e}");
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
                assert!(
                    stderr.contains("Downloading europe")
                        || stderr.contains("HTTP")
                        || stderr.contains("geofabrik"),
                    "Expected Europe download indicators in stderr: {stderr}"
                );
            } else {
                panic!("Europe download failed to start successfully");
            }
        }
        Err(e) => {
            panic!("Europe download test failed: {e}");
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
                assert!(
                    stderr.contains("Downloading europe/monaco")
                        || stderr.contains("HTTP")
                        || stderr.contains("geofabrik"),
                    "Expected Monaco download indicators in stderr: {stderr}"
                );
            } else {
                panic!("Monaco download failed to start successfully");
            }
        }
        Err(e) => {
            panic!("Monaco download test failed: {e}");
        }
    }
}

#[test]
#[cfg(not(feature = "ci-tests-disabled"))]
fn test_invalid_continent_fails_gracefully() {
    println!("Testing invalid continent (invalid-continent) fails gracefully...");

    match test_download_starts("invalid-continent", 5) {
        Ok((stdout, stderr, success)) => {
            println!("Invalid continent test completed:");
            println!("Stdout: {}", stdout);
            println!("Stderr: {}", stderr);

            // For invalid continent, either the process should fail (success=false)
            // OR it should show error messages in stderr even if it runs briefly
            if success {
                // If success=true, stderr should contain error indicators
                assert!(
                    stderr.contains("404")
                        || stderr.contains("not found")
                        || stderr.contains("HttpError")
                        || stderr.contains("error"),
                    "Expected error indicators in stderr for invalid continent: {stderr}"
                );
            } else {
                // If success=false, that's the expected behavior
                assert!(
                    stderr.contains("404")
                        || stderr.contains("not found")
                        || stderr.contains("HttpError"),
                    "Expected 404 or not found error for invalid continent: {stderr}"
                );
            }
        }
        Err(e) => {
            // This is expected - invalid continent should fail.
            // Geofabrik may return 404, a redirect to an HTML error page,
            // or fail to provide Content-Length (causing "Could not determine file size").
            println!("Invalid continent correctly failed: {e}");
            assert!(
                e.contains("404")
                    || e.contains("not found")
                    || e.contains("Could not determine file size")
                    || e.contains("HTTP error"),
                "Expected HTTP error for invalid continent: {e}"
            );
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

            assert!(
                success,
                "Antarctica download should succeed since it's a valid Geofabrik continent"
            );
            assert!(
                stderr.contains("Downloading from HTTP"),
                "Should show HTTP download source"
            );
            assert!(
                stderr.contains("antarctica"),
                "Should reference antarctica in output"
            );
        }
        Err(e) => {
            panic!("Antarctica download test failed: {e}");
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
                assert!(
                    stderr.contains("Downloading europe/belgium")
                        || stderr.contains("HTTP")
                        || stderr.contains("geofabrik"),
                    "Expected Belgium download indicators in stderr: {stderr}"
                );
            } else {
                panic!("Belgium download failed to start successfully");
            }
        }
        Err(e) => {
            panic!("Belgium download test failed: {e}");
        }
    }
}

/// Test dry run mode for all source types
#[test]
#[cfg(not(feature = "ci-tests-disabled"))]
fn test_dry_run_mode() {
    println!("Testing dry run mode for different sources...");

    // Use the same binary detection logic as other tests
    let binary_name = if cfg!(windows) {
        "butterfly-dl.exe"
    } else {
        "butterfly-dl"
    };

    // Calculate workspace root (two levels up from package dir)
    let workspace_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let debug_binary = workspace_root
        .join("target")
        .join("debug")
        .join(binary_name);
    let release_binary = workspace_root
        .join("target")
        .join("release")
        .join(binary_name);

    let binary_path = if debug_binary.exists() {
        debug_binary.to_string_lossy().to_string()
    } else if release_binary.exists() {
        release_binary.to_string_lossy().to_string()
    } else {
        // Build the binary first to avoid chicken-and-egg problem
        let build_output = Command::new("cargo")
            .args(["build", "--bin", "butterfly-dl"])
            .current_dir(workspace_root)
            .output()
            .expect("Failed to build butterfly-dl binary");

        if !build_output.status.success() {
            panic!(
                "Failed to build butterfly-dl: {}",
                String::from_utf8_lossy(&build_output.stderr)
            );
        }

        debug_binary.to_string_lossy().to_string()
    };

    let sources = ["planet", "europe", "europe/monaco", "europe/belgium"];

    for source in &sources {
        let output = Command::new(&binary_path)
            .arg(source)
            .arg("--dry-run")
            .output()
            .expect("Failed to run dry-run command");

        let stderr = String::from_utf8_lossy(&output.stderr);
        println!("Dry run for {source}: {stderr}");

        assert!(
            output.status.success(),
            "Dry run should succeed for {source}"
        );
        assert!(
            stderr.contains("DRY RUN"),
            "Expected DRY RUN indicator for {source}"
        );
        assert!(
            stderr.contains(source),
            "Expected source name in output for {source}"
        );
    }
}

/// Cleanup function to remove any test files
#[cfg(test)]
mod cleanup {
    use std::fs;

    #[ctor::dtor]
    fn cleanup() {
        let temp_dir = if cfg!(windows) {
            std::env::temp_dir()
        } else {
            std::path::PathBuf::from("/tmp")
        };

        let _ = fs::remove_file(temp_dir.join("test-planet.pbf"));
        let _ = fs::remove_file(temp_dir.join("test-europe.pbf"));
        let _ = fs::remove_file(temp_dir.join("test-europe_monaco.pbf"));
        let _ = fs::remove_file(temp_dir.join("test-europe_belgium.pbf"));
        let _ = fs::remove_file(temp_dir.join("test-antarctica.pbf"));
    }
}
