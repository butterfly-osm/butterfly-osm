use assert_cmd::Command;
use predicates::prelude::*;
use std::fs;
use std::path::PathBuf;
use tempfile::tempdir;

/// Get test data directory
fn get_test_data_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("data")
}

/// Download Monaco PBF file if it doesn't exist
fn ensure_monaco_pbf() -> PathBuf {
    let test_data_dir = get_test_data_dir();
    fs::create_dir_all(&test_data_dir).expect("Failed to create test data directory");

    let monaco_file = test_data_dir.join("monaco.pbf");

    // If file doesn't exist, download it using butterfly-dl
    if !monaco_file.exists() {
        println!("Downloading Monaco PBF file for tests...");

        // Try to find butterfly-dl binary
        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let workspace_root = manifest_dir
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .to_path_buf();

        let debug_bin = workspace_root.join("target/debug/butterfly-dl");
        let release_bin = workspace_root.join("target/release/butterfly-dl");

        let butterfly_dl = if debug_bin.exists() {
            debug_bin
        } else if release_bin.exists() {
            release_bin
        } else {
            // Build butterfly-dl if it doesn't exist
            println!("Building butterfly-dl for test data download...");
            let build_output = std::process::Command::new("cargo")
                .args(["build", "--package", "butterfly-dl"])
                .current_dir(&workspace_root)
                .output()
                .expect("Failed to build butterfly-dl");

            if !build_output.status.success() {
                panic!(
                    "Failed to build butterfly-dl: {}",
                    String::from_utf8_lossy(&build_output.stderr)
                );
            }

            workspace_root.join("target/debug/butterfly-dl")
        };

        // Download Monaco
        let output = std::process::Command::new(&butterfly_dl)
            .args(["europe/monaco", monaco_file.to_str().unwrap()])
            .output()
            .expect("Failed to run butterfly-dl");

        if !output.status.success() {
            panic!(
                "Failed to download Monaco PBF: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }

        println!("Monaco PBF downloaded successfully");
    }

    monaco_file
}

/// Calculate MD5 hash of a file
fn md5_file(path: &PathBuf) -> String {
    let contents = fs::read(path).expect("Failed to read file for MD5");
    format!("{:x}", md5::compute(contents))
}

#[test]
fn test_cli_help_works() {
    let mut cmd = Command::cargo_bin("butterfly-shrink").unwrap();
    cmd.arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("butterfly-shrink"))
        .stdout(predicate::str::contains(
            "A tool to shrink OpenStreetMap data",
        ));
}

#[test]
fn test_cli_version_works() {
    let mut cmd = Command::cargo_bin("butterfly-shrink").unwrap();
    cmd.arg("--version")
        .assert()
        .success()
        .stdout(predicate::str::contains(env!("CARGO_PKG_VERSION")));
}

#[test]
fn test_echo_roundtrip() {
    let monaco_pbf = ensure_monaco_pbf();
    let temp_dir = tempdir().unwrap();
    let output_file = temp_dir.path().join("echo.pbf");

    // Run butterfly-shrink to echo the file
    let mut cmd = Command::cargo_bin("butterfly-shrink").unwrap();
    cmd.args([monaco_pbf.to_str().unwrap(), output_file.to_str().unwrap()])
        .assert()
        .success();

    // Verify files are identical
    assert_eq!(
        md5_file(&monaco_pbf),
        md5_file(&output_file),
        "Echo output should be bitwise identical to input"
    );
}

#[test]
fn test_missing_input_file() {
    let temp_dir = tempdir().unwrap();
    let output_file = temp_dir.path().join("output.pbf");

    let mut cmd = Command::cargo_bin("butterfly-shrink").unwrap();
    cmd.args(["nonexistent.pbf", output_file.to_str().unwrap()])
        .assert()
        .failure()
        .stderr(predicate::str::contains("Input file not found"));
}
