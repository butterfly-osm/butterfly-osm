use std::path::PathBuf;
use std::process::Command;

/// Get the path to the butterfly-shrink binary, building it if necessary
fn get_butterfly_shrink_binary() -> PathBuf {
    // Determine binary name based on platform
    let binary_name = if cfg!(windows) {
        "butterfly-shrink.exe"
    } else {
        "butterfly-shrink"
    };

    // Calculate workspace root (two levels up from package dir)
    let workspace_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();

    // Check for debug and release binaries
    let debug_binary = workspace_root
        .join("target")
        .join("debug")
        .join(binary_name);
    let release_binary = workspace_root
        .join("target")
        .join("release")
        .join(binary_name);

    // Use existing binary if available
    if debug_binary.exists() {
        return debug_binary;
    } else if release_binary.exists() {
        return release_binary;
    }

    // Build the binary if it doesn't exist
    let output = Command::new("cargo")
        .args(["build", "--bin", "butterfly-shrink"])
        .current_dir(workspace_root)
        .output()
        .expect("Failed to build butterfly-shrink binary");

    if !output.status.success() {
        panic!(
            "Failed to build butterfly-shrink: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    // Return path to the built binary (should be in debug after build)
    debug_binary
}

#[test]
fn test_cli_help_works() {
    let binary_path = get_butterfly_shrink_binary();

    let output = Command::new(&binary_path)
        .arg("--help")
        .output()
        .expect("Failed to execute butterfly-shrink --help");

    assert!(
        output.status.success(),
        "Help command should exit successfully"
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("butterfly-shrink"),
        "Help output should contain program name"
    );
    assert!(
        stdout.contains("Usage:"),
        "Help output should contain usage information"
    );
}

#[test]
fn test_cli_version_works() {
    let binary_path = get_butterfly_shrink_binary();

    let output = Command::new(&binary_path)
        .arg("--version")
        .output()
        .expect("Failed to execute butterfly-shrink --version");

    assert!(
        output.status.success(),
        "Version command should exit successfully"
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("2.0.0"),
        "Version output should contain version number"
    );
}

#[test]
fn test_cli_basic_functionality() {
    let binary_path = get_butterfly_shrink_binary();

    let output = Command::new(&binary_path)
        .args(["--name", "test"])
        .output()
        .expect("Failed to execute butterfly-shrink --name test");

    assert!(
        output.status.success(),
        "Basic command should exit successfully"
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Hello, test!"),
        "Output should contain greeting with provided name"
    );
}
