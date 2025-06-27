use std::process::Command;

#[test]
fn test_cli_help_works() {
    let output = Command::new("cargo")
        .args(["run", "--bin", "butterfly-shrink", "--", "--help"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success(), "Command should exit successfully");
    
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("butterfly-shrink"), "Help output should contain program name");
    assert!(stdout.contains("Usage:"), "Help output should contain usage information");
}

#[test]
fn test_cli_version_works() {
    let output = Command::new("cargo")
        .args(["run", "--bin", "butterfly-shrink", "--", "--version"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success(), "Version command should exit successfully");
    
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("2.0.0"), "Version output should contain version number");
}

#[test]
fn test_cli_basic_functionality() {
    let output = Command::new("cargo")
        .args(["run", "--bin", "butterfly-shrink", "--", "--name", "test"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success(), "Basic command should exit successfully");
    
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Hello, test!"), "Output should contain greeting with provided name");
}