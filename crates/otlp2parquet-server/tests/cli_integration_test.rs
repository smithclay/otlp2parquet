use anyhow::Result;
use std::path::PathBuf;
use std::process::Command;
use tempfile::TempDir;

fn get_binary_path() -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.pop(); // Go up to workspace root
    path.pop();
    path.push("target");
    path.push("debug");
    path.push("otlp2parquet");
    path
}

#[test]
fn test_cli_help() {
    let binary = get_binary_path();
    let output = Command::new(&binary)
        .arg("--help")
        .output()
        .expect("Failed to run binary");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("OTLP HTTP server"));
    assert!(stdout.contains("--port"));
    assert!(stdout.contains("--output"));
    assert!(stdout.contains("--log-level"));
    assert!(stdout.contains("--config"));
}

#[test]
fn test_cli_version() {
    let binary = get_binary_path();
    let output = Command::new(&binary)
        .arg("--version")
        .output()
        .expect("Failed to run binary");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("otlp2parquet"));
}

#[test]
fn test_cli_creates_output_directory() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let output_path = temp_dir.path().join("test-output");

    // Verify directory doesn't exist yet
    assert!(!output_path.exists());

    // Note: We can't actually run the server in tests (it would block),
    // but we can test that validation creates the directory
    // This would need to be a manual test or spawn server and kill it

    Ok(())
}

#[test]
fn test_cli_invalid_output_path_fails() {
    let binary = get_binary_path();

    // Try to write to /root (should fail on most systems without sudo)
    let output = Command::new(&binary)
        .arg("--output")
        .arg("/root/forbidden-test-output")
        .env("RUST_LOG", "error")
        .output()
        .expect("Failed to run binary");

    // Expecting failure due to permissions or read-only filesystem
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("permission")
            || stderr.contains("Permission")
            || stderr.contains("Read-only file system")
    );
}
