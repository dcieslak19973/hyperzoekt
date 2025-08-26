use assert_cmd::Command;
use std::fs;
use tempfile::tempdir;

#[test]
fn stream_once_ingests_to_db() -> Result<(), Box<dyn std::error::Error>> {
    // Create a small temp repo with a single file
    let dir = tempdir()?;
    let file_path = dir.path().join("hello.rs");
    fs::write(&file_path, "fn hello() { println!(\"hi\"); }\n")?;

    // Run the binary with --stream-once and root pointing at the temp file
    let mut cmd = Command::cargo_bin("hyperzoekt")?;
    cmd.arg("--stream-once").arg("--root").arg(&file_path);
    cmd.env("RUST_LOG", "info");

    // The binary should exit successfully
    cmd.assert().success();
    Ok(())
}
