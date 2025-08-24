use assert_cmd::Command;
use std::fs;
use tempfile::tempdir;

#[test]
fn debug_mode_writes_jsonl() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let cfg_path = dir.path().join("debug_test.toml");
    let out_dir = dir.path().join("out");
    fs::create_dir_all(&out_dir)?;
    let out_file = out_dir.join("test_out.jsonl");
    let toml = format!(
        r#"debug = true
out = "{}"
root = "."
"#,
        out_file.display()
    );
    fs::write(&cfg_path, toml)?;

    // Run the binary built in workspace
    Command::cargo_bin("hyperzoekt")?
        .arg("--config")
        .arg(cfg_path.as_os_str())
        .assert()
        .success();

    let contents = fs::read_to_string(out_file)?;
    assert!(
        !contents.trim().is_empty(),
        "expected non-empty jsonl output"
    );
    Ok(())
}
