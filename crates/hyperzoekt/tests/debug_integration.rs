// Copyright 2025 HyperZoekt Project
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
