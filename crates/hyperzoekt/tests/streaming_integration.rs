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
