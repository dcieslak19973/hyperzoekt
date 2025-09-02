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

use serde::Deserialize;

#[derive(Deserialize)]
struct LocalAppConfig {
    debug: Option<bool>,
    channel_capacity: Option<usize>,
}

#[test]
fn appconfig_load_from_file() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let cfg_path = dir.path().join("test_config.toml");
    let toml = r#"
    debug = true
    out = ".data/test_out.jsonl"
    channel_capacity = 42
    "#;
    std::fs::write(&cfg_path, toml)?;
    let s = std::fs::read_to_string(&cfg_path)?;
    let cfg: LocalAppConfig = toml::from_str(&s)?;
    assert!(cfg.debug.unwrap_or(false));
    assert_eq!(cfg.channel_capacity.unwrap_or(0), 42);
    Ok(())
}
