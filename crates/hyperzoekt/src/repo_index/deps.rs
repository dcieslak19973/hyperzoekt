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
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dependency {
    pub name: String,
    pub version: Option<String>,
    pub language: String,
}

/// Collect known dependency files in the repository root
pub fn collect_dependency_files(repo_root: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let candidates = vec![
        "requirements.txt",
        "pyproject.toml",
        "Pipfile",
        "Cargo.toml",
    ];
    for cand in candidates {
        let p = repo_root.join(cand);
        if p.exists() {
            out.push(p);
        }
    }
    out
}

/// Parse requirements.txt (very small parser: lines `name==version` or `name>=version`)
pub fn parse_requirements_txt(path: &Path) -> Vec<Dependency> {
    let s = fs::read_to_string(path).unwrap_or_default();
    let mut deps = Vec::new();
    for line in s.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with('-') {
            continue;
        }
        // Split on common operators
        let mut name = line.to_string();
        let mut version = None;
        for sep in &["==", ">=", "<=", "~=", "!=", ">", "<"] {
            if let Some(pos) = line.find(sep) {
                name = line[..pos].trim().to_string();
                version = Some(line[pos + sep.len()..].trim().to_string());
                break;
            }
        }
        deps.push(Dependency {
            name,
            version,
            language: "python".to_string(),
        });
    }
    deps
}

/// Parse Cargo.toml dependencies from the [dependencies] table (uses `toml` crate)
pub fn parse_cargo_toml(path: &Path) -> Vec<Dependency> {
    let s = fs::read_to_string(path).unwrap_or_default();
    let mut deps = Vec::new();
    if let Ok(value) = s.parse::<toml::Value>() {
        if let Some(table) = value.get("dependencies") {
            if let Some(tbl) = table.as_table() {
                for (k, v) in tbl.iter() {
                    let mut version = None;
                    if v.is_str() {
                        version = v.as_str().map(|s| s.to_string());
                    } else if v.is_table() {
                        if let Some(ver) = v.get("version") {
                            version = ver.as_str().map(|s| s.to_string());
                        }
                    }
                    deps.push(Dependency {
                        name: k.clone(),
                        version,
                        language: "rust".to_string(),
                    });
                }
            }
        }
        // also check [dev-dependencies]
        if let Some(table) = value.get("dev-dependencies") {
            if let Some(tbl) = table.as_table() {
                for (k, v) in tbl.iter() {
                    let mut version = None;
                    if v.is_str() {
                        version = v.as_str().map(|s| s.to_string());
                    } else if v.is_table() {
                        if let Some(ver) = v.get("version") {
                            version = ver.as_str().map(|s| s.to_string());
                        }
                    }
                    deps.push(Dependency {
                        name: k.clone(),
                        version,
                        language: "rust".to_string(),
                    });
                }
            }
        }
    }
    deps
}

/// High-level collector: returns list of dependencies found in repo_root
pub fn collect_dependencies(repo_root: &Path) -> Vec<Dependency> {
    let mut out = Vec::new();
    for f in collect_dependency_files(repo_root) {
        if let Some(name) = f.file_name().and_then(|s| s.to_str()) {
            if name.eq_ignore_ascii_case("requirements.txt") || name.eq_ignore_ascii_case("Pipfile")
            {
                out.extend(parse_requirements_txt(&f));
            } else if name.eq_ignore_ascii_case("cargo.toml")
                || name.eq_ignore_ascii_case("Cargo.toml")
            {
                out.extend(parse_cargo_toml(&f));
            } else if name.eq_ignore_ascii_case("pyproject.toml") {
                // naive parse: look for [project] or [tool.poetry] metadata -> optional
                // skip for now
            }
        }
    }
    out
}
