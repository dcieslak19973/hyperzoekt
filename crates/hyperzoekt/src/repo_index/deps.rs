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
        "build.gradle",
        "build.gradle.kts",
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

/// Parse Gradle build files (Groovy DSL). Uses zoekt-rs typesitter extractor
/// to perform static extraction of declared dependencies. Returns Dependencies
/// with `language` set to "java" for Groovy/Kotlin Gradle builds.
pub fn parse_gradle_build_file(path: &Path) -> Vec<Dependency> {
    let s = fs::read_to_string(path).unwrap_or_default();
    let mut out: Vec<Dependency> = Vec::new();

    // Use the zoekt-rs extractor when available (path dependency in workspace)
    let extracted = zoekt_rs::typesitter::extract_gradle_dependencies_typesitter(&s);
    for d in extracted {
        // clone fields we will use to avoid moving out of `d`
        let group = d.group.clone();
        let name_field = d.name.clone();
        let notation = d.notation.clone();
        let version = d.version.clone();

        // default name: prefer declared name or fallback to notation
        let mut name = name_field.clone().unwrap_or_else(|| notation.clone());

        // If external and we have group + name, prefer group:name
        if matches!(d.kind, zoekt_rs::typesitter::GradleDepKind::External) {
            if let (Some(g), Some(n)) = (group, name_field) {
                name = format!("{}:{}", g, n);
            }
        }

        // prefer kotlin language tag for `.kts` files
        let lang_tag = match path.extension().and_then(|s| s.to_str()) {
            Some(ext) if ext.eq_ignore_ascii_case("kts") => "kotlin",
            _ => "java",
        };

        out.push(Dependency {
            name,
            version,
            language: lang_tag.to_string(),
        });
    }
    out
}

/// Parse a Gradle version catalog (libs.versions.toml) and return a mapping
/// from alias -> (group, name, Option<version>) when available.
pub fn parse_gradle_version_catalog(
    path: &Path,
) -> std::collections::HashMap<String, (String, String, Option<String>)> {
    let mut out = std::collections::HashMap::new();
    let s = fs::read_to_string(path).unwrap_or_default();
    if let Ok(value) = s.parse::<toml::Value>() {
        // versions table for resolving version.ref
        let versions = value.get("versions").and_then(|v| v.as_table()).cloned();
        if let Some(libs) = value.get("libraries").and_then(|v| v.as_table()) {
            for (k, v) in libs.iter() {
                // v can be a table or string; handle common shapes
                if let Some(m) = v.as_str() {
                    // module string like "group:name[:version]"
                    let parts: Vec<&str> = m.split(':').collect();
                    if parts.len() >= 2 {
                        let group = parts[0].to_string();
                        let name = parts[1].to_string();
                        let version = if parts.len() >= 3 {
                            Some(parts[2].to_string())
                        } else {
                            None
                        };
                        out.insert(k.clone(), (group, name, version));
                    }
                } else if let Some(tbl) = v.as_table() {
                    // module field
                    if let Some(module_val) = tbl.get("module").and_then(|x| x.as_str()) {
                        let parts: Vec<&str> = module_val.split(':').collect();
                        if parts.len() >= 2 {
                            let group = parts[0].to_string();
                            let name = parts[1].to_string();
                            let version = if parts.len() >= 3 {
                                Some(parts[2].to_string())
                            } else if let Some(ver) = tbl.get("version").and_then(|x| x.as_str()) {
                                Some(ver.to_string())
                            } else if let Some(ver_ref) =
                                tbl.get("version.ref").and_then(|x| x.as_str())
                            {
                                // resolve from versions table
                                ver_ref.split('.').next().and_then(|ref_key| {
                                    versions
                                        .as_ref()
                                        .and_then(|vt| vt.get(ref_key))
                                        .and_then(|vv| vv.as_str())
                                        .map(|s| s.to_string())
                                })
                            } else {
                                None
                            };
                            out.insert(k.clone(), (group, name, version));
                            continue;
                        }
                    }

                    // group/name fields
                    let group_val = tbl
                        .get("group")
                        .and_then(|x| x.as_str())
                        .map(|s| s.to_string());
                    let name_val = tbl
                        .get("name")
                        .and_then(|x| x.as_str())
                        .map(|s| s.to_string());
                    let version = tbl
                        .get("version")
                        .and_then(|x| x.as_str())
                        .map(|s| s.to_string())
                        .or_else(|| {
                            tbl.get("version.ref")
                                .and_then(|x| x.as_str())
                                .and_then(|ref_key| {
                                    versions
                                        .as_ref()
                                        .and_then(|vt| vt.get(ref_key))
                                        .and_then(|vv| vv.as_str())
                                        .map(|s| s.to_string())
                                })
                        });
                    if let (Some(g), Some(n)) = (group_val, name_val) {
                        out.insert(k.clone(), (g, n, version));
                    }
                }
            }
        }
    }
    out
}

/// High-level collector: returns list of dependencies found in repo_root
pub fn collect_dependencies(repo_root: &Path) -> Vec<Dependency> {
    let mut out = Vec::new();
    // Try to locate version catalog: gradle/libs.versions.toml
    let mut catalog_map = std::collections::HashMap::new();
    let catalog1 = repo_root.join("gradle").join("libs.versions.toml");
    let catalog2 = repo_root.join("libs.versions.toml");
    if catalog1.exists() {
        catalog_map = parse_gradle_version_catalog(&catalog1);
    } else if catalog2.exists() {
        catalog_map = parse_gradle_version_catalog(&catalog2);
    }
    for f in collect_dependency_files(repo_root) {
        if let Some(name) = f.file_name().and_then(|s| s.to_str()) {
            if name.eq_ignore_ascii_case("requirements.txt") || name.eq_ignore_ascii_case("Pipfile")
            {
                out.extend(parse_requirements_txt(&f));
            } else if name.eq_ignore_ascii_case("cargo.toml")
                || name.eq_ignore_ascii_case("Cargo.toml")
            {
                out.extend(parse_cargo_toml(&f));
            } else if name.eq_ignore_ascii_case("build.gradle")
                || name.eq_ignore_ascii_case("build.gradle.kts")
            {
                // parse gradle file and then attempt to resolve any catalog aliases
                let mut parsed = parse_gradle_build_file(&f);
                // resolve libs.xxx names using catalog_map (if entry name looks like foo.bar or libs.foo.bar)
                for p in parsed.iter_mut() {
                    if p.name.starts_with("libs.") {
                        let alias = p.name.trim_start_matches("libs.");
                        if let Some((g, n, ver)) = catalog_map.get(alias) {
                            p.name = format!("{}:{}", g, n);
                            if p.version.is_none() {
                                p.version = ver.clone();
                            }
                        }
                    } else if catalog_map.contains_key(&p.name) {
                        if let Some((g, n, ver)) = catalog_map.get(&p.name) {
                            p.name = format!("{}:{}", g, n);
                            if p.version.is_none() {
                                p.version = ver.clone();
                            }
                        }
                    }
                }
                out.extend(parsed);
            } else if name.eq_ignore_ascii_case("pyproject.toml") {
                // naive parse: look for [project] or [tool.poetry] metadata -> optional
                // skip for now
            }
        }
    }
    out
}
