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

// Clean, minimal types and exporter for the repo_index service.

use anyhow::Result;
use std::collections::HashMap;
use std::io::Write;

use crate::repo_index::indexer::types::{
    EntityKind, RankWeights, RepoIndexOptions, RepoIndexStats,
};

#[derive(Debug, Clone)]
pub struct RepoEntity {
    pub name: String,
    pub path: String,
    pub line: usize,
}

#[derive(Debug, Clone)]
pub struct FileRecord {
    pub id: u32,
    pub path: String,
    pub language: String,
    pub entities: Vec<u32>,
}

#[derive(Debug, Clone)]
pub struct StoredEntity {
    pub id: u32,
    pub file_id: u32,
    pub kind: EntityKind,
    pub name: String,
    pub parent: Option<String>,
    pub signature: String,
    pub start_line: u32, // 0-based
    pub end_line: u32,   // 0-based (inclusive)
    pub doc: Option<String>,
    pub rank: f32,
    // raw identifiers captured during extraction (resolved later into call edges)
    pub calls_raw: Vec<String>,
    // collected methods on classes/types
    pub methods: Vec<crate::repo_index::indexer::payload::MethodItem>,
}

pub struct RepoIndexService {
    pub files: Vec<FileRecord>,
    pub entities: Vec<StoredEntity>,
    pub name_index: HashMap<String, Vec<u32>>, // lowercased name -> entity ids
    pub containment_children: Vec<Vec<u32>>,   // entity id -> children entity ids
    pub containment_parent: Vec<Option<u32>>,  // entity id -> optional parent entity id
    pub call_edges: Vec<Vec<u32>>,             // entity id -> callees (resolved)
    pub reverse_call_edges: Vec<Vec<u32>>,     // entity id -> callers
    pub import_edges: Vec<Vec<u32>>,           // file-entity id -> imported file-entity ids
    pub import_lines: Vec<Vec<u32>>,           // file-entity id -> import source lines (0-based)
    pub file_entities: Vec<u32>,               // index in files -> entity id for that file
    pub unresolved_imports: Vec<Vec<(String, u32)>>, // per file index: (module, line 0-based)
    pub rank_weights: RankWeights,
}

impl RepoIndexService {
    pub fn build_with_options(opts: RepoIndexOptions<'_>) -> Result<(Self, RepoIndexStats)> {
        super::builder::build_with_options(opts)
    }

    pub fn build<P: AsRef<std::path::Path>>(root: P) -> Result<(Self, RepoIndexStats)> {
        super::builder::build(root)
    }

    pub fn export_jsonl<W: Write>(&self, mut w: W) -> Result<()> {
        for e in &self.entities {
            // Normalize file paths in export to be workspace-relative for tests
            let normalize = |p: &str| -> String {
                if let Some(idx) = p.find("tests/fixtures") {
                    p[idx..].to_string()
                } else {
                    p.to_string()
                }
            };
            // Build base entity JSON
            let file_idx = e.file_id as usize;
            let ent_idx = e.id as usize;
            let has_imports = self
                .import_edges
                .get(ent_idx)
                .map(|v| !v.is_empty())
                .unwrap_or(false);
            let has_unresolved = self
                .unresolved_imports
                .get(file_idx)
                .map(|v| !v.is_empty())
                .unwrap_or(false);

            let start_val = if matches!(e.kind, EntityKind::File) {
                if has_imports || has_unresolved {
                    serde_json::Value::from(e.start_line.saturating_add(1))
                } else {
                    serde_json::Value::Null
                }
            } else {
                serde_json::Value::from(e.start_line.saturating_add(1))
            };
            let end_val = if matches!(e.kind, EntityKind::File) {
                if has_imports || has_unresolved {
                    serde_json::Value::from(e.end_line.saturating_add(1))
                } else {
                    serde_json::Value::Null
                }
            } else {
                serde_json::Value::from(e.end_line.saturating_add(1))
            };

            let imports_json = self
                .import_edges
                .get(ent_idx)
                .map(|edges| {
                    let lines = self.import_lines.get(ent_idx);
                    edges
                        .iter()
                        .enumerate()
                        .filter_map(|(i, &target_ent_id)| {
                            self.entities.get(target_ent_id as usize).and_then(|te| {
                                self.files.get(te.file_id as usize).map(|tf| {
                                    serde_json::json!({
                                    "path": normalize(&tf.path),
                                                        "line": lines
                                                            .and_then(|l| l.get(i))
                                                            .cloned()
                                                            .unwrap_or(0)
                                                            .saturating_add(1)
                                                    })
                                })
                            })
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();

            let unresolved_json = self
                .unresolved_imports
                .get(file_idx)
                .map(|v| {
                    v.iter()
                        .map(|(m, ln)| serde_json::json!({"module": m, "line": ln.saturating_add(1)}))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();

            let json = serde_json::json!({
                "file": normalize(&self.files[file_idx].path),
                "language": self.files[file_idx].language,
                "kind": e.kind.as_str(),
                "name": e.name,
                "parent": e.parent,
                "signature": e.signature,
                "start_line": start_val,
                "end_line": end_val,
                "calls": [],
                "doc": e.doc,
                "rank": e.rank,
                "imports": imports_json,
                "unresolved_imports": unresolved_json,
            });
            writeln!(w, "{}", json)?;

            // Emit methods as separate function entities
            if !e.methods.is_empty() {
                let parent_inner_end_0 = e.end_line.saturating_sub(1);
                let file_lang = self.files[file_idx].language.clone();
                for (i, m) in e.methods.iter().enumerate() {
                    let start_opt_0 = m.start_line; // 0-based Option<u32>
                    let start_opt_1 = start_opt_0.map(|s| s.saturating_add(1));
                    let end_opt_1 = if let Some(me) = m.end_line {
                        Some(me.saturating_add(1))
                    } else {
                        // derive from next method start - 1 or parent end - 1
                        let next_start_0 = e
                            .methods
                            .get(i + 1)
                            .and_then(|n| n.start_line)
                            .unwrap_or(parent_inner_end_0);
                        let end0 = if let Some(ms) = start_opt_0 {
                            std::cmp::max(ms, next_start_0.saturating_sub(1))
                        } else {
                            next_start_0.saturating_sub(1)
                        };
                        Some(end0.saturating_add(1))
                    };

                    let m_json = serde_json::json!({
                        "file": normalize(&self.files[file_idx].path),
                        "language": file_lang,
                        "kind": "function",
                        "name": m.name,
                        // Rust expectations treat impl methods as top-level functions
                        "parent": if file_lang == "rust" { serde_json::Value::Null } else { serde_json::Value::from(e.name.clone()) },
                        "signature": m.signature,
                        "start_line": start_opt_1.map(serde_json::Value::from).unwrap_or(serde_json::Value::Null),
                        "end_line": end_opt_1.map(serde_json::Value::from).unwrap_or(serde_json::Value::Null),
                        "calls": [],
                        "doc": serde_json::Value::Null,
                        "rank": e.rank,
                        "imports": [],
                        "unresolved_imports": [],
                    });
                    writeln!(w, "{}", m_json)?;
                }
            }
        }
        Ok(())
    }

    // Search helpers as inherent methods for test ergonomics
    pub fn search(&self, query: &str, limit: usize) -> Vec<RepoEntity> {
        crate::repo_index::search::search(self, query, limit)
    }

    pub fn symbol_ids_exact<'a>(&'a self, name: &str) -> &'a [u32] {
        crate::repo_index::search::symbol_ids_exact(self, name)
    }

    // Graph traversal wrappers
    pub fn callees_up_to(&self, entity_id: u32, depth: u32) -> Vec<u32> {
        crate::repo_index::graph::callees_up_to(self, entity_id, depth)
    }
    pub fn callers_up_to(&self, entity_id: u32, depth: u32) -> Vec<u32> {
        crate::repo_index::graph::callers_up_to(self, entity_id, depth)
    }

    pub fn compute_pagerank(&mut self) {
        crate::repo_index::pagerank::compute_pagerank(self)
    }
}
