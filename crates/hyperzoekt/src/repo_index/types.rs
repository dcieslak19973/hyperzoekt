// content moved from service/types.rs

use crate::internal::{RepoIndexOptions, RepoIndexStats};
use anyhow::Result;
use serde::Serialize;
use std::collections::HashMap;
use std::io::Write;

#[derive(Debug, Clone, Serialize)]
pub struct RepoEntity {
    pub name: String,
    pub path: String,
    pub line: usize,
}

#[derive(Debug)]
pub struct FileRecord {
    pub id: u32,
    pub path: String,
    pub language: String,
    pub entities: Vec<u32>,
}

#[derive(Debug)]
pub struct StoredEntity {
    pub id: u32,
    pub file_id: u32,
    pub kind: crate::internal::EntityKind,
    pub name: String,
    pub parent: Option<String>,
    pub signature: String,
    pub start_line: u32,
    pub end_line: u32,
    pub calls: Vec<String>,
    pub doc: Option<String>,
    pub rank: f32, // placeholder for future PageRank
}

impl StoredEntity {
    fn to_json(&self, files: &[FileRecord]) -> serde_json::Value {
        serde_json::json!({
            "file": files[self.file_id as usize].path,
            "language": files[self.file_id as usize].language,
            "kind": self.kind.as_str(),
            "name": self.name,
            "parent": self.parent,
            "signature": self.signature,
            "start_line": self.start_line,
            "end_line": self.end_line,
            "calls": self.calls,
            "doc": self.doc,
            "rank": self.rank,
        })
    }
}

// StdHashSet is not needed at top-level; modules use their own imports when required.

pub struct RepoIndexService {
    pub files: Vec<FileRecord>,
    pub entities: Vec<StoredEntity>,
    pub(crate) name_index: HashMap<String, Vec<u32>>, // lowercase name -> entity ids (crate visible)
    // Graph adjacency lists (indices reference entities vector)
    pub containment_children: Vec<Vec<u32>>, // entity id -> child entity ids
    pub containment_parent: Vec<Option<u32>>, // entity id -> optional parent entity id
    pub call_edges: Vec<Vec<u32>>,           // entity id -> outgoing calls (resolved entity ids)
    pub reverse_call_edges: Vec<Vec<u32>>,   // entity id -> incoming calls (callers)
    // import_edges: entity id -> list of target file-entity ids
    pub import_edges: Vec<Vec<u32>>,
    // import_lines: entity id -> parallel list of source line numbers for each import edge
    pub import_lines: Vec<Vec<u32>>,
    pub file_entities: Vec<u32>, // mapping file index -> file entity id
    // per-file unresolved imports as (module_basename, line)
    pub unresolved_imports: Vec<Vec<(String, u32)>>, // per file index unresolved module basenames with line numbers
    pub rank_weights: crate::internal::RankWeights,  // configured (possibly env overridden) weights
}

impl RepoIndexService {
    /// Detailed builder that accepts `RepoIndexOptions` for advanced usage.
    ///
    /// Returns the constructed service and some indexing statistics on success.
    pub fn build_with_options(opts: RepoIndexOptions<'_>) -> Result<(Self, RepoIndexStats)> {
        // Delegate to the extracted builder implementation in `repo_index::builder`.
        crate::repo_index::builder::build_with_options(opts)
    }

    /// NOTE: internal entity `start_line` and `end_line` values are stored 0-based
    /// (Tree-sitter rows). Export helpers such as `export_jsonl` and the CLI convert
    /// these to 1-based line numbers for IDE-friendly output. Keep internal logic
    /// working with 0-based values to avoid double-conversion.
    /// Backwards-compatible convenience constructor used by tests/callers.
    pub fn build<P: AsRef<std::path::Path>>(root: P) -> Result<(Self, RepoIndexStats)> {
        crate::repo_index::builder::build(root)
    }

    /// Search for exact identifier matches. Returns matching entities.
    pub fn search(&self, query: &str, limit: usize) -> Vec<RepoEntity> {
        crate::repo_index::search::search(self, query, limit)
    }

    pub fn search_symbol_exact(&self, name: &str) -> Vec<&StoredEntity> {
        crate::repo_index::search::search_symbol_exact(self, name)
    }

    pub fn search_symbol_fuzzy_ranked(&self, query: &str, limit: usize) -> Vec<&StoredEntity> {
        crate::repo_index::search::search_symbol_fuzzy_ranked(self, query, limit)
    }

    pub fn symbol_ids_exact(&self, name: &str) -> &[u32] {
        crate::repo_index::search::symbol_ids_exact(self, name)
    }

    pub fn children_of(&self, entity_id: u32) -> &[u32] {
        self.containment_children
            .get(entity_id as usize)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    pub fn parent_of(&self, entity_id: u32) -> Option<u32> {
        self.containment_parent
            .get(entity_id as usize)
            .and_then(|p| *p)
    }

    pub fn outgoing_calls(&self, entity_id: u32) -> &[u32] {
        self.call_edges
            .get(entity_id as usize)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    pub fn incoming_calls(&self, entity_id: u32) -> &[u32] {
        self.reverse_call_edges
            .get(entity_id as usize)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    pub fn imported_files(&self, file_entity_id: u32) -> &[u32] {
        self.import_edges
            .get(file_entity_id as usize)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }
    pub fn unresolved_imports_for_file_index(&self, file_index: usize) -> &[(String, u32)] {
        static EMPTY_PAIR_SLICE: &[(String, u32)] = &[];
        self.unresolved_imports
            .get(file_index)
            .map(|v| v.as_slice())
            .unwrap_or(EMPTY_PAIR_SLICE)
    }

    // (Removed duplicate legacy import heuristic block; enhanced version defined above)
    pub fn export_jsonl<W: Write>(&self, mut w: W) -> Result<()> {
        for e in &self.entities {
            let json = serde_json::json!({
                "file": self.files[e.file_id as usize].path,
                "language": self.files[e.file_id as usize].language,
                "kind": e.kind.as_str(),
                "name": e.name,
                "parent": e.parent,
                "signature": e.signature,
                // export 1-based line numbers; if this is a file pseudo-entity with no
                // imports and no unresolved imports, emit `null` to avoid misleading 1/1
                // values. Consumers should handle nulls as "not-applicable".
                "start_line": (if e.kind.as_str() == "file" {
                    let file_idx = self.files[e.file_id as usize].id as usize;
                    let has_imports = self.import_edges.get(e.id as usize).map(|v| !v.is_empty()).unwrap_or(false);
                    let has_unres = self.unresolved_imports.get(file_idx).map(|v| !v.is_empty()).unwrap_or(false);
                    if has_imports || has_unres {
                        serde_json::Value::from(e.start_line.saturating_add(1))
                    } else {
                        serde_json::Value::Null
                    }
                } else {
                    serde_json::Value::from(e.start_line.saturating_add(1))
                }),
                "end_line": (if e.kind.as_str() == "file" {
                    let file_idx = self.files[e.file_id as usize].id as usize;
                    let has_imports = self.import_edges.get(e.id as usize).map(|v| !v.is_empty()).unwrap_or(false);
                    let has_unres = self.unresolved_imports.get(file_idx).map(|v| !v.is_empty()).unwrap_or(false);
                    if has_imports || has_unres {
                        serde_json::Value::from(e.end_line.saturating_add(1))
                    } else {
                        serde_json::Value::Null
                    }
                } else {
                    serde_json::Value::from(e.end_line.saturating_add(1))
                }),
                "calls": e.calls,
                "doc": e.doc,
                "rank": e.rank,
                // provide imports/unresolved_imports with 1-based lines
                "imports": self.import_edges.get(e.id as usize).map(|edges| {
                    let lines = self.import_lines.get(e.id as usize);
                    edges
                        .iter()
                        .enumerate()
                        .filter_map(|(i, &eid)| {
                            self.entities.get(eid as usize).and_then(|te| {
                                self.files.get(te.file_id as usize).map(|tf| {
                                    serde_json::json!({"path": tf.path, "line": lines.and_then(|l| l.get(i)).cloned().unwrap_or(0).saturating_add(1)})
                                })
                            })
                        })
                        .collect::<Vec<_>>()
                }).unwrap_or_default(),
                "unresolved_imports": self.unresolved_imports.get(self.files[e.file_id as usize].id as usize).map(|v| {
                    v.iter().map(|(m, ln)| serde_json::json!({"module": m, "line": ln.saturating_add(1)})).collect::<Vec<_>>()
                }).unwrap_or_default(),
            });
            writeln!(w, "{}", json)?;
        }
        Ok(())
    }

    // Graph traversal helpers
    pub fn callees_up_to(&self, entity_id: u32, depth: u32) -> Vec<u32> {
        crate::repo_index::graph::callees_up_to(self, entity_id, depth)
    }
    pub fn callers_up_to(&self, entity_id: u32, depth: u32) -> Vec<u32> {
        crate::repo_index::graph::callers_up_to(self, entity_id, depth)
    }

    // Weighted PageRank over multi-edge graph using configured RankWeights.
    pub fn compute_pagerank(&mut self) {
        crate::repo_index::pagerank::compute_pagerank(self)
    }
}
