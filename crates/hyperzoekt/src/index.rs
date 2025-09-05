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

use crate::repo_index::indexer::payload::{EntityPayload, ImportItem, UnresolvedImport};
use crate::repo_index::indexer::{EntityKind, RepoIndexOptions, RepoIndexStats};
use crate::repo_index::RepoIndexService;
use std::path::Path;

pub fn index_single_file(
    path: &Path,
) -> Result<(Vec<EntityPayload>, RepoIndexStats), anyhow::Error> {
    let mut opts_builder = RepoIndexOptions::builder();
    opts_builder = opts_builder.root(path);
    let opts = opts_builder.output_null().build();
    let (svc, stats) = RepoIndexService::build_with_options(opts)?;
    let mut payloads: Vec<EntityPayload> = Vec::new();
    for ent in svc.entities.iter() {
        let file = &svc.files[ent.file_id as usize];
        let mut imports: Vec<ImportItem> = Vec::new();
        let mut unresolved_imports: Vec<UnresolvedImport> = Vec::new();
        if matches!(ent.kind, EntityKind::File) {
            if let Some(edge_list) = svc.import_edges.get(ent.id as usize) {
                let lines = svc.import_lines.get(ent.id as usize);
                for (i, &target_eid) in edge_list.iter().enumerate() {
                    if let Some(target_ent) = svc.entities.get(target_eid as usize) {
                        let target_file_idx = target_ent.file_id as usize;
                        if let Some(target_file) = svc.files.get(target_file_idx) {
                            let line_no = lines
                                .and_then(|l| l.get(i))
                                .cloned()
                                .unwrap_or(0)
                                .saturating_add(1);
                            imports.push(ImportItem {
                                path: target_file.path.clone(),
                                line: line_no,
                            });
                        }
                    }
                }
            }
            if let Some(unres) = svc.unresolved_imports.get(ent.file_id as usize) {
                for (m, lineno) in unres {
                    unresolved_imports.push(UnresolvedImport {
                        module: m.clone(),
                        line: lineno.saturating_add(1),
                    });
                }
            }
        }
        let (start_field, end_field) = if matches!(ent.kind, EntityKind::File) {
            let has_imports = !imports.is_empty();
            let has_unresolved = !unresolved_imports.is_empty();
            if has_imports || has_unresolved {
                (
                    Some(ent.start_line.saturating_add(1)),
                    Some(ent.end_line.saturating_add(1)),
                )
            } else {
                (None, None)
            }
        } else {
            (
                Some(ent.start_line.saturating_add(1)),
                Some(ent.end_line.saturating_add(1)),
            )
        };

        // compute stable id
        let project = std::env::var("SURREAL_PROJECT").unwrap_or_else(|_| "local-project".into());
        let repo = std::env::var("SURREAL_REPO").unwrap_or_else(|_| {
            std::path::Path::new(&path)
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("local-repo")
                .to_string()
        });
        let branch = std::env::var("SURREAL_BRANCH").unwrap_or_else(|_| "local-branch".into());
        let commit = std::env::var("SURREAL_COMMIT").unwrap_or_else(|_| "local-commit".into());
        let stable_id = crate::utils::generate_stable_id(
            &project,
            &repo,
            &branch,
            &commit,
            &file.path,
            &ent.name,
            &ent.signature,
        );

        payloads.push(EntityPayload {
            file: file.path.clone(),
            language: file.language.clone(),
            kind: ent.kind.as_str().to_string(),
            name: ent.name.clone(),
            parent: ent.parent.clone(),
            signature: ent.signature.clone(),
            start_line: start_field,
            end_line: end_field,
            calls: ent.calls.clone(),
            doc: ent.doc.clone(),
            rank: ent.rank,
            imports,
            unresolved_imports,
            stable_id,
        });
    }
    Ok((payloads, stats))
}
