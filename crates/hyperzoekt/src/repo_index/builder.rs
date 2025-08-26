// content moved from service/builder.rs

use crate::repo_index::indexer::{
    detect_language, extract_entities, lang_to_ts, RepoIndexOptions, RepoIndexStats,
};
use anyhow::Result;
use ignore::WalkBuilder;
use std::collections::{HashMap, HashSet as StdHashSet};
use tree_sitter::{Language, Parser};

// Bring the types from the sibling `types` module into scope.
use super::types::{FileRecord, RepoIndexService, StoredEntity};

/// Detailed builder that accepts `RepoIndexOptions` for advanced usage.
///
/// Returns the constructed service and some indexing statistics on success.
pub fn build_with_options(
    opts: RepoIndexOptions<'_>,
) -> Result<(RepoIndexService, RepoIndexStats)> {
    let start = std::time::Instant::now();
    let root = opts.root;
    let walker = WalkBuilder::new(root)
        .standard_filters(true)
        .add_custom_ignore_filename(".gitignore")
        .build();
    let mut files_map: HashMap<String, u32> = HashMap::new();
    let mut files: Vec<FileRecord> = Vec::new();
    let mut entities_store: Vec<StoredEntity> = Vec::new();
    let mut name_index: HashMap<String, Vec<u32>> = HashMap::new();
    let mut file_count = 0usize;
    let mut parser = Parser::new();
    // Temporary store of raw import module names with line numbers per file index (not entity id yet)
    let mut file_import_modules: HashMap<u32, Vec<(String, usize)>> = HashMap::new();
    for dent in walker {
        let dent = match dent {
            Ok(d) => d,
            Err(_) => continue,
        };
        let path = dent.path();
        if !path.is_file() {
            continue;
        }
        let lang: &str = match detect_language(path) {
            Some(l) => l,
            None => continue,
        };
        if let Some(include) = &opts.include_langs {
            if !include.contains(lang) {
                continue;
            }
        }
        let language: Language = match lang_to_ts(lang) {
            Some(l) => l,
            None => continue,
        };
        let src = match std::fs::read_to_string(path) {
            Ok(s) => s,
            Err(_) => continue,
        };
        if parser.set_language(&language).is_err() {
            if matches!(lang, "c_sharp" | "swift") {
                let imports =
                    crate::repo_index::indexer::helpers::extract_import_modules(lang, &src);
                let file_path_str = path.display().to_string();
                let file_id = *files_map.entry(file_path_str.clone()).or_insert_with(|| {
                    let id = files.len() as u32;
                    files.push(FileRecord {
                        id,
                        path: file_path_str.clone(),
                        language: lang.to_string(),
                        entities: Vec::new(),
                    });
                    id
                });
                if !imports.is_empty() {
                    file_import_modules
                        .entry(file_id)
                        .or_default()
                        .extend(imports);
                }
            }
            continue;
        }
        let tree = match parser.parse(&src, None) {
            Some(t) => t,
            None => continue,
        };
        let mut entities_local = Vec::new();
        extract_entities(lang, &tree, &src, path, &mut entities_local);
        let imports = crate::repo_index::indexer::helpers::extract_import_modules(lang, &src);
        let file_path_str = path.display().to_string();
        let file_id = *files_map.entry(file_path_str.clone()).or_insert_with(|| {
            let id = files.len() as u32;
            files.push(FileRecord {
                id,
                path: file_path_str.clone(),
                language: lang.to_string(),
                entities: Vec::new(),
            });
            id
        });
        if !imports.is_empty() {
            file_import_modules
                .entry(file_id)
                .or_default()
                .extend(imports);
        }
        for ent in entities_local {
            let id = entities_store.len() as u32;
            if let Some(fr) = files.get_mut(file_id as usize) {
                fr.entities.push(id);
            }
            name_index
                .entry(ent.name.to_lowercase())
                .or_default()
                .push(id);
            entities_store.push(StoredEntity {
                id,
                file_id,
                kind: crate::repo_index::indexer::types::EntityKind::parse_str(ent.kind),
                name: ent.name,
                parent: ent.parent,
                signature: ent.signature,
                start_line: ent.start_line as u32,
                end_line: ent.end_line as u32,
                calls: ent.calls.unwrap_or_default(),
                doc: ent.doc,
                rank: 0.0,
            });
        }
        file_count += 1;
    }
    // Add file pseudo-entities
    let mut file_entities: Vec<u32> = Vec::with_capacity(files.len());
    for f in &files {
        let id = entities_store.len() as u32;
        file_entities.push(id);
        // derive file-level start/end from any recorded import line numbers for the file
        let (start_line_u32, end_line_u32) = if let Some(mods) = file_import_modules.get(&f.id) {
            if !mods.is_empty() {
                let mut min_ln = usize::MAX;
                let mut max_ln = 0usize;
                for (_m, ln) in mods.iter() {
                    min_ln = std::cmp::min(min_ln, *ln);
                    max_ln = std::cmp::max(max_ln, *ln);
                }
                (min_ln as u32, max_ln as u32)
            } else {
                (0u32, 0u32)
            }
        } else {
            (0u32, 0u32)
        };
        entities_store.push(StoredEntity {
            id,
            file_id: f.id,
            kind: crate::repo_index::indexer::types::EntityKind::File,
            name: std::path::Path::new(&f.path)
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("")
                .to_string(),
            parent: None,
            signature: String::new(),
            start_line: start_line_u32,
            end_line: end_line_u32,
            calls: Vec::new(),
            doc: None,
            rank: 0.0,
        });
    }
    let entity_len = entities_store.len();
    let mut containment_children: Vec<Vec<u32>> = vec![Vec::new(); entity_len];
    let mut containment_parent: Vec<Option<u32>> = vec![None; entity_len];
    let mut call_edges: Vec<Vec<u32>> = vec![Vec::new(); entity_len];
    let mut reverse_call_edges: Vec<Vec<u32>> = vec![Vec::new(); entity_len];
    let mut import_edges: Vec<Vec<u32>> = vec![Vec::new(); entity_len];
    let mut import_lines: Vec<Vec<u32>> = vec![Vec::new(); entity_len];
    let mut unresolved_imports_per_file: Vec<Vec<(String, u32)>> = vec![Vec::new(); files.len()];
    let mut scope_map: HashMap<(u32, String), u32> = HashMap::new();
    for e in &entities_store {
        scope_map.insert((e.file_id, e.name.to_lowercase()), e.id);
    }
    for e in &entities_store {
        if let Some(parent_name) = &e.parent {
            let key = (e.file_id, parent_name.to_lowercase());
            if let Some(parent_id) = scope_map.get(&key) {
                containment_parent[e.id as usize] = Some(*parent_id);
                containment_children[*parent_id as usize].push(e.id);
            }
        }
    }
    for e in &entities_store {
        if matches!(e.kind, crate::repo_index::indexer::types::EntityKind::File) {
            continue;
        }
        let mut resolved_local: StdHashSet<u32> = StdHashSet::new();
        for call_name in &e.calls {
            if let Some(callee_id) = scope_map.get(&(e.file_id, call_name.to_lowercase())) {
                call_edges[e.id as usize].push(*callee_id);
                reverse_call_edges[*callee_id as usize].push(e.id);
                resolved_local.insert(*callee_id);
            }
        }
    }
    for e in &entities_store {
        if matches!(e.kind, crate::repo_index::indexer::types::EntityKind::File) {
            continue;
        }
        let already: StdHashSet<u32> = call_edges[e.id as usize].iter().cloned().collect();
        for call_name in &e.calls {
            let key = call_name.to_lowercase();
            if let Some(ids) = name_index.get(&key) {
                if ids.len() == 1 {
                    let target = ids[0];
                    if !already.contains(&target) && target != e.id {
                        call_edges[e.id as usize].push(target);
                        reverse_call_edges[target as usize].push(e.id);
                    }
                }
            }
        }
    }
    let mut basename_map: HashMap<String, u32> = HashMap::new();
    let mut basename_counts: HashMap<String, u32> = HashMap::new();
    for (idx, f) in files.iter().enumerate() {
        if let Some(stem) = std::path::Path::new(&f.path)
            .file_stem()
            .and_then(|s| s.to_str())
        {
            let key = stem.to_lowercase();
            *basename_counts.entry(key.clone()).or_insert(0) += 1;
            basename_map.entry(key).or_insert(file_entities[idx]);
        }
    }
    for (fid, mods) in file_import_modules.into_iter() {
        let file_entity_id = file_entities[fid as usize];
        let mut added: StdHashSet<u32> = StdHashSet::new();
        for (m, lineno) in mods {
            let key = m.to_lowercase();
            if let Some(count) = basename_counts.get(&key) {
                if *count == 1 {
                    if let Some(target_file_entity) = basename_map.get(&key) {
                        if added.insert(*target_file_entity) {
                            import_edges[file_entity_id as usize].push(*target_file_entity);
                            import_lines[file_entity_id as usize].push(lineno as u32);
                        }
                        continue;
                    }
                }
                unresolved_imports_per_file[fid as usize].push((m.clone(), lineno as u32));
            } else {
                unresolved_imports_per_file[fid as usize].push((m.clone(), lineno as u32));
            }
        }
    }
    let stats = RepoIndexStats {
        files_indexed: file_count,
        entities_indexed: entities_store.len(),
        duration: start.elapsed(),
    };
    let rank_weights = crate::repo_index::indexer::types::RankWeights::from_env();
    let mut svc = RepoIndexService {
        files,
        entities: entities_store,
        name_index,
        containment_children,
        containment_parent,
        call_edges,
        reverse_call_edges,
        import_edges,
        import_lines,
        file_entities,
        unresolved_imports: unresolved_imports_per_file,
        rank_weights,
    };
    svc.compute_pagerank();
    Ok((svc, stats))
}

pub fn build<P: AsRef<std::path::Path>>(root: P) -> Result<(RepoIndexService, RepoIndexStats)> {
    let opts = crate::repo_index::indexer::types::RepoIndexOptions::builder()
        .root(root.as_ref())
        .output_null()
        .build();
    build_with_options(opts)
}
// Index builder helpers for RepoIndexService.
// For now this file re-exports the heavy-lifting build_with_options
// implementation from `types.rs` to keep incremental changes minimal.

// Future: move build implementation here and expose a smaller public API.
