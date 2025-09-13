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
    // Store import aliases per file (module_token, optional_alias, lineno)
    let mut file_import_aliases: HashMap<u32, Vec<(String, Option<String>, usize)>> =
        HashMap::new();
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
                // Normalize to a workspace-relative tests/fixtures path when present
                let file_path_str = if let Some(idx) = file_path_str.find("tests/fixtures") {
                    file_path_str[idx..].to_string()
                } else {
                    file_path_str
                };
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
        // Normalize to a workspace-relative tests/fixtures path when present
        let file_path_str = if let Some(idx) = file_path_str.find("tests/fixtures") {
            file_path_str[idx..].to_string()
        } else {
            file_path_str
        };
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
        // If we parsed and collected aliases via tree-sitter earlier, attach them now.
        // Use typesitter-based extraction for JS/TS and also for Python, Rust, and Go.
        if matches!(
            lang,
            "javascript" | "typescript" | "tsx" | "python" | "rust" | "go"
        ) {
            // call tree-sitter based extractor again (cheap) and store results for later alias resolution
            let aliases = crate::repo_index::indexer::helpers::extract_import_aliases_from_tree(
                lang, &tree, &src,
            );
            if !aliases.is_empty() {
                file_import_aliases
                    .entry(file_id)
                    .or_default()
                    .extend(aliases);
            }
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
            // Also index method names (lowercased) to point to the parent entity id so
            // symbol searches for method identifiers (e.g. "new") will return the
            // containing type/entity. This makes search_symbol_exact find methods.
            for m in &ent.methods {
                if !m.name.is_empty() {
                    name_index
                        .entry(m.name.to_lowercase())
                        .or_default()
                        .push(id);
                }
            }
            entities_store.push(StoredEntity {
                id,
                file_id,
                kind: crate::repo_index::indexer::types::EntityKind::parse_str(ent.kind),
                name: ent.name,
                parent: ent.parent,
                signature: ent.signature,
                start_line: ent.start_line as u32,
                end_line: ent.end_line as u32,
                // calls_raw captured in extractor; will resolve later
                doc: ent.doc,
                rank: 0.0,
                calls_raw: ent.calls_raw,
                methods: ent.methods,
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
            // file pseudo-entity has no calls
            doc: None,
            rank: 0.0,
            calls_raw: Vec::new(),
            methods: Vec::new(),
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
        // calls removed
    }
    // Resolve raw call identifiers collected during extraction into call_edges using a simple heuristic.
    // Build global lowercase name -> entity ids map (may be ambiguous).
    let mut global_name_map: HashMap<String, Vec<u32>> = HashMap::new();
    for ent in &entities_store {
        global_name_map
            .entry(ent.name.to_lowercase())
            .or_default()
            .push(ent.id);
    }
    // (call resolution will occur after imports are processed)
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

    // Build a module map: map plausible dotted module names to file entity ids.
    // For a file path like "pkg/a.py" we map "pkg.a" -> file_entity.
    let mut module_map: HashMap<String, u32> = HashMap::new();
    for (idx, f) in files.iter().enumerate() {
        // derive module path from file path relative to repo root
        let path = std::path::Path::new(&f.path);
        if let Some(components) = path.parent() {
            let mut parts: Vec<String> = Vec::new();
            for c in components.iter() {
                if let Some(s) = c.to_str() {
                    parts.push(s.to_string());
                }
            }
            if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                parts.push(stem.to_string());
                let modname = parts.join(".").to_lowercase();
                module_map.entry(modname).or_insert(file_entities[idx]);
            }
        } else if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
            let modname = stem.to_lowercase();
            module_map.entry(modname).or_insert(file_entities[idx]);
        }
    }
    // Note: some alias entries are populated during parsing (for parsed files)
    // and others will be added here by scanning file sources for non-parsed files
    for f in files.iter() {
        // ensure an entry exists for each file so later loops can assume presence
        file_import_aliases.entry(f.id).or_default();
        let fp = &f.path;
        let src = match std::fs::read_to_string(fp) {
            Ok(s) => s,
            Err(_) => continue,
        };
        let lang = &f.language;
        // only add fallback line-based aliases when no tree-sitter-derived aliases exist
        if file_import_aliases
            .get(&f.id)
            .map(|v| v.is_empty())
            .unwrap_or(true)
        {
            let aliases = crate::repo_index::indexer::helpers::extract_import_aliases(lang, &src);
            if !aliases.is_empty() {
                file_import_aliases.insert(f.id, aliases);
            }
        }
    }

    for (&fid, mods) in file_import_modules.iter() {
        let file_entity_id = file_entities[fid as usize];
        let mut added: StdHashSet<u32> = StdHashSet::new();
        for (m, lineno) in mods {
            let key = m.to_lowercase();
            // Prefer exact dotted module match first
            if let Some(&target) = module_map.get(&key) {
                if added.insert(target) {
                    import_edges[file_entity_id as usize].push(target);
                    import_lines[file_entity_id as usize].push(*lineno as u32);
                }
                continue;
            }
            // Try longest-prefix dotted match: e.g. import pkg.sub where module_map may have pkg.sub.module
            if key.contains('.') {
                let parts: Vec<&str> = key.split('.').collect();
                let mut found_lp = None;
                for i in (1..=parts.len()).rev() {
                    let prefix = parts[..i].join(".");
                    if let Some(&target) = module_map.get(&prefix) {
                        found_lp = Some(target);
                        break;
                    }
                }
                if let Some(target) = found_lp {
                    if added.insert(target) {
                        import_edges[file_entity_id as usize].push(target);
                        import_lines[file_entity_id as usize].push(*lineno as u32);
                    }
                    continue;
                }
            }
            // Fallback to basename unique mapping
            if let Some(count) = basename_counts.get(&key) {
                if *count == 1 {
                    if let Some(target_file_entity) = basename_map.get(&key) {
                        if added.insert(*target_file_entity) {
                            import_edges[file_entity_id as usize].push(*target_file_entity);
                            import_lines[file_entity_id as usize].push(*lineno as u32);
                        }
                        continue;
                    }
                }
                unresolved_imports_per_file[fid as usize].push((m.clone(), *lineno as u32));
            } else {
                unresolved_imports_per_file[fid as usize].push((m.clone(), *lineno as u32));
            }
        }
    }

    // Collect alias candidates: (file_id, module_token, optional_alias, lineno)
    let mut alias_candidates: Vec<(u32, String, Option<String>, usize)> = Vec::new();
    // Debug: dump collected aliases for inspection when running tests.
    for (fid, aliases) in file_import_aliases.iter() {
        for (module, alias_opt, ln) in aliases.iter() {
            alias_candidates.push((*fid, module.clone(), alias_opt.clone(), *ln));
        }
    }

    // Cross-file call resolution: build a map from file_entity -> list of entity ids in that file
    let mut file_to_entities: HashMap<u32, Vec<u32>> = HashMap::new();
    for ent in &entities_store {
        file_to_entities
            .entry(ent.file_id)
            .or_default()
            .push(ent.id);
    }

    // Now resolve alias candidates into a concrete alias_map.
    // alias_map: (requesting_file_id, alias_lower) -> (module_or_symbol, target_entity_id)
    let mut alias_map: HashMap<(u32, String), (String, u32)> = HashMap::new();
    for (req_fid, module_tok, alias_opt, _ln) in alias_candidates.drain(..) {
        if let Some(alias) = alias_opt {
            let alias_l = alias.to_lowercase();
            let module_l = module_tok.to_lowercase();
            if module_l.contains('.') {
                // symbol-level token like "pkg.a" or "pkg.module.symbol"
                // split into module path and symbol name
                if let Some(pos) = module_l.rfind('.') {
                    let modpath = &module_l[..pos];
                    let sym = &module_l[pos + 1..];
                    // find file_entity for modpath using module_map longest-prefix
                    let mut target_fe: Option<u32> = None;
                    if let Some(&fe) = module_map.get(modpath) {
                        target_fe = Some(fe);
                    } else {
                        let parts: Vec<&str> = modpath.split('.').collect();
                        for i in (1..=parts.len()).rev() {
                            let prefix = parts[..i].join(".");
                            if let Some(&fe) = module_map.get(&prefix) {
                                target_fe = Some(fe);
                                break;
                            }
                        }
                    }
                    if let Some(fe) = target_fe {
                        let file_idx = entities_store[fe as usize].file_id;
                        if let Some(cands) = file_to_entities.get(&file_idx) {
                            for &cand in cands {
                                if entities_store[cand as usize].name.to_lowercase() == sym {
                                    alias_map.insert(
                                        (req_fid, alias_l.clone()),
                                        (module_l.clone(), cand),
                                    );
                                    break;
                                }
                            }
                        }
                    } else {
                        // Fallback: if we couldn't find the module file, try to resolve the symbol
                        // globally (unique name) as a best-effort for conditional or dynamic imports.
                        if let Some(targets) = global_name_map.get(sym) {
                            if targets.len() == 1 {
                                alias_map.insert(
                                    (req_fid, alias_l.clone()),
                                    (module_l.clone(), targets[0]),
                                );
                            }
                        }
                    }
                }
            } else {
                // module-level alias like import pkg.a as x -> try to map module token to file_entity
                // prefer exact then longest-prefix
                let mut matched: Option<u32> = None;
                if let Some(&fe) = module_map.get(&module_l) {
                    matched = Some(fe);
                } else if module_l.contains('.') {
                    let parts: Vec<&str> = module_l.split('.').collect();
                    for i in (1..=parts.len()).rev() {
                        let prefix = parts[..i].join(".");
                        if let Some(&fe) = module_map.get(&prefix) {
                            matched = Some(fe);
                            break;
                        }
                    }
                }
                if let Some(fe) = matched {
                    alias_map.insert((req_fid, alias_l.clone()), (module_l.clone(), fe));
                }
            }
        }
    }

    // Now resolve captured raw call identifiers into edges, consulting imports
    for ent in &entities_store {
        if ent.calls_raw.is_empty() {
            continue;
        }
        for raw in &ent.calls_raw {
            let norm_last = raw
                .rsplit('.')
                .next()
                .map(|s| s.to_lowercase())
                .unwrap_or_else(|| raw.to_lowercase());

            let mut resolved: Option<u32> = None;

            // 1) same-file scope
            let key_file = (ent.file_id, norm_last.clone());
            if let Some(id) = scope_map.get(&key_file) {
                resolved = Some(*id);
            }

            // 2) same-parent/class
            if resolved.is_none() {
                if let Some(parent_name) = &ent.parent {
                    let key_parent = (ent.file_id, parent_name.to_lowercase());
                    if let Some(parent_id) = scope_map.get(&key_parent) {
                        for &child_id in &containment_children[*parent_id as usize] {
                            if entities_store[child_id as usize].name.to_lowercase() == norm_last {
                                resolved = Some(child_id);
                                break;
                            }
                        }
                    }
                }
            }

            // 3) global unique
            if resolved.is_none() {
                if let Some(targets) = global_name_map.get(&norm_last) {
                    if targets.len() == 1 {
                        resolved = Some(targets[0]);
                    }
                }
            }

            // 3.5) symbol-level alias in this file (e.g., `from pkg.a import foo as foo_cond` -> foo_cond)
            if resolved.is_none() {
                if let Some((_, target)) = alias_map.get(&(ent.file_id, norm_last.clone())) {
                    resolved = Some(*target);
                }
            }

            // 4) consult imports for this file
            if resolved.is_none() {
                if let Some(imports) = import_edges.get(ent.file_id as usize) {
                    for &import_file_entity in imports {
                        // import_file_entity is a file-entity id; find the file id in the entities list
                        let file_idx = entities_store[import_file_entity as usize].file_id;
                        if let Some(cands) = file_to_entities.get(&file_idx) {
                            for &cand in cands {
                                if entities_store[cand as usize].name.to_lowercase() == norm_last {
                                    resolved = Some(cand);
                                    break;
                                }
                            }
                        }
                        if resolved.is_some() {
                            break;
                        }
                    }
                }
            }

            // 5) If still unresolved, and the raw call looks like "alias.something...",
            // consult alias_map for a mapping of alias -> (module_name, file_entity).
            if resolved.is_none() && raw.contains('.') {
                let mut parts = raw.split('.');
                if let Some(first) = parts.next() {
                    let first_l = first.to_lowercase();
                    if let Some((_modname, file_entity_id)) = alias_map.get(&(ent.file_id, first_l))
                    {
                        // try to resolve the remaining name (last segment) inside that file_entity's file
                        let last = raw
                            .rsplit('.')
                            .next()
                            .map(|s| s.to_lowercase())
                            .unwrap_or_default();
                        let file_idx = entities_store[*file_entity_id as usize].file_id;
                        if let Some(cands) = file_to_entities.get(&file_idx) {
                            for &cand in cands {
                                if entities_store[cand as usize].name.to_lowercase() == last {
                                    resolved = Some(cand);
                                    break;
                                }
                            }
                        }
                    }
                }
            }

            if let Some(tgt) = resolved {
                if tgt != ent.id {
                    call_edges[ent.id as usize].push(tgt);
                    reverse_call_edges[tgt as usize].push(ent.id);
                }
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
