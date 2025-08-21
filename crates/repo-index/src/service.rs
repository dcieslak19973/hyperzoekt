use crate::internal::{
    detect_language, extract_entities, lang_to_ts, RankWeights, RepoIndexOptions, RepoIndexStats,
};
use anyhow::Result;
use ignore::WalkBuilder;
use std::collections::{HashMap, HashSet, VecDeque};
use std::io::Write;
use tree_sitter::{Language, Parser};

#[derive(Debug, Clone)]
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
    pub kind: super::internal::EntityKind,
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

use std::collections::HashSet as StdHashSet;

pub struct RepoIndexService {
    pub files: Vec<FileRecord>,
    pub entities: Vec<StoredEntity>,
    pub(crate) name_index: HashMap<String, Vec<u32>>, // lowercase name -> entity ids (crate visible)
    // Graph adjacency lists (indices reference entities vector)
    pub containment_children: Vec<Vec<u32>>, // entity id -> child entity ids
    pub containment_parent: Vec<Option<u32>>, // entity id -> optional parent entity id
    pub call_edges: Vec<Vec<u32>>,           // entity id -> outgoing calls (resolved entity ids)
    pub reverse_call_edges: Vec<Vec<u32>>,   // entity id -> incoming calls (callers)
    pub import_edges: Vec<Vec<u32>>, // placeholder until Step 4 (file-level imports -> file entity ids)
    pub file_entities: Vec<u32>,     // mapping file index -> file entity id
    pub unresolved_imports: Vec<Vec<String>>, // per file index unresolved module basenames
    pub rank_weights: super::internal::RankWeights, // configured (possibly env overridden) weights
}

/// Service that holds the repository index built from Tree-sitter parsed files.
///
/// Construction helpers:
/// - `RepoIndexService::build(root)` is a simple convenience constructor that
///   indexes the provided `root` path using default `RepoIndexOptions`.
/// - `RepoIndexService::build_with_options(opts)` is the advanced constructor
///   that accepts a `RepoIndexOptions` struct for fine-grained control over
///   which languages to include, output handling, and other indexing options.
///
/// Use `build` for typical workflows and `build_with_options` when you need
/// custom behavior or to programmatically inspect `RepoIndexStats` and
/// intermediate results.
impl RepoIndexService {
    /// Detailed builder that accepts `RepoIndexOptions` for advanced usage.
    ///
    /// Returns the constructed service and some indexing statistics on success.
    pub fn build_with_options(opts: RepoIndexOptions<'_>) -> Result<(Self, RepoIndexStats)> {
        // (Implementation adapted from Goose PR service.rs)
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
        // Temporary store of raw import module names per file index (not entity id yet)
        let mut file_import_modules: HashMap<u32, HashSet<String>> = HashMap::new();
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
                    let imports = super::internal::extract_import_modules(lang, &src);
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
            let imports = super::internal::extract_import_modules(lang, &src);
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
                    kind: super::internal::EntityKind::parse_str(ent.kind),
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
            entities_store.push(StoredEntity {
                id,
                file_id: f.id,
                kind: super::internal::EntityKind::File,
                name: std::path::Path::new(&f.path)
                    .file_name()
                    .and_then(|s| s.to_str())
                    .unwrap_or("")
                    .to_string(),
                parent: None,
                signature: String::new(),
                start_line: 0,
                end_line: 0,
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
        let mut unresolved_imports_per_file: Vec<Vec<String>> = vec![Vec::new(); files.len()];
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
            if matches!(e.kind, super::internal::EntityKind::File) {
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
            if matches!(e.kind, super::internal::EntityKind::File) {
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
            for m in mods {
                let key = m.to_lowercase();
                if let Some(count) = basename_counts.get(&key) {
                    if *count == 1 {
                        if let Some(target_file_entity) = basename_map.get(&key) {
                            if added.insert(*target_file_entity) {
                                import_edges[file_entity_id as usize].push(*target_file_entity);
                            }
                            continue;
                        }
                    }
                    unresolved_imports_per_file[fid as usize].push(m.clone());
                } else {
                    unresolved_imports_per_file[fid as usize].push(m.clone());
                }
            }
        }
        let stats = RepoIndexStats {
            files_indexed: file_count,
            entities_indexed: entities_store.len(),
            duration: start.elapsed(),
        };
        let rank_weights = super::internal::RankWeights::from_env();
        let mut svc = Self {
            files,
            entities: entities_store,
            name_index,
            containment_children,
            containment_parent,
            call_edges,
            reverse_call_edges,
            import_edges,
            file_entities,
            unresolved_imports: unresolved_imports_per_file,
            rank_weights,
        };
        svc.compute_pagerank();
        Ok((svc, stats))
    }

    /// Backwards-compatible convenience constructor used by tests/callers.
    pub fn build<P: AsRef<std::path::Path>>(root: P) -> Result<(Self, RepoIndexStats)> {
        let opts = crate::internal::RepoIndexOptions::builder()
            .root(root.as_ref())
            .output_null()
            .build();
        Self::build_with_options(opts)
    }
    /// Search for exact identifier matches. Returns matching entities.
    pub fn search(&self, query: &str, limit: usize) -> Vec<RepoEntity> {
        let results = self.search_symbol_exact(query);
        results
            .into_iter()
            .take(limit)
            .map(|e| RepoEntity {
                name: e.name.clone(),
                path: self.files[e.file_id as usize].path.clone(),
                line: e.start_line as usize,
            })
            .collect()
    }

    pub fn search_symbol_exact(&self, name: &str) -> Vec<&StoredEntity> {
        let key = name.to_lowercase();
        self.name_index
            .get(&key)
            .map(|ids| {
                ids.iter()
                    .filter_map(|id| self.entities.get(*id as usize))
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn search_symbol_fuzzy_ranked(&self, query: &str, limit: usize) -> Vec<&StoredEntity> {
        let q_lower = query.to_lowercase();
        let mut min_rank = f32::MAX;
        let mut max_rank = f32::MIN;
        for e in &self.entities {
            if e.rank < min_rank {
                min_rank = e.rank;
            }
            if e.rank > max_rank {
                max_rank = e.rank;
            }
        }
        let rank_range = if (max_rank - min_rank).abs() < 1e-9 {
            1.0
        } else {
            max_rank - min_rank
        };
        fn levenshtein(a: &str, b: &str) -> usize {
            // small helper (O(len^2)) acceptable for modest entity counts
            let mut dp: Vec<usize> = (0..=b.len()).collect();
            for (i, ca) in a.chars().enumerate() {
                let mut prev = dp[0];
                dp[0] = i + 1;
                for (j, cb) in b.chars().enumerate() {
                    let temp = dp[j + 1];
                    dp[j + 1] = if ca == cb {
                        prev
                    } else {
                        1 + prev.min(dp[j]).min(dp[j + 1])
                    };
                    prev = temp;
                }
            }
            *dp.last().unwrap()
        }
        let mut scored: Vec<(f32, &StoredEntity)> = Vec::new();
        for e in &self.entities {
            if e.kind.as_str() == "file" {
                continue;
            }
            let name_lower = e.name.to_lowercase();
            let mut lex = 0.0f32;
            if name_lower == q_lower {
                lex = 1.0;
            } else if name_lower.starts_with(&q_lower) {
                lex = 0.8;
            } else if name_lower.contains(&q_lower) {
                lex = 0.5;
            } else {
                let dist = levenshtein(&name_lower, &q_lower);
                if dist <= 2 {
                    lex = (0.3 - 0.1 * dist as f32).max(0.0);
                }
            }
            if lex > 0.0 {
                let norm_rank = (e.rank - min_rank) / rank_range;
                let final_score = lex * 0.6 + norm_rank * 0.4;
                scored.push((final_score, e));
            }
        }
        scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
        scored.truncate(limit);
        scored.into_iter().map(|(_, e)| e).collect()
    }

    pub fn symbol_ids_exact(&self, name: &str) -> &[u32] {
        static EMPTY: [u32; 0] = [];
        self.name_index
            .get(&name.to_lowercase())
            .map(|v| v.as_slice())
            .unwrap_or(&EMPTY)
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
    pub fn unresolved_imports_for_file_index(&self, file_index: usize) -> &[String] {
        self.unresolved_imports
            .get(file_index)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
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
                "start_line": e.start_line,
                "end_line": e.end_line,
                "calls": e.calls,
                "doc": e.doc,
                "rank": e.rank,
            });
            writeln!(w, "{}", json)?;
        }
        Ok(())
    }

    // Graph traversal helpers
    pub fn callees_up_to(&self, entity_id: u32, depth: u32) -> Vec<u32> {
        self.bfs_depth(entity_id, depth, true)
    }
    pub fn callers_up_to(&self, entity_id: u32, depth: u32) -> Vec<u32> {
        self.bfs_depth(entity_id, depth, false)
    }
    fn bfs_depth(&self, start: u32, depth: u32, forward: bool) -> Vec<u32> {
        if depth == 0 {
            return Vec::new();
        }
        let mut visited: HashSet<u32> = HashSet::new();
        let mut out: Vec<u32> = Vec::new();
        let mut q: VecDeque<(u32, u32)> = VecDeque::new();
        q.push_back((start, 0));
        visited.insert(start);
        while let Some((node, d)) = q.pop_front() {
            if d == depth {
                continue;
            }
            let neigh = if forward {
                &self.call_edges
            } else {
                &self.reverse_call_edges
            };
            for &n in neigh.get(node as usize).unwrap_or(&Vec::new()) {
                if visited.insert(n) {
                    out.push(n);
                    q.push_back((n, d + 1));
                }
            }
        }
        out
    }

    // Weighted PageRank over multi-edge graph using configured RankWeights.
    pub fn compute_pagerank(&mut self) {
        let n = self.entities.len();
        if n == 0 {
            return;
        }
        let RankWeights {
            call: w_call,
            import: w_import,
            containment: w_contain,
            damping,
            iterations,
        } = self.rank_weights;
        let init = 1.0f32 / n as f32;
        let mut rank = vec![init; n];
        let mut new_rank = vec![0.0f32; n];
        // Pre-build adjacency with normalized probabilities per source entity
        let mut outgoing: Vec<Vec<(u32, f32)>> = Vec::with_capacity(n);
        for i in 0..n {
            let mut edges: Vec<(u32, f32)> = Vec::new();
            for &t in self.call_edges.get(i).unwrap_or(&Vec::new()) {
                edges.push((t, w_call));
            }
            for &t in self.import_edges.get(i).unwrap_or(&Vec::new()) {
                edges.push((t, w_import));
            }
            for &t in self.containment_children.get(i).unwrap_or(&Vec::new()) {
                edges.push((t, w_contain));
            }
            if let Some(Some(p)) = self.containment_parent.get(i) {
                edges.push((*p, w_contain));
            }
            let sum: f32 = edges.iter().map(|(_, w)| *w).sum();
            if sum > 0.0 {
                for e in edges.iter_mut() {
                    e.1 /= sum;
                }
            }
            outgoing.push(edges);
        }
        let teleport = (1.0 - damping) / n as f32;
        for _ in 0..iterations {
            // reset
            new_rank.fill(0.0);
            let mut dangling_sum = 0.0f32;
            for i in 0..n {
                let r = rank[i];
                if outgoing[i].is_empty() {
                    dangling_sum += r;
                    continue;
                }
                for &(t, w) in &outgoing[i] {
                    new_rank[t as usize] += r * w * damping;
                }
            }
            let dangling_contrib = if n > 0 {
                (dangling_sum * damping) / n as f32
            } else {
                0.0
            };
            for item in new_rank.iter_mut().take(n) {
                *item += teleport + dangling_contrib;
            }
            std::mem::swap(&mut rank, &mut new_rank);
        }
        // Store back into entities
        for (i, r) in rank.into_iter().enumerate() {
            if let Some(ent) = self.entities.get_mut(i) {
                ent.rank = r;
            }
        }
    }
}
