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

use crate::repo_index::indexer::types::{
    Progress, RepoIndexOptions, RepoIndexOutput, RepoIndexStats,
};
use ignore::WalkBuilder;
use rayon::prelude::*;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::channel;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tree_sitter::Language;

// Language crates
use tree_sitter_c_sharp as ts_c_sharp;
use tree_sitter_cpp as ts_cpp;
use tree_sitter_go as ts_go;
use tree_sitter_java as ts_java;
use tree_sitter_javascript as ts_javascript;
use tree_sitter_python as ts_python;
use tree_sitter_rust as ts_rust;
use tree_sitter_swift as ts_swift;
use tree_sitter_typescript as ts_typescript;
use tree_sitter_verilog as ts_verilog;

pub fn detect_language(path: &Path) -> Option<&'static str> {
    match path.extension().and_then(|e| e.to_str()) {
        Some("rs") => Some("rust"),
        Some("py") => Some("python"),
        Some("js") => Some("javascript"),
        Some("ts") => Some("typescript"),
        Some("tsx") => Some("tsx"),
        Some("cpp") | Some("cc") | Some("cxx") | Some("hpp") | Some("h") => Some("cpp"),
        Some("v") | Some("sv") | Some("svh") => Some("verilog"),
        Some("java") => Some("java"),
        Some("cs") => Some("c_sharp"),
        Some("swift") => Some("swift"),
        Some("go") => Some("go"),
        _ => None,
    }
}
pub fn lang_to_ts(lang: &str) -> Option<Language> {
    Some(match lang {
        "rust" => ts_rust::LANGUAGE.into(),
        "python" => ts_python::LANGUAGE.into(),
        "javascript" => ts_javascript::LANGUAGE.into(),
        "typescript" => ts_typescript::LANGUAGE_TYPESCRIPT.into(),
        "tsx" => ts_typescript::LANGUAGE_TSX.into(),
        "cpp" => ts_cpp::LANGUAGE.into(),
        "verilog" => ts_verilog::LANGUAGE.into(),
        "java" => ts_java::LANGUAGE.into(),
        "c_sharp" => ts_c_sharp::LANGUAGE.into(),
        "swift" => ts_swift::LANGUAGE.into(),
        "go" => ts_go::LANGUAGE.into(),
        _ => return None,
    })
}

pub fn index_repository(mut opts: RepoIndexOptions<'_>) -> anyhow::Result<RepoIndexStats> {
    let start = Instant::now();
    let mut entities = 0usize; // snapshot from writer.counter
    let mut files = 0usize;

    // Prepare a parallel-safe file handle if the output is a FilePath.
    let mut parallel_file: Option<Arc<Mutex<File>>> = None;
    let mut file_handle_opt: Option<File> = None;
    // Borrow inspect output so we don't move it yet.
    if let RepoIndexOutput::FilePath(p) = &opts.output {
        let f = File::create(p)?;
        parallel_file = Some(Arc::new(Mutex::new(f.try_clone()?)));
        file_handle_opt = Some(File::create(p)?);
    }

    let walker = WalkBuilder::new(opts.root)
        .standard_filters(true)
        .add_custom_ignore_filename(".gitignore")
        .build();
    // Collect candidate file paths first so we can parallelize parsing.
    let mut paths: Vec<std::path::PathBuf> = Vec::new();
    for dent in walker {
        let dent = match dent {
            Ok(d) => d,
            Err(_) => continue,
        };
        let path = dent.into_path();
        if !path.is_file() {
            continue;
        }
        let lang = match detect_language(&path) {
            Some(l) => l,
            None => continue,
        };
        if let Some(include) = &opts.include_langs {
            if !include.contains(lang) {
                continue;
            }
        }
        paths.push(path);
    }

    if opts.concurrency <= 1 || parallel_file.is_none() {
        // Serial path: take ownership of the output so we can move
        // writer references into the CountingWriter without borrow issues.
        let output = std::mem::replace(&mut opts.output, RepoIndexOutput::Null);
        // No per-file event emission

        match output {
            RepoIndexOutput::Writer(w) => {
                let mut cw = crate::repo_index::indexer::types::CountingWriter::new(w);
                // fallback to single-threaded behavior preserving original ordering
                // Reuse per-language Parser instances across files.
                let mut parsers: std::collections::HashMap<&'static str, tree_sitter::Parser> =
                    std::collections::HashMap::new();
                for path in paths.into_iter() {
                    process_path_with_parsers(
                        &path,
                        &mut cw,
                        &mut files,
                        &mut entities,
                        &mut parsers,
                    )?;
                    if let Some(cb) = opts.progress {
                        cb(Progress {
                            current_file: Some(&path),
                            files_indexed: files,
                            entities_indexed: entities,
                        });
                    }
                }
            }
            RepoIndexOutput::FilePath(_) => {
                // We created file_handle_opt earlier when FilePath was present.
                if let Some(fh) = file_handle_opt {
                    let mut cw = crate::repo_index::indexer::types::CountingWriter::new(fh);
                    // Reuse per-language Parser instances across files.
                    let mut parsers: std::collections::HashMap<&'static str, tree_sitter::Parser> =
                        std::collections::HashMap::new();
                    for path in paths.into_iter() {
                        process_path_with_parsers(
                            &path,
                            &mut cw,
                            &mut files,
                            &mut entities,
                            &mut parsers,
                        )?;
                        if let Some(cb) = opts.progress {
                            cb(Progress {
                                current_file: Some(&path),
                                files_indexed: files,
                                entities_indexed: entities,
                            });
                        }
                    }
                }
            }
            RepoIndexOutput::Null => {
                let mut cw =
                    crate::repo_index::indexer::types::CountingWriter::new(std::io::sink());
                // Reuse per-language Parser instances across files.
                let mut parsers: std::collections::HashMap<&'static str, tree_sitter::Parser> =
                    std::collections::HashMap::new();
                for path in paths.into_iter() {
                    process_path_with_parsers(
                        &path,
                        &mut cw,
                        &mut files,
                        &mut entities,
                        &mut parsers,
                    )?;
                    if let Some(cb) = opts.progress {
                        cb(Progress {
                            current_file: Some(&path),
                            files_indexed: files,
                            entities_indexed: entities,
                        });
                    }
                }
            }
        }
    } else {
        // Multi-threaded: use Rayon to parse & extract entities in parallel,
        // then send serialized lines back to a writer thread to avoid locking
        // the output writer on each write.
        let (tx, rx) = channel::<Vec<String>>();
        let writer_tx = tx.clone();
        // For parallel-safe writing we own an Arc<Mutex<File>> or use sink.
        // We already ensured `parallel_file` is Some above; clone the Arc for the writer thread.
        let arc_file = match &parallel_file {
            Some(af) => af.clone(),
            None => unreachable!("parallel_file must be Some when concurrency > 1"),
        };
        let lines_counter = Arc::new(AtomicUsize::new(0));
        let lines_counter_writer = lines_counter.clone();
        // Spawn writer thread
        let writer_handle = std::thread::spawn(move || {
            let mut files_written = 0usize;
            for lines in rx.iter() {
                if lines.is_empty() {
                    files_written += 1;
                    continue;
                }
                // Join lines into a single buffer to write under a single lock
                let mut buf = String::with_capacity(lines.iter().map(|s| s.len() + 1).sum());
                for line in &lines {
                    buf.push_str(line);
                    buf.push('\n');
                }
                if let Ok(mut f) = arc_file.lock() {
                    let _ = f.write_all(buf.as_bytes());
                }
                lines_counter_writer.fetch_add(lines.len(), Ordering::SeqCst);
                files_written += 1;
            }
            // flush at end
            if let Ok(mut f) = arc_file.lock() {
                let _ = f.flush();
            }
            files_written
        });

        // Parallel processing using a local Rayon thread pool sized by opts.concurrency
        // Use per-thread state to reuse parsers and batch multiple files before sending to writer.
        struct ThreadState {
            parsers: std::collections::HashMap<&'static str, tree_sitter::Parser>,
            batch: Vec<String>,
            batch_bytes: usize,
            sender: std::sync::mpsc::Sender<Vec<String>>,
            batch_max_files: usize,
            batch_max_bytes: usize,
        }
        impl Drop for ThreadState {
            fn drop(&mut self) {
                if !self.batch.is_empty() {
                    let to_send = std::mem::take(&mut self.batch);
                    let _ = self.sender.send(to_send);
                }
            }
        }

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(opts.concurrency)
            .build()?;
        pool.install(|| {
            // clone sender for per-thread initializers
            let tx_for_threads = writer_tx.clone();
            paths
                .into_par_iter()
                .with_max_len(1)
                .map_init(
                    || ThreadState {
                        parsers: std::collections::HashMap::new(),
                        batch: Vec::new(),
                        batch_bytes: 0,
                        sender: tx_for_threads.clone(),
                        batch_max_files: 16,        // flush after 16 files
                        batch_max_bytes: 64 * 1024, // or after ~64KB
                    },
                    |state, path| {
                        // parse and append to per-thread batch
                        if let Ok(mut lines) =
                            process_path_collect_with_parsers(&mut state.parsers, &path)
                        {
                            if !lines.is_empty() {
                                let added_bytes: usize = lines.iter().map(|s| s.len() + 1).sum();
                                state.batch_bytes += added_bytes;
                                state.batch.append(&mut lines);
                                if state.batch.len() >= state.batch_max_files
                                    || state.batch_bytes >= state.batch_max_bytes
                                {
                                    let to_send = std::mem::take(&mut state.batch);
                                    state.batch_bytes = 0;
                                    let _ = state.sender.send(to_send);
                                }
                            }
                        }
                    },
                )
                .for_each(|_| ());
        });

        // Drop sender to close channel and join writer
        drop(tx);
        let written = writer_handle.join().unwrap_or(0usize);
        files = written;
        entities = lines_counter.load(Ordering::SeqCst);
        if let Some(cb) = opts.progress {
            cb(Progress {
                current_file: None,
                files_indexed: files,
                entities_indexed: entities,
            });
        }
    }
    Ok(RepoIndexStats {
        files_indexed: files,
        entities_indexed: entities,
        duration: start.elapsed(),
        time_reading: std::time::Duration::ZERO,
        time_parsing: std::time::Duration::ZERO,
        time_extracting: std::time::Duration::ZERO,
        time_alias_tree: std::time::Duration::ZERO,
        time_alias_fallback: std::time::Duration::ZERO,
        time_module_map: std::time::Duration::ZERO,
        time_import_edges: std::time::Duration::ZERO,
        time_scope_containment: std::time::Duration::ZERO,
        time_alias_resolution: std::time::Duration::ZERO,
        time_calls_resolution: std::time::Duration::ZERO,
        time_pagerank: std::time::Duration::ZERO,
        docs_nanos: 0,
        calls_nanos: 0,
        docs_attempts: 0,
        calls_attempts: 0,
    })
}

// Process a single path and write directly to counting writer (single-threaded path)
fn process_path<W: Write>(
    path: &std::path::PathBuf,
    cw: &mut crate::repo_index::indexer::types::CountingWriter<W>,
    files: &mut usize,
    entities: &mut usize,
) -> anyhow::Result<()> {
    let lang = match detect_language(path) {
        Some(l) => l,
        None => return Ok(()),
    };
    let language = match lang_to_ts(lang) {
        Some(l) => l,
        None => return Ok(()),
    };
    let src = match std::fs::read_to_string(path) {
        Ok(s) => s,
        Err(_) => return Ok(()),
    };
    let mut parser = tree_sitter::Parser::new();
    if parser.set_language(&language).is_err() {
        return Ok(());
    }
    let tree = match parser.parse(&src, None) {
        Some(t) => t,
        None => return Ok(()),
    };
    let mut entities_local: Vec<crate::repo_index::indexer::types::Entity> = Vec::new();
    crate::repo_index::indexer::extract::extract_entities(
        lang,
        &tree,
        &src,
        path,
        &mut entities_local,
    );
    for e in entities_local.into_iter() {
        let mut map = serde_json::Map::new();
        map.insert("file".to_string(), serde_json::Value::String(e.file));
        map.insert(
            "language".to_string(),
            serde_json::Value::String(e.language.to_string()),
        );
        map.insert(
            "kind".to_string(),
            serde_json::Value::String(e.kind.to_string()),
        );
        map.insert("name".to_string(), serde_json::Value::String(e.name));
        if let Some(p) = e.parent {
            map.insert("parent".to_string(), serde_json::Value::String(p));
        }
        map.insert(
            "signature".to_string(),
            serde_json::Value::String(e.signature),
        );
        map.insert(
            "start_line".to_string(),
            serde_json::Value::Number(serde_json::Number::from(e.start_line.saturating_add(1))),
        );
        map.insert(
            "end_line".to_string(),
            serde_json::Value::Number(serde_json::Number::from(e.end_line.saturating_add(1))),
        );
        if let Some(doc) = e.doc {
            map.insert("doc".to_string(), serde_json::Value::String(doc));
        }
        let json = serde_json::Value::Object(map);
        writeln!(cw, "{}", serde_json::to_string(&json)?)?;
    }
    *files += 1;
    cw.flush()?;
    *entities = cw.count();
    Ok(())
}

// Process a single path using a provided Parser (reused across files)
fn process_path_with_parser<W: Write>(
    path: &std::path::PathBuf,
    cw: &mut crate::repo_index::indexer::types::CountingWriter<W>,
    files: &mut usize,
    entities: &mut usize,
    parser: &mut tree_sitter::Parser,
) -> anyhow::Result<()> {
    let lang = match detect_language(path) {
        Some(l) => l,
        None => return Ok(()),
    };
    let language = match lang_to_ts(lang) {
        Some(l) => l,
        None => return Ok(()),
    };
    let src = match std::fs::read_to_string(path) {
        Ok(s) => s,
        Err(_) => return Ok(()),
    };
    if parser.set_language(&language).is_err() {
        return Ok(());
    }
    let tree = match parser.parse(&src, None) {
        Some(t) => t,
        None => return Ok(()),
    };
    let mut entities_local: Vec<crate::repo_index::indexer::types::Entity> = Vec::new();
    crate::repo_index::indexer::extract::extract_entities(
        lang,
        &tree,
        &src,
        path,
        &mut entities_local,
    );
    for e in entities_local.into_iter() {
        let mut map = serde_json::Map::new();
        map.insert("file".to_string(), serde_json::Value::String(e.file));
        map.insert(
            "language".to_string(),
            serde_json::Value::String(e.language.to_string()),
        );
        map.insert(
            "kind".to_string(),
            serde_json::Value::String(e.kind.to_string()),
        );
        map.insert("name".to_string(), serde_json::Value::String(e.name));
        if let Some(p) = e.parent {
            map.insert("parent".to_string(), serde_json::Value::String(p));
        }
        map.insert(
            "signature".to_string(),
            serde_json::Value::String(e.signature),
        );
        map.insert(
            "start_line".to_string(),
            serde_json::Value::Number(serde_json::Number::from(e.start_line.saturating_add(1))),
        );
        map.insert(
            "end_line".to_string(),
            serde_json::Value::Number(serde_json::Number::from(e.end_line.saturating_add(1))),
        );
        if let Some(doc) = e.doc {
            map.insert("doc".to_string(), serde_json::Value::String(doc));
        }
        let json = serde_json::Value::Object(map);
        writeln!(cw, "{}", serde_json::to_string(&json)?)?;
    }
    *files += 1;
    cw.flush()?;
    *entities = cw.count();
    Ok(())
}

// Process a single path using per-language Parser pool (reused across files)
fn process_path_with_parsers<W: Write>(
    path: &std::path::PathBuf,
    cw: &mut crate::repo_index::indexer::types::CountingWriter<W>,
    files: &mut usize,
    entities: &mut usize,
    parsers: &mut std::collections::HashMap<&'static str, tree_sitter::Parser>,
) -> anyhow::Result<()> {
    let lang = match detect_language(path) {
        Some(l) => l,
        None => return Ok(()),
    };
    let language = match lang_to_ts(lang) {
        Some(l) => l,
        None => return Ok(()),
    };
    let src = match std::fs::read_to_string(path) {
        Ok(s) => s,
        Err(_) => return Ok(()),
    };
    let parser = if let Some(p) = parsers.get_mut(lang) {
        p
    } else {
        let mut p = tree_sitter::Parser::new();
        if p.set_language(&language).is_err() {
            return Ok(());
        }
        parsers.insert(lang, p);
        parsers.get_mut(lang).unwrap()
    };
    let tree = match parser.parse(&src, None) {
        Some(t) => t,
        None => return Ok(()),
    };
    let mut entities_local: Vec<crate::repo_index::indexer::types::Entity> = Vec::new();
    crate::repo_index::indexer::extract::extract_entities(
        lang,
        &tree,
        &src,
        path,
        &mut entities_local,
    );
    for e in entities_local.into_iter() {
        let mut map = serde_json::Map::new();
        map.insert("file".to_string(), serde_json::Value::String(e.file));
        map.insert(
            "language".to_string(),
            serde_json::Value::String(e.language.to_string()),
        );
        map.insert(
            "kind".to_string(),
            serde_json::Value::String(e.kind.to_string()),
        );
        map.insert("name".to_string(), serde_json::Value::String(e.name));
        if let Some(p) = e.parent {
            map.insert("parent".to_string(), serde_json::Value::String(p));
        }
        map.insert(
            "signature".to_string(),
            serde_json::Value::String(e.signature),
        );
        map.insert(
            "start_line".to_string(),
            serde_json::Value::Number(serde_json::Number::from(e.start_line.saturating_add(1))),
        );
        map.insert(
            "end_line".to_string(),
            serde_json::Value::Number(serde_json::Number::from(e.end_line.saturating_add(1))),
        );
        if let Some(doc) = e.doc {
            map.insert("doc".to_string(), serde_json::Value::String(doc));
        }
        let json = serde_json::Value::Object(map);
        writeln!(cw, "{}", serde_json::to_string(&json)?)?;
    }
    *files += 1;
    cw.flush()?;
    *entities = cw.count();
    Ok(())
}

// Process path and return serialized JSONL lines (parallel path)
fn process_path_collect(path: &std::path::PathBuf) -> anyhow::Result<Vec<String>> {
    let mut lines: Vec<String> = Vec::new();
    let lang = match detect_language(path) {
        Some(l) => l,
        None => return Ok(lines),
    };
    let language = match lang_to_ts(lang) {
        Some(l) => l,
        None => return Ok(lines),
    };
    let src = match std::fs::read_to_string(path) {
        Ok(s) => s,
        Err(_) => return Ok(lines),
    };
    let mut parser = tree_sitter::Parser::new();
    if parser.set_language(&language).is_err() {
        return Ok(lines);
    }
    let tree = match parser.parse(&src, None) {
        Some(t) => t,
        None => return Ok(lines),
    };
    let mut entities_local: Vec<crate::repo_index::indexer::types::Entity> = Vec::new();
    crate::repo_index::indexer::extract::extract_entities(
        lang,
        &tree,
        &src,
        path,
        &mut entities_local,
    );
    for e in entities_local.into_iter() {
        let mut map = serde_json::Map::new();
        map.insert("file".to_string(), serde_json::Value::String(e.file));
        map.insert(
            "language".to_string(),
            serde_json::Value::String(e.language.to_string()),
        );
        map.insert(
            "kind".to_string(),
            serde_json::Value::String(e.kind.to_string()),
        );
        map.insert("name".to_string(), serde_json::Value::String(e.name));
        if let Some(p) = e.parent {
            map.insert("parent".to_string(), serde_json::Value::String(p));
        }
        map.insert(
            "signature".to_string(),
            serde_json::Value::String(e.signature),
        );
        map.insert(
            "start_line".to_string(),
            serde_json::Value::Number(serde_json::Number::from(e.start_line.saturating_add(1))),
        );
        map.insert(
            "end_line".to_string(),
            serde_json::Value::Number(serde_json::Number::from(e.end_line.saturating_add(1))),
        );
        if let Some(doc) = e.doc {
            map.insert("doc".to_string(), serde_json::Value::String(doc));
        }
        let json = serde_json::Value::Object(map);
        lines.push(serde_json::to_string(&json)?);
    }
    Ok(lines)
}

// Like `process_path_collect` but reuses Parser instances from `parsers` keyed by language
fn process_path_collect_with_parsers(
    parsers: &mut std::collections::HashMap<&'static str, tree_sitter::Parser>,
    path: &std::path::PathBuf,
) -> anyhow::Result<Vec<String>> {
    let mut lines: Vec<String> = Vec::new();
    let lang = match detect_language(path) {
        Some(l) => l,
        None => return Ok(lines),
    };
    let language = match lang_to_ts(lang) {
        Some(l) => l,
        None => return Ok(lines),
    };
    let src = match std::fs::read_to_string(path) {
        Ok(s) => s,
        Err(_) => return Ok(lines),
    };
    // Get or create parser for this language in the per-thread map
    let parser = parsers.entry(lang).or_insert_with(|| {
        let mut p = tree_sitter::Parser::new();
        let _ = p.set_language(&language);
        p
    });
    let tree = match parser.parse(&src, None) {
        Some(t) => t,
        None => return Ok(lines),
    };
    let mut entities_local: Vec<crate::repo_index::indexer::types::Entity> = Vec::new();
    crate::repo_index::indexer::extract::extract_entities(
        lang,
        &tree,
        &src,
        path,
        &mut entities_local,
    );
    for e in entities_local.into_iter() {
        let mut map = serde_json::Map::new();
        map.insert("file".to_string(), serde_json::Value::String(e.file));
        map.insert(
            "language".to_string(),
            serde_json::Value::String(e.language.to_string()),
        );
        map.insert(
            "kind".to_string(),
            serde_json::Value::String(e.kind.to_string()),
        );
        map.insert("name".to_string(), serde_json::Value::String(e.name));
        if let Some(p) = e.parent {
            map.insert("parent".to_string(), serde_json::Value::String(p));
        }
        map.insert(
            "signature".to_string(),
            serde_json::Value::String(e.signature),
        );
        map.insert(
            "start_line".to_string(),
            serde_json::Value::Number(serde_json::Number::from(e.start_line.saturating_add(1))),
        );
        map.insert(
            "end_line".to_string(),
            serde_json::Value::Number(serde_json::Number::from(e.end_line.saturating_add(1))),
        );
        if let Some(doc) = e.doc {
            map.insert("doc".to_string(), serde_json::Value::String(doc));
        }
        let json = serde_json::Value::Object(map);
        lines.push(serde_json::to_string(&json)?);
    }
    Ok(lines)
}
