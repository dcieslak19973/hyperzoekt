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
use std::fs::File;
use std::io::Write;
use std::path::Path;
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

pub fn index_repository(opts: RepoIndexOptions<'_>) -> anyhow::Result<RepoIndexStats> {
    let start = Instant::now();
    let mut entities = 0usize; // snapshot from writer.counter
    let mut files = 0usize;

    let mut file_handle; // scoped for writer
    let writer: Box<dyn Write> = match opts.output {
        RepoIndexOutput::FilePath(p) => {
            file_handle = Box::new(File::create(p)?);
            Box::new(&mut *file_handle)
        }
        RepoIndexOutput::Writer(w) => Box::new(w),
        RepoIndexOutput::Null => Box::new(std::io::sink()),
    };
    let mut cw = crate::repo_index::indexer::types::CountingWriter::new(writer);

    let walker = WalkBuilder::new(opts.root)
        .standard_filters(true)
        .add_custom_ignore_filename(".gitignore")
        .build();

    for dent in walker {
        let dent = match dent {
            Ok(d) => d,
            Err(_) => continue,
        };
        let path = dent.path();
        if !path.is_file() {
            continue;
        }
        let lang = match detect_language(path) {
            Some(l) => l,
            None => continue,
        };
        if let Some(include) = &opts.include_langs {
            if !include.contains(lang) {
                continue;
            }
        }
        let language = match lang_to_ts(lang) {
            Some(l) => l,
            None => continue,
        };
        let src = match std::fs::read_to_string(path) {
            Ok(s) => s,
            Err(_) => continue,
        };
        let mut parser = tree_sitter::Parser::new();
        if parser.set_language(&language).is_err() {
            continue;
        }
        let tree = match parser.parse(&src, None) {
            Some(t) => t,
            None => continue,
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
            // convert internal 0-based line numbers to 1-based for exported JSONL
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
            if let Some(calls) = e.calls {
                map.insert("calls".to_string(), serde_json::to_value(calls)?);
            }
            if let Some(doc) = e.doc {
                map.insert("doc".to_string(), serde_json::Value::String(doc));
            }
            let json = serde_json::Value::Object(map);
            writeln!(cw, "{}", serde_json::to_string(&json)?)?;
        }
        files += 1;
        cw.flush()?;
        entities = cw.count();
        if let Some(cb) = opts.progress {
            cb(Progress {
                current_file: Some(path),
                files_indexed: files,
                entities_indexed: entities,
            });
        }
    }
    Ok(RepoIndexStats {
        files_indexed: files,
        entities_indexed: entities,
        duration: start.elapsed(),
    })
}
