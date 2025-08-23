use anyhow::Result;
use ignore::WalkBuilder;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs::File;
use std::io::{self, Write};
use std::path::Path;
use std::time::{Duration, Instant};
use tree_sitter::{Language, Node, Parser};

// Language crates (feature-level currently all under single feature)
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

// --- Enhanced import extraction helper (multi-language heuristics) ---

/// Extract import module basenames and their source line numbers from a file's source text.
///
/// Returns a Vec of (module_basename, line_number) pairs. IMPORTANT: line numbers
/// returned here are 0-based (they directly reflect Tree-sitter's `start_position().row`).
/// The indexer keeps internal AST/entity coordinates 0-based. Do NOT convert to 1-based
/// here — conversion to IDE-friendly 1-based line numbers happens only at the outer
/// exporter boundary (for example `export_jsonl` or the CLI), so that internal logic
/// and comparisons remain consistent and avoid double-conversion bugs.
pub fn extract_import_modules(lang: &str, source: &str) -> Vec<(String, usize)> {
    // return a list of (module_basename, line_number) pairs. Line numbers are 0-based
    // to match Tree-sitter's start_line convention used elsewhere.
    let mut vec: Vec<(String, usize)> = Vec::new();
    match lang {
        // Python: handle aliases, relative, multi-import
        "python" => {
            for (lineno, line) in source.lines().enumerate() {
                let t = line.trim();
                if let Some(rest) = t.strip_prefix("import ") {
                    for part in rest.split(',') {
                        let mut token = part.trim();
                        if let Some(idx) = token.find(" as ") {
                            token = &token[..idx];
                        }
                        token = token.split_whitespace().next().unwrap_or("");
                        if !token.is_empty() {
                            vec.push((token.to_string(), lineno));
                        }
                    }
                } else if t.starts_with("from ") {
                    if let Some(after_from) = t.strip_prefix("from ") {
                        if let Some((module, _imports)) = after_from.split_once(" import ") {
                            let mut mod_token = module.trim();
                            mod_token = mod_token.trim_start_matches('.'); // collapse relative dots
                            mod_token = mod_token.split('.').next().unwrap_or("");
                            if !mod_token.is_empty() {
                                vec.push((mod_token.to_string(), lineno));
                            }
                        }
                    }
                }
            }
        }
        // Local quoted includes only
        "cpp" => {
            for (lineno, line) in source.lines().enumerate() {
                let t = line.trim();
                if t.starts_with("#include \"") {
                    if let Some(start) = t.find('"') {
                        if let Some(end_rel) = t[start + 1..].find('"') {
                            let name = &t[start + 1..start + 1 + end_rel];
                            if !name.is_empty() {
                                vec.push((normalize_module_basename(name), lineno));
                            }
                        }
                    }
                }
            }
        }
        "java" => {
            for (lineno, line) in source.lines().enumerate() {
                let t = line.trim();
                if let Some(mut rest) = t.strip_prefix("import ") {
                    if let Some(r) = rest.strip_prefix("static ") {
                        rest = r;
                    }
                    if let Some(semi) = rest.find(';') {
                        rest = &rest[..semi];
                    }
                    let last = rest.rsplit('.').next().unwrap_or(rest).trim();
                    if !last.is_empty() && last != "*" {
                        vec.push((last.to_string(), lineno));
                    }
                }
            }
        }
        "javascript" | "typescript" => {
            for (lineno, line) in source.lines().enumerate() {
                let t = line.trim();
                if t.starts_with("import ") {
                    if let Some(idx) = t.find(" from ") {
                        let rest = &t[idx + 6..];
                        if let Some(m) = extract_quoted(rest) {
                            vec.push((normalize_module_basename(&m), lineno));
                        }
                    } else if t.starts_with("import ") && (t.contains('"') || t.contains('\'')) {
                        if let Some(m) = extract_quoted(t) {
                            vec.push((normalize_module_basename(&m), lineno));
                        }
                    }
                } else if t.contains("require(") {
                    if let Some(m) = between(t, "require(", ")") {
                        let m2 = m.trim_matches(&['"', '\''] as &[_]);
                        vec.push((normalize_module_basename(m2), lineno));
                    }
                }
            }
        }
        "go" => {
            let mut in_block = false;
            for (lineno, line) in source.lines().enumerate() {
                let t = line.trim();
                // start of a block import: import (
                if t.starts_with("import (") {
                    in_block = true;
                    continue;
                }
                // single-line import: import "fmt" or import alias "pkg"
                if t.starts_with("import ") {
                    if let Some(m) = extract_quoted(t) {
                        vec.push((normalize_module_basename(&m), lineno));
                    }
                    continue;
                }
                if in_block {
                    if t.starts_with(')') {
                        in_block = false;
                        continue;
                    }
                    if let Some(m) = extract_quoted(t) {
                        vec.push((normalize_module_basename(&m), lineno));
                    }
                }
            }
        }
        "rust" => {
            for (lineno, line) in source.lines().enumerate() {
                let t = line.trim();
                if let Some(rest) = t.strip_prefix("mod ") {
                    let token = rest
                        .split(|c: char| c == ';' || c == '{' || c.is_whitespace())
                        .next()
                        .unwrap_or("");
                    if !token.is_empty() {
                        vec.push((token.to_string(), lineno));
                    }
                } else if let Some(after) = t.strip_prefix("use ") {
                    let mut first = after
                        .split(|c: char| c == ':' || c == ';' || c == '{' || c.is_whitespace())
                        .next()
                        .unwrap_or("");
                    if ["crate", "super", "self"].contains(&first) {
                        let remainder = after
                            .trim_start_matches(first)
                            .trim_start_matches(':')
                            .trim_start_matches(':');
                        first = remainder
                            .split(|c: char| c == ':' || c == ';' || c == '{' || c.is_whitespace())
                            .next()
                            .unwrap_or("");
                    }
                    if !first.is_empty() && first != "*" {
                        vec.push((first.to_string(), lineno));
                    }
                }
            }
        }
        "c_sharp" => {
            for (lineno, line) in source.lines().enumerate() {
                let t = line.trim();
                if let Some(after) = t.strip_prefix("using ") {
                    let head = after.split('=').next().unwrap_or(after);
                    let first = head
                        .split(|c: char| c == '.' || c == ';' || c.is_whitespace())
                        .next()
                        .unwrap_or("");
                    if !first.is_empty() {
                        vec.push((first.to_string(), lineno));
                    }
                }
            }
        }
        "swift" => {
            for (lineno, line) in source.lines().enumerate() {
                let t = line.trim();
                if let Some(rest) = t.strip_prefix("@testable import ") {
                    let tok = rest.split_whitespace().next().unwrap_or("");
                    if !tok.is_empty() {
                        vec.push((tok.to_string(), lineno));
                    }
                } else if let Some(rest) = t.strip_prefix("import ") {
                    let tok = rest.split_whitespace().next().unwrap_or("");
                    if !tok.is_empty() {
                        vec.push((tok.to_string(), lineno));
                    }
                }
            }
        }
        _ => {}
    }
    vec
}

fn extract_quoted(s: &str) -> Option<String> {
    let mut current = None;
    for (i, ch) in s.char_indices() {
        if ch == '"' || ch == '\'' {
            if current.is_none() {
                current = Some((ch, i));
            } else if let Some((qc, start)) = current {
                if qc == ch {
                    return Some(s[start + 1..i].to_string());
                }
            }
        }
    }
    None
}

fn between<'a>(s: &'a str, start: &str, end: &str) -> Option<&'a str> {
    let a = s.find(start)? + start.len();
    let rest = &s[a..];
    let b = rest.find(end)?;
    Some(&rest[..b])
}

fn normalize_module_basename(module: &str) -> String {
    let last = module.rsplit('/').next().unwrap_or(module);
    let stem = last.split('.').next().unwrap_or(last);
    stem.to_string()
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct RepoIndexStats {
    pub files_indexed: usize,
    pub entities_indexed: usize,
    pub duration: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entity<'a> {
    pub file: String,
    pub language: &'a str,
    pub kind: &'static str, // class | function | method | other
    pub name: String,
    pub parent: Option<String>,
    pub signature: String,
    pub start_line: usize,
    pub end_line: usize,
    pub calls: Option<Vec<String>>, // simple callee names
    pub doc: Option<String>,
}

// Small enum used by the treesitter-backed service to classify entities.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EntityKind {
    File,
    Class,
    Function,
    Method,
    Other,
}

impl EntityKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            EntityKind::File => "file",
            EntityKind::Class => "class",
            EntityKind::Function => "function",
            EntityKind::Method => "method",
            EntityKind::Other => "other",
        }
    }
    pub fn parse_str(s: &str) -> Self {
        match s {
            "file" => EntityKind::File,
            "class" => EntityKind::Class,
            "function" => EntityKind::Function,
            "method" => EntityKind::Method,
            _ => EntityKind::Other,
        }
    }
}

impl std::str::FromStr for EntityKind {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(EntityKind::parse_str(s))
    }
}

/// Weights for the PageRank computation. Values may come from environment
/// variables for easy experimentation; sensible defaults are provided.
#[derive(Clone, Copy, Debug)]
pub struct RankWeights {
    pub call: f32,
    pub import: f32,
    pub containment: f32,
    pub damping: f32,
    pub iterations: usize,
}

impl Default for RankWeights {
    fn default() -> Self {
        Self {
            call: 1.0,
            import: 0.5,
            containment: 0.25,
            damping: 0.85,
            iterations: 20,
        }
    }
}

impl RankWeights {
    pub fn from_env() -> Self {
        let mut w = RankWeights::default();
        if let Ok(v) = std::env::var("HZ_RANK_CALL") {
            if let Ok(f) = v.parse::<f32>() {
                w.call = f;
            }
        }
        if let Ok(v) = std::env::var("HZ_RANK_IMPORT") {
            if let Ok(f) = v.parse::<f32>() {
                w.import = f;
            }
        }
        if let Ok(v) = std::env::var("HZ_RANK_CONTAIN") {
            if let Ok(f) = v.parse::<f32>() {
                w.containment = f;
            }
        }
        if let Ok(v) = std::env::var("HZ_RANK_DAMPING") {
            if let Ok(f) = v.parse::<f32>() {
                w.damping = f;
            }
        }
        if let Ok(v) = std::env::var("HZ_RANK_ITERS") {
            if let Ok(i) = v.parse::<usize>() {
                w.iterations = i;
            }
        }
        w
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Progress<'a> {
    pub current_file: Option<&'a Path>,
    pub files_indexed: usize,
    pub entities_indexed: usize,
}

pub type ProgressCallback<'a> = dyn Fn(Progress<'_>) + Send + Sync + 'a;

pub struct RepoIndexOptions<'a> {
    pub root: &'a Path,
    pub output: RepoIndexOutput<'a>,
    pub include_langs: Option<HashSet<&'a str>>, // if None index all supported
    pub progress: Option<&'a ProgressCallback<'a>>,
}

pub enum RepoIndexOutput<'a> {
    FilePath(&'a Path),
    Writer(&'a mut dyn Write),
    Null, // discard (still counts entities via sink)
}

#[derive(Default)]
pub struct RepoIndexOptionsBuilder<'a> {
    root: Option<&'a Path>,
    output: Option<RepoIndexOutput<'a>>,
    include_langs: Option<HashSet<&'a str>>,
    progress: Option<&'a ProgressCallback<'a>>,
}

impl<'a> RepoIndexOptions<'a> {
    pub fn builder() -> RepoIndexOptionsBuilder<'a> {
        RepoIndexOptionsBuilder::default()
    }
}

impl<'a> RepoIndexOptionsBuilder<'a> {
    pub fn root(mut self, root: &'a Path) -> Self {
        self.root = Some(root);
        self
    }
    pub fn output_file(mut self, path: &'a Path) -> Self {
        self.output = Some(RepoIndexOutput::FilePath(path));
        self
    }
    pub fn output_writer(mut self, w: &'a mut dyn Write) -> Self {
        self.output = Some(RepoIndexOutput::Writer(w));
        self
    }
    pub fn output_null(mut self) -> Self {
        self.output = Some(RepoIndexOutput::Null);
        self
    }
    pub fn include_langs(mut self, langs: HashSet<&'a str>) -> Self {
        self.include_langs = Some(langs);
        self
    }
    pub fn progress(mut self, cb: &'a ProgressCallback<'a>) -> Self {
        self.progress = Some(cb);
        self
    }
    pub fn build(self) -> RepoIndexOptions<'a> {
        RepoIndexOptions {
            root: self.root.expect("root required"),
            output: self.output.expect("output required"),
            include_langs: self.include_langs,
            progress: self.progress,
        }
    }
}

struct CountingWriter<W: Write> {
    inner: W,
    counter: usize,
}
impl<W: Write> CountingWriter<W> {
    fn new(inner: W) -> Self {
        Self { inner, counter: 0 }
    }
    fn count(&self) -> usize {
        self.counter
    }
}
impl<W: Write> Write for CountingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let written = self.inner.write(buf)?;
        self.counter += buf[..written].iter().filter(|b| **b == b'\n').count();
        Ok(written)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

pub(crate) fn detect_language(path: &Path) -> Option<&'static str> {
    match path.extension().and_then(|e| e.to_str()) {
        Some("rs") => Some("rust"),
        Some("py") => Some("python"),
        Some("js") => Some("javascript"),
        Some("ts") | Some("tsx") => Some("typescript"),
        Some("cpp") | Some("cc") | Some("cxx") | Some("hpp") | Some("h") => Some("cpp"),
        Some("v") | Some("sv") | Some("svh") => Some("verilog"),
        Some("java") => Some("java"),
        Some("cs") => Some("c_sharp"),
        Some("swift") => Some("swift"),
        Some("go") => Some("go"),
        _ => None,
    }
}
pub(crate) fn lang_to_ts(lang: &str) -> Option<Language> {
    Some(match lang {
        "rust" => ts_rust::LANGUAGE.into(),
        "python" => ts_python::LANGUAGE.into(),
        "javascript" => ts_javascript::LANGUAGE.into(),
        "typescript" => ts_typescript::LANGUAGE_TYPESCRIPT.into(),
        "cpp" => ts_cpp::LANGUAGE.into(),
        "verilog" => ts_verilog::LANGUAGE.into(),
        "java" => ts_java::LANGUAGE.into(),
        "c_sharp" => ts_c_sharp::LANGUAGE.into(),
        "swift" => ts_swift::LANGUAGE.into(),
        "go" => ts_go::LANGUAGE.into(),
        _ => return None,
    })
}

pub fn index_repository(opts: RepoIndexOptions<'_>) -> Result<RepoIndexStats> {
    let start = Instant::now();
    let mut entities = 0usize; // we will snapshot from writer.counter
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
    // Counting writer increments entities when we newline an entity JSON
    let mut cw = CountingWriter::new(writer);

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
        let mut parser = Parser::new();
        if parser.set_language(&language).is_err() {
            continue;
        }
        let tree = match parser.parse(&src, None) {
            Some(t) => t,
            None => continue,
        };
        let mut entities_local: Vec<Entity> = Vec::new();
        extract_entities(lang, &tree, &src, path, &mut entities_local);
        for e in entities_local.into_iter() {
            writeln!(cw, "{}", serde_json::to_string(&e)?)?;
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

// ---------------- Entity Extraction ----------------

pub(crate) fn extract_entities<'a>(
    lang: &'a str,
    tree: &tree_sitter::Tree,
    src: &'a str,
    path: &Path,
    out: &mut Vec<Entity<'a>>,
) {
    match lang {
        "rust" => generic_class_function_walk(lang, tree, src, path, out, &RustLangSpec),
        "python" => generic_class_function_walk(lang, tree, src, path, out, &PythonLangSpec),
        "javascript" => generic_class_function_walk(lang, tree, src, path, out, &JsLangSpec),
        "typescript" => generic_class_function_walk(lang, tree, src, path, out, &TsLangSpec),
        "java" => generic_class_function_walk(lang, tree, src, path, out, &JavaLangSpec),
        "go" => generic_class_function_walk(lang, tree, src, path, out, &GoLangSpec),
        "cpp" => generic_class_function_walk(lang, tree, src, path, out, &CppLangSpec),
        "c_sharp" => generic_class_function_walk(lang, tree, src, path, out, &CSharpLangSpec),
        "swift" => generic_class_function_walk(lang, tree, src, path, out, &SwiftLangSpec),
        "verilog" => generic_class_function_walk(lang, tree, src, path, out, &VerilogLangSpec),
        _ => {
            // fallback: whole file summary as one entity
            // Use 0-based line numbers internally (start at 0, end at last row = lines-1).
            out.push(Entity {
                file: path.display().to_string(),
                language: lang,
                kind: "file",
                name: path
                    .file_name()
                    .and_then(|s| s.to_str())
                    .unwrap_or("")
                    .to_string(),
                parent: None,
                signature: src.lines().next().unwrap_or("").to_string(),
                start_line: 0,
                end_line: src.lines().count().saturating_sub(1),
                calls: None,
                doc: None,
            });
        }
    }
}

// Spec describing language-specific node kinds & field names
trait LangSpec {
    fn class_kind(&self) -> &'static str;
    fn class_name_field(&self) -> &'static str;
    fn function_kind(&self) -> &'static str;
    fn function_name_field(&self) -> &'static str;
}

struct RustLangSpec;
impl LangSpec for RustLangSpec {
    fn class_kind(&self) -> &'static str {
        "struct_item"
    }
    fn class_name_field(&self) -> &'static str {
        "name"
    }
    fn function_kind(&self) -> &'static str {
        "function_item"
    }
    fn function_name_field(&self) -> &'static str {
        "name"
    }
}
struct PythonLangSpec;
impl LangSpec for PythonLangSpec {
    fn class_kind(&self) -> &'static str {
        "class_definition"
    }
    fn class_name_field(&self) -> &'static str {
        "name"
    }
    fn function_kind(&self) -> &'static str {
        "function_definition"
    }
    fn function_name_field(&self) -> &'static str {
        "name"
    }
}
struct JsLangSpec;
impl LangSpec for JsLangSpec {
    fn class_kind(&self) -> &'static str {
        "class_declaration"
    }
    fn class_name_field(&self) -> &'static str {
        "name"
    }
    fn function_kind(&self) -> &'static str {
        "function_declaration"
    }
    fn function_name_field(&self) -> &'static str {
        "name"
    }
}
struct TsLangSpec;
impl LangSpec for TsLangSpec {
    fn class_kind(&self) -> &'static str {
        "class_declaration"
    }
    fn class_name_field(&self) -> &'static str {
        "name"
    }
    fn function_kind(&self) -> &'static str {
        "function_declaration"
    }
    fn function_name_field(&self) -> &'static str {
        "name"
    }
}
struct JavaLangSpec;
impl LangSpec for JavaLangSpec {
    fn class_kind(&self) -> &'static str {
        "class_declaration"
    }
    fn class_name_field(&self) -> &'static str {
        "name"
    }
    fn function_kind(&self) -> &'static str {
        "method_declaration"
    }
    fn function_name_field(&self) -> &'static str {
        "name"
    }
}
struct GoLangSpec;
impl LangSpec for GoLangSpec {
    fn class_kind(&self) -> &'static str {
        "type_declaration"
    }
    fn class_name_field(&self) -> &'static str {
        "name"
    }
    fn function_kind(&self) -> &'static str {
        "function_declaration"
    }
    fn function_name_field(&self) -> &'static str {
        "name"
    }
}
struct CppLangSpec;
impl LangSpec for CppLangSpec {
    fn class_kind(&self) -> &'static str {
        "class_specifier"
    }
    fn class_name_field(&self) -> &'static str {
        "name"
    }
    fn function_kind(&self) -> &'static str {
        "function_definition"
    }
    fn function_name_field(&self) -> &'static str {
        "declarator"
    }
}
struct CSharpLangSpec;
impl LangSpec for CSharpLangSpec {
    fn class_kind(&self) -> &'static str {
        "class_declaration"
    }
    fn class_name_field(&self) -> &'static str {
        "name"
    }
    fn function_kind(&self) -> &'static str {
        "method_declaration"
    }
    fn function_name_field(&self) -> &'static str {
        "name"
    }
}
struct SwiftLangSpec;
impl LangSpec for SwiftLangSpec {
    fn class_kind(&self) -> &'static str {
        "class_declaration"
    }
    fn class_name_field(&self) -> &'static str {
        "name"
    }
    fn function_kind(&self) -> &'static str {
        "function_declaration"
    }
    fn function_name_field(&self) -> &'static str {
        "name"
    }
}
struct VerilogLangSpec;
impl LangSpec for VerilogLangSpec {
    // Tree-sitter verilog uses `module_declaration` and various function/task nodes.
    // These names are chosen to match common tree-sitter-verilog grammars; if your
    // grammar differs adjust accordingly.
    fn class_kind(&self) -> &'static str {
        "module_declaration"
    }
    fn class_name_field(&self) -> &'static str {
        "name"
    }
    fn function_kind(&self) -> &'static str {
        "function_declaration"
    }
    fn function_name_field(&self) -> &'static str {
        "name"
    }
}

fn node_text<'a>(node: Node<'a>, src: &'a str) -> &'a str {
    node.utf8_text(src.as_bytes()).unwrap_or("")
}

fn extract_node_name<'a>(node: Node<'a>, src: &'a str, preferred_field: &str) -> String {
    // try preferred field name
    let mut found: Option<&str> = None;
    if let Some(n) = node.child_by_field_name(preferred_field) {
        let s = node_text(n, src);
        if !s.is_empty() {
            found = Some(s);
        }
    }
    // common fallbacks
    if found.is_none() {
        for alt in ["name", "identifier", "module_identifier", "declarator"].iter() {
            if let Some(n) = node.child_by_field_name(alt) {
                let s = node_text(n, src);
                if !s.is_empty() {
                    found = Some(s);
                    break;
                }
            }
        }
    }
    // scan children for an identifier-like node
    if found.is_none() {
        let mut w = node.walk();
        for child in node.children(&mut w) {
            let k = child.kind();
            if k.contains("ident") || k.contains("name") || k.contains("module") {
                let s = node_text(child, src);
                if !s.is_empty() {
                    found = Some(s);
                    break;
                }
            }
        }
    }

    // If we didn't find a clear identifier from fields/children, fall back to
    // tokenizing the node signature (useful for grammars that embed the name
    // inside the full declaration text, e.g. Verilog's "function automatic int add(...);").
    let mut candidate = if let Some(s) = found {
        s.to_string()
    } else {
        // extract the node's signature text and use that as a candidate
        extract_signature(&node, src).to_string()
    };

    // If the candidate derived from a child/field is suspiciously short
    // (e.g., single-letter parameter names produced by some grammars),
    // prefer the signature-derived identifier which is likely the real name.
    if candidate.trim().len() <= 2 {
        let sig = extract_signature(&node, src).to_string();
        if !sig.is_empty() {
            candidate = sig;
        }
    }

    // Normalize to a bare identifier: if the candidate looks like a
    // declaration/signature (contains '('), prefer the token immediately
    // before the '('; otherwise prefer the last identifier-like token.
    if let Some(pidx) = candidate.find('(') {
        // slice up to '(' and extract tokens
        let before = &candidate[..pidx];
        let mut tokens: Vec<String> = Vec::new();
        let mut cur = String::new();
        for ch in before.chars() {
            if ch == '_' || ch.is_ascii_alphanumeric() {
                cur.push(ch);
            } else if !cur.is_empty() {
                tokens.push(cur.clone());
                cur.clear();
            }
        }
        if !cur.is_empty() {
            tokens.push(cur);
        }
        if let Some(last) = tokens.last() {
            return last.clone();
        }
    }

    // Fallback: last identifier-like token in whole candidate
    let mut tokens: Vec<String> = Vec::new();
    let mut cur = String::new();
    for ch in candidate.chars() {
        if ch == '_' || ch.is_ascii_alphanumeric() {
            cur.push(ch);
        } else if !cur.is_empty() {
            tokens.push(cur.clone());
            cur.clear();
        }
    }
    if !cur.is_empty() {
        tokens.push(cur);
    }
    if let Some(last) = tokens.last() {
        return last.clone();
    }
    candidate.trim().to_string()
}

fn generic_class_function_walk<'a>(
    lang: &'a str,
    tree: &tree_sitter::Tree,
    src: &'a str,
    path: &Path,
    out: &mut Vec<Entity<'a>>,
    spec: &dyn LangSpec,
) {
    let mut stack = vec![(tree.root_node(), None::<String>)];
    while let Some((node, parent)) = stack.pop() {
        let kind = node.kind();
        if kind == spec.class_kind() {
            let name = extract_node_name(node, src, spec.class_name_field());
            let entity = Entity {
                file: path.display().to_string(),
                language: lang,
                kind: "class",
                name: name.clone(),
                parent: parent.clone(),
                signature: extract_signature(&node, src).to_string(),
                // store 0-based Tree-sitter rows internally; exporters convert to 1-based
                start_line: node.start_position().row,
                end_line: node.end_position().row,
                calls: None,
                doc: extract_doc_comments(&node, src),
            };
            out.push(entity);
            for child in node.children(&mut node.walk()) {
                stack.push((child, Some(name.to_string())));
            }
        } else if kind == spec.function_kind() {
            let name = extract_node_name(node, src, spec.function_name_field());
            let calls = collect_call_idents(node, src);
            out.push(Entity {
                file: path.display().to_string(),
                language: lang,
                kind: "function",
                name: name.clone(),
                parent: parent.clone(),
                signature: extract_signature(&node, src).to_string(),
                // store 0-based Tree-sitter rows internally; exporters convert to 1-based
                start_line: node.start_position().row,
                end_line: node.end_position().row,
                calls: if calls.is_empty() { None } else { Some(calls) },
                doc: extract_doc_comments(&node, src),
            });
            for child in node.children(&mut node.walk()) {
                stack.push((child, parent.clone()));
            }
        } else {
            for child in node.children(&mut node.walk()) {
                stack.push((child, parent.clone()));
            }
        }
    }
}

fn extract_signature<'a>(node: &Node<'a>, src: &'a str) -> &'a str {
    let text = src.get(node.byte_range()).unwrap_or("");
    // Up to first '{' or newline (whichever shorter) for brace languages; else first line
    if let Some(idx) = text.find('{') {
        text[..idx].trim_end()
    } else {
        text.lines().next().unwrap_or("")
    }
}

fn extract_doc_comments<'a>(node: &Node<'a>, src: &'a str) -> Option<String> {
    let start_byte = node.start_byte();
    let prefix = &src[..start_byte.min(src.len())];
    let mut lines: Vec<&str> = prefix.lines().collect();
    let mut doc_rev: Vec<String> = Vec::new();
    while let Some(&line) = lines.last() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            break;
        }
        if trimmed.starts_with("///")
            || trimmed.starts_with("//")
            || trimmed.starts_with('#')
            || trimmed.starts_with("/*")
            || trimmed.starts_with('*')
        {
            doc_rev.push(
                trimmed
                    .trim_start_matches("///")
                    .trim_start_matches("//")
                    .trim_start_matches('#')
                    .trim_start_matches('*')
                    .trim()
                    .to_string(),
            );
            lines.pop();
        } else {
            break;
        }
    }
    if doc_rev.is_empty() {
        None
    } else {
        doc_rev.reverse();
        Some(doc_rev.join("\n"))
    }
}

fn collect_call_idents(func_node: Node, src: &str) -> Vec<String> {
    let mut calls = Vec::new();
    let mut stack = vec![func_node];
    while let Some(node) = stack.pop() {
        let kind = node.kind();
        // Heuristic: identifier nodes inside call_expression or function_call
        if kind == "call_expression" || kind == "function_call" {
            // language dependent
            if let Some(child) = node.child_by_field_name("function") {
                calls.push(node_text(child, src).to_string());
            } else if let Some(first) = node.child(0) {
                if first.is_named() {
                    calls.push(node_text(first, src).to_string());
                }
            }
        }
        for child in node.children(&mut node.walk()) {
            stack.push(child);
        }
    }
    calls.sort();
    calls.dedup();
    calls
}
