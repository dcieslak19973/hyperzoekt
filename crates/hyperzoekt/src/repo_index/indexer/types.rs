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

use num_cpus;
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::path::Path;
use std::time::Duration;

#[derive(Debug, Clone, Default, Serialize)]
pub struct RepoIndexStats {
    pub files_indexed: usize,
    pub entities_indexed: usize,
    pub duration: Duration,
    pub time_reading: Duration,
    pub time_parsing: Duration,
    pub time_extracting: Duration,
    pub time_alias_tree: Duration,
    pub time_alias_fallback: Duration,
    pub time_module_map: Duration,
    pub time_import_edges: Duration,
    pub time_scope_containment: Duration,
    pub time_alias_resolution: Duration,
    pub time_calls_resolution: Duration,
    pub time_pagerank: Duration,
    pub docs_nanos: u128,
    pub calls_nanos: u128,
    pub docs_attempts: usize,
    pub calls_attempts: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entity<'a> {
    pub file: String,
    pub language: &'a str,
    pub kind: &'static str,
    pub name: String,
    pub parent: Option<String>,
    pub signature: String,
    pub start_line: usize,
    pub end_line: usize,
    // calls removed (graph edges now)
    pub doc: Option<String>,
    // Raw identifier names of callees encountered inside this entity (functions/methods only).
    // These are resolved into graph call edges during index building and not persisted.
    pub calls_raw: Vec<String>,
    // Methods found on classes (collected when kind == class)
    pub methods: Vec<crate::repo_index::indexer::payload::MethodItem>,
}

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

#[derive(Clone, Copy, Debug)]
pub struct RankWeights {
    pub call: f32,
    pub import: f32,
    pub containment: f32,
    pub damping: f32,
    pub iterations: usize,
    pub epsilon: f32,
}
impl Default for RankWeights {
    fn default() -> Self {
        Self {
            call: 1.0,
            import: 0.5,
            containment: 0.25,
            damping: 0.85,
            iterations: 10,
            epsilon: 1e-6,
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
        if let Ok(v) = std::env::var("HZ_RANK_EPS") {
            if let Ok(f) = v.parse::<f32>() {
                w.epsilon = f.max(0.0);
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
    pub include_langs: Option<std::collections::HashSet<&'a str>>,
    pub progress: Option<&'a ProgressCallback<'a>>,
    pub concurrency: usize,
}
pub enum RepoIndexOutput<'a> {
    FilePath(&'a Path),
    Writer(&'a mut dyn Write),
    Null,
}
#[derive(Default)]
pub struct RepoIndexOptionsBuilder<'a> {
    root: Option<&'a Path>,
    output: Option<RepoIndexOutput<'a>>,
    include_langs: Option<std::collections::HashSet<&'a str>>,
    progress: Option<&'a ProgressCallback<'a>>,
    concurrency: usize,
}

pub(crate) struct CountingWriter<W: Write> {
    inner: W,
    counter: usize,
}
impl<W: Write> CountingWriter<W> {
    pub fn new(inner: W) -> Self {
        Self { inner, counter: 0 }
    }
    pub fn count(&self) -> usize {
        self.counter
    }
}
impl<W: Write> Write for CountingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let written = self.inner.write(buf)?;
        self.counter += buf[..written].iter().filter(|b| **b == b'\n').count();
        Ok(written)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
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
    pub fn include_langs(mut self, langs: std::collections::HashSet<&'a str>) -> Self {
        self.include_langs = Some(langs);
        self
    }
    pub fn progress(mut self, cb: &'a ProgressCallback<'a>) -> Self {
        self.progress = Some(cb);
        self
    }
    pub fn concurrency(mut self, n: usize) -> Self {
        self.concurrency = n;
        self
    }
    pub fn build(self) -> RepoIndexOptions<'a> {
        let default_conc = if self.concurrency == 0 {
            // prefer explicit env var, fallback to num_cpus
            if let Ok(v) = std::env::var("HZ_INDEX_MAX_CONCURRENCY") {
                if let Ok(n) = v.parse::<usize>() {
                    if n > 0 {
                        n
                    } else {
                        num_cpus::get()
                    }
                } else {
                    num_cpus::get()
                }
            } else {
                num_cpus::get()
            }
        } else {
            self.concurrency
        };
        RepoIndexOptions {
            root: self.root.expect("root required"),
            output: self.output.expect("output required"),
            include_langs: self.include_langs,
            progress: self.progress,
            concurrency: default_conc,
        }
    }
}
