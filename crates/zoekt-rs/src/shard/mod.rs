//! Shard module: exposes `ShardReader`, `ShardWriter`, and `ShardSearcher`.
//!
//! This file is intentionally small: heavy search logic lives in
//! `shard/searcher.rs` and helpers live in `shard/utils.rs`.

use std::collections::{BTreeMap, HashMap};

/// Shard format constants and canonical type aliases used by submodules.
pub const MAGIC: u32 = 0x5a4f_454b; // 'ZOEK'
pub const VERSION: u32 = 5;

pub type PostingsMap = HashMap<[u8; 3], BTreeMap<u32, Vec<u32>>>;
pub type SymbolPostingsMap = HashMap<[u8; 3], Vec<u32>>;
pub type SymbolTuple = (String, Option<u32>, Option<u32>);
pub type SymbolsTable = Vec<Vec<SymbolTuple>>;

mod writer;
pub use writer::ShardWriter;

pub(crate) mod writer_utils;
// writer_utils is crate-private; not re-exported.

mod reader;
pub use reader::ShardReader;

mod utils;
// Re-export selected helpers from utils into the shard module namespace so
// child modules (e.g. `shard::searcher`) can refer to them as `super::...`.
pub(crate) use utils::{
    intersect_sorted, line_bounds, line_for_offset, read_var_u32_from_mmap, trim_first_n_chars,
    trim_last_n_chars,
};

mod searcher;
pub use searcher::ShardSearcher;

/// Lightweight search match returned by the searcher.
#[derive(Debug, Clone)]
pub struct SearchMatch {
    pub doc: u32,
    pub path: String,
    pub line: u32,
    pub start: u32,
    pub end: u32,
    pub before: String,
    pub line_text: String,
    pub after: String,
    // Lightweight relevance score for ranking matches across files (higher is better).
    pub score: f32,
}

#[derive(Default, Debug, Clone)]
pub struct SearchOpts {
    pub path_prefix: Option<String>,
    pub path_regex: Option<regex::Regex>,
    pub limit: Option<usize>,
    pub context: usize,
    pub branch: Option<String>,
    // If set, trim the before/after context to this many characters each.
    pub snippet_max_chars: Option<usize>,
}

// No bridge functions remain; `shard::searcher` is the canonical place for
// search implementations and callers should construct a `ShardSearcher` and
// call methods on it directly.

// Tests are kept in the `tests/` integration directory. Do not include them here
// to avoid duplicate compilation as both unit and integration tests.
