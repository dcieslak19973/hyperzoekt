//! Test-only helpers that require access to crate internals.
//!
//! This module lives in `src/` so integration tests under `tests/` (which are
//! compiled as a separate test crate) can access helpers that call `pub(crate)`
//! items. Keeping these helpers in `src/` preserves the ability to exercise
//! internal implementation details from integration tests while avoiding
//! widening the public API surface for consumers.
//!
//! See CONTRIBUTING.md for the recommended pattern.

use crate::shard::ShardReader;
use anyhow::Result;
use std::collections::HashMap;

/// Test-only wrapper to access `ShardReader::symbol_postings_map` (a `pub(crate)` API).
///
/// Integration tests call this helper via `zoekt_rs::test_helpers::symbol_postings_map(&r)`.
/// This keeps `symbol_postings_map` crate-private while allowing tests to verify
/// its behavior.
pub fn symbol_postings_map(r: &ShardReader) -> Result<HashMap<[u8; 3], Vec<u32>>> {
    r.symbol_postings_map()
}
