// Minimal repo-index crate skeleton for Tree-sitter integration

#![allow(dead_code)]

pub mod internal;
pub mod service;
pub use service::RepoIndexService;
