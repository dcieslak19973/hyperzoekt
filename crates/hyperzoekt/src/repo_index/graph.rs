// content moved from service/graph.rs

use crate::repo_index::types::RepoIndexService;
use std::collections::{HashSet, VecDeque};

/// Breadth-first search up to `depth`. If `forward` is true, follow call_edges; otherwise follow reverse_call_edges.
pub fn bfs_depth(svc: &RepoIndexService, start: u32, depth: u32, forward: bool) -> Vec<u32> {
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
            &svc.call_edges
        } else {
            &svc.reverse_call_edges
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

pub fn callees_up_to(svc: &RepoIndexService, entity_id: u32, depth: u32) -> Vec<u32> {
    bfs_depth(svc, entity_id, depth, true)
}

pub fn callers_up_to(svc: &RepoIndexService, entity_id: u32, depth: u32) -> Vec<u32> {
    bfs_depth(svc, entity_id, depth, false)
}
// Graph helpers / traversal for RepoIndexService.

// Future: additional graph helpers can be added here.
