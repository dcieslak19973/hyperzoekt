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
#[cfg(test)]
mod tests {
    use super::*;
    use crate::repo_index::indexer::types::{EntityKind, RankWeights};
    use crate::repo_index::types::{FileRecord, RepoIndexService, StoredEntity};

    fn make_graph_svc() -> RepoIndexService {
        let files = vec![FileRecord {
            id: 0,
            path: "a.rs".into(),
            language: "rust".into(),
            entities: vec![0, 1, 2],
        }];
        let e0 = StoredEntity {
            id: 0,
            file_id: 0,
            kind: EntityKind::Function,
            name: "a".into(),
            parent: None,
            signature: "".into(),
            start_line: 1,
            end_line: 1,
            calls: vec!["b".into()],
            doc: None,
            rank: 0.0,
        };
        let e1 = StoredEntity {
            id: 1,
            file_id: 0,
            kind: EntityKind::Function,
            name: "b".into(),
            parent: None,
            signature: "".into(),
            start_line: 2,
            end_line: 2,
            calls: vec!["c".into()],
            doc: None,
            rank: 0.0,
        };
        let e2 = StoredEntity {
            id: 2,
            file_id: 0,
            kind: EntityKind::Function,
            name: "c".into(),
            parent: None,
            signature: "".into(),
            start_line: 3,
            end_line: 3,
            calls: vec![],
            doc: None,
            rank: 0.0,
        };
        let mut name_index = std::collections::HashMap::new();
        name_index.insert("a".to_string(), vec![0]);
        name_index.insert("b".to_string(), vec![1]);
        name_index.insert("c".to_string(), vec![2]);
        RepoIndexService {
            files,
            entities: vec![e0, e1, e2],
            name_index,
            containment_children: vec![Vec::new(); 3],
            containment_parent: vec![None; 3],
            call_edges: vec![vec![1], vec![2], vec![]],
            reverse_call_edges: vec![vec![], vec![0], vec![1]],
            import_edges: vec![Vec::new(); 3],
            import_lines: vec![Vec::new(); 3],
            file_entities: vec![0],
            unresolved_imports: vec![Vec::new()],
            rank_weights: RankWeights::default(),
        }
    }

    #[test]
    fn bfs_callees_depth_1() {
        let svc = make_graph_svc();
        let out = callees_up_to(&svc, 0, 1);
        assert_eq!(out, vec![1]);
    }

    #[test]
    fn bfs_callees_depth_2() {
        let svc = make_graph_svc();
        let out = callees_up_to(&svc, 0, 2);
        assert_eq!(out, vec![1, 2]);
    }
}
// Graph helpers / traversal for RepoIndexService.

// Future: additional graph helpers can be added here.
