use hyperzoekt::repo_index::graph::callees_up_to;
use hyperzoekt::repo_index::indexer::types::{EntityKind, RankWeights};
use hyperzoekt::repo_index::types::{FileRecord, RepoIndexService, StoredEntity};

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
