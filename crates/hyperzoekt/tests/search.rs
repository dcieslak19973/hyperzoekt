use hyperzoekt::repo_index::indexer::types::{EntityKind, RankWeights};
use hyperzoekt::repo_index::search::{search, search_symbol_fuzzy_ranked};
use hyperzoekt::repo_index::types::{FileRecord, RepoIndexService, StoredEntity};

fn make_simple_svc() -> RepoIndexService {
    let files = vec![FileRecord {
        id: 0,
        path: "a.rs".into(),
        language: "rust".into(),
        entities: vec![0, 1],
    }];
    let e0 = StoredEntity {
        id: 0,
        file_id: 0,
        kind: EntityKind::Function,
        name: "foo".into(),
        parent: None,
        signature: "".into(),
        start_line: 1,
        end_line: 2,
        calls: vec!["bar".into()],
        doc: None,
        rank: 0.5,
    };
    let e1 = StoredEntity {
        id: 1,
        file_id: 0,
        kind: EntityKind::Function,
        name: "bar".into(),
        parent: None,
        signature: "".into(),
        start_line: 10,
        end_line: 11,
        calls: vec![],
        doc: None,
        rank: 0.9,
    };
    let mut name_index = std::collections::HashMap::new();
    name_index.insert("foo".to_string(), vec![0]);
    name_index.insert("bar".to_string(), vec![1]);
    RepoIndexService {
        files,
        entities: vec![e0, e1],
        name_index,
        containment_children: vec![Vec::new(); 2],
        containment_parent: vec![None; 2],
        call_edges: vec![vec![1], vec![]],
        reverse_call_edges: vec![vec![], vec![0]],
        import_edges: vec![Vec::new(); 2],
        import_lines: vec![Vec::new(); 2],
        file_entities: vec![0],
        unresolved_imports: vec![Vec::new()],
        rank_weights: RankWeights::default(),
    }
}

#[test]
fn search_exact_returns_expected() {
    let svc = make_simple_svc();
    let res = search(&svc, "foo", 10);
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].name, "foo");
}

#[test]
fn fuzzy_ranking_prefers_high_rank() {
    let svc = make_simple_svc();
    let res = search_symbol_fuzzy_ranked(&svc, "b", 10);
    assert!(!res.is_empty());
    // highest ranked 'bar' should be first
    assert_eq!(res[0].name, "bar");
}
