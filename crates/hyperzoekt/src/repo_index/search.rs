// content moved from service/search.rs

use crate::repo_index::types::{RepoEntity, RepoIndexService, StoredEntity};

pub fn search(svc: &RepoIndexService, query: &str, limit: usize) -> Vec<RepoEntity> {
    let results = search_symbol_exact(svc, query);
    results
        .into_iter()
        .take(limit)
        .map(|e| RepoEntity {
            name: e.name.clone(),
            path: svc.files[e.file_id as usize].path.clone(),
            line: e.start_line as usize,
        })
        .collect()
}

pub fn search_symbol_exact<'a>(svc: &'a RepoIndexService, name: &str) -> Vec<&'a StoredEntity> {
    let key = name.to_lowercase();
    svc.name_index
        .get(&key)
        .map(|ids| {
            ids.iter()
                .filter_map(|id| svc.entities.get(*id as usize))
                .collect()
        })
        .unwrap_or_default()
}

pub fn search_symbol_fuzzy_ranked<'a>(
    svc: &'a RepoIndexService,
    query: &str,
    limit: usize,
) -> Vec<&'a StoredEntity> {
    let q_lower = query.to_lowercase();
    let mut min_rank = f32::MAX;
    let mut max_rank = f32::MIN;
    for e in &svc.entities {
        if e.rank < min_rank {
            min_rank = e.rank;
        }
        if e.rank > max_rank {
            max_rank = e.rank;
        }
    }
    let rank_range = if (max_rank - min_rank).abs() < 1e-9 {
        1.0
    } else {
        max_rank - min_rank
    };
    fn levenshtein(a: &str, b: &str) -> usize {
        let mut dp: Vec<usize> = (0..=b.len()).collect();
        for (i, ca) in a.chars().enumerate() {
            let mut prev = dp[0];
            dp[0] = i + 1;
            for (j, cb) in b.chars().enumerate() {
                let temp = dp[j + 1];
                dp[j + 1] = if ca == cb {
                    prev
                } else {
                    1 + prev.min(dp[j]).min(dp[j + 1])
                };
                prev = temp;
            }
        }
        *dp.last().unwrap()
    }
    let mut scored: Vec<(f32, &StoredEntity)> = Vec::new();
    for e in &svc.entities {
        if e.kind.as_str() == "file" {
            continue;
        }
        let name_lower = e.name.to_lowercase();
        let mut lex = 0.0f32;
        if name_lower == q_lower {
            lex = 1.0;
        } else if name_lower.starts_with(&q_lower) {
            lex = 0.8;
        } else if name_lower.contains(&q_lower) {
            lex = 0.5;
        } else {
            let dist = levenshtein(&name_lower, &q_lower);
            if dist <= 2 {
                lex = (0.3 - 0.1 * dist as f32).max(0.0);
            }
        }
        if lex > 0.0 {
            let norm_rank = (e.rank - min_rank) / rank_range;
            let final_score = lex * 0.6 + norm_rank * 0.4;
            scored.push((final_score, e));
        }
    }
    scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
    scored.truncate(limit);
    scored.into_iter().map(|(_, e)| e).collect()
}

pub fn symbol_ids_exact<'a>(svc: &'a RepoIndexService, name: &str) -> &'a [u32] {
    static EMPTY: [u32; 0] = [];
    svc.name_index
        .get(&name.to_lowercase())
        .map(|v| v.as_slice())
        .unwrap_or(&EMPTY)
}
// Search helpers for RepoIndexService.
// Future: move additional helpers here.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repo_index::types::{FileRecord, RepoIndexService, StoredEntity};

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
            kind: crate::internal::EntityKind::Function,
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
            kind: crate::internal::EntityKind::Function,
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
            rank_weights: crate::internal::RankWeights::default(),
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
}
