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
