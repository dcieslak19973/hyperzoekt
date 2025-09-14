// tmp_runner: clean single-file benchmark harness
use anyhow::Result;
use clap::Parser;
use ignore::WalkBuilder;
use rayon::prelude::*;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

#[derive(Parser)]
struct Opts {
    /// Path to a local repo on disk
    path: PathBuf,
    /// Worker threads for pooled mode (0 = default)
    #[clap(long, default_value_t = 0)]
    concurrency: usize,
    /// Mode: hyperzoekt | ast-perfile | ast-pooled | distributed-ast | compare | compare-pagerank | profile-ast
    #[clap(long, default_value = "hyperzoekt")]
    mode: String,
    /// Emit JSON instead of text (compare mode)
    #[clap(long, action)]
    json: bool,
}

fn main() -> Result<()> {
    let opts = Opts::parse();

    // Prepare a minimal hyperzoekt builder for the 'hyperzoekt' mode.
    let mut builder = hyperzoekt::repo_index::indexer::RepoIndexOptions::builder();
    builder = builder.root(&opts.path);
    builder = builder.output_null();
    builder = builder.progress(&|_p: hyperzoekt::repo_index::indexer::types::Progress<'_>| {});
    let cfg = builder.build();
    // no per-file event emission

    match opts.mode.as_str() {
        "hyperzoekt" => {
            let (_svc, stats) = hyperzoekt::repo_index::RepoIndexService::build_with_options(cfg)?;
            println!(
                "hyperzoekt: files_indexed={} entities_indexed={} duration={:?}",
                stats.files_indexed, stats.entities_indexed, stats.duration
            );
            println!(
                "  phases(ms): read={} parse={} extract={} alias_tree={} alias_fallback={} module_map={} import_edges={} scope_contain={} alias_resolve={} calls_resolve={} pagerank={}",
                stats.time_reading.as_millis(),
                stats.time_parsing.as_millis(),
                stats.time_extracting.as_millis(),
                stats.time_alias_tree.as_millis(),
                stats.time_alias_fallback.as_millis(),
                stats.time_module_map.as_millis(),
                stats.time_import_edges.as_millis(),
                stats.time_scope_containment.as_millis(),
                stats.time_alias_resolution.as_millis(),
                stats.time_calls_resolution.as_millis(),
                stats.time_pagerank.as_millis(),
            );
            println!(
                "  extractor: docs_ms={} calls_ms={} docs_attempts={} calls_attempts={}",
                (stats.docs_nanos / 1_000_000),
                (stats.calls_nanos / 1_000_000),
                stats.docs_attempts,
                stats.calls_attempts
            );
        }
        "compare-pagerank" => {
            // Build full service using current implementation to get graph and new ranks
            let (svc, stats) = hyperzoekt::repo_index::RepoIndexService::build_with_options(cfg)?;
            println!(
                "hyperzoekt: files_indexed={} entities_indexed={} duration={:?}",
                stats.files_indexed, stats.entities_indexed, stats.duration
            );

            // Capture new ranks
            let new_ranks: Vec<f32> = svc.entities.iter().map(|e| e.rank).collect();

            // Compute legacy ranks via petgraph on the same graph
            let legacy = compute_legacy_pagerank(&svc);

            // Compare
            let cmp = compare_ranks(&new_ranks, &legacy);
            println!(
                "pagerank-compare: pearson={:.6} spearman={:.6} l1={:.6} l2={:.6} max_abs={:.6} top50_overlap={:.2}%",
                cmp.pearson,
                cmp.spearman,
                cmp.l1,
                cmp.l2,
                cmp.max_abs,
                cmp.topk_overlap * 100.0
            );

            // Show worst offenders
            for (i, (ent_idx, delta, old_v, new_v)) in cmp.worst.iter().take(10).enumerate() {
                let name = svc
                    .entities
                    .get(*ent_idx)
                    .map(|e| e.name.as_str())
                    .unwrap_or("<unknown>");
                println!(
                    "  {}. id={} name={} | legacy={:.6} new={:.6} | abs-delta={:.6}",
                    i + 1,
                    ent_idx,
                    name,
                    old_v,
                    new_v,
                    delta
                );
            }
        }
        "ast-perfile" => {
            let (f, e, d) = ast_benchmark_per_file(&opts.path)?;
            println!(
                "ast-perfile: files_indexed={} entities_indexed={} duration={:?}",
                f, e, d
            );
        }
        "ast-pooled" => {
            let (f, e, d) = ast_benchmark_pooled(&opts.path, opts.concurrency)?;
            println!(
                "ast-pooled: files_indexed={} entities_indexed={} duration={:?}",
                f, e, d
            );
        }
        "distributed-ast" => {
            let (f, e, d) = distributed_ast(&opts.path, opts.concurrency)?;
            println!(
                "distributed-ast: files_indexed={} symbols_extracted={} duration={:?}",
                f, e, d
            );
        }
        "compare" => {
            // Run fast modes first for quick feedback
            let (ast_pool_files, ast_pool_entities, ast_pool_dur, ast_pool_langs) =
                ast_benchmark_pooled_with_langs(&opts.path, opts.concurrency)?;
            println!(
                "ast-pooled: files_indexed={} entities_indexed={} duration={:?}",
                ast_pool_files, ast_pool_entities, ast_pool_dur
            );

            let (dist_files, dist_symbols, dist_dur, dist_langs) =
                distributed_ast_with_langs(&opts.path, opts.concurrency)?;
            println!(
                "distributed-ast: files_indexed={} symbols_extracted={} duration={:?}",
                dist_files, dist_symbols, dist_dur
            );

            let (ast_pf_files, ast_pf_entities, ast_pf_dur) = ast_benchmark_per_file(&opts.path)?;
            println!(
                "ast-perfile: files_indexed={} entities_indexed={} duration={:?}",
                ast_pf_files, ast_pf_entities, ast_pf_dur
            );

            // Full hyperzoekt index (can be slow)
            let start_hz = Instant::now();
            let (_svc, hz_stats) =
                hyperzoekt::repo_index::RepoIndexService::build_with_options(cfg)?;
            let hz_dur = start_hz.elapsed();
            println!(
                "hyperzoekt: files_indexed={} entities_indexed={} duration={:?}",
                hz_stats.files_indexed, hz_stats.entities_indexed, hz_dur
            );

            // Side-by-side differences
            let sym_vs_ent_diff = (dist_symbols as isize) - (hz_stats.entities_indexed as isize);
            let sym_vs_ent_pct = if hz_stats.entities_indexed > 0 {
                (dist_symbols as f64) / (hz_stats.entities_indexed as f64) - 1.0
            } else {
                0.0
            };
            let astpool_vs_ent_diff =
                (ast_pool_entities as isize) - (hz_stats.entities_indexed as isize);
            let astpool_vs_ent_pct = if hz_stats.entities_indexed > 0 {
                (ast_pool_entities as f64) / (hz_stats.entities_indexed as f64) - 1.0
            } else {
                0.0
            };

            if opts.json {
                #[derive(serde::Serialize)]
                struct ModeStats<'a> {
                    files: usize,
                    entities: usize,
                    symbols: usize,
                    duration_ms: u128,
                    per_lang: &'a HashMap<String, (usize, usize)>,
                }
                #[derive(serde::Serialize)]
                struct CompareOut<'a> {
                    hyperzoekt: ModeStats<'a>,
                    ast_pooled: ModeStats<'a>,
                    ast_perfile: ModeStats<'a>,
                    distributed_ast: ModeStats<'a>,
                    diffs: serde_json::Value,
                }
                let diffs = serde_json::json!({
                    "ast_pooled_vs_hyperzoekt": {
                        "diff": astpool_vs_ent_diff,
                        "pct": astpool_vs_ent_pct
                    },
                    "distributed_vs_hyperzoekt": {
                        "diff": sym_vs_ent_diff,
                        "pct": sym_vs_ent_pct
                    }
                });
                let out = CompareOut {
                    hyperzoekt: ModeStats {
                        files: hz_stats.files_indexed,
                        entities: hz_stats.entities_indexed,
                        symbols: 0,
                        duration_ms: hz_dur.as_millis(),
                        per_lang: &HashMap::new(),
                    },
                    ast_pooled: ModeStats {
                        files: ast_pool_files,
                        entities: ast_pool_entities,
                        symbols: 0,
                        duration_ms: ast_pool_dur.as_millis(),
                        per_lang: &ast_pool_langs,
                    },
                    ast_perfile: ModeStats {
                        files: ast_pf_files,
                        entities: ast_pf_entities,
                        symbols: 0,
                        duration_ms: ast_pf_dur.as_millis(),
                        per_lang: &HashMap::new(),
                    },
                    distributed_ast: ModeStats {
                        files: dist_files,
                        entities: 0,
                        symbols: dist_symbols,
                        duration_ms: dist_dur.as_millis(),
                        per_lang: &dist_langs,
                    },
                    diffs,
                };
                println!("{}", serde_json::to_string_pretty(&out)?);
            } else {
                println!(
                    "\nSummary: files(hz/ast-pooled/ast-perfile/dist) = {}/{}/{}/{}",
                    hz_stats.files_indexed, ast_pool_files, ast_pf_files, dist_files
                );
                println!(
                    "Entities vs Symbols: hyperzoekt_entities={} | ast-pooled_entities={} (diff={} {:+.2}%) | distributed_symbols={} (diff={} {:+.2}%)",
                    hz_stats.entities_indexed,
                    ast_pool_entities,
                    astpool_vs_ent_diff,
                    astpool_vs_ent_pct * 100.0,
                    dist_symbols,
                    sym_vs_ent_diff,
                    sym_vs_ent_pct * 100.0
                );
                println!(
                    "Durations: hyperzoekt={:?} | ast-pooled={:?} | ast-perfile={:?} | distributed-ast={:?}",
                    hz_dur, ast_pool_dur, ast_pf_dur, dist_dur
                );
                // Print per-language breakdowns for quick insight
                if !ast_pool_langs.is_empty() || !dist_langs.is_empty() {
                    println!("\nPer-language (entities: ast-pooled, symbols: distributed-ast):");
                    let mut keys: Vec<&String> =
                        ast_pool_langs.keys().chain(dist_langs.keys()).collect();
                    keys.sort();
                    keys.dedup();
                    for k in keys {
                        let (ap_files, ap_entities) =
                            ast_pool_langs.get(k).copied().unwrap_or((0, 0));
                        let (dz_files, dz_symbols) = dist_langs.get(k).copied().unwrap_or((0, 0));
                        println!(
                            "  {}: files(ast-pooled/dist)={}/{} entities={} symbols={}",
                            k, ap_files, dz_files, ap_entities, dz_symbols
                        );
                    }
                }
            }
        }
        "profile-ast" => {
            let (files, entities, parse_ns, docs_ns, calls_ns, docs_cnt, calls_cnt, dur) =
                profile_ast(&opts.path, opts.concurrency)?;
            println!(
                "profile-ast: files={} entities={} duration={:?} parse_ms={} docs_ms={} calls_ms={} docs_count={} calls_count={}",
                files,
                entities,
                dur,
                parse_ns / 1_000_000,
                docs_ns / 1_000_000,
                calls_ns / 1_000_000,
                docs_cnt,
                calls_cnt
            );
        }
        "profile-ast-pooled" => {
            let (files, entities, parse_ns, docs_ns, calls_ns, docs_cnt, calls_cnt, dur) =
                profile_ast_pooled(&opts.path, opts.concurrency)?;
            println!(
                "profile-ast-pooled: files={} entities={} duration={:?} parse_ms={} docs_ms={} calls_ms={} docs_count={} calls_count={}",
                files,
                entities,
                dur,
                parse_ns / 1_000_000,
                docs_ns / 1_000_000,
                calls_ns / 1_000_000,
                docs_cnt,
                calls_cnt
            );
        }
        other => return Err(anyhow::anyhow!(format!("unknown mode: {}", other))),
    }

    Ok(())
}

struct RankCompare {
    pearson: f64,
    spearman: f64,
    l1: f64,
    l2: f64,
    max_abs: f64,
    topk_overlap: f64,
    worst: Vec<(usize, f64, f64, f64)>,
}

fn compare_ranks(new_ranks: &[f32], legacy_ranks: &[f32]) -> RankCompare {
    let n = new_ranks.len().min(legacy_ranks.len());
    let a: Vec<f64> = new_ranks[..n].iter().map(|&v| v as f64).collect();
    let b: Vec<f64> = legacy_ranks[..n].iter().map(|&v| v as f64).collect();

    // Pearson
    let mean = |v: &[f64]| v.iter().sum::<f64>() / (v.len() as f64);
    let ma = mean(&a);
    let mb = mean(&b);
    let mut num = 0.0;
    let mut da = 0.0;
    let mut db = 0.0;
    for i in 0..n {
        let xa = a[i] - ma;
        let xb = b[i] - mb;
        num += xa * xb;
        da += xa * xa;
        db += xb * xb;
    }
    let pearson = if da > 0.0 && db > 0.0 {
        num / (da.sqrt() * db.sqrt())
    } else {
        1.0
    };

    // Spearman via ranking
    let mut ia: Vec<usize> = (0..n).collect();
    ia.sort_by(|&i, &j| a[i].partial_cmp(&a[j]).unwrap());
    let mut ib: Vec<usize> = (0..n).collect();
    ib.sort_by(|&i, &j| b[i].partial_cmp(&b[j]).unwrap());
    let mut rank_a = vec![0usize; n];
    for (r, &i) in ia.iter().enumerate() {
        rank_a[i] = r;
    }
    let mut rank_b = vec![0usize; n];
    for (r, &i) in ib.iter().enumerate() {
        rank_b[i] = r;
    }
    let mut d2 = 0f64;
    for i in 0..n {
        let d = (rank_a[i] as i64 - rank_b[i] as i64) as f64;
        d2 += d * d;
    }
    let spearman = 1.0 - (6.0 * d2) / ((n as f64) * ((n as f64).powi(2) - 1.0)).max(1.0);

    // Errors
    let mut l1 = 0.0;
    let mut l2 = 0.0;
    let mut max_abs = 0.0;
    let mut worst: Vec<(usize, f64, f64, f64)> = Vec::new();
    for i in 0..n {
        let d = (a[i] - b[i]).abs();
        l1 += d;
        l2 += d * d;
        if d > max_abs {
            max_abs = d;
        }
        worst.push((i, d, b[i], a[i]));
    }
    l2 = l2.sqrt();
    worst.sort_by(|x, y| y.1.partial_cmp(&x.1).unwrap());

    // Top-K overlap (K=50 or n)
    let k = n.min(50);
    let mut top_a: Vec<usize> = (0..n).collect();
    top_a.sort_by(|&i, &j| a[j].partial_cmp(&a[i]).unwrap());
    top_a.truncate(k);
    let mut top_b: Vec<usize> = (0..n).collect();
    top_b.sort_by(|&i, &j| b[j].partial_cmp(&b[i]).unwrap());
    top_b.truncate(k);
    let set_a: std::collections::HashSet<usize> = top_a.into_iter().collect();
    let set_b: std::collections::HashSet<usize> = top_b.into_iter().collect();
    let inter = set_a.intersection(&set_b).count();
    let topk_overlap = inter as f64 / (k as f64);

    RankCompare {
        pearson,
        spearman,
        l1,
        l2,
        max_abs,
        topk_overlap,
        worst,
    }
}

fn compute_legacy_pagerank(svc: &hyperzoekt::repo_index::types::RepoIndexService) -> Vec<f32> {
    use petgraph::algo::page_rank;
    use petgraph::graph::DiGraph;
    let n = svc.entities.len();
    if n == 0 {
        return vec![];
    }

    // Build graph with node indices aligned to entity indices.
    let mut g: DiGraph<(), f32> = DiGraph::with_capacity(n, 0);
    let nodes: Vec<_> = (0..n).map(|_| g.add_node(())).collect();
    let w = svc.rank_weights;

    for u in 0..n {
        if let Some(edges) = svc.call_edges.get(u) {
            for &v in edges {
                g.add_edge(nodes[u], nodes[v as usize], w.call);
            }
        }
        if let Some(edges) = svc.import_edges.get(u) {
            for &v in edges {
                g.add_edge(nodes[u], nodes[v as usize], w.import);
            }
        }
        if let Some(children) = svc.containment_children.get(u) {
            for &v in children {
                g.add_edge(nodes[u], nodes[v as usize], w.containment);
            }
        }
        if let Some(Some(p)) = svc.containment_parent.get(u) {
            g.add_edge(nodes[u], nodes[*p as usize], w.containment);
        }
    }

    let pr = page_rank(&g, w.damping as f64, w.iterations);
    // Map back to entity order by index
    let mut out = vec![0.0f32; n];
    for i in 0..n {
        out[i] = pr[i] as f32;
    }
    // Normalize to sum=1 for apples-to-apples
    let s: f32 = out.iter().sum();
    if s > 0.0 {
        for v in &mut out {
            *v /= s;
        }
    }
    out
}

fn ast_benchmark_per_file(root: &PathBuf) -> Result<(usize, usize, Duration)> {
    use tree_sitter::Parser;

    let walker = WalkBuilder::new(root).standard_filters(true).build();
    let mut files = 0usize;
    let mut entities = 0usize;
    let start = Instant::now();

    for dent in walker {
        let dent = match dent {
            Ok(d) => d,
            Err(_) => continue,
        };
        let path = dent.into_path();
        if !path.is_file() {
            continue;
        }
        let lang = match hyperzoekt::repo_index::indexer::index::detect_language(&path) {
            Some(l) => l,
            None => continue,
        };
        let ts_lang = match hyperzoekt::repo_index::indexer::index::lang_to_ts(lang) {
            Some(l) => l,
            None => continue,
        };
        let src = match std::fs::read_to_string(&path) {
            Ok(s) => s,
            Err(_) => continue,
        };

        let mut parser = Parser::new();
        if parser.set_language(&ts_lang).is_err() {
            continue;
        }
        let tree = match parser.parse(&src, None) {
            Some(t) => t,
            None => continue,
        };

        let mut entities_local: Vec<hyperzoekt::repo_index::indexer::types::Entity> = Vec::new();
        hyperzoekt::repo_index::indexer::extract::extract_entities(
            lang,
            &tree,
            &src,
            &path,
            &mut entities_local,
        );
        entities += entities_local.len();
        files += 1;
    }

    Ok((files, entities, start.elapsed()))
}

fn ast_benchmark_pooled(root: &PathBuf, concurrency: usize) -> Result<(usize, usize, Duration)> {
    let walker = WalkBuilder::new(root).standard_filters(true).build();
    let mut paths: Vec<PathBuf> = Vec::new();
    for dent in walker {
        let dent = match dent {
            Ok(d) => d,
            Err(_) => continue,
        };
        let path = dent.into_path();
        if !path.is_file() {
            continue;
        }
        if hyperzoekt::repo_index::indexer::index::detect_language(&path).is_none() {
            continue;
        }
        paths.push(path);
    }

    let files_counter = AtomicUsize::new(0);
    let entities_counter = AtomicUsize::new(0);
    let start = Instant::now();

    let pool = if concurrency > 0 {
        rayon::ThreadPoolBuilder::new()
            .num_threads(concurrency)
            .build()?
    } else {
        rayon::ThreadPoolBuilder::new().build()?
    };

    pool.install(|| {
        paths.par_iter().for_each(|path| {
            let mut parsers: HashMap<&'static str, tree_sitter::Parser> = HashMap::new();
            if let Ok(count) = ast_process_with_parsers(&mut parsers, path) {
                files_counter.fetch_add(1, Ordering::SeqCst);
                entities_counter.fetch_add(count, Ordering::SeqCst);
            }
        });
    });

    Ok((
        files_counter.load(Ordering::SeqCst),
        entities_counter.load(Ordering::SeqCst),
        start.elapsed(),
    ))
}

type LangCounts = HashMap<String, (usize, usize)>;

fn ast_benchmark_pooled_with_langs(
    root: &PathBuf,
    concurrency: usize,
) -> Result<(usize, usize, Duration, LangCounts)> {
    let walker = WalkBuilder::new(root).standard_filters(true).build();
    let mut paths: Vec<PathBuf> = Vec::new();
    for dent in walker {
        let dent = match dent {
            Ok(d) => d,
            Err(_) => continue,
        };
        let path = dent.into_path();
        if !path.is_file() {
            continue;
        }
        if hyperzoekt::repo_index::indexer::index::detect_language(&path).is_none() {
            continue;
        }
        paths.push(path);
    }

    let files_counter = AtomicUsize::new(0);
    let entities_counter = AtomicUsize::new(0);
    let lang_map = std::sync::Mutex::new(HashMap::<String, (usize, usize)>::new());
    let start = Instant::now();

    let pool = if concurrency > 0 {
        rayon::ThreadPoolBuilder::new()
            .num_threads(concurrency)
            .build()?
    } else {
        rayon::ThreadPoolBuilder::new().build()?
    };

    pool.install(|| {
        paths.par_iter().for_each(|path| {
            let mut parsers: HashMap<&'static str, tree_sitter::Parser> = HashMap::new();
            if let Some(lang) = hyperzoekt::repo_index::indexer::index::detect_language(path) {
                if let Ok(count) = ast_process_with_parsers(&mut parsers, path) {
                    files_counter.fetch_add(1, Ordering::SeqCst);
                    entities_counter.fetch_add(count, Ordering::SeqCst);
                    let mut lm = lang_map.lock().unwrap();
                    let entry = lm.entry(lang.to_string()).or_insert((0, 0));
                    entry.0 += 1; // files
                    entry.1 += count; // entities
                }
            }
        });
    });

    Ok((
        files_counter.load(Ordering::SeqCst),
        entities_counter.load(Ordering::SeqCst),
        start.elapsed(),
        lang_map.into_inner().unwrap(),
    ))
}

fn ast_process_with_parsers(
    parsers: &mut HashMap<&'static str, tree_sitter::Parser>,
    path: &PathBuf,
) -> Result<usize> {
    let lang = match hyperzoekt::repo_index::indexer::index::detect_language(path) {
        Some(l) => l,
        None => return Ok(0),
    };
    let language = match hyperzoekt::repo_index::indexer::index::lang_to_ts(lang) {
        Some(l) => l,
        None => return Ok(0),
    };
    let src = match std::fs::read_to_string(path) {
        Ok(s) => s,
        Err(_) => return Ok(0),
    };

    let parser = parsers.entry(lang).or_insert_with(|| {
        let mut p = tree_sitter::Parser::new();
        let _ = p.set_language(&language);
        p
    });
    let tree = match parser.parse(&src, None) {
        Some(t) => t,
        None => return Ok(0),
    };
    let mut entities_local: Vec<hyperzoekt::repo_index::indexer::types::Entity> = Vec::new();
    hyperzoekt::repo_index::indexer::extract::extract_entities(
        lang,
        &tree,
        &src,
        path,
        &mut entities_local,
    );
    Ok(entities_local.len())
}

fn distributed_ast(root: &PathBuf, concurrency: usize) -> Result<(usize, usize, Duration)> {
    // Gather eligible files with known extensions that typesitter maps.
    let walker = WalkBuilder::new(root).standard_filters(true).build();
    let mut paths: Vec<PathBuf> = Vec::new();
    for dent in walker {
        let dent = match dent {
            Ok(d) => d,
            Err(_) => continue,
        };
        let p = dent.into_path();
        if !p.is_file() {
            continue;
        }
        if let Some(ext) = p.extension().and_then(|s| s.to_str()) {
            let ext = ext.to_ascii_lowercase();
            if matches!(
                ext.as_str(),
                "rs" | "go"
                    | "py"
                    | "js"
                    | "jsx"
                    | "ts"
                    | "tsx"
                    | "c"
                    | "h"
                    | "cpp"
                    | "cc"
                    | "cxx"
                    | "hpp"
                    | "hxx"
                    | "cs"
                    | "swift"
                    | "v"
                    | "sv"
                    | "ml"
                    | "mli"
                    | "mll"
            ) {
                paths.push(p);
            }
        }
    }

    let files_counter = AtomicUsize::new(0);
    let symbols_counter = AtomicUsize::new(0);
    let start = Instant::now();

    let pool = if concurrency > 0 {
        rayon::ThreadPoolBuilder::new()
            .num_threads(concurrency)
            .build()?
    } else {
        rayon::ThreadPoolBuilder::new().build()?
    };

    pool.install(|| {
        paths.par_iter().for_each(|p| {
            let ext = p
                .extension()
                .and_then(|s| s.to_str())
                .map(|s| s.to_ascii_lowercase())
                .unwrap_or_default();
            let Ok(src) = std::fs::read_to_string(p) else {
                return;
            };
            let syms = zoekt_rs::typesitter::extract_symbols_typesitter(&src, &ext);
            files_counter.fetch_add(1, Ordering::SeqCst);
            symbols_counter.fetch_add(syms.len(), Ordering::SeqCst);
        });
    });

    Ok((
        files_counter.load(Ordering::SeqCst),
        symbols_counter.load(Ordering::SeqCst),
        start.elapsed(),
    ))
}

fn distributed_ast_with_langs(
    root: &PathBuf,
    concurrency: usize,
) -> Result<(usize, usize, Duration, LangCounts)> {
    let walker = WalkBuilder::new(root).standard_filters(true).build();
    let mut paths: Vec<PathBuf> = Vec::new();
    for dent in walker {
        let dent = match dent {
            Ok(d) => d,
            Err(_) => continue,
        };
        let p = dent.into_path();
        if !p.is_file() {
            continue;
        }
        if let Some(ext) = p.extension().and_then(|s| s.to_str()) {
            let ext = ext.to_ascii_lowercase();
            if matches!(
                ext.as_str(),
                "rs" | "go"
                    | "py"
                    | "js"
                    | "jsx"
                    | "ts"
                    | "tsx"
                    | "c"
                    | "h"
                    | "cpp"
                    | "cc"
                    | "cxx"
                    | "hpp"
                    | "hxx"
                    | "cs"
                    | "swift"
                    | "v"
                    | "sv"
                    | "ml"
                    | "mli"
                    | "mll"
            ) {
                paths.push(p);
            }
        }
    }

    let files_counter = AtomicUsize::new(0);
    let symbols_counter = AtomicUsize::new(0);
    let lang_map = std::sync::Mutex::new(HashMap::<String, (usize, usize)>::new());
    let start = Instant::now();

    let pool = if concurrency > 0 {
        rayon::ThreadPoolBuilder::new()
            .num_threads(concurrency)
            .build()?
    } else {
        rayon::ThreadPoolBuilder::new().build()?
    };

    pool.install(|| {
        paths.par_iter().for_each(|p| {
            let ext = p
                .extension()
                .and_then(|s| s.to_str())
                .map(|s| s.to_ascii_lowercase())
                .unwrap_or_default();
            let Ok(src) = std::fs::read_to_string(p) else {
                return;
            };
            let syms = zoekt_rs::typesitter::extract_symbols_typesitter(&src, &ext);
            files_counter.fetch_add(1, Ordering::SeqCst);
            symbols_counter.fetch_add(syms.len(), Ordering::SeqCst);
            let mut lm = lang_map.lock().unwrap();
            let entry = lm.entry(ext).or_insert((0, 0));
            entry.0 += 1; // files
            entry.1 += syms.len(); // symbols
        });
    });

    Ok((
        files_counter.load(Ordering::SeqCst),
        symbols_counter.load(Ordering::SeqCst),
        start.elapsed(),
        lang_map.into_inner().unwrap(),
    ))
}

#[allow(clippy::type_complexity)]
fn profile_ast(
    root: &PathBuf,
    concurrency: usize,
) -> Result<(usize, usize, u128, u128, u128, usize, usize, Duration)> {
    use tree_sitter::Parser;
    let walker = WalkBuilder::new(root).standard_filters(true).build();
    let mut paths: Vec<PathBuf> = Vec::new();
    for dent in walker {
        let dent = match dent {
            Ok(d) => d,
            Err(_) => continue,
        };
        let p = dent.into_path();
        if p.is_file() {
            paths.push(p);
        }
    }

    let files_counter = AtomicUsize::new(0);
    let entities_counter = AtomicUsize::new(0);
    let parse_nanos = std::sync::atomic::AtomicUsize::new(0);
    let docs_nanos = std::sync::atomic::AtomicUsize::new(0);
    let calls_nanos = std::sync::atomic::AtomicUsize::new(0);
    let docs_count = std::sync::atomic::AtomicUsize::new(0);
    let calls_count = std::sync::atomic::AtomicUsize::new(0);
    let start = Instant::now();

    let pool = if concurrency > 0 {
        rayon::ThreadPoolBuilder::new()
            .num_threads(concurrency)
            .build()?
    } else {
        rayon::ThreadPoolBuilder::new().build()?
    };

    pool.install(|| {
        paths.par_iter().for_each(|path| {
            let Some(lang) = hyperzoekt::repo_index::indexer::index::detect_language(path) else {
                return;
            };
            let Some(language) = hyperzoekt::repo_index::indexer::index::lang_to_ts(lang) else {
                return;
            };
            let Ok(src) = std::fs::read_to_string(path) else {
                return;
            };
            let mut parser = Parser::new();
            if parser.set_language(&language).is_err() {
                return;
            }
            let t0 = Instant::now();
            let Some(tree) = parser.parse(&src, None) else {
                return;
            };
            let parse_elapsed = t0.elapsed().as_nanos();
            let mut ents: Vec<hyperzoekt::repo_index::indexer::types::Entity> = Vec::new();
            let stats = hyperzoekt::repo_index::indexer::extract::extract_entities_with_stats(
                lang, &tree, &src, path, &mut ents,
            );
            files_counter.fetch_add(1, Ordering::SeqCst);
            entities_counter.fetch_add(ents.len(), Ordering::SeqCst);
            parse_nanos.fetch_add(parse_elapsed as usize, Ordering::SeqCst);
            docs_nanos.fetch_add(stats.docs_nanos as usize, Ordering::SeqCst);
            calls_nanos.fetch_add(stats.calls_nanos as usize, Ordering::SeqCst);
            docs_count.fetch_add(stats.docs_attempts as usize, Ordering::SeqCst);
            calls_count.fetch_add(stats.calls_attempts as usize, Ordering::SeqCst);
        });
    });

    Ok((
        files_counter.load(Ordering::SeqCst),
        entities_counter.load(Ordering::SeqCst),
        parse_nanos.load(Ordering::SeqCst) as u128,
        docs_nanos.load(Ordering::SeqCst) as u128,
        calls_nanos.load(Ordering::SeqCst) as u128,
        docs_count.load(Ordering::SeqCst),
        calls_count.load(Ordering::SeqCst),
        start.elapsed(),
    ))
}

#[allow(clippy::type_complexity)]
fn profile_ast_pooled(
    root: &PathBuf,
    concurrency: usize,
) -> Result<(usize, usize, u128, u128, u128, usize, usize, Duration)> {
    use tree_sitter::Parser;
    let walker = WalkBuilder::new(root).standard_filters(true).build();
    let mut paths: Vec<PathBuf> = Vec::new();
    for dent in walker {
        let dent = match dent {
            Ok(d) => d,
            Err(_) => continue,
        };
        let p = dent.into_path();
        if !p.is_file() {
            continue;
        }
        if hyperzoekt::repo_index::indexer::index::detect_language(&p).is_some() {
            paths.push(p);
        }
    }

    let files_counter = AtomicUsize::new(0);
    let entities_counter = AtomicUsize::new(0);
    let parse_nanos = std::sync::atomic::AtomicUsize::new(0);
    let docs_nanos = std::sync::atomic::AtomicUsize::new(0);
    let calls_nanos = std::sync::atomic::AtomicUsize::new(0);
    let docs_count = std::sync::atomic::AtomicUsize::new(0);
    let calls_count = std::sync::atomic::AtomicUsize::new(0);
    let start = Instant::now();

    let pool = if concurrency > 0 {
        rayon::ThreadPoolBuilder::new()
            .num_threads(concurrency)
            .build()?
    } else {
        rayon::ThreadPoolBuilder::new().build()?
    };

    pool.install(|| {
        paths
            .par_iter()
            .with_max_len(1)
            .map_init(HashMap::<&'static str, Parser>::new, |parsers, path| {
                let Some(lang) = hyperzoekt::repo_index::indexer::index::detect_language(path)
                else {
                    return;
                };
                let Some(language) = hyperzoekt::repo_index::indexer::index::lang_to_ts(lang)
                else {
                    return;
                };
                let Ok(src) = std::fs::read_to_string(path) else {
                    return;
                };
                let parser = if let Some(p) = parsers.get_mut(lang) {
                    p
                } else {
                    let mut p = Parser::new();
                    if p.set_language(&language).is_err() {
                        return;
                    }
                    parsers.insert(lang, p);
                    parsers.get_mut(lang).unwrap()
                };
                let t0 = Instant::now();
                let Some(tree) = parser.parse(&src, None) else {
                    return;
                };
                let parse_elapsed = t0.elapsed().as_nanos();
                let mut ents: Vec<hyperzoekt::repo_index::indexer::types::Entity> = Vec::new();
                let stats = hyperzoekt::repo_index::indexer::extract::extract_entities_with_stats(
                    lang, &tree, &src, path, &mut ents,
                );
                files_counter.fetch_add(1, Ordering::SeqCst);
                entities_counter.fetch_add(ents.len(), Ordering::SeqCst);
                parse_nanos.fetch_add(parse_elapsed as usize, Ordering::SeqCst);
                docs_nanos.fetch_add(stats.docs_nanos as usize, Ordering::SeqCst);
                calls_nanos.fetch_add(stats.calls_nanos as usize, Ordering::SeqCst);
                docs_count.fetch_add(stats.docs_attempts as usize, Ordering::SeqCst);
                calls_count.fetch_add(stats.calls_attempts as usize, Ordering::SeqCst);
            })
            .for_each(|_| ());
    });

    Ok((
        files_counter.load(Ordering::SeqCst),
        entities_counter.load(Ordering::SeqCst),
        parse_nanos.load(Ordering::SeqCst) as u128,
        docs_nanos.load(Ordering::SeqCst) as u128,
        calls_nanos.load(Ordering::SeqCst) as u128,
        docs_count.load(Ordering::SeqCst),
        calls_count.load(Ordering::SeqCst),
        start.elapsed(),
    ))
}
