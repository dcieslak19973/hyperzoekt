use clap::Parser;
use std::fs::{self, File};
use std::io::Write;
use std::path::PathBuf;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use serde_json::json;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::thread;
use zoekt_rs::IndexBuilder;
use zoekt_rs::{Query, Searcher, ShardReader, ShardSearcher, ShardWriter};

fn get_max_rss_kb() -> Option<u64> {
    if let Ok(s) = std::fs::read_to_string("/proc/self/status") {
        for line in s.lines() {
            if line.starts_with("VmHWM:") || line.starts_with("VmRSS:") {
                let parts: Vec<_> = line.split_whitespace().collect();
                if parts.len() >= 2 {
                    if let Ok(kb) = parts[1].parse::<u64>() {
                        return Some(kb);
                    }
                }
            }
        }
    }
    None
}

fn mean_ms(samples: &[u128]) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    let sum: u128 = samples.iter().sum();
    (sum as f64) / (samples.len() as f64) / 1000.0
}

fn pctile_ms(samples: &mut [u128], pct: f64) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    samples.sort_unstable();
    let idx = ((samples.len() as f64) * pct).ceil() as usize - 1;
    let idx = idx.min(samples.len() - 1);
    samples[idx] as f64 / 1000.0
}

// small merge helpers for shard-mode results
fn intersect_sorted_vec(a: &[u32], b: &[u32]) -> Vec<u32> {
    let mut out = Vec::new();
    let mut i = 0usize;
    let mut j = 0usize;
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                out.push(a[i]);
                i += 1;
                j += 1;
            }
        }
    }
    out
}

fn union_sorted_vec(a: &[u32], b: &[u32]) -> Vec<u32> {
    let mut out = Vec::with_capacity(a.len() + b.len());
    let mut i = 0usize;
    let mut j = 0usize;
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => {
                out.push(a[i]);
                i += 1;
            }
            std::cmp::Ordering::Greater => {
                out.push(b[j]);
                j += 1;
            }
            std::cmp::Ordering::Equal => {
                out.push(a[i]);
                i += 1;
                j += 1;
            }
        }
    }
    while i < a.len() {
        out.push(a[i]);
        i += 1;
    }
    while j < b.len() {
        out.push(b[j]);
        j += 1;
    }
    out
}

fn difference_sorted_vec(a: &[u32], b: &[u32]) -> Vec<u32> {
    let mut out = Vec::new();
    let mut i = 0usize;
    let mut j = 0usize;
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => {
                out.push(a[i]);
                i += 1;
            }
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                i += 1;
                j += 1;
            }
        }
    }
    while i < a.len() {
        out.push(a[i]);
        i += 1;
    }
    out
}

fn run_query_on_shard(rdr: &ShardReader, q: &Query) -> Vec<u32> {
    let s = ShardSearcher::new(rdr);
    match q {
        Query::Literal(sq) => s.search_literal(sq).into_iter().map(|(d, _)| d).collect(),
        Query::Regex(r) => s
            .search_regex_prefiltered(r)
            .into_iter()
            .map(|(d, _)| d)
            .collect(),
        Query::And(a, b) => {
            let la = run_query_on_shard(rdr, a);
            if la.is_empty() {
                return vec![];
            }
            let lb = run_query_on_shard(rdr, b);
            intersect_sorted_vec(&la, &lb)
        }
        Query::Or(a, b) => {
            let la = run_query_on_shard(rdr, a);
            if la.is_empty() {
                return run_query_on_shard(rdr, b);
            }
            let lb = run_query_on_shard(rdr, b);
            if lb.is_empty() {
                return la;
            }
            union_sorted_vec(&la, &lb)
        }
        Query::Not(inner) => {
            let all: Vec<u32> = (0..rdr.doc_count()).collect();
            let sub = run_query_on_shard(rdr, inner);
            difference_sorted_vec(&all, &sub)
        }
    }
}

fn run_symbol_select_on_shard(
    rdr: &ShardReader,
    pattern: &str,
    is_regex: bool,
    case_sensitive: bool,
) -> Vec<u32> {
    let s = ShardSearcher::new(rdr);
    let res = s.search_symbols_prefiltered(Some(pattern), is_regex, case_sensitive);
    res.into_iter().map(|r| r.doc).collect()
}

#[derive(Parser)]
struct Opts {
    /// Path to index root (will be built if path doesn't contain an index)
    path: PathBuf,
    /// Optional path to a prebuilt shard file; if set the bench will use the shard reader/searcher
    #[clap(long)]
    shard: Option<PathBuf>,
    /// Optionally write a shard file to <path>/.data/index.shard
    #[clap(long)]
    write_shard: bool,
    /// Do not write shard (overrides default)
    #[clap(long)]
    no_write_shard: bool,
    /// Number of warmup iterations per query
    #[clap(long, default_value = "5")]
    warmup: usize,
    /// Number of measured iterations per query
    #[clap(long, default_value = "50")]
    iters: usize,
    /// Total time budget for the whole bench in seconds (includes indexing)
    #[clap(long, default_value = "180")]
    time_budget: u64,
    /// Print progress/heartbeat during measured iterations
    #[clap(long)]
    verbose: bool,
    /// How many iterations between verbose heartbeat messages
    #[clap(long, default_value = "10")]
    verbose_every: usize,
    /// Optional description to include in output JSON
    #[clap(long)]
    description: Option<String>,
    /// When set, write two shards (with and without symbol postings) and compare symbol-select timings
    #[clap(long)]
    compare_symbol_postings: bool,
    /// Maximum number of branches to index when no explicit branches list is provided
    #[clap(long, default_value = "1")]
    max_branches: usize,
}

// result structs are emitted via serde_json::json!

fn main() -> anyhow::Result<()> {
    let opts = Opts::parse();

    // Total time budget handling
    let total_start = Instant::now();
    let budget = std::time::Duration::from_secs(opts.time_budget);

    // Always build the in-memory index so we have index metadata.
    let rss_before_index = get_max_rss_kb();
    println!("starting index build for {}...", opts.path.display());

    // start a heartbeat thread so users see the process is alive during long indexing
    let hb_stop = Arc::new(AtomicBool::new(false));
    let hb_clone = hb_stop.clone();
    let hb_path = opts.path.clone();
    let hb_handle = thread::spawn(move || {
        let start = Instant::now();
        while !hb_clone.load(Ordering::Relaxed) {
            eprintln!(
                "indexing {}: elapsed {}s",
                hb_path.display(),
                start.elapsed().as_secs()
            );
            std::thread::sleep(std::time::Duration::from_secs(5));
        }
    });

    let ib_start = Instant::now();
    let mut ib = IndexBuilder::new(opts.path.clone()).max_file_size(1_000_000);
    // respect user's max_branches choice
    ib = ib.max_branches(opts.max_branches);
    let idx_inner = ib.build()?;
    let ib_dur = ib_start.elapsed();
    // stop heartbeat and join
    hb_stop.store(true, Ordering::Relaxed);
    let _ = hb_handle.join();
    let rss_after_index = get_max_rss_kb();
    // share index between search and optional writer thread
    let idx = Arc::new(idx_inner);
    let scanned = idx.total_scanned_bytes();

    // we check the budget inline at points where we need to stop

    // choose mode: shard if provided, otherwise use in-memory index
    let use_shard = opts.shard.is_some();

    // sample queries tuned for C/C++ codebases (linux tree):
    // - common C tokens
    // - absent token for short-circuit
    // - AND where one side empty
    // - regex to match "int main" or typical function declarations
    let sample_queries = vec![
        Query::Literal("malloc".into()),
        Query::Literal("kmalloc".into()),
        Query::Literal("pthread_create".into()),
        Query::Literal("EXPORT_SYMBOL".into()),
        Query::Literal("nonexistent_token_abcdefg".into()),
        Query::And(
            Box::new(Query::Literal("malloc".into())),
            Box::new(Query::Literal("nonexistent_token_abcdefg".into())),
        ),
        Query::Regex("\\bint\\s+main\\b".into()),
        Query::Regex("\\bstatic\\s+inline\\b".into()),
        // function pointer pattern: e.g. 'int (*fnptr)('
        Query::Regex("\\(\\s*\\*\\s*\\w+\\s*\\)".into()),
    ];

    // create progress bars using MultiProgress so both overall and per-query bars render
    let m = MultiProgress::new();
    let overall_pb = m.add(ProgressBar::new(sample_queries.len() as u64));
    overall_pb.set_style(
        ProgressStyle::with_template("{msg} [{bar:40.cyan/blue}] {pos}/{len} ({eta})").unwrap(),
    );
    overall_pb.set_message("queries");

    // prepare results and index metadata
    let mut results = Vec::new();
    let mut index_path: Option<String> = None;
    let mut index_size_bytes: Option<u64> = None;
    let mut index_elapsed_ms: Option<u128> = None;
    let mut scanned_bytes: Option<u64> = None;
    let mut shard_write_ms: Option<u128> = None;
    let mut shard_size_bytes: Option<u64> = None;

    // By default do NOT write a shard. Write only when --write-shard is passed
    // and no explicit --shard file was provided.
    let mut do_write_shard = false;
    if opts.write_shard && opts.shard.is_none() {
        do_write_shard = true;
    }
    // If we're writing a shard, do it concurrently so searches can start immediately.
    let mut shard_writer_handle: Option<std::thread::JoinHandle<anyhow::Result<(u128, u64)>>> =
        None;
    if do_write_shard && !use_shard {
        let dir = opts.path.join(".data");
        std::fs::create_dir_all(&dir)?;
        let shard = dir.join("index.shard");
        println!("starting background shard write to {} ...", shard.display());
        // spinner attached to MultiProgress
        let pb = m.add(ProgressBar::new_spinner());
        pb.set_style(ProgressStyle::with_template("{spinner} {msg}").unwrap());
        pb.set_message("writing shard");
        pb.enable_steady_tick(std::time::Duration::from_millis(100));

        let idx_clone = idx.clone();
        let shard_clone = shard.clone();
        // spawn writer thread
        shard_writer_handle = Some(std::thread::spawn(
            move || -> anyhow::Result<(u128, u64)> {
                let start = Instant::now();
                ShardWriter::new(&shard_clone).write_from_index(&idx_clone)?;
                let elapsed = start.elapsed().as_millis();
                let size = std::fs::metadata(&shard_clone)?.len();
                Ok((elapsed, size))
            },
        ));
        // record index_path immediately so JSON can reference it even before write completes
        index_path = Some(shard.display().to_string());
    } else if !do_write_shard {
        println!("not writing shard (pass --write-shard to enable)");
    }

    // If a shard path was provided, prefer it for searching; otherwise use in-memory index
    if use_shard {
        let shard_path = opts.shard.as_ref().unwrap();
        // record shard metadata (if not writing our own)
        if index_path.is_none() {
            index_path = Some(shard_path.display().to_string());
            if let Ok(m) = std::fs::metadata(shard_path) {
                index_size_bytes = Some(m.len());
            }
        }
        println!("using shard {}", shard_path.display());
        let rdr = ShardReader::open(shard_path)?;
        'outer_shard: for (qi, q) in sample_queries.iter().enumerate() {
            println!("query {}/{}: {}", qi + 1, sample_queries.len(), q);
            // per-query progress bar for measured iterations (attach to MultiProgress)
            let per_pb = m.add(ProgressBar::new(opts.iters as u64));
            per_pb.set_style(
                ProgressStyle::with_template("{msg} {bar:40.magenta/white} {pos}/{len} ({eta})")
                    .unwrap(),
            );
            per_pb.set_message(format!("q {}/{}", qi + 1, sample_queries.len()));
            // warmup (but respect budget)
            for _ in 0..opts.warmup {
                if total_start.elapsed() >= budget {
                    break 'outer_shard;
                }
                run_query_on_shard(&rdr, q);
            }
            let mut times = Vec::new();
            let rss_before = get_max_rss_kb();
            for i in 0..opts.iters {
                if total_start.elapsed() >= budget {
                    break;
                }
                if opts.verbose && i > 0 && (i % opts.verbose_every == 0) && overall_pb.is_hidden()
                {
                    // only print heartbeat if the overall progress bar isn't visible
                    println!("  heartbeat: query {} iter {}/{}", qi + 1, i, opts.iters);
                }
                let start = Instant::now();
                let _ = run_query_on_shard(&rdr, q);
                let dur = start.elapsed();
                times.push(dur.as_micros());
                per_pb.inc(1);
            }
            per_pb.finish_and_clear();
            overall_pb.inc(1);
            if total_start.elapsed() >= budget {
                break 'outer_shard;
            }
            let rss_after = get_max_rss_kb();
            let mut times_copy = times.clone();
            let mean = mean_ms(&times_copy);
            let median = pctile_ms(&mut times_copy, 0.5);
            let p95 = pctile_ms(&mut times_copy, 0.95);
            results.push(json!({
                "query": q.to_string(),
                "iters": opts.iters,
                "completed_iters": times.len(),
                "times_us": times,
                "mean_ms": mean,
                "median_ms": median,
                "p95_ms": p95,
                "rss_kb_before": rss_before,
                "rss_kb_after": rss_after,
            }));
        }

        // Additionally, run a small symbol-select benchmark suite when in shard mode.
        let symbol_patterns = [
            ("DoThing", false, true),
            ("dothing", false, false),
            ("Foo", false, false),
            ("^DoThing$", true, true),
        ];
        let mut symbol_results = Vec::new();
        for (pat, is_re, cs) in symbol_patterns.iter() {
            let mut times = Vec::new();
            // warmups
            for _ in 0..opts.warmup {
                if total_start.elapsed() >= budget {
                    break;
                }
                let _ = run_symbol_select_on_shard(&rdr, pat, *is_re, *cs);
            }
            for _ in 0..opts.iters {
                if total_start.elapsed() >= budget {
                    break;
                }
                let start = Instant::now();
                let _ = run_symbol_select_on_shard(&rdr, pat, *is_re, *cs);
                times.push(start.elapsed().as_micros());
            }
            let mut times_copy = times.clone();
            let mean = mean_ms(&times_copy);
            let median = pctile_ms(&mut times_copy, 0.5);
            let p95 = pctile_ms(&mut times_copy, 0.95);
            symbol_results.push(json!({
                "pattern": pat,
                "is_regex": is_re,
                "case_sensitive": cs,
                "iters": opts.iters,
                "completed_iters": times.len(),
                "times_us": times,
                "mean_ms": mean,
                "median_ms": median,
                "p95_ms": p95,
            }));
        }
        // attach symbol results to overall results
        results.push(json!({ "symbol_select_results": symbol_results }));

        // If compare_symbol_postings requested, write a second shard without symbol postings
        if opts.compare_symbol_postings {
            println!("compare_symbol_postings: generating no-symbols shard...");
            let dir = opts.path.join(".data");
            std::fs::create_dir_all(&dir)?;
            let shard_with = dir.join("index-with-symbols.shard");
            let shard_without = dir.join("index-no-symbols.shard");
            // write both shards synchronously here to avoid thread complexity in the benchmark flow
            ShardWriter::new(&shard_with).write_from_index_with_options(&idx, true)?;
            ShardWriter::new(&shard_without).write_from_index_with_options(&idx, false)?;

            let rdr_with = ShardReader::open(&shard_with)?;
            let rdr_without = ShardReader::open(&shard_without)?;

            // run the same symbol patterns on both and collect results
            let symbol_patterns = [
                ("DoThing", false, true),
                ("dothing", false, false),
                ("Foo", false, false),
                ("^DoThing$", true, true),
            ];
            let mut with_results = Vec::new();
            let mut without_results = Vec::new();
            for (pat, is_re, cs) in symbol_patterns.iter() {
                // with postings
                let mut times = Vec::new();
                for _ in 0..opts.warmup {
                    if total_start.elapsed() >= budget {
                        break;
                    }
                    let _ = run_symbol_select_on_shard(&rdr_with, pat, *is_re, *cs);
                }
                for _ in 0..opts.iters {
                    if total_start.elapsed() >= budget {
                        break;
                    }
                    let start = Instant::now();
                    let _ = run_symbol_select_on_shard(&rdr_with, pat, *is_re, *cs);
                    times.push(start.elapsed().as_micros());
                }
                let mut tcopy = times.clone();
                with_results.push(json!({
                    "pattern": pat,
                    "is_regex": is_re,
                    "case_sensitive": cs,
                    "times_us": times,
                    "mean_ms": mean_ms(&tcopy),
                    "median_ms": pctile_ms(&mut tcopy, 0.5),
                    "p95_ms": pctile_ms(&mut tcopy, 0.95),
                }));

                // without postings
                let mut times2 = Vec::new();
                for _ in 0..opts.warmup {
                    if total_start.elapsed() >= budget {
                        break;
                    }
                    let _ = run_symbol_select_on_shard(&rdr_without, pat, *is_re, *cs);
                }
                for _ in 0..opts.iters {
                    if total_start.elapsed() >= budget {
                        break;
                    }
                    let start = Instant::now();
                    let _ = run_symbol_select_on_shard(&rdr_without, pat, *is_re, *cs);
                    times2.push(start.elapsed().as_micros());
                }
                let mut tcopy2 = times2.clone();
                without_results.push(json!({
                    "pattern": pat,
                    "is_regex": is_re,
                    "case_sensitive": cs,
                    "times_us": times2,
                    "mean_ms": mean_ms(&tcopy2),
                    "median_ms": pctile_ms(&mut tcopy2, 0.5),
                    "p95_ms": pctile_ms(&mut tcopy2, 0.95),
                }));
            }
            // attach compare results
            results.push(json!({
                "symbol_compare": {
                    "with_postings": with_results,
                    "without_postings": without_results,
                }
            }));
        }
    } else {
        // use in-memory index for searching
        index_elapsed_ms = Some(ib_dur.as_millis());
        scanned_bytes = Some(scanned);
        // memory metrics before/after indexing
        // (we recorded rss_before_index and rss_after_index above)
        println!(
            "built in-memory index: elapsed {} ms, scanned {} bytes",
            ib_dur.as_millis(),
            scanned
        );
        let searcher = Searcher::new(&idx);
        'outer_idx: for (qi, q) in sample_queries.iter().enumerate() {
            println!("query {}/{}: {}", qi + 1, sample_queries.len(), q);
            let per_pb = m.add(ProgressBar::new(opts.iters as u64));
            per_pb.set_style(
                ProgressStyle::with_template("{msg} {bar:40.magenta/white} {pos}/{len} ({eta})")
                    .unwrap(),
            );
            per_pb.set_message(format!("q {}/{}", qi + 1, sample_queries.len()));
            for _ in 0..opts.warmup {
                if total_start.elapsed() >= budget {
                    break 'outer_idx;
                }
                let _ = searcher.search(q);
            }
            if total_start.elapsed() >= budget {
                break;
            }
            let mut times = Vec::new();
            let rss_before = get_max_rss_kb();
            for i in 0..opts.iters {
                if total_start.elapsed() >= budget {
                    break;
                }
                if opts.verbose && i > 0 && (i % opts.verbose_every == 0) && overall_pb.is_hidden()
                {
                    println!("  heartbeat: query {} iter {}/{}", qi + 1, i, opts.iters);
                }
                let start = Instant::now();
                let _ = searcher.search(q);
                let dur = start.elapsed();
                times.push(dur.as_micros());
                per_pb.inc(1);
            }
            per_pb.finish_and_clear();
            overall_pb.inc(1);
            if total_start.elapsed() >= budget {
                break 'outer_idx;
            }
            let rss_after = get_max_rss_kb();
            let mut times_copy = times.clone();
            let mean = mean_ms(&times_copy);
            let median = pctile_ms(&mut times_copy, 0.5);
            let p95 = pctile_ms(&mut times_copy, 0.95);
            results.push(json!({
                "query": q.to_string(),
                "iters": opts.iters,
                "completed_iters": times.len(),
                "times_us": times,
                "mean_ms": mean,
                "median_ms": median,
                "p95_ms": p95,
                "rss_kb_before": rss_before,
                "rss_kb_after": rss_after,
            }));
        }
    }

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    // wait for shard writer (if any) and capture metrics
    if let Some(h) = shard_writer_handle {
        if let Ok(res) = h.join() {
            match res {
                Ok((ms, size)) => {
                    shard_write_ms = Some(ms);
                    shard_size_bytes = Some(size);
                    // if we hadn't set index_size_bytes, set it now
                    if index_size_bytes.is_none() {
                        index_size_bytes = Some(size);
                    }
                    println!("finished shard write: {} ms, {} bytes", ms, size);
                }
                Err(e) => eprintln!("shard write failed: {}", e),
            }
        } else {
            eprintln!("shard writer thread panicked");
        }
    }

    let out = json!({
        "timestamp_ms": now,
    "path": opts.path.display().to_string(),
    "description": opts.description,
    "results": results,
    // index metadata
    "index_path": index_path,
    "index_size_bytes": index_size_bytes,
    "index_elapsed_ms": index_elapsed_ms,
    "scanned_bytes": scanned_bytes,
    // memory / shard metrics
    "rss_kb_before_index": rss_before_index,
    "rss_kb_after_index": rss_after_index,
    "shard_write_ms": shard_write_ms,
    "shard_size_bytes": shard_size_bytes,
    });

    fs::create_dir_all(".bench_results")?;
    let fname = format!(".bench_results/query-bench-{}.json", now);
    let mut f = File::create(&fname)?;
    let s = serde_json::to_string_pretty(&out)?;
    f.write_all(s.as_bytes())?;
    println!("wrote {}", fname);
    Ok(())
}
