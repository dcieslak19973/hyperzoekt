use anyhow::Result;
use clap::Parser;
use zoekt_rs::{
    build_in_memory_index,
    query::{QueryPlan, QueryResult, Searcher, SelectKind},
    SearchMatch, SearchOpts, ShardReader, ShardSearcher,
};

#[derive(Parser, Debug)]
#[command(name = "zr-search", about = "Search using in-memory index (demo)")]
struct Args {
    /// Path to repository root
    repo: std::path::PathBuf,
    /// Query (literal by default). You can also pass Zoekt-like filters: repo:, file:, lang:, branch:, case:, path:only, content:only, select=repo|file|symbol.
    query: String,
    /// Treat query as regex
    #[arg(long)]
    regex: bool,
    /// Use a prebuilt shard instead of in-memory indexing
    #[arg(long)]
    shard: Option<std::path::PathBuf>,
    /// Emit JSON (NDJSON)
    #[arg(long)]
    json: bool,
    /// Limit number of matches
    #[arg(long)]
    limit: Option<usize>,
    /// Include N lines of context around the match (currently 0 = only line)
    #[arg(long, default_value_t = 0)]
    context: usize,
    /// Filter by path prefix
    #[arg(long)]
    path_prefix: Option<String>,
    /// Filter by path regex
    #[arg(long)]
    path_regex: Option<String>,
    /// Branch filter (shard search only for now; must match shard metadata)
    #[arg(long)]
    branch: Option<String>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    if let Some(shard) = args.shard.as_ref() {
        let rdr = ShardReader::open(shard)?;
        let s = ShardSearcher::new(&rdr);
        let opts = build_opts(&args)?;

        // Build a QueryPlan similarly to in-memory path so we can honor select= filters.
        let mut qstr = args.query.clone();
        if args.regex && !qstr.contains("regex:") && !qstr.contains("re:") {
            qstr = format!("{} regex:yes", qstr);
        }
        let plan = QueryPlan::parse(&qstr)?;

        if plan.select == SelectKind::Symbol {
            // Use shard-backed symbol search
            let results = s.search_symbols_prefiltered(
                plan.pattern.as_deref(),
                plan.regex,
                plan.case_sensitive,
            );
            print_symbol_results(results, args.json)?;
        } else if plan.regex {
            let matches = s.search_regex_confirmed(&args.query, &opts);
            print_matches(matches, args.json)?;
        } else {
            let matches = s.search_literal_with_context_opts(&args.query, &opts);
            print_matches(matches, args.json)?;
        }
    } else {
        let idx = build_in_memory_index(&args.repo)?;
        let s = Searcher::new(&idx);
        // If --regex given, we annotate plan with regex:yes
        let mut qstr = args.query.clone();
        if args.regex && !qstr.contains("regex:") && !qstr.contains("re:") {
            qstr = format!("{} regex:yes", qstr);
        }
        let plan = QueryPlan::parse(&qstr)?;
        for r in s.search_plan(&plan) {
            println!("{}", r.path);
        }
    }
    Ok(())
}

fn build_opts(args: &Args) -> Result<SearchOpts> {
    let path_regex = if let Some(re) = &args.path_regex {
        Some(regex::Regex::new(re)?)
    } else {
        None
    };
    // context is accepted but currently unused beyond the matching line
    let _ = args.context;
    Ok(SearchOpts {
        path_prefix: args.path_prefix.clone(),
        path_regex,
        limit: args.limit,
        context: args.context,
        branch: args.branch.clone(),
    })
}

fn print_matches(matches: Vec<SearchMatch>, json: bool) -> Result<()> {
    if json {
        for m in matches {
            let v = serde_json::json!({
                "doc": m.doc,
                "path": m.path,
                "line": m.line,
                "start": m.start,
                "end": m.end,
                "before": m.before,
                "line": m.line_text,
                "after": m.after,
            });
            println!("{}", v);
        }
    } else {
        for m in matches {
            println!(
                "{}:{}:{}-{}:\n{}{}{}",
                m.path, m.line, m.start, m.end, m.before, m.line_text, m.after
            );
        }
    }
    Ok(())
}

fn print_symbol_results(results: Vec<QueryResult>, json: bool) -> Result<()> {
    if json {
        for r in results {
            let v = serde_json::json!({
                "doc": r.doc,
                "path": r.path,
                "symbol": r.symbol,
                "symbol_loc": r.symbol_loc.map(|s| serde_json::json!({"name": s.name, "start": s.start, "line": s.line})),
            });
            println!("{}", v);
        }
    } else {
        for r in results {
            if let Some(loc) = r.symbol_loc {
                println!(
                    "{}:symbol:{}:{} (line {:?})",
                    r.path,
                    loc.name,
                    loc.start.unwrap_or(0),
                    loc.line
                );
            } else if let Some(sym) = r.symbol {
                println!("{}:symbol:{}", r.path, sym);
            } else {
                println!("{}:symbol:<unknown>", r.path);
            }
        }
    }
    Ok(())
}
