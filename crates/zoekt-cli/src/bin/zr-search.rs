use anyhow::Result;
use clap::Parser;
use zoekt_rs::{
    build_in_memory_index,
    query::{Query, Searcher},
    SearchMatch, SearchOpts, ShardReader, ShardSearcher,
};

#[derive(Parser, Debug)]
#[command(name = "zr-search", about = "Search using in-memory index (demo)")]
struct Args {
    /// Path to repository root
    repo: std::path::PathBuf,
    /// Query (literal or regex if --regex)
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
}

fn main() -> Result<()> {
    let args = Args::parse();
    if let Some(shard) = args.shard.as_ref() {
        let rdr = ShardReader::open(shard)?;
        let s = ShardSearcher::new(&rdr);
        let opts = build_opts(&args)?;
        if args.regex {
            let matches = s.search_regex_confirmed(&args.query, &opts);
            print_matches(matches, args.json)?;
        } else {
            let matches = s.search_literal_with_context_opts(&args.query, &opts);
            print_matches(matches, args.json)?;
        }
    } else {
        let idx = build_in_memory_index(&args.repo)?;
        let s = Searcher::new(&idx);
        let q = if args.regex {
            Query::Regex(args.query)
        } else {
            Query::Literal(args.query)
        };
        for r in s.search(&q) {
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
