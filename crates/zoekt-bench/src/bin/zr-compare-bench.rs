use clap::Parser;
use serde_json::Value;
use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

fn load(path: &PathBuf) -> serde_json::Result<Value> {
    let s = fs::read_to_string(path).expect("read file");
    serde_json::from_str(&s)
}

#[derive(Parser)]
struct Opts {
    /// Old bench JSON
    old: PathBuf,
    /// New bench JSON
    new: PathBuf,
    /// Emit a JSON comparison report to this path
    #[clap(long)]
    json_out: Option<PathBuf>,
    /// Disable the console table output
    #[clap(long)]
    no_table: bool,
    /// Percent threshold to consider a regression (default 10.0)
    #[clap(long, default_value = "10.0")]
    threshold: f64,
    /// Show top-N regressions
    #[clap(long, default_value = "5")]
    top: usize,
}

fn find_index_file(root: &str) -> Option<PathBuf> {
    let p = Path::new(root);
    let candidates = [p.join(".data").join("index.shard"), p.join("index.shard")];
    for c in &candidates {
        if c.exists() {
            return Some(c.to_path_buf());
        }
    }
    None
}

// Color helpers for deltas and percents
fn colorize_delta_bytes(delta: i64) -> String {
    if delta == 0 {
        format!("{:+}", delta)
    } else if delta > 0 {
        format!("\x1b[31m{:+}\x1b[0m", delta)
    } else {
        format!("\x1b[32m{:+}\x1b[0m", delta)
    }
}

fn colorize_delta_ms(delta: f64) -> String {
    if delta == 0.0 {
        format!("{:+.1}", delta)
    } else if delta > 0.0 {
        format!("\x1b[31m{:+.1}\x1b[0m", delta)
    } else {
        format!("\x1b[32m{:+.1}\x1b[0m", delta)
    }
}

fn colorize_pct(pct: f64) -> String {
    if pct == 0.0 {
        format!("{:+.1}", pct)
    } else if pct > 0.0 {
        format!("\x1b[31m{:+.1}\x1b[0m", pct)
    } else {
        format!("\x1b[32m{:+.1}\x1b[0m", pct)
    }
}

fn main() {
    let opts = Opts::parse();
    let a = opts.old.clone();
    let b = opts.new.clone();
    let va = load(&a).expect("parse a");
    let vb = load(&b).expect("parse b");

    let ra = va["results"].as_array().expect("results array");
    let rb = vb["results"].as_array().expect("results array");

    println!("Comparing {} -> {}", a.display(), b.display());

    let mut ma: HashMap<String, &Value> = HashMap::new();
    for r in ra.iter() {
        if let Some(qv) = r.get("query") {
            if let Some(qs) = qv.as_str() {
                ma.insert(qs.to_string(), r);
            }
        }
    }
    let mut mb: HashMap<String, &Value> = HashMap::new();
    for r in rb.iter() {
        if let Some(qv) = r.get("query") {
            if let Some(qs) = qv.as_str() {
                mb.insert(qs.to_string(), r);
            }
        }
    }

    let mut keys: Vec<String> = ma.keys().chain(mb.keys()).cloned().collect();
    keys.sort();
    keys.dedup();

    let idx_a = va
        .get("index_path")
        .and_then(|v| v.as_str())
        .map(PathBuf::from)
        .or_else(|| {
            va.get("path")
                .and_then(|v| v.as_str())
                .and_then(find_index_file)
        });
    let idx_b = vb
        .get("index_path")
        .and_then(|v| v.as_str())
        .map(PathBuf::from)
        .or_else(|| {
            vb.get("path")
                .and_then(|v| v.as_str())
                .and_then(find_index_file)
        });

    let mut report = serde_json::map::Map::new();
    let mut entries: Vec<serde_json::Value> = Vec::new();

    for k in keys.iter() {
        let aa = ma.get(k.as_str());
        let bb = mb.get(k.as_str());
        match (aa, bb) {
            (Some(ae), Some(be)) => {
                let mean_a = ae["mean_ms"].as_f64().unwrap_or(0.0);
                let mean_b = be["mean_ms"].as_f64().unwrap_or(0.0);
                let med_a = ae["median_ms"].as_f64().unwrap_or(0.0);
                let med_b = be["median_ms"].as_f64().unwrap_or(0.0);
                let p95_a = ae["p95_ms"].as_f64().unwrap_or(0.0);
                let p95_b = be["p95_ms"].as_f64().unwrap_or(0.0);
                let pct_mean = if mean_a == 0.0 {
                    0.0
                } else {
                    (mean_b - mean_a) / mean_a * 100.0
                };

                let rss_before_a = ae
                    .get("rss_kb_before")
                    .and_then(|v| v.as_f64())
                    .unwrap_or(0.0);
                let rss_after_a = ae
                    .get("rss_kb_after")
                    .and_then(|v| v.as_f64())
                    .unwrap_or(0.0);
                let rss_before_b = be
                    .get("rss_kb_before")
                    .and_then(|v| v.as_f64())
                    .unwrap_or(0.0);
                let rss_after_b = be
                    .get("rss_kb_after")
                    .and_then(|v| v.as_f64())
                    .unwrap_or(0.0);

                let size_a = va
                    .get("index_size_bytes")
                    .and_then(|v| v.as_i64())
                    .or_else(|| {
                        idx_a
                            .as_ref()
                            .and_then(|p| fs::metadata(p).ok().map(|m| m.len() as i64))
                    })
                    .unwrap_or(0);
                let size_b = vb
                    .get("index_size_bytes")
                    .and_then(|v| v.as_i64())
                    .or_else(|| {
                        idx_b
                            .as_ref()
                            .and_then(|p| fs::metadata(p).ok().map(|m| m.len() as i64))
                    })
                    .unwrap_or(0);
                let index_elapsed_a = va
                    .get("index_elapsed_ms")
                    .and_then(|v| v.as_f64())
                    .unwrap_or(0.0);
                let index_elapsed_b = vb
                    .get("index_elapsed_ms")
                    .and_then(|v| v.as_f64())
                    .unwrap_or(0.0);
                let shard_write_ms_a = va
                    .get("shard_write_ms")
                    .and_then(|v| v.as_f64())
                    .unwrap_or(0.0);
                let shard_write_ms_b = vb
                    .get("shard_write_ms")
                    .and_then(|v| v.as_f64())
                    .unwrap_or(0.0);
                let shard_size_a = va
                    .get("shard_size_bytes")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0);
                let shard_size_b = vb
                    .get("shard_size_bytes")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0);

                if !opts.no_table {
                    println!(
                        "query={} mean: {:.3} -> {:.3} ms ({:+.2}%), median: {:.3} -> {:.3} ms, p95: {:.3} -> {:.3} ms, rss_kb: [{:.0}->{:.0}] -> [{:.0}->{:.0}]",
                        k, mean_a, mean_b, pct_mean, med_a, med_b, p95_a, p95_b, rss_before_a, rss_after_a, rss_before_b, rss_after_b
                    );
                }

                entries.push(serde_json::json!({
                    "query": k,
                    "mean_a_ms": mean_a,
                    "mean_b_ms": mean_b,
                    "pct_mean": pct_mean,
                    "median_a_ms": med_a,
                    "median_b_ms": med_b,
                    "p95_a_ms": p95_a,
                    "p95_b_ms": p95_b,
                    "rss_before_a_kb": rss_before_a,
                    "rss_after_a_kb": rss_after_a,
                    "rss_before_b_kb": rss_before_b,
                    "rss_after_b_kb": rss_after_b,
                    "index_size_a": size_a,
                    "index_size_b": size_b,
                    "index_elapsed_a_ms": index_elapsed_a,
                    "index_elapsed_b_ms": index_elapsed_b,
                    "shard_write_ms_a": shard_write_ms_a,
                    "shard_write_ms_b": shard_write_ms_b,
                    "shard_size_a": shard_size_a,
                    "shard_size_b": shard_size_b,
                }));
            }
            (Some(aa), None) => {
                let mean_a = aa["mean_ms"].as_f64().unwrap_or(0.0);
                if !opts.no_table {
                    println!("query={} present in old only mean={:.3} ms", k, mean_a);
                }
                entries.push(
                    serde_json::json!({"query": k, "present": "old_only", "mean_a_ms": mean_a}),
                );
            }
            (None, Some(bb)) => {
                let mean_b = bb["mean_ms"].as_f64().unwrap_or(0.0);
                if !opts.no_table {
                    println!("query={} present in new only mean={:.3} ms", k, mean_b);
                }
                entries.push(
                    serde_json::json!({"query": k, "present": "new_only", "mean_b_ms": mean_b}),
                );
            }
            _ => {}
        }
    }

    report.insert("entries".to_string(), serde_json::Value::Array(entries));

    // indexing summary (bench-level)
    let index_size_a = va
        .get("index_size_bytes")
        .and_then(|v| v.as_i64())
        .or_else(|| {
            idx_a
                .as_ref()
                .and_then(|p| fs::metadata(p).ok().map(|m| m.len() as i64))
        })
        .unwrap_or(0);
    let index_size_b = vb
        .get("index_size_bytes")
        .and_then(|v| v.as_i64())
        .or_else(|| {
            idx_b
                .as_ref()
                .and_then(|p| fs::metadata(p).ok().map(|m| m.len() as i64))
        })
        .unwrap_or(0);
    let index_size_delta = index_size_b - index_size_a;
    let index_size_delta_pct = if index_size_a == 0 {
        0.0
    } else {
        (index_size_delta as f64) / (index_size_a as f64) * 100.0
    };
    let index_elapsed_a = va
        .get("index_elapsed_ms")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);
    let index_elapsed_b = vb
        .get("index_elapsed_ms")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);
    let index_elapsed_delta = index_elapsed_b - index_elapsed_a;
    let shard_write_ms_a = va
        .get("shard_write_ms")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);
    let shard_write_ms_b = vb
        .get("shard_write_ms")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);
    let shard_write_ms_delta = shard_write_ms_b - shard_write_ms_a;
    let shard_size_a = va
        .get("shard_size_bytes")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let shard_size_b = vb
        .get("shard_size_bytes")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let shard_size_delta = shard_size_b - shard_size_a;

    let mut summary = serde_json::map::Map::new();
    summary.insert("index_size_a".to_string(), serde_json::json!(index_size_a));
    summary.insert("index_size_b".to_string(), serde_json::json!(index_size_b));
    summary.insert(
        "index_size_delta".to_string(),
        serde_json::json!(index_size_delta),
    );
    summary.insert(
        "index_size_delta_pct".to_string(),
        serde_json::json!(index_size_delta_pct),
    );
    summary.insert(
        "index_elapsed_a_ms".to_string(),
        serde_json::json!(index_elapsed_a),
    );
    summary.insert(
        "index_elapsed_b_ms".to_string(),
        serde_json::json!(index_elapsed_b),
    );
    summary.insert(
        "index_elapsed_delta_ms".to_string(),
        serde_json::json!(index_elapsed_delta),
    );
    summary.insert(
        "shard_write_ms_a".to_string(),
        serde_json::json!(shard_write_ms_a),
    );
    summary.insert(
        "shard_write_ms_b".to_string(),
        serde_json::json!(shard_write_ms_b),
    );
    summary.insert(
        "shard_write_ms_delta".to_string(),
        serde_json::json!(shard_write_ms_delta),
    );
    summary.insert("shard_size_a".to_string(), serde_json::json!(shard_size_a));
    summary.insert("shard_size_b".to_string(), serde_json::json!(shard_size_b));
    summary.insert(
        "shard_size_delta".to_string(),
        serde_json::json!(shard_size_delta),
    );
    report.insert(
        "indexing_summary".to_string(),
        serde_json::Value::Object(summary),
    );

    if !opts.no_table {
        println!("\nIndexing summary:");
        // header
        println!(
            "{:<22} | {:>15} | {:>15} | {:>15} | {:>8}",
            "metric", "old", "new", "delta", "%chg"
        );
        println!("{:-<80}", "");

        // index size row
        let idx_size_delta_str = colorize_delta_bytes(index_size_delta);
        let idx_size_pct_str = colorize_pct(index_size_delta_pct);
        println!(
            "{:<22} | {:>15} | {:>15} | {:>15} | {:>8}",
            "index_size_bytes", index_size_a, index_size_b, idx_size_delta_str, idx_size_pct_str
        );

        // index elapsed row (ms)
        let idx_elapsed_delta_str = colorize_delta_ms(index_elapsed_delta);
        let idx_elapsed_pct = if index_elapsed_a == 0.0 {
            0.0
        } else {
            (index_elapsed_delta / index_elapsed_a) * 100.0
        };
        let idx_elapsed_pct_str = colorize_pct(idx_elapsed_pct);
        println!(
            "{:<22} | {:>15.1} | {:>15.1} | {:>15} | {:>8}",
            "index_elapsed_ms",
            index_elapsed_a,
            index_elapsed_b,
            idx_elapsed_delta_str,
            idx_elapsed_pct_str
        );

        // shard write row (ms)
        let shard_write_delta_str = colorize_delta_ms(shard_write_ms_delta);
        let shard_write_pct = if shard_write_ms_a == 0.0 {
            0.0
        } else {
            (shard_write_ms_delta / shard_write_ms_a) * 100.0
        };
        let shard_write_pct_str = colorize_pct(shard_write_pct);
        println!(
            "{:<22} | {:>15.1} | {:>15.1} | {:>15} | {:>8}",
            "shard_write_ms",
            shard_write_ms_a,
            shard_write_ms_b,
            shard_write_delta_str,
            shard_write_pct_str
        );

        // shard size row
        let shard_size_delta_str = colorize_delta_bytes(shard_size_delta);
        let shard_size_pct = if shard_size_a == 0 {
            0.0
        } else {
            (shard_size_delta as f64) / (shard_size_a as f64) * 100.0
        };
        let shard_size_pct_str = colorize_pct(shard_size_pct);
        println!(
            "{:<22} | {:>15} | {:>15} | {:>15} | {:>8}",
            "shard_size_bytes",
            shard_size_a,
            shard_size_b,
            shard_size_delta_str,
            shard_size_pct_str
        );
    }

    // top regressions
    let mut regs: Vec<(String, f64)> = Vec::new();
    if let Some(serde_json::Value::Array(es)) = report.get("entries") {
        for e in es.iter() {
            if let (Some(q), Some(pct)) = (
                e.get("query").and_then(|v| v.as_str()),
                e.get("pct_mean").and_then(|v| v.as_f64()),
            ) {
                if pct > opts.threshold {
                    regs.push((q.to_string(), pct));
                }
            }
        }
    }
    regs.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    if !opts.no_table {
        println!("\nTop regressions (> {}%):", opts.threshold);
        for (i, (q, pct)) in regs.iter().take(opts.top).enumerate() {
            println!("{}. {} ({:+.2}%)", i + 1, q, pct);
        }
    }

    if let Some(p) = opts.json_out {
        let mut f = fs::File::create(&p).expect("create out");
        let s = serde_json::to_string_pretty(&serde_json::Value::Object(report)).expect("json");
        f.write_all(s.as_bytes()).expect("write");
        eprintln!("wrote json report: {}", p.display());
    }
}
