use std::fs::File;
use std::io::Write;
use std::path::PathBuf;

fn escape_csv(s: &str) -> String {
    let mut out = String::from("\"");
    for ch in s.chars() {
        if ch == '"' {
            out.push_str("\"\"");
        } else {
            out.push(ch);
        }
    }
    out.push('"');
    out
}

fn main() -> anyhow::Result<()> {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let test_file = manifest.join("../zoekt-rs/tests/go_regex_parity.rs");
    let src = std::fs::read_to_string(&test_file)?;

    let re = regex::Regex::new(r#"\(\s*"((?:\\.|[^"\\])*)"\s*,\s*"(conj|none|either)"\s*\)"#)?;

    let out_path = PathBuf::from("/tmp/prefilter_inspect.csv");
    let mut out = File::create(&out_path)?;
    writeln!(out, "pattern,expect,actual")?;

    use zoekt_rs::regex_analyze::{prefilter_from_regex, Prefilter};

    let mut counts = std::collections::BTreeMap::new();
    let mut total = 0usize;

    for cap in re.captures_iter(&src) {
        let pat_esc = &cap[1];
        let expect = &cap[2];
        // unescape Rust-style escapes in the literal as present in the test file
        let pat = match unescape_rust_literal(pat_esc) {
            Ok(s) => s,
            Err(_) => pat_esc.to_string(),
        };

        let pf = prefilter_from_regex(&pat);
        let actual = match pf {
            Prefilter::Conj(_) => "Conj",
            Prefilter::Disj(_) => "Disj",
            Prefilter::None => "None",
        };

        writeln!(out, "{},{} ,{}", escape_csv(&pat), expect, actual)?;
        *counts.entry(actual.to_string()).or_insert(0usize) += 1;
        total += 1;
    }

    eprintln!("wrote {} patterns to {}", total, out_path.display());
    eprintln!("summary:");
    for (k, v) in counts.iter() {
        eprintln!("  {}: {}", k, v);
    }

    Ok(())
}

// minimal unescape for common Rust escapes used in the test file (\n, \\)
fn unescape_rust_literal(s: &str) -> Result<String, ()> {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(ch) = chars.next() {
        if ch == '\\' {
            match chars.next() {
                Some('n') => out.push('\n'),
                Some('r') => out.push('\r'),
                Some('t') => out.push('\t'),
                Some('\\') => out.push('\\'),
                Some('"') => out.push('"'),
                Some('0') => out.push('\0'),
                Some(c) => {
                    out.push(c);
                }
                None => return Err(()),
            }
        } else {
            out.push(ch);
        }
    }
    Ok(out)
}
