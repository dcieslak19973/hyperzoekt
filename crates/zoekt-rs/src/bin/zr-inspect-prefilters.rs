// Copyright 2025 HyperZoekt Project
// Derived from sourcegraph/zoekt (https://github.com/sourcegraph/zoekt)
// Copyright 2016 Google Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
    let test_dir = manifest.join("../zoekt-rs/tests/regex/go");
    let mut src = String::new();
    if test_dir.exists() {
        for entry in std::fs::read_dir(&test_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("rs") {
                let s = std::fs::read_to_string(&path)?;
                src.push_str(&s);
                src.push('\n');
            }
        }
    } else {
        // fallback: try the single parity file path for older layouts
        let test_file = manifest.join("../zoekt-rs/tests/regex/go/parity.rs");
        src = std::fs::read_to_string(&test_file)?;
    }

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
