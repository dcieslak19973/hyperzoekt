//! Regex analysis approximations to derive trigram sets for prefiltering.
//! This is a simplified version of Zoekt's regex/syntax analysis.

use crate::trigram::trigrams;
use regex::Regex;
use regex_syntax::hir;
use regex_syntax::Parser as RsParser;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Prefilter {
    /// A set of required trigrams (any doc must contain all)
    Conj(Vec<[u8; 3]>),
    /// Disjunction of conjunctions: any branch's conjunctive tris may match
    Disj(Vec<Vec<[u8; 3]>>),
    /// Can't derive trigrams reliably
    None,
}

/// Try to derive a prefilter from a regex by extracting literal runs >=3.
///
/// This is a heuristic: we extract all contiguous literal runs (taking
/// backslash-escaped chars as literals), ignore character classes and other
/// meta constructs, then compute the union of trigrams for runs with at
/// least three bytes. This improves recall for common patterns like
/// "\bint\s+main\b" (extracts "int" and "main").
pub fn prefilter_from_regex(pattern: &str) -> Prefilter {
    // Strip common anchors and word-boundary tokens to avoid confusing the heuristic
    // e.g. \bword\b should still allow extraction of the literal "word".
    let s = pattern
        .replace("^", "")
        .replace("$", "")
        .replace("\\b", "")
        .replace("\\B", "");
    // Expand any \Q ... \E quoted-literal sequences into escaped
    // literal characters so the HIR parser and literal extractor can
    // treat them as plain literals. Example: "\Q+|*?{[\E" ->
    // "\+\|\*\?\{\[" which yields a contiguous literal run.
    let s = {
        let mut out = String::with_capacity(s.len());
        let mut rest = s.as_str();
        while let Some(pos) = rest.find("\\Q") {
            out.push_str(&rest[..pos]);
            rest = &rest[pos + 2..];
            // find closing \E; if none, consume rest as literal
            let end = rest.find("\\E").unwrap_or(rest.len());
            let quoted = &rest[..end];
            for ch in quoted.chars() {
                // Escape regex metacharacters so parser treats them as literals
                match ch {
                    '\\' | '.' | '+' | '*' | '?' | '(' | ')' | '[' | ']' | '{' | '}' | '|'
                    | '^' | '$' | '\n' | '\r' | '\t' => {
                        out.push('\\');
                        out.push(ch);
                    }
                    _ => out.push(ch),
                }
            }
            // advance rest past the consumed quoted section and the optional \E
            rest = if end < rest.len() {
                &rest[end + 2..]
            } else {
                &rest[end..]
            };
        }
        out.push_str(rest);
        out
    };
    // DEBUG: when diagnosing parity, print processed pattern for \Q sequences
    if pattern.contains("\\Q") {
        // no debug prints
        // eprintln!("[debug] original pattern: {:?}", pattern);
        // eprintln!("[debug] processed pattern for parse: {:?}", s);
    }
    if Regex::new(&s).is_err() {
        return Prefilter::None;
    }

    // Use regex-syntax to parse the pattern and extract literal runs more safely.
    let mut parser = RsParser::new();
    let hir = match parser.parse(&s) {
        Ok(h) => h,
        Err(_) => return Prefilter::None,
    };

    // Helper: collect literal runs (UTF-8 strings) from a given HIR node.
    fn collect_runs_from_hir(h: &hir::Hir) -> Vec<String> {
        let mut runs: Vec<String> = Vec::new();
        for lits in [
            &hir::literal::Literals::prefixes(h),
            &hir::literal::Literals::suffixes(h),
        ] {
            for lit in lits.literals() {
                if lit.len() >= 3 {
                    if let Ok(s) = std::str::from_utf8(lit) {
                        runs.push(s.to_string());
                    }
                }
            }
        }
        // dedupe and apply the same filters as the outer logic
        runs.sort();
        runs.dedup();
        const MAX_RUN_BYTES: usize = 128;
        runs.retain(|r| r.len() <= MAX_RUN_BYTES);
        runs.retain(|r| {
            if r.len() <= 8 {
                return true;
            }
            let bytes = r.as_bytes();
            let first = bytes[0];
            !bytes.iter().all(|&b| b == first)
        });
        runs
    }

    // Helper: derive trigrams from a set of runs using same rules as before
    fn tris_from_runs(runs: &[String]) -> Vec<[u8; 3]> {
        use std::collections::HashSet;
        let mut seen: HashSet<[u8; 3]> = HashSet::new();
        let mut tris: Vec<[u8; 3]> = Vec::new();
        for run in runs.iter() {
            let bytes = run.as_bytes();
            let has_nonword = bytes.iter().any(|&b| {
                let mut bb = b;
                if bb.is_ascii_uppercase() {
                    bb += 32;
                }
                !(bb.is_ascii_lowercase() || bb.is_ascii_digit() || bb == b'_')
            });

            if has_nonword {
                if bytes.is_ascii() && bytes.len() >= 3 {
                    for i in 0..(bytes.len() - 2) {
                        let mut t = [bytes[i], bytes[i + 1], bytes[i + 2]];
                        for b in &mut t {
                            if b.is_ascii_uppercase() {
                                *b = b.to_ascii_lowercase();
                            }
                        }
                        if seen.insert(t) {
                            tris.push(t);
                        }
                    }
                }
            } else {
                for t in trigrams(run) {
                    if seen.insert(t) {
                        tris.push(t);
                    }
                }
            }
        }
        tris
    }

    // If the top-level expression is an alternation, collect per-branch
    // trigrams and return a disjunction of conjunctions. This mirrors the
    // semantics of patterns like `ab|cd` where either branch suffices.
    if let hir::HirKind::Alternation(ref alts) = hir.kind() {
        let mut disj: Vec<Vec<[u8; 3]>> = Vec::new();
        for alt in alts.iter() {
            let runs = collect_runs_from_hir(alt);
            if runs.is_empty() {
                // this branch has no safe runs; skip it
                continue;
            }
            let tris = tris_from_runs(&runs);
            if tris.is_empty() {
                continue;
            }
            disj.push(tris);
        }
        if disj.is_empty() {
            return Prefilter::None;
        }
        // Conservative alternation factoring: if every branch shares at
        // least one trigram in common, we can safely return a Conj of
        // that intersection (it's required by all branches). This
        // improves parity in cases where alternation branches overlap
        // on literal substrings. Otherwise, fall back to Disj.
        use std::collections::HashSet;
        if !disj.is_empty() {
            let mut iter = disj.iter();
            if let Some(first) = iter.next() {
                let mut inter: HashSet<[u8; 3]> = first.iter().cloned().collect();
                for branch in iter {
                    let bset: HashSet<[u8; 3]> = branch.iter().cloned().collect();
                    inter = inter.intersection(&bset).cloned().collect();
                    if inter.is_empty() {
                        break;
                    }
                }
                if !inter.is_empty() {
                    let mut v: Vec<[u8; 3]> = inter.into_iter().collect();
                    v.sort();
                    return Prefilter::Conj(v);
                }
            }
        }
        return Prefilter::Disj(disj);
    }
    // For non-alternation top-level expressions, fall back to the original
    // collection over the whole HIR and produce a single conjunctive prefilter.
    let runs = collect_runs_from_hir(&hir);
    if runs.is_empty() {
        return Prefilter::None;
    }
    let tris = tris_from_runs(&runs);
    if tris.is_empty() {
        Prefilter::None
    } else {
        Prefilter::Conj(tris)
    }
}

// Note: previous AST-based manual extraction helper removed in favor of
// the HIR-based literal extraction helpers present in regex-syntax.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn regex_literal_prefilter() {
        let pf = prefilter_from_regex("fooBar");
        match pf {
            Prefilter::Conj(v) => assert!(v.len() >= 1),
            _ => panic!("expected conj"),
        }
    }
}
