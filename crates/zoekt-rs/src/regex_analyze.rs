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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ByteAnchors {
    // Distinct bytes that are very likely required by the regex (ASCII domain)
    pub bytes: Vec<u8>,
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

/// Extract a small set of single-byte anchors from the HIR when there are no
/// usable trigram literals. We look for literal bytes in Class::Bytes, literal
/// sequences shorter than 3, and obvious punctuation in concatenations.
pub fn byte_anchors_from_regex(pattern: &str) -> Option<ByteAnchors> {
    use regex_syntax::hir::{Class, Hir, HirKind, Literal};
    let hir = regex_syntax::Parser::new().parse(pattern).ok()?;
    let mut set: std::collections::BTreeSet<u8> = std::collections::BTreeSet::new();

    fn walk(h: &Hir, out: &mut std::collections::BTreeSet<u8>) {
        match h.kind() {
            HirKind::Literal(Literal::Unicode(c)) => {
                if c.is_ascii() {
                    out.insert((*c) as u8);
                }
            }
            HirKind::Literal(Literal::Byte(b)) => {
                out.insert(*b);
            }
            HirKind::Class(Class::Bytes(bytes)) => {
                for rng in bytes.iter() {
                    // pick edges as anchors for tightness
                    out.insert(rng.start());
                    out.insert(rng.end());
                }
            }
            HirKind::Concat(list) | HirKind::Alternation(list) => {
                for sub in list {
                    walk(sub, out);
                }
            }
            HirKind::Repetition(rep) => {
                // Always walk inner; repetition tightness is applied by outer logic.
                walk(&rep.hir, out);
            }
            HirKind::Group(g) => walk(&g.hir, out),
            _ => {}
        }
    }
    walk(&hir, &mut set);

    // Filter to ASCII punctuation and common anchor-worthy bytes
    let mut bytes: Vec<u8> = set.into_iter().filter(|b| b.is_ascii_graphic()).collect();
    // Prioritize '(', ')', '*', '+', '-', '_', '/', '\\'
    bytes.sort_by_key(|b| match *b {
        b'(' | b')' | b'*' | b'+' | b'-' | b'_' | b'/' | b'\\' => 0,
        _ => 1,
    });
    bytes.truncate(4);
    if bytes.is_empty() {
        None
    } else {
        Some(ByteAnchors { bytes })
    }
}

/// Extract longer ASCII substrings that are likely required by the regex.
/// Heuristic: collect maximal runs of fixed bytes in concatenations; for
/// alternations, intersect across branches; ignore variable classes except
/// single-byte classes; ignore zero-width and zero-occurrence repetitions.
pub fn required_substrings_from_regex(pattern: &str) -> Vec<Vec<u8>> {
    use regex_syntax::hir::{Class, Hir, HirKind, Literal, RepetitionKind};
    let hir = match regex_syntax::Parser::new().parse(pattern) {
        Ok(h) => h,
        Err(_) => return Vec::new(),
    };

    fn singleton_byte_from_class(bs: &regex_syntax::hir::ClassBytes) -> Option<u8> {
        let mut it = bs.iter();
        let first = it.next()?;
        if it.next().is_none() && first.start() == first.end() {
            Some(first.start())
        } else {
            None
        }
    }

    fn collect(h: &Hir) -> Vec<Vec<u8>> {
        match h.kind() {
            HirKind::Alternation(list) => {
                let mut common: Option<std::collections::BTreeSet<Vec<u8>>> = None;
                for sub in list {
                    let v = collect(sub);
                    let set: std::collections::BTreeSet<Vec<u8>> = v.into_iter().collect();
                    common = Some(match common {
                        None => set,
                        Some(prev) => prev.intersection(&set).cloned().collect(),
                    });
                }
                common.map(|s| s.into_iter().collect()).unwrap_or_default()
            }
            HirKind::Concat(list) => {
                let mut out: Vec<Vec<u8>> = Vec::new();
                let mut cur: Vec<u8> = Vec::new();
                for sub in list {
                    match sub.kind() {
                        HirKind::Literal(Literal::Unicode(c)) if c.is_ascii() => {
                            cur.push((*c) as u8)
                        }
                        HirKind::Literal(Literal::Byte(b)) => cur.push(*b),
                        HirKind::Class(Class::Bytes(bytes)) => {
                            if let Some(b) = singleton_byte_from_class(bytes) {
                                cur.push(b)
                            } else {
                                if cur.len() >= 2 {
                                    out.push(cur.clone());
                                }
                                cur.clear();
                            }
                        }
                        _ => {
                            if cur.len() >= 2 {
                                out.push(cur.clone());
                            }
                            cur.clear();
                        }
                    }
                }
                if cur.len() >= 2 {
                    out.push(cur);
                }
                out
            }
            HirKind::Repetition(rep) => {
                // Only consider repetitions that require at least one occurrence; otherwise skip.
                match rep.kind {
                    RepetitionKind::ZeroOrMore | RepetitionKind::ZeroOrOne => Vec::new(),
                    _ => collect(&rep.hir),
                }
            }
            HirKind::Group(g) => collect(&g.hir),
            _ => Vec::new(),
        }
    }

    let mut subs = collect(&hir);
    // Keep unique substrings, prioritize longer ones.
    subs.sort_by_key(|v| std::cmp::Reverse(v.len()));
    subs.dedup();
    // Make sure outputs are ASCII only (since we scan bytes)
    subs.retain(|v| v.iter().all(|b| b.is_ascii()));
    subs.truncate(3);
    subs
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

    #[test]
    fn regex_prefilter_edge_cases() {
        // patterns with clear literal runs
        let p = prefilter_from_regex("\\bint\\s+main\\b");
        match p {
            Prefilter::Conj(v) => assert!(v.len() >= 1),
            Prefilter::Disj(d) => assert!(d.iter().any(|v| !v.is_empty())),
            Prefilter::None => panic!("expected conj for int main"),
        }

        let p = prefilter_from_regex("foo|bar");
        match p {
            Prefilter::Conj(v) => assert!(v.len() >= 1),
            Prefilter::Disj(d) => assert!(d.iter().any(|v| !v.is_empty())),
            Prefilter::None => panic!("expected conj or disjunction for foo|bar"),
        }

        // short runs should yield None
        let p = prefilter_from_regex("a.b");
        assert_eq!(p, Prefilter::None);

        // purely meta patterns should be None
        let p = prefilter_from_regex("\\w+\\d*");
        assert_eq!(p, Prefilter::None);
    }

    #[test]
    fn regex_prefilter_more_upstream_patterns() {
        // common word-boundary searches
        let p = prefilter_from_regex("\\bthe\\b");
        match p {
            Prefilter::Conj(v) => assert!(v.len() >= 1),
            Prefilter::Disj(d) => assert!(d.iter().any(|v| !v.is_empty())),
            Prefilter::None => (),
        }

        let p = prefilter_from_regex("\\b\\w{2,}\\b");
        assert_eq!(p, Prefilter::None);

        let p = prefilter_from_regex("foo{3}bar");
        match p {
            Prefilter::Conj(v) => assert!(v.len() >= 1),
            _ => panic!("expected conj for foo{{3}}bar"),
        }
    }

    #[test]
    fn regex_prefilter_strict_golden() {
        use crate::regex_analyze::Prefilter;

        let pf = prefilter_from_regex("abcdef");
        match pf {
            Prefilter::Conj(v) => {
                let mut expect: Vec<[u8; 3]> = vec![*b"abc", *b"bcd", *b"cde", *b"def"];
                expect.sort();
                let mut got = v.clone();
                got.sort();
                assert_eq!(got, expect);
            }
            _ => panic!("expected Conj for simple literal"),
        }
    }
}
