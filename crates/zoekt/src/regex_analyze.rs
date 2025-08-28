//! Regex analysis approximations to derive trigram sets for prefiltering.
//! This is a simplified version of Zoekt's regex/syntax analysis.

use crate::trigram::trigrams;
use regex::Regex;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Prefilter {
    /// A set of required trigrams (any doc must contain all)
    Conj(Vec<[u8; 3]>),
    /// Can't derive trigrams reliably
    None,
}

/// Try to derive a prefilter from a regex by extracting literal runs >=3.
pub fn prefilter_from_regex(pattern: &str) -> Prefilter {
    // Try to find the longest literal substring (very naive)
    // Strip common anchors to avoid confusing the heuristic
    let s = pattern.replace("^", "").replace("$", "");
    // If the pattern compiles, try to detect a literal substring via captures
    if Regex::new(&s).is_ok() {
        // If it's a simple literal, use it
        if let Some(lit) = literal_candidate(&s) {
            let tris: Vec<_> = trigrams(&lit).collect();
            if tris.is_empty() {
                Prefilter::None
            } else {
                Prefilter::Conj(tris)
            }
        } else {
            Prefilter::None
        }
    } else {
        Prefilter::None
    }
}

fn literal_candidate(pat: &str) -> Option<String> {
    // crude: if it has meta chars likely not a pure literal
    if pat.chars().any(|c| "*+?[]()|{}".contains(c)) {
        return None;
    }
    Some(pat.to_string())
}

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
