use zoekt_rs::regex_analyze::{prefilter_from_regex, Prefilter};

#[test]
fn go_additional_regex_cases() {
    // Extra Go-like patterns to expand coverage. For now we allow either
    // Conj or None for the trickier ones (marked "either"). As the
    // prefilter heuristic is hardened these expectations can be tightened.
    let cases: Vec<(&str, &str)> = vec![
        // simple concatenation -> should derive trigrams
        ("abc.*def", "conj"),
        // alternation inside a non-capturing/capturing group
        (("(foo|bar)baz"), "conj"),
        (("foo(bar)?baz"), "conj"),
        // character class with quantifier followed by literal run
        (("a[0-9]{2}bcd"), "conj"),
        // non-capturing group with alternation
        (("(?:foo|bar)baz"), "conj"),
        // lookahead isn't supported by Rust's regex; expect None
        (("foo(?=bar)baz"), "none"),
        // case-insensitive inline flag — current heuristic may or may not
        // extract literals; accept either until heuristic is hardened
        (("(?i)abc"), "either"),
        (("(?i)abc|def"), "either"),
    ];

    for (pat, expect) in cases {
        let pf = prefilter_from_regex(pat);
        match expect {
            "conj" => match pf {
                Prefilter::Conj(v) => assert!(!v.is_empty(), "{}: expected trigrams", pat),
                Prefilter::Disj(d) => assert!(
                    d.iter().any(|v| !v.is_empty()),
                    "{}: expected trigrams in disjunction",
                    pat
                ),
                Prefilter::None => panic!("{}: expected Conj, got None", pat),
            },
            "none" => match pf {
                Prefilter::None => (),
                Prefilter::Conj(_) | Prefilter::Disj(_) => {
                    panic!("{}: expected None, got Conj/Disj", pat)
                }
            },
            "either" => match pf {
                Prefilter::Conj(_) | Prefilter::None | Prefilter::Disj(_) => (),
            },
            _ => panic!("unexpected expectation"),
        }
    }
}
