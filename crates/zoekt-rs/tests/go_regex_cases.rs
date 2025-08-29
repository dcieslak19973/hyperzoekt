use zoekt_rs::regex_analyze::{prefilter_from_regex, Prefilter};

#[test]
fn go_derived_regex_cases() {
    // pattern -> expected: "conj" (must derive trigrams), "none" (no trigrams), "either"
    let cases: Vec<(&str, &str)> = vec![
        ("\\bword\\b", "either"),
        ("\\bthe\\b", "either"),
        ("\\b\\w{2,}\\b", "none"),
        ("\\bLITERAL\\b", "either"),
        ("\\bthread\\b", "conj"),
        ("\\bint\\s+main\\b", "conj"),
        ("\\d", "none"),
        ("(?i)\\w", "none"),
        ("a\\.bcd", "conj"),
        ("foo|bar", "conj"),
        ("foo{3}bar", "conj"),
        ("a[bc]def", "conj"),
        ("a.b", "none"),
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
                _ => panic!("{}: expected Conj/Disj, got None", pat),
            },
            "none" => assert!(matches!(pf, Prefilter::None), "{}: expected None", pat),
            "either" => match pf {
                Prefilter::Conj(_) | Prefilter::None | Prefilter::Disj(_) => (),
            },
            _ => unreachable!(),
        }
    }
}
