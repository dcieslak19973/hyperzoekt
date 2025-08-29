use zoekt_rs::regex_analyze::{prefilter_from_regex, Prefilter};

#[test]
fn regex_prefilter_edge_cases() {
    // patterns with clear literal runs
    let p = prefilter_from_regex("\\bint\\s+main\\b");
    match p {
        Prefilter::Conj(v) => assert!(v.len() >= 1, "expected some trigrams for int/main"),
        Prefilter::Disj(d) => assert!(
            d.iter().any(|v| !v.is_empty()),
            "expected some trigrams in disjunction"
        ),
        Prefilter::None => panic!("expected conj for int main"),
    }

    let p = prefilter_from_regex("foo|bar");
    match p {
        Prefilter::Conj(v) => assert!(v.len() >= 1),
        Prefilter::Disj(d) => assert!(d.iter().any(|v| !v.is_empty())),
        Prefilter::None => panic!("expected conj or disjunction for foo|bar"),
    }

    // character class should separate runs
    let p = prefilter_from_regex("a[bc]def");
    match p {
        Prefilter::Conj(v) => assert!(v.len() >= 1),
        Prefilter::Disj(d) => assert!(d.iter().any(|v| !v.is_empty())),
        Prefilter::None => panic!("expected conj for a[bc]def"),
    }

    // escaped dot is literal
    let p = prefilter_from_regex("a\\.bcd");
    match p {
        Prefilter::Conj(v) => assert!(v.len() >= 1),
        Prefilter::Disj(d) => assert!(d.iter().any(|v| !v.is_empty())),
        Prefilter::None => panic!("expected conj for a\\.bcd"),
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

    // quantifiers producing longer literals
    let p = prefilter_from_regex("foo{3}bar");
    match p {
        Prefilter::Conj(v) => assert!(v.len() >= 1),
        _ => panic!("expected conj for foo{{3}}bar"),
    }

    // word search for 'thread' should produce trigrams when literals present
    let p = prefilter_from_regex("\\bthread\\b");
    match p {
        Prefilter::Conj(v) => assert!(v.len() >= 1),
        Prefilter::Disj(d) => assert!(d.iter().any(|v| !v.is_empty())),
        Prefilter::None => (),
    }
}
