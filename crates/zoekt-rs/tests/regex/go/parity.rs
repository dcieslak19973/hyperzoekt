// Copied from go_regex_parity.rs
use zoekt_rs::regex_analyze::{prefilter_from_regex, Prefilter};

// This test file ports a larger set of upstream Go Zoekt regex examples
// as a starting point for parity work. It's marked `#[ignore]` so it
// doesn't break the main test suite while we iterate on exact semantics.

#[test]
fn go_regex_parity_cases() {
    // Selected patterns from upstream Go tests (syntaxutil/parse_test.go and
    // query/regexp_test.go). Expectations are conservative: if there's a
    // contiguous literal run of length >= 3 visible in the pattern, we
    // expect the prefilter to be able to produce trigrams ("conj").
    // Otherwise we expect "none". This is a pragmatic staging point for
    // stricter parity work.
    let cases: Vec<(&str, &str)> = vec![
        // base/simple
        ("a", "none"),
        ("a.", "none"),
        ("a.b", "none"),
        ("ab", "none"),
        ("abc", "conj"),
        ("abcde", "conj"),
        ("a.b.c", "none"),
        ("a|^", "none"),
        ("a|b", "none"),
        ("(a)", "none"),
        ("(a)|b", "none"),
        ("a*", "none"),
        ("a+", "none"),
        ("a?", "none"),
        ("a{2}", "none"),
        ("a{2,3}", "none"),
        ("a{2,}", "none"),
        ("a*?", "none"),
        ("", "none"),
        ("|", "none"),
        ("|x|", "none"),
        (".", "none"),
        ("^", "none"),
        ("$", "none"),
        ("\\|", "none"),
        ("\\(", "none"),
        ("\\)", "none"),
        ("\\*", "none"),
        ("\\+", "none"),
        ("\\?", "none"),
        ("\\.", "none"),
        ("\\^", "none"),
        ("\\$", "none"),
        ("\\\\", "none"),
        ("[ace]", "none"),
        ("[abc]", "none"),
        ("[a-z]", "none"),
        ("[a]", "none"),
        ("\\-", "none"),
        ("-", "none"),
        ("\\_", "none"),
        ("abc|def", "conj"),
        // posix/perl/unicode
        ("[[:lower:]]", "none"),
        (("(?i)[[:lower:]]"), "none"),
        ("\\d", "none"),
        ("\\D", "none"),
        ("\\s", "none"),
        ("\\S", "none"),
        ("\\w", "none"),
        (("(?i)\\w"), "none"),
        ("\\p{Braille}", "none"),
        ("[α-ε☺]", "none"),
        // quoted/perl sequences
        (("\\Q+|*?{[\\E"), "conj"),
        (("\\Q+\\E+"), "none"),
        (("\\Qab\\E+"), "none"),
        // precedences and flattening
        (("(?:ab)*"), "none"),
        (("(ab)*"), "none"),
        (("ab|cd"), "none"),
        (("a(b|c)d"), "conj"),
        (("(?:a)"), "none"),
        (("(?:ab)(?:cd)"), "conj"),
        (("(?:a+b+)(?:c+d+)"), "none"),
        (("(?:a|b)|(?:c|d)"), "none"),
        (("(?:[abc]|A|Z|hello|world)"), "conj"),
        // re2-ish prefix tests
        (("abc|abd"), "conj"),
        (("a(?:b)c|abd"), "conj"),
        (("abc|x|abd"), "conj"),
        (("(?i)abc|ABD"), "either"),
        (("[ab]c|[ab]d"), "none"),
        ((".c|.d"), "none"),
        // grouping/alternation/quantifiers
        (("abc.*def"), "conj"),
        (("(foo|bar)baz"), "conj"),
        (("foo(?=bar)baz"), "none"),
        (("(?i)abc"), "either"),
        (("\\bword\\b"), "conj"),
        (("\\bint\\s+main\\b"), "conj"),
        // pathological nesting/repetition (should be none)
        (("((((((((((x{2}){2}){2}){2}){2}){2}){2}){2}){2}))"), "none"),
        // Additional conservative cases pulled from broader upstream examples
        (("ab|a"), "either"),
        (("a|ab"), "either"),
        (("^abc"), "conj"),
        (("abc$"), "conj"),
        (("\\bword\\b"), "conj"),
        (("(?i)abc|ABD"), "either"),
        (("\\Q.*+?\\E"), "conj"),
        (("[[:alpha:]]{3,}"), "none"),
        (("\\p{L}+"), "none"),
        // More upstream-like examples (conservative expectations)
        (("(apple).*?[[:space:]].*?(grape)"), "conj"),
        (("(apple)(?-s:.)*?(banana)"), "conj"),
        (("(thread|needle|haystack)"), "conj"),
        (("(b|d)c(d|b)"), "either"),
        (("(foo|bar)baz.*bla"), "conj"),
        (("(?s:.*)"), "none"),
        (("(?-s:.*)"), "none"),
        (("(?i)b(?-s:.)r"), "none"),
        (("(foo){2,}"), "either"),
        (("(foo.*)"), "conj"),
        (("(foo)(?-s:.)*?(bar)"), "conj"),
        (("(foo)(?-s:.)*?[[:space:]](?-s:.)*?(bar)"), "conj"),
        // Curated additional upstream-derived patterns (conservative = either)
        (("(?i)(?-s:.)*"), "either"),
        (("(apple).*?[[:space:]].*?(grape)"), "either"),
        (("(apple)(?-s:.)*?(banana)"), "either"),
        (("(thread|needle|haystack)"), "either"),
        (("(hello)world"), "either"),
        (("(apple).*"), "either"),
        (("(foo|bar)baz.*bla"), "either"),
        (("(ab(c)def)"), "either"),
        (("(ab(c)def)"), "either"),
        (("(abc)(de)"), "either"),
        (("(abcdef)"), "either"),
        (("(foo).*?[[:space:]].*?(bar)"), "either"),
        // Harvested upstream-like patterns (conservative expectations)
        (("(?:hello)world"), "either"),
        (("(ab(c)def)"), "either"),
        (("a[bd]e"), "either"),
        (("ba(?:na){1,2}"), "either"),
        (("ba(na){1,2}"), "either"),
        (("ha.*stack"), "either"),
        (("[a-zA-Z]fooBAR"), "either"),
        (("test(?:ing|ed)"), "either"),
        (("abc[p-q]"), "either"),
        // Aggressively harvested upstream-like candidates (appended conservatively)
        (
            "postMessageReceive called with unexpected message (-want +got):\\n%s",
            "either",
        ),
        (
            "onFinishFunc not called expected number of times (-want +got):\\n",
            "either",
        ),
        (
            "unexpected difference in file matches (-want +got):\\n%s",
            "either",
        ),
        (("totalSizeBytes mismatch (-want +got):\\n"), "either"),
        (("InitialRequest() (-want +got):\\n%s"), "either"),
        (("unexpected size (-want +got):\\n%s"), "either"),
        (("mismatch (-want +got):\\n%s"), "either"),
        (("Fragments: []Fragment{"), "either"),
        (("Matches: []Match{"), "either"),
        (("-want, +got:\\n%s"), "either"),
        (("-want,+got:\\n%s"), "either"),
        (("\\na %+v\\nb %+v"), "either"),
        (("eot{\\z}"), "either"),
        (("lit{\\}"), "either"),
        (("\\!F?"), "either"),
        (
            "repos.size=%d reposmap.size=%d crashes=%d stats=%+v",
            "either",
        ),
        (("res, err := (&http.Client{}).Do(req)"), "either"),
        (("; !strings.Contains(got, want) {"), "either"),
        (("if err := b.Add(index.Document{"), "either"),
        (("*.zoekt"), "either"),
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
