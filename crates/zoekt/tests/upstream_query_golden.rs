// Ported golden cases from upstream Zoekt query/regexp tests.
use zoekt_rs::query as query_mod;
use zoekt_rs::regex_analyze;

#[test]
fn parse_basic_cases_from_upstream() {
    // Collected upstream query parse inputs. We run them through the Rust
    // parser to record current behavior. Upstream expectations may differ; we
    // intentionally do not assert exact outcomes here to start collecting
    // golden inputs for later parity work.
    let cases = [
        "\\bword\\b",
        "fi\"le:bla\"",
        "(abc def)",
        "(abc",
        "--",
        "\"abc",
        "abc.*def",
        "abc\\.\\*def",
        "lang:c++",
    ];

    for inp in cases {
        // Ensure parsing runs and does not panic; record result (ok or err)
        let _ = query_mod::QueryPlan::parse(inp);
    }
}

#[test]
fn regex_prefilter_alternation_cases() {
    // Alternation cases where prefiltering is deterministic:
    // - "foobar|foobaz" shares prefix trigrams -> should factor into Conj of intersection
    // - "abc|def" has disjoint branches -> should return Disj of two single-trigram branches
    use regex_analyze::Prefilter;

    // Conj intersection case
    let pf = regex_analyze::prefilter_from_regex("foobar|foobaz");
    match pf {
        Prefilter::Conj(mut v) => {
            v.sort();
            let mut expect = vec![*b"foo", *b"oob", *b"oba"];
            expect.sort();
            assert_eq!(v, expect);
        }
        other => panic!("expected Conj for foobar|foobaz, got: {:?}", other),
    }

    // Disj separate-branches case
    let pf = regex_analyze::prefilter_from_regex("abc|def");
    match pf {
        Prefilter::Disj(mut branches) => {
            // Normalize ordering inside each branch
            for b in branches.iter_mut() {
                b.sort();
            }
            let mut expected: Vec<Vec<[u8; 3]>> = vec![vec![*b"abc"], vec![*b"def"]];
            for b in expected.iter_mut() {
                b.sort();
            }
            assert_eq!(branches, expected);
        }
        other => panic!("expected Disj for abc|def, got: {:?}", other),
    }
}

#[test]
fn regexp_character_class_case() {
    // from upstream: abc[p-q]
    // For upstream regex case, exercise regex prefilter extraction instead.
    let pf = regex_analyze::prefilter_from_regex("abc[p-q]");
    match pf {
        regex_analyze::Prefilter::None => panic!("expected some prefilter for abc[p-q]"),
        _ => {}
    }
}

#[test]
fn regex_prefilter_strict_golden() {
    // Strict golden assertions for deterministic regex prefilter outputs.
    // These cases are chosen because they contain clear literal runs that
    // deterministically produce trigram Conj results.
    use regex_analyze::Prefilter;

    // simple literal -> expect trigrams for "abcdef" (lowercased)
    let pf = regex_analyze::prefilter_from_regex("abcdef");
    match pf {
        Prefilter::Conj(v) => {
            // expect trigrams: "abc", "bcd", "cde", "def"
            let mut expect: Vec<[u8; 3]> = vec![*b"abc", *b"bcd", *b"cde", *b"def"];
            expect.sort();
            let mut got = v.clone();
            got.sort();
            assert_eq!(got, expect);
        }
        _ => panic!("expected Conj for simple literal"),
    }

    // mixed case with non-word char -> falls back to ascii-literal trigram extraction
    let pf = regex_analyze::prefilter_from_regex("Hello-World");
    match pf {
        Prefilter::Conj(v) => {
            // "Hello-World" has non-word '-' so trigram extraction uses byte windows
            // After lowercasing: "hello-world" -> trigrams: "hel","ell","llo","lo-","o-w","-wo","wor","orl","rld"
            let mut expect = vec![
                *b"hel", *b"ell", *b"llo", *b"lo-", *b"o-w", *b"-wo", *b"wor", *b"orl", *b"rld",
            ];
            // normalize bytes to lowercase (already lower in literals above)
            expect.sort();
            let mut got = v.clone();
            got.sort();
            assert_eq!(got, expect);
        }
        _ => panic!("expected Conj for Hello-World"),
    }
    // recommended next strict cases:
    // 1) word-boundary and whitespace: "\bint\s+main\b" -> trigrams for "int" and "main"
    let pf = regex_analyze::prefilter_from_regex("\\bint\\s+main\\b");
    match pf {
        Prefilter::Conj(v) => {
            let mut expect = vec![*b"int", *b"mai", *b"ain"];
            expect.sort();
            let mut got = v.clone();
            got.sort();
            assert_eq!(got, expect);
        }
        _ => panic!("expected Conj for \"\\bint\\s+main\\b\""),
    }

    // 2) mixed-case word literal: "fooBar" -> lowercased trigrams for "foobar"
    let pf = regex_analyze::prefilter_from_regex("fooBar");
    match pf {
        Prefilter::Conj(v) => {
            let mut expect = vec![*b"foo", *b"oob", *b"oba", *b"bar"];
            expect.sort();
            let mut got = v.clone();
            got.sort();
            assert_eq!(got, expect);
        }
        _ => panic!("expected Conj for fooBar"),
    }

    // 3) quoted-literal: "\\Q+|*?\\E" -> expanded to "\\+\\|\\*\\?" and trigrams over bytes
    let pf = regex_analyze::prefilter_from_regex("\\Q+|*?\\E");
    match pf {
        Prefilter::Conj(v) => {
            // The processed quoted literal expands to the literal characters
            // "+|*?" which yields trigrams "+|*" and "|*?".
            let mut expect = vec![*b"+|*", *b"|*?"];
            expect.sort();
            let mut got = v.clone();
            got.sort();
            assert_eq!(got, expect);
        }
        _ => panic!("expected Conj for quoted-literal case"),
    }

    // 4) static inline -> expect trigrams from "static" and "inline"
    let pf = regex_analyze::prefilter_from_regex("\\bstatic\\s+inline\\b");
    match pf {
        Prefilter::Conj(v) => {
            let mut expect = vec![
                *b"sta", *b"tat", *b"ati", *b"tic", *b"inl", *b"nli", *b"lin", *b"ine",
            ];
            expect.sort();
            let mut got = v.clone();
            got.sort();
            assert_eq!(got, expect);
        }
        _ => panic!("expected Conj for static inline"),
    }

    // 5) fooBar variants: ensure lowercasing and trigram extraction are stable
    for case in ["FooBAR", "FOObar", "fooBar"] {
        let pf = regex_analyze::prefilter_from_regex(case);
        match pf {
            Prefilter::Conj(v) => {
                let mut expect = vec![*b"foo", *b"oob", *b"oba", *b"bar"];
                expect.sort();
                let mut got = v.clone();
                got.sort();
                assert_eq!(got, expect, "case {} produced unexpected trigrams", case);
            }
            _ => panic!("expected Conj for fooBar variants"),
        }
    }
}
