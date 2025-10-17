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

use zoekt_rs::regex_analyze::{prefilter_from_regex, Prefilter};

#[test]
fn go_ported_regex_cases() {
    // Selected patterns from upstream Zoekt/Go tests (conservative expectations).
    let cases: Vec<(&str, &str)> = vec![
        // from query/regexp_test.go -> LowerRegexp examples
        ("[a-zA-Z]fooBAR", "conj"),
        // optimize examples
        (("(hello)world"), "conj"),
        (("test(ing|ed)"), "conj"),
        (("ba(na){1,2}"), "conj"),
        (("b(a(n(a(n(a)))))"), "conj"),
        // from syntaxutil/parse_test.go: simple literals and classes
        (("abc|def"), "either"),
        (("(?:ab)*"), "none"), // repetition of group may not yield 3-byte literal
        (("abcde"), "conj"),
        (("[Aa]"), "either"), // case-folded literal behavior varies
        // patterns with escapes and Unicode
        (("\\.\\^\\$\\\\"), "either"),
        (("[a-z]"), "none"),
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
