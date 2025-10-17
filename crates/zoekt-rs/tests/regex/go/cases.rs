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
        (("(?i)\\w"), "none"),
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
