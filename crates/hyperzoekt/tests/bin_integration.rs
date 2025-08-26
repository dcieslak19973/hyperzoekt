use assert_cmd::Command;
// parse JSONL directly and assert richer properties
use serde_json::Value;
use std::fs;
use std::path::PathBuf;

#[test]
fn index_fixture_repo_writes_jsonl() {
    let mut cmd = Command::cargo_bin("hyperzoekt").unwrap();
    // use the canonical fixture that contains multiple language examples
    let fixture = PathBuf::from("tests/fixtures/example-treesitter-repo");
    let out_dir = PathBuf::from(".data");
    let _ = std::fs::create_dir_all(&out_dir);
    let preferred = out_dir.join("bin_integration_out.jsonl");
    if preferred.exists() {
        let _ = fs::remove_file(&preferred);
    }
    // request the preferred output path; the indexer may write another file name
    // but we will fall back to any .jsonl in .data/ when reading the results.
    cmd.env("RUST_LOG", "info")
        .arg("--incremental")
        .arg("--root")
        .arg(&fixture)
        .arg("--out")
        .arg(&preferred);
    cmd.assert().success();

    // Read the single canonical output file written by the indexer
    let chosen = preferred.clone();
    assert!(
        chosen.exists(),
        "expected canonical jsonl output at {:?}",
        chosen
    );
    let mut objs: Vec<Value> = Vec::new();
    let content = fs::read_to_string(&chosen).expect("read chosen jsonl");
    for line in content.lines() {
        if line.trim().is_empty() {
            continue;
        }
        objs.push(serde_json::from_str(line).expect("parse jsonl"));
    }

    // Load data-driven expectations
    let expect_str = fs::read_to_string("tests/expected_values.json").expect("read expectations");
    let expects: Value = serde_json::from_str(&expect_str).expect("parse expectations json");

    // helper to find an entity by (name, language)
    let _find = |name: &str, lang: &str| -> Option<&Value> {
        objs.iter().find(|v| {
            v.get("name").and_then(|n| n.as_str()) == Some(name)
                && v.get("language").and_then(|l| l.as_str()) == Some(lang)
        })
    };

    // If the expectations document requests verilog checks, validate them.
    if expects
        .get("languages")
        .and_then(|l| l.get("verilog"))
        .is_some()
    {
        // Target the canonical example file (`example.v`) for strict checks so
        // generated/verilog files do not change which `foo` module is asserted.
        let example_path = "tests/fixtures/example-treesitter-repo/src/verilog/example.v";
        if let Some(foo) = objs.iter().find(|v| {
            v.get("name").and_then(|n| n.as_str()) == Some("foo")
                && v.get("language").and_then(|l| l.as_str()) == Some("verilog")
                && v.get("file").and_then(|f| f.as_str()) == Some(example_path)
        }) {
            assert_eq!(foo.get("start_line").and_then(|v| v.as_i64()), Some(1));
            assert_eq!(foo.get("end_line").and_then(|v| v.as_i64()), Some(6));

            if let Some(add) = objs.iter().find(|v| {
                v.get("name").and_then(|n| n.as_str()) == Some("add")
                    && v.get("language").and_then(|l| l.as_str()) == Some("verilog")
                    && v.get("file").and_then(|f| f.as_str()) == Some(example_path)
            }) {
                assert_eq!(add.get("start_line").and_then(|v| v.as_i64()), Some(3));
                assert_eq!(add.get("end_line").and_then(|v| v.as_i64()), Some(5));
                assert_eq!(add.get("parent").and_then(|p| p.as_str()), Some("foo"));
            }
        }
    }

    // Basic sanity: output file should contain at least one entity
    assert!(!objs.is_empty(), "expected at least one indexed entity");

    // Data-driven sanity: total entities >= configured minimum
    if let Some(min_total) = expects.get("total_entities_min").and_then(|v| v.as_i64()) {
        assert!(
            objs.len() as i64 >= min_total,
            "total entities < expected min"
        );
    }

    // --- Robust, per-language assertions ---
    // For each language we assert: an expected top-level type exists, exported
    // start/end lines are 1-based and end >= start, top-level parent is null,
    // and the `calls` field (if present) is an array of strings.

    // Data-driven per-language checks: for each language listed in
    // `tests/expected_values.json` ensure at least the configured minimum
    // number of entities were exported for that language. This makes the
    // test robust when fixture files (e.g. files named "test.*") are removed.
    if let Some(langs) = expects.get("languages").and_then(|v| v.as_object()) {
        for (lang, cfg) in langs {
            let min = cfg
                .get("min_entities")
                .and_then(|v| v.as_i64())
                .unwrap_or(1);
            let count = objs
                .iter()
                .filter(|v| v.get("language").and_then(|l| l.as_str()) == Some(lang.as_str()))
                .count() as i64;
            assert!(
                count >= min,
                "language {} exported {} entities (expected >= {})",
                lang,
                count,
                min
            );
        }
    }

    // Additional sanity: for each language at least one entity (any) should
    // have a well-formed `calls` array (possibly empty). This checks shape
    // consistency across the export.
    for lang in [
        "cpp",
        "c_sharp",
        "go",
        "java",
        "javascript",
        "python",
        "rust",
        "swift",
        "typescript",
        "verilog",
    ] {
        let any = objs
            .iter()
            .find(|v| v.get("language").and_then(|l| l.as_str()) == Some(lang));
        assert!(
            any.is_some(),
            "expected at least one entity for language {}",
            lang
        );

        // If we find an entity for this language, check one of them has `calls` as an array if present
        if let Some(v) = any {
            if let Some(calls) = v.get("calls") {
                assert!(
                    calls.is_array(),
                    "calls must be an array for language {}",
                    lang
                );
            }
        }

        // Validate that at least one exported entity for this language has
        // well-formed start/end line numbers (1-based, end >= start).
        let ent_with_lines = objs.iter().find(|v| {
            v.get("language").and_then(|l| l.as_str()) == Some(lang)
                && v.get("start_line").is_some()
                && v.get("end_line").is_some()
        });
        assert!(
            ent_with_lines.is_some(),
            "no entity with start_line/end_line exported for language {}",
            lang
        );
        if let Some(e) = ent_with_lines {
            let start = e
                .get("start_line")
                .and_then(|v| v.as_i64())
                .expect("start_line should be integer");
            let end = e
                .get("end_line")
                .and_then(|v| v.as_i64())
                .expect("end_line should be integer");
            assert!(
                start >= 1,
                "start_line should be 1-based for language {}",
                lang
            );
            assert!(
                end >= start,
                "end_line should be >= start_line for language {}",
                lang
            );
        }

        // Ensure there is at least one top-level entity for this language whose
        // `parent` is null/missing (i.e. a top-level symbol). If present, also
        // validate its line numbers.
        let top_level = objs.iter().find(|v| {
            v.get("language").and_then(|l| l.as_str()) == Some(lang)
                && v.get("parent").is_none_or(|p| p.is_null())
        });
        assert!(
            top_level.is_some(),
            "expected a top-level (parent=null) entity for language {}",
            lang
        );
        if let Some(t) = top_level {
            if let (Some(sv), Some(ev)) = (t.get("start_line"), t.get("end_line")) {
                let s = sv.as_i64().expect("start_line should be integer");
                let e = ev.as_i64().expect("end_line should be integer");
                assert!(
                    s >= 1,
                    "top-level start_line should be 1-based for {}",
                    lang
                );
                assert!(
                    e >= s,
                    "top-level end_line should be >= start_line for {}",
                    lang
                );
            }
        }
    }

    // clean up chosen output
    // --- Import assertions ---
    // helper to find a file-kind entity by filename and language
    let find_file = |name: &str, lang: &str, path_suffix: Option<&str>| -> Option<&Value> {
        objs.iter().find(|v| {
            v.get("kind").and_then(|k| k.as_str()) == Some("file")
                && v.get("name").and_then(|n| n.as_str()) == Some(name)
                && v.get("language").and_then(|l| l.as_str()) == Some(lang)
                && match path_suffix {
                    Some(suff) => v
                        .get("file")
                        .and_then(|f| f.as_str())
                        .is_some_and(|fp| fp.ends_with(suff)),
                    None => true,
                }
        })
    };

    // C++: example.cpp should import point.hpp (line 1) and pose.hpp (line 2)
    let cpp_example = find_file(
        "example.cpp",
        "cpp",
        Some("tests/fixtures/example-treesitter-repo/src/cpp/example.cpp"),
    )
    .expect("expected cpp file entity example.cpp");
    let cpp_imports = cpp_example
        .get("imports")
        .expect("imports present on cpp file")
        .as_array()
        .expect("imports is array");
    assert!(
        cpp_imports.len() >= 2,
        "expected at least two imports for example.cpp"
    );
    let first = &cpp_imports[0];
    assert_eq!(
        first.get("path").and_then(|p| p.as_str()),
        Some("tests/fixtures/example-treesitter-repo/src/cpp/point.hpp")
    );
    assert_eq!(first.get("line").and_then(|l| l.as_i64()), Some(1));
    let second = &cpp_imports[1];
    assert_eq!(
        second.get("path").and_then(|p| p.as_str()),
        Some("tests/fixtures/example-treesitter-repo/src/cpp/pose.hpp")
    );
    assert_eq!(second.get("line").and_then(|l| l.as_i64()), Some(2));

    // C++: pose.hpp should import point.hpp at line 3
    let pose = find_file(
        "pose.hpp",
        "cpp",
        Some("tests/fixtures/example-treesitter-repo/src/cpp/pose.hpp"),
    )
    .expect("expected cpp file entity pose.hpp");
    let pose_imports = pose
        .get("imports")
        .and_then(|v| v.as_array())
        .expect("pose imports array");
    assert!(!pose_imports.is_empty(), "pose.hpp should have imports");
    let p0 = &pose_imports[0];
    assert_eq!(
        p0.get("path").and_then(|p| p.as_str()),
        Some("tests/fixtures/example-treesitter-repo/src/cpp/point.hpp")
    );
    assert_eq!(p0.get("line").and_then(|l| l.as_i64()), Some(3));

    // Go: geometry.go should have unresolved_imports for `math` at line 5
    let go_file = find_file(
        "geometry.go",
        "go",
        Some("tests/fixtures/example-treesitter-repo/src/go/geometry.go"),
    )
    .expect("expected go file entity geometry.go");
    let go_unresolved = go_file
        .get("unresolved_imports")
        .expect("unresolved_imports present on go file")
        .as_array()
        .expect("unresolved_imports is array");
    assert!(
        !go_unresolved.is_empty(),
        "expected unresolved imports for go file"
    );
    let gu = &go_unresolved[0];
    assert_eq!(gu.get("module").and_then(|m| m.as_str()), Some("math"));
    assert_eq!(gu.get("line").and_then(|l| l.as_i64()), Some(5));

    // Python: example.py should have unresolved_imports for `math` at line 2
    let py_file = find_file(
        "example.py",
        "python",
        Some("tests/fixtures/example-treesitter-repo/src/python/example.py"),
    )
    .expect("expected python file entity example.py");
    let py_unresolved = py_file
        .get("unresolved_imports")
        .expect("unresolved_imports present on python file")
        .as_array()
        .expect("unresolved_imports is array");
    assert!(
        !py_unresolved.is_empty(),
        "expected unresolved imports for python file"
    );
    let pu = &py_unresolved[0];
    assert_eq!(pu.get("module").and_then(|m| m.as_str()), Some("math"));
    assert_eq!(pu.get("line").and_then(|l| l.as_i64()), Some(2));

    // Exception fixture files: ensure our added examples are exported as file entities
    let _ex_py = find_file("exceptions_01.py", "python", None)
        .expect("expected python exceptions_01.py file entity");
    let _ex_swift = find_file("exceptions_01.swift", "swift", None)
        .expect("expected swift exceptions_01.swift file entity");
    let _ex_tsx = find_file("exceptions_01.tsx", "tsx", None)
        .expect("expected tsx exceptions_01.tsx file entity");
    let _ex_verilog = find_file("exceptions_01.v", "verilog", None)
        .expect("expected verilog exceptions_01.v file entity");
    // Rust error examples (may be named errors_01.rs/errors_02.rs)
    let _ = find_file("errors_01.rs", "rust", None);
    let _ = find_file("errors_02.rs", "rust", None);

    let _ = fs::remove_file(&chosen);

    // --- Validate exported start/end lines match actual file lengths for
    // canonical example files (skip generated/ directory files).
    let example_files = vec![
        (
            "cpp",
            "tests/fixtures/example-treesitter-repo/src/cpp/example.cpp",
        ),
        (
            "c_sharp",
            "tests/fixtures/example-treesitter-repo/src/csharp/geometry_examples.cs",
        ),
        (
            "go",
            "tests/fixtures/example-treesitter-repo/src/go/geometry.go",
        ),
        (
            "java",
            "tests/fixtures/example-treesitter-repo/src/java/GeometryExample.java",
        ),
        (
            "javascript",
            "tests/fixtures/example-treesitter-repo/src/js/example.js",
        ),
        (
            "python",
            "tests/fixtures/example-treesitter-repo/src/python/example.py",
        ),
        (
            "rust",
            "tests/fixtures/example-treesitter-repo/src/rust/geometry.rs",
        ),
        (
            "swift",
            "tests/fixtures/example-treesitter-repo/src/swift/example.swift",
        ),
        (
            "typescript",
            "tests/fixtures/example-treesitter-repo/src/typescript/example.ts",
        ),
        (
            "tsx",
            "tests/fixtures/example-treesitter-repo/src/tsx/PointComponent.tsx",
        ),
        (
            "verilog",
            "tests/fixtures/example-treesitter-repo/src/verilog/example.v",
        ),
    ];

    for (lang, path) in example_files {
        // only check files that actually exist in the fixture
        if !std::path::Path::new(path).exists() {
            continue;
        }

        // find the file-kind entity exported for this file and language
        let file_ent = find_file(
            std::path::Path::new(path)
                .file_name()
                .and_then(|s| s.to_str())
                .expect("filename"),
            lang,
            Some(path),
        )
        .unwrap_or_else(|| panic!("expected file entity for {}", path));

        // read file and count lines (use ".lines().count()" which matches exporter semantics)
        let content = fs::read_to_string(path).expect("read fixture file");
        let line_count = content.lines().count() as i64;

        // If the expectations document includes explicit file_line_expectations
        // for this path, use those values; otherwise fall back to asserting
        // the exporter provided the full file line count.
        if let Some(fmap) = expects
            .get("file_line_expectations")
            .and_then(|v| v.as_object())
        {
            if let Some(fe) = fmap.get(path) {
                // start_line expectation (may be null)
                if fe.get("start_line").is_none_or(|v| v.is_null()) {
                    assert!(
                        file_ent.get("start_line").is_none_or(|v| v.is_null()),
                        "expected start_line null for {} ({})",
                        lang,
                        path
                    );
                } else {
                    let es = fe
                        .get("start_line")
                        .and_then(|v| v.as_i64())
                        .expect("expect start int");
                    let fs = file_ent
                        .get("start_line")
                        .and_then(|v| v.as_i64())
                        .expect("found start int");
                    assert_eq!(fs, es, "start_line for {} ({})", lang, path);
                }

                if fe.get("end_line").is_none_or(|v| v.is_null()) {
                    assert!(
                        file_ent.get("end_line").is_none_or(|v| v.is_null()),
                        "expected end_line null for {} ({})",
                        lang,
                        path
                    );
                } else {
                    let ee = fe
                        .get("end_line")
                        .and_then(|v| v.as_i64())
                        .expect("expect end int");
                    let fev = file_ent
                        .get("end_line")
                        .and_then(|v| v.as_i64())
                        .expect("found end int");
                    assert_eq!(fev, ee, "end_line for {} ({})", lang, path);
                }
                continue;
            }
        }

        // exported start_line should be 1 and end_line should equal the file's line count
        assert_eq!(
            file_ent.get("start_line").and_then(|v| v.as_i64()),
            Some(1),
            "start_line for {} ({})",
            lang,
            path
        );
        assert_eq!(
            file_ent.get("end_line").and_then(|v| v.as_i64()),
            Some(line_count),
            "end_line for {} ({})",
            lang,
            path
        );
    }

    // --- Entity-level expectations (data-driven) ---
    if let Some(ent_map) = expects
        .get("entity_expectations")
        .and_then(|v| v.as_object())
    {
        for (file, ents) in ent_map {
            // ents is an array of expectation objects
            if let Some(arr) = ents.as_array() {
                for ent in arr {
                    let kind = ent
                        .get("kind")
                        .and_then(|v| v.as_str())
                        .expect("expectation kind");
                    let name = ent
                        .get("name")
                        .and_then(|v| v.as_str())
                        .expect("expectation name");
                    let parent_val = ent.get("parent");

                    let expected_start = ent.get("start_line");
                    let expected_end = ent.get("end_line");

                    // helper to check parent equality (handles null)
                    let parent_matches = |o: &Value| -> bool {
                        match parent_val {
                            Some(p) if p.is_null() => o.get("parent").is_none_or(|pp| pp.is_null()),
                            Some(p) => p.as_str().is_some_and(|ps| {
                                o.get("parent").and_then(|pp| pp.as_str()) == Some(ps)
                            }),
                            None => true,
                        }
                    };

                    // collect candidates matching file/kind/name/parent (there may be duplicates — e.g. ctor/dtor)
                    let candidates: Vec<&Value> = objs
                        .iter()
                        .filter(|o| {
                            o.get("file").and_then(|f| f.as_str()) == Some(file.as_str())
                                && o.get("kind").and_then(|k| k.as_str()) == Some(kind)
                                && o.get("name").and_then(|n| n.as_str()) == Some(name)
                                && parent_matches(o)
                        })
                        .collect();

                    let found = if candidates.is_empty() {
                        None
                    } else {
                        // If expectation includes a start_line or end_line prefer a candidate that matches those
                        let mut picked: Option<&Value> = None;
                        if let Some(esv) = expected_start.and_then(|v| v.as_i64()) {
                            picked = candidates
                                .iter()
                                .find(|o| o.get("start_line").and_then(|v| v.as_i64()) == Some(esv))
                                .copied();
                        }
                        if picked.is_none() {
                            if let Some(eev) = expected_end.and_then(|v| v.as_i64()) {
                                picked = candidates
                                    .iter()
                                    .find(|o| {
                                        o.get("end_line").and_then(|v| v.as_i64()) == Some(eev)
                                    })
                                    .copied();
                            }
                        }
                        if picked.is_none() {
                            // fallback to first candidate
                            picked = candidates.into_iter().next();
                        }
                        picked
                    };

                    assert!(
                        found.is_some(),
                        "expected entity {} {} in file {}",
                        kind,
                        name,
                        file
                    );
                    let fobj = found.unwrap();

                    // compare start_line (handle null expectation)
                    if expected_start.is_none_or(|v| v.is_null()) {
                        assert!(
                            fobj.get("start_line").is_none_or(|v| v.is_null()),
                            "expected start_line null for {} in {}",
                            name,
                            file
                        );
                    } else {
                        let es = expected_start
                            .and_then(|v| v.as_i64())
                            .expect("expected start as int");
                        let fs = fobj
                            .get("start_line")
                            .and_then(|v| v.as_i64())
                            .expect("found start int");
                        assert_eq!(fs, es, "start_line mismatch for {} in {}", name, file);
                    }

                    // compare end_line (handle null expectation)
                    if expected_end.is_none_or(|v| v.is_null()) {
                        assert!(
                            fobj.get("end_line").is_none_or(|v| v.is_null()),
                            "expected end_line null for {} in {}",
                            name,
                            file
                        );
                    } else {
                        let ee = expected_end
                            .and_then(|v| v.as_i64())
                            .expect("expected end as int");
                        let fe = fobj
                            .get("end_line")
                            .and_then(|v| v.as_i64())
                            .expect("found end int");
                        assert_eq!(fe, ee, "end_line mismatch for {} in {}", name, file);
                    }
                }
            }
        }
    }
}
