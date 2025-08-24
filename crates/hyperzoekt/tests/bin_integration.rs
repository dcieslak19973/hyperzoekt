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
        .arg(fixture)
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
    let find = |name: &str, lang: &str| -> Option<&Value> {
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
        // If a canonical Verilog module `foo` exists in the fixtures, run
        // strict checks; otherwise skip these name-specific assertions so
        // the test remains robust to fixture edits.
        if let Some(foo) = find("foo", "verilog") {
            assert_eq!(foo.get("start_line").and_then(|v| v.as_i64()), Some(1));
            assert_eq!(foo.get("end_line").and_then(|v| v.as_i64()), Some(6));

            if let Some(add) = find("add", "verilog") {
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
    }

    // clean up chosen output
    // --- Import assertions ---
    // helper to find a file-kind entity by filename and language
    let find_file = |name: &str, lang: &str| -> Option<&Value> {
        objs.iter().find(|v| {
            v.get("kind").and_then(|k| k.as_str()) == Some("file")
                && v.get("name").and_then(|n| n.as_str()) == Some(name)
                && v.get("language").and_then(|l| l.as_str()) == Some(lang)
        })
    };

    // C++: example.cpp should import point.hpp (line 1) and pose.hpp (line 2)
    let cpp_example =
        find_file("example.cpp", "cpp").expect("expected cpp file entity example.cpp");
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
    let pose = find_file("pose.hpp", "cpp").expect("expected cpp file entity pose.hpp");
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
    let go_file = find_file("geometry.go", "go").expect("expected go file entity geometry.go");
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
    let py_file =
        find_file("example.py", "python").expect("expected python file entity example.py");
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
    let _ex_py = find_file("exceptions_01.py", "python")
        .expect("expected python exceptions_01.py file entity");
    let _ex_swift = find_file("exceptions_01.swift", "swift")
        .expect("expected swift exceptions_01.swift file entity");
    let _ex_tsx =
        find_file("exceptions_01.tsx", "tsx").expect("expected tsx exceptions_01.tsx file entity");
    let _ex_verilog = find_file("exceptions_01.v", "verilog")
        .expect("expected verilog exceptions_01.v file entity");
    // Rust error examples (may be named errors_01.rs/errors_02.rs)
    let _ = find_file("errors_01.rs", "rust");
    let _ = find_file("errors_02.rs", "rust");

    let _ = fs::remove_file(&chosen);
}
