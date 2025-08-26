use hyperzoekt::repo_index::RepoIndexService;

#[test]
fn exported_lines_are_one_based() {
    let repo = std::path::Path::new("tests/fixtures/example-treesitter-repo");
    let (svc, _stats) = RepoIndexService::build(repo).unwrap();
    // use service export helper to produce JSONL in-memory
    let mut buf: Vec<u8> = Vec::new();
    svc.export_jsonl(&mut buf).unwrap();
    let s = String::from_utf8(buf).unwrap();
    // find the file pseudo-entity for pose.hpp
    for line in s.lines() {
        if line.contains("pose.hpp") && line.contains("\"kind\":\"file\"") {
            let v: serde_json::Value = serde_json::from_str(line).unwrap();
            let start_line = v.get("start_line").and_then(|x| x.as_i64()).unwrap();
            assert!(start_line >= 1, "start_line should be 1-based");
            let imports = v.get("imports").and_then(|x| x.as_array()).unwrap();
            // if imports present, their 'line' field should be 1-based
            for imp in imports {
                let il = imp.get("line").and_then(|x| x.as_i64()).unwrap();
                assert!(il >= 1, "import line should be 1-based");
            }
            return;
        }
    }
    panic!("pose.hpp file entity not found in export_jsonl output");
}
