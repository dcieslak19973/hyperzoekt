// Copyright 2025 HyperZoekt Project
use std::fs::write;
use tempfile::TempDir;

#[test]
fn js_default_and_named_aliases_resolve() {
    let tmp = TempDir::new().expect("tmpdir");
    let repo_root = tmp.path().join("repo");
    std::fs::create_dir_all(&repo_root).expect("mkdir repo");
    let src_mod = repo_root.join("mymod.js");
    let src_app = repo_root.join("app.js");
    let mod_src = r#"
export function foo() { return 1; }
export function bar() { return 2; }
"#;
    let app_src = r#"
import defaultExport from './mymod.js';
import { foo as f } from './mymod.js';
import * as all from './mymod.js';

function caller_default() { defaultExport.foo(); }
function caller_named() { f(); }
function caller_star() { all.bar(); }
"#;
    write(&src_mod, mod_src).expect("write mod");
    write(&src_app, app_src).expect("write app");

    let (svc, _stats) = hyperzoekt::repo_index::RepoIndexService::build(repo_root).expect("build");
    let mut foo_id = None;
    let mut bar_id = None;
    let mut caller_default = None;
    let mut caller_named = None;
    let mut caller_star = None;
    for e in &svc.entities {
        if e.name == "foo" {
            foo_id = Some(e.id);
        }
        if e.name == "bar" {
            bar_id = Some(e.id);
        }
        if e.name == "caller_default" {
            caller_default = Some(e.id);
        }
        if e.name == "caller_named" {
            caller_named = Some(e.id);
        }
        if e.name == "caller_star" {
            caller_star = Some(e.id);
        }
    }
    assert!(foo_id.is_some() && bar_id.is_some());
    let cdef = svc.call_edges[caller_default.unwrap() as usize].clone();
    let cnamed = svc.call_edges[caller_named.unwrap() as usize].clone();
    let cstar = svc.call_edges[caller_star.unwrap() as usize].clone();
    assert!(cdef.contains(&foo_id.unwrap()));
    assert!(cnamed.contains(&foo_id.unwrap()));
    assert!(cstar.contains(&bar_id.unwrap()));
}
