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

use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use tempfile::tempdir;
use zoekt_rs::test_helpers::process_pending_from_paths;

#[test]
fn process_pending_text_and_binary() {
    let td = tempdir().unwrap();
    let base = td.path().to_path_buf();

    // text file
    let txt = PathBuf::from("foo.txt");
    let mut f = File::create(td.path().join(&txt)).unwrap();
    f.write_all(b"hello world\nfn main() {}").unwrap();

    // binary file
    let bin = PathBuf::from("bin.dat");
    let mut fb = File::create(td.path().join(&bin)).unwrap();
    fb.write_all(&[0u8, 1, 2, 0, 3, 4]).unwrap();

    let processed =
        process_pending_from_paths(base.clone(), vec![txt.clone(), bin.clone()], false, Some(1));
    assert_eq!(processed.len(), 2);
    let ptxt = processed.iter().find(|(path, _)| *path == txt).unwrap();
    assert!(ptxt.1, "text file should have content");
    let pbin = processed.iter().find(|(path, _)| *path == bin).unwrap();
    assert!(!pbin.1, "binary file should not have content");
}
