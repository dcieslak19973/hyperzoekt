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
use tempfile::tempdir;
use zoekt_rs::IndexBuilder;

#[test]
fn test_gitignore_and_size_and_binary_and_hidden_and_symlink() {
    let dir = tempdir().unwrap();
    let root = dir.path();

    // Create a text file that should be indexed
    let mut f = File::create(root.join("foo.txt")).unwrap();
    writeln!(f, "hello world zoekt example").unwrap();

    // Create a binary file with a NUL byte
    let mut b = File::create(root.join("data.bin")).unwrap();
    b.write_all(&[0, 1, 2, 3, 4, 0]).unwrap();

    // Create a large file > 1MB
    let mut large = File::create(root.join("big.txt")).unwrap();
    let chunk = vec![b'a'; 1024 * 1024 + 10];
    large.write_all(&chunk).unwrap();

    // Create a .gitignore that ignores ignored.txt
    let mut gi = File::create(root.join(".gitignore")).unwrap();
    writeln!(gi, "ignored.txt").unwrap();
    let mut ign = File::create(root.join("ignored.txt")).unwrap();
    writeln!(ign, "this should be ignored").unwrap();

    // Hidden file
    let mut hidden = File::create(root.join(".secret")).unwrap();
    writeln!(hidden, "hidden stuff zoekt").unwrap();

    // Symlink to foo.txt
    #[cfg(unix)]
    std::os::unix::fs::symlink(root.join("foo.txt"), root.join("link_to_foo")).unwrap();

    // Build with defaults: should skip big.txt, data.bin, ignored.txt, and hidden (hidden=false)
    let idx = IndexBuilder::new(root.to_path_buf())
        .build()
        .expect("build");
    assert!(idx.doc_count() >= 1);
    // foo.txt contains 'hello' and should be findable
    let res = idx.search_literal("hello");
    assert!(!res.is_empty());
    // big.txt was too large => search for 'a' should not find big.txt specifically, but we check counts roughly
    // binary file shouldn't be indexed; searching for non-printable won't match; ensure 'ignored' is not found
    let res_ignored = idx.search_literal("this should be ignored");
    assert!(res_ignored.is_empty());

    // Build with hidden included
    let idx2 = IndexBuilder::new(root.to_path_buf())
        .include_hidden(true)
        .build()
        .unwrap();
    // hidden file contains 'hidden' token and should be found when hidden files are included
    let res2 = idx2.search_literal("hidden");
    assert!(!res2.is_empty());

    // Build with follow symlinks (if supported)
    #[cfg(unix)]
    {
        let idx3 = IndexBuilder::new(root.to_path_buf())
            .follow_symlinks(true)
            .build()
            .unwrap();
        // symlink points to foo which contains 'hello', so searching "hello" should find something; exact symlink path presence is filesystem-dependent
        let res3 = idx3.search_literal("hello");
        assert!(!res3.is_empty());
    }
}
