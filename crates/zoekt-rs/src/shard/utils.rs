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

use anyhow::{bail, Result};
use memmap2::Mmap;

/// Read a u32 varint encoded in LEB128 style from an mmap buffer.
pub(crate) fn read_var_u32_from_mmap(mmap: &Mmap, off: &mut usize) -> Result<u32> {
    let mut shift = 0u32;
    let mut out: u32 = 0;
    loop {
        if *off >= mmap.len() {
            bail!("unexpected EOF while reading varint");
        }
        let b = mmap[*off];
        *off += 1;
        out |= ((b & 0x7F) as u32) << shift;
        if (b & 0x80) == 0 {
            return Ok(out);
        }
        shift += 7;
        if shift >= 35 {
            bail!("varint too long");
        }
    }
}

pub(crate) fn intersect_sorted(a: &[u32], b: &[u32]) -> Vec<u32> {
    let mut i = 0;
    let mut j = 0;
    let mut out = Vec::new();
    while i < a.len() && j < b.len() {
        if a[i] == b[j] {
            out.push(a[i]);
            i += 1;
            j += 1;
        } else if a[i] < b[j] {
            i += 1;
        } else {
            j += 1;
        }
    }
    out
}

pub(crate) fn line_for_offset(starts: &[u32], pos: u32) -> (usize, u32) {
    let mut lo = 0usize;
    let mut hi = starts.len();
    while lo < hi {
        let mid = (lo + hi) / 2;
        if starts[mid] <= pos {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    let idx = lo.saturating_sub(1);
    (idx, starts[idx])
}

pub(crate) fn line_bounds(starts: &[u32], line_idx: usize, file_len: usize) -> (usize, usize) {
    if starts.is_empty() {
        return (0, 0);
    }
    let beg = starts[line_idx] as usize;
    let end = if line_idx + 1 < starts.len() {
        starts[line_idx + 1] as usize
    } else {
        file_len
    };
    (beg, end)
}

pub(crate) fn trim_last_n_chars(s: &str, max: usize, add_ellipsis: bool) -> String {
    if max == 0 {
        return String::new();
    }
    let mut count = 0usize;
    let mut split_idx = s.len();
    for (idx, _ch) in s.char_indices().rev() {
        count += 1;
        if count == max {
            split_idx = idx;
            break;
        }
    }
    if s.chars().count() <= max {
        return s.to_string();
    }
    let tail = &s[split_idx..];
    if add_ellipsis {
        format!("…{}", tail)
    } else {
        tail.to_string()
    }
}

pub(crate) fn trim_first_n_chars(s: &str, max: usize, add_ellipsis: bool) -> String {
    if max == 0 {
        return String::new();
    }
    let mut count = 0usize;
    let mut split_idx = s.len();
    for (idx, ch) in s.char_indices() {
        count += 1;
        if count == max {
            split_idx = idx + ch.len_utf8();
            break;
        }
    }
    if s.chars().count() <= max {
        return s.to_string();
    }
    let head = &s[..split_idx];
    if add_ellipsis {
        format!("{}…", head)
    } else {
        head.to_string()
    }
}
