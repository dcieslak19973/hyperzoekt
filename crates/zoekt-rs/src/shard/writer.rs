use anyhow::{bail, Context, Result};
use memmap2::Mmap;
use rayon::prelude::*;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

use crate::index::InMemoryIndex;
use crate::trigram::emit_trigrams_with_pos;

use super::writer_utils::{radix_sort_u128, write_var_u32};
use super::{PostingsMap, MAGIC, VERSION};

pub struct ShardWriter {
    path: PathBuf,
}

impl ShardWriter {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
        }
    }

    pub fn write_from_index(&self, idx: &InMemoryIndex) -> Result<()> {
        self.write_from_index_with_options(idx, true)
    }

    /// Write shard with option to include symbol trigram postings.
    pub fn write_from_index_with_options(
        &self,
        idx: &InMemoryIndex,
        include_symbol_postings: bool,
    ) -> Result<()> {
        let global_start = Instant::now();

        let mut f = File::create(&self.path).context("create shard file")?;
        let inner = idx.read_inner();

        // Header placeholders; we'll patch offsets later.
        let mut header = Vec::new();
        header.extend(&MAGIC.to_le_bytes());
        header.extend(&VERSION.to_le_bytes());
        header.extend(&(inner.docs.len() as u32).to_le_bytes());
        header.extend(&0u64.to_le_bytes()); // docs_off
        header.extend(&0u64.to_le_bytes()); // postings_off
        header.extend(&0u64.to_le_bytes()); // meta_off
        header.extend(&0u64.to_le_bytes()); // line_index_off
        header.extend(&0u64.to_le_bytes()); // line_data_off
        f.write_all(&header)?;

        // Write docs table
        let docs_off = f.stream_position()?;
        for d in &inner.docs {
            let p = d.path.display().to_string();
            let b = p.as_bytes();
            if b.len() > u16::MAX as usize {
                bail!("path too long")
            }
            f.write_all(&(b.len() as u16).to_le_bytes())?;
            f.write_all(b)?;
        }

        // First pass: collect per-doc contents and line starts (sequential).
        let mut lines_per_doc: Vec<Vec<u32>> = Vec::with_capacity(inner.docs.len());
        let mut contents: Vec<String> = Vec::with_capacity(inner.docs.len());
        for (doc_idx, meta) in inner.docs.iter().enumerate() {
            // load content (prefer in-memory doc_contents when present)
            let content = if let Some(opt) = inner.doc_contents.get(doc_idx) {
                if let Some(s) = opt.as_ref() {
                    s.clone()
                } else {
                    let p = inner.repo.root.join(&meta.path);
                    match File::open(&p).and_then(|file| unsafe { Mmap::map(&file) }) {
                        Ok(mmap) => std::str::from_utf8(&mmap[..])
                            .map(|s| s.to_string())
                            .unwrap_or_default(),
                        Err(_) => std::fs::read_to_string(p).unwrap_or_default(),
                    }
                }
            } else {
                let p = inner.repo.root.join(&meta.path);
                match File::open(&p).and_then(|file| unsafe { Mmap::map(&file) }) {
                    Ok(mmap) => std::str::from_utf8(&mmap[..])
                        .map(|s| s.to_string())
                        .unwrap_or_default(),
                    Err(_) => std::fs::read_to_string(p).unwrap_or_default(),
                }
            };
            // collect line starts
            let bytes = content.as_bytes();
            let mut starts: Vec<u32> = vec![0u32];
            for (i, &b) in bytes.iter().enumerate() {
                if b == b'\n' {
                    let next = i as u32 + 1;
                    if (next as usize) < bytes.len() {
                        starts.push(next);
                    }
                }
            }
            lines_per_doc.push(starts);
            contents.push(content);
        }

        let num_threads = rayon::current_num_threads();
        let shard_count = std::cmp::max(4, num_threads * 4);

        // Radix sort for u128 optimized for our 88-bit encoded keys (tri24|doc32|pos32).
        // use radix_sort_u128 from writer_utils

        // Collect per-thread shard buffers
        let shards_vec: Vec<Vec<u128>> = contents
            .par_iter()
            .enumerate()
            .fold(
                || {
                    let mut v = Vec::with_capacity(shard_count);
                    for _ in 0..shard_count {
                        v.push(Vec::new());
                    }
                    v
                },
                |mut acc, (doc_idx, content)| {
                    let mut buf: Vec<([u8; 3], u32)> = Vec::with_capacity(128);
                    emit_trigrams_with_pos(content, &mut buf);
                    for (tri, pos) in buf {
                        let h = ((tri[0] as usize) << 16)
                            ^ ((tri[1] as usize) << 8)
                            ^ (tri[2] as usize);
                        let shard = h % shard_count;
                        let tri24 =
                            ((tri[0] as u128) << 16) | ((tri[1] as u128) << 8) | (tri[2] as u128);
                        let key: u128 = (tri24 << 64) | ((doc_idx as u128) << 32) | (pos as u128);
                        acc[shard].push(key);
                    }
                    acc
                },
            )
            .reduce(
                || {
                    let mut v = Vec::with_capacity(shard_count);
                    for _ in 0..shard_count {
                        v.push(Vec::new());
                    }
                    v
                },
                |mut a, b| {
                    for (i, mut sub) in b.into_iter().enumerate() {
                        a[i].append(&mut sub);
                    }
                    a
                },
            );

        // Sort each shard in parallel, build per-shard postings maps, then merge.
        let shard_postings: Vec<PostingsMap> = shards_vec
            .into_par_iter()
            .map(|mut keys| {
                if keys.len() > 1 {
                    radix_sort_u128(&mut keys);
                }
                let mut map: PostingsMap = HashMap::new();
                let mut i = 0usize;
                while i < keys.len() {
                    let k = keys[i];
                    let tri24 = ((k >> 64) & 0xFFFFFF) as u32;
                    let b0 = ((tri24 >> 16) & 0xFF) as u8;
                    let b1 = ((tri24 >> 8) & 0xFF) as u8;
                    let b2 = (tri24 & 0xFF) as u8;
                    let tri = [b0, b1, b2];
                    let mut btree: BTreeMap<u32, Vec<u32>> = BTreeMap::new();
                    while i < keys.len() {
                        let k2 = keys[i];
                        let tri2 = ((k2 >> 64) & 0xFFFFFF) as u32;
                        if tri2 != tri24 {
                            break;
                        }
                        let doc = ((k2 >> 32) & 0xFFFF_FFFF) as u32;
                        let pos = (k2 & 0xFFFF_FFFF) as u32;
                        btree.entry(doc).or_default().push(pos);
                        i += 1;
                    }
                    map.insert(tri, btree);
                }
                map
            })
            .collect();

        // Merge per-shard postings
        let mut term_map: PostingsMap = HashMap::new();
        for shard_map in shard_postings.into_iter() {
            for (tri, btree) in shard_map.into_iter() {
                let entry = term_map.entry(tri).or_default();
                for (doc, mut positions) in btree.into_iter() {
                    entry.entry(doc).or_default().append(&mut positions);
                }
            }
        }

        // Serialize postings (content trigram map) using delta+varint encoding into a buffer
        let postings_off = f.stream_position()?;
        let mut content_buf: Vec<u8> = Vec::new();
        content_buf.extend(&(term_map.len() as u32).to_le_bytes());
        for (tri, posting_tree) in term_map.iter() {
            content_buf.extend(&tri[..]);
            content_buf.extend(&(posting_tree.len() as u32).to_le_bytes());
            let mut prev_doc: u32 = 0;
            for (doc, pos_list) in posting_tree.iter() {
                let mut positions = pos_list.clone();
                positions.sort_unstable();
                let doc_delta = doc.wrapping_sub(prev_doc);
                write_var_u32(&mut content_buf, doc_delta)?;
                write_var_u32(&mut content_buf, positions.len() as u32)?;
                let mut prev_pos: u32 = 0;
                for p in positions.iter() {
                    let pos_delta = p.wrapping_sub(prev_pos);
                    write_var_u32(&mut content_buf, pos_delta)?;
                    prev_pos = *p;
                }
                prev_doc = *doc;
            }
        }
        f.write_all(&content_buf)?;

        // Symbol postings (second map)
        if include_symbol_postings {
            let sym_map = &inner.symbol_trigrams;
            let mut sym_buf: Vec<u8> = Vec::new();
            sym_buf.extend(&(sym_map.len() as u32).to_le_bytes());
            for (tri, docs) in sym_map.iter() {
                sym_buf.extend(&tri[..]);
                sym_buf.extend(&(docs.len() as u32).to_le_bytes());
                let mut prev_doc: u32 = 0;
                for d in docs.iter() {
                    let doc_delta = d.wrapping_sub(prev_doc);
                    write_var_u32(&mut sym_buf, doc_delta)?;
                    // npos == 0 for symbol postings
                    write_var_u32(&mut sym_buf, 0)?;
                    prev_doc = *d;
                }
            }
            f.write_all(&sym_buf)?;
        } else {
            // write zero symbol map
            f.write_all(&0u32.to_le_bytes())?;
        }

        // Write metadata section (repo name/root/hash/branches)
        let meta_off = f.stream_position()?;
        let mut hasher = Sha256::new();
        for d in &inner.docs {
            let p = inner.repo.root.join(&d.path);
            if let Ok(bytes) = std::fs::read(&p) {
                hasher.update(&bytes);
            }
        }
        let hash = hasher.finalize();
        let repo_name = inner.repo.name.as_bytes();
        let repo_root = inner.repo.root.display().to_string();
        let repo_root_b = repo_root.as_bytes();
        f.write_all(&(repo_name.len() as u16).to_le_bytes())?;
        f.write_all(repo_name)?;
        f.write_all(&(repo_root_b.len() as u16).to_le_bytes())?;
        f.write_all(repo_root_b)?;
        f.write_all(&hash[..])?; // 32 bytes
        let branches = &inner.repo.branches;
        f.write_all(&(branches.len() as u16).to_le_bytes())?;
        for b in branches {
            let bb = b.as_bytes();
            f.write_all(&(bb.len() as u16).to_le_bytes())?;
            f.write_all(bb)?;
        }

        // Write per-doc symbols
        for d in &inner.docs {
            let syms = &d.symbols;
            f.write_all(&(syms.len() as u16).to_le_bytes())?;
            for s in syms {
                let nb = s.name.as_bytes();
                if nb.len() > u16::MAX as usize {
                    bail!("symbol name too long");
                }
                f.write_all(&(nb.len() as u16).to_le_bytes())?;
                f.write_all(nb)?;
                let start = s.start.unwrap_or(u32::MAX);
                let line = s.line.unwrap_or(u32::MAX);
                f.write_all(&start.to_le_bytes())?;
                f.write_all(&line.to_le_bytes())?;
            }
        }

        // Write line index section without per-doc backpatching
        let line_index_off = f.stream_position()?;
        let mut line_data: Vec<u8> = Vec::new();
        let mut entries: Vec<(u64, u32)> = Vec::with_capacity(lines_per_doc.len());
        for starts in &lines_per_doc {
            let entry_off = line_data.len() as u64;
            line_data.extend(&(starts.len() as u32).to_le_bytes());
            for s in starts {
                line_data.extend(&s.to_le_bytes());
            }
            entries.push((entry_off, starts.len() as u32));
        }
        let line_index_table_size = (entries.len() * 12) as u64;
        let line_data_off = line_index_off + line_index_table_size;
        for (rel_off, cnt) in entries.iter() {
            let abs_off = line_data_off + *rel_off;
            f.write_all(&abs_off.to_le_bytes())?;
            f.write_all(&cnt.to_le_bytes())?;
        }
        f.write_all(&line_data)?;

        // Patch header with offsets
        f.flush()?;
        f.seek(SeekFrom::Start(0))?;
        let mut header2 = Vec::new();
        header2.extend(&MAGIC.to_le_bytes());
        header2.extend(&VERSION.to_le_bytes());
        header2.extend(&(inner.docs.len() as u32).to_le_bytes());
        header2.extend(&docs_off.to_le_bytes());
        header2.extend(&postings_off.to_le_bytes());
        header2.extend(&meta_off.to_le_bytes());
        header2.extend(&line_index_off.to_le_bytes());
        header2.extend(&line_data_off.to_le_bytes());
        f.write_all(&header2)?;

        let total_ms = (Instant::now() - global_start).as_millis();
        eprintln!("INDEXING_TIMINGS total_ms={}ms", total_ms);
        Ok(())
    }
}
