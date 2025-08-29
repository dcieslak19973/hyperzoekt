//! Persistent shard format (very early draft)
//! Binary LE format:
//! [magic: u32] [version: u32]
//! [doc_count: u32]
//! [docs table offset: u64]
//! [postings offset: u64]
//! Docs table: doc_count entries of [path_len: u16][path_bytes]
//! Postings: simple map dump: [term_count: u32] then for each term:
//!   [tri: [u8;3]] [list_len: u32] [doc_id: u32]*
//!
//! This is intentionally simple to get end-to-end read/write/search working.

use anyhow::{bail, Context, Result};
use memmap2::Mmap;
use std::{
    collections::{BTreeMap, HashMap},
    fs::File,
    io::{Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use crate::{
    index::InMemoryIndex,
    regex_analyze::prefilter_from_regex,
    trigram::{emit_trigrams_with_pos, trigrams},
};
use sha2::{Digest, Sha256};
use std::time::Instant;

// Type aliases to reduce clippy::type_complexity warnings.
type SymbolTuple = (String, Option<u32>, Option<u32>);
type SymbolsTable = Vec<Vec<SymbolTuple>>;
type PostingsMap = HashMap<[u8; 3], BTreeMap<u32, Vec<u32>>>;
type SymbolPostingsMap = HashMap<[u8; 3], Vec<u32>>;

// Varint helpers: simple LEB128-style unsigned varint for u32.
fn write_var_u32<W: Write>(w: &mut W, mut v: u32) -> Result<()> {
    let mut buf = [0u8; 5];
    let mut i = 0;
    loop {
        let byte = (v & 0x7F) as u8;
        v >>= 7;
        if v == 0 {
            buf[i] = byte;
            i += 1;
            break;
        } else {
            buf[i] = byte | 0x80;
            i += 1;
        }
    }
    w.write_all(&buf[..i])?;
    Ok(())
}

fn read_var_u32_from_mmap(mmap: &Mmap, off: &mut usize) -> Result<u32> {
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

const MAGIC: u32 = 0x5a4f_454b; // 'ZOEK'
const VERSION: u32 = 5;

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
        // Simpler, robust sequential writer implementation.
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

        // Per-thread sharded collection + per-shard parallel radix sort and merge.
        use rayon::prelude::*;

        let num_threads = rayon::current_num_threads();
        let shard_count = std::cmp::max(4, num_threads * 4);

        // Radix sort for u128 optimized for our 88-bit encoded keys (tri24|doc32|pos32).
        // Use 11-bit LSD passes (8 passes -> 88 bits) for better cache locality.
        fn radix_sort_u128(buf: &mut [u128]) {
            if buf.len() <= 1 {
                return;
            }
            let mut tmp: Vec<u128> = vec![0u128; buf.len()];
            // We'll perform 8 passes of 11 bits each (total 88 bits)
            const RADIX: usize = 1 << 11; // 2048
            let mut from = buf;
            let mut to = tmp.as_mut_slice();
            for pass in 0..8 {
                let shift = pass * 11;
                let mut counts = vec![0usize; RADIX];
                // count
                for &k in from.iter() {
                    let bucket = ((k >> shift) & 0x7FF) as usize;
                    counts[bucket] += 1;
                }
                // prefix sum
                let mut sum = 0usize;
                for c in counts.iter_mut() {
                    let v = *c;
                    *c = sum;
                    sum += v;
                }
                // scatter
                for &k in from.iter() {
                    let bucket = ((k >> shift) & 0x7FF) as usize;
                    to[counts[bucket]] = k;
                    counts[bucket] += 1;
                }
                std::mem::swap(&mut from, &mut to);
            }
            // After 8 passes the sorted data resides in `buf` (no copy needed)
        }

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
                // ensure positions are sorted and delta-encoded
                let mut positions = pos_list.clone();
                positions.sort_unstable();
                let doc_delta = doc.wrapping_sub(prev_doc);
                write_var_u32(&mut content_buf, doc_delta)?;
                // write npos
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

pub struct ShardReader {
    mmap: Mmap,
    doc_count: u32,
    docs_off: u64,
    postings_off: u64,
    line_index_off: u64,
    // parsed metadata
    _repo_name: String,
    repo_root: String,
    _repo_hash: [u8; 32],
    branches: Vec<String>,
    // parsed per-doc symbols stored as Vec<Vec<(name,start,line)>>
    symbols: SymbolsTable,
}

impl ShardReader {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let file = File::open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        // parse header
        if mmap.len() < 12 + 8 * 5 {
            bail!("file too small")
        }
        let magic = u32::from_le_bytes(mmap[0..4].try_into().with_context(|| {
            format!(
                "shard truncated or malformed while reading magic (len={})",
                mmap.len()
            )
        })?);
        let ver = u32::from_le_bytes(mmap[4..8].try_into().with_context(|| {
            format!(
                "shard truncated or malformed while reading version (len={})",
                mmap.len()
            )
        })?);
        if magic != MAGIC || ver != VERSION {
            bail!("bad header")
        }
        let doc_count = u32::from_le_bytes(
            mmap[8..12]
                .try_into()
                .with_context(|| "shard truncated or malformed while reading doc_count")?,
        );
        let docs_off = u64::from_le_bytes(
            mmap[12..20]
                .try_into()
                .with_context(|| "shard truncated or malformed while reading docs_off")?,
        );
        let postings_off = u64::from_le_bytes(
            mmap[20..28]
                .try_into()
                .with_context(|| "shard truncated or malformed while reading postings_off")?,
        );
        let meta_off = u64::from_le_bytes(
            mmap[28..36]
                .try_into()
                .with_context(|| "shard truncated or malformed while reading meta_off")?,
        );
        let line_index_off = u64::from_le_bytes(
            mmap[36..44]
                .try_into()
                .with_context(|| "shard truncated or malformed while reading line_index_off")?,
        );
        let _line_data_off = u64::from_le_bytes(
            mmap[44..52]
                .try_into()
                .with_context(|| "shard truncated or malformed while reading line_data_off")?,
        );
        // parse metadata
        let mut moff = meta_off as usize;
        let n1 = u16::from_le_bytes(mmap[moff..moff + 2].try_into().unwrap()) as usize;
        moff += 2;
        let _repo_name = std::str::from_utf8(&mmap[moff..moff + n1])
            .with_context(|| {
                format!(
                    "shard metadata corrupted: repo_name not valid UTF-8 (meta_off={}, moff={})",
                    meta_off, moff
                )
            })?
            .to_string();
        moff += n1;
        let n2 = u16::from_le_bytes(mmap[moff..moff + 2].try_into().unwrap()) as usize;
        moff += 2;
        let repo_root = std::str::from_utf8(&mmap[moff..moff + n2])
            .with_context(|| {
                format!(
                    "shard metadata corrupted: repo_root not valid UTF-8 (meta_off={}, moff={})",
                    meta_off, moff
                )
            })?
            .to_string();
        moff += n2;
        let mut _repo_hash = [0u8; 32];
        _repo_hash.copy_from_slice(&mmap[moff..moff + 32]);
        moff += 32;
        // branches
        let mut branches = Vec::new();
        let n_br = u16::from_le_bytes(mmap[moff..moff + 2].try_into().unwrap()) as usize;
        moff += 2;
        for _ in 0..n_br {
            let bl = u16::from_le_bytes(mmap[moff..moff + 2].try_into().unwrap()) as usize;
            moff += 2;
            let b = std::str::from_utf8(&mmap[moff..moff + bl])
                .with_context(|| format!("shard metadata corrupted: branch name not valid UTF-8 (meta_off={}, moff={})", meta_off, moff))?
                .to_string();
            moff += bl;
            branches.push(b);
        }
        // per-doc symbols: for each doc, [count:u16] then entries of [name_len:u16][name][start:u32][line:u32]
        let mut symbols: SymbolsTable = Vec::with_capacity(doc_count as usize);
        for d in 0..(doc_count as usize) {
            let cnt = u16::from_le_bytes(mmap[moff..moff + 2].try_into().with_context(|| {
                format!(
                    "shard truncated while reading symbol count (meta_off={}, moff={}, doc={})",
                    meta_off, moff, d
                )
            })?) as usize;
            moff += 2;
            let mut syms = Vec::with_capacity(cnt);
            for _ in 0..cnt {
                let nl = u16::from_le_bytes(mmap[moff..moff + 2].try_into().with_context(|| {
                    format!(
                        "shard truncated while reading symbol name len (meta_off={}, moff={})",
                        meta_off, moff
                    )
                })?) as usize;
                moff += 2;
                let name = std::str::from_utf8(&mmap[moff..moff + nl])
                    .with_context(|| format!("shard metadata corrupted: symbol name not valid UTF-8 (meta_off={}, moff={})", meta_off, moff))?
                    .to_string();
                moff += nl;
                let start =
                    u32::from_le_bytes(mmap[moff..moff + 4].try_into().with_context(|| {
                        format!(
                            "shard truncated while reading symbol start (meta_off={}, moff={})",
                            meta_off, moff
                        )
                    })?);
                moff += 4;
                let line =
                    u32::from_le_bytes(mmap[moff..moff + 4].try_into().with_context(|| {
                        format!(
                            "shard truncated while reading symbol line (meta_off={}, moff={})",
                            meta_off, moff
                        )
                    })?);
                moff += 4;
                let start_opt = if start == u32::MAX { None } else { Some(start) };
                let line_opt = if line == u32::MAX { None } else { Some(line) };
                syms.push((name, start_opt, line_opt));
            }
            symbols.push(syms);
        }
        Ok(Self {
            mmap,
            doc_count,
            docs_off,
            postings_off,
            line_index_off,
            _repo_name,
            repo_root,
            _repo_hash,
            branches,
            symbols,
        })
    }

    pub fn doc_count(&self) -> u32 {
        self.doc_count
    }
    pub fn repo_root(&self) -> &str {
        &self.repo_root
    }
    pub fn repo_name(&self) -> &str {
        &self._repo_name
    }
    pub fn repo_hash(&self) -> [u8; 32] {
        self._repo_hash
    }
    pub fn branches(&self) -> &[String] {
        &self.branches
    }

    /// Return per-doc symbols parsed from the shard metadata, if any.
    pub fn symbols_for_doc(&self, doc: u32) -> Option<&[SymbolTuple]> {
        if (doc as usize) < self.symbols.len() {
            Some(&self.symbols[doc as usize])
        } else {
            None
        }
    }

    fn iter_docs(&self) -> Result<Vec<String>> {
        let mut off = self.docs_off as usize;
        let mut out = Vec::with_capacity(self.doc_count as usize);
        for _ in 0..self.doc_count {
            let len =
                u16::from_le_bytes(self.mmap[off..off + 2].try_into().with_context(|| {
                    format!("shard truncated while reading doc len (off={})", off)
                })?) as usize;
            off += 2;
            let s = std::str::from_utf8(&self.mmap[off..off + len])
                .with_context(|| {
                    format!(
                        "shard metadata corrupted: doc path not valid UTF-8 (off={})",
                        off
                    )
                })?
                .to_string();
            off += len;
            out.push(s);
        }
        Ok(out)
    }

    fn postings_map(&self) -> Result<PostingsMap> {
        let mut off = self.postings_off as usize;
        let mut map = HashMap::new();
        let term_count =
            u32::from_le_bytes(self.mmap[off..off + 4].try_into().with_context(|| {
                format!("shard truncated while reading term_count (off={})", off)
            })?) as usize;
        off += 4;
        for _ in 0..term_count {
            let tri: [u8; 3] = self.mmap[off..off + 3]
                .try_into()
                .with_context(|| format!("shard truncated while reading trigram (off={})", off))?;
            off += 3;
            let n_docs =
                u32::from_le_bytes(self.mmap[off..off + 4].try_into().with_context(|| {
                    format!("shard truncated while reading n_docs (off={})", off)
                })?) as usize;
            off += 4;
            let mut postings = BTreeMap::new();
            let mut prev_doc: u32 = 0;
            for _ in 0..n_docs {
                let doc_delta = read_var_u32_from_mmap(&self.mmap, &mut off)?;
                let doc = prev_doc.wrapping_add(doc_delta);
                prev_doc = doc;
                let npos = read_var_u32_from_mmap(&self.mmap, &mut off)? as usize;
                let mut pos = Vec::with_capacity(npos);
                let mut prev_pos: u32 = 0;
                for _ in 0..npos {
                    let pos_delta = read_var_u32_from_mmap(&self.mmap, &mut off)?;
                    let p = prev_pos.wrapping_add(pos_delta);
                    pos.push(p);
                    prev_pos = p;
                }
                postings.insert(doc, pos);
            }
            map.insert(tri, postings);
        }
        Ok(map)
    }

    fn symbol_postings_map(&self) -> Result<SymbolPostingsMap> {
        // The postings section contains first the content trigram map, then the symbol trigram map.
        let mut off = self.postings_off as usize;
        // parse and skip content map (delta+varint encoded)
        let content_term_count =
            u32::from_le_bytes(self.mmap[off..off + 4].try_into().with_context(|| {
                format!(
                    "shard truncated while reading content term_count (off={})",
                    off
                )
            })?) as usize;
        off += 4;
        for _ in 0..content_term_count {
            off += 3; // tri
            let n_docs =
                u32::from_le_bytes(self.mmap[off..off + 4].try_into().with_context(|| {
                    format!("shard truncated while reading content n_docs (off={})", off)
                })?) as usize;
            off += 4;
            let mut prev_doc: u32 = 0;
            for _ in 0..n_docs {
                let doc_delta = read_var_u32_from_mmap(&self.mmap, &mut off)?;
                let doc = prev_doc.wrapping_add(doc_delta);
                prev_doc = doc;
                let npos = read_var_u32_from_mmap(&self.mmap, &mut off)? as usize;
                let mut prev_pos: u32 = 0;
                for _ in 0..npos {
                    let pos_delta = read_var_u32_from_mmap(&self.mmap, &mut off)?;
                    let p = prev_pos.wrapping_add(pos_delta);
                    prev_pos = p;
                }
            }
        }
        // now read symbol map
        let mut sym_map: SymbolPostingsMap = HashMap::new();
        let sym_count =
            u32::from_le_bytes(self.mmap[off..off + 4].try_into().with_context(|| {
                format!(
                    "shard truncated while reading symbol term_count (off={})",
                    off
                )
            })?) as usize;
        off += 4;
        for _ in 0..sym_count {
            let tri: [u8; 3] = self.mmap[off..off + 3].try_into().with_context(|| {
                format!("shard truncated while reading symbol trigram (off={})", off)
            })?;
            off += 3;
            let n_docs =
                u32::from_le_bytes(self.mmap[off..off + 4].try_into().with_context(|| {
                    format!("shard truncated while reading symbol n_docs (off={})", off)
                })?) as usize;
            off += 4;
            let mut docs = Vec::with_capacity(n_docs);
            let mut prev_doc: u32 = 0;
            for _ in 0..n_docs {
                let doc_delta = read_var_u32_from_mmap(&self.mmap, &mut off)?;
                let d = prev_doc.wrapping_add(doc_delta);
                prev_doc = d;
                let _npos = read_var_u32_from_mmap(&self.mmap, &mut off)?; // usually zero
                docs.push(d);
            }
            sym_map.insert(tri, docs);
        }
        Ok(sym_map)
    }

    fn line_index_entry(&self, doc: u32) -> Option<(u64, u32)> {
        if doc >= self.doc_count {
            return None;
        }
        let base = self.line_index_off as usize + (doc as usize) * 12;
        let off = u64::from_le_bytes(self.mmap[base..base + 8].try_into().ok()?);
        let cnt = u32::from_le_bytes(self.mmap[base + 8..base + 12].try_into().ok()?);
        Some((off, cnt))
    }

    fn load_line_starts(&self, doc: u32) -> Option<Vec<u32>> {
        let (off, cnt) = self.line_index_entry(doc)?;
        let mut p = off as usize;
        let n = u32::from_le_bytes(self.mmap[p..p + 4].try_into().ok()?) as usize;
        p += 4;
        if n as u32 != cnt {
            return None;
        }
        let mut v = Vec::with_capacity(n);
        for _ in 0..n {
            let s = u32::from_le_bytes(self.mmap[p..p + 4].try_into().ok()?);
            p += 4;
            v.push(s);
        }
        Some(v)
    }
}

pub struct ShardSearcher<'a> {
    rdr: &'a ShardReader,
}
impl<'a> ShardSearcher<'a> {
    pub fn new(rdr: &'a ShardReader) -> Self {
        Self { rdr }
    }

    pub fn search_literal(&self, needle: &str) -> Vec<(u32, String)> {
        let tris: Vec<_> = trigrams(needle).collect();
        if tris.is_empty() {
            return vec![];
        }
        let map = match self.rdr.postings_map() {
            Ok(m) => m,
            Err(_) => return vec![],
        };
        let mut cand: Option<Vec<u32>> = None;
        for t in tris {
            if let Some(v) = map.get(&t) {
                let docs: Vec<u32> = v.keys().copied().collect();
                cand = Some(match cand {
                    None => docs,
                    Some(prev) => intersect_sorted(&prev, &docs),
                });
            } else {
                return vec![];
            }
        }
        let paths: Vec<_> = match self.rdr.iter_docs() {
            Ok(p) => p,
            Err(_) => return vec![],
        };
        cand.unwrap_or_default()
            .into_iter()
            .map(|d| (d, paths[d as usize].clone()))
            .collect()
    }

    pub fn search_regex_prefiltered(&self, pattern: &str) -> Vec<(u32, String)> {
        use regex::Regex;
        let _re = match Regex::new(pattern) {
            Ok(r) => r,
            Err(_) => return vec![],
        };
        let pf = prefilter_from_regex(pattern);
        let map = match self.rdr.postings_map() {
            Ok(m) => m,
            Err(_) => return vec![],
        };
        let paths: Vec<_> = match self.rdr.iter_docs() {
            Ok(p) => p,
            Err(_) => return vec![],
        };
        let mut cand: Option<Vec<u32>> = None;
        match pf {
            crate::regex_analyze::Prefilter::Conj(tris) => {
                for t in tris {
                    if let Some(v) = map.get(&t) {
                        let docs: Vec<u32> = v.keys().copied().collect();
                        cand = Some(match cand {
                            None => docs,
                            Some(prev) => intersect_sorted(&prev, &docs),
                        });
                    } else {
                        return vec![];
                    }
                }
            }
            crate::regex_analyze::Prefilter::Disj(disj) => {
                // For disjunction, union all per-branch candidate sets.
                let mut union_docs: Vec<u32> = Vec::new();
                use std::collections::BTreeSet;
                let mut set: BTreeSet<u32> = BTreeSet::new();
                for tris in disj {
                    let mut branch_cand: Option<Vec<u32>> = None;
                    let mut branch_ok = true;
                    for t in tris {
                        if let Some(v) = map.get(&t) {
                            let docs: Vec<u32> = v.keys().copied().collect();
                            branch_cand = Some(match branch_cand {
                                None => docs,
                                Some(prev) => intersect_sorted(&prev, &docs),
                            });
                        } else {
                            branch_ok = false;
                            break;
                        }
                    }
                    if branch_ok {
                        if let Some(bc) = branch_cand {
                            for d in bc.into_iter() {
                                set.insert(d);
                            }
                        }
                    }
                }
                union_docs.extend(set);
                cand = Some(union_docs);
            }
            _ => {
                cand = Some((0..paths.len() as u32).collect());
            }
        }
        cand.unwrap_or_default()
            .into_iter()
            .map(|d| (d, paths[d as usize].clone()))
            .collect()
    }

    /// Structured matches using positions and line tables
    pub fn search_literal_with_context(&self, needle: &str) -> Vec<SearchMatch> {
        self.search_literal_with_context_opts(needle, &SearchOpts::default())
    }

    pub fn search_literal_with_context_opts(
        &self,
        needle: &str,
        opts: &SearchOpts,
    ) -> Vec<SearchMatch> {
        if let Some(b) = &opts.branch {
            if !self.rdr.branches().iter().any(|x| x == b) {
                return vec![];
            }
        }
        let needle_b = needle.as_bytes();
        if needle_b.len() < 3 {
            return vec![];
        }
        let tris: Vec<_> = trigrams(needle).collect();
        if tris.is_empty() {
            return vec![];
        }
        let first = tris[0];
        let map = match self.rdr.postings_map() {
            Ok(m) => m,
            Err(_) => return vec![],
        };
        let mut cand: Option<Vec<u32>> = None;
        for t in tris.iter() {
            if let Some(v) = map.get(t) {
                let docs: Vec<u32> = v.keys().copied().collect();
                cand = Some(match cand {
                    None => docs,
                    Some(prev) => intersect_sorted(&prev, &docs),
                });
            } else {
                return vec![];
            }
        }
        let paths: Vec<_> = match self.rdr.iter_docs() {
            Ok(p) => p,
            Err(_) => return vec![],
        };
        let mut out = Vec::new();
        if let Some(cdocs) = cand {
            for d in cdocs {
                let path = &paths[d as usize];
                if let Some(pref) = &opts.path_prefix {
                    if !path.starts_with(pref) {
                        continue;
                    }
                }
                if let Some(re) = &opts.path_regex {
                    if !re.is_match(path) {
                        continue;
                    }
                }
                let pmap = match map.get(&first) {
                    Some(m) => m,
                    None => continue,
                };
                let positions = match pmap.get(&d) {
                    Some(v) => v,
                    None => continue,
                };
                let full_path = std::path::Path::new(&self.rdr.repo_root).join(&paths[d as usize]);
                let text = match std::fs::read(&full_path) {
                    Ok(b) => b,
                    Err(_) => continue,
                };
                for &pos in positions.iter() {
                    let pos = pos as usize;
                    if pos + needle_b.len() <= text.len()
                        && &text[pos..pos + needle_b.len()] == needle_b
                    {
                        let starts = match self.rdr.load_line_starts(d) {
                            Some(s) => s,
                            None => vec![0],
                        };
                        let (line_idx, line_start) = line_for_offset(&starts, pos as u32);
                        let (ctx_beg, ctx_end) =
                            context_byte_range(&starts, line_idx, opts.context, text.len());
                        let (line_beg, line_end) = line_bounds(&starts, line_idx, text.len());
                        let start_col = pos as u32 - line_start;
                        let end_col = start_col + needle_b.len() as u32;
                        let before = String::from_utf8_lossy(&text[ctx_beg..line_beg]).to_string();
                        let line_text =
                            String::from_utf8_lossy(&text[line_beg..line_end]).to_string();
                        let after = String::from_utf8_lossy(&text[line_end..ctx_end]).to_string();
                        out.push(SearchMatch {
                            doc: d,
                            path: paths[d as usize].clone(),
                            line: line_idx as u32 + 1,
                            start: start_col,
                            end: end_col,
                            before,
                            line_text,
                            after,
                        });
                        if let Some(limit) = opts.limit {
                            if out.len() >= limit {
                                return out;
                            }
                        }
                    }
                }
            }
        }
        out
    }

    pub fn search_regex_confirmed(&self, pattern: &str, opts: &SearchOpts) -> Vec<SearchMatch> {
        if let Some(b) = &opts.branch {
            if !self.rdr.branches().iter().any(|x| x == b) {
                return vec![];
            }
        }
        let re = match regex::Regex::new(pattern) {
            Ok(r) => r,
            Err(_) => return vec![],
        };
        let map = match self.rdr.postings_map() {
            Ok(m) => m,
            Err(_) => return vec![],
        };
        let paths: Vec<_> = match self.rdr.iter_docs() {
            Ok(p) => p,
            Err(_) => return vec![],
        };
        let mut cand: Option<Vec<u32>> = None;
        if let crate::regex_analyze::Prefilter::Conj(tris) = prefilter_from_regex(pattern) {
            for t in tris.iter() {
                if let Some(v) = map.get(t) {
                    let docs: Vec<u32> = v.keys().copied().collect();
                    cand = Some(match cand {
                        None => docs,
                        Some(prev) => intersect_sorted(&prev, &docs),
                    });
                } else {
                    return vec![];
                }
            }
        } else {
            cand = Some((0..paths.len() as u32).collect());
        }
        let mut out = Vec::new();
        if let Some(cdocs) = cand {
            for d in cdocs {
                let path = &paths[d as usize];
                if let Some(pref) = &opts.path_prefix {
                    if !path.starts_with(pref) {
                        continue;
                    }
                }
                if let Some(rex) = &opts.path_regex {
                    if !rex.is_match(path) {
                        continue;
                    }
                }
                let full_path = std::path::Path::new(&self.rdr.repo_root).join(path);
                let content = match std::fs::read_to_string(&full_path) {
                    Ok(s) => s,
                    Err(_) => continue,
                };
                let starts = match self.rdr.load_line_starts(d) {
                    Some(s) => s,
                    None => vec![0],
                };
                for m in re.find_iter(&content) {
                    let pos = m.start() as u32;
                    let (line_idx, line_start) = line_for_offset(&starts, pos);
                    let (ctx_beg, ctx_end) =
                        context_byte_range(&starts, line_idx, opts.context, content.len());
                    let (line_beg, line_end) = line_bounds(&starts, line_idx, content.len());
                    let start_col = pos - line_start;
                    let end_col = start_col + (m.end() - m.start()) as u32;
                    let before = content[ctx_beg..line_beg].to_string();
                    let line_text = content[line_beg..line_end].to_string();
                    let after = content[line_end..ctx_end].to_string();
                    out.push(SearchMatch {
                        doc: d,
                        path: path.clone(),
                        line: line_idx as u32 + 1,
                        start: start_col,
                        end: end_col,
                        before,
                        line_text,
                        after,
                    });
                    if let Some(limit) = opts.limit {
                        if out.len() >= limit {
                            return out;
                        }
                    }
                }
            }
        }
        out
    }

    /// Search symbols in the shard. If `pattern` is Some, filter symbol names by the pattern
    /// (regex when `is_regex` is true). Returns `crate::query::QueryResult` with `symbol_loc` populated
    /// from the shard metadata when available.
    pub fn search_symbols_prefiltered(
        &self,
        pattern: Option<&str>,
        is_regex: bool,
        case_sensitive: bool,
    ) -> Vec<crate::query::QueryResult> {
        let paths = match self.rdr.iter_docs() {
            Ok(p) => p,
            Err(_) => return vec![],
        };
        let mut out = Vec::new();

        // If we have a pattern, try to prefilter candidate docs using symbol trigram postings.
        if let Some(pat) = pattern {
            // If regex, extract alnum substrings >=3 as heuristic; else use trigrams of the pattern.
            let mut tris: Vec<[u8; 3]> = Vec::new();
            if is_regex {
                let mut subs: Vec<String> = Vec::new();
                let mut cur = String::new();
                for ch in pat.chars() {
                    if ch.is_alphanumeric() {
                        cur.push(ch);
                    } else {
                        if cur.len() >= 3 {
                            subs.push(cur.clone());
                        }
                        cur.clear();
                    }
                }
                if cur.len() >= 3 {
                    subs.push(cur);
                }
                if let Some(sub) = subs.first() {
                    tris = crate::trigram::trigrams(sub).collect();
                }
            } else {
                tris = crate::trigram::trigrams(pat).collect();
            }

            if !tris.is_empty() {
                if let Ok(sym_map) = self.rdr.symbol_postings_map() {
                    let mut cand_docs: Option<Vec<u32>> = None;
                    for t in tris {
                        if let Some(list) = sym_map.get(&t) {
                            let mut docs = list.clone();
                            docs.sort_unstable();
                            docs.dedup();
                            cand_docs = Some(match cand_docs {
                                None => docs,
                                Some(prev) => intersect_sorted(&prev, &docs),
                            });
                        } else {
                            cand_docs = Some(Vec::new());
                            break;
                        }
                    }
                    if let Some(cdocs) = cand_docs {
                        let filtered_set: std::collections::HashSet<u32> =
                            (0..paths.len() as u32).collect();
                        for d in cdocs.into_iter().filter(|d| filtered_set.contains(d)) {
                            if let Some(sym_list) = self.rdr.symbols_for_doc(d) {
                                for sym_tuple in sym_list.iter() {
                                    let name = &sym_tuple.0;
                                    let matched = if is_regex {
                                        let mut pat_s = pat.to_string();
                                        if !case_sensitive && !pat_s.starts_with("(?i)") {
                                            pat_s = format!("(?i){}", pat_s);
                                        }
                                        if let Ok(re) = regex::Regex::new(&pat_s) {
                                            re.is_match(name)
                                        } else {
                                            false
                                        }
                                    } else if case_sensitive {
                                        name.contains(pat)
                                    } else {
                                        name.to_lowercase().contains(&pat.to_lowercase())
                                    };
                                    if matched {
                                        let sym = crate::types::Symbol {
                                            name: name.clone(),
                                            start: sym_tuple.1,
                                            line: sym_tuple.2,
                                        };
                                        out.push(crate::query::QueryResult {
                                            doc: d,
                                            path: paths[d as usize].clone(),
                                            symbol: Some(name.clone()),
                                            symbol_loc: Some(sym),
                                        });
                                    }
                                }
                            }
                        }
                        return out;
                    }
                }
            }
        }

        // Fallback: scan all docs' symbols as before
        for d in 0..(paths.len() as u32) {
            if let Some(sym_list) = self.rdr.symbols_for_doc(d) {
                for sym_tuple in sym_list.iter() {
                    let name = &sym_tuple.0;
                    let matched = if let Some(pat) = pattern {
                        if is_regex {
                            let mut pat_s = pat.to_string();
                            if !case_sensitive && !pat_s.starts_with("(?i)") {
                                pat_s = format!("(?i){}", pat_s);
                            }
                            if let Ok(re) = regex::Regex::new(&pat_s) {
                                re.is_match(name)
                            } else {
                                false
                            }
                        } else if case_sensitive {
                            name.contains(pat)
                        } else {
                            name.to_lowercase().contains(&pat.to_lowercase())
                        }
                    } else {
                        true
                    };
                    if matched {
                        let sym = crate::types::Symbol {
                            name: name.clone(),
                            start: sym_tuple.1,
                            line: sym_tuple.2,
                        };
                        out.push(crate::query::QueryResult {
                            doc: d,
                            path: paths[d as usize].clone(),
                            symbol: Some(name.clone()),
                            symbol_loc: Some(sym),
                        });
                    }
                }
            }
        }

        out
    }
}

fn intersect_sorted(a: &[u32], b: &[u32]) -> Vec<u32> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::build_in_memory_index;

    #[test]
    fn write_read_search_roundtrip() -> Result<()> {
        let dir = tempfile::tempdir()?;
        std::fs::write(dir.path().join("a.txt"), b"hello zoekt shard")?;
        let idx = build_in_memory_index(dir.path())?;
        let shard_path = dir.path().join("index.shard");
        ShardWriter::new(&shard_path).write_from_index(&idx)?;

        let reader = ShardReader::open(&shard_path)?;
        let searcher = ShardSearcher::new(&reader);
        // debug: dump postings and docs
        if let Ok(pm) = reader.postings_map() {
            eprintln!("postings_map keys: {}", pm.len());
            for (k, v) in pm.iter() {
                eprintln!("tri={:?} -> docs={:?}", k, v.keys().collect::<Vec<_>>());
            }
        }
        if let Ok(docs) = reader.iter_docs() {
            eprintln!("docs: {:?}", docs);
        }
        let hits = searcher.search_literal_with_context("zoekt");
        assert!(hits
            .iter()
            .any(|m| m.path.ends_with("a.txt") && m.line >= 1));
        Ok(())
    }

    #[test]
    fn corrupt_shard_metadata_returns_error() -> Result<()> {
        let dir = tempfile::tempdir()?;
        std::fs::write(dir.path().join("a.txt"), b"hello zoekt shard")?;
        let idx = build_in_memory_index(dir.path())?;
        let shard_path = dir.path().join("index.shard");
        ShardWriter::new(&shard_path).write_from_index(&idx)?;

        // Read shard bytes and corrupt the repo_name bytes to invalid UTF-8
        let mut bytes = std::fs::read(&shard_path)?;
        // Find meta_off at header offset 28..36
        let meta_off = u64::from_le_bytes(bytes[28..36].try_into().unwrap()) as usize;
        // repo_name length is first u16 at meta_off
        let n1 = u16::from_le_bytes(bytes[meta_off..meta_off + 2].try_into().unwrap()) as usize;
        let name_start = meta_off + 2;
        if n1 > 0 {
            // Flip first byte to 0xFF to make invalid UTF-8
            bytes[name_start] = 0xFF;
        }
        let corrupt_path = dir.path().join("index-corrupt.shard");
        std::fs::write(&corrupt_path, &bytes)?;

        let res = ShardReader::open(&corrupt_path);
        assert!(res.is_err());
        let msg = res.err().unwrap().to_string();
        assert!(msg.contains("repo_name not valid UTF-8") || msg.contains("shard truncated"));
        Ok(())
    }

    #[test]
    fn symbol_trigram_prefilter_roundtrip() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let content = r#"
        // top-level functions
        fn Foo_bar() {}
        fn Other() {}
        "#;
        std::fs::write(dir.path().join("a.rs"), content)?;
        let idx = crate::build_in_memory_index(dir.path())?;
        let shard_path = dir.path().join("index.shard");
        ShardWriter::new(&shard_path).write_from_index(&idx)?;

        let reader = ShardReader::open(&shard_path)?;
        let searcher = ShardSearcher::new(&reader);
        // search for symbol substring "Foo" (non-regex)
        let res = searcher.search_symbols_prefiltered(Some("Foo"), false, false);
        // should find Foo_bar symbol
        assert!(res.iter().any(|r| r.symbol.as_deref() == Some("Foo_bar")));
        Ok(())
    }

    #[test]
    fn symbol_regex_case_sensitive_and_insensitive() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let content = r#"
        // top-level functions
        fn DoThing() {}
        fn dothing() {}
        "#;
        std::fs::write(dir.path().join("a.rs"), content)?;
        let idx = crate::build_in_memory_index(dir.path())?;
        let shard_path = dir.path().join("index.shard");
        ShardWriter::new(&shard_path).write_from_index(&idx)?;

        let reader = ShardReader::open(&shard_path)?;
        let searcher = ShardSearcher::new(&reader);

        // Case-sensitive regex should only match DoThing
        let res_cs = searcher.search_symbols_prefiltered(Some("^DoThing$"), true, true);
        assert!(res_cs
            .iter()
            .any(|r| r.symbol.as_deref() == Some("DoThing")));
        assert!(!res_cs
            .iter()
            .any(|r| r.symbol.as_deref() == Some("dothing")));

        // Case-insensitive regex should match both
        let res_ci = searcher.search_symbols_prefiltered(Some("^dothing$"), true, false);
        // should find both variants (pattern applied case-insensitively)
        assert!(res_ci
            .iter()
            .any(|r| r.symbol.as_deref() == Some("DoThing")));
        assert!(res_ci
            .iter()
            .any(|r| r.symbol.as_deref() == Some("dothing")));

        Ok(())
    }

    #[test]
    fn symbol_unicode_and_multibyte_names() -> Result<()> {
        let dir = tempfile::tempdir()?;
        // include an accented e (U+00E9) and some other multibyte chars
        let content = "fn caf\u{e9}() {}\nfn 漢字_func() {}\n";
        std::fs::write(dir.path().join("a.rs"), content)?;
        let idx = crate::build_in_memory_index(dir.path())?;
        let shard_path = dir.path().join("index.shard");
        ShardWriter::new(&shard_path).write_from_index(&idx)?;

        let reader = ShardReader::open(&shard_path)?;
        let searcher = ShardSearcher::new(&reader);

        // literal match for caf e9 (case-sensitive)
        let res = searcher.search_symbols_prefiltered(Some("caf\u{e9}"), false, true);
        assert!(res.iter().any(|r| r.symbol.as_deref() == Some("caf\u{e9}")));

        // literal match for non-ascii name
        let res2 = searcher.search_symbols_prefiltered(Some("漢字_func"), false, true);
        assert!(res2
            .iter()
            .any(|r| r.symbol.as_deref() == Some("漢字_func")));

        // regex unicode, case-insensitive should still find the accented name
        let res3 = searcher.search_symbols_prefiltered(Some("caf."), true, false);
        assert!(res3
            .iter()
            .any(|r| r.symbol.as_deref() == Some("caf\u{e9}")));

        Ok(())
    }

    #[test]
    fn many_symbols_in_single_doc() -> Result<()> {
        let dir = tempfile::tempdir()?;
        // generate many symbol names to ensure postings scale
        let mut content = String::new();
        let count = 200usize;
        for i in 0..count {
            content.push_str(&format!("fn sym_{:04}() {{}}\n", i));
        }
        std::fs::write(dir.path().join("big.rs"), content)?;
        let idx = crate::build_in_memory_index(dir.path())?;
        let shard_path = dir.path().join("index.shard");
        ShardWriter::new(&shard_path).write_from_index(&idx)?;

        let reader = ShardReader::open(&shard_path)?;
        let searcher = ShardSearcher::new(&reader);

        // search for common prefix
        let res = searcher.search_symbols_prefiltered(Some("sym_00"), false, false);
        // expect at least several matches; exact count should be >= 11 (sym_0000..sym_0010 etc.)
        assert!(res.len() >= 11, "expected many symbols, got {}", res.len());

        // search for a specific one
        let res2 = searcher.search_symbols_prefiltered(Some("sym_0199"), false, false);
        assert!(res2.iter().any(|r| r.symbol.as_deref() == Some("sym_0199")));

        Ok(())
    }

    #[test]
    fn symbol_postings_deduped_roundtrip() -> Result<()> {
        let dir = tempfile::tempdir()?;
        // Create two files with identical symbol names to ensure postings would
        // contain duplicates if not deduped.
        let content1 = "fn Dup() {}\n";
        let content2 = "fn Dup() {}\n";
        std::fs::write(dir.path().join("a.rs"), content1)?;
        std::fs::write(dir.path().join("b.rs"), content2)?;
        let idx = crate::build_in_memory_index(dir.path())?;
        let shard_path = dir.path().join("index.shard");
        ShardWriter::new(&shard_path).write_from_index(&idx)?;

        let reader = ShardReader::open(&shard_path)?;
        // symbol_postings_map should contain trigrams mapping to doc ids with no duplicates
        let sym_map = reader.symbol_postings_map()?;
        for (_tri, docs) in sym_map.iter() {
            // docs should be unique and sorted
            let mut sorted = docs.clone();
            sorted.sort_unstable();
            let mut dedup = sorted.clone();
            dedup.dedup();
            assert_eq!(sorted, dedup);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct SearchMatch {
    pub doc: u32,
    pub path: String,
    pub line: u32,
    pub start: u32,
    pub end: u32,
    pub before: String,
    pub line_text: String,
    pub after: String,
}

#[derive(Default, Debug, Clone)]
pub struct SearchOpts {
    pub path_prefix: Option<String>,
    pub path_regex: Option<regex::Regex>,
    pub limit: Option<usize>,
    pub context: usize,
    pub branch: Option<String>,
}

fn line_for_offset(starts: &[u32], pos: u32) -> (usize, u32) {
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

fn context_byte_range(
    starts: &[u32],
    line_idx: usize,
    context: usize,
    file_len: usize,
) -> (usize, usize) {
    if starts.is_empty() {
        return (0, file_len);
    }
    let begin_line = line_idx.saturating_sub(context);
    let last_idx = starts.len().saturating_sub(1);
    let end_line = std::cmp::min(line_idx + context, last_idx);
    let beg = starts[begin_line] as usize;
    let end = if end_line + 1 < starts.len() {
        starts[end_line + 1] as usize
    } else {
        file_len
    };
    (beg, end)
}

fn line_bounds(starts: &[u32], line_idx: usize, file_len: usize) -> (usize, usize) {
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
