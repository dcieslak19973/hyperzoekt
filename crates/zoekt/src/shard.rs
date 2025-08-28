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
    index::{InMemoryIndex, RepoDocId},
    regex_analyze::prefilter_from_regex,
    trigram::{trigrams, trigrams_with_pos},
};
use sha2::{Digest, Sha256};

const MAGIC: u32 = 0x5a4f_454b; // 'ZOEK'
const VERSION: u32 = 4;

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
        let mut f = File::create(&self.path).context("create shard file")?;
        let inner = idx.read_inner();
        // Header placeholders; we'll fill offsets after writing sections.
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

        // Build postings: trigram -> BTreeMap<doc, Vec<u32 /*pos*/>>
        let mut term_map: HashMap<[u8; 3], BTreeMap<RepoDocId, Vec<u32>>> = HashMap::new();
        // Build line starts per doc (byte offsets)
        let mut lines_per_doc: Vec<Vec<u32>> = Vec::with_capacity(inner.docs.len());
        for (i, meta) in inner.docs.iter().enumerate() {
            let text =
                std::fs::read_to_string(inner.repo.root.join(&meta.path)).unwrap_or_default();
            for (tri, pos) in trigrams_with_pos(&text) {
                term_map
                    .entry(tri)
                    .or_default()
                    .entry(i as RepoDocId)
                    .or_default()
                    .push(pos);
            }
            // line starts for this doc
            let mut starts = vec![0u32];
            let bytes = text.as_bytes();
            for (idx, b) in bytes.iter().enumerate() {
                if *b == b'\n' {
                    let next = idx as u32 + 1;
                    if (next as usize) < bytes.len() {
                        starts.push(next);
                    }
                }
            }
            lines_per_doc.push(starts);
        }

        // Write postings section
        let postings_off = f.stream_position()?;
        f.write_all(&(term_map.len() as u32).to_le_bytes())?;
        for (tri, postings) in term_map.iter() {
            f.write_all(tri)?; // 3 bytes
            f.write_all(&(postings.len() as u32).to_le_bytes())?; // doc count
            for (doc, pos_list) in postings.iter() {
                f.write_all(&doc.to_le_bytes())?;
                f.write_all(&(pos_list.len() as u32).to_le_bytes())?;
                for p in pos_list {
                    f.write_all(&p.to_le_bytes())?;
                }
            }
        }

        // Write metadata section (repo name/root/hash/branches)
        let meta_off = f.stream_position()?;
        let repo_name = inner.repo.name.as_bytes();
        let repo_root = inner.repo.root.display().to_string();
        let repo_root_b = repo_root.as_bytes();
        let mut hasher = Sha256::new();
        for d in &inner.docs {
            let p = inner.repo.root.join(&d.path);
            if let Ok(bytes) = std::fs::read(&p) {
                hasher.update(&bytes);
            }
        }
        let hash = hasher.finalize();
        f.write_all(&(repo_name.len() as u16).to_le_bytes())?;
        f.write_all(repo_name)?;
        f.write_all(&(repo_root_b.len() as u16).to_le_bytes())?;
        f.write_all(repo_root_b)?;
        f.write_all(&hash[..])?; // 32 bytes
                                 // branches: count + each [len:u16][bytes]
        let branches = &inner.repo.branches;
        f.write_all(&(branches.len() as u16).to_le_bytes())?;
        for b in branches {
            let bb = b.as_bytes();
            f.write_all(&(bb.len() as u16).to_le_bytes())?;
            f.write_all(bb)?;
        }

        // Write line index section: for each doc, store (data_off, count); then write line data blocks
        let line_index_off = f.stream_position()?;
        for _ in 0..inner.docs.len() {
            f.write_all(&0u64.to_le_bytes())?;
            f.write_all(&0u32.to_le_bytes())?;
        }
        let line_data_off = f.stream_position()?;
        let mut idx_ptr = line_index_off;
        for starts in &lines_per_doc {
            let entry_off = f.stream_position()?;
            f.write_all(&(starts.len() as u32).to_le_bytes())?;
            for s in starts {
                f.write_all(&s.to_le_bytes())?;
            }
            // backpatch
            let cur = f.stream_position()?;
            f.seek(SeekFrom::Start(idx_ptr))?;
            f.write_all(&entry_off.to_le_bytes())?;
            f.write_all(&(starts.len() as u32).to_le_bytes())?;
            idx_ptr += 12; // 8 + 4
            f.seek(SeekFrom::Start(cur))?;
        }

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
}

impl ShardReader {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let file = File::open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        // parse header
        if mmap.len() < 12 + 8 * 5 {
            bail!("file too small")
        }
        let magic = u32::from_le_bytes(mmap[0..4].try_into().unwrap());
        let ver = u32::from_le_bytes(mmap[4..8].try_into().unwrap());
        if magic != MAGIC || ver != VERSION {
            bail!("bad header")
        }
        let doc_count = u32::from_le_bytes(mmap[8..12].try_into().unwrap());
        let docs_off = u64::from_le_bytes(mmap[12..20].try_into().unwrap());
        let postings_off = u64::from_le_bytes(mmap[20..28].try_into().unwrap());
        let meta_off = u64::from_le_bytes(mmap[28..36].try_into().unwrap());
        let line_index_off = u64::from_le_bytes(mmap[36..44].try_into().unwrap());
        let _line_data_off = u64::from_le_bytes(mmap[44..52].try_into().unwrap());
        // parse metadata
        let mut moff = meta_off as usize;
        let n1 = u16::from_le_bytes(mmap[moff..moff + 2].try_into().unwrap()) as usize;
        moff += 2;
        let _repo_name = std::str::from_utf8(&mmap[moff..moff + n1])
            .unwrap()
            .to_string();
        moff += n1;
        let n2 = u16::from_le_bytes(mmap[moff..moff + 2].try_into().unwrap()) as usize;
        moff += 2;
        let repo_root = std::str::from_utf8(&mmap[moff..moff + n2])
            .unwrap()
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
                .unwrap()
                .to_string();
            moff += bl;
            branches.push(b);
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

    fn iter_docs(&self) -> impl Iterator<Item = String> + '_ {
        let mut off = self.docs_off as usize;
        (0..self.doc_count).map(move |_| {
            let len = u16::from_le_bytes(self.mmap[off..off + 2].try_into().unwrap()) as usize;
            off += 2;
            let s = std::str::from_utf8(&self.mmap[off..off + len])
                .unwrap()
                .to_string();
            off += len;
            s
        })
    }

    fn postings_map(&self) -> HashMap<[u8; 3], BTreeMap<u32, Vec<u32>>> {
        let mut off = self.postings_off as usize;
        let mut map = HashMap::new();
        let term_count = u32::from_le_bytes(self.mmap[off..off + 4].try_into().unwrap()) as usize;
        off += 4;
        for _ in 0..term_count {
            let tri: [u8; 3] = self.mmap[off..off + 3].try_into().unwrap();
            off += 3;
            let n_docs = u32::from_le_bytes(self.mmap[off..off + 4].try_into().unwrap()) as usize;
            off += 4;
            let mut postings = BTreeMap::new();
            for _ in 0..n_docs {
                let d = u32::from_le_bytes(self.mmap[off..off + 4].try_into().unwrap());
                off += 4;
                let npos = u32::from_le_bytes(self.mmap[off..off + 4].try_into().unwrap()) as usize;
                off += 4;
                let mut pos = Vec::with_capacity(npos);
                for _ in 0..npos {
                    let p = u32::from_le_bytes(self.mmap[off..off + 4].try_into().unwrap());
                    off += 4;
                    pos.push(p);
                }
                postings.insert(d, pos);
            }
            map.insert(tri, postings);
        }
        map
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
        // Use postings prefilter then confirm by scanning files on disk relative to unknown root.
        // For now, we only return doc ids/paths from the shard and do not open files.
        let tris: Vec<_> = trigrams(needle).collect();
        if tris.is_empty() {
            return vec![];
        }
        let map = self.rdr.postings_map();
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
        let paths: Vec<_> = self.rdr.iter_docs().collect();
        cand.unwrap_or_default()
            .into_iter()
            .map(|d| (d, paths[d as usize].clone()))
            .collect()
    }

    pub fn search_regex_prefiltered(&self, pattern: &str) -> Vec<(u32, String)> {
        // Use a simple prefilter to get candidate docs via trigrams, then confirm by regex.
        use regex::Regex;
        let _re = match Regex::new(pattern) {
            Ok(r) => r,
            Err(_) => return vec![],
        };
        let pf = prefilter_from_regex(pattern);
        let paths: Vec<_> = self.rdr.iter_docs().collect();
        let map = self.rdr.postings_map();
        let mut cand: Option<Vec<u32>> = None;
        if let crate::regex_analyze::Prefilter::Conj(tris) = pf {
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
        } else {
            // no prefilter: consider all docs
            cand = Some((0..paths.len() as u32).collect());
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
        // branch filter (repo-level). If set and not present, return empty.
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
        let map = self.rdr.postings_map();
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
        let paths: Vec<_> = self.rdr.iter_docs().collect();
        let mut out = Vec::new();
        if let Some(cdocs) = cand {
            for d in cdocs {
                // path filters
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
        let paths: Vec<_> = self.rdr.iter_docs().collect();
        // candidate docs via prefilter
        let map = self.rdr.postings_map();
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
        let hits = searcher.search_literal_with_context("zoekt");
        assert!(hits
            .iter()
            .any(|m| m.path.ends_with("a.txt") && m.line >= 1));
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
