use super::read_var_u32_from_mmap;
use super::{MAGIC, VERSION};
use anyhow::{bail, Context, Result};
use memmap2::Mmap;
use std::{
    collections::{BTreeMap, HashMap},
    fs::File,
    path::Path,
};

pub struct ShardReader {
    mmap: Mmap,
    doc_count: u32,
    #[allow(dead_code)]
    docs_off: u64,
    postings_off: u64,
    line_index_off: u64,
    doc_paths: Vec<String>,
    content_term_index: HashMap<[u8; 3], TermEntry>,
    symbol_term_index: HashMap<[u8; 3], TermEntry>,
    // expose term indices to parent module within crate for efficient access
    // (kept pub(crate) to avoid leaking to the public API)
    _repo_name: String,
    repo_root: String,
    _repo_hash: [u8; 32],
    branches: Vec<String>,
    symbols: super::SymbolsTable,
}

#[derive(Clone, Copy, Debug)]
struct TermEntry {
    off: usize,
    n_docs: u32,
}

impl ShardReader {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let file = File::open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };
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
        let mut branches = Vec::new();
        let n_br = u16::from_le_bytes(mmap[moff..moff + 2].try_into().unwrap()) as usize;
        moff += 2;
        for _ in 0..n_br {
            let bl = u16::from_le_bytes(mmap[moff..moff + 2].try_into().unwrap()) as usize;
            moff += 2;
            let b = std::str::from_utf8(&mmap[moff..moff + bl]).with_context(|| format!("shard metadata corrupted: branch name not valid UTF-8 (meta_off={}, moff={})", meta_off, moff))?.to_string();
            moff += bl;
            branches.push(b);
        }
        let mut symbols: super::SymbolsTable = Vec::with_capacity(doc_count as usize);
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
                let name = std::str::from_utf8(&mmap[moff..moff + nl]).with_context(|| format!("shard metadata corrupted: symbol name not valid UTF-8 (meta_off={}, moff={})", meta_off, moff))?.to_string();
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
        let mut doff = docs_off as usize;
        let mut doc_paths: Vec<String> = Vec::with_capacity(doc_count as usize);
        for _ in 0..doc_count {
            let len =
                u16::from_le_bytes(mmap[doff..doff + 2].try_into().with_context(|| {
                    format!("shard truncated while reading doc len (off={})", doff)
                })?) as usize;
            doff += 2;
            let s = std::str::from_utf8(&mmap[doff..doff + len])
                .with_context(|| {
                    format!(
                        "shard metadata corrupted: doc path not valid UTF-8 (off={})",
                        doff
                    )
                })?
                .to_string();
            doff += len;
            doc_paths.push(s);
        }
        let (content_term_index, symbol_term_index) =
            Self::build_term_indices(&mmap, postings_off)?;
        Ok(Self {
            mmap,
            doc_count,
            docs_off,
            postings_off,
            line_index_off,
            doc_paths,
            content_term_index,
            symbol_term_index,
            _repo_name,
            repo_root,
            _repo_hash,
            branches,
            symbols,
        })
    }

    /// Return document frequency (n_docs) for a content trigram key if present.
    pub fn content_term_df(&self, tri: &[u8; 3]) -> Option<u32> {
        self.content_term_index.get(tri).map(|e| e.n_docs)
    }

    /// Return document frequency (n_docs) for a symbol trigram key if present.
    pub fn symbol_term_df(&self, tri: &[u8; 3]) -> Option<u32> {
        self.symbol_term_index.get(tri).map(|e| e.n_docs)
    }

    /// Apply a mapper to the content term index entry for `tri`, returning the mapped value.
    /// The mapper receives only the document frequency (n_docs) to avoid exposing private types.
    pub fn map_content_term<F, T>(&self, tri: &[u8; 3], f: F) -> Option<T>
    where
        F: FnOnce(u32) -> T,
    {
        self.content_term_index.get(tri).map(|e| f(e.n_docs))
    }

    #[allow(clippy::type_complexity)]
    fn build_term_indices(
        mmap: &Mmap,
        postings_off: u64,
    ) -> Result<(HashMap<[u8; 3], TermEntry>, HashMap<[u8; 3], TermEntry>)> {
        let mut off = postings_off as usize;
        let mut content_index: HashMap<[u8; 3], TermEntry> = HashMap::new();
        let term_count =
            u32::from_le_bytes(mmap[off..off + 4].try_into().with_context(|| {
                format!("shard truncated while reading term_count (off={})", off)
            })?) as usize;
        off += 4;
        for _ in 0..term_count {
            let tri: [u8; 3] = mmap[off..off + 3]
                .try_into()
                .with_context(|| format!("shard truncated while reading trigram (off={})", off))?;
            off += 3;
            let n_docs =
                u32::from_le_bytes(mmap[off..off + 4].try_into().with_context(|| {
                    format!("shard truncated while reading n_docs (off={})", off)
                })?);
            off += 4;
            let entry_off = off;
            let mut local_off = off;
            for _ in 0..n_docs as usize {
                let _ = read_var_u32_from_mmap(mmap, &mut local_off)?;
                let npos = read_var_u32_from_mmap(mmap, &mut local_off)? as usize;
                for _ in 0..npos {
                    let _ = read_var_u32_from_mmap(mmap, &mut local_off)?;
                }
            }
            content_index.insert(
                tri,
                TermEntry {
                    off: entry_off,
                    n_docs,
                },
            );
            off = local_off;
        }
        let mut sym_index: HashMap<[u8; 3], TermEntry> = HashMap::new();
        let sym_count = u32::from_le_bytes(mmap[off..off + 4].try_into().with_context(|| {
            format!(
                "shard truncated while reading symbol term_count (off={})",
                off
            )
        })?) as usize;
        off += 4;
        for _ in 0..sym_count {
            let tri: [u8; 3] = mmap[off..off + 3].try_into().with_context(|| {
                format!("shard truncated while reading symbol trigram (off={})", off)
            })?;
            off += 3;
            let n_docs = u32::from_le_bytes(mmap[off..off + 4].try_into().with_context(|| {
                format!("shard truncated while reading symbol n_docs (off={})", off)
            })?);
            off += 4;
            let entry_off = off;
            let mut local_off = off;
            let mut prev_doc: u32 = 0;
            for _ in 0..n_docs as usize {
                let d = read_var_u32_from_mmap(mmap, &mut local_off)?;
                let _doc = prev_doc.wrapping_add(d);
                prev_doc = _doc;
                let _npos = read_var_u32_from_mmap(mmap, &mut local_off)?;
            }
            sym_index.insert(
                tri,
                TermEntry {
                    off: entry_off,
                    n_docs,
                },
            );
            off = local_off;
        }
        Ok((content_index, sym_index))
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
    pub fn paths(&self) -> &[String] {
        &self.doc_paths
    }
    pub fn symbols_for_doc(&self, doc: u32) -> Option<&[super::SymbolTuple]> {
        if (doc as usize) < self.symbols.len() {
            Some(&self.symbols[doc as usize])
        } else {
            None
        }
    }
    #[allow(dead_code)]
    fn iter_docs(&self) -> Result<Vec<String>> {
        Ok(self.doc_paths.clone())
    }

    pub(crate) fn postings_map(&self) -> Result<super::PostingsMap> {
        let mut off = self.postings_off as usize;
        let mut map: super::PostingsMap = HashMap::new();
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
                let doc_delta = super::read_var_u32_from_mmap(&self.mmap, &mut off)?;
                let doc = prev_doc.wrapping_add(doc_delta);
                prev_doc = doc;
                let npos = super::read_var_u32_from_mmap(&self.mmap, &mut off)? as usize;
                let mut pos = Vec::with_capacity(npos);
                let mut prev_pos: u32 = 0;
                for _ in 0..npos {
                    let pos_delta = super::read_var_u32_from_mmap(&self.mmap, &mut off)?;
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

    // content_term_docs, decode_content_term, content_term_positions_for_doc, symbol_term_docs
    pub(crate) fn content_term_docs(&self, tri: &[u8; 3]) -> Option<Vec<u32>> {
        let entry = self.content_term_index.get(tri)?;
        let mut off = entry.off;
        let mut out = Vec::with_capacity(entry.n_docs as usize);
        let mut prev_doc: u32 = 0;
        for _ in 0..entry.n_docs {
            let doc_delta = super::read_var_u32_from_mmap(&self.mmap, &mut off).ok()?;
            let doc = prev_doc.wrapping_add(doc_delta);
            prev_doc = doc;
            out.push(doc);
            let npos = super::read_var_u32_from_mmap(&self.mmap, &mut off).ok()? as usize;
            for _ in 0..npos {
                let _ = super::read_var_u32_from_mmap(&self.mmap, &mut off).ok()?;
            }
        }
        Some(out)
    }

    #[allow(dead_code)]
    pub(crate) fn decode_content_term(&self, tri: &[u8; 3]) -> Option<BTreeMap<u32, Vec<u32>>> {
        let entry = self.content_term_index.get(tri)?;
        let mut off = entry.off;
        let mut out: BTreeMap<u32, Vec<u32>> = BTreeMap::new();
        let mut prev_doc: u32 = 0;
        for _ in 0..entry.n_docs {
            let doc_delta = read_var_u32_from_mmap(&self.mmap, &mut off).ok()?;
            let doc = prev_doc.wrapping_add(doc_delta);
            prev_doc = doc;
            let npos = read_var_u32_from_mmap(&self.mmap, &mut off).ok()? as usize;
            let mut pos = Vec::with_capacity(std::cmp::min(npos, 256));
            let mut prev_pos: u32 = 0;
            for _ in 0..npos {
                let pos_delta = read_var_u32_from_mmap(&self.mmap, &mut off).ok()?;
                let p = prev_pos.wrapping_add(pos_delta);
                prev_pos = p;
                pos.push(p);
            }
            out.insert(doc, pos);
        }
        Some(out)
    }

    #[allow(dead_code)]
    pub(crate) fn content_term_positions_for_doc(
        &self,
        tri: &[u8; 3],
        target_doc: u32,
    ) -> Option<Vec<u32>> {
        let entry = self.content_term_index.get(tri)?;
        let mut off = entry.off;
        let mut prev_doc: u32 = 0;
        for _ in 0..entry.n_docs {
            let doc_delta = read_var_u32_from_mmap(&self.mmap, &mut off).ok()?;
            let doc = prev_doc.wrapping_add(doc_delta);
            prev_doc = doc;
            let npos = read_var_u32_from_mmap(&self.mmap, &mut off).ok()? as usize;
            if doc == target_doc {
                let mut pos = Vec::with_capacity(std::cmp::min(npos, 256));
                let mut prev_pos: u32 = 0;
                for _ in 0..npos {
                    let pos_delta = read_var_u32_from_mmap(&self.mmap, &mut off).ok()?;
                    let p = prev_pos.wrapping_add(pos_delta);
                    prev_pos = p;
                    pos.push(p);
                }
                return Some(pos);
            } else {
                for _ in 0..npos {
                    let _ = read_var_u32_from_mmap(&self.mmap, &mut off).ok()?;
                }
            }
        }
        Some(Vec::new())
    }

    #[allow(dead_code)]
    pub(crate) fn symbol_term_docs(&self, tri: &[u8; 3]) -> Option<Vec<u32>> {
        let entry = self.symbol_term_index.get(tri)?;
        let mut off = entry.off;
        let mut out = Vec::with_capacity(entry.n_docs as usize);
        let mut prev_doc: u32 = 0;
        for _ in 0..entry.n_docs {
            let d = read_var_u32_from_mmap(&self.mmap, &mut off).ok()?;
            let doc = prev_doc.wrapping_add(d);
            prev_doc = doc;
            let _npos = read_var_u32_from_mmap(&self.mmap, &mut off).ok()?;
            out.push(doc);
        }
        Some(out)
    }

    pub(crate) fn symbol_postings_map(&self) -> Result<super::SymbolPostingsMap> {
        let mut off = self.postings_off as usize;
        let content_term_count =
            u32::from_le_bytes(self.mmap[off..off + 4].try_into().with_context(|| {
                format!(
                    "shard truncated while reading content term_count (off={})",
                    off
                )
            })?) as usize;
        off += 4;
        for _ in 0..content_term_count {
            off += 3;
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
        let mut sym_map: super::SymbolPostingsMap = HashMap::new();
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
                let _npos = read_var_u32_from_mmap(&self.mmap, &mut off)?;
                docs.push(d);
            }
            sym_map.insert(tri, docs);
        }
        Ok(sym_map)
    }

    pub(crate) fn line_index_entry(&self, doc: u32) -> Option<(u64, u32)> {
        if doc >= self.doc_count {
            return None;
        }
        let base = self.line_index_off as usize + (doc as usize) * 12;
        let off = u64::from_le_bytes(self.mmap[base..base + 8].try_into().ok()?);
        let cnt = u32::from_le_bytes(self.mmap[base + 8..base + 12].try_into().ok()?);
        Some((off, cnt))
    }

    pub(crate) fn load_line_starts(&self, doc: u32) -> Option<Vec<u32>> {
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
