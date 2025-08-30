use crate::types::DocumentMeta;
use std::path::PathBuf;

/// Pending file metadata gathered during scanning.
#[derive(Debug, Clone)]
pub(crate) struct PendingFile {
    pub rel: PathBuf,
    pub size: usize,
    pub lang: Option<String>,
    pub branches: Vec<String>,
    pub base: Option<PathBuf>,
}

#[derive(Debug)]
pub(crate) struct ProcessedDoc {
    pub idx: usize,
    pub doc: DocumentMeta,
    pub content: Option<String>,
    pub sym_names: Vec<String>,
    pub keep_content: bool,
}

/// Process a list of `PendingFile` entries into `ProcessedDoc` using the
/// same parallel pipeline previously embedded in `builder.rs`.
pub(crate) fn process_pending(
    pending: Vec<PendingFile>,
    root: PathBuf,
    enable_symbols: bool,
    thread_cap: Option<usize>,
) -> Vec<ProcessedDoc> {
    use rayon::prelude::*;
    use rayon::ThreadPoolBuilder;

    let avail = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    let default_cap = std::cmp::min(avail, 8).max(1);
    let env_cap = std::env::var("ZOEKT_INDEX_THREADS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .map(|n| n.max(1));
    let cap = thread_cap
        .or(env_cap)
        .unwrap_or(default_cap)
        .min(avail)
        .max(1);
    if let Ok(pool) = ThreadPoolBuilder::new().num_threads(cap).build() {
        pool.install(|| {
            pending
                .par_iter()
                .enumerate()
                .map(|(idx, pf)| {
                    let fullp = match &pf.base {
                        Some(base) => base.join(&pf.rel),
                        None => root.join(&pf.rel),
                    };
                    let bytes = std::fs::read(&fullp).ok();
                    let mut doc = DocumentMeta {
                        path: pf.rel.clone(),
                        lang: pf.lang.clone(),
                        size: bytes.as_ref().map(|b| b.len()).unwrap_or(pf.size) as u64,
                        branches: pf.branches.clone(),
                        symbols: Vec::new(),
                    };
                    let mut symbol_terms: Vec<String> = Vec::new();
                    let mut content: Option<String> = None;
                    if let Some(b) = &bytes {
                        let text = String::from_utf8_lossy(b).into_owned();
                        if crate::index::utils::is_text(b) {
                            content = Some(text.clone());
                        }
                        if enable_symbols {
                            doc.symbols = crate::index::utils::extract_symbols(
                                &text,
                                doc.path.extension().and_then(|s| s.to_str()).unwrap_or(""),
                            );
                            symbol_terms =
                                doc.symbols.iter().map(|s| s.name.to_lowercase()).collect();
                        }
                    }
                    let keep = pf.base.is_some();
                    ProcessedDoc {
                        idx,
                        doc,
                        content,
                        sym_names: symbol_terms,
                        keep_content: keep,
                    }
                })
                .collect()
        })
    } else {
        pending
            .into_par_iter()
            .enumerate()
            .map(|(idx, pf)| {
                let fullp = match &pf.base {
                    Some(base) => base.join(&pf.rel),
                    None => root.join(&pf.rel),
                };
                let bytes = std::fs::read(&fullp).ok();
                let mut doc = DocumentMeta {
                    path: pf.rel.clone(),
                    lang: pf.lang.clone(),
                    size: bytes.as_ref().map(|b| b.len()).unwrap_or(pf.size) as u64,
                    branches: pf.branches.clone(),
                    symbols: Vec::new(),
                };
                let mut symbol_terms: Vec<String> = Vec::new();
                let mut content: Option<String> = None;
                if let Some(b) = &bytes {
                    let text = String::from_utf8_lossy(b).into_owned();
                    if crate::index::utils::is_text(b) {
                        content = Some(text.clone());
                    }
                    if enable_symbols {
                        doc.symbols = crate::index::utils::extract_symbols(
                            &text,
                            doc.path.extension().and_then(|s| s.to_str()).unwrap_or(""),
                        );
                        symbol_terms = doc.symbols.iter().map(|s| s.name.to_lowercase()).collect();
                    }
                }
                let keep = pf.base.is_some();
                ProcessedDoc {
                    idx,
                    doc,
                    content,
                    sym_names: symbol_terms,
                    keep_content: keep,
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use std::path::PathBuf;
    use tempfile::tempdir;

    #[test]
    fn test_process_pending_happy_path() {
        let td = tempdir().unwrap();
        let base = td.path().to_path_buf();
        let rel = PathBuf::from("foo.txt");
        let content = b"hello world\nfn main() {}";
        let mut f = File::create(td.path().join(&rel)).unwrap();
        f.write_all(content).unwrap();

        let pending = vec![PendingFile {
            rel: rel.clone(),
            size: content.len(),
            lang: Some("rs".to_string()),
            branches: vec!["HEAD".to_string()],
            base: Some(base.clone()),
        }];

        let processed = process_pending(pending, base, false, Some(1));
        assert_eq!(processed.len(), 1);
        let p = &processed[0];
        assert_eq!(p.doc.path, rel);
        assert!(p.content.is_some(), "text file should yield content");
    }

    #[test]
    fn test_process_pending_binary_file() {
        let td = tempdir().unwrap();
        let base = td.path().to_path_buf();
        let rel = PathBuf::from("bin.dat");
        // include NUL bytes so is_text returns false
        let content = [0u8, 1, 2, 0, 3, 4];
        let mut f = File::create(td.path().join(&rel)).unwrap();
        f.write_all(&content).unwrap();

        let pending = vec![PendingFile {
            rel: rel.clone(),
            size: content.len(),
            lang: None,
            branches: vec!["HEAD".to_string()],
            base: Some(base.clone()),
        }];

        let processed = process_pending(pending, base, false, Some(1));
        assert_eq!(processed.len(), 1);
        let p = &processed[0];
        assert_eq!(p.doc.path, rel);
        assert!(p.content.is_none(), "binary file should not keep content");
    }
}
