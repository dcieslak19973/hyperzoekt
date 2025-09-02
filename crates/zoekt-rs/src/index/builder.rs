use crate::types::{DocumentMeta, RepoMeta};
use std::collections::HashMap;
use std::path::PathBuf;
// std::process::Stdio removed (unused)

use super::in_memory::{InMemoryIndex, InMemoryIndexInner, RepoDocId};
use super::process::{process_pending, PendingFile};
use super::utils::*;

use std::path::Path;

/// Helper function to inject credentials into a git URL if available from environment variables.
/// Returns the original URL if no credentials are found or if it's not an HTTPS URL.
fn inject_credentials(url: &str) -> String {
    if !url.starts_with("https://") {
        return url.to_string();
    }

    // Check if credentials are already present (user:pass@host)
    if let Some(at_pos) = url.find('@') {
        if let Some(scheme_end) = url.find("://") {
            let host_part = &url[scheme_end + 3..at_pos];
            if host_part.contains(':') {
                // Credentials already present, return as-is
                return url.to_string();
            }
        }
    }

    // Detect platform and get credentials
    let (username_env, token_env) = if url.contains("github.com") {
        ("GITHUB_USERNAME", "GITHUB_TOKEN")
    } else if url.contains("gitlab.com") {
        ("GITLAB_USERNAME", "GITLAB_TOKEN")
    } else if url.contains("bitbucket.org") {
        ("BITBUCKET_USERNAME", "BITBUCKET_TOKEN")
    } else {
        return url.to_string();
    };

    let username = std::env::var(username_env).ok();
    let token = std::env::var(token_env).ok();

    if let (Some(user), Some(tok)) = (username, token) {
        // Insert credentials into URL: https://user:tok@host/path
        if let Some(host_start) = url.find("://") {
            let host_path = &url[host_start + 3..];
            if let Some(slash_pos) = host_path.find('/') {
                let host = &host_path[..slash_pos];
                let path = &host_path[slash_pos..];
                return format!("https://{}:{}@{}{}", user, tok, host, path);
            } else {
                // No path, just host
                return format!("https://{}:{}@{}", user, tok, host_path);
            }
        }
    }

    url.to_string()
}

/// Attempt a non-interactive `git clone --depth 1 <url> <dst>`.
///
/// `runner` is an optional test hook that receives (url, dst) and returns
/// a simulated `std::process::Output` to ease unit testing.
#[allow(clippy::type_complexity)]
pub(crate) fn try_git_clone_fallback(
    url: &str,
    dst: &Path,
    runner: Option<&dyn Fn(&str, &Path) -> std::io::Result<std::process::Output>>,
) -> Result<(), crate::index::IndexError> {
    let authenticated_url = inject_credentials(url);

    if let Some(r) = runner {
        let output = r(&authenticated_url, dst)
            .map_err(|e| crate::index::IndexError::Other(e.to_string()))?;
        if output.status.success() {
            return Ok(());
        }
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();
        let serr = stderr.trim();
        let serr_l = serr.to_lowercase();
        if serr_l.contains("authentication failed")
            || serr_l.contains("permission denied")
            || serr_l.contains("fatal: could not read")
        {
            return Err(crate::index::IndexError::CloneDenied(serr.to_string()));
        } else if serr_l.contains("not found")
            || serr_l.contains("repository not found")
            || serr.contains("404")
        {
            return Err(crate::index::IndexError::RepoNotFound(serr.to_string()));
        } else {
            return Err(crate::index::IndexError::CloneError(serr.to_string()));
        }
    }

    let mut cmd = std::process::Command::new("git");
    cmd.arg("clone")
        .arg("--depth")
        .arg("1")
        .arg(&authenticated_url)
        .arg(dst);
    cmd.env("GIT_TERMINAL_PROMPT", "0");
    if url.starts_with("git@") || url.starts_with("ssh://") {
        cmd.env("GIT_SSH_COMMAND", "ssh -o BatchMode=yes");
    }
    let output = cmd
        .stderr(std::process::Stdio::piped())
        .output()
        .map_err(|e| {
            crate::index::IndexError::Other(format!("failed to spawn git clone: {}", e))
        })?;
    if output.status.success() {
        return Ok(());
    }
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    let serr = stderr.trim();
    let serr_l = serr.to_lowercase();
    if serr_l.contains("authentication failed")
        || serr_l.contains("permission denied")
        || serr_l.contains("fatal: could not read")
    {
        Err(crate::index::IndexError::CloneDenied(serr.to_string()))
    } else if serr_l.contains("not found")
        || serr_l.contains("repository not found")
        || serr.contains("404")
    {
        Err(crate::index::IndexError::RepoNotFound(serr.to_string()))
    } else {
        Err(crate::index::IndexError::CloneError(serr.to_string()))
    }
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use super::*;
    fn make_output_cmd(stderr: &str, exit_code: i32) -> std::process::Output {
        // Use shell to produce controlled stderr and exit code.
        #[cfg(unix)]
        {
            let cmd = format!("(>&2 echo '{}'); exit {}", stderr, exit_code);
            std::process::Command::new("sh")
                .arg("-c")
                .arg(cmd)
                .output()
                .expect("failed to run shell to simulate git output")
        }
        #[cfg(windows)]
        {
            let cmd = format!(
                "Write-Error '{}' ; exit {}",
                stderr.replace('"', "\""),
                exit_code
            );
            std::process::Command::new("powershell")
                .arg("-Command")
                .arg(cmd)
                .output()
                .expect("failed to run PowerShell to simulate git output")
        }
    }

    #[test]
    fn test_clone_denied_maps_to_clone_denied() {
        let runner = |_: &str, _: &std::path::Path| -> std::io::Result<std::process::Output> {
            Ok(make_output_cmd("Authentication failed", 1))
        };
        let td = tempfile::tempdir().unwrap();
        let res = try_git_clone_fallback("https://example.com/repo.git", td.path(), Some(&runner));
        assert!(matches!(res, Err(crate::index::IndexError::CloneDenied(_))));
    }

    #[test]
    fn test_repo_not_found_maps_to_repo_not_found() {
        let runner = |_: &str, _: &std::path::Path| -> std::io::Result<std::process::Output> {
            Ok(make_output_cmd("Repository not found", 1))
        };
        let td = tempfile::tempdir().unwrap();
        let res = try_git_clone_fallback("https://example.com/repo.git", td.path(), Some(&runner));
        assert!(matches!(
            res,
            Err(crate::index::IndexError::RepoNotFound(_))
        ));
    }

    #[test]
    fn test_inject_credentials_no_env_vars() {
        let result = inject_credentials("https://github.com/user/repo.git");
        assert_eq!(result, "https://github.com/user/repo.git");
    }

    #[test]
    fn test_inject_credentials_ssh_url() {
        let result = inject_credentials("git@github.com:user/repo.git");
        assert_eq!(result, "git@github.com:user/repo.git");
    }

    #[test]
    fn test_inject_credentials_already_has_credentials() {
        let result = inject_credentials("https://existing:creds@github.com/user/repo.git");
        assert_eq!(result, "https://existing:creds@github.com/user/repo.git");
    }

    #[test]
    fn test_inject_credentials_non_matching_host() {
        let result = inject_credentials("https://example.com/user/repo.git");
        assert_eq!(result, "https://example.com/user/repo.git");
    }
}

pub struct IndexBuilder {
    root: PathBuf,
    include: Option<regex::Regex>,
    exclude: Option<regex::Regex>,
    max_file_size: usize,
    follow_symlinks: bool,
    include_hidden: bool,
    branches: Option<Vec<String>>,
    max_branches: usize,
    enable_symbols: bool,
    thread_cap: Option<usize>,
}

impl IndexBuilder {
    pub fn new(root: PathBuf) -> Self {
        Self {
            root,
            include: None,
            exclude: None,
            max_file_size: 1_000_000,
            follow_symlinks: false,
            include_hidden: false,
            branches: None,
            max_branches: 1,
            enable_symbols: true,
            thread_cap: None,
        }
    }

    pub fn max_file_size(mut self, sz: usize) -> Self {
        self.max_file_size = sz;
        self
    }

    pub fn follow_symlinks(mut self, follow: bool) -> Self {
        self.follow_symlinks = follow;
        self
    }

    pub fn include_hidden(mut self, include: bool) -> Self {
        self.include_hidden = include;
        self
    }

    pub fn include_regex(mut self, re: regex::Regex) -> Self {
        self.include = Some(re);
        self
    }

    pub fn branches(mut self, bs: Vec<String>) -> Self {
        self.branches = Some(bs);
        self
    }

    pub fn max_branches(mut self, n: usize) -> Self {
        self.max_branches = n.max(1);
        self
    }

    pub fn exclude_regex(mut self, re: regex::Regex) -> Self {
        self.exclude = Some(re);
        self
    }

    pub fn enable_symbols(mut self, enable: bool) -> Self {
        self.enable_symbols = enable;
        self
    }

    pub fn index_threads(mut self, n: usize) -> Self {
        self.thread_cap = Some(n.max(1));
        self
    }

    pub fn build(self) -> std::result::Result<InMemoryIndex, crate::index::IndexError> {
        // Clone remote git URLs into a tempdir and use that as repo_root.
        let mut repo_root = self.root.clone();
        let root_s = repo_root.to_string_lossy();
        let mut _repo_clone_tempdir: Option<tempfile::TempDir> = None;
        if root_s.starts_with("http://")
            || root_s.starts_with("https://")
            || root_s.starts_with("git@")
            || root_s.starts_with("ssh://")
        {
            let td =
                tempfile::tempdir().map_err(|e| crate::index::IndexError::Other(e.to_string()))?;
            let authenticated_url = inject_credentials(root_s.as_ref());
            // Try libgit2 clone first, fall back to git CLI if libgit2 fails.
            match git2::Repository::clone(&authenticated_url, td.path()) {
                Ok(_) => {
                    repo_root = td.path().to_path_buf();
                    _repo_clone_tempdir = Some(td);
                }
                Err(e) => {
                    // fall back to `git clone --depth 1` but run non-interactively
                    let res = crate::index::builder::try_git_clone_fallback(
                        root_s.as_ref(),
                        td.path(),
                        None,
                    );
                    match res {
                        Ok(()) => {
                            repo_root = td.path().to_path_buf();
                            _repo_clone_tempdir = Some(td);
                        }
                        Err(err) => {
                            return Err(crate::index::IndexError::Other(format!(
                                "libgit2: {} ; fallback: {}",
                                e, err
                            )));
                        }
                    }
                }
            }
        }

        // Repo meta will reflect the list of branches we indexed (or HEAD by default)
        let mut repo_branches: Vec<String> = vec!["HEAD".to_string()];
        if let Some(bs) = &self.branches {
            repo_branches = bs.clone();
        } else if let Some(chosen) =
            crate::index::git::choose_branches(&repo_root, self.max_branches)
        {
            repo_branches = chosen;
        }

        // Derive a friendly repo name. For local paths use the directory
        // basename; for remote URLs prefer the last URL path component
        // (strip trailing `.git` if present).
        let name = {
            let s = self.root.to_string_lossy();
            if s.starts_with("http://")
                || s.starts_with("https://")
                || s.starts_with("git@")
                || s.starts_with("ssh://")
            {
                // take last path segment
                let trimmed = s.trim_end_matches('/');
                let last = trimmed.rsplit('/').next().unwrap_or(trimmed.as_ref());
                let last = last.rsplit(':').next().unwrap_or(last); // handle git@host:owner/repo.git
                last.trim_end_matches(".git").to_string()
            } else {
                repo_root
                    .file_name()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .to_string()
            }
        };

        let repo = RepoMeta {
            name,
            root: repo_root.clone(),
            branches: repo_branches.clone(),
            visibility: crate::types::RepoVisibility::Public, // Default to public for indexing
            owner: None,
            allowed_users: Vec::new(),
            last_commit_sha: None,
        };

        let mut pending: Vec<PendingFile> = Vec::new();
        let mut _branch_tempdirs: Vec<tempfile::TempDir> = Vec::new();
        // keep the repo clone tempdir alive alongside branch tempdirs
        // remember whether we cloned so we can decide to keep content in-memory
        let repo_was_cloned = _repo_clone_tempdir.is_some();
        if let Some(td) = _repo_clone_tempdir {
            _branch_tempdirs.push(td);
        }

        if let Some(bs) = &self.branches {
            for b in bs {
                let td = crate::index::git::extract_branch_to_tempdir(&repo_root, b)
                    .map_err(|e| crate::index::IndexError::Other(e.to_string()))?;

                let mut builder = ignore::WalkBuilder::new(td.path());
                builder.hidden(!self.include_hidden);
                builder.follow_links(self.follow_symlinks);
                builder.git_ignore(true);
                let walker = builder.build();
                for result in walker
                    .filter_map(Result::ok)
                    .filter(|e| e.file_type().map(|t| t.is_file()).unwrap_or(false))
                {
                    let rel = pathdiff::diff_paths(result.path(), td.path())
                        .unwrap_or_else(|| PathBuf::from(result.file_name()));
                    if let Some(inc) = &self.include {
                        if !inc.is_match(rel.to_string_lossy().as_ref()) {
                            continue;
                        }
                    }
                    if let Some(exc) = &self.exclude {
                        if exc.is_match(rel.to_string_lossy().as_ref()) {
                            continue;
                        }
                    }
                    let size = result.metadata().map(|m| m.len()).unwrap_or(0) as usize;
                    if size > self.max_file_size {
                        continue;
                    }
                    let lang = detect_lang_from_ext(&rel);
                    pending.push(PendingFile {
                        rel,
                        size,
                        lang,
                        branches: vec![b.clone()],
                        base: Some(td.path().to_path_buf()),
                    });
                }
                _branch_tempdirs.push(td);
            }
        } else {
            let mut builder = ignore::WalkBuilder::new(&repo_root);
            builder.hidden(!self.include_hidden);
            builder.follow_links(self.follow_symlinks);
            builder.git_ignore(true);
            let mut ignore_patterns: Vec<String> = Vec::new();
            if let Ok(gitignore_content) = std::fs::read_to_string(repo_root.join(".gitignore")) {
                for line in gitignore_content.lines() {
                    let pat = line.trim();
                    if pat.is_empty() || pat.starts_with('#') {
                        continue;
                    }
                    ignore_patterns.push(pat.to_string());
                }
            }
            let walker = builder.build();
            for result in walker
                .filter_map(Result::ok)
                .filter(|e| e.file_type().map(|t| t.is_file()).unwrap_or(false))
            {
                let rel = pathdiff::diff_paths(result.path(), &repo_root)
                    .unwrap_or_else(|| PathBuf::from(result.file_name()));
                if !ignore_patterns.is_empty() {
                    let rel_s = rel.to_string_lossy();
                    let base = result
                        .path()
                        .file_name()
                        .map(|s| s.to_string_lossy().to_string());
                    let mut skipped = false;
                    for pat in &ignore_patterns {
                        if pat.starts_with('/') {
                            let p = pat.trim_start_matches('/');
                            if rel_s == p {
                                skipped = true;
                                break;
                            }
                        } else if let Some(b) = &base {
                            if b == pat {
                                skipped = true;
                                break;
                            }
                        }
                    }
                    if skipped {
                        continue;
                    }
                }
                if let Some(inc) = &self.include {
                    if !inc.is_match(rel.to_string_lossy().as_ref()) {
                        continue;
                    }
                }
                if let Some(exc) = &self.exclude {
                    if exc.is_match(rel.to_string_lossy().as_ref()) {
                        continue;
                    }
                }
                let size = result.metadata().map(|m| m.len()).unwrap_or(0) as usize;
                if size > self.max_file_size {
                    continue;
                }
                let lang = detect_lang_from_ext(&rel);
                pending.push(PendingFile {
                    rel,
                    size,
                    lang,
                    branches: vec!["HEAD".to_string()],
                    base: if repo_was_cloned {
                        Some(repo_root.clone())
                    } else {
                        None
                    },
                });
            }
        }

        let enable_symbols = self.enable_symbols;
        let root = repo_root.clone();
        let processed = process_pending(pending, root.clone(), enable_symbols, self.thread_cap);

        let mut processed = processed;
        processed.sort_by_key(|p| p.idx);

        let mut docs: Vec<DocumentMeta> = Vec::with_capacity(processed.len());
        let mut doc_contents: Vec<Option<String>> = Vec::with_capacity(processed.len());
        let mut terms: HashMap<String, Vec<RepoDocId>> = HashMap::new();
        let mut symbol_terms: HashMap<String, Vec<RepoDocId>> = HashMap::new();
        let mut symbol_trigrams: HashMap<[u8; 3], Vec<RepoDocId>> = HashMap::new();

        for (i, p) in processed.into_iter().enumerate() {
            let doc_id = i as RepoDocId;
            if let Some(ref text) = p.content {
                let mut seen: fnv::FnvHashSet<String> = fnv::FnvHashSet::default();
                for tok in text.split(|c: char| !c.is_alphanumeric() && c != '_') {
                    if tok.is_empty() {
                        continue;
                    }
                    seen.insert(tok.to_lowercase());
                }
                for tok in seen.into_iter() {
                    terms.entry(tok).or_default().push(doc_id);
                }
            }

            if enable_symbols {
                for key in p.sym_names {
                    symbol_terms.entry(key).or_default().push(doc_id);
                }
                for sym in &p.doc.symbols {
                    for tri in crate::trigram::trigrams(&sym.name) {
                        symbol_trigrams.entry(tri).or_default().push(doc_id);
                    }
                }
            }
            docs.push(p.doc);
            if p.keep_content {
                doc_contents.push(p.content);
            } else {
                doc_contents.push(None);
            }
        }

        for (_k, v) in symbol_terms.iter_mut() {
            v.sort_unstable();
            v.dedup();
        }
        for (_k, v) in symbol_trigrams.iter_mut() {
            v.sort_unstable();
            v.dedup();
        }

        let inner = InMemoryIndexInner {
            repo,
            docs,
            terms,
            symbol_terms,
            symbol_trigrams,
            doc_contents,
        };
        Ok(InMemoryIndex::from_inner(inner))
    }
}
