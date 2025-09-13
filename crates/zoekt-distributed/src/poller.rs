use anyhow::Result;
use once_cell::sync::Lazy;
use serde_json::Value;
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::watch;

use crate::redis_adapter::{create_redis_pool, DynRedis, RealRedis};

fn glob_to_regex(pat: &str) -> String {
    let mut regex_pattern = String::new();
    regex_pattern.push('^');
    // NOTE: The glob -> regex conversion implemented by `glob_to_regex` is
    // duplicated later in this file inside `matches_pattern`. Consider extracting
    // a single shared helper to avoid code duplication and keep behaviour
    // consistent across the module (and to make future changes easier).
    for ch in pat.chars() {
        match ch {
            '*' => regex_pattern.push_str(".*"),
            '?' => regex_pattern.push('.'),
            '[' => regex_pattern.push('['),
            ']' => regex_pattern.push(']'),
            '^' => regex_pattern.push_str("\\^"),
            '$' => regex_pattern.push_str("\\$"),
            '.' => regex_pattern.push_str("\\."),
            '+' => regex_pattern.push_str("\\+"),
            '|' => regex_pattern.push_str("\\|"),
            '(' => regex_pattern.push_str("\\("),
            ')' => regex_pattern.push_str("\\)"),
            '{' => regex_pattern.push_str("\\{"),
            '}' => regex_pattern.push_str("\\}"),
            '\\' => regex_pattern.push_str("\\\\"),
            _ => regex_pattern.push(ch),
        }
    }
    regex_pattern.push('$');
    regex_pattern
}

fn parse_patterns_from_value(v: &Option<Value>) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    if let Some(val) = v {
        if val.is_array() {
            if let Some(arr) = val.as_array() {
                for it in arr {
                    if let Some(s) = it.as_str() {
                        if !s.trim().is_empty() {
                            out.push(s.trim().to_string());
                        }
                    }
                }
            }
        } else if val.is_string() {
            if let Some(s) = val.as_str() {
                for part in s.split(',') {
                    let p = part.trim();
                    if !p.is_empty() {
                        out.push(p.to_string());
                    }
                }
            }
        }
    }
    out
}

/// Preview builder: returns matching branches and tags (tags limited by max_refs)
#[allow(clippy::too_many_arguments)]
pub async fn preview_builder(
    base_repo: &str,
    include_branches_v: Option<Value>,
    exclude_branches_v: Option<Value>,
    include_tags_v: Option<Value>,
    exclude_tags_v: Option<Value>,
    include_owner_v: Option<Value>,
    exclude_owner_v: Option<Value>,
    max_refs: usize,
) -> Result<serde_json::Value, String> {
    // fetch remote refs
    // before fetching, apply owner include/exclude filters if present
    let include_owner = parse_patterns_from_value(&include_owner_v);
    let exclude_owner = parse_patterns_from_value(&exclude_owner_v);
    let compile_owner = |patterns: &Vec<String>| -> Vec<regex::Regex> {
        let mut out: Vec<regex::Regex> = Vec::new();
        for p in patterns.iter() {
            let pat = if p.contains('*') || p.contains('?') || p.contains('[') {
                glob_to_regex(p)
            } else {
                p.clone()
            };
            if let Ok(r) = regex::Regex::new(&pat) {
                out.push(r);
            }
        }
        out
    };
    let inc_o = compile_owner(&include_owner);
    let exc_o = compile_owner(&exclude_owner);

    // parse owner from base_repo and apply filters
    let owner = parse_owner_from_git_url(base_repo).unwrap_or_default();
    let owner_matches = |name: &str, inc: &Vec<regex::Regex>, exc: &Vec<regex::Regex>| -> bool {
        for ex in exc {
            if ex.is_match(name) {
                return false;
            }
        }
        if inc.is_empty() {
            return true;
        }
        for r in inc {
            if r.is_match(name) {
                return true;
            }
        }
        false
    };
    if !owner_matches(&owner, &inc_o, &exc_o) {
        // owner filters exclude this base_repo
        return Ok(serde_json::json!({"branches": [], "tags": []}));
    }

    let branches = fetch_remote_branches(base_repo).await?;
    let tags = fetch_remote_tags(base_repo).await?;

    Ok(preview_from_refs(
        branches,
        tags,
        include_branches_v,
        exclude_branches_v,
        include_tags_v,
        exclude_tags_v,
        max_refs,
    ))
}

fn parse_owner_from_git_url(git_url: &str) -> Option<String> {
    // Extract the owner path portion from a git URL. The owner is defined as
    // everything in the repository path except the final segment (the repo
    // name). This supports multi-segment owners such as `org/suborg`.
    //
    // Examples:
    // - git@github.com:org/repo.git -> Some("org")
    // - git@github.com:org/suborg/repo.git -> Some("org/suborg")
    // - https://host/owner/repo.git -> Some("owner")
    // - ssh://git@host/owner/sub/repo.git -> Some("owner/sub")

    // Handle scp-like syntax: git@host:owner/repo.git
    if git_url.starts_with("git@") {
        if let Some(idx) = git_url.find(':') {
            let path = &git_url[idx + 1..];
            let path = path.trim_end_matches(".git");
            let parts: Vec<&str> = path.split('/').collect();
            if parts.len() >= 2 {
                // owner = all but the last segment
                let owner = parts[..parts.len() - 1].join("/");
                return Some(owner);
            }
        }
    }

    // For URLs like https://host/owner/repo.git or ssh://git@host/owner/repo.git
    let after_scheme = if let Some(idx) = git_url.find("://") {
        &git_url[idx + 3..]
    } else {
        git_url
    };
    let path = if let Some(pos) = after_scheme.find('/') {
        &after_scheme[pos + 1..]
    } else {
        after_scheme
    };
    let path = path.trim_end_matches(".git");
    let parts: Vec<&str> = path.split('/').collect();
    if parts.len() >= 2 {
        let owner = parts[..parts.len() - 1].join("/");
        return Some(owner);
    }
    None
}

/// Sort tags semver-first and apply max limit. Returns tags in ascending order for client readability.
fn sort_and_limit_tags(mut tags: Vec<String>, max_tags: usize) -> Vec<String> {
    tags.dedup();
    // Try semver-aware sorting: parse tags (strip leading 'v') as semver
    let mut parsed: Vec<(semver::Version, String)> = Vec::new();
    let mut unparsed: Vec<String> = Vec::new();
    for t in tags.into_iter() {
        let s = t.clone();
        let naked = s.strip_prefix('v').unwrap_or(&s);
        match semver::Version::parse(naked) {
            Ok(ver) => parsed.push((ver, s)),
            Err(_) => unparsed.push(s),
        }
    }
    // sort parsed descending by version
    parsed.sort_by(|a, b| b.0.cmp(&a.0));
    // sort unparsed lexicographically descending as a heuristic
    unparsed.sort();
    unparsed.reverse();

    // build final ordered list: parsed (newest first) then unparsed (newest-heuristic)
    let mut ordered: Vec<String> = Vec::new();
    for (_ver, s) in parsed.iter() {
        ordered.push(s.clone());
    }
    for s in unparsed.iter() {
        ordered.push(s.clone());
    }

    if ordered.len() > max_tags {
        ordered.truncate(max_tags);
    }
    // return ascending order for client readability
    ordered.reverse();
    ordered
}

fn preview_from_refs(
    branches: Vec<String>,
    tags: Vec<String>,
    include_branches_v: Option<Value>,
    exclude_branches_v: Option<Value>,
    include_tags_v: Option<Value>,
    exclude_tags_v: Option<Value>,
    max_refs: usize,
) -> serde_json::Value {
    let include_branches = parse_patterns_from_value(&include_branches_v);
    let exclude_branches = parse_patterns_from_value(&exclude_branches_v);
    let include_tags = parse_patterns_from_value(&include_tags_v);
    let exclude_tags = parse_patterns_from_value(&exclude_tags_v);

    let compile = |patterns: &Vec<String>| -> Vec<regex::Regex> {
        let mut out: Vec<regex::Regex> = Vec::new();
        for p in patterns.iter() {
            let pat = if p.contains('*') || p.contains('?') || p.contains('[') {
                glob_to_regex(p)
            } else {
                p.clone()
            };
            if let Ok(r) = regex::Regex::new(&pat) {
                out.push(r);
            }
        }
        out
    };

    let inc_b = compile(&include_branches);
    let exc_b = compile(&exclude_branches);
    let inc_t = compile(&include_tags);
    let exc_t = compile(&exclude_tags);

    let matches_any = |name: &str, inc: &Vec<regex::Regex>, exc: &Vec<regex::Regex>| -> bool {
        for ex in exc {
            if ex.is_match(name) {
                return false;
            }
        }
        if inc.is_empty() {
            return true;
        }
        for r in inc {
            if r.is_match(name) {
                return true;
            }
        }
        false
    };

    let mut matched_branches: Vec<String> = Vec::new();
    for b in branches.iter() {
        if matches_any(b, &inc_b, &exc_b) {
            matched_branches.push(b.clone());
        }
    }

    let mut matched_tags: Vec<String> = Vec::new();
    for t in tags.iter() {
        if matches_any(t, &inc_t, &exc_t) {
            matched_tags.push(t.clone());
        }
    }

    let final_tags = sort_and_limit_tags(matched_tags, max_refs);

    serde_json::json!({"branches": matched_branches, "tags": final_tags})
}

/// Simple poller metrics accessible to the admin poller binary for /metrics
pub struct PollerMetricsSnapshot {
    pub polls_run: u64,
    pub polls_failed: u64,
    pub last_poll_unix: u64,
}

struct PollerMetrics {
    polls_run: AtomicU64,
    polls_failed: AtomicU64,
    last_poll_unix: AtomicU64,
}

impl PollerMetrics {
    const fn new() -> Self {
        Self {
            polls_run: AtomicU64::new(0),
            polls_failed: AtomicU64::new(0),
            last_poll_unix: AtomicU64::new(0),
        }
    }
}

static METRICS: Lazy<PollerMetrics> = Lazy::new(PollerMetrics::new);

pub fn snapshot_metrics() -> PollerMetricsSnapshot {
    PollerMetricsSnapshot {
        polls_run: METRICS.polls_run.load(Ordering::SeqCst),
        polls_failed: METRICS.polls_failed.load(Ordering::SeqCst),
        last_poll_unix: METRICS.last_poll_unix.load(Ordering::SeqCst),
    }
}

/// Poller configuration
#[derive(Clone, Debug)]
pub struct PollerConfig {
    pub poll_interval: Duration,
}

async fn fetch_remote_branches(url: &str) -> Result<Vec<String>, String> {
    // Run `git ls-remote --heads <url>` and parse refs
    let mut cmd = Command::new("git");
    cmd.args(["ls-remote", "--heads", url]);
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::null());

    let mut child = cmd
        .spawn()
        .map_err(|e| format!("failed to spawn git: {}", e))?;
    let out = child
        .stdout
        .take()
        .ok_or_else(|| "git produced no stdout".to_string())?;

    let mut reader = BufReader::new(out);
    let mut s = String::new();
    reader
        .read_to_string(&mut s)
        .await
        .map_err(|e| format!("failed to read git output: {}", e))?;

    let _ = child
        .wait()
        .await
        .map_err(|e| format!("git wait failed: {}", e))?;

    let mut branches = Vec::new();
    for line in s.lines() {
        if let Some(refpart) = line.split('\t').nth(1) {
            if refpart.starts_with("refs/heads/") {
                let br = refpart.trim_start_matches("refs/heads/").to_string();
                branches.push(br);
            }
        }
    }
    Ok(branches)
}

async fn fetch_remote_tags(url: &str) -> Result<Vec<String>, String> {
    // Run `git ls-remote --tags <url>` and parse refs
    let mut cmd = Command::new("git");
    cmd.args(["ls-remote", "--tags", url]);
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::null());

    let mut child = cmd
        .spawn()
        .map_err(|e| format!("failed to spawn git: {}", e))?;
    let out = child
        .stdout
        .take()
        .ok_or_else(|| "git produced no stdout".to_string())?;

    let mut reader = BufReader::new(out);
    let mut s = String::new();
    reader
        .read_to_string(&mut s)
        .await
        .map_err(|e| format!("failed to read git output: {}", e))?;

    let _ = child
        .wait()
        .await
        .map_err(|e| format!("git wait failed: {}", e))?;

    let mut tags = Vec::new();
    for line in s.lines() {
        if let Some(refpart) = line.split('\t').nth(1) {
            if refpart.starts_with("refs/tags/") {
                // strip possible ^{} suffix that appears for annotated tags
                let mut t = refpart.trim_start_matches("refs/tags/").to_string();
                if let Some(idx) = t.find("^{}") {
                    t.truncate(idx);
                }
                tags.push(t);
            }
        }
    }
    tags.sort();
    tags.dedup();
    Ok(tags)
}

fn matches_pattern(pattern: &str, branch: &str) -> bool {
    // Convert glob to regex similar to existing helper
    let mut regex_pattern = String::new();
    regex_pattern.push('^');

    for ch in pattern.chars() {
        match ch {
            '*' => regex_pattern.push_str(".*"),
            '?' => regex_pattern.push('.'),
            '[' => regex_pattern.push('['),
            ']' => regex_pattern.push(']'),
            '^' => regex_pattern.push_str("\\^"),
            '$' => regex_pattern.push_str("\\$"),
            '.' => regex_pattern.push_str("\\."),
            '+' => regex_pattern.push_str("\\+"),
            '|' => regex_pattern.push_str("\\|"),
            '(' => regex_pattern.push_str("\\("),
            ')' => regex_pattern.push_str("\\)"),
            '{' => regex_pattern.push_str("\\{"),
            '}' => regex_pattern.push_str("\\}"),
            '\\' => regex_pattern.push_str("\\\\"),
            _ => regex_pattern.push(ch),
        }
    }
    regex_pattern.push('$');
    match regex::Regex::new(&regex_pattern) {
        Ok(re) => re.is_match(branch),
        Err(_) => false,
    }
}

async fn expand_pattern(url: &str, pattern: &str) -> Result<Vec<String>, String> {
    if pattern.trim().is_empty() {
        return Ok(vec!["main".to_string()]);
    }
    let branches = fetch_remote_branches(url).await?;
    let mut expanded = Vec::new();
    for part in pattern.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        if part.contains('*') || part.contains('?') || part.contains('[') {
            for b in &branches {
                if matches_pattern(part, b) {
                    expanded.push(b.clone());
                }
            }
        } else {
            // exact name: include even if not present (indexer will fail later)
            expanded.push(part.to_string());
        }
    }
    if expanded.is_empty() {
        Ok(vec!["main".to_string()])
    } else {
        expanded.sort();
        expanded.dedup();
        Ok(expanded)
    }
}

pub async fn run_poller(mut shutdown_rx: watch::Receiver<()>, cfg: PollerConfig) -> Result<()> {
    // Create redis connection
    let pool_opt = create_redis_pool();
    let pool_arc: Option<Arc<dyn DynRedis>> =
        pool_opt.map(|p| Arc::new(RealRedis { pool: p }) as Arc<dyn DynRedis>);

    if pool_arc.is_none() {
        tracing::warn!("poller: no redis connection available, exiting");
        return Ok(());
    }
    let pool = pool_arc.unwrap();

    tracing::info!("poller: starting with interval={:?}", cfg.poll_interval);

    // Initialize metrics
    METRICS.polls_run.store(0, Ordering::SeqCst);
    METRICS.polls_failed.store(0, Ordering::SeqCst);

    loop {
        // shutdown check
        if shutdown_rx.has_changed().unwrap_or(false) {
            tracing::info!("poller: shutdown requested");
            break;
        }

        // Read repo list and metadata
        // increment polls_run counter (this is a poll attempt)
        METRICS.polls_run.fetch_add(1, Ordering::SeqCst);
        let now_unix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        METRICS.last_poll_unix.store(now_unix, Ordering::SeqCst);

        match (*pool).hgetall("zoekt:repos").await {
            Ok(entries) => {
                for (name, url) in entries {
                    // Read meta for branch pattern
                    let meta_json = (*pool).hget("zoekt:repo_meta", &name).await.ok().flatten();
                    let mut pattern = "main".to_string();
                    if let Some(mj) = meta_json {
                        if let Ok(v) = serde_json::from_str::<Value>(&mj) {
                            if let Some(b) = v.get("branches") {
                                if !b.is_null() {
                                    if let Some(s) = b.as_str() {
                                        pattern = s.to_string();
                                    } else {
                                        pattern = b.to_string();
                                    }
                                }
                            }
                        }
                    }

                    // Expand pattern and write zoekt:repo_branches entries
                    match expand_pattern(&url, &pattern).await {
                        Ok(expanded) => {
                            let branch_map_key = "zoekt:repo_branches";
                            for b in expanded.iter() {
                                let field = format!("{}|{}", name, b);
                                if let Err(e) = pool.hset(branch_map_key, &field, &url).await {
                                    tracing::warn!(repo=%name, branch=%b, error=?e, "poller: failed to write branch entry");
                                } else {
                                    tracing::debug!(repo=%name, branch=%b, "poller: wrote branch entry");
                                }
                            }
                        }
                        Err(e) => {
                            tracing::warn!(repo=%name, error=%e, "poller: failed to expand branches");
                            METRICS.polls_failed.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                }
            }
            Err(e) => tracing::warn!(error=?e, "poller: failed to read zoekt:repos"),
        }

        // Evaluate simple "repo builders" stored in Redis hash `zoekt:repo_builders`.
        // Each field is expected to be JSON with a minimal schema:
        // { "id": "builder-name", "base_repo": "git@... or https://...", "include_branches": ["regex"], "exclude_branches": ["regex"], "include_tags": ["regex"], "exclude_tags": ["regex"], "default_branch": "main" }
        match (*pool).hgetall("zoekt:repo_builders").await {
            Ok(builders) => {
                // collect current builder ids for cleanup logic
                let mut current_ids: Vec<String> = Vec::new();
                for (bid, bj) in &builders {
                    if let Ok(v) = serde_json::from_str::<Value>(bj.as_str()) {
                        let id = v
                            .get("id")
                            .and_then(|x| x.as_str())
                            .unwrap_or(bid)
                            .to_string();
                        current_ids.push(id);
                    } else {
                        current_ids.push(bid.clone());
                    }
                }

                tracing::debug!(builder_count = builders.len(), current_ids = ?current_ids, redis_url = ?std::env::var("REDIS_URL"), "poller: read zoekt:repo_builders");

                // cleanup tombstoned/removed builders: any builder:: prefix not in current_ids will be removed
                // extracted into helper so tests can validate cleanup without running the full poller
                let _ = cleanup_builder_entries(pool.clone()).await;

                for (bid, bj) in builders {
                    let parsed = serde_json::from_str::<Value>(bj.as_str()).ok();
                    if parsed.is_none() {
                        tracing::warn!(builder=%bid, "poller: invalid json for repo_builder");
                        continue;
                    }
                    let v = parsed.unwrap();
                    let id = v
                        .get("id")
                        .and_then(|x| x.as_str())
                        .unwrap_or(bid.as_str())
                        .to_string();
                    let base_repo = v
                        .get("base_repo")
                        .and_then(|x| x.as_str())
                        .unwrap_or("")
                        .to_string();
                    if base_repo.is_empty() {
                        tracing::warn!(builder=%id, "poller: repo_builder missing base_repo");
                        continue;
                    }

                    // Collect include/exclude regex lists
                    let include_branches: Vec<String> = v
                        .get("include_branches")
                        .map(|x| parse_patterns_from_value(&Some(x.clone())))
                        .unwrap_or_default();
                    let exclude_branches: Vec<String> = v
                        .get("exclude_branches")
                        .map(|x| parse_patterns_from_value(&Some(x.clone())))
                        .unwrap_or_default();
                    let include_owner: Vec<String> = v
                        .get("include_owner")
                        .map(|x| parse_patterns_from_value(&Some(x.clone())))
                        .unwrap_or_default();
                    let exclude_owner: Vec<String> = v
                        .get("exclude_owner")
                        .map(|x| parse_patterns_from_value(&Some(x.clone())))
                        .unwrap_or_default();
                    let include_tags: Vec<String> = v
                        .get("include_tags")
                        .map(|x| parse_patterns_from_value(&Some(x.clone())))
                        .unwrap_or_default();
                    let exclude_tags: Vec<String> = v
                        .get("exclude_tags")
                        .map(|x| parse_patterns_from_value(&Some(x.clone())))
                        .unwrap_or_default();
                    let default_branch = v
                        .get("default_branch")
                        .and_then(|x| x.as_str())
                        .unwrap_or("main")
                        .to_string();
                    let max_tags =
                        v.get("max_tags").and_then(|x| x.as_u64()).unwrap_or(200) as usize;

                    // pre-compile regex lists
                    let compile_list = |arr: &Vec<String>| -> Vec<regex::Regex> {
                        let mut out: Vec<regex::Regex> = Vec::new();
                        for p in arr.iter() {
                            let pat = if p.contains('*') || p.contains('?') || p.contains('[') {
                                glob_to_regex(p)
                            } else {
                                p.clone()
                            };
                            if let Ok(r) = regex::Regex::new(&pat) {
                                out.push(r);
                            } else {
                                tracing::warn!(builder=%id, pattern=%p, "poller: invalid regex in builder; skipping pattern");
                            }
                        }
                        out
                    };

                    let inc_b = compile_list(&include_branches);
                    let exc_b = compile_list(&exclude_branches);
                    let inc_o = compile_list(&include_owner);
                    let exc_o = compile_list(&exclude_owner);
                    let inc_t = compile_list(&include_tags);
                    let exc_t = compile_list(&exclude_tags);

                    let matches_any =
                        |name: &str, inc: &Vec<regex::Regex>, exc: &Vec<regex::Regex>| -> bool {
                            for ex in exc {
                                if ex.is_match(name) {
                                    return false;
                                }
                            }
                            if inc.is_empty() {
                                return true;
                            }
                            for r in inc {
                                if r.is_match(name) {
                                    return true;
                                }
                            }
                            false
                        };

                    // fetch remote refs
                    let branches = match fetch_remote_branches(&base_repo).await {
                        Ok(b) => b,
                        Err(e) => {
                            tracing::warn!(builder=%id, error=%e, "poller: failed to fetch branches for builder");
                            continue;
                        }
                    };
                    // check owner include/exclude filters against parsed owner
                    let owner = parse_owner_from_git_url(&base_repo).unwrap_or_default();
                    let owner_matches =
                        |name: &str, inc: &Vec<regex::Regex>, exc: &Vec<regex::Regex>| -> bool {
                            for ex in exc {
                                if ex.is_match(name) {
                                    return false;
                                }
                            }
                            if inc.is_empty() {
                                return true;
                            }
                            for r in inc {
                                if r.is_match(name) {
                                    return true;
                                }
                            }
                            false
                        };
                    if !owner_matches(&owner, &inc_o, &exc_o) {
                        tracing::debug!(builder=%id, owner=%owner, "poller: owner filters exclude this base_repo");
                        continue;
                    }
                    let tags = match fetch_remote_tags(&base_repo).await {
                        Ok(t) => t,
                        Err(e) => {
                            tracing::warn!(builder=%id, error=%e, "poller: failed to fetch tags for builder");
                            Vec::new()
                        }
                    };

                    // produce entries for matching branches
                    for b in branches.iter() {
                        if !matches_any(b, &inc_b, &exc_b) {
                            continue;
                        }
                        let field = format!("builder::{}|{}", id, b);
                        if let Err(e) = pool.hset("zoekt:repo_branches", &field, &base_repo).await {
                            tracing::warn!(builder=%id, branch=%b, error=?e, "poller: failed to write builder branch entry");
                        } else {
                            let meta = serde_json::json!({
                                "base_repo": base_repo,
                                "origin_builder": id,
                                "ref": b,
                                "kind": "branch",
                                "default_branch": default_branch,
                            });
                            let _ = pool
                                .hset("zoekt:repo_branch_meta", &field, &meta.to_string())
                                .await;
                        }
                    }

                    // produce entries for matching tags, but limit to max_tags newest
                    let mut matched_tags: Vec<String> = Vec::new();
                    for t in tags.iter() {
                        if matches_any(t, &inc_t, &exc_t) {
                            matched_tags.push(t.clone());
                        }
                    }
                    // deduplicate
                    matched_tags.dedup();
                    // Try semver-aware sorting: parse tags (strip leading 'v') as semver
                    // Collect parsed and unparsed separately
                    let mut parsed: Vec<(semver::Version, String)> = Vec::new();
                    let mut unparsed: Vec<String> = Vec::new();
                    for t in matched_tags.into_iter() {
                        let s = t.clone();
                        let naked = s.strip_prefix('v').unwrap_or(&s);
                        match semver::Version::parse(naked) {
                            Ok(ver) => parsed.push((ver, s)),
                            Err(_) => unparsed.push(s),
                        }
                    }
                    // sort parsed descending by version
                    parsed.sort_by(|a, b| b.0.cmp(&a.0));
                    // sort unparsed lexicographically descending as a heuristic
                    unparsed.sort();
                    unparsed.reverse();

                    // build final ordered list: parsed (newest first) then unparsed (newest-heuristic)
                    let mut ordered: Vec<String> = Vec::new();
                    for (_ver, s) in parsed.iter() {
                        ordered.push(s.clone());
                    }
                    for s in unparsed.iter() {
                        ordered.push(s.clone());
                    }

                    if ordered.len() > max_tags {
                        ordered.truncate(max_tags);
                    }
                    // store back into matched_tags in ascending order for client readability
                    ordered.reverse();
                    matched_tags = ordered;
                    for t in matched_tags.iter() {
                        let field = format!("builder::{}|{}", id, t);
                        if let Err(e) = pool.hset("zoekt:repo_branches", &field, &base_repo).await {
                            tracing::warn!(builder=%id, tag=%t, error=?e, "poller: failed to write builder tag entry");
                        } else {
                            let meta = serde_json::json!({
                                "base_repo": base_repo,
                                "origin_builder": id,
                                "ref": t,
                                "kind": "tag",
                                "default_branch": default_branch,
                            });
                            let _ = pool
                                .hset("zoekt:repo_branch_meta", &field, &meta.to_string())
                                .await;
                        }
                    }
                }
            }
            Err(e) => {
                tracing::debug!(error=?e, "poller: failed to read zoekt:repo_builders (ok if none)")
            }
        }

        // Wait or exit early on shutdown
        tokio::select! {
            _ = tokio::time::sleep(cfg.poll_interval) => {},
            _ = shutdown_rx.changed() => break,
        }
    }

    tracing::info!("poller: exiting");
    Ok(())
}

/// Cleanup builder:: entries not backed by current builders in `zoekt:repo_builders`.
/// This is extracted so tests can call it directly with a mock Redis implementation.
pub async fn cleanup_builder_entries(pool: Arc<dyn DynRedis>) -> Result<(), anyhow::Error> {
    // Read current builders
    let builders = pool.hgetall("zoekt:repo_builders").await?;
    let mut current_ids: Vec<String> = Vec::new();
    for (_bid, bj) in &builders {
        if let Ok(v) = serde_json::from_str::<Value>(bj.as_str()) {
            let id = v
                .get("id")
                .and_then(|x| x.as_str())
                .unwrap_or("")
                .to_string();
            if !id.is_empty() {
                current_ids.push(id);
            }
        }
    }

    tracing::debug!(current_builders = ?current_ids, "poller: cleanup_builder_entries - current builder ids");

    if let Ok(entries) = pool.hgetall("zoekt:repo_branches").await {
        tracing::debug!(
            count = entries.len(),
            "poller: cleanup_builder_entries - found repo_branches entries"
        );
        for (field, _val) in entries.iter() {
            tracing::trace!(entry = %field, "poller: cleanup_builder_entries visiting entry");
            if field.starts_with("builder::") {
                if let Some((prefix, _rest)) = field.split_once('|') {
                    if let Some(id) = prefix.strip_prefix("builder::") {
                        if !current_ids.contains(&id.to_string()) {
                            tracing::info!(field = %field, missing_builder = %id, "poller: cleanup_builder_entries deleting orphan builder entry");
                            match pool.hdel("zoekt:repo_branches", field).await {
                                Ok(true) => {
                                    tracing::debug!(field = %field, "poller: deleted zoekt:repo_branches field")
                                }
                                Ok(false) => {
                                    tracing::warn!(field = %field, "poller: attempted delete but field not found")
                                }
                                Err(e) => {
                                    tracing::warn!(field = %field, error = ?e, "poller: failed to delete zoekt:repo_branches field")
                                }
                            }
                            match pool.hdel("zoekt:repo_branch_meta", field).await {
                                Ok(true) => {
                                    tracing::debug!(field = %field, "poller: deleted zoekt:repo_branch_meta field")
                                }
                                Ok(false) => {
                                    tracing::warn!(field = %field, "poller: attempted delete of meta but field not found")
                                }
                                Err(e) => {
                                    tracing::warn!(field = %field, error = ?e, "poller: failed to delete zoekt:repo_branch_meta field")
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sort_and_limit_tags_semver() {
        let tags = vec![
            "v1.2.3".to_string(),
            "v1.10.0".to_string(),
            "2.0.0".to_string(),
            "v1.2.3-alpha".to_string(),
            "not-a-version".to_string(),
            "v0.9.0".to_string(),
        ];
        let res = sort_and_limit_tags(tags, 10);
        // Expected order ascending for client readability: smallest -> largest
        // Semver ordering descending would be: 2.0.0, v1.10.0, v1.2.3, v1.2.3-alpha, v0.9.0, not-a-version
        // After truncation and reversing to ascending, expect: not-a-version, v0.9.0, v1.2.3-alpha, v1.2.3, v1.10.0, 2.0.0
        assert_eq!(res.len(), 6);
        assert_eq!(res[0], "not-a-version");
        assert_eq!(res[5], "2.0.0");
    }

    #[test]
    fn test_sort_and_limit_tags_truncate() {
        let tags = vec![
            "v1.0.0".to_string(),
            "v1.1.0".to_string(),
            "v1.2.0".to_string(),
        ];
        let res = sort_and_limit_tags(tags, 2);
        // Should return 2 tags in ascending order
        assert_eq!(res.len(), 2);
        assert_eq!(res[0], "v1.1.0");
        assert_eq!(res[1], "v1.2.0");
    }

    #[test]
    fn test_preview_from_refs_filters_and_limits() {
        let branches = vec![
            "main".to_string(),
            "develop".to_string(),
            "feature/x".to_string(),
        ];
        let tags = vec![
            "v1.0.0".to_string(),
            "v1.1.0".to_string(),
            "v2.0.0".to_string(),
            "rc-1".to_string(),
        ];

        let include_branches = Some(serde_json::Value::String("main,feature/*".to_string()));
        let exclude_branches = None;
        let include_tags = Some(serde_json::Value::String("v1.*".to_string()));
        let exclude_tags = None;

        let out = preview_from_refs(
            branches,
            tags,
            include_branches,
            exclude_branches,
            include_tags,
            exclude_tags,
            10,
        );
        assert!(out.get("branches").is_some());
        let b = out.get("branches").unwrap().as_array().unwrap();
        // should include main and feature/x (feature matches feature/*)
        assert_eq!(b.len(), 2);
        let t = out.get("tags").unwrap().as_array().unwrap();
        // include_tags matches v1.0.0 and v1.1.0 only, and they should be ordered ascending
        assert_eq!(t.len(), 2);
        assert_eq!(t[0].as_str().unwrap(), "v1.0.0");
        assert_eq!(t[1].as_str().unwrap(), "v1.1.0");
    }

    #[test]
    fn test_parse_owner_from_git_url_multi_segment() {
        let a = "git@github.com:org/repo.git";
        assert_eq!(parse_owner_from_git_url(a).as_deref(), Some("org"));

        let b = "git@github.com:org/suborg/repo.git";
        assert_eq!(parse_owner_from_git_url(b).as_deref(), Some("org/suborg"));

        let c = "https://github.com/owner/repo.git";
        assert_eq!(parse_owner_from_git_url(c).as_deref(), Some("owner"));

        let d = "ssh://git@host/owner/sub/repo.git";
        assert_eq!(parse_owner_from_git_url(d).as_deref(), Some("owner/sub"));
    }
}

#[cfg(test)]
mod integration_tests {
    use super::*;
    use async_trait::async_trait;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::Mutex;

    struct MockRedis {
        // simple in-memory hash maps for keys
        data: Mutex<HashMap<String, HashMap<String, String>>>,
    }

    impl MockRedis {
        fn new() -> Self {
            Self {
                data: Mutex::new(HashMap::new()),
            }
        }
    }

    #[async_trait]
    impl crate::redis_adapter::DynRedis for MockRedis {
        async fn ping(&self) -> anyhow::Result<()> {
            Ok(())
        }

        async fn hgetall(&self, key: &str) -> anyhow::Result<Vec<(String, String)>> {
            let map = self.data.lock().unwrap();
            if let Some(h) = map.get(key) {
                Ok(h.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            } else {
                Ok(Vec::new())
            }
        }

        async fn eval_i32(
            &self,
            _script: &str,
            _keys: &[&str],
            _args: &[&str],
        ) -> anyhow::Result<i32> {
            Ok(0)
        }

        async fn eval_vec_string(
            &self,
            _script: &str,
            _keys: &[&str],
            _args: &[&str],
        ) -> anyhow::Result<Vec<String>> {
            Ok(Vec::new())
        }

        async fn hset(&self, key: &str, field: &str, value: &str) -> anyhow::Result<()> {
            let mut map = self.data.lock().unwrap();
            let entry = map.entry(key.to_string()).or_insert_with(HashMap::new);
            entry.insert(field.to_string(), value.to_string());
            Ok(())
        }

        async fn hget(&self, key: &str, field: &str) -> anyhow::Result<Option<String>> {
            let map = self.data.lock().unwrap();
            Ok(map.get(key).and_then(|h| h.get(field).cloned()))
        }

        async fn get(&self, _key: &str) -> anyhow::Result<Option<String>> {
            Ok(None)
        }

        async fn hdel(&self, key: &str, field: &str) -> anyhow::Result<bool> {
            let mut map = self.data.lock().unwrap();
            if let Some(h) = map.get_mut(key) {
                let removed = h.remove(field).is_some();
                Ok(removed)
            } else {
                Ok(false)
            }
        }
    }

    #[tokio::test]
    async fn test_cleanup_removes_builder_entries_after_deletion() {
        let mock = Arc::new(MockRedis::new());

        // Seed a builder entry and corresponding builder:: fields
        let builder_json = serde_json::json!({
            "id": "b1",
            "base_repo": "git@github.com:org/repo.git"
        });

        mock.hset("zoekt:repo_builders", "b1", &builder_json.to_string())
            .await
            .unwrap();

        // add builder-generated branch and tag entries
        mock.hset(
            "zoekt:repo_branches",
            "builder::b1|main",
            "git@github.com:org/repo.git",
        )
        .await
        .unwrap();
        mock.hset("zoekt:repo_branch_meta", "builder::b1|main", "meta")
            .await
            .unwrap();
        mock.hset(
            "zoekt:repo_branches",
            "builder::b1|v1.0.0",
            "git@github.com:org/repo.git",
        )
        .await
        .unwrap();
        mock.hset("zoekt:repo_branch_meta", "builder::b1|v1.0.0", "meta")
            .await
            .unwrap();

        // ensure entries exist
        let before = mock.hgetall("zoekt:repo_branches").await.unwrap();
        assert!(before.iter().any(|(k, _)| k == "builder::b1|main"));
        assert!(before.iter().any(|(k, _)| k == "builder::b1|v1.0.0"));

        // Now delete the builder (simulate admin deletion)
        let _ = mock.hdel("zoekt:repo_builders", "b1").await.unwrap();

        // Run cleanup
        let pool_arc: Arc<dyn crate::redis_adapter::DynRedis> = mock.clone();
        cleanup_builder_entries(pool_arc).await.unwrap();

        // After cleanup, builder:: entries should be removed
        let after = mock.hgetall("zoekt:repo_branches").await.unwrap();
        assert!(!after.iter().any(|(k, _)| k == "builder::b1|main"));
        assert!(!after.iter().any(|(k, _)| k == "builder::b1|v1.0.0"));
    }

    // End-to-end test that runs the actual run_poller loop against a real Redis instance.
    // This test only runs when REDIS_URL is set in the environment. It will use a random
    // Redis database index (appended to the URL) to isolate keys and flush the DB before
    // and after the test so it won't interfere with other tests using the same REDIS_URL.
    #[tokio::test]
    async fn test_run_poller_end_to_end_cleans_up_builder_entries() {
        use std::time::Duration;

        // Base REDIS_URL must be provided by the test environment to run this test.
        let base = match std::env::var("REDIS_URL") {
            Ok(v) => v,
            Err(_) => {
                eprintln!("REDIS_URL not set; skipping end-to-end poller test");
                return;
            }
        };

        // Determine DB index for deterministic runs. Prefer TEST_REDIS_DB if set
        // (CI sets this per-job), otherwise fall back to 13 for local debugging.
        let db: u8 = match std::env::var("TEST_REDIS_DB") {
            Ok(s) => s.parse().unwrap_or(13),
            Err(_) => 13,
        };
        let url = format!("{}/{}", base.trim_end_matches('/'), db);
        // Temporarily set REDIS_URL for create_redis_pool
        let orig = std::env::var("REDIS_URL").ok();
        std::env::set_var("REDIS_URL", &url);

        // Create pool and wrap RealRedis similarly to run_poller
        let pool_opt = crate::redis_adapter::create_redis_pool();
        if pool_opt.is_none() {
            eprintln!("failed to create redis pool for test; skipping end-to-end poller test");
            return;
        }
        let pool = pool_opt.unwrap();
        // Verify Redis is reachable before continuing
        let real = crate::redis_adapter::RealRedis { pool: pool.clone() };
        if real.ping().await.is_err() {
            eprintln!("Redis ping failed; skipping end-to-end poller test");
            return;
        }
        let pool_arc: Arc<dyn crate::redis_adapter::DynRedis> = Arc::new(real);

        // Flush DB to start clean
        let _ = pool_arc
            .eval_i32("redis.call('FLUSHDB'); return 0", &[], &[])
            .await
            .unwrap();

        tracing::info!(
            debug_db = db,
            "poller test: flushed DB for deterministic run"
        );

        // Dump current keys for debugging
        if let Ok(blds) = pool_arc.hgetall("zoekt:repo_builders").await {
            tracing::info!(builders = ?blds, "poller test: builders after flush (should be empty)");
        }
        if let Ok(rb) = pool_arc.hgetall("zoekt:repo_branches").await {
            tracing::info!(repo_branches = ?rb, "poller test: repo_branches after flush (should be empty)");
        }

        // Seed builder and entries
        let builder_json = serde_json::json!({
            "id": "e2e_b",
            "base_repo": "git@github.com:org/repo.git"
        });
        pool_arc
            .hset("zoekt:repo_builders", "e2e_b", &builder_json.to_string())
            .await
            .unwrap();
        pool_arc
            .hset(
                "zoekt:repo_branches",
                "builder::e2e_b|main",
                "git@github.com:org/repo.git",
            )
            .await
            .unwrap();

        // Dump state after seeding builder and branch
        if let Ok(blds) = pool_arc.hgetall("zoekt:repo_builders").await {
            tracing::info!(builders = ?blds, "poller test: builders after seeding");
        }
        if let Ok(rb) = pool_arc.hgetall("zoekt:repo_branches").await {
            tracing::info!(repo_branches = ?rb, "poller test: repo_branches after seeding");
        }

        // spawn poller
        let (tx, rx) = watch::channel(());
        let cfg = PollerConfig {
            poll_interval: Duration::from_millis(100),
        };

        let handle = tokio::spawn(async move {
            let _ = run_poller(rx, cfg).await;
        });

        // allow poller to start and perform a pass
        tokio::time::sleep(Duration::from_millis(200)).await;

        // delete builder to trigger cleanup in the next poll
        let _ = pool_arc.hdel("zoekt:repo_builders", "e2e_b").await.unwrap();

        // Call cleanup directly to verify cleanup logic in isolation
        cleanup_builder_entries(pool_arc.clone()).await.unwrap();
        let after_direct = pool_arc.hgetall("zoekt:repo_branches").await.unwrap();
        tracing::info!(repo_branches_after_direct_cleanup = ?after_direct, "poller test: repo_branches after direct cleanup call");
        assert!(
            !after_direct.iter().any(|(k, _)| k == "builder::e2e_b|main"),
            "builder entry should have been removed by direct cleanup but remains"
        );

        // allow poller another cycle to observe deletion and cleanup (give more time)
        tokio::time::sleep(Duration::from_millis(1000)).await;

        // Dump state after cleanup attempt by poller
        if let Ok(blds) = pool_arc.hgetall("zoekt:repo_builders").await {
            tracing::info!(builders = ?blds, "poller test: builders after delete");
        }
        let after = pool_arc.hgetall("zoekt:repo_branches").await.unwrap();
        tracing::info!(repo_branches_after_cleanup = ?after, "poller test: repo_branches after cleanup attempt");
        assert!(
            !after.iter().any(|(k, _)| k == "builder::e2e_b|main"),
            "builder entry should have been removed but remains"
        );

        // shutdown poller
        let _ = tx.send(());
        let _ = handle.await;

        // flush DB again to leave a clean state
        let _ = pool_arc
            .eval_i32("redis.call('FLUSHDB'); return 0", &[], &[])
            .await
            .unwrap();

        // restore original REDIS_URL
        if let Some(origv) = orig {
            std::env::set_var("REDIS_URL", origv);
        }
    }
}
