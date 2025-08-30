use anyhow::Result;
use std::path::Path;

/// Choose a set of branches for a repository at `repo_root` similar to the
/// logic previously embedded in `builder.rs` (prefer HEAD, then recent tips).
pub(crate) fn choose_branches(repo_root: &Path, max_branches: usize) -> Option<Vec<String>> {
    if let Ok(repo) = git2::Repository::open(repo_root) {
        let mut chosen: Vec<String> = Vec::new();
        if let Ok(head) = repo.head() {
            if let Some(name) = head.shorthand() {
                chosen.push(name.to_string());
            }
        }
        let mut tips: Vec<(String, i64)> = Vec::new();
        if let Ok(mut refs) = repo.references() {
            while let Some(Ok(r)) = refs.next() {
                if let Some(name) = r.shorthand() {
                    if let Some(rname) = r.name() {
                        if rname.starts_with("refs/heads/") {
                            if let Ok(resolved) = r.resolve() {
                                if let Ok(target) = resolved.peel_to_commit() {
                                    let time = target.time().seconds();
                                    tips.push((name.to_string(), time));
                                }
                            }
                        }
                    }
                }
            }
        }
        tips.sort_by(|a, b| b.1.cmp(&a.1));
        for (n, _) in tips.into_iter() {
            if chosen.contains(&n) {
                continue;
            }
            if chosen.len() >= max_branches {
                break;
            }
            chosen.push(n);
        }
        if !chosen.is_empty() {
            return Some(chosen);
        }
    }
    None
}

/// Extracts the tree for `branch` from `repo_root` into a temporary directory.
/// Tries libgit2 extraction first, falls back to `git archive | tar` when needed.
pub(crate) fn extract_branch_to_tempdir(
    repo_root: &Path,
    branch: &str,
) -> Result<tempfile::TempDir> {
    let td = tempfile::tempdir()?;
    let libgit2_ok = crate::index::utils::extract_branch_tree_libgit2(repo_root, branch, td.path());
    if libgit2_ok.is_ok() {
        return Ok(td);
    }

    // Fallback to `git archive | tar` flow
    let mut git = std::process::Command::new("git")
        .arg("-C")
        .arg(repo_root)
        .arg("archive")
        .arg("--format=tar")
        .arg(branch)
        .stdout(std::process::Stdio::piped())
        .spawn()?;
    let git_stdout = git.stdout.take().unwrap();
    let mut tar = std::process::Command::new("tar")
        .arg("-x")
        .arg("-C")
        .arg(td.path())
        .stdin(std::process::Stdio::piped())
        .spawn()?;
    if let Some(mut tar_stdin) = tar.stdin.take() {
        std::io::copy(&mut std::io::BufReader::new(git_stdout), &mut tar_stdin)?;
    }
    let git_status = git.wait()?;
    let tar_status = tar.wait()?;
    if !git_status.success() || !tar_status.success() {
        anyhow::bail!("git archive fallback failed");
    }
    Ok(td)
}
