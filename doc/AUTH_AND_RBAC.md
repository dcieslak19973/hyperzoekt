# Auth and RBAC for remote repositories

This document describes a practical design and implementation plan for handling authentication and enforcing remote source control RBAC in hyperzoekt/zoekt-rs. It focuses on two deployment modes (local and cluster), how to probe access, where to plug in provider-specific checks (GitHub/GitLab/Bitbucket), token handling, and recommended Kubernetes patterns.

## Checklist

- [ ] Decide where user tokens are collected (WebUI / MCP) and how long to cache them.
- [x] Distinguish local vs remote repo specs and skip checks in local mode.
- [ ] Add a pluggable access-checker trait and provider-specific checkers.
- [x] Provide a safe, non-interactive `git`-based probe as a fallback.
- [ ] Add provider API checkers (GitHub/GitLab/Bitbucket) for authoritative per-user RBAC.
- [ ] Add caching, audit logging, and tests (unit + gated integration).

## Goals

- Keep the local (developer) experience fast and simple: no auth checks required for local paths.
- In multi-tenant cluster mode, consult SCM (GitHub/GitLab/Bitbucket) in a secure, auditable way to determine whether the requesting user can access a given remote repository.
- Provide a small, pluggable API so provider-specific checks (REST APIs) can be added over time.
- Avoid prompting the process for credentials during probes; probes must be non-interactive and suitable for servers.

## High-level approach

1. Add a deploy-mode toggle: `local` or `cluster`.
   - Default: `local` (no remote auth checks).
   - Cluster: enforce checks for remote repos before cloning/indexing.
   - Config: env var `ZOEKT_DEPLOY_MODE=local|cluster` or an `IndexBuilder::deploy_mode(...)` option.

2. Detect local vs remote repo specs (same simple checks used elsewhere):
   - If the spec is a path that exists -> local repo.
   - If spec looks like a URL (`http://`, `https://`, `git@`, `ssh://`) -> remote.

3. Provide an `AccessDecision` enum and pluggable `RepoAccessChecker` trait.
   - AccessDecision: `Accessible | NeedsAuth | Forbidden | NotFound | Error(anyhow::Error)`.
   - Trait: `fn check_repo(&self, repo_spec: &str, user_token: Option<&UserToken>) -> AccessDecision`.

4. Implement two checkers initially:
   - CliProbeChecker (fallback): runs `git ls-remote --heads <url>` with environment variables to avoid prompts:
     - `GIT_TERMINAL_PROMPT=0` to block interactive prompts
     - For SSH URLs: `GIT_SSH_COMMAND='ssh -o BatchMode=yes'` so SSH will not ask for passphrase/password
     - Map exit code & stderr text heuristics to AccessDecision.
   - ProviderApiChecker (per-provider): call GitHub/GitLab/Bitbucket REST APIs with the user token for authoritative results.

5. Wire access check into `IndexBuilder::build()` in cluster mode before cloning a remote repo.
   - If `Accessible` -> proceed.
   - Otherwise return a typed error so UI/MCP can present a clear message (NeedsAuth / Forbidden / NotFound).

## CLI probe (non-interactive) — rationale and mapping

- Rationale: quick, low-friction, works with many remote forms and uses the host's configured credential helpers/SSH agent. It is best-effort and is suitable when no per-user token is available.

- Command:

```bash
GIT_TERMINAL_PROMPT=0 git -c core.askPass= ls-remote --heads <url>
# for ssh urls do:
GIT_TERMINAL_PROMPT=0 GIT_SSH_COMMAND='ssh -o BatchMode=yes' git ls-remote --heads <url>
```

- Interpretation (example heuristics):
  - exit status 0: Accessible
  - stderr contains `Authentication failed`, `access denied`, `Permission denied`: NeedsAuth
  - stderr contains `not found`, `Repository not found`, HTTP 404: NotFound
  - stderr contains `403` or `remote: HTTP Basic: Access denied`: Forbidden or NeedsAuth (use 403->Forbidden)
  - network failures/timeouts -> Error

> Note: heuristics are not perfect. Prefer provider API checks for authoritative RBAC in cluster mode when user tokens are available.

## Provider API checks (authoritative for per-user RBAC)

- Use provider REST APIs with the user's token. This is required to reliably enforce per-user RBAC.

- GitHub example:
  - Endpoint: GET https://api.github.com/repos/{owner}/{repo}
  - Authorization: `token <PAT>` or Bearer for OAuth tokens
  - Responses:
    - 200 -> Accessible
    - 401 -> NeedsAuth (token missing/invalid)
    - 403 -> Forbidden (token valid but insufficient permissions)
    - 404 -> NotFound
    - other 5xx -> Error

- GitLab / Bitbucket: equivalent project endpoints exist; URL encoding and project-id lookup may be required.

- Implementation notes:
  - Use HTTPS API calls via `reqwest` (or the existing HTTP client in your stack).
  - Avoid logging tokens or injecting them into logs.
  - If provider returns ambiguous errors (e.g., rate-limited), map to `Error` and surface diagnostic messages.

## User token model & storage (k8s)

- User token struct (minimal):
  - provider: {GitHub, GitLab, Bitbucket, Unknown}
  - token: opaque string (PAT / OAuth access token)
  - token_type: optional (PAT vs OAuth)
  - Store tokens in Kubernetes as per-user `Secret`s (one Secret per user per provider) or in an external secret manager (Vault, Secret Manager).
  - The indexing service reads the token when the user initiates an action; the service should not store tokens unencrypted in its DB.
  - RBAC: limit which service account in the cluster can read which secrets. Ideally the MCP authorizes access to the per-user secret and passes the token to the indexer call.

- WebUI/MCP flow (recommended):
  1. User initiates "index remote repo" in the UI.
  2. If provider token is not present, UI starts OAuth flow or asks the user for a PAT.
  3. After successful OAuth/PAT entry, store the token securely (k8s Secret).
  4. MCP invokes the indexer, passing a reference to the token (or the token itself via secure channel). Indexer calls ProviderApiChecker.
  5. If access granted, indexing proceeds; otherwise UI prompts user.

## Where to place code (suggestions)

- Add a new module: `crates/zoekt-rs/src/index/access.rs` or extend `git.rs`.
  - Define `AccessDecision` and `RepoAccessChecker` trait here.
  - Implement `CliProbeChecker` and `GitHubApiChecker`.


## Caching and rate-limiting

- Cache `AccessDecision` per (user, repo_spec) for a short TTL (5–15 minutes) to reduce duplicate probes and avoid rate limits.
- For provider APIs, cache per-user results; for public repos, global cache is safe.
## Audit & logging

- Log probe results and user identity (avoid logging tokens or secrets).
- For cluster mode, emit structured audit events for decisions (Accessible/Denied) for observability and troubleshooting.

## Tests

- Unit tests:
  - Parse stderr mappings into AccessDecision independently of running `git`.
  - Provider API mapping functions (map status codes to AccessDecision).

- Integration tests (gated with env var):
  - `ZOEKT_TEST_REMOTE_PROBE=1` to run actual probes against a public repo and (optionally) against a private repo using a provided token.
```rust
pub enum DeployMode { Local, Cluster }
```
  - if mode == Local -> proceed
  - else -> call checker


- Never log tokens.
- Use non-interactive probes (no credential prompts) for safety in servers.

I can implement the minimal, low-risk pieces now:

- Add `AccessDecision` and `RepoAccessChecker` and a `CliProbeChecker` implementation.
- Wire the check into `IndexBuilder::build()` so cluster mode enforces checks before cloning.
- Add unit tests and a gated integration test.

If you want per-user authoritative RBAC, I can then add provider-specific checkers (starting with GitHub) and example k8s Secret manifests and WebUI/OAuth notes.


When running in cluster mode with multiple users, searches will commonly span many indexed repositories. Some of those repos may be private or otherwise guarded by RBAC in the remote SCM. This section documents how to ensure that a user only receives search results from repositories they are allowed to read.

Goals

- A user must never receive content (paths, snippets, symbols) from repos they are not authorized to read.
- Searches across many repos should remain performant and scalable.
- RBAC decisions should be auditable and reasonably fresh (reflect recent membership/permission changes).

High-level approaches

1) Pre-filter repos (recommended):
  - Before running the actual search, compute the set of repositories the requesting user can access.
  - Run the search only against that subset of repositories. This avoids needing post-filtering of results and scales better when indexes are sharded per-repo or per-group.

2) Post-filter results (fallback):
  - Run the search across all candidate indexes, then drop any matches that belong to repos the user cannot access.
  - Simpler to implement but potentially wasteful in CPU and IO because you run queries on repos you will discard.

3) Index-time ACL embedding (advanced):
  - Embed ACL metadata in the index (per-repo or per-doc) and enforce at query time inside the search engine.
  - This allows a single index to be queried with a per-user filter, but requires storing ACLs in the index and may complicate index updates.

Recommended design

- Use pre-filtering as the default. Maintain a fast cache (in-memory or Redis) of accessible repositories per user so queries can be routed to the right shards quickly.
- For public repos or local paths, skip expensive checks.
- Use provider API checks (per-user token) or cached CLI-based probe results to populate the per-user accessible set.

Implementation sketch

1) Data model additions
  - Repo metadata (already present in `RepoMeta`) should include a canonical remote spec and a `visibility` hint (public/private/unknown).
  - Maintain an access-cache keyed by (user_id, repo_spec) -> AccessDecision with TTL.

2) Pre-query flow
  - Client (WebUI / MCP) sends a search request plus the identity of the requesting user.
  - Query service asks the access-cache for the list of repos this user can access (or computes it if missing):
     * For each repo: if cache has `Accessible` -> include.
     * If cache missing or stale -> run checker (Provider API with user token if available; else CLI probe) and write back cache.
  - Once the allowed repo list is collected, dispatch the query only to those repositories' indexes/shards.

3) Parallelization and batching
  - Batch access checks when possible (e.g., provider APIs can accept multiple repo queries or you can fire many checks in parallel).
  - For large repo sets, compute allowed repos in the background and keep a per-user allowed-repo manifest updated.

4) Cache eviction and freshness
  - Use a TTL (e.g., 5–15 minutes) for per-repo-per-user cache entries.
  - Also subscribe to provider webhooks (GitHub team/org membership, repository permission changes) and invalidate affected cache entries proactively.

5) Handling RBAC changes
  - If a user's access is revoked, the cache may still allow results until TTL expiry; use webhooks to reduce window.
  - Optionally, on sensitive actions (e.g., showing a diff of a recently changed PR) re-validate access immediately.

6) Auditing and telemetry
  - Log which repos were checked and the outcome (Accessible/Denied) along with user_id (never log tokens).
  - Emit an audit event for denied requests to help admins investigate.

7) UI considerations
  - Indicate when results were filtered for permissions (for transparency), for example: "Results shown from X of Y scanned repos; some private repos require access." 
  - If the search would return zero results because the user lacks access to all matching repos, provide a helpful message and an affordance to add credentials / request access.

Edge cases and trade-offs

- Scalability: running live provider API checks at query time for every repo and every user does not scale. Rely on caching and background manifests.
- Consistency window: there will be a short window between permission change and enforcement unless you implement immediate invalidation via webhooks.
- False positives/negatives: CLI probe heuristics may misclassify some edge-cases; prefer provider API checks when enforcing per-user RBAC.

Security notes

- Never substitute a service-level permissive token for a user's token when enforcing per-user RBAC—use provider APIs with the user's token where you must decide per-user access.
- If you must use a service token for performance, ensure you map decisions back to the user only after checking provider APIs for team/org membership when possible.

Contract and success criteria

- Inputs: user identity (id, provider token optional), a search query, and an index manifest of repositories.
- Output: search matches only from repos the user is authorized to read.
- Error modes: when access cannot be determined, either fail closed (deny results) or fail open depending on configuration; failing closed is safer for private data.

Next steps

- Implement per-user access-cache and pre-filtering in the search dispatch layer.
- Add provider API checkers for GitHub, GitLab, and Bitbucket.
- Wire provider webhooks to invalidate cache entries.
- Add UI messages and an option for users to provide tokens via the WebUI/MCP.

This section describes a practical, incremental path to enforce RBAC reliably while keeping multi-repo searches fast and user-friendly. If you want, I can implement the access-cache + pre-filter wiring next (small, testable change) and follow up with GitHub API checker and webhook wiring.
