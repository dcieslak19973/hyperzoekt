-- SurrealDB schema for hyperzoekt minimal commit/ref model
-- Collections: commits, refs, snapshot_meta

-- Commits: immutable snapshots of repository state
-- Fields:
--  id: string (primary key) - commit id (content-addressed or VCS commit hash)
--  repo: string - repository id
--  parents: array<string> - parent commit ids
--  tree: string - snapshot id or pointer to stored tree
--  author: string?
--  committer: string?
--  message: string?
--  timestamp: datetime?
--  indexed_at: datetime? - when indexing completed

DEFINE TABLE commits SCHEMAFULL;

-- Example insert:
-- INSERT INTO commits (id, repo, parents, tree, author, message, timestamp) VALUES (
--   "sha256:...", "repo:foo", [], "tree:xxx", "Alice <a@x>", "Initial import", time::now()
-- );

-- Refs: mutable pointers (branches) or immutable pointers (tags) to commits
-- Fields:
--  id: string (primary key) - e.g. "repo:refs/heads/main" or a generated id
--  repo: string
--  name: string - human name like "refs/heads/main" or "v1.2.3"
--  kind: string - "branch" | "tag"
--  target: string - commit id this ref points to
--  created_at: datetime
--  updated_at: datetime
--  created_by: string?

DEFINE TABLE refs SCHEMAFULL;

-- Snapshot metadata: maps snapshots to commits
-- Fields:
--  id: string (primary key) - snapshot id (e.g. "snapshot:repo:commit:sha...")
--  repo: string
--  commit: string - commit id
--  created_at: datetime
--  size_bytes: number?

DEFINE TABLE snapshot_meta SCHEMAFULL;

-- Example ref creation (branch):
-- INSERT INTO refs (id, repo, name, kind, target, created_at, updated_at) VALUES (
--   "repo:refs/heads/main", "repo:foo", "refs/heads/main", "branch", "sha256:...", time::now(), time::now()
-- );

-- Example tag creation (immutable):
-- INSERT INTO refs (id, repo, name, kind, target, created_at) VALUES (
--   "repo:refs/tags/v1.0.0", "repo:foo", "refs/tags/v1.0.0", "tag", "sha256:...", time::now()
-- );

-- Note: enforce tag immutability at service layer; SurrealDB can accept triggers or rules if needed later.
