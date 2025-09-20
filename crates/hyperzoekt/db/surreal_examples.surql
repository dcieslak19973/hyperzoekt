-- These are example SurrealQL snippets the service layer can execute
-- to implement commits/refs/snapshot_meta operations. Enforce invariants
-- 1) Insert a commit (immutable)
-- Use the commit id you choose (e.g. VCS commit sha or content-addressed hash)
--
-- LET $commit_id = "sha256:...";
-- LET $repo = "repo:foo";
-- LET $parents = [];
-- LET $tree = "tree:...";
-- LET $author = "Alice <a@x>";
-- LET $message = "Initial commit";
-- INSERT INTO commits (id, repo, parents, tree, author, message, timestamp) VALUES ($commit_id, $repo, $parents, $tree, $author, $message, time::now());


-- 2) Create a branch (mutable pointer)
-- id convention: "<repo>:refs/heads/<branch>" but you can choose any stable id
--
-- LET $branch_id = $repo + ":refs/heads/main";
-- LET $branch_name = "refs/heads/main";
-- LET $target_commit = $commit_id;
-- INSERT INTO refs (id, repo, name, kind, target, created_at, updated_at) VALUES ($branch_id, $repo, $branch_name, "branch", $target_commit, time::now(), time::now());


-- 3) Move a branch (atomic update)
-- Use a transaction when updating the branch head to avoid races
-- This example updates target if and only if the ref exists and is a branch
--
-- BEGIN TRANSACTION;
-- LET $existing = (SELECT * FROM refs WHERE id = $branch_id AND kind = "branch");
-- IF (len($existing) == 1) {
--   UPDATE refs SET target = $new_commit, updated_at = time::now() WHERE id = $branch_id;
-- } ELSE {
--   -- branch not found or not a branch: create or error as appropriate
--   RAISE "branch-not-found";
-- }
-- COMMIT TRANSACTION;


-- 4) Create a tag (immutable pointer)
-- Enforce immutability by checking for existence first; do NOT allow updates
--
-- LET $tag_id = $repo + ":refs/tags/v1.0.0";
-- LET $tag_name = "refs/tags/v1.0.0";
-- LET $tag_target = $commit_id;
-- BEGIN TRANSACTION;
-- LET $found = (SELECT * FROM refs WHERE id = $tag_id);
-- IF (len($found) == 0) {
--   INSERT INTO refs (id, repo, name, kind, target, created_at) VALUES ($tag_id, $repo, $tag_name, "tag", $tag_target, time::now());
-- } ELSE {
--   RAISE "tag-already-exists";
-- }
-- COMMIT TRANSACTION;


-- 5) Resolve a ref name to a commit id
-- Resolution policy examples:
--  - Accept fully-qualified ref names ("refs/heads/main", "refs/tags/v1.0.0")
--  - Accept short branch names ("main") by first trying "refs/heads/<name>"
--
-- LET $raw = "main"; -- or "refs/heads/main" or "refs/tags/v1.0.0"
-- LET $candidates = [ $raw, "refs/heads/" + $raw, "refs/tags/" + $raw ];
-- LET $rows = (SELECT * FROM refs WHERE repo = $repo AND name IN $candidates);
-- -- Service should apply disambiguation rules: prefer exact match, then branch, then tag
-- -- Example selection logic (simplified):
-- LET $exact = (SELECT * FROM $rows WHERE name = $raw);
-- IF (len($exact) > 0) { LET $ref = $exact[0]; } ELSE {
--   LET $branch_match = (SELECT * FROM $rows WHERE name = "refs/heads/" + $raw);
--   IF (len($branch_match) > 0) { LET $ref = $branch_match[0]; } ELSE {
--     LET $tag_match = (SELECT * FROM $rows WHERE name = "refs/tags/" + $raw);
--     IF (len($tag_match) > 0) { LET $ref = $tag_match[0]; } ELSE { RAISE "ref-not-found"; }
--   }
-- }
-- -- finally, $ref.target holds the commit id


-- 6) Lookup snapshot metadata for a commit
-- Find a search snapshot associated with a commit
--
-- LET $commit = $commit_id;
-- SELECT * FROM snapshot_meta WHERE repo = $repo AND commit = $commit LIMIT 1;


-- 7) Create snapshot metadata (after snapshot build completes)
-- Use a canonical snapshot naming scheme, e.g. "snapshot:<repo>:commit:<commit_id>"
--
-- LET $snapshot_id = "snapshot:" + $repo + ":commit:" + $commit;
-- INSERT INTO snapshot_meta (id, repo, commit, created_at, size_bytes) VALUES ($snapshot_id, $repo, $commit, time::now(), $size_bytes);


-- 8) Cleanup / retention: delete snapshot metadata for old commits
-- Delete snapshot_meta entries first, and then remove on-disk snapshot artifacts via operator tooling
--
-- DELETE FROM snapshot_meta WHERE repo = $repo AND created_at < time::now() - dur("P90D");


-- Notes & Recommendations:
-- - Enforce tag immutability in the service layer: check existence before creating.
-- - Use transactions for branch moves to avoid races.
-- - Keep commit objects immutable; do not update commits.
-- - Keep index metadata small and authoritative for mapping commit -> index.
-- - Consider adding SurrealDB indexes on `refs(repo, name)` and `snapshot_meta(repo, commit)` for fast lookup.
