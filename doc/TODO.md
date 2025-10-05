# TODO for hyperzoekt

1. Extract the tree-sitter work from the `goose` PR: https://github.com/block/goose/pull/3389
2. Figure out how to store the results of a tree-sitter run into a hypergraph
3. Create some combination of UI, CLI, and MCP (both stdio and streaming http)
4. Port zoekt (https://github.com/sourcegraph/zoekt)
5. Blend the zoekt port with the hypergraph
6. Add support for indexing multiple repos (BitBucket, GitLab, GitHub)
7. Add support for syncing permissions from BitBucket and GitLab into the hypergraph to support a multiuser environment

8. Revise/verify `streaming_chunked` behavior
	 - Assessment: the streaming-thread has a `streaming_chunked` mode (enabled via `SURREAL_STREAM_CHUNKED=1`) that is intended to split accumulated entity payloads into multiple inline-`CREATE` transactions sized by `batch_capacity`. This should prevent a single very-large transaction when the accumulator is large.
	 - Fact-check (quick): current code applies file-related UPDATE/CREATE parts first (as one batch) and then iterates `for chunk in acc.chunks(batch_capacity)` to emit per-chunk `BEGIN; CREATE ...; COMMIT;` queries, so the implementation does perform chunking. Caveats: if the accumulated `acc` size is <= `batch_capacity` you'll still observe a single chunk; file-updates are sent as a separate batch which may skew observed metrics.
	 - Next actions (TODO):
			* Add a small integration test or reproducible script that exercises chunked streaming with `SURREAL_STREAM_CHUNKED=1` and asserts `batches_sent` > 1 for large inputs (exercise acc > batch_capacity).
		 * Consider making `streaming_chunked` the default (remove env guard) after the test/validation.
		 * Re-run the batch sweep after any change and update `doc/BATCH_SIZE_TUNING.md` and `doc/SURREALDB-INTEGRATION.md` if behavior changes.

Longer term, we want to be mindful that the MCP interface should be optimized for how
an LLM would use it.  Since most LLMs explore codebases using tools like `grep` and
`ripgrep`, we should assume that the LLM would want to interact with this search tool
in the same way.

9. Add support for discovering and storing git tags associated with commits
   - Currently only branches are created as refs pointing to commits during indexing
   - Need to scan the git repository for tags and create tag refs similar to branch refs
   - This would allow queries to find commits by tag names

10. Store the actual default branch name when indexing repositories
   - Current implementation: WebUI hardcodes common default branch names ('main', 'master', 'trunk') when looking up SBOM dependencies
   - Better approach: Query git's HEAD symbolic ref (e.g., `git symbolic-ref HEAD`) during initial repository indexing to determine the actual default branch
   - Store this information in the database (either in a separate `repos` table or as metadata in the refs table)
   - Benefits: More accurate, supports non-standard default branch names, avoids ambiguity when multiple common branch names exist
   - Location: `crates/hyperzoekt/src/bin/hyperzoekt-webui.rs` function `get_sbom_dependencies_for_repo`