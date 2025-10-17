# ISSUE: Triage — Missing Embeddings

This document is a short, focused triage checklist and issue template to
help maintainers and operators debug cases where some entities do not have
embeddings persisted.

Keep the issue concise: follow the checklist below and attach the requested
artifacts (logs, DLQ lines, SurrealDB query results). This makes root-cause
analysis fast and avoids guessing.

---

## Summary

Symptoms:
- Some `entity_snapshot` / `content` records have empty embeddings (`[]`) or
  `embedding_len == 0`.
- DLQ entries exist under the host-visible DLQ path (see `HZ_EMBED_DLQ_PATH`).

Important note: the writer intentionally does *not* fabricate source snippets
(e.g. falling back to `name`) when `source_content` is missing. This avoids
masking upstream extraction problems (usually in the Tree-sitter indexer).
When you see missing embeddings, start by verifying whether `source_content`
was produced for the affected entities.

---

## Quick checks (run first)

1. Confirm embedding runtime is enabled (or intentionally disabled):

```bash
# If HZ_EMBED_MODEL is empty, no embeddings will be computed inline
echo "HZ_EMBED_MODEL=${HZ_EMBED_MODEL:-}":
env | grep HZ_EMBED_MODEL

# If embedding jobs are enabled instead of inline compute
env | grep HZ_ENABLE_EMBED_JOBS
```

2. Check TEI base and DLQ path:

```bash
env | grep HZ_TEI_BASE
env | grep HZ_EMBED_DLQ_PATH
```

3. Run a focused test that verifies the chunking + DLQ helper (runs locally):

```bash
# runs tests that exercise chunking and DLQ append helpers
cargo test -p hyperzoekt --test embedding_chunking -- --nocapture
```

---

## Inspect DB rows (SurrealDB)

Use your SurrealDB HTTP client or `surreal` CLI. Examples below assume a
connected `surreal` shell or HTTP query interface.

1) Find entity snapshots with empty or missing embeddings:

```sql
-- snapshots where embedding array is empty or embedding_len == 0
SELECT id, stable_id, source_content, embedding, embedding_len, embedding_model
FROM entity_snapshot
WHERE embedding = [] OR embedding_len = 0 OR embedding IS NULL;
```

2) Find `content` rows used as deduplicated source content (if applicable):

```sql
SELECT id, snippet, embedding, embedding_len, embedding_model FROM content
WHERE embedding = [] OR embedding_len = 0 OR embedding IS NULL;
```

3) If you know a `stable_id` affected, show surrounding fields for context:

```sql
SELECT * FROM entity_snapshot WHERE stable_id = "<STABLE_ID>" LIMIT 1;
SELECT * FROM entity WHERE stable_id = "<STABLE_ID>" LIMIT 1;
```

Attach the JSON result (or a paste of the row) to the issue.

---

## Inspect the host DLQ

If `HZ_EMBED_DLQ_PATH` is configured, the writer appends lines describing
failed/partial embedding attempts.

Examples:

```bash
# Show the last 200 lines
tail -n 200 "$HZ_EMBED_DLQ_PATH" | sed -n '1,200p'

# Grep for a given stable_id
grep "<STABLE_ID>" "$HZ_EMBED_DLQ_PATH" || true

# If each line is JSON-ish or contains structured fields you can pipe to jq
# (some fields may be appended as key=value pairs)
tail -n 200 "$HZ_EMBED_DLQ_PATH" | jq -R -r '.' | sed -n '1,200p'
```

When filing an issue, paste the relevant DLQ line(s) and timestamp. Useful
fields to include: stable_id, repo_name, reason (e.g. `serialize_error`,
`empty_embeddings`, `all_chunks_failed`), snippet length, and the timestamp.

---

## Capture writer/indexer logs

Reproduce the run while collecting logs at debug level for the writer/indexer
so we can see the embedding path. Example environment for a local run:

```bash
RUST_LOG=hyperzoekt=debug,reqwest=debug cargo run -p hyperzoekt --bin hyperzoekt-indexer -- --index-once
```

Helpful debug flags (writer-specific):
- `HZ_DEBUG_SQL=1` — print SQL statements to stdout for the writer
- `HZ_DUMP_SQL_STATEMENTS=1` — dump full statement list (useful for tests)

Search the logs for these markers produced by the writer codebase:

- `initial_batch: snapshot left without embedding id=` — indicates a snapshot
  was persisted without embedding and why (check `snippet_len` printed).
- `append_to_embed_dlq(` — DLQ writes
- `compute_embeddings_for_payloads` / `sending` / `TEI` / `413` — TEI interaction

Example grep commands (adjust log file/path as needed):

```bash
# tail logs and filter embedding-related messages
tail -n 500 /path/to/writer.log | grep -E "embed|DLQ|initial_batch: snapshot left without embedding|compute_embeddings_for_payloads" -i
```

Attach the smallest useful log excerpt (5–50 lines) that shows the failure or
DLQ write.

---

## Inspect TEI (embedding service) interactions

If you run a local TEI (or can proxy the request), confirm that the writer
is sending expected JSON and TEI returns a vector for each input.

1) Quick smoke curl against TEI to understand response shape:

```bash
# Example request body (single input)
curl -sS -X POST "$HZ_TEI_BASE/embeddings" \
  -H "Content-Type: application/json" \
  -d '{"input": "fn foo() { println!(\"hi\"); }"}' | jq
```

2) If TEI returns 413/oversize responses, the writer attempts chunking and
retries; capture a failing request/response pair and include it in the issue.
If you cannot capture live traffic, start a local debug TEI (or a simple HTTP
server) to log incoming payloads and responses.

Note: the writer expects TEI or OpenAI-style JSON. Include the raw TEI
response when filing an issue.

---

## Verify Tree-sitter / indexer extraction

Most missing embeddings are caused by missing `source_content` for entities.
Verify the indexer produced `source_content` for the affected stable_ids by
running the indexer locally and exporting a small set of payloads.

1) Run the indexer (single-repo) and persist the produced payloads (fast way):

```bash
# Example: build payloads with the repo-index library API and write to JSONL
# (adapt to your local repo path)
RUST_LOG=debug cargo run -p hyperzoekt --bin hyperzoekt-indexer -- --root /path/to/repo --index-once
# or call a small helper program that uses RepoIndexService to write JSONL
```

2) Inspect sample payload objects for `source_content` presence:
- File entities: `source_content` may be stored on snapshot rows
- Methods/functions: `methods[].source_content` should contain the snippet

If `source_content` is empty in the indexer output, the problem is upstream
in the Tree-sitter extraction (file/line slicing or missing ranges).

---

## What to include when opening an issue

Minimum useful information:
- Short description of symptom ("N of M entities missing embeddings")
- Environment variables (HZ_EMBED_MODEL, HZ_TEI_BASE, HZ_ENABLE_EMBED_JOBS,
  HZ_EMBED_DLQ_PATH)
- Small log excerpt showing the DLQ write or `snapshot left without embedding`
- Relevant DLQ line(s)
- SurrealDB query result for the affected rows (JSON)
- Affected `stable_id`(s) and repo name(s)
- TEI response (if available) or note that TEI wasn't contacted

Optional but helpful:
- A minimal reproduction repo or commands to reproduce locally
- Any recent changes to Tree-sitter grammars, indexer options, or line/byte
  slicing code

---

## Quick checklist for triage engineers

1. Confirm HZ_EMBED_MODEL set and TEI reachable.
2. Check DLQ for failing entries and copy lines.
3. Run SurrealDB queries for empty embedding rows.
4. Collect writer logs (RUST_LOG=debug) around the time of failure.
5. If `source_content` is empty in the indexer output, open the issue and
   mark it upstream (indexer/tree-sitter) with the sample payload.

---

If you'd like, maintainers can add a `scripts/collect-embed-triage.sh` helper
that automates many of the commands above. Open a follow-up PR and I can help
add that.
