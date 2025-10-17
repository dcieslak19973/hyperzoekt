# Example LLM chain: using `hirag_retrieve` + `fetch_entity_snapshot`

This document shows a concrete example of how to call the MCP tools provided by hyperzoekt to answer a code question using HiRAG retrieval and entity snapshot lookups.

Goal
-----
Given a natural language question about the codebase, we will:
1. Call `hirag_retrieve` to get hierarchical clusters and important entities.
2. Select a small set of member stable_ids to inspect.
3. Call `fetch_entity_snapshot` for those stable_ids to obtain `source_content` and metadata.
4. Build a concise LLM prompt that includes cluster summaries and small source snippets and ask the model to answer the question while citing specific entities and repo names.

RPC calls
---------
1) Call `hirag_retrieve`:

Request (JSON-RPC-like):
{
  "jsonrpc": "2.0",
  "method": "tools/call",
  "params": {
    "name": "hirag_retrieve",
    "arguments": { "q": "How is configuration validated when loading a repo?", "top_k": 3 }
  },
  "id": 1
}

Response (abbreviated):
- `result` -> contains `content` which is an array; take `content[0].text` and parse it as JSON.

Parsed `hirag_retrieve` JSON shape (toy example):
{
  "top_entities": ["ent::cfg::loader::validate", "ent::repo::open"],
  "clusters": [
    {
      "label": "Config parsing and validation",
      "summary": "Functions that parse config files and coerce types; they raise errors when required fields are missing.",
      "members": ["ent::cfg::parser::parse_file", "ent::cfg::loader::validate"],
      "member_repos": ["repoA", "repoA"]
    },
    {
      "label": "Repository open paths",
      "summary": "Entry points for opening repositories and resolving refs.",
      "members": ["ent::repo::open"],
      "member_repos": ["repoB"]
    }
  ],
  "bridge": [],
  "member_fetch_tool": "fetch_entity_snapshot"
}

2) Choose members to inspect
---------------------------
Pick up to N members from clusters you consider relevant. For example: `ent::cfg::loader::validate` and `ent::repo::open`.

3) Call `fetch_entity_snapshot` for each stable_id
--------------------------------------------------
Request:
{
  "jsonrpc": "2.0",
  "method": "tools/call",
  "params": {
    "name": "fetch_entity_snapshot",
    "arguments": { "stable_id": "ent::cfg::loader::validate" }
  },
  "id": 2
}

Response `content[0].text` (JSON string):
{
  "stable_id": "ent::cfg::loader::validate",
  "repo_name": "repoA",
  "file": "src/config/loader.rs",
  "start_line": 120,
  "source_display": "src/config/loader.rs",
  "source_content": "fn validate(cfg: &Config) -> Result<(), Error> { ... }"
}

Paging and slicing notes
------------------------
- To avoid fetching very large `source_content` blobs, `fetch_entity_snapshot` supports optional `start` and `limit` parameters in its input. These request an offset and maximum length (in bytes or characters; server semantics may vary) so clients can fetch only a small snippet.
- When `start`/`limit` are used the tool may return the fields `snippet`, `snippet_start`, `snippet_end`, and `total_len` instead of `source_content`. `snippet` contains the requested slice. `total_len` is the length of the full content when known.
- If more content remains beyond the returned slice the MCP server indicates that by setting a top-level `next_cursor` string on the JSON-RPC result. Internally the handler may append a hidden `ToolContent::text("__NEXT_CURSOR__:<offset>")` marker which the MCP server extracts and exposes as `next_cursor` to the caller. Clients should use `next_cursor` as the next `start` offset to continue paging.

Example: request the first 200 bytes of a snapshot

Request arguments:
{
  "stable_id": "ent::cfg::loader::validate",
  "start": 0,
  "limit": 200
}

If the snapshot has length 1500 bytes, the MCP response will contain the JSON text payload with `snippet`, `snippet_start=0`, `snippet_end=200`, `total_len=1500`, and a `next_cursor` value of "200" indicating there is more to fetch.

4) Build an LLM prompt using cluster summaries + small snippets
--------------------------------------------------------------
- Include a short instruction to "use only the facts from the following cluster summaries and code snippets; cite stable_ids and repo names in your answer".
- Keep each snippet short (a few lines). If snapshots are long, fetch only the needed ranges or include trimmed content.

Prompt template (system + user):

System prompt (guidance):
You are a code-savvy assistant. Answer using only the provided cluster summaries and code snippets. Whenever you assert a fact from the code, cite the stable_id and repo name in square brackets. If the evidence is insufficient, answer "INSUFFICIENT_INFO".

User prompt (with embedded evidence):
Question: How is configuration validated when loading a repo?

HiRAG cluster summaries:
- Config parsing and validation: Functions that parse config files and coerce types; they raise errors when required fields are missing.
- Repository open paths: Entry points for opening repositories and resolving refs.

Selected code snippets:
- ent::cfg::loader::validate [repoA]
  ```rust
  fn validate(cfg: &Config) -> Result<(), Error> {
      if cfg.version.is_none() {
          return Err(Error::MissingField("version".into()));
      }
      // other checks...
  }
  ```

- ent::repo::open [repoB]
  ```rust
  fn open(path: &Path) -> Repo {
      let cfg = load_config(path.join("config.yml"));
      validate(&cfg)?; // calls into cfg::loader::validate
      // repo open logic
  }
  ```

Task: Answer the question in 3-5 concise sentences and list up to 3 code locations (stable_id + repo_name + short reason) you'd inspect for implementing a fix.

5) Example LLM output (desired):
- Answer (short): "Configuration validation is performed by `ent::cfg::loader::validate`, which checks required fields like `version` and returns an error when missing [ent::cfg::loader::validate, repoA]. The repository open path calls `validate` after loading the config, so repo-level errors surface during open [ent::repo::open, repoB]." 
- Code locations to inspect:
  1. ent::cfg::loader::validate (repoA) — implements field checks and error returns.
  2. ent::repo::open (repoB) — loads config and invokes validation.
  3. ent::cfg::parser::parse_file (repoA) — ensure parser produces the required fields for validation.

Notes and tips
-------------
- Keep the LLM prompt concise. Too much raw JSON can make the model noisy; prefer selecting cluster summaries and 1-3 short snippets to ground answers.
- Use the `member_fetch_tool` field to programmatically discover which tool to call for fetching member snapshots.
- Consider adding a small client wrapper that automates the chain: call `hirag_retrieve`, pick members by cluster relevance, call `fetch_entity_snapshot`, build prompt, call your LLM.

If you'd like, I can:
- Add an example client script (Python or JS) that executes this chain against the MCP endpoint and produces the final LLM prompt automatically.
- Implement `fetch_entity_snapshot` slicing (start/end) so clients can request only a small snippet instead of whole source_content.

*** End of example file
