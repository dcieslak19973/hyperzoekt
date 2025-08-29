zoekt-rs (minimal)

A tiny, in-memory Zoekt-like indexer/searcher to prototype blended search in the hyperzoekt workspace.

Status: experimental. Not feature-complete and not compatible with Go Zoekt indices.

API:
- IndexBuilder::new(repo_root).build() -> InMemoryIndex
- Searcher::new(&index).search(Query::Literal/Regex)
