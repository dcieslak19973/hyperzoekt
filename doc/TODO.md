# TODO for hyperzoekt

1. Extract the tree-sitter work from the `goose` PR: https://github.com/block/goose/pull/3389
2. Figure out how to store the results of a tree-sitter run into a hypergraph
3. Create some combination of UI, CLI, and MCP (both stdio and streaming http)
4. Port zoekt (https://github.com/sourcegraph/zoekt)
5. Blend the zoekt port with the hypergraph
6. Add support for indexing multiple repos (BitBucket, GitLab, GitHub)
7. Add support for syncing permissions from BitBucket and GitLab into the hypergraph to support a multiuser environment
