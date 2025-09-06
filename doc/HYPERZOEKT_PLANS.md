# Hyperzoekt Development Plan

## Overview
Hyperzoekt extends the zoekt ecosystem to provide semantic enrichment of indexed repositories. It combines fast trigram-based indexing with AST extraction, call-graph analysis, and PageRank-style ranking to surface higher-quality search results and usage information.

This document summarizes the architecture, trade-offs, and an implementation roadmap for incremental development. It assumes Hyperzoekt can build on `zoekt-rs` and `zoekt-distributed` rather than re-implementing core indexing logic.

## Core Objectives
- **Semantic Enrichment**: Extract ASTs, build call graphs, compute PageRank for code entities (functions, classes, etc.).
- **Preserve Performance**: Leverage zoekt-distributed's fast repo processing; avoid bottlenecks in hyperzoekt.
- **Modularity**: Keep zoekt-rs and zoekt-distributed "pure" zoekt; hyperzoekt as a separate layer.
- **Scalability**: Support distributed deployment via Docker, with multiple entry points (admin, search, MCP).
- **DRY Principle**: Reuse existing AST parsing from zoekt-rs; no duplication.

## Architecture
### High-Level Flow
1. **Input**: zoekt-distributed processes repos via `RepoIndexService::build(root)`, emitting events or exposing APIs.
2. **Enrichment**: Hyperzoekt consumes data (via events or direct calls), extracts/enriches ASTs, and stores results.
3. **Output**: Semantic search APIs (HTTP, MCP) for queries like "find usages" or "ranked code".

### Components
- **zoekt-distributed (Unchanged)**:
  - Handles repo ingestion, indexing, and search.
  - Emits events at repo/file/AST levels (optional, for decoupling).
  - Public API: `RepoIndexService::build(root)`, `search(query)`.

- **zoekt-rs (Unchanged)**:
  - Core library for indexing, trigram search, and AST parsing.
  - Hyperzoekt reuses AST extraction to avoid re-parsing.

- **hyperzoekt (New/Extended)**:
  - **Dependencies**: `zoekt-rs` and/or `zoekt-distributed` (workspace paths).
  - **Modules**:
    - `src/enrichment_service.rs`: Orchestrates pipeline; calls zoekt-distributed for indexing.
    - `src/ast_extractor.rs`: Reuses zoekt-rs for AST (if needed for extension).
    - `src/call_graph.rs`: Builds graph from AST (using `petgraph`).
    - `src/pagerank.rs`: Computes importance scores.
    - `src/event_listener.rs`: (If event-driven) Subscribes to zoekt-distributed events.
    - `src/service.rs`: Main API for enriched search.
  - **Bins** (Multi-Entry-Point Docker):
    - `hz-admin`: Management/config.
    - `hz-http-search`: HTTP endpoint for semantic queries.
    - `hz-mcp-search`: MCP handler for AI tools.
    - `hz-enricher`: Background processor for enrichment.
  - **Storage**: Enriched data in SurrealDB or custom format; separate from zoekt-distributed.

### Integration Options
- **Direct Dependency (Preferred for Simplicity)**: Hyperzoekt calls zoekt-distributed's APIs directly. E.g., `let index = zoekt_distributed::RepoIndexService::build(root);` then enrich.
- **Event-Driven (For Decoupling)**: zoekt-distributed emits events (via Redis pub/sub or channels) with AST/file data. Hyperzoekt listens and processes. See `EVENT_PAYLOAD_TRADEOFFS.md` for payload considerations.
- **Hybrid**: Use direct calls for core, events for async notifications.

## Implementation Steps
1. **Setup Dependencies**: Add `zoekt-rs` and `zoekt-distributed` to `crates/hyperzoekt/Cargo.toml`.
2. **Prototype Enrichment**: Build `enrichment_service.rs` to call zoekt-distributed and extract AST via zoekt-rs.
3. **Add Semantic Modules**: Implement `call_graph.rs` and `pagerank.rs` using AST data.
4. **Event System (If Chosen)**: Add `src/events.rs` to zoekt-distributed for emission; `event_listener.rs` to hyperzoekt.
5. **APIs and Bins**: Extend `service.rs` for search; add bins in `src/bin/`.
6. **Docker**: Update `docker/docker-compose.yml` with hyperzoekt services (depends_on zoekt-distributed).
7. **Testing**: Unit tests for modules; integration with test repos.
8. **Validation**: Benchmark against zoekt-distributed; ensure no regressions.

## Trade-offs and Considerations
- **Event Payloads**: See `EVENT_PAYLOAD_TRADEOFFS.md` for including AST/file contents (benefits: no re-read/re-compute; drawbacks: size overhead).
- **Performance**: Enrichment may be slower than raw indexing; optimize with async/parallel processing.
- **Multi-Language**: Inherits from zoekt-rs; expand if needed (e.g., tree-sitter).
- **Storage**: Enriched indexes could be large; use efficient serialization.
- **Versioning**: Keep APIs stable; handle dependency updates carefully.

## Milestones
- **Phase 1**: Dependency setup and basic enrichment pipeline.
- **Phase 2**: Call graph and PageRank implementation.
- **Phase 3**: APIs, bins, and Docker integration.
- **Phase 4**: Testing, benchmarking, and docs updates.

This plan provides a roadmap for hyperzoekt while respecting the existing codebase. Iterate as needed based on prototyping results.
