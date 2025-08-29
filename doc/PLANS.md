# Project Plans and Architecture

## Overview
This document records the evolving plans and architectural decisions for the hyperzoekt project.

## Hypergraph Data Model
- Store results of tree-sitter traversals as nodes and hyperedges.
- Represent code entities, relationships, and repository structure.
- Extend the hypergraph to include:
  - Users, groups, and memberships
  - Access levels and permissions for repositories
  - Mapping code entities to repositories
  - Pull requests (PRs) and diffs, linked to code and users

## Goals
- Enable rich queries and analysis (permissions, code ownership, change tracking)
- Support multiuser environments
- Facilitate advanced UI/CLI features

## Next Steps
- Define initial hypergraph schema
- Prototype tree-sitter integration
- Plan for repository and user data ingestion

---

This document will be updated as the project progresses.

## SurrealDB pivot (short summary)

Decision: pivot from an in-repo hypergraph representation toward persisting entities and edges in SurrealDB.

Rationale: SurrealDB offers a flexible document+graph model, indexes, and query power that simplify cross-repo graph queries and make operationalizing the index easier (single store for entities, files, and edges).

Planned approach: have `hyperzoekt` write directly to SurrealDB, upserting records as they are produced by the Tree-sitter indexer; provide an option to run an embedded SurrealDB instance for local dev and an option to connect to remote SurrealDB for production. JSONL export will be supported only for debugging and offline inspection.

See `doc/SURREALDB-INTEGRATION.md` for the full design and migration steps.

---

## Additional Goals

These ideas will need some refinement but for now:

1. Stack trace analysis
   A. Especially around determining whether an error was caused by a change in code (internal) or by a change in data/environment (external)
2. Use HiRAG to build a KG about the code (and documentation in the repo in the form of markdown, etc.)
   A. Leave the HiRAG open ended so a user or other system could present additional information (e.g. JIRA tickets, Emails/Slack conversations about the code, etc.)
3. Calculate Code Churn and Defect Density for analysis/storage in the KG