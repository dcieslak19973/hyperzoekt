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
