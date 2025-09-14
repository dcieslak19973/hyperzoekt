# Documentation Table of Contents

This file lists the primary documents in `doc/` and their main sections. Use it as a quick navigation aid.

## Documents

- [Usage and developer notes](USAGE.md)
  - Build
  - Run the indexer binary
  - Tests
  - Linting and formatting
  - Notes about Tree-sitter grammars
  - Troubleshooting
  - Embedded SurrealDB default behavior
  - OpenTelemetry / Tracing

- [Hyperzoekt Development Plan](HYPERZOEKT_PLANS.md)
  - Overview
  - Core Objectives
  - Architecture
    - High-Level Flow
    - Components
    - Integration Options
  - Implementation Steps
  - Trade-offs and Considerations
  - Milestones

- [SurrealDB Integration Design](SURREALDB-INTEGRATION.md)
  - Modes
  - Schema (starter)
  - Integration patterns
  - Operational considerations
  - Practical batching notes

- [Redis BRPOPLPUSH Message Queue Pattern](REDIS_MESSAGE_QUEUE.md)
  - Overview
  - Pattern Description
  - Implementation Details
    - Queue Structure
    - Message Flow
    - Key Components
      - EventConsumer
      - Recovery Mechanism
  - Configuration
  - Monitoring and Observability
  - Best Practices
  - Integration with Zoekt-Distributed

- [Event Payload Trade-offs](EVENT_PAYLOAD_TRADEOFFS.md)
  - Overview
  - Benefits of Including AST or File Contents
  - Trade-offs and Mitigations
  - Recommendations

- [Data model: SurrealDB mapping for hyperzoekt](DATA_MODEL.md)
  - Goals
  - Principle: use REFERENCEs for links
  - Core tables and fields
  - Permissions (read-only access model)
  - Graph queries you will want
  - Diagram

- [Auth and RBAC for remote repositories](AUTH_AND_RBAC.md)
  - Checklist
  - Goals
  - High-level approach
  - Provider API checks
  - User token model & storage
  - Caching and rate-limiting
  - Audit & logging

- [ZOEKT-BENCHMARKING](ZOEKT-BENCHMARKING.md)
  - Goals
  - Where the tools live
  - Bench JSON schema
  - Running a reliable benchmark
  - Interpreting results

- [Project Plans and Architecture (PLANS)](PLANS.md)

- [TODO list](TODO.md)

- [Hyperzoekt — User Guide](USER_GUIDE.md)
  - Prerequisites
  - Starting services locally
  - Using the Admin UI to add remote repositories
  - Confirming processing
  - Using the search UIs
  - Troubleshooting and security notes

If you want a richer, auto-generated TOC that includes deeper headings or links to specific subsections, I can generate a full HTML or Markdown TOC with anchors for every H2/H3 found across the `doc/` files.
