# Event Payload Trade-offs: Including AST or File Contents

## Overview
In an event-driven architecture for hyperzoekt (building on zoekt-distributed), we emit events at various processing levels (repo, file, AST). While keeping message sizes small is ideal for performance and scalability, there are benefits to including AST structures or full file contents in event payloads. This document outlines the trade-offs, focusing on scenarios where larger payloads may be justified.

## Benefits of Including AST or File Contents
- **Avoid Re-reading Files**: Receivers (e.g., hyperzoekt) don't need to re-access the original file system or repository. This reduces I/O overhead, especially in distributed setups where files might be on remote storage or cached temporarily.
- **Skip Re-computation of AST**: Since zoekt-rs already parses ASTs efficiently, embedding the parsed AST (e.g., as a serialized structure) prevents hyperzoekt from re-parsing. This saves CPU cycles and ensures consistency—no risk of parsing discrepancies due to different versions or configs.
- **Decoupling and Reliability**: The event becomes self-contained. Hyperzoekt can process asynchronously without depending on zoekt-distributed's state or file availability. Useful for fault tolerance (e.g., if files are deleted post-processing) or cross-service communication.
- **Faster Enrichment Pipeline**: Direct access to AST enables immediate call graph construction and PageRank computation, reducing latency in hyperzoekt's pipeline.
- **Multi-Language Flexibility**: Including raw file contents allows hyperzoekt to handle additional parsing (e.g., via tree-sitter) if needed, without assuming zoekt-rs's output covers all cases.
- **Debugging and Auditing**: Payloads with contents provide richer context for logging, monitoring, or troubleshooting events.

## Trade-offs and Mitigations
- **Message Size and Overhead**: Larger payloads increase network bandwidth, Redis memory usage, and serialization/deserialization time. Mitigate by:
  - Compressing payloads (e.g., using gzip in serde).
  - Batching events or using streaming for large files.
  - Selective inclusion: Only include contents for "interesting" files (e.g., based on language or size thresholds).
- **Memory and Storage**: Receivers must handle larger in-memory buffers. Use async processing and limits to prevent OOM.
- **Security/Privacy**: File contents might include sensitive data. Encrypt payloads or filter contents if needed.
- **Scalability**: In high-volume scenarios, prefer metadata-only events and let receivers fetch on-demand. Test with realistic loads.
- **Versioning**: AST structures must be backward-compatible; changes could break consumers.

## Recommendations
- **Default to Metadata-Only**: For most cases, emit lightweight events with IDs/paths, and let hyperzoekt fetch/recompute as needed.
- **Opt-In for Contents**: Add a config flag (e.g., in zoekt-distributed) to include contents/AST when benefits outweigh costs (e.g., in low-latency, trusted environments).
- **Hybrid Approach**: Emit AST summaries (e.g., function names, imports) for quick processing, with an option to fetch full contents via a separate API.
- **Measurement**: Prototype with sample repos to measure size vs. performance gains. Use tools like `cargo flamegraph` for profiling.

This approach balances efficiency with the need for rich data, ensuring hyperzoekt can leverage zoekt-distributed's work without unnecessary duplication.
