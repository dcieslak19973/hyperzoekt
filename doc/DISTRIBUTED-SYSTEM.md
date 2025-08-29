# In-memory serving fleet (shards only live in memory)

This document describes a single, consistent architecture for an in-memory-only serving fleet: no job queue, no object store, no persistent shard artifacts — shards live only in memory on one or more fleet instances. The design focuses on safe ownership/claiming, autoscaling, eviction, and operational rules.

Status
- Draft / actionable design. The goal is a coherent, minimal system where nodes self-claim repos, index them into memory, and serve queries from in-memory indices.

Goals
- Run an autoscalable fleet of instances that collectively hold indexes for many repos (example: 500). Each repo is owned and served by one (or more) nodes in memory.
- Avoid persistent shard artifacts: no object store, no disk-based shard persistence required for normal operation.
- Support safe failover: when a node dies, another node can re-claim and reindex the repo.

Core components
- Catalog (lightweight authoritative store): a small transactional store (Postgres, etcd, Consul, DynamoDB) that holds per-repo ownership and lease information. This is the only durable component.
- Fleet nodes (index+serve): each node claims repos from the catalog, performs shallow clone/indexing, stores the index in memory, and serves queries for repos it owns.
- Optional shared git mirror/cache: speeds up clones and reduces external git server load. Not required for correctness but strongly recommended for performance.

Why no persistence?
- Simplicity: fewer moving parts and operational burden (no object store uploads, no artifact lifecycle).
- Lowest query latency: memory-resident indexes avoid mmap/page faults and give the fastest query response.
- Elasticity: nodes can claim and drop repos dynamically; autoscaling adjusts total memory available.

Ownership & claim model (catalog-based, recommended)
- Catalog row per repo (single authoritative source): { repo_id, owner_id, lease_expires, commit_hash, ready, last_indexed_at, size_bytes }
- Claim protocol (atomic CAS): node attempts to claim a repo by updating the catalog row only if owner is NULL or lease_expires < now. On success the node is owner for a TTL.
- Heartbeat: owner renews lease periodically (interval << TTL). If lease expires the repo becomes claimable by others.
- Voluntary release: node can release a claim (e.g., during graceful drain) so others can take over immediately.

Index lifecycle (claim → index → serve)
1. Node discovers a claimable repo in the catalog and attempts to claim it.
2. On successful claim, node determines latest commit (catalog or git server) and checks local cache for a matching in-memory index.
3. If no valid in-memory index exists, node performs a shallow clone (or uses mirror cache) and runs the indexer to build the in-memory index.
4. Node marks the repo row as ready and begins serving queries for that repo.
5. Node heartbeats the claim while serving. If the node plans to drain, it can release the claim or mark the repo draining for quick takeover.

Serving & routing
- Catalog maps repo -> owner node. Queries are routed to the owner. If owner is not ready, the caller gets a "warming" response or is routed to a fallback/replica if available.
- For scale, use a small routing tier (envoy/nginx) or a lightweight service discovery that reads the catalog to map repo -> owner.

Lease, handoff and graceful drain
- Lease TTLs should be short (e.g., 30s–2m) with heartbeats at a fraction of TTL.
- Drain flow: node sets repo state to draining, finishes in-flight queries, optionally streams index to a new owner (advanced), then releases claim.
- If a node crashes, the lease expires and another node claims + reindexes.

Eviction and memory management
- Each node maintains a configurable memory budget for in-memory indices.
- Eviction policy: LRU by last-served time or a custom cost metric (size × traffic). Evicted repos are released in the catalog so other nodes can claim and reindex them.
- Pinning: provide a mechanism to mark specific repos as pinned (non-evictable) when needed.

Autoscaling & placement
- Scale-up triggers: high count of unclaimed repos, high average claim latency, or CPU/memory saturation across the fleet.
- Scale-down: sustained low claim count and low CPU; but perform graceful drain before terminating nodes to avoid losing many claims at once.
- Placement: prefer replica placement close to git mirror/cache or client traffic for lower latency.

Replication & high availability (optional)
- If reindex-on-fail is too expensive, implement N-way replication: a repo can be owned by N nodes (primary + replicas). Replicas index proactively and stand ready to serve if primary fails.
- Replication increases memory usage but decreases failover reindex cost and improves read availability.

Failure modes & mitigations
- Node crash: lease expiry allows another node to claim and reindex. Mitigation: keep local persistent repo mirror or use shallow fetch to speed reindex.
- Thundering reindex after outage: add randomized claim jitter, exponential backoff, and limit concurrent reindexes per node.
- Split-brain: atomic CAS claims in the catalog prevent dual-ownership; keep TTLs small and require owner_id checks for operations.
- Corrupted in-memory index: detect via checksums or simple health checks; if corrupted, reindex from source.

Operational concerns
- Repo clones: use shared mirrors or per-node persistent cache to avoid repeated full clones.
- Secrets: store git credentials securely (vault, k8s secrets); nodes use short-lived tokens to access repos.
- Observability: per-repo and per-node metrics (index_time_ms, memory_bytes, last_served_at, claim_latency). Tracing across clone → index → serve is critical.

When to choose this design
- Choose in-memory-only fleet when:
  - Low-latency serving is paramount and reindex cost is acceptable.
  - You prefer simpler operation without artifact storage and lifecycle management.
  - You can autoscale to provide enough aggregate RAM to cover the working set (hot repos), and you accept reindexing cold repos on failover.

When not ideal
- Avoid if per-repo index builds are extremely expensive and reindex-on-fail is unacceptable, or if you cannot afford the memory footprint at scale.

Prototype & next steps
1. Implement a minimal catalog API and a single-node claiming simulation to validate claim/lease semantics and reindex timings.
2. Add per-node in-memory cache with eviction and a safe release flow (release catalog row on eviction).
3. Run a simulation with 500 fixture repos of varying sizes to tune TTLs, clone/parallelism, and autoscaling thresholds.

Open questions
- How many replicas (if any) should we allow per-repo? (N=1 default)
- What is the acceptable reindex time for a typical repo (SLO) to guide eviction and autoscaling policies?
- Do we want optional snapshotting to disk for very large repos to speed rehydration on restart (still optional, outside normal ops)?

Last updated: 2025-08-28
