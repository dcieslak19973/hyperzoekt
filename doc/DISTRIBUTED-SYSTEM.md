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

Note: for the bidding-based prototype described below, initial lease acquisition is performed exclusively via the bidding system (see the "Bidding-based lease allocation" section). In other words:

- The only way to obtain an initial lease on a repo in the prototype is through the bidding flow.
- There is no alternate direct-acquire path (for example, a node cannot directly SET NX or CAS a catalog row to get the initial lease without first winning an auction).

The catalog-based model described here is presented as an alternative, general-purpose pattern for ownership/claims in a catalog-backed deployment, but it is not used as the initial-lease path in the bidding prototype.

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

## Bidding-based lease allocation (prototype)

The current prototype implements a light-weight bidding mechanism for indexer nodes to "bid" on repos they want to index. The goal is to favor nodes with more free memory for larger indexing work and to allow a short window where nodes can advertise capacity before a winner is chosen.

How it works (summary):
- When an indexer fails to acquire a lease immediately, it may place a single outstanding bid for that repo advertising its currently available free memory (bytes).
- Only one unaccepted bid per holder (node) is allowed at a time; attempts to bid while another bid is pending will be rejected.
- The first bid for a repo starts the auction clock. The auction window is 60 seconds after the first bid. After the window elapses the system evaluates all bids for the repo.
- Bids are ranked by available memory (higher is better). Ties are broken by bid time (earlier wins).
- The evaluator attempts an atomic SET NX of the lease key on behalf of the winning holder. If the SET NX succeeds the lease is assigned to the winner; otherwise the auction ends with no winner (another actor raced to acquire the lease).

Short-circuiting auctions (MAX_BIDS)
- To make auctions finish earlier in practice (especially in tests or small clusters) the prototype supports a short-circuit mechanism: when a configured number of bids for a repo is reached, the auction evaluation is triggered immediately instead of waiting for the full 60s window. The behaviour is controlled by the `ZOEKT_MAX_BIDS` configuration (environment variable). A common pattern is to set `ZOEKT_MAX_BIDS` to the number of indexer nodes in the cluster so the auction ends as soon as all indexers have placed their bids.

Dynamic MAX_BIDS for autoscaling
- Motivation: in a static cluster it's convenient to set `ZOEKT_MAX_BIDS` to the number of indexer nodes so auctions end as soon as all nodes have bid. In an autoscaling environment the number of indexers will change over time, so a fixed env var will either under-count (slow auctions) or over-count (never reach the threshold). To support autoscaling we must be able to update the effective `MAX_BIDS` value at runtime.

- Recommended approaches:
  - Heartbeat-based approach (recommended): each indexer periodically reports liveness into Redis and the controller (or each node) counts the number of indexers that have heartbeated within the last epoch (for example 30–60s). Use that count as the effective `max_bids`.
    - Implementation sketch (Redis ZSET):
      - Each indexer periodically runs: `ZADD cluster:indexers <unix_ts> <node_id>` (or updates its score to the current timestamp).
      - To compute active indexers in the last N seconds: remove old entries and count the remainder:
        - `ZREMRANGEBYSCORE cluster:indexers 0 <cutoff_ts>`
        - `ZCOUNT cluster:indexers <cutoff_ts> +inf` (or `ZCARD cluster:indexers` after trim)
      - This avoids using `KEYS` and is efficient for moderate cluster sizes.
    - Benefits: accurate, low-latency count; no global config update required when indexers scale; robust to indexer restarts.

  - Leader-driven push: have a leader (or controller) publish changes to a Redis pub/sub channel or a control topic; nodes subscribe and update their in-memory `max_bids` immediately on message receipt. This reduces polling load and is suitable when changes are infrequent but must be fast.
  - Orchestration rollout: use Kubernetes ConfigMap + rollout to update env var across all nodes. This is simple but causes a full rolling restart and is less suitable for frequent autoscaler-driven changes.

- Safety and behavior guarantees:
  - Grace window: when `MAX_BIDS` decreases, nodes that already placed bids should still be honored for the current auction window. Implement the update so it doesn't retroactively cancel ongoing auctions.
  - Backstop timeout: always keep the auction wall-clock window (60s) as a hard upper bound even when `MAX_BIDS` is large or misconfigured. This prevents auctions from hanging indefinitely if the dynamic config becomes unreachable.
  - Per-repo override: allow an optional per-repo max-bids override (configurable in the catalog) for special cases (very large repos or reserved workloads).
  - Metrics & alerts: emit metrics for configured_max_bids, bids_received, auctions_short_circuited, auctions_timed_out. Alert when configured_max_bids diverges significantly from observed active indexers or when many auctions are being short-circuited unexpectedly.

- Implementation notes:
  - Make the in-process `max_bids` an atomic integer or lock-guarded field that can be updated without restarting the node, and ensure reads are cheap in hot paths.
  - Apply updates outside of any held locks that would prevent the node from handling auction evaluation; prefer using small critical sections so a config update cannot deadlock bidding logic.
  - Coordinate config changes with the autoscaler/controller: when scaling up, increment the cluster `max_bids` value before new nodes register as indexers; when scaling down, decrement after nodes have drained and released leases.

- Example (leader push using Redis pub/sub):
  1. Controller updates `cluster:config:max_bids` in Redis and publishes `max_bids_changed` with the new value.
  2. Each node subscribes to `max_bids_changed`, receives the new value, and replaces its runtime `max_bids` atomically.
  3. Ongoing auctions continue to completion, new auctions use the updated threshold.

By making `MAX_BIDS` configurable at runtime and coordinating changes via a small control plane, auctions will remain responsive during autoscaling while preserving correctness and observability.

IMPORTANT: In this prototype the bidding flow is the authoritative and exclusive mechanism for obtaining an initial lease. Nodes must place bids and win an auction to become the initial holder for a repo. There is intentionally no alternative direct-acquire path available for initial lease assignment; any direct SET NX/CAS behavior that would give a node initial ownership is considered outside the prototype's rules and should not be used.
- The implementation supports both a Redis-backed mode (ZSET per-repo + per-holder pending keys + start keys) and an in-memory fallback for tests. The in-memory mode keeps per-repo bid lists and scheduled evaluation tasks.

Design rationale and constraints:
- Bidding gives a simple way to let nodes advertise capacity without central scheduling.
- The 60s window trades responsiveness for better packing decisions; it can be tuned or made configurable per-cluster.
- One-pending-bid-per-node reduces noisy re-bidding and keeps the bookkeeping small.

Future enhancements (planned):
1. Enforce a minimum bid for a previously-indexed repo based on its historical memory footprint. This avoids wasting time on nodes that can't possibly index the repo.
2. Trip an autoscaler if a repo remains unleased for more than N bidding epochs (e.g., 5 auctions) to trigger cluster scale-up.

Other considerations / gaps
- Race conditions: the evaluator does an atomic SET NX to claim the lease; if that fails (some other node acquired the key), the auction simply ends without a winner. We could optionally iterate to the next-best bid and retry, but that increases complexity and requires careful ordering to avoid livelock.
- Duplicate claims / split-brain: the canonical Redis-backed path uses atomic SET NX for the lease key, preventing dual-ownership for successful claims. However there is a small window where multiple nodes might think they will win (between evaluation and SET NX) — the atomic SET NX ensures only one actually holds the lease.
- Auction scheduling duplication: the current prototype schedules auction evaluation in-process. In a multi-process cluster this could lead to multiple evaluators running for the same repo. A production design should use a distributed leader/election or a Redis-based lock (SETNX with TTL) to ensure exactly-one evaluator.
- Member encoding: the Redis prototype encodes bid members as `holder|ts`. This is sufficient for the prototype but could be replaced with a robust serialization (JSON or base64) to avoid parsing issues when holder ids contain the separator.
- Pending-bid cleanup: pending markers (per-holder keys) expire after the auction window plus a small buffer; if an auction is canceled or a winner accepted, the implementation attempts to delete the pending marker. In failure scenarios it's possible a holder's pending marker remains set briefly and prevents new bids; the expiry ensures eventual recovery.
- Starvation / unpicked repos: a repo may fail to be picked if:
  - No nodes bid (e.g., all nodes are saturated or misconfigured). This can be detected by counting empty auctions and triggering autoscaling or operator alerts.
  - All bids fail the final SET NX because another non-bid acquisition occurred (e.g., direct acquisition by a node that didn't bid). This is a benign race — the repo will be available for future auctions.
- Multi-claim scenario: multiple indexers should not be able to hold the same lease because we rely on an atomic key (SET NX) in Redis. In the in-memory fallback used for tests, the code also checks existing lease expiry and inserts into a shared in-process map. For production correctness, use the Redis-backed path.
- Auction evaluation frequency and cost: evaluating auctions requires reading ZSET members and attempting SET NX. Ensure these operations are rate-limited and monitored to avoid creating load on Redis during large-scale bidding.

Recommendations
- Use the Redis-backed mode in production and implement a small distributed lock for auction evaluation to ensure single-evaluator semantics.
- Add metrics: auctions_started, auctions_won, auctions_no_winner, bids_placed, bids_rejected_already_pending, bid_window_ms (histogram). These will make tuning easier.
- Consider allowing a secondary pass to try the next-best bid if the top bid fails SET NX, for better resilience when races occur.

If you'd like, I can add a small section with example Redis key names and a suggested Lua script for winner selection + SET NX to make the final acceptance atomic and compact.

Last updated: 2025-08-28
