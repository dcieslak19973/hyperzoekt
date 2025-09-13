# Docker notes

This compose file now runs a dedicated admin poller service that periodically expands repository branch patterns and writes per-branch entries into Redis (`zoekt:repo_branches`).

Services related to the new poller:

- `zoekt-admin-poller` — the new process that runs the poller and exposes two observability endpoints on port `9900`:
  - `GET /health` — returns 200 when Redis is reachable (poller can connect to Redis), otherwise 503.
  - `GET /metrics` — Prometheus-style text with a few basic counters:
    - `dzr_poller_polls_run_total` — total poll attempts
    - `dzr_poller_polls_failed_total` — failed poll operations
    - `dzr_poller_last_poll_unix_seconds` — unix timestamp of last poll

The poller container is built from the same project Dockerfile and will start as a separate process. The compose file maps port `9900` on the host so you can inspect metrics locally.

- `poller-prober` — a lightweight curl-based container used by docker-compose to probe the poller's `/health` endpoint and determine service readiness.

Notes and troubleshooting:

- The poller depends on Redis via `depends_on` and the Redis service healthcheck. If your environment changes Redis address, set `REDIS_URL` for the poller (same as other services).

- You can override the poll interval by setting `ZOEKTD_POLL_INTERVAL_SECONDS` in the compose environment for `zoekt-admin-poller`.

- To view metrics locally after bringing up the stack:

```bash
# Build and start services
docker compose up --build

# Check health
curl -v http://localhost:9900/health

# Get metrics
curl http://localhost:9900/metrics
```

If you want the poller to be probed from outside (for example in Kubernetes), adapt the readiness/liveness probes to call `/health` or `/metrics` accordingly.
