Testing notes
--------------

The `zoekt-distributed` crate and its integration tests use Redis. To avoid
cross-test interference when running tests against a shared Redis instance,
you can set `TEST_REDIS_DB` (or `REDIS_DB`) to a numeric DB index. When set,
the test harness will append `/<db>` to `REDIS_URL` (or to constructed default
URLs), isolating test data in that DB.

Example (local run):

```bash
TEST_REDIS_DB=13 REDIS_URL=redis://127.0.0.1:7777 cargo test -p zoekt-distributed --lib
```

This keeps production usage unchanged and only affects test runs that set the
environment variable.
