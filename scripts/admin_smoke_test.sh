#!/usr/bin/env bash
set -euo pipefail

# Simple automated smoke test for the admin UI + poller + Redis.
# - Logs in with credentials
# - Fetches index page to extract CSRF token
# - Posts /create with a unique repo name and wildcard branches
# - Verifies that zoekt:repo_meta contains the repo
# - Lists zoekt:repo_branches before and after waiting for the poller to run

ADMIN_URL=${ADMIN_URL:-http://127.0.0.1:7878}
REDIS_PORT=${REDIS_PORT:-7777}
USER=${ZOEKT_ADMIN_USERNAME:-admin}
PASS=${ZOEKT_ADMIN_PASSWORD:-password}
POLL_WAIT=${POLL_WAIT:-6}    # seconds to wait for poller expansion
COOKIEFILE="/tmp/admin_smoke_cookies_$$.txt"
INDEX_HTML="/tmp/admin_smoke_index_$$.html"
CREATE_RESP="/tmp/admin_smoke_create_$$.txt"

trap 'rm -f "$COOKIEFILE" "$INDEX_HTML" "$CREATE_RESP"' EXIT

echo "Admin URL: $ADMIN_URL"

echo "1) Logging in as $USER"
curl -s -c "$COOKIEFILE" -X POST -d "username=$USER&password=$PASS" "$ADMIN_URL/login" -o /dev/null

if [ ! -s "$COOKIEFILE" ]; then
  echo "ERROR: login did not produce a cookie file"
  exit 1
fi

# Unique repo name
NAME="smoke-$(date +%Y%m%dT%H%M%S)-$RANDOM"
# Randomize REPO_URL by default to avoid collisions; can be overridden via env REPO_URL
REPO_URL=${REPO_URL:-"https://github.com/example/repo-$(date +%s)-$RANDOM.git"}
# Allow overriding branch pattern via environment; default to features/*
BRANCHES_PATTERN=${BRANCHES_PATTERN:-"features/*"}
FREQ=60

echo "2) Fetching index to extract CSRF token"
curl -s -b "$COOKIEFILE" "$ADMIN_URL/" -o "$INDEX_HTML"
CSRF=$(sed -n 's/.*name="csrf" value="\([^"]*\)".*/\1/p' "$INDEX_HTML" | head -n1 || true)

if [ -z "$CSRF" ]; then
  echo "ERROR: failed to extract CSRF token from index page"
  echo "--- index html (tail 80) ---"
  tail -n 80 "$INDEX_HTML" || true
  exit 2
fi

echo "Extracted CSRF: $CSRF"

echo "3) POSTing /create (repo=$NAME, branches=$BRANCHES_PATTERN)"
START_TS=$(date +%s%3N)
curl -s -S -i -b "$COOKIEFILE" \
  --data-urlencode "name=$NAME" \
  --data-urlencode "url=$REPO_URL" \
  --data-urlencode "frequency=$FREQ" \
  --data-urlencode "branches=$BRANCHES_PATTERN" \
  --data-urlencode "csrf=$CSRF" \
  "$ADMIN_URL/create" -o "$CREATE_RESP" || true
END_TS=$(date +%s%3N)
ELAPSED_MS=$((END_TS-START_TS))

echo "Create request elapsed: ${ELAPSED_MS}ms"

echo "--- Create response head ---"
head -n 40 "$CREATE_RESP" || true

# Check for success (201) or redirect
if grep -q "HTTP/1.1 201" "$CREATE_RESP" || grep -q "HTTP/1.1 303" "$CREATE_RESP" || grep -q "HTTP/1.1 302" "$CREATE_RESP"; then
  echo "Create appears successful (status 201/303/302)."
else
  echo "Create did not return success. Inspecting response..."
  cat "$CREATE_RESP"
  # If conflict, still continue to verify that the repo exists in Redis
fi

echo "4) Inspecting Redis: zoekt:repo_meta HGET $NAME"
redis-cli -p "$REDIS_PORT" HGET zoekt:repo_meta "$NAME" || true

echo "5) zoekt:repo_branches keys (before wait) matching $NAME"
redis-cli -p "$REDIS_PORT" HKEYS zoekt:repo_branches | grep "$NAME" || true

if [ "$POLL_WAIT" -gt 0 ]; then
  echo "6) Waiting $POLL_WAIT seconds for poller to expand wildcard patterns..."
  sleep "$POLL_WAIT"
fi

echo "7) zoekt:repo_branches keys (after wait) matching $NAME"
redis-cli -p "$REDIS_PORT" HKEYS zoekt:repo_branches | grep "$NAME" || true

# Final quick check: ensure meta exists
META=$(redis-cli -p "$REDIS_PORT" HGET zoekt:repo_meta "$NAME" || true)
if [ -z "$META" ] || [ "$META" = "(nil)" ]; then
  echo "ERROR: zoekt:repo_meta entry not found for $NAME"
  exit 3
fi

echo "Smoke test completed. Repository meta present."

# Determine if poller expanded branches (best-effort)
BRANCH_KEYS_COUNT=$(redis-cli -p "$REDIS_PORT" HKEYS zoekt:repo_branches | grep -c "$NAME" || true)
if [ "$BRANCH_KEYS_COUNT" -gt 0 ]; then
  echo "Found $BRANCH_KEYS_COUNT branch entries for $NAME in zoekt:repo_branches"
else
  echo "No branch entries found for $NAME in zoekt:repo_branches. Poller may not have expanded branches yet or repo URL unreachable."
fi

exit 0
