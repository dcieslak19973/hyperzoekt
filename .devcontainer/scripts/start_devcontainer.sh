#!/usr/bin/env bash
set -euo pipefail

# Ensure cargo bin dir is on PATH
export PATH="$HOME/.cargo/bin:$PATH"

# Start sccache server using workspace-mounted cache dir (idempotent)
SCCACHE_DIR=/workspaces/hyperzoekt/.sccache
mkdir -p "$SCCACHE_DIR"
# Ensure a writable CARGO_HOME for the vscode user
CARGO_HOME_DIR="/home/vscode/.cargo"
mkdir -p "$CARGO_HOME_DIR"
chmod 0777 "$CARGO_HOME_DIR" || true
if id -u vscode >/dev/null 2>&1; then
	sudo chown -R vscode:vscode "$CARGO_HOME_DIR" || true
fi
# ensure ownership is set to the vscode user so builds don't produce root-owned files
if id -u vscode >/dev/null 2>&1; then
	sudo chown -R vscode:vscode /workspaces/hyperzoekt/.sccache || true
	sudo chown -R vscode:vscode /workspaces/hyperzoekt/target || true
fi
## Ensure the target directory exists and is owned before any builds to reduce races
# Prefer a container-local target dir to avoid workspace-mounted filesystem races
export CARGO_TARGET_DIR="/tmp/hyperzoekt-target"
mkdir -p "$CARGO_TARGET_DIR"/debug/deps || true
chmod 0777 "$CARGO_TARGET_DIR" || true
if id -u vscode >/dev/null 2>&1; then
	sudo chown -R vscode:vscode "$CARGO_TARGET_DIR" || true
fi
# allow the vscode user and processes to write into the workspace cache
chmod 0777 "$SCCACHE_DIR" || true
# start sccache as the vscode user
if id -u vscode >/dev/null 2>&1; then
  # Use absolute sccache binary installed in /usr/local/bin to avoid PATH/sudo issues
  SCCACHE_BIN="/usr/local/bin/sccache"
  if [ ! -x "$SCCACHE_BIN" ]; then
    SCCACHE_BIN="$(command -v sccache || true)"
  fi
  if [ -n "$SCCACHE_BIN" ]; then
    # Disable preprocessor/C caching by setting SCCACHE_DIRECT=false when starting the server
    sudo -u vscode env SCCACHE_DIR="$SCCACHE_DIR" SCCACHE_DIRECT=false "$SCCACHE_BIN" --stop-server >/dev/null 2>&1 || true
    sudo -u vscode env SCCACHE_DIR="$SCCACHE_DIR" SCCACHE_DIRECT=false "$SCCACHE_BIN" --start-server || true
    sudo -u vscode env SCCACHE_DIR="$SCCACHE_DIR" SCCACHE_DIRECT=false "$SCCACHE_BIN" --show-stats || true
  else
    echo "sccache not found; skipping sccache server start"
  fi
else
	# Disable preprocessor/C caching when running without vscode user
	SCCACHE_BIN="/usr/local/bin/sccache"
	if [ ! -x "$SCCACHE_BIN" ]; then
		SCCACHE_BIN="$(command -v sccache || true)"
	fi
	if [ -n "$SCCACHE_BIN" ]; then
		SCCACHE_DIR="$SCCACHE_DIR" SCCACHE_DIRECT=false "$SCCACHE_BIN" --stop-server >/dev/null 2>&1 || true
		SCCACHE_DIR="$SCCACHE_DIR" SCCACHE_DIRECT=false "$SCCACHE_BIN" --start-server || true
		SCCACHE_DIR="$SCCACHE_DIR" SCCACHE_DIRECT=false "$SCCACHE_BIN" --show-stats || true
	else
		echo "sccache not found; skipping sccache server start"
	fi
fi

exit 0
