#!/usr/bin/env bash
set -euo pipefail

# Ensure PATH includes system and user cargo bins
export PATH="/usr/local/bin:$HOME/.cargo/bin:$PATH"

# Install rustup into the vscode user's home if it's missing so the vscode user
# has a writable rust toolchain installation. Run rustup installs with
# RUSTC_WRAPPER unset so sccache does not intercept rustup/rustc probes.
if id -u vscode >/dev/null 2>&1; then
  if [ ! -x "/home/vscode/.cargo/bin/rustup" ]; then
    echo "rustup not found for vscode user; installing rustup into /home/vscode"
    # Use --no-modify-path to avoid changing shell rc files during install
    sudo -u vscode env HOME=/home/vscode PATH=/usr/local/bin:/usr/bin:/bin RUSTC_WRAPPER= bash -lc 'curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --no-modify-path' || true
    # Ensure the newly-installed rustup uses the desired default toolchain
    sudo -u vscode env HOME=/home/vscode PATH=/home/vscode/.cargo/bin:/usr/local/bin:/usr/bin:/bin RUSTC_WRAPPER= bash -lc 'rustup default 1.89 || true'
  else
    echo "rustup already present for vscode"
  fi
else
  echo "vscode user not present; skipping per-user rustup install"
fi
# Prefer writing repo-level cargo config into the workspace, but fall back to the
# user's cargo config if the workspace is not writable in postCreate.
WORKSPACE_CARGO_DIR="/workspaces/hyperzoekt/.cargo"
if mkdir -p "$WORKSPACE_CARGO_DIR" 2>/dev/null && [ -w "$WORKSPACE_CARGO_DIR" ]; then
  CARGO_CFG_PATH="$WORKSPACE_CARGO_DIR/config.toml"
else
  mkdir -p "$HOME/.cargo"
  CARGO_CFG_PATH="$HOME/.cargo/config.toml"
fi
cat > "$CARGO_CFG_PATH" <<'CARGOCFG'
[build]
rustc-wrapper = "sccache"

[target.x86_64-unknown-linux-gnu]
linker = "/usr/bin/cc"
CARGOCFG

# Register mold or ld.lld as /usr/bin/ld (preferred). Do not replace cc with mold
if [ "$(id -u)" -eq 0 ]; then
  if command -v mold >/dev/null 2>&1; then
    echo "mold is installed; registering as /usr/bin/ld (priority 60)"
    update-alternatives --install /usr/bin/ld ld /usr/bin/mold 60 || true
  elif command -v ld.lld >/dev/null 2>&1; then
    echo "ld.lld is installed; registering as /usr/bin/ld (priority 40)"
    update-alternatives --install /usr/bin/ld ld /usr/bin/ld.lld 40 || true
  else
    echo "no mold or ld.lld found; leaving system linker as-is"
  fi
else
  # Non-root: skip registering system alternatives to avoid permission errors
  if command -v mold >/dev/null 2>&1; then
    echo "mold is installed but not running as root; skipping update-alternatives"
  elif command -v ld.lld >/dev/null 2>&1; then
    echo "ld.lld is installed but not running as root; skipping update-alternatives"
  fi
fi

# Ensure /usr/bin/cc points to gcc to avoid accidentally invoking mold as cc
if command -v gcc >/dev/null 2>&1; then
  if [ "$(id -u)" -eq 0 ]; then
    update-alternatives --install /usr/bin/cc cc /usr/bin/gcc 30 || true
    update-alternatives --set cc /usr/bin/gcc || true
  else
    echo "running as non-root; skipping update-alternatives for cc"
  fi
fi

# Ensure cargo bin dir is on PATH for installed cargo binaries (sccache may be installed there)
export PATH="$HOME/.cargo/bin:$PATH"

# prefer system gcc for C builds to avoid wrappers
export CC=/usr/bin/gcc

# Use a container-local target dir for builds to avoid workspace-mounted filesystem races.
# This improves reliability of sccache when cargo writes/reads artifacts.
export CARGO_TARGET_DIR="/tmp/hyperzoekt-target"
mkdir -p "$CARGO_TARGET_DIR"
chmod 0777 "$CARGO_TARGET_DIR" || true
if id -u vscode >/dev/null 2>&1; then
  sudo chown -R vscode:vscode "$CARGO_TARGET_DIR" || true
fi

# Ensure CARGO_HOME is available and writable for the vscode user to avoid writing into /usr/local
CARGO_HOME_DIR="/home/vscode/.cargo"
mkdir -p "$CARGO_HOME_DIR"
chmod 0777 "$CARGO_HOME_DIR" || true
if id -u vscode >/dev/null 2>&1; then
  sudo chown -R vscode:vscode "$CARGO_HOME_DIR" || true
fi

# Install sccache if missing and cargo is available
SCCACHE_BIN="/usr/local/bin/sccache"
if [ ! -x "$SCCACHE_BIN" ]; then
  # fallback to cargo-installed one in $HOME/.cargo/bin
  SCCACHE_BIN="$(command -v sccache || true)"
fi
if [ -z "$SCCACHE_BIN" ]; then
  if command -v cargo >/dev/null 2>&1; then
    echo "sccache not found; installing via cargo into /usr/local"
    # ensure cargo uses a writable CARGO_HOME
    sudo -u vscode env CARGO_HOME="$CARGO_HOME_DIR" cargo install --root /usr/local sccache || true
    SCCACHE_BIN="/usr/local/bin/sccache"
  else
    echo "sccache not found and cargo unavailable; skipping sccache install."
  fi
else
  echo "sccache found at: $SCCACHE_BIN"
fi

# Start sccache server using a workspace-mounted cache dir to persist across rebuilds
SCCACHE_DIR=/workspaces/hyperzoekt/.sccache
mkdir -p "$SCCACHE_DIR" && chmod 0777 "$SCCACHE_DIR" || true
# ensure ownership is set to the vscode user so builds don't produce root-owned files
if id -u vscode >/dev/null 2>&1; then
  sudo chown -R vscode:vscode "$SCCACHE_DIR" || true
  if [ -d /workspaces/hyperzoekt/target ]; then
    sudo chown -R vscode:vscode /workspaces/hyperzoekt/target || true
  fi
fi
# start sccache as the vscode user when possible
if id -u vscode >/dev/null 2>&1; then
  # Start sccache without preprocessor/C caching (only if sccache is present)
  if [ -x "/usr/local/bin/sccache" ]; then
    SCCACHE_BIN="/usr/local/bin/sccache"
  else
    SCCACHE_BIN="$(command -v sccache || true)"
  fi
  if [ -n "$SCCACHE_BIN" ]; then
    sudo -u vscode env CARGO_HOME="$CARGO_HOME_DIR" SCCACHE_DIR="$SCCACHE_DIR" SCCACHE_DIRECT=false "$SCCACHE_BIN" --stop-server >/dev/null 2>&1 || true
    sudo -u vscode env CARGO_HOME="$CARGO_HOME_DIR" SCCACHE_DIR="$SCCACHE_DIR" SCCACHE_DIRECT=false "$SCCACHE_BIN" --start-server || true
    sudo -u vscode env CARGO_HOME="$CARGO_HOME_DIR" SCCACHE_DIR="$SCCACHE_DIR" SCCACHE_DIRECT=false "$SCCACHE_BIN" --show-stats || true
  else
    echo "sccache not available; skipping sccache server start."
  fi
else
  # Disable preprocessor/C caching when running as root during setup
  if [ -x "/usr/local/bin/sccache" ]; then
    SCCACHE_BIN="/usr/local/bin/sccache"
  else
    SCCACHE_BIN="$(command -v sccache || true)"
  fi
  if [ -n "$SCCACHE_BIN" ]; then
    "$SCCACHE_BIN" --stop-server >/dev/null 2>&1 || true
    SCCACHE_DIR="$SCCACHE_DIR" SCCACHE_DIRECT=false "$SCCACHE_BIN" --start-server || true
    SCCACHE_DIR="$SCCACHE_DIR" SCCACHE_DIRECT=false "$SCCACHE_BIN" --show-stats || true
  else
    echo "sccache not available; skipping sccache server start."
  fi
fi

# run a quick build as the vscode user to prime caches (non-fatal)
if id -u vscode >/dev/null 2>&1; then
  # Use SCCACHE_DIRECT=false to avoid caching C/C++ preprocessor outputs during the prime build
  if command -v cargo >/dev/null 2>&1; then
    if command -v sccache >/dev/null 2>&1; then
      # use absolute SCCACHE_BIN if available
      if [ -x "/usr/local/bin/sccache" ]; then
        SCCACHE_BIN="/usr/local/bin/sccache"
      else
        SCCACHE_BIN="$(command -v sccache || true)"
      fi
  # Ensure container-local target dirs exist and are owned before building to avoid races
  mkdir -p "$CARGO_TARGET_DIR"/debug/deps
  sudo chown -R vscode:vscode "$CARGO_TARGET_DIR" || true
  sudo -u vscode env CARGO_HOME="$CARGO_HOME_DIR" CC=$CC SCCACHE_DIR="$SCCACHE_DIR" SCCACHE_DIRECT=false RUSTC_WRAPPER="$SCCACHE_BIN" CARGO_TARGET_DIR="$CARGO_TARGET_DIR" cargo build --manifest-path crates/hyperzoekt/Cargo.toml || true
  # Allow sccache to flush and settle file operations before attempting parallel builds
  sync || true
  sleep 2
    else
      # cargo available but sccache not present; run a best-effort build to populate target/ (non-fatal)
      sudo -u vscode env CC=$CC cargo build --manifest-path crates/hyperzoekt/Cargo.toml || true
    fi
  else
    echo "cargo not available; skipping prime build."
  fi
else
  if command -v cargo >/dev/null 2>&1; then
    cargo build || true
  else
    echo "cargo not available; skipping prime build."
  fi
fi

# Ensure a mold-linker wrapper exists and is executable (idempotent).
# Prefer installing to /usr/local/bin, but fall back to $HOME/.local/bin if not writable.
if [ -x /usr/bin/mold ] || [ -x /usr/bin/cc ]; then
  TARGET_LINKER="/usr/local/bin/mold-linker"
  if [ ! -w "$(dirname "$TARGET_LINKER")" ] && [ "$(id -u)" -ne 0 ]; then
    mkdir -p "$HOME/.local/bin"
    TARGET_LINKER="$HOME/.local/bin/mold-linker"
    export PATH="$HOME/.local/bin:$PATH"
  fi
  if [ ! -f "$TARGET_LINKER" ]; then
    cat > "$TARGET_LINKER" <<'EOF'
#!/usr/bin/env bash
exec /usr/bin/cc -fuse-ld=mold "$@"
EOF
    chmod +x "$TARGET_LINKER" || true
    echo "created mold-linker at $TARGET_LINKER"
  fi
fi
