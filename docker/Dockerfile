# Use Rust image for building
FROM rust:latest AS builder

# Install required dependencies
RUN apt-get update && apt-get install -y \
    libssl-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Disable sccache to avoid compilation issues
ENV RUSTC_WRAPPER=

# Set the working directory
WORKDIR /app

# Copy dependency files first for better caching
COPY Cargo.toml Cargo.lock ./
COPY crates/hyperzoekt/Cargo.toml crates/hyperzoekt/
COPY crates/zoekt-rs/Cargo.toml crates/zoekt-rs/
COPY crates/zoekt-distributed/Cargo.toml crates/zoekt-distributed/

# Copy source code
COPY crates/ ./crates/
COPY static/ ./static/
COPY scripts/ ./scripts/

# Force rebuild by using build timestamp
ARG BUILD_TIMESTAMP
ENV BUILD_TIMESTAMP=${BUILD_TIMESTAMP}

# Build zoekt-distributed binaries (no extra features)
RUN cargo build --release --bin dzr-admin --bin dzr-http-search --bin dzr-indexer --bin dzr-mcp-search --bin dzr-admin-poller

# Build hyperzoekt binaries with optional OTEL feature (safe even if unused at runtime)
RUN cargo build --release -p hyperzoekt --features otel --bin hyperzoekt-indexer --bin hyperzoekt-embed-worker --bin hyperzoekt-webui --bin hyperzoekt-mcp --bin hyperzoekt-sbom-worker

# Use Debian trixie slim as the runtime image (Debian testing with latest GLIBC)
FROM debian:trixie-slim

# Allow injecting a build id at build time which is preserved into the runtime image
# so the application can expose it via the /health endpoint.
ARG HYPERZOEKT_BUILD_ID
ENV HYPERZOEKT_BUILD_ID=${HYPERZOEKT_BUILD_ID}

# Install ca-certificates for HTTPS requests and git for repository operations
ARG DEP_SCAN_VERSION=v5.5.0
RUN set -eux; \
    apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates git nodejs npm default-jre-headless curl python3-venv python3-pip pipx \
        build-essential libssl-dev libffi-dev python3-dev \
        && rm -rf /var/lib/apt/lists/*;

# Install CycloneDX `cdxgen` via npm and install owasp-depscan via pipx for dep-scan
RUN set -eux; \
        export NPM_CONFIG_PREFIX=/usr/local; \
        npm set progress=false; npm config set loglevel silent; \
        npm install -g @cyclonedx/cdxgen || true; \
        if command -v cdxgen >/dev/null 2>&1; then cdxgen --version >/dev/null 2>&1 || true; fi; \
        # Install owasp-depscan into a system virtualenv to avoid pip/pipx root issues
        VENV_DIR=/opt/owasp-depscan; \
        python3 -m venv "$VENV_DIR"; \
        "$VENV_DIR/bin/pip" install --upgrade pip setuptools wheel >/dev/null 2>&1 || true; \
        # Default to normal install; set DEPSCAN_ALL=1 when building to install extras
        if [ "${DEPSCAN_ALL:-0}" = "1" ]; then \
            "$VENV_DIR/bin/pip" install --no-cache-dir "owasp-depscan[all]" >/dev/null 2>&1 || true; \
        else \
            "$VENV_DIR/bin/pip" install --no-cache-dir owasp-depscan >/dev/null 2>&1 || true; \
        fi; \
    if [ -x "$VENV_DIR/bin/depscan" ]; then ln -sf "$VENV_DIR/bin/depscan" /usr/local/bin/dep-scan || true; fi

# Set the working directory
WORKDIR /app

# Copy the built binaries from the builder stage
COPY --from=builder /app/target/release/dzr-admin /app/
COPY --from=builder /app/target/release/dzr-admin-poller /app/
COPY --from=builder /app/target/release/dzr-http-search /app/
COPY --from=builder /app/target/release/dzr-indexer /app/
COPY --from=builder /app/target/release/dzr-mcp-search /app/
COPY --from=builder /app/target/release/hyperzoekt-embed-worker /app/
COPY --from=builder /app/target/release/hyperzoekt-indexer /app/
COPY --from=builder /app/target/release/hyperzoekt-mcp /app/
COPY --from=builder /app/target/release/hyperzoekt-webui /app/
COPY --from=builder /app/target/release/hyperzoekt-sbom-worker /app/

# Copy configuration and static files
COPY --from=builder /app/crates/zoekt-distributed/config.toml /app/
COPY --from=builder /app/static /static/
COPY --from=builder /app/crates/zoekt-distributed/static /app/crates/zoekt-distributed/static/
COPY --from=builder /app/crates/hyperzoekt/static /app/crates/hyperzoekt/static/

# Default command (can be overridden)
CMD ["/app/dzr-http-search"]