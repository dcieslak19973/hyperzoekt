### Multi-stage Dockerfile
# - Build Rust binaries in a builder image
# - Attempt a musl static build so we can use a tiny Chainguard runtime
# - If musl static linking is not possible for some crates, the Dockerfile falls back to a slim Debian runtime

FROM --platform=$BUILDPLATFORM rust:1.72-bullseye AS builder
ARG CARGO_HOME=/cargo
ENV CARGO_HOME=${CARGO_HOME}
WORKDIR /workspace

# Install necessary native deps for some crates (openssl, pkg-config)
RUN apt-get update && apt-get install -y --no-install-recommends \
    musl-tools \
    clang \
    pkg-config \
    libssl-dev \
    ca-certificates \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy workspace
COPY . .

# Pre-fetch cargo dependencies to cache layers
RUN cargo fetch --locked

# Build with musl target for static binaries (preferred for tiny runtime)
ARG TARGET=x86_64-unknown-linux-musl
RUN rustup target add ${TARGET} || true

# Try static musl release build. Some crates may not compile statically; fallback handled later.
RUN set -eux; \
    if cargo build --release --target ${TARGET} --workspace; then \
        echo "musl build succeeded" > /build_ok; \
    else \
        echo "musl build failed" > /build_ok; \
    fi

# Collect binaries path variables (release target may be musl or default)
RUN mkdir -p /out
RUN if [ -f /build_ok ] && grep -q "musl build succeeded" /build_ok; then \
      cp target/${TARGET}/release/* /out/ || true; \
    else \
      cp target/release/* /out/ || true; \
    fi

# Small Chainguard runtime using distroless images. Prefer `chainguard/static` if musl succeeded.
FROM chainguard/static:nonroot as runtime-musl
COPY --from=builder /out/ /usr/local/bin/
COPY --from=builder /workspace/static /static
COPY --from=builder /workspace/crates/zoekt-distributed/static /usr/local/share/zoekt-distributed/static
COPY --from=builder /workspace/crates/hyperzoekt/static /usr/local/share/hyperzoekt/static

WORKDIR /app
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/zoekt-http-search"]

# Fallback runtime (glibc-based) if musl static linking failed
FROM debian:trixie-slim AS runtime-glibc
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates git && rm -rf /var/lib/apt/lists/*
COPY --from=builder /out/ /usr/local/bin/
COPY --from=builder /workspace/static /static
COPY --from=builder /workspace/crates/zoekt-distributed/static /usr/local/share/zoekt-distributed/static
COPY --from=builder /workspace/crates/hyperzoekt/static /usr/local/share/hyperzoekt/static

WORKDIR /app
ENTRYPOINT ["/usr/local/bin/zoekt-http-search"]
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

# Copy the entire workspace
COPY . .

# Build zoekt-distributed binaries (no extra features)
RUN cargo build --release --bin dzr-admin --bin dzr-http-search --bin dzr-indexer --bin dzr-mcp-search --bin dzr-admin-poller

# Build hyperzoekt binaries with optional OTEL feature (safe even if unused at runtime)
RUN cargo build --release -p hyperzoekt --features otel --bin hyperzoekt-indexer --bin hyperzoekt-webui --bin hyperzoekt-embed-worker --bin hyperzoekt-mcp

# Use Debian trixie slim as the runtime image (Debian testing with latest GLIBC)
FROM debian:trixie-slim

# Install ca-certificates for HTTPS requests and git for repository operations
RUN apt-get update && apt-get install -y ca-certificates git && rm -rf /var/lib/apt/lists/*

# Set the working directory
WORKDIR /app

# Copy the built binaries from the builder stage
COPY --from=builder /app/target/release/dzr-admin /app/zoekt-admin
COPY --from=builder /app/target/release/dzr-http-search /app/zoekt-http-search
COPY --from=builder /app/target/release/dzr-indexer /app/zoekt-indexer
COPY --from=builder /app/target/release/dzr-mcp-search /app/zoekt-mcp-search
COPY --from=builder /app/target/release/dzr-admin-poller /app/zoekt-admin-poller
COPY --from=builder /app/target/release/hyperzoekt-indexer /app/
COPY --from=builder /app/target/release/hyperzoekt-webui /app/
COPY --from=builder /app/target/release/hyperzoekt-embed-worker /app/
COPY --from=builder /app/target/release/hyperzoekt-mcp /app/

# Copy configuration and static files
COPY --from=builder /app/crates/zoekt-distributed/config.toml /app/
COPY --from=builder /app/static /static/
COPY --from=builder /app/crates/zoekt-distributed/static /app/crates/zoekt-distributed/static/
COPY --from=builder /app/crates/hyperzoekt/static /app/crates/hyperzoekt/static/

# Default command (can be overridden)
CMD ["/app/zoekt-http-search"]
