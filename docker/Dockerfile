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
RUN cargo build --release -p hyperzoekt --features otel --bin hyperzoekt-indexer --bin hyperzoekt-webui

# Use Debian trixie slim as the runtime image (Debian testing with latest GLIBC)
FROM debian:trixie-slim

# Install ca-certificates for HTTPS requests and git for repository operations
RUN apt-get update && apt-get install -y ca-certificates git && rm -rf /var/lib/apt/lists/*

# Set the working directory
WORKDIR /app

# Copy the built binaries from the builder stage
COPY --from=builder /app/target/release/dzr-admin /app/
COPY --from=builder /app/target/release/dzr-http-search /app/
COPY --from=builder /app/target/release/dzr-indexer /app/
COPY --from=builder /app/target/release/dzr-mcp-search /app/
COPY --from=builder /app/target/release/dzr-admin-poller /app/
COPY --from=builder /app/target/release/hyperzoekt-indexer /app/
COPY --from=builder /app/target/release/hyperzoekt-webui /app/

# Copy configuration and static files
COPY --from=builder /app/crates/zoekt-distributed/config.toml /app/
COPY --from=builder /app/static /static/
COPY --from=builder /app/crates/zoekt-distributed/static /app/crates/zoekt-distributed/static/
COPY --from=builder /app/crates/hyperzoekt/static /app/crates/hyperzoekt/static/

# Default command (can be overridden)
CMD ["/app/dzr-http-search"]
