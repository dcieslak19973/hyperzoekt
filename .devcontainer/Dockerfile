FROM rust:1.89

# Install common build deps
RUN apt-get update \
  && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    build-essential \
    pkg-config \
    libssl-dev \
  openssh-client \
    ca-certificates \
  && rm -rf /var/lib/apt/lists/*

# Create workspace dir
WORKDIR /workspaces/hyperzoekt

# Default to root (matches devcontainer.json)
USER root

# Keep image small by clearing caches
RUN rustup component add rustfmt clippy || true

CMD ["/bin/bash"]
