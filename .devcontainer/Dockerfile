FROM rust:1.89

# Install common build deps
RUN apt-get update \
  && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
  build-essential \
  pkg-config \
  libssl-dev \
  openssh-client \
  ca-certificates \
  curl \
  gnupg \
  apt-transport-https \
  jq \
  ripgrep \
  && rm -rf /var/lib/apt/lists/*

# Install GitHub CLI (gh) using the official GitHub CLI apt repository
RUN curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg \
    | dd of=/usr/share/keyrings/githubcli-archive-keyring.gpg \
    && chmod go+r /usr/share/keyrings/githubcli-archive-keyring.gpg \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" \
    | tee /etc/apt/sources.list.d/github-cli.list > /dev/null \
    && apt-get update \
    && apt-get install -y --no-install-recommends gh \
    && rm -rf /var/lib/apt/lists/*

# Create workspace dir
WORKDIR /workspaces/hyperzoekt

# Default to root (matches devcontainer.json)
USER root

# Keep image small by clearing caches
RUN rustup component add rustfmt clippy || true

CMD ["/bin/bash"]
