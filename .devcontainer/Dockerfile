FROM rust:1.89

# Install common build deps and faster linkers (lld, mold)
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
  lld \
  mold \
  && rm -rf /var/lib/apt/lists/*

# Register mold/ld.lld as alternatives for /usr/bin/ld so they can be used as the system linker
RUN if [ -x /usr/bin/mold ]; then \
      update-alternatives --install /usr/bin/ld ld /usr/bin/mold 60 || true; \
    fi; \
    if [ -x /usr/bin/ld.lld ]; then \
      update-alternatives --install /usr/bin/ld ld /usr/bin/ld.lld 40 || true; \
    fi

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

# Create a non-root 'vscode' user and use it as the default remote user
ARG USERNAME=vscode
ARG USER_UID=1000
ARG USER_GID=$USER_UID

RUN groupadd --gid ${USER_GID} ${USERNAME} || true \
  && useradd --uid ${USER_UID} --gid ${USER_GID} -m ${USERNAME} || true \
  && apt-get update && apt-get install -y sudo || true \
  && echo "${USERNAME} ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/${USERNAME} || true

# Install sccache system-wide so it's available regardless of sudo contexts and PATH changes
RUN set -eux; \
  rustup default 1.89 || true; \
  # install sccache into /usr/local so the binary is at /usr/local/bin/sccache
  cargo install --root /usr/local sccache || true; \
  # create a mold-linker in /usr/local/bin
  mkdir -p /usr/local/bin; \
  printf '%s\n' '#!/usr/bin/env bash' 'exec /usr/bin/cc -fuse-ld=mold "$@"' > /usr/local/bin/mold-linker; \
  chmod +x /usr/local/bin/mold-linker || true

# Default to the non-root vscode user for the devcontainer
USER ${USERNAME}
ENV PATH="/home/${USERNAME}/.cargo/bin:/usr/local/bin:${PATH}"

# Keep image small by clearing caches
RUN rustup component add rustfmt clippy || true

CMD ["/bin/bash"]
