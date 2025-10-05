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
  strace \
  gnupg \
  apt-transport-https \
  jq \
  vim \
  ripgrep \
  lld \
  mold \
  locales \
  redis-server \
  docker.io \
  net-tools \
  iproute2 \
  sudo \
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

# Install Helm (pinned tarball) – avoids flaky/404 apt repo (baltocdn 404 observed)
ARG HELM_VERSION=v3.15.3
RUN set -eux; \
  arch="$(uname -m)"; \
  case "$arch" in \
  x86_64|amd64) helm_arch=amd64 ;; \
  aarch64|arm64) helm_arch=arm64 ;; \
  *) echo "Unsupported architecture: $arch" >&2; exit 1 ;; \
  esac; \
  curl -L "https://get.helm.sh/helm-${HELM_VERSION}-linux-${helm_arch}.tar.gz" -o /tmp/helm.tgz; \
  tar -xzf /tmp/helm.tgz -C /tmp; \
  mv /tmp/linux-${helm_arch}/helm /usr/local/bin/helm; \
  chmod +x /usr/local/bin/helm; \
  rm -rf /tmp/helm.tgz /tmp/linux-${helm_arch}; \
  helm version || true

# Install kubeconform for Kubernetes schema validation
RUN curl -L https://github.com/yannh/kubeconform/releases/latest/download/kubeconform-linux-amd64.tar.gz \
  | tar xz -C /usr/local/bin kubeconform \
  && chmod +x /usr/local/bin/kubeconform

# Install helm-docs for generating Helm chart documentation
RUN curl -L https://github.com/norwoodj/helm-docs/releases/latest/download/helm-docs_1.14.2_Linux_x86_64.tar.gz \
  | tar xz -C /usr/local/bin helm-docs \
  && chmod +x /usr/local/bin/helm-docs

# Install SurrealDB CLI (pin a version for reproducibility; update as needed)
# To update: adjust SURREALDB_VERSION below. We use the official release tarball.
ARG SURREALDB_VERSION=v2.3.7
RUN set -eux; \
  arch="$(uname -m)"; \
  case "$arch" in \
  x86_64|amd64) surreal_arch=amd64 ;; \
  aarch64|arm64) surreal_arch=arm64 ;; \
  *) echo "Unsupported architecture: $arch" >&2; exit 1 ;; \
  esac; \
  curl -L "https://github.com/surrealdb/surrealdb/releases/download/${SURREALDB_VERSION}/surreal-${SURREALDB_VERSION}.linux-${surreal_arch}.tgz" -o /tmp/surreal.tgz; \
  tar -xzf /tmp/surreal.tgz -C /usr/local/bin surreal; \
  rm /tmp/surreal.tgz; \
  chmod +x /usr/local/bin/surreal; \
  /usr/local/bin/surreal version || true

ARG DEP_SCAN_VERSION=v5.5.0
ARG DEPSCAN_ALL=0
RUN set -eux; \
  apt-get update; \
  DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    nodejs npm default-jre-headless curl ca-certificates python3-venv python3-pip \
    build-essential libssl-dev libffi-dev python3-dev || true; \
  rm -rf /var/lib/apt/lists/*; \
  # Ensure npm globals install to /usr/local so binaries are system-wide
  export NPM_CONFIG_PREFIX=/usr/local; \
  npm set progress=false; npm config set loglevel silent; \
  npm install -g @cyclonedx/cdxgen || true; \
  if command -v cdxgen >/dev/null 2>&1; then cdxgen --version >/dev/null 2>&1 || true; fi; \
  # Install owasp-depscan into a system virtualenv to avoid pip/pipx user-path issues
  VENV_DIR=/opt/owasp-depscan; \
  python3 -m venv "$VENV_DIR"; \
  "$VENV_DIR/bin/pip" install --upgrade pip setuptools wheel >/dev/null 2>&1 || true; \
  if [ "${DEPSCAN_ALL:-0}" = "1" ]; then \
    "$VENV_DIR/bin/pip" install --no-cache-dir "owasp-depscan[all]" >/dev/null 2>&1 || true; \
  else \
    "$VENV_DIR/bin/pip" install --no-cache-dir owasp-depscan >/dev/null 2>&1 || true; \
  fi; \
  if [ -x "$VENV_DIR/bin/depscan" ]; then ln -sf "$VENV_DIR/bin/depscan" /usr/local/bin/dep-scan || true; fi

# Create workspace dir
WORKDIR /workspaces/hyperzoekt

# Create a non-root 'vscode' user and use it as the default remote user
ARG USERNAME=vscode
ARG USER_UID=1000
ARG USER_GID=$USER_UID

RUN groupadd --gid ${USER_GID} ${USERNAME} || true \
  && useradd --uid ${USER_UID} --gid ${USER_GID} -m ${USERNAME} || true \
  && echo "${USERNAME} ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/${USERNAME} || true \
  && usermod -aG docker ${USERNAME} || true

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

# Ensure a UTF-8 locale is available and default to it so services like redis
# can configure their locale without warnings. Do locale generation here so
# the result is baked into the image rather than done at runtime.
ENV LANG=en_US.UTF-8
ENV LANGUAGE=en_US:en
ENV LC_ALL=C.UTF-8

# Generate the en_US.UTF-8 locale during image build so the locale is
# available to all processes without requiring runtime setup.
USER root
RUN locale-gen en_US.UTF-8 && update-locale LANG=en_US.UTF-8 || true
USER ${USERNAME}

# Keep image small by clearing caches
RUN rustup component add rustfmt clippy || true

CMD ["/bin/bash"]
