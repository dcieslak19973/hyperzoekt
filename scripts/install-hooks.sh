#!/usr/bin/env bash
set -euo pipefail

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
REPO_ROOT="$HERE"

echo "Installing git hooks from $REPO_ROOT/.githooks"
git -C "$REPO_ROOT" config core.hooksPath "$REPO_ROOT/.githooks"
echo "Git hooks installed. To revert: git config --unset core.hooksPath"
