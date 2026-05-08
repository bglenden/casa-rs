#!/usr/bin/env bash

set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

bash scripts/install-local-suite.sh "$@"

if [[ "$(uname -s)" == "Darwin" ]]; then
  bash apps/casars-mac/script/install-local-gui.sh "$@"
else
  echo "==> Skipping Swift GUI local install on non-macOS platform"
fi
