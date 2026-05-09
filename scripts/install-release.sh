#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/install-release.sh <version> [install-suite options]

Installs a published casa-rs release suite without rebuilding the checkout or
running release verification gates.

Examples:
  scripts/install-release.sh 0.17.0
  scripts/install-release.sh 0.17.0 --force
  scripts/install-release.sh 0.17.0 --install-root ~/.local/opt/casa-rs-test
EOF
}

if [[ $# -lt 1 ]]; then
  usage
  exit 1
fi

version="$1"
shift

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

scripts/install-suite.sh --version "$version" "$@"
