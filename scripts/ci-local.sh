#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/ci-local.sh [build|lint_test|coverage|all|shell]

Local reproduction helper for the minimal GitHub Actions CI environment.

Commands:
  build      Build the local ci-minimal image
  lint_test  Run the lint_test job commands in the container
  coverage   Run the coverage job commands in the container
  all        Run lint_test followed by coverage
  shell      Open an interactive shell in the container

Examples:
  scripts/ci-local.sh build
  scripts/ci-local.sh lint_test
  scripts/ci-local.sh coverage
  scripts/ci-local.sh all
EOF
}

repo_root="$(git rev-parse --show-toplevel)"
image_name="casa-rs-ci-minimal"
command="${1:-all}"

run_in_container() {
  local script="$1"
  docker run --rm \
    -v "$repo_root:/workspace" \
    -w /workspace \
    "$image_name" \
    bash -lc "$script"
}

case "$command" in
  build)
    docker build -f "$repo_root/Dockerfile.ci-minimal" -t "$image_name" "$repo_root"
    ;;
  lint_test)
    run_in_container '
      cargo fmt --all -- --check &&
      cargo clippy --workspace --all-targets -- -D warnings &&
      cargo test --workspace &&
      cargo run -p casacore-aipsio --example t_aipsio &&
      cargo run -p casacore-tables --example t_table
    '
    ;;
  coverage)
    run_in_container '
      cargo tarpaulin --workspace --timeout 120 --out Stdout --fail-under 75
    '
    ;;
  all)
    run_in_container '
      cargo fmt --all -- --check &&
      cargo clippy --workspace --all-targets -- -D warnings &&
      cargo test --workspace &&
      cargo run -p casacore-aipsio --example t_aipsio &&
      cargo run -p casacore-tables --example t_table &&
      cargo tarpaulin --workspace --timeout 120 --out Stdout --fail-under 75
    '
    ;;
  shell)
    docker run --rm -it \
      -v "$repo_root:/workspace" \
      -w /workspace \
      "$image_name" \
      bash
    ;;
  -h|--help|help)
    usage
    ;;
  *)
    usage
    exit 1
    ;;
esac
