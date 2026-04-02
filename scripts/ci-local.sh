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
git_dir="$(git rev-parse --git-dir)"
git_common_dir="$(git rev-parse --git-common-dir)"
image_name="casa-rs-ci-minimal"
command="${1:-all}"

abspath_from_repo_root() {
  case "$1" in
    /*) printf '%s\n' "$1" ;;
    *) printf '%s\n' "$repo_root/${1#./}" ;;
  esac
}

git_dir="$(abspath_from_repo_root "$git_dir")"
git_common_dir="$(abspath_from_repo_root "$git_common_dir")"

path_is_within_repo_root() {
  case "$1" in
    "$repo_root"/*) return 0 ;;
    *) return 1 ;;
  esac
}

container_mount_args() {
  local -a mounts=("-v" "$repo_root:$repo_root")

  if ! path_is_within_repo_root "$git_dir"; then
    mounts+=("-v" "$git_dir:$git_dir")
  fi

  if [[ "$git_common_dir" != "$git_dir" ]] && ! path_is_within_repo_root "$git_common_dir"; then
    mounts+=("-v" "$git_common_dir:$git_common_dir")
  fi

  printf '%s\0' "${mounts[@]}"
}

run_in_container() {
  local script="$1"
  local -a mounts=()
  while IFS= read -r -d '' arg; do
    mounts+=("$arg")
  done < <(container_mount_args)

  docker run --rm \
    "${mounts[@]}" \
    -w "$repo_root" \
    "$image_name" \
    bash -lc "$script"
}

case "$command" in
  build)
    docker build -f "$repo_root/Dockerfile.ci-minimal" -t "$image_name" "$repo_root"
    ;;
  lint_test)
    run_in_container '
      ./scripts/check-spdx.sh &&
      cargo fmt --all -- --check &&
      cargo clippy --workspace --all-targets -- -D warnings &&
      cargo test --workspace &&
      cargo run -p casacore-aipsio --example t_aipsio &&
      cargo run -p casacore-tables --example t_table
    '
    ;;
  coverage)
    run_in_container '
      ./scripts/run-coverage.sh --ci-like
    '
    ;;
  all)
    run_in_container '
      ./scripts/check-spdx.sh &&
      cargo fmt --all -- --check &&
      cargo clippy --workspace --all-targets -- -D warnings &&
      cargo test --workspace &&
      cargo run -p casacore-aipsio --example t_aipsio &&
      cargo run -p casacore-tables --example t_table &&
      ./scripts/run-coverage.sh --ci-like
    '
    ;;
  shell)
    mounts=()
    while IFS= read -r -d '' arg; do
      mounts+=("$arg")
    done < <(container_mount_args)
    docker run --rm -it \
      "${mounts[@]}" \
      -w "$repo_root" \
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
