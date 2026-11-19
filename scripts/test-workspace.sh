#!/usr/bin/env bash

# Runs the workspace or selected packages' Rust tests with cargo-nextest (one process per test, in
# parallel) plus doctests, which nextest does not run. Without cargo-nextest
# it falls back to serial `cargo test`; the imager progress observer is
# process-global, so libtest's shared-process harness must run one test at a
# time.

set -euo pipefail

export CARGO_INCREMENTAL=0

if [[ $# -eq 0 ]]; then
  set -- --workspace
fi

run_doctests() {
  local packages=()
  local scope=("$@")
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --workspace|--all)
        cargo test "${scope[@]}" --doc
        return
        ;;
      --) break ;;
      --package|-p)
        packages+=("$2")
        shift
        ;;
      --package=*) packages+=("${1#--package=}") ;;
      -p?*) packages+=("${1#-p}") ;;
    esac
    shift
  done

  if [[ ${#packages[@]} -gt 0 ]] && command -v python3 >/dev/null 2>&1; then
    local doctests
    doctests=$(cargo metadata --no-deps --format-version 1 | python3 -c '
import json
import sys

packages = {}
for package in json.load(sys.stdin)["packages"]:
    enabled = any(target["doctest"] for target in package["targets"])
    name, version = package["name"], package["version"]
    packages[name] = enabled
    packages[f"{name}@{version}"] = enabled
    packages[f"{name}:{version}"] = enabled
print("yes" if any(packages.get(spec, True) for spec in sys.argv[1:]) else "no")
' "${packages[@]}")
    if [[ "$doctests" == no ]]; then
      echo "No doctest targets in the selected packages."
      return
    fi
  fi
  cargo test "${scope[@]}" --doc
}

if cargo nextest --version >/dev/null 2>&1; then
  cargo nextest run "$@"
  run_doctests "$@"
else
  echo "cargo-nextest not found; running tests serially (brew install cargo-nextest)" >&2
  RUST_TEST_THREADS=1 cargo test "$@"
fi
