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

# Opt-in suites stay out of default workspace test runs (`--workspace`/`--all`)
# because `[workspace] default-members` has no effect when `--workspace` is passed.
OPT_IN_SUITES=(
  --exclude casacore-interop
  --exclude casa-parity
  --exclude casa-perf-suite
)

final_args=()
exclude_opt_in=false
inserted_excludes=false
for arg in "$@"; do
  if [[ "$arg" == "--" ]]; then
    if "$exclude_opt_in" && ! "$inserted_excludes"; then
      final_args+=("${OPT_IN_SUITES[@]}")
      inserted_excludes=true
    fi
  elif [[ "$arg" == "--workspace" || "$arg" == "--all" ]]; then
    exclude_opt_in=true
  fi
  final_args+=("$arg")
done
if "$exclude_opt_in" && ! "$inserted_excludes"; then
  final_args+=("${OPT_IN_SUITES[@]}")
fi
set -- "${final_args[@]}"

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
