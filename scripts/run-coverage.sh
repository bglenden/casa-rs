#!/usr/bin/env bash

set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

tarpaulin_timeout="${TARPAULIN_TIMEOUT:-300}"

# Keep the coverage gate focused on shipped code plus functional tests.
# Large perf/profile harnesses are useful for benchmarking, but they make
# line-coverage drift when benchmarks are added, renamed, or explicitly ignored.
cargo tarpaulin \
  --workspace \
  --timeout "$tarpaulin_timeout" \
  --out Stdout \
  --fail-under 75 \
  --exclude-files \
  '*/examples/*' \
  '*/tests/*perf*.rs'
