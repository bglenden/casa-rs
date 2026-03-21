#!/usr/bin/env bash

set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

# Keep the coverage gate focused on shipped code plus functional tests.
# Large perf/profile harnesses are useful for benchmarking, but they make
# line-coverage drift when benchmarks are added, renamed, or explicitly ignored.
cargo tarpaulin \
  --workspace \
  --timeout 120 \
  --out Stdout \
  --fail-under 75 \
  --exclude-files \
  '*/examples/*' \
  '*/tests/*perf*.rs'
