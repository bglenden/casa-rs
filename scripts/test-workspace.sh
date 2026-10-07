#!/usr/bin/env bash

# Runs the workspace Rust tests: cargo-nextest (one process per test, in
# parallel) plus doctests, which nextest does not run. Without cargo-nextest
# it falls back to serial `cargo test`; the imager progress observer is
# process-global, so libtest's shared-process harness must run one test at a
# time.

set -euo pipefail

export CARGO_INCREMENTAL=0

if cargo nextest --version >/dev/null 2>&1; then
  cargo nextest run --workspace
  cargo test --workspace --doc
else
  echo "cargo-nextest not found; running tests serially (brew install cargo-nextest)" >&2
  RUST_TEST_THREADS=1 cargo test --workspace
fi
