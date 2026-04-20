#!/usr/bin/env bash
# SPDX-License-Identifier: LGPL-3.0-or-later
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

format_elapsed() {
  local total_seconds="$1"
  local minutes=$(( total_seconds / 60 ))
  local seconds=$(( total_seconds % 60 ))
  printf '%dm%02ds' "$minutes" "$seconds"
}

run_timed_step() {
  local label="$1"
  shift
  local started_at
  local finished_at
  started_at="$(date +%s)"
  echo "==> $label"
  "$@"
  finished_at="$(date +%s)"
  echo "==> $label completed in $(format_elapsed $(( finished_at - started_at )))"
}

if ! command -v pkg-config >/dev/null 2>&1 || ! pkg-config --exists casacore; then
  echo "==> Skipping release performance suite: casacore pkg-config metadata is unavailable"
  exit 0
fi

script_started_at="$(date +%s)"
run_timed_step \
  "Running casa-test-support release performance suite" \
  cargo test -p casa-test-support --release --features performance-tests --tests -- --nocapture
run_timed_step \
  "Running MeasurementSet release performance suite" \
  cargo test -p casa-ms --release --features performance-tests --test ms_perf_vs_cpp -- --nocapture
script_finished_at="$(date +%s)"
echo "Release performance suite completed in $(format_elapsed $(( script_finished_at - script_started_at )))"
