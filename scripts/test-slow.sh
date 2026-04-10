#!/usr/bin/env bash

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

script_started_at="$(date +%s)"
run_timed_step \
  "Running slow msexplore CASA parity suite" \
  cargo test -p casa-ms --features slow-tests --test msexplore_casa_parity
run_timed_step \
  "Running slow casars-imager CASA parity suite" \
  cargo test -p casars-imager --features slow-tests --test imager_casa_parity
script_finished_at="$(date +%s)"
echo "Slow test suite completed in $(format_elapsed $(( script_finished_at - script_started_at )))"
