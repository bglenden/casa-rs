#!/usr/bin/env bash

set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"
export CARGO_INCREMENTAL=0

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
cargo run -q -p casa-test-support --bin casatestdata-preflight -- \
  --tier slow-parity \
  --require measurementset/vla/ngc5921.ms \
  --require measurementset/vla/ref_vlass_wtsp_creation.ms \
  --require measurementset/alma/alma_ephemobj_icrs.ms
run_timed_step \
  "Running recorded CASA parity comparisons" \
  cargo test -p casa-parity --test api
run_timed_step \
  "Running recorded CASA moving-field phase-centre parity" \
  cargo test -p casa-imaging --lib \
    store_law::t41_trackfield_oracle::t41_trackfield_phase_centre_matches_casa_at_three_row_times \
    -- --ignored --exact
run_timed_step \
  "Running recorded CASA moving-field MFS envelope parity" \
  cargo test -p casa-parity --test api \
    t41_moving_source_casa_oracle::moving_field_mfs_envelope_matches_casa_field_origin_direction \
    -- --ignored --exact
run_timed_step \
  "Running slow msexplore CASA parity suite" \
  cargo test -p casars-cli --features slow-tests --test msexplore_casa_parity
script_finished_at="$(date +%s)"
echo "Slow test suite completed in $(format_elapsed $(( script_finished_at - script_started_at )))"
