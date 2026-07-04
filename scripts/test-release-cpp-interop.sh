#!/usr/bin/env bash
# SPDX-License-Identifier: LGPL-3.0-or-later
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"
export CARGO_INCREMENTAL=0

die() {
  echo "test-release-cpp-interop.sh: $*" >&2
  exit 1
}

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

command -v pkg-config >/dev/null 2>&1 || die "pkg-config is required"
pkg-config --exists casacore || die "casacore pkg-config metadata is required for the blocking C++ interop gate"

script_started_at="$(date +%s)"
cargo run -q -p casa-test-support --bin casatestdata-preflight -- \
  --tier slow-parity \
  --require unittest/importvla/AS758_C030425.xp1 \
  --require unittest/importvla/AS758_C030426.xp5
run_timed_step \
  "Running casa-test-support C++ interop suite" \
  cargo test -p casa-test-support --features cpp-interop-tests --tests
run_timed_step \
  "Running MeasurementSet C++ interop suite" \
  cargo test -p casa-ms --features cpp-interop-tests \
    --test ms_data_interop \
    --test ms_full_verify_vs_cpp \
    --test spectral_frame_parity
run_timed_step \
  "Running VLA C++ interop suite" \
  cargo test -p casa-vla --features cpp-interop-tests \
    --test cpp_first_record_parity \
    --test cpp_import_ms_parity \
    --test cpp_import_ms_parity_matrix \
    --test real_importvla_parity \
    --test spectral_conversion_xp1_debug
script_finished_at="$(date +%s)"
echo "C++ interop release gate completed in $(format_elapsed $(( script_finished_at - script_started_at )))"
