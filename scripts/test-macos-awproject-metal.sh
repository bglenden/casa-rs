#!/usr/bin/env bash
set -euo pipefail

if [[ "$(uname -s)" != "Darwin" ]]; then
  echo "macOS Metal acceptance requires Darwin" >&2
  exit 1
fi

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

receipt_log="$(mktemp "${TMPDIR:-/tmp}/casa-rs-metal-acceptance.XXXXXX")"
trap 'rm -f "$receipt_log"' EXIT

run_test() {
  local test_name="$1"
  shift
  CARGO_INCREMENTAL=0 cargo test -p casa-imaging --lib "tests::${test_name}" -- "$@" --exact --nocapture 2>&1 \
    | tee -a "$receipt_log"
}

run_imager_test() {
  local test_name="$1"
  CARGO_INCREMENTAL=0 cargo test -p casars-imager --lib "tests::${test_name}" -- --ignored --exact --nocapture 2>&1 \
    | tee -a "$receipt_log"
}

run_test macos_metal_capability_diagnostic_distinguishes_process_access
for test_name in \
  grouped_metal_live_receipts_charge_first_compensation_then_reuse_it \
  awproject_final_host_f64_tile_replay_matches_source_order_reference \
  awproject_metal_segmented_dispatch_matches_full_plane_dispatch \
  awproject_global_replay_requires_metal_storage_without_generic_scratch \
  awproject_source_major_grouped_initial_partitions_match_source_order_reference \
  awproject_metal_residual_only_dispatch_uses_two_planes \
  awproject_metal_phase_stencil_matches_casa_f32_product \
  awproject_metal_tile_grid_matches_source_major_fixed_grid \
  awproject_metal_prediction_tile_chain_matches_zero_model_reference \
  awproject_metal_unique_prediction_recurrence_matches_nonzero_model_reference
do
  run_test "$test_name" --ignored
done
run_imager_test awproject_mtmfs_bounded_stream_metal_matches_cpu_products

grep -Fq \
  "macos_metal_capability test=awproject_source_major_grouped_initial_partitions_match_source_order_reference macos=true apple_silicon=true hardware_os_support=true default_device_created=true status=DeviceCreated" \
  "$receipt_log"
grep -Fq \
  "macos_metal_acceptance_receipt test=awproject_source_major_grouped_initial_partitions_match_source_order_reference device_created=true pipeline_created=true dispatch_completed=true output_verified=true" \
  "$receipt_log"
grep -Fq \
  "macos_metal_acceptance_receipt test=awproject_source_major_grouped_initial_partitions_match_source_order_reference role_segmented=true psf_high_planes=3 residual_high_planes=2 weight_term_high_planes=1 weight_term_low_planes=1 bit_exact=true" \
  "$receipt_log"
grep -Fq \
  "macos_metal_acceptance_receipt test=awproject_mtmfs_bounded_stream_metal_matches_cpu_products device_created=true pipeline_created=true dispatch_completed=true output_verified=true" \
  "$receipt_log"

echo "macos_metal_acceptance status=passed device_created=true pipelines_created=true dispatches_completed=true outputs_verified=true"
