#!/usr/bin/env bash

set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"
export CARGO_INCREMENTAL=0

required_datasets=(
  measurementset/alma/M51.ms
  measurementset/alma/refim_alma_mosaic.ms
  measurementset/evla/papersky_mosaic.ms
  measurementset/evla/refim_Cband.G37line.ms
  measurementset/evla/refim_mawproject.ms
  measurementset/evla/refim_mawproject_offcenter.ms
  measurementset/evla/refim_mawproject_twopointings.ms
  measurementset/evla/refim_oneshiftpoint.mosaic.ms
  measurementset/vla/n2403.short.ms
  measurementset/vla/ngc5921.ms
  measurementset/vla/ngc5921_with_flags.ms
  measurementset/vla/polcal_CIRCULAR_BASIS.ms
  measurementset/vla/ref_vlass_wtsp_creation.ms
  measurementset/vla/refim_point.ms
  measurementset/vla/refim_point_linXY.ms
  measurementset/vla/refim_point_stokes.ms
  measurementset/vla/refim_point_withline.ms
  measurementset/vla/refim_twochan.ms
  measurementset/vla/refim_twopoints_twochan.ms
  measurementset/vla/sim_data_VLA_jet.ms
  measurementset/vla/vla_wideband_2ptg_w_squint.ms
  unittest/tclean/refim_eptwochan.ms
  unittest/tclean/refim_point.ms
  unittest/tclean/refim_point_descendingfreqs.ms
  unittest/tclean/refim_point_wterm_vlad.ms
  unittest/tclean/refim_twochan.ms
  unittest/tclean/refim_twopoints_twochan.ms
)

preflight_args=(--tier slow-parity)
for dataset in "${required_datasets[@]}"; do
  preflight_args+=(--require "$dataset")
done

cargo run -q -p casa-test-support --bin casatestdata-preflight -- "${preflight_args[@]}"
cargo test -p casars-imager --features slow-tests --test imager_casa_parity
