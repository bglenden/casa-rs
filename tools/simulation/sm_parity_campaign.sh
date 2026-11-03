#!/usr/bin/env bash
# SPDX-License-Identifier: LGPL-3.0-or-later
#
# Re-run the CASA `sm` parity comparisons of the simulation programme (#841)
# at the current revision, for the reviewed reference (#1053).
#
#   CASA_RS_CASA_PYTHON=.../CASA.app/Contents/MacOS/python3 \
#       tools/simulation/sm_parity_campaign.sh OUT [SET ...]
#
# SET is a scenario file's tag (observe predict components sd openfromms
# noise corrupt atm trop; all by default). For each, CASA's `sm` runs the
# scenarios (casa_sm_observe_oracle.py), casa-rs's session replays the
# manifest (the observe_scenarios example) and compare_sm_ms.py compares
# the two, writing OUT/<set>/report.json. The model images are written first
# under OUT/models: alma/ and alma-big/ by casa_sm_predict_oracle.py, the
# rest by make_bench_model.py from sm_predict_scenarios.json. Both sides read
# the same images, so they need not be the earlier runs' bit for bit.
# sm_parity_receipt.py then summarizes the reports into a receipt.
#
# CASA's antenna configurations are read from ~/.casa/data/alma/simmos.
# Run from the repository root, with a clean tree, so the revision names the
# code that ran. CASA_RS_SM_BACKEND (default cpu) is the replay's --backend.

set -euo pipefail

usage() {
  sed -n '4,22p' "$0" | sed 's/^# \{0,1\}//'
}

if [[ $# -lt 1 || "$1" == "-h" || "$1" == "--help" ]]; then
  usage
  exit 2
fi
casapy="${CASA_RS_CASA_PYTHON:?set CASA_RS_CASA_PYTHON to the python3 of CASA 6.7.6.14}"
out="$1"
shift
sets=("$@")
if [[ ${#sets[@]} -eq 0 ]]; then
  sets=(observe predict components sd openfromms noise corrupt atm trop)
fi
backend="${CASA_RS_SM_BACKEND:-cpu}"
target="${CARGO_TARGET_DIR:-target}"
export CARGO_INCREMENTAL=0

cargo build --release -q -p casa-simulation --example observe_scenarios
cargo build --release -q -p casa-atmosphere --example at_tool_quantities
replay="$target/release/examples/observe_scenarios"
atmosphere="$target/release/examples/at_tool_quantities"

models="$out/models"
mkdir -p "$models"
needs_models=0
for set in "${sets[@]}"; do
  case "$set" in predict | components | sd | openfromms | noise) needs_models=1 ;; esac
done
if [[ $needs_models -eq 1 ]]; then
  if [[ ! -d "$models/alma/model4.input.image" ]]; then
    "$casapy" tools/simulation/casa_sm_predict_oracle.py --out "$models/alma" --no-vp
  fi
  if [[ ! -d "$models/alma-big/model4.input.image" ]]; then
    "$casapy" tools/simulation/casa_sm_predict_oracle.py --out "$models/alma-big" --no-vp \
      --imsize 512 --cell-arcsec 0.15 \
      --ra 0.0007615435494667714rad --dec=-0.0001523087098933758rad
  fi
  "$casapy" tools/simulation/make_bench_model.py \
    --scenarios tools/simulation/sm_predict_scenarios.json --model-root "$models"
fi

status=0
for set in "${sets[@]}"; do
  scenarios="tools/simulation/sm_${set}_scenarios.json"
  [[ -f "$scenarios" ]] || { echo "sm-parity: no $scenarios" >&2; exit 2; }
  echo "==> $set" >&2
  "$casapy" tools/simulation/casa_sm_observe_oracle.py \
    --scenarios "$scenarios" --model-root "$models" --out "$out/$set/casa"
  "$replay" "$out/$set/casa/manifest.json" "$out/$set/casa-rs" --backend "$backend"
  # macOS's bash 3.2 calls an empty array unbound under `set -u`.
  extra=()
  case "$set" in atm | trop) extra=(--atmosphere-model "$atmosphere") ;; esac
  if ! "$casapy" tools/simulation/compare_sm_ms.py --casa "$out/$set/casa" --casars "$out/$set/casa-rs" \
    ${extra[@]+"${extra[@]}"} --json "$out/$set/report.json" > "$out/$set/compare.log" 2>&1; then
    status=1
  fi
  tail -n 30 "$out/$set/compare.log" >&2
done
exit $status
