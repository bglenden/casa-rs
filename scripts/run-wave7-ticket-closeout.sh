#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

stamp="$(date -u +%Y%m%dT%H%M%SZ)"
outdir="${WAVE7_TICKET_OUTDIR:-target/wave7-ticket-closeout/$stamp}"
run_benches="${WAVE7_RUN_TARGETED_BENCHES:-0}"
run_issue197="${WAVE7_RUN_ISSUE197:-$run_benches}"
run_issue198="${WAVE7_RUN_ISSUE198:-$run_benches}"
run_issue199="${WAVE7_RUN_ISSUE199:-$run_benches}"
repeats="${WAVE7_TICKET_REPEATS:-1}"

mkdir -p "$outdir"
log_file="$outdir/wave7-ticket-closeout.log"

if [[ -z "${CASA_RS_CASA_PYTHON:-}" && -x "$HOME/SoftwareProjects/casa-build/venv/bin/python" ]]; then
  export CASA_RS_CASA_PYTHON="$HOME/SoftwareProjects/casa-build/venv/bin/python"
fi

if [[ -z "${CASA_RS_TUTORIAL_DATA_ROOT:-}" && -d "$HOME/SoftwareProjects/casa-tutorial-data" ]]; then
  export CASA_RS_TUTORIAL_DATA_ROOT="$HOME/SoftwareProjects/casa-tutorial-data"
fi

if [[ -z "${CASA_RS_TESTDATA_ROOT:-}" && -d "$HOME/SoftwareProjects/casatestdata" ]]; then
  export CASA_RS_TESTDATA_ROOT="$HOME/SoftwareProjects/casatestdata"
fi

require_tutorial_key() {
  local key="$1"
  cargo run -q -p casa-test-support --bin casatestdata-preflight -- \
    --tier tutorial-parity \
    --require-registry-key "$key"
}

run_optional() {
  local label="$1"
  shift
  echo "==> $label"
  if "$@"; then
    echo "status=passed label=$label"
  else
    local status=$?
    echo "status=failed label=$label exit=$status"
    return 0
  fi
  echo
}

extract_twhya_selfcal() {
  local dest_root="$outdir/twhya"
  local ms_path="$dest_root/twhya_selfcal.ms"
  local archive="${CASA_RS_TUTORIAL_DATA_ROOT:-}/tutorial-parity/alma/first-look/twhya/twhya_selfcal.ms.tgz"
  if [[ -d "$ms_path" ]]; then
    printf "%s\n" "$ms_path"
    return 0
  fi
  if [[ ! -f "$archive" ]]; then
    echo "error: missing TW Hydra selfcal archive: $archive" >&2
    return 2
  fi
  mkdir -p "$dest_root"
  tar -xf "$archive" -C "$dest_root"
  printf "%s\n" "$ms_path"
}

run_imager_profiles() {
  local antennae_ms="${CASA_RS_TUTORIAL_DATA_ROOT:-}/tutorial-parity/alma/antennae/band7/Antennae_Band7_CalibratedData/Antennae_North.cal.ms"
  local vla_ms="${CASA_RS_TUTORIAL_DATA_ROOT:-}/tutorial-parity/vla/3c391/EVLA_3C391_FinalCalibratedMosaicMS/3c391_ctm_mosaic_spw0.ms"

  cargo build --release -p casars-imager --bin casars-imager >/dev/null

  if ! /usr/bin/time -lp target/release/casars-imager \
    --ms "$antennae_ms" \
    --imagename "$outdir/issue197-antennae-rust" \
    --managed-output true \
    --no-preview-pngs \
    --phasecenter-field 12 \
    --spw "0:1~50;120~164" \
    --datacolumn DATA \
    --deconvolver hogbom \
    --imsize 500 \
    --cell-arcsec 0.13 \
    --niter 32 \
    --minor-cycle-length 32 \
    --casa-hogbom-iterations \
    --gain 0.1 \
    --threshold-jy 0.0004 \
    >"$outdir/issue197-antennae-managed-output.json" \
    2>"$outdir/issue197-antennae-timing.stderr"; then
    cat "$outdir/issue197-antennae-timing.stderr"
    return 1
  fi

  summarize_imager_json "$outdir/issue197-antennae-managed-output.json"
  cat "$outdir/issue197-antennae-timing.stderr"
  echo

  if ! /usr/bin/time -lp target/release/casars-imager \
    --ms "$vla_ms" \
    --imagename "$outdir/issue197-3c391-rust" \
    --managed-output true \
    --no-preview-pngs \
    --phasecenter-field 0 \
    --deconvolver multiscale \
    --scales 0,5,15,45 \
    --smallscalebias 0.9 \
    --weighting briggs \
    --robust 0.5 \
    --imsize 480 \
    --cell-arcsec 2.5 \
    --niter 500 \
    --gain 0.1 \
    --threshold-jy 0.001 \
    --minor-cycle-length 500 \
    >"$outdir/issue197-3c391-managed-output.json" \
    2>"$outdir/issue197-3c391-timing.stderr"; then
    cat "$outdir/issue197-3c391-timing.stderr"
    return 1
  fi

  summarize_imager_json "$outdir/issue197-3c391-managed-output.json"
  cat "$outdir/issue197-3c391-timing.stderr"
}

summarize_imager_json() {
  local path="$1"
  python3 - "$path" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, "r", encoding="utf-8") as handle:
    obj = json.load(handle)
run = obj["run"]
frontend = {name: value / 1.0e9 for name, value in run["frontend_timings"]["values_ns"]}
core = {name: value / 1.0e9 for name, value in run["stage_timings"]["values_ns"]}
print(f"managed_output={path}")
print(f"  gridded_samples={run['gridded_samples']} major_cycles={run['major_cycles']} minor_iterations={run['minor_iterations']}")
print("  frontend_seconds:")
for name in ["open_measurement_set", "prepare_plane_input", "run_imaging", "write_products", "total"]:
    if name in frontend:
        print(f"    {name}={frontend[name]:.6f}")
print("  core_seconds:")
for name in [
    "controller_overhead",
    "weighting",
    "psf_grid",
    "psf_fft",
    "residual_degrid_grid",
    "residual_fft",
    "residual_normalize",
    "minor_cycle",
    "major_cycle_refresh",
    "restore",
    "total",
]:
    if name in core:
        print(f"    {name}={core[name]:.6f}")
PY
}

run_calibration_apply_bench() {
  local twhya_ms
  twhya_ms="$(extract_twhya_selfcal)"
  CAL_BENCH_REPEATS="$repeats" \
  CAL_BENCH_FIELD="${WAVE7_TWHY_CAL_FIELD:-5}" \
  CAL_BENCH_SPW="${WAVE7_TWHY_CAL_SPW:-0}" \
  CAL_BENCH_REFANT="${WAVE7_TWHY_CAL_REFANT:-DV22}" \
  CAL_BENCH_JSON_OUT="$outdir/issue198-twhya-applycal-bench.json" \
    scripts/bench-calibrate-vs-casa.sh "$twhya_ms" \
    >"$outdir/issue198-twhya-applycal-bench.log" 2>&1
  tail -60 "$outdir/issue198-twhya-applycal-bench.log"
}

run_plot_bench() {
  BENCH_REPEATS="$repeats" \
    scripts/bench-msexplore-vs-casa.sh \
    >"$outdir/issue199-plotms-msexplore-bench.log" 2>&1
  tail -60 "$outdir/issue199-plotms-msexplore-bench.log"
}

main() {
  echo "wave7_ticket_closeout_stamp=$stamp"
  echo "outdir=$outdir"
  echo "run_benches=$run_benches"
  echo "run_issue197=$run_issue197"
  echo "run_issue198=$run_issue198"
  echo "run_issue199=$run_issue199"
  echo "repeats=$repeats"
  echo "CASA_RS_CASA_PYTHON=${CASA_RS_CASA_PYTHON:-}"
  echo "CASA_RS_TUTORIAL_DATA_ROOT=${CASA_RS_TUTORIAL_DATA_ROOT:-}"
  echo "CASA_RS_TESTDATA_ROOT=${CASA_RS_TESTDATA_ROOT:-}"
  echo

  echo "==> Tutorial dataset preflight"
  require_tutorial_key alma/antennae/band7/calibrated-data
  require_tutorial_key vla/3c391/final-calibrated-mosaic-ms
  require_tutorial_key alma/first-look/twhya/selfcal-ms
  echo

  cat >"$outdir/README.md" <<EOF
# Wave 7 Ticket Closeout Run

Stamp: \`$stamp\`

This directory records targeted Wave 7 ticket closeout evidence.

- \`issue197-antennae-managed-output.json\`: casa-rs stage profile for #197 Antennae.
- \`issue197-antennae-timing.stderr\`: wall-clock timing for #197 Antennae.
- \`issue197-3c391-managed-output.json\`: casa-rs stage profile for #197 3C391.
- \`issue197-3c391-timing.stderr\`: wall-clock timing for #197 3C391.
- \`issue198-twhya-applycal-bench.log\`: TW Hydra applycal benchmark for #198.
- \`issue198-twhya-applycal-bench.json\`: machine-readable #198 benchmark summary.
- \`issue199-plotms-msexplore-bench.log\`: direct plotms versus msexplore timing for #199, or the exact plotms blocker.

Set \`WAVE7_RUN_TARGETED_BENCHES=1\` to run all targeted benchmarks, or set
\`WAVE7_RUN_ISSUE197=1\`, \`WAVE7_RUN_ISSUE198=1\`, or \`WAVE7_RUN_ISSUE199=1\`
to run one ticket slice.
EOF

  if [[ "$run_issue197" != "1" && "$run_issue198" != "1" && "$run_issue199" != "1" ]]; then
    echo "==> Skipping targeted benchmark runs; set WAVE7_RUN_TARGETED_BENCHES=1 to run them"
    return 0
  fi

  if [[ "$run_issue197" == "1" ]]; then
    run_optional "issue #197 imaging stage profiles" run_imager_profiles
  fi
  if [[ "$run_issue198" == "1" ]]; then
    run_optional "issue #198 TW Hydra applycal benchmark" run_calibration_apply_bench
  fi
  if [[ "$run_issue199" == "1" ]]; then
    run_optional "issue #199 plotms versus msexplore benchmark" run_plot_bench
  fi
}

main "$@" 2>&1 | tee "$log_file"
