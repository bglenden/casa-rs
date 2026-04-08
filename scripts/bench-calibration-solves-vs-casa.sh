#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

if [[ -z "${CASA_RS_TESTDATA_ROOT:-}" && -d "$HOME/SoftwareProjects/casatestdata" ]]; then
  export CASA_RS_TESTDATA_ROOT="$HOME/SoftwareProjects/casatestdata"
fi

if [[ -z "${CASA_RS_CASA_PYTHON:-}" && -x "$HOME/SoftwareProjects/casa-build/venv/bin/python" ]]; then
  export CASA_RS_CASA_PYTHON="$HOME/SoftwareProjects/casa-build/venv/bin/python"
fi

if [[ $# -gt 1 ]]; then
  echo "usage: $0 [measurementset-path]" >&2
  exit 2
fi

if [[ $# -eq 1 ]]; then
  ms_path="$1"
elif [[ -n "${CASA_RS_TESTDATA_ROOT:-}" ]]; then
  ms_path="$CASA_RS_TESTDATA_ROOT/measurementset/vla/ngc5921.ms"
else
  echo "error: pass a MeasurementSet path or set CASA_RS_TESTDATA_ROOT" >&2
  exit 2
fi

if [[ ! -d "$ms_path" ]]; then
  echo "error: MeasurementSet not found: $ms_path" >&2
  exit 2
fi

if [[ -z "${CASA_RS_CASA_PYTHON:-}" ]]; then
  echo "error: CASA_RS_CASA_PYTHON is not set and no default CASA python was found" >&2
  exit 2
fi

repeats="${CAL_SOLVE_BENCH_REPEATS:-3}"
workflows="${CAL_SOLVE_BENCH_WORKFLOWS:-solve_gain,solve_bandpass,fluxscale}"
field="${CAL_SOLVE_BENCH_FIELD:-0}"
bandpass_field="${CAL_SOLVE_BENCH_BANDPASS_FIELD:-$field}"
gain_field="${CAL_SOLVE_BENCH_GAIN_FIELD:-$field}"
ap_field="${CAL_SOLVE_BENCH_AP_FIELD:-0,1}"
spw="${CAL_SOLVE_BENCH_SPW:-0}"
refant="${CAL_SOLVE_BENCH_REFANT:-VA15}"
flux_reference="${CAL_SOLVE_BENCH_REFERENCE:-1331+30500002_0}"
flux_transfer="${CAL_SOLVE_BENCH_TRANSFER:-1445+09900002_0}"

echo "ms_path=$ms_path"
echo "CASA_RS_CASA_PYTHON=$CASA_RS_CASA_PYTHON"
echo "repeats=$repeats"
echo "workflows=$workflows"
echo "gain_field=$gain_field bandpass_field=$bandpass_field ap_field=$ap_field spw=$spw refant=$refant"
echo "flux_reference=$flux_reference flux_transfer=$flux_transfer"
echo "timing_includes=full command wall-clock, excludes prerequisite table generation"
echo

cargo build --release -p casa-calibration --bin calibrate >/dev/null

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

median_from_file() {
  python3 - "$1" <<'PY'
import statistics
import sys

with open(sys.argv[1], "r", encoding="utf-8") as handle:
    values = [float(line.strip()) for line in handle if line.strip()]
if not values:
    raise SystemExit("no benchmark values provided")
print(f"{statistics.median(values):.6f}")
PY
}

workflow_enabled() {
  local needle="$1"
  IFS=',' read -r -a selected <<<"$workflows"
  for workflow in "${selected[@]}"; do
    if [[ "${workflow// /}" == "$needle" ]]; then
      return 0
    fi
  done
  return 1
}

echo "Generating CASA prerequisite calibration tables..."
phase_gcal="$tmpdir/phase.gcal"
amp_gcal="$tmpdir/amp.gcal"
CASA_RS_BENCH_MS="$ms_path" \
CASA_RS_BENCH_PHASE="$phase_gcal" \
CASA_RS_BENCH_AMP="$amp_gcal" \
CASA_RS_BENCH_GAIN_FIELD="$gain_field" \
CASA_RS_BENCH_AP_FIELD="$ap_field" \
CASA_RS_BENCH_SPW="$spw" \
CASA_RS_BENCH_REFANT="$refant" \
  "$CASA_RS_CASA_PYTHON" - <<'PY'
import os
from casatasks import gaincal

gaincal(
    vis=os.environ["CASA_RS_BENCH_MS"],
    caltable=os.environ["CASA_RS_BENCH_PHASE"],
    field=os.environ["CASA_RS_BENCH_GAIN_FIELD"],
    spw=os.environ["CASA_RS_BENCH_SPW"],
    solint="inf",
    refant=os.environ["CASA_RS_BENCH_REFANT"],
    calmode="p",
    minsnr=0.0,
)

gaincal(
    vis=os.environ["CASA_RS_BENCH_MS"],
    caltable=os.environ["CASA_RS_BENCH_AMP"],
    field=os.environ["CASA_RS_BENCH_AP_FIELD"],
    spw=os.environ["CASA_RS_BENCH_SPW"],
    solint="inf",
    refant=os.environ["CASA_RS_BENCH_REFANT"],
    calmode="ap",
    gaintable=[os.environ["CASA_RS_BENCH_PHASE"]],
    minsnr=0.0,
)
PY
echo

bench_rust() {
  local label="$1"
  shift
  local times="$tmpdir/${label}-rust-times.txt"
  : >"$times"
  echo "Rust $label timings (seconds):"
  for run in $(seq 1 "$repeats"); do
    local stderr="$tmpdir/${label}-rust-$run.stderr"
    local run_args=("$@")
    local i
    for ((i = 0; i < ${#run_args[@]}; i++)); do
      if [[ "${run_args[$i]}" == "--out" && $((i + 1)) -lt ${#run_args[@]} ]]; then
        local out_path="${run_args[$((i + 1))]}"
        local extension=""
        local stem="$out_path"
        if [[ "$out_path" == *.* ]]; then
          extension=".${out_path##*.}"
          stem="${out_path%.*}"
        fi
        run_args[$((i + 1))]="${stem}-${run}${extension}"
      fi
    done
    /usr/bin/time -lp target/release/calibrate "${run_args[@]}" >/dev/null 2>"$stderr"
    local real_seconds
    real_seconds="$(awk '/^real / {print $2}' "$stderr")"
    printf "  run=%s real=%s\n" "$run" "$real_seconds"
    printf "%s\n" "$real_seconds" >>"$times"
  done
  echo "  median=$(median_from_file "$times")"
  echo
}

bench_casa_gain() {
  local label="$1"
  local times="$tmpdir/${label}-casa-times.txt"
  : >"$times"
  echo "CASA $label timings (seconds):"
  for run in $(seq 1 "$repeats"); do
    local out="$tmpdir/${label}-casa-$run.gcal"
    local seconds
    seconds="$(
      CASA_RS_BENCH_MS="$ms_path" \
      CASA_RS_BENCH_OUT="$out" \
      CASA_RS_BENCH_FIELD="$gain_field" \
      CASA_RS_BENCH_SPW="$spw" \
      CASA_RS_BENCH_REFANT="$refant" \
        "$CASA_RS_CASA_PYTHON" - <<'PY'
import os
import time
from casatasks import gaincal

started = time.perf_counter()
gaincal(
    vis=os.environ["CASA_RS_BENCH_MS"],
    caltable=os.environ["CASA_RS_BENCH_OUT"],
    field=os.environ["CASA_RS_BENCH_FIELD"],
    spw=os.environ["CASA_RS_BENCH_SPW"],
    solint="inf",
    refant=os.environ["CASA_RS_BENCH_REFANT"],
    calmode="p",
    minsnr=0.0,
)
print(f"{time.perf_counter() - started:.6f}")
PY
)"
    printf "  run=%s real=%s\n" "$run" "$seconds"
    printf "%s\n" "$seconds" >>"$times"
  done
  echo "  median=$(median_from_file "$times")"
  echo
}

bench_casa_bandpass() {
  local label="$1"
  local times="$tmpdir/${label}-casa-times.txt"
  : >"$times"
  echo "CASA $label timings (seconds):"
  for run in $(seq 1 "$repeats"); do
    local out="$tmpdir/${label}-casa-$run.bcal"
    local seconds
    seconds="$(
      CASA_RS_BENCH_MS="$ms_path" \
      CASA_RS_BENCH_OUT="$out" \
      CASA_RS_BENCH_FIELD="$bandpass_field" \
      CASA_RS_BENCH_SPW="$spw" \
      CASA_RS_BENCH_REFANT="$refant" \
      CASA_RS_BENCH_GAIN="$phase_gcal" \
        "$CASA_RS_CASA_PYTHON" - <<'PY'
import os
import time
from casatasks import bandpass

started = time.perf_counter()
bandpass(
    vis=os.environ["CASA_RS_BENCH_MS"],
    caltable=os.environ["CASA_RS_BENCH_OUT"],
    field=os.environ["CASA_RS_BENCH_FIELD"],
    spw=os.environ["CASA_RS_BENCH_SPW"],
    refant=os.environ["CASA_RS_BENCH_REFANT"],
    bandtype="B",
    solint="inf",
    gaintable=[os.environ["CASA_RS_BENCH_GAIN"]],
)
print(f"{time.perf_counter() - started:.6f}")
PY
)"
    printf "  run=%s real=%s\n" "$run" "$seconds"
    printf "%s\n" "$seconds" >>"$times"
  done
  echo "  median=$(median_from_file "$times")"
  echo
}

bench_casa_fluxscale() {
  local label="$1"
  local times="$tmpdir/${label}-casa-times.txt"
  : >"$times"
  echo "CASA $label timings (seconds):"
  for run in $(seq 1 "$repeats"); do
    local out="$tmpdir/${label}-casa-$run.gcal"
    local seconds
    seconds="$(
      CASA_RS_BENCH_MS="$ms_path" \
      CASA_RS_BENCH_IN="$amp_gcal" \
      CASA_RS_BENCH_OUT="$out" \
      CASA_RS_BENCH_REFERENCE="$flux_reference" \
      CASA_RS_BENCH_TRANSFER="$flux_transfer" \
        "$CASA_RS_CASA_PYTHON" - <<'PY'
import os
import time
from casatasks import fluxscale

started = time.perf_counter()
fluxscale(
    vis=os.environ["CASA_RS_BENCH_MS"],
    caltable=os.environ["CASA_RS_BENCH_IN"],
    fluxtable=os.environ["CASA_RS_BENCH_OUT"],
    reference=os.environ["CASA_RS_BENCH_REFERENCE"],
    transfer=os.environ["CASA_RS_BENCH_TRANSFER"],
)
print(f"{time.perf_counter() - started:.6f}")
PY
)"
    printf "  run=%s real=%s\n" "$run" "$seconds"
    printf "%s\n" "$seconds" >>"$times"
  done
  echo "  median=$(median_from_file "$times")"
  echo
}

if workflow_enabled "solve_gain"; then
  bench_rust \
    "solve_gain" \
    --ms "$ms_path" \
    --mode solve_gain \
    --out "$tmpdir/rust-solve_gain.gcal" \
    --refant "$refant" \
    --gain-type g \
    --mode-gain p \
    --solint inf \
    --field "$gain_field" \
    --spw "$spw" \
    --format json
  bench_casa_gain "solve_gain"
fi

if workflow_enabled "solve_bandpass"; then
  bench_rust \
    "solve_bandpass" \
    --ms "$ms_path" \
    --mode solve_bandpass \
    --out "$tmpdir/rust-solve_bandpass.bcal" \
    --refant "$refant" \
    --bandtype b \
    --gaintables "$phase_gcal" \
    --field "$bandpass_field" \
    --spw "$spw" \
    --format json
  bench_casa_bandpass "solve_bandpass"
fi

if workflow_enabled "fluxscale"; then
  bench_rust \
    "fluxscale" \
    --mode fluxscale \
    --in "$amp_gcal" \
    --out "$tmpdir/rust-fluxscale.gcal" \
    --reference "$flux_reference" \
    --transfer "$flux_transfer" \
    --format json
  bench_casa_fluxscale "fluxscale"
fi
