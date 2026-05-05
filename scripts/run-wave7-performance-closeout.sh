#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

stamp="$(date -u +%Y%m%dT%H%M%SZ)"
outdir="${WAVE7_OUTDIR:-target/wave7-performance-closeout/$stamp}"
run_benches="${WAVE7_RUN_BENCHES:-0}"
bench_repeats="${WAVE7_BENCH_REPEATS:-3}"

mkdir -p "$outdir"
log_file="$outdir/wave7-performance-closeout.log"

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

check_tutorial_key() {
  local key="$1"
  if ! require_tutorial_key "$key"; then
    echo "wave7 preflight warning: missing tutorial registry key $key"
    if [[ -n "${CASA_RS_TUTORIAL_DATA_ROOT:-}" ]]; then
      echo "wave7 preflight hint: selected tutorial root may not contain this staged key"
    else
      echo "wave7 preflight hint: set CASA_RS_TUTORIAL_DATA_ROOT to the staged tutorial mirror for one-off local data"
    fi
  fi
}

main() {
  echo "wave7_performance_closeout_stamp=$stamp"
  echo "outdir=$outdir"
  echo "run_benches=$run_benches"
  echo "bench_repeats=$bench_repeats"
  echo "CASA_RS_CASA_PYTHON=${CASA_RS_CASA_PYTHON:-}"
  echo "CASA_RS_TUTORIAL_DATA_ROOT=${CASA_RS_TUTORIAL_DATA_ROOT:-}"
  echo "CASA_RS_TESTDATA_ROOT=${CASA_RS_TESTDATA_ROOT:-}"
  echo

  echo "==> Tutorial dataset preflight"
  check_tutorial_key alma/first-look/twhya/calibrated-ms
  check_tutorial_key alma/first-look/twhya/continuum-image
  check_tutorial_key alma/first-look/twhya/n2hp-image
  check_tutorial_key simulation/vla-ppdisk/model-fits
  check_tutorial_key alma/antennae/band7/calibrated-data
  check_tutorial_key alma/antennae/band7/reference-images
  check_tutorial_key alma/m100/band3-combine/aca-reference-images
  check_tutorial_key alma/m100/band3-combine/reference-images
  check_tutorial_key vla/3c391/final-calibrated-mosaic-ms
  echo

  cat >"$outdir/README.md" <<EOF
# Wave 7 Performance Closeout Run

Stamp: \`$stamp\`

This directory records a Wave 7 performance closeout harness run.

- \`wave7-performance-closeout.log\`: preflight and benchmark output
- \`bench-imager-vs-casa.log\`: optional imaging benchmark log
- \`bench-msexplore-vs-casa.log\`: optional plotting benchmark log
- \`bench-calibrate-vs-casa.log\`: optional calibration benchmark log

Set \`WAVE7_RUN_BENCHES=1\` to run the optional benchmark logs.
EOF

  if [[ "$run_benches" != "1" ]]; then
    echo "==> Skipping heavy benchmark runs; set WAVE7_RUN_BENCHES=1 to run them"
    return 0
  fi

  echo "==> Shared fixture preflight for benchmark scripts"
  cargo run -q -p casa-test-support --bin casatestdata-preflight -- --tier default-fixture
  echo

  echo "==> Running imaging benchmark"
  BENCH_REPEATS="$bench_repeats" scripts/bench-imager-vs-casa.sh \
    >"$outdir/bench-imager-vs-casa.log" 2>&1
  tail -40 "$outdir/bench-imager-vs-casa.log"
  echo

  echo "==> Running msexplore benchmark"
  BENCH_REPEATS="$bench_repeats" scripts/bench-msexplore-vs-casa.sh \
    >"$outdir/bench-msexplore-vs-casa.log" 2>&1
  tail -40 "$outdir/bench-msexplore-vs-casa.log"
  echo

  echo "==> Running calibration benchmark"
  CAL_BENCH_REPEATS="$bench_repeats" scripts/bench-calibrate-vs-casa.sh \
    >"$outdir/bench-calibrate-vs-casa.log" 2>&1
  tail -40 "$outdir/bench-calibrate-vs-casa.log"
}

main "$@" 2>&1 | tee "$log_file"
