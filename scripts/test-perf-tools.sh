#!/usr/bin/env bash

# Runs the tests of the imaging performance and evidence tools under
# tools/perf/imager, and of the simulation oracle and benchmark tools under
# tools/simulation, with pytest, which collects both the unittest and the
# pytest style files and imports perf_harness as a package. A tool whose
# casa-rs inputs go away should fail here, not linger. Needs pytest and
# numpy, scipy and matplotlib; astropy is optional (one FITS check skips
# without it). CI's perf_tools job installs them and enforces the run;
# `--if-available` (used by `just quick`) skips with a notice on a host
# whose Python lacks them.

set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

if_available=false
if [[ "${1:-}" == "--if-available" ]]; then
  if_available=true
  shift
fi

python_bin="${PYTHON:-python3}"
if ! "$python_bin" -c 'import pytest, numpy, scipy, matplotlib' 2>/dev/null; then
  message="$python_bin lacks pytest, numpy, scipy or matplotlib"
  if "$if_available"; then
    echo "test-perf-tools.sh: SKIPPED: $message; CI's perf_tools job runs these tests" >&2
    exit 0
  fi
  echo "test-perf-tools.sh: $message; install them or set PYTHON" >&2
  exit 2
fi

PYTHONDONTWRITEBYTECODE=1 MPLBACKEND=Agg \
  "$python_bin" -m pytest -q -p no:cacheprovider tools/perf/imager tools/simulation "$@"
