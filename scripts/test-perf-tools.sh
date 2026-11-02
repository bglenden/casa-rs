#!/usr/bin/env bash

# Runs the tests of the imaging performance and evidence tools under
# tools/perf/imager, of the simulation oracle and benchmark tools under
# tools/simulation, of the ATM oracle checker under tools/atmosphere, and of
# the tutorial-parity runner under scripts/tutorial_parity, whose adapters
# drive the task binaries' command lines, with pytest, which collects both the unittest and the
# pytest style files and imports perf_harness as a package. A tool whose
# casa-rs inputs go away should fail here, not linger. Needs pytest and
# numpy, scipy and matplotlib; astropy is optional (one FITS check skips
# without it). CI's perf_tools job installs them and enforces the run; a
# Claude Code cloud session (CLAUDE_CODE_REMOTE=true) installs them here on
# first use, since its SessionStart hook leaves them out to start quickly.
# Elsewhere `--if-available` (used by `just quick`) skips with a notice on a
# host whose Python lacks them.

set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

if_available=false
if [[ "${1:-}" == "--if-available" ]]; then
  if_available=true
  shift
fi

python_bin="${PYTHON:-python3}"
if [[ "${CLAUDE_CODE_REMOTE:-}" == "true" ]] &&
  ! "$python_bin" -c 'import pytest, numpy, scipy, matplotlib, astropy' 2>/dev/null; then
  echo "test-perf-tools.sh: installing the Python test packages for this cloud session" >&2
  "$python_bin" -m pip install --quiet --disable-pip-version-check --root-user-action=ignore \
    pytest numpy scipy matplotlib astropy ||
    echo "test-perf-tools.sh: pip install failed" >&2
fi
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
  "$python_bin" -m pytest -q -p no:cacheprovider tools/perf/imager tools/simulation tools/atmosphere \
  scripts/tutorial_parity "$@"
