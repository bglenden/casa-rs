#!/usr/bin/env bash

set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

if [[ -z "${CASA_RS_TESTDATA_ROOT:-}" && -d "$HOME/SoftwareProjects/casatestdata" ]]; then
  export CASA_RS_TESTDATA_ROOT="$HOME/SoftwareProjects/casatestdata"
fi

if [[ -z "${CASA_RS_CASA_PYTHON:-}" && -x "$HOME/SoftwareProjects/casa-build/venv/bin/python" ]]; then
  export CASA_RS_CASA_PYTHON="$HOME/SoftwareProjects/casa-build/venv/bin/python"
fi

if [[ $# -gt 1 ]]; then
  echo "usage: $0 [output-directory]" >&2
  exit 2
fi

if [[ $# -eq 1 ]]; then
  outdir="$1"
else
  outdir="$(mktemp -d)"
fi

ms_path="${CASA_RS_TESTDATA_ROOT:-}/measurementset/vla/ngc5921.ms"
if [[ ! -d "$ms_path" ]]; then
  echo "error: MeasurementSet not found: $ms_path" >&2
  exit 2
fi
if [[ -z "${CASA_RS_CASA_PYTHON:-}" ]]; then
  echo "error: CASA_RS_CASA_PYTHON is not set and no default CASA python was found" >&2
  exit 2
fi

mkdir -p "$outdir"

CASA_RS_CAL_MS="$ms_path" \
CASA_RS_CAL_OUT="$outdir" \
  "$CASA_RS_CASA_PYTHON" - <<'PY'
import os
from casatasks import bandpass, gaincal

vis = os.environ["CASA_RS_CAL_MS"]
out = os.environ["CASA_RS_CAL_OUT"]
phase = os.path.join(out, "phase.gcal")
tsolve = os.path.join(out, "t.gcal")
bp = os.path.join(out, "b.bcal")

gaincal(
    vis=vis,
    caltable=phase,
    field="0",
    spw="0",
    solint="inf",
    refant="VA15",
    calmode="p",
    minsnr=0.0,
)
gaincal(
    vis=vis,
    caltable=tsolve,
    field="0",
    spw="0",
    solint="inf",
    refant="VA15",
    calmode="p",
    gaintype="T",
    minsnr=0.0,
)
bandpass(
    vis=vis,
    caltable=bp,
    field="0",
    spw="0",
    solint="inf",
    refant="VA15",
    bandtype="B",
    gaintable=[phase],
    minsnr=0.0,
)
PY

echo "Generated CASA calibration exemplars in: $outdir"
echo "  $outdir/phase.gcal"
echo "  $outdir/t.gcal"
echo "  $outdir/b.bcal"
