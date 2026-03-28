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

echo "CASA_RS_TESTDATA_ROOT=${CASA_RS_TESTDATA_ROOT:-<unset>}"
echo "CASA_RS_CASA_PYTHON=${CASA_RS_CASA_PYTHON:-<unset>}"

if [[ -n "${CASA_RS_CASA_PYTHON:-}" ]]; then
  "$CASA_RS_CASA_PYTHON" - <<'PY'
import importlib.util
import sys

try:
    import casatasks
except Exception as exc:
    print(f"CASA preflight: cannot import casatasks: {exc}", file=sys.stderr)
    raise SystemExit(0)

has_plotms = hasattr(casatasks, "plotms") or importlib.util.find_spec("casaplotms") is not None
print(f"CASA preflight: casatasks ok, plotms={'yes' if has_plotms else 'no'}")
PY
fi

echo "Running listobs CASA parity..."
cargo test -p casacore-ms --test listobs_casa_parity -- --nocapture

echo "Running msexplore CASA parity..."
cargo test -p casacore-ms --test msexplore_casa_parity -- --nocapture
