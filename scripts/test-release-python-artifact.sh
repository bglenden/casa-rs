#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: $0 <wheel-or-sdist> <expected-python-version>" >&2
  exit 2
fi

repo_root="$(git rev-parse --show-toplevel)"
tmp_root="$(mktemp -d)"
trap 'rm -rf "$tmp_root"' EXIT
python_bin="$tmp_root/venv/bin/python"

python -m venv "$tmp_root/venv"
"$python_bin" -m pip install "$1"

measures_root="$tmp_root/casa-measures"
python "$repo_root/scripts/install-measures-snapshot.py" "$measures_root"
mkdir "$tmp_root/work" "$tmp_root/no-commands"
tar -xzf "$repo_root/libs/casacore/casa-ms/tests/fixtures/mssel_test_small.ms.tgz" -C "$tmp_root/work"
cd "$tmp_root/work"

# Run the packaged task from an empty workspace apart from the fixture, with
# its own measures runtime and no CLI or Cargo commands available.
PATH="$tmp_root/no-commands" CARGO_TARGET_DIR="$tmp_root/no-commands" \
  CASA_RS_MEASURESPATH="$measures_root" "$python_bin" -I - "$2" <<'PY'
import inspect
from pathlib import Path
import sys

import casars
import casars._core
from casars import tasks
from casars.parameters import TaskParameters

for module in (casars, casars._core):
    module_path = Path(module.__file__).resolve()
    assert Path(sys.prefix).resolve() in module_path.parents, module_path
assert casars.__version__ == sys.argv[1], (casars.__version__, sys.argv[1])

for name in ("calibrate", "importvla", "msexplore", "tclean", "immoments", "impbcor", "exportfits", "importfits"):
    assert callable(getattr(tasks, name)), name
assert "vis" in inspect.signature(tasks.calibrate).parameters
assert "imagename" in inspect.signature(tasks.immoments).parameters
assert TaskParameters.defaults("calibrate").surface == "calibrate"

# This field and spectral window contain unflagged cross-correlations.
result = tasks.tclean(
    vis="mssel_test_small.ms",
    imagename="smoke",
    field="1",
    spw="1",
    datacolumn="data",
    imsize=32,
    cell="0.04arcsec",
    backend="cpu",
    restoration=False,
)
assert result["nmajordone"] == 1, result
assert Path("smoke.psf").is_dir()
assert Path("smoke.residual").is_dir()
print(f"Installed casa-rs-python {casars.__version__} ran tclean and wrote PSF and residual images")
PY
