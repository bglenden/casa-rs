#!/usr/bin/env bash

set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"
export CARGO_INCREMENTAL=0

extras="test"
editable_only=false
for flag in "$@"; do
  case "$flag" in
    --plot) extras="test,plot" ;;
    --editable-only) editable_only=true ;;
    *)
      echo "usage: $0 [--plot] [--editable-only]" >&2
      exit 2
      ;;
  esac
done

python_bin="$("$repo_root/scripts/resolve-python.sh" 3.10)"
tmp_root="$(mktemp -d "${TMPDIR:-/tmp}/casars-python.XXXXXXXX")"
cleanup() {
  rm -rf "$tmp_root"
  rm -rf frontends/casars-python/.pytest_cache frontends/casars-python/python/casars/__pycache__
  find frontends/casars-python/python/casars -maxdepth 1 \( -name '_core*.so' -o -name '_core*.pyd' \) -delete
}
trap cleanup EXIT

# casars.tools.simulator loads a measures runtime at sm.open; unless one is
# configured, test against the source release snapshot, as the Rust tests in CI do.
if [[ -z "${CASA_RS_MEASURESPATH:-}" ]]; then
  measures_root="$tmp_root/casa-measures"
  "$python_bin" scripts/install-measures-snapshot.py "$measures_root"
  export CASA_RS_MEASURESPATH="$measures_root"
fi

editable_venv="$tmp_root/editable-venv"

echo "==> Installing editable casars package and running Python tests"
"$python_bin" -m venv "$editable_venv"
source "$editable_venv/bin/activate"
# The interrupt tests use the opt-in checkpoints in the editable build.
MATURIN_PEP517_ARGS="${MATURIN_PEP517_ARGS:-} --features transform-test-hooks" \
  python -m pip install -e "frontends/casars-python[$extras]"
pytest frontends/casars-python/python/tests -q
deactivate
if [[ "$editable_only" == true ]]; then
  exit 0
fi

# The built wheel and sdist are tested too: an editable checkout can import
# files a packaged install lacks. Their tests run from outside the source tree
# and first prove that `casars` and `casars._core` come from the install.
tests_dir="$repo_root/frontends/casars-python/python/tests"
test_installed() {
  local venv="$1"
  shift
  (
    cd "$tmp_root"
    "$venv/bin/python" - "$venv" <<'PY'
import pathlib
import sys

import casars
import casars._core
import casars._plot_document

venv = pathlib.Path(sys.argv[1]).resolve()
for module in (casars, casars._core, casars._plot_document):
    path = pathlib.Path(module.__file__).resolve()
    if venv not in path.parents:
        raise SystemExit(f"{module.__name__} imported from {path}, not from {venv}")
PY
    "$venv/bin/pytest" -q -p no:cacheprovider "$@"
  )
}

dist_dir="$tmp_root/dist"
echo "==> Building the casars wheel and sdist"
"$repo_root/scripts/build-python-dist.sh" "$dist_dir"
shopt -s nullglob
wheels=("$dist_dir"/*.whl)
sdists=("$dist_dir"/*.tar.gz)
shopt -u nullglob
if [[ ${#wheels[@]} -ne 1 || ${#sdists[@]} -ne 1 ]]; then
  echo "expected one wheel and one sdist in $dist_dir" >&2
  exit 1
fi

wheel_venv="$tmp_root/wheel-venv"
echo "==> Installing the built wheel and running Python tests"
"$python_bin" -m venv "$wheel_venv"
"$wheel_venv/bin/python" -m pip install "${wheels[0]}[$extras]"

# Run from a separate workspace with an empty PATH, even if the caller
# already has a suite installed or task binaries built in the checkout.
echo "==> Running an installed wheel task with CLI commands unavailable"
no_cli_workspace="$tmp_root/wheel-workspace"
no_cli_path="$tmp_root/no-commands"
mkdir "$no_cli_workspace" "$no_cli_path"
tar -xzf libs/casacore/casa-ms/tests/fixtures/mssel_test_small.ms.tgz -C "$no_cli_workspace"
(
  cd "$no_cli_workspace"
  for variable in $(compgen -e); do
    case "$variable" in
      CASARS_*) unset "$variable" ;;
    esac
  done
  PATH="$no_cli_path" CARGO_TARGET_DIR="$no_cli_path" "$wheel_venv/bin/python" -I - "$wheel_venv" <<'PY'
from pathlib import Path
import shutil
import sys

import casars._core
from casars import tasks

assert Path(sys.argv[1]).resolve() in Path(casars._core.__file__).resolve().parents
assert shutil.which("flagmanager") is None
ms = Path("mssel_test_small.ms")
completion = tasks.run("flagmanager", vis=ms, mode="list")
assert completion.successful, completion.describe()
assert tasks.flagmanager(vis=ms, mode="list") == {"MS": str(ms.resolve())}
PY
)
test_installed "$wheel_venv" "$tests_dir"

# A development build is enough to prove that the sdist is complete and
# builds. Use a fresh target: reproducible sdist timestamps can make Cargo
# reuse stale workspace artifacts from a previous archive in a shared target.
sdist_venv="$tmp_root/sdist-venv"
echo "==> Installing from the sdist and running the binding tests"
"$python_bin" -m venv "$sdist_venv"
MATURIN_PEP517_ARGS="--profile dev" \
  CARGO_TARGET_DIR="$tmp_root/sdist-target" \
  "$sdist_venv/bin/python" -m pip install "${sdists[0]}[$extras]"
test_installed "$sdist_venv" \
  "$tests_dir/test_binding_snapshots.py" \
  "$tests_dir/test_parameters.py" \
  "$tests_dir/test_data.py" \
  "$tests_dir/test_plot_document.py" \
  "$tests_dir/test_simutil_graphics.py" \
  "$tests_dir/test_simutil.py::test_the_class_has_casas_signatures"
