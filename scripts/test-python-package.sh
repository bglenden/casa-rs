#!/usr/bin/env bash

set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"
export CARGO_INCREMENTAL=0

extras="test"
case "${1:-}" in
  "") ;;
  --plot) extras="test,plot" ;;
  *)
    echo "usage: $0 [--plot]" >&2
    exit 2
    ;;
esac

python_bin="$("$repo_root/scripts/resolve-python.sh" 3.10)"
tmp_root="$(mktemp -d)"
cleanup() {
  rm -rf "$tmp_root"
  rm -rf crates/casars-python/.pytest_cache crates/casars-python/python/casars/__pycache__
  find crates/casars-python/python/casars -maxdepth 1 \( -name '_core*.so' -o -name '_core*.pyd' \) -delete
}
trap cleanup EXIT

# casars.tools.simulator loads a measures runtime at sm.open; unless one is
# configured, test against the bundled snapshot, as the Rust tests in CI do.
if [[ -z "${CASA_RS_MEASURESPATH:-}" ]]; then
  measures_root="$tmp_root/casa-measures"
  mkdir -p "$measures_root"
  tar -xzf crates/casa-measures-data/data/casa-measures-runtime.tar.gz -C "$measures_root"
  cp crates/casa-measures-data/data/casa-measures-runtime.provenance.json \
    "$measures_root/.casa-rs-measures-provenance.json"
  export CASA_RS_MEASURESPATH="$measures_root"
fi

# The end-to-end tests start a real task executable, which a development
# checkout's resolver finds in this build (#931); they must not skip here.
echo "==> Building the task executables the end-to-end tests run"
cargo build -q -p casa-ms --bin flagmanager --bin flagdata -p casa-calibration --bin calibrate
cargo build -q -p casa-images --bin immath
cargo build -q -p casars-imexplore --bin imexplore
cargo build -q -p casars-msconvert --bin msconvert
export CASARS_REQUIRE_TASK_BINARIES=1

editable_venv="$tmp_root/editable-venv"

echo "==> Installing editable casars package and running Python tests"
"$python_bin" -m venv "$editable_venv"
source "$editable_venv/bin/activate"
python -m pip install -e "crates/casars-python[$extras]"
pytest crates/casars-python/python/tests -q
deactivate

# The built wheel and sdist are tested too: an editable checkout can import
# files a packaged install lacks. Their tests run from outside the source tree
# and first prove that `casars` and `casars._core` come from the install.
tests_dir="$repo_root/crates/casars-python/python/tests"
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

venv = pathlib.Path(sys.argv[1]).resolve()
for module in (casars, casars._core):
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
test_installed "$wheel_venv" "$tests_dir"

# A development build is enough to prove that the sdist is complete and
# builds; the shared target directory saves recompiling its dependencies.
sdist_venv="$tmp_root/sdist-venv"
echo "==> Installing from the sdist and running the binding tests"
"$python_bin" -m venv "$sdist_venv"
MATURIN_PEP517_ARGS="--profile dev" \
  CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$repo_root/target}" \
  "$sdist_venv/bin/python" -m pip install "${sdists[0]}[$extras]"
test_installed "$sdist_venv" \
  "$tests_dir/test_binding_snapshots.py" \
  "$tests_dir/test_parameters.py" \
  "$tests_dir/test_data.py"
