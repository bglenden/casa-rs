#!/usr/bin/env bash

set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

python_bin="${PYTHON_BIN:-python3}"
tmp_root="$(mktemp -d)"
cleanup() {
  rm -rf "$tmp_root"
  rm -rf crates/casars-python/.pytest_cache crates/casars-python/python/casars/__pycache__
  find crates/casars-python/python/casars -maxdepth 1 \( -name '_core*.so' -o -name '_core*.pyd' \) -delete
}
trap cleanup EXIT

editable_venv="$tmp_root/editable-venv"
wheel_venv="$tmp_root/wheel-venv"
wheelhouse="$tmp_root/wheelhouse"

echo "==> Building calibrate binary for protocol smoke checks"
cargo build -q -p casa-calibration --bin calibrate

echo "==> Installing editable casars package and running Python tests"
"$python_bin" -m venv "$editable_venv"
source "$editable_venv/bin/activate"
python -m pip install --upgrade pip
python -m pip install -e 'crates/casars-python[test]'
pytest crates/casars-python/python/tests -q
deactivate

echo "==> Building distributable casars wheel"
mkdir -p "$wheelhouse"
"$python_bin" -m pip wheel --no-deps --wheel-dir "$wheelhouse" crates/casars-python
wheel_path="$(find "$wheelhouse" -maxdepth 1 -name 'casa_rs_python-*.whl' | head -n 1)"
[[ -n "$wheel_path" ]] || {
  echo "failed to locate built casa-rs-python wheel" >&2
  exit 1
}

echo "==> Installing wheel into a clean environment and running smoke checks"
"$python_bin" -m venv "$wheel_venv"
source "$wheel_venv/bin/activate"
python -m pip install --upgrade pip
python -m pip install "$wheel_path"
CASARS_CALIBRATE_BIN="$repo_root/target/debug/calibrate" python - <<'PY'
import casars
from casars.tasks import calibrate

assert hasattr(casars, "Image")
assert hasattr(casars, "Table")
assert casars.__version__

info = calibrate.protocol_info()
assert info.protocol_name == "casa_calibration_task"
assert info.protocol_version == 1

schema = calibrate.schema()
assert "request_schema" in schema
assert "result_schema" in schema

calibrate.validate_signature_parity()
PY
deactivate
