#!/usr/bin/env bash

set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

python_bin="$("$repo_root/scripts/resolve-python.sh" 3.10)"
tmp_root="$(mktemp -d)"
cleanup() {
  rm -rf "$tmp_root"
  rm -rf crates/casars-python/.pytest_cache crates/casars-python/python/casars/__pycache__
  find crates/casars-python/python/casars -maxdepth 1 \( -name '_core*.so' -o -name '_core*.pyd' \) -delete
}
trap cleanup EXIT

editable_venv="$tmp_root/editable-venv"

echo "==> Installing editable casars package and running Python tests"
"$python_bin" -m venv "$editable_venv"
source "$editable_venv/bin/activate"
python -m pip install -e 'crates/casars-python[test]'
pytest crates/casars-python/python/tests -q
deactivate
