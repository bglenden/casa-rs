#!/usr/bin/env bash

set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

python_bin="${PYTHON_BIN:-python3}"
tmp_root="$(mktemp -d)"
site_dir="${1:-$tmp_root/site}"
cleanup() {
  rm -rf "$tmp_root"
  rm -rf crates/casars-python/.pytest_cache crates/casars-python/python/casars/__pycache__
  find crates/casars-python/python/casars -maxdepth 1 \( -name '_core*.so' -o -name '_core*.pyd' \) -delete
}
trap cleanup EXIT

docs_venv="$tmp_root/docs-venv"

echo "==> Installing docs dependencies for casa-rs Python docs"
"$python_bin" -m venv "$docs_venv"
source "$docs_venv/bin/activate"
python -m pip install --upgrade pip
python -m pip install -e 'crates/casars-python[docs]'

echo "==> Building MkDocs site into $site_dir"
mkdocs build --strict --site-dir "$site_dir"
deactivate
