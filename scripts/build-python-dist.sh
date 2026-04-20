#!/usr/bin/env bash

set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

python_bin="$("$repo_root/scripts/resolve-python.sh" 3.10)"
out_dir="${1:-dist/python}"
tmp_root="$(mktemp -d)"
cleanup() {
  rm -rf "$tmp_root"
  rm -rf crates/casars-python/.pytest_cache crates/casars-python/python/casars/__pycache__
  find crates/casars-python/python/casars -maxdepth 1 \( -name '_core*.so' -o -name '_core*.pyd' \) -delete
}
trap cleanup EXIT

venv_dir="$tmp_root/maturin-venv"

echo "==> Installing maturin into an isolated environment"
"$python_bin" -m venv "$venv_dir"
source "$venv_dir/bin/activate"
python -m pip install --upgrade pip
python -m pip install 'maturin>=1.7,<2'

mkdir -p "$out_dir"

echo "==> Building casa-rs-python wheel"
maturin build \
  --release \
  --manifest-path crates/casars-python/Cargo.toml \
  --interpreter "$python_bin" \
  --out "$out_dir"

echo "==> Building casa-rs-python sdist"
maturin sdist \
  --manifest-path crates/casars-python/Cargo.toml \
  --out "$out_dir"

echo "==> Built Python artifacts:"
find "$out_dir" -maxdepth 1 -type f | sort
