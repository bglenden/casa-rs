#!/usr/bin/env bash

# Generate the Python UniFFI binding of casars-frontend-services and install it
# as crates/casars-python/python/casars/_frontend.py. With --check, generate it
# into a temporary directory and fail when the committed module differs.

set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
mode="write"
if [[ "${1:-}" == "--check" ]]; then
  mode="check"
  shift
fi
if [[ "$mode" == "check" ]]; then
  if [[ $# -ne 0 ]]; then
    echo "usage: $0 [--check] [output-directory]" >&2
    exit 2
  fi
  out_root="$(mktemp -d "${TMPDIR:-/tmp}/casars-frontend-bindings.XXXXXX")"
  trap 'rm -rf "$out_root"' EXIT
else
  out_root="${1:-$repo_root/target/frontend-bindings}"
fi
python_out="$out_root/python"
committed="$repo_root/crates/casars-python/python/casars/_frontend.py"

cd "$repo_root"

case "$(uname -s)" in
  Darwin)
    lib_name="libcasars_frontend_services.dylib"
    ;;
  Linux)
    lib_name="libcasars_frontend_services.so"
    ;;
  *)
    echo "unsupported platform for frontend binding generation: $(uname -s)" >&2
    exit 2
    ;;
esac

lib_path="${CARGO_TARGET_DIR:-$repo_root/target}/debug/$lib_name"

echo "==> Building Rust frontend services"
cargo build -p casars-frontend-services

echo "==> Generating Python UniFFI bindings"
mkdir -p "$python_out"
cargo run -p casars-frontend-services --bin casars-frontend-bindgen -- "$lib_path" "$python_out"
cp "$lib_path" "$python_out/$lib_name"
python3 scripts/package-python-frontend-binding.py \
  "$python_out/casars_frontend_services.py" \
  "$python_out/_frontend.py"
perl -pi -e 's/[ \t]+$//' "$python_out/_frontend.py"

if [[ "$mode" == "check" ]]; then
  if ! diff -u "$committed" "$python_out/_frontend.py"; then
    echo "frontend bindings are stale; run scripts/generate-frontend-bindings.sh" >&2
    exit 1
  fi
  echo "frontend bindings are current"
else
  cp "$python_out/_frontend.py" "$committed"
  echo "generated bindings under $out_root and updated $committed"
fi
