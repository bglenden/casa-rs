#!/usr/bin/env bash

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
swift_out="$out_root/swift"

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

lib_path="$repo_root/target/debug/$lib_name"

echo "==> Building Rust frontend services"
cargo build -p casars-frontend-services

echo "==> Generating Python UniFFI bindings"
mkdir -p "$python_out"
cargo run -p casars-frontend-services --bin casars-frontend-bindgen -- python "$lib_path" "$python_out"
cp "$lib_path" "$python_out/$lib_name"
python3 scripts/package-python-frontend-binding.py \
  "$python_out/casars_frontend_services.py" \
  "$python_out/_frontend.py"
perl -pi -e 's/[ \t]+$//' "$python_out/_frontend.py"

echo "==> Generating Swift UniFFI bindings"
mkdir -p "$swift_out"
cargo run -p casars-frontend-services --bin casars-frontend-bindgen -- swift "$lib_path" "$swift_out"
perl -pi -e 's/[ \t]+$//' "$swift_out/CasarsFrontendServices.swift"
perl -pi -e 's/[ \t]+$//' "$swift_out/CasarsFrontendServicesFFI.h"

if [[ "$mode" == "write" && -d "$repo_root/apps/casars-mac/Sources/CasarsFrontendServices" ]]; then
  cp "$swift_out/CasarsFrontendServices.swift" \
    "$repo_root/apps/casars-mac/Sources/CasarsFrontendServices/CasarsFrontendServices.swift"
  cp "$swift_out/CasarsFrontendServicesFFI.h" \
    "$repo_root/apps/casars-mac/Sources/CasarsFrontendServicesFFI/CasarsFrontendServicesFFI.h"
  cp "$swift_out/CasarsFrontendServicesFFI.modulemap" \
    "$repo_root/apps/casars-mac/Sources/CasarsFrontendServicesFFI/module.modulemap"
  cp "$python_out/_frontend.py" \
    "$repo_root/crates/casars-python/python/casars/_frontend.py"
fi

if [[ "$mode" == "check" ]]; then
  check_root="${CASARS_FRONTEND_BINDINGS_CHECK_ROOT:-$repo_root/apps/casars-mac/Sources}"
  status=0
  compare_artifact() {
    local generated="$1"
    local committed="$2"
    if ! diff -u "$committed" "$generated"; then
      status=1
    fi
  }
  compare_artifact \
    "$swift_out/CasarsFrontendServices.swift" \
    "$check_root/CasarsFrontendServices/CasarsFrontendServices.swift"
  compare_artifact \
    "$swift_out/CasarsFrontendServicesFFI.h" \
    "$check_root/CasarsFrontendServicesFFI/CasarsFrontendServicesFFI.h"
  compare_artifact \
    "$swift_out/CasarsFrontendServicesFFI.modulemap" \
    "$check_root/CasarsFrontendServicesFFI/module.modulemap"
  compare_artifact \
    "$python_out/_frontend.py" \
    "$repo_root/crates/casars-python/python/casars/_frontend.py"
  if [[ "$status" -ne 0 ]]; then
    echo "frontend bindings are stale; run scripts/generate-frontend-bindings.sh" >&2
    exit "$status"
  fi
  echo "frontend bindings are current"
else
  echo "generated bindings under $out_root"
fi
